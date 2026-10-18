use apalis_core::{backend::TaskResult, task::status::Status, worker::context::WorkerContext};
use serde_json::Value;

use crate::{
    Config, Connection,
    connection::run_blocking,
    error::Error,
    queries::{current_timestamp, read_meta, tables, write, write_meta},
};

pub(crate) async fn handle_results(
    conn: &Connection,
    ack_payloads: Vec<TaskResult<Value>>,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error> {
    if ack_payloads.is_empty() {
        return Ok(());
    }
    let db = conn.db();
    let cfg = config.clone();
    let worker = worker.name().to_owned();
    let retried = run_blocking(move || {
        write(&db, cfg.durability, |txn| {
            handle_results_txn(txn, &cfg, &worker, ack_payloads)
        })
    })
    .await?;
    if config.emit_events && retried > 0 {
        conn.notify(config.queue.as_ref());
    }
    Ok(())
}

/// Returns the number of tasks scheduled for a retry
fn handle_results_txn(
    txn: &redb::WriteTransaction,
    config: &Config,
    worker: &str,
    ack_payloads: Vec<TaskResult<Value>>,
) -> Result<u32, Error> {
    let inflight_name = config.inflight_jobs_set();
    let done_name = config.done_jobs_set();
    let dead_name = config.dead_jobs_set();
    let scheduled_name = config.scheduled_jobs_set();
    let meta_name = config.job_meta_hash();

    let mut inflight = txn.open_multimap_table(tables::inflight(&inflight_name))?;
    let mut done = txn.open_table(tables::timeline(&done_name))?;
    let mut dead = txn.open_table(tables::timeline(&dead_name))?;
    let mut scheduled = txn.open_table(tables::timeline(&scheduled_name))?;
    let mut meta = txn.open_table(tables::meta(&meta_name))?;

    let now = current_timestamp();
    let mut retried = 0;

    for payload in ack_payloads {
        let task_id = payload.task_id.to_string();
        if !inflight.remove(worker, task_id.as_str())? {
            // The task was reclaimed from this worker, e.g. after missed heartbeats
            tracing::warn!(task_id, "ignoring result for task not held by worker");
            continue;
        }
        let Some(mut task_meta) = read_meta(&meta, &task_id)? else {
            tracing::warn!(task_id, "acknowledged task has no metadata");
            continue;
        };

        task_meta.attempts = payload.attempt;
        task_meta.result = Some(payload.result);

        match payload.status {
            Status::Done => {
                done.insert((now, task_id.as_str()), ())?;
                task_meta.status = Status::Done;
                task_meta.done_at = Some(now);
            }
            Status::Failed | Status::Pending if payload.attempt < task_meta.max_attempts => {
                // Simple linear backoff, mirroring apalis-redis
                let run_at = now + payload.attempt as u64;
                scheduled.insert((run_at, task_id.as_str()), ())?;
                task_meta.status = Status::Pending;
                task_meta.run_at = Some(run_at);
                retried += 1;
            }
            Status::Killed => {
                dead.insert((now, task_id.as_str()), ())?;
                task_meta.status = Status::Killed;
                task_meta.done_at = Some(now);
            }
            _ => {
                dead.insert((now, task_id.as_str()), ())?;
                task_meta.status = Status::Failed;
                task_meta.done_at = Some(now);
            }
        }
        write_meta(&mut meta, &task_id, &task_meta)?;
    }

    Ok(retried)
}
