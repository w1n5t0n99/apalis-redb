use apalis_core::backend::WorkerFilter;
use redb::ReadableTable;

use crate::{
    Config, Connection,
    connection::run_blocking,
    error::Error,
    queries::{WorkerRecord, current_timestamp, push_active, reset_to_pending, tables, write},
};

/// Re-enqueues tasks held by workers whose heartbeat has expired, and
/// removes those workers. Returns the number of tasks re-enqueued.
pub(crate) async fn reenqueue_orphaned(
    conn: &Connection,
    config: &Config,
    worker_filter: &WorkerFilter,
) -> Result<u32, Error> {
    let now = current_timestamp();
    let expired_before = match worker_filter {
        WorkerFilter::Only(_) => now.saturating_sub(config.heartbeat_interval.as_secs()),
        _ => now.saturating_sub(config.orphaned_duration().as_secs()),
    };
    let db = conn.db();
    let cfg = config.clone();
    let filter = worker_filter.clone();
    let processed = run_blocking(move || {
        write(&db, cfg.durability, |txn| {
            let mut workers = txn.open_table(tables::workers(&cfg.workers_set()))?;
            let mut expired = Vec::new();
            for entry in workers.iter()? {
                let (name, record) = entry?;
                let name = name.value();
                let selected = match &filter {
                    WorkerFilter::Only(only) => name == only,
                    WorkerFilter::AllExcept(except) => name != except,
                    _ => true,
                };
                if !selected {
                    continue;
                }
                let record: WorkerRecord = serde_json::from_str(record.value())?;
                if record.last_seen <= expired_before {
                    expired.push(name.to_owned());
                }
            }

            let mut inflight =
                txn.open_multimap_table(tables::inflight(&cfg.inflight_jobs_set()))?;
            let mut active = txn.open_table(tables::active(&cfg.active_jobs_list()))?;
            let mut meta = txn.open_table(tables::meta(&cfg.job_meta_hash()))?;
            let mut processed = 0;
            for worker in &expired {
                let task_ids: Vec<String> = inflight
                    .remove_all(worker.as_str())?
                    .map(|id| id.map(|id| id.value().to_owned()))
                    .collect::<Result<_, _>>()?;
                for task_id in &task_ids {
                    push_active(&mut active, task_id)?;
                    reset_to_pending(&mut meta, task_id)?;
                }
                processed += task_ids.len() as u32;
                workers.remove(worker.as_str())?;
            }
            Ok(processed)
        })
    })
    .await?;
    if config.emit_events && processed > 0 {
        conn.notify(config.queue.as_ref());
    }
    Ok(processed)
}
