use apalis_core::task::status::Status;
use redb::ReadableTable;
use ulid::Ulid;

use crate::{
    Config, Connection, RedbTask,
    connection::run_blocking,
    error::Error,
    queries::{
        DEFAULT_MAX_ATTEMPTS, QUEUES, TaskMeta, current_timestamp, push_active, tables, write,
        write_meta,
    },
};

/// Pushes tasks to redb in a single transaction.
///
/// Returns the number of tasks `(enqueued, scheduled)`. Tasks whose id or
/// idempotency key already exists are skipped.
pub async fn push_tasks(
    conn: &Connection,
    config: &Config,
    tasks: Vec<RedbTask>,
) -> Result<(u32, u32), Error> {
    let db = conn.db();
    let cfg = config.clone();
    let res =
        run_blocking(move || write(&db, cfg.durability, |txn| push_tasks_txn(txn, &cfg, &tasks)))
            .await?;
    if config.emit_events && res != (0, 0) {
        conn.notify(config.queue.as_ref());
    }
    Ok(res)
}

fn push_tasks_txn(
    txn: &redb::WriteTransaction,
    config: &Config,
    tasks: &[RedbTask],
) -> Result<(u32, u32), Error> {
    let data_name = config.job_data_hash();
    let meta_name = config.job_meta_hash();
    let active_name = config.active_jobs_list();
    let scheduled_name = config.scheduled_jobs_set();
    let idem_name = config.idempotency_key_set();

    let mut data = txn.open_table(tables::data(&data_name))?;
    let mut meta = txn.open_table(tables::meta(&meta_name))?;
    let mut active = txn.open_table(tables::active(&active_name))?;
    let mut scheduled = txn.open_table(tables::timeline(&scheduled_name))?;
    let mut idempotency = txn.open_table(tables::idempotency(&idem_name))?;

    let now = current_timestamp();
    let ttl = config.idempotency_ttl.map(|d| d.as_secs()).unwrap_or(0);
    let mut enqueued = 0;
    let mut newly_scheduled = 0;

    for task in tasks {
        let task_id = task
            .task_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| Ulid::generate().to_string());

        let idempotency_key = task.idempotency_key().filter(|k| !k.is_empty());
        if let Some(key) = idempotency_key {
            let claimed = idempotency
                .get(key)?
                .is_some_and(|v| v.value().1 == 0 || v.value().1 > now);
            if claimed {
                continue;
            }
            let expires_at = if ttl > 0 { now + ttl } else { 0 };
            idempotency.insert(key, (task_id.as_str(), expires_at))?;
        }

        if data.get(task_id.as_str())?.is_some() {
            continue;
        }
        data.insert(task_id.as_str(), task.args.as_slice())?;

        let run_at = task.run_at().filter(|run_at| *run_at > now);
        let task_meta = TaskMeta {
            attempts: task.attempt(),
            max_attempts: task.max_attempts().unwrap_or(DEFAULT_MAX_ATTEMPTS),
            status: Status::Pending,
            idempotency_key: idempotency_key.map(ToOwned::to_owned),
            metadata: task.metadata().as_map().clone(),
            priority: task.priority(),
            run_at,
            lock_by: None,
            lock_at: None,
            done_at: None,
            result: None,
        };
        write_meta(&mut meta, &task_id, &task_meta)?;

        match run_at {
            Some(run_at) => {
                scheduled.insert((run_at, task_id.as_str()), ())?;
                newly_scheduled += 1;
            }
            None => {
                push_active(&mut active, &task_id)?;
                enqueued += 1;
            }
        }
    }

    let mut queues = txn.open_table(QUEUES)?;
    if queues.get(config.queue.as_ref())?.is_none() {
        queues.insert(config.queue.as_ref(), now)?;
    }

    Ok((enqueued, newly_scheduled))
}
