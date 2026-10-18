use apalis_core::worker::context::WorkerContext;
use redb::ReadableTable;

use crate::{
    Config, Connection, Durability,
    connection::run_blocking,
    error::Error,
    queries::{QUEUES, WorkerRecord, current_timestamp, tables, task_lease, write},
};

pub(crate) async fn register_worker(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error> {
    let db = conn.db();
    let config = config.clone();
    let name = worker.name().to_owned();
    let service = worker.get_service().to_owned();
    run_blocking(move || {
        write(&db, Durability::Immediate, |txn| {
            let now = current_timestamp();
            let threshold = config.orphaned_duration().as_secs();
            let mut workers = txn.open_table(tables::workers(&config.workers_set()))?;
            if let Some(existing) = workers.get(name.as_str())? {
                let existing: WorkerRecord = serde_json::from_str(existing.value())?;
                if now.saturating_sub(existing.last_seen) < threshold {
                    return Err(Error::WorkerAlreadyExists(name.clone()));
                }
            }
            let record = WorkerRecord {
                last_seen: now,
                started_at: now,
                backend: "RedbStorage".to_owned(),
                service,
            };
            workers.insert(name.as_str(), serde_json::to_string(&record)?.as_str())?;

            let mut queues = txn.open_table(QUEUES)?;
            queues.insert(config.queue.as_ref(), now)?;
            Ok(())
        })
    })
    .await
}

/// Removes a stopping worker, returning any tasks it still holds to the queue
pub(crate) async fn unregister_worker(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error> {
    let released = task_lease::release_task_ids(conn, config, worker.name(), None).await?;
    if released > 0 {
        tracing::debug!("released {released} task(s) held by stopping worker");
    }
    let db = conn.db();
    let workers_set = config.workers_set();
    let name = worker.name().to_owned();
    run_blocking(move || {
        write(&db, Durability::Immediate, |txn| {
            txn.open_table(tables::workers(&workers_set))?
                .remove(name.as_str())?;
            Ok(())
        })
    })
    .await
}
