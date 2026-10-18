use std::collections::HashSet;

use apalis_core::{task::status::Status, worker::context::WorkerContext};
use redb::{ReadableMultimapTable, ReadableTable};

use crate::{
    Config, Connection, RedbTask,
    connection::run_blocking,
    error::Error,
    queries::{
        current_timestamp, push_active, read_meta, reset_to_pending, tables, write, write_meta,
    },
};

fn held_by(
    inflight: &redb::MultimapTable<'_, &'static str, &'static str>,
    worker: &str,
) -> Result<HashSet<String>, Error> {
    let mut held = HashSet::new();
    for task_id in inflight.get(worker)? {
        held.insert(task_id?.value().to_owned());
    }
    Ok(held)
}

/// Marks the tasks as running and locked by the worker
pub(crate) async fn acquire_leases(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
    task_ids: Vec<String>,
) -> Result<u32, Error> {
    if task_ids.is_empty() {
        return Ok(0);
    }
    let db = conn.db();
    let config = config.clone();
    let worker = worker.name().to_owned();
    run_blocking(move || {
        write(&db, config.durability, |txn| {
            let workers = txn.open_table(tables::workers(&config.workers_set()))?;
            if workers.get(worker.as_str())?.is_none() {
                return Err(Error::WorkerNotRegistered(worker.clone()));
            }
            let inflight =
                txn.open_multimap_table(tables::inflight(&config.inflight_jobs_set()))?;
            let held = held_by(&inflight, &worker)?;

            let mut meta = txn.open_table(tables::meta(&config.job_meta_hash()))?;
            let now = current_timestamp();
            let mut acquired = 0;
            for task_id in &task_ids {
                if !held.contains(task_id) {
                    tracing::warn!(task_id, "cannot lock task not held by worker");
                    continue;
                }
                if let Some(mut m) = read_meta(&meta, task_id)? {
                    m.status = Status::Running;
                    m.lock_at = Some(now);
                    m.lock_by = Some(worker.clone());
                    write_meta(&mut meta, task_id, &m)?;
                    acquired += 1;
                }
            }
            Ok(acquired)
        })
    })
    .await
}

/// Refreshes the lock timestamp of tasks the worker is currently running
pub(crate) async fn renew_leases(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<u32, Error> {
    if worker.task_count() == 0 {
        return Ok(0);
    }

    #[cfg(feature = "task-tracking")]
    let task_ids = worker
        .tasks()
        .map_err(|_| Error::WorkerOutOfSync)?
        .iter()
        .map(|t| t.task_id().to_string())
        .collect::<Vec<_>>();

    #[cfg(not(feature = "task-tracking"))]
    let task_ids: Vec<String> = vec![];

    if task_ids.is_empty() {
        return Ok(0);
    }

    let db = conn.db();
    let config = config.clone();
    let worker = worker.name().to_owned();
    run_blocking(move || {
        write(&db, config.durability, |txn| {
            let inflight =
                txn.open_multimap_table(tables::inflight(&config.inflight_jobs_set()))?;
            let held = held_by(&inflight, &worker)?;
            let mut meta = txn.open_table(tables::meta(&config.job_meta_hash()))?;
            let now = current_timestamp();
            let mut renewed = 0;
            for task_id in task_ids.iter().filter(|id| held.contains(*id)) {
                if let Some(mut m) = read_meta(&meta, task_id)? {
                    m.lock_at = Some(now);
                    write_meta(&mut meta, task_id, &m)?;
                    renewed += 1;
                }
            }
            Ok(renewed)
        })
    })
    .await
}

/// Returns tasks held by the worker to the active queue
pub(crate) async fn release_leases(
    conn: &Connection,
    config: &Config,
    worker: &WorkerContext,
    tasks: &[RedbTask],
) -> Result<u64, Error> {
    let task_ids: Vec<String> = tasks
        .iter()
        .filter_map(|t| t.task_id().map(|id| id.to_string()))
        .collect();
    release_task_ids(conn, config, worker.name(), Some(task_ids)).await
}

/// Returns the given tasks, or every task when `None`, held by the worker to the active queue
pub(crate) async fn release_task_ids(
    conn: &Connection,
    config: &Config,
    worker: &str,
    task_ids: Option<Vec<String>>,
) -> Result<u64, Error> {
    let db = conn.db();
    let cfg = config.clone();
    let worker = worker.to_owned();
    let released = run_blocking(move || {
        write(&db, cfg.durability, |txn| {
            let mut inflight =
                txn.open_multimap_table(tables::inflight(&cfg.inflight_jobs_set()))?;
            let mut active = txn.open_table(tables::active(&cfg.active_jobs_list()))?;
            let mut meta = txn.open_table(tables::meta(&cfg.job_meta_hash()))?;

            let task_ids = match task_ids {
                Some(ids) => ids,
                None => held_by(&inflight, &worker)?.into_iter().collect(),
            };
            let mut released = 0;
            for task_id in &task_ids {
                if inflight.remove(worker.as_str(), task_id.as_str())? {
                    push_active(&mut active, task_id)?;
                    reset_to_pending(&mut meta, task_id)?;
                    released += 1;
                }
            }
            Ok(released)
        })
    })
    .await?;
    if config.emit_events && released > 0 {
        conn.notify(config.queue.as_ref());
    }
    Ok(released)
}
