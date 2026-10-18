use apalis_core::worker::context::WorkerContext;
use redb::ReadableTable;

use crate::{
    Config, Connection, RedbTask,
    connection::run_blocking,
    error::Error,
    queries::{build_task, current_timestamp, push_active, read_meta, tables, write},
};

/// Manually fetch the next set of jobs
pub async fn fetch_next(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<Vec<RedbTask>, Error> {
    fetch_next_with_schedule(conn, worker, config)
        .await
        .map(|(tasks, _)| tasks)
}

/// Fetch the next set of jobs, also returning when the earliest remaining
/// scheduled task becomes due
pub(crate) async fn fetch_next_with_schedule(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(Vec<RedbTask>, Option<u64>), Error> {
    let db = conn.db();
    let config = config.clone();
    let worker = worker.name().to_owned();
    run_blocking(move || {
        write(&db, config.durability, |txn| {
            fetch_next_txn(txn, &config, &worker)
        })
    })
    .await
}

fn fetch_next_txn(
    txn: &redb::WriteTransaction,
    config: &Config,
    worker: &str,
) -> Result<(Vec<RedbTask>, Option<u64>), Error> {
    let workers_name = config.workers_set();
    let active_name = config.active_jobs_list();
    let scheduled_name = config.scheduled_jobs_set();
    let inflight_name = config.inflight_jobs_set();
    let data_name = config.job_data_hash();
    let meta_name = config.job_meta_hash();

    // Ensure the worker is registered
    let workers = txn.open_table(tables::workers(&workers_name))?;
    if workers.get(worker)?.is_none() {
        return Err(Error::WorkerNotRegistered(worker.to_owned()));
    }

    let mut active = txn.open_table(tables::active(&active_name))?;
    let mut scheduled = txn.open_table(tables::timeline(&scheduled_name))?;

    // 1. Promote any due scheduled jobs
    let now = current_timestamp();
    let due: Vec<(u64, String)> = scheduled
        .range(..(now + 1, ""))?
        .map(|entry| entry.map(|(k, _)| (k.value().0, k.value().1.to_owned())))
        .collect::<Result<_, _>>()?;
    for (run_at, task_id) in &due {
        scheduled.remove((*run_at, task_id.as_str()))?;
        push_active(&mut active, task_id)?;
    }
    let next_scheduled = scheduled.first()?.map(|(k, _)| k.value().0);

    // 2. Fetch jobs from the active queue
    let mut task_ids = Vec::with_capacity(config.batch_size);
    while task_ids.len() < config.batch_size {
        match active.pop_first()? {
            Some((_, task_id)) => task_ids.push(task_id.value().to_owned()),
            None => break,
        }
    }

    if task_ids.is_empty() {
        return Ok((vec![], next_scheduled));
    }

    // 3. Mark jobs as inflight and load them
    let mut inflight = txn.open_multimap_table(tables::inflight(&inflight_name))?;
    let data = txn.open_table(tables::data(&data_name))?;
    let meta = txn.open_table(tables::meta(&meta_name))?;
    let mut tasks = Vec::with_capacity(task_ids.len());
    for task_id in &task_ids {
        let (Some(args), Some(task_meta)) =
            (data.get(task_id.as_str())?, read_meta(&meta, task_id)?)
        else {
            tracing::warn!(task_id, "skipping task with missing data");
            continue;
        };
        inflight.insert(worker, task_id.as_str())?;
        tasks.push(build_task(
            task_id,
            args.value().to_vec(),
            task_meta,
            &config.queue,
        )?);
    }

    Ok((tasks, next_scheduled))
}
