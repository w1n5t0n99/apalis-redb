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

/// The most expired idempotency keys one push deletes, so a large backlog
/// is cleared over several pushes instead of slowing down one
const MAX_EXPIRED_KEYS_PER_PUSH: usize = 100;

/// Pushes tasks to redb in a single transaction.
///
/// Returns the number of tasks `(enqueued, scheduled)`. Tasks whose id or
/// unexpired idempotency key already exists are skipped. The transaction also
/// deletes up to [`MAX_EXPIRED_KEYS_PER_PUSH`] expired idempotency keys.
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
    let expiry_name = config.idempotency_expiry_set();

    let mut data = txn.open_table(tables::data(&data_name))?;
    let mut meta = txn.open_table(tables::meta(&meta_name))?;
    let mut active = txn.open_table(tables::active(&active_name))?;
    let mut scheduled = txn.open_table(tables::timeline(&scheduled_name))?;
    let mut idempotency = txn.open_table(tables::idempotency(&idem_name))?;
    let mut expiry = txn.open_table(tables::timeline(&expiry_name))?;

    let now = current_timestamp();
    remove_expired_keys(&mut idempotency, &mut expiry, now)?;

    // Round up so a sub-second TTL doesn't become 0, which means "never expires"
    let ttl = config
        .idempotency_ttl
        .map(|d| d.as_secs() + u64::from(d.subsec_nanos() > 0))
        .unwrap_or(0);
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
            let previous = idempotency
                .insert(key, (task_id.as_str(), expires_at))?
                .map(|v| v.value().1);
            if let Some(previous) = previous.filter(|&at| at != 0) {
                expiry.remove((previous, key))?;
            }
            if expires_at != 0 {
                expiry.insert((expires_at, key), ())?;
            }
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

/// Deletes idempotency keys that expired at or before `now`, oldest first
fn remove_expired_keys(
    idempotency: &mut redb::Table<'_, &'static str, (&'static str, u64)>,
    expiry: &mut redb::Table<'_, (u64, &'static str), ()>,
    now: u64,
) -> Result<(), Error> {
    let mut expired = Vec::new();
    for entry in expiry
        .range(..(now + 1, ""))?
        .take(MAX_EXPIRED_KEYS_PER_PUSH)
    {
        let (key, _) = entry?;
        let (expires_at, key) = key.value();
        expired.push((expires_at, key.to_owned()));
    }
    for (expires_at, key) in &expired {
        expiry.remove((*expires_at, key.as_str()))?;
        let current = idempotency.get(key.as_str())?.map(|v| v.value().1);
        if current == Some(*expires_at) {
            idempotency.remove(key.as_str())?;
        }
    }
    Ok(())
}
