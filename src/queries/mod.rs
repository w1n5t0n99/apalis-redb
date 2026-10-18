use std::{
    collections::HashMap,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::{
    backend::queue::Queue,
    task::{builder::TaskBuilder, metadata::MetadataStore, status::Status, task_id::TaskId},
};
use redb::{
    Database, MultimapTableDefinition, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, TableError, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Durability, RedbTask, error::Error};

mod fetch_by_id;
pub(crate) mod fetch_next;
mod handle_results;
mod keep_alive;
mod list_queues;
mod list_tasks;
mod list_workers;
mod metrics;
mod push_tasks;
mod reenqueue;
mod register_worker;
mod task_lease;
mod vacuum;
mod wait_for;

pub use fetch_next::fetch_next;
pub(crate) use handle_results::handle_results;
pub(crate) use keep_alive::keep_alive;
pub use push_tasks::push_tasks;
pub(crate) use reenqueue::reenqueue_orphaned;
pub(crate) use register_worker::{register_worker, unregister_worker};
pub(crate) use task_lease::{acquire_leases, release_leases, renew_leases};

/// The default number of attempts a task gets when none is set
pub(crate) const DEFAULT_MAX_ATTEMPTS: usize = 25;

/// Global table tracking all known queues and when they were last registered
pub(crate) const QUEUES: TableDefinition<'static, &'static str, u64> =
    TableDefinition::new("apalis:queues");

pub(crate) fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Typed definitions for the tables backing a single queue
pub(crate) mod tables {
    use super::*;

    /// task_id -> encoded args
    pub(crate) fn data(name: &str) -> TableDefinition<'_, &'static str, &'static [u8]> {
        TableDefinition::new(name)
    }
    /// task_id -> json [`TaskMeta`]
    pub(crate) fn meta(name: &str) -> TableDefinition<'_, &'static str, &'static str> {
        TableDefinition::new(name)
    }
    /// sequence -> task_id, FIFO order
    pub(crate) fn active(name: &str) -> TableDefinition<'_, u64, &'static str> {
        TableDefinition::new(name)
    }
    /// (timestamp, task_id) -> (), used for scheduled, done and dead sets
    pub(crate) fn timeline(name: &str) -> TableDefinition<'_, (u64, &'static str), ()> {
        TableDefinition::new(name)
    }
    /// worker -> task_id
    pub(crate) fn inflight(name: &str) -> MultimapTableDefinition<'_, &'static str, &'static str> {
        MultimapTableDefinition::new(name)
    }
    /// worker -> json [`WorkerRecord`]
    pub(crate) fn workers(name: &str) -> TableDefinition<'_, &'static str, &'static str> {
        TableDefinition::new(name)
    }
    /// idempotency key -> (task_id, expires_at); `expires_at == 0` never expires
    pub(crate) fn idempotency(
        name: &str,
    ) -> TableDefinition<'_, &'static str, (&'static str, u64)> {
        TableDefinition::new(name)
    }
}

/// Metadata stored alongside each task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskMeta {
    pub(crate) attempts: usize,
    pub(crate) max_attempts: usize,
    pub(crate) status: Status,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    #[serde(default)]
    pub(crate) metadata: HashMap<String, String>,
    #[serde(default)]
    pub(crate) priority: Option<usize>,
    #[serde(default)]
    pub(crate) run_at: Option<u64>,
    #[serde(default)]
    pub(crate) lock_by: Option<String>,
    #[serde(default)]
    pub(crate) lock_at: Option<u64>,
    #[serde(default)]
    pub(crate) done_at: Option<u64>,
    #[serde(default)]
    pub(crate) result: Option<Result<Value, String>>,
}

impl TaskMeta {
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.status, Status::Done | Status::Failed | Status::Killed)
    }
}

/// Metadata stored for each registered worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerRecord {
    pub(crate) last_seen: u64,
    pub(crate) started_at: u64,
    pub(crate) backend: String,
    pub(crate) service: String,
}

/// Build a compact task from its stored parts
pub(crate) fn build_task(
    task_id: &str,
    data: Vec<u8>,
    meta: TaskMeta,
    queue: &Queue,
) -> Result<RedbTask, Error> {
    let task_id = TaskId::from_str(task_id).map_err(Error::TaskIdError)?;
    let mut task = TaskBuilder::new(data)
        .task_id(task_id)
        .status(meta.status)
        .with_metadata(MetadataStore::from_map(meta.metadata))
        .lock_by(meta.lock_by)
        .lock_at(meta.lock_at)
        .done_at(meta.done_at)
        .max_attempts(meta.max_attempts)
        .attempt(meta.attempts)
        .queue(queue.clone());
    if let Some(run_at) = meta.run_at {
        task = task.run_at_timestamp(run_at);
    }
    if let Some(priority) = meta.priority {
        task = task.priority(priority);
    }
    if let Some(key) = meta.idempotency_key {
        task = task.idempotency_key(key);
    }
    Ok(task.build())
}

pub(crate) fn read_meta<T: ReadableTable<&'static str, &'static str>>(
    table: &T,
    task_id: &str,
) -> Result<Option<TaskMeta>, Error> {
    match table.get(task_id)? {
        Some(raw) => Ok(Some(serde_json::from_str(raw.value())?)),
        None => Ok(None),
    }
}

pub(crate) fn write_meta(
    table: &mut redb::Table<'_, &'static str, &'static str>,
    task_id: &str,
    meta: &TaskMeta,
) -> Result<(), Error> {
    table.insert(task_id, serde_json::to_string(meta)?.as_str())?;
    Ok(())
}

/// Appends a task id to the tail of the active list
pub(crate) fn push_active(
    table: &mut redb::Table<'_, u64, &'static str>,
    task_id: &str,
) -> Result<(), Error> {
    let next = table.last()?.map(|(k, _)| k.value() + 1).unwrap_or(0);
    table.insert(next, task_id)?;
    Ok(())
}

/// Marks a task as pending again after it was released by a worker
pub(crate) fn reset_to_pending(
    meta: &mut redb::Table<'_, &'static str, &'static str>,
    task_id: &str,
) -> Result<(), Error> {
    if let Some(mut m) = read_meta(meta, task_id)? {
        m.status = Status::Pending;
        m.lock_at = None;
        m.lock_by = None;
        write_meta(meta, task_id, &m)?;
    }
    Ok(())
}

/// Runs `f` inside a write transaction, committing on success
pub(crate) fn write<T>(
    db: &Database,
    durability: Durability,
    f: impl FnOnce(&WriteTransaction) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut txn = db.begin_write()?;
    txn.set_durability(durability.into())?;
    let out = f(&txn)?;
    txn.commit()?;
    Ok(out)
}

/// Runs `f` inside a read transaction
pub(crate) fn read<T>(
    db: &Database,
    f: impl FnOnce(&ReadTransaction) -> Result<T, Error>,
) -> Result<T, Error> {
    let txn = db.begin_read()?;
    f(&txn)
}

/// Opens a table for reading, returning `None` if it has never been written
pub(crate) fn open_read<K: redb::Key + 'static, V: redb::Value + 'static>(
    txn: &ReadTransaction,
    def: TableDefinition<'_, K, V>,
) -> Result<Option<redb::ReadOnlyTable<K, V>>, Error> {
    match txn.open_table(def) {
        Ok(table) => Ok(Some(table)),
        Err(TableError::TableDoesNotExist(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Opens a multimap table for reading, returning `None` if it has never been written
pub(crate) fn open_read_multimap<K: redb::Key + 'static, V: redb::Key + 'static>(
    txn: &ReadTransaction,
    def: MultimapTableDefinition<'_, K, V>,
) -> Result<Option<redb::ReadOnlyMultimapTable<K, V>>, Error> {
    match txn.open_multimap_table(def) {
        Ok(table) => Ok(Some(table)),
        Err(TableError::TableDoesNotExist(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Lists all queues known to the database
pub(crate) fn list_queue_names(txn: &ReadTransaction) -> Result<Vec<String>, Error> {
    let Some(table) = open_read(txn, QUEUES)? else {
        return Ok(vec![]);
    };
    let mut queues = Vec::new();
    for entry in table.iter()? {
        let (name, _) = entry?;
        queues.push(name.value().to_owned());
    }
    Ok(queues)
}
