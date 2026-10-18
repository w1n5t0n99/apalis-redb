use apalis_core::worker::context::WorkerContext;
use redb::ReadableTable;

use crate::{
    Config, Connection, Durability,
    connection::run_blocking,
    error::Error,
    queries::{WorkerRecord, current_timestamp, tables, write},
};

pub(crate) async fn keep_alive(
    conn: &Connection,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error> {
    let db = conn.db();
    let workers_set = config.workers_set();
    let name = worker.name().to_owned();
    run_blocking(move || {
        write(&db, Durability::Immediate, |txn| {
            let mut workers = txn.open_table(tables::workers(&workers_set))?;
            let mut record: WorkerRecord = match workers.get(name.as_str())? {
                Some(raw) => serde_json::from_str(raw.value())?,
                None => return Err(Error::WorkerNotRegistered(name.clone())),
            };
            record.last_seen = current_timestamp();
            workers.insert(name.as_str(), serde_json::to_string(&record)?.as_str())?;
            Ok(())
        })
    })
    .await
}
