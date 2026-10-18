use apalis_core::backend::{ListWorkers, RunningWorker};
use redb::{ReadTransaction, ReadableTable};

use crate::{
    Config, RedbStorage,
    connection::run_blocking,
    error::Error,
    queries::{WorkerRecord, list_queue_names, open_read, read, tables},
};

pub(crate) fn workers_in_queue(
    txn: &ReadTransaction,
    config: &Config,
) -> Result<Vec<RunningWorker>, Error> {
    let Some(table) = open_read(txn, tables::workers(&config.workers_set()))? else {
        return Ok(vec![]);
    };
    let mut workers = Vec::new();
    for entry in table.iter()? {
        let (name, raw) = entry?;
        let record: WorkerRecord = serde_json::from_str(raw.value())?;
        workers.push(RunningWorker {
            id: name.value().to_owned(),
            queue: config.queue.to_string(),
            backend: record.backend,
            started_at: record.started_at,
            last_heartbeat: record.last_seen,
            layers: record.service,
        });
    }
    Ok(workers)
}

impl<Args> ListWorkers for RedbStorage<Args>
where
    Args: 'static + Send + Sync,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        run_blocking(move || read(&db, |txn| workers_in_queue(txn, &config)))
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let db = self.persist.conn.db();
        run_blocking(move || {
            read(&db, |txn| {
                let mut workers = Vec::new();
                for queue in list_queue_names(txn)? {
                    workers.extend(workers_in_queue(txn, &Config::default().queue(queue))?);
                }
                Ok(workers)
            })
        })
    }
}
