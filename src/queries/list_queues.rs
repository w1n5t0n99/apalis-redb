use apalis_core::backend::{ListQueues, QueueInfo};

use crate::{
    Config, RedbStorage,
    connection::run_blocking,
    error::Error,
    queries::{list_queue_names, list_workers::workers_in_queue, metrics::queue_counts, read},
};

impl<Args> ListQueues for RedbStorage<Args>
where
    Args: 'static + Send,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Error>> + Send {
        let db = self.persist.conn.db();
        run_blocking(move || {
            read(&db, |txn| {
                let mut queues = Vec::new();
                for name in list_queue_names(txn)? {
                    let config = Config::default().queue(&name);
                    let counts = queue_counts(txn, &config)?;
                    let workers = workers_in_queue(txn, &config)?
                        .into_iter()
                        .map(|w| w.id)
                        .collect();
                    queues.push(QueueInfo {
                        name,
                        stats: counts.statistics(),
                        workers,
                        activity: counts.activity.iter().map(|n| *n as usize).collect(),
                    });
                }
                Ok(queues)
            })
        })
    }
}
