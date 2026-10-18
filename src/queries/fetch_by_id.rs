use apalis_core::{backend::FetchById, task::task_id::TaskId};

use crate::{
    RedbStorage, RedbTask,
    connection::run_blocking,
    queries::{build_task, open_read, read, read_meta, tables},
};

impl<Args> FetchById for RedbStorage<Args>
where
    Args: 'static + Send,
{
    async fn fetch_by_id(&mut self, task_id: &TaskId) -> Result<Option<RedbTask>, Self::Error> {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        let task_id = task_id.to_string();
        run_blocking(move || {
            read(&db, |txn| {
                let (Some(data), Some(meta)) = (
                    open_read(txn, tables::data(&config.job_data_hash()))?,
                    open_read(txn, tables::meta(&config.job_meta_hash()))?,
                ) else {
                    return Ok(None);
                };
                let (Some(args), Some(task_meta)) =
                    (data.get(task_id.as_str())?, read_meta(&meta, &task_id)?)
                else {
                    return Ok(None);
                };
                build_task(&task_id, args.value().to_vec(), task_meta, &config.queue).map(Some)
            })
        })
        .await
    }
}
