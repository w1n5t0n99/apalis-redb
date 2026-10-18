use apalis_core::{
    backend::{Filter, ListAllTasks, ListTasks},
    task::status::Status,
};
use redb::{ReadTransaction, ReadableTable};

use crate::{
    Config, RedbStorage, RedbTask,
    connection::run_blocking,
    error::Error,
    queries::{TaskMeta, build_task, list_queue_names, open_read, read, tables},
};

/// Collects tasks of a queue matching `status`, skipping the first `skip`
/// matches and stopping once `out` holds `limit` tasks.
fn collect_tasks(
    txn: &ReadTransaction,
    config: &Config,
    status: Option<&Status>,
    skip: &mut usize,
    limit: usize,
    out: &mut Vec<RedbTask>,
) -> Result<(), Error> {
    let (Some(data), Some(meta)) = (
        open_read(txn, tables::data(&config.job_data_hash()))?,
        open_read(txn, tables::meta(&config.job_meta_hash()))?,
    ) else {
        return Ok(());
    };
    for entry in meta.iter()? {
        if out.len() >= limit {
            break;
        }
        let (task_id, raw) = entry?;
        let task_meta: TaskMeta = serde_json::from_str(raw.value())?;
        if status.is_some_and(|s| *s != task_meta.status) {
            continue;
        }
        if *skip > 0 {
            *skip -= 1;
            continue;
        }
        let task_id = task_id.value();
        if let Some(args) = data.get(task_id)? {
            out.push(build_task(
                task_id,
                args.value().to_vec(),
                task_meta,
                &config.queue,
            )?);
        }
    }
    Ok(())
}

fn pagination(filter: &Filter) -> (usize, usize) {
    let limit = filter.limit() as usize;
    (filter.page.saturating_sub(1) as usize * limit, limit)
}

impl<Args> ListTasks for RedbStorage<Args>
where
    Args: 'static + Send + Sync,
{
    async fn list_tasks(&self, filter: &Filter) -> Result<Vec<RedbTask>, Self::Error> {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        let filter = filter.clone();
        run_blocking(move || {
            read(&db, |txn| {
                let (mut skip, limit) = pagination(&filter);
                let mut out = Vec::with_capacity(limit);
                collect_tasks(
                    txn,
                    &config,
                    filter.status.as_ref(),
                    &mut skip,
                    limit,
                    &mut out,
                )?;
                Ok(out)
            })
        })
        .await
    }
}

impl<Args> ListAllTasks for RedbStorage<Args>
where
    Args: 'static + Send + Sync,
{
    async fn list_all_tasks(&self, filter: &Filter) -> Result<Vec<RedbTask>, Self::Error> {
        let db = self.persist.conn.db();
        let filter = filter.clone();
        run_blocking(move || {
            read(&db, |txn| {
                let (mut skip, limit) = pagination(&filter);
                let mut out = Vec::with_capacity(limit);
                for queue in list_queue_names(txn)? {
                    if out.len() >= limit {
                        break;
                    }
                    let config = Config::default().queue(queue);
                    collect_tasks(
                        txn,
                        &config,
                        filter.status.as_ref(),
                        &mut skip,
                        limit,
                        &mut out,
                    )?;
                }
                Ok(out)
            })
        })
        .await
    }
}
