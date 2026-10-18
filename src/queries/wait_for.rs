use std::{collections::HashSet, time::Duration};

use apalis_core::{
    backend::{TaskResult, WaitForCompletion},
    task::task_id::TaskId,
    timer::sleep,
};
use futures::stream::{self, BoxStream, StreamExt};
use serde::de::DeserializeOwned;

use crate::{
    RedbStorage,
    connection::run_blocking,
    error::Error,
    queries::{open_read, read, read_meta, tables},
};

impl<Res, Args> WaitForCompletion<Res> for RedbStorage<Args>
where
    Args: Unpin + Send + Sync + 'static,
    Res: Send + DeserializeOwned + 'static,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res>, Error>>;

    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let storage = self.clone();
        let pending: HashSet<TaskId> = task_ids.into_iter().collect();

        stream::unfold(
            (storage, pending),
            |(mut storage, mut pending)| async move {
                if pending.is_empty() {
                    return None;
                }
                match storage.check_status(pending.iter().cloned()).await {
                    Ok(results) if results.is_empty() => {
                        sleep(Duration::from_millis(100)).await;
                        Some((vec![], (storage, pending)))
                    }
                    Ok(results) => {
                        for result in &results {
                            pending.remove(result.task_id());
                        }
                        Some((results.into_iter().map(Ok).collect(), (storage, pending)))
                    }
                    Err(e) => Some((vec![Err(e)], (storage, HashSet::new()))),
                }
            },
        )
        .flat_map(stream::iter)
        .boxed()
    }

    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Res>>, Self::Error> {
        let task_ids: Vec<TaskId> = task_ids.into_iter().collect();
        if task_ids.is_empty() {
            return Ok(vec![]);
        }
        let db = self.persist.conn.db();
        let meta_name = self.persist.config.job_meta_hash();
        run_blocking(move || {
            read(&db, |txn| {
                let Some(meta) = open_read(txn, tables::meta(&meta_name))? else {
                    return Ok(vec![]);
                };
                let mut results = Vec::new();
                for task_id in task_ids {
                    let Some(task_meta) = read_meta(&meta, &task_id.to_string())? else {
                        continue;
                    };
                    if !task_meta.is_terminal() {
                        continue;
                    }
                    let Some(result) = task_meta.result else {
                        continue;
                    };
                    let result = match result {
                        Ok(value) => Ok(serde_json::from_value(value)?),
                        Err(e) => Err(e),
                    };
                    results.push(TaskResult {
                        task_id,
                        attempt: task_meta.attempts,
                        status: task_meta.status,
                        result,
                    });
                }
                Ok(results)
            })
        })
        .await
    }
}
