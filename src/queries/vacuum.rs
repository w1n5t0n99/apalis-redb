use std::time::Duration;

use apalis_core::{backend::Vacuum, task::status::Status};
use redb::ReadableTable;

use crate::{
    Config, RedbStorage,
    connection::run_blocking,
    error::Error,
    queries::{TaskMeta, current_timestamp, tables, write},
};

/// Deletes finished tasks completed at or before `cutoff`
fn vacuum_txn(
    txn: &redb::WriteTransaction,
    config: &Config,
    cutoff: Option<u64>,
) -> Result<usize, Error> {
    let mut data = txn.open_table(tables::data(&config.job_data_hash()))?;
    let mut meta = txn.open_table(tables::meta(&config.job_meta_hash()))?;
    let mut done = txn.open_table(tables::timeline(&config.done_jobs_set()))?;
    let mut dead = txn.open_table(tables::timeline(&config.dead_jobs_set()))?;

    let mut finished = Vec::new();
    for entry in meta.iter()? {
        let (task_id, raw) = entry?;
        let task_meta: TaskMeta = serde_json::from_str(raw.value())?;
        if !task_meta.is_terminal() {
            continue;
        }
        let done_at = task_meta.done_at.unwrap_or_default();
        if cutoff.is_some_and(|cutoff| done_at > cutoff) {
            continue;
        }
        finished.push((task_id.value().to_owned(), task_meta.status, done_at));
    }

    for (task_id, status, done_at) in &finished {
        data.remove(task_id.as_str())?;
        meta.remove(task_id.as_str())?;
        let timeline = if *status == Status::Done {
            &mut done
        } else {
            &mut dead
        };
        timeline.remove((*done_at, task_id.as_str()))?;
    }
    Ok(finished.len())
}

impl<Args> Vacuum for RedbStorage<Args>
where
    Args: Unpin + Send + Sync + 'static,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        run_blocking(move || write(&db, config.durability, |txn| vacuum_txn(txn, &config, None)))
            .await
    }

    async fn vacuum_before(&mut self, duration: Duration) -> Result<usize, Self::Error> {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        let cutoff = current_timestamp().saturating_sub(duration.as_secs());
        run_blocking(move || {
            write(&db, config.durability, |txn| {
                vacuum_txn(txn, &config, Some(cutoff))
            })
        })
        .await
    }
}
