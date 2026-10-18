use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use apalis_core::{
    backend::{
        WorkerFilter,
        persistence::{Persistence, TaskEvent},
    },
    task::Task,
    worker::context::WorkerContext,
};
use serde_json::Value;

use crate::{
    Config, Connection, RedbTask,
    error::Error,
    queries::{
        self, keep_alive, push_tasks, reenqueue_orphaned, register_worker, release_leases,
        renew_leases,
    },
};

/// Marker for "no scheduled task"
pub(crate) const NO_SCHEDULE: u64 = u64::MAX;

/// The default persistence implementation
#[derive(Debug, Clone)]
pub struct RedbPersistence {
    pub(crate) conn: Connection,
    pub(crate) config: Config,
    /// When the earliest scheduled task becomes due, as seen by the last fetch
    pub(crate) next_scheduled: Arc<AtomicU64>,
}

impl RedbPersistence {
    pub(crate) fn new(conn: Connection, config: Config) -> Self {
        Self {
            conn,
            config,
            next_scheduled: Arc::new(AtomicU64::new(NO_SCHEDULE)),
        }
    }
}

impl Persistence for RedbPersistence {
    type Compact = Vec<u8>;
    type Error = Error;
    type Response = Value;

    async fn register(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let count = reenqueue_orphaned(
            &self.conn,
            &self.config,
            &WorkerFilter::Only(worker.name().to_owned()),
        )
        .await?;
        if count > 0 {
            tracing::debug!("{count} re-enqueued orphaned tasks");
        }
        register_worker(&self.conn, worker, &self.config).await?;
        tracing::debug!("registered worker successfully");
        Ok(())
    }

    async fn heartbeat(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        keep_alive(&self.conn, worker, &self.config).await?;
        if self.config.lock_tasks {
            let count = renew_leases(&self.conn, worker, &self.config).await?;
            tracing::debug!("renewed leases for {count} tasks");
        }
        let count = reenqueue_orphaned(
            &self.conn,
            &self.config,
            &WorkerFilter::AllExcept(worker.name().to_owned()),
        )
        .await?;
        if count > 0 {
            tracing::debug!("re-enqueued {count} orphaned task(s)");
        }
        Ok(())
    }

    async fn fetch_next(&mut self, worker: &WorkerContext) -> Result<Vec<RedbTask>, Error> {
        let (tasks, next_scheduled) =
            queries::fetch_next::fetch_next_with_schedule(&self.conn, worker, &self.config).await?;
        self.next_scheduled
            .store(next_scheduled.unwrap_or(NO_SCHEDULE), Ordering::Release);
        Ok(tasks)
    }

    async fn handle_events(
        &mut self,
        messages: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> Result<(), Error> {
        let mut lock_ids = Vec::new();
        let mut ack_payloads = Vec::new();
        for msg in messages {
            match msg {
                TaskEvent::Lock { task_id } => lock_ids.push(task_id.to_string()),
                TaskEvent::Complete(payload) => ack_payloads.push(payload),
                _ => {}
            }
        }
        if lock_ids.is_empty() && ack_payloads.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Processing {} locks and {} acks",
            lock_ids.len(),
            ack_payloads.len()
        );

        if !lock_ids.is_empty() {
            queries::acquire_leases(&self.conn, worker, &self.config, lock_ids).await?;
        }
        if !ack_payloads.is_empty() {
            queries::handle_results(&self.conn, ack_payloads, worker, &self.config).await?;
        }
        Ok(())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<RedbTask>,
        worker: &WorkerContext,
    ) -> Result<u64, Error> {
        let count = release_leases(&self.conn, &self.config, worker, &tasks).await?;
        if self.config.lock_tasks && count as usize != tasks.len() {
            return Err(Error::ReenqueueMismatch {
                queued: tasks.len(),
                abandoned: count as usize,
            });
        }
        Ok(count)
    }

    async fn push_tasks(&mut self, tasks: Vec<Task<Self::Compact>>) -> Result<(), Self::Error> {
        push_tasks(&self.conn, &self.config, tasks).await?;
        Ok(())
    }
}
