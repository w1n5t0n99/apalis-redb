#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll, ready},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        finalize::Durable,
        future::BoxSyncFuture,
        persistence::{Persisted, TaskPersistLayer},
    },
    features_table,
    task::Task,
    worker::context::WorkerContext,
};

use futures::{FutureExt, Sink};
use futures_timer::Delay;

pub use redb::{Builder, Database};

mod config;

mod connection;

/// Raw queries used to make low level redb calls
pub mod queries;

/// Factory utilities for building [`RedbStorage`] instances.
pub mod factory;

use serde_json::Value;
use ulid::Ulid;

pub use crate::config::{Config, Durability};

pub use crate::{
    connection::{Connection, connect},
    error::Error,
    persist::RedbPersistence,
};

mod persist;

mod error;

/// A redb task type alias
pub type RedbTask<Args = Vec<u8>> = Task<Args>;

/// Represents a [Backend] that uses redb for storage.
///
#[doc = "# Feature Support\n"]
#[doc = features_table! {
    setup = r#"
    # {
    #    use apalis_redb::{RedbStorage, Config};
    #    let dir = tempfile::tempdir().unwrap();
    #    let conn = apalis_redb::connect(dir.path().join("tasks.redb")).await.expect("Could not connect");
    #    std::mem::forget(dir);
    #    const MOD_PATH: &str = module_path!();
    #    let config = Config::default().queue(MOD_PATH);
    #    RedbStorage::<u32>::new(conn).with_config(config)
    # };
    "#,
    TaskSink => supported("Ability to push new tasks", true),
    MakeShared => supported("Share the same connection across multiple workers", false),
    Workflow => supported("Supports workflows and orchestration", true),
    WebUI => supported("Supports `apalis-board` for monitoring and managing tasks", true),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    Serialization => supported("Supports multiple serialization formats such as JSON and MessagePack", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
}]
#[derive(Debug)]
pub struct RedbStorage<Args> {
    job_type: PhantomData<Args>,
    persist: Persisted<RedbPersistence>,
    codec: JsonCodec,
    cleanup: Option<BoxSyncFuture<Result<(), Error>>>,
    /// Wakes the worker when the earliest scheduled task becomes due
    schedule_timer: Option<(u64, Delay)>,
}

impl<Args> Clone for RedbStorage<Args> {
    fn clone(&self) -> Self {
        Self {
            job_type: PhantomData,
            persist: self.persist.clone(),
            codec: self.codec.clone(),
            cleanup: None,
            schedule_timer: None,
        }
    }
}

impl<T> RedbStorage<T> {
    /// Start a new storage using the connection
    pub fn new(conn: Connection) -> RedbStorage<T> {
        let config = Config::default().queue(std::any::type_name::<T>());
        RedbStorage {
            job_type: PhantomData,
            persist: Persisted::new(RedbPersistence::new(conn, config)),
            codec: JsonCodec::default(),
            cleanup: None,
            schedule_timer: None,
        }
    }

    /// Customize the backend
    pub fn with_config(mut self, config: Config) -> RedbStorage<T> {
        self.persist.config = config;
        RedbStorage {
            job_type: PhantomData,
            persist: self.persist,
            codec: self.codec,
            cleanup: None,
            schedule_timer: None,
        }
    }

    /// Get current connection
    pub fn get_connection(&mut self) -> &mut Connection {
        &mut self.persist.conn
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &Config {
        &self.persist.config
    }

    /// Arms a timer for the earliest scheduled task seen by the last fetch
    fn poll_schedule_timer(&mut self, cx: &mut Context<'_>) {
        let next = self.persist.next_scheduled.load(Ordering::Acquire);
        if next == persist::NO_SCHEDULE {
            self.schedule_timer = None;
            return;
        }
        if self
            .schedule_timer
            .as_ref()
            .is_none_or(|(at, _)| *at != next)
        {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let delay = Duration::from_secs(next).saturating_sub(now);
            self.schedule_timer = Some((next, Delay::new(delay)));
        }
        if let Some((_, timer)) = &mut self.schedule_timer
            && timer.poll_unpin(cx).is_ready()
        {
            self.schedule_timer = None;
            cx.waker().wake_by_ref();
        }
    }
}

impl<Args> Backend for RedbStorage<Args> {
    type Task = RedbTask;

    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let heartbeat_interval = self.persist.config.heartbeat_interval;
        self.persist.poll_ready(cx, worker, heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        // Register before fetching so a push racing with an empty fetch is not missed
        if self.persist.config.emit_events {
            let queue = self.persist.config.queue.clone();
            self.persist
                .conn
                .register_waker(queue.as_ref(), worker.name(), cx.waker());
        }
        let next = self.persist.poll_next(cx, worker);
        if next.is_pending() {
            self.poll_schedule_timer(cx);
        }
        next
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self.cleanup.is_none() {
            ready!(self.persist.poll_close(cx, worker))?;
            let worker = worker.clone();
            let config = self.config().clone();
            let conn = self.persist.conn.clone();
            conn.unregister_waker(config.queue.as_ref(), worker.name());
            self.cleanup = Some(
                async move { queries::unregister_worker(&conn, &worker, &config).await }
                    .boxed()
                    .into(),
            );
        };

        self.cleanup.as_mut().unwrap().poll_unpin(cx)
    }
}

impl<Args> BackendConfig for RedbStorage<Args> {
    type Args = Args;

    type Id = Ulid;

    type Kind = Durable;

    type Config = Config;

    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.persist.config
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        self.persist
            .layer(JsonCodec::<Value>::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args> WireFormatBackend for RedbStorage<Args> {
    type Codec = JsonCodec;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<Args> Sink<RedbTask> for RedbStorage<Args>
where
    Args: Unpin + Send + Sync + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_ready(Pin::new(&mut self.get_mut().persist), cx)
    }

    fn start_send(self: Pin<&mut Self>, item: RedbTask) -> Result<(), Self::Error> {
        Sink::start_send(Pin::new(&mut self.get_mut().persist), item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_flush(Pin::new(&mut self.get_mut().persist), cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(Pin::new(&mut self.get_mut().persist), cx)
    }
}
