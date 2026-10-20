use std::time::Duration;

use serde::{Deserialize, Serialize};

pub(crate) use apalis_core::backend::queue::Queue;

const ACTIVE_TASKS_LIST: &str = "{queue}:active";
const WORKERS_SET: &str = "{queue}:workers";
const DEAD_TASKS_SET: &str = "{queue}:dead";
const DONE_TASKS_SET: &str = "{queue}:done";
const INFLIGHT_TASKS_SET: &str = "{queue}:inflight";
const TASK_DATA_HASH: &str = "{queue}:data";
const JOB_META_HASH: &str = "{queue}:meta";
const SCHEDULED_TASKS_SET: &str = "{queue}:scheduled";
const IDEMPOTENCY_KEY_SET: &str = "{queue}:idempotency";
const IDEMPOTENCY_EXPIRY_SET: &str = "{queue}:idempotency_expiry";

/// Configuration for a worker's queue, batching, and liveness detection.
///
/// `Config` controls how jobs are fetched from a queue and how worker
/// liveness is monitored.
///
/// # Defaults
///
/// - `batch_size`: `10`
/// - `heartbeat_interval`: `30` seconds
/// - `missed_heartbeats`: `10`
/// - `queue`: `"default"`
/// - `database_url`: `None`
/// - `lock_tasks`: `true`
/// - `persist_results`: `true`
/// - `emit_events`: `true`
/// - `idempotency_ttl`: `None`
/// - `durability`: [`Durability::Immediate`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The maximum number of jobs fetched in a single batch.
    ///
    /// Must be greater than zero.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// The interval between worker heartbeats.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// The number of missed heartbeats allowed before a worker is
    /// considered dead.
    #[serde(default = "default_missed_heartbeats")]
    pub missed_heartbeats: usize,

    /// The queue from which jobs are consumed.
    pub queue: Queue,

    /// An optional database path used by the worker.
    pub database_url: Option<String>,

    /// Whether tasks should be locked while being processed.
    #[serde(default = "default_events")]
    pub lock_tasks: bool,

    /// Whether job results should be persisted.
    #[serde(default = "default_events")]
    pub persist_results: bool,

    /// Whether to wake in-process workers when tasks become available.
    ///
    /// When disabled, workers only pick up new tasks on their heartbeat
    /// or when combined with a poll strategy such as `poll_with_interval`.
    #[serde(default = "default_events")]
    pub emit_events: bool,

    /// How long an idempotency key should be retained.
    ///
    /// When `None`, idempotency keys do not expire.
    /// When `Some(duration)`, the key expires after the configured duration.
    #[serde(default)]
    pub idempotency_ttl: Option<Duration>,

    /// How durably queue operations (push, fetch, ack, lock, release, vacuum)
    /// are committed.
    ///
    /// Worker registration, heartbeats and shutdown always commit with
    /// [`Durability::Immediate`], which also flushes every earlier commit.
    #[serde(default)]
    pub durability: Durability,
}

/// The durability level of redb write transactions.
///
/// See [`redb::Durability`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Durability {
    /// Commits are persisted to disk before they return. Safe, but every
    /// commit pays for an fsync.
    #[default]
    Immediate,
    /// Commits are not persisted until a later [`Durability::Immediate`]
    /// commit. Much faster, but a crash or power loss discards every commit
    /// since the last durable one.
    ///
    /// While a worker is running its heartbeat commits durably, so at most
    /// one `heartbeat_interval` of queue changes can be lost. A producer with
    /// no running worker may lose everything it pushed.
    None,
}

impl From<Durability> for redb::Durability {
    fn from(durability: Durability) -> Self {
        match durability {
            Durability::Immediate => redb::Durability::Immediate,
            Durability::None => redb::Durability::None,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch_size: 10,
            heartbeat_interval: Duration::from_secs(30),
            missed_heartbeats: 10,
            queue: Queue::from("default"),
            database_url: None,
            lock_tasks: true,
            persist_results: true,
            emit_events: true,
            idempotency_ttl: None,
            durability: Durability::Immediate,
        }
    }
}

fn default_batch_size() -> usize {
    10
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_missed_heartbeats() -> usize {
    10
}

fn default_events() -> bool {
    true
}

impl Config {
    /// Sets the maximum number of jobs to fetch in a single batch.
    ///
    /// # Panics
    ///
    /// Panics if `size` is `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    ///
    /// let config = Config::default().batch_size(50);
    ///
    /// assert_eq!(config.batch_size, 50);
    /// ```
    #[must_use]
    pub fn batch_size(mut self, size: usize) -> Self {
        assert!(size > 0, "batch size cannot be 0");
        self.batch_size = size;
        self
    }

    /// Sets the interval between worker heartbeats.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default()
    ///     .heartbeat_interval(Duration::from_secs(15));
    ///
    /// assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
    /// ```
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the queue from which jobs are consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use apalis_redb::Config;
    /// let config = Config::default()
    ///     .queue("high-priority");
    ///
    /// assert_eq!(config.queue.as_ref(), "high-priority");
    /// ```
    #[must_use]
    pub fn queue(mut self, queue: impl AsRef<str>) -> Self {
        self.queue = Queue::from(queue.as_ref());
        self
    }

    /// Sets the number of missed heartbeats allowed before a worker is
    /// considered dead.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default().missed_heartbeats(3);
    ///
    /// assert_eq!(config.missed_heartbeats, 3);
    /// assert_eq!(
    ///     config.orphaned_duration(),
    ///     Duration::from_secs(90)
    /// );
    /// ```
    #[must_use]
    pub fn missed_heartbeats(mut self, missed_heartbeats: usize) -> Self {
        self.missed_heartbeats = missed_heartbeats;
        self
    }

    /// Sets the database path used by the worker.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    ///
    /// let config = Config::default()
    ///     .database_url("tasks.redb");
    ///
    /// assert_eq!(config.database_url.as_deref(), Some("tasks.redb"));
    /// ```
    #[must_use]
    pub fn database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }

    /// Enables or disables task locking and worker lease management.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    ///
    /// let config = Config::default().lock_tasks(false);
    ///
    /// assert!(!config.lock_tasks);
    /// ```
    #[must_use]
    pub fn lock_tasks(mut self, lock_tasks: bool) -> Self {
        self.lock_tasks = lock_tasks;
        self
    }

    /// Enables or disables waking in-process workers when tasks become available.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    ///
    /// let config = Config::default().emit_events(false);
    ///
    /// assert!(!config.emit_events);
    /// ```
    #[must_use]
    pub fn emit_events(mut self, emit_events: bool) -> Self {
        self.emit_events = emit_events;
        self
    }

    /// Enables or disables result persistence.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    ///
    /// let config = Config::default().persist_results(false);
    ///
    /// assert!(!config.persist_results);
    /// ```
    #[must_use]
    pub fn persist_results(mut self, persist_results: bool) -> Self {
        self.persist_results = persist_results;
        self
    }

    /// Sets the time-to-live for task idempotency keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default()
    ///     .idempotency_ttl(Some(Duration::from_secs(3600)));
    ///
    /// assert_eq!(
    ///     config.idempotency_ttl,
    ///     Some(Duration::from_secs(3600))
    /// );
    /// ```
    #[must_use]
    pub fn idempotency_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.idempotency_ttl = ttl;
        self
    }

    /// Sets the durability of queue operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redb::{Config, Durability};
    ///
    /// let config = Config::default().durability(Durability::None);
    ///
    /// assert_eq!(config.durability, Durability::None);
    /// ```
    #[must_use]
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Returns the amount of time after which a worker may be considered
    /// orphaned.
    ///
    /// ```text
    /// heartbeat_interval × missed_heartbeats
    /// ```
    #[must_use]
    pub fn orphaned_duration(&self) -> Duration {
        self.heartbeat_interval * self.missed_heartbeats as u32
    }

    /// Returns the redb table holding the ordered list of pending task ids.
    pub fn active_jobs_list(&self) -> String {
        ACTIVE_TASKS_LIST.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding the workers registered with the queue.
    pub fn workers_set(&self) -> String {
        WORKERS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding dead (failed or killed) task ids.
    pub fn dead_jobs_set(&self) -> String {
        DEAD_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding successfully completed task ids.
    pub fn done_jobs_set(&self) -> String {
        DONE_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb multimap table mapping workers to the tasks they hold.
    pub fn inflight_jobs_set(&self) -> String {
        INFLIGHT_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding encoded task arguments.
    pub fn job_data_hash(&self) -> String {
        TASK_DATA_HASH.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding task metadata.
    pub fn job_meta_hash(&self) -> String {
        JOB_META_HASH.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding scheduled (delayed or retrying) task ids.
    pub fn scheduled_jobs_set(&self) -> String {
        SCHEDULED_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table holding idempotency keys preventing duplicates.
    pub fn idempotency_key_set(&self) -> String {
        IDEMPOTENCY_KEY_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the redb table indexing idempotency keys by expiry time.
    ///
    /// apalis-redis has no equivalent, since Redis expires keys itself.
    pub(crate) fn idempotency_expiry_set(&self) -> String {
        IDEMPOTENCY_EXPIRY_SET.replace("{queue}", self.queue.as_ref())
    }
}
