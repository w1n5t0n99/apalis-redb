use std::{
    collections::HashMap,
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    task::Waker,
};

use redb::Database;

use crate::error::Error;

/// A cloneable handle to a redb [`Database`].
///
/// redb is an embedded database, so every worker and producer sharing a
/// database file must share the same `Connection`. Besides the database it
/// carries an in-process notifier used to wake idle workers as soon as tasks
/// become available.
#[derive(Clone)]
pub struct Connection {
    db: Arc<Database>,
    notifier: Arc<Notifier>,
}

impl Connection {
    /// Wrap an opened redb database
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(db),
            notifier: Arc::default(),
        }
    }

    /// Get the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    pub(crate) fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    pub(crate) fn register_waker(&self, queue: &str, worker: &str, waker: &Waker) {
        self.notifier.register(queue, worker, waker);
    }

    pub(crate) fn unregister_waker(&self, queue: &str, worker: &str) {
        self.notifier.unregister(queue, worker);
    }

    pub(crate) fn notify(&self, queue: &str) {
        self.notifier.notify(queue);
    }
}

impl From<Database> for Connection {
    fn from(db: Database) -> Self {
        Self::new(db)
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

/// Wakers of idle workers, keyed by queue then worker name
#[derive(Debug, Default)]
struct Notifier {
    wakers: Mutex<HashMap<String, HashMap<String, Waker>>>,
}

impl Notifier {
    fn register(&self, queue: &str, worker: &str, waker: &Waker) {
        let Ok(mut wakers) = self.wakers.lock() else {
            return;
        };
        let queue = wakers.entry(queue.to_owned()).or_default();
        match queue.get_mut(worker) {
            Some(existing) if existing.will_wake(waker) => {}
            Some(existing) => existing.clone_from(waker),
            None => {
                queue.insert(worker.to_owned(), waker.clone());
            }
        }
    }

    fn unregister(&self, queue: &str, worker: &str) {
        if let Ok(mut wakers) = self.wakers.lock()
            && let Some(queue) = wakers.get_mut(queue)
        {
            queue.remove(worker);
        }
    }

    fn notify(&self, queue: &str) {
        let Ok(wakers) = self.wakers.lock() else {
            return;
        };
        if let Some(queue) = wakers.get(queue) {
            for waker in queue.values() {
                waker.wake_by_ref();
            }
        }
    }
}

/// Shorthand to create or open a redb database at `path`
pub async fn connect<P: AsRef<Path>>(path: P) -> Result<Connection, Error> {
    let path = path.as_ref().to_owned();
    run_blocking(move || Ok(Database::create(path)?))
        .await
        .map(Connection::new)
}

/// Runs a blocking redb operation off the async executor when a supported
/// runtime is available, otherwise runs it inline.
pub(crate) async fn run_blocking<F, T>(f: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "tokio-comp")]
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return handle
            .spawn_blocking(f)
            .await
            .map_err(|e| Error::BlockingTaskFailed(e.to_string()))?;
    }

    #[cfg(feature = "smol-comp")]
    return blocking::unblock(f).await;

    #[allow(unreachable_code)]
    f()
}
