use apalis_core::task::{status::StatusError, task_id::TaskIdError};

/// Represents a wrapper for errors encountered on this crate
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Inner engine error
    #[error(transparent)]
    Database(#[from] redb::Error),
    /// Error handling json
    #[error("JsonError: {0}")]
    Json(#[from] serde_json::Error),
    /// Reenqueue Mismatch error
    #[error("ReenqueueMismatch: Queued [{queued}] , Abandoned[{abandoned}] ")]
    ReenqueueMismatch {
        /// The db count
        queued: usize,
        /// The workers count
        abandoned: usize,
    },
    /// Error decoding the task_id
    #[error("TaskIdError: {0}")]
    TaskIdError(TaskIdError),
    /// Error decoding the task status
    #[error("StatusError: {0}")]
    StatusError(StatusError),
    /// Worker was removed in the database
    #[error("WorkerOutOfSync")]
    WorkerOutOfSync,

    /// Tried to register a worker that already exists
    #[error("WorkerAlreadyExists: {0}")]
    WorkerAlreadyExists(String),

    /// The worker is not registered with the queue
    #[error("WorkerNotRegistered: {0}")]
    WorkerNotRegistered(String),

    /// A blocking database operation panicked or was cancelled
    #[error("BlockingTaskFailed: {0}")]
    BlockingTaskFailed(String),
}

macro_rules! impl_from_redb {
    ($($ty:ty),*) => {
        $(
            impl From<$ty> for Error {
                fn from(e: $ty) -> Self {
                    Error::Database(e.into())
                }
            }
        )*
    };
}

impl_from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);
