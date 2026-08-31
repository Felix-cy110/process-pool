use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum PoolError {
    #[error("the process pool has not been initialized")]
    NotInitialized,
    #[error(
        "the process pool is already initialized; restart the service to use another configuration"
    )]
    AlreadyInitialized,
    #[error("invalid pool configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to start worker process: {0}")]
    SpawnFailed(String),
    #[error("the process pool is saturated")]
    Rejected,
    #[error("the task was discarded by the rejection policy")]
    Discarded,
    #[error("the process pool is closed")]
    Closed,
    #[error("worker I/O failed: {0}")]
    WorkerIo(String),
    #[error("worker protocol error: {0}")]
    Protocol(String),
    #[error("worker exited before returning a response")]
    WorkerExited,
    #[error("task timed out after {timeout_ms} ms")]
    TaskTimeout { timeout_ms: u64 },
    #[error("worker returned error {code}: {message}")]
    WorkerReturned {
        code: String,
        message: String,
        details: Option<serde_json::Value>,
    },
}
