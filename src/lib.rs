//! Reusable child-process pool.
//!
//! The pool owns long-lived worker processes and exchanges one NDJSON message per
//! request and response over each worker's stdin/stdout. Workers are single-flight,
//! while the pool provides parallelism by managing multiple worker processes.

mod config;
mod error;
mod pool;
mod protocol;
pub mod server;

pub use config::{
    PoolConfig, ProcessFactoryConfig, RejectedExecutionHandler, TimeUnit, WorkQueueConfig,
};
pub use error::PoolError;
pub use pool::{PoolStats, ProcessPool, WorkerState, WorkerStats};
pub use protocol::{WorkerError, WorkerRequest, WorkerResponse};
