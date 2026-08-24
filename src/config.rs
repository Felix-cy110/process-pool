use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

use crate::PoolError;

/// Unit used by the `keep_alive_time` pool parameter.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
    Minutes,
}

impl TimeUnit {
    pub(crate) fn duration(self, value: u64) -> Result<Duration, PoolError> {
        match self {
            Self::Milliseconds => Ok(Duration::from_millis(value)),
            Self::Seconds => Ok(Duration::from_secs(value)),
            Self::Minutes => Ok(Duration::from_secs(value.checked_mul(60).ok_or_else(
                || PoolError::InvalidConfig("keep_alive_time is too large".into()),
            )?)),
        }
    }
}

/// Queue used after all core processes are busy.
///
/// A capacity of zero behaves like Java's `SynchronousQueue`: tasks are handed
/// directly to a worker or cause the pool to grow up to its maximum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkQueueConfig {
    Bounded { capacity: usize },
}

impl WorkQueueConfig {
    pub(crate) fn capacity(&self) -> usize {
        match self {
            Self::Bounded { capacity } => *capacity,
        }
    }
}

/// Serializable equivalent of Java's `ThreadFactory` for child processes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessFactoryConfig {
    pub program: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub current_dir: Option<PathBuf>,
}

/// Policy applied when all processes are busy and the work queue is full.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RejectedExecutionHandler {
    /// Immediately return a saturation error. Recommended for RPC services.
    Abort,
    /// Drop the new task, while still notifying its caller with an error.
    Discard,
    /// Drop the oldest queued task and enqueue the new task.
    DiscardOldest,
    /// Run the task in a one-shot process outside the managed maximum.
    ///
    /// This is only an analogy to Java's `CallerRunsPolicy` and can create many
    /// processes under load. Prefer `abort` for an RPC-facing deployment.
    CallerRuns,
}

/// The seven Java `ThreadPoolExecutor`-inspired initialization parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub core_pool_size: usize,
    pub maximum_pool_size: usize,
    pub keep_alive_time: u64,
    pub time_unit: TimeUnit,
    pub work_queue: WorkQueueConfig,
    pub process_factory: ProcessFactoryConfig,
    pub rejected_execution_handler: RejectedExecutionHandler,
}

impl PoolConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        core_pool_size: usize,
        maximum_pool_size: usize,
        keep_alive_time: u64,
        time_unit: TimeUnit,
        work_queue: WorkQueueConfig,
        process_factory: ProcessFactoryConfig,
        rejected_execution_handler: RejectedExecutionHandler,
    ) -> Result<Self, PoolError> {
        let config = Self {
            core_pool_size,
            maximum_pool_size,
            keep_alive_time,
            time_unit,
            work_queue,
            process_factory,
            rejected_execution_handler,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), PoolError> {
        if self.maximum_pool_size == 0 {
            return Err(PoolError::InvalidConfig(
                "maximum_pool_size must be greater than zero".into(),
            ));
        }
        if self.core_pool_size > self.maximum_pool_size {
            return Err(PoolError::InvalidConfig(
                "core_pool_size must not exceed maximum_pool_size".into(),
            ));
        }
        if self.process_factory.program.as_os_str().is_empty() {
            return Err(PoolError::InvalidConfig(
                "process_factory.program must not be empty".into(),
            ));
        }
        self.time_unit.duration(self.keep_alive_time)?;
        Ok(())
    }

    pub(crate) fn keep_alive(&self) -> Result<Duration, PoolError> {
        self.time_unit.duration(self.keep_alive_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn factory() -> ProcessFactoryConfig {
        ProcessFactoryConfig {
            program: "worker".into(),
            args: vec![],
            env: BTreeMap::new(),
            current_dir: None,
        }
    }

    #[test]
    fn validates_the_seven_parameters() {
        let error = PoolConfig::new(
            3,
            2,
            30,
            TimeUnit::Seconds,
            WorkQueueConfig::Bounded { capacity: 10 },
            factory(),
            RejectedExecutionHandler::Abort,
        )
        .unwrap_err();

        assert!(error.to_string().contains("core_pool_size"));
    }

    #[test]
    fn zero_capacity_queue_is_valid() {
        assert!(
            PoolConfig::new(
                0,
                1,
                0,
                TimeUnit::Milliseconds,
                WorkQueueConfig::Bounded { capacity: 0 },
                factory(),
                RejectedExecutionHandler::Abort,
            )
            .is_ok()
        );
    }
}
