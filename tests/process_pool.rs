use std::{collections::BTreeMap, time::Duration};

use process_pool::{
    PoolConfig, PoolError, ProcessFactoryConfig, ProcessPool, RejectedExecutionHandler, TimeUnit,
    WorkQueueConfig, WorkerState,
};
use serde_json::{Value, json};

fn config(core: usize, maximum: usize, queue_capacity: usize, keep_alive_ms: u64) -> PoolConfig {
    PoolConfig::new(
        core,
        maximum,
        keep_alive_ms,
        TimeUnit::Milliseconds,
        WorkQueueConfig::Bounded {
            capacity: queue_capacity,
        },
        ProcessFactoryConfig {
            program: env!("CARGO_BIN_EXE_echo-worker").into(),
            args: vec![],
            env: BTreeMap::new(),
            current_dir: None,
        },
        RejectedExecutionHandler::Abort,
    )
    .unwrap()
}

fn pid(result: &Value) -> u64 {
    result["pid"].as_u64().unwrap()
}

#[tokio::test]
async fn reuses_the_same_process_for_sequential_tasks() {
    let pool = ProcessPool::new(config(1, 1, 8, 1_000)).await.unwrap();

    let first = pool
        .execute(json!({ "op": "echo", "value": 1 }), Duration::from_secs(1))
        .await
        .unwrap();
    let second = pool
        .execute(json!({ "op": "echo", "value": 2 }), Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(pid(&first), pid(&second));
    assert_eq!(second["value"], 2);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn exposes_worker_level_monitoring_data() {
    let pool = ProcessPool::new(config(1, 1, 8, 1_000)).await.unwrap();
    let initial = pool.stats().await.unwrap();
    assert_eq!(initial.work_queue_capacity, 8);
    assert_eq!(initial.keep_alive_ms, 1_000);
    assert!(initial.workers.is_empty());

    let task_pool = pool.clone();
    let task = tokio::spawn(async move {
        task_pool
            .execute(
                json!({ "op": "sleep", "millis": 80 }),
                Duration::from_secs(1),
            )
            .await
    });
    wait_until_busy(&pool).await;

    let busy = pool.stats().await.unwrap();
    assert_eq!(busy.workers[0].state, WorkerState::Busy);
    assert!(busy.workers[0].process_id.is_some());
    assert!(busy.workers[0].current_task_id.is_some());
    task.await.unwrap().unwrap();

    let idle = pool.stats().await.unwrap();
    assert_eq!(idle.workers[0].state, WorkerState::Idle);
    assert_eq!(idle.workers[0].handled_task_count, 1);
    assert!(idle.workers[0].last_task_duration_ms.is_some());
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn grows_up_to_the_maximum_when_the_queue_is_full() {
    let pool = ProcessPool::new(config(1, 2, 0, 1_000)).await.unwrap();

    let first = pool.execute(
        json!({ "op": "sleep", "millis": 100, "value": "first" }),
        Duration::from_secs(1),
    );
    let second = pool.execute(
        json!({ "op": "sleep", "millis": 100, "value": "second" }),
        Duration::from_secs(1),
    );
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();

    assert_ne!(pid(&first), pid(&second));
    assert_eq!(pool.stats().await.unwrap().worker_count, 2);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn initialization_and_completion_do_not_implicitly_prewarm() {
    let pool = ProcessPool::new(config(4, 6, 8, 1_000)).await.unwrap();
    assert_eq!(pool.stats().await.unwrap().worker_count, 0);
    for expected in 1..=4 {
        pool.execute(json!({"op":"echo"}), Duration::from_secs(1))
            .await
            .unwrap();
        // Completion must not silently fill the remaining configured core slots.
        assert_eq!(pool.stats().await.unwrap().worker_count, expected);
    }
    pool.execute(json!({"op":"echo"}), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(pool.stats().await.unwrap().worker_count, 4);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_prewarm_only_creates_missing_core_workers() {
    let pool = ProcessPool::new(config(3, 5, 8, 1_000)).await.unwrap();
    pool.execute(json!({"op":"echo"}), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(pool.prestart_core_workers().await.unwrap(), 2);
    assert_eq!(pool.prestart_core_workers().await.unwrap(), 0);
    assert_eq!(pool.stats().await.unwrap().worker_count, 3);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn failure_replaces_only_the_activated_core_capacity() {
    let pool = ProcessPool::new(config(4, 6, 8, 1_000)).await.unwrap();
    let error = pool
        .execute(
            json!({"op":"sleep", "millis":200}),
            Duration::from_millis(30),
        )
        .await
        .unwrap_err();
    assert_eq!(error, PoolError::TaskTimeout { timeout_ms: 30 });
    let stats = pool.stats().await.unwrap();
    assert_eq!(stats.worker_count, 1);
    assert_eq!(stats.idle_worker_count, 1);
    assert_eq!(stats.failed_task_count, 1);
    assert_eq!(pool.prestart_core_workers().await.unwrap(), 3);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_core_pool_starts_on_demand_and_can_return_to_zero() {
    let pool = ProcessPool::new(config(0, 2, 8, 0)).await.unwrap();
    assert_eq!(pool.prestart_core_workers().await.unwrap(), 0);
    pool.execute(json!({"op":"echo"}), Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.unwrap().worker_count != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    pool.execute(json!({"op":"echo"}), Duration::from_secs(1))
        .await
        .unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn bad_executable_is_reported_when_starting_not_when_configuring() {
    let mut config = config(1, 1, 0, 1_000);
    config.process_factory.program = "/nonexistent/process-pool-test-worker".into();
    let pool = ProcessPool::new(config).await.unwrap();
    assert!(matches!(
        pool.prestart_core_workers().await,
        Err(PoolError::SpawnFailed(_))
    ));
    assert!(matches!(
        pool.execute(json!({}), Duration::from_secs(1)).await,
        Err(PoolError::SpawnFailed(_))
    ));
    assert_eq!(pool.stats().await.unwrap().worker_count, 0);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_policy_rejects_when_saturated() {
    let pool = ProcessPool::new(config(1, 1, 0, 1_000)).await.unwrap();
    let first_pool = pool.clone();
    let first = tokio::spawn(async move {
        first_pool
            .execute(
                json!({ "op": "sleep", "millis": 150 }),
                Duration::from_secs(1),
            )
            .await
    });

    wait_until_busy(&pool).await;
    let rejected = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await;

    assert_eq!(rejected.unwrap_err(), PoolError::Rejected);
    first.await.unwrap().unwrap();
    assert_eq!(pool.stats().await.unwrap().rejected_task_count, 1);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn timeout_kills_and_replaces_a_core_worker() {
    let pool = ProcessPool::new(config(1, 1, 1, 1_000)).await.unwrap();
    let before = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await
        .unwrap();

    let timeout = pool
        .execute(
            json!({ "op": "sleep", "millis": 200 }),
            Duration::from_millis(30),
        )
        .await
        .unwrap_err();
    assert_eq!(timeout, PoolError::TaskTimeout { timeout_ms: 30 });

    let after = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await
        .unwrap();
    assert_ne!(pid(&before), pid(&after));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn worker_application_errors_do_not_discard_a_healthy_process() {
    let pool = ProcessPool::new(config(1, 1, 1, 1_000)).await.unwrap();
    let before = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await
        .unwrap();
    let error = pool
        .execute(json!({ "op": "fail" }), Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(matches!(error, PoolError::WorkerReturned { .. }));
    let after = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(pid(&before), pid(&after));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn retires_non_core_workers_after_keep_alive() {
    let pool = ProcessPool::new(config(1, 2, 0, 60)).await.unwrap();
    let first = pool.execute(
        json!({ "op": "sleep", "millis": 50 }),
        Duration::from_secs(1),
    );
    let second = pool.execute(
        json!({ "op": "sleep", "millis": 50 }),
        Duration::from_secs(1),
    );
    let (first, second) = tokio::join!(first, second);
    first.unwrap();
    second.unwrap();
    assert_eq!(pool.stats().await.unwrap().worker_count, 2);

    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(pool.stats().await.unwrap().worker_count, 1);
    pool.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn notices_and_replaces_a_core_worker_that_exits_while_idle() {
    let pool = ProcessPool::new(config(1, 1, 1, 1_000)).await.unwrap();
    let before = pool
        .execute(json!({ "op": "echo" }), Duration::from_secs(1))
        .await
        .unwrap();
    let old_pid = pid(&before);

    let status = std::process::Command::new("kill")
        .args(["-9", &old_pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    for _ in 0..50 {
        let result = pool
            .execute(json!({ "op": "echo" }), Duration::from_secs(1))
            .await;
        if let Ok(result) = result
            && pid(&result) != old_pid
        {
            pool.shutdown().await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("core worker was not replaced after it exited");
}

async fn wait_until_busy(pool: &ProcessPool) {
    for _ in 0..50 {
        if pool.stats().await.unwrap().busy_worker_count == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("worker did not become busy");
}
