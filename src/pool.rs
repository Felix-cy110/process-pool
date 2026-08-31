use std::{
    collections::{HashMap, VecDeque},
    process::Stdio,
    time::Duration,
};

use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tracing::{debug, warn};

use crate::{
    PoolConfig, PoolError, ProcessFactoryConfig, RejectedExecutionHandler, WorkerRequest,
    WorkerResponse,
};

type TaskOutcome = Result<Value, PoolError>;

/// A cheap-to-clone handle to the process-pool supervisor.
#[derive(Clone)]
pub struct ProcessPool {
    command_tx: mpsc::UnboundedSender<CommandMessage>,
}

impl ProcessPool {
    /// Creates the pool without starting workers. Workers are created on demand.
    pub async fn new(config: PoolConfig) -> Result<Self, PoolError> {
        config.validate()?;
        let keep_alive = config.keep_alive()?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let supervisor = Supervisor::new(config, keep_alive, event_tx);
        tokio::spawn(supervisor.run(command_rx, event_rx));

        Ok(Self { command_tx })
    }

    /// Explicitly warms up to the configured core size, returning the number started.
    /// Already-created workers are preserved; repeated calls are idempotent.
    pub async fn prestart_core_workers(&self) -> Result<usize, PoolError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandMessage::Prestart { response_tx })
            .map_err(|_| PoolError::Closed)?;
        response_rx.await.map_err(|_| PoolError::Closed)?
    }

    /// Executes one JSON payload and waits for the corresponding worker response.
    pub async fn execute(&self, payload: Value, timeout: Duration) -> Result<Value, PoolError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandMessage::Submit {
                payload,
                timeout,
                response_tx,
            })
            .map_err(|_| PoolError::Closed)?;
        response_rx.await.map_err(|_| PoolError::Closed)?
    }

    pub async fn stats(&self) -> Result<PoolStats, PoolError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandMessage::Stats { response_tx })
            .map_err(|_| PoolError::Closed)?;
        response_rx.await.map_err(|_| PoolError::Closed)
    }

    /// Stops workers immediately. Queued and in-flight calls finish with `Closed`.
    pub async fn shutdown(&self) -> Result<(), PoolError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CommandMessage::Shutdown { response_tx })
            .map_err(|_| PoolError::Closed)?;
        response_rx.await.map_err(|_| PoolError::Closed)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PoolStats {
    pub core_pool_size: usize,
    pub maximum_pool_size: usize,
    pub keep_alive_ms: u64,
    pub work_queue_capacity: usize,
    pub rejection_policy: RejectedExecutionHandler,
    pub worker_count: usize,
    pub idle_worker_count: usize,
    pub busy_worker_count: usize,
    pub queued_task_count: usize,
    pub completed_task_count: u64,
    pub failed_task_count: u64,
    pub rejected_task_count: u64,
    pub caller_runs_task_count: u64,
    pub workers: Vec<WorkerStats>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Idle,
    Busy,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkerStats {
    pub worker_id: u64,
    pub process_id: Option<u32>,
    pub state: WorkerState,
    pub current_task_id: Option<u64>,
    pub state_for_ms: u64,
    pub uptime_ms: u64,
    pub handled_task_count: u64,
    pub last_task_duration_ms: Option<u64>,
}

enum CommandMessage {
    Prestart {
        response_tx: oneshot::Sender<Result<usize, PoolError>>,
    },
    Submit {
        payload: Value,
        timeout: Duration,
        response_tx: oneshot::Sender<TaskOutcome>,
    },
    Stats {
        response_tx: oneshot::Sender<PoolStats>,
    },
    Shutdown {
        response_tx: oneshot::Sender<()>,
    },
}

struct Job {
    id: u64,
    payload: Value,
    timeout: Duration,
    response_tx: oneshot::Sender<TaskOutcome>,
}

enum WorkerEvent {
    Finished {
        worker_id: u64,
        job: Job,
        outcome: TaskOutcome,
        reusable: bool,
    },
    OneShotFinished {
        response_tx: oneshot::Sender<TaskOutcome>,
        outcome: TaskOutcome,
    },
    Stopped {
        worker_id: u64,
        reason: String,
    },
}

struct WorkerSlot {
    job_tx: mpsc::UnboundedSender<Job>,
    process_id: Option<u32>,
    started_at: Instant,
    state_since: Instant,
    idle_since: Option<Instant>,
    current_task_id: Option<u64>,
    handled_task_count: u64,
    last_task_duration: Option<Duration>,
    task: JoinHandle<()>,
}

struct Supervisor {
    config: PoolConfig,
    keep_alive: Duration,
    event_tx: mpsc::UnboundedSender<WorkerEvent>,
    workers: HashMap<u64, WorkerSlot>,
    queue: VecDeque<Job>,
    next_worker_id: u64,
    next_job_id: u64,
    // Only replace core workers that have actually been started. Configuring a
    // large core size must not turn task completion into implicit prewarming.
    minimum_workers: usize,
    completed_task_count: u64,
    failed_task_count: u64,
    rejected_task_count: u64,
    caller_runs_task_count: u64,
}

impl Supervisor {
    fn new(
        config: PoolConfig,
        keep_alive: Duration,
        event_tx: mpsc::UnboundedSender<WorkerEvent>,
    ) -> Self {
        Self {
            config,
            keep_alive,
            event_tx,
            workers: HashMap::new(),
            queue: VecDeque::new(),
            next_worker_id: 1,
            next_job_id: 1,
            minimum_workers: 0,
            completed_task_count: 0,
            failed_task_count: 0,
            rejected_task_count: 0,
            caller_runs_task_count: 0,
        }
    }

    fn prestart_core_workers(&mut self) -> Result<usize, PoolError> {
        let missing = self
            .config
            .core_pool_size
            .saturating_sub(self.workers.len());
        for _ in 0..missing {
            let worker_id = self.spawn_worker()?;
            if let Some(job) = self.queue.pop_front()
                && let Err(job) = self.dispatch(worker_id, job)
            {
                self.place_job(job);
            }
        }
        Ok(missing)
    }

    async fn run(
        mut self,
        mut command_rx: mpsc::UnboundedReceiver<CommandMessage>,
        mut event_rx: mpsc::UnboundedReceiver<WorkerEvent>,
    ) {
        let tick_duration = idle_tick_duration(self.keep_alive);
        let mut idle_tick = tokio::time::interval(tick_duration);
        idle_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                command = command_rx.recv() => {
                    match command {
                        Some(CommandMessage::Prestart { response_tx }) => {
                            let _ = response_tx.send(self.prestart_core_workers());
                        }
                        Some(CommandMessage::Submit { payload, timeout, response_tx }) => {
                            self.submit(payload, timeout, response_tx);
                        }
                        Some(CommandMessage::Stats { response_tx }) => {
                            let _ = response_tx.send(self.stats_snapshot());
                        }
                        Some(CommandMessage::Shutdown { response_tx }) => {
                            self.stop_all_workers();
                            self.fail_queued(PoolError::Closed);
                            let _ = response_tx.send(());
                            break;
                        }
                        None => {
                            self.stop_all_workers();
                            self.fail_queued(PoolError::Closed);
                            break;
                        }
                    }
                }
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_worker_event(event);
                    }
                }
                _ = idle_tick.tick() => {
                    self.retire_idle_workers();
                }
            }
        }
    }

    fn submit(
        &mut self,
        payload: Value,
        timeout: Duration,
        response_tx: oneshot::Sender<TaskOutcome>,
    ) {
        let job = Job {
            id: self.next_job_id,
            payload,
            timeout,
            response_tx,
        };
        self.next_job_id = self.next_job_id.wrapping_add(1);
        self.place_job(job);
    }

    fn place_job(&mut self, mut job: Job) {
        // As in ThreadPoolExecutor, first grow toward core size on task arrival.
        // Reuse/queue only after that target has been reached.
        if self.workers.len() < self.config.core_pool_size {
            self.start_job(job);
            return;
        }
        while let Some(worker_id) = self.idle_worker_id() {
            match self.dispatch(worker_id, job) {
                Ok(()) => return,
                Err(returned_job) => job = returned_job,
            }
        }

        if self.workers.is_empty() {
            self.start_job(job);
            return;
        }

        if self.queue.len() < self.config.work_queue.capacity() {
            self.queue.push_back(job);
            return;
        }

        if self.workers.len() < self.config.maximum_pool_size {
            self.start_job(job);
            return;
        }

        self.apply_rejection_policy(job);
    }

    fn start_job(&mut self, job: Job) {
        match self.spawn_worker() {
            Ok(worker_id) => {
                if let Err(job) = self.dispatch(worker_id, job) {
                    let _ = job.response_tx.send(Err(PoolError::SpawnFailed(
                        "new worker stopped before accepting a task".into(),
                    )));
                }
            }
            Err(error) => {
                let _ = job.response_tx.send(Err(error));
            }
        }
    }

    fn apply_rejection_policy(&mut self, job: Job) {
        self.rejected_task_count += 1;
        match self.config.rejected_execution_handler {
            RejectedExecutionHandler::Abort => {
                let _ = job.response_tx.send(Err(PoolError::Rejected));
            }
            RejectedExecutionHandler::Discard => {
                let _ = job.response_tx.send(Err(PoolError::Discarded));
            }
            RejectedExecutionHandler::DiscardOldest => {
                if let Some(oldest) = self.queue.pop_front() {
                    let _ = oldest.response_tx.send(Err(PoolError::Discarded));
                    self.queue.push_back(job);
                } else {
                    let _ = job.response_tx.send(Err(PoolError::Rejected));
                }
            }
            RejectedExecutionHandler::CallerRuns => {
                self.caller_runs_task_count += 1;
                let factory = self.config.process_factory.clone();
                let event_tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let outcome =
                        run_one_shot_worker(factory, job.id, job.payload, job.timeout).await;
                    let _ = event_tx.send(WorkerEvent::OneShotFinished {
                        response_tx: job.response_tx,
                        outcome,
                    });
                });
            }
        }
    }

    fn handle_worker_event(&mut self, event: WorkerEvent) {
        match event {
            WorkerEvent::Finished {
                worker_id,
                job,
                outcome,
                reusable,
            } => {
                self.record_outcome(&outcome);

                if reusable {
                    if let Some(worker) = self.workers.get_mut(&worker_id) {
                        let now = Instant::now();
                        worker.last_task_duration = Some(now.duration_since(worker.state_since));
                        worker.handled_task_count += 1;
                        worker.current_task_id = None;
                        worker.idle_since = Some(now);
                        worker.state_since = now;
                    }
                } else {
                    self.remove_worker(worker_id);
                }
                let _ = job.response_tx.send(outcome);

                self.drain_one_queued_task();
                self.restore_core_workers();
            }
            WorkerEvent::OneShotFinished {
                response_tx,
                outcome,
            } => {
                self.record_outcome(&outcome);
                let _ = response_tx.send(outcome);
            }
            WorkerEvent::Stopped { worker_id, reason } => {
                warn!(worker_id, %reason, "idle worker process stopped");
                self.remove_worker(worker_id);
                self.drain_one_queued_task();
                self.restore_core_workers();
            }
        }
    }

    fn record_outcome(&mut self, outcome: &TaskOutcome) {
        if outcome.is_ok() {
            self.completed_task_count += 1;
        } else {
            self.failed_task_count += 1;
        }
    }

    fn drain_one_queued_task(&mut self) {
        let Some(job) = self.queue.pop_front() else {
            return;
        };

        if let Some(worker_id) = self.idle_worker_id() {
            if let Err(job) = self.dispatch(worker_id, job) {
                self.place_job(job);
            }
            return;
        }

        if self.workers.is_empty() || self.workers.len() < self.config.core_pool_size {
            match self.spawn_worker() {
                Ok(worker_id) => {
                    if let Err(job) = self.dispatch(worker_id, job) {
                        let _ = job.response_tx.send(Err(PoolError::SpawnFailed(
                            "replacement worker stopped before accepting a task".into(),
                        )));
                    }
                }
                Err(error) => {
                    let _ = job.response_tx.send(Err(error.clone()));
                    if self.workers.is_empty() {
                        self.fail_queued(error);
                    }
                }
            }
        } else {
            self.queue.push_front(job);
        }
    }

    fn restore_core_workers(&mut self) {
        while self.workers.len() < self.minimum_workers {
            match self.spawn_worker() {
                Ok(worker_id) => {
                    if let Some(job) = self.queue.pop_front()
                        && let Err(job) = self.dispatch(worker_id, job)
                    {
                        let _ = job.response_tx.send(Err(PoolError::SpawnFailed(
                            "replacement worker stopped before accepting a task".into(),
                        )));
                    }
                }
                Err(error) => {
                    warn!(%error, "failed to restore a core worker");
                    if self.workers.is_empty() {
                        self.fail_queued(error);
                    }
                    break;
                }
            }
        }
    }

    fn spawn_worker(&mut self) -> Result<u64, PoolError> {
        let worker_id = self.next_worker_id;
        self.next_worker_id = self.next_worker_id.wrapping_add(1);
        let child = spawn_child(&self.config.process_factory)?;
        let process_id = child.id();
        let (job_tx, job_rx) = mpsc::unbounded_channel();
        let event_tx = self.event_tx.clone();
        let task = tokio::spawn(worker_loop(worker_id, child, job_rx, event_tx));
        let now = Instant::now();
        self.workers.insert(
            worker_id,
            WorkerSlot {
                job_tx,
                process_id,
                started_at: now,
                state_since: now,
                idle_since: Some(now),
                current_task_id: None,
                handled_task_count: 0,
                last_task_duration: None,
                task,
            },
        );
        self.minimum_workers = self
            .minimum_workers
            .max(self.workers.len().min(self.config.core_pool_size));
        debug!(worker_id, "started worker process");
        Ok(worker_id)
    }

    fn dispatch(&mut self, worker_id: u64, job: Job) -> Result<(), Job> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return Err(job);
        };
        let job_id = job.id;
        worker.state_since = Instant::now();
        worker.idle_since = None;
        worker.current_task_id = Some(job_id);
        match worker.job_tx.send(job) {
            Ok(()) => Ok(()),
            Err(error) => {
                let job = error.0;
                self.remove_worker(worker_id);
                Err(job)
            }
        }
    }

    fn idle_worker_id(&self) -> Option<u64> {
        self.workers
            .iter()
            .find_map(|(id, worker)| worker.idle_since.map(|_| *id))
    }

    fn retire_idle_workers(&mut self) {
        let mut removable = self
            .workers
            .len()
            .saturating_sub(self.config.core_pool_size);
        if removable == 0 || !self.queue.is_empty() {
            return;
        }

        let now = Instant::now();
        let worker_ids: Vec<u64> = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                worker
                    .idle_since
                    .filter(|idle_since| now.duration_since(*idle_since) >= self.keep_alive)
                    .map(|_| *id)
            })
            .collect();

        for worker_id in worker_ids {
            if removable == 0 {
                break;
            }
            self.remove_worker(worker_id);
            removable -= 1;
            debug!(worker_id, "retired idle worker process");
        }
    }

    fn remove_worker(&mut self, worker_id: u64) {
        if let Some(worker) = self.workers.remove(&worker_id) {
            worker.task.abort();
        }
    }

    fn stop_all_workers(&mut self) {
        for (_, worker) in self.workers.drain() {
            worker.task.abort();
        }
    }

    fn fail_queued(&mut self, error: PoolError) {
        while let Some(job) = self.queue.pop_front() {
            let _ = job.response_tx.send(Err(error.clone()));
        }
    }

    fn stats_snapshot(&self) -> PoolStats {
        let now = Instant::now();
        let idle_worker_count = self
            .workers
            .values()
            .filter(|worker| worker.idle_since.is_some())
            .count();
        let mut workers: Vec<WorkerStats> = self
            .workers
            .iter()
            .map(|(worker_id, worker)| WorkerStats {
                worker_id: *worker_id,
                process_id: worker.process_id,
                state: if worker.idle_since.is_some() {
                    WorkerState::Idle
                } else {
                    WorkerState::Busy
                },
                current_task_id: worker.current_task_id,
                state_for_ms: duration_ms(now.duration_since(worker.state_since)),
                uptime_ms: duration_ms(now.duration_since(worker.started_at)),
                handled_task_count: worker.handled_task_count,
                last_task_duration_ms: worker.last_task_duration.map(duration_ms),
            })
            .collect();
        workers.sort_unstable_by_key(|worker| worker.worker_id);
        PoolStats {
            core_pool_size: self.config.core_pool_size,
            maximum_pool_size: self.config.maximum_pool_size,
            keep_alive_ms: duration_ms(self.keep_alive),
            work_queue_capacity: self.config.work_queue.capacity(),
            rejection_policy: self.config.rejected_execution_handler,
            worker_count: self.workers.len(),
            idle_worker_count,
            busy_worker_count: self.workers.len() - idle_worker_count,
            queued_task_count: self.queue.len(),
            completed_task_count: self.completed_task_count,
            failed_task_count: self.failed_task_count,
            rejected_task_count: self.rejected_task_count,
            caller_runs_task_count: self.caller_runs_task_count,
            workers,
        }
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn idle_tick_duration(keep_alive: Duration) -> Duration {
    if keep_alive.is_zero() {
        return Duration::from_millis(50);
    }
    let half = keep_alive / 2;
    half.clamp(Duration::from_millis(50), Duration::from_secs(1))
}

fn spawn_child(factory: &ProcessFactoryConfig) -> Result<Child, PoolError> {
    let mut command = Command::new(&factory.program);
    command
        .args(&factory.args)
        .envs(&factory.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(current_dir) = &factory.current_dir {
        command.current_dir(current_dir);
    }
    command
        .spawn()
        .map_err(|error| PoolError::SpawnFailed(error.to_string()))
}

async fn worker_loop(
    worker_id: u64,
    mut child: Child,
    mut job_rx: mpsc::UnboundedReceiver<Job>,
    event_tx: mpsc::UnboundedSender<WorkerEvent>,
) {
    let Some(mut stdin) = child.stdin.take() else {
        return;
    };
    let Some(stdout) = child.stdout.take() else {
        return;
    };
    let mut lines = BufReader::new(stdout).lines();

    loop {
        let job = tokio::select! {
            job = job_rx.recv() => {
                let Some(job) = job else {
                    break;
                };
                job
            }
            status = child.wait() => {
                let reason = match status {
                    Ok(status) => format!("exited with status {status}"),
                    Err(error) => format!("failed while waiting for exit: {error}"),
                };
                let _ = event_tx.send(WorkerEvent::Stopped { worker_id, reason });
                return;
            }
        };
        let (outcome, reusable) =
            exchange_with_timeout(&mut stdin, &mut lines, job.id, &job.payload, job.timeout).await;
        let should_stop = !reusable;
        if event_tx
            .send(WorkerEvent::Finished {
                worker_id,
                job,
                outcome,
                reusable,
            })
            .is_err()
        {
            break;
        }
        if should_stop {
            break;
        }
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn run_one_shot_worker(
    factory: ProcessFactoryConfig,
    job_id: u64,
    payload: Value,
    timeout: Duration,
) -> TaskOutcome {
    let mut child = spawn_child(&factory)?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        PoolError::SpawnFailed("worker stdin was not configured as a pipe".into())
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        PoolError::SpawnFailed("worker stdout was not configured as a pipe".into())
    })?;
    let mut lines = BufReader::new(stdout).lines();
    let (outcome, _) =
        exchange_with_timeout(&mut stdin, &mut lines, job_id, &payload, timeout).await;
    let _ = child.kill().await;
    let _ = child.wait().await;
    outcome
}

async fn exchange_with_timeout(
    stdin: &mut ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    job_id: u64,
    payload: &Value,
    timeout: Duration,
) -> (TaskOutcome, bool) {
    match tokio::time::timeout(timeout, exchange_one(stdin, lines, job_id, payload)).await {
        Ok(Ok(response)) => {
            let outcome = worker_response_to_result(response);
            let reusable = !matches!(outcome, Err(PoolError::Protocol(_)));
            (outcome, reusable)
        }
        Ok(Err(error)) => (Err(error), false),
        Err(_) => (
            Err(PoolError::TaskTimeout {
                timeout_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
            }),
            false,
        ),
    }
}

async fn exchange_one(
    stdin: &mut ChildStdin,
    lines: &mut Lines<BufReader<ChildStdout>>,
    job_id: u64,
    payload: &Value,
) -> Result<WorkerResponse, PoolError> {
    let request = WorkerRequest {
        id: job_id,
        payload: payload.clone(),
    };
    let mut encoded = serde_json::to_vec(&request)
        .map_err(|error| PoolError::Protocol(format!("cannot encode request: {error}")))?;
    encoded.push(b'\n');
    stdin
        .write_all(&encoded)
        .await
        .map_err(|error| PoolError::WorkerIo(error.to_string()))?;
    stdin
        .flush()
        .await
        .map_err(|error| PoolError::WorkerIo(error.to_string()))?;

    let line = lines
        .next_line()
        .await
        .map_err(|error| PoolError::WorkerIo(error.to_string()))?
        .ok_or(PoolError::WorkerExited)?;
    let response: WorkerResponse = serde_json::from_str(&line)
        .map_err(|error| PoolError::Protocol(format!("invalid worker response: {error}")))?;
    if response.id != job_id {
        return Err(PoolError::Protocol(format!(
            "response id {} does not match request id {job_id}",
            response.id
        )));
    }
    Ok(response)
}

fn worker_response_to_result(response: WorkerResponse) -> TaskOutcome {
    match (response.ok, response.result, response.error) {
        (true, result, None) => Ok(result.unwrap_or(Value::Null)),
        (false, None, Some(error)) => Err(PoolError::WorkerReturned {
            code: error.code,
            message: error.message,
            details: error.details,
        }),
        (true, _, Some(_)) => Err(PoolError::Protocol(
            "successful response must not contain error".into(),
        )),
        (false, Some(_), _) => Err(PoolError::Protocol(
            "failed response must not contain result".into(),
        )),
        (false, None, None) => Err(PoolError::Protocol(
            "failed response must contain error".into(),
        )),
    }
}
