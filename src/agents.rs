//! Stateful Claude Code processes. They use Claude's control protocol, not the
//! stateless worker protocol: a conversation is always routed to its own PID.
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, mpsc, oneshot},
    time::{Instant, timeout},
};

pub const CONDUIT_REPOSITORY: &str = "https://github.com/cogwheel0/conduit.git";
const EVENT_LIMIT: usize = 256;
const LINE_LIMIT: usize = 1024 * 1024;
const DISPLAY_LIMIT: usize = 16 * 1024;
static IDS: AtomicU64 = AtomicU64::new(1);
type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub claude_program: PathBuf,
    pub workspace_root: PathBuf,
    pub max_agents: usize,
}

#[derive(Clone)]
pub struct AgentManager(Arc<Manager>);
struct Manager {
    config: AgentConfig,
    registry: Mutex<BTreeMap<String, Handle>>,
    // Serializes workspace creation and lifecycle changes, never model turns.
    lifecycle: Mutex<bool>, // true once shutdown begins
}
#[derive(Clone)]
struct Handle {
    record: Arc<Mutex<Record>>,
    tx: mpsc::Sender<Envelope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Starting,
    Idle,
    Busy,
    AwaitingPermission,
    Interrupting,
    Stopped,
    Failed,
}
impl AgentState {
    fn running(self) -> bool {
        !matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Clone, Serialize)]
pub struct AgentInfo {
    pub id: String,
    pub label: String,
    pub generation: u64,
    pub pid: Option<u32>,
    pub cwd: PathBuf,
    pub session_id: Option<String>,
    pub state: AgentState,
    pub started_at_ms: u64,
    pub completed_turns: u64,
    pub failed_turns: u64,
    pub last_error: Option<String>,
    pub pending_permissions: BTreeMap<String, Value>,
}
struct Record {
    info: AgentInfo,
    events: VecDeque<Value>,
    sequence: u64,
}
impl Record {
    fn event(&mut self, kind: &str, data: Value) {
        self.sequence += 1;
        let serialized = data.to_string();
        let data = if serialized.len() > DISPLAY_LIMIT {
            json!({"truncated": true, "preview": clip(&serialized, DISPLAY_LIMIT)})
        } else {
            data
        };
        self.events
            .push_back(json!({"id":self.sequence,"at_ms":now_ms(),"kind":kind,"data":data}));
        if self.events.len() > EVENT_LIMIT {
            self.events.pop_front();
        }
    }
    fn snapshot(&self) -> Value {
        serde_json::to_value(&self.info).expect("serializable agent")
    }
}

enum Action {
    Send(String),
    Interrupt,
    Permission { request_id: String, allow: bool },
    Stop,
}
struct Envelope {
    generation: u64,
    action: Action,
    reply: oneshot::Sender<Result<Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateParams {
    #[serde(default)]
    label: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    agent_id: String,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetParams {
    agent_id: String,
    #[serde(default)]
    after_event_id: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendParams {
    agent_id: String,
    generation: u64,
    prompt: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionParams {
    agent_id: String,
    generation: u64,
    request_id: String,
    allow: bool,
}
fn params<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|e| format!("参数不合法：{e}"))
}

impl AgentManager {
    pub fn new(mut config: AgentConfig) -> Result<Self> {
        if !(1..=64).contains(&config.max_agents) {
            return Err("max-cc-agents 必须为 1–64".into());
        }
        if config.workspace_root.is_relative() {
            config.workspace_root = std::env::current_dir()
                .map_err(|e| e.to_string())?
                .join(config.workspace_root);
        }
        Ok(Self(Arc::new(Manager {
            config,
            registry: Mutex::new(BTreeMap::new()),
            lifecycle: Mutex::new(false),
        })))
    }

    pub async fn rpc(&self, method: &str, value: Value) -> Result<Value> {
        match method {
            "cc.status" | "cc.prepare" => {
                if !value.as_object().is_some_and(|o| o.is_empty()) {
                    return Err("此方法不接受参数".into());
                }
                if method == "cc.prepare" {
                    self.prepare().await
                } else {
                    self.status().await
                }
            }
            "cc.create" => self.create(params::<CreateParams>(value)?.label).await,
            "cc.get" => {
                let p: GetParams = params(value)?;
                let handle = self.handle(&p.agent_id).await?;
                let record = handle.record.lock().await;
                let oldest = record
                    .events
                    .front()
                    .and_then(|e| e["id"].as_u64())
                    .unwrap_or(0);
                Ok(
                    json!({"agent": record.info, "events": record.events.iter().filter(|e| e["id"].as_u64().unwrap_or(0) > p.after_event_id).collect::<Vec<_>>(),
                    "cursor": record.sequence, "truncated": p.after_event_id.saturating_add(1) < oldest}),
                )
            }
            "cc.send" => {
                let p: SendParams = params(value)?;
                if p.prompt.trim().is_empty() || p.prompt.len() > 32768 {
                    return Err("提示词不能为空且不能超过 32 KiB".into());
                }
                request(
                    &self.handle(&p.agent_id).await?,
                    p.generation,
                    Action::Send(p.prompt),
                )
                .await
            }
            "cc.permission" => {
                let p: PermissionParams = params(value)?;
                request(
                    &self.handle(&p.agent_id).await?,
                    p.generation,
                    Action::Permission {
                        request_id: p.request_id,
                        allow: p.allow,
                    },
                )
                .await
            }
            "cc.interrupt" => {
                let p: Target = params(value)?;
                request(
                    &self.handle(&p.agent_id).await?,
                    p.generation,
                    Action::Interrupt,
                )
                .await
            }
            "cc.stop" | "cc.restart" => {
                let p: Target = params(value)?;
                let closed = self.0.lifecycle.lock().await;
                if *closed {
                    return Err("服务正在关闭".into());
                }
                let handle = self.handle(&p.agent_id).await?;
                let info = handle.record.lock().await.info.clone();
                if info.generation != p.generation {
                    return Err("进程已更换，请刷新后操作".into());
                }
                if info.state.running() {
                    request(&handle, p.generation, Action::Stop).await?;
                }
                // A stop reply is sent only after the child has been reaped.
                if method == "cc.restart" {
                    if self.running_count().await >= self.0.config.max_agents {
                        return Err("已达到 CC 进程上限".into());
                    }
                    let mut next = handle.record.lock().await;
                    next.info.generation += 1;
                    let record = handle.record.clone();
                    drop(next);
                    let replacement = self.spawn(record).await?;
                    self.0.registry.lock().await.insert(p.agent_id, replacement);
                }
                Ok(handle.record.lock().await.snapshot())
            }
            _ => Err("未知 CC 方法".into()),
        }
    }

    async fn handle(&self, id: &str) -> Result<Handle> {
        self.0
            .registry
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| "未找到此 Agent".into())
    }
    async fn running_count(&self) -> usize {
        let handles: Vec<_> = self.0.registry.lock().await.values().cloned().collect();
        let mut count = 0;
        for h in handles {
            if h.record.lock().await.info.state.running() {
                count += 1;
            }
        }
        count
    }
    pub async fn status(&self) -> Result<Value> {
        let handles: Vec<_> = self.0.registry.lock().await.values().cloned().collect();
        let mut agents = Vec::new();
        for h in handles {
            agents.push(h.record.lock().await.snapshot());
        }
        Ok(
            json!({"enabled":true,"repository":CONDUIT_REPOSITORY,"repository_path":self.base(),
            "repository_ready":self.base().join(".git").exists(),"claude_program":self.0.config.claude_program,
            "max_agents":self.0.config.max_agents,"agents":agents}),
        )
    }
    fn base(&self) -> PathBuf {
        self.0.config.workspace_root.join("conduit")
    }

    async fn verify_repository(&self) -> Result<()> {
        let output = git(&self.base(), &["remote", "get-url", "origin"]).await?;
        if output.trim() != CONDUIT_REPOSITORY {
            return Err("现有 conduit 目录的 origin 与预期仓库不一致，未作修改".into());
        }
        Ok(())
    }
    pub async fn prepare(&self) -> Result<Value> {
        let closed = self.0.lifecycle.lock().await;
        if *closed {
            return Err("服务正在关闭".into());
        }
        let output = timeout(
            Duration::from_secs(10),
            Command::new(&self.0.config.claude_program)
                .arg("--version")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| "claude --version 超时")?
        .map_err(|e| format!("无法启动本机 Claude Code，请先自行安装并登录：{e}"))?;
        if !output.status.success() {
            return Err("claude --version 失败，请检查本机安装".into());
        }
        tokio::fs::create_dir_all(&self.0.config.workspace_root)
            .await
            .map_err(|e| e.to_string())?;
        if !self.base().exists() {
            git(
                &self.0.config.workspace_root,
                &["clone", "--", CONDUIT_REPOSITORY, "conduit"],
            )
            .await?;
        }
        self.verify_repository().await?;
        let mut status = self.status().await?;
        status["claude_version"] = json!(clip(String::from_utf8_lossy(&output.stdout).trim(), 256));
        Ok(status)
    }
    async fn create(&self, label: String) -> Result<Value> {
        if label.chars().count() > 80 {
            return Err("名称不能超过 80 字符".into());
        }
        let closed = self.0.lifecycle.lock().await;
        if *closed {
            return Err("服务正在关闭".into());
        }
        if self.running_count().await >= self.0.config.max_agents {
            return Err("已达到 CC 进程上限，请先停止一个 Agent".into());
        }
        if self.0.registry.lock().await.len() >= 128 {
            return Err("本次服务最多保留 128 个 Agent 记录，请重启服务后再创建".into());
        }
        self.verify_repository()
            .await
            .map_err(|e| format!("请先准备 conduit 项目：{e}"))?;
        let id = format!("cc-{}-{}", now_ms(), IDS.fetch_add(1, Ordering::Relaxed));
        let cwd = self
            .0
            .config
            .workspace_root
            .join("agents")
            .join(&id)
            .join("conduit");
        tokio::fs::create_dir_all(cwd.parent().expect("parent"))
            .await
            .map_err(|e| e.to_string())?;
        git(
            &self.base(),
            &[
                "worktree",
                "add",
                "--detach",
                cwd.to_str().ok_or("目录不是 UTF-8")?,
                "HEAD",
            ],
        )
        .await?;
        let record = Arc::new(Mutex::new(Record {
            info: AgentInfo {
                label: if label.trim().is_empty() {
                    id.clone()
                } else {
                    label
                },
                id: id.clone(),
                generation: 1,
                pid: None,
                cwd,
                session_id: None,
                state: AgentState::Starting,
                started_at_ms: now_ms(),
                completed_turns: 0,
                failed_turns: 0,
                last_error: None,
                pending_permissions: BTreeMap::new(),
            },
            events: VecDeque::new(),
            sequence: 0,
        }));
        let handle = self.spawn(record.clone()).await?;
        self.0.registry.lock().await.insert(id, handle);
        let snapshot = record.lock().await.snapshot();
        Ok(snapshot)
    }
    async fn spawn(&self, record: Arc<Mutex<Record>>) -> Result<Handle> {
        let mut r = record.lock().await;
        let mut command = Command::new(&self.0.config.claude_program);
        command
            .args([
                "-p",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--verbose",
                "--include-partial-messages",
                "--safe-mode",
                "--permission-mode",
                "manual",
                "--permission-prompt-tool",
                "stdio",
            ])
            .current_dir(&r.info.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(session) = &r.info.session_id {
            command.args(["--resume", session]);
        }
        #[cfg(unix)]
        command.process_group(0);
        let child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let error = format!(
                    "无法启动本机 Claude Code：{e}；工作目录保留在 {}",
                    r.info.cwd.display()
                );
                r.info.state = AgentState::Failed;
                r.info.last_error = Some(error.clone());
                return Err(error);
            }
        };
        r.info.pid = child.id();
        r.info.state = AgentState::Starting;
        r.info.started_at_ms = now_ms();
        r.info.last_error = None;
        r.info.pending_permissions.clear();
        r.event(
            "lifecycle",
            json!({"message":"Claude Code 已启动，等待控制协议握手", "pid":child.id()}),
        );
        drop(r);
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(run_agent(child, record.clone(), rx));
        Ok(Handle { record, tx })
    }
    pub async fn shutdown(&self) {
        let mut closed = self.0.lifecycle.lock().await;
        *closed = true;
        let handles: Vec<_> = self.0.registry.lock().await.values().cloned().collect();
        let mut tasks = tokio::task::JoinSet::new();
        for h in handles {
            tasks.spawn(async move {
                let info = h.record.lock().await.info.clone();
                if info.state.running() {
                    let _ = request(&h, info.generation, Action::Stop).await;
                }
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

async fn request(handle: &Handle, generation: u64, action: Action) -> Result<Value> {
    let (reply, rx) = oneshot::channel();
    handle
        .tx
        .try_send(Envelope {
            generation,
            action,
            reply,
        })
        .map_err(|_| "进程已退出或控制队列繁忙，请刷新状态")?;
    timeout(Duration::from_secs(10), rx)
        .await
        .map_err(|_| "控制请求等待超时，请先刷新状态，不要自动重试")?
        .map_err(|_| "进程已退出，请刷新状态".to_owned())?
}
async fn write_message(stdin: &mut ChildStdin, value: Value) -> Result<()> {
    let mut line = value.to_string();
    line.push('\n');
    timeout(Duration::from_secs(3), stdin.write_all(line.as_bytes()))
        .await
        .map_err(|_| "Claude Code 输入管道超时")?
        .map_err(|e| e.to_string())
}

async fn run_agent(
    mut child: Child,
    record: Arc<Mutex<Record>>,
    mut commands: mpsc::Receiver<Envelope>,
) {
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("piped stdin");
    let (output_tx, mut output_rx) = mpsc::channel(64);
    let stdout_task = tokio::spawn(read_output(
        child.stdout.take().expect("stdout"),
        "stdout",
        output_tx.clone(),
    ));
    let stderr_task = tokio::spawn(read_output(
        child.stderr.take().expect("stderr"),
        "stderr",
        output_tx,
    ));
    let init_id = "pool-initialize";
    let mut failure = write_message(&mut stdin, json!({"type":"control_request","request_id":init_id,"request":{"subtype":"initialize","hooks":null}})).await.err();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut ready = false;
    let mut stopped = false;
    let mut stop_reply = None;
    while failure.is_none() {
        tokio::select! {
            status = child.wait() => {
                failure = Some(format!("Claude Code 已退出：{status:?}"));
                break;
            }
            _ = tokio::time::sleep_until(deadline), if !ready => { failure = Some("Claude Code 控制协议握手超时，请检查 CLI 版本与登录状态".into()); }
            command = commands.recv() => {
                let Some(command) = command else { stopped = true; break; };
                let mut r = record.lock().await;
                if command.generation != r.info.generation {
                    let _ = command.reply.send(Err("进程已更换，请刷新后操作".into()));
                    continue;
                }
                if matches!(command.action, Action::Stop) { stopped = true; stop_reply = Some(command.reply); break; }
                let result = apply_action(&mut r, &mut stdin, command.action).await;
                // A failed pipe write is terminal; validation errors are not.
                if let Err(error) = &result && error.starts_with("写入失败") { failure = Some(error.clone()); }
                let _ = command.reply.send(result.map(|()| r.snapshot()));
            }
            output = output_rx.recv() => {
                match output {
                    Some(Ok((source, line))) => {
                        let mut r = record.lock().await;
                        if source == "stderr" { r.event("stderr", json!({"text":line})); continue; }
                        match serde_json::from_str::<Value>(&line) {
                            Ok(message) => {
                                if message["type"] == "control_response" && message["response"]["request_id"] == init_id {
                                    if message["response"]["subtype"] == "success" {
                                        ready = true; r.info.state = AgentState::Idle;
                                        r.event("lifecycle", json!({"message":"会话已就绪，可发送提示词"}));
                                    } else { failure = Some(format!("Claude Code 初始化失败：{}", message["response"]["error"])); }
                                } else if let Err(e) = handle_output(&mut r, &mut stdin, message).await { failure = Some(e); }
                            }
                            Err(e) => { failure = Some(format!("Claude Code 返回了非 stream-json 输出：{e}")); }
                        }
                    }
                    Some(Err(e)) => { failure = Some(e); }
                    None => { failure = Some("Claude Code 输出已关闭".into()); }
                }
            }
        }
    }
    drop(stdin);
    // Only signal the process group we created; never accept a PID from HTTP.
    terminate(&mut child, pid).await;
    stdout_task.abort();
    stderr_task.abort();
    let mut r = record.lock().await;
    r.info.pending_permissions.clear();
    r.info.state = if stopped {
        AgentState::Stopped
    } else {
        AgentState::Failed
    };
    r.info.last_error = failure.clone();
    r.event("lifecycle", json!({"message":if stopped { "进程已停止，工作目录和会话文件保留".to_owned() } else { failure.unwrap_or_default() }}));
    if let Some(reply) = stop_reply {
        let _ = reply.send(Ok(r.snapshot()));
    }
}

async fn apply_action(r: &mut Record, stdin: &mut ChildStdin, action: Action) -> Result<()> {
    let message = match action {
        Action::Send(prompt) => {
            if r.info.state != AgentState::Idle {
                return Err("Agent 尚未空闲；等待本轮结束或先中断".into());
            }
            r.event("user", json!({"text":prompt}));
            r.info.state = AgentState::Busy;
            json!({"type":"user","session_id":r.info.session_id.as_deref().unwrap_or(""),"parent_tool_use_id":null,"message":{"role":"user","content":prompt}})
        }
        Action::Interrupt => {
            if !matches!(
                r.info.state,
                AgentState::Busy | AgentState::AwaitingPermission
            ) {
                return Err("当前没有可中断的任务".into());
            }
            r.info.state = AgentState::Interrupting;
            r.info.pending_permissions.clear();
            json!({"type":"control_request","request_id":format!("interrupt-{}",r.sequence),"request":{"subtype":"interrupt"}})
        }
        Action::Permission { request_id, allow } => {
            let request = r
                .info
                .pending_permissions
                .remove(&request_id)
                .ok_or("权限请求已失效，请刷新")?;
            let answer = if allow {
                json!({"behavior":"allow","updatedInput":request["input"]})
            } else {
                json!({"behavior":"deny","message":"用户在进程池 Web 调试页拒绝了此工具调用"})
            };
            r.event(
                "permission",
                json!({"request_id":request_id,"allow":allow,"tool_name":request["tool_name"]}),
            );
            if r.info.pending_permissions.is_empty() {
                r.info.state = AgentState::Busy;
            }
            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":answer}})
        }
        Action::Stop => unreachable!(),
    };
    write_message(stdin, message)
        .await
        .map_err(|e| format!("写入失败：{e}"))
}

async fn handle_output(r: &mut Record, stdin: &mut ChildStdin, message: Value) -> Result<()> {
    if let Some(session) = message["session_id"].as_str() {
        r.info.session_id = Some(session.to_owned());
    }
    match message["type"].as_str().unwrap_or("") {
        "control_request" => {
            let id = message["request_id"]
                .as_str()
                .ok_or("权限请求缺少 request_id")?;
            if message["request"]["subtype"] == "can_use_tool"
                && matches!(
                    r.info.state,
                    AgentState::Busy | AgentState::AwaitingPermission
                )
                && r.info.pending_permissions.len() < 16
                && message.to_string().len() <= 65536
            {
                r.info
                    .pending_permissions
                    .insert(id.to_owned(), message["request"].clone());
                r.info.state = AgentState::AwaitingPermission;
            } else {
                write_message(stdin, json!({"type":"control_response","response":{"subtype":"error","request_id":id,"error":"Unsupported or inactive permission request"}})).await?;
            }
        }
        "control_cancel_request" => {
            if let Some(id) = message["request_id"].as_str() {
                r.info.pending_permissions.remove(id);
            }
            if r.info.state == AgentState::AwaitingPermission
                && r.info.pending_permissions.is_empty()
            {
                r.info.state = AgentState::Busy;
            }
        }
        "result" => {
            if message["is_error"] == true || message["subtype"] != "success" {
                r.info.failed_turns += 1;
                r.info.last_error = Some(clip(&message.to_string(), DISPLAY_LIMIT));
            } else {
                r.info.completed_turns += 1;
                r.info.last_error = None;
            }
            r.info.pending_permissions.clear();
            r.info.state = AgentState::Idle;
        }
        _ => {}
    }
    r.event("claude", message);
    Ok(())
}

async fn read_output(
    reader: impl AsyncRead + Unpin,
    source: &'static str,
    tx: mpsc::Sender<Result<(&'static str, String)>>,
) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        let buffer = match reader.fill_buf().await {
            Ok(buffer) => buffer,
            Err(e) => {
                let _ = tx.send(Err(e.to_string())).await;
                return;
            }
        };
        if buffer.is_empty() {
            if !line.is_empty() {
                let _ = tx
                    .send(Ok((source, String::from_utf8_lossy(&line).into_owned())))
                    .await;
            }
            return;
        }
        let length = buffer
            .iter()
            .position(|b| *b == b'\n')
            .map(|p| p + 1)
            .unwrap_or(buffer.len());
        if line.len() + length > LINE_LIMIT {
            let _ = tx
                .send(Err(format!("{source} 单行超过 1 MiB，已停止进程")))
                .await;
            return;
        }
        let complete = buffer[length - 1] == b'\n';
        line.extend_from_slice(&buffer[..length]);
        reader.consume(length);
        if complete {
            if tx
                .send(Ok((
                    source,
                    String::from_utf8_lossy(&line).trim_end().to_owned(),
                )))
                .await
                .is_err()
            {
                return;
            }
            line.clear();
        }
    }
}

async fn terminate(child: &mut Child, pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    let _ = child.start_kill();
    if timeout(Duration::from_secs(3), child.wait()).await.is_err() {
        let _ = child.start_kill();
    }
    #[cfg(unix)]
    if let Some(pid) = pid {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    let _ = child.wait().await;
}
async fn git(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = timeout(
        Duration::from_secs(120),
        Command::new("git")
            .args(["-c", "core.hooksPath=/dev/null"])
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "Git 操作超时，已有文件保留；请检查本地目录后重试")?
    .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Git 操作失败：{}",
            clip(&String::from_utf8_lossy(&output.stderr), 2048)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn clip(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
