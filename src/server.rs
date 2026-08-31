use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    PoolConfig, PoolError, ProcessFactoryConfig, ProcessPool, RejectedExecutionHandler, TimeUnit,
    WorkQueueConfig, agents::AgentManager,
};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pool: Arc<Mutex<Option<ProcessPool>>>,
    factories: Arc<BTreeMap<String, ProcessFactoryConfig>>,
    default_timeout: Duration,
    agents: Option<AgentManager>,
}

/// Seven caller-supplied parameters. The factory is a registered name, never a command.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializeParams {
    core_pool_size: usize,
    maximum_pool_size: usize,
    keep_alive_time: u64,
    time_unit: TimeUnit,
    work_queue: WorkQueueConfig,
    process_factory: String,
    rejected_execution_handler: RejectedExecutionHandler,
}

impl AppState {
    pub fn new(
        factories: BTreeMap<String, ProcessFactoryConfig>,
        default_timeout: Duration,
    ) -> Self {
        Self {
            pool: Arc::new(Mutex::new(None)),
            factories: Arc::new(factories),
            default_timeout,
            agents: None,
        }
    }

    /// Enable only when the actual HTTP listener is loopback-bound.
    pub fn with_agents(mut self, agents: AgentManager) -> Self {
        self.agents = Some(agents);
        self
    }

    /// Trusted local initialization (e.g. the optional CLI configuration file).
    pub async fn initialize_local(&self, config: PoolConfig) -> Result<Value, PoolError> {
        // Check-and-install is atomic even for concurrent RPC initializers.
        let mut slot = self.pool.lock().await;
        if slot.is_some() {
            return Err(PoolError::AlreadyInitialized);
        }
        let pool = ProcessPool::new(config).await?;
        let stats = pool.stats().await?;
        *slot = Some(pool);
        Ok(initialized_snapshot(stats))
    }

    async fn initialize(&self, params: InitializeParams) -> Result<Value, PoolError> {
        let factory = self
            .factories
            .get(&params.process_factory)
            .cloned()
            .ok_or_else(|| {
                PoolError::InvalidConfig(format!(
                    "unknown registered process_factory: {}",
                    params.process_factory
                ))
            })?;
        let config = PoolConfig::new(
            params.core_pool_size,
            params.maximum_pool_size,
            params.keep_alive_time,
            params.time_unit,
            params.work_queue,
            factory,
            params.rejected_execution_handler,
        )?;
        self.initialize_local(config).await
    }

    async fn get_pool(&self) -> Result<ProcessPool, PoolError> {
        self.pool
            .lock()
            .await
            .clone()
            .ok_or(PoolError::NotInitialized)
    }

    async fn snapshot(&self) -> Result<Value, PoolError> {
        let pool = self.pool.lock().await.clone();
        match pool {
            Some(pool) => Ok(initialized_snapshot(pool.stats().await?)),
            None => Ok(json!({ "initialized": false })),
        }
    }

    pub async fn shutdown(&self) {
        if let Some(agents) = &self.agents {
            agents.shutdown().await;
        }
        if let Some(pool) = self.pool.lock().await.take() {
            let _ = pool.shutdown().await;
        }
    }
}

fn initialized_snapshot(stats: crate::PoolStats) -> Value {
    let mut value = serde_json::to_value(stats).expect("PoolStats is serializable");
    value["initialized"] = json!(true);
    value
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteParams {
    payload: Value,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/assets/dashboard.css", get(dashboard_css))
        .route("/assets/dashboard.js", get(dashboard_js))
        .route("/assets/rpc-client.js", get(rpc_client_js))
        .route("/assets/debugger.js", get(debugger_js))
        .route("/assets/agents.js", get(agents_js))
        .route("/api/stats", get(stats_api))
        .route("/api/factories", get(factories_api))
        .route("/rpc", post(rpc))
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .with_state(state)
}

async fn dashboard() -> impl IntoResponse {
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
            ),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        Html(include_str!("../web/index.html")),
    )
}

async fn dashboard_css() -> impl IntoResponse {
    static_asset(
        "text/css; charset=utf-8",
        include_str!("../web/dashboard.css"),
    )
}

async fn dashboard_js() -> impl IntoResponse {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/dashboard.js"),
    )
}

async fn rpc_client_js() -> impl IntoResponse {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/rpc-client.js"),
    )
}

async fn debugger_js() -> impl IntoResponse {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/debugger.js"),
    )
}

async fn agents_js() -> impl IntoResponse {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/agents.js"),
    )
}

fn static_asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
}

async fn stats_api(State(state): State<AppState>) -> Response {
    match state.snapshot().await {
        Ok(stats) => ([(header::CACHE_CONTROL, "no-store")], Json(stats)).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn rpc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RpcRequest>,
) -> Json<RpcResponse> {
    let id = request.id;
    if request.jsonrpc != "2.0" {
        return Json(RpcResponse::error(
            id,
            -32600,
            "jsonrpc must be \"2.0\"",
            None,
        ));
    }

    if request.method.starts_with("cc.") {
        if !local_same_origin(&headers) {
            return Json(RpcResponse::error(
                id,
                -32101,
                "CC 管理仅允许本机同源访问",
                None,
            ));
        }
        let Some(agents) = &state.agents else {
            return Json(if request.method == "cc.status" {
                RpcResponse::success(
                    id,
                    json!({"enabled":false,"agents":[],"reason":"CC 管理仅在 loopback 监听地址启用"}),
                )
            } else {
                RpcResponse::error(id, -32101, "CC 管理未启用，请使用 127.0.0.1 监听地址", None)
            });
        };
        return Json(
            match agents
                .rpc(&request.method, request.params.unwrap_or(json!({})))
                .await
            {
                Ok(value) => RpcResponse::success(id, value),
                Err(error) => RpcResponse::error(id, -32100, error, None),
            },
        );
    }

    let response = match request.method.as_str() {
        "pool.initialize" => {
            match serde_json::from_value::<InitializeParams>(request.params.unwrap_or(Value::Null))
            {
                Ok(params) => match state.initialize(params).await {
                    Ok(snapshot) => RpcResponse::success(id, snapshot),
                    Err(error) => RpcResponse::pool_error(id, error),
                },
                Err(error) => RpcResponse::error(
                    id,
                    -32602,
                    format!("invalid initialization params: {error}"),
                    None,
                ),
            }
        }
        "pool.prestart" => {
            if !empty_params(&request.params) {
                RpcResponse::error(id, -32602, "pool.prestart takes no parameters", None)
            } else {
                match state.get_pool().await {
                    Ok(pool) => match pool.prestart_core_workers().await {
                        Ok(count) => {
                            RpcResponse::success(id, json!({ "started_worker_count": count }))
                        }
                        Err(error) => RpcResponse::pool_error(id, error),
                    },
                    Err(error) => RpcResponse::pool_error(id, error),
                }
            }
        }
        "pool.execute" => match parse_execute_params(request.params) {
            Ok(params) => {
                let timeout = Duration::from_millis(
                    params
                        .timeout_ms
                        .unwrap_or(state.default_timeout.as_millis() as u64),
                );
                match state.get_pool().await {
                    Ok(pool) => match pool.execute(params.payload, timeout).await {
                        Ok(result) => RpcResponse::success(id, result),
                        Err(error) => RpcResponse::pool_error(id, error),
                    },
                    Err(error) => RpcResponse::pool_error(id, error),
                }
            }
            Err(error) => RpcResponse::error(id, -32602, error, None),
        },
        "pool.stats" => match state.snapshot().await {
            Ok(stats) => RpcResponse::success(id, stats),
            Err(error) => RpcResponse::pool_error(id, error),
        },
        _ => RpcResponse::error(id, -32601, "method not found", None),
    };
    Json(response)
}

fn local_same_origin(headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Ok(authority) = host.parse::<axum::http::uri::Authority>() else {
        return false;
    };
    if !matches!(authority.host(), "localhost" | "127.0.0.1" | "[::1]") {
        return false;
    }
    if let Some(site) = headers.get("sec-fetch-site")
        && site != "same-origin"
        && site != "none"
    {
        return false;
    }
    match headers.get(header::ORIGIN) {
        Some(origin) => origin.to_str().is_ok_and(|o| o == format!("http://{host}")),
        None => true, // local CLI callers do not send Origin
    }
}

fn parse_execute_params(params: Option<Value>) -> Result<ExecuteParams, String> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| format!("invalid pool.execute params: {error}"))
}

fn empty_params(params: &Option<Value>) -> bool {
    match params {
        None | Some(Value::Null) => true,
        Some(Value::Object(map)) => map.is_empty(),
        _ => false,
    }
}

async fn factories_api(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "factories": state.factories.keys().collect::<Vec<_>>() })),
    )
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readiness(State(state): State<AppState>) -> Response {
    match state.snapshot().await {
        Ok(snapshot) if snapshot["initialized"] == true => (
            StatusCode::OK,
            Json(json!({ "status": "ready", "workers": snapshot["worker_count"] })),
        )
            .into_response(),
        Ok(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "reason": "pool_not_initialized" })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "error": error.to_string() })),
        )
            .into_response(),
    }
}

impl RpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i32, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data,
            }),
        }
    }

    fn pool_error(id: Value, error: PoolError) -> Self {
        let code = match error {
            PoolError::NotInitialized => -32006,
            PoolError::AlreadyInitialized => -32007,
            PoolError::InvalidConfig(_) => -32602,
            PoolError::Rejected => -32001,
            PoolError::Discarded => -32002,
            PoolError::TaskTimeout { .. } => -32003,
            PoolError::WorkerReturned { .. } => -32004,
            PoolError::Closed => -32005,
            PoolError::SpawnFailed(_)
            | PoolError::WorkerIo(_)
            | PoolError::Protocol(_)
            | PoolError::WorkerExited => -32000,
        };
        Self::error(
            id,
            code,
            error.to_string(),
            Some(json!({ "kind": format!("{error:?}") })),
        )
    }
}

#[cfg(test)]
mod cc_access_tests {
    use super::*;

    #[test]
    fn only_local_same_origin_authorities_are_accepted() {
        let mut headers = HeaderMap::new();
        assert!(!local_same_origin(&headers));
        for host in ["localhost:7788", "127.0.0.1:7788", "[::1]:7788"] {
            headers.insert(header::HOST, host.parse().unwrap());
            assert!(local_same_origin(&headers), "{host}");
            headers.insert(header::ORIGIN, format!("http://{host}").parse().unwrap());
            assert!(local_same_origin(&headers));
            headers.insert(header::ORIGIN, "http://localhost:1".parse().unwrap());
            assert!(!local_same_origin(&headers));
            headers.remove(header::ORIGIN);
        }
        for host in [
            "attacker.invalid",
            "127.0.0.1.attacker.invalid",
            "localhost.attacker.invalid",
            "192.168.1.1:7788",
        ] {
            headers.insert(header::HOST, host.parse().unwrap());
            assert!(!local_same_origin(&headers), "{host}");
        }
    }
}
