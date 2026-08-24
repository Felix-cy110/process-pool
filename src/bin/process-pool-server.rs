use std::{net::SocketAddr, path::PathBuf, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use clap::Parser;
use process_pool::{PoolConfig, PoolError, ProcessPool};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Reusable process pool exposed through HTTP JSON-RPC 2.0")]
struct Args {
    /// JSON file containing the seven process-pool parameters.
    #[arg(long, default_value = "examples/pool-config.json")]
    config: PathBuf,

    #[arg(long, default_value = "127.0.0.1:3000")]
    listen: SocketAddr,

    /// Per-call timeout used when `timeout_ms` is absent from RPC params.
    #[arg(long, default_value_t = 30_000)]
    default_timeout_ms: u64,
}

#[derive(Clone)]
struct AppState {
    pool: ProcessPool,
    default_timeout: Duration,
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if let Err(error) = run(Args::parse()).await {
        error!(%error, "server stopped with an error");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config_text = tokio::fs::read_to_string(&args.config).await?;
    let config: PoolConfig = serde_json::from_str(&config_text)?;
    let pool = ProcessPool::new(config).await?;
    let state = AppState {
        pool: pool.clone(),
        default_timeout: Duration::from_millis(args.default_timeout_ms),
    };
    let app = Router::new()
        .route("/rpc", post(rpc))
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .with_state(state);
    let listener = TcpListener::bind(args.listen).await?;
    info!(listen = %args.listen, "process-pool server is ready");

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    let _ = pool.shutdown().await;
    serve_result?;
    Ok(())
}

async fn rpc(State(state): State<AppState>, Json(request): Json<RpcRequest>) -> Json<RpcResponse> {
    let id = request.id;
    if request.jsonrpc != "2.0" {
        return Json(RpcResponse::error(
            id,
            -32600,
            "jsonrpc must be \"2.0\"",
            None,
        ));
    }

    let response = match request.method.as_str() {
        "pool.execute" => match parse_execute_params(request.params) {
            Ok(params) => {
                let timeout = Duration::from_millis(
                    params
                        .timeout_ms
                        .unwrap_or(state.default_timeout.as_millis() as u64),
                );
                match state.pool.execute(params.payload, timeout).await {
                    Ok(result) => RpcResponse::success(id, result),
                    Err(error) => RpcResponse::pool_error(id, error),
                }
            }
            Err(error) => RpcResponse::error(id, -32602, error, None),
        },
        "pool.stats" => match state.pool.stats().await {
            Ok(stats) => RpcResponse::success(
                id,
                serde_json::to_value(stats).expect("PoolStats is serializable"),
            ),
            Err(error) => RpcResponse::pool_error(id, error),
        },
        _ => RpcResponse::error(id, -32601, "method not found", None),
    };
    Json(response)
}

fn parse_execute_params(params: Option<Value>) -> Result<ExecuteParams, String> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|error| format!("invalid pool.execute params: {error}"))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readiness(State(state): State<AppState>) -> Json<Value> {
    match state.pool.stats().await {
        Ok(stats) => Json(json!({
            "status": "ready",
            "workers": stats.worker_count,
            "core_workers": stats.core_pool_size
        })),
        Err(error) => Json(json!({ "status": "not_ready", "error": error.to_string() })),
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
            PoolError::Rejected => -32001,
            PoolError::Discarded => -32002,
            PoolError::TaskTimeout { .. } => -32003,
            PoolError::WorkerReturned { .. } => -32004,
            PoolError::Closed => -32005,
            PoolError::InvalidConfig(_)
            | PoolError::SpawnFailed(_)
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
