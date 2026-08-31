use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use process_pool::{
    PoolConfig, ProcessFactoryConfig,
    server::{AppState, router},
};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    about = "Reusable process pool with caller-supplied initialization and optional prewarming"
)]
struct Args {
    /// Optional trusted local seven-parameter config. Omit to initialize via RPC/Web.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Trusted worker registry. Defaults to examples/worker-factories.json in RPC mode.
    #[arg(long)]
    factories: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1:7788")]
    listen: SocketAddr,

    #[arg(long, default_value_t = 30_000)]
    default_timeout_ms: u64,
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
    // Local configuration already contains its factory and needs no registry.
    let registry_path = args.factories.or_else(|| {
        args.config
            .is_none()
            .then(|| PathBuf::from("examples/worker-factories.json"))
    });
    let factories: BTreeMap<String, ProcessFactoryConfig> = match registry_path {
        Some(path) => serde_json::from_str(&tokio::fs::read_to_string(path).await?)?,
        None => BTreeMap::new(),
    };
    let state = AppState::new(factories, Duration::from_millis(args.default_timeout_ms));
    if let Some(path) = args.config {
        let config: PoolConfig = serde_json::from_str(&tokio::fs::read_to_string(path).await?)?;
        state.initialize_local(config).await?;
    }
    let listener = TcpListener::bind(args.listen).await?;
    info!(listen = %listener.local_addr()?, "process-pool HTTP service ready; workers start only on tasks or explicit prewarm");
    let result = axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    state.shutdown().await;
    result?;
    Ok(())
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
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_does_not_implicitly_load_pool_configuration() {
        let args = Args::try_parse_from(["process-pool-server"]).unwrap();
        assert!(args.config.is_none());
        assert_eq!(args.listen.port(), 7788);
    }
}
