mod config;

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use worker_cp::{ControlPlaneSession, WorkerRuntimeDispatcher};
use worker_flight::{FlightServerBuilder, FlightServerConfig};
use worker_storage::engine::duckdb::storage_engine_impl::MemoryDuckDBEngine;
use worker_storage::engine::duckdb::DuckDBQueryEngine;
use worker_storage::loader::DataLoader;
use worker_storage::sql::SqlTransformer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = config::Cli::parse();
    let config::Command::Run(args) = cli.command;
    let cfg = config::LeibrixWorkerConfig::from_run_args(args).context("invalid configuration")?;

    info!(
        worker_id = %cfg.worker_id,
        tenant_id = %cfg.tenant_id,
        master_endpoint = %cfg.master_endpoint,
        "liebrix-worker starting"
    );

    let engine = Arc::new(
        MemoryDuckDBEngine::new_with_fresh_db(cfg.duckdb.clone())
            .context("failed to initialise DuckDB storage engine")?,
    );
    info!("DuckDB storage engine ready");

    let shared_db = engine.shared_database();
    let query_engine = Arc::new(DuckDBQueryEngine::new(Arc::clone(&shared_db), 32));
    let sql_transformer = Arc::new(RwLock::new(SqlTransformer::new()));

    let data_loader = Arc::new(DataLoader::new(Arc::clone(&engine)));
    // The outgoing sender is injected after the session starts (step 7).
    let dispatcher = Arc::new(WorkerRuntimeDispatcher::new(
        data_loader,
        Arc::clone(&engine),
        cfg.cp_max_concurrent_loads,
    ));

    let cp_cfg = cfg
        .cp_config()
        .context("failed to build control-plane configuration")?;
    let session = ControlPlaneSession::start(cp_cfg, Arc::clone(&dispatcher))
        .await
        .context("failed to start control plane session")?;
    info!("control plane session active");
    dispatcher.init_sender(session.outgoing_sender());

    // Start the Arrow Flight server if a listen address was configured.
    let mut flight_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>> = None;
    let flight_task = if let Some(addr) = cfg.query_listen_addr {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let flight_config = FlightServerConfig {
            bind_addr: addr,
            tls_config: None, // TLS wired via a follow-up task
            max_message_size: 16 * 1024 * 1024,
            concurrency_limit: 1000,
        };
        let builder = FlightServerBuilder::new(
            flight_config,
            Arc::clone(&query_engine),
            Arc::clone(&sql_transformer),
            cfg.tenant_id.clone(),
            Arc::clone(&shared_db),
        );
        info!(%addr, "Arrow Flight server starting");
        let handle = tokio::spawn(async move {
            builder
                .run_with_shutdown(async move {
                    let _ = rx.await;
                })
                .await
        });
        flight_shutdown_tx = Some(tx);
        Some(handle)
    } else {
        info!("Arrow Flight server disabled (no query_listen_addr configured)");
        None
    };

    let mut status_rx = session.status();
    loop {
        tokio::select! {
            biased;

            // Graceful shutdown on Ctrl+C / SIGTERM.
            _ = signal::ctrl_c() => {
                info!("received shutdown signal; exiting");
                break;
            }

            // React to session lifecycle changes.
            result = status_rx.changed() => {
                match result {
                    Err(_) => {
                        // Sender dropped — session tasks have exited.
                        warn!("session status channel closed; exiting");
                        break;
                    }
                    Ok(()) => {
                        use worker_cp::SessionStatus;
                        let status = status_rx.borrow_and_update().clone();
                        match status {
                            SessionStatus::Disconnected(err) => {
                                error!(error = %err, "control plane session disconnected; exiting");
                                // Signal the Flight server to stop before returning.
                                if let Some(tx) = flight_shutdown_tx.take() {
                                    let _ = tx.send(());
                                }
                                // Return an error so the process exits with a non-zero code,
                                // signalling to a supervisor (e.g., Kubernetes) that a restart
                                // is needed.
                                return Err(anyhow::anyhow!("session disconnected: {err}"));
                            }
                            other => {
                                info!(status = ?other, "session status changed");
                            }
                        }
                    }
                }
            }
        }
    }

    // Gracefully stop the Flight server and wait for it to finish.
    if let Some(tx) = flight_shutdown_tx.take() {
        let _ = tx.send(());
    }
    if let Some(handle) = flight_task {
        if let Err(e) = handle.await {
            warn!(error = ?e, "Arrow Flight server task panicked during shutdown");
        }
    }

    info!("liebrix-worker shutdown complete");
    Ok(())
}
