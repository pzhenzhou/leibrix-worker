mod config;

use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use tokio::signal;
use tracing::{error, info, warn};
use worker_cp::{ControlPlaneSession, WorkerRuntimeDispatcher};
use worker_storage::engine::duckdb::storage_engine_impl::MemoryDuckDBEngine;
use worker_storage::loader::DataLoader;

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

    let data_loader = Arc::new(DataLoader::new(Arc::clone(&engine)));
    // The outgoing sender is injected after the session starts (step 7).
    let dispatcher = Arc::new(WorkerRuntimeDispatcher::new(
        data_loader,
        Arc::clone(&engine),
        cfg.cp_max_concurrent_loads,
    ));

    let cp_cfg = cfg.cp_config();
    let session = ControlPlaneSession::start(cp_cfg, Arc::clone(&dispatcher))
        .await
        .context("failed to start control plane session")?;
    info!("control plane session active");
    dispatcher.init_sender(session.outgoing_sender());
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

    info!("liebrix-worker shutdown complete");
    Ok(())
}
