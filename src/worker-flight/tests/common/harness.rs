#![allow(dead_code)]

//! `FlightTestHarness` — server lifecycle for e2e tests.
//!
//! Starts a real `WorkerFlightService` on an ephemeral `127.0.0.1:0` port,
//! connects an Arrow Flight client, and exposes the shared `DuckDBQueryEngine`
//! and `SharedDatabase` for data seeding and reference comparisons.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use bytes::Bytes;
use tokio::sync::{oneshot, RwLock};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use worker_flight::{FlightServerBuilder, FlightServerConfig, FlightServerError};
use worker_storage::engine::duckdb::{
    storage_engine_impl::MemoryDuckDBEngine, DuckDBConfig, DuckDBQueryEngine, SharedDatabase,
};
use worker_storage::sql::SqlTransformer;

/// End-to-end test harness for `WorkerFlightService`.
///
/// Both the server and `query_engine` share the same `Arc<SharedDatabase>`, so
/// data seeded before the server starts is immediately visible through it.
pub struct FlightTestHarness {
    /// Arrow Flight gRPC client connected to the test server.
    pub client: FlightServiceClient<Channel>,
    /// Shared DuckDB instance — used by the server AND for direct seeding.
    pub shared_db: Arc<SharedDatabase>,
    /// Reference query engine sharing the same DB as the server.
    pub query_engine: Arc<DuckDBQueryEngine>,
    /// SQL transformer — register datasets here before running queries.
    pub sql_transformer: Arc<RwLock<SqlTransformer>>,
    /// Tenant ID the server enforces on every request.
    pub tenant_id: String,
    /// Bound address of the running server.
    pub addr: SocketAddr,

    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: JoinHandle<Result<(), FlightServerError>>,
}

impl FlightTestHarness {
    /// Start a Flight server on `127.0.0.1:0` for `tenant_id`.
    ///
    /// Steps:
    /// 1. Creates a fresh in-memory `SharedDatabase` with default config.
    /// 2. Builds `DuckDBQueryEngine` sharing that database.
    /// 3. Starts `WorkerFlightService` on an OS-assigned ephemeral port.
    /// 4. Connects a Flight client and waits until `health_check` responds.
    pub async fn start(tenant_id: &str) -> Self {
        super::init_tracing();

        // --- storage setup -----------------------------------------------
        let storage_engine = MemoryDuckDBEngine::new_with_fresh_db(DuckDBConfig::default())
            .expect("create MemoryDuckDBEngine");
        let shared_db = storage_engine.shared_database();
        let query_engine = Arc::new(DuckDBQueryEngine::new(Arc::clone(&shared_db), 32));
        let sql_transformer = Arc::new(RwLock::new(SqlTransformer::new()));

        // --- server startup -----------------------------------------------
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let cfg = FlightServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            tls_config: None,
            max_message_size: 16 * 1024 * 1024,
            concurrency_limit: 100,
        };

        let builder = FlightServerBuilder::new(
            cfg,
            Arc::clone(&query_engine),
            Arc::clone(&sql_transformer),
            tenant_id.to_string(),
            Arc::clone(&shared_db),
        );

        let (server_future, addr) = builder
            .run_with_shutdown_and_addr(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("bind ephemeral Flight server");

        let server_handle = tokio::spawn(server_future);

        // --- client connection -------------------------------------------
        let mut client = FlightServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("connect Flight client");

        Self::wait_for_ready(&mut client).await;

        Self {
            client,
            shared_db,
            query_engine,
            sql_transformer,
            tenant_id: tenant_id.to_string(),
            addr,
            shutdown_tx: Some(shutdown_tx),
            server_handle,
        }
    }

    /// Gracefully stop the server and await its task.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.server_handle.await;
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Poll `health_check` until the server responds, backing off up to ~1 s.
    async fn wait_for_ready(client: &mut FlightServiceClient<Channel>) {
        let action = Action {
            r#type: "health_check".to_string(),
            body: Bytes::new(),
        };
        for attempt in 0..50u64 {
            if client
                .do_action(tonic::Request::new(action.clone()))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20 * (attempt + 1))).await;
        }
        panic!("FlightTestHarness: server did not become ready within ~1 second");
    }
}
