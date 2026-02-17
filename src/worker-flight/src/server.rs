//! Arrow Flight server startup and shutdown.
//!
//! This module provides the server builder and runner for the Arrow Flight
//! query service.

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::{Identity, Server};
use tracing::{info, warn};

use worker_storage::engine::query_engine::QueryEngine;
use worker_storage::sql::SqlTransformer;

use crate::config::FlightServerConfig;
use crate::service::WorkerFlightService;

/// Builder for creating and configuring the Arrow Flight server.
pub struct FlightServerBuilder<Q>
where
    Q: QueryEngine + 'static,
{
    config: FlightServerConfig,
    query_engine: Arc<Q>,
    sql_transformer: Arc<SqlTransformer>,
    tenant_id: String,
}

impl<Q> FlightServerBuilder<Q>
where
    Q: QueryEngine + 'static,
{
    /// Create a new FlightServerBuilder.
    pub fn new(
        config: FlightServerConfig,
        query_engine: Arc<Q>,
        sql_transformer: Arc<SqlTransformer>,
        tenant_id: String,
    ) -> Self {
        Self {
            config,
            query_engine,
            sql_transformer,
            tenant_id,
        }
    }

    /// Build and run the Flight server.
    ///
    /// This method will block until the server is shut down.
    ///
    /// # Errors
    /// Returns an error if the server fails to start or encounters a runtime error.
    pub async fn run(self) -> Result<(), FlightServerError> {
        let flight_service = WorkerFlightService::new(
            self.query_engine,
            self.sql_transformer,
            self.tenant_id.clone(),
        );

        let flight_server = FlightServiceServer::new(flight_service);

        // Build the tonic server
        let mut server_builder = Server::builder();

        // Configure TLS if provided
        if let Some(tls_config) = self.config.tls_config {
            info!("TLS enabled for Flight server");
            server_builder = server_builder
                .tls_config(tls_config)
                .map_err(FlightServerError::TlsConfig)?;
        } else {
            warn!("TLS is disabled for Flight server - not recommended for production");
        }

        // Apply server configuration
        let router = server_builder
            .tcp_nodelay(true)
            .max_frame_size(Some(self.config.max_message_size as u32))
            .concurrency_limit_per_connection(self.config.concurrency_limit)
            .add_service(flight_server);

        info!(
            bind_addr = %self.config.bind_addr,
            tenant_id = %self.tenant_id,
            max_message_size = self.config.max_message_size,
            concurrency_limit = self.config.concurrency_limit,
            "Starting Arrow Flight server"
        );

        router
            .serve(self.config.bind_addr)
            .await
            .map_err(FlightServerError::ServerRun)?;

        info!("Arrow Flight server stopped");
        Ok(())
    }

    /// Build and run the Flight server with graceful shutdown.
    ///
    /// The server will shut down when the shutdown signal is received.
    ///
    /// # Arguments
    /// * `shutdown_signal` - A future that completes when shutdown is requested
    ///
    /// # Errors
    /// Returns an error if the server fails to start or encounters a runtime error.
    pub async fn run_with_shutdown<F>(self, shutdown_signal: F) -> Result<(), FlightServerError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let flight_service = WorkerFlightService::new(
            self.query_engine,
            self.sql_transformer,
            self.tenant_id.clone(),
        );

        let flight_server = FlightServiceServer::new(flight_service);

        // Build the tonic server
        let mut server_builder = Server::builder();

        // Configure TLS if provided
        if let Some(tls_config) = self.config.tls_config {
            info!("TLS enabled for Flight server");
            server_builder = server_builder
                .tls_config(tls_config)
                .map_err(FlightServerError::TlsConfig)?;
        } else {
            warn!("TLS is disabled for Flight server - not recommended for production");
        }

        // Apply server configuration
        let router = server_builder
            .tcp_nodelay(true)
            .max_frame_size(Some(self.config.max_message_size as u32))
            .concurrency_limit_per_connection(self.config.concurrency_limit)
            .add_service(flight_server);

        info!(
            bind_addr = %self.config.bind_addr,
            tenant_id = %self.tenant_id,
            max_message_size = self.config.max_message_size,
            concurrency_limit = self.config.concurrency_limit,
            "Starting Arrow Flight server with graceful shutdown"
        );

        router
            .serve_with_shutdown(self.config.bind_addr, shutdown_signal)
            .await
            .map_err(FlightServerError::ServerRun)?;

        info!("Arrow Flight server stopped gracefully");
        Ok(())
    }
}

/// Errors that can occur during Flight server operation.
#[derive(Debug, thiserror::Error)]
pub enum FlightServerError {
    #[error("TLS configuration error: {0}")]
    TlsConfig(#[source] tonic::transport::Error),

    #[error("Server runtime error: {0}")]
    ServerRun(#[source] tonic::transport::Error),

    #[error("Failed to read certificate file: {0}")]
    CertificateRead(#[source] std::io::Error),

    #[error("Failed to read key file: {0}")]
    KeyRead(#[source] std::io::Error),
}

/// Helper function to load TLS identity from certificate and key files.
///
/// # Arguments
/// * `cert_path` - Path to the PEM-encoded certificate file
/// * `key_path` - Path to the PEM-encoded private key file
///
/// # Returns
/// A tonic Identity for server TLS configuration.
pub async fn load_identity(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<Identity, FlightServerError> {
    let cert = tokio::fs::read(cert_path)
        .await
        .map_err(FlightServerError::CertificateRead)?;
    let key = tokio::fs::read(key_path)
        .await
        .map_err(FlightServerError::KeyRead)?;

    Ok(Identity::from_pem(cert, key))
}