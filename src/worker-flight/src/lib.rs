//! Arrow Flight Query Service for Leibrix-Worker.
//!
//! This crate provides a high-performance query interface using the Arrow Flight
//! protocol for transferring large datasets using the Arrow columnar memory format.
//!
//! # Architecture
//!
//! The Arrow Flight service enables zero-copy transfer of query results by:
//! - Streaming Arrow RecordBatches directly from DuckDB query execution
//! - Transforming logical table queries to physical macro calls
//! - Validating tenant access at the protocol level
//!
//! # Key Components
//!
//! - [`WorkerFlightService`]: The main service implementing `FlightService` trait
//! - [`FlightServerConfig`]: Configuration for the Arrow Flight server
//! - [`FlightServerBuilder`]: Builder for creating and running the server
//! - [`QueryTicket`]: Serializable query parameters for DoGet requests
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use worker_flight::{FlightServerConfig, FlightServerBuilder, FlightQueryService};
//! use worker_storage::sql::SqlTransformer;
//!
//! // Create the query engine and transformer
//! let query_engine = Arc::new(DuckDBQueryEngine::new(...));
//! let sql_transformer = Arc::new(SqlTransformer::new());
//!
//! // Build and run the server
//! let config = FlightServerConfig::default();
//! let builder = FlightServerBuilder::new(
//!     config,
//!     query_engine,
//!     sql_transformer,
//!     "tenant-123".to_string(),
//! );
//!
//! builder.run().await?;
//! ```

pub mod config;
pub mod error;
pub mod server;
pub mod service;
pub mod ticket;

// Re-export main types for convenient access
pub use config::FlightServerConfig;
pub use error::{map_query_error_to_status, map_transform_error_to_status};
pub use server::{load_identity, FlightServerBuilder, FlightServerError};
pub use service::WorkerFlightService;
pub use ticket::{QueryTicket, TicketError};
