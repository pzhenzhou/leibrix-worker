use crate::engine::storage_engine::{EpochView, StorageError};
use arrow::datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Catalog {
    Iceberg {
        uri: String,
    },
    StarRocks {
        catalog: String,
        host: String,
        port: usize,
        database: String,
        user: String,
        password: String,
        max_concurrency: Option<usize>,
        pool_options: Option<ConnectionPoolOptions>,
    },
    Jdbc {
        uri: String,
        driver: String,
        pool_options: Option<ConnectionPoolOptions>,
    },
}

/// Reusable connection pool configuration for SQL-based catalogs
#[derive(Debug, Clone)]
pub struct ConnectionPoolOptions {
    pub max_connections: Option<u32>,
    pub min_connections: Option<u32>,
    pub acquire_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    pub idle_timeout_ms: Option<u64>,
    pub max_lifetime_ms: Option<u64>,
    pub batch_size: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct DataSource {
    pub catalog: Catalog,
    pub database: String,
    pub table: String,
    // Simplified partition info needed for data fetching
    pub filter: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LoadRequest {
    pub dataset_id: String,
    pub epoch_view: EpochView,
    pub source: DataSource,
    pub schema: Arc<Schema>,
}

/// Logical partitioning strategy for a source table.
///
///
/// NOTE:
/// - The current implementation does NOT use `partition` at all.
/// - All correctness and filtering constraints MUST be expressed via
///   `DataSource::filter`.
/// - `partition` is reserved for future DataLoader-controlled parallelism.
#[derive(Debug, Clone)]
pub enum PartitionSpec {
    /// Explicit SQL `WHERE` fragments for each shard/task.
    ///
    /// Semantics:
    /// - Each string is intended to be a valid SQL boolean expression that
    ///   can be combined with the base `filter`.
    /// - In the future, the DataLoader may:
    ///   - For each shard `s` in `shards`, create an internal task whose
    ///     effective predicate is something like:
    ///         `(<base_filter>) AND (<s>)`
    ///   - Run multiple shards in parallel, while treating them as one
    ///     logical epoch load at the API level.
    ///
    /// Example (conceptual):
    /// - Base `filter`:
    ///       "dt >= '2025-01-01' AND dt < '2025-02-01'"
    /// - `shards`:
    ///       [
    ///         "bucket_id BETWEEN 0 AND 127",
    ///         "bucket_id BETWEEN 128 AND 255",
    ///       ]
    /// - Future DataLoader behavior:
    ///       Task 1: WHERE <filter> AND bucket_id BETWEEN 0 AND 127
    ///       Task 2: WHERE <filter> AND bucket_id BETWEEN 128 AND 255
    Shards(Vec<String>),
}

use std::time::Duration;
use thiserror::Error;

/// Error type for all source adapters (StarRocks, Iceberg, JDBC, …).
///
/// `catalog` is just a human-readable identifier like `"starrocks"`,
/// `"iceberg"`, `"jdbc"`, etc. It’s only used for logging / debugging.
#[derive(Debug, Error)]
pub enum SourceError {
    /// The requested catalog type is not supported by this build or adapter.
    #[error("unsupported catalog `{catalog}`")]
    UnsupportedCatalog { catalog: String },

    /// The request is invalid for this source: bad filter, missing required
    /// fields, or unsupported options.
    #[error("invalid request for `{catalog}`: {reason}")]
    InvalidRequest { catalog: String, reason: String },

    /// Static configuration error: invalid DSN/URI, credentials, etc.
    #[error("configuration error for `{catalog}`: {reason}")]
    Config { catalog: String, reason: String },

    /// Network-level failure: connection refused, reset, DNS, TLS, etc.
    #[error("network error for `{catalog}`: {source}")]
    Network {
        catalog: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The source engine did not respond in time or explicitly timed out.
    #[error("timeout talking to `{catalog}` after {duration:?}")]
    Timeout { catalog: String, duration: Duration },

    /// Engine is up but cannot serve the request right now:
    /// overloaded, too many connections, temporarily unavailable, etc.
    #[error("engine `{catalog}` unavailable: {reason}")]
    EngineUnavailable { catalog: String, reason: String },

    /// Query-level error: SQL syntax error, table not found, permission denied, etc.
    #[error("query error on `{catalog}`: {message}")]
    Query {
        catalog: String,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Protocol-level error: invalid frames, unexpected response, etc.
    #[error("protocol error on `{catalog}`: {message}")]
    Protocol {
        catalog: String,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Catch-all for unexpected adapter-level bugs or panics wrapped as errors.
    #[error("internal adapter error on `{catalog}`: {source}")]
    Internal {
        catalog: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl SourceError {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            SourceError::Network { .. }
                | SourceError::Timeout { .. }
                | SourceError::EngineUnavailable { .. }
        )
    }
}

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("storage engine error: {0}")]
    Storage(#[from] StorageError),

    #[error("source adapter error: {0}")]
    Source(#[from] SourceError),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("load timeout after {0:?}")]
    Timeout(Duration),
}
