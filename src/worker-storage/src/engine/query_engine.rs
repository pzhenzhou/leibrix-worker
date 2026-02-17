use crate::engine::storage_engine::TableMetadata;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use futures_core::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

pub type QueryResultStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, QueryError>> + Send>>;

#[derive(Debug, Error, Clone)]
pub enum QueryError {
    #[error("query timeout after {0}s")]
    Timeout(u64),

    #[error("DuckDB error: {0}")]
    DuckDB(String), // Catch-all for DuckDB errors including OOM

    #[error("SQL syntax error: {0}")]
    Sql(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("tenant validation failed: {0}")]
    TenantValidation(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// QueryEngine provides read-only access to the DuckDB database
/// for executing SQL queries via Arrow Flight SQL.
pub trait QueryEngine: Send + Sync {
    fn get_table_schema(
        &self,
        table_name: &str,
    ) -> impl Future<Output = Result<Arc<Schema>, QueryError>> + Send;

    /// Get full metadata including schema + statistics
    ///
    /// Use for:
    /// - Flight SQL GetFlightInfo RPC (needs total_records/total_bytes)
    /// - Cost-based query planning
    /// - Admin/monitoring dashboards
    ///
    /// WARNING: For tables with millions of rows, this requires a full
    /// table scan to compute row_count. Consider caching results.
    fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> impl Future<Output = Result<TableMetadata, QueryError>> + Send;

    /// List all tables for a dataset (for catalog queries).
    fn list_tables(
        &self,
        dataset_id: &str,
    ) -> impl Future<Output = Result<Vec<String>, QueryError>> + Send;

    /// Execute a SQL query and return a stream of Arrow RecordBatches.
    ///
    /// This method:
    /// 1. Opens a new connection to the shared database
    /// 2. Applies resource limits timeout
    /// 3. Executes the query
    /// 4. Streams results as Arrow RecordBatches
    fn execute_query(
        &self,
        sql: &str,
        memory_limit: Option<usize>,
        timeout_secs: Option<Duration>,
    ) -> impl Future<Output = Result<QueryResultStream, QueryError>> + Send;
}
