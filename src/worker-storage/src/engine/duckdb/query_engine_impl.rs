use crate::engine::query_engine::{QueryEngine, QueryError, QueryResultStream};
use crate::engine::storage_engine::TableMetadata;
use arrow_schema::Schema;
use duckdb::Connection;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

/// Default timeout for queries if not specified (5 minutes)
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 300;
/// Default memory limit per query if not specified (1024 MB)
const DEFAULT_QUERY_MEMORY_MB: usize = 1024;

pub struct DuckDBQueryEngine {
    /// Database path to connect to (e.g., ":memory:leibrix_db")
    database_path: String,
    /// Semaphore to limit concurrent queries
    query_limiter: Arc<Semaphore>,
    /// Maximum concurrent queries allowed
    max_concurrent_queries: usize,
}

impl DuckDBQueryEngine {
    pub fn new(database_path: String, max_concurrent_queries: usize) -> Self {
        let query_limiter = Arc::new(Semaphore::new(max_concurrent_queries));
        Self {
            database_path,
            query_limiter,
            max_concurrent_queries,
        }
    }

    fn classify_anyhow_error(err: anyhow::Error, table_name: &str) -> QueryError {
        let msg = err.to_string();
        if msg.contains("does not exist") || msg.contains("not found") {
            QueryError::TableNotFound(table_name.to_string())
        } else if msg.contains("Out of Memory") || msg.contains("failed to allocate") {
            QueryError::DuckDB(format!("Out of memory: {}", msg))
        } else {
            QueryError::DuckDB(msg)
        }
    }

    fn classify_duckdb_error(err: duckdb::Error) -> QueryError {
        let msg = err.to_string();
        if msg.contains("Out of Memory") || msg.contains("failed to allocate") {
            QueryError::DuckDB(format!("Out of memory: {}", msg))
        } else if msg.contains("syntax error")
            || msg.contains("Parser Error")
            || msg.contains("Syntax Error")
        {
            QueryError::Sql(msg)
        } else {
            QueryError::DuckDB(msg)
        }
    }
}

impl QueryEngine for DuckDBQueryEngine {
    fn get_table_schema(
        &self,
        table_name: &str,
    ) -> impl Future<Output = Result<Arc<Schema>, QueryError>> + Send {
        let database_path = self.database_path.clone();
        let table_name = table_name.to_string();
        let table_name_log = table_name.clone();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = Connection::open(&database_path).map_err(|e| {
                    QueryError::Internal(format!("failed to open connection: {}", e))
                })?;

                // Use DuckDB's DESCRIBE to get schema
                super::helper::query_table_schema(&conn, &table_name)
                    .map_err(|e| Self::classify_anyhow_error(e, &table_name))
            })
            .await
            .map_err(|e| QueryError::Internal(format!("task join error: {}", e)))?;

            if let Err(ref e) = result {
                error!(table_name = %table_name_log, error = %e, "get_table_schema failed");
            }
            result
        }
    }

    fn get_table_metadata(
        &self,
        table_name: &str,
    ) -> impl Future<Output = Result<TableMetadata, QueryError>> + Send {
        let database_path = self.database_path.clone();
        let table_name = table_name.to_string();
        let table_name_log = table_name.clone();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = Connection::open(&database_path).map_err(|e| {
                    QueryError::Internal(format!("failed to open connection: {}", e))
                })?;

                let schema = super::helper::query_table_schema(&conn, &table_name)
                    .map_err(|e| Self::classify_anyhow_error(e, &table_name))?;

                let total_rows = super::helper::query_table_row_count(&conn, &table_name)
                    .map_err(|e| Self::classify_anyhow_error(e, &table_name))?;

                // Get table size (fallback to estimate if storage_info fails)
                let total_bytes = super::helper::query_table_size(&conn, &table_name)
                    .unwrap_or_else(|_| super::helper::estimate_table_bytes(total_rows, &schema));

                Ok(TableMetadata {
                    table_name,
                    total_rows,
                    total_bytes,
                    create_at: super::helper::now_secs(),
                    schema,
                })
            })
            .await
            .map_err(|e| QueryError::Internal(format!("task join error: {}", e)))?;

            if let Err(ref e) = result {
                error!(table_name = %table_name_log, error = %e, "get_table_metadata failed");
            }
            result
        }
    }

    fn list_tables(
        &self,
        dataset_id: &str,
    ) -> impl Future<Output = Result<Vec<String>, QueryError>> + Send {
        let database_path = self.database_path.clone();
        let prefix = format!("{}__%", dataset_id);
        let dataset_id_log = dataset_id.to_string();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = Connection::open(&database_path).map_err(|e| {
                    QueryError::Internal(format!("failed to open connection: {}", e))
                })?;
                // dataset is logical , a high-level concept, is a grouping of data (e.g., a project, a user, a topic).
                // Tables are physical entities in the database.
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE ?",
                    )
                    .map_err(|e| QueryError::Internal(format!("failed to query tables: {}", e)))?;

                let tables = stmt
                    .query_map([prefix], |row| row.get(0))
                    .map_err(|e| {
                        QueryError::Internal(format!("failed to execute tables query: {}", e))
                    })?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(|e| {
                        QueryError::Internal(format!("failed to collect tables: {}", e))
                    })?;
                Ok(tables)
            })
            .await
            .map_err(|e| QueryError::Internal(format!("task join error: {}", e)))?;

            if let Err(ref e) = result {
                error!(dataset_id = %dataset_id_log, error = %e, "list_tables failed");
            }
            result
        }
    }

    fn execute_query(
        &self,
        sql: &str,
        memory_limit: Option<usize>,
        timeout_secs: Option<Duration>,
    ) -> impl Future<Output = Result<QueryResultStream, QueryError>> + Send {
        let query_limiter = self.query_limiter.clone();
        let database_path = self.database_path.clone();
        let sql = sql.to_string();
        let memory_mb = memory_limit.unwrap_or(DEFAULT_QUERY_MEMORY_MB);
        let timeout = timeout_secs.unwrap_or(Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS));

        async move {
            // Clone sql for logging in both blocking task and timeout supervision
            let sql_log_blocking = sql.clone();
            let sql_log_supervisor = sql.clone();

            // Acquire concurrency permit (ASYNC scope)
            let permit = query_limiter
                .acquire_owned()
                .await
                .map_err(|_| QueryError::Internal("query limiter closed".into()))?;

            let (tx, rx): (
                tokio::sync::mpsc::Sender<Result<arrow::array::RecordBatch, QueryError>>,
                tokio::sync::mpsc::Receiver<Result<arrow::array::RecordBatch, QueryError>>,
            ) = tokio::sync::mpsc::channel(100);
            let tx_blocking = tx.clone(); // Clone for blocking task

            let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let cancelled_blocking = cancelled.clone();

            let blocking_task = tokio::task::spawn_blocking(move || {
                // Hold permit for full execution
                let _permit = permit;

                // Open connection and configure
                let conn = match Connection::open(&database_path) {
                    Ok(c) => c,
                    Err(e) => {
                        let err = QueryError::Internal(format!("open failed: {}", e));
                        error!(error = %err, "execute_query: connection open failed");
                        let _ = tx_blocking.blocking_send(Err(err.clone()));
                        return Err(err);
                    }
                };

                if let Err(e) = conn.execute(&format!("SET memory_limit='{}MB'", memory_mb), []) {
                    let err = QueryError::Internal(format!("set memory limit failed: {}", e));
                    error!(error = %err, "execute_query: set memory limit failed");
                    let _ = tx_blocking.blocking_send(Err(err.clone()));
                    return Err(err);
                }

                if let Err(e) = conn.execute("SET enable_progress_bar=false", []) {
                    let err = QueryError::Internal(format!("disable progress bar failed: {}", e));
                    error!(error = %err, "execute_query: disable progress bar failed");
                    let _ = tx_blocking.blocking_send(Err(err.clone()));
                    return Err(err);
                }

                // Prepare statement
                let mut stmt = match conn.prepare(&sql) {
                    Ok(s) => s,
                    Err(e) => {
                        let err = Self::classify_duckdb_error(e);
                        error!(sql = %sql, error = %err, "execute_query: prepare statement failed");
                        let _ = tx_blocking.blocking_send(Err(err.clone()));
                        return Err(err);
                    }
                };

                // Execute query and get Arrow reader
                let arrow_reader = match stmt.query_arrow([]) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = Self::classify_duckdb_error(e);
                        error!(sql = %sql, error = %err, "execute_query: query_arrow failed");
                        let _ = tx_blocking.blocking_send(Err(err.clone()));
                        return Err(err);
                    }
                };

                // Stream results - arrow_reader is an iterator over RecordBatch directly
                for batch in arrow_reader {
                    // Cooperative cancellation
                    if cancelled_blocking.load(std::sync::atomic::Ordering::Relaxed) {
                        info!(sql = %sql_log_blocking, "execute_query: query cancelled due to timeout");
                        break;
                    }

                    // Send batch to channel
                    if tx_blocking.blocking_send(Ok(batch)).is_err() {
                        // Client disconnected
                        info!(sql = %sql_log_blocking, "execute_query: query cancelled due to client disconnect");
                        break;
                    }
                }

                Ok::<(), QueryError>(())
            });

            // 5. Timeout supervision (ASYNC)
            tokio::spawn({
                let tx = tx.clone();
                let cancelled = cancelled.clone();

                async move {
                    match tokio::time::timeout(timeout, blocking_task).await {
                        Ok(Ok(Ok(()))) => {
                            // Normal completion
                        }
                        Ok(Ok(Err(e))) => {
                            error!(sql = %sql_log_supervisor, error = %e, "execute_query: query execution failed");
                            let _ = tx.send(Err(e)).await;
                        }
                        Ok(Err(join_err)) => {
                            let err =
                                QueryError::Internal(format!("blocking task panic: {}", join_err));
                            error!(sql = %sql_log_supervisor, error = %err, "execute_query: blocking task panicked");
                            let _ = tx.send(Err(err)).await;
                        }
                        Err(_) => {
                            // Timeout fired
                            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                            let err = QueryError::Timeout(timeout.as_secs());
                            warn!(sql = %sql_log_supervisor, timeout_secs = timeout.as_secs(), "execute_query: query timed out");
                            let _ = tx.send(Err(err)).await;
                        }
                    }
                }
            });

            // 6. Return streaming receiver
            let stream = ReceiverStream::new(rx);
            Ok(Box::pin(stream) as QueryResultStream)
        }
    }
}
