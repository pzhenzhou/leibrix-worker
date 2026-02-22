use super::SharedDatabase;
use crate::engine::query_engine::{QueryEngine, QueryError, QueryResultStream};
use crate::engine::storage_engine::TableMetadata;
use arrow_schema::Schema;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

/// Default timeout for queries if not specified (5 minutes)
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 300;

/// Result type for streaming record batches out of the query engine.
type BatchResult = Result<arrow::array::RecordBatch, QueryError>;

pub struct DuckDBQueryEngine {
    /// Shared database handle for creating read connections
    pub shared_db: Arc<SharedDatabase>,
    /// Semaphore to limit concurrent queries
    query_limiter: Arc<Semaphore>,
    /// Maximum concurrent queries allowed
    #[allow(dead_code)]
    max_concurrent_queries: usize,
}

impl DuckDBQueryEngine {
    /// Creates a new query engine using the shared database.
    /// This allows read-write separation with StorageEngine.
    pub fn new(shared_db: Arc<SharedDatabase>, max_concurrent_queries: usize) -> Self {
        let query_limiter = Arc::new(Semaphore::new(max_concurrent_queries));
        Self {
            shared_db,
            query_limiter,
            max_concurrent_queries,
        }
    }

    fn classify_anyhow_error(err: anyhow::Error, table_name: &str) -> QueryError {
        // Use {:#} to get the full error chain including all nested causes
        let full_msg = format!("{:#}", err);

        if full_msg.contains("does not exist")
            || full_msg.contains("not found")
            || full_msg.contains("Table with name")
            || full_msg.contains("Catalog Error")
        {
            QueryError::TableNotFound(table_name.to_string())
        } else if full_msg.contains("Out of Memory") || full_msg.contains("failed to allocate") {
            QueryError::DuckDB(format!("Out of memory: {}", full_msg))
        } else {
            QueryError::DuckDB(err.to_string())
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
        let shared_db = self.shared_db.clone();
        let table_name = table_name.to_string();
        let table_name_log = table_name.clone();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = shared_db.get().map_err(|e| {
                    QueryError::Internal(format!("failed to get pooled connection: {}", e))
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
        let shared_db = self.shared_db.clone();
        let table_name = table_name.to_string();
        let table_name_log = table_name.clone();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = shared_db.get().map_err(|e| {
                    QueryError::Internal(format!("failed to get pooled connection: {}", e))
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
        let shared_db = self.shared_db.clone();
        let prefix = format!("{}__%", dataset_id);
        let dataset_id_log = dataset_id.to_string();

        async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = shared_db.get().map_err(|e| {
                    QueryError::Internal(format!("failed to get pooled connection: {}", e))
                })?;
                // dataset is logical, a high-level concept, is a grouping of data (e.g., a project, a user, a topic).
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
        // Memory limit is now set once at database initialization (apply_global_settings),
        // not per-query, to avoid races on the database-global setting.
        _memory_limit: Option<usize>,
        timeout_secs: Option<Duration>,
    ) -> impl Future<Output = Result<QueryResultStream, QueryError>> + Send {
        let query_limiter = self.query_limiter.clone();
        let shared_db = self.shared_db.clone();
        let sql = sql.to_string();
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
                tokio::sync::mpsc::Sender<BatchResult>,
                tokio::sync::mpsc::Receiver<BatchResult>,
            ) = tokio::sync::mpsc::channel(100);
            let tx_blocking = tx.clone(); // Clone for blocking task

            // Channel for the interrupt handle: the blocking task sends it once the
            // connection is acquired so the timeout supervisor can hard-cancel DuckDB
            // via duckdb_interrupt() instead of relying on the cooperative AtomicBool
            // (which only fires between batch iterations).
            let (interrupt_tx, interrupt_rx) =
                tokio::sync::oneshot::channel::<std::sync::Arc<duckdb::InterruptHandle>>();

            let blocking_task = tokio::task::spawn_blocking(move || {
                // Hold permit for full execution
                let _permit = permit;

                // Get pooled connection from shared database
                let conn = match shared_db.get() {
                    Ok(c) => c,
                    Err(e) => {
                        let err =
                            QueryError::Internal(format!("failed to get pooled connection: {}", e));
                        error!(error = %err, "execute_query: connection failed");
                        let _ = tx_blocking.blocking_send(Err(err.clone()));
                        return Err(err);
                    }
                };

                // Send the interrupt handle to the timeout supervisor so it can
                // call duckdb_interrupt() to hard-cancel in-flight DuckDB operations.
                let _ = interrupt_tx.send(conn.interrupt_handle());

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

                let arrow_reader = match stmt.query_arrow([]) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = Self::classify_duckdb_error(e);
                        error!(sql = %sql, error = %err, "execute_query: query_arrow failed");
                        let _ = tx_blocking.blocking_send(Err(err.clone()));
                        return Err(err);
                    }
                };

                for batch in arrow_reader {
                    // Use try_send in a timed backoff loop instead of blocking_send.
                    // blocking_send can hold the spawn_blocking OS thread indefinitely if
                    // the consumer is slow or the client has disconnected, eventually
                    // exhausting tokio's blocking thread pool.  Here we retry for at most
                    // 5 seconds, then treat a perpetually-full channel as a disconnect.
                    let mut msg = Ok(batch);
                    let send_deadline =
                        std::time::Instant::now() + std::time::Duration::from_secs(5);
                    let sent = loop {
                        match tx_blocking.try_send(msg) {
                            Ok(()) => break true,
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                break false;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                                msg = returned; // reclaim value for next attempt
                                if std::time::Instant::now() >= send_deadline {
                                    warn!(
                                        sql = %sql_log_blocking,
                                        "execute_query: output channel full for >5s, treating as client disconnect"
                                    );
                                    break false;
                                }
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                        }
                    };
                    if !sent {
                        info!(sql = %sql_log_blocking, "execute_query: query cancelled due to client disconnect");
                        break;
                    }
                }

                Ok::<(), QueryError>(())
            });

            // Timeout supervision — uses duckdb_interrupt() for hard cancellation.
            tokio::spawn({
                let tx = tx.clone();

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
                            // Timeout fired — hard-cancel the in-flight DuckDB operation
                            // via duckdb_interrupt() so we don't have to wait for the next
                            // batch boundary.
                            if let Ok(handle) = interrupt_rx.await {
                                handle.interrupt();
                            }
                            let err = QueryError::Timeout(timeout.as_secs());
                            warn!(sql = %sql_log_supervisor, timeout_secs = timeout.as_secs(), "execute_query: query timed out, duckdb_interrupt sent");
                            let _ = tx.send(Err(err)).await;
                        }
                    }
                }
            });

            let stream = ReceiverStream::new(rx);
            Ok(Box::pin(stream) as QueryResultStream)
        }
    }
}
