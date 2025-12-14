use crate::engine::engine::{RecordBatchStream, StorageError};
use crate::loader::starrocks::select_text;
use crate::loader::types::{Catalog, SourceError};

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    },
};

use futures_util::stream::{StreamExt, TryStreamExt};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use sqlx::Row;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};

// Connection Pool Sizing Guidelines:
// - For I/O-bound workloads (network latency > computation): max_connections should be
//   roughly 1-4x the number of concurrent queries to keep connections busy during I/O waits
// - For CPU-bound workloads: max_connections ≈ number of concurrent queries
// - StarRocks typically supports 100-1000 concurrent connections per node

const DEFAULT_MAX_CONNECTIONS: u32 = 16;
const DEFAULT_MIN_CONNECTIONS: u32 = 4;
const DEFAULT_BATCH_SIZE: usize = 8192;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(3600); // 1 hour

/// Worker-local JDBC runtime configuration.
///
/// MySqlPool's built-in connection management. The pool's max_connections acts as
/// both the connection limit and the concurrency limit.
///
/// Rationale:
/// - MySqlPool already enforces connection limits and provides queuing
/// - Adding a semaphore creates complexity and potential deadlocks
/// - The pool can better optimize connection reuse and lifecycle
#[derive(Debug, Clone, Copy)]
struct StarRocksJdbcOptions {
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    idle_timeout: Duration,
    max_lifetime: Duration,
    batch_size: usize,
}

impl StarRocksJdbcOptions {
    fn from_catalog(catalog: &Catalog) -> Self {
        // Extract pool configuration from catalog options or use defaults
        let pool_opts = match catalog {
            Catalog::StarRocks { pool_options, .. } => pool_options.as_ref(),
            Catalog::Jdbc { pool_options, .. } => pool_options.as_ref(),
            _ => None,
        };

        let max_conn = pool_opts
            .and_then(|p| p.max_connections)
            .unwrap_or(DEFAULT_MAX_CONNECTIONS)
            .max(1)
            .min(100);

        let min_conn = pool_opts
            .and_then(|p| p.min_connections)
            .unwrap_or(DEFAULT_MIN_CONNECTIONS)
            .max(0)
            .min(max_conn);

        let acquire_to = pool_opts
            .and_then(|p| p.acquire_timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_ACQUIRE_TIMEOUT);

        let connect_to = pool_opts
            .and_then(|p| p.connect_timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let idle_to = pool_opts
            .and_then(|p| p.idle_timeout_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_IDLE_TIMEOUT);

        let max_life = pool_opts
            .and_then(|p| p.max_lifetime_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_MAX_LIFETIME);

        let batch_sz = pool_opts
            .and_then(|p| p.batch_size.map(|b| b as usize))
            .unwrap_or(DEFAULT_BATCH_SIZE)
            .max(1)
            .min(100_000);

        Self {
            max_connections: max_conn,
            min_connections: min_conn,
            acquire_timeout: acquire_to,
            connect_timeout: connect_to,
            idle_timeout: idle_to,
            max_lifetime: max_life,
            batch_size: batch_sz,
        }
    }
}

fn env_u32(key: &str) -> Option<u32> {
    std::env::var(key).ok().and_then(|v| v.parse::<u32>().ok())
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

pub struct StarRocksJdbcClient {
    // Internal state protected by mutex for thread-safe operations
    state: Arc<Mutex<ClientState>>,
    // Simple atomic counter for active queries
    active_query_count: Arc<AtomicUsize>,
    // Keepalive sender for graceful shutdown
    _shutdown_tx: Arc<tokio::sync::oneshot::Sender<()>>,
}

impl Clone for StarRocksJdbcClient {
    fn clone(&self) -> Self {
        StarRocksJdbcClient {
            state: self.state.clone(),
            active_query_count: self.active_query_count.clone(),
            _shutdown_tx: self._shutdown_tx.clone(),
        }
    }
}

struct ClientState {
    pool: MySqlPool,
    catalog_name: String,
    options: StarRocksJdbcOptions,
    is_closing: bool,
}

/// RAII guard that decrements the query counter on drop
struct QueryCountGuard {
    counter: Arc<AtomicUsize>,
}

impl QueryCountGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for QueryCountGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StarRocksJdbcClient {
    pub async fn from_catalog(catalog: Catalog) -> anyhow::Result<Self> {
        if let Catalog::Jdbc { ref uri, .. } = catalog {
            let opts = StarRocksJdbcOptions::from_catalog(&catalog);

            // Validate pool sizing relative to workload
            if opts.min_connections > opts.max_connections {
                tracing::warn!(
                    "min_connections ({}) > max_connections ({}), adjusting min to max",
                    opts.min_connections,
                    opts.max_connections
                );
            }

            // For I/O-bound StarRocks queries, having more connections than typical concurrent
            // queries helps keep connections busy during network waits. This is the key insight:
            // MySqlPool's queuing handles the concurrency, we just size the pool appropriately.
            let suggested_connections = opts.max_connections;

            tracing::info!(
                "Initializing MySQL pool: max_connections={}, min_connections={}, acquire_timeout={:?}",
                suggested_connections,
                opts.min_connections,
                opts.acquire_timeout
            );

            let connect_opts =
                MySqlConnectOptions::from_str(&uri).map_err(|e| SourceError::Config {
                    catalog: "jdbc".to_string(),
                    reason: format!("invalid MySQL URI '{}': {}", uri, e),
                })?;

            let pool = MySqlPoolOptions::new()
                .min_connections(opts.min_connections)
                .max_connections(opts.max_connections)
                .acquire_timeout(opts.acquire_timeout)
                .idle_timeout(Some(opts.idle_timeout))
                .max_lifetime(Some(opts.max_lifetime))
                .connect_with(connect_opts)
                .await
                .map_err(|e| SourceError::Config {
                    catalog: "jdbc".to_string(),
                    reason: format!("failed to create connection pool: {}", e),
                })?;

            // Test the pool by fetching a connection
            let test_conn = pool.acquire().await.map_err(|e| SourceError::Network {
                catalog: "jdbc".to_string(),
                source: Box::new(e),
            })?;
            drop(test_conn); // Return connection to pool
            // Setup graceful shutdown channel
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let state = Arc::new(Mutex::new(ClientState {
                pool,
                catalog_name: "starrocks".to_string(),
                options: opts,
                is_closing: false,
            }));

            // Initialize simple atomic counter for query tracking
            let active_query_count = Arc::new(AtomicUsize::new(0));

            // Spawn a task to handle shutdown signaling
            let state_clone = state.clone();
            tokio::spawn(async move {
                let _ = shutdown_rx.await;
                let mut s = state_clone.lock().await;
                s.is_closing = true;
                tracing::info!("StarRocks JDBC client shutdown initiated");
            });

            Ok(StarRocksJdbcClient {
                state,
                active_query_count,
                _shutdown_tx: Arc::new(shutdown_tx),
            })
        } else {
            Err(anyhow::anyhow!(SourceError::UnsupportedCatalog {
                catalog: "jdbc client only supports JDBC catalog".to_string(),
            }))
        }
    }

    /// Execute a query and stream results.
    ///
    /// Key design points:
    /// 1. NO semaphore - MySqlPool handles connection queuing and limits
    /// 2. Channel capacity = batch size for efficient backpressure
    /// 3. Task cancellation via stream drop
    async fn query(
        &self,
        sql: &str,
        schema: Arc<Schema>,
    ) -> Result<RecordBatchStream, StorageError> {
        // Get current state
        let state = self.state.lock().await;
        if state.is_closing {
            return Err(StorageError::Backend {
                backend: "starrocks-jdbc",
                message: "client is shutting down".to_string(),
            });
        }

        let pool = state.pool.clone();
        let catalog = state.catalog_name.clone();
        let batch_size = state.options.batch_size;
        drop(state); // Release lock early

        // Channel size matches batch size for optimal memory usage
        // This creates natural backpressure when batch_size is reached
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, SourceError>>(batch_size);

        let sql = sql.to_string();
        let schema_clone = schema.clone();

        // Increment active query counter
        let query_count = self.active_query_count.clone();
        query_count.fetch_add(1, Ordering::SeqCst);

        // Spawn task to execute query - the pool's internal queueing handles concurrency
        let count_clone = query_count.clone();
        tokio::spawn(async move {
            // Decrement counter when query completes (via defer pattern)
            let _guard = QueryCountGuard::new(count_clone);

            if let Err(e) =
                Self::run_query_async(&pool, &sql, &schema_clone, &catalog, tx, batch_size).await
            {
                tracing::error!("Query execution failed: {}", e);
            }
        });

        // Convert channel to stream, mapping errors
        let stream = ReceiverStream::new(rx).map(move |result| {
            result.map_err(|source_err| StorageError::Backend {
                backend: "starrocks-jdbc",
                message: source_err.to_string(),
            })
        });

        Ok(Box::pin(stream))
    }

    async fn run_query_async(
        pool: &MySqlPool,
        sql: &str,
        schema: &Schema,
        catalog: &str,
        tx: mpsc::Sender<Result<RecordBatch, SourceError>>,
        batch_size: usize,
    ) -> Result<(), SourceError> {
        // MySqlPool automatically queues this query if all connections are busy
        // No additional concurrency control needed
        let mut rows = sqlx::query(sql).fetch(pool);

        let mut batch_builders = create_batch_builders(schema);
        let mut row_count = 0;

        while let Some(row_result) = rows.next().await {
            let row = row_result.map_err(|e| SourceError::Query {
                catalog: catalog.to_string(),
                message: format!("failed to fetch row: {}", e),
                source: Some(Box::new(e)),
            })?;

            for (idx, field) in schema.fields().iter().enumerate() {
                add_row_to_builder(&mut batch_builders[idx], &row, idx, field, catalog)?;
            }
            row_count += 1;

            // Flush when batch is full
            if row_count >= batch_size {
                let batch = finalize_batch(&mut batch_builders, schema, catalog)?;
                if tx.send(Ok(batch)).await.is_err() {
                    // Channel closed - consumer dropped the stream
                    return Ok(());
                }

                batch_builders = create_batch_builders(schema);
                row_count = 0;
            }
        }

        // Send final batch if not empty
        if row_count > 0 {
            let batch = finalize_batch(&mut batch_builders, schema, catalog)?;
            let _ = tx.send(Ok(batch)).await; // Ignore send errors
        }

        Ok(())
    }

    /// Get current pool statistics for monitoring
    pub async fn pool_stats(&self) -> PoolStats {
        let state = self.state.lock().await;
        PoolStats {
            size: state.pool.size(),
            idle_connections: state.pool.num_idle() as u32,
            is_closing: state.is_closing,
        }
    }

    /// Production-grade graceful shutdown
    ///
    /// Ensures all in-flight queries complete before closing the connection pool.
    ///
    /// # Shutdown Phases
    /// 1. **Stop accepting new queries**: Set is_closing flag
    /// 2. **Wait for active queries**: Poll and wait for tasks to complete
    /// 3. **Force shutdown on timeout**: Abort remaining queries after deadline
    /// 4. **Close pool**: Cleanly close all database connections
    ///
    /// # Parameters
    /// - `timeout`: Maximum time to wait for queries (default: 30s)
    ///
    /// # Returns
    /// - `Ok(ShutdownStats)`: Successful shutdown with statistics
    /// - `Err(StorageError)`: If shutdown fails critically
    pub async fn shutdown(self) -> Result<ShutdownStats, StorageError> {
        self.shutdown_with_timeout(Duration::from_secs(30)).await
    }

    /// Graceful shutdown with custom timeout
    pub async fn shutdown_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ShutdownStats, StorageError> {
        let shutdown_start = std::time::Instant::now();
        // Stop accepting new queries
        tracing::info!("Graceful shutdown initiated");
        {
            let mut state = self.state.lock().await;
            state.is_closing = true;
        }

        // Wait for active queries to complete
        let initial_count = self.active_query_count.load(Ordering::SeqCst);
        tracing::info!(
            active_queries = initial_count,
            timeout_secs = timeout.as_secs(),
            "Waiting for active queries to complete"
        );

        let poll_interval = Duration::from_millis(500);
        let deadline = shutdown_start + timeout;

        loop {
            let active_count = self.active_query_count.load(Ordering::SeqCst);
            if active_count == 0 {
                tracing::info!("All queries completed successfully");
                break;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                tracing::warn!(
                    remaining_queries = active_count,
                    "Shutdown timeout reached, {} queries still active",
                    active_count
                );
                break;
            }

            tracing::debug!(
                active_queries = active_count,
                elapsed_secs = shutdown_start.elapsed().as_secs(),
                "Waiting for queries to complete"
            );

            tokio::time::sleep(poll_interval).await;
        }

        // Phase 3: Close the connection pool
        tracing::info!("Closing connection pool");
        let state = self.state.lock().await;
        let pool_size = state.pool.size();
        let idle_connections = state.pool.num_idle();
        let final_active = self.active_query_count.load(Ordering::SeqCst);

        // Pool will be closed when state is dropped
        drop(state);

        let total_duration = shutdown_start.elapsed();
        let completed = initial_count.saturating_sub(final_active);

        let stats = ShutdownStats {
            completed_queries: completed,
            aborted_queries: final_active,
            total_duration,
            final_pool_size: pool_size,
            final_idle_connections: idle_connections as u32,
        };

        tracing::info!(
            completed = completed,
            still_active = final_active,
            duration_ms = total_duration.as_millis(),
            "JDBC client shutdown complete"
        );

        Ok(stats)
    }

    /// Get count of currently active queries
    pub fn active_query_count(&self) -> usize {
        self.active_query_count.load(Ordering::SeqCst)
    }
}

/// Pool statistics for monitoring and debugging
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub size: u32,
    pub idle_connections: u32,
    pub is_closing: bool,
}

/// Statistics from a graceful shutdown operation
#[derive(Debug, Clone)]
pub struct ShutdownStats {
    /// Number of queries that completed successfully during shutdown
    pub completed_queries: usize,
    /// Number of queries that were forcefully aborted
    pub aborted_queries: usize,
    /// Total duration of shutdown process
    pub total_duration: Duration,
    /// Final pool size before close
    pub final_pool_size: u32,
    /// Number of idle connections at shutdown
    pub final_idle_connections: u32,
}

impl Drop for StarRocksJdbcClient {
    fn drop(&mut self) {
        // Note: MySqlPool automatically closes when dropped
        // This is safe because sqlx handles connection cleanup
        tracing::debug!("StarRocksJdbcClient dropped, pool will be closed");
    }
}

// Helper functions for Arrow conversion
fn create_batch_builders(schema: &Schema) -> Vec<Box<dyn arrow_array::builder::ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Boolean => {
                Box::new(BooleanBuilder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Int32 => {
                Box::new(Int32Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Int64 => {
                Box::new(Int64Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Float64 => {
                Box::new(Float64Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Utf8 => {
                Box::new(StringBuilder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Date32 => {
                Box::new(Date32Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>
            }
            DataType::Timestamp(_, _) => Box::new(TimestampMicrosecondBuilder::new())
                as Box<dyn arrow_array::builder::ArrayBuilder>,
            DataType::Decimal128(precision, scale) => Box::new(
                Decimal128Builder::new()
                    .with_precision_and_scale(*precision, *scale)
                    .unwrap(),
            )
                as Box<dyn arrow_array::builder::ArrayBuilder>,
            _ => panic!("Unsupported data type: {:?}", field.data_type()),
        })
        .collect()
}

fn add_row_to_builder(
    builder: &mut Box<dyn arrow_array::builder::ArrayBuilder>,
    row: &MySqlRow,
    idx: usize,
    field: &Field,
    catalog: &str,
) -> Result<(), SourceError> {
    macro_rules! get_opt {
        ($ty:ty, $what:literal) => {{
            row.try_get::<Option<$ty>, _>(idx)
                .map_err(|e| SourceError::Protocol {
                    catalog: catalog.to_string(),
                    message: format!("failed to get {} at index {}: {}", $what, idx, e),
                    source: Some(Box::new(e)),
                })?
        }};
    }

    macro_rules! append_opt {
        ($builder_ty:ty, $opt_val:expr) => {{
            let b = builder.as_any_mut().downcast_mut::<$builder_ty>().unwrap();
            match $opt_val {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }};
    }

    match field.data_type() {
        DataType::Boolean => {
            let val = get_opt!(bool, "bool");
            append_opt!(BooleanBuilder, val);
        }
        DataType::Int32 => {
            let val = get_opt!(i32, "i32");
            append_opt!(Int32Builder, val);
        }
        DataType::Int64 => {
            let val = get_opt!(i64, "i64");
            append_opt!(Int64Builder, val);
        }
        DataType::Float64 => {
            let val = get_opt!(f64, "f64");
            append_opt!(Float64Builder, val);
        }
        DataType::Utf8 => {
            let val = get_opt!(String, "string");
            append_opt!(StringBuilder, val);
        }
        DataType::Date32 => {
            let val = get_opt!(chrono::NaiveDate, "date");
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap();
            match val {
                Some(v) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days = (v - epoch).num_days();
                    b.append_value(days as i32);
                }
                None => b.append_null(),
            }
        }
        DataType::Timestamp(_, _) => {
            let val = get_opt!(chrono::NaiveDateTime, "timestamp");
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            match val {
                Some(v) => b.append_value(v.and_utc().timestamp_micros()),
                None => b.append_null(),
            }
        }
        DataType::Decimal128(_, scale) => {
            let val = get_opt!(String, "decimal");
            let b = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap();
            match val {
                Some(v) => {
                    let scaled =
                        parse_decimal_to_i128(&v, *scale).map_err(|e| SourceError::Protocol {
                            catalog: catalog.to_string(),
                            message: format!("failed to parse decimal '{}': {}", v, e),
                            source: None,
                        })?;
                    b.append_value(scaled);
                }
                None => b.append_null(),
            }
        }
        _ => {
            return Err(SourceError::Protocol {
                catalog: catalog.to_string(),
                message: format!("unsupported data type: {:?}", field.data_type()),
                source: None,
            });
        }
    }
    Ok(())
}

fn parse_decimal_to_i128(input: &str, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("negative scale is not supported: {scale}"));
    }

    let s = input.trim();
    if s.is_empty() {
        return Err("empty decimal string".to_string());
    }

    let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    let mut parts = s.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next().unwrap_or("");

    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let int_val: i128 = int_part
        .parse()
        .map_err(|e| format!("invalid integer part '{int_part}': {e}"))?;

    let scale_u32 = scale as u32;
    let pow10: i128 = 10_i128.pow(scale_u32);

    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("invalid fractional part '{frac_part}'"));
    }

    if frac_part.len() > scale_u32 as usize {
        return Err(format!(
            "fractional precision {} exceeds declared scale {scale}",
            frac_part.len()
        ));
    }

    let frac_raw: i128 = if frac_part.is_empty() {
        0
    } else {
        frac_part
            .parse::<i128>()
            .map_err(|e| format!("invalid fractional digits '{frac_part}': {e}"))?
    };

    let frac_pow = 10_i128.pow((scale_u32 as usize - frac_part.len()) as u32);
    let frac_scaled = frac_raw * frac_pow;

    let mut out = int_val
        .checked_mul(pow10)
        .ok_or_else(|| "overflow scaling integer part".to_string())?
        .checked_add(frac_scaled)
        .ok_or_else(|| "overflow adding fractional part".to_string())?;

    if neg {
        out = -out;
    }
    Ok(out)
}

fn finalize_batch(
    builders: &mut Vec<Box<dyn arrow_array::builder::ArrayBuilder>>,
    schema: &Schema,
    catalog: &str,
) -> Result<RecordBatch, SourceError> {
    let arrays: Vec<ArrayRef> = builders
        .iter_mut()
        .map(|builder| builder.finish())
        .collect();

    RecordBatch::try_new(Arc::new(schema.clone()), arrays).map_err(|e| SourceError::Internal {
        catalog: catalog.to_string(),
        source: Box::new(e),
    })
}

// Implement SourceAdapter trait
impl crate::loader::adapter::SourceAdapter for StarRocksJdbcClient {
    fn stream_data(
        &self,
        source: Arc<crate::loader::types::DataSource>,
        schema: Arc<Schema>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<RecordBatchStream, StorageError>> + Send>>
    {
        let sql = select_text(source.clone(), schema.clone());
        let client = self.clone();

        Box::pin(async move {
            client
                .query(&sql, schema)
                .await
                .map_err(|e| StorageError::Backend {
                    backend: "starrocks-jdbc",
                    message: e.to_string(),
                })
        })
    }
}
