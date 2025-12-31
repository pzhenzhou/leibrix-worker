use crate::engine::storage_engine::{RecordBatchStream, StorageError};
use crate::loader::types::{Catalog, DataSource, SourceError};
use crate::loader::starrocks::select_text;
use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use base64::Engine;
use futures_util::{TryStreamExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;



const DEFAULT_MAX_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct StarRocksAdbcClient {
    catalog_name: String,
    host: String,
    port: usize,
    database: String,
    user: String,
    password: String,
    max_concurrency: usize,
    limiter: Arc<Semaphore>,
    /// Shared Tokio runtime handle for async operations.
    /// This avoids creating a new runtime per query execution.
    runtime_handle: tokio::runtime::Handle,
}

impl StarRocksAdbcClient {
    /// Creates a new StarRocks ADBC client from a catalog configuration.
    ///
    /// # Arguments
    /// * `catalog` - The StarRocks catalog configuration
    /// * `runtime_handle` - Handle to an existing Tokio runtime for executing async operations.
    ///   If None, attempts to get the current runtime's handle. If no runtime exists,
    ///   returns an error.
    pub fn from_catalog(
        catalog: Catalog,
        runtime_handle: Option<tokio::runtime::Handle>,
    ) -> anyhow::Result<Self> {
        if let Catalog::StarRocks {
            catalog: catalog_name,
            host,
            port,
            database,
            user,
            password,
            max_concurrency,
            ..
        } = catalog
        {
            let concurrency = if let Some(concurrency) = max_concurrency {
                concurrency
            } else {
                DEFAULT_MAX_CONCURRENCY
            };
            
            // Get runtime handle: provided, current, or fail
            let runtime_handle = match runtime_handle {
                Some(handle) => handle,
                None => tokio::runtime::Handle::try_current().map_err(|_| {
                    anyhow::anyhow!(SourceError::Config {
                        catalog: catalog_name.clone(),
                        reason: "no Tokio runtime available and none provided".to_string(),
                    })
                })?,
            };
            
            Ok(StarRocksAdbcClient {
                catalog_name,
                host,
                port,
                database,
                user,
                password,
                max_concurrency: concurrency,
                limiter: Arc::new(Semaphore::new(concurrency)),
                runtime_handle,
            })
        } else {
            Err(anyhow::anyhow!(SourceError::UnsupportedCatalog {
                catalog: "adbc client only supports StarRocks catalog".to_string(),
            }))
        }
    }

    /// Executes a SQL query and returns a stream of Arrow RecordBatches.
    ///
    /// This async method bridges to the blocking Flight SQL operations by:
    /// 1. Acquiring a semaphore permit to enforce concurrent query limits
    /// 2. Spawning a blocking task to execute the query via `run_query_blocking`
    /// 3. Converting the mpsc receiver into a Stream
    ///
    /// The permit is held for the entire duration of the query execution and
    /// automatically released when the stream is dropped or completes.
    async fn query(&self, sql: &str) -> anyhow::Result<RecordBatchStream> {
        let catalog = self.catalog_name.clone();
        
        // 1. Acquire semaphore permit to enforce concurrent query limits
        let permit = self.limiter.clone().acquire_owned().await.map_err(|e| {
            SourceError::EngineUnavailable {
                catalog: catalog.clone(),
                reason: format!("semaphore closed: {e}"),
            }
        })?;

        // 2. Create channel for streaming results from blocking task
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, SourceError>>(100);
        
        // Clone data needed for the blocking task
        let sql = sql.to_string();
        let client = StarRocksAdbcClient {
            catalog_name: self.catalog_name.clone(),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            max_concurrency: self.max_concurrency,
            limiter: self.limiter.clone(),
            runtime_handle: self.runtime_handle.clone(),
        };

        // 3. Spawn blocking task to execute the query
        tokio::task::spawn_blocking(move || {
            // Hold permit for the duration of the blocking operation
            let _permit = permit;
            
            // Execute blocking query
            if let Err(e) = client.run_query_blocking(&sql, tx.clone()) {
                // Log error but don't panic - error will be propagated via channel
                tracing::error!("Query execution failed: {}", e);
            }
            // Channel automatically closes when tx is dropped here
        });

        // 4. Convert mpsc receiver into a Stream, mapping SourceError to StorageError
        let stream = ReceiverStream::new(rx).map(|result| {
            result.map_err(|source_err| {
                // Convert SourceError to StorageError
                StorageError::Backend {
                    backend: "starrocks-flight",
                    message: source_err.to_string(),
                }
            })
        });

        Ok(Box::pin(stream))
    }

    fn run_query_blocking(
        &self,
        sql: &str,
        tx: mpsc::Sender<Result<RecordBatch, SourceError>>,
    ) -> anyhow::Result<()> {
        let catalog = self.catalog_name.clone();
        let uri = format!("http://{}:{}", self.host, self.port);
        let sql = sql.to_string();
        let user = self.user.clone();
        let password = self.password.clone();
        let database = self.database.clone();

        // Use the shared runtime handle to block on async operations.
        // This is much more efficient than creating a new runtime per call.
        self.runtime_handle.block_on(async {
            // 1. Establish connection to Flight SQL endpoint
            let channel = tonic::transport::Channel::from_shared(uri.clone())
                .map_err(|e| SourceError::Config {
                    catalog: catalog.clone(),
                    reason: format!("invalid URI '{}': {}", uri, e),
                })?
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(300))
                .connect()
                .await
                .map_err(|e| SourceError::Network {
                    catalog: catalog.clone(),
                    source: Box::new(e),
                })?;

            let mut client = FlightSqlServiceClient::new(channel);

            // 2. Set authentication headers if credentials provided
            if !user.is_empty() {
                // StarRocks Flight SQL uses basic auth
                let auth = format!("{}:{}", user, password);
                let encoded = base64::engine::general_purpose::STANDARD.encode(auth.as_bytes());
                client.set_header("authorization", format!("Basic {}", encoded));
            }

            // 3. Set database context
            if !database.is_empty() {
                client.set_header("database", database);
            }

            // 4. Execute query and get FlightInfo
            let flight_info: arrow_flight::FlightInfo = client
                .execute(sql.clone(), None)
                .await
                .map_err(|e| SourceError::Query {
                    catalog: catalog.clone(),
                    message: format!("failed to execute query: {}", e),
                    source: Some(Box::new(e)),
                })?;

            // 5. Fetch results from each endpoint
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    // Get stream for this endpoint
                    let mut stream: FlightRecordBatchStream = client
                        .do_get(ticket)
                        .await
                        .map_err(|e| SourceError::Protocol {
                            catalog: catalog.clone(),
                            message: format!("failed to fetch results: {}", e),
                            source: Some(Box::new(e)),
                        })?;

                    // Stream record batches through the channel
                    while let Some(batch_result) = stream.try_next().await.map_err(|e: FlightError| {
                        SourceError::Protocol {
                            catalog: catalog.clone(),
                            message: format!("error reading result stream: {}", e),
                            source: Some(Box::new(e)),
                        }
                    })? {
                        // Send batch to channel
                        if let Err(_) = tx.send(Ok(batch_result)).await {
                            // Channel closed, receiver dropped
                            return Err(SourceError::Internal {
                                catalog: catalog.clone(),
                                source: "result channel closed unexpectedly".into(),
                            });
                        }
                    }
                }
            }

            Ok::<(), SourceError>(())
        })?;

        Ok(())
    }
}

// Implement SourceAdapter trait
impl crate::loader::adapter::SourceAdapter for StarRocksAdbcClient {
    fn stream_data(
        &self,
        source: Arc<DataSource>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RecordBatchStream, StorageError>> + Send>> {
        let sql = select_text(source.clone(), schema.clone());
        let client = self.clone();
        
        Box::pin(async move {
            client.query(&sql).await.map_err(|e| StorageError::Backend {
                backend: "starrocks-adbc",
                message: e.to_string(),
            })
        })
    }
}
