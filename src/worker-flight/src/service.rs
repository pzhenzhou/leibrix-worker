//! Arrow Flight query service implementation.
//!
//! This module implements the FlightService trait for:
//! - SQL query execution over Arrow Flight
//! - LDP stage execution via DoAction `submit_stage`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures_util::stream::{self, BoxStream};
use futures_util::TryStreamExt;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, instrument, warn};

use worker_storage::engine::query_engine::QueryEngine;
use worker_storage::sql::SqlTransformer;

use crate::error::{map_query_error_to_status, map_transform_error_to_status};
use crate::ticket::{FlightTicket, QueryTicket, StageResultTicket};

// ============================================================================
// Stage Result Store
// ============================================================================

/// Key for stored stage results.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StageResultKey {
    pub query_id: String,
    pub stage_id: u32,
    pub partition: Option<u32>,
}

impl StageResultKey {
    pub fn new(query_id: String, stage_id: u32) -> Self {
        Self {
            query_id,
            stage_id,
            partition: None,
        }
    }

    pub fn with_partition(query_id: String, stage_id: u32, partition: u32) -> Self {
        Self {
            query_id,
            stage_id,
            partition: Some(partition),
        }
    }

    /// Create key from a StageResultTicket.
    pub fn from_ticket(ticket: &StageResultTicket) -> Self {
        Self {
            query_id: ticket.query_id.clone(),
            stage_id: ticket.stage_id,
            partition: ticket.partition,
        }
    }
}

/// Cached result of a stage execution.
#[derive(Debug)]
pub struct CachedStageResult {
    /// Output batches from the stage.
    pub batches: Vec<RecordBatch>,
    /// Execution statistics.
    pub stats: CachedStageStats,
    /// When the result was created.
    pub created_at: Instant,
}

impl CachedStageResult {
    /// Create a new cached stage result.
    pub fn new(batches: Vec<RecordBatch>, stats: CachedStageStats) -> Self {
        Self {
            batches,
            stats,
            created_at: Instant::now(),
        }
    }
}

/// Statistics for a cached stage result.
#[derive(Clone, Debug, Default)]
pub struct CachedStageStats {
    pub rows_produced: u64,
    pub bytes_produced: u64,
    pub execution_time_ms: u64,
}

/// In-memory store for stage execution results.
///
/// Results are cached here after `submit_stage` and retrieved via `do_get`.
/// TODO: Add TTL-based expiration for stale results.
#[derive(Default)]
pub struct StageResultStore {
    results: RwLock<HashMap<StageResultKey, CachedStageResult>>,
}

impl StageResultStore {
    pub fn new() -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a cached stage result.
    pub async fn insert(&self, key: StageResultKey, result: CachedStageResult) {
        self.results.write().await.insert(key, result);
    }

    /// Get a reference to a cached stage result (doesn't remove it).
    pub async fn get(&self, key: &StageResultKey) -> Option<CachedStageStats> {
        self.results.read().await.get(key).map(|r| r.stats.clone())
    }

    /// Store a stage result.
    pub async fn store(
        &self,
        key: StageResultKey,
        batches: Vec<RecordBatch>,
        stats: CachedStageStats,
    ) {
        let result = CachedStageResult::new(batches, stats);
        self.results.write().await.insert(key, result);
    }

    /// Retrieve and remove a stage result.
    pub async fn take(&self, key: &StageResultKey) -> Option<CachedStageResult> {
        self.results.write().await.remove(key)
    }

    /// Check if a result exists.
    pub async fn contains(&self, key: &StageResultKey) -> bool {
        self.results.read().await.contains_key(key)
    }

    /// Get the number of cached results.
    pub async fn len(&self) -> usize {
        self.results.read().await.len()
    }
}

/// Arrow Flight query service that provides SQL query execution over Arrow Flight protocol.
///
/// Also handles LDP stage execution via DoAction `submit_stage`.
///
/// Generic over `Q: QueryEngine` to enable:
/// - Zero-cost abstraction (static dispatch)
/// - Easy testing with mock implementations
/// - Type safety without runtime overhead
///
/// This follows Rust best practices by using generics instead of trait objects,
/// since `QueryEngine` uses RPITIT (Return Position Impl Trait In Traits) which
/// is not object-safe.
pub struct WorkerFlightService<Q>
where
    Q: QueryEngine,
{
    query_engine: Arc<Q>,
    sql_transformer: Arc<SqlTransformer>,
    /// Tenant ID bound at startup (shared-nothing architecture)
    tenant_id: String,
    /// Cache for stage execution results (for LDP DoGet retrieval)
    stage_results: Arc<StageResultStore>,
}

impl<Q> WorkerFlightService<Q>
where
    Q: QueryEngine,
{
    /// Create a new FlightQueryService.
    ///
    /// # Arguments
    /// * `query_engine` - The query engine to execute SQL queries
    /// * `sql_transformer` - Transforms logical tables to macro calls
    /// * `tenant_id` - Tenant ID for this worker (validated on each request)
    pub fn new(
        query_engine: Arc<Q>,
        sql_transformer: Arc<SqlTransformer>,
        tenant_id: String,
    ) -> Self {
        Self {
            query_engine,
            sql_transformer,
            tenant_id,
            stage_results: Arc::new(StageResultStore::new()),
        }
    }

    /// Validate that the request tenant matches this worker's tenant.
    fn validate_tenant(&self, request_tenant: &str) -> Result<(), Status> {
        if request_tenant != self.tenant_id {
            return Err(Status::permission_denied(format!(
                "Tenant ID mismatch: expected '{}', got '{}'",
                self.tenant_id, request_tenant
            )));
        }
        Ok(())
    }

    /// Handle the `submit_stage` DoAction.
    ///
    /// This method:
    /// 1. Deserializes the SubmitStageRequest from the action body
    /// 2. Validates tenant ID
    /// 3. Deserializes and registers exchange inputs as temporary tables
    /// 4. Executes the SQL stage via DuckDB
    /// 5. Stores results in the stage result cache
    /// 6. Returns a serialized SubmitStageResponse with a ticket
    async fn handle_submit_stage(
        &self,
        body: &[u8],
    ) -> Result<Response<BoxStream<'static, Result<arrow_flight::Result, Status>>>, Status> {
        use worker_storage::ldp::{
            deserialize_exchange_inputs, deserialize_submit_stage_request,
            serialize_submit_stage_response,
        };

        // Deserialize the request
        let request = deserialize_submit_stage_request(body).map_err(|e| {
            error!(error = %e, "Failed to deserialize SubmitStageRequest");
            Status::invalid_argument(format!("Invalid SubmitStageRequest: {}", e))
        })?;

        info!(
            tenant_id = %request.tenant_id,
            query_id = %request.query_id,
            stage_id = request.stage_id,
            stage_sql_len = request.stage_sql.len(),
            exchange_inputs = request.exchange_inputs.len(),
            "Received submit_stage request"
        );

        // Validate tenant
        self.validate_tenant(&request.tenant_id)?;

        // Deserialize exchange inputs from Arrow IPC streams
        let exchange_input_batches = deserialize_exchange_inputs(&request.exchange_inputs)
            .map_err(|e| {
                error!(error = %e, "Failed to deserialize exchange inputs");
                Status::internal(format!("Failed to deserialize exchange inputs: {}", e))
            })?;

        info!(
            query_id = %request.query_id,
            stage_id = request.stage_id,
            exchange_tables = exchange_input_batches.len(),
            total_input_rows = exchange_input_batches.values()
                .flat_map(|batches| batches.iter().map(|b| b.num_rows()))
                .sum::<usize>(),
            "Deserialized exchange inputs"
        );

        // Execute the stage with DuckDB (with timeout)
        let start_time = Instant::now();
        
        let batches = if request.stage_sql.is_empty() {
            // Empty plan (placeholder for testing)
            warn!(
                query_id = %request.query_id,
                stage_id = request.stage_id,
                "Empty stage SQL, returning empty result"
            );
            Vec::new()
        } else {
            // Execute with timeout
            let timeout_ms = request.limits.as_ref()
                .map(|l| l.timeout_ms)
                .unwrap_or(300_000); // Default 5 minutes
            
            let execution_future = self.execute_stage_with_duckdb(
                &request.query_id,
                request.stage_id,
                &request.stage_sql,
                &exchange_input_batches,
            );

            match tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                execution_future
            ).await {
                Ok(result) => result.map_err(|e| {
                    error!(
                        query_id = %request.query_id,
                        stage_id = request.stage_id,
                        error = %e,
                        "Stage execution failed"
                    );
                    Status::internal(format!("Stage execution failed: {}", e))
                })?,
                Err(_) => {
                    error!(
                        query_id = %request.query_id,
                        stage_id = request.stage_id,
                        timeout_ms = timeout_ms,
                        "Stage execution timed out"
                    );
                    return Err(Status::deadline_exceeded(format!(
                        "Stage execution timed out after {}ms",
                        timeout_ms
                    )));
                }
            }
        };

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        
        // Calculate stats
        let rows_produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes_produced: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        // Check output limits
        if let Some(limits) = &request.limits {
            if rows_produced > limits.max_rows_output {
                error!(
                    query_id = %request.query_id,
                    stage_id = request.stage_id,
                    rows = rows_produced,
                    limit = limits.max_rows_output,
                    "Stage exceeded row output limit"
                );
                return Err(Status::resource_exhausted(format!(
                    "Stage exceeded row limit: {} > {}",
                    rows_produced, limits.max_rows_output
                )));
            }

            if bytes_produced > limits.max_bytes_output {
                error!(
                    query_id = %request.query_id,
                    stage_id = request.stage_id,
                    bytes = bytes_produced,
                    limit = limits.max_bytes_output,
                    "Stage exceeded byte output limit"
                );
                return Err(Status::resource_exhausted(format!(
                    "Stage exceeded byte limit: {} > {}",
                    bytes_produced, limits.max_bytes_output
                )));
            }
        }

        let stats = CachedStageStats {
            rows_produced,
            bytes_produced,
            execution_time_ms,
        };

        info!(
            query_id = %request.query_id,
            stage_id = request.stage_id,
            rows = rows_produced,
            bytes = bytes_produced,
            time_ms = execution_time_ms,
            "Stage execution completed"
        );

        // Store result in cache
        let key = StageResultKey::new(request.query_id.clone(), request.stage_id);
        self.stage_results.store(key, batches, stats.clone()).await;

        // Create ticket for retrieving results
        let result_ticket = StageResultTicket::new(
            request.tenant_id.clone(),
            request.query_id.clone(),
            request.stage_id,
        );
        let ticket_bytes = result_ticket
            .encode()
            .map_err(|e| Status::internal(format!("Failed to encode result ticket: {}", e)))?
            .ticket
            .to_vec();

        // Serialize response
        let response_bytes = serialize_submit_stage_response(
            true,
            "",
            &ticket_bytes,
            Some((stats.rows_produced, stats.bytes_produced, stats.execution_time_ms)),
        )
        .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?;

        let result = arrow_flight::Result {
            body: response_bytes.into(),
        };
        let stream = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// Handle cancel_query action to cancel a running query.
    async fn handle_cancel_query(
        &self,
        body: &[u8],
    ) -> Result<Response<BoxStream<'static, Result<arrow_flight::Result, Status>>>, Status> {
        use serde_json;

        // Parse the cancel query request
        #[derive(serde::Deserialize)]
        struct CancelQueryRequest {
            query_id: String,
        }

        let request: CancelQueryRequest = serde_json::from_slice(body).map_err(|e| {
            error!(error = %e, "Failed to parse cancel query request");
            Status::invalid_argument(format!("Invalid cancel query request: {}", e))
        })?;

        info!(
            query_id = %request.query_id,
            "Received cancel_query request"
        );

        // Validate tenant by checking if the query exists in our results store
        // For now, we just log that cancellation was requested
        warn!(
            query_id = %request.query_id,
            "Query cancellation requested (not yet fully implemented)"
        );

        // In a real implementation, we would:
        // 1. Find all stage results for this query in the store
        // 2. Cancel any ongoing executions
        // 3. Remove cached results
        
        // For now, just remove any cached results for this query
        let mut results = self.stage_results.results.write().await;
        results.retain(|key, _| key.query_id != request.query_id);
        drop(results);

        // Create response
        let response_body = format!("Query {} cancelled", request.query_id);
        let result = arrow_flight::Result {
            body: response_body.into_bytes().into(),
        };
        let stream = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// Execute a stage's SQL with DuckDB and exchange inputs.
    ///
    /// This method runs in a blocking task to avoid blocking the async runtime.
    async fn execute_stage_with_duckdb(
        &self,
        query_id: &str,
        stage_id: u32,
        stage_sql: &str,
        exchange_inputs: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, String> {
        use duckdb::Connection;
        use worker_storage::engine::duckdb::arrow_utils::{
            drop_temp_table, register_arrow_batches,
        };

        // Clone data for move into blocking task
        let stage_sql = stage_sql.to_string();
        let exchange_inputs = exchange_inputs.clone();
        let query_id = query_id.to_string();

        // Execute in blocking task (DuckDB operations are synchronous)
        tokio::task::spawn_blocking(move || {
            // Create DuckDB connection
            let conn = Connection::open_in_memory()
                .map_err(|e| format!("Failed to create DuckDB connection: {}", e))?;

            debug!(
                query_id = %query_id,
                stage_id = stage_id,
                "Created DuckDB connection for stage execution"
            );

            // Register exchange inputs as temporary tables
            let mut registered_tables = Vec::new();
            for (table_name, batches) in &exchange_inputs {
                if batches.is_empty() {
                    debug!(
                        query_id = %query_id,
                        stage_id = stage_id,
                        table = %table_name,
                        "Skipping empty exchange input"
                    );
                    continue;
                }

                register_arrow_batches(&conn, table_name, batches).map_err(|e| {
                    format!("Failed to register table '{}': {}", table_name, e)
                })?;

                registered_tables.push(table_name.clone());

                debug!(
                    query_id = %query_id,
                    stage_id = stage_id,
                    table = %table_name,
                    rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    "Registered exchange input table"
                );
            }

            // Execute SQL stage
            debug!(
                query_id = %query_id,
                stage_id = stage_id,
                registered_tables = ?registered_tables,
                "Executing stage SQL"
            );

            let result = {
                let mut stmt = conn.prepare(&stage_sql)
                    .map_err(|e| format!("Failed to prepare SQL: {}", e))?;
                let rows = stmt.query_arrow([])
                    .map_err(|e| format!("Failed to execute SQL: {}", e))?;
                Ok::<Vec<RecordBatch>, String>(rows.collect())
            };

            // Clean up registered tables
            for table_name in &registered_tables {
                let _ = drop_temp_table(&conn, table_name);
            }

            match result {
                Ok(batches) => {
                    debug!(
                        query_id = %query_id,
                        stage_id = stage_id,
                        output_batches = batches.len(),
                        output_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                        "Stage SQL execution successful"
                    );
                    Ok(batches)
                }
                Err(e) => Err(format!("Stage SQL execution failed: {}", e)),
            }
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
    }

    /// Handle SQL query execution via DoGet.
    async fn handle_query_do_get(
        &self,
        query_ticket: QueryTicket,
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        debug!(sql = %query_ticket.sql, tenant = %query_ticket.tenant_id, "Handling query DoGet");

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&query_ticket.sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %query_ticket.sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed for execution"
        );

        // Execute the transformed query
        let timeout = query_ticket.timeout_secs.map(Duration::from_secs);
        let result_stream = self
            .query_engine
            .execute_query(
                &transform_result.transformed_sql,
                query_ticket.memory_limit_mb,
                timeout,
            )
            .await
            .map_err(map_query_error_to_status)?;

        // Convert QueryResultStream to FlightData stream
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(result_stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    /// Handle LDP stage result retrieval via DoGet.
    async fn handle_stage_result_do_get(
        &self,
        stage_ticket: StageResultTicket,
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        info!(
            query_id = %stage_ticket.query_id,
            stage_id = stage_ticket.stage_id,
            partition = ?stage_ticket.partition,
            "Handling stage result DoGet"
        );

        // Look up the cached stage result
        let key = StageResultKey::from_ticket(&stage_ticket);
        let cached_result = self.stage_results.take(&key).await.ok_or_else(|| {
            warn!(
                query_id = %stage_ticket.query_id,
                stage_id = stage_ticket.stage_id,
                "Stage result not found in cache"
            );
            Status::not_found(format!(
                "Stage result not found: query={}, stage={}, partition={:?}",
                stage_ticket.query_id, stage_ticket.stage_id, stage_ticket.partition
            ))
        })?;

        info!(
            query_id = %stage_ticket.query_id,
            stage_id = stage_ticket.stage_id,
            batches = cached_result.batches.len(),
            rows = cached_result.stats.rows_produced,
            "Retrieved stage result from cache"
        );

        // Convert batches to FlightData stream
        let batches = cached_result.batches;
        
        if batches.is_empty() {
            // Return empty stream with proper type
            let empty_stream: BoxStream<'static, Result<FlightData, Status>> = 
                Box::pin(stream::empty());
            return Ok(Response::new(empty_stream));
        }

        // Create stream from batches
        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }
}

#[tonic::async_trait]
impl<Q> FlightService for WorkerFlightService<Q>
where
    Q: QueryEngine + 'static,
{
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    /// Handshake is not required for this service.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake is not implemented"))
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    /// List available logical datasets.
    ///
    /// Returns FlightInfo for each registered logical dataset.
    #[instrument(skip(self, _request), level = "debug")]
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        debug!("list_flights called");

        // Get registered logical dataset IDs from the transformer
        let dataset_ids: Vec<String> = self
            .sql_transformer
            .registered_dataset_ids()
            .into_iter()
            .collect();

        // Create FlightInfo for each dataset
        let flight_infos: Vec<Result<FlightInfo, Status>> = dataset_ids
            .into_iter()
            .map(|dataset_id| {
                Ok(FlightInfo::new().with_descriptor(FlightDescriptor::new_path(vec![dataset_id])))
            })
            .collect();

        let stream = stream::iter(flight_infos);
        Ok(Response::new(Box::pin(stream)))
    }
    /// Get flight info for a SQL query.
    ///
    /// The FlightDescriptor.cmd contains the SQL query against logical table names.
    /// Returns schema and endpoint information for subsequent DoGet calls.
    #[instrument(skip(self, request), level = "debug")]
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        debug!(?descriptor, "get_flight_info called");

        // Extract SQL from command
        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid SQL encoding: {}", e)))?;

        if sql.is_empty() {
            return Err(Status::invalid_argument("Empty SQL query"));
        }

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed"
        );

        // Extract schema from the first logical table that was replaced
        let schema = if !transform_result.tables_replaced.is_empty() {
            let first_table = &transform_result.tables_replaced[0];
            debug!(table = %first_table, "Extracting schema from first logical table");
            
            self.query_engine
                .get_table_schema(first_table)
                .await
                .map_err(map_query_error_to_status)?
        } else {
            // No logical tables found in the query
            // This could happen for queries like "SELECT 1" or queries against non-registered tables
            return Err(Status::invalid_argument(
                "Cannot determine schema: query does not reference any registered logical tables. \
                 Use get_schema with a query containing logical table references."
            ));
        };

        debug!(fields = schema.fields().len(), "Schema extracted successfully");

        // Create a ticket with the original SQL (transformation happens at DoGet time)
        let ticket = QueryTicket::new(sql.clone(), self.tenant_id.clone());
        let encoded_ticket = ticket
            .encode()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Build FlightInfo with endpoint and schema
        let endpoint = FlightEndpoint::new().with_ticket(encoded_ticket);

        let flight_info = FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to set schema in FlightInfo: {}", e)))?;

        Ok(Response::new(flight_info))
    }
    /// Poll flight info is not supported.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }
    /// Get schema for a query (delegates to get_flight_info).
    #[instrument(skip(self, request), level = "debug")]
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        debug!(?descriptor, "get_schema called");

        // Extract SQL from command
        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid SQL encoding: {}", e)))?;

        if sql.is_empty() {
            return Err(Status::invalid_argument("Empty SQL query"));
        }

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed for get_schema"
        );

        // Extract schema from the first logical table
        let schema = if !transform_result.tables_replaced.is_empty() {
            let first_table = &transform_result.tables_replaced[0];
            debug!(table = %first_table, "Extracting schema from first logical table");
            
            self.query_engine
                .get_table_schema(first_table)
                .await
                .map_err(map_query_error_to_status)?
        } else {
            // No logical tables found in the query
            return Err(Status::invalid_argument(
                "Cannot determine schema: query does not reference any registered logical tables"
            ));
        };

        debug!(fields = schema.fields().len(), "Schema extracted successfully");

        // Encode schema to Arrow IPC format
        let schema_bytes = {
            let mut buf = Vec::new();
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &schema)
                .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?;
            writer.finish()
                .map_err(|e| Status::internal(format!("Failed to finish schema encoding: {}", e)))?;
            buf
        };

        Ok(Response::new(SchemaResult {
            schema: schema_bytes.into(),
        }))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Execute a query or retrieve stage results as FlightData stream.
    ///
    /// The Ticket can be:
    /// - `QueryTicket`: SQL query execution (legacy format also supported)
    /// - `StageResultTicket`: LDP stage output retrieval
    #[instrument(skip(self, request), level = "debug")]
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        debug!("do_get called");

        // Decode the ticket using unified FlightTicket
        let flight_ticket = FlightTicket::decode(&ticket)?;

        // Validate tenant
        self.validate_tenant(flight_ticket.tenant_id())?;

        match flight_ticket {
            FlightTicket::Query(query_ticket) => {
                self.handle_query_do_get(query_ticket).await
            }
            FlightTicket::StageResult(stage_ticket) => {
                self.handle_stage_result_do_get(stage_ticket).await
            }
        }
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    /// DoPut is not supported (read-only query service).
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "DoPut is not supported; this is a read-only query service",
        ))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// DoExchange is not supported.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    /// Execute custom actions.
    ///
    /// Supported actions:
    /// - `health_check`: Returns service health status
    /// - `submit_stage`: Execute an LDP stage and return a ticket for results
    /// - `cancel_query`: Cancel a running query (TODO)
    #[instrument(skip(self, request), level = "debug")]
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        debug!(action_type = %action.r#type, "do_action called");

        match action.r#type.as_str() {
            "health_check" => {
                let result = arrow_flight::Result {
                    body: b"OK".to_vec().into(),
                };
                let stream = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "submit_stage" => {
                self.handle_submit_stage(&action.body).await
            }
            "cancel_query" => {
                // TODO: Implement query cancellation
                self.handle_cancel_query(&action.body).await
            }
            _ => {
                warn!(action_type = %action.r#type, "Unknown action type");
                Err(Status::invalid_argument(format!(
                    "Unknown action type: {}",
                    action.r#type
                )))
            }
        }
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    /// List available actions.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "health_check".to_string(),
                description: "Check service health".to_string(),
            }),
            Ok(ActionType {
                r#type: "submit_stage".to_string(),
                description: "Submit an LDP stage for execution (body: SubmitStageRequest proto)".to_string(),
            }),
            Ok(ActionType {
                r#type: "cancel_query".to_string(),
                description: "Cancel a running query (not yet implemented)".to_string(),
            }),
        ];

        let stream = stream::iter(actions);
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ticket::{FlightTicket, StageResultTicket};
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_stage_result_store_insert_and_get() {
        let store = StageResultStore::new();
        let key = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let result = CachedStageResult::new(
            vec![batch],
            CachedStageStats {
                rows_produced: 3,
                bytes_produced: 12,
                execution_time_ms: 10,
            },
        );

        store.insert(key.clone(), result).await;

        // Verify it exists
        assert!(store.get(&key).await.is_some());

        // Take it out
        let taken = store.take(&key).await;
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().stats.rows_produced, 3);

        // Should be gone now
        assert!(store.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_stage_result_key_from_ticket() {
        let ticket = StageResultTicket {
            tenant_id: "t1".to_string(),
            query_id: "q1".to_string(),
            stage_id: 5,
            partition: Some(2),
        };

        let key = StageResultKey::from_ticket(&ticket);
        assert_eq!(key.query_id, "q1");
        assert_eq!(key.stage_id, 5);
        assert_eq!(key.partition, Some(2));
    }

    #[tokio::test]
    async fn test_stage_result_store_with_partition() {
        let store = StageResultStore::new();
        
        // Insert results for different partitions
        let key_p0 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: Some(0),
        };
        let key_p1 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: Some(1),
        };

        let result0 = CachedStageResult::new(
            vec![],
            CachedStageStats {
                rows_produced: 10,
                bytes_produced: 100,
                execution_time_ms: 5,
            },
        );
        let result1 = CachedStageResult::new(
            vec![],
            CachedStageStats {
                rows_produced: 20,
                bytes_produced: 200,
                execution_time_ms: 8,
            },
        );

        store.insert(key_p0.clone(), result0).await;
        store.insert(key_p1.clone(), result1).await;

        // Both should exist independently
        let r0 = store.take(&key_p0).await.unwrap();
        let r1 = store.take(&key_p1).await.unwrap();

        assert_eq!(r0.stats.rows_produced, 10);
        assert_eq!(r1.stats.rows_produced, 20);
    }

    #[tokio::test]
    async fn test_flight_ticket_decoding() {
        // Test that FlightTicket can decode both query and stage result tickets
        let query_ticket = FlightTicket::Query(QueryTicket::new(
            "SELECT 1".to_string(),
            "test-tenant".to_string(),
        ));

        let stage_ticket = FlightTicket::StageResult(StageResultTicket {
            tenant_id: "test-tenant".to_string(),
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: None,
        });

        // Verify encoding/decoding works
        let query_bytes = serde_json::to_vec(&query_ticket).unwrap();
        let decoded: FlightTicket = serde_json::from_slice(&query_bytes).unwrap();
        assert_eq!(decoded.tenant_id(), "test-tenant");

        let stage_bytes = serde_json::to_vec(&stage_ticket).unwrap();
        let decoded: FlightTicket = serde_json::from_slice(&stage_bytes).unwrap();
        assert_eq!(decoded.tenant_id(), "test-tenant");
    }

    #[test]
    fn test_cached_stage_result_creation() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let stats = CachedStageStats {
            rows_produced: 5,
            bytes_produced: 20,
            execution_time_ms: 15,
        };

        let result = CachedStageResult::new(vec![batch], stats);
        
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.stats.rows_produced, 5);
        assert_eq!(result.stats.bytes_produced, 20);
        assert_eq!(result.stats.execution_time_ms, 15);
    }

    #[test]
    fn test_stage_result_key_equality() {
        let key1 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: None,
        };
        let key2 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: None,
        };
        let key3 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 2,
            partition: None,
        };

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_stage_result_key_hash() {
        use std::collections::HashMap;

        let key1 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: Some(0),
        };
        let key2 = StageResultKey {
            query_id: "q1".to_string(),
            stage_id: 1,
            partition: Some(0),
        };

        let mut map = HashMap::new();
        map.insert(key1, "value1");
        
        // key2 should be able to retrieve the value since it equals key1
        assert_eq!(map.get(&key2), Some(&"value1"));
    }
}
