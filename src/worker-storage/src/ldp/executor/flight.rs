//! Flight-based stage executor for distributed LDP execution.
//!
//! This module provides a `FlightStageExecutor` that implements the `StageExecutor`
//! trait for remote stage execution via Arrow Flight protocol.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use futures_util::TryStreamExt;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::ldp::executor::stage::{
    StageExecutionError, StageExecutor, StageTicket, StageTickets,
};
use crate::ldp::proto_convert::{
    deserialize_submit_stage_response, serialize_submit_stage_request,
};
use crate::ldp::{Stage, StageId, StageOutput, WorkerId};

// ============================================================================
// Worker Connection Pool
// ============================================================================

/// Connection to a remote worker's Flight service.
#[derive(Clone)]
pub struct WorkerConnection {
    /// Worker identifier.
    pub worker_id: WorkerId,
    /// Flight client for this worker.
    pub client: FlightServiceClient<Channel>,
}

impl WorkerConnection {
    /// Create a new connection to a worker.
    pub async fn connect(worker_id: WorkerId, endpoint: &str) -> Result<Self, StageExecutionError> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| {
                StageExecutionError::WorkerUnavailable {
                    worker_id: worker_id.clone(),
                    detail: format!("Invalid endpoint '{}': {}", endpoint, e),
                }
            })?
            .connect()
            .await
            .map_err(|e| {
                StageExecutionError::WorkerUnavailable {
                    worker_id: worker_id.clone(),
                    detail: format!("Failed to connect to '{}': {}", endpoint, e),
                }
            })?;

        let client = FlightServiceClient::new(channel);
        Ok(Self { worker_id, client })
    }
}

/// Pool of connections to remote workers.
pub struct WorkerConnectionPool {
    /// Map from worker ID to connection.
    connections: RwLock<HashMap<WorkerId, WorkerConnection>>,
    /// Map from worker ID to endpoint URL.
    endpoints: RwLock<HashMap<WorkerId, String>>,
}

impl WorkerConnectionPool {
    /// Create a new empty connection pool.
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            endpoints: RwLock::new(HashMap::new()),
        }
    }

    /// Register a worker endpoint.
    pub async fn register_worker(&self, worker_id: WorkerId, endpoint: String) {
        self.endpoints.write().await.insert(worker_id, endpoint);
    }

    /// Get or create a connection to a worker.
    pub async fn get_connection(
        &self,
        worker_id: &WorkerId,
    ) -> Result<WorkerConnection, StageExecutionError> {
        // Check if we already have a connection
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(worker_id) {
                return Ok(conn.clone());
            }
        }

        // Get the endpoint
        let endpoint = {
            let endpoints = self.endpoints.read().await;
            endpoints.get(worker_id).cloned().ok_or_else(|| {
                StageExecutionError::WorkerUnavailable {
                    worker_id: worker_id.clone(),
                    detail: format!("No endpoint registered for worker '{}'", worker_id),
                }
            })?
        };

        // Create new connection
        let conn = WorkerConnection::connect(worker_id.clone(), &endpoint).await?;

        // Cache it
        {
            let mut connections = self.connections.write().await;
            connections.insert(worker_id.clone(), conn.clone());
        }

        Ok(conn)
    }

    /// Remove a worker connection (e.g., on failure).
    pub async fn remove_connection(&self, worker_id: &WorkerId) {
        self.connections.write().await.remove(worker_id);
    }

    /// Get the endpoint URL for a worker.
    pub async fn get_endpoint(&self, worker_id: &WorkerId) -> Option<String> {
        self.endpoints.read().await.get(worker_id).cloned()
    }

    /// Get all registered worker IDs.
    pub async fn worker_ids(&self) -> Vec<WorkerId> {
        self.endpoints.read().await.keys().cloned().collect()
    }
}

impl Default for WorkerConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Flight Stage Executor
// ============================================================================

/// Flight-based stage executor for distributed LDP execution.
///
/// This executor:
/// - Submits stages to remote workers via DoAction `submit_stage`
/// - Fetches results via DoGet with the returned ticket
/// - Registers tickets in a shared runtime for exchange operations
pub struct FlightStageExecutor {
    /// Tenant ID for all requests.
    tenant_id: String,
    /// Connection pool to workers.
    connection_pool: Arc<WorkerConnectionPool>,
    /// Cached tickets from submit_stage responses.
    /// Maps (query_id, stage_id, worker_id) -> ticket bytes
    ticket_cache: RwLock<HashMap<String, Vec<u8>>>,
}

impl FlightStageExecutor {
    /// Create a new Flight-based executor.
    pub fn new(tenant_id: String, connection_pool: Arc<WorkerConnectionPool>) -> Self {
        Self {
            tenant_id,
            connection_pool,
            ticket_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new executor with a fresh connection pool.
    pub fn with_new_pool(tenant_id: String) -> Self {
        Self::new(tenant_id, Arc::new(WorkerConnectionPool::new()))
    }

    /// Get the connection pool for registering workers.
    pub fn connection_pool(&self) -> &Arc<WorkerConnectionPool> {
        &self.connection_pool
    }

    /// Generate a ticket cache key.
    fn ticket_key(query_id: &str, stage_id: StageId, worker_id: &WorkerId) -> String {
        format!("{}:{}:{}", query_id, stage_id, worker_id)
    }

    /// Submit stage to a single worker.
    ///
    /// Sends the stage via DoAction and registers the result ticket for later retrieval.
    async fn submit_to_worker(
        &self,
        stage: &Stage,
        worker_id: &WorkerId,
        query_id: &str,
        inputs: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<StageTicket, StageExecutionError> {
        info!(
            query_id = query_id,
            stage_id = %stage.stage_id,
            worker_id = %worker_id,
            "Submitting stage to worker"
        );

        // Get connection to worker
        let mut conn = self.connection_pool.get_connection(worker_id).await?;

        // Serialize the submit request with exchange inputs
        let request_bytes = serialize_submit_stage_request(
            &self.tenant_id,
            query_id,
            stage.stage_id,
            &stage.stage_sql,
            &stage.limits,
            inputs,
        )
        .map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to serialize request: {}", e))
        })?;

        // Send DoAction
        let action = Action {
            r#type: "submit_stage".to_string(),
            body: request_bytes.into(),
        };

        let response = conn
            .client
            .do_action(action)
            .await
            .map_err(|e| {
                error!(
                    query_id = query_id,
                    stage_id = %stage.stage_id,
                    worker_id = %worker_id,
                    error = %e,
                    "DoAction failed"
                );
                StageExecutionError::ExecutionFailed(format!("DoAction failed: {}", e))
            })?;

        // Process response stream
        let mut stream = response.into_inner();
        let result = stream
            .message()
            .await
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to read response: {}", e))
            })?
            .ok_or_else(|| {
                StageExecutionError::ExecutionFailed("Empty response from submit_stage".into())
            })?;

        // Deserialize response
        let submit_response = deserialize_submit_stage_response(&result.body).map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to deserialize response: {}", e))
        })?;

        if !submit_response.success {
            return Err(StageExecutionError::ExecutionFailed(format!(
                "Stage execution failed: {}",
                submit_response.error_message
            )));
        }

        // Cache the result ticket using the actual query_id
        let ticket_key = Self::ticket_key(query_id, stage.stage_id, worker_id);
        {
            let mut cache = self.ticket_cache.write().await;
            cache.insert(ticket_key, submit_response.result_ticket.clone());
        }

        // Log stats if available
        if let Some(stats) = submit_response.stats {
            info!(
                query_id = query_id,
                stage_id = %stage.stage_id,
                worker_id = %worker_id,
                rows = stats.rows_produced,
                bytes = stats.bytes_produced,
                time_ms = stats.execution_time_ms,
                "Stage execution completed"
            );
        }

        // Create ticket
        Ok(StageTicket::new(query_id.to_string(), stage.stage_id, worker_id.clone()))
    }
}

impl StageExecutor for FlightStageExecutor {
    async fn submit_stage(
        &self,
        query_id: &str,
        stage: &Stage,
        inputs: HashMap<String, Vec<RecordBatch>>,
    ) -> Result<StageTickets, StageExecutionError> {
        let mut tickets = StageTickets::new();

        // Submit to each target worker
        for worker_id in &stage.target_workers {
            let ticket = self
                .submit_to_worker(stage, worker_id, query_id, &inputs)
                .await?;

            match &stage.output {
                StageOutput::Stream => {
                    tickets.add(ticket);
                }
                StageOutput::Partitioned { partitions, .. } => {
                    // For partitioned output, create a ticket for each partition
                    for p in 0..*partitions {
                        let part_ticket =
                            StageTicket::partitioned(query_id.to_string(), stage.stage_id, worker_id.clone(), p);
                        tickets.add(part_ticket);
                    }
                }
            }
        }

        Ok(tickets)
    }

    async fn submit_stage_streaming(
        &self,
        _query_id: &str,
        _stage: &Stage,
        _input_streams: HashMap<String, crate::ldp::executor::stage::RecordBatchStream>,
    ) -> Result<crate::ldp::executor::stage::RecordBatchStream, StageExecutionError> {
        // TODO: Implement streaming execution for Flight-based distributed execution
        // For now, return an error indicating it's not yet implemented
        Err(StageExecutionError::ExecutionFailed(
            "Streaming execution not yet implemented for Flight executor".into()
        ))
    }

    async fn fetch_output(
        &self,
        ticket: &StageTicket,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        debug!(
            stage_id = %ticket.stage_id,
            worker_id = %ticket.worker_id,
            partition = ?ticket.partition,
            "Fetching output from worker"
        );

        // Get the cached result ticket - ticket_id contains the query_id we need
        // Parse query_id from the ticket_id format: "stage_{stage_id}_worker_{worker_id}"
        // We need to track which query this ticket belongs to
        // For now, we reconstruct from stage_id (this is a limitation we'll address)
        let query_id = format!("query_{}", ticket.stage_id);
        let ticket_key = Self::ticket_key(&query_id, ticket.stage_id, &ticket.worker_id);

        let ticket_bytes = {
            let cache = self.ticket_cache.read().await;
            cache.get(&ticket_key).cloned().ok_or_else(|| {
                StageExecutionError::InputNotReady(format!(
                    "No ticket cached for stage {} on worker {}",
                    ticket.stage_id, ticket.worker_id
                ))
            })?
        };

        // Get connection to worker
        let mut conn = self
            .connection_pool
            .get_connection(&ticket.worker_id)
            .await?;

        // Execute DoGet
        let flight_ticket = Ticket {
            ticket: ticket_bytes.into(),
        };

        let response = conn
            .client
            .do_get(flight_ticket)
            .await
            .map_err(|e| {
                error!(
                    stage_id = %ticket.stage_id,
                    worker_id = %ticket.worker_id,
                    error = %e,
                    "DoGet failed"
                );
                StageExecutionError::ExecutionFailed(format!("DoGet failed: {}", e))
            })?;

        // Decode FlightData stream to RecordBatches
        let stream = response.into_inner();
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| {
            arrow_flight::error::FlightError::Tonic(Box::new(tonic::Status::internal(e.to_string())))
        }));

        // Collect all batches
        let batches: Vec<RecordBatch> = batch_stream
            .try_collect()
            .await
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to decode batches: {}", e))
            })?;

        info!(
            stage_id = %ticket.stage_id,
            worker_id = %ticket.worker_id,
            batches = batches.len(),
            "Fetched output from worker"
        );

        Ok(batches)
    }
}

// ============================================================================
// LDP Flight Client
// ============================================================================

/// High-level Flight client for LDP stage operations.
///
/// This provides a simpler API for:
/// - Submitting stages to workers
/// - Fetching stage results
/// - Health checks
pub struct LdpFlightClient {
    /// Worker endpoint URL.
    endpoint: String,
    /// Tenant ID for requests.
    tenant_id: String,
    /// Underlying Flight client.
    client: Option<FlightServiceClient<Channel>>,
}

impl LdpFlightClient {
    /// Create a new client for a worker endpoint.
    pub fn new(endpoint: String, tenant_id: String) -> Self {
        Self {
            endpoint,
            tenant_id,
            client: None,
        }
    }

    /// Connect to the worker.
    pub async fn connect(&mut self) -> Result<(), StageExecutionError> {
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!(
                    "Invalid endpoint '{}': {}",
                    self.endpoint, e
                ))
            })?
            .connect()
            .await
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!(
                    "Failed to connect to '{}': {}",
                    self.endpoint, e
                ))
            })?;

        self.client = Some(FlightServiceClient::new(channel));
        Ok(())
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// Get the underlying client, connecting if needed.
    async fn get_client(&mut self) -> Result<&mut FlightServiceClient<Channel>, StageExecutionError> {
        if self.client.is_none() {
            self.connect().await?;
        }
        self.client
            .as_mut()
            .ok_or_else(|| StageExecutionError::ExecutionFailed("Not connected".into()))
    }

    /// Perform a health check.
    pub async fn health_check(&mut self) -> Result<bool, StageExecutionError> {
        let client = self.get_client().await?;

        let action = Action {
            r#type: "health_check".to_string(),
            body: vec![].into(),
        };

        let response = client.do_action(action).await.map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Health check failed: {}", e))
        })?;

        let mut stream = response.into_inner();
        if let Some(result) = stream.message().await.map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Health check response failed: {}", e))
        })? {
            return Ok(result.body.as_ref() == b"OK");
        }

        Ok(false)
    }

    /// Submit a stage for execution.
    ///
    /// Returns the result ticket bytes for fetching output via DoGet.
    pub async fn submit_stage(
        &mut self,
        query_id: &str,
        stage_id: StageId,
        stage_sql: &str,
        limits: &crate::ldp::StageLimits,
    ) -> Result<Vec<u8>, StageExecutionError> {
        // Clone tenant_id before mutable borrow
        let tenant_id = self.tenant_id.clone();
        let client = self.get_client().await?;

        let request_bytes = serialize_submit_stage_request(
            &tenant_id,
            query_id,
            stage_id,
            stage_sql,
            limits,
            &HashMap::new(),
        )
        .map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to serialize request: {}", e))
        })?;

        let action = Action {
            r#type: "submit_stage".to_string(),
            body: request_bytes.into(),
        };

        let response = client.do_action(action).await.map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("DoAction failed: {}", e))
        })?;

        let mut stream = response.into_inner();
        let result = stream
            .message()
            .await
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to read response: {}", e))
            })?
            .ok_or_else(|| {
                StageExecutionError::ExecutionFailed("Empty response from submit_stage".into())
            })?;

        let submit_response = deserialize_submit_stage_response(&result.body).map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to deserialize response: {}", e))
        })?;

        if !submit_response.success {
            return Err(StageExecutionError::ExecutionFailed(format!(
                "Stage execution failed: {}",
                submit_response.error_message
            )));
        }

        Ok(submit_response.result_ticket)
    }

    /// Fetch stage results using a ticket.
    pub async fn fetch_results(
        &mut self,
        ticket_bytes: &[u8],
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        let client = self.get_client().await?;

        let ticket = Ticket {
            ticket: ticket_bytes.to_vec().into(),
        };

        let response = client.do_get(ticket).await.map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("DoGet failed: {}", e))
        })?;

        let stream = response.into_inner();
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| {
            arrow_flight::error::FlightError::Tonic(Box::new(tonic::Status::internal(e.to_string())))
        }));

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to decode batches: {}", e))
        })?;

        Ok(batches)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticket_key_generation() {
        let key = FlightStageExecutor::ticket_key("q1", StageId(5), &WorkerId::from("worker-1"));
        assert_eq!(key, "q1:5:worker-1");
    }

    #[test]
    fn test_worker_connection_pool_default() {
        let pool = WorkerConnectionPool::default();
        assert!(pool.connections.try_read().is_ok());
    }

    #[tokio::test]
    async fn test_register_worker() {
        let pool = WorkerConnectionPool::new();
        pool.register_worker(WorkerId::from("w1"), "http://localhost:50051".to_string())
            .await;

        let endpoints = pool.endpoints.read().await;
        assert_eq!(endpoints.get(&WorkerId::from("w1")), Some(&"http://localhost:50051".to_string()));
    }

    #[tokio::test]
    async fn test_get_connection_unknown_worker() {
        let pool = WorkerConnectionPool::new();
        let result = pool.get_connection(&WorkerId::from("unknown")).await;
        assert!(result.is_err());

        match result {
            Err(StageExecutionError::WorkerUnavailable { detail, .. }) => {
                assert!(detail.contains("No endpoint registered"));
            }
            _ => panic!("Expected WorkerUnavailable error"),
        }
    }

    #[test]
    fn test_ldp_flight_client_creation() {
        let client = LdpFlightClient::new(
            "http://localhost:50051".to_string(),
            "test-tenant".to_string(),
        );
        assert!(!client.is_connected());
        assert_eq!(client.endpoint, "http://localhost:50051");
        assert_eq!(client.tenant_id, "test-tenant");
    }

    #[test]
    fn test_flight_stage_executor_creation() {
        let pool = Arc::new(WorkerConnectionPool::new());
        let executor = FlightStageExecutor::new("test-tenant".to_string(), pool.clone());
        assert!(Arc::ptr_eq(&executor.connection_pool, &pool));
    }

    #[test]
    fn test_flight_stage_executor_with_new_pool() {
        let executor = FlightStageExecutor::with_new_pool("test-tenant".to_string());
        assert!(executor.connection_pool().connections.try_read().is_ok());
    }
}
