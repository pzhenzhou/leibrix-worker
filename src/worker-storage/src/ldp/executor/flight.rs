//! Flight-based stage executor for distributed LDP execution.
//!
//! This module provides a `FlightStageExecutor` that implements the `StageExecutor`
//! trait for remote stage execution via Arrow Flight protocol.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use dashmap::DashMap;
use futures_util::TryStreamExt;
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use crate::ldp::executor::stage::{StageExecutionError, StageExecutor, StageTicket, StageTickets};
use crate::ldp::proto_convert::{
    deserialize_submit_stage_response, serialize_submit_stage_request,
};
use crate::ldp::{Stage, StageId, StageOutput, WorkerId};

// ============================================================================
// Worker Connection Pool
// ============================================================================

/// Default TTL for cached Flight connections (5 minutes).
const CONNECTION_TTL_DEFAULT: Duration = Duration::from_secs(300);

/// Hard upper-bound on the number of cached connections.
const MAX_CONNECTIONS_CAP: usize = 100;

/// Configuration for [`WorkerConnectionPool`].
#[derive(Debug, Clone)]
pub struct WorkerConnectionPoolConfig {
    /// How long a cached connection remains valid before it is evicted.
    /// Default: 5 minutes.
    pub connection_ttl: Duration,
    /// Maximum number of cached connections.
    /// `0` means "use the number of registered workers, capped at
    /// [`MAX_CONNECTIONS_CAP`]".  Any non-zero value is capped at
    /// [`MAX_CONNECTIONS_CAP`].  Default: 0 (dynamic).
    pub max_connections: usize,
}

impl Default for WorkerConnectionPoolConfig {
    fn default() -> Self {
        Self {
            connection_ttl: CONNECTION_TTL_DEFAULT,
            max_connections: 0,
        }
    }
}

/// A timestamped, cached connection entry stored inside a per-worker mutex.
struct CachedEntry {
    conn: WorkerConnection,
    /// When this connection was established; used for TTL eviction.
    created_at: Instant,
}

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
            .map_err(|e| StageExecutionError::WorkerUnavailable {
                worker_id: worker_id.clone(),
                detail: format!("Invalid endpoint '{}': {}", endpoint, e),
            })?
            .connect()
            .await
            .map_err(|e| StageExecutionError::WorkerUnavailable {
                worker_id: worker_id.clone(),
                detail: format!("Failed to connect to '{}': {}", endpoint, e),
            })?;

        let client = FlightServiceClient::new(channel);
        Ok(Self { worker_id, client })
    }

    /// Perform a lightweight health check by sending a `health_check` DoAction.
    ///
    /// Returns `true` if the remote peer responded without a transport error.
    /// The response payload is intentionally ignored — any successful gRPC
    /// round-trip is sufficient proof that the channel is alive.
    pub async fn health_check(&mut self) -> bool {
        let action = Action {
            r#type: "health_check".to_string(),
            body: vec![].into(),
        };
        match self.client.do_action(action).await {
            Ok(response) => {
                // Drain the single-message response to release server resources.
                let mut stream = response.into_inner();
                let _ = stream.message().await;
                true
            }
            Err(_) => false,
        }
    }
}

/// Pool of connections to remote workers, with TTL eviction, per-worker
/// mutex-serialised connection setup, health checks, and a configurable cap.
pub struct WorkerConnectionPool {
    /// Per-worker mutex-guarded connection slot.
    ///
    /// Using `Arc<AsyncMutex<Option<CachedEntry>>>` per worker guarantees that
    /// at most one `connect()` call runs concurrently for any given worker,
    /// eliminating the double-connect race that plain `DashMap` check-then-act
    /// patterns suffer from.
    connections: DashMap<WorkerId, Arc<AsyncMutex<Option<CachedEntry>>>>,
    /// Map from worker ID to endpoint URL.
    endpoints: DashMap<WorkerId, String>,
    /// Pool configuration.
    config: WorkerConnectionPoolConfig,
}

impl WorkerConnectionPool {
    /// Create a new empty connection pool with default configuration.
    pub fn new() -> Self {
        Self::with_config(WorkerConnectionPoolConfig::default())
    }

    /// Create a new pool with the supplied configuration.
    pub fn with_config(config: WorkerConnectionPoolConfig) -> Self {
        Self {
            connections: DashMap::new(),
            endpoints: DashMap::new(),
            config,
        }
    }

    /// Register a worker endpoint.
    pub async fn register_worker(&self, worker_id: WorkerId, endpoint: String) {
        self.endpoints.insert(worker_id, endpoint);
    }

    /// Get or create a connection to a worker.
    ///
    /// # Guarantees
    ///
    /// * **No double-connect race**: the per-worker `AsyncMutex` ensures only
    ///   one caller at a time attempts to establish a new connection to any
    ///   given worker.
    /// * **TTL eviction**: connections older than `config.connection_ttl` are
    ///   evicted and re-established transparently.
    /// * **Health check**: before returning a cached connection, a lightweight
    ///   `health_check` DoAction is sent; unhealthy connections are evicted and
    ///   reconnected.
    /// * **Cap enforcement**: if the pool is at capacity, the oldest cached
    ///   connection (across all workers) is evicted first.
    pub async fn get_connection(
        &self,
        worker_id: &WorkerId,
    ) -> Result<WorkerConnection, StageExecutionError> {
        // Step 1 — Atomically obtain the per-worker slot.
        // `DashMap::entry(...).or_insert_with(...)` is synchronous, so this is
        // race-free without holding any async lock.
        let slot = self
            .connections
            .entry(worker_id.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone();

        // Step 2 — Lock this worker's slot exclusively.
        // Any concurrent caller for the same worker blocks here, preventing
        // duplicate connections.
        let mut guard = slot.lock().await;

        // Step 3 — Validate an existing cached connection.
        if let Some(entry) = guard.as_mut() {
            let expired = entry.created_at.elapsed() >= self.config.connection_ttl;
            if !expired {
                if entry.conn.health_check().await {
                    return Ok(entry.conn.clone());
                }
                warn!(
                    worker_id = %worker_id,
                    "Cached connection failed health check; reconnecting"
                );
            } else {
                info!(
                    worker_id = %worker_id,
                    elapsed_secs = entry.created_at.elapsed().as_secs(),
                    "Cached connection exceeded TTL; reconnecting"
                );
            }
            // Evict the stale / unhealthy entry.
            *guard = None;
        }

        // Step 4 — Enforce the connection cap before creating a new one.
        let limit = self.effective_max_connections();
        if limit > 0 && self.active_connection_count() >= limit {
            self.try_evict_oldest(worker_id);
        }

        // Step 5 — Resolve endpoint.
        let endpoint = self
            .endpoints
            .get(worker_id)
            .map(|e| e.clone())
            .ok_or_else(|| StageExecutionError::WorkerUnavailable {
                worker_id: worker_id.clone(),
                detail: format!("No endpoint registered for worker '{}'", worker_id),
            })?;

        // Step 6 — Connect.
        let conn = WorkerConnection::connect(worker_id.clone(), &endpoint).await?;

        // Step 7 — Cache with a fresh timestamp.
        *guard = Some(CachedEntry {
            conn: conn.clone(),
            created_at: Instant::now(),
        });

        Ok(conn)
    }

    /// Evict all cached connections whose age exceeds the configured TTL.
    ///
    /// Uses `try_lock` so it never blocks; slots held by concurrent callers are
    /// simply skipped — they will be evicted on the next opportunity.
    pub async fn evict_stale_connections(&self) {
        let ttl = self.config.connection_ttl;
        for entry in self.connections.iter() {
            if let Ok(mut guard) = entry.value().try_lock() {
                if let Some(cached) = guard.as_ref() {
                    if cached.created_at.elapsed() >= ttl {
                        info!(
                            worker_id = %entry.key(),
                            "Evicting stale connection (TTL exceeded)"
                        );
                        *guard = None;
                    }
                }
            }
        }
    }

    /// Remove the cached connection for `worker_id` (e.g., after a send error).
    pub async fn remove_connection(&self, worker_id: &WorkerId) {
        if let Some(slot) = self.connections.get(worker_id) {
            let mut guard = slot.lock().await;
            *guard = None;
        }
    }

    /// Get the endpoint URL for a worker.
    pub async fn get_endpoint(&self, worker_id: &WorkerId) -> Option<String> {
        self.endpoints.get(worker_id).map(|e| e.clone())
    }

    /// Get all registered worker IDs.
    pub async fn worker_ids(&self) -> Vec<WorkerId> {
        self.endpoints
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    // ------------------------------------------------------------------ //
    // Internal helpers                                                     //
    // ------------------------------------------------------------------ //

    /// Effective maximum connections: the configured value (capped at
    /// [`MAX_CONNECTIONS_CAP`]), or when configured as `0`, the current number
    /// of registered workers (also capped).
    fn effective_max_connections(&self) -> usize {
        if self.config.max_connections == 0 {
            self.endpoints.len().min(MAX_CONNECTIONS_CAP)
        } else {
            self.config.max_connections.min(MAX_CONNECTIONS_CAP)
        }
    }

    /// Count slots that currently hold a live (non-`None`) entry using
    /// non-blocking `try_lock`.
    fn active_connection_count(&self) -> usize {
        self.connections
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .try_lock()
                    .map(|g| g.is_some())
                    .unwrap_or(false)
            })
            .count()
    }

    /// Try to evict the oldest cached connection (excluding `skip_worker`),
    /// using `try_lock` to avoid blocking or deadlocking.
    fn try_evict_oldest(&self, skip_worker: &WorkerId) {
        let mut oldest_elapsed = Duration::ZERO;
        let mut oldest_worker: Option<WorkerId> = None;

        for entry in self.connections.iter() {
            if entry.key() == skip_worker {
                continue;
            }
            if let Ok(guard) = entry.value().try_lock() {
                if let Some(cached) = guard.as_ref() {
                    let elapsed = cached.created_at.elapsed();
                    if elapsed > oldest_elapsed {
                        oldest_elapsed = elapsed;
                        oldest_worker = Some(entry.key().clone());
                    }
                }
            }
        }

        if let Some(worker) = oldest_worker {
            if let Some(slot) = self.connections.get(&worker) {
                if let Ok(mut guard) = slot.try_lock() {
                    info!(
                        worker_id = %worker,
                        "Evicting oldest connection to enforce pool cap"
                    );
                    *guard = None;
                }
            }
        }
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

/// How long a cached stage ticket remains valid before it is evicted.
/// This matches the TTL used by the Flight service's stage result store.
const TICKET_CACHE_TTL: Duration = Duration::from_secs(300);

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
    /// Cached tickets from submit_stage responses, with insertion timestamp for TTL eviction.
    /// Maps (query_id, stage_id, worker_id) -> (ticket bytes, inserted_at)
    ticket_cache: DashMap<String, (Vec<u8>, Instant)>,
}

impl FlightStageExecutor {
    /// Create a new Flight-based executor.
    pub fn new(tenant_id: String, connection_pool: Arc<WorkerConnectionPool>) -> Self {
        Self {
            tenant_id,
            connection_pool,
            ticket_cache: DashMap::new(),
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

    /// Evict all ticket cache entries older than [`TICKET_CACHE_TTL`].
    ///
    /// Call this periodically to prevent the cache from growing unboundedly
    /// across long-running coordinators that issue many queries.
    pub fn evict_expired_tickets(&self) {
        self.ticket_cache
            .retain(|_, (_, inserted_at)| inserted_at.elapsed() < TICKET_CACHE_TTL);
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

        let response = match conn.client.do_action(action).await {
            Ok(r) => r,
            Err(e) => {
                error!(
                    query_id = query_id,
                    stage_id = %stage.stage_id,
                    worker_id = %worker_id,
                    error = %e,
                    "DoAction failed — evicting stale connection"
                );
                self.connection_pool.remove_connection(worker_id).await;
                return Err(StageExecutionError::WorkerUnavailable {
                    worker_id: worker_id.clone(),
                    detail: format!("DoAction failed: {}", e),
                });
            }
        };

        // Process response stream
        let mut stream = response.into_inner();
        let result = match stream.message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                self.connection_pool.remove_connection(worker_id).await;
                return Err(StageExecutionError::ExecutionFailed(
                    "Empty response from submit_stage".into(),
                ));
            }
            Err(e) => {
                error!(
                    query_id = query_id,
                    stage_id = %stage.stage_id,
                    worker_id = %worker_id,
                    error = %e,
                    "Stream read failed — evicting stale connection"
                );
                self.connection_pool.remove_connection(worker_id).await;
                return Err(StageExecutionError::ExecutionFailed(format!(
                    "Failed to read response: {}",
                    e
                )));
            }
        };

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

        // Cache the result ticket using the actual query_id, recording the insertion time
        // so that the entry can be evicted once it exceeds TICKET_CACHE_TTL.
        let ticket_key = Self::ticket_key(query_id, stage.stage_id, worker_id);
        self.ticket_cache.insert(
            ticket_key,
            (submit_response.result_ticket.clone(), Instant::now()),
        );

        // Opportunistically evict any stale tickets on each insert.
        // The cache is small (one entry per worker per stage), so O(n) is fine here.
        self.evict_expired_tickets();

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

        // Create ticket and attach the raw Flight ticket bytes from the remote
        // worker so that the coordinator can register the correct bytes with the
        // exchange runtime (instead of the synthetic local ticket_id bytes).
        let mut ticket = StageTicket::new(
            query_id.to_string(),
            stage.stage_id,
            worker_id.clone(),
        );
        ticket.flight_ticket_bytes = Some(submit_response.result_ticket.clone());
        Ok(ticket)
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
                        let part_ticket = StageTicket::partitioned(
                            query_id.to_string(),
                            stage.stage_id,
                            worker_id.clone(),
                            p,
                        );
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
            "Streaming execution not yet implemented for Flight executor".into(),
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

        // Use the real query_id from the ticket (set when the ticket was created by submit_to_worker)
        let ticket_key = Self::ticket_key(&ticket.query_id, ticket.stage_id, &ticket.worker_id);

        let ticket_bytes = {
            self.ticket_cache
                .get(&ticket_key)
                .and_then(|entry| {
                    let (bytes, inserted_at) = entry.value();
                    if inserted_at.elapsed() < TICKET_CACHE_TTL {
                        Some(bytes.clone())
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
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

        let response = conn.client.do_get(flight_ticket).await.map_err(|e| {
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
            arrow_flight::error::FlightError::Tonic(Box::new(tonic::Status::internal(
                e.to_string(),
            )))
        }));

        // Collect all batches
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.map_err(|e| {
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
    async fn get_client(
        &mut self,
    ) -> Result<&mut FlightServiceClient<Channel>, StageExecutionError> {
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

        let response = client
            .do_action(action)
            .await
            .map_err(|e| StageExecutionError::ExecutionFailed(format!("DoAction failed: {}", e)))?;

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

        let response = client
            .do_get(ticket)
            .await
            .map_err(|e| StageExecutionError::ExecutionFailed(format!("DoGet failed: {}", e)))?;

        let stream = response.into_inner();
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| {
            arrow_flight::error::FlightError::Tonic(Box::new(tonic::Status::internal(
                e.to_string(),
            )))
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
        assert!(pool.connections.is_empty());
        assert!(pool.endpoints.is_empty());
    }

    #[test]
    fn test_worker_connection_pool_config_defaults() {
        let cfg = WorkerConnectionPoolConfig::default();
        assert_eq!(cfg.connection_ttl, CONNECTION_TTL_DEFAULT);
        assert_eq!(cfg.max_connections, 0);
    }

    #[test]
    fn test_effective_max_connections_dynamic() {
        // When max_connections == 0, effective limit == number of registered workers.
        let pool = WorkerConnectionPool::new();
        assert_eq!(pool.effective_max_connections(), 0); // no workers yet

        // Simulate registered workers by inserting into the endpoints map directly.
        pool.endpoints
            .insert(WorkerId::from("w1"), "http://localhost:50051".into());
        pool.endpoints
            .insert(WorkerId::from("w2"), "http://localhost:50052".into());
        assert_eq!(pool.effective_max_connections(), 2);
    }

    #[test]
    fn test_effective_max_connections_fixed() {
        let pool = WorkerConnectionPool::with_config(WorkerConnectionPoolConfig {
            connection_ttl: CONNECTION_TTL_DEFAULT,
            max_connections: 42,
        });
        assert_eq!(pool.effective_max_connections(), 42);
    }

    #[test]
    fn test_effective_max_connections_cap() {
        // A configured value above MAX_CONNECTIONS_CAP is clamped to the cap.
        let pool = WorkerConnectionPool::with_config(WorkerConnectionPoolConfig {
            connection_ttl: CONNECTION_TTL_DEFAULT,
            max_connections: 200,
        });
        assert_eq!(pool.effective_max_connections(), MAX_CONNECTIONS_CAP);
    }

    #[tokio::test]
    async fn test_register_worker() {
        let pool = WorkerConnectionPool::new();
        pool.register_worker(WorkerId::from("w1"), "http://localhost:50051".to_string())
            .await;

        assert_eq!(
            pool.endpoints.get(&WorkerId::from("w1")).map(|e| e.clone()),
            Some("http://localhost:50051".to_string())
        );
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

    #[tokio::test]
    async fn test_evict_stale_connections() {
        // Build a pool with a very short TTL so we can trigger eviction immediately.
        let pool = WorkerConnectionPool::with_config(WorkerConnectionPoolConfig {
            connection_ttl: Duration::from_millis(1),
            max_connections: 10,
        });

        // Manually plant an already-expired entry without going through connect().
        // We create a slot and set its CachedEntry created_at in the past.
        // Because we can't construct a real FlightServiceClient without a server,
        // we just check that evict_stale_connections() doesn't panic on an empty
        // pool and that it correctly skips locked slots.
        pool.register_worker(WorkerId::from("w1"), "http://localhost:50051".to_string())
            .await;

        // Ensure the slot exists (empty).
        let slot = pool
            .connections
            .entry(WorkerId::from("w1"))
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone();

        // Lock the slot and verify evict_stale_connections skips it gracefully.
        let _guard = slot.try_lock().expect("slot should be unlocked");
        pool.evict_stale_connections().await; // must not panic or deadlock
    }

    #[tokio::test]
    async fn test_remove_connection_no_panic_on_unknown_worker() {
        // remove_connection on a worker that has no slot should be a no-op.
        let pool = WorkerConnectionPool::new();
        pool.remove_connection(&WorkerId::from("ghost")).await; // must not panic
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
        assert!(executor.connection_pool().connections.is_empty());
    }
}
