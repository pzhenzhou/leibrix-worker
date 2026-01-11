//! Exchange runtime for LDP execution.
//!
//! This module handles data movement between stages via exchanges:
//! - Gather: Collect data from multiple workers to one
//! - Broadcast: Replicate data to multiple workers
//! - HashPartition: Redistribute data by hash of key columns
//!
//! # Distributed vs Local Exchange
//! - `ExchangeRuntime<E>`: Local exchange using any `StageExecutor`
//! - `DistributedExchangeRuntime`: Remote exchange via Arrow Flight

use crate::ldp::executor::flight::{LdpFlightClient, WorkerConnectionPool};
use crate::ldp::executor::stage::{StageExecutionError, StageExecutor, StageTicket, StageTickets};
use crate::ldp::{Exchange, ExchangeEdge, ExchangeId, LdpPlan, StageLimits, StageId, WorkerId};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Runtime for executing exchange operations.
///
/// Handles fetching data from upstream stages and transforming it
/// according to the exchange type (Gather, Broadcast, HashPartition).
pub struct ExchangeRuntime<E: StageExecutor> {
    /// Stage executor for fetching data.
    executor: Arc<E>,
}

impl<E: StageExecutor> ExchangeRuntime<E> {
    /// Create a new exchange runtime.
    pub fn new(executor: Arc<E>) -> Self {
        Self { executor }
    }

    /// Resolve inputs for a stage from upstream exchanges.
    ///
    /// # Arguments
    /// * `plan` - The LDP plan
    /// * `stage_id` - The stage we're resolving inputs for
    /// * `stage_outputs` - Outputs from already-executed stages
    ///
    /// # Returns
    /// HashMap mapping table_name -> batches for each exchange input.
    pub async fn resolve_inputs(
        &self,
        plan: &LdpPlan,
        stage_id: StageId,
        stage_outputs: &HashMap<StageId, StageTickets>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, ExchangeError> {
        let mut inputs = HashMap::new();

        // Find the stage
        let stage = plan
            .get_stage(stage_id)
            .ok_or(ExchangeError::StageNotFound(stage_id))?;

        // Process each input
        for input in &stage.inputs {
            match input {
                crate::ldp::StageInput::LocalCatalog => {
                    // LocalCatalog inputs are handled by DuckDB directly
                    continue;
                }
                crate::ldp::StageInput::ExchangeInput {
                    exchange_id,
                    table_name,
                } => {
                    // Find the exchange edge
                    let edge = plan
                        .edges
                        .iter()
                        .find(|e| e.exchange_id == *exchange_id)
                        .ok_or(ExchangeError::ExchangeNotFound(*exchange_id))?;

                    // Get upstream stage outputs
                    let upstream_tickets = stage_outputs
                        .get(&edge.from_stage)
                        .ok_or(ExchangeError::UpstreamNotReady(edge.from_stage))?;

                    // Execute the exchange
                    let batches = self
                        .execute_exchange(edge, upstream_tickets, &stage.target_workers)
                        .await?;

                    inputs.insert(table_name.clone(), batches);
                }
            }
        }

        Ok(inputs)
    }

    /// Execute an exchange operation.
    async fn execute_exchange(
        &self,
        edge: &ExchangeEdge,
        upstream_tickets: &StageTickets,
        target_workers: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        match &edge.kind {
            Exchange::Gather { target } => {
                self.execute_gather(upstream_tickets, target).await
            }
            Exchange::Broadcast { targets } => {
                self.execute_broadcast(upstream_tickets, targets).await
            }
            Exchange::HashPartition {
                field_refs,
                partitions,
            } => {
                self.execute_hash_partition(
                    upstream_tickets,
                    field_refs,
                    *partitions,
                    &edge.partition_to_worker,
                    target_workers,
                )
                .await
            }
        }
    }

    /// Execute a Gather exchange.
    ///
    /// Collects all upstream outputs and concatenates them.
    async fn execute_gather(
        &self,
        upstream_tickets: &StageTickets,
        _target: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        let mut all_batches = Vec::new();

        // Fetch from all upstream tickets
        for ticket in upstream_tickets.all() {
            let batches = self.executor.fetch_output(ticket).await.map_err(|e| {
                ExchangeError::FetchFailed(format!("Failed to fetch {}: {}", ticket.ticket_id, e))
            })?;
            all_batches.extend(batches);
        }

        // Optionally concatenate all batches into fewer batches
        // For now, just return all batches
        Ok(all_batches)
    }

    /// Execute a Broadcast exchange.
    ///
    /// Replicates the upstream data to all targets.
    async fn execute_broadcast(
        &self,
        upstream_tickets: &StageTickets,
        _targets: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        // For broadcast, we just need to fetch the data once
        // Each target will receive a copy
        let mut all_batches = Vec::new();

        for ticket in upstream_tickets.all() {
            let batches = self.executor.fetch_output(ticket).await.map_err(|e| {
                ExchangeError::FetchFailed(format!("Failed to fetch {}: {}", ticket.ticket_id, e))
            })?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Execute a HashPartition exchange.
    ///
    /// Redistributes data by hash of key columns.
    /// For local execution, we still partition the data but return only
    /// the partitions for the "local" worker to simulate distributed behavior.
    async fn execute_hash_partition(
        &self,
        upstream_tickets: &StageTickets,
        field_refs: &[u32],
        num_partitions: u32,
        partition_to_worker: &[WorkerId],
        target_workers: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        // Collect all upstream data
        let mut all_batches = Vec::new();

        for ticket in upstream_tickets.all() {
            let batches = self.executor.fetch_output(ticket).await.map_err(|e| {
                ExchangeError::FetchFailed(format!("Failed to fetch {}: {}", ticket.ticket_id, e))
            })?;
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // For local execution, we assume we're the "first" target worker
        // In a real distributed system, this would be determined by the actual worker ID
        let default_worker = "local".to_string();
        let local_worker = target_workers.first().unwrap_or(&default_worker);

        // Find which partitions belong to this worker
        let local_partitions: Vec<u32> = partition_to_worker
            .iter()
            .enumerate()
            .filter_map(|(partition_id, worker_id)| {
                if worker_id == local_worker {
                    Some(partition_id as u32)
                } else {
                    None
                }
            })
            .collect();

        if local_partitions.is_empty() {
            // No partitions for this worker
            return Ok(vec![]);
        }

        // Hash partition all batches
        let mut partitioned_batches: Vec<Vec<RecordBatch>> = vec![vec![]; num_partitions as usize];
        
        for batch in all_batches {
            let partitions = hash_partition_batch(&batch, field_refs, num_partitions)?;
            for (partition_id, partition_batch) in partitions.into_iter().enumerate() {
                if partition_batch.num_rows() > 0 {
                    partitioned_batches[partition_id].push(partition_batch);
                }
            }
        }

        // Return only the partitions for this worker
        let mut result = Vec::new();
        for partition_id in local_partitions {
            result.extend(partitioned_batches[partition_id as usize].drain(..));
        }

        Ok(result)
    }
}

/// Hash partition a batch by specified columns.
///
/// # Arguments
/// * `batch` - The batch to partition
/// * `field_refs` - Column indices to hash
/// * `num_partitions` - Number of output partitions
///
/// # Returns
/// Vec of batches, one per partition.
pub fn hash_partition_batch(
    batch: &RecordBatch,
    field_refs: &[u32],
    num_partitions: u32,
) -> Result<Vec<RecordBatch>, ExchangeError> {
    use arrow::array::{Array, UInt32Array};
    use arrow::compute::{filter_record_batch, take};
    use std::hash::{Hash, Hasher};

    if batch.num_rows() == 0 {
        return Ok(vec![batch.clone(); num_partitions as usize]);
    }

    if field_refs.is_empty() {
        // No partitioning keys - all to partition 0
        let mut result = vec![RecordBatch::new_empty(batch.schema()); num_partitions as usize];
        result[0] = batch.clone();
        return Ok(result);
    }

    // Compute hash for each row
    let num_rows = batch.num_rows();
    let mut partition_ids = vec![0u32; num_rows];

    // Simple hash computation (production would use better hashing)
    for row in 0..num_rows {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        for &col_idx in field_refs {
            let col = batch.column(col_idx as usize);
            // Hash the column value at this row
            // This is simplified - real impl would handle all Arrow types
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                if arr.is_valid(row) {
                    arr.value(row).hash(&mut hasher);
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                if arr.is_valid(row) {
                    arr.value(row).hash(&mut hasher);
                }
            }
            // Add more type handlers as needed
        }

        partition_ids[row] = (hasher.finish() % num_partitions as u64) as u32;
    }

    // Create output batches by filtering
    let partition_id_array = UInt32Array::from(partition_ids.clone());
    let mut results = Vec::with_capacity(num_partitions as usize);

    for p in 0..num_partitions {
        // Create boolean mask for this partition
        let mask: arrow::array::BooleanArray = partition_id_array
            .iter()
            .map(|v| v.map(|id| id == p))
            .collect();

        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| ExchangeError::PartitionFailed(e.to_string()))?;

        results.push(filtered);
    }

    Ok(results)
}

/// Concatenate multiple record batches into one.
pub fn concat_record_batches(batches: &[RecordBatch]) -> Result<RecordBatch, ExchangeError> {
    if batches.is_empty() {
        return Err(ExchangeError::NoBatches);
    }

    let schema = batches[0].schema();
    concat_batches(&schema, batches).map_err(|e| ExchangeError::ConcatFailed(e.to_string()))
}

/// Error during exchange execution.
#[derive(Clone, Debug)]
pub enum ExchangeError {
    /// Stage not found.
    StageNotFound(StageId),
    /// Exchange not found.
    ExchangeNotFound(ExchangeId),
    /// Upstream stage output not ready.
    UpstreamNotReady(StageId),
    /// Failed to fetch data.
    FetchFailed(String),
    /// Failed to partition data.
    PartitionFailed(String),
    /// No batches to process.
    NoBatches,
    /// Failed to concatenate batches.
    ConcatFailed(String),
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeError::StageNotFound(id) => write!(f, "Stage {} not found", id),
            ExchangeError::ExchangeNotFound(id) => write!(f, "Exchange {} not found", id),
            ExchangeError::UpstreamNotReady(id) => write!(f, "Upstream stage {} not ready", id),
            ExchangeError::FetchFailed(msg) => write!(f, "Fetch failed: {}", msg),
            ExchangeError::PartitionFailed(msg) => write!(f, "Partition failed: {}", msg),
            ExchangeError::NoBatches => write!(f, "No batches to process"),
            ExchangeError::ConcatFailed(msg) => write!(f, "Concat failed: {}", msg),
        }
    }
}

impl std::error::Error for ExchangeError {}

// ============================================================================
// Distributed Exchange Runtime
// ============================================================================

/// Remote ticket for retrieving stage output from a worker.
#[derive(Clone, Debug)]
pub struct RemoteTicket {
    /// Worker that holds the data.
    pub worker_id: WorkerId,
    /// Ticket bytes for DoGet.
    pub ticket_bytes: Vec<u8>,
}

/// Runtime for distributed exchange operations via Arrow Flight.
///
/// This handles actual network data movement between workers:
/// - Gather: Fetches data from multiple remote workers to coordinator
/// - Broadcast: Pushes data to multiple remote workers
/// - HashPartition: Routes partitioned data to target workers
pub struct DistributedExchangeRuntime {
    /// Tenant ID for all requests.
    tenant_id: String,
    /// Connection pool to workers.
    connection_pool: Arc<WorkerConnectionPool>,
    /// Cached remote tickets (query_id:stage_id:worker_id -> ticket_bytes).
    remote_tickets: RwLock<HashMap<String, Vec<u8>>>,
}

impl DistributedExchangeRuntime {
    /// Create a new distributed exchange runtime.
    pub fn new(tenant_id: String, connection_pool: Arc<WorkerConnectionPool>) -> Self {
        Self {
            tenant_id,
            connection_pool,
            remote_tickets: RwLock::new(HashMap::new()),
        }
    }

    /// Create with a fresh connection pool.
    pub fn with_new_pool(tenant_id: String) -> Self {
        Self::new(tenant_id, Arc::new(WorkerConnectionPool::new()))
    }

    /// Get the connection pool for registering workers.
    pub fn connection_pool(&self) -> &Arc<WorkerConnectionPool> {
        &self.connection_pool
    }

    /// Register a result ticket from a stage submission.
    pub async fn register_ticket(
        &self,
        query_id: &str,
        stage_id: u32,
        worker_id: &str,
        ticket_bytes: Vec<u8>,
    ) {
        let key = format!("{}:{}:{}", query_id, stage_id, worker_id);
        self.remote_tickets.write().await.insert(key, ticket_bytes);
    }

    /// Get a registered ticket.
    pub async fn get_ticket(
        &self,
        query_id: &str,
        stage_id: u32,
        worker_id: &str,
    ) -> Option<Vec<u8>> {
        let key = format!("{}:{}:{}", query_id, stage_id, worker_id);
        self.remote_tickets.read().await.get(&key).cloned()
    }

    // ========================================================================
    // Distributed Gather
    // ========================================================================

    /// Execute a distributed Gather exchange.
    ///
    /// Fetches data from multiple remote workers and combines them at the
    /// coordinator. Uses Arrow Flight DoGet to retrieve results.
    ///
    /// # Arguments
    /// * `query_id` - Query identifier
    /// * `stage_id` - Upstream stage that produced the data
    /// * `source_workers` - Workers that have data to gather
    /// * `target_worker` - Coordinator receiving the gathered data
    ///
    /// # Returns
    /// Combined record batches from all source workers.
    pub async fn execute_gather(
        &self,
        query_id: &str,
        stage_id: u32,
        source_workers: &[WorkerId],
        _target_worker: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = stage_id,
            sources = ?source_workers,
            "Executing distributed gather"
        );

        let mut all_batches = Vec::new();
        let mut errors = Vec::new();

        // Fetch from each source worker in parallel
        let fetch_futures: Vec<_> = source_workers
            .iter()
            .map(|worker_id| {
                self.fetch_from_worker(query_id, stage_id, worker_id)
            })
            .collect();

        // Execute all fetches concurrently
        let results = futures_util::future::join_all(fetch_futures).await;

        for (worker_id, result) in source_workers.iter().zip(results.into_iter()) {
            match result {
                Ok(batches) => {
                    debug!(
                        query_id = query_id,
                        stage_id = stage_id,
                        worker_id = %worker_id,
                        batches = batches.len(),
                        "Gathered data from worker"
                    );
                    all_batches.extend(batches);
                }
                Err(e) => {
                    error!(
                        query_id = query_id,
                        stage_id = stage_id,
                        worker_id = %worker_id,
                        error = %e,
                        "Failed to gather from worker"
                    );
                    errors.push(format!("{}: {}", worker_id, e));
                }
            }
        }

        if !errors.is_empty() && all_batches.is_empty() {
            return Err(ExchangeError::FetchFailed(errors.join("; ")));
        }

        if !errors.is_empty() {
            warn!(
                query_id = query_id,
                stage_id = stage_id,
                "Partial gather success: {} batches collected, {} workers failed",
                all_batches.len(),
                errors.len()
            );
        }

        info!(
            query_id = query_id,
            stage_id = stage_id,
            total_batches = all_batches.len(),
            total_rows = all_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Gather complete"
        );

        Ok(all_batches)
    }

    /// Fetch data from a single worker via Flight DoGet.
    async fn fetch_from_worker(
        &self,
        query_id: &str,
        stage_id: u32,
        worker_id: &str,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        // Get the ticket for this worker's output
        let ticket_bytes = self
            .get_ticket(query_id, stage_id, worker_id)
            .await
            .ok_or_else(|| {
                ExchangeError::FetchFailed(format!(
                    "No ticket for query={}, stage={}, worker={}",
                    query_id, stage_id, worker_id
                ))
            })?;

        // Create a client for this worker
        let mut client = LdpFlightClient::new(
            self.get_worker_endpoint(worker_id).await?,
            self.tenant_id.clone(),
        );

        // Fetch results
        client.fetch_results(&ticket_bytes).await.map_err(|e| {
            ExchangeError::FetchFailed(format!("Flight fetch from {} failed: {}", worker_id, e))
        })
    }

    /// Get the endpoint URL for a worker.
    async fn get_worker_endpoint(&self, worker_id: &str) -> Result<String, ExchangeError> {
        self.connection_pool
            .get_endpoint(worker_id)
            .await
            .ok_or_else(|| {
                ExchangeError::FetchFailed(format!("No endpoint registered for worker {}", worker_id))
            })
    }

    // ========================================================================
    // Distributed Broadcast
    // ========================================================================

    /// Execute a distributed Broadcast exchange.
    ///
    /// Fetches data from the source and makes it available to all target workers.
    /// For now, the broadcast data is returned and will be sent via DoPut or
    /// made available via a shared store.
    ///
    /// # Arguments
    /// * `query_id` - Query identifier
    /// * `stage_id` - Upstream stage that produced the data
    /// * `source_worker` - Worker that has the data to broadcast
    /// * `target_workers` - Workers that need the broadcasted data
    ///
    /// # Returns
    /// The data to broadcast (same data for all targets).
    pub async fn execute_broadcast(
        &self,
        query_id: &str,
        stage_id: u32,
        source_worker: &WorkerId,
        target_workers: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = stage_id,
            source = %source_worker,
            targets = ?target_workers,
            "Executing distributed broadcast"
        );

        // Fetch the data from the source
        let batches = self
            .fetch_from_worker(query_id, stage_id, source_worker)
            .await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = batches.iter().map(|b| {
            b.columns().iter().map(|c| c.get_array_memory_size()).sum::<usize>()
        }).sum();

        info!(
            query_id = query_id,
            stage_id = stage_id,
            rows = total_rows,
            bytes = total_bytes,
            targets = target_workers.len(),
            "Broadcast data fetched, ready for distribution"
        );

        // TODO: In a full implementation, we would push this data to each target worker
        // via Flight DoPut. For now, return the data for the coordinator to distribute.
        // The coordinator can either:
        // 1. Include broadcast data in the next stage's submit_stage request
        // 2. Push via DoPut to each target worker

        Ok(batches)
    }

    // ========================================================================
    // Distributed Hash Partition
    // ========================================================================

    /// Execute a distributed HashPartition exchange.
    ///
    /// Fetches data from all source workers, partitions it by hash, and routes
    /// each partition to its target worker.
    ///
    /// # Arguments
    /// * `query_id` - Query identifier
    /// * `stage_id` - Upstream stage that produced the data
    /// * `source_workers` - Workers that have data to redistribute
    /// * `field_refs` - Column indices to hash for partitioning
    /// * `num_partitions` - Number of partitions to create
    /// * `partition_to_worker` - Mapping from partition ID to target worker
    /// * `local_worker` - The worker requesting its partition of data
    ///
    /// # Returns
    /// Record batches for the local worker's partition(s).
    pub async fn execute_hash_partition(
        &self,
        query_id: &str,
        stage_id: u32,
        source_workers: &[WorkerId],
        field_refs: &[u32],
        num_partitions: u32,
        partition_to_worker: &[WorkerId],
        local_worker: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = stage_id,
            sources = ?source_workers,
            partitions = num_partitions,
            local_worker = %local_worker,
            "Executing distributed hash partition"
        );

        // First, gather all data from source workers
        let all_batches = self
            .execute_gather(query_id, stage_id, source_workers, local_worker)
            .await?;

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // Find which partitions belong to this local worker
        let local_partitions: Vec<u32> = partition_to_worker
            .iter()
            .enumerate()
            .filter(|(_, w)| *w == local_worker)
            .map(|(i, _)| i as u32)
            .collect();

        debug!(
            query_id = query_id,
            stage_id = stage_id,
            local_worker = %local_worker,
            local_partitions = ?local_partitions,
            "Filtering to local partitions"
        );

        // Partition each batch and collect the local partitions
        let mut local_batches = Vec::new();

        for batch in all_batches {
            let partitioned = hash_partition_batch(&batch, field_refs, num_partitions)?;

            for p in &local_partitions {
                let partition_batch = &partitioned[*p as usize];
                if partition_batch.num_rows() > 0 {
                    local_batches.push(partition_batch.clone());
                }
            }
        }

        info!(
            query_id = query_id,
            stage_id = stage_id,
            local_worker = %local_worker,
            batches = local_batches.len(),
            rows = local_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Hash partition complete for local worker"
        );

        Ok(local_batches)
    }

    // ========================================================================
    // Full Exchange Execution
    // ========================================================================

    /// Execute an exchange based on its type.
    pub async fn execute_exchange(
        &self,
        query_id: &str,
        edge: &ExchangeEdge,
        source_workers: &[WorkerId],
        local_worker: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        match &edge.kind {
            Exchange::Gather { target } => {
                self.execute_gather(query_id, edge.from_stage, source_workers, target)
                    .await
            }
            Exchange::Broadcast { targets } => {
                // For broadcast, source is typically a single worker
                let source = source_workers.first().ok_or_else(|| {
                    ExchangeError::FetchFailed("No source worker for broadcast".into())
                })?;
                self.execute_broadcast(query_id, edge.from_stage, source, targets)
                    .await
            }
            Exchange::HashPartition {
                field_refs,
                partitions,
            } => {
                self.execute_hash_partition(
                    query_id,
                    edge.from_stage,
                    source_workers,
                    field_refs,
                    *partitions,
                    &edge.partition_to_worker,
                    local_worker,
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let name_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_hash_partition_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);

        let result = hash_partition_batch(&batch, &[0], 4).unwrap();
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_hash_partition_single_key() {
        let batch = create_test_batch();

        let result = hash_partition_batch(&batch, &[0], 4).unwrap();
        assert_eq!(result.len(), 4);

        // Total rows should match
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn test_concat_batches() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let result = concat_record_batches(&[batch1, batch2]).unwrap();
        assert_eq!(result.num_rows(), 16);
    }

    // ========================================================================
    // Distributed Exchange Runtime Tests
    // ========================================================================

    #[tokio::test]
    async fn test_distributed_exchange_runtime_creation() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());
        // Verify connection pool is accessible
        let workers = runtime.connection_pool().worker_ids().await;
        assert!(workers.is_empty());
    }

    #[tokio::test]
    async fn test_distributed_exchange_ticket_registration() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());

        // Register a ticket
        runtime
            .register_ticket("q1", 1, "worker-1", vec![1, 2, 3])
            .await;

        // Retrieve it
        let ticket = runtime.get_ticket("q1", 1, "worker-1").await;
        assert_eq!(ticket, Some(vec![1, 2, 3]));

        // Non-existent ticket returns None
        let missing = runtime.get_ticket("q1", 2, "worker-1").await;
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_distributed_exchange_get_worker_endpoint_missing() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());

        let result = runtime.get_worker_endpoint("unknown-worker").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_distributed_exchange_get_worker_endpoint() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());

        // Register a worker
        runtime
            .connection_pool()
            .register_worker("w1".to_string(), "http://localhost:50051".to_string())
            .await;

        let result = runtime.get_worker_endpoint("w1").await;
        assert_eq!(result.unwrap(), "http://localhost:50051");
    }

    #[test]
    fn test_remote_ticket_creation() {
        let ticket = RemoteTicket {
            worker_id: "worker-1".to_string(),
            ticket_bytes: vec![1, 2, 3, 4],
        };

        assert_eq!(ticket.worker_id, "worker-1");
        assert_eq!(ticket.ticket_bytes.len(), 4);
    }

    #[test]
    fn test_hash_partition_no_keys() {
        let batch = create_test_batch();

        // Empty field_refs means all data goes to partition 0
        let result = hash_partition_batch(&batch, &[], 4).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].num_rows(), 8);
        assert_eq!(result[1].num_rows(), 0);
        assert_eq!(result[2].num_rows(), 0);
        assert_eq!(result[3].num_rows(), 0);
    }

    #[test]
    fn test_hash_partition_multiple_keys() {
        let batch = create_test_batch();

        // Partition by both columns
        let result = hash_partition_batch(&batch, &[0, 1], 4).unwrap();
        assert_eq!(result.len(), 4);

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }
}
