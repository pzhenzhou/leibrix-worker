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
use crate::ldp::executor::metrics::ExchangeMetricsRegistry;
use crate::ldp::executor::skew::SkewHandler;
use crate::ldp::executor::stage::{StageExecutor, StageTickets};
use crate::ldp::{Exchange, ExchangeEdge, ExchangeId, LdpPlan, StageId, WorkerId};
use crate::sql::logical_plan::ColumnRef;
use arrow::array::{Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
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
        _target_workers: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        match &edge.kind {
            Exchange::Gather { target } => {
                self.execute_gather(upstream_tickets, target).await
            }
            Exchange::Broadcast { targets } => {
                self.execute_broadcast(upstream_tickets, targets).await
            }
            Exchange::HashPartition {
                column_refs,
                partitions,
            } => {
                // Legacy path: return ALL partitions (used by resolve_inputs
                // which doesn't distinguish per-worker).
                // The per-worker path is execute_hash_partition_for_worker.
                self.execute_hash_partition_all(
                    upstream_tickets,
                    column_refs,
                    *partitions,
                )
                .await
            }
        }
    }

    /// Execute a hash partition exchange and return ALL partitions (not filtered
    /// by target worker). This is the legacy fallback for `resolve_inputs`.
    async fn execute_hash_partition_all(
        &self,
        upstream_tickets: &StageTickets,
        column_refs: &[ColumnRef],
        num_partitions: u32,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
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

        let schema = all_batches[0].schema();
        let field_refs = resolve_column_indices(column_refs, &schema)?;

        let mut result = Vec::new();
        for batch in &all_batches {
            let partitions = hash_partition_batch(batch, &field_refs, num_partitions)?;
            for partition_batch in partitions {
                if partition_batch.num_rows() > 0 {
                    result.push(partition_batch);
                }
            }
        }

        if result.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        Ok(result)
    }

    /// Execute a Gather exchange.
    ///
    /// Collects all upstream outputs and concatenates them.
    /// If all upstream workers produced zero rows, returns an empty batch
    /// with the correct schema so that downstream SQL can still reference columns.
    async fn execute_gather(
        &self,
        upstream_tickets: &StageTickets,
        _target: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        let mut all_batches = Vec::new();
        let mut last_schema: Option<SchemaRef> = None;

        // Fetch from all upstream tickets
        for ticket in upstream_tickets.all() {
            let batches = self.executor.fetch_output(ticket).await.map_err(|e| {
                ExchangeError::FetchFailed(format!("Failed to fetch {}: {}", ticket.ticket_id, e))
            })?;
            for batch in &batches {
                if batch.num_columns() > 0 {
                    last_schema = Some(batch.schema());
                }
            }
            all_batches.extend(batches);
        }

        // If all batches are empty but we have a schema, return one empty batch
        // so the downstream stage gets the correct column definitions.
        if all_batches.is_empty() {
            if let Some(schema) = last_schema {
                return Ok(vec![RecordBatch::new_empty(schema)]);
            }
        }

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

    /// Execute a HashPartition exchange for a specific target worker.
    ///
    /// Redistributes data by hash of key columns and returns only the
    /// partitions assigned to `target_worker_id`.
    async fn execute_hash_partition_for_worker(
        &self,
        upstream_tickets: &StageTickets,
        column_refs: &[ColumnRef],
        num_partitions: u32,
        partition_to_worker: &[WorkerId],
        target_worker_id: &WorkerId,
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

        // Resolve column references to indices using the first batch's schema
        let schema = all_batches[0].schema();
        let field_refs = resolve_column_indices(column_refs, &schema)?;

        // Find which partitions belong to this worker
        let local_partitions: Vec<u32> = partition_to_worker
            .iter()
            .enumerate()
            .filter_map(|(partition_id, worker_id)| {
                if worker_id == target_worker_id {
                    Some(partition_id as u32)
                } else {
                    None
                }
            })
            .collect();

        if local_partitions.is_empty() {
            // No partitions for this worker — return empty batch with correct schema
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        // Hash partition all batches
        let mut partitioned_batches: Vec<Vec<RecordBatch>> = vec![vec![]; num_partitions as usize];
        
        for batch in &all_batches {
            let partitions = hash_partition_batch(batch, &field_refs, num_partitions)?;
            for (partition_id, partition_batch) in partitions.into_iter().enumerate() {
                if partition_batch.num_rows() > 0 {
                    partitioned_batches[partition_id].push(partition_batch);
                }
            }
        }

        // Return only the partitions for this worker
        let mut result = Vec::new();
        for partition_id in &local_partitions {
            result.append(&mut partitioned_batches[*partition_id as usize]);
        }

        // If all local partitions were empty, return an empty batch with correct schema
        if result.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        Ok(result)
    }

    /// Resolve inputs for a specific target worker.
    ///
    /// Unlike `resolve_inputs` which resolves for the first target worker only,
    /// this method takes a specific worker ID and returns the correct subset of
    /// exchange data for that worker. This is essential for correct hash partition
    /// exchanges where each worker must receive different data.
    pub async fn resolve_inputs_for_worker(
        &self,
        plan: &LdpPlan,
        stage_id: StageId,
        target_worker_id: &WorkerId,
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
                    continue;
                }
                crate::ldp::StageInput::ExchangeInput {
                    exchange_id,
                    table_name,
                } => {
                    let edge = plan
                        .edges
                        .iter()
                        .find(|e| e.exchange_id == *exchange_id)
                        .ok_or(ExchangeError::ExchangeNotFound(*exchange_id))?;

                    let upstream_tickets = stage_outputs
                        .get(&edge.from_stage)
                        .ok_or(ExchangeError::UpstreamNotReady(edge.from_stage))?;

                    let batches = self
                        .execute_exchange_for_worker(edge, upstream_tickets, target_worker_id)
                        .await?;

                    inputs.insert(table_name.clone(), batches);
                }
            }
        }

        Ok(inputs)
    }

    /// Execute an exchange operation for a specific target worker.
    async fn execute_exchange_for_worker(
        &self,
        edge: &ExchangeEdge,
        upstream_tickets: &StageTickets,
        target_worker_id: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        match &edge.kind {
            Exchange::Gather { target } => {
                // Gather: all data goes to the target
                self.execute_gather(upstream_tickets, target).await
            }
            Exchange::Broadcast { .. } => {
                // Broadcast: all targets get the same data
                self.execute_broadcast(
                    upstream_tickets,
                    std::slice::from_ref(target_worker_id),
                )
                .await
            }
            Exchange::HashPartition {
                column_refs,
                partitions,
            } => {
                self.execute_hash_partition_for_worker(
                    upstream_tickets,
                    column_refs,
                    *partitions,
                    &edge.partition_to_worker,
                    target_worker_id,
                )
                .await
            }
        }
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
    // Use skew-aware partitioning by default
    hash_partition_batch_with_skew_handling(batch, field_refs, num_partitions, true)
}

/// Hash partition a batch by specified columns with optional skew handling.
///
/// # Arguments
/// * `batch` - The batch to partition
/// * `field_refs` - Column indices to hash
/// * `num_partitions` - Number of output partitions
/// * `enable_skew_handling` - Whether to detect and handle data skew
///
/// # Returns
/// Vec of batches, one per partition.
pub fn hash_partition_batch_with_skew_handling(
    batch: &RecordBatch,
    field_refs: &[u32],
    num_partitions: u32,
    enable_skew_handling: bool,
) -> Result<Vec<RecordBatch>, ExchangeError> {
    if enable_skew_handling {
        // Use skew-aware partitioning
        let skew_handler = SkewHandler::new();
        skew_handler.skew_aware_hash_partition(batch, field_refs, num_partitions)
    } else {
        // Use traditional hash partitioning
        hash_partition_batch_traditional(batch, field_refs, num_partitions)
    }
}

/// Traditional hash partitioning without skew handling.
fn hash_partition_batch_traditional(
    batch: &RecordBatch,
    field_refs: &[u32],
    num_partitions: u32,
) -> Result<Vec<RecordBatch>, ExchangeError> {
    use arrow::array::UInt32Array;
    use arrow::compute::filter_record_batch;
    use std::hash::Hasher;

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
    for (row, partition_id) in partition_ids.iter_mut().enumerate().take(num_rows) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        for &col_idx in field_refs {
            let col = batch.column(col_idx as usize);
            // Hash the column value at this row
            hash_column_value(col.as_ref(), row, &mut hasher)?;
        }

        *partition_id = (hasher.finish() % num_partitions as u64) as u32;
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

/// Resolve column references to field indices in an Arrow schema.
///
/// This function maps `ColumnRef` (table.column names) to column indices
/// for use in hash partitioning and other operations.
///
/// At exchange boundaries, table qualifiers are stripped since the upstream
/// table context is lost. The function tries multiple resolution strategies:
/// 1. Exact match with table qualifier (if provided)
/// 2. Match by column name only (unqualified)
/// 3. Case-insensitive match
///
/// # Arguments
/// * `column_refs` - Column references to resolve (e.g., `[{table: "sales", column: "region"}]`)
/// * `schema` - Arrow schema of the data
///
/// # Returns
/// * `Ok(Vec<u32>)` - Field indices corresponding to each column reference
/// * `Err(ExchangeError::ColumnNotFound)` - One or more columns not found
///
/// # Example
/// ```ignore
/// let refs = vec![
///     ColumnRef::qualified("sales", "region"),
///     ColumnRef::unqualified("country"),
/// ];
/// let indices = resolve_column_indices(&refs, &batch.schema())?;
/// // indices might be [2, 5] if those are the positions in the schema
/// ```
pub fn resolve_column_indices(
    column_refs: &[ColumnRef],
    schema: &SchemaRef,
) -> Result<Vec<u32>, ExchangeError> {
    let mut indices = Vec::with_capacity(column_refs.len());

    for col_ref in column_refs {
        let idx = find_column_index(col_ref, schema)?;
        indices.push(idx);
    }

    Ok(indices)
}

/// Find the index of a single column in the schema.
fn find_column_index(col_ref: &ColumnRef, schema: &SchemaRef) -> Result<u32, ExchangeError> {
    let fields = schema.fields();

    // Strategy 1: Try exact match with table qualifier (if present)
    if let Some(table) = &col_ref.table {
        // Look for fully qualified name: "table.column"
        let qualified_name = format!("{}.{}", table, col_ref.column);
        if let Some((idx, _)) = fields.find(&qualified_name) {
            return Ok(idx as u32);
        }
    }

    // Strategy 2: Match by column name only (most common at exchange boundaries)
    if let Some((idx, _)) = fields.find(&col_ref.column) {
        return Ok(idx as u32);
    }

    // Strategy 3: Case-insensitive match
    let col_lower = col_ref.column.to_lowercase();
    for (idx, field) in fields.iter().enumerate() {
        if field.name().to_lowercase() == col_lower {
            return Ok(idx as u32);
        }
    }

    // Column not found - provide helpful error message
    let available_columns: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
    Err(ExchangeError::ColumnNotFound(format!(
        "Column '{}' not found in schema. Available columns: [{}]",
        if let Some(table) = &col_ref.table {
            format!("{}.{}", table, col_ref.column)
        } else {
            col_ref.column.clone()
        },
        available_columns.join(", ")
    )))
}

/// Hash a single value from a column at the given row index.
fn hash_column_value(
    array: &dyn Array,
    row: usize,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) -> Result<(), ExchangeError> {
    use arrow::array::*;
    use std::hash::Hash;

    // If the value is null, we still want to hash it consistently
    if array.is_null(row) {
        // Hash a special null marker
        "NULL_VALUE".hash(hasher);
        return Ok(());
    }

    // Handle different Arrow types
    macro_rules! hash_primitive {
        ($arr:expr, $t:ty) => {{
            let arr = $arr.as_any().downcast_ref::<$t>().unwrap();
            arr.value(row).hash(hasher);
        }};
    }

    match array.data_type() {
        // Signed integers
        arrow::datatypes::DataType::Int8 => {
            hash_primitive!(array, Int8Array);
        }
        arrow::datatypes::DataType::Int16 => {
            hash_primitive!(array, Int16Array);
        }
        arrow::datatypes::DataType::Int32 => {
            hash_primitive!(array, Int32Array);
        }
        arrow::datatypes::DataType::Int64 => {
            hash_primitive!(array, Int64Array);
        }
        // Unsigned integers
        arrow::datatypes::DataType::UInt8 => {
            hash_primitive!(array, UInt8Array);
        }
        arrow::datatypes::DataType::UInt16 => {
            hash_primitive!(array, UInt16Array);
        }
        arrow::datatypes::DataType::UInt32 => {
            hash_primitive!(array, UInt32Array);
        }
        arrow::datatypes::DataType::UInt64 => {
            hash_primitive!(array, UInt64Array);
        }
        // Floating point
        arrow::datatypes::DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            // Handle NaN specially as it's not equal to itself
            let val = arr.value(row);
            if val.is_nan() {
                "NaN".hash(hasher);
            } else {
                val.to_bits().hash(hasher);
            }
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            // Handle NaN specially as it's not equal to itself
            let val = arr.value(row);
            if val.is_nan() {
                "NaN".hash(hasher);
            } else {
                val.to_bits().hash(hasher);
            }
        }
        // Strings
        arrow::datatypes::DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        // Dates
        arrow::datatypes::DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        arrow::datatypes::DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        // Timestamps
        arrow::datatypes::DataType::Timestamp(unit, _) => {
            match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    arr.value(row).hash(hasher);
                },
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    arr.value(row).hash(hasher);
                },
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    arr.value(row).hash(hasher);
                },
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    arr.value(row).hash(hasher);
                },
            }
        }
        // Decimal
        arrow::datatypes::DataType::Decimal128(_, _) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            arr.value(row).hash(hasher);
        }
        arrow::datatypes::DataType::Decimal256(_, _) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            // Use value() which returns i256, then convert to bytes for hashing
            arr.value(row).to_le_bytes().hash(hasher);
        }
        // Boolean
        arrow::datatypes::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        // Binary
        arrow::datatypes::DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        arrow::datatypes::DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        // Fixed-size binary
        arrow::datatypes::DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            arr.value(row).hash(hasher);
        }
        // Dictionary types - hash the underlying value
        arrow::datatypes::DataType::Dictionary(_, value_type) => {
            // For dictionary arrays, we need to get the value from the dictionary
            // and hash it according to its value type
            // We'll handle different key types generically by using the value directly
            let debug_str = format!("dictionary_{}", value_type);
            debug_str.hash(hasher);
            return Err(ExchangeError::PartitionFailed(
                format!("Dictionary type not fully supported for hash partitioning: {:?}", array.data_type())
            ));
        }
        // Other types that don't have direct hash support
        _ => {
            // For unsupported types, we hash their debug representation
            let debug_str = format!("{:?}", array.data_type());
            debug_str.hash(hasher);
            return Err(ExchangeError::PartitionFailed(
                format!("Unsupported data type for hash partitioning: {:?}", array.data_type())
            ));
        }
    }

    Ok(())
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
#[derive(Clone, Debug, thiserror::Error)]
pub enum ExchangeError {
    /// Stage not found.
    #[error("Stage {0} not found")]
    StageNotFound(StageId),
    /// Exchange not found.
    #[error("Exchange {0} not found")]
    ExchangeNotFound(ExchangeId),
    /// Upstream stage output not ready.
    #[error("Upstream stage {0} not ready")]
    UpstreamNotReady(StageId),
    /// Failed to fetch data.
    #[error("Fetch failed: {0}")]
    FetchFailed(String),
    /// Failed to partition data.
    #[error("Partition failed: {0}")]
    PartitionFailed(String),
    /// No batches to process.
    #[error("No batches to process")]
    NoBatches,
    /// Failed to concatenate batches.
    #[error("Concat failed: {0}")]
    ConcatFailed(String),
    /// Column not found during resolution.
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
}

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
    /// Metrics registry for tracking exchange performance.
    metrics_registry: Arc<ExchangeMetricsRegistry>,
}

impl DistributedExchangeRuntime {
    /// Create a new distributed exchange runtime.
    pub fn new(tenant_id: String, connection_pool: Arc<WorkerConnectionPool>) -> Self {
        Self {
            tenant_id,
            connection_pool,
            remote_tickets: RwLock::new(HashMap::new()),
            metrics_registry: Arc::new(ExchangeMetricsRegistry::new()),
        }
    }

    /// Create with a fresh connection pool.
    pub fn with_new_pool(tenant_id: String) -> Self {
        Self::new(tenant_id, Arc::new(WorkerConnectionPool::new()))
    }

    /// Create with a custom metrics registry.
    pub fn with_metrics_registry(
        tenant_id: String,
        connection_pool: Arc<WorkerConnectionPool>,
        metrics_registry: Arc<ExchangeMetricsRegistry>,
    ) -> Self {
        Self {
            tenant_id,
            connection_pool,
            remote_tickets: RwLock::new(HashMap::new()),
            metrics_registry,
        }
    }

    /// Get the metrics registry.
    pub fn metrics_registry(&self) -> &Arc<ExchangeMetricsRegistry> {
        &self.metrics_registry
    }

    /// Get the connection pool for registering workers.
    pub fn connection_pool(&self) -> &Arc<WorkerConnectionPool> {
        &self.connection_pool
    }

    /// Register a result ticket from a stage submission.
    pub async fn register_ticket(
        &self,
        query_id: &str,
        stage_id: StageId,
        worker_id: &WorkerId,
        ticket_bytes: Vec<u8>,
    ) {
        let key = format!("{}:{}:{}", query_id, stage_id, worker_id);
        self.remote_tickets.write().await.insert(key, ticket_bytes);
    }

    /// Get a registered ticket.
    pub async fn get_ticket(
        &self,
        query_id: &str,
        stage_id: StageId,
        worker_id: &WorkerId,
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
        stage_id: StageId,
        source_workers: &[WorkerId],
        _target_worker: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = %stage_id,
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
                        stage_id = %stage_id,
                        worker_id = %worker_id,
                        batches = batches.len(),
                        "Gathered data from worker"
                    );
                    all_batches.extend(batches);
                }
                Err(e) => {
                    error!(
                        query_id = query_id,
                        stage_id = %stage_id,
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
                stage_id = %stage_id,
                "Partial gather success: {} batches collected, {} workers failed",
                all_batches.len(),
                errors.len()
            );
        }

        info!(
            query_id = query_id,
            stage_id = %stage_id,
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
        stage_id: StageId,
        worker_id: &WorkerId,
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
    async fn get_worker_endpoint(&self, worker_id: &WorkerId) -> Result<String, ExchangeError> {
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
        stage_id: StageId,
        source_worker: &WorkerId,
        target_workers: &[WorkerId],
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = %stage_id,
            source = %source_worker,
            targets = ?target_workers,
            "Executing distributed broadcast"
        );

        // Create broadcast metrics
        let mut metrics = crate::ldp::executor::metrics::BroadcastExchangeMetrics::new(
            ExchangeId(stage_id.0), // Using stage_id as exchange_id for this context
            query_id.to_string(),
            source_worker.clone(),
            target_workers.to_vec(),
            256 * 1024 * 1024, // Default broadcast threshold of 256MB
        );

        // Fetch the data from the source
        let batches = self
            .fetch_from_worker(query_id, stage_id, source_worker)
            .await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = batches.iter().map(|b| {
            b.columns().iter().map(|c| c.get_array_memory_size()).sum::<usize>()
        }).sum();

        // Update metrics with data information
        metrics.update_data(total_rows as u64, total_bytes as u64);

        info!(
            query_id = query_id,
            stage_id = %stage_id,
            rows = total_rows,
            bytes = total_bytes,
            targets = target_workers.len(),
            "Broadcast data fetched, ready for distribution"
        );

        // Finalize metrics
        metrics.finalize();
        
        // Log the metrics
        metrics.log_metrics();
        
        // Register metrics in the registry
        self.metrics_registry
            .register_broadcast_metrics(metrics)
            .await;

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
    /// * `column_refs` - Column references to hash for partitioning
    /// * `num_partitions` - Number of partitions to create
    /// * `partition_to_worker` - Mapping from partition ID to target worker
    /// * `local_worker` - The worker requesting its partition of data
    ///
    /// # Returns
    /// Record batches for the local worker's partition(s).
    pub async fn execute_hash_partition(
        &self,
        query_id: &str,
        stage_id: StageId,
        source_workers: &[WorkerId],
        column_refs: &[ColumnRef],
        num_partitions: u32,
        partition_to_worker: &[WorkerId],
        local_worker: &WorkerId,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        info!(
            query_id = query_id,
            stage_id = %stage_id,
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

        // Resolve column references to indices using the first batch's schema
        let schema = all_batches[0].schema();
        let field_refs = resolve_column_indices(column_refs, &schema)?;

        // Find which partitions belong to this local worker
        let local_partitions: Vec<u32> = partition_to_worker
            .iter()
            .enumerate()
            .filter(|(_, w)| *w == local_worker)
            .map(|(i, _)| i as u32)
            .collect();

        debug!(
            query_id = query_id,
            stage_id = %stage_id,
            local_worker = %local_worker,
            local_partitions = ?local_partitions,
            "Filtering to local partitions"
        );

        // Partition each batch and collect the local partitions
        let mut local_batches = Vec::new();

        for batch in all_batches {
            let partitioned = hash_partition_batch(&batch, &field_refs, num_partitions)?;

            for p in &local_partitions {
                let partition_batch = &partitioned[*p as usize];
                if partition_batch.num_rows() > 0 {
                    local_batches.push(partition_batch.clone());
                }
            }
        }

        info!(
            query_id = query_id,
            stage_id = %stage_id,
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
                column_refs,
                partitions,
            } => {
                self.execute_hash_partition(
                    query_id,
                    edge.from_stage,
                    source_workers,
                    column_refs,
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
            .register_ticket("q1", StageId(1), &WorkerId::from("worker-1"), vec![1, 2, 3])
            .await;

        // Retrieve it
        let ticket = runtime.get_ticket("q1", StageId(1), &WorkerId::from("worker-1")).await;
        assert_eq!(ticket, Some(vec![1, 2, 3]));

        // Non-existent ticket returns None
        let missing = runtime.get_ticket("q1", StageId(2), &WorkerId::from("worker-1")).await;
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_distributed_exchange_get_worker_endpoint_missing() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());

        let result = runtime.get_worker_endpoint(&WorkerId::from("unknown-worker")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_distributed_exchange_get_worker_endpoint() {
        let runtime = DistributedExchangeRuntime::with_new_pool("test-tenant".to_string());

        // Register a worker
        runtime
            .connection_pool()
            .register_worker(WorkerId::from("w1"), "http://localhost:50051".to_string())
            .await;

        let result = runtime.get_worker_endpoint(&WorkerId::from("w1")).await;
        assert_eq!(result.unwrap(), "http://localhost:50051");
    }

    #[test]
    fn test_remote_ticket_creation() {
        let ticket = RemoteTicket {
            worker_id: WorkerId::from("worker-1"),
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

    // ========================================================================
    // Column Resolution Tests
    // ========================================================================

    #[test]
    fn test_resolve_column_indices_unqualified() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let refs = vec![
            ColumnRef::unqualified("id"),
            ColumnRef::unqualified("name"),
            ColumnRef::unqualified("age"),
        ];

        let indices = resolve_column_indices(&refs, &schema).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_resolve_column_indices_qualified() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("orders.id", DataType::Int64, false),
            Field::new("orders.customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        let refs = vec![
            ColumnRef::qualified("orders", "id"),
            ColumnRef::qualified("orders", "customer_id"),
        ];

        let indices = resolve_column_indices(&refs, &schema).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_resolve_column_indices_strips_qualifier() {
        // Schema has unqualified column names (common at exchange boundaries)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        // But ColumnRef still has table qualifier from upstream
        let refs = vec![
            ColumnRef::qualified("sales", "id"),
            ColumnRef::qualified("sales", "region"),
        ];

        // Should still resolve by column name alone
        let indices = resolve_column_indices(&refs, &schema).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_resolve_column_indices_case_insensitive() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("CustomerID", DataType::Int64, false),
            Field::new("OrderDate", DataType::Utf8, false),
        ]));

        let refs = vec![
            ColumnRef::unqualified("customerid"),  // lowercase
            ColumnRef::unqualified("ORDERDATE"),   // uppercase
        ];

        let indices = resolve_column_indices(&refs, &schema).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn test_resolve_column_indices_not_found() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let refs = vec![
            ColumnRef::unqualified("id"),
            ColumnRef::unqualified("nonexistent"),  // This doesn't exist
        ];

        let result = resolve_column_indices(&refs, &schema);
        assert!(result.is_err());
        
        if let Err(ExchangeError::ColumnNotFound(msg)) = result {
            assert!(msg.contains("nonexistent"));
            assert!(msg.contains("Available columns"));
        } else {
            panic!("Expected ColumnNotFound error");
        }
    }

    #[test]
    fn test_resolve_column_indices_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let refs: Vec<crate::sql::logical_plan::ColumnRef> = vec![];
        let indices = resolve_column_indices(&refs, &schema).unwrap();
        assert_eq!(indices, Vec::<u32>::new());
    }
}
