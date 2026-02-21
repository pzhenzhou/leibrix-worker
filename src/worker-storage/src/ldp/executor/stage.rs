//! Stage execution for LDP plans.
//!
//! This module handles submitting stages to workers and executing them locally.

use crate::engine::duckdb::SharedDatabase;
use crate::ldp::executor::monitor::StageExecutionMonitor;
use crate::ldp::{Stage, StageId, StageOutput, WorkerId};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Ticket representing a stage's output stream.
///
/// In a distributed setting, this would be a Flight ticket.
/// For local execution, it wraps the output batches directly.
#[derive(Clone, Debug)]
pub struct StageTicket {
    /// Query identifier for ticket scoping.
    pub query_id: String,
    /// Stage that produced this output.
    pub stage_id: StageId,
    /// Worker that executed the stage.
    pub worker_id: WorkerId,
    /// Partition number (for partitioned outputs).
    pub partition: Option<u32>,
    /// Ticket identifier for Flight DoGet (in distributed mode).
    pub ticket_id: String,
}

impl StageTicket {
    /// Create a new stage ticket.
    pub fn new(query_id: String, stage_id: StageId, worker_id: WorkerId) -> Self {
        let ticket_id = format!("stage_{}_worker_{}", stage_id, worker_id);
        Self {
            query_id,
            stage_id,
            worker_id,
            partition: None,
            ticket_id,
        }
    }

    /// Create a partitioned stage ticket.
    pub fn partitioned(
        query_id: String,
        stage_id: StageId,
        worker_id: WorkerId,
        partition: u32,
    ) -> Self {
        let ticket_id = format!("stage_{}_worker_{}_p{}", stage_id, worker_id, partition);
        Self {
            query_id,
            stage_id,
            worker_id,
            partition: Some(partition),
            ticket_id,
        }
    }
}

/// Collection of tickets from a stage execution across workers.
#[derive(Clone, Debug, Default)]
pub struct StageTickets {
    /// Tickets from each worker that executed the stage.
    /// For non-partitioned output: one ticket per worker.
    /// For partitioned output: multiple tickets per worker.
    pub tickets: Vec<StageTicket>,
}

impl StageTickets {
    /// Create empty tickets collection.
    pub fn new() -> Self {
        Self { tickets: vec![] }
    }

    /// Add a ticket.
    pub fn add(&mut self, ticket: StageTicket) {
        self.tickets.push(ticket);
    }

    /// Get all tickets.
    pub fn all(&self) -> &[StageTicket] {
        &self.tickets
    }

    /// Get tickets for a specific worker.
    pub fn for_worker(&self, worker_id: &WorkerId) -> Vec<&StageTicket> {
        self.tickets
            .iter()
            .filter(|t| &t.worker_id == worker_id)
            .collect()
    }

    /// Get tickets for a specific partition.
    pub fn for_partition(&self, partition: u32) -> Vec<&StageTicket> {
        self.tickets
            .iter()
            .filter(|t| t.partition == Some(partition))
            .collect()
    }
}

/// Result of stage execution.
#[derive(Debug)]
pub struct StageResult {
    /// Stage that was executed.
    pub stage_id: StageId,
    /// Output tickets.
    pub tickets: StageTickets,
    /// Execution statistics.
    pub stats: StageExecutionStats,
}

/// Statistics from stage execution.
#[derive(Clone, Debug, Default)]
pub struct StageExecutionStats {
    /// Total rows produced.
    pub rows_produced: u64,
    /// Total bytes produced.
    pub bytes_produced: u64,
    /// Execution time in milliseconds.
    pub execution_time_ms: u64,
}

/// Streaming RecordBatch output for pipelined stage execution.
///
/// This wraps a tokio mpsc receiver that yields RecordBatches incrementally,
/// enabling concurrent stage execution with bounded memory buffers.
pub struct RecordBatchStream {
    /// Receiver for record batches.
    receiver: mpsc::Receiver<Result<RecordBatch, StageExecutionError>>,
    /// Schema of the record batches in this stream.
    schema: Arc<Schema>,
}

impl RecordBatchStream {
    /// Create a new record batch stream.
    pub fn new(
        receiver: mpsc::Receiver<Result<RecordBatch, StageExecutionError>>,
        schema: Arc<Schema>,
    ) -> Self {
        Self { receiver, schema }
    }

    /// Get the next record batch from the stream.
    ///
    /// Returns None when the stream is exhausted.
    pub async fn next(&mut self) -> Option<Result<RecordBatch, StageExecutionError>> {
        self.receiver.recv().await
    }

    /// Get the schema of batches in this stream.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Collect all remaining batches into a vector.
    ///
    /// This consumes the stream and blocks until all batches are received.
    pub async fn collect(mut self) -> Result<Vec<RecordBatch>, StageExecutionError> {
        let mut batches = Vec::new();
        while let Some(batch_result) = self.next().await {
            batches.push(batch_result?);
        }
        Ok(batches)
    }
}

/// Error during stage execution.
#[derive(Clone, Debug, thiserror::Error)]
pub enum StageExecutionError {
    /// Stage not found in plan.
    #[error("Stage {0} not found")]
    StageNotFound(StageId),
    /// Worker not available or unreachable.
    #[error("Worker {worker_id} unavailable: {detail}")]
    WorkerUnavailable { worker_id: WorkerId, detail: String },
    /// SQL/stage execution failed.
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    /// Output limits exceeded.
    #[error("{limit_type} limit exceeded: {actual} > {limit}")]
    LimitsExceeded {
        limit_type: String,
        actual: u64,
        limit: u64,
    },
    /// Exchange input not ready.
    #[error("Input {0} not ready")]
    InputNotReady(String),
}

/// Trait for stage execution backends.
///
/// This abstracts over local DuckDB execution vs. distributed Flight-based execution.
#[allow(async_fn_in_trait)]
pub trait StageExecutor: Send + Sync {
    /// Submit a stage for execution on specified workers.
    ///
    /// # Arguments
    /// * `query_id` - The query identifier for ticket registration
    /// * `stage` - The stage to execute
    /// * `inputs` - Resolved input data for exchange inputs
    ///
    /// # Returns
    /// Tickets for retrieving the stage output.
    async fn submit_stage(
        &self,
        query_id: &str,
        stage: &Stage,
        inputs: HashMap<String, Vec<RecordBatch>>,
    ) -> Result<StageTickets, StageExecutionError>;

    /// Submit a stage for streaming execution with incremental inputs.
    ///
    /// This enables pipelined execution where downstream stages can begin
    /// processing before upstream stages complete.
    ///
    /// # Arguments
    /// * `query_id` - The query identifier for ticket registration
    /// * `stage` - The stage to execute
    /// * `input_streams` - Streaming inputs from upstream stages
    ///
    /// # Returns
    /// A stream of output batches that can be consumed incrementally.
    async fn submit_stage_streaming(
        &self,
        query_id: &str,
        stage: &Stage,
        input_streams: HashMap<String, RecordBatchStream>,
    ) -> Result<RecordBatchStream, StageExecutionError>;

    /// Fetch output from a stage ticket.
    ///
    /// # Arguments
    /// * `ticket` - The ticket to fetch
    ///
    /// # Returns
    /// Arrow record batches from the stage output.
    async fn fetch_output(
        &self,
        ticket: &StageTicket,
    ) -> Result<Vec<RecordBatch>, StageExecutionError>;
}

/// Local stage executor using DuckDB.
///
/// This executor runs stages locally on a single DuckDB instance.
/// Useful for testing and single-node execution.
pub struct LocalStageExecutor {
    /// Cached stage outputs (for local execution without Flight).
    outputs: DashMap<String, Vec<RecordBatch>>,
    /// Worker DuckDB databases used to execute local catalog scans.
    worker_databases: HashMap<WorkerId, Arc<SharedDatabase>>,
}

impl LocalStageExecutor {
    /// Create a new local executor.
    pub fn new() -> Self {
        Self {
            outputs: DashMap::new(),
            worker_databases: HashMap::new(),
        }
    }

    /// Create a local executor backed by per-worker DuckDB databases.
    pub fn with_databases(worker_databases: HashMap<WorkerId, Arc<SharedDatabase>>) -> Self {
        Self {
            outputs: DashMap::new(),
            worker_databases,
        }
    }

    /// Execute a stage's SQL with monitoring.
    ///
    /// This function:
    /// 1. Creates a DuckDB connection
    /// 2. Registers exchange input tables from the `inputs` HashMap
    /// 3. Executes SQL via DuckDB's Arrow query interface
    /// 4. Monitors execution against limits
    /// 5. Collects and returns output batches
    async fn execute_sql_with_monitor(
        &self,
        worker_id: &WorkerId,
        stage_sql: &str,
        inputs: &HashMap<String, Vec<RecordBatch>>,
        requires_local_catalog: bool,
        monitor: &StageExecutionMonitor,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        use duckdb::Connection;

        // Handle empty SQL (e.g., placeholder stages)
        if stage_sql.is_empty() {
            return Ok(vec![]);
        }

        // Execute stage SQL against the target worker's shared database when available.
        // For local-catalog stages (table/macro scans), never fall back to another worker:
        // that would hide placement bugs and produce non-deterministic behavior.
        let target_db = self.worker_databases.get(worker_id).cloned();

        let result = if let Some(shared_db) = target_db {
            let conn = shared_db.get().map_err(|e| {
                StageExecutionError::ExecutionFailed(format!(
                    "Failed to get DuckDB connection for worker '{}': {}",
                    worker_id, e
                ))
            })?;
            Self::run_sql_with_inputs(&conn, stage_sql, inputs)
        } else {
            if requires_local_catalog {
                return Err(StageExecutionError::ExecutionFailed(format!(
                    "Missing worker database for '{}' while stage requires local catalog access",
                    worker_id
                )));
            }
            let conn = Connection::open_in_memory().map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to open DuckDB: {}", e))
            })?;
            Self::run_sql_with_inputs(&conn, stage_sql, inputs)
        };

        // Handle execution result
        match result {
            Ok(batches) => {
                // Update monitor with output counts
                let rows_produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let bytes_produced: u64 = batches
                    .iter()
                    .map(|b| b.get_array_memory_size() as u64)
                    .sum();

                monitor.increment_rows_produced(rows_produced);
                monitor.increment_bytes_produced(bytes_produced);

                // Check limits
                monitor.check_limits().map_err(|e| {
                    StageExecutionError::ExecutionFailed(format!(
                        "Limit exceeded during execution: {}",
                        e
                    ))
                })?;

                Ok(batches)
            }
            Err(e) => Err(e),
        }
    }

    /// Register exchange inputs, execute SQL, and clean up temp tables.
    fn run_sql_with_inputs(
        conn: &duckdb::Connection,
        stage_sql: &str,
        inputs: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        use crate::engine::duckdb::arrow_utils::{drop_temp_table, register_arrow_batches};

        let mut registered_tables = Vec::new();
        for (table_name, batches) in inputs {
            if !batches.is_empty() {
                register_arrow_batches(conn, table_name, batches).map_err(|e| {
                    StageExecutionError::ExecutionFailed(format!(
                        "Failed to register table '{}': {:#}",
                        table_name, e
                    ))
                })?;
                registered_tables.push(table_name.clone());
            } else {
                // Exchange returned a truly empty Vec (no batches at all, no
                // schema).  After the exchange-level schema-preservation fix
                // this path should be extremely rare — it only triggers when
                // zero upstream tickets existed for the exchange.  Instead of
                // creating a wrong-schema stub (`SELECT 1 WHERE FALSE`), skip
                // the table so that any SQL referencing it fails with a clear
                // "table does not exist" error rather than a silent column
                // mismatch.
                tracing::warn!(
                    table_name = table_name.as_str(),
                    "Exchange input has no batches and no schema — \
                     skipping table registration (SQL referencing this \
                     table will fail)"
                );
            }
        }

        // Wrap SQL execution in an immediately-invoked closure so that `?`
        // returns from the closure, not from `run_sql_with_inputs`.  This
        // guarantees the cleanup loop below always runs, even when prepare or
        // query_arrow fails, which is essential for SharedDatabase (pooled)
        // connections where a leaked temp table would persist across stages.
        let result: Result<Vec<RecordBatch>, StageExecutionError> = (|| {
            let mut stmt = conn.prepare(stage_sql).map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to prepare SQL: {}", e))
            })?;

            let rows = stmt.query_arrow([]).map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Failed to execute SQL: {}", e))
            })?;
            let schema = rows.get_schema();

            let batches: Vec<RecordBatch> = rows.collect();
            if batches.is_empty() {
                Ok(vec![RecordBatch::new_empty(schema)])
            } else {
                Ok(batches)
            }
        })();

        // Always clean up, regardless of whether execution succeeded or failed.
        for table_name in &registered_tables {
            let _ = drop_temp_table(conn, table_name);
        }

        result
    }

    /// Check if output batches exceed stage limits.
    fn check_output_limits(
        batches: &[RecordBatch],
        limits: &crate::ldp::StageLimits,
    ) -> Result<(), StageExecutionError> {
        let rows_produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes_produced: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        if rows_produced > limits.max_rows_output {
            return Err(StageExecutionError::LimitsExceeded {
                limit_type: "rows".to_string(),
                actual: rows_produced,
                limit: limits.max_rows_output,
            });
        }

        if bytes_produced > limits.max_bytes_output {
            return Err(StageExecutionError::LimitsExceeded {
                limit_type: "bytes".to_string(),
                actual: bytes_produced,
                limit: limits.max_bytes_output,
            });
        }

        Ok(())
    }
}

impl Default for LocalStageExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl StageExecutor for LocalStageExecutor {
    async fn submit_stage(
        &self,
        query_id: &str,
        stage: &Stage,
        inputs: HashMap<String, Vec<RecordBatch>>,
    ) -> Result<StageTickets, StageExecutionError> {
        let mut tickets = StageTickets::new();
        let requires_local_catalog = stage
            .inputs
            .iter()
            .any(|input| matches!(input, crate::ldp::StageInput::LocalCatalog));

        // Create a monitor for this stage execution
        let monitor = Arc::new(StageExecutionMonitor::new(stage.limits.clone()));

        // Execute on each target worker (locally, we simulate this)
        for worker_id in &stage.target_workers {
            // Execute the SQL stage with monitoring
            let output = self
                .execute_sql_with_monitor(
                    worker_id,
                    &stage.stage_sql,
                    &inputs,
                    requires_local_catalog,
                    &monitor,
                )
                .await?;

            // Check output limits
            Self::check_output_limits(&output, &stage.limits)?;

            // Create ticket and store output
            match &stage.output {
                StageOutput::Stream => {
                    let ticket =
                        StageTicket::new(query_id.to_string(), stage.stage_id, worker_id.clone());

                    // Store output for later retrieval
                    self.outputs.insert(ticket.ticket_id.clone(), output);

                    tickets.add(ticket);
                }
                StageOutput::Partitioned {
                    partitions,
                    column_refs,
                } => {
                    // Partition the output data by hash
                    let partitioned_data =
                        Self::partition_batches(&output, *partitions, column_refs)?;

                    // Create tickets and store each partition
                    for (partition_id, partition_batches) in
                        partitioned_data.into_iter().enumerate()
                    {
                        let part_ticket = StageTicket::partitioned(
                            query_id.to_string(),
                            stage.stage_id,
                            worker_id.clone(),
                            partition_id as u32,
                        );
                        tickets.add(part_ticket.clone());

                        // Store partitioned output
                        self.outputs
                            .insert(part_ticket.ticket_id.clone(), partition_batches);
                    }
                }
            };
        }

        Ok(tickets)
    }

    async fn submit_stage_streaming(
        &self,
        query_id: &str,
        stage: &Stage,
        input_streams: HashMap<String, RecordBatchStream>,
    ) -> Result<RecordBatchStream, StageExecutionError> {
        // Get the output schema (we'll need it for the stream)
        // For now, we'll execute and get the schema from the first batch
        // In a real implementation, we could extract it from the stage SQL
        let (tx, rx) = mpsc::channel(32);

        // Clone stage for the async task
        let stage_clone = stage.clone();
        let _query_id = query_id.to_string();

        // Spawn background task to execute stage with streaming inputs
        tokio::spawn(async move {
            // First, collect all input batches from streams
            let mut inputs = HashMap::new();
            for (table_name, mut stream) in input_streams {
                let mut batches = Vec::new();
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => batches.push(batch),
                        Err(e) => {
                            // Send error downstream
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
                inputs.insert(table_name, batches);
            }

            // Execute SQL with DuckDB
            match execute_stage_batches(&stage_clone, &inputs).await {
                Ok(output_batches) => {
                    // Stream output batches to downstream
                    for batch in output_batches {
                        if tx.send(Ok(batch)).await.is_err() {
                            // Downstream closed, stop execution
                            break;
                        }
                    }
                }
                Err(e) => {
                    // Send error downstream
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        // Create a dummy schema - in practice we'd extract this from SQL metadata or first batch
        // For now, return a stream that will yield batches with their own schemas
        let schema = Arc::new(Schema::empty());

        Ok(RecordBatchStream::new(rx, schema))
    }

    async fn fetch_output(
        &self,
        ticket: &StageTicket,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        self.outputs
            .get(&ticket.ticket_id)
            .map(|v| v.clone())
            .ok_or_else(|| StageExecutionError::InputNotReady(ticket.ticket_id.clone()))
    }
}

/// Helper function to execute a stage with batched inputs.
async fn execute_stage_batches(
    stage: &Stage,
    inputs: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>, StageExecutionError> {
    use crate::engine::duckdb::arrow_utils::{drop_temp_table, register_arrow_batches};
    use duckdb::Connection;

    // Handle empty SQL
    if stage.stage_sql.is_empty() {
        return Ok(vec![]);
    }

    // Create DuckDB connection
    let conn = Connection::open_in_memory().map_err(|e| {
        StageExecutionError::ExecutionFailed(format!("Failed to open DuckDB: {}", e))
    })?;

    // Register all exchange inputs as temporary tables
    let mut registered_tables = Vec::new();
    for (table_name, batches) in inputs {
        if !batches.is_empty() {
            register_arrow_batches(&conn, table_name, batches).map_err(|e| {
                StageExecutionError::ExecutionFailed(format!(
                    "Failed to register table '{}': {:#}",
                    table_name, e
                ))
            })?;
            registered_tables.push(table_name.clone());
        } else {
            // Exchange returned a truly empty Vec — skip table registration.
            // See run_sql_with_inputs() for the detailed rationale.
            tracing::warn!(
                table_name = table_name.as_str(),
                "Exchange input has no batches and no schema — \
                 skipping table registration (SQL referencing this \
                 table will fail)"
            );
        }
    }

    // Wrap SQL execution in an immediately-invoked closure so that `?`
    // returns from the closure, not from `execute_stage_batches`.  This
    // guarantees the cleanup loop below always runs even when SQL fails.
    let result: Result<Vec<RecordBatch>, StageExecutionError> = (|| {
        let mut stmt = conn.prepare(&stage.stage_sql).map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to prepare SQL: {}", e))
        })?;

        let rows = stmt.query_arrow([]).map_err(|e| {
            StageExecutionError::ExecutionFailed(format!("Failed to execute SQL: {}", e))
        })?;
        let schema = rows.get_schema();

        let batches: Vec<RecordBatch> = rows.collect();
        if batches.is_empty() {
            Ok(vec![RecordBatch::new_empty(schema)])
        } else {
            Ok(batches)
        }
    })();

    // Always clean up, regardless of whether execution succeeded or failed.
    for table_name in &registered_tables {
        let _ = drop_temp_table(&conn, table_name);
    }

    result
}

/// Helper function to partition record batches by hash.
///
/// This implements a simple round-robin partitioning strategy.
/// In production, this would hash based on partition keys, but for now
/// we distribute rows evenly across partitions.
impl LocalStageExecutor {
    fn partition_batches(
        batches: &[RecordBatch],
        num_partitions: u32,
        column_refs: &[crate::sql::logical_plan::ColumnRef],
    ) -> Result<Vec<Vec<RecordBatch>>, StageExecutionError> {
        use crate::ldp::executor::exchange::{hash_partition_batch, resolve_column_indices};
        use arrow::compute::concat_batches;

        if batches.is_empty() {
            // Return empty partitions
            return Ok(vec![vec![]; num_partitions as usize]);
        }

        // Concatenate all batches to simplify partitioning
        let schema = batches[0].schema();
        let combined = if batches.len() == 1 {
            batches[0].clone()
        } else {
            concat_batches(&schema, batches).map_err(|e| {
                StageExecutionError::ExecutionFailed(format!(
                    "Failed to concatenate batches for partitioning: {}",
                    e
                ))
            })?
        };

        let total_rows = combined.num_rows();
        if total_rows == 0 {
            return Ok(vec![vec![]; num_partitions as usize]);
        }

        // Resolve column references to field indices
        let field_indices = resolve_column_indices(column_refs, &schema).map_err(|e| {
            StageExecutionError::ExecutionFailed(format!(
                "Failed to resolve column references: {}",
                e
            ))
        })?;

        // Use the same hash partitioning logic as exchanges to keep consistency.
        let partitions = hash_partition_batch(&combined, &field_indices, num_partitions)
            .map_err(|e| {
                StageExecutionError::ExecutionFailed(format!("Partitioning failed: {}", e))
            })?
            .into_iter()
            .map(|batch| {
                if batch.num_rows() > 0 {
                    vec![batch]
                } else {
                    vec![]
                }
            })
            .collect();

        Ok(partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::{Exchange, ExchangeEdge, ExchangeId, LdpPlan, Stage, StageInput, StageLimits};

    fn create_test_plan() -> LdpPlan {
        let mut plan = LdpPlan::new("test_query".into(), "coordinator".into());

        // Stage 0: Leaf scan
        plan.stages.push(Stage {
            stage_id: StageId(0),
            target_workers: vec!["w1".into(), "w2".into()],
            inputs: vec![StageInput::LocalCatalog],
            output: StageOutput::Stream,
            stage_sql: String::new(),
            limits: StageLimits::default(),
        });

        // Stage 1: Final aggregation
        plan.stages.push(Stage {
            stage_id: StageId(1),
            target_workers: vec!["coordinator".into()],
            inputs: vec![StageInput::ExchangeInput {
                exchange_id: ExchangeId(0),
                table_name: "__exchange_0".into(),
            }],
            output: StageOutput::Stream,
            stage_sql: String::new(),
            limits: StageLimits::default(),
        });

        // Exchange: Gather from Stage 0 to Stage 1
        plan.edges.push(ExchangeEdge {
            exchange_id: ExchangeId(0),
            kind: Exchange::Gather {
                target: "coordinator".into(),
            },
            from_stage: StageId(0),
            to_stage: StageId(1),
            partition_to_worker: vec![],
        });

        plan
    }

    #[test]
    fn test_topological_sort() {
        let plan = create_test_plan();
        let sorted = plan.topological_order();

        // Stage 0 should come before Stage 1
        assert_eq!(sorted, vec![StageId(0), StageId(1)]);
    }

    #[test]
    fn test_stage_tickets() {
        let mut tickets = StageTickets::new();

        tickets.add(StageTicket::new(
            "test_query".to_string(),
            StageId(0),
            "w1".into(),
        ));
        tickets.add(StageTicket::new(
            "test_query".to_string(),
            StageId(0),
            "w2".into(),
        ));
        tickets.add(StageTicket::partitioned(
            "test_query".to_string(),
            StageId(1),
            "coordinator".into(),
            0,
        ));
        tickets.add(StageTicket::partitioned(
            "test_query".to_string(),
            StageId(1),
            "coordinator".into(),
            1,
        ));

        assert_eq!(tickets.all().len(), 4);
        assert_eq!(tickets.for_worker(&WorkerId::from("w1")).len(), 1);
        assert_eq!(tickets.for_partition(0).len(), 1);
    }

    #[tokio::test]
    async fn test_local_executor() {
        let executor = LocalStageExecutor::new();

        let stage = Stage {
            stage_id: StageId(0),
            target_workers: vec!["local".into()],
            inputs: vec![StageInput::LocalCatalog],
            output: StageOutput::Stream,
            stage_sql: String::new(),
            limits: StageLimits::default(),
        };

        let result = executor
            .submit_stage("test_query", &stage, HashMap::new())
            .await
            .unwrap();

        assert_eq!(result.tickets.len(), 1);
    }

    // =========================================================================
    // Temp-table cleanup tests
    //
    // These tests verify that exchange temp tables are always dropped after
    // stage SQL execution, regardless of whether execution succeeds or fails.
    // The invariant is especially important when the caller reuses a pooled
    // (SharedDatabase) connection across multiple stages: a leaked temp table
    // would persist on that connection and pollute subsequent stage executions.
    // =========================================================================

    /// After a successful execution, the temp table must no longer exist.
    #[test]
    fn test_run_sql_cleans_up_temp_tables_on_success() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use duckdb::Connection;
        use std::sync::Arc;

        let conn = Connection::open_in_memory().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![batch]);

        let result = LocalStageExecutor::run_sql_with_inputs(
            &conn,
            "SELECT id FROM \"__exchange_0\"",
            &inputs,
        );

        assert!(result.is_ok(), "SQL execution should have succeeded");

        // The temp table must be gone so the pooled connection is clean.
        assert!(
            conn.prepare("SELECT 1 FROM \"__exchange_0\" LIMIT 1")
                .is_err(),
            "Temp table __exchange_0 must be cleaned up after successful execution"
        );
    }

    /// After a failed execution (bad SQL), the temp table must still be dropped.
    #[test]
    fn test_run_sql_cleans_up_temp_tables_on_error() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use duckdb::Connection;
        use std::sync::Arc;

        let conn = Connection::open_in_memory().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![batch]);

        // Invalid SQL: prepare will fail, which previously caused an early
        // `?` return that bypassed the cleanup loop.
        let result =
            LocalStageExecutor::run_sql_with_inputs(&conn, "THIS IS NOT VALID SQL !!!", &inputs);

        assert!(result.is_err(), "Invalid SQL should have returned an error");

        // Despite the error, the temp table must be cleaned up.
        assert!(
            conn.prepare("SELECT 1 FROM \"__exchange_0\" LIMIT 1")
                .is_err(),
            "Temp table __exchange_0 must be cleaned up even after a failed execution"
        );
    }

    // =========================================================================
    // execute_stage_batches tests
    //
    // These tests exercise the free async helper `execute_stage_batches`, which
    // opens its own in-memory DuckDB connection per call.  They verify that the
    // function produces correct output on the happy path and returns a proper
    // Err (rather than panicking) when SQL is invalid — both cases exercise the
    // IIFE-based cleanup pattern introduced in the temp-table-leak fix.
    // =========================================================================

    /// `execute_stage_batches` — successful execution returns the expected rows.
    #[tokio::test]
    async fn test_execute_stage_batches_success() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let stage = Stage {
            stage_id: StageId(0),
            target_workers: vec!["local".into()],
            inputs: vec![],
            output: StageOutput::default(),
            stage_sql: "SELECT id FROM \"__exchange_0\"".to_string(),
            limits: StageLimits::default(),
        };

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![batch]);

        let result = execute_stage_batches(&stage, &inputs).await;

        assert!(result.is_ok(), "Expected Ok but got: {:?}", result);
        let total_rows: usize = result.unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Expected 3 rows in output");
    }

    /// `execute_stage_batches` — SQL error returns Err without panicking.
    ///
    /// Before the IIFE fix an early `?` return inside the execution block would
    /// skip the cleanup loop.  Although the connection is local so the leak is
    /// transient, the function must still surface the error as `Err` rather than
    /// panicking or silently succeeding.
    #[tokio::test]
    async fn test_execute_stage_batches_error_on_invalid_sql() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        let stage = Stage {
            stage_id: StageId(0),
            target_workers: vec!["local".into()],
            inputs: vec![],
            output: StageOutput::default(),
            stage_sql: "THIS IS NOT VALID SQL !!".to_string(),
            limits: StageLimits::default(),
        };

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![batch]);

        let result = execute_stage_batches(&stage, &inputs).await;

        assert!(result.is_err(), "Expected Err for invalid SQL");
    }

    // =========================================================================
    // Empty exchange schema-preservation tests
    //
    // Verify that a 0-row RecordBatch with the correct schema (produced by the
    // exchange-level fix) allows downstream SQL to reference columns by name,
    // and that a truly empty Vec<RecordBatch> (no schema) is handled gracefully
    // by skipping table registration.
    // =========================================================================

    /// A 0-row batch carrying the correct schema should produce a table with
    /// the right columns so that `SELECT col FROM __exchange_0` works.
    #[test]
    fn test_run_sql_with_empty_schema_preserving_batch() {
        use arrow::datatypes::{DataType, Field, Schema};
        use duckdb::Connection;
        use std::sync::Arc;

        let conn = Connection::open_in_memory().unwrap();

        // Build a 0-row batch that carries a 2-column schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, false),
        ]));
        let empty_batch = RecordBatch::new_empty(schema);

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![empty_batch]);

        // SQL references columns by name — this would fail with the old
        // `SELECT 1 WHERE FALSE` stub because neither "region" nor "amount"
        // existed in that schema.
        let result = LocalStageExecutor::run_sql_with_inputs(
            &conn,
            "SELECT region, amount FROM \"__exchange_0\"",
            &inputs,
        );

        assert!(
            result.is_ok(),
            "SQL with column references should succeed on a 0-row schema-preserving table: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Expected 0 rows from an empty exchange");
    }

    /// A truly empty Vec (no batches, no schema) should be skipped rather than
    /// creating a wrong-schema stub table.
    #[test]
    fn test_run_sql_with_truly_empty_input_skips_table() {
        use duckdb::Connection;

        let conn = Connection::open_in_memory().unwrap();

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![]);

        // SQL references the exchange table — it should fail because the empty
        // input was skipped (no table created).
        let result = LocalStageExecutor::run_sql_with_inputs(
            &conn,
            "SELECT region FROM \"__exchange_0\"",
            &inputs,
        );

        assert!(
            result.is_err(),
            "SQL should fail when exchange has no batches and no schema"
        );

        let err_msg = format!("{:?}", result.err().unwrap());
        // DuckDB should report the table as non-existent rather than a
        // column mismatch on a wrong-schema stub.
        assert!(
            err_msg.contains("__exchange_0"),
            "Error should mention the missing table name, got: {}",
            err_msg
        );
    }

    /// `execute_stage_batches` with a 0-row schema-preserving batch.
    #[tokio::test]
    async fn test_execute_stage_batches_empty_schema_preserving_batch() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("total", DataType::Float64, true),
        ]));
        let empty_batch = RecordBatch::new_empty(schema);

        let stage = Stage {
            stage_id: StageId(0),
            target_workers: vec!["local".into()],
            inputs: vec![],
            output: StageOutput::default(),
            stage_sql: "SELECT customer_id, total FROM \"__exchange_0\"".to_string(),
            limits: StageLimits::default(),
        };

        let mut inputs = HashMap::new();
        inputs.insert("__exchange_0".to_string(), vec![empty_batch]);

        let result = execute_stage_batches(&stage, &inputs).await;

        assert!(
            result.is_ok(),
            "Schema-preserving empty batch should allow column-referencing SQL: {:?}",
            result.err()
        );
        let total_rows: usize = result.unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
