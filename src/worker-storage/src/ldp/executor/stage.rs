//! Stage execution for LDP plans.
//!
//! This module handles submitting stages to workers and executing them locally.

use crate::ldp::{LdpPlan, Stage, StageId, StageInput, StageLimits, StageOutput, WorkerId};
use crate::ldp::executor::monitor::{SharedStageExecutionMonitor, StageExecutionMonitor};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

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
    pub fn partitioned(query_id: String, stage_id: StageId, worker_id: WorkerId, partition: u32) -> Self {
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
    pub fn for_worker(&self, worker_id: &str) -> Vec<&StageTicket> {
        self.tickets
            .iter()
            .filter(|t| t.worker_id == worker_id)
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

/// Error during stage execution.
#[derive(Clone, Debug)]
pub enum StageExecutionError {
    /// Stage not found in plan.
    StageNotFound(StageId),
    /// Worker not available.
    WorkerUnavailable(WorkerId),
    /// Substrait plan execution failed.
    ExecutionFailed(String),
    /// Output limits exceeded.
    LimitsExceeded {
        limit_type: String,
        actual: u64,
        limit: u64,
    },
    /// Exchange input not ready.
    InputNotReady(String),
}

impl std::fmt::Display for StageExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageExecutionError::StageNotFound(id) => write!(f, "Stage {} not found", id),
            StageExecutionError::WorkerUnavailable(w) => write!(f, "Worker {} unavailable", w),
            StageExecutionError::ExecutionFailed(msg) => write!(f, "Execution failed: {}", msg),
            StageExecutionError::LimitsExceeded {
                limit_type,
                actual,
                limit,
            } => write!(
                f,
                "{} limit exceeded: {} > {}",
                limit_type, actual, limit
            ),
            StageExecutionError::InputNotReady(name) => {
                write!(f, "Input {} not ready", name)
            }
        }
    }
}

impl std::error::Error for StageExecutionError {}

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

    /// Fetch output from a stage ticket.
    ///
    /// # Arguments
    /// * `ticket` - The ticket to fetch
    ///
    /// # Returns
    /// Arrow record batches from the stage output.
    async fn fetch_output(&self, ticket: &StageTicket) -> Result<Vec<RecordBatch>, StageExecutionError>;
}

/// Local stage executor using DuckDB.
///
/// This executor runs stages locally on a single DuckDB instance.
/// Useful for testing and single-node execution.
pub struct LocalStageExecutor {
    /// Cached stage outputs (for local execution without Flight).
    outputs: tokio::sync::RwLock<HashMap<String, Vec<RecordBatch>>>,
}

impl LocalStageExecutor {
    /// Create a new local executor.
    pub fn new() -> Self {
        Self {
            outputs: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Execute a stage's Substrait plan with DuckDB.
    ///
    /// This function:
    /// 1. Creates a DuckDB connection
    /// 2. Registers exchange input tables from the `inputs` HashMap
    /// 3. Executes the Substrait plan via `from_substrait()`
    /// 4. Collects and returns output batches
    async fn execute_substrait(
        &self,
        substrait_bytes: &[u8],
        inputs: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        use crate::engine::duckdb::substrait::{
            duckdb_from_substrait_batches, drop_temp_table, register_arrow_batches,
        };
        use duckdb::Connection;

        // Handle empty substrait plan (e.g., placeholder stages)
        if substrait_bytes.is_empty() {
            return Ok(vec![]);
        }

        // Create a DuckDB connection for this execution
        let conn = Connection::open_in_memory()
            .map_err(|e| StageExecutionError::ExecutionFailed(format!("Failed to open DuckDB: {}", e)))?;

        // Register all exchange inputs as temporary tables
        let mut registered_tables = Vec::new();
        for (table_name, batches) in inputs {
            if !batches.is_empty() {
                register_arrow_batches(&conn, table_name, batches)
                    .map_err(|e| StageExecutionError::ExecutionFailed(
                        format!("Failed to register table '{}': {}", table_name, e)
                    ))?;
                registered_tables.push(table_name.clone());
            }
        }

        // Execute the Substrait plan
        let result = duckdb_from_substrait_batches(&conn, substrait_bytes);

        // Clean up registered tables (optional, connection will be dropped anyway)
        for table_name in &registered_tables {
            let _ = drop_temp_table(&conn, table_name);
        }

        // Handle execution result
        match result {
            Ok(batches) => Ok(batches),
            Err(e) => {
                // Check if it's a substrait extension not available error
                let err_msg = e.to_string();
                if err_msg.contains("substrait") && err_msg.contains("extension") {
                    Err(StageExecutionError::ExecutionFailed(
                        "DuckDB substrait extension not available. Install with: INSTALL substrait".into()
                    ))
                } else {
                    Err(StageExecutionError::ExecutionFailed(err_msg))
                }
            }
        }
    }

    /// Execute a stage's Substrait plan with monitoring.
    ///
    /// This function:
    /// 1. Creates a DuckDB connection
    /// 2. Registers exchange input tables from the `inputs` HashMap
    /// 3. Executes the Substrait plan via `from_substrait()`
    /// 4. Monitors execution against limits
    /// 5. Collects and returns output batches
    async fn execute_substrait_with_monitor(
        &self,
        substrait_bytes: &[u8],
        inputs: &HashMap<String, Vec<RecordBatch>>,
        monitor: &StageExecutionMonitor,
    ) -> Result<Vec<RecordBatch>, StageExecutionError> {
        use crate::engine::duckdb::substrait::{
            duckdb_from_substrait_batches, drop_temp_table, register_arrow_batches,
        };
        use duckdb::Connection;

        // Handle empty substrait plan (e.g., placeholder stages)
        if substrait_bytes.is_empty() {
            return Ok(vec![]);
        }

        // Create a DuckDB connection for this execution
        let conn = Connection::open_in_memory()
            .map_err(|e| StageExecutionError::ExecutionFailed(format!("Failed to open DuckDB: {}", e)))?;

        // Register all exchange inputs as temporary tables
        let mut registered_tables = Vec::new();
        for (table_name, batches) in inputs {
            if !batches.is_empty() {
                register_arrow_batches(&conn, table_name, batches)
                    .map_err(|e| StageExecutionError::ExecutionFailed(
                        format!("Failed to register table '{}': {}", table_name, e)
                    ))?;
                registered_tables.push(table_name.clone());
            }
        }

        // Execute the Substrait plan
        let result = duckdb_from_substrait_batches(&conn, substrait_bytes);

        // Clean up registered tables (optional, connection will be dropped anyway)
        for table_name in &registered_tables {
            let _ = drop_temp_table(&conn, table_name);
        }

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
                monitor.check_limits().map_err(|e| StageExecutionError::ExecutionFailed(
                    format!("Limit exceeded during execution: {}", e)
                ))?;
                
                Ok(batches)
            },
            Err(e) => {
                // Check if it's a substrait extension not available error
                let err_msg = e.to_string();
                if err_msg.contains("substrait") && err_msg.contains("extension") {
                    Err(StageExecutionError::ExecutionFailed(
                        "DuckDB substrait extension not available. Install with: INSTALL substrait".into()
                    ))
                } else {
                    Err(StageExecutionError::ExecutionFailed(err_msg))
                }
            }
        }
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

        // Create a monitor for this stage execution
        let monitor = Arc::new(StageExecutionMonitor::new(stage.limits.clone()));

        // Execute on each target worker (locally, we simulate this)
        for worker_id in &stage.target_workers {
            // Execute the substrait plan with monitoring
            let output = self
                .execute_substrait_with_monitor(&stage.substrait_plan, &inputs, &monitor)
                .await?;

            // Check output limits
            Self::check_output_limits(&output, &stage.limits)?;

            // Create ticket and store output
            match &stage.output {
                StageOutput::Stream => {
                    let ticket = StageTicket::new(query_id.to_string(), stage.stage_id, worker_id.clone());
                    
                    // Store output for later retrieval
                    {
                        let mut outputs = self.outputs.write().await;
                        outputs.insert(ticket.ticket_id.clone(), output);
                    }

                    tickets.add(ticket);
                }
                StageOutput::Partitioned { partitions, field_refs } => {
                    // Partition the output data by hash
                    let partitioned_data = Self::partition_batches(&output, *partitions, field_refs)?;
                    
                    // Create tickets and store each partition
                    for (partition_id, partition_batches) in partitioned_data.into_iter().enumerate() {
                        let part_ticket =
                            StageTicket::partitioned(query_id.to_string(), stage.stage_id, worker_id.clone(), partition_id as u32);
                        tickets.add(part_ticket.clone());

                        // Store partitioned output
                        let mut outputs = self.outputs.write().await;
                        outputs.insert(part_ticket.ticket_id.clone(), partition_batches);
                    }
                }
            };
        }

        Ok(tickets)
    }

    async fn fetch_output(&self, ticket: &StageTicket) -> Result<Vec<RecordBatch>, StageExecutionError> {
        let outputs = self.outputs.read().await;
        outputs
            .get(&ticket.ticket_id)
            .cloned()
            .ok_or_else(|| StageExecutionError::InputNotReady(ticket.ticket_id.clone()))
    }
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
        field_refs: &[u32],
    ) -> Result<Vec<Vec<RecordBatch>>, StageExecutionError> {
        use crate::ldp::executor::exchange::hash_partition_batch;
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
            concat_batches(&schema, batches)
                .map_err(|e| StageExecutionError::ExecutionFailed(
                    format!("Failed to concatenate batches for partitioning: {}", e)
                ))?
        };

        let total_rows = combined.num_rows();
        if total_rows == 0 {
            return Ok(vec![vec![]; num_partitions as usize]);
        }

        // Use the same hash partitioning logic as exchanges to keep consistency.
        let partitions = hash_partition_batch(&combined, field_refs, num_partitions)
            .map_err(|e| StageExecutionError::ExecutionFailed(format!("Partitioning failed: {}", e)))?
            .into_iter()
            .map(|batch| if batch.num_rows() > 0 { vec![batch] } else { vec![] })
            .collect();

        Ok(partitions)
    }
}

/// Topologically sort stages for execution order.
///
/// Returns stages in order such that all dependencies are executed first.
pub fn topological_sort(plan: &LdpPlan) -> Vec<StageId> {
    use std::collections::{HashSet, VecDeque};

    let mut in_degree: HashMap<StageId, usize> = HashMap::new();
    let mut adjacency: HashMap<StageId, Vec<StageId>> = HashMap::new();

    // Initialize all stages
    for stage in &plan.stages {
        in_degree.entry(stage.stage_id).or_insert(0);
        adjacency.entry(stage.stage_id).or_default();
    }

    // Build graph from edges
    for edge in &plan.edges {
        adjacency
            .entry(edge.from_stage)
            .or_default()
            .push(edge.to_stage);
        *in_degree.entry(edge.to_stage).or_insert(0) += 1;
    }

    // Kahn's algorithm
    let mut queue: VecDeque<StageId> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut sorted = Vec::new();

    while let Some(stage_id) = queue.pop_front() {
        sorted.push(stage_id);

        if let Some(neighbors) = adjacency.get(&stage_id) {
            for &neighbor in neighbors {
                if let Some(deg) = in_degree.get_mut(&neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
    }

    sorted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::{Exchange, ExchangeEdge, LdpPlan, Stage, StageInput, StageLimits};

    fn create_test_plan() -> LdpPlan {
        let mut plan = LdpPlan::new("test_query".into(), "coordinator".into());

        // Stage 0: Leaf scan
        plan.stages.push(Stage {
            stage_id: 0,
            target_workers: vec!["w1".into(), "w2".into()],
            inputs: vec![StageInput::LocalCatalog],
            output: StageOutput::Stream,
            substrait_plan: vec![],
            limits: StageLimits::default(),
        });

        // Stage 1: Final aggregation
        plan.stages.push(Stage {
            stage_id: 1,
            target_workers: vec!["coordinator".into()],
            inputs: vec![StageInput::ExchangeInput {
                exchange_id: 0,
                table_name: "__exchange_0".into(),
            }],
            output: StageOutput::Stream,
            substrait_plan: vec![],
            limits: StageLimits::default(),
        });

        // Exchange: Gather from Stage 0 to Stage 1
        plan.edges.push(ExchangeEdge {
            exchange_id: 0,
            kind: Exchange::Gather {
                target: "coordinator".into(),
            },
            from_stage: 0,
            to_stage: 1,
            partition_to_worker: vec![],
        });

        plan
    }

    #[test]
    fn test_topological_sort() {
        let plan = create_test_plan();
        let sorted = topological_sort(&plan);

        // Stage 0 should come before Stage 1
        assert_eq!(sorted, vec![0, 1]);
    }

    #[test]
    fn test_stage_tickets() {
        let mut tickets = StageTickets::new();

        tickets.add(StageTicket::new("test_query".to_string(), 0, "w1".into()));
        tickets.add(StageTicket::new("test_query".to_string(), 0, "w2".into()));
        tickets.add(StageTicket::partitioned("test_query".to_string(), 1, "coordinator".into(), 0));
        tickets.add(StageTicket::partitioned("test_query".to_string(), 1, "coordinator".into(), 1));

        assert_eq!(tickets.all().len(), 4);
        assert_eq!(tickets.for_worker("w1").len(), 1);
        assert_eq!(tickets.for_partition(0).len(), 1);
    }

    #[tokio::test]
    async fn test_local_executor() {
        let executor = LocalStageExecutor::new();

        let stage = Stage {
            stage_id: 0,
            target_workers: vec!["local".into()],
            inputs: vec![StageInput::LocalCatalog],
            output: StageOutput::Stream,
            substrait_plan: vec![],
            limits: StageLimits::default(),
        };

        let result = executor
            .submit_stage("test_query", &stage, HashMap::new())
            .await
            .unwrap();

        assert_eq!(result.tickets.len(), 1);
    }
}
