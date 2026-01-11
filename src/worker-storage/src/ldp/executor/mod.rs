//! LDP Executor module.
//!
//! This module implements the runtime execution of LDP plans:
//! - Stage execution on workers
//! - Exchange data movement between stages
//! - Overall plan coordination
//!
//! # Architecture
//! ```text
//! LdpCoordinator (orchestrates full query lifecycle)
//!   ├── SQL Transformation + Admission Control
//!   ├── LDP Planning (SQL → Substrait → LDP)
//!   └── LdpExecutor (stage execution)
//!         ├── StageExecutor (trait)
//!         │     ├── LocalStageExecutor (single-node DuckDB)
//!         │     └── FlightStageExecutor (distributed via Arrow Flight)
//!         └── ExchangeRuntime
//!               ├── Gather (collect to one)
//!               ├── Broadcast (replicate to all)
//!               └── HashPartition (redistribute by hash)
//! ```

pub mod coordinator;
pub mod exchange;
pub mod flight;
pub mod stage;

pub use exchange::{
    concat_record_batches, hash_partition_batch, DistributedExchangeRuntime, ExchangeError,
    ExchangeRuntime, RemoteTicket,
};
pub use flight::{FlightStageExecutor, LdpFlightClient, WorkerConnection, WorkerConnectionPool};
pub use stage::{
    LocalStageExecutor, StageExecutionError, StageExecutionStats, StageExecutor, StageResult,
    StageTicket, StageTickets,
};
pub use coordinator::{
    CoordinatorConfig, CoordinatorError, LdpCoordinator, QueryResult, QueryStats, StageStatus,
};

use crate::ldp::{LdpPlan, StageId};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Main LDP executor that coordinates stage execution and exchanges.
pub struct LdpExecutor<E: StageExecutor> {
    /// Stage executor backend.
    stage_executor: Arc<E>,
    /// Exchange runtime.
    exchange_runtime: ExchangeRuntime<E>,
}

impl<E: StageExecutor> LdpExecutor<E> {
    /// Create a new LDP executor.
    pub fn new(stage_executor: Arc<E>) -> Self {
        let exchange_runtime = ExchangeRuntime::new(stage_executor.clone());
        Self {
            stage_executor,
            exchange_runtime,
        }
    }

    /// Execute an LDP plan and return the final result.
    ///
    /// Executes stages in topological order, running exchanges between them.
    ///
    /// # Arguments
    /// * `plan` - The LDP plan to execute
    ///
    /// # Returns
    /// The final output batches from the root stage.
    pub async fn execute(&self, plan: &LdpPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
        // Track completed stage outputs
        let mut stage_outputs: HashMap<StageId, StageTickets> = HashMap::new();

        // Get execution order (topologically sorted)
        let execution_order = plan.topological_order();

        // Execute each stage in order
        for stage_id in execution_order {
            let stage = plan
                .get_stage(stage_id)
                .ok_or(ExecutionError::StageNotFound(stage_id))?;

            // Resolve inputs from upstream exchanges
            let inputs = self
                .exchange_runtime
                .resolve_inputs(plan, stage_id, &stage_outputs)
                .await
                .map_err(|e| ExecutionError::ExchangeFailed(format!("{}", e)))?;

            // Submit stage for execution with query_id
            let tickets = self
                .stage_executor
                .submit_stage(&plan.query_id, stage, inputs)
                .await
                .map_err(|e| ExecutionError::StageFailed(stage_id, format!("{}", e)))?;

            stage_outputs.insert(stage_id, tickets);
        }

        // Fetch final output from root stage
        let root_tickets = stage_outputs
            .get(&plan.root_stage)
            .ok_or(ExecutionError::StageNotFound(plan.root_stage))?;

        let mut final_batches = Vec::new();
        for ticket in root_tickets.all() {
            let batches = self
                .stage_executor
                .fetch_output(ticket)
                .await
                .map_err(|e| ExecutionError::StageFailed(plan.root_stage, format!("{}", e)))?;
            final_batches.extend(batches);
        }

        Ok(final_batches)
    }

    /// Execute a single stage (for testing).
    pub async fn execute_stage(
        &self,
        plan: &LdpPlan,
        stage_id: StageId,
        inputs: HashMap<String, Vec<RecordBatch>>,
    ) -> Result<StageTickets, ExecutionError> {
        let stage = plan
            .get_stage(stage_id)
            .ok_or(ExecutionError::StageNotFound(stage_id))?;

        self.stage_executor
            .submit_stage(&plan.query_id, stage, inputs)
            .await
            .map_err(|e| ExecutionError::StageFailed(stage_id, format!("{}", e)))
    }
}

/// Error during LDP execution.
#[derive(Clone, Debug)]
pub enum ExecutionError {
    /// Stage not found in plan.
    StageNotFound(StageId),
    /// Stage execution failed.
    StageFailed(StageId, String),
    /// Exchange execution failed.
    ExchangeFailed(String),
    /// Plan is invalid.
    InvalidPlan(String),
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::StageNotFound(id) => write!(f, "Stage {} not found", id),
            ExecutionError::StageFailed(id, msg) => {
                write!(f, "Stage {} execution failed: {}", id, msg)
            }
            ExecutionError::ExchangeFailed(msg) => write!(f, "Exchange failed: {}", msg),
            ExecutionError::InvalidPlan(msg) => write!(f, "Invalid plan: {}", msg),
        }
    }
}

impl std::error::Error for ExecutionError {}

/// Convenience function to create and run an executor with the local backend.
pub async fn execute_local(plan: &LdpPlan) -> Result<Vec<RecordBatch>, ExecutionError> {
    let executor = Arc::new(LocalStageExecutor::new());
    let ldp_executor = LdpExecutor::new(executor);
    ldp_executor.execute(plan).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_executor_creation() {
        let executor = Arc::new(LocalStageExecutor::new());
        let _ldp_executor = LdpExecutor::new(executor);
    }
}
