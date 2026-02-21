//! LDP Coordinator for end-to-end query execution.
//!
//! The coordinator is responsible for the complete query lifecycle:
//! 1. SQL parsing and admission control
//! 2. LDP planning (SQL → Substrait → LDP)
//! 3. Stage scheduling to workers
//! 4. Exchange execution between stages
//! 5. Result streaming back to client
//!
//! # Architecture
//! ```text
//! Client SQL Query
//!        │
//!        ▼
//! ┌──────────────────────────────────────────────┐
//! │              LdpCoordinator                   │
//! │  ┌─────────────────────────────────────────┐ │
//! │  │ 1. Admission Control                    │ │
//! │  │    - Reject recursive CTEs              │ │
//! │  │    - Require time-range predicates      │ │
//! │  └─────────────────────────────────────────┘ │
//! │                    │                         │
//! │                    ▼                         │
//! │  ┌─────────────────────────────────────────┐ │
//! │  │ 2. SQL Transformation                   │ │
//! │  │    - Rewrite to table macros            │ │
//! │  │    - Extract date ranges                │ │
//! │  └─────────────────────────────────────────┘ │
//! │                    │                         │
//! │                    ▼                         │
//! │  ┌─────────────────────────────────────────┐ │
//! │  │ 3. LDP Planning                         │ │
//! │  │    - SQL → Substrait (DuckDB)           │ │
//! │  │    - Annotate distributions             │ │
//! │  │    - Cut into stages                    │ │
//! │  └─────────────────────────────────────────┘ │
//! │                    │                         │
//! │                    ▼                         │
//! │  ┌─────────────────────────────────────────┐ │
//! │  │ 4. Stage Execution                      │ │
//! │  │    - Topological ordering               │ │
//! │  │    - Submit to workers via Flight       │ │
//! │  │    - Execute exchanges                  │ │
//! │  └─────────────────────────────────────────┘ │
//! │                    │                         │
//! │                    ▼                         │
//! │  ┌─────────────────────────────────────────┐ │
//! │  │ 5. Result Streaming                     │ │
//! │  │    - Fetch final stage output           │ │
//! │  │    - Stream Arrow batches to client     │ │
//! │  └─────────────────────────────────────────┘ │
//! └──────────────────────────────────────────────┘
//!        │
//!        ▼
//! Arrow RecordBatch Stream
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use duckdb::Connection;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::engine::duckdb::SharedDatabase;
use crate::ldp::executor::monitor::SharedStageExecutionMonitor;
use crate::ldp::executor::stage::{
    RecordBatchStream, StageExecutionError, StageExecutor, StageTickets,
};
use crate::ldp::executor::{
    DistributedExchangeRuntime, ExchangeRuntime, ExecutionError, FlightStageExecutor,
    LocalStageExecutor, WorkerConnectionPool,
};
use crate::ldp::planner::metadata::{InMemoryMetadata, Metadata};
use crate::ldp::planner::pipeline::{plan_ldp, PipelineError};
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::planner::storage_metadata::ClusterMetadata;
use crate::ldp::{Exchange, ExchangeId, LdpPlan, Stage, StageId, WorkerId};
use crate::sql::{AdmissionError, RegisteredDataset, SqlTransformer};
use arrow::datatypes::{DataType, Schema};

// ============================================================================
// Type Aliases
// ============================================================================

/// Result type for inter-stage record batch channels.
type StageBatchResult = Result<RecordBatch, StageExecutionError>;

/// Map from exchange id to the (sender, receiver) channel pair connecting stages.
type StageChannelMap = HashMap<
    ExchangeId,
    (
        mpsc::Sender<StageBatchResult>,
        mpsc::Receiver<StageBatchResult>,
    ),
>;

// ============================================================================
// Coordinator Error
// ============================================================================

/// Errors that can occur during query coordination.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    /// Query was rejected by admission control.
    #[error("Query rejected: {0}")]
    AdmissionRejected(#[from] AdmissionError),
    /// SQL transformation failed.
    #[error("SQL transformation failed: {0}")]
    TransformFailed(String),
    /// LDP planning failed.
    #[error("LDP planning failed: {0}")]
    PlanningFailed(#[from] PipelineError),
    /// Stage execution failed.
    #[error("Execution failed: {0}")]
    ExecutionFailed(#[from] ExecutionError),
    /// DuckDB connection error.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    /// Worker communication failed.
    #[error("Worker {0} failed: {1}")]
    WorkerFailed(WorkerId, String),
    /// Query timeout.
    #[error("Query timeout after {0:?}")]
    Timeout(Duration),
    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<StageExecutionError> for CoordinatorError {
    fn from(err: StageExecutionError) -> Self {
        CoordinatorError::ExecutionFailed(ExecutionError::ExchangeFailed(err.to_string()))
    }
}

// ============================================================================
// Query Result
// ============================================================================

/// Result of a coordinated query execution.
#[derive(Debug)]
pub struct QueryResult {
    /// Query identifier.
    pub query_id: String,
    /// Result batches.
    pub batches: Vec<RecordBatch>,
    /// Execution statistics.
    pub stats: QueryStats,
}

/// Statistics about query execution.
#[derive(Clone, Debug, Default)]
pub struct QueryStats {
    /// Total execution time.
    pub total_time: Duration,
    /// Planning time.
    pub planning_time: Duration,
    /// Execution time.
    pub execution_time: Duration,
    /// Number of stages executed.
    pub stages_executed: usize,
    /// Total rows produced.
    pub rows_produced: u64,
    /// Total bytes produced.
    pub bytes_produced: u64,
    /// Workers involved.
    pub workers_used: Vec<WorkerId>,
}

// ============================================================================
// Coordinator Configuration
// ============================================================================

/// Configuration for the LDP coordinator.
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Query timeout.
    pub query_timeout: Duration,
    /// Maximum concurrent stages.
    pub max_concurrent_stages: usize,
    /// Maximum retries per stage.
    pub max_stage_retries: u32,
    /// Planner policy.
    pub planner_policy: PlannerPolicy,
    /// Enable distributed execution (vs local-only).
    pub distributed: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            tenant_id: "default".to_string(),
            query_timeout: Duration::from_secs(300), // 5 minutes
            max_concurrent_stages: 16,
            max_stage_retries: 3,
            planner_policy: PlannerPolicy::default(),
            distributed: false,
        }
    }
}

impl CoordinatorConfig {
    /// Create a new coordinator config with the given tenant ID.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            ..Default::default()
        }
    }

    /// Enable distributed execution.
    pub fn with_distributed(mut self, enabled: bool) -> Self {
        self.distributed = enabled;
        self
    }

    /// Set query timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Set planner policy.
    pub fn with_policy(mut self, policy: PlannerPolicy) -> Self {
        self.planner_policy = policy;
        self
    }

    /// Set maximum stage retries.
    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.max_stage_retries = max_retries;
        self
    }
}

// ============================================================================
// Stage Execution Status
// ============================================================================

/// Status of a stage during execution.
#[derive(Clone, Debug)]
pub enum StageStatus {
    /// Stage is pending execution.
    Pending,
    /// Stage is currently running.
    Running { started_at: Instant },
    /// Stage completed successfully.
    Completed {
        tickets: StageTickets,
        duration: Duration,
    },
    /// Stage failed.
    Failed { error: String, retries: u32 },
}

// ============================================================================
// LDP Coordinator
// ============================================================================

/// Main coordinator for distributed query execution.
///
/// The coordinator manages the complete lifecycle of a query from SQL
/// to result streaming.
// `duckdb::Connection` is not `Sync`, so `Arc<RwLock<Connection>>` is not
// `Send + Sync`. The `RwLock` serialises access; the `Arc` provides shared
// ownership required for the coordinator to be stored behind `Arc` in tests.
#[allow(clippy::arc_with_non_send_sync)]
pub struct LdpCoordinator<M: Metadata = ClusterMetadata> {
    /// Configuration.
    config: CoordinatorConfig,
    /// SQL transformer for query rewriting.
    transformer: RwLock<SqlTransformer>,
    /// Metadata provider.
    metadata: Arc<M>,
    /// DuckDB connection for planning.
    conn: Arc<RwLock<Connection>>,
    /// Worker connection pool (for distributed mode).
    connection_pool: Arc<WorkerConnectionPool>,
    /// Stage execution status tracking.
    stage_status: DashMap<(String, StageId), StageStatus>,
    /// Active stage monitors for cancellation.
    stage_monitors: DashMap<(String, StageId), SharedStageExecutionMonitor>,
    /// Worker-local DuckDB databases used by local execution in tests.
    worker_databases: DashMap<WorkerId, Arc<SharedDatabase>>,
}

#[allow(clippy::arc_with_non_send_sync)]
impl<M: Metadata + Send + Sync + 'static> LdpCoordinator<M> {
    /// Create a new coordinator with in-memory DuckDB.
    pub fn new(config: CoordinatorConfig, metadata: Arc<M>) -> Result<Self, CoordinatorError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| CoordinatorError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            config,
            transformer: RwLock::new(SqlTransformer::new()),
            metadata,
            conn: Arc::new(RwLock::new(conn)),
            connection_pool: Arc::new(WorkerConnectionPool::new()),
            stage_status: DashMap::new(),
            stage_monitors: DashMap::new(),
            worker_databases: DashMap::new(),
        })
    }

    /// Create a coordinator with an existing DuckDB connection.
    pub fn with_connection(config: CoordinatorConfig, metadata: Arc<M>, conn: Connection) -> Self {
        Self {
            config,
            transformer: RwLock::new(SqlTransformer::new()),
            metadata,
            conn: Arc::new(RwLock::new(conn)),
            connection_pool: Arc::new(WorkerConnectionPool::new()),
            stage_status: DashMap::new(),
            stage_monitors: DashMap::new(),
            worker_databases: DashMap::new(),
        }
    }

    /// Register a dataset for SQL transformation.
    pub async fn register_dataset(&self, dataset: RegisteredDataset) {
        self.transformer.write().await.register_dataset(dataset);
    }

    /// Register a dataset schema for planning (creates planning macro in coordinator's DuckDB).
    ///
    /// This method creates a dummy table macro on the coordinator's DuckDB instance
    /// that provides schema information for Substrait generation. The macro returns
    /// no rows (WHERE FALSE) since the coordinator doesn't hold actual data.
    ///
    /// In production, Gateway would call this method to provide schema metadata
    /// from the Control Plane before executing a query.
    ///
    /// # Arguments
    /// * `dataset_id` - The logical dataset name (e.g., "lineitem", "orders")
    /// * `schema` - The Arrow schema of the dataset
    ///
    /// # Example
    /// ```ignore
    /// coordinator.register_dataset_schema("lineitem", lineitem_schema).await?;
    /// ```
    pub async fn register_dataset_schema(
        &self,
        dataset_id: &str,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), CoordinatorError> {
        let conn = self.conn.write().await;

        // Generate column definitions from schema
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| {
                let col_name = field.name();
                let col_type = arrow_type_to_duckdb_type(field.data_type());
                format!("CAST(NULL AS {}) AS {}", col_type, col_name)
            })
            .collect();

        let columns_sql = columns.join(", ");

        // Create a macro that returns the schema but no rows
        let macro_sql = format!(
            "CREATE OR REPLACE MACRO scan_{}(start_date, end_date) AS TABLE (SELECT {} WHERE FALSE)",
            dataset_id, columns_sql
        );

        conn.execute_batch(&macro_sql).map_err(|e| {
            CoordinatorError::ConnectionFailed(format!(
                "Failed to register schema for dataset '{}': {}",
                dataset_id, e
            ))
        })?;

        Ok(())
    }

    /// Register a remote worker for distributed execution.
    pub async fn register_worker(&self, worker_id: &str, endpoint: &str) {
        self.connection_pool
            .register_worker(WorkerId::from(worker_id), endpoint.to_string())
            .await;
    }

    /// Register a worker's shared DuckDB database for local test execution.
    pub fn register_worker_database(&self, worker_id: WorkerId, database: Arc<SharedDatabase>) {
        self.worker_databases.insert(worker_id, database);
    }

    /// Get the metadata provider.
    pub fn metadata(&self) -> &Arc<M> {
        &self.metadata
    }

    /// Get the worker connection pool.
    pub fn connection_pool(&self) -> &Arc<WorkerConnectionPool> {
        &self.connection_pool
    }

    // ========================================================================
    // Main Query Execution
    // ========================================================================

    /// Execute a SQL query and return results.
    ///
    /// This is the main entry point for query execution. It performs:
    /// 1. Admission control
    /// 2. SQL transformation
    /// 3. LDP planning
    /// 4. Stage execution
    /// 5. Result collection
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute.
    ///
    /// # Returns
    /// * `Ok(QueryResult)` - Query completed successfully.
    /// * `Err(CoordinatorError)` - Query failed.
    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult, CoordinatorError> {
        let total_start = Instant::now();
        let query_id = generate_query_id();

        info!(query_id = %query_id, "Starting query execution");

        // Step 1: Admission control
        let transformer = self.transformer.read().await;
        transformer
            .admit(sql)
            .map_err(CoordinatorError::AdmissionRejected)?;
        debug!(query_id = %query_id, "Query admitted");

        // Step 2: SQL transformation
        let transform_result = transformer
            .transform(sql)
            .map_err(|e| CoordinatorError::TransformFailed(e.to_string()))?;

        let transformed_sql = &transform_result.transformed_sql;
        debug!(
            query_id = %query_id,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed"
        );

        // Drop the transformer lock before planning
        drop(transformer);

        // Step 3: LDP planning
        let planning_start = Instant::now();
        let plan = self.plan_query(transformed_sql, &query_id).await?;
        let planning_time = planning_start.elapsed();

        info!(
            query_id = %query_id,
            stages = plan.stages.len(),
            planning_time_ms = planning_time.as_millis(),
            "LDP plan generated"
        );

        // Step 4: Execute plan
        let execution_start = Instant::now();
        let (batches, workers_used) = self.execute_plan(&plan, &query_id).await?;
        let execution_time = execution_start.elapsed();

        // Calculate stats
        let rows_produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes_produced: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        let stats = QueryStats {
            total_time: total_start.elapsed(),
            planning_time,
            execution_time,
            stages_executed: plan.stages.len(),
            rows_produced,
            bytes_produced,
            workers_used,
        };

        info!(
            query_id = %query_id,
            total_time_ms = stats.total_time.as_millis(),
            rows = rows_produced,
            stages = stats.stages_executed,
            "Query completed"
        );

        Ok(QueryResult {
            query_id,
            batches,
            stats,
        })
    }

    /// Execute a SQL query with a pre-generated query ID.
    pub async fn execute_query_with_id(
        &self,
        sql: &str,
        query_id: &str,
    ) -> Result<QueryResult, CoordinatorError> {
        let total_start = Instant::now();

        info!(query_id = %query_id, "Starting query execution");

        // Step 1: Admission control
        let transformer = self.transformer.read().await;
        transformer
            .admit(sql)
            .map_err(CoordinatorError::AdmissionRejected)?;

        // Step 2: SQL transformation
        let transform_result = transformer
            .transform(sql)
            .map_err(|e| CoordinatorError::TransformFailed(e.to_string()))?;
        let transformed_sql = &transform_result.transformed_sql;
        drop(transformer);

        // Step 3: LDP planning
        let planning_start = Instant::now();
        let plan = self.plan_query(transformed_sql, query_id).await?;
        let planning_time = planning_start.elapsed();

        // Step 4: Execute plan
        let execution_start = Instant::now();
        let (batches, workers_used) = self.execute_plan(&plan, query_id).await?;
        let execution_time = execution_start.elapsed();

        // Calculate stats
        let rows_produced: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes_produced: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        let stats = QueryStats {
            total_time: total_start.elapsed(),
            planning_time,
            execution_time,
            stages_executed: plan.stages.len(),
            rows_produced,
            bytes_produced,
            workers_used,
        };

        Ok(QueryResult {
            query_id: query_id.to_string(),
            batches,
            stats,
        })
    }

    // ========================================================================
    // Planning
    // ========================================================================

    /// Plan a SQL query into an LDP plan.
    async fn plan_query(&self, sql: &str, query_id: &str) -> Result<LdpPlan, CoordinatorError> {
        let plan = plan_ldp(
            sql,
            self.metadata.as_ref(),
            &self.config.planner_policy,
            query_id,
        )?;
        Ok(plan)
    }

    /// Plan without transformation (for already-transformed SQL or testing).
    pub async fn plan_raw(&self, sql: &str, query_id: &str) -> Result<LdpPlan, CoordinatorError> {
        self.plan_query(sql, query_id).await
    }

    // ========================================================================
    // Execution
    // ========================================================================

    /// Execute an LDP plan.
    async fn execute_plan(
        &self,
        plan: &LdpPlan,
        query_id: &str,
    ) -> Result<(Vec<RecordBatch>, Vec<WorkerId>), CoordinatorError> {
        // Check if streaming pipeline is enabled
        if self.config.planner_policy.enable_streaming_pipeline {
            // Try streaming execution with fallback to batch
            match self.execute_pipelined(plan, query_id).await {
                Ok((batches, workers)) => {
                    info!(
                        query_id = %query_id,
                        stages = plan.stages.len(),
                        "Streaming pipeline execution succeeded"
                    );
                    return Ok((batches, workers));
                }
                Err(e) => {
                    warn!(
                        query_id = %query_id,
                        error = %e,
                        "Streaming pipeline failed, falling back to batch execution"
                    );
                    // Fall through to batch execution
                }
            }
        }

        // Batch execution (original path)
        if self.config.distributed {
            self.execute_distributed(plan, query_id).await
        } else {
            self.execute_local(plan, query_id).await
        }
    }

    /// Compute execution levels for parallel stage execution.
    ///
    /// This groups stages by dependency levels, allowing stages at the same level
    /// to be executed in parallel since they don't depend on each other.
    fn compute_execution_levels(&self, plan: &LdpPlan) -> Vec<Vec<StageId>> {
        use std::collections::{HashMap, VecDeque};

        // Calculate in-degrees for each stage (number of dependencies)
        let mut in_degree: HashMap<StageId, usize> = HashMap::new();

        // Initialize all stages with 0 in-degree
        for stage in &plan.stages {
            in_degree.insert(stage.stage_id, 0);
        }

        // Count dependencies for each stage
        for edge in &plan.edges {
            *in_degree.get_mut(&edge.to_stage).unwrap_or(&mut 0) += 1;
        }

        // Find stages with no dependencies (in-degree = 0)
        let mut queue: VecDeque<StageId> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(&stage_id, _)| stage_id)
            .collect();

        let mut levels = Vec::new();

        // Process stages in topological order, grouping by level
        while !queue.is_empty() {
            let current_level_size = queue.len();
            let mut current_level = Vec::new();

            // Process all stages at the current level
            for _ in 0..current_level_size {
                if let Some(stage_id) = queue.pop_front() {
                    current_level.push(stage_id);

                    // Reduce in-degree of dependent stages
                    for edge in &plan.edges {
                        if edge.from_stage == stage_id {
                            let dependent_stage = edge.to_stage;
                            if let Some(degree) = in_degree.get_mut(&dependent_stage) {
                                *degree -= 1;

                                // If in-degree becomes 0, this stage can be processed in the next level
                                if *degree == 0 {
                                    queue.push_back(dependent_stage);
                                }
                            }
                        }
                    }
                }
            }

            // Defensive check: if we have stages remaining but couldn't process any this round,
            // there's a cycle in the DAG (should never happen in valid plans)
            if current_level.is_empty() && !queue.is_empty() {
                panic!(
                    "Cycle detected in stage dependencies. Remaining stages: {:?}",
                    queue.iter().collect::<Vec<_>>()
                );
            }

            levels.push(current_level);
        }

        levels
    }

    /// Execute plan locally (single-node).
    ///
    /// For stages with exchange inputs, resolves inputs **per-worker** so that
    /// each worker receives the correct subset of data (important for hash
    /// partition exchanges where each worker must see different partitions).
    async fn execute_local(
        &self,
        plan: &LdpPlan,
        query_id: &str,
    ) -> Result<(Vec<RecordBatch>, Vec<WorkerId>), CoordinatorError> {
        let databases: HashMap<WorkerId, Arc<SharedDatabase>> = self
            .worker_databases
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let executor = Arc::new(LocalStageExecutor::with_databases(databases));
        let exchange_runtime = Arc::new(ExchangeRuntime::new(executor.clone()));

        let mut stage_outputs: HashMap<StageId, StageTickets> = HashMap::new();
        let mut workers_used: Vec<WorkerId> = Vec::new();

        // Execute stages in parallel levels based on dependencies
        let execution_levels = self.compute_execution_levels(plan);

        for level_stages in execution_levels {
            // Execute all stages at this level concurrently
            let level_futures = level_stages.iter().map(|&stage_id| {
                let executor = executor.clone();
                let exchange_runtime = exchange_runtime.clone();
                let plan_clone = plan.clone();
                let query_id = query_id.to_string();
                let stage_outputs_clone = stage_outputs.clone();
                async move {
                    let stage = plan_clone
                        .get_stage(stage_id)
                        .ok_or(ExecutionError::StageNotFound(stage_id))?
                        .clone();

                    // Check if stage has exchange inputs (requires per-worker resolution)
                    let has_exchange_inputs = stage
                        .inputs
                        .iter()
                        .any(|input| matches!(input, crate::ldp::StageInput::ExchangeInput { .. }));

                    let tickets = if has_exchange_inputs {
                        // Per-worker input resolution: each worker gets its own
                        // subset of exchange data (critical for hash partition).
                        let mut all_tickets = StageTickets::new();
                        for worker_id in &stage.target_workers {
                            let worker_inputs = exchange_runtime
                                .resolve_inputs_for_worker(
                                    &plan_clone,
                                    stage_id,
                                    worker_id,
                                    &stage_outputs_clone,
                                )
                                .await
                                .map_err(|e: crate::ldp::executor::exchange::ExchangeError| {
                                    ExecutionError::ExchangeFailed(e.to_string())
                                })?;

                            // Create a single-worker stage for this worker
                            let worker_stage = Stage {
                                stage_id: stage.stage_id,
                                target_workers: vec![worker_id.clone()],
                                inputs: stage.inputs.clone(),
                                output: stage.output.clone(),
                                stage_sql: stage.stage_sql.clone(),
                                limits: stage.limits.clone(),
                            };

                            let worker_tickets = self
                                .execute_stage_with_retry(
                                    &*executor,
                                    &worker_stage,
                                    worker_inputs,
                                    &query_id,
                                )
                                .await?;

                            for t in worker_tickets.all() {
                                all_tickets.add(t.clone());
                            }
                        }
                        all_tickets
                    } else {
                        // No exchange inputs: resolve once (LocalCatalog only)
                        let inputs = exchange_runtime
                            .resolve_inputs(&plan_clone, stage_id, &stage_outputs_clone)
                            .await
                            .map_err(|e: crate::ldp::executor::exchange::ExchangeError| {
                                ExecutionError::ExchangeFailed(e.to_string())
                            })?;

                        self.execute_stage_with_retry(&*executor, &stage, inputs, &query_id)
                            .await?
                    };

                    Ok::<(StageId, StageTickets), CoordinatorError>((stage_id, tickets))
                }
            });

            let level_results = futures_util::future::join_all(level_futures).await;

            // Process results and update stage_outputs
            for stage_result in level_results {
                let (stage_id, tickets) = match stage_result {
                    Ok(value) => value,
                    Err(e) => {
                        // Stage failed - cancel all stages in this query to prevent orphaned tasks
                        warn!(
                            query_id = %query_id,
                            error = %e,
                            "Stage failed during parallel execution, cancelling query"
                        );
                        // Best effort cleanup - ignore cancellation errors
                        let _ = self.cancel_query(query_id).await;
                        return Err(e);
                    }
                };

                for ticket in tickets.all() {
                    if !workers_used.contains(&ticket.worker_id) {
                        workers_used.push(ticket.worker_id.clone());
                    }
                }

                self.set_stage_status(
                    query_id,
                    stage_id,
                    StageStatus::Completed {
                        tickets: tickets.clone(),
                        duration: Duration::from_secs(0),
                    },
                )
                .await;

                stage_outputs.insert(stage_id, tickets);
            }
        }

        // Fetch final output
        let root_tickets = stage_outputs
            .get(&plan.root_stage)
            .ok_or(ExecutionError::StageNotFound(plan.root_stage))?;

        let mut final_batches = Vec::new();
        for ticket in root_tickets.all() {
            let batches = executor
                .fetch_output(ticket)
                .await
                .map_err(|e| ExecutionError::StageFailed(plan.root_stage, e.to_string()))?;
            final_batches.extend(batches);
        }

        Ok((final_batches, workers_used))
    }

    /// Execute plan in distributed mode.
    async fn execute_distributed(
        &self,
        plan: &LdpPlan,
        query_id: &str,
    ) -> Result<(Vec<RecordBatch>, Vec<WorkerId>), CoordinatorError> {
        let executor = Arc::new(FlightStageExecutor::new(
            self.config.tenant_id.clone(),
            self.connection_pool.clone(),
        ));
        let exchange_runtime = Arc::new(DistributedExchangeRuntime::new(
            self.config.tenant_id.clone(),
            self.connection_pool.clone(),
        ));

        let mut stage_outputs: HashMap<StageId, StageTickets> = HashMap::new();
        let mut workers_used: Vec<WorkerId> = Vec::new();

        // Execute stages in parallel levels
        let execution_levels = self.compute_execution_levels(plan);

        for level_stages in execution_levels {
            let level_futures = level_stages.iter().map(|&stage_id| {
                let plan_clone = plan.clone();
                let query_id = query_id.to_string();
                let executor = executor.clone();
                let exchange_runtime = exchange_runtime.clone();
                let stage_outputs_clone = stage_outputs.clone();

                async move {
                    let stage = plan_clone
                        .get_stage(stage_id)
                        .ok_or(ExecutionError::StageNotFound(stage_id))?
                        .clone();

                    self.set_stage_status(
                        &query_id,
                        stage_id,
                        StageStatus::Running {
                            started_at: Instant::now(),
                        },
                    )
                    .await;

                    info!(
                        query_id = %query_id,
                        stage_id = %stage_id,
                        target_workers = ?stage.target_workers,
                        "Executing stage on workers"
                    );

                    let mut all_tickets = StageTickets::new();
                    let mut stage_workers = Vec::new();

                    for target_worker in &stage.target_workers {
                        let inputs = self
                            .resolve_distributed_inputs_for_worker(
                                &plan_clone,
                                stage_id,
                                target_worker,
                                &stage_outputs_clone,
                                exchange_runtime.as_ref(),
                            )
                            .await?;

                        debug!(
                            query_id = %query_id,
                            stage_id = %stage_id,
                            worker = %target_worker,
                            input_tables = inputs.len(),
                            "Resolved inputs for worker"
                        );

                        let worker_stage = Stage {
                            stage_id: stage.stage_id,
                            target_workers: vec![target_worker.clone()],
                            inputs: stage.inputs.clone(),
                            output: stage.output.clone(),
                            stage_sql: stage.stage_sql.clone(),
                            limits: stage.limits.clone(),
                        };

                        let tickets = self
                            .execute_stage_with_retry(&*executor, &worker_stage, inputs, &query_id)
                            .await?;

                        for ticket in tickets.all() {
                            all_tickets.add(ticket.clone());
                            if !stage_workers.contains(&ticket.worker_id) {
                                stage_workers.push(ticket.worker_id.clone());
                            }
                        }
                    }

                    Ok::<(StageId, StageTickets, Vec<WorkerId>), CoordinatorError>((
                        stage_id,
                        all_tickets,
                        stage_workers,
                    ))
                }
            });

            let level_results = futures_util::future::join_all(level_futures).await;

            for stage_result in level_results {
                let (stage_id, all_tickets, stage_workers) = match stage_result {
                    Ok(value) => value,
                    Err(e) => {
                        // Stage failed - cancel query to prevent orphaned tasks
                        warn!(
                            query_id = %query_id,
                            error = %e,
                            "Distributed stage failed during parallel execution, cancelling query"
                        );
                        let _ = self.cancel_query(query_id).await;
                        return Err(e);
                    }
                };

                for worker_id in stage_workers {
                    if !workers_used.contains(&worker_id) {
                        workers_used.push(worker_id);
                    }
                }

                self.set_stage_status(
                    query_id,
                    stage_id,
                    StageStatus::Completed {
                        tickets: all_tickets.clone(),
                        duration: Duration::from_secs(0),
                    },
                )
                .await;

                stage_outputs.insert(stage_id, all_tickets);
            }
        }

        // Fetch final output via Flight
        let root_tickets = stage_outputs
            .get(&plan.root_stage)
            .ok_or(ExecutionError::StageNotFound(plan.root_stage))?;

        let mut final_batches = Vec::new();
        for ticket in root_tickets.all() {
            let batches = executor
                .fetch_output(ticket)
                .await
                .map_err(|e| ExecutionError::StageFailed(plan.root_stage, e.to_string()))?;
            final_batches.extend(batches);
        }

        Ok((final_batches, workers_used))
    }

    /// Execute plan with streaming pipeline for concurrent stage execution.
    ///
    /// Stages are spawned concurrently and connected via memory-bounded channels,
    /// enabling data streaming between stages with automatic backpressure.
    ///
    /// Benefits:
    /// - Reduced latency: Downstream stages start as soon as first batch arrives
    /// - Lower memory: No need to buffer full stage outputs
    /// - Better utilization: Workers process data concurrently
    async fn execute_pipelined(
        &self,
        plan: &LdpPlan,
        query_id: &str,
    ) -> Result<(Vec<RecordBatch>, Vec<WorkerId>), CoordinatorError> {
        info!(
            query_id = %query_id,
            stages = plan.stages.len(),
            "Starting streaming pipelined execution"
        );

        // Only local execution is currently supported for streaming
        if self.config.distributed {
            return Err(CoordinatorError::InvalidConfig(
                "Streaming pipeline not yet supported for distributed execution".to_string(),
            ));
        }

        let databases: HashMap<WorkerId, Arc<SharedDatabase>> = self
            .worker_databases
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let executor = Arc::new(LocalStageExecutor::with_databases(databases));
        let mut workers_used: Vec<WorkerId> = Vec::new();

        // Create inter-stage channels with memory-bounded buffers
        let mut stage_channels: StageChannelMap = HashMap::new();

        for edge in &plan.edges {
            let buffer_capacity =
                self.compute_buffer_capacity(self.config.planner_policy.pipeline_buffer_bytes);
            let (tx, rx) = mpsc::channel(buffer_capacity);
            stage_channels.insert(edge.exchange_id, (tx, rx));
        }

        // Create a special channel for the root stage output
        let buffer_capacity =
            self.compute_buffer_capacity(self.config.planner_policy.pipeline_buffer_bytes);
        let (root_tx, mut root_rx) = mpsc::channel(buffer_capacity);

        // Track stage outputs for collecting workers used
        let mut stage_workers: HashMap<StageId, Vec<WorkerId>> = HashMap::new();

        // Spawn all stages concurrently
        let mut stage_handles = Vec::new();

        for stage in &plan.stages {
            // Resolve input streams from upstream stages
            let input_streams = self.resolve_input_streams(stage, &mut stage_channels)?;

            // Clone executor for this stage
            let executor_clone = executor.clone();
            let stage_clone = stage.clone();
            let query_id_clone = query_id.to_string();

            // Get output channels for this stage
            let mut output_txs =
                self.get_output_channels(stage.stage_id, &plan.edges, &stage_channels)?;

            // If this is the root stage, add the root output channel
            if stage.stage_id == plan.root_stage {
                output_txs.push(root_tx.clone());
            }

            // Track workers for this stage
            let workers = stage.target_workers.clone();
            stage_workers.insert(stage.stage_id, workers.clone());

            // Spawn stage execution
            let handle = tokio::spawn(async move {
                debug!(
                    query_id = %query_id_clone,
                    stage_id = %stage_clone.stage_id,
                    "Starting streaming stage execution"
                );

                // Execute stage with streaming inputs
                let mut output_stream = executor_clone
                    .submit_stage_streaming(&query_id_clone, &stage_clone, input_streams)
                    .await
                    .map_err(|e| {
                        CoordinatorError::ExecutionFailed(ExecutionError::StageFailed(
                            stage_clone.stage_id,
                            e.to_string(),
                        ))
                    })?;

                // Fan out to all downstream stages
                let mut batch_count = 0;
                while let Some(batch_result) = output_stream.next().await {
                    let batch = batch_result.map_err(|e| {
                        CoordinatorError::ExecutionFailed(ExecutionError::StageFailed(
                            stage_clone.stage_id,
                            e.to_string(),
                        ))
                    })?;

                    batch_count += 1;

                    // Send to all downstream stages
                    for tx in &output_txs {
                        tx.send(Ok(batch.clone())).await.map_err(|_| {
                            CoordinatorError::ExecutionFailed(ExecutionError::StageFailed(
                                stage_clone.stage_id,
                                "Downstream stage closed".into(),
                            ))
                        })?;
                    }
                }

                debug!(
                    query_id = %query_id_clone,
                    stage_id = %stage_clone.stage_id,
                    batches_produced = batch_count,
                    "Streaming stage execution completed"
                );

                Ok::<(), CoordinatorError>(())
            });

            stage_handles.push((stage.stage_id, handle));
        }

        // Collect results from root stage in the background while stages run
        let query_id_clone = query_id.to_string();
        let collection_handle = tokio::spawn(async move {
            let mut results = Vec::new();
            let mut batch_count = 0;

            while let Some(batch_result) = root_rx.recv().await {
                let batch = batch_result?;
                batch_count += 1;
                results.push(batch);
            }

            debug!(
                query_id = %query_id_clone,
                batches = batch_count,
                "Collected results from root stage"
            );

            Ok::<Vec<RecordBatch>, CoordinatorError>(results)
        });

        // Wait for all stages to complete
        for (stage_id, handle) in stage_handles {
            handle.await.map_err(|e| {
                CoordinatorError::ExecutionFailed(ExecutionError::StageFailed(
                    stage_id,
                    e.to_string(),
                ))
            })??;
        }

        // All stages completed - drop the root_tx to signal EOF to collection task
        drop(root_tx);

        // Wait for result collection to complete
        let results = collection_handle.await.map_err(|e| {
            CoordinatorError::ExecutionFailed(ExecutionError::ExchangeFailed(format!(
                "Result collection failed: {}",
                e
            )))
        })??;

        // Collect workers used
        for worker_list in stage_workers.values() {
            for worker in worker_list {
                if !workers_used.contains(worker) {
                    workers_used.push(worker.clone());
                }
            }
        }

        info!(
            query_id = %query_id,
            stages = plan.stages.len(),
            result_batches = results.len(),
            "Streaming pipelined execution completed successfully"
        );

        Ok((results, workers_used))
    }

    /// Compute buffer capacity based on target buffer bytes.
    fn compute_buffer_capacity(&self, target_buffer_bytes: u64) -> usize {
        // Estimate batch size (default: 4MB per batch)
        const DEFAULT_BATCH_BYTES: u64 = 4 * 1024 * 1024;

        // Compute capacity in number of batches
        (target_buffer_bytes / DEFAULT_BATCH_BYTES).clamp(2, 64) as usize
    }

    /// Resolve input streams for a stage from inter-stage channels.
    fn resolve_input_streams(
        &self,
        stage: &Stage,
        stage_channels: &mut StageChannelMap,
    ) -> Result<HashMap<String, RecordBatchStream>, CoordinatorError> {
        let mut input_streams = HashMap::new();

        for input in &stage.inputs {
            match input {
                crate::ldp::StageInput::ExchangeInput {
                    exchange_id,
                    table_name,
                } => {
                    // Remove the receiver from the map (it will be consumed by this stage)
                    if let Some((_, rx)) = stage_channels.remove(exchange_id) {
                        // Wrap receiver in RecordBatchStream
                        // TODO: Get actual schema from plan metadata
                        let schema = Arc::new(Schema::empty());
                        let stream = RecordBatchStream::new(rx, schema);
                        input_streams.insert(table_name.clone(), stream);
                    } else {
                        return Err(CoordinatorError::ExecutionFailed(
                            ExecutionError::ExchangeFailed(format!(
                                "Exchange {} not found in stage channels",
                                exchange_id
                            )),
                        ));
                    }
                }
                crate::ldp::StageInput::LocalCatalog => {
                    // Local catalog scans don't need input streams - they read from storage
                }
            }
        }

        Ok(input_streams)
    }

    /// Get output channel senders for a stage based on its downstream edges.
    fn get_output_channels(
        &self,
        stage_id: StageId,
        edges: &[crate::ldp::ExchangeEdge],
        stage_channels: &StageChannelMap,
    ) -> Result<Vec<mpsc::Sender<StageBatchResult>>, CoordinatorError> {
        let mut output_txs = Vec::new();

        // Find all edges where this stage is the source
        for edge in edges {
            if edge.from_stage == stage_id {
                if let Some((tx, _)) = stage_channels.get(&edge.exchange_id) {
                    output_txs.push(tx.clone());
                }
            }
        }

        Ok(output_txs)
    }

    /// Execute a stage with retry logic.
    async fn execute_stage_with_retry<E: StageExecutor>(
        &self,
        executor: &E,
        stage: &Stage,
        inputs: HashMap<String, Vec<RecordBatch>>,
        query_id: &str,
    ) -> Result<StageTickets, ExecutionError> {
        let mut last_error = None;
        let mut retries = 0;

        while retries <= self.config.max_stage_retries {
            match executor.submit_stage(query_id, stage, inputs.clone()).await {
                Ok(tickets) => return Ok(tickets),
                Err(e) => {
                    retries += 1;
                    warn!(
                        query_id = %query_id,
                        stage_id = %stage.stage_id,
                        retry = retries,
                        error = %e,
                        "Stage execution failed, retrying"
                    );
                    last_error = Some(e);

                    // Exponential backoff
                    if retries <= self.config.max_stage_retries {
                        tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(retries - 1)))
                            .await;
                    }
                }
            }
        }

        Err(ExecutionError::StageFailed(
            stage.stage_id,
            format!(
                "Failed after {} retries: {}",
                retries,
                last_error.map(|e| e.to_string()).unwrap_or_default()
            ),
        ))
    }

    /// Resolve inputs for a specific target worker in distributed execution.
    ///
    /// This method fetches the correct subset of data for each worker based on
    /// the exchange type:
    /// - Gather: Only the target worker receives data
    /// - Broadcast: All target workers receive the same data
    /// - HashPartition: Each worker receives its assigned partitions
    async fn resolve_distributed_inputs_for_worker(
        &self,
        plan: &LdpPlan,
        stage_id: StageId,
        target_worker: &WorkerId,
        stage_outputs: &HashMap<StageId, StageTickets>,
        exchange_runtime: &DistributedExchangeRuntime,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, CoordinatorError> {
        let stage = plan
            .get_stage(stage_id)
            .ok_or(ExecutionError::StageNotFound(stage_id))?;

        let mut inputs = HashMap::new();

        for input in &stage.inputs {
            if let crate::ldp::StageInput::ExchangeInput {
                exchange_id,
                table_name,
            } = input
            {
                // Find the edge for this exchange
                if let Some(edge) = plan.edges.iter().find(|e| e.exchange_id == *exchange_id) {
                    let upstream_stage = edge.from_stage;

                    if let Some(upstream_tickets) = stage_outputs.get(&upstream_stage) {
                        // Get source workers from upstream tickets
                        let source_workers: Vec<WorkerId> = upstream_tickets
                            .all()
                            .iter()
                            .map(|t| t.worker_id.clone())
                            .collect();

                        // Call the correct exchange method based on exchange type
                        let batches = match &edge.kind {
                            Exchange::Gather { target } => {
                                // Only the target worker receives gathered data
                                if target_worker != target {
                                    warn!(
                                        query_id = %plan.query_id,
                                        stage_id = %stage_id,
                                        worker = %target_worker,
                                        target = %target,
                                        "Worker is not the gather target, skipping"
                                    );
                                    vec![]
                                } else {
                                    debug!(
                                        query_id = %plan.query_id,
                                        stage_id = %stage_id,
                                        worker = %target_worker,
                                        sources = ?source_workers,
                                        "Executing gather exchange"
                                    );
                                    exchange_runtime
                                        .execute_gather(
                                            plan.query_id.as_ref(),
                                            upstream_stage,
                                            &source_workers,
                                            target,
                                        )
                                        .await
                                        .map_err(|e| {
                                            ExecutionError::ExchangeFailed(e.to_string())
                                        })?
                                }
                            }
                            Exchange::Broadcast { targets } => {
                                // All target workers receive the same broadcasted data
                                if !targets.contains(target_worker) {
                                    warn!(
                                        query_id = %plan.query_id,
                                        stage_id = %stage_id,
                                        worker = %target_worker,
                                        targets = ?targets,
                                        "Worker not in broadcast targets, skipping"
                                    );
                                    vec![]
                                } else {
                                    debug!(
                                        query_id = %plan.query_id,
                                        stage_id = %stage_id,
                                        worker = %target_worker,
                                        sources = ?source_workers,
                                        "Executing broadcast exchange"
                                    );
                                    // Fetch from source (typically single worker for broadcast)
                                    let source = source_workers.first().ok_or_else(|| {
                                        ExecutionError::ExchangeFailed(
                                            "No source worker for broadcast".into(),
                                        )
                                    })?;

                                    exchange_runtime
                                        .execute_broadcast(
                                            plan.query_id.as_ref(),
                                            upstream_stage,
                                            source,
                                            targets,
                                        )
                                        .await
                                        .map_err(|e| {
                                            ExecutionError::ExchangeFailed(e.to_string())
                                        })?
                                }
                            }
                            Exchange::HashPartition {
                                column_refs,
                                partitions,
                            } => {
                                // Each worker receives only its assigned partitions
                                debug!(
                                    query_id = %plan.query_id,
                                    stage_id = %stage_id,
                                    worker = %target_worker,
                                    sources = ?source_workers,
                                    partitions = partitions,
                                    "Executing hash partition exchange"
                                );
                                use crate::ldp::executor::exchange::HashPartitionRequest;
                                exchange_runtime
                                    .execute_hash_partition(HashPartitionRequest {
                                        query_id: plan.query_id.as_ref(),
                                        stage_id: upstream_stage,
                                        source_workers: &source_workers,
                                        column_refs,
                                        num_partitions: *partitions,
                                        partition_to_worker: &edge.partition_to_worker,
                                        local_worker: target_worker,
                                    })
                                    .await
                                    .map_err(|e| ExecutionError::ExchangeFailed(e.to_string()))?
                            }
                        };

                        inputs.insert(table_name.clone(), batches);
                    }
                }
            }
        }

        Ok(inputs)
    }

    // ========================================================================
    // Status Tracking
    // ========================================================================

    /// Set stage execution status.
    async fn set_stage_status(&self, query_id: &str, stage_id: StageId, status: StageStatus) {
        self.stage_status
            .insert((query_id.to_string(), stage_id), status);
    }

    /// Register a stage monitor for cancellation tracking.
    pub async fn register_stage_monitor(
        &self,
        query_id: &str,
        stage_id: StageId,
        monitor: SharedStageExecutionMonitor,
    ) {
        self.stage_monitors
            .insert((query_id.to_string(), stage_id), monitor);
    }

    /// Unregister a stage monitor after completion.
    pub async fn unregister_stage_monitor(&self, query_id: &str, stage_id: StageId) {
        self.stage_monitors
            .remove(&(query_id.to_string(), stage_id));
    }

    /// Cancel a running query and all its stages.
    pub async fn cancel_query(
        &self,
        query_id: &str,
    ) -> Result<CancellationResult, CoordinatorError> {
        info!(query_id = %query_id, "Cancelling query");

        // Find all in-flight stages for this query
        let in_flight_stages: Vec<StageId> = self
            .stage_status
            .iter()
            .filter(|entry| {
                entry.key().0 == query_id && matches!(entry.value(), StageStatus::Running { .. })
            })
            .map(|entry| entry.key().1)
            .collect();

        // Cancel all in-flight stages
        for stage_id in &in_flight_stages {
            self.cancel_stage(query_id, *stage_id).await?;
        }

        // Cleanup resources
        self.cleanup_query_resources(query_id).await?;

        Ok(CancellationResult {
            query_id: query_id.to_string(),
            stages_cancelled: in_flight_stages.len(),
            cleanup_completed: true,
        })
    }

    /// Cancel a specific stage
    async fn cancel_stage(
        &self,
        query_id: &str,
        stage_id: StageId,
    ) -> Result<(), CoordinatorError> {
        // For distributed execution, propagate cancellation to all participant workers via Flight
        if self.config.distributed {
            let pool = Arc::clone(&self.connection_pool);
            let query_id_owned = query_id.to_string();
            tokio::spawn(async move {
                let workers = pool.worker_ids().await;
                for worker_id in workers {
                    if let Ok(mut conn) = pool.get_connection(&worker_id).await {
                        let body = serde_json::json!({ "query_id": query_id_owned })
                            .to_string()
                            .into_bytes();
                        let action = arrow_flight::Action {
                            r#type: "cancel_query".to_string(),
                            body: body.into(),
                        };
                        // Best-effort: ignore errors since the worker may have already finished
                        let _ = conn.client.do_action(action).await;
                    }
                }
            });
        } else {
            // For local execution, cancel via monitor
            if let Some(monitor) = self
                .stage_monitors
                .get(&(query_id.to_string(), stage_id))
                .map(|m| m.clone())
            {
                monitor.cancel();
                debug!(
                    query_id = %query_id,
                    stage_id = %stage_id,
                    "Cancelled local stage via monitor"
                );
            } else {
                warn!(
                    query_id = %query_id,
                    stage_id = %stage_id,
                    "No monitor found for stage, cannot cancel"
                );
            }
        }

        Ok(())
    }

    /// Cleanup query resources
    async fn cleanup_query_resources(&self, query_id: &str) -> Result<(), CoordinatorError> {
        // Clear stage status
        self.clear_query_status(query_id).await;

        info!(query_id = %query_id, "Query resources cleaned up");
        Ok(())
    }

    /// Clear all status entries for a query
    async fn clear_query_status(&self, query_id: &str) {
        self.stage_status.retain(|(qid, _), _| qid != query_id);
        self.stage_monitors.retain(|(qid, _), _| qid != query_id);
    }

    /// Get stage execution status.
    pub async fn get_stage_status(&self, query_id: &str, stage_id: StageId) -> Option<StageStatus> {
        self.stage_status
            .get(&(query_id.to_string(), stage_id))
            .map(|v| v.clone())
    }
}

/// Result of a cancellation operation.
#[derive(Debug, Clone)]
pub struct CancellationResult {
    /// Query identifier that was cancelled.
    pub query_id: String,
    /// Number of stages that were cancelled.
    pub stages_cancelled: usize,
    /// Whether resource cleanup was completed.
    pub cleanup_completed: bool,
}

// ============================================================================
// Helper Functions
// ============================================================================

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert Arrow data type to DuckDB SQL type string.
fn arrow_type_to_duckdb_type(data_type: &DataType) -> &str {
    match data_type {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Boolean => "BOOLEAN",
        DataType::Date32 => "DATE",
        DataType::Date64 => "TIMESTAMP",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        _ => "VARCHAR", // Fallback for unsupported types
    }
}

/// Generate a unique query ID.
fn generate_query_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("q_{:x}", timestamp % 0xFFFFFFFFFFFF)
}

// ============================================================================
// Convenience Constructors
// ============================================================================

impl LdpCoordinator<InMemoryMetadata> {
    /// Create a simple coordinator with in-memory metadata.
    pub fn simple(tenant_id: impl Into<String>) -> Result<Self, CoordinatorError> {
        let config = CoordinatorConfig::new(tenant_id);
        let metadata = Arc::new(InMemoryMetadata::new());
        Self::new(config, metadata)
    }
}

impl LdpCoordinator<ClusterMetadata> {
    /// Create a coordinator for distributed execution.
    pub fn distributed(tenant_id: impl Into<String>) -> Result<Self, CoordinatorError> {
        let config = CoordinatorConfig::new(tenant_id).with_distributed(true);
        let metadata = Arc::new(ClusterMetadata::new());
        Self::new(config, metadata)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_creation() {
        let coordinator = LdpCoordinator::simple("test_tenant").unwrap();
        assert!(!coordinator.config.distributed);
    }

    #[tokio::test]
    async fn test_distributed_coordinator_creation() {
        let coordinator = LdpCoordinator::distributed("test_tenant").unwrap();
        assert!(coordinator.config.distributed);
    }

    #[tokio::test]
    async fn test_register_dataset() {
        let coordinator = LdpCoordinator::simple("test_tenant").unwrap();
        let dataset = RegisteredDataset::new("sales".to_string(), "dt".to_string());
        coordinator.register_dataset(dataset).await;

        let transformer = coordinator.transformer.read().await;
        assert!(transformer.is_registered("sales"));
    }

    #[tokio::test]
    async fn test_register_worker() {
        let coordinator = LdpCoordinator::distributed("test_tenant").unwrap();
        coordinator
            .register_worker("worker1", "http://localhost:50051")
            .await;

        // Worker should be registered in connection pool
        // (actual connection happens on first use)
    }

    #[tokio::test]
    async fn test_query_id_generation() {
        let id1 = generate_query_id();
        let id2 = generate_query_id();

        assert_ne!(id1, id2);
        assert!(id1.starts_with("q_"));
        assert_eq!(id1.len(), 14); // "q_" + 12 chars
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = CoordinatorConfig::new("tenant1")
            .with_distributed(true)
            .with_timeout(Duration::from_secs(60));

        assert_eq!(config.tenant_id, "tenant1");
        assert!(config.distributed);
        assert_eq!(config.query_timeout, Duration::from_secs(60));
    }
}
