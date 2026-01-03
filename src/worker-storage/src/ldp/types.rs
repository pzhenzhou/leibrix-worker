//! Core data structures for Leibrix Distributed Plan (LDP).
//!
//! These types define the distributed execution model including:
//! - Distribution properties for tracking data placement
//! - Exchange types for data movement between stages
//! - Stage and LdpPlan for the execution plan structure

use std::collections::HashMap;

// ============================================================================
// Identifiers
// ============================================================================

/// Unique identifier for a query execution.
pub type QueryId = String;

/// Identifier for a stage within an LDP plan.
pub type StageId = u32;

/// Identifier for an exchange between stages.
pub type ExchangeId = u32;

/// Identifier for a worker node.
pub type WorkerId = String;

/// Identifier for an epoch (time-partitioned data segment).
pub type EpochId = String;

// ============================================================================
// Distribution Properties
// ============================================================================

/// Describes how data is distributed across workers.
///
/// This is the key abstraction for the unified planning algorithm.
/// Each Substrait Rel node is annotated with its output distribution.
#[derive(Clone, Debug, PartialEq)]
pub enum Distribution {
    /// All data resides on a single node.
    /// Used after Gather exchange or for scalar aggregates.
    Singleton { worker: WorkerId },

    /// Data is partitioned by hash of field references across workers.
    /// Field refs are indices into the schema (Substrait convention).
    HashPartitioned {
        field_refs: Vec<u32>,
        workers: Vec<WorkerId>,
    },

    /// Data is partitioned by epoch (natural time-based sharding).
    /// This is the distribution after reading from local epoch tables.
    EpochPartitioned { workers: Vec<WorkerId> },

    /// Data is replicated on all specified workers.
    /// Result of Broadcast exchange.
    Replicated { workers: Vec<WorkerId> },
}

impl Distribution {
    /// Get all workers that hold data for this distribution.
    pub fn workers(&self) -> &[WorkerId] {
        match self {
            Distribution::Singleton { worker } => std::slice::from_ref(worker),
            Distribution::HashPartitioned { workers, .. } => workers,
            Distribution::EpochPartitioned { workers } => workers,
            Distribution::Replicated { workers } => workers,
        }
    }

    /// Check if distribution is on a single node.
    pub fn is_singleton(&self) -> bool {
        matches!(self, Distribution::Singleton { .. })
    }
}

/// Annotation attached to each Substrait Rel node during planning.
#[derive(Clone, Debug)]
pub struct DistributionAnnotation {
    /// The distribution of data at this node's output.
    pub distribution: Distribution,

    /// Estimated number of rows at this node's output.
    pub est_rows: u64,

    /// Estimated bytes at this node's output.
    pub est_bytes: u64,
}

impl DistributionAnnotation {
    /// Create a new annotation.
    pub fn new(distribution: Distribution, est_rows: u64, est_bytes: u64) -> Self {
        Self {
            distribution,
            est_rows,
            est_bytes,
        }
    }
}

/// Specifies the distribution requirements for an operator's input.
///
/// Used in `get_requirements()` - THE ONLY place we inspect RelType.
#[derive(Clone, Debug, PartialEq)]
pub enum RequiredDistribution {
    /// Any distribution is acceptable (e.g., Filter, Project).
    Any,

    /// Data must be on a single node (e.g., Sort, Fetch, scalar Aggregate).
    Singleton,

    /// Data must be hash-partitioned by these field references.
    /// (e.g., GROUP BY columns, JOIN keys).
    HashPartitioned { field_refs: Vec<u32> },
}

impl RequiredDistribution {
    /// Check if the given distribution satisfies this requirement.
    pub fn is_satisfied_by(&self, actual: &Distribution) -> bool {
        match self {
            RequiredDistribution::Any => true,
            RequiredDistribution::Singleton => actual.is_singleton(),
            RequiredDistribution::HashPartitioned { field_refs } => {
                match actual {
                    Distribution::HashPartitioned {
                        field_refs: actual_refs,
                        ..
                    } => field_refs == actual_refs,
                    Distribution::Replicated { .. } => true, // Replicated satisfies any partitioning
                    _ => false,
                }
            }
        }
    }
}

/// Defines how data moves between stages.
///
/// Exchanges are inserted at stage boundaries when the actual distribution
/// does not satisfy the required distribution.
#[derive(Clone, Debug, PartialEq)]
pub enum Exchange {
    /// Gather all data to a single target node.
    /// Used for ORDER BY, LIMIT, scalar aggregates.
    Gather { target: WorkerId },

    /// Broadcast data to all target workers.
    /// Used for small dimension tables in broadcast joins.
    Broadcast { targets: Vec<WorkerId> },

    /// Repartition data by hash of field references.
    /// Used for shuffle joins and distributed GROUP BY.
    HashPartition {
        field_refs: Vec<u32>,
        partitions: u32,
    },
}

impl Exchange {
    /// Create a Gather exchange to the specified coordinator.
    pub fn gather(coordinator: WorkerId) -> Self {
        Exchange::Gather {
            target: coordinator,
        }
    }

    /// Create a Broadcast exchange to specified workers.
    pub fn broadcast(workers: Vec<WorkerId>) -> Self {
        Exchange::Broadcast { targets: workers }
    }

    /// Create a HashPartition exchange.
    pub fn hash_partition(field_refs: Vec<u32>, partitions: u32) -> Self {
        Exchange::HashPartition {
            field_refs,
            partitions,
        }
    }
}

/// Describes how a stage receives its input data.
#[derive(Clone, Debug, PartialEq)]
pub enum StageInput {
    /// Stage reads from local DuckDB catalog (epoch tables).
    LocalCatalog,

    /// Stage reads from an exchange output.
    ExchangeInput {
        /// The exchange that produces this input.
        exchange_id: ExchangeId,
        /// Table name to register the exchange data as in DuckDB.
        /// Format: "__exchange_{id}"
        table_name: String,
    },
}

/// Describes the output format of a stage.
#[derive(Clone, Debug, PartialEq)]
pub enum StageOutput {
    /// Single Arrow stream output.
    Stream,

    /// Multiple partitioned streams (for HashPartition exchange).
    Partitioned { partitions: u32 },
}

impl Default for StageOutput {
    fn default() -> Self {
        StageOutput::Stream
    }
}

/// Resource limits for stage execution.
#[derive(Clone, Debug, PartialEq)]
pub struct StageLimits {
    /// Maximum bytes to output from this stage.
    pub max_bytes_output: u64,

    /// Maximum rows to output from this stage.
    pub max_rows_output: u64,

    /// Execution timeout in milliseconds.
    pub timeout_ms: u64,

    /// Memory budget for this stage in bytes.
    pub memory_bytes: u64,
}

impl Default for StageLimits {
    fn default() -> Self {
        Self {
            max_bytes_output: 5 * 1024 * 1024 * 1024, // 5GB
            max_rows_output: 100_000_000,             // 100M rows
            timeout_ms: 300_000,                      // 5 minutes
            memory_bytes: 2 * 1024 * 1024 * 1024,     // 2GB
        }
    }
}

/// A single execution stage in the LDP plan.
///
/// Each stage is a valid Substrait subplan that can be executed
/// independently on a set of workers.
#[derive(Clone, Debug)]
pub struct Stage {
    /// Unique identifier for this stage.
    pub stage_id: StageId,

    /// Workers that will execute this stage.
    pub target_workers: Vec<WorkerId>,

    /// Input sources for this stage.
    pub inputs: Vec<StageInput>,

    /// Output format of this stage.
    pub output: StageOutput,

    /// Substrait plan fragment (serialized bytes).
    /// This is NOT a custom IR - it's the actual Substrait protobuf.
    pub substrait_plan: Vec<u8>,

    /// Resource limits for execution.
    pub limits: StageLimits,
}

impl Stage {
    /// Create a new stage with the given ID and plan.
    pub fn new(stage_id: StageId, substrait_plan: Vec<u8>) -> Self {
        Self {
            stage_id,
            target_workers: Vec::new(),
            inputs: Vec::new(),
            output: StageOutput::default(),
            substrait_plan,
            limits: StageLimits::default(),
        }
    }

    /// Check if this stage has any exchange inputs.
    pub fn has_exchange_inputs(&self) -> bool {
        self.inputs
            .iter()
            .any(|i| matches!(i, StageInput::ExchangeInput { .. }))
    }
}

/// Represents data flow between two stages via an exchange.
#[derive(Clone, Debug)]
pub struct ExchangeEdge {
    /// Unique identifier for this exchange.
    pub exchange_id: ExchangeId,

    /// The type of exchange operation.
    pub kind: Exchange,

    /// Source stage that produces data.
    pub from_stage: StageId,

    /// Target stage that consumes data.
    pub to_stage: StageId,

    /// For HashPartition: mapping from partition number to worker.
    /// partition_to_worker[i] is the worker for partition i.
    pub partition_to_worker: Vec<WorkerId>,
}

/// The complete Leibrix Distributed Plan for a query.
///
/// An LdpPlan consists of:
/// - A DAG of stages connected by exchange edges
/// - Metadata about the coordinator and query
#[derive(Clone, Debug)]
pub struct LdpPlan {
    /// Unique identifier for this query execution.
    pub query_id: QueryId,

    /// The coordinator worker (where the final result is assembled).
    pub coordinator: WorkerId,

    /// All stages in the plan (topologically ordered if generated correctly).
    pub stages: Vec<Stage>,

    /// Exchange edges connecting stages.
    pub edges: Vec<ExchangeEdge>,
}

impl LdpPlan {
    /// Create a new empty LDP plan.
    pub fn new(query_id: QueryId, coordinator: WorkerId) -> Self {
        Self {
            query_id,
            coordinator,
            stages: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Get the final stage (root of the DAG).
    /// This is typically the last stage in the topologically sorted list.
    pub fn final_stage(&self) -> Option<&Stage> {
        self.stages.last()
    }

    /// Get a stage by ID.
    pub fn get_stage(&self, stage_id: StageId) -> Option<&Stage> {
        self.stages.iter().find(|s| s.stage_id == stage_id)
    }

    /// Get all stages that depend on the given stage (downstream).
    pub fn downstream_stages(&self, stage_id: StageId) -> Vec<StageId> {
        self.edges
            .iter()
            .filter(|e| e.from_stage == stage_id)
            .map(|e| e.to_stage)
            .collect()
    }

    /// Get all stages that this stage depends on (upstream).
    pub fn upstream_stages(&self, stage_id: StageId) -> Vec<StageId> {
        self.edges
            .iter()
            .filter(|e| e.to_stage == stage_id)
            .map(|e| e.from_stage)
            .collect()
    }
}

/// Statistics for an epoch, used for cost estimation.
#[derive(Clone, Debug, Default)]
pub struct EpochStats {
    /// Number of rows in the epoch.
    pub rows: u64,

    /// Size in bytes.
    pub bytes: u64,

    /// Optional: Number of distinct values for common columns.
    /// Key is column name, value is estimated NDV.
    pub ndv: HashMap<String, u64>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_workers() {
        let singleton = Distribution::Singleton {
            worker: "w1".to_string(),
        };
        assert_eq!(singleton.workers(), &["w1"]);
        assert!(singleton.is_singleton());

        let partitioned = Distribution::EpochPartitioned {
            workers: vec!["w1".to_string(), "w2".to_string()],
        };
        assert_eq!(partitioned.workers().len(), 2);
        assert!(!partitioned.is_singleton());
    }

    #[test]
    fn test_required_distribution_satisfaction() {
        let singleton = Distribution::Singleton {
            worker: "w1".to_string(),
        };

        assert!(RequiredDistribution::Any.is_satisfied_by(&singleton));
        assert!(RequiredDistribution::Singleton.is_satisfied_by(&singleton));
        assert!(
            !RequiredDistribution::HashPartitioned {
                field_refs: vec![0]
            }
            .is_satisfied_by(&singleton)
        );

        let hash_partitioned = Distribution::HashPartitioned {
            field_refs: vec![0, 1],
            workers: vec!["w1".to_string(), "w2".to_string()],
        };

        assert!(
            RequiredDistribution::HashPartitioned {
                field_refs: vec![0, 1]
            }
            .is_satisfied_by(&hash_partitioned)
        );
        assert!(
            !RequiredDistribution::HashPartitioned {
                field_refs: vec![0]
            }
            .is_satisfied_by(&hash_partitioned)
        );
    }

    #[test]
    fn test_stage_has_exchange_inputs() {
        let mut stage = Stage::new(0, vec![]);
        assert!(!stage.has_exchange_inputs());

        stage.inputs.push(StageInput::LocalCatalog);
        assert!(!stage.has_exchange_inputs());

        stage.inputs.push(StageInput::ExchangeInput {
            exchange_id: 0,
            table_name: "__exchange_0".to_string(),
        });
        assert!(stage.has_exchange_inputs());
    }

    #[test]
    fn test_ldp_plan_navigation() {
        let mut plan = LdpPlan::new("q1".to_string(), "coordinator".to_string());

        plan.stages.push(Stage::new(0, vec![]));
        plan.stages.push(Stage::new(1, vec![]));

        plan.edges.push(ExchangeEdge {
            exchange_id: 0,
            kind: Exchange::Gather {
                target: "coordinator".to_string(),
            },
            from_stage: 0,
            to_stage: 1,
            partition_to_worker: vec![],
        });

        assert_eq!(plan.downstream_stages(0), vec![1]);
        assert_eq!(plan.upstream_stages(1), vec![0]);
        assert!(plan.upstream_stages(0).is_empty());
        assert!(plan.downstream_stages(1).is_empty());
    }
}
