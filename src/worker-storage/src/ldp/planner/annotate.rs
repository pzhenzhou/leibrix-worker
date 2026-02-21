//! Distribution annotation and exchange enforcement for LogicalPlan.
//!
//! This module implements the core LDP planning algorithm:
//! 1. Bottom-up annotation of distribution properties
//! 2. Top-down enforcement of requirements with exchange insertion
//!
//! # Algorithm Overview
//! ```text
//! annotate_logical_plan(plan):
//!   1. Recursively process children
//!   2. Get requirements for this operator  
//!   3. For each unsatisfied requirement, determine exchange
//!   4. Compute output distribution based on operator semantics
//!   5. Return annotated node with any inserted exchanges
//! ```

use crate::ldp::planner::exchange::{
    determine_exchange, determine_join_exchanges, ExchangeDecision, JoinExchangeContext,
    RejectReason,
};
use crate::ldp::planner::metadata::Metadata;
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::planner::requirements::get_logical_plan_requirements;
use crate::ldp::{Distribution, DistributionAnnotation, Exchange, StatsSource, WorkerId};
use crate::sql::logical_plan::LogicalPlan;

/// An annotated LogicalPlan with distribution information and optional exchange.
#[derive(Clone, Debug)]
pub struct AnnotatedPlan {
    /// The logical plan node.
    pub plan: LogicalPlan,

    /// Distribution annotation (output distribution + stats).
    ///
    /// **Note**: When `exchange_before` is `Some`, this holds the *post-exchange*
    /// distribution (what the parent sees).  The original pre-exchange
    /// distribution is stored in `pre_exchange_distribution`.
    pub annotation: DistributionAnnotation,

    /// Exchange to insert BEFORE this node (if any).
    /// This exchange transforms the child's distribution to meet requirements.
    pub exchange_before: Option<Exchange>,

    /// The distribution this node *actually* produces before the exchange is
    /// applied.  Stage cutting uses this to assign the correct target workers
    /// to the stage (which executes BEFORE the exchange).  `None` when there
    /// is no exchange.
    pub pre_exchange_distribution: Option<Distribution>,

    /// Annotated children (in order matching plan's inputs).
    pub children: Vec<AnnotatedPlan>,
}

impl AnnotatedPlan {
    /// Create a new annotated plan without exchange.
    pub fn new(
        plan: LogicalPlan,
        annotation: DistributionAnnotation,
        children: Vec<AnnotatedPlan>,
    ) -> Self {
        Self {
            plan,
            annotation,
            exchange_before: None,
            pre_exchange_distribution: None,
            children,
        }
    }

    /// Create a new annotated plan with an exchange.
    pub fn with_exchange(
        plan: LogicalPlan,
        annotation: DistributionAnnotation,
        children: Vec<AnnotatedPlan>,
        exchange: Exchange,
    ) -> Self {
        Self {
            plan,
            annotation,
            exchange_before: Some(exchange),
            pre_exchange_distribution: None,
            children,
        }
    }

    /// Check if this node or any descendant has an exchange.
    pub fn has_any_exchange(&self) -> bool {
        self.exchange_before.is_some() || self.children.iter().any(|c| c.has_any_exchange())
    }

    /// Count total exchanges in this tree.
    pub fn count_exchanges(&self) -> usize {
        let self_count = if self.exchange_before.is_some() { 1 } else { 0 };
        let child_count: usize = self.children.iter().map(|c| c.count_exchanges()).sum();
        self_count + child_count
    }
}

/// Result of the annotation process.
pub type AnnotationResult = Result<AnnotatedPlan, PlanningError>;

/// Error during LDP planning.
#[derive(Clone, Debug, thiserror::Error)]
pub enum PlanningError {
    /// Query rejected due to data movement limits.
    #[error("Query rejected: {0}")]
    Rejected(RejectReason),

    /// Missing metadata for table.
    #[error("Missing metadata for table: {0}")]
    MissingMetadata(String),

    /// Invalid plan structure.
    #[error("Invalid plan: {0}")]
    InvalidPlan(String),
}

/// Main entry point: annotate a LogicalPlan and enforce distribution requirements.
///
/// This function:
/// 1. Recursively processes the plan bottom-up
/// 2. Annotates each node with its output distribution
/// 3. Inserts exchanges where requirements are not satisfied
///
/// # Arguments
/// * `plan` - The root of the LogicalPlan
/// * `policy` - Planning thresholds and configuration
/// * `metadata` - Access to epoch statistics and placement
///
/// # Returns
/// * `Ok(AnnotatedPlan)` - Successfully annotated plan with exchanges
/// * `Err(PlanningError)` - Planning failed (query rejected or invalid)
pub fn annotate_logical_plan<M: Metadata>(
    plan: &LogicalPlan,
    policy: &PlannerPolicy,
    metadata: &M,
) -> AnnotationResult {
    annotate_recursive(plan, policy, metadata, &[])
}

/// Recursive helper for annotation.
fn annotate_recursive<M: Metadata>(
    plan: &LogicalPlan,
    policy: &PlannerPolicy,
    metadata: &M,
    target_workers: &[WorkerId],
) -> AnnotationResult {
    // 1. Process children recursively
    let logical_children = plan.children();
    let mut annotated_children: Vec<AnnotatedPlan> = Vec::with_capacity(logical_children.len());

    for child in logical_children {
        let annotated = annotate_recursive(child, policy, metadata, target_workers)?;
        annotated_children.push(annotated);
    }

    // 2. Get requirements for this operator
    let requirements = get_logical_plan_requirements(plan);

    // 3. Check if this is a join (needs special handling)
    let is_join = matches!(plan, LogicalPlan::Join { .. });

    // 4. Process requirements and determine exchanges
    let mut exchanges_to_insert: Vec<Option<Exchange>> = vec![None; annotated_children.len()];

    if is_join && annotated_children.len() == 2 {
        // Special handling for joins - use determine_join_exchanges
        let (left_exchange, right_exchange) = handle_join_exchanges(
            plan,
            &annotated_children[0],
            &annotated_children[1],
            policy,
            target_workers,
        )?;

        if let ExchangeDecision::Insert(ex) = left_exchange {
            exchanges_to_insert[0] = Some(ex);
        }
        if let ExchangeDecision::Insert(ex) = right_exchange {
            exchanges_to_insert[1] = Some(ex);
        }
    } else {
        // Standard requirement checking
        for (i, (child, required)) in annotated_children
            .iter()
            .zip(requirements.iter())
            .enumerate()
        {
            let actual = &child.annotation.distribution;

            if required.is_satisfied_by(actual) {
                continue;
            }

            let decision = determine_exchange(
                actual,
                &child.annotation,
                required,
                policy,
                false, // not join context for non-joins
                target_workers,
            );

            match decision {
                ExchangeDecision::None => {}
                ExchangeDecision::Insert(exchange) => {
                    exchanges_to_insert[i] = Some(exchange);
                }
                ExchangeDecision::Reject(reason) => {
                    return Err(PlanningError::Rejected(reason));
                }
            }
        }
    }

    // 5. Apply exchanges to children
    for (child, exchange_opt) in annotated_children
        .iter_mut()
        .zip(exchanges_to_insert.into_iter())
    {
        if let Some(exchange) = exchange_opt {
            // Save the pre-exchange distribution so stage cutting can assign
            // the correct target workers to the stage that runs BEFORE the exchange.
            let pre_exchange_dist = child.annotation.distribution.clone();

            // Update child's annotation to reflect post-exchange distribution
            // (this is what the parent operator sees).
            let new_distribution =
                compute_post_exchange_distribution(&exchange, policy, pre_exchange_dist.workers());
            child.annotation.distribution = new_distribution;
            child.exchange_before = Some(exchange);
            child.pre_exchange_distribution = Some(pre_exchange_dist);
        }
    }

    // 6. Compute this node's output distribution
    let child_distributions: Vec<&Distribution> = annotated_children
        .iter()
        .map(|c| &c.annotation.distribution)
        .collect();

    let (output_distribution, output_rows, output_bytes, output_stats_source) =
        compute_output_distribution(
            plan,
            &child_distributions,
            &annotated_children,
            policy,
            metadata,
        )?;

    let annotation = DistributionAnnotation::with_source(
        output_distribution,
        output_rows,
        output_bytes,
        output_stats_source,
    );

    Ok(AnnotatedPlan::new(
        plan.clone(),
        annotation,
        annotated_children,
    ))
}

/// Handle exchange determination for join operators.
fn handle_join_exchanges(
    plan: &LogicalPlan,
    left_child: &AnnotatedPlan,
    right_child: &AnnotatedPlan,
    policy: &PlannerPolicy,
    target_workers: &[WorkerId],
) -> Result<(ExchangeDecision, ExchangeDecision), PlanningError> {
    // Extract join keys from LogicalPlan::Join
    let (left_keys, right_keys) = match plan {
        LogicalPlan::Join {
            left_keys,
            right_keys,
            ..
        } => {
            if left_keys.is_empty() || right_keys.is_empty() {
                //Cross join - no hash partitioning
                return Ok((ExchangeDecision::None, ExchangeDecision::None));
            }
            (left_keys.clone(), right_keys.clone())
        }
        _ => return Ok((ExchangeDecision::None, ExchangeDecision::None)),
    };

    let (left_decision, right_decision) = determine_join_exchanges(JoinExchangeContext {
        left_annotation: &left_child.annotation,
        right_annotation: &right_child.annotation,
        left_actual: &left_child.annotation.distribution,
        right_actual: &right_child.annotation.distribution,
        left_keys: &left_keys,
        right_keys: &right_keys,
        policy,
        target_workers,
    });

    // Check for rejections
    if let ExchangeDecision::Reject(reason) = &left_decision {
        return Err(PlanningError::Rejected(reason.clone()));
    }
    if let ExchangeDecision::Reject(reason) = &right_decision {
        return Err(PlanningError::Rejected(reason.clone()));
    }

    Ok((left_decision, right_decision))
}

/// Compute the distribution after an exchange is applied.
fn compute_post_exchange_distribution(
    exchange: &Exchange,
    policy: &PlannerPolicy,
    upstream_workers: &[WorkerId],
) -> Distribution {
    match exchange {
        Exchange::Gather { target } => Distribution::Singleton {
            worker: target.clone(),
        },
        Exchange::Broadcast { targets } => Distribution::Replicated {
            workers: targets.clone(),
        },
        Exchange::HashPartition {
            column_refs,
            partitions,
        } => {
            // After hash partition, data should be spread across known workers.
            // Fall back to coordinator if no upstream workers are available.
            let workers = if !upstream_workers.is_empty() {
                upstream_workers.to_vec()
            } else {
                vec![policy.coordinator.clone(); *partitions as usize]
            };
            Distribution::HashPartitioned {
                column_refs: column_refs.clone(),
                workers,
            }
        }
    }
}

/// Compute the output distribution for an operator given its inputs.
///
/// # Arguments
/// * `plan` - The operator
/// * `child_distributions` - Distributions of child inputs (post-exchange)
/// * `annotated_children` - Full annotations of children (for stats)
/// * `policy` - Planning configuration
/// * `metadata` - Access to table statistics
///
/// # Returns
/// (distribution, est_rows, est_bytes, stats_source)
fn compute_output_distribution<M: Metadata>(
    plan: &LogicalPlan,
    child_distributions: &[&Distribution],
    annotated_children: &[AnnotatedPlan],
    policy: &PlannerPolicy,
    metadata: &M,
) -> Result<(Distribution, u64, u64, StatsSource), PlanningError> {
    match plan {
        // ===== Leaf: Scan =====
        LogicalPlan::Scan { table_name, .. } => {
            let scan_stats = metadata.get_table_scan_stats(table_name);
            Ok((
                scan_stats.to_distribution(),
                scan_stats.rows,
                scan_stats.bytes,
                scan_stats.stats_source,
            ))
        }

        // ===== ExchangeRead (placeholder for remote data) =====
        LogicalPlan::ExchangeRead { .. } => {
            // ExchangeRead nodes are only used during stage cutting.
            // At planning time, they shouldn't appear.
            Ok((
                Distribution::Singleton {
                    worker: policy.coordinator.clone(),
                },
                0,
                0,
                StatsSource::Unknown,
            ))
        }

        // ===== Distribution-preserving operators =====
        LogicalPlan::Filter { .. } => {
            // Filter preserves distribution, applies selectivity to stats
            if let Some(child) = annotated_children.first() {
                // Heuristic: assume 50% selectivity
                let selectivity = 0.5;
                let est_rows = (child.annotation.est_rows as f64 * selectivity) as u64;
                let est_bytes = (child.annotation.est_bytes as f64 * selectivity) as u64;
                Ok((
                    child.annotation.distribution.clone(),
                    est_rows,
                    est_bytes,
                    child.annotation.stats_source,
                ))
            } else {
                Ok((
                    Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                    0,
                    0,
                    StatsSource::Unknown,
                ))
            }
        }

        LogicalPlan::Project { .. } => {
            // Project preserves distribution
            if let Some(child) = annotated_children.first() {
                Ok((
                    child.annotation.distribution.clone(),
                    child.annotation.est_rows,
                    child.annotation.est_bytes, // Could adjust based on projection
                    child.annotation.stats_source,
                ))
            } else {
                Ok((
                    Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                    0,
                    0,
                    StatsSource::Unknown,
                ))
            }
        }

        // ===== Singleton-output operators =====
        LogicalPlan::Sort { .. } | LogicalPlan::Limit { .. } => {
            // After gather, output is Singleton
            let (rows, bytes, source) = if let Some(child) = annotated_children.first() {
                (
                    child.annotation.est_rows,
                    child.annotation.est_bytes,
                    child.annotation.stats_source,
                )
            } else {
                (0, 0, StatsSource::Unknown)
            };

            Ok((
                Distribution::Singleton {
                    worker: policy.coordinator.clone(),
                },
                rows,
                bytes,
                source,
            ))
        }

        // ===== Aggregate =====
        LogicalPlan::Aggregate { group_keys, .. } => {
            let (rows, bytes, source) = if let Some(child) = annotated_children.first() {
                if group_keys.is_empty() {
                    // Global aggregate: 1 row output
                    (1, 100, child.annotation.stats_source)
                } else {
                    // Grouped aggregate: estimate based on NDV (heuristic)
                    let estimated_groups =
                        estimate_group_cardinality(child.annotation.est_rows, group_keys.len());
                    let bytes_per_row = if child.annotation.est_rows > 0 {
                        child.annotation.est_bytes / child.annotation.est_rows
                    } else {
                        100
                    };
                    (
                        estimated_groups,
                        estimated_groups * bytes_per_row,
                        child.annotation.stats_source,
                    )
                }
            } else {
                (0, 0, StatsSource::Unknown)
            };

            // Output distribution depends on whether this is partial or final agg
            let output_dist = if let Some(child_dist) = child_distributions.first() {
                match child_dist {
                    Distribution::Singleton { worker } => Distribution::Singleton {
                        worker: worker.clone(),
                    },
                    Distribution::HashPartitioned {
                        column_refs,
                        workers,
                    } => {
                        // Partial aggregate preserves partitioning
                        Distribution::HashPartitioned {
                            column_refs: column_refs.clone(),
                            workers: workers.clone(),
                        }
                    }
                    _ => Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                }
            } else {
                Distribution::Singleton {
                    worker: policy.coordinator.clone(),
                }
            };

            Ok((output_dist, rows, bytes, source))
        }

        // ===== Join operators =====
        LogicalPlan::Join { .. } => {
            // Join output distribution follows the left side (probe side)
            let (dist, rows, bytes, source) = if annotated_children.len() >= 2 {
                let left = &annotated_children[0].annotation;
                let right = &annotated_children[1].annotation;

                // Heuristic: output rows = left_rows * selectivity (0.1 for equi-join)
                let est_rows = (left.est_rows as f64 * 0.1) as u64;
                let bytes_per_row = if left.est_rows > 0 {
                    (left.est_bytes + right.est_bytes) / (left.est_rows.max(1))
                } else {
                    100
                };

                let combined_source = combine_stats_sources(left.stats_source, right.stats_source);

                (
                    left.distribution.clone(),
                    est_rows,
                    est_rows * bytes_per_row,
                    combined_source,
                )
            } else {
                (
                    Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                    0,
                    0,
                    StatsSource::Unknown,
                )
            };

            Ok((dist, rows, bytes, source))
        }

        // ===== Set operators =====
        LogicalPlan::SetOp { .. } => {
            // UNION ALL: sum of rows, inherit distribution from first input
            let total_rows: u64 = annotated_children
                .iter()
                .map(|c| c.annotation.est_rows)
                .sum();
            let total_bytes: u64 = annotated_children
                .iter()
                .map(|c| c.annotation.est_bytes)
                .sum();

            let dist = annotated_children
                .first()
                .map(|c| c.annotation.distribution.clone())
                .unwrap_or(Distribution::Singleton {
                    worker: policy.coordinator.clone(),
                });

            let source = annotated_children
                .iter()
                .fold(StatsSource::Exact, |acc, c| {
                    combine_stats_sources(acc, c.annotation.stats_source)
                });

            Ok((dist, total_rows, total_bytes, source))
        }

        // ===== Window =====
        LogicalPlan::Window { .. } => {
            // Window preserves row count, might change distribution based on PARTITION BY
            if let Some(child) = annotated_children.first() {
                Ok((
                    child.annotation.distribution.clone(),
                    child.annotation.est_rows,
                    child.annotation.est_bytes,
                    child.annotation.stats_source,
                ))
            } else {
                Ok((
                    Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                    0,
                    0,
                    StatsSource::Unknown,
                ))
            }
        }

        // ===== SubqueryScan =====
        LogicalPlan::SubqueryScan { .. } => {
            // Subquery scan preserves child distribution
            if let Some(child) = annotated_children.first() {
                Ok((
                    child.annotation.distribution.clone(),
                    child.annotation.est_rows,
                    child.annotation.est_bytes,
                    child.annotation.stats_source,
                ))
            } else {
                Ok((
                    Distribution::Singleton {
                        worker: policy.coordinator.clone(),
                    },
                    0,
                    0,
                    StatsSource::Unknown,
                ))
            }
        }
    }
}

/// Estimate cardinality after grouping.
fn estimate_group_cardinality(input_rows: u64, num_group_keys: usize) -> u64 {
    if num_group_keys == 0 {
        return 1;
    }

    // Heuristic: sqrt(input_rows) for 1 key, less reduction for more keys
    let base = (input_rows as f64).sqrt() as u64;
    let factor = (num_group_keys as f64).sqrt();
    (base as f64 * factor) as u64
}

/// Combine two stats sources (takes the less confident one).
fn combine_stats_sources(a: StatsSource, b: StatsSource) -> StatsSource {
    match (a, b) {
        (StatsSource::Exact, StatsSource::Exact) => StatsSource::Exact,
        (StatsSource::Unknown, _) | (_, StatsSource::Unknown) => StatsSource::Unknown,
        _ => StatsSource::Estimated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::planner::metadata::InMemoryMetadata;
    use crate::ldp::EpochStats;
    use crate::sql::logical_plan::ColumnRef;
    use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart, TableFactor};

    fn test_policy() -> PlannerPolicy {
        PlannerPolicy::with_coordinator("coordinator")
            .broadcast_bytes_max(1000)
            .shuffle_bytes_max(10000)
            .gather_rows_max(10000)
            .gather_bytes_max(100000)
    }

    fn test_metadata() -> InMemoryMetadata {
        InMemoryMetadata::new()
            .with_epoch("e1", "sales", EpochStats::exact(1000, 10000), "w1".into())
            .with_epoch("e2", "sales", EpochStats::exact(2000, 20000), "w2".into())
    }

    fn create_scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: table.to_string(),
            alias: None,
            table_factor: TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
        }
    }

    #[test]
    fn test_annotate_scan() {
        let policy = test_policy();
        let metadata = test_metadata();
        let plan = create_scan("sales");

        let result = annotate_logical_plan(&plan, &policy, &metadata);
        assert!(result.is_ok());

        let annotated = result.unwrap();
        assert_eq!(annotated.annotation.est_rows, 3000); // 1000 + 2000
        assert_eq!(annotated.annotation.est_bytes, 30000); // 10000 + 20000
        assert!(annotated.annotation.stats_source.is_exact());
        assert!(matches!(
            annotated.annotation.distribution,
            Distribution::EpochPartitioned { .. }
        ));
    }

    #[test]
    fn test_annotate_filter() {
        let policy = test_policy();
        let metadata = test_metadata();

        let plan = LogicalPlan::Filter {
            input: Box::new(create_scan("sales")),
            predicate: Expr::Identifier(Ident::new("dummy")),
        };

        let result = annotate_logical_plan(&plan, &policy, &metadata);
        assert!(result.is_ok());

        let annotated = result.unwrap();
        // Filter applies 50% selectivity heuristic
        assert_eq!(annotated.annotation.est_rows, 1500);
        assert!(annotated.children.len() == 1);
    }

    #[test]
    fn test_annotate_sort_inserts_gather() {
        let policy = test_policy();
        let metadata = test_metadata();

        let plan = LogicalPlan::Sort {
            input: Box::new(create_scan("sales")),
            order_by: vec![],
        };

        let result = annotate_logical_plan(&plan, &policy, &metadata);
        assert!(result.is_ok());

        let annotated = result.unwrap();
        // Sort requires Singleton, so gather should be inserted on child
        assert!(annotated.children[0].exchange_before.is_some());
        assert!(matches!(
            annotated.children[0].exchange_before,
            Some(Exchange::Gather { .. })
        ));
    }

    #[test]
    fn test_count_exchanges() {
        let policy = test_policy();
        let metadata = test_metadata();

        let plan = LogicalPlan::Sort {
            input: Box::new(create_scan("sales")),
            order_by: vec![],
        };

        let annotated = annotate_logical_plan(&plan, &policy, &metadata).unwrap();
        assert_eq!(annotated.count_exchanges(), 1);
    }

    #[test]
    fn test_compute_post_exchange_uses_upstream_workers() {
        let exchange = Exchange::hash_partition(vec![ColumnRef::unqualified("id")], 4);
        let dist = compute_post_exchange_distribution(
            &exchange,
            &test_policy(),
            &["w1".into(), "w2".into()],
        );

        match dist {
            Distribution::HashPartitioned { workers, .. } => {
                assert_eq!(workers, vec![WorkerId::from("w1"), WorkerId::from("w2")]);
            }
            _ => panic!("expected hash partitioned distribution"),
        }
    }
}
