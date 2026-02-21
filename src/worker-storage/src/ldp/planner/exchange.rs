//! Exchange selection logic for LDP planning.
//!
//! Determines which exchange type to use when the actual distribution
//! does not satisfy the required distribution.
//!
//! # Baseline-First Strategy
//! - Shuffle (HashPartition) is the default/baseline (always correct)
//! - Broadcast is an optimization that requires:
//!   - Exact statistics
//!   - Join context (not aggregation)
//!   - Small data size (<= broadcast_bytes_max)

use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::{Distribution, DistributionAnnotation, Exchange, RequiredDistribution, WorkerId};
use crate::sql::logical_plan::ColumnRef;

/// Context bundle for determining exchange decisions for a join operator.
pub struct JoinExchangeContext<'a> {
    /// Statistics annotation for the left input.
    pub left_annotation: &'a DistributionAnnotation,
    /// Statistics annotation for the right input.
    pub right_annotation: &'a DistributionAnnotation,
    /// Actual output distribution of the left input.
    pub left_actual: &'a Distribution,
    /// Actual output distribution of the right input.
    pub right_actual: &'a Distribution,
    /// Join keys for the left side.
    pub left_keys: &'a [ColumnRef],
    /// Join keys for the right side.
    pub right_keys: &'a [ColumnRef],
    /// Planner policy (thresholds, limits).
    pub policy: &'a PlannerPolicy,
    /// Workers that will execute the join stage.
    pub target_workers: &'a [WorkerId],
}

/// Result of exchange determination.
#[derive(Clone, Debug)]
pub enum ExchangeDecision {
    /// No exchange needed - current distribution satisfies requirement.
    None,

    /// Insert the specified exchange.
    Insert(Exchange),

    /// Query should be rejected - data movement exceeds limits.
    Reject(RejectReason),
}

/// Reason for rejecting a query due to data movement limits.
#[derive(Clone, Debug, PartialEq)]
pub enum RejectReason {
    /// Shuffle would exceed shuffle_bytes_max.
    ShuffleTooLarge { estimated_bytes: u64, limit: u64 },

    /// Gather would exceed gather limits.
    GatherTooLarge {
        estimated_rows: u64,
        estimated_bytes: u64,
        row_limit: u64,
        byte_limit: u64,
    },

    /// Cannot determine appropriate exchange.
    CannotDetermineExchange(String),
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::ShuffleTooLarge {
                estimated_bytes,
                limit,
            } => {
                write!(
                    f,
                    "Shuffle too large: {} bytes exceeds limit of {} bytes",
                    estimated_bytes, limit
                )
            }
            RejectReason::GatherTooLarge {
                estimated_rows,
                estimated_bytes,
                row_limit,
                byte_limit,
            } => {
                write!(
                    f,
                    "Gather too large: {} rows/{} bytes exceeds limits of {} rows/{} bytes",
                    estimated_rows, estimated_bytes, row_limit, byte_limit
                )
            }
            RejectReason::CannotDetermineExchange(msg) => {
                write!(f, "Cannot determine exchange: {}", msg)
            }
        }
    }
}

/// Determine the exchange needed to transform actual distribution to required.
///
/// # Arguments
/// * `actual` - The current distribution of the data
/// * `annotation` - Statistics about the data (rows, bytes, confidence)
/// * `required` - The distribution requirement
/// * `policy` - Planning thresholds and configuration
/// * `is_join_context` - Whether this is for a join input (enables broadcast optimization)
/// * `target_workers` - Workers for the downstream stage (for Gather/Broadcast targets)
///
/// # Returns
/// * `ExchangeDecision::None` if no exchange is needed
/// * `ExchangeDecision::Insert(exchange)` if an exchange should be inserted
/// * `ExchangeDecision::Reject(reason)` if the query should be rejected
pub fn determine_exchange(
    actual: &Distribution,
    annotation: &DistributionAnnotation,
    required: &RequiredDistribution,
    policy: &PlannerPolicy,
    is_join_context: bool,
    target_workers: &[WorkerId],
) -> ExchangeDecision {
    // Fast path: already satisfied
    if required.is_satisfied_by(actual) {
        return ExchangeDecision::None;
    }

    // Get estimates with safety factor applied
    let (est_rows, est_bytes) = annotation.with_safety_factor(policy.safety_factor);
    let stats_exact = annotation.stats_source.is_exact();

    match required {
        RequiredDistribution::Any => {
            // Any is always satisfied - should not reach here
            ExchangeDecision::None
        }

        RequiredDistribution::Singleton => {
            // Need to gather all data to coordinator
            determine_gather_exchange(est_rows, est_bytes, stats_exact, policy)
        }

        RequiredDistribution::HashPartitioned { column_refs } => {
            // Need to redistribute by hash keys
            determine_hash_or_broadcast_exchange(
                actual,
                annotation,
                column_refs,
                policy,
                is_join_context,
                target_workers,
            )
        }
    }
}

/// Determine if Gather is feasible, or should be rejected.
fn determine_gather_exchange(
    est_rows: u64,
    est_bytes: u64,
    stats_exact: bool,
    policy: &PlannerPolicy,
) -> ExchangeDecision {
    if policy.can_gather_with_safety(est_rows, est_bytes, stats_exact) {
        ExchangeDecision::Insert(Exchange::gather(policy.coordinator.clone()))
    } else {
        ExchangeDecision::Reject(RejectReason::GatherTooLarge {
            estimated_rows: est_rows,
            estimated_bytes: est_bytes,
            row_limit: policy.gather_rows_max,
            byte_limit: policy.gather_bytes_max,
        })
    }
}

/// Determine whether to use HashPartition or Broadcast for a hash requirement.
///
/// # Baseline-First Logic
/// 1. Check if broadcast optimization applies (join context + exact stats + small)
/// 2. Otherwise, use shuffle (hash partition) as baseline
/// 3. Check shuffle size limits
fn determine_hash_or_broadcast_exchange(
    actual: &Distribution,
    annotation: &DistributionAnnotation,
    required_column_refs: &[ColumnRef],
    policy: &PlannerPolicy,
    is_join_context: bool,
    target_workers: &[WorkerId],
) -> ExchangeDecision {
    let (_, est_bytes) = annotation.with_safety_factor(policy.safety_factor);
    let stats_exact = annotation.stats_source.is_exact();

    // === Broadcast Optimization ===
    // Only applies when:
    // 1. This is a join context (not aggregation GROUP BY)
    // 2. Stats are exact (we can trust the size estimate)
    // 3. Data is small enough to broadcast
    if policy.can_optimize_to_broadcast(est_bytes, stats_exact, is_join_context) {
        // Broadcast to all target workers
        let targets = if target_workers.is_empty() {
            actual.workers().to_vec()
        } else {
            target_workers.to_vec()
        };

        return ExchangeDecision::Insert(Exchange::broadcast(targets));
    }

    // === Baseline: Hash Partition ===
    // Check if shuffle is within limits
    if !policy.can_shuffle(est_bytes) {
        return ExchangeDecision::Reject(RejectReason::ShuffleTooLarge {
            estimated_bytes: est_bytes,
            limit: policy.shuffle_bytes_max,
        });
    }

    // Create hash partition exchange
    ExchangeDecision::Insert(Exchange::hash_partition(
        required_column_refs.to_vec(),
        policy.default_partitions,
    ))
}

/// Determine exchange for a join input.
///
/// This is a higher-level function that handles join-specific logic,
/// including broadcast decisions for both sides.
///
/// # Arguments
/// * `left_annotation` - Stats for left input
/// * `right_annotation` - Stats for right input  
/// * `left_actual` - Distribution of left input
/// * `right_actual` - Distribution of right input
/// * `left_keys` - Join keys for left side
/// * `right_keys` - Join keys for right side
/// * `policy` - Planning thresholds
/// * `target_workers` - Workers for the join stage
///
/// # Returns
/// (left_decision, right_decision) - Exchange decisions for each side
pub fn determine_join_exchanges(
    ctx: JoinExchangeContext<'_>,
) -> (ExchangeDecision, ExchangeDecision) {
    let JoinExchangeContext {
        left_annotation,
        right_annotation,
        left_actual,
        right_actual,
        left_keys,
        right_keys,
        policy,
        target_workers,
    } = ctx;
    // === Colocation Optimization 1: Replicated build side ===
    // If the right side is already replicated by control-plane placement,
    // no exchange is needed for either side.
    if matches!(right_actual, Distribution::Replicated { .. }) {
        tracing::info!(
            "Detected replicated build side - skipping exchanges (colocation optimization)"
        );
        return (ExchangeDecision::None, ExchangeDecision::None);
    }

    // === Colocation Optimization 2: Co-partitioned join ===
    // If both sides are hash-partitioned on the join keys with matching worker sets,
    // they are already colocated for local joins.
    if let (
        Distribution::HashPartitioned {
            column_refs: left_partition_keys,
            workers: left_workers,
        },
        Distribution::HashPartitioned {
            column_refs: right_partition_keys,
            workers: right_workers,
        },
    ) = (left_actual, right_actual)
    {
        if keys_match(left_partition_keys, left_keys)
            && keys_match(right_partition_keys, right_keys)
            && left_workers == right_workers
        {
            tracing::info!(
                "Detected co-partitioned join on matching keys - skipping exchanges (colocation optimization)"
            );
            return (ExchangeDecision::None, ExchangeDecision::None);
        }
    }

    let left_requirement = RequiredDistribution::HashPartitioned {
        column_refs: left_keys.to_vec(),
    };
    let right_requirement = RequiredDistribution::HashPartitioned {
        column_refs: right_keys.to_vec(),
    };

    // Check if already satisfied
    let left_satisfied = left_requirement.is_satisfied_by(left_actual);
    let right_satisfied = right_requirement.is_satisfied_by(right_actual);

    if left_satisfied && right_satisfied {
        return (ExchangeDecision::None, ExchangeDecision::None);
    }

    // Get size estimates
    let (_, left_bytes) = left_annotation.with_safety_factor(policy.safety_factor);
    let (_, right_bytes) = right_annotation.with_safety_factor(policy.safety_factor);
    let left_exact = left_annotation.stats_source.is_exact();
    let right_exact = right_annotation.stats_source.is_exact();

    // === Broadcast Strategy Selection ===
    // Prefer broadcasting the smaller side if it meets criteria.
    //
    // Broadcast targets: when broadcasting side X to side Y, the targets are
    // Y's workers (where the non-broadcast data resides). If `target_workers`
    // was provided externally we honour it; otherwise fall back to the other
    // side's worker set so the annotator sees the correct post-exchange
    // distribution and stage cutting assigns the right target workers.
    let left_can_broadcast = policy.can_optimize_to_broadcast(left_bytes, left_exact, true);
    let right_can_broadcast = policy.can_optimize_to_broadcast(right_bytes, right_exact, true);

    // Resolve broadcast targets: prefer explicit target_workers; fall back to
    // the other join side's workers.
    let broadcast_targets_for_left = if !target_workers.is_empty() {
        target_workers.to_vec()
    } else {
        right_actual.workers().to_vec()
    };
    let broadcast_targets_for_right = if !target_workers.is_empty() {
        target_workers.to_vec()
    } else {
        left_actual.workers().to_vec()
    };

    match (left_can_broadcast, right_can_broadcast) {
        // Both can broadcast - pick smaller
        (true, true) => {
            if left_bytes <= right_bytes {
                // Broadcast left to right's workers
                let left_exchange = if left_satisfied {
                    ExchangeDecision::None
                } else {
                    ExchangeDecision::Insert(Exchange::broadcast(broadcast_targets_for_left))
                };
                (left_exchange, ExchangeDecision::None)
            } else {
                // Broadcast right to left's workers
                let right_exchange = if right_satisfied {
                    ExchangeDecision::None
                } else {
                    ExchangeDecision::Insert(Exchange::broadcast(broadcast_targets_for_right))
                };
                (ExchangeDecision::None, right_exchange)
            }
        }
        // Only left can broadcast → broadcast left to right's workers
        (true, false) => {
            let left_exchange = if left_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::broadcast(broadcast_targets_for_left))
            };
            (left_exchange, ExchangeDecision::None)
        }
        // Only right can broadcast → broadcast right to left's workers
        (false, true) => {
            let right_exchange = if right_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::broadcast(broadcast_targets_for_right))
            };
            (ExchangeDecision::None, right_exchange)
        }
        // Neither can broadcast - shuffle both
        (false, false) => {
            let total_shuffle = left_bytes + right_bytes;

            // Check total shuffle limit
            if !policy.can_shuffle(total_shuffle) {
                return (
                    ExchangeDecision::Reject(RejectReason::ShuffleTooLarge {
                        estimated_bytes: total_shuffle,
                        limit: policy.shuffle_bytes_max,
                    }),
                    ExchangeDecision::Reject(RejectReason::ShuffleTooLarge {
                        estimated_bytes: total_shuffle,
                        limit: policy.shuffle_bytes_max,
                    }),
                );
            }

            let left_exchange = if left_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::hash_partition(
                    left_keys.to_vec(),
                    policy.default_partitions,
                ))
            };

            let right_exchange = if right_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::hash_partition(
                    right_keys.to_vec(),
                    policy.default_partitions,
                ))
            };

            (left_exchange, right_exchange)
        }
    }
}

fn keys_match(partition_keys: &[ColumnRef], join_keys: &[ColumnRef]) -> bool {
    partition_keys.len() == join_keys.len()
        && partition_keys
            .iter()
            .zip(join_keys.iter())
            .all(|(partition_key, join_key)| partition_key.matches(join_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::StatsSource;

    fn test_policy() -> PlannerPolicy {
        PlannerPolicy::with_coordinator("coordinator")
            .broadcast_bytes_max(100) // 100 bytes for easy testing
            .shuffle_bytes_max(1000)
            .gather_rows_max(100)
            .gather_bytes_max(500)
    }

    #[test]
    fn test_already_satisfied() {
        let policy = test_policy();
        let actual = Distribution::Singleton {
            worker: "w1".into(),
        };
        let annotation = DistributionAnnotation::new(actual.clone(), 10, 100);

        let decision = determine_exchange(
            &actual,
            &annotation,
            &RequiredDistribution::Singleton,
            &policy,
            false,
            &[],
        );

        assert!(matches!(decision, ExchangeDecision::None));
    }

    #[test]
    fn test_gather_exchange() {
        let policy = test_policy();
        let actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into(), "w2".into()],
        };
        let annotation = DistributionAnnotation::new(actual.clone(), 50, 200);

        let decision = determine_exchange(
            &actual,
            &annotation,
            &RequiredDistribution::Singleton,
            &policy,
            false,
            &[],
        );

        match decision {
            ExchangeDecision::Insert(Exchange::Gather { target }) => {
                assert_eq!(target, "coordinator");
            }
            _ => panic!("Expected Gather exchange"),
        }
    }

    #[test]
    fn test_gather_rejected_too_large() {
        let policy = test_policy();
        let actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into(), "w2".into()],
        };
        let annotation = DistributionAnnotation::new(actual.clone(), 1000, 10000);

        let decision = determine_exchange(
            &actual,
            &annotation,
            &RequiredDistribution::Singleton,
            &policy,
            false,
            &[],
        );

        assert!(matches!(decision, ExchangeDecision::Reject(_)));
    }

    #[test]
    fn test_broadcast_in_join_context_with_exact_stats() {
        let policy = test_policy();
        let actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into()],
        };
        // Small data with exact stats
        let annotation = DistributionAnnotation::with_source(
            actual.clone(),
            10,
            50, // under broadcast_bytes_max of 100
            StatsSource::Exact,
        );

        let required = RequiredDistribution::HashPartitioned {
            column_refs: vec![ColumnRef {
                table: None,
                column: "col0".into(),
            }],
        };

        // In join context with exact stats -> broadcast
        let decision = determine_exchange(
            &actual,
            &annotation,
            &required,
            &policy,
            true, // join context
            &["w1".into(), "w2".into()],
        );

        match decision {
            ExchangeDecision::Insert(Exchange::Broadcast { targets }) => {
                assert_eq!(targets.len(), 2);
            }
            _ => panic!("Expected Broadcast exchange, got {:?}", decision),
        }
    }

    #[test]
    fn test_shuffle_without_join_context() {
        let policy = test_policy();
        let actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into()],
        };
        // Small data but NOT join context
        let annotation = DistributionAnnotation::with_source(
            actual.clone(),
            10,
            50, // under broadcast_bytes_max
            StatsSource::Exact,
        );

        let required = RequiredDistribution::HashPartitioned {
            column_refs: vec![ColumnRef {
                table: None,
                column: "col0".into(),
            }],
        };

        // Not join context -> shuffle even if small
        let decision = determine_exchange(
            &actual,
            &annotation,
            &required,
            &policy,
            false, // NOT join context
            &[],
        );

        match decision {
            ExchangeDecision::Insert(Exchange::HashPartition { column_refs, .. }) => {
                assert_eq!(column_refs.len(), 1);
                assert_eq!(column_refs[0].column, "col0");
            }
            _ => panic!("Expected HashPartition exchange, got {:?}", decision),
        }
    }

    #[test]
    fn test_shuffle_with_uncertain_stats() {
        let policy = test_policy();
        let actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into()],
        };
        // Small data but UNCERTAIN stats
        let annotation = DistributionAnnotation::with_source(
            actual.clone(),
            10,
            50,
            StatsSource::Estimated, // not exact
        );

        let required = RequiredDistribution::HashPartitioned {
            column_refs: vec![ColumnRef {
                table: None,
                column: "col0".into(),
            }],
        };

        // Join context but uncertain stats -> shuffle (baseline)
        let decision = determine_exchange(
            &actual,
            &annotation,
            &required,
            &policy,
            true, // join context
            &[],
        );

        // With safety factor 2.0, 50 bytes becomes 100 bytes
        // which equals broadcast_bytes_max, but we need EXACT stats for broadcast
        match decision {
            ExchangeDecision::Insert(Exchange::HashPartition { .. }) => {}
            _ => panic!(
                "Expected HashPartition exchange (baseline), got {:?}",
                decision
            ),
        }
    }

    #[test]
    fn test_join_shortcut_replicated_build_side() {
        let policy = test_policy();
        let left_actual = Distribution::EpochPartitioned {
            workers: vec!["w1".into(), "w2".into()],
        };
        let right_actual = Distribution::Replicated {
            workers: vec!["w1".into(), "w2".into()],
        };

        let left_annotation = DistributionAnnotation::with_source(
            left_actual.clone(),
            10_000,
            1_000_000,
            StatsSource::Exact,
        );
        let right_annotation = DistributionAnnotation::with_source(
            right_actual.clone(),
            1_000,
            100_000,
            StatsSource::Exact,
        );

        let left_key = ColumnRef {
            table: None,
            column: "col0".into(),
        };
        let right_key = ColumnRef {
            table: None,
            column: "col0".into(),
        };

        let (left_decision, right_decision) = determine_join_exchanges(JoinExchangeContext {
            left_annotation: &left_annotation,
            right_annotation: &right_annotation,
            left_actual: &left_actual,
            right_actual: &right_actual,
            left_keys: &[left_key],
            right_keys: &[right_key],
            policy: &policy,
            target_workers: &["w1".into(), "w2".into()],
        });

        assert!(matches!(left_decision, ExchangeDecision::None));
        assert!(matches!(right_decision, ExchangeDecision::None));
    }

    #[test]
    fn test_join_shortcut_co_partitioned() {
        let policy = test_policy();
        let left_actual = Distribution::HashPartitioned {
            column_refs: vec![ColumnRef {
                table: None,
                column: "col0".into(),
            }],
            workers: vec!["w1".into(), "w2".into(), "w3".into()],
        };
        let right_actual = Distribution::HashPartitioned {
            column_refs: vec![ColumnRef {
                table: None,
                column: "col0".into(),
            }],
            workers: vec!["w1".into(), "w2".into(), "w3".into()],
        };

        let left_annotation = DistributionAnnotation::with_source(
            left_actual.clone(),
            100_000,
            10_000_000,
            StatsSource::Exact,
        );
        let right_annotation = DistributionAnnotation::with_source(
            right_actual.clone(),
            100_000,
            10_000_000,
            StatsSource::Exact,
        );

        let left_key = ColumnRef {
            table: None,
            column: "col0".into(),
        };
        let right_key = ColumnRef {
            table: None,
            column: "col0".into(),
        };

        let (left_decision, right_decision) = determine_join_exchanges(JoinExchangeContext {
            left_annotation: &left_annotation,
            right_annotation: &right_annotation,
            left_actual: &left_actual,
            right_actual: &right_actual,
            left_keys: &[left_key],
            right_keys: &[right_key],
            policy: &policy,
            target_workers: &["w1".into(), "w2".into(), "w3".into()],
        });

        assert!(matches!(left_decision, ExchangeDecision::None));
        assert!(matches!(right_decision, ExchangeDecision::None));
    }
}
