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

        RequiredDistribution::HashPartitioned { field_refs } => {
            // Need to redistribute by hash keys
            determine_hash_or_broadcast_exchange(
                actual,
                annotation,
                field_refs,
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
    required_field_refs: &[u32],
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
        required_field_refs.to_vec(),
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
    left_annotation: &DistributionAnnotation,
    right_annotation: &DistributionAnnotation,
    left_actual: &Distribution,
    right_actual: &Distribution,
    left_keys: &[u32],
    right_keys: &[u32],
    policy: &PlannerPolicy,
    target_workers: &[WorkerId],
) -> (ExchangeDecision, ExchangeDecision) {
    let left_requirement = RequiredDistribution::HashPartitioned {
        field_refs: left_keys.to_vec(),
    };
    let right_requirement = RequiredDistribution::HashPartitioned {
        field_refs: right_keys.to_vec(),
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
    // Prefer broadcasting the smaller side if it meets criteria
    let left_can_broadcast = policy.can_optimize_to_broadcast(left_bytes, left_exact, true);
    let right_can_broadcast = policy.can_optimize_to_broadcast(right_bytes, right_exact, true);

    match (left_can_broadcast, right_can_broadcast) {
        // Both can broadcast - pick smaller
        (true, true) => {
            if left_bytes <= right_bytes {
                // Broadcast left
                let left_exchange = if left_satisfied {
                    ExchangeDecision::None
                } else {
                    ExchangeDecision::Insert(Exchange::broadcast(target_workers.to_vec()))
                };
                (left_exchange, ExchangeDecision::None)
            } else {
                // Broadcast right
                let right_exchange = if right_satisfied {
                    ExchangeDecision::None
                } else {
                    ExchangeDecision::Insert(Exchange::broadcast(target_workers.to_vec()))
                };
                (ExchangeDecision::None, right_exchange)
            }
        }
        // Only left can broadcast
        (true, false) => {
            let left_exchange = if left_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::broadcast(target_workers.to_vec()))
            };
            (left_exchange, ExchangeDecision::None)
        }
        // Only right can broadcast
        (false, true) => {
            let right_exchange = if right_satisfied {
                ExchangeDecision::None
            } else {
                ExchangeDecision::Insert(Exchange::broadcast(target_workers.to_vec()))
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
            field_refs: vec![0],
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
            field_refs: vec![0],
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
            ExchangeDecision::Insert(Exchange::HashPartition { field_refs, .. }) => {
                assert_eq!(field_refs, vec![0]);
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
            field_refs: vec![0],
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
            _ => panic!("Expected HashPartition exchange (baseline), got {:?}", decision),
        }
    }
}
