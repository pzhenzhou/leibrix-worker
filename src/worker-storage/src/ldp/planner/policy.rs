//! Planner policy configuration.
//!
//! Defines bounds and thresholds for cost-based planning decisions.
//!
//! # Design Principles
//! - **Baseline-first**: Shuffle is the default strategy (always correct)
//! - **Stats enable optimization**: Broadcast only when stats are exact and small
//! - **Conservative admission**: Apply safety factor when stats are uncertain

use crate::ldp::types::WorkerId;

/// Configuration for the LDP planner's cost-based decisions.
///
/// These bounds determine when the planner will:
/// - Use broadcast vs hash partition for joins
/// - Accept or reject queries based on estimated data movement
/// - Choose the number of hash partitions
///
/// # Baseline-First Strategy
/// The planner uses conservative defaults when statistics are uncertain:
/// - Hash partition shuffle is always the baseline (correct for any data size)
/// - Broadcast optimization requires exact stats AND join context AND small size
#[derive(Clone, Debug, PartialEq)]
pub struct PlannerPolicy {
    /// Maximum bytes for broadcast exchange.
    /// Data smaller than this MAY be broadcast (if stats are exact and join context).
    /// Default: 256MB
    pub broadcast_bytes_max: u64,

    /// Maximum bytes for shuffle (hash partition) exchange.
    /// Queries requiring larger shuffles will be rejected.
    /// Default: 2GB
    pub shuffle_bytes_max: u64,

    /// Maximum rows to gather to a single node.
    /// Default: 50 million
    pub gather_rows_max: u64,

    /// Maximum bytes to gather to a single node.
    /// Queries requiring larger gathers will be rejected.
    /// Default: 5GB
    pub gather_bytes_max: u64,

    /// Default number of hash partitions for shuffle.
    /// Used when hash partitioning is selected.
    /// Default: 16
    pub default_partitions: u32,

    /// Coordinator worker ID (where gather operations send data).
    /// This is typically set at runtime based on which worker
    /// receives the query.
    pub coordinator: WorkerId,

    /// Safety factor applied to estimated stats when confidence is low.
    /// Used for conservative admission control.
    /// Default: 2.0 (assume 2x the estimated size)
    pub safety_factor: f64,

    /// Enable streaming pipelined execution path.
    /// When true, stages execute concurrently with bounded memory buffers,
    /// reducing latency and memory footprint compared to batch mode.
    /// Default: false (batch mode) for stability.
    pub enable_streaming_pipeline: bool,

    /// Target buffer size per exchange for streaming mode.
    /// Controls memory usage during pipelined execution.
    /// Default: 32MB.
    pub pipeline_buffer_bytes: u64,
}

impl Default for PlannerPolicy {
    fn default() -> Self {
        Self {
            broadcast_bytes_max: 256 * 1024 * 1024,       // 256MB
            shuffle_bytes_max: 2 * 1024 * 1024 * 1024,    // 2GB
            gather_rows_max: 50_000_000,                  // 50M rows
            gather_bytes_max: 5 * 1024 * 1024 * 1024,     // 5GB
            default_partitions: 16,
            coordinator: WorkerId::default(),
            safety_factor: 2.0,                           // 2x for uncertain stats
            enable_streaming_pipeline: false,
            pipeline_buffer_bytes: 32 * 1024 * 1024,
        }
    }
}

impl PlannerPolicy {
    /// Conservative preset: disable broadcast optimization, maximize safety.
    pub fn conservative() -> Self {
        Self {
            broadcast_bytes_max: 0,
            shuffle_bytes_max: u64::MAX,
            gather_rows_max: u64::MAX,
            gather_bytes_max: u64::MAX,
            default_partitions: 16,
            coordinator: WorkerId::default(),
            safety_factor: 3.0,
            enable_streaming_pipeline: false,
            pipeline_buffer_bytes: 32 * 1024 * 1024,
        }
    }

    /// Aggressive preset: favor broadcast and trust stats more.
    pub fn aggressive() -> Self {
        Self {
            broadcast_bytes_max: 1024 * 1024 * 1024,      // 1GB
            shuffle_bytes_max: 10 * 1024 * 1024 * 1024,   // 10GB
            gather_rows_max: 100_000_000,
            gather_bytes_max: 10 * 1024 * 1024 * 1024,
            default_partitions: 32,
            coordinator: WorkerId::default(),
            safety_factor: 1.1,
            enable_streaming_pipeline: false,
            pipeline_buffer_bytes: 32 * 1024 * 1024,
        }
    }

    /// Testing preset: enables common optimizations with deterministic thresholds.
    pub fn for_testing() -> Self {
        Self::default()
            .with_broadcast_enabled(true)
            .with_safety_factor(1.0)
    }

    /// Create a new policy with the given coordinator.
    pub fn with_coordinator(coordinator: impl Into<WorkerId>) -> Self {
        Self {
            coordinator: coordinator.into(),
            ..Default::default()
        }
    }

    /// Builder method to set broadcast threshold.
    pub fn broadcast_bytes_max(mut self, bytes: u64) -> Self {
        self.broadcast_bytes_max = bytes;
        self
    }

    /// Builder method to set shuffle threshold.
    pub fn shuffle_bytes_max(mut self, bytes: u64) -> Self {
        self.shuffle_bytes_max = bytes;
        self
    }

    /// Builder method to set gather rows limit.
    pub fn gather_rows_max(mut self, rows: u64) -> Self {
        self.gather_rows_max = rows;
        self
    }

    /// Builder method to set gather bytes limit.
    pub fn gather_bytes_max(mut self, bytes: u64) -> Self {
        self.gather_bytes_max = bytes;
        self
    }

    /// Builder method to set default partitions.
    pub fn default_partitions(mut self, partitions: u32) -> Self {
        self.default_partitions = partitions;
        self
    }

    /// Builder method to set safety factor.
    pub fn safety_factor(mut self, factor: f64) -> Self {
        self.safety_factor = factor;
        self
    }

    /// Alias for consistency with fluent style in tests/configuration.
    pub fn with_safety_factor(self, factor: f64) -> Self {
        self.safety_factor(factor)
    }

    /// Enable/disable broadcast by threshold toggling.
    pub fn with_broadcast_enabled(mut self, enabled: bool) -> Self {
        self.broadcast_bytes_max = if enabled {
            256 * 1024 * 1024
        } else {
            0
        };
        self
    }

    /// Enable or disable streaming pipelined execution.
    /// When enabled, stages execute concurrently with bounded buffers.
    pub fn with_streaming_pipeline(mut self, enabled: bool) -> Self {
        self.enable_streaming_pipeline = enabled;
        self
    }

    /// Set target streaming exchange buffer size.
    /// Controls memory usage during pipelined execution.
    pub fn with_pipeline_buffer_bytes(mut self, bytes: u64) -> Self {
        self.pipeline_buffer_bytes = bytes;
        self
    }

    /// Check if data size is suitable for broadcast.
    /// Note: This only checks size. Caller must also verify:
    /// - Context is JOIN (not GROUP BY)
    /// - Stats source is Exact
    pub fn can_broadcast(&self, estimated_bytes: u64) -> bool {
        estimated_bytes <= self.broadcast_bytes_max
    }

    /// Check if broadcast optimization can be applied.
    /// Requires: join context, exact stats, and small size.
    pub fn can_optimize_to_broadcast(
        &self,
        estimated_bytes: u64,
        stats_exact: bool,
        is_join_context: bool,
    ) -> bool {
        is_join_context && stats_exact && estimated_bytes <= self.broadcast_bytes_max
    }

    /// Check if data size is within shuffle limits.
    pub fn can_shuffle(&self, estimated_bytes: u64) -> bool {
        estimated_bytes <= self.shuffle_bytes_max
    }

    /// Check if data can be gathered to a single node.
    /// Applies safety factor if stats are uncertain.
    pub fn can_gather(&self, estimated_rows: u64, estimated_bytes: u64) -> bool {
        estimated_rows <= self.gather_rows_max && estimated_bytes <= self.gather_bytes_max
    }

    /// Check if data can be gathered, applying safety factor for uncertain stats.
    pub fn can_gather_with_safety(
        &self,
        estimated_rows: u64,
        estimated_bytes: u64,
        stats_exact: bool,
    ) -> bool {
        let (rows, bytes) = if stats_exact {
            (estimated_rows, estimated_bytes)
        } else {
            (
                (estimated_rows as f64 * self.safety_factor) as u64,
                (estimated_bytes as f64 * self.safety_factor) as u64,
            )
        };
        rows <= self.gather_rows_max && bytes <= self.gather_bytes_max
    }

    /// Apply safety factor to estimates if stats are uncertain.
    pub fn apply_safety_factor(&self, value: u64, stats_exact: bool) -> u64 {
        if stats_exact {
            value
        } else {
            (value as f64 * self.safety_factor) as u64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = PlannerPolicy::default();
        assert_eq!(policy.broadcast_bytes_max, 256 * 1024 * 1024);
        assert_eq!(policy.shuffle_bytes_max, 2 * 1024 * 1024 * 1024);
        assert_eq!(policy.gather_rows_max, 50_000_000);
        assert_eq!(policy.gather_bytes_max, 5 * 1024 * 1024 * 1024);
        assert_eq!(policy.default_partitions, 16);
        assert_eq!(policy.safety_factor, 2.0);
        assert!(!policy.enable_streaming_pipeline);
        assert_eq!(policy.pipeline_buffer_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn test_policy_builder() {
        let policy = PlannerPolicy::with_coordinator("worker-1")
            .broadcast_bytes_max(128 * 1024 * 1024)
            .default_partitions(32);

        assert_eq!(policy.coordinator, WorkerId::from("worker-1"));
        assert_eq!(policy.broadcast_bytes_max, 128 * 1024 * 1024);
        assert_eq!(policy.default_partitions, 32);
    }

    #[test]
    fn test_can_broadcast() {
        let policy = PlannerPolicy::default();

        // Under threshold
        assert!(policy.can_broadcast(100 * 1024 * 1024)); // 100MB
        // At threshold
        assert!(policy.can_broadcast(256 * 1024 * 1024)); // 256MB
        // Over threshold
        assert!(!policy.can_broadcast(300 * 1024 * 1024)); // 300MB
    }

    #[test]
    fn test_can_gather() {
        let policy = PlannerPolicy::default();

        // Within both limits
        assert!(policy.can_gather(10_000_000, 1024 * 1024 * 1024));
        // Over row limit
        assert!(!policy.can_gather(100_000_000, 1024 * 1024 * 1024));
        // Over byte limit
        assert!(!policy.can_gather(10_000_000, 10 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_can_optimize_to_broadcast() {
        let policy = PlannerPolicy::default();

        // All conditions met
        assert!(policy.can_optimize_to_broadcast(
            100 * 1024 * 1024, // 100MB
            true,              // exact stats
            true,              // join context
        ));

        // Not join context (e.g., GROUP BY) - should NOT broadcast
        assert!(!policy.can_optimize_to_broadcast(
            100 * 1024 * 1024,
            true,
            false, // not join
        ));

        // Stats not exact - should NOT broadcast
        assert!(!policy.can_optimize_to_broadcast(
            100 * 1024 * 1024,
            false, // uncertain stats
            true,
        ));

        // Too large - should NOT broadcast
        assert!(!policy.can_optimize_to_broadcast(
            300 * 1024 * 1024, // 300MB > 256MB
            true,
            true,
        ));
    }

    #[test]
    fn test_can_gather_with_safety() {
        let policy = PlannerPolicy::default();

        // Exact stats - no safety factor
        assert!(policy.can_gather_with_safety(
            40_000_000,
            4 * 1024 * 1024 * 1024,
            true, // exact
        ));

        // Uncertain stats - 2x safety factor applied
        // 30M rows * 2 = 60M > 50M limit
        assert!(!policy.can_gather_with_safety(
            30_000_000,
            1024 * 1024 * 1024,
            false, // uncertain
        ));

        // Uncertain stats but small enough even with 2x
        // 20M rows * 2 = 40M < 50M limit
        assert!(policy.can_gather_with_safety(
            20_000_000,
            1024 * 1024 * 1024,
            false, // uncertain
        ));
    }

    #[test]
    fn test_policy_presets() {
        let conservative = PlannerPolicy::conservative();
        assert_eq!(conservative.broadcast_bytes_max, 0);
        assert_eq!(conservative.safety_factor, 3.0);

        let aggressive = PlannerPolicy::aggressive();
        assert_eq!(aggressive.broadcast_bytes_max, 1024 * 1024 * 1024);
        assert_eq!(aggressive.default_partitions, 32);
        assert_eq!(aggressive.safety_factor, 1.1);

        let testing = PlannerPolicy::for_testing();
        assert!(testing.broadcast_bytes_max > 0);
        assert_eq!(testing.safety_factor, 1.0);
    }
}
