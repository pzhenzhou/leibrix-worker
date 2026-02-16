//! Plan inspection helpers for LDP tests.

use crate::ldp::{Exchange, LdpPlan};
use std::collections::HashMap;

/// Utility for inspecting LDP plans in tests.
pub struct PlanInspector<'a> {
    plan: &'a LdpPlan,
}

impl<'a> PlanInspector<'a> {
    /// Create an inspector for the provided plan.
    pub fn new(plan: &'a LdpPlan) -> Self {
        Self { plan }
    }

    /// Count exchanges by type.
    pub fn count_exchanges(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for edge in &self.plan.edges {
            let key = match &edge.kind {
                Exchange::Gather { .. } => "Gather",
                Exchange::Broadcast { .. } => "Broadcast",
                Exchange::HashPartition { .. } => "HashPartition",
            };
            *counts.entry(key.to_string()).or_insert(0) += 1;
        }
        counts
    }

    /// Number of stages in the plan.
    pub fn stage_count(&self) -> usize {
        self.plan.stages.len()
    }

    /// Whether any exchange of the given type exists.
    pub fn has_exchange_type(&self, exchange_name: &str) -> bool {
        self.count_exchanges()
            .get(exchange_name)
            .copied()
            .unwrap_or(0)
            > 0
    }

    /// Print a human-readable plan summary.
    pub fn print(&self) {
        println!("=== LDP Plan ===");
        println!("Stages: {}", self.plan.stages.len());
        println!("Exchanges:");
        for edge in &self.plan.edges {
            println!(
                "  {:?}: Stage {} -> Stage {}",
                edge.kind, edge.from_stage, edge.to_stage
            );
        }
    }

    /// Coarse data movement estimate.
    ///
    /// Currently this returns number of exchange edges as a placeholder unit.
    pub fn estimate_data_movement(&self) -> u64 {
        self.plan.edges.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::{ExchangeEdge, LdpPlan, Stage};
    use crate::sql::logical_plan::ColumnRef;

    #[test]
    fn test_plan_inspector_counts_exchanges() {
        let mut plan = LdpPlan::with_root("q1".into(), "w1".into(), 2);
        plan.stages.push(Stage::new(0, String::new()));
        plan.stages.push(Stage::new(1, String::new()));
        plan.stages.push(Stage::new(2, String::new()));

        plan.edges.push(ExchangeEdge {
            exchange_id: 1,
            kind: Exchange::Broadcast {
                targets: vec!["w1".into(), "w2".into()],
            },
            from_stage: 0,
            to_stage: 2,
            partition_to_worker: vec![],
        });
        plan.edges.push(ExchangeEdge {
            exchange_id: 2,
            kind: Exchange::HashPartition {
                column_refs: vec![ColumnRef::unqualified("col0")],
                partitions: 16,
            },
            from_stage: 1,
            to_stage: 2,
            partition_to_worker: vec![],
        });
        plan.edges.push(ExchangeEdge {
            exchange_id: 3,
            kind: Exchange::HashPartition {
                column_refs: vec![ColumnRef::unqualified("col1")],
                partitions: 16,
            },
            from_stage: 0,
            to_stage: 1,
            partition_to_worker: vec![],
        });

        let inspector = PlanInspector::new(&plan);
        let counts = inspector.count_exchanges();

        assert_eq!(counts.get("Broadcast"), Some(&1));
        assert_eq!(counts.get("HashPartition"), Some(&2));
        assert_eq!(inspector.stage_count(), 3);
        assert!(inspector.has_exchange_type("Broadcast"));
        assert_eq!(inspector.estimate_data_movement(), 3);
    }
}
