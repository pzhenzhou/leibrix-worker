//! Planner correctness tests for LDP.
//!
//! These integration tests verify that the planner produces correct distributed
//! plans by constructing LogicalPlan trees, running them through
//! `plan_ldp_from_logical_plan`, and asserting on the resulting LdpPlan structure.
//!
//! # Test Scenarios
//! 1. Sort on multi-worker data -> Gather exchange
//! 2. Global aggregate (no GROUP BY) -> Gather exchange
//! 3. Grouped aggregate (GROUP BY) -> HashPartition exchange
//! 4. Join with exact small stats -> Broadcast exchange
//! 5. Join with estimated stats -> HashPartition (shuffle) exchange
//! 6. Join with co-partitioned inputs -> no exchange
//! 7. Join with replicated build side -> no exchange
//! 8. Query exceeding gather limits -> rejected
//! 9. Query exceeding shuffle limits -> rejected
//! 10. Multi-stage plan structure verification

use worker_storage::ldp::{
    plan_ldp_from_logical_plan, Distribution, EpochStats, Exchange, InMemoryMetadata,
    PipelineError, PlanInspector, PlannerPolicy, PlanningError, StatsSource,
};
use worker_storage::sql::logical_plan::{ColumnRef, LogicalPlan};

use sqlparser::ast::{
    Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart,
    OrderByExpr, TableFactor,
};

// ============================================================================
// Helpers
// ============================================================================

/// Create a Scan for the given table name.
fn scan(table: &str) -> LogicalPlan {
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

/// Create a Sort wrapping the given input.
fn sort(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Sort {
        input: Box::new(input),
        order_by: vec![],
    }
}

/// Create a Limit (FETCH) wrapping the given input.
fn limit_plan(input: LogicalPlan, count: u64) -> LogicalPlan {
    LogicalPlan::Limit {
        input: Box::new(input),
        limit: Some(Expr::Value(
            sqlparser::ast::Value::Number(count.to_string(), false).into(),
        )),
        offset: None,
    }
}

/// Create a Filter wrapping the given input.
fn filter(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(input),
        predicate: Expr::Value(sqlparser::ast::Value::Boolean(true).into()),
    }
}

/// Create a global aggregate (no GROUP BY) wrapping the given input.
fn global_aggregate(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        aggr_exprs: vec![],
        having: None,
        group_keys: vec![],
    }
}

/// Create a grouped aggregate (GROUP BY on given column names).
fn grouped_aggregate(input: LogicalPlan, group_cols: &[&str]) -> LogicalPlan {
    let group_keys: Vec<ColumnRef> = group_cols
        .iter()
        .map(|c| ColumnRef::unqualified(*c))
        .collect();
    let group_exprs: Vec<Expr> = group_cols
        .iter()
        .map(|c| Expr::Identifier(Ident::new(*c)))
        .collect();

    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: GroupByExpr::Expressions(group_exprs, vec![]),
        aggr_exprs: vec![],
        having: None,
        group_keys,
    }
}

/// Create a Join on the given left/right inputs and key column names.
fn join_plan(
    left: LogicalPlan,
    right: LogicalPlan,
    left_key_cols: &[(&str, &str)],
    right_key_cols: &[(&str, &str)],
) -> LogicalPlan {
    let left_keys: Vec<ColumnRef> = left_key_cols
        .iter()
        .map(|(t, c)| ColumnRef::qualified(*t, *c))
        .collect();
    let right_keys: Vec<ColumnRef> = right_key_cols
        .iter()
        .map(|(t, c)| ColumnRef::qualified(*t, *c))
        .collect();

    // Build a simple ON clause
    let on_expr = build_join_on_expr(left_key_cols, right_key_cols);

    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_op: JoinOperator::Inner(JoinConstraint::On(on_expr)),
        left_keys,
        right_keys,
    }
}

/// Build a join ON expression from key pairs.
fn build_join_on_expr(left_keys: &[(&str, &str)], right_keys: &[(&str, &str)]) -> Expr {
    let pairs: Vec<Expr> = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|((lt, lc), (rt, rc))| Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(*lt),
                Ident::new(*lc),
            ])),
            op: sqlparser::ast::BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(*rt),
                Ident::new(*rc),
            ])),
        })
        .collect();

    pairs
        .into_iter()
        .reduce(|acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: sqlparser::ast::BinaryOperator::And,
            right: Box::new(expr),
        })
        .unwrap_or(Expr::Value(sqlparser::ast::Value::Boolean(true).into()))
}

/// Metadata with multi-worker epoch-partitioned data for a table.
fn multi_worker_metadata(table: &str) -> InMemoryMetadata {
    InMemoryMetadata::new()
        .with_epoch("e1", table, EpochStats::exact(1000, 10_000), "w1".into())
        .with_epoch("e2", table, EpochStats::exact(2000, 20_000), "w2".into())
}

/// Policy with generous limits (allowing most plans through).
fn generous_policy() -> PlannerPolicy {
    PlannerPolicy::with_coordinator("coordinator")
        .broadcast_bytes_max(1_000_000)
        .shuffle_bytes_max(100_000_000)
        .gather_rows_max(1_000_000)
        .gather_bytes_max(100_000_000)
}

/// Policy with tight gather limits to trigger rejection.
fn tight_gather_policy() -> PlannerPolicy {
    PlannerPolicy::with_coordinator("coordinator")
        .broadcast_bytes_max(100)
        .shuffle_bytes_max(100_000_000)
        .gather_rows_max(100)
        .gather_bytes_max(500)
}

/// Policy with tight shuffle limits to trigger rejection.
fn tight_shuffle_policy() -> PlannerPolicy {
    PlannerPolicy::with_coordinator("coordinator")
        .broadcast_bytes_max(100)
        .shuffle_bytes_max(100)
        .gather_rows_max(100_000)
        .gather_bytes_max(1_000_000)
}

// ============================================================================
// Test 1: Sort on multi-worker data -> Gather exchange
// ============================================================================

#[test]
fn test_sort_on_multi_worker_data_inserts_gather() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    let plan = sort(scan("sales"));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_sort").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("Gather"),
        "Sort on multi-worker data must insert a Gather exchange"
    );
    assert!(
        inspector.stage_count() >= 2,
        "Sort plan must have at least 2 stages, got {}",
        inspector.stage_count()
    );
    assert_eq!(ldp.query_id, "q_sort");
}

// ============================================================================
// Test 2: Global aggregate (no GROUP BY) -> Gather exchange
// ============================================================================

#[test]
fn test_global_aggregate_inserts_gather() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    let plan = global_aggregate(scan("sales"));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_global_agg").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("Gather"),
        "Global aggregate on multi-worker data must insert a Gather exchange"
    );
    assert!(inspector.stage_count() >= 2);
}

// ============================================================================
// Test 3: Grouped aggregate (GROUP BY) -> HashPartition exchange
// ============================================================================

#[test]
fn test_grouped_aggregate_inserts_hash_partition() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    let plan = grouped_aggregate(scan("sales"), &["col0"]);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_grouped_agg").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("HashPartition"),
        "Grouped aggregate on multi-worker data must insert a HashPartition exchange"
    );
    assert!(inspector.stage_count() >= 2);

    for edge in &ldp.edges {
        if let Exchange::HashPartition { column_refs, .. } = &edge.kind {
            assert_eq!(
                column_refs.len(),
                1,
                "HashPartition should partition on 1 column (the GROUP BY key)"
            );
            assert_eq!(
                column_refs[0].column, "col0",
                "HashPartition should partition on the GROUP BY key column"
            );
        }
    }
}

// ============================================================================
// Test 4: Join with exact small stats -> Broadcast exchange
// ============================================================================

#[test]
fn test_join_exact_small_stats_uses_broadcast() {
    let mut metadata = multi_worker_metadata("orders");
    metadata.register_table_stats(
        "customers",
        vec!["w1".into()],
        Distribution::Singleton {
            worker: "w1".into(),
        },
        50,
        500,
        StatsSource::Exact,
    );

    let policy = PlannerPolicy::with_coordinator("coordinator")
        .broadcast_bytes_max(10_000)
        .shuffle_bytes_max(100_000_000)
        .gather_rows_max(1_000_000)
        .gather_bytes_max(100_000_000);

    let left = scan("orders");
    let right = scan("customers");
    let plan = join_plan(left, right, &[("orders", "id")], &[("customers", "id")]);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_broadcast_join").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("Broadcast"),
        "Join with exact small stats should use Broadcast exchange. Exchanges: {:?}",
        inspector.count_exchanges()
    );
}

// ============================================================================
// Test 5: Join with estimated stats -> HashPartition (shuffle) exchange
// ============================================================================

#[test]
fn test_join_estimated_stats_uses_shuffle() {
    let mut metadata = multi_worker_metadata("orders");
    metadata.register_table_stats(
        "customers",
        vec!["w1".into()],
        Distribution::Singleton {
            worker: "w1".into(),
        },
        50,
        500,
        StatsSource::Estimated,
    );

    let policy = PlannerPolicy::with_coordinator("coordinator")
        .broadcast_bytes_max(10_000)
        .shuffle_bytes_max(100_000_000)
        .gather_rows_max(1_000_000)
        .gather_bytes_max(100_000_000);

    let left = scan("orders");
    let right = scan("customers");
    let plan = join_plan(left, right, &[("orders", "id")], &[("customers", "id")]);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_shuffle_join").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        !inspector.has_exchange_type("Broadcast"),
        "Join with estimated stats should NOT use Broadcast exchange"
    );
    assert!(inspector.stage_count() >= 1, "Plan should have stages");
}

// ============================================================================
// Test 6: Join with co-partitioned inputs -> no exchange needed
// ============================================================================

#[test]
fn test_join_co_partitioned_no_exchange() {
    let mut metadata = InMemoryMetadata::new();
    metadata.register_table_stats(
        "orders",
        vec!["w1".into(), "w2".into(), "w3".into()],
        Distribution::HashPartitioned {
            column_refs: vec![ColumnRef::unqualified("id")],
            workers: vec!["w1".into(), "w2".into(), "w3".into()],
        },
        100_000,
        10_000_000,
        StatsSource::Exact,
    );
    metadata.register_table_stats(
        "line_items",
        vec!["w1".into(), "w2".into(), "w3".into()],
        Distribution::HashPartitioned {
            column_refs: vec![ColumnRef::unqualified("id")],
            workers: vec!["w1".into(), "w2".into(), "w3".into()],
        },
        500_000,
        50_000_000,
        StatsSource::Exact,
    );

    let policy = generous_policy();

    let left = scan("orders");
    let right = scan("line_items");
    let plan = join_plan(left, right, &[("orders", "id")], &[("line_items", "id")]);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_copartitioned").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        !inspector.has_exchange_type("HashPartition"),
        "Co-partitioned join should NOT insert HashPartition exchange"
    );
    assert!(
        !inspector.has_exchange_type("Broadcast"),
        "Co-partitioned join should NOT insert Broadcast exchange"
    );
    assert!(
        !inspector.has_exchange_type("Gather"),
        "Co-partitioned join should NOT insert Gather exchange"
    );
}

// ============================================================================
// Test 7: Join with replicated build side -> no exchange needed
// ============================================================================

#[test]
fn test_join_replicated_build_side_no_exchange() {
    let mut metadata = InMemoryMetadata::new();
    metadata.register_table_stats(
        "orders",
        vec!["w1".into(), "w2".into()],
        Distribution::EpochPartitioned {
            workers: vec!["w1".into(), "w2".into()],
        },
        100_000,
        10_000_000,
        StatsSource::Exact,
    );
    metadata.register_table_stats(
        "products",
        vec!["w1".into(), "w2".into()],
        Distribution::Replicated {
            workers: vec!["w1".into(), "w2".into()],
        },
        1_000,
        100_000,
        StatsSource::Exact,
    );

    let policy = generous_policy();

    let left = scan("orders");
    let right = scan("products");
    let plan = join_plan(left, right, &[("orders", "id")], &[("products", "id")]);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_replicated").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        !inspector.has_exchange_type("HashPartition"),
        "Join with replicated build side should NOT insert HashPartition"
    );
    assert!(
        !inspector.has_exchange_type("Broadcast"),
        "Join with replicated build side should NOT insert Broadcast"
    );
}

// ============================================================================
// Test 8: Query exceeding gather limits -> rejected
// ============================================================================

#[test]
fn test_gather_exceeds_limits_rejected() {
    let metadata = InMemoryMetadata::new()
        .with_epoch(
            "e1",
            "big_table",
            EpochStats::exact(10_000, 100_000),
            "w1".into(),
        )
        .with_epoch(
            "e2",
            "big_table",
            EpochStats::exact(10_000, 100_000),
            "w2".into(),
        );

    let policy = tight_gather_policy();

    let plan = sort(scan("big_table"));

    let result = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_rejected_gather");

    assert!(
        result.is_err(),
        "Sort on large data with tight gather limits should be rejected"
    );

    match result {
        Err(PipelineError::Planning(PlanningError::Rejected(_))) => {
            // Expected
        }
        Err(other) => {
            panic!("Expected PlanningError::Rejected, got: {:?}", other)
        }
        Ok(ldp) => {
            let inspector = PlanInspector::new(&ldp);
            panic!(
                "Expected rejection but got plan with {} stages and exchanges: {:?}",
                inspector.stage_count(),
                inspector.count_exchanges()
            )
        }
    }
}

// ============================================================================
// Test 9: Query exceeding shuffle limits -> rejected
// ============================================================================

#[test]
fn test_shuffle_exceeds_limits_rejected() {
    let metadata = InMemoryMetadata::new()
        .with_epoch(
            "e1",
            "big_table",
            EpochStats::exact(10_000, 100_000),
            "w1".into(),
        )
        .with_epoch(
            "e2",
            "big_table",
            EpochStats::exact(10_000, 100_000),
            "w2".into(),
        );

    let policy = tight_shuffle_policy();

    let plan = grouped_aggregate(scan("big_table"), &["col0"]);

    let result = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_rejected_shuffle");

    assert!(
        result.is_err(),
        "Grouped aggregate on large data with tight shuffle limits should be rejected"
    );

    match result {
        Err(PipelineError::Planning(PlanningError::Rejected(_))) => {
            // Expected
        }
        Err(other) => {
            panic!("Expected PlanningError::Rejected, got: {:?}", other)
        }
        Ok(ldp) => {
            let inspector = PlanInspector::new(&ldp);
            panic!(
                "Expected rejection but got plan with {} stages and exchanges: {:?}",
                inspector.stage_count(),
                inspector.count_exchanges()
            )
        }
    }
}

// ============================================================================
// Test 10: Multi-stage plan structure verification
// ============================================================================

#[test]
fn test_multi_stage_plan_structure() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    // Sort(GroupedAggregate(Filter(Scan)))
    let plan = sort(grouped_aggregate(filter(scan("sales")), &["col0"]));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_multi_stage").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("HashPartition"),
        "Multi-stage plan should have HashPartition for grouped aggregate"
    );
    assert!(
        inspector.has_exchange_type("Gather"),
        "Multi-stage plan should have Gather for sort"
    );

    assert!(
        inspector.stage_count() >= 3,
        "Multi-stage plan should have at least 3 stages, got {}",
        inspector.stage_count()
    );

    let topo_order = ldp.topological_order();
    assert_eq!(
        topo_order.len(),
        ldp.stages.len(),
        "Topological order should include all stages"
    );

    assert!(
        ldp.get_stage(ldp.root_stage).is_some(),
        "Root stage should exist in plan"
    );

    let exchanges = inspector.count_exchanges();
    let total_exchanges: usize = exchanges.values().sum();
    assert_eq!(
        inspector.estimate_data_movement(),
        total_exchanges as u64,
        "Data movement estimate should match total exchange count"
    );
}

// ============================================================================
// Additional correctness tests
// ============================================================================

#[test]
fn test_fetch_inserts_gather_on_distributed_data() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    let plan = limit_plan(scan("sales"), 10);

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_fetch").unwrap();
    let inspector = PlanInspector::new(&ldp);

    assert!(
        inspector.has_exchange_type("Gather"),
        "Fetch on multi-worker data must insert a Gather exchange"
    );
}

#[test]
fn test_filter_preserves_distribution_no_exchange() {
    let metadata = multi_worker_metadata("sales");
    let policy = generous_policy();

    let plan = filter(scan("sales"));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_filter").unwrap();
    let inspector = PlanInspector::new(&ldp);

    let total: usize = inspector.count_exchanges().values().sum();
    assert_eq!(
        total, 0,
        "Filter should not insert any exchanges, got {:?}",
        inspector.count_exchanges()
    );

    assert_eq!(
        inspector.stage_count(),
        1,
        "Filter-only plan should have exactly 1 stage"
    );
}

#[test]
fn test_single_worker_sort_no_gather() {
    let metadata = InMemoryMetadata::new().with_epoch(
        "e1",
        "small_table",
        EpochStats::exact(100, 1000),
        "w1".into(),
    );

    let policy = generous_policy();

    let plan = sort(scan("small_table"));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_single_sort").unwrap();
    let inspector = PlanInspector::new(&ldp);

    let total: usize = inspector.count_exchanges().values().sum();
    assert_eq!(
        total, 0,
        "Sort on single-worker data should NOT insert any exchange, got {:?}",
        inspector.count_exchanges()
    );
}

#[test]
fn test_unknown_table_falls_back_gracefully() {
    let metadata = InMemoryMetadata::new();
    let policy = generous_policy();

    let plan = scan("nonexistent_table");

    let result = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_unknown");
    assert!(
        result.is_ok(),
        "Planning for unknown table should succeed with fallback, got: {:?}",
        result.err()
    );

    let ldp = result.unwrap();
    assert_eq!(ldp.stages.len(), 1, "Unknown table plan should have 1 stage");
}

#[test]
fn test_join_both_sides_need_redistribution() {
    let metadata = InMemoryMetadata::new()
        .with_epoch(
            "e1",
            "orders",
            EpochStats::exact(5_000, 50_000),
            "w1".into(),
        )
        .with_epoch(
            "e2",
            "orders",
            EpochStats::exact(5_000, 50_000),
            "w2".into(),
        )
        .with_epoch(
            "e3",
            "items",
            EpochStats::exact(10_000, 100_000),
            "w1".into(),
        )
        .with_epoch(
            "e4",
            "items",
            EpochStats::exact(10_000, 100_000),
            "w2".into(),
        );

    let policy = generous_policy();

    let left = scan("orders");
    let right = scan("items");
    let plan = join_plan(left, right, &[("orders", "id")], &[("items", "id")]);

    let ldp =
        plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_both_shuffle").unwrap();
    let inspector = PlanInspector::new(&ldp);

    let total: usize = inspector.count_exchanges().values().sum();
    assert!(
        total >= 1,
        "Join on epoch-partitioned tables should insert at least 1 exchange, got {:?}",
        inspector.count_exchanges()
    );

    assert!(
        inspector.stage_count() >= 2,
        "Join plan should have at least 2 stages"
    );
}

#[test]
fn test_plan_coordinator_is_set() {
    let metadata = multi_worker_metadata("sales");
    let policy = PlannerPolicy::with_coordinator("my_coordinator")
        .broadcast_bytes_max(1_000_000)
        .shuffle_bytes_max(100_000_000)
        .gather_rows_max(1_000_000)
        .gather_bytes_max(100_000_000);

    let plan = sort(scan("sales"));

    let ldp = plan_ldp_from_logical_plan(&plan, &metadata, &policy, "q_coord").unwrap();

    assert_eq!(
        ldp.coordinator, "my_coordinator",
        "Plan coordinator should match policy coordinator"
    );

    for edge in &ldp.edges {
        if let Exchange::Gather { target } = &edge.kind {
            assert_eq!(
                target, "my_coordinator",
                "Gather exchange target should be the coordinator"
            );
        }
    }
}
