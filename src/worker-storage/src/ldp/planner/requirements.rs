//! Distribution requirements for LogicalPlan operators.
//!
//! This module contains `get_logical_plan_requirements()` - THE ONLY place where we
//! inspect the specific LogicalPlan variant to determine distribution requirements.
//!
//! # Design Principle
//! All operator-specific logic for requirements is centralized here.
//! Other planner code only deals with generic plan nodes and distributions.

use crate::ldp::{Distribution, RequiredDistribution};
use crate::sql::logical_plan::LogicalPlan;

/// Get the distribution requirements for a LogicalPlan's inputs.
///
/// This is THE ONLY place we inspect LogicalPlan variants to determine requirements.
/// Returns a vector of requirements, one per input (in order).
///
/// # Requirements by Operator Type
///
/// | Operator | Input Count | Requirements |
/// |----------|-------------|--------------|
/// | Scan | 0 | (none) |
/// | ExchangeRead | 0 | (none) |
/// | Filter | 1 | Any |
/// | Project | 1 | Any |
/// | Aggregate | 1 | HashPartitioned(group_keys) or Singleton if global |
/// | Sort | 1 | Singleton |
/// | Limit | 1 | Singleton |
/// | Join | 2 | HashPartitioned(join_keys) for each side (or Any for cross join) |
/// | SetOp | 2+ | Any for all inputs |
/// | Window | 1 | Singleton (conservative; could partition by PARTITION BY keys) |
/// | SubqueryScan | 1 | Any |
///
/// # Returns
/// - Empty vector for leaf operators (Scan, ExchangeRead)
/// - One requirement for unary operators (Filter, Project, Sort, etc.)
/// - Two requirements for binary operators (Join)
/// - N requirements for N-ary operators (SetOp/Union)
pub fn get_logical_plan_requirements(plan: &LogicalPlan) -> Vec<RequiredDistribution> {
    match plan {
        // ===== Leaf operators (0 inputs) =====
        LogicalPlan::Scan { .. } | LogicalPlan::ExchangeRead { .. } => vec![],

        // ===== Distribution-preserving unary operators =====
        // These don't care about input distribution
        LogicalPlan::Filter { .. } | LogicalPlan::Project { .. } => {
            vec![RequiredDistribution::Any]
        }

        // ===== Singleton-requiring operators =====
        // Global Sort requires all data on one node
        LogicalPlan::Sort { .. } => vec![RequiredDistribution::Singleton],

        // LIMIT/OFFSET requires all data on one node
        LogicalPlan::Limit { .. } => vec![RequiredDistribution::Singleton],

        // ===== Aggregate =====
        // Global aggregate (no GROUP BY) -> Singleton
        // Grouped aggregate -> HashPartitioned by group keys
        LogicalPlan::Aggregate { group_keys, .. } => {
            if group_keys.is_empty() {
                // Global aggregate: SUM(*), COUNT(*), etc.
                vec![RequiredDistribution::Singleton]
            } else {
                // Grouped aggregate: GROUP BY col1, col2
                vec![RequiredDistribution::HashPartitioned {
                    column_refs: group_keys.clone(),
                }]
            }
        }

        // ===== Join operators =====
        // Both inputs need to be hash-partitioned by their respective join keys
        LogicalPlan::Join {
            left_keys,
            right_keys,
            ..
        } => {
            if left_keys.is_empty() || right_keys.is_empty() {
                // Cross join - no partitioning requirement
                // (one side will be broadcast in practice)
                vec![RequiredDistribution::Any, RequiredDistribution::Any]
            } else {
                // Equi-join with keys
                vec![
                    RequiredDistribution::HashPartitioned {
                        column_refs: left_keys.clone(),
                    },
                    RequiredDistribution::HashPartitioned {
                        column_refs: right_keys.clone(),
                    },
                ]
            }
        }

        // ===== Set operators (UNION, INTERSECT, EXCEPT) =====
        LogicalPlan::SetOp { .. } => {
            // Both inputs can have any distribution
            // (they'll be gathered/combined at the coordinator)
            vec![RequiredDistribution::Any, RequiredDistribution::Any]
        }

        // ===== Window functions =====
        // Window may need partitioning by PARTITION BY clause
        // For now, conservatively require Singleton (can be optimized later
        // when we properly extract PARTITION BY keys from the window function spec)
        LogicalPlan::Window { .. } => {
            // TODO: Extract partition keys from window expressions
            // when better analysis is implemented
            vec![RequiredDistribution::Singleton]
        }

        // ===== SubqueryScan =====
        LogicalPlan::SubqueryScan { .. } => {
            vec![RequiredDistribution::Any]
        }
    }
}

/// Check if the actual distribution satisfies the required distribution.
///
/// This is a convenience wrapper around `RequiredDistribution::is_satisfied_by`.
pub fn satisfies(actual: &Distribution, required: &RequiredDistribution) -> bool {
    required.is_satisfied_by(actual)
}

/// Check if all requirements are satisfied by the given child distributions.
///
/// # Arguments
/// * `child_distributions` - The actual distributions of child nodes
/// * `requirements` - The requirements from `get_logical_plan_requirements()`
///
/// # Returns
/// * `true` if all requirements are satisfied
/// * `false` if any requirement is not satisfied
pub fn all_satisfied(
    child_distributions: &[Distribution],
    requirements: &[RequiredDistribution],
) -> bool {
    if child_distributions.len() != requirements.len() {
        return false;
    }

    child_distributions
        .iter()
        .zip(requirements.iter())
        .all(|(actual, required)| satisfies(actual, required))
}

/// Get the unsatisfied requirements with their indices.
///
/// Returns tuples of (input_index, actual_distribution, required_distribution)
/// for each unsatisfied requirement.
pub fn unsatisfied_requirements<'a>(
    child_distributions: &'a [Distribution],
    requirements: &'a [RequiredDistribution],
) -> Vec<(usize, &'a Distribution, &'a RequiredDistribution)> {
    child_distributions
        .iter()
        .zip(requirements.iter())
        .enumerate()
        .filter(|(_, (actual, required))| !satisfies(actual, required))
        .map(|(idx, (actual, required))| (idx, actual, required))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::logical_plan::ColumnRef;
    use sqlparser::ast::{
        Expr, Ident, ObjectName, ObjectNamePart, SetOperator, SetQuantifier, TableFactor,
    };

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
    fn test_scan_requirements() {
        let scan = create_scan("test");
        let reqs = get_logical_plan_requirements(&scan);
        assert!(reqs.is_empty(), "Scan should have no input requirements");
    }

    #[test]
    fn test_exchange_read_requirements() {
        let exchange_read = LogicalPlan::ExchangeRead {
            exchange_id: 0,
            alias: "__exchange_0".to_string(),
        };
        let reqs = get_logical_plan_requirements(&exchange_read);
        assert!(
            reqs.is_empty(),
            "ExchangeRead should have no input requirements"
        );
    }

    #[test]
    fn test_filter_requirements() {
        let filter = LogicalPlan::Filter {
            input: Box::new(create_scan("test")),
            predicate: Expr::Identifier(Ident::new("dummy")),
        };

        let reqs = get_logical_plan_requirements(&filter);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Any);
    }

    #[test]
    fn test_project_requirements() {
        let project = LogicalPlan::Project {
            input: Box::new(create_scan("test")),
            items: vec![],
        };

        let reqs = get_logical_plan_requirements(&project);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Any);
    }

    #[test]
    fn test_sort_requirements() {
        let sort = LogicalPlan::Sort {
            input: Box::new(create_scan("test")),
            order_by: vec![],
        };

        let reqs = get_logical_plan_requirements(&sort);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_limit_requirements() {
        let limit = LogicalPlan::Limit {
            input: Box::new(create_scan("test")),
            limit: None,
            offset: None,
        };

        let reqs = get_logical_plan_requirements(&limit);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_global_aggregate_requirements() {
        // Empty group_keys = global aggregate
        let agg = LogicalPlan::Aggregate {
            input: Box::new(create_scan("test")),
            group_by: sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]),
            aggr_exprs: vec![],
            having: None,
            group_keys: vec![], // Empty = global aggregate
        };

        let reqs = get_logical_plan_requirements(&agg);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_grouped_aggregate_requirements() {
        let agg = LogicalPlan::Aggregate {
            input: Box::new(create_scan("test")),
            group_by: sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]),
            aggr_exprs: vec![],
            having: None,
            group_keys: vec![
                ColumnRef::unqualified("region"),
                ColumnRef::unqualified("category"),
            ],
        };

        let reqs = get_logical_plan_requirements(&agg);
        assert_eq!(reqs.len(), 1);
        match &reqs[0] {
            RequiredDistribution::HashPartitioned { column_refs } => {
                assert_eq!(column_refs.len(), 2);
                assert_eq!(column_refs[0].column, "region");
                assert_eq!(column_refs[1].column, "category");
            }
            _ => panic!("Expected HashPartitioned requirement"),
        }
    }

    #[test]
    fn test_join_requirements() {
        let join = LogicalPlan::Join {
            left: Box::new(create_scan("orders")),
            right: Box::new(create_scan("lineitem")),
            join_op: sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::None),
            left_keys: vec![ColumnRef::qualified("orders", "id")],
            right_keys: vec![ColumnRef::qualified("lineitem", "order_id")],
        };

        let reqs = get_logical_plan_requirements(&join);
        assert_eq!(reqs.len(), 2);

        match &reqs[0] {
            RequiredDistribution::HashPartitioned { column_refs } => {
                assert_eq!(column_refs.len(), 1);
                assert_eq!(column_refs[0].column, "id");
            }
            _ => panic!("Expected HashPartitioned for left"),
        }

        match &reqs[1] {
            RequiredDistribution::HashPartitioned { column_refs } => {
                assert_eq!(column_refs.len(), 1);
                assert_eq!(column_refs[0].column, "order_id");
            }
            _ => panic!("Expected HashPartitioned for right"),
        }
    }

    #[test]
    fn test_cross_join_requirements() {
        let cross_join = LogicalPlan::Join {
            left: Box::new(create_scan("t1")),
            right: Box::new(create_scan("t2")),
            join_op: sqlparser::ast::JoinOperator::CrossJoin(sqlparser::ast::JoinConstraint::None),
            left_keys: vec![], // Empty = cross join
            right_keys: vec![],
        };

        let reqs = get_logical_plan_requirements(&cross_join);
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0], RequiredDistribution::Any);
        assert_eq!(reqs[1], RequiredDistribution::Any);
    }

    #[test]
    fn test_set_op_requirements() {
        let union = LogicalPlan::SetOp {
            left: Box::new(create_scan("t1")),
            right: Box::new(create_scan("t2")),
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
        };

        let reqs = get_logical_plan_requirements(&union);
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0], RequiredDistribution::Any);
        assert_eq!(reqs[1], RequiredDistribution::Any);
    }

    #[test]
    fn test_window_requirements() {
        let window = LogicalPlan::Window {
            input: Box::new(create_scan("test")),
            window_exprs: vec![],
        };

        let reqs = get_logical_plan_requirements(&window);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_subquery_scan_requirements() {
        let subquery = LogicalPlan::SubqueryScan {
            input: Box::new(create_scan("test")),
            alias: sqlparser::ast::TableAlias {
                name: Ident::new("sub"),
                columns: vec![],
                explicit: false,
            },
        };

        let reqs = get_logical_plan_requirements(&subquery);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Any);
    }

    #[test]
    fn test_satisfies() {
        let singleton = Distribution::Singleton {
            worker: "w1".into(),
        };
        let hash_part = Distribution::HashPartitioned {
            column_refs: vec![ColumnRef::unqualified("id")],
            workers: vec!["w1".into(), "w2".into()],
        };
        let replicated = Distribution::Replicated {
            workers: vec!["w1".into(), "w2".into()],
        };

        // Any is satisfied by everything
        assert!(satisfies(&singleton, &RequiredDistribution::Any));
        assert!(satisfies(&hash_part, &RequiredDistribution::Any));

        // Singleton requirement
        assert!(satisfies(&singleton, &RequiredDistribution::Singleton));
        assert!(!satisfies(&hash_part, &RequiredDistribution::Singleton));

        // HashPartitioned requirement
        let hash_req = RequiredDistribution::HashPartitioned {
            column_refs: vec![ColumnRef::unqualified("id")],
        };
        assert!(satisfies(&hash_part, &hash_req));
        assert!(satisfies(&replicated, &hash_req)); // Replicated satisfies any hash req
        assert!(!satisfies(&singleton, &hash_req));
    }

    #[test]
    fn test_all_satisfied() {
        let dists = vec![
            Distribution::Singleton {
                worker: "w1".into(),
            },
            Distribution::HashPartitioned {
                column_refs: vec![ColumnRef::unqualified("id")],
                workers: vec!["w1".into()],
            },
        ];

        let reqs_satisfied = vec![
            RequiredDistribution::Any,
            RequiredDistribution::HashPartitioned {
                column_refs: vec![ColumnRef::unqualified("id")],
            },
        ];
        assert!(all_satisfied(&dists, &reqs_satisfied));

        let reqs_unsatisfied = vec![
            RequiredDistribution::HashPartitioned {
                column_refs: vec![ColumnRef::unqualified("other")],
            },
            RequiredDistribution::Singleton,
        ];
        assert!(!all_satisfied(&dists, &reqs_unsatisfied));
    }
}
