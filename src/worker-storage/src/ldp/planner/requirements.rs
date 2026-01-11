//! Distribution requirements for Substrait operators.
//!
//! This module contains `get_requirements()` - THE ONLY place where we
//! inspect the specific RelType to determine distribution requirements.
//!
//! # Design Principle
//! All operator-specific logic for requirements is centralized here.
//! Other planner code only deals with generic Rel nodes and distributions.

use crate::ldp::substrait::helpers::{
    all_groupings_empty, extract_grouping_field_refs, extract_hash_join_key_refs,
    extract_join_key_refs,
};
use crate::ldp::{Distribution, RequiredDistribution};
use substrait::proto::rel::RelType;
use substrait::proto::Rel;

/// Get the distribution requirements for a Rel's inputs.
///
/// This is THE ONLY place we inspect RelType to determine requirements.
/// Returns a vector of requirements, one per input (in order).
///
/// # Requirements by Operator Type
///
/// | Operator | Input Count | Requirements |
/// |----------|-------------|--------------|
/// | Read | 0 | (none) |
/// | Filter | 1 | Any |
/// | Project | 1 | Any |
/// | Aggregate | 1 | HashPartitioned(group_keys) or Singleton if global |
/// | Sort | 1 | Singleton |
/// | Fetch | 1 | Singleton |
/// | Join | 2 | HashPartitioned(join_keys) for each side |
/// | HashJoin | 2 | HashPartitioned(join_keys) for each side |
/// | Cross | 2 | Any, Any (broadcast smaller side separately) |
/// | Set | N | Any for all inputs |
///
/// # Returns
/// - Empty vector for leaf operators (Read, ExtensionLeaf)
/// - One requirement for unary operators (Filter, Project, Sort, etc.)
/// - Two requirements for binary operators (Join, Cross)
/// - N requirements for N-ary operators (Set/Union)
pub fn get_requirements(rel: &Rel) -> Vec<RequiredDistribution> {
    let Some(rel_type) = &rel.rel_type else {
        return vec![];
    };

    match rel_type {
        // ===== Leaf operators (0 inputs) =====
        RelType::Read(_) | RelType::ExtensionLeaf(_) => vec![],

        // ===== Distribution-preserving unary operators =====
        // These don't care about input distribution
        RelType::Filter(_) | RelType::Project(_) => {
            vec![RequiredDistribution::Any]
        }

        // ===== Singleton-requiring operators =====
        // Global Sort requires all data on one node
        RelType::Sort(_) => vec![RequiredDistribution::Singleton],

        // LIMIT/OFFSET requires all data on one node
        RelType::Fetch(_) => vec![RequiredDistribution::Singleton],

        // ===== Aggregate =====
        // Global aggregate (no GROUP BY) -> Singleton
        // Grouped aggregate -> HashPartitioned by group keys
        RelType::Aggregate(agg) => {
            if all_groupings_empty(agg) {
                // Global aggregate: SUM(*), COUNT(*), etc.
                vec![RequiredDistribution::Singleton]
            } else {
                // Grouped aggregate: GROUP BY col1, col2
                let group_keys = extract_grouping_field_refs(agg);
                if group_keys.is_empty() {
                    // Fallback: treat as global
                    vec![RequiredDistribution::Singleton]
                } else {
                    vec![RequiredDistribution::HashPartitioned {
                        field_refs: group_keys,
                    }]
                }
            }
        }

        // ===== Join operators =====
        // Both inputs need to be hash-partitioned by their respective join keys
        RelType::Join(join) => {
            match extract_join_key_refs(join) {
                Ok(keys) if keys.is_valid() => {
                    // Equi-join with extractable keys
                    vec![
                        RequiredDistribution::HashPartitioned {
                            field_refs: keys.left_keys,
                        },
                        RequiredDistribution::HashPartitioned {
                            field_refs: keys.right_keys,
                        },
                    ]
                }
                Ok(keys) if keys.is_cross_join() => {
                    // Cross join - no partitioning requirement
                    // (one side will be broadcast in practice)
                    vec![RequiredDistribution::Any, RequiredDistribution::Any]
                }
                _ => {
                    // Can't extract keys - treat as requiring Singleton (conservative)
                    vec![
                        RequiredDistribution::Singleton,
                        RequiredDistribution::Singleton,
                    ]
                }
            }
        }

        // HashJoin has explicit key fields
        RelType::HashJoin(join) => {
            let (left_keys, right_keys) = extract_hash_join_key_refs(join);
            if !left_keys.is_empty() && !right_keys.is_empty() {
                vec![
                    RequiredDistribution::HashPartitioned {
                        field_refs: left_keys,
                    },
                    RequiredDistribution::HashPartitioned {
                        field_refs: right_keys,
                    },
                ]
            } else {
                // No keys - treat as cross join
                vec![RequiredDistribution::Any, RequiredDistribution::Any]
            }
        }

        // MergeJoin also has explicit key fields
        RelType::MergeJoin(join) => {
            let left_keys: Vec<u32> = join
                .left_keys
                .iter()
                .filter_map(|fr| {
                    crate::ldp::substrait::helpers::extract_field_ref_from_expression(
                        &substrait::proto::Expression {
                            rex_type: Some(substrait::proto::expression::RexType::Selection(
                                Box::new(fr.clone()),
                            )),
                        },
                    )
                })
                .collect();

            let right_keys: Vec<u32> = join
                .right_keys
                .iter()
                .filter_map(|fr| {
                    crate::ldp::substrait::helpers::extract_field_ref_from_expression(
                        &substrait::proto::Expression {
                            rex_type: Some(substrait::proto::expression::RexType::Selection(
                                Box::new(fr.clone()),
                            )),
                        },
                    )
                })
                .collect();

            if !left_keys.is_empty() && !right_keys.is_empty() {
                vec![
                    RequiredDistribution::HashPartitioned {
                        field_refs: left_keys,
                    },
                    RequiredDistribution::HashPartitioned {
                        field_refs: right_keys,
                    },
                ]
            } else {
                vec![RequiredDistribution::Any, RequiredDistribution::Any]
            }
        }

        // NestedLoopJoin - no hash partitioning possible
        RelType::NestedLoopJoin(_) => {
            vec![RequiredDistribution::Any, RequiredDistribution::Any]
        }

        // Cross join
        RelType::Cross(_) => {
            vec![RequiredDistribution::Any, RequiredDistribution::Any]
        }

        // ===== Set operators (UNION, INTERSECT, EXCEPT) =====
        RelType::Set(set) => {
            // All inputs can have any distribution
            // (they'll be gathered/combined at the coordinator)
            vec![RequiredDistribution::Any; set.inputs.len()]
        }

        // ===== Window functions =====
        // Window may need partitioning by PARTITION BY clause
        // For now, conservatively require Singleton (can be optimized later
        // when we properly extract PARTITION BY keys from the window function spec)
        RelType::Window(_window) => {
            // TODO: Extract partition keys from window.partition_expressions
            // when the Substrait API is better understood
            vec![RequiredDistribution::Singleton]
        }

        // ===== Extension operators =====
        RelType::ExtensionSingle(_) => {
            // Conservative: pass through
            vec![RequiredDistribution::Any]
        }

        RelType::ExtensionMulti(ext) => {
            // One Any per input
            vec![RequiredDistribution::Any; ext.inputs.len()]
        }

        // ===== Write/DDL operators =====
        RelType::Write(_) => vec![RequiredDistribution::Any],
        RelType::Ddl(_) => vec![],
        RelType::Update(_) => vec![RequiredDistribution::Any],

        // ===== Exchange operator =====
        // Exchange itself doesn't impose requirements on its input
        RelType::Exchange(_) => vec![RequiredDistribution::Any],

        // ===== Expand (for CUBE/ROLLUP) =====
        RelType::Expand(_) => vec![RequiredDistribution::Any],

        // ===== Reference (named relation reference) =====
        RelType::Reference(_) => vec![],
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
/// * `requirements` - The requirements from `get_requirements()`
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
    use substrait::proto::read_rel::ReadType;
    use substrait::proto::{AggregateRel, FetchRel, FilterRel, ProjectRel, ReadRel, SortRel};

    fn create_read_rel() -> Rel {
        Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                read_type: Some(ReadType::NamedTable(
                    substrait::proto::read_rel::NamedTable {
                        names: vec!["test".to_string()],
                        advanced_extension: None,
                    },
                )),
                ..Default::default()
            }))),
        }
    }

    #[test]
    fn test_read_requirements() {
        let read = create_read_rel();
        let reqs = get_requirements(&read);
        assert!(reqs.is_empty(), "Read should have no input requirements");
    }

    #[test]
    fn test_filter_requirements() {
        let filter = Rel {
            rel_type: Some(RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(create_read_rel())),
                ..Default::default()
            }))),
        };

        let reqs = get_requirements(&filter);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Any);
    }

    #[test]
    fn test_project_requirements() {
        let project = Rel {
            rel_type: Some(RelType::Project(Box::new(ProjectRel {
                input: Some(Box::new(create_read_rel())),
                ..Default::default()
            }))),
        };

        let reqs = get_requirements(&project);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Any);
    }

    #[test]
    fn test_sort_requirements() {
        let sort = Rel {
            rel_type: Some(RelType::Sort(Box::new(SortRel {
                input: Some(Box::new(create_read_rel())),
                ..Default::default()
            }))),
        };

        let reqs = get_requirements(&sort);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_fetch_requirements() {
        let fetch = Rel {
            rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                input: Some(Box::new(create_read_rel())),
                ..Default::default()
            }))),
        };

        let reqs = get_requirements(&fetch);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_global_aggregate_requirements() {
        // Empty groupings = global aggregate
        let agg = Rel {
            rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                input: Some(Box::new(create_read_rel())),
                groupings: vec![],
                ..Default::default()
            }))),
        };

        let reqs = get_requirements(&agg);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0], RequiredDistribution::Singleton);
    }

    #[test]
    fn test_satisfies() {
        let singleton = Distribution::Singleton {
            worker: "w1".into(),
        };
        let hash_part = Distribution::HashPartitioned {
            field_refs: vec![0],
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
            field_refs: vec![0],
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
                field_refs: vec![0],
                workers: vec!["w1".into()],
            },
        ];

        let reqs_satisfied = vec![
            RequiredDistribution::Any,
            RequiredDistribution::HashPartitioned {
                field_refs: vec![0],
            },
        ];
        assert!(all_satisfied(&dists, &reqs_satisfied));

        let reqs_unsatisfied = vec![
            RequiredDistribution::HashPartitioned {
                field_refs: vec![1],
            },
            RequiredDistribution::Singleton,
        ];
        assert!(!all_satisfied(&dists, &reqs_unsatisfied));
    }
}
