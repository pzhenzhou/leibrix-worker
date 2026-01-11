//! Child extraction from Substrait Rel nodes.
//!
//! This module provides functions to extract child relations from Substrait Rels.

use substrait::proto::rel::RelType;
use substrait::proto::Rel;

/// Extract children from a Substrait Rel.
///
/// Returns references to child Rels based on the RelType variant.
/// Each RelType has different child structure:
/// - Read, Reference, ExtensionLeaf: No children (leaf nodes)
/// - Filter, Project, Sort, Fetch, Aggregate: Single input child
/// - Join, HashJoin, MergeJoin, NestedLoopJoin, Cross: Two children (left, right)
/// - Set, ExtensionMulti: Multiple children
///
/// # Example
/// ```ignore
/// let children = get_substrait_children(&rel);
/// for child in children {
///     // Process each child...
/// }
/// ```
pub fn get_substrait_children(rel: &Rel) -> Vec<&Rel> {
    match &rel.rel_type {
        Some(rel_type) => get_children_from_rel_type(rel_type),
        None => vec![],
    }
}

/// Extract mutable children from a Substrait Rel.
///
/// Returns mutable references to child Rels.
pub fn get_substrait_children_mut(rel: &mut Rel) -> Vec<&mut Rel> {
    match &mut rel.rel_type {
        Some(rel_type) => get_children_from_rel_type_mut(rel_type),
        None => vec![],
    }
}

/// Internal helper to extract children from RelType.
fn get_children_from_rel_type(rel_type: &RelType) -> Vec<&Rel> {
    match rel_type {
        // Leaf nodes - no children
        RelType::Read(_) => vec![],
        RelType::Reference(_) => vec![],
        RelType::ExtensionLeaf(_) => vec![],

        // Single-input operators
        RelType::Filter(filter) => filter.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Project(project) => project.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Sort(sort) => sort.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Fetch(fetch) => fetch.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Aggregate(agg) => agg.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::ExtensionSingle(ext) => ext.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Write(write) => write.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Ddl(ddl) => ddl
            .view_definition
            .as_ref()
            .map_or(vec![], |r| vec![r.as_ref()]),

        // UpdateRel is a leaf-like operation (operates on table, not relation)
        RelType::Update(_) => vec![],

        RelType::Window(window) => window.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        RelType::Exchange(exchange) => {
            exchange.input.as_ref().map_or(vec![], |r| vec![r.as_ref()])
        }

        RelType::Expand(expand) => expand.input.as_ref().map_or(vec![], |r| vec![r.as_ref()]),

        // Two-input operators (joins)
        RelType::Join(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &join.left {
                children.push(left.as_ref());
            }
            if let Some(right) = &join.right {
                children.push(right.as_ref());
            }
            children
        }

        RelType::HashJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &join.left {
                children.push(left.as_ref());
            }
            if let Some(right) = &join.right {
                children.push(right.as_ref());
            }
            children
        }

        RelType::MergeJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &join.left {
                children.push(left.as_ref());
            }
            if let Some(right) = &join.right {
                children.push(right.as_ref());
            }
            children
        }

        RelType::NestedLoopJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &join.left {
                children.push(left.as_ref());
            }
            if let Some(right) = &join.right {
                children.push(right.as_ref());
            }
            children
        }

        RelType::Cross(cross) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &cross.left {
                children.push(left.as_ref());
            }
            if let Some(right) = &cross.right {
                children.push(right.as_ref());
            }
            children
        }

        // Multi-input operators
        RelType::Set(set) => set.inputs.iter().collect(),

        RelType::ExtensionMulti(ext) => ext.inputs.iter().collect(),
    }
}

/// Internal helper to extract mutable children from RelType.
fn get_children_from_rel_type_mut(rel_type: &mut RelType) -> Vec<&mut Rel> {
    match rel_type {
        // Leaf nodes - no children
        RelType::Read(_) => vec![],
        RelType::Reference(_) => vec![],
        RelType::ExtensionLeaf(_) => vec![],

        // Single-input operators
        RelType::Filter(filter) => filter
            .input
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Project(project) => project
            .input
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Sort(sort) => sort.input.as_mut().map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Fetch(fetch) => fetch.input.as_mut().map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Aggregate(agg) => agg.input.as_mut().map_or(vec![], |r| vec![r.as_mut()]),

        RelType::ExtensionSingle(ext) => ext.input.as_mut().map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Write(write) => write.input.as_mut().map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Ddl(ddl) => ddl
            .view_definition
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        // UpdateRel is a leaf-like operation (operates on table, not relation)
        RelType::Update(_) => vec![],

        RelType::Window(window) => window
            .input
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Exchange(exchange) => exchange
            .input
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        RelType::Expand(expand) => expand
            .input
            .as_mut()
            .map_or(vec![], |r| vec![r.as_mut()]),

        // Two-input operators (joins)
        RelType::Join(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &mut join.left {
                children.push(left.as_mut());
            }
            if let Some(right) = &mut join.right {
                children.push(right.as_mut());
            }
            children
        }

        RelType::HashJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &mut join.left {
                children.push(left.as_mut());
            }
            if let Some(right) = &mut join.right {
                children.push(right.as_mut());
            }
            children
        }

        RelType::MergeJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &mut join.left {
                children.push(left.as_mut());
            }
            if let Some(right) = &mut join.right {
                children.push(right.as_mut());
            }
            children
        }

        RelType::NestedLoopJoin(join) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &mut join.left {
                children.push(left.as_mut());
            }
            if let Some(right) = &mut join.right {
                children.push(right.as_mut());
            }
            children
        }

        RelType::Cross(cross) => {
            let mut children = Vec::with_capacity(2);
            if let Some(left) = &mut cross.left {
                children.push(left.as_mut());
            }
            if let Some(right) = &mut cross.right {
                children.push(right.as_mut());
            }
            children
        }

        // Multi-input operators
        RelType::Set(set) => set.inputs.iter_mut().collect(),

        RelType::ExtensionMulti(ext) => ext.inputs.iter_mut().collect(),
    }
}

/// Get the number of children for a Rel.
pub fn get_child_count(rel: &Rel) -> usize {
    get_substrait_children(rel).len()
}

/// Check if a Rel is a leaf node (has no children).
pub fn is_leaf_node(rel: &Rel) -> bool {
    get_child_count(rel) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrait::proto::{
        read_rel::ReadType, AggregateRel, CrossRel,
        FilterRel, JoinRel, ProjectRel, ReadRel,
    };

    fn create_read_rel() -> Rel {
        Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                read_type: Some(ReadType::NamedTable(
                    substrait::proto::read_rel::NamedTable {
                        names: vec!["test_table".to_string()],
                        advanced_extension: None,
                    },
                )),
                ..Default::default()
            }))),
        }
    }

    fn create_filter_rel(input: Rel) -> Rel {
        Rel {
            rel_type: Some(RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(input)),
                condition: None,
                common: None,
                advanced_extension: None,
            }))),
        }
    }

    fn create_project_rel(input: Rel) -> Rel {
        Rel {
            rel_type: Some(RelType::Project(Box::new(ProjectRel {
                input: Some(Box::new(input)),
                expressions: vec![],
                common: None,
                advanced_extension: None,
            }))),
        }
    }

    #[test]
    fn test_read_has_no_children() {
        let read = create_read_rel();
        assert!(is_leaf_node(&read));
        assert_eq!(get_child_count(&read), 0);
    }

    #[test]
    fn test_filter_has_one_child() {
        let read = create_read_rel();
        let filter = create_filter_rel(read);

        let children = get_substrait_children(&filter);
        assert_eq!(children.len(), 1);
        assert!(!is_leaf_node(&filter));
    }

    #[test]
    fn test_project_over_filter() {
        let read = create_read_rel();
        let filter = create_filter_rel(read);
        let project = create_project_rel(filter);

        let project_children = get_substrait_children(&project);
        assert_eq!(project_children.len(), 1);

        // The child should be a filter
        if let Some(RelType::Filter(_)) = &project_children[0].rel_type {
            // correct
        } else {
            panic!("Expected Filter child");
        }
    }

    #[test]
    fn test_join_has_two_children() {
        let left = create_read_rel();
        let right = create_read_rel();

        let join = Rel {
            rel_type: Some(RelType::Join(Box::new(JoinRel {
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
                expression: None,
                post_join_filter: None,
                r#type: 0,
                common: None,
                advanced_extension: None,
            }))),
        };

        let children = get_substrait_children(&join);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_cross_has_two_children() {
        let left = create_read_rel();
        let right = create_read_rel();

        let cross = Rel {
            rel_type: Some(RelType::Cross(Box::new(CrossRel {
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
                common: None,
                advanced_extension: None,
            }))),
        };

        let children = get_substrait_children(&cross);
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_empty_rel() {
        let empty = Rel { rel_type: None };
        assert!(is_leaf_node(&empty));
        assert_eq!(get_substrait_children(&empty).len(), 0);
    }
}
