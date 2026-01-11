//! Substrait Rel reconstruction utilities.
//!
//! This module provides functions to reconstruct and serialize Substrait Rels.

use prost::Message;
use substrait::proto::read_rel::{NamedTable, ReadType};
use substrait::proto::rel::RelType;
use substrait::proto::{NamedStruct, ReadRel, Rel, Type};

/// Reconstruct a Rel with new children.
///
/// This is essential for the stage cutting phase where we need to
/// replace child Rels with ReadRels that read from exchange outputs.
///
/// # Arguments
/// * `rel` - The original Rel to reconstruct
/// * `new_children` - New child Rels to insert
///
/// # Returns
/// A new Rel with the same operator type but new children.
///
/// # Panics
/// Panics if the number of new children doesn't match the operator's expected child count.
pub fn reconstruct_rel_with_children(rel: &Rel, new_children: Vec<Rel>) -> Rel {
    let Some(rel_type) = &rel.rel_type else {
        return Rel { rel_type: None };
    };

    let new_rel_type = match rel_type {
        // Leaf nodes - should have no new children
        RelType::Read(read) => {
            assert!(
                new_children.is_empty(),
                "Read is a leaf node, cannot have children"
            );
            RelType::Read(read.clone())
        }
        RelType::Reference(r) => {
            assert!(
                new_children.is_empty(),
                "Reference is a leaf node, cannot have children"
            );
            RelType::Reference(r.clone())
        }
        RelType::ExtensionLeaf(ext) => {
            assert!(
                new_children.is_empty(),
                "ExtensionLeaf is a leaf node, cannot have children"
            );
            RelType::ExtensionLeaf(ext.clone())
        }

        // Single-input operators
        RelType::Filter(filter) => {
            assert_eq!(new_children.len(), 1, "Filter requires exactly 1 child");
            let mut new_filter = filter.as_ref().clone();
            new_filter.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Filter(Box::new(new_filter))
        }

        RelType::Project(project) => {
            assert_eq!(new_children.len(), 1, "Project requires exactly 1 child");
            let mut new_project = project.as_ref().clone();
            new_project.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Project(Box::new(new_project))
        }

        RelType::Sort(sort) => {
            assert_eq!(new_children.len(), 1, "Sort requires exactly 1 child");
            let mut new_sort = sort.as_ref().clone();
            new_sort.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Sort(Box::new(new_sort))
        }

        RelType::Fetch(fetch) => {
            assert_eq!(new_children.len(), 1, "Fetch requires exactly 1 child");
            let mut new_fetch = fetch.as_ref().clone();
            new_fetch.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Fetch(Box::new(new_fetch))
        }

        RelType::Aggregate(agg) => {
            assert_eq!(new_children.len(), 1, "Aggregate requires exactly 1 child");
            let mut new_agg = agg.as_ref().clone();
            new_agg.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Aggregate(Box::new(new_agg))
        }

        RelType::ExtensionSingle(ext) => {
            assert_eq!(
                new_children.len(),
                1,
                "ExtensionSingle requires exactly 1 child"
            );
            let mut new_ext = ext.as_ref().clone();
            new_ext.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::ExtensionSingle(Box::new(new_ext))
        }

        RelType::Write(write) => {
            assert_eq!(new_children.len(), 1, "Write requires exactly 1 child");
            let mut new_write = write.as_ref().clone();
            new_write.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Write(Box::new(new_write))
        }

        RelType::Ddl(ddl) => {
            // DDL might have 0 or 1 child (view definition)
            let mut new_ddl = ddl.as_ref().clone();
            if !new_children.is_empty() {
                assert_eq!(new_children.len(), 1, "Ddl has at most 1 child");
                new_ddl.view_definition = Some(Box::new(new_children.into_iter().next().unwrap()));
            }
            RelType::Ddl(Box::new(new_ddl))
        }

        // UpdateRel is a leaf-like operation (operates on table, not relation)
        RelType::Update(update) => {
            assert!(
                new_children.is_empty(),
                "Update is a leaf-like node, cannot have children"
            );
            RelType::Update(update.clone())
        }

        RelType::Window(window) => {
            assert_eq!(new_children.len(), 1, "Window requires exactly 1 child");
            let mut new_window = window.as_ref().clone();
            new_window.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Window(Box::new(new_window))
        }

        RelType::Exchange(exchange) => {
            assert_eq!(new_children.len(), 1, "Exchange requires exactly 1 child");
            let mut new_exchange = exchange.as_ref().clone();
            new_exchange.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Exchange(Box::new(new_exchange))
        }

        RelType::Expand(expand) => {
            assert_eq!(new_children.len(), 1, "Expand requires exactly 1 child");
            let mut new_expand = expand.as_ref().clone();
            new_expand.input = Some(Box::new(new_children.into_iter().next().unwrap()));
            RelType::Expand(Box::new(new_expand))
        }

        // Two-input operators (joins)
        RelType::Join(join) => {
            assert_eq!(new_children.len(), 2, "Join requires exactly 2 children");
            let mut new_join = join.as_ref().clone();
            let mut iter = new_children.into_iter();
            new_join.left = Some(Box::new(iter.next().unwrap()));
            new_join.right = Some(Box::new(iter.next().unwrap()));
            RelType::Join(Box::new(new_join))
        }

        RelType::HashJoin(join) => {
            assert_eq!(new_children.len(), 2, "HashJoin requires exactly 2 children");
            let mut new_join = join.as_ref().clone();
            let mut iter = new_children.into_iter();
            new_join.left = Some(Box::new(iter.next().unwrap()));
            new_join.right = Some(Box::new(iter.next().unwrap()));
            RelType::HashJoin(Box::new(new_join))
        }

        RelType::MergeJoin(join) => {
            assert_eq!(
                new_children.len(),
                2,
                "MergeJoin requires exactly 2 children"
            );
            let mut new_join = join.as_ref().clone();
            let mut iter = new_children.into_iter();
            new_join.left = Some(Box::new(iter.next().unwrap()));
            new_join.right = Some(Box::new(iter.next().unwrap()));
            RelType::MergeJoin(Box::new(new_join))
        }

        RelType::NestedLoopJoin(join) => {
            assert_eq!(
                new_children.len(),
                2,
                "NestedLoopJoin requires exactly 2 children"
            );
            let mut new_join = join.as_ref().clone();
            let mut iter = new_children.into_iter();
            new_join.left = Some(Box::new(iter.next().unwrap()));
            new_join.right = Some(Box::new(iter.next().unwrap()));
            RelType::NestedLoopJoin(Box::new(new_join))
        }

        RelType::Cross(cross) => {
            assert_eq!(new_children.len(), 2, "Cross requires exactly 2 children");
            let mut new_cross = cross.as_ref().clone();
            let mut iter = new_children.into_iter();
            new_cross.left = Some(Box::new(iter.next().unwrap()));
            new_cross.right = Some(Box::new(iter.next().unwrap()));
            RelType::Cross(Box::new(new_cross))
        }

        // Multi-input operators
        RelType::Set(set) => {
            let mut new_set = set.clone();
            new_set.inputs = new_children;
            RelType::Set(new_set)
        }

        RelType::ExtensionMulti(ext) => {
            let mut new_ext = ext.clone();
            new_ext.inputs = new_children;
            RelType::ExtensionMulti(new_ext)
        }
    };

    Rel {
        rel_type: Some(new_rel_type),
    }
}

/// Create a ReadRel for an exchange input table.
///
/// This creates a named table read that references a virtual table
/// registered from exchange output (e.g., "__exchange_0").
///
/// # Arguments
/// * `table_name` - The name of the exchange table (e.g., "__exchange_0")
/// * `schema` - Optional schema for the table
///
/// # Example
/// ```ignore
/// let read_rel = create_read_rel("__exchange_0", Some(schema));
/// ```
pub fn create_read_rel(table_name: &str, schema: Option<NamedStruct>) -> Rel {
    let read_rel = ReadRel {
        read_type: Some(ReadType::NamedTable(NamedTable {
            names: vec![table_name.to_string()],
            advanced_extension: None,
        })),
        base_schema: schema,
        filter: None,
        best_effort_filter: None,
        projection: None,
        advanced_extension: None,
        common: None,
    };

    Rel {
        rel_type: Some(RelType::Read(Box::new(read_rel))),
    }
}

/// Create a ReadRel with a simple schema.
///
/// Helper function that creates a ReadRel with column names and types.
///
/// # Arguments
/// * `table_name` - The name of the table
/// * `columns` - Vector of (column_name, type) pairs
pub fn create_read_rel_with_schema(table_name: &str, columns: Vec<(&str, Type)>) -> Rel {
    let names: Vec<String> = columns.iter().map(|(name, _)| name.to_string()).collect();
    let types: Vec<Type> = columns.into_iter().map(|(_, t)| t).collect();

    let schema = NamedStruct {
        names,
        r#struct: Some(substrait::proto::r#type::Struct { types, ..Default::default() }),
    };

    create_read_rel(table_name, Some(schema))
}

/// Serialize a Rel to Substrait protobuf bytes.
///
/// This is used to create the `substrait_plan` field in a Stage.
pub fn serialize_rel_to_substrait(rel: &Rel) -> Vec<u8> {
    rel.encode_to_vec()
}

/// Deserialize Substrait protobuf bytes to a Rel.
///
/// # Errors
/// Returns an error if the bytes are not valid Substrait protobuf.
pub fn deserialize_rel_from_substrait(bytes: &[u8]) -> Result<Rel, prost::DecodeError> {
    <Rel as Message>::decode(bytes)
}

/// Create an exchange table name from an exchange ID.
///
/// Format: "__exchange_{id}"
pub fn exchange_table_name(exchange_id: u32) -> String {
    format!("__exchange_{}", exchange_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrait::proto::{FilterRel, JoinRel, ProjectRel};

    fn create_test_read_rel(name: &str) -> Rel {
        create_read_rel(name, None)
    }

    #[test]
    fn test_reconstruct_filter() {
        let original_input = create_test_read_rel("original");
        let filter = Rel {
            rel_type: Some(RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(original_input)),
                condition: None,
                common: None,
                advanced_extension: None,
            }))),
        };

        let new_input = create_test_read_rel("new_input");
        let reconstructed = reconstruct_rel_with_children(&filter, vec![new_input]);

        // Verify it's still a filter
        if let Some(RelType::Filter(f)) = &reconstructed.rel_type {
            // Verify the input was replaced
            if let Some(input) = &f.input {
                if let Some(RelType::Read(r)) = &input.rel_type {
                    if let Some(ReadType::NamedTable(nt)) = &r.read_type {
                        assert_eq!(nt.names[0], "new_input");
                        return;
                    }
                }
            }
        }
        panic!("Reconstruction failed");
    }

    #[test]
    fn test_reconstruct_project() {
        let original_input = create_test_read_rel("original");
        let project = Rel {
            rel_type: Some(RelType::Project(Box::new(ProjectRel {
                input: Some(Box::new(original_input)),
                expressions: vec![],
                common: None,
                advanced_extension: None,
            }))),
        };

        let new_input = create_test_read_rel("new_input");
        let reconstructed = reconstruct_rel_with_children(&project, vec![new_input]);

        if let Some(RelType::Project(p)) = &reconstructed.rel_type {
            if let Some(input) = &p.input {
                if let Some(RelType::Read(r)) = &input.rel_type {
                    if let Some(ReadType::NamedTable(nt)) = &r.read_type {
                        assert_eq!(nt.names[0], "new_input");
                        return;
                    }
                }
            }
        }
        panic!("Reconstruction failed");
    }

    #[test]
    fn test_reconstruct_join() {
        let left = create_test_read_rel("left");
        let right = create_test_read_rel("right");

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

        let new_left = create_test_read_rel("new_left");
        let new_right = create_test_read_rel("new_right");
        let reconstructed = reconstruct_rel_with_children(&join, vec![new_left, new_right]);

        if let Some(RelType::Join(j)) = &reconstructed.rel_type {
            // Check left
            if let Some(l) = &j.left {
                if let Some(RelType::Read(r)) = &l.rel_type {
                    if let Some(ReadType::NamedTable(nt)) = &r.read_type {
                        assert_eq!(nt.names[0], "new_left");
                    }
                }
            }
            // Check right
            if let Some(r) = &j.right {
                if let Some(RelType::Read(read)) = &r.rel_type {
                    if let Some(ReadType::NamedTable(nt)) = &read.read_type {
                        assert_eq!(nt.names[0], "new_right");
                        return;
                    }
                }
            }
        }
        panic!("Reconstruction failed");
    }

    #[test]
    fn test_create_read_rel() {
        let rel = create_read_rel("test_table", None);

        if let Some(RelType::Read(r)) = &rel.rel_type {
            if let Some(ReadType::NamedTable(nt)) = &r.read_type {
                assert_eq!(nt.names[0], "test_table");
                return;
            }
        }
        panic!("create_read_rel failed");
    }

    #[test]
    fn test_serialize_deserialize() {
        let original = create_read_rel("test_table", None);
        let bytes = serialize_rel_to_substrait(&original);
        let deserialized = deserialize_rel_from_substrait(&bytes).unwrap();

        // Verify round-trip
        if let Some(RelType::Read(r)) = &deserialized.rel_type {
            if let Some(ReadType::NamedTable(nt)) = &r.read_type {
                assert_eq!(nt.names[0], "test_table");
                return;
            }
        }
        panic!("Serialize/deserialize round-trip failed");
    }

    #[test]
    fn test_exchange_table_name() {
        assert_eq!(exchange_table_name(0), "__exchange_0");
        assert_eq!(exchange_table_name(42), "__exchange_42");
    }

    #[test]
    #[should_panic(expected = "Read is a leaf node")]
    fn test_reconstruct_read_with_children_panics() {
        let read = create_test_read_rel("test");
        let child = create_test_read_rel("child");
        reconstruct_rel_with_children(&read, vec![child]);
    }

    #[test]
    #[should_panic(expected = "Filter requires exactly 1 child")]
    fn test_reconstruct_filter_wrong_child_count_panics() {
        let input = create_test_read_rel("original");
        let filter = Rel {
            rel_type: Some(RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(input)),
                ..Default::default()
            }))),
        };

        // Try to reconstruct with 2 children instead of 1
        let child1 = create_test_read_rel("child1");
        let child2 = create_test_read_rel("child2");
        reconstruct_rel_with_children(&filter, vec![child1, child2]);
    }
}
