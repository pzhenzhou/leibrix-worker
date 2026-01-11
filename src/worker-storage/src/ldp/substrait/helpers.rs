//! Helper functions for extracting information from Substrait Rels.
//!
//! This module provides utilities to extract field references and other
//! structural information from Substrait operator nodes.

use substrait::proto::aggregate_rel::Grouping;
use substrait::proto::expression::field_reference::ReferenceType;
use substrait::proto::expression::reference_segment::ReferenceType as SegmentRefType;
use substrait::proto::expression::RexType;
use substrait::proto::rel::RelType;
use substrait::proto::{AggregateRel, Expression, JoinRel, Rel};

/// Extract field references from an AggregateRel's grouping expressions.
///
/// Returns the field indices (0-based) that are used in GROUP BY.
/// These are Substrait field references pointing into the input schema.
///
/// # Example
/// ```ignore
/// // For: SELECT region, SUM(sales) FROM orders GROUP BY region
/// // Returns: vec![0] if region is the first column
/// let group_refs = extract_grouping_field_refs(&aggregate_rel);
/// ```
#[allow(deprecated)] // grouping_expressions is deprecated in newer substrait versions
pub fn extract_grouping_field_refs(agg: &AggregateRel) -> Vec<u32> {
    let mut field_refs = Vec::new();

    for grouping in &agg.groupings {
        for expr in &grouping.grouping_expressions {
            if let Some(field_ref) = extract_field_ref_from_expression(expr) {
                if !field_refs.contains(&field_ref) {
                    field_refs.push(field_ref);
                }
            }
        }
    }

    field_refs
}

/// Check if all groupings in an AggregateRel are empty (global aggregate).
///
/// A global aggregate has no GROUP BY clause, meaning all groupings
/// have empty `grouping_expressions`.
#[allow(deprecated)] // grouping_expressions is deprecated in newer substrait versions
pub fn all_groupings_empty(agg: &AggregateRel) -> bool {
    if agg.groupings.is_empty() {
        return true;
    }
    agg.groupings
        .iter()
        .all(|g| g.grouping_expressions.is_empty())
}

/// Extract join key field references from a JoinRel.
///
/// Result type for join key extraction.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinKeyRefs {
    /// Field indices referencing columns in the LEFT input's schema.
    pub left_keys: Vec<u32>,
    /// Field indices referencing columns in the RIGHT input's schema.
    /// These are already offset-adjusted (0-based relative to right schema).
    pub right_keys: Vec<u32>,
}

impl JoinKeyRefs {
    /// Check if join keys were successfully extracted.
    pub fn is_valid(&self) -> bool {
        !self.left_keys.is_empty()
            && !self.right_keys.is_empty()
            && self.left_keys.len() == self.right_keys.len()
    }

    /// Check if this represents a cross join (no keys).
    pub fn is_cross_join(&self) -> bool {
        self.left_keys.is_empty() && self.right_keys.is_empty()
    }
}

/// Error type for join key extraction.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinKeyError {
    /// No join expression found (might be a cross join).
    NoExpression,
    /// Could not determine left input schema width.
    MissingLeftInput,
    /// Join keys could not be extracted from expression.
    ExtractionFailed(String),
    /// Mismatched key counts between left and right.
    MismatchedKeyCounts { left: usize, right: usize },
}

impl std::fmt::Display for JoinKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinKeyError::NoExpression => write!(f, "No join expression found"),
            JoinKeyError::MissingLeftInput => write!(f, "Cannot determine left input schema width"),
            JoinKeyError::ExtractionFailed(msg) => write!(f, "Join key extraction failed: {}", msg),
            JoinKeyError::MismatchedKeyCounts { left, right } => {
                write!(f, "Mismatched key counts: left={}, right={}", left, right)
            }
        }
    }
}

impl std::error::Error for JoinKeyError {}

/// Extract join key field references from a JoinRel.
///
/// Returns (left_keys, right_keys) where each is a vector of field indices.
/// The left keys reference columns in the left input's schema (0-based).
/// The right keys reference columns in the right input's schema (0-based, offset-adjusted).
///
/// # Schema Offset Handling
/// In Substrait, join inputs are flattened into a single schema:
/// - Left fields:  0 .. left_schema_width
/// - Right fields: left_schema_width .. total_width
///
/// This function determines left_schema_width from the left input's base_schema
/// and adjusts right field references accordingly.
///
/// # Note
/// For equi-joins, the join expression is typically a comparison like:
/// `left.col0 = right.col0 AND left.col1 = right.col1`
pub fn extract_join_key_refs(join: &JoinRel) -> Result<JoinKeyRefs, JoinKeyError> {
    // Determine the width of the left input schema
    let left_width = get_left_input_width(join)?;

    let Some(expr) = &join.expression else {
        // No expression - this is a cross join
        return Ok(JoinKeyRefs {
            left_keys: vec![],
            right_keys: vec![],
        });
    };

    let mut all_field_refs = Vec::new();
    extract_field_refs_from_expression(expr, &mut all_field_refs);

    if all_field_refs.is_empty() {
        return Err(JoinKeyError::ExtractionFailed(
            "No field references found in join expression".to_string(),
        ));
    }

    // Separate into left and right based on schema offset
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    for field_ref in all_field_refs {
        if field_ref < left_width {
            if !left_keys.contains(&field_ref) {
                left_keys.push(field_ref);
            }
        } else {
            // Adjust to 0-based index relative to right schema
            let adjusted = field_ref - left_width;
            if !right_keys.contains(&adjusted) {
                right_keys.push(adjusted);
            }
        }
    }

    Ok(JoinKeyRefs {
        left_keys,
        right_keys,
    })
}

/// Legacy function for backward compatibility.
/// Returns empty vectors on error instead of Result.
#[deprecated(note = "Use extract_join_key_refs() which returns Result")]
pub fn extract_join_key_refs_legacy(join: &JoinRel) -> (Vec<u32>, Vec<u32>) {
    match extract_join_key_refs(join) {
        Ok(keys) => (keys.left_keys, keys.right_keys),
        Err(_) => (vec![], vec![]),
    }
}

/// Get the schema width (number of fields) of the left input of a join.
fn get_left_input_width(join: &JoinRel) -> Result<u32, JoinKeyError> {
    let left_input = join.left.as_ref().ok_or(JoinKeyError::MissingLeftInput)?;

    // Try to get schema from the left input
    // For ReadRel, we can get it from base_schema
    // For other operators, we'd need to propagate schema through the tree
    if let Some(rel_type) = &left_input.rel_type {
        match rel_type {
            RelType::Read(read) => {
                if let Some(schema) = &read.base_schema {
                    return Ok(schema.names.len() as u32);
                }
            }
            RelType::Project(project) => {
                // Project output width = number of expressions
                return Ok(project.expressions.len() as u32);
            }
            RelType::Filter(filter) => {
                // Filter preserves input width
                if let Some(input) = &filter.input {
                    return get_rel_output_width(input);
                }
            }
            RelType::Aggregate(agg) => {
                // Aggregate output = grouping fields + measures
                #[allow(deprecated)]
                let grouping_count: usize = agg
                    .groupings
                    .iter()
                    .map(|g| g.grouping_expressions.len())
                    .sum();
                let measure_count = agg.measures.len();
                return Ok((grouping_count + measure_count) as u32);
            }
            // For other operators, fall back to a heuristic
            _ => {}
        }
    }

    // Fallback: try to recursively find schema width
    get_rel_output_width(left_input)
}

/// Recursively determine the output width of a Rel.
fn get_rel_output_width(rel: &Rel) -> Result<u32, JoinKeyError> {
    if let Some(rel_type) = &rel.rel_type {
        match rel_type {
            RelType::Read(read) => {
                if let Some(schema) = &read.base_schema {
                    return Ok(schema.names.len() as u32);
                }
            }
            RelType::Project(project) => {
                return Ok(project.expressions.len() as u32);
            }
            RelType::Filter(filter) => {
                // Filter preserves input width
                if let Some(input) = filter.input.as_ref() {
                    return get_rel_output_width(input);
                }
            }
            RelType::Sort(sort) => {
                // Sort preserves input width
                if let Some(input) = sort.input.as_ref() {
                    return get_rel_output_width(input);
                }
            }
            RelType::Aggregate(agg) => {
                #[allow(deprecated)]
                let grouping_count: usize = agg
                    .groupings
                    .iter()
                    .map(|g| g.grouping_expressions.len())
                    .sum();
                let measure_count = agg.measures.len();
                return Ok((grouping_count + measure_count) as u32);
            }
            _ => {}
        }
    }

    // Default fallback - assume reasonable width
    // This is a last resort; callers should ensure schema info is available
    Err(JoinKeyError::MissingLeftInput)
}

/// Extract all field references from an expression (recursively).
fn extract_field_refs_from_expression(expr: &Expression, refs: &mut Vec<u32>) {
    match &expr.rex_type {
        Some(RexType::Selection(field_ref)) => {
            if let Some(idx) = extract_field_ref_from_reference_type(&field_ref.reference_type) {
                refs.push(idx);
            }
        }
        Some(RexType::ScalarFunction(func)) => {
            // Recurse into function arguments
            for arg in &func.arguments {
                if let Some(substrait::proto::function_argument::ArgType::Value(v)) = &arg.arg_type
                {
                    extract_field_refs_from_expression(v, refs);
                }
            }
        }
        Some(RexType::IfThen(if_then)) => {
            for clause in &if_then.ifs {
                if let Some(cond) = &clause.r#if {
                    extract_field_refs_from_expression(cond, refs);
                }
                if let Some(then) = &clause.then {
                    extract_field_refs_from_expression(then, refs);
                }
            }
            if let Some(else_clause) = &if_then.r#else {
                extract_field_refs_from_expression(else_clause, refs);
            }
        }
        Some(RexType::Cast(cast)) => {
            if let Some(input) = &cast.input {
                extract_field_refs_from_expression(input, refs);
            }
        }
        _ => {}
    }
}

/// Extract join keys from HashJoinRel's direct key fields.
///
/// HashJoinRel has explicit `left_keys` and `right_keys` fields,
/// making extraction straightforward.
pub fn extract_hash_join_key_refs(
    join: &substrait::proto::HashJoinRel,
) -> (Vec<u32>, Vec<u32>) {
    let left_keys: Vec<u32> = join
        .left_keys
        .iter()
        .filter_map(|fr| extract_field_ref_from_field_reference(fr))
        .collect();

    let right_keys: Vec<u32> = join
        .right_keys
        .iter()
        .filter_map(|fr| extract_field_ref_from_field_reference(fr))
        .collect();

    (left_keys, right_keys)
}

/// Extract a field reference index from an Expression.
///
/// Returns Some(field_index) if the expression is a direct field reference,
/// None otherwise.
pub fn extract_field_ref_from_expression(expr: &Expression) -> Option<u32> {
    match &expr.rex_type {
        Some(RexType::Selection(field_ref)) => {
            extract_field_ref_from_reference_type(&field_ref.reference_type)
        }
        _ => None,
    }
}

/// Extract field reference from a FieldReference.
fn extract_field_ref_from_field_reference(
    fr: &substrait::proto::expression::FieldReference,
) -> Option<u32> {
    extract_field_ref_from_reference_type(&fr.reference_type)
}

/// Extract field reference from ReferenceType.
fn extract_field_ref_from_reference_type(
    ref_type: &Option<ReferenceType>,
) -> Option<u32> {
    match ref_type {
        Some(ReferenceType::DirectReference(segment)) => {
            match &segment.reference_type {
                Some(SegmentRefType::StructField(sf)) => Some(sf.field as u32),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the RelType variant name for debugging/logging.
pub fn get_rel_type_name(rel: &Rel) -> &'static str {
    match &rel.rel_type {
        Some(RelType::Read(_)) => "Read",
        Some(RelType::Filter(_)) => "Filter",
        Some(RelType::Fetch(_)) => "Fetch",
        Some(RelType::Aggregate(_)) => "Aggregate",
        Some(RelType::Sort(_)) => "Sort",
        Some(RelType::Join(_)) => "Join",
        Some(RelType::Project(_)) => "Project",
        Some(RelType::Set(_)) => "Set",
        Some(RelType::ExtensionSingle(_)) => "ExtensionSingle",
        Some(RelType::ExtensionMulti(_)) => "ExtensionMulti",
        Some(RelType::ExtensionLeaf(_)) => "ExtensionLeaf",
        Some(RelType::Cross(_)) => "Cross",
        Some(RelType::Reference(_)) => "Reference",
        Some(RelType::Write(_)) => "Write",
        Some(RelType::Ddl(_)) => "Ddl",
        Some(RelType::Update(_)) => "Update",
        Some(RelType::HashJoin(_)) => "HashJoin",
        Some(RelType::MergeJoin(_)) => "MergeJoin",
        Some(RelType::NestedLoopJoin(_)) => "NestedLoopJoin",
        Some(RelType::Window(_)) => "Window",
        Some(RelType::Exchange(_)) => "Exchange",
        Some(RelType::Expand(_)) => "Expand",
        None => "Unknown",
    }
}

/// Check if a Rel requires data to be on a single node.
///
/// Returns true for operators that typically require Singleton distribution:
/// - Sort (global ordering)
/// - Fetch (LIMIT/OFFSET)
/// - Global Aggregate (no GROUP BY)
pub fn requires_singleton(rel: &Rel) -> bool {
    match &rel.rel_type {
        Some(RelType::Sort(_)) => true,
        Some(RelType::Fetch(_)) => true,
        Some(RelType::Aggregate(agg)) => all_groupings_empty(agg),
        _ => false,
    }
}

/// Check if a Rel is a join operator.
pub fn is_join_operator(rel: &Rel) -> bool {
    matches!(
        &rel.rel_type,
        Some(RelType::Join(_))
            | Some(RelType::HashJoin(_))
            | Some(RelType::MergeJoin(_))
            | Some(RelType::NestedLoopJoin(_))
            | Some(RelType::Cross(_))
    )
}

/// Check if a Rel is distribution-agnostic (preserves input distribution).
pub fn is_distribution_agnostic(rel: &Rel) -> bool {
    matches!(
        &rel.rel_type,
        Some(RelType::Filter(_)) | Some(RelType::Project(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use substrait::proto::expression::field_reference::ReferenceType;
    use substrait::proto::expression::reference_segment::{
        ReferenceType as SegmentRefType, StructField,
    };
    use substrait::proto::expression::{FieldReference, ReferenceSegment, RexType};
    use substrait::proto::{read_rel::ReadType, Expression, ReadRel};

    fn create_field_ref_expression(field_idx: i32) -> Expression {
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(SegmentRefType::StructField(Box::new(StructField {
                        field: field_idx,
                        child: None,
                    }))),
                })),
                root_type: None,
            }))),
        }
    }

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
    fn test_extract_field_ref() {
        let expr = create_field_ref_expression(3);
        let field_ref = extract_field_ref_from_expression(&expr);
        assert_eq!(field_ref, Some(3));
    }

    #[test]
    fn test_extract_grouping_field_refs() {
        let agg = AggregateRel {
            groupings: vec![Grouping {
                grouping_expressions: vec![
                    create_field_ref_expression(0),
                    create_field_ref_expression(2),
                ],
                expression_references: vec![],
            }],
            ..Default::default()
        };

        let refs = extract_grouping_field_refs(&agg);
        assert_eq!(refs, vec![0, 2]);
    }

    #[test]
    fn test_all_groupings_empty() {
        // Empty groupings = global aggregate
        let global_agg = AggregateRel {
            groupings: vec![],
            ..Default::default()
        };
        assert!(all_groupings_empty(&global_agg));

        // Groupings with empty expressions = global aggregate
        let empty_expr_agg = AggregateRel {
            groupings: vec![Grouping {
                grouping_expressions: vec![],
                expression_references: vec![],
            }],
            ..Default::default()
        };
        assert!(all_groupings_empty(&empty_expr_agg));

        // Groupings with expressions = grouped aggregate
        let grouped_agg = AggregateRel {
            groupings: vec![Grouping {
                grouping_expressions: vec![create_field_ref_expression(0)],
                expression_references: vec![],
            }],
            ..Default::default()
        };
        assert!(!all_groupings_empty(&grouped_agg));
    }

    #[test]
    fn test_get_rel_type_name() {
        let read = create_read_rel();
        assert_eq!(get_rel_type_name(&read), "Read");

        let empty = Rel { rel_type: None };
        assert_eq!(get_rel_type_name(&empty), "Unknown");
    }

    #[test]
    fn test_requires_singleton() {
        let read = create_read_rel();
        assert!(!requires_singleton(&read));

        let sort = Rel {
            rel_type: Some(RelType::Sort(Box::new(substrait::proto::SortRel {
                input: Some(Box::new(read.clone())),
                ..Default::default()
            }))),
        };
        assert!(requires_singleton(&sort));

        let fetch = Rel {
            rel_type: Some(RelType::Fetch(Box::new(substrait::proto::FetchRel {
                input: Some(Box::new(read)),
                ..Default::default()
            }))),
        };
        assert!(requires_singleton(&fetch));
    }

    #[test]
    fn test_is_join_operator() {
        let read = create_read_rel();
        assert!(!is_join_operator(&read));

        let join = Rel {
            rel_type: Some(RelType::Join(Box::new(JoinRel::default()))),
        };
        assert!(is_join_operator(&join));

        let hash_join = Rel {
            rel_type: Some(RelType::HashJoin(Box::new(
                substrait::proto::HashJoinRel::default(),
            ))),
        };
        assert!(is_join_operator(&hash_join));
    }

    #[test]
    fn test_is_distribution_agnostic() {
        let read = create_read_rel();
        assert!(!is_distribution_agnostic(&read));

        let filter = Rel {
            rel_type: Some(RelType::Filter(Box::new(substrait::proto::FilterRel {
                input: Some(Box::new(read.clone())),
                ..Default::default()
            }))),
        };
        assert!(is_distribution_agnostic(&filter));

        let project = Rel {
            rel_type: Some(RelType::Project(Box::new(substrait::proto::ProjectRel {
                input: Some(Box::new(read)),
                ..Default::default()
            }))),
        };
        assert!(is_distribution_agnostic(&project));
    }
}
