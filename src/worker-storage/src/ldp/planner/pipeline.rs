//! LDP Planning Pipeline.
//!
//! This module provides the main entry point for converting SQL queries
//! into Leibrix Distributed Plans (LDP).
//!
//! # Pipeline Overview
//! ```text
//! SQL → Substrait → Annotate → Cut → LdpPlan
//! ```

use crate::engine::duckdb::substrait::duckdb_get_substrait;
use crate::ldp::planner::annotate::{annotate_and_enforce, PlanningError};
use crate::ldp::planner::cut::cut_into_stages;
use crate::ldp::planner::metadata::Metadata;
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::substrait::reconstruct::deserialize_rel_from_substrait;
use crate::ldp::LdpPlan;
use duckdb::Connection;
use substrait::proto::Plan;

/// Error during LDP pipeline execution.
#[derive(Debug)]
pub enum PipelineError {
    /// Failed to get Substrait from DuckDB.
    SubstraitConversion(String),
    /// Failed to deserialize Substrait.
    SubstraitDeserialize(String),
    /// Planning failed.
    Planning(PlanningError),
    /// DuckDB connection error.
    DuckDb(String),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::SubstraitConversion(msg) => {
                write!(f, "Substrait conversion failed: {}", msg)
            }
            PipelineError::SubstraitDeserialize(msg) => {
                write!(f, "Substrait deserialization failed: {}", msg)
            }
            PipelineError::Planning(err) => write!(f, "Planning failed: {}", err),
            PipelineError::DuckDb(msg) => write!(f, "DuckDB error: {}", msg),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<PlanningError> for PipelineError {
    fn from(err: PlanningError) -> Self {
        PipelineError::Planning(err)
    }
}

/// Main entry point for LDP planning.
///
/// Converts a SQL query into a Leibrix Distributed Plan (LDP) ready for execution.
///
/// # Pipeline Steps
/// 1. Use DuckDB to compile SQL → Substrait
/// 2. Deserialize Substrait into Rel tree
/// 3. Annotate with distribution properties and insert exchanges
/// 4. Cut into stages at exchange boundaries
/// 5. Return LdpPlan ready for execution
///
/// # Arguments
/// * `sql` - The SQL query to plan
/// * `conn` - DuckDB connection (for SQL → Substrait conversion)
/// * `metadata` - Access to epoch statistics and placement
/// * `policy` - Planner thresholds and configuration
/// * `query_id` - Unique identifier for this query
///
/// # Returns
/// * `Ok(LdpPlan)` - Successfully generated distributed plan
/// * `Err(PipelineError)` - Planning failed
///
/// # Example
/// ```ignore
/// let conn = Connection::open_in_memory()?;
/// let metadata = InMemoryMetadata::new();
/// let policy = PlannerPolicy::default();
///
/// let plan = plan_ldp(
///     "SELECT country, SUM(revenue) FROM sales GROUP BY country",
///     &conn,
///     &metadata,
///     &policy,
///     "query_001"
/// )?;
/// ```
pub fn plan_ldp<M: Metadata>(
    sql: &str,
    conn: &Connection,
    metadata: &M,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> Result<LdpPlan, PipelineError> {
    let query_id = query_id.into();

    // Step 1: SQL → Substrait via DuckDB
    let substrait_bytes = duckdb_get_substrait(conn, sql)
        .map_err(|e| PipelineError::SubstraitConversion(e.to_string()))?;

    // Step 2: Deserialize Substrait Plan
    let plan = deserialize_substrait_plan(&substrait_bytes)?;

    // Extract the root Rel from the Plan
    let root_rel = extract_root_rel(&plan)?;

    // Step 3: Annotate and enforce distribution requirements
    let annotated = annotate_and_enforce(&root_rel, policy, metadata)?;

    // Step 4: Cut into stages at exchange boundaries
    let ldp_plan = cut_into_stages(&annotated, policy, &query_id);

    Ok(ldp_plan)
}

/// Plan from pre-compiled Substrait bytes (for testing or when Substrait is already available).
///
/// Skips the SQL → Substrait step and directly processes Substrait bytes.
pub fn plan_ldp_from_substrait<M: Metadata>(
    substrait_bytes: &[u8],
    metadata: &M,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> Result<LdpPlan, PipelineError> {
    let query_id = query_id.into();

    // Deserialize Substrait Plan
    let plan = deserialize_substrait_plan(substrait_bytes)?;

    // Extract the root Rel
    let root_rel = extract_root_rel(&plan)?;

    // Annotate and enforce
    let annotated = annotate_and_enforce(&root_rel, policy, metadata)?;

    // Cut into stages
    let ldp_plan = cut_into_stages(&annotated, policy, &query_id);

    Ok(ldp_plan)
}

/// Plan from a Substrait Rel directly (for testing).
pub fn plan_ldp_from_rel<M: Metadata>(
    rel: &substrait::proto::Rel,
    metadata: &M,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> Result<LdpPlan, PipelineError> {
    let query_id = query_id.into();

    // Annotate and enforce
    let annotated = annotate_and_enforce(rel, policy, metadata)?;

    // Cut into stages
    let ldp_plan = cut_into_stages(&annotated, policy, &query_id);

    Ok(ldp_plan)
}

/// Deserialize a Substrait Plan from bytes.
fn deserialize_substrait_plan(bytes: &[u8]) -> Result<Plan, PipelineError> {
    use prost::Message;

    Plan::decode(bytes).map_err(|e| PipelineError::SubstraitDeserialize(e.to_string()))
}

/// Extract the root Rel from a Substrait Plan.
fn extract_root_rel(plan: &Plan) -> Result<substrait::proto::Rel, PipelineError> {
    // A Substrait Plan contains relations in plan.relations
    // Each relation can be a Root or a Rel
    // We need to find the main query relation

    for relation in &plan.relations {
        if let Some(rel_type) = &relation.rel_type {
            match rel_type {
                substrait::proto::plan_rel::RelType::Root(root) => {
                    if let Some(input) = &root.input {
                        return Ok(input.clone());
                    }
                }
                substrait::proto::plan_rel::RelType::Rel(rel) => {
                    return Ok(rel.clone());
                }
            }
        }
    }

    Err(PipelineError::SubstraitDeserialize(
        "No root relation found in Substrait plan".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::planner::metadata::InMemoryMetadata;
    use crate::ldp::planner::policy::PlannerPolicy;

    #[test]
    fn test_plan_ldp_from_rel() {
        // Create a simple ReadRel for testing
        use crate::ldp::substrait::reconstruct::create_read_rel;

        let read_rel = create_read_rel("test_table", None);
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::default();

        let result = plan_ldp_from_rel(&read_rel, &metadata, &policy, "test_query");

        // Should succeed for a simple read
        assert!(result.is_ok());

        let plan = result.unwrap();
        assert_eq!(plan.query_id, "test_query");
        assert!(!plan.stages.is_empty());
    }
}
