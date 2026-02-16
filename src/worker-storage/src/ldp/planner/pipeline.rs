//! LDP Planning Pipeline.
//!
//! This module provides the main entry point for converting SQL queries
//! into Leibrix Distributed Plans (LDP).
//!
//! # Pipeline Overview
//! ```text
//! SQL → parse → LogicalPlan → annotate → cut → LdpPlan
//! ```

use crate::ldp::planner::annotate::{annotate_logical_plan, PlanningError};
use crate::ldp::planner::cut::cut_into_stages;
use crate::ldp::planner::metadata::Metadata;
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::LdpPlan;
use crate::sql::logical_plan::{LogicalPlan, PlanContext};
use crate::sql::{build_logical_plan, parse_sql, PlanBuildError, SqlTransformError};

/// Error during LDP pipeline execution.
#[derive(Debug)]
pub enum PipelineError {
    /// SQL parsing failed.
    Parse(SqlTransformError),
    /// Logical plan building failed.
    PlanBuild(PlanBuildError),
    /// Planning (annotation/enforcement) failed.
    Planning(PlanningError),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::Parse(err) => write!(f, "SQL parse failed: {}", err),
            PipelineError::PlanBuild(err) => write!(f, "Plan build failed: {}", err),
            PipelineError::Planning(err) => write!(f, "Planning failed: {}", err),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<SqlTransformError> for PipelineError {
    fn from(err: SqlTransformError) -> Self {
        PipelineError::Parse(err)
    }
}

impl From<PlanBuildError> for PipelineError {
    fn from(err: PlanBuildError) -> Self {
        PipelineError::PlanBuild(err)
    }
}

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
/// 1. Parse SQL string into AST Statement
/// 2. Build LogicalPlan from Statement
/// 3. Annotate with distribution properties and insert exchanges
/// 4. Cut into stages at exchange boundaries
/// 5. Return LdpPlan ready for execution
///
/// # Arguments
/// * `sql` - The SQL query to plan
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
/// let metadata = InMemoryMetadata::new();
/// let policy = PlannerPolicy::default();
///
/// let plan = plan_ldp(
///     "SELECT country, SUM(revenue) FROM sales GROUP BY country",
///     &metadata,
///     &policy,
///     "query_001"
/// )?;
/// ```
pub fn plan_ldp<M: Metadata>(
    sql: &str,
    metadata: &M,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> Result<LdpPlan, PipelineError> {
    let query_id = query_id.into();

    // Step 1: Parse SQL → Statement
    let stmt = parse_sql(sql)?;

    // Step 2: Build LogicalPlan from Statement
    let build_result = build_logical_plan(&stmt)?;
    let logical_plan = build_result.plan;
    let context = build_result.context;

    // Step 3: Annotate and enforce distribution requirements
    let annotated = annotate_logical_plan(&logical_plan, policy, metadata)?;

    // Step 4: Cut into stages at exchange boundaries
    let ldp_plan = cut_into_stages(&annotated, policy, &query_id, &context);

    Ok(ldp_plan)
}

/// Plan from a pre-built LogicalPlan (for testing or when plan is already available).
///
/// Skips the SQL parsing and plan building steps.
pub fn plan_ldp_from_logical_plan<M: Metadata>(
    logical_plan: &LogicalPlan,
    metadata: &M,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> Result<LdpPlan, PipelineError> {
    let query_id = query_id.into();

    // Annotate and enforce
    let annotated = annotate_logical_plan(logical_plan, policy, metadata)?;

    // Cut into stages (no CTE context for pre-built plans)
    let ldp_plan = cut_into_stages(&annotated, policy, &query_id, &PlanContext::default());

    Ok(ldp_plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::planner::metadata::InMemoryMetadata;
    use crate::ldp::planner::policy::PlannerPolicy;
    use crate::sql::logical_plan::LogicalPlan;
    use sqlparser::ast::TableFactor;

    fn create_simple_scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: "test_table".into(),
            alias: None,
            table_factor: TableFactor::Table {
                name: vec![sqlparser::ast::Ident::new("test_table")].into(),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
        }
    }

    #[test]
    fn test_plan_ldp_from_logical_plan() {
        let logical_plan = create_simple_scan();
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::with_coordinator("coordinator");

        let result = plan_ldp_from_logical_plan(&logical_plan, &metadata, &policy, "test_query");

        // Should succeed for a simple scan
        assert!(result.is_ok());

        let plan = result.unwrap();
        assert_eq!(plan.query_id, "test_query");
        assert!(!plan.stages.is_empty());
    }

    #[test]
    fn test_plan_ldp_simple_select() {
        let sql = "SELECT * FROM test_table";
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::with_coordinator("coordinator");

        let result = plan_ldp(sql, &metadata, &policy, "test_query");

        // Should succeed for simple SQL
        assert!(result.is_ok());

        let plan = result.unwrap();
        assert_eq!(plan.query_id, "test_query");
        assert!(!plan.stages.is_empty());
    }

    #[test]
    fn test_plan_ldp_invalid_sql() {
        let sql = "SELECT * FROM"; // Invalid SQL
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::with_coordinator("coordinator");

        let result = plan_ldp(sql, &metadata, &policy, "test_query");

        // Should fail with parse error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), PipelineError::Parse(_)));
    }

    #[test]
    fn test_plan_ldp_with_filter() {
        let sql = "SELECT * FROM test_table WHERE id > 100";
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::with_coordinator("coordinator");

        let result = plan_ldp(sql, &metadata, &policy, "test_query");

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert!(!plan.stages.is_empty());
    }

    #[test]
    fn test_plan_ldp_with_aggregation() {
        let sql = "SELECT country, COUNT(*) FROM test_table GROUP BY country";
        let metadata = InMemoryMetadata::new();
        let policy = PlannerPolicy::with_coordinator("coordinator");

        let result = plan_ldp(sql, &metadata, &policy, "test_query");

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert!(!plan.stages.is_empty());
    }
}
