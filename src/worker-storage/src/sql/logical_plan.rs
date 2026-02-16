//! Logical plan types for LDP distribution planning and stage SQL generation.
//!
//! This module defines the relational algebra tree that replaces Substrait in the
//! LDP pipeline. Each [`LogicalPlan`] node carries sqlparser AST expressions for two
//! purposes:
//!
//! - **Distribution planning**: `group_keys`, `left_keys`, `right_keys`, variant type
//!   (used by `annotate.rs`, `requirements.rs`)
//! - **SQL generation**: `predicate`, `items`, `order_by`, `group_by`, `join_op`, `limit`
//!   (used by `stage_sql_gen.rs`)
//!
//! These concerns don't interfere — the planner ignores AST expressions; the SQL
//! generator ignores distribution keys.
//!
//! # Architecture Context
//!
//! LDP follows the SQL-delegating distributed system pattern (Citus, Vitess, PolarDB-X):
//! the planning layer decides data movement; DuckDB on each node handles optimization
//! and execution. This module provides the intermediate representation between SQL
//! parsing and distributed plan annotation.
//!
//! See `docs/ldp_substrait_removal_design.md` §5 for the full design rationale.

use std::fmt;

use sqlparser::ast::{
    Cte, Expr, GroupByExpr, JoinOperator, OrderByExpr, SelectItem, SetOperator, SetQuantifier,
    TableAlias, TableFactor,
};

// ---------------------------------------------------------------------------
// LogicalPlan
// ---------------------------------------------------------------------------

/// Relational algebra tree for LDP distribution planning and stage SQL generation.
///
/// Standard structure from distributed database literature.
/// Each node carries sqlparser AST expressions for SQL regeneration.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Leaf: table scan.
    ///
    /// `table_name` is the resolved logical name (for metadata lookup).
    /// `table_factor` is the original AST node (for SQL regeneration — handles
    /// macro calls, aliases, etc.).
    Scan {
        table_name: String,
        alias: Option<TableAlias>,
        /// Original `TableFactor` for SQL regeneration.
        table_factor: TableFactor,
    },

    /// σ (selection). Distribution-preserving.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    /// π (projection). Distribution-preserving.
    Project {
        input: Box<LogicalPlan>,
        items: Vec<SelectItem>,
    },

    /// γ (aggregation).
    ///
    /// `aggr_exprs` carries the full SELECT list for SQL generation (aggregate
    /// functions + group-by columns). `group_keys` are extracted separately for
    /// distribution planning.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: GroupByExpr,
        aggr_exprs: Vec<SelectItem>,
        having: Option<Expr>,
        /// Extracted group-by columns for distribution planning.
        group_keys: Vec<ColumnRef>,
    },

    /// τ (sort). Requires Singleton distribution.
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },

    /// LIMIT / OFFSET. Requires Singleton distribution.
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },

    /// ⋈ (join).
    ///
    /// `join_op` carries the original join type + ON/USING clause for SQL
    /// regeneration. `left_keys` / `right_keys` are extracted equi-join columns
    /// for distribution planning. Empty keys (non-equi join or unrecognized
    /// expression) → conservative Singleton requirement.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        /// Original join operator (carries join type + constraint).
        join_op: JoinOperator,
        /// Extracted equi-join keys for distribution planning (left side).
        left_keys: Vec<ColumnRef>,
        /// Extracted equi-join keys for distribution planning (right side).
        right_keys: Vec<ColumnRef>,
    },

    /// ∪ / ∩ / ∖ (set operations).
    SetOp {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        op: SetOperator,
        /// Set quantifier (ALL, DISTINCT, etc.) — preserved from sqlparser for
        /// accurate SQL round-tripping.
        set_quantifier: SetQuantifier,
    },

    /// Window functions. Conservative: requires Singleton distribution.
    ///
    /// Carries the full SELECT list (both window and non-window expressions).
    /// Exists solely to signal distribution requirements to the planner; SQL
    /// generation treats it like a `Project` emitting all items in a single
    /// `SELECT` clause.
    Window {
        input: Box<LogicalPlan>,
        window_exprs: Vec<SelectItem>,
    },

    /// Subquery scan (derived table).
    SubqueryScan {
        input: Box<LogicalPlan>,
        alias: TableAlias,
    },

    /// Exchange placeholder inserted during stage cutting.
    ///
    /// Replaces the upstream plan fragment. The downstream stage reads from a
    /// temp table named `alias` (e.g., `"__exchange_0"`).
    ExchangeRead {
        exchange_id: u32,
        /// Temp table name, e.g., `"__exchange_0"`.
        alias: String,
    },
}

impl LogicalPlan {
    /// Returns references to the immediate children of this plan node.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. } | LogicalPlan::ExchangeRead { .. } => vec![],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::SubqueryScan { input, .. } => vec![input.as_ref()],
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SetOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        }
    }

    /// Returns mutable references to the immediate children of this plan node.
    pub fn children_mut(&mut self) -> Vec<&mut LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. } | LogicalPlan::ExchangeRead { .. } => vec![],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::SubqueryScan { input, .. } => vec![input.as_mut()],
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SetOp { left, right, .. } => vec![left.as_mut(), right.as_mut()],
        }
    }

    /// Returns a human-readable name for this plan node (for debugging/logging).
    pub fn node_name(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::SetOp { .. } => "SetOp",
            LogicalPlan::Window { .. } => "Window",
            LogicalPlan::SubqueryScan { .. } => "SubqueryScan",
            LogicalPlan::ExchangeRead { .. } => "ExchangeRead",
        }
    }

    /// Returns `true` if this is a leaf node (no children).
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            LogicalPlan::Scan { .. } | LogicalPlan::ExchangeRead { .. }
        )
    }
}

// ---------------------------------------------------------------------------
// ColumnRef
// ---------------------------------------------------------------------------

/// Column reference for distribution keys (join keys, group keys).
///
/// Named-only — no positional indices. This avoids the need for schema
/// propagation through every operator (which Substrait's positional
/// `field_ref: u32` required).
///
/// # Qualifier Stripping at Exchange Boundaries
///
/// When stage cutting inserts an exchange, the downstream stage reads from
/// `__exchange_N` — the original table qualifiers are meaningless. The stage
/// cutter strips table qualifiers, leaving only column names. This is safe
/// because column names within a single stage's exchange output are unique
/// by construction (the SQL generator controls the upstream SELECT list).
///
/// See `docs/ldp_substrait_removal_design.md` §5.2.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Optional table/alias qualifier (e.g., `"o"` in `o.id`).
    pub table: Option<String>,
    /// Column name (e.g., `"id"` in `o.id`).
    pub column: String,
}

impl ColumnRef {
    /// Create a column reference without a table qualifier.
    pub fn unqualified(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Create a column reference with a table qualifier.
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }

    /// Create a `ColumnRef` with the table qualifier stripped.
    ///
    /// Used at exchange boundaries where the original table qualifier is
    /// meaningless (see §5.2).
    pub fn stripped(&self) -> Self {
        Self {
            table: None,
            column: self.column.clone(),
        }
    }

    /// Case-insensitive matching.
    ///
    /// If either reference lacks a table qualifier, matches on column name only.
    /// This is the correct behavior because after exchange boundaries, table
    /// qualifiers are stripped.
    pub fn matches(&self, other: &ColumnRef) -> bool {
        let col_eq = self.column.eq_ignore_ascii_case(&other.column);
        match (&self.table, &other.table) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b) && col_eq,
            _ => col_eq,
        }
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.table {
            Some(t) => write!(f, "{}.{}", t, self.column),
            None => write!(f, "{}", self.column),
        }
    }
}

// ---------------------------------------------------------------------------
// PlanContext
// ---------------------------------------------------------------------------

/// Context carried alongside a [`LogicalPlan`] during planning.
///
/// Contains information that doesn't belong in the plan tree itself but is
/// needed for SQL generation (e.g., CTE definitions from the original query's
/// `WITH` clause).
///
/// See `docs/ldp_substrait_removal_design.md` §6.5.
#[derive(Clone, Debug, Default)]
pub struct PlanContext {
    /// CTE definitions from the original query's `WITH` clause.
    ///
    /// Stored as sqlparser `Cte` nodes for direct SQL regeneration. During
    /// stage SQL generation, CTEs are inlined into the stage SQL that
    /// references them. If multiple stages reference the same CTE, the
    /// definition is duplicated — correct because DuckDB re-evaluates
    /// non-materialized CTEs. The admission controller already rejects
    /// recursive CTEs.
    pub cte_definitions: Vec<Cte>,
}

impl PlanContext {
    /// Create an empty context (no CTEs).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a context with CTE definitions.
    pub fn with_ctes(cte_definitions: Vec<Cte>) -> Self {
        Self { cte_definitions }
    }

    /// Returns `true` if there are no CTE definitions.
    pub fn is_empty(&self) -> bool {
        self.cte_definitions.is_empty()
    }
}

// ---------------------------------------------------------------------------
// PlanBuildError
// ---------------------------------------------------------------------------

/// Errors that can occur during SQL AST → [`LogicalPlan`] conversion.
#[derive(Debug, Clone)]
pub enum PlanBuildError {
    /// The SQL statement type is not supported (only SELECT queries are).
    UnsupportedStatement(String),
    /// Unsupported SQL feature encountered during plan building.
    UnsupportedFeature(String),
    /// Internal error (should not happen).
    Internal(String),
}

impl fmt::Display for PlanBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanBuildError::UnsupportedStatement(s) => {
                write!(f, "unsupported statement: {}", s)
            }
            PlanBuildError::UnsupportedFeature(s) => {
                write!(f, "unsupported SQL feature: {}", s)
            }
            PlanBuildError::Internal(s) => {
                write!(f, "internal plan build error: {}", s)
            }
        }
    }
}

impl std::error::Error for PlanBuildError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    fn make_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            alias: None,
            table_factor: TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
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
    fn test_leaf_children_empty() {
        let scan = make_scan("orders");
        assert!(scan.is_leaf());
        assert!(scan.children().is_empty());
    }

    #[test]
    fn test_filter_has_one_child() {
        let plan = LogicalPlan::Filter {
            input: Box::new(make_scan("orders")),
            predicate: Expr::Value(sqlparser::ast::Value::Boolean(true).into()),
        };
        assert!(!plan.is_leaf());
        assert_eq!(plan.children().len(), 1);
        assert_eq!(plan.node_name(), "Filter");
    }

    #[test]
    fn test_join_has_two_children() {
        let join = LogicalPlan::Join {
            left: Box::new(make_scan("orders")),
            right: Box::new(make_scan("lineitem")),
            join_op: JoinOperator::Inner(sqlparser::ast::JoinConstraint::Natural),
            left_keys: vec![],
            right_keys: vec![],
        };
        assert_eq!(join.children().len(), 2);
        assert_eq!(join.node_name(), "Join");
    }

    #[test]
    fn test_column_ref_matches() {
        let qualified = ColumnRef::qualified("o", "id");
        let unqualified = ColumnRef::unqualified("id");
        let other_qual = ColumnRef::qualified("o", "ID");
        let diff_table = ColumnRef::qualified("l", "id");

        // Qualified matches qualified (same table, case-insensitive)
        assert!(qualified.matches(&other_qual));
        // Unqualified matches anything with same column name
        assert!(unqualified.matches(&qualified));
        assert!(qualified.matches(&unqualified));
        // Different table qualifier → no match
        assert!(!qualified.matches(&diff_table));
        // Unqualified matches different table qualifier (match on column only)
        assert!(unqualified.matches(&diff_table));
    }

    #[test]
    fn test_column_ref_stripped() {
        let cr = ColumnRef::qualified("orders", "id");
        let stripped = cr.stripped();
        assert_eq!(stripped.table, None);
        assert_eq!(stripped.column, "id");
    }

    #[test]
    fn test_column_ref_display() {
        assert_eq!(ColumnRef::qualified("o", "id").to_string(), "o.id");
        assert_eq!(ColumnRef::unqualified("id").to_string(), "id");
    }

    #[test]
    fn test_plan_context_default_empty() {
        let ctx = PlanContext::new();
        assert!(ctx.is_empty());
        assert!(ctx.cte_definitions.is_empty());
    }

    #[test]
    fn test_plan_build_error_display() {
        let err = PlanBuildError::UnsupportedStatement("INSERT".into());
        assert!(err.to_string().contains("INSERT"));

        let err = PlanBuildError::UnsupportedFeature("LATERAL JOIN".into());
        assert!(err.to_string().contains("LATERAL JOIN"));
    }

    #[test]
    fn test_exchange_read_is_leaf() {
        let ex = LogicalPlan::ExchangeRead {
            exchange_id: 0,
            alias: "__exchange_0".to_string(),
        };
        assert!(ex.is_leaf());
        assert_eq!(ex.node_name(), "ExchangeRead");
    }

    #[test]
    fn test_set_op_has_two_children() {
        let setop = LogicalPlan::SetOp {
            left: Box::new(make_scan("t1")),
            right: Box::new(make_scan("t2")),
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
        };
        assert_eq!(setop.children().len(), 2);
    }
}
