//! Admission control for SQL queries.
//!
//! This module implements structural admission checks that must pass
//! before a query is allowed to execute. These checks are deterministic
//! and do not depend on statistics.
//!
//! # Admission Rules
//!
//! 1. **Recursive CTE Rejection**: Recursive CTEs (`WITH RECURSIVE`) are not supported
//!    in distributed execution and are rejected.
//!
//! 2. **Time Range Requirement**: Queries against distributed tables must include
//!    a time-range predicate to bound the scan. Queries without time bounds would
//!    scan all epochs across all workers, which is unbounded.
//!
//! # Design Principles
//!
//! - **Structural checks only**: No statistics are used for admission decisions.
//! - **Deterministic**: Same query always gets same admission result.
//! - **Fail-fast**: Reject early before expensive planning/execution.

use std::collections::HashSet;

use sqlparser::ast::*;

use super::error::SqlTransformError;
use super::parser::parse_sql;
use super::types::RegisteredDataset;

/// Errors that cause query rejection during admission control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    /// Query contains a recursive CTE which is not supported.
    RecursiveCte {
        /// Name of the recursive CTE.
        cte_name: String,
    },

    /// Query against a distributed table lacks a time-range predicate.
    MissingTimeRange {
        /// Name of the table missing time bounds.
        table_name: String,
        /// The time column that should have a predicate.
        time_column: String,
    },

    /// Failed to parse the SQL query.
    ParseError(String),

    /// Query statement type is not supported.
    UnsupportedStatement(String),
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdmissionError::RecursiveCte { cte_name } => {
                write!(
                    f,
                    "Recursive CTEs are not supported in distributed execution: WITH RECURSIVE {}",
                    cte_name
                )
            }
            AdmissionError::MissingTimeRange {
                table_name,
                time_column,
            } => {
                write!(
                    f,
                    "Query on distributed table '{}' requires a time-range predicate on column '{}'. \
                     Add a WHERE clause like: {} BETWEEN '...' AND '...'",
                    table_name, time_column, time_column
                )
            }
            AdmissionError::ParseError(msg) => {
                write!(f, "Failed to parse SQL: {}", msg)
            }
            AdmissionError::UnsupportedStatement(msg) => {
                write!(f, "Unsupported statement: {}", msg)
            }
        }
    }
}

impl std::error::Error for AdmissionError {}

impl From<SqlTransformError> for AdmissionError {
    fn from(err: SqlTransformError) -> Self {
        match err {
            SqlTransformError::ParseError(msg) => AdmissionError::ParseError(msg),
            SqlTransformError::UnsupportedStatement(msg) => AdmissionError::UnsupportedStatement(msg),
            other => AdmissionError::ParseError(other.to_string()),
        }
    }
}

/// Result type for admission control.
pub type AdmissionResult<T> = Result<T, AdmissionError>;

/// Admission controller for SQL queries.
///
/// Performs structural checks on SQL queries to ensure they are suitable
/// for distributed execution.
///
/// # Example
///
/// ```rust
/// use worker_storage::sql::{AdmissionController, RegisteredDataset};
///
/// let mut controller = AdmissionController::new();
/// controller.register_dataset(RegisteredDataset::new(
///     "sales_data".to_string(),
///     "dt".to_string(),
/// ));
///
/// // This will pass - has time range
/// let result = controller.check(
///     "SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'"
/// );
/// assert!(result.is_ok());
///
/// // This will fail - recursive CTE
/// let result = controller.check(
///     "WITH RECURSIVE cte AS (SELECT 1 UNION SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"
/// );
/// assert!(result.is_err());
/// ```
#[derive(Default)]
pub struct AdmissionController {
    /// Registered distributed datasets that require time-range predicates.
    registered_datasets: std::collections::HashMap<String, RegisteredDataset>,
}

impl AdmissionController {
    /// Create a new admission controller.
    pub fn new() -> Self {
        Self {
            registered_datasets: std::collections::HashMap::new(),
        }
    }

    /// Register a distributed dataset that requires time-range predicates.
    pub fn register_dataset(&mut self, dataset: RegisteredDataset) {
        self.registered_datasets
            .insert(dataset.dataset_id.clone(), dataset);
    }

    /// Unregister a dataset.
    pub fn unregister_dataset(&mut self, dataset_id: &str) {
        self.registered_datasets.remove(dataset_id);
    }

    /// Get registered dataset IDs.
    pub fn registered_dataset_ids(&self) -> HashSet<String> {
        self.registered_datasets.keys().cloned().collect()
    }

    /// Check if a query passes admission control.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to check.
    ///
    /// # Returns
    /// * `Ok(())` - Query is admitted for execution.
    /// * `Err(AdmissionError)` - Query is rejected with reason.
    pub fn check(&self, sql: &str) -> AdmissionResult<()> {
        // Parse SQL
        let stmt = parse_sql(sql)?;

        // Check for recursive CTE
        self.check_recursive_cte(&stmt)?;

        // Check for time-range predicates on distributed tables
        self.check_time_range_predicates(&stmt)?;

        Ok(())
    }

    /// Check if the statement contains a recursive CTE.
    fn check_recursive_cte(&self, stmt: &Statement) -> AdmissionResult<()> {
        match stmt {
            Statement::Query(query) => self.check_query_for_recursive_cte(query),
            _ => Ok(()), // Non-query statements pass this check
        }
    }

    /// Recursively check a query for recursive CTEs.
    fn check_query_for_recursive_cte(&self, query: &Query) -> AdmissionResult<()> {
        // Check WITH clause
        if let Some(with) = &query.with {
            if with.recursive {
                // Find the first CTE name for error message
                let cte_name = with
                    .cte_tables
                    .first()
                    .map(|cte| cte.alias.name.value.clone())
                    .unwrap_or_else(|| "unknown".to_string());

                return Err(AdmissionError::RecursiveCte { cte_name });
            }

            // Also check nested queries within CTEs
            for cte in &with.cte_tables {
                self.check_query_for_recursive_cte(&cte.query)?;
            }
        }

        // Check the body
        self.check_set_expr_for_recursive_cte(&query.body)?;

        Ok(())
    }

    /// Check a set expression for recursive CTEs.
    fn check_set_expr_for_recursive_cte(&self, expr: &SetExpr) -> AdmissionResult<()> {
        match expr {
            SetExpr::Select(select) => self.check_select_for_recursive_cte(select),
            SetExpr::Query(query) => self.check_query_for_recursive_cte(query),
            SetExpr::SetOperation { left, right, .. } => {
                self.check_set_expr_for_recursive_cte(left)?;
                self.check_set_expr_for_recursive_cte(right)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Check a select for recursive CTEs in subqueries.
    fn check_select_for_recursive_cte(&self, select: &Select) -> AdmissionResult<()> {
        // Check FROM clause for subqueries
        for twj in &select.from {
            self.check_table_with_joins_for_recursive_cte(twj)?;
        }

        // Check WHERE clause for subqueries
        if let Some(selection) = &select.selection {
            self.check_expr_for_recursive_cte(selection)?;
        }

        Ok(())
    }

    /// Check table with joins for recursive CTEs.
    fn check_table_with_joins_for_recursive_cte(
        &self,
        twj: &TableWithJoins,
    ) -> AdmissionResult<()> {
        self.check_table_factor_for_recursive_cte(&twj.relation)?;

        for join in &twj.joins {
            self.check_table_factor_for_recursive_cte(&join.relation)?;
        }

        Ok(())
    }

    /// Check table factor for recursive CTEs.
    fn check_table_factor_for_recursive_cte(&self, factor: &TableFactor) -> AdmissionResult<()> {
        match factor {
            TableFactor::Derived { subquery, .. } => {
                self.check_query_for_recursive_cte(subquery)?;
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.check_table_with_joins_for_recursive_cte(table_with_joins)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Check an expression for recursive CTEs in subqueries.
    fn check_expr_for_recursive_cte(&self, expr: &Expr) -> AdmissionResult<()> {
        match expr {
            Expr::Subquery(query) => self.check_query_for_recursive_cte(query),
            Expr::InSubquery { subquery, .. } => self.check_query_for_recursive_cte(subquery),
            Expr::Exists { subquery, .. } => self.check_query_for_recursive_cte(subquery),
            Expr::BinaryOp { left, right, .. } => {
                self.check_expr_for_recursive_cte(left)?;
                self.check_expr_for_recursive_cte(right)?;
                Ok(())
            }
            Expr::UnaryOp { expr: inner, .. } => self.check_expr_for_recursive_cte(inner),
            Expr::Nested(inner) => self.check_expr_for_recursive_cte(inner),
            _ => Ok(()),
        }
    }

    /// Check that distributed tables have time-range predicates.
    fn check_time_range_predicates(&self, stmt: &Statement) -> AdmissionResult<()> {
        match stmt {
            Statement::Query(query) => self.check_query_time_ranges(query),
            _ => Err(AdmissionError::UnsupportedStatement(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Check a query for time-range predicates on distributed tables.
    fn check_query_time_ranges(&self, query: &Query) -> AdmissionResult<()> {
        // Collect all table references
        let tables_in_query = self.collect_table_references(query);

        // Find distributed tables that are referenced
        let distributed_tables: Vec<&RegisteredDataset> = tables_in_query
            .iter()
            .filter_map(|name| self.registered_datasets.get(name))
            .collect();

        // If no distributed tables, pass
        if distributed_tables.is_empty() {
            return Ok(());
        }

        // Collect all predicates
        let predicates = self.collect_predicates(query);

        // Check each distributed table has a time predicate
        for dataset in distributed_tables {
            if !self.has_time_predicate(&predicates, &dataset.time_column_name) {
                return Err(AdmissionError::MissingTimeRange {
                    table_name: dataset.dataset_id.clone(),
                    time_column: dataset.time_column_name.clone(),
                });
            }
        }

        Ok(())
    }

    /// Collect all table names referenced in a query.
    fn collect_table_references(&self, query: &Query) -> HashSet<String> {
        let mut tables = HashSet::new();

        // Collect from CTEs
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                tables.extend(self.collect_table_references(&cte.query));
            }
        }

        // Collect from body
        self.collect_tables_from_set_expr(&query.body, &mut tables);

        tables
    }

    /// Collect tables from a set expression.
    fn collect_tables_from_set_expr(&self, expr: &SetExpr, tables: &mut HashSet<String>) {
        match expr {
            SetExpr::Select(select) => {
                self.collect_tables_from_select(select, tables);
            }
            SetExpr::Query(query) => {
                tables.extend(self.collect_table_references(query));
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.collect_tables_from_set_expr(left, tables);
                self.collect_tables_from_set_expr(right, tables);
            }
            _ => {}
        }
    }

    /// Collect tables from a select statement.
    fn collect_tables_from_select(&self, select: &Select, tables: &mut HashSet<String>) {
        for twj in &select.from {
            self.collect_tables_from_table_with_joins(twj, tables);
        }
    }

    /// Collect tables from table with joins.
    fn collect_tables_from_table_with_joins(
        &self,
        twj: &TableWithJoins,
        tables: &mut HashSet<String>,
    ) {
        self.collect_tables_from_table_factor(&twj.relation, tables);

        for join in &twj.joins {
            self.collect_tables_from_table_factor(&join.relation, tables);
        }
    }

    /// Collect tables from table factor.
    fn collect_tables_from_table_factor(&self, factor: &TableFactor, tables: &mut HashSet<String>) {
        match factor {
            TableFactor::Table { name, .. } => {
                if let Some(table_name) = extract_table_name(name) {
                    tables.insert(table_name);
                }
            }
            TableFactor::Derived { subquery, .. } => {
                tables.extend(self.collect_table_references(subquery));
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.collect_tables_from_table_with_joins(table_with_joins, tables);
            }
            _ => {}
        }
    }

    /// Collect all predicate expressions from a query.
    fn collect_predicates(&self, query: &Query) -> Vec<Expr> {
        let mut predicates = Vec::new();

        // Collect from CTEs
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                predicates.extend(self.collect_predicates(&cte.query));
            }
        }

        // Collect from body
        self.collect_predicates_from_set_expr(&query.body, &mut predicates);

        predicates
    }

    /// Collect predicates from set expression.
    fn collect_predicates_from_set_expr(&self, expr: &SetExpr, predicates: &mut Vec<Expr>) {
        match expr {
            SetExpr::Select(select) => {
                if let Some(selection) = &select.selection {
                    predicates.push(selection.clone());
                }

                // Also collect from joins
                for twj in &select.from {
                    self.collect_predicates_from_joins(twj, predicates);
                }
            }
            SetExpr::Query(query) => {
                predicates.extend(self.collect_predicates(query));
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.collect_predicates_from_set_expr(left, predicates);
                self.collect_predicates_from_set_expr(right, predicates);
            }
            _ => {}
        }
    }

    /// Collect predicates from join ON clauses.
    fn collect_predicates_from_joins(&self, twj: &TableWithJoins, predicates: &mut Vec<Expr>) {
        for join in &twj.joins {
            if let Some(expr) = super::extract_join_on_expr(&join.join_operator) {
                predicates.push(expr.clone());
            }
        }
    }

    /// Check if predicates contain a time predicate for the given column.
    fn has_time_predicate(&self, predicates: &[Expr], time_column: &str) -> bool {
        for pred in predicates {
            if self.expr_references_column(pred, time_column) {
                return true;
            }
        }
        false
    }

    /// Check if an expression references a column (potentially with comparison).
    fn expr_references_column(&self, expr: &Expr, column_name: &str) -> bool {
        match expr {
            // Column reference
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column_name),

            // Compound identifier like table.column
            Expr::CompoundIdentifier(parts) => {
                parts
                    .last()
                    .is_some_and(|p| p.value.eq_ignore_ascii_case(column_name))
            }

            // Binary comparison (col = x, col > x, etc.)
            Expr::BinaryOp { left, right, .. } => {
                self.expr_references_column(left, column_name)
                    || self.expr_references_column(right, column_name)
            }

            // BETWEEN expression
            Expr::Between { expr: inner, .. } => self.expr_references_column(inner, column_name),

            // IN expression
            Expr::InList { expr: inner, .. } => self.expr_references_column(inner, column_name),

            // Nested/parenthesized
            Expr::Nested(inner) => self.expr_references_column(inner, column_name),

            // Other cases don't reference the column directly
            _ => false,
        }
    }
}

/// Extract table name from ObjectName.
fn extract_table_name(name: &ObjectName) -> Option<String> {
    name.0.last().and_then(|part| match part {
        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_controller(datasets: &[(&str, &str)]) -> AdmissionController {
        let mut controller = AdmissionController::new();
        for (id, time_col) in datasets {
            controller.register_dataset(RegisteredDataset::new(
                id.to_string(),
                time_col.to_string(),
            ));
        }
        controller
    }

    // =========================================================================
    // Recursive CTE Tests
    // =========================================================================

    #[test]
    fn test_reject_recursive_cte() {
        let controller = create_controller(&[]);
        let sql = "WITH RECURSIVE cte AS (SELECT 1 AS n UNION SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte";

        let result = controller.check(sql);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, AdmissionError::RecursiveCte { .. }));
    }

    #[test]
    fn test_allow_non_recursive_cte() {
        let controller = create_controller(&[]);
        let sql = "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_nested_recursive_cte() {
        let controller = create_controller(&[]);
        let sql = "SELECT * FROM (WITH RECURSIVE cte AS (SELECT 1 UNION SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte) AS sub";

        let result = controller.check(sql);
        assert!(result.is_err());
    }

    // =========================================================================
    // Time Range Tests
    // =========================================================================

    #[test]
    fn test_allow_query_with_time_range() {
        let controller = create_controller(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt >= '2025-01-01' AND dt < '2025-02-01'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_allow_query_with_between() {
        let controller = create_controller(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_query_without_time_range() {
        let controller = create_controller(&[("sales_data", "dt")]);
        let sql = "SELECT COUNT(*) FROM sales_data";

        let result = controller.check(sql);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, AdmissionError::MissingTimeRange { .. }));
    }

    #[test]
    fn test_allow_unregistered_table_without_time_range() {
        let controller = create_controller(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM other_table";

        // other_table is not a registered distributed table, so no time range required
        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_join_missing_time_range() {
        let controller = create_controller(&[("sales_data", "dt"), ("customer_data", "created_at")]);
        let sql = "SELECT * FROM sales_data s JOIN customer_data c ON s.cid = c.id WHERE s.dt >= '2025-01-01'";

        // sales_data has time range, but customer_data does not
        let result = controller.check(sql);
        assert!(result.is_err());

        let err = result.unwrap_err();
        if let AdmissionError::MissingTimeRange { table_name, .. } = err {
            assert_eq!(table_name, "customer_data");
        } else {
            panic!("Expected MissingTimeRange error");
        }
    }

    #[test]
    fn test_allow_join_with_all_time_ranges() {
        let controller = create_controller(&[("sales_data", "dt"), ("customer_data", "created_at")]);
        let sql = "SELECT * FROM sales_data s JOIN customer_data c ON s.cid = c.id \
                   WHERE s.dt >= '2025-01-01' AND c.created_at < '2025-06-01'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_time_predicate_in_join_on_clause() {
        let controller = create_controller(&[("sales_data", "dt")]);
        // Time predicate in JOIN ON clause should also count
        let sql = "SELECT * FROM other_table o JOIN sales_data s ON o.id = s.id AND s.dt >= '2025-01-01'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    #[test]
    fn test_case_insensitive_column_match() {
        let controller = create_controller(&[("sales_data", "DT")]);
        let sql = "SELECT * FROM sales_data WHERE dt >= '2025-01-01'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_qualified_column_name() {
        let controller = create_controller(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data s WHERE s.dt >= '2025-01-01'";

        let result = controller.check(sql);
        assert!(result.is_ok());
    }
}
