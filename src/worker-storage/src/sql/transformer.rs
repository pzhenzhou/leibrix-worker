//! SQL transformation: rewrites queries to use table macros.
//!
//! This module implements Phase 4 of the SQL rewriting algorithm.
//! It replaces logical table references with macro calls while
//! preserving the original predicates for semantic parity.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use sqlparser::ast::*;

use super::admission::{AdmissionController, AdmissionError};
use super::analyzer::PredicateAnalyzer;
use super::discovery::TableDiscovery;
use super::error::{SqlTransformError, TransformError};
use super::parser::{parse_sql, to_sql};
use super::types::*;

/// SQL transformer that rewrites queries to use table macros.
///
/// Transforms client queries from logical dataset names to macro calls
/// with date range parameters, enabling epoch pruning while maintaining
/// query semantics.
///
/// # Example
///
/// ```ignore
/// let mut transformer = SqlTransformer::new();
/// transformer.register_dataset(RegisteredDataset::new("sales".into(), "dt".into()));
///
/// let result = transformer.transform("SELECT * FROM sales WHERE dt = '2025-01-01'")?;
/// // result.transformed_sql: "SELECT * FROM scan_sales(DATE '2025-01-01', DATE '2025-01-02') WHERE dt = '2025-01-01'"
/// ```
pub struct SqlTransformer {
    /// Registered logical datasets
    registered_datasets: HashMap<String, RegisteredDataset>,
}

impl SqlTransformer {
    /// Create a new SQL transformer.
    pub fn new() -> Self {
        Self {
            registered_datasets: HashMap::new(),
        }
    }

    /// Register a logical dataset for transformation.
    pub fn register_dataset(&mut self, dataset: RegisteredDataset) {
        self.registered_datasets
            .insert(dataset.dataset_id.clone(), dataset);
    }

    /// Unregister a logical dataset.
    pub fn unregister_dataset(&mut self, dataset_id: &str) {
        self.registered_datasets.remove(dataset_id);
    }

    /// Check if a dataset is registered.
    pub fn is_registered(&self, dataset_id: &str) -> bool {
        self.registered_datasets.contains_key(dataset_id)
    }

    /// Get registered dataset names.
    pub fn registered_dataset_ids(&self) -> HashSet<String> {
        self.registered_datasets.keys().cloned().collect()
    }

    /// Create an admission controller with the same registered datasets.
    ///
    /// This is useful if you want to perform admission checks separately
    /// from transformation.
    pub fn create_admission_controller(&self) -> AdmissionController {
        let mut controller = AdmissionController::new();
        for dataset in self.registered_datasets.values() {
            controller.register_dataset(dataset.clone());
        }
        controller
    }

    /// Check if a query passes admission control.
    ///
    /// This performs structural checks to ensure the query is suitable
    /// for distributed execution:
    /// - Rejects recursive CTEs
    /// - Requires time-range predicates on distributed tables
    ///
    /// # Arguments
    /// * `sql` - The SQL query to check.
    ///
    /// # Returns
    /// * `Ok(())` - Query is admitted for execution.
    /// * `Err(AdmissionError)` - Query is rejected with reason.
    pub fn admit(&self, sql: &str) -> Result<(), AdmissionError> {
        let controller = self.create_admission_controller();
        controller.check(sql)
    }

    /// Perform admission control and transformation in one step.
    ///
    /// This is the recommended method for production use as it ensures
    /// queries are validated before transformation.
    ///
    /// # Process
    /// 1. Run admission checks (recursive CTE, time-range requirements)
    /// 2. If admitted, transform the query to use table macros
    ///
    /// # Arguments
    /// * `sql` - The SQL query to process.
    ///
    /// # Returns
    /// * `Ok(TransformResult)` - Query admitted and transformed.
    /// * `Err(TransformError)` - Admission failed or transformation failed.
    pub fn admit_and_transform(&self, sql: &str) -> Result<TransformResult, TransformError> {
        // Step 1: Admission control
        self.admit(sql).map_err(TransformError::Admission)?;

        // Step 2: Transform
        self.transform(sql).map_err(TransformError::Transform)
    }

    /// Transform a SQL query to use table macros.
    ///
    /// # Process
    /// 1. Parse SQL into AST
    /// 2. Discover logical table references
    /// 3. If no logical tables found, return original SQL unchanged
    /// 4. Extract date range predicates for each table
    /// 5. Rewrite AST to replace tables with macro calls
    /// 6. Regenerate SQL from modified AST
    ///
    /// # Important
    /// Original WHERE predicates are preserved for semantic parity.
    /// The macro parameters act as "hints" for epoch pruning.
    pub fn transform(&self, sql: &str) -> Result<TransformResult, SqlTransformError> {
        // Phase 1: Parse
        let mut stmt = parse_sql(sql)?;

        // Phase 2: Discover logical tables
        let registered_ids = self.registered_dataset_ids();
        let tables = TableDiscovery::new(registered_ids).discover(&stmt)?;

        // Fast path: no logical tables found
        if tables.is_empty() {
            return Ok(TransformResult {
                original_sql: sql.to_string(),
                transformed_sql: sql.to_string(),
                tables_replaced: vec![],
                date_ranges: HashMap::new(),
            });
        }

        // Phase 3: Extract date ranges
        let time_columns: HashMap<String, String> = self
            .registered_datasets
            .iter()
            .map(|(id, ds)| (id.clone(), ds.time_column_name.clone()))
            .collect();
        let analyzer = PredicateAnalyzer::new(time_columns);
        let date_ranges = analyzer.extract_date_ranges(&stmt, &tables)?;

        // Build replacement map: dataset_id -> (macro_name, date_range)
        let replacements: HashMap<String, (&RegisteredDataset, DateRange)> = tables
            .iter()
            .filter_map(|t| {
                let dataset = self.registered_datasets.get(&t.dataset_id)?;
                let range = date_ranges.get(&t.dataset_id)?.clone();
                Some((t.dataset_id.clone(), (dataset, range)))
            })
            .collect();

        // Phase 4: Transform AST
        self.rewrite_statement(&mut stmt, &replacements)?;

        // Generate transformed SQL
        let transformed_sql = to_sql(&stmt);

        // Collect replaced table names
        let tables_replaced: Vec<String> = tables.iter().map(|t| t.dataset_id.clone()).collect();

        Ok(TransformResult {
            original_sql: sql.to_string(),
            transformed_sql,
            tables_replaced,
            date_ranges,
        })
    }

    /// Rewrite a statement in place.
    fn rewrite_statement(
        &self,
        stmt: &mut Statement,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        match stmt {
            Statement::Query(query) => self.rewrite_query(query, replacements),
            _ => Err(SqlTransformError::UnsupportedStatement(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Rewrite a query in place.
    fn rewrite_query(
        &self,
        query: &mut Query,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        // Rewrite CTEs
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.rewrite_query(&mut cte.query, replacements)?;
            }
        }

        // Rewrite main query body
        self.rewrite_set_expr(&mut query.body, replacements)?;

        Ok(())
    }

    /// Rewrite a set expression in place.
    fn rewrite_set_expr(
        &self,
        expr: &mut SetExpr,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        match expr {
            SetExpr::Select(select) => {
                self.rewrite_select(select, replacements)?;
            }
            SetExpr::Query(query) => {
                self.rewrite_query(query, replacements)?;
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left, replacements)?;
                self.rewrite_set_expr(right, replacements)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Rewrite a SELECT in place.
    fn rewrite_select(
        &self,
        select: &mut Select,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        // Rewrite FROM clause
        for twj in &mut select.from {
            self.rewrite_table_with_joins(twj, replacements)?;
        }

        // Rewrite subqueries in WHERE
        if let Some(selection) = &mut select.selection {
            self.rewrite_expr_subqueries(selection, replacements)?;
        }

        // Rewrite subqueries in SELECT list
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.rewrite_expr_subqueries(expr, replacements)?;
                }
                _ => {}
            }
        }

        // Rewrite subqueries in HAVING
        if let Some(having) = &mut select.having {
            self.rewrite_expr_subqueries(having, replacements)?;
        }

        Ok(())
    }

    /// Rewrite a table with joins in place.
    fn rewrite_table_with_joins(
        &self,
        twj: &mut TableWithJoins,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        self.rewrite_table_factor(&mut twj.relation, replacements)?;

        for join in &mut twj.joins {
            self.rewrite_table_factor(&mut join.relation, replacements)?;

            // Rewrite subqueries in JOIN ON
            if let Some(expr) = super::extract_join_on_expr_mut(&mut join.join_operator) {
                self.rewrite_expr_subqueries(expr, replacements)?;
            }
        }

        Ok(())
    }

    /// Rewrite a table factor in place.
    fn rewrite_table_factor(
        &self,
        factor: &mut TableFactor,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = extract_table_name(name);

                if let Some((dataset, range)) = replacements.get(&table_name) {
                    // Create macro call to replace table
                    let new_factor = self.create_macro_call(dataset, range, alias.clone());
                    *factor = new_factor;
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.rewrite_query(subquery, replacements)?;
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.rewrite_table_with_joins(table_with_joins, replacements)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Rewrite subqueries in an expression.
    fn rewrite_expr_subqueries(
        &self,
        expr: &mut Expr,
        replacements: &HashMap<String, (&RegisteredDataset, DateRange)>,
    ) -> Result<(), SqlTransformError> {
        match expr {
            Expr::Subquery(query) => {
                self.rewrite_query(query, replacements)?;
            }
            Expr::InSubquery { subquery, .. } => {
                self.rewrite_query(subquery, replacements)?;
            }
            Expr::Exists { subquery, .. } => {
                self.rewrite_query(subquery, replacements)?;
            }
            Expr::BinaryOp { left, right, .. } => {
                self.rewrite_expr_subqueries(left, replacements)?;
                self.rewrite_expr_subqueries(right, replacements)?;
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.rewrite_expr_subqueries(inner, replacements)?;
            }
            Expr::Nested(inner) => {
                self.rewrite_expr_subqueries(inner, replacements)?;
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                self.rewrite_expr_subqueries(inner, replacements)?;
                self.rewrite_expr_subqueries(low, replacements)?;
                self.rewrite_expr_subqueries(high, replacements)?;
            }
            Expr::Case {
                operand,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.rewrite_expr_subqueries(op, replacements)?;
                }
                // Note: CaseWhen fields are not mutable in sqlparser's AST.
                // Subqueries in CASE conditions/results are rare; skip for now.
                // If needed, we would need to rebuild the entire Case expression.
                if let Some(el) = else_result {
                    self.rewrite_expr_subqueries(el, replacements)?;
                }
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(arg_list) = &mut func.args {
                    for arg in &mut arg_list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.rewrite_expr_subqueries(e, replacements)?;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Create a macro call table factor.
    fn create_macro_call(
        &self,
        dataset: &RegisteredDataset,
        range: &DateRange,
        original_alias: Option<TableAlias>,
    ) -> TableFactor {
        // Determine date arguments
        let (start_arg, end_arg) = self.format_date_args(range);

        // Create function arguments
        let args = vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(start_arg)),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(end_arg)),
        ];

        // Create table function factor
        TableFactor::Function {
            lateral: false,
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                &dataset.macro_name,
            ))]),
            args,
            alias: original_alias,
        }
    }

    /// Format date range as SQL expressions for macro arguments.
    fn format_date_args(&self, range: &DateRange) -> (Expr, Expr) {
        // Start date
        let start_expr = match &range.start {
            Some(DateBound::Literal(s)) => create_date_literal(s),
            Some(DateBound::Parameter(p)) => create_placeholder(p),
            None => create_date_literal(WIDE_SCAN_START),
        };

        // End date (convert to exclusive if was inclusive)
        let end_expr = match &range.end {
            Some(DateBound::Literal(s)) => {
                if range.end_inclusive {
                    // Add one day for exclusive end
                    create_date_literal(&add_one_day(s))
                } else {
                    create_date_literal(s)
                }
            }
            Some(DateBound::Parameter(p)) => create_placeholder(p),
            None => create_date_literal(WIDE_SCAN_END),
        };

        (start_expr, end_expr)
    }
}

impl Default for SqlTransformer {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract table name from ObjectName.
fn extract_table_name(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// Create a DATE literal expression (DATE '...' format).
fn create_date_literal(date_str: &str) -> Expr {
    Expr::TypedString(TypedString {
        data_type: DataType::Date,
        value: Value::SingleQuotedString(date_str.to_string()).into(),
        uses_odbc_syntax: false,
    })
}

/// Create a placeholder expression.
fn create_placeholder(p: &str) -> Expr {
    Expr::Value(Value::Placeholder(p.to_string()).into())
}

/// Add one day to a date string using chrono for robust date arithmetic.
/// Assumes YYYY-MM-DD format. Falls back to original string on parse error.
fn add_one_day(date_str: &str) -> String {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.succ_opt())
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| date_str.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_transformer(datasets: &[(&str, &str)]) -> SqlTransformer {
        let mut transformer = SqlTransformer::new();
        for (id, time_col) in datasets {
            transformer
                .register_dataset(RegisteredDataset::new(id.to_string(), time_col.to_string()));
        }
        transformer
    }

    #[test]
    fn test_transform_simple_select() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt = '2025-01-15'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("DATE '2025-01-15'"));
        // Original predicate should be preserved
        assert!(result.transformed_sql.contains("WHERE"));
        assert_eq!(result.tables_replaced, vec!["sales_data"]);
    }

    #[test]
    fn test_transform_between() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("DATE '2025-01-01'"));
        // End should be exclusive: 2025-01-31 + 1 day = 2025-02-01
        assert!(result.transformed_sql.contains("DATE '2025-02-01'"));
    }

    #[test]
    fn test_transform_with_alias() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT s.* FROM sales_data AS s WHERE s.dt >= '2025-01-01'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("AS s"));
    }

    #[test]
    fn test_transform_no_predicate() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT COUNT(*) FROM sales_data";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        // Should use wide scan bounds
        assert!(result.transformed_sql.contains("DATE '1970-01-01'"));
        assert!(result.transformed_sql.contains("DATE '9999-12-31'"));
    }

    #[test]
    fn test_transform_join() {
        let transformer =
            create_transformer(&[("sales_data", "dt"), ("customer_data", "created_at")]);
        let sql = "SELECT s.*, c.name FROM sales_data s JOIN customer_data c ON s.cid = c.id \
                   WHERE s.dt >= '2025-01-01' AND c.created_at < '2025-06-01'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("scan_customer_data"));
        assert_eq!(result.tables_replaced.len(), 2);
    }

    #[test]
    fn test_transform_cte() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "WITH recent AS (SELECT * FROM sales_data WHERE dt > '2025-01-01') \
                   SELECT * FROM recent";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
    }

    #[test]
    fn test_transform_subquery() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM (SELECT * FROM sales_data WHERE dt = '2025-01-15') AS sub";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
    }

    #[test]
    fn test_transform_unregistered_table() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM other_table";

        let result = transformer.transform(sql).unwrap();

        // No transformation should occur
        assert_eq!(result.original_sql, result.transformed_sql);
        assert!(result.tables_replaced.is_empty());
    }

    #[test]
    fn test_transform_mixed_tables() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT s.*, o.* FROM sales_data s JOIN other_table o ON s.id = o.id \
                   WHERE s.dt = '2025-01-15'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("other_table")); // unchanged
        assert_eq!(result.tables_replaced, vec!["sales_data"]);
    }

    #[test]
    fn test_transform_union() {
        let transformer = create_transformer(&[("sales_data", "dt"), ("archive_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt = '2025-01-15' \
                   UNION ALL SELECT * FROM archive_data WHERE dt = '2024-01-15'";

        let result = transformer.transform(sql).unwrap();

        assert!(result.transformed_sql.contains("scan_sales_data"));
        assert!(result.transformed_sql.contains("scan_archive_data"));
    }

    #[test]
    fn test_add_one_day() {
        assert_eq!(add_one_day("2025-01-15"), "2025-01-16");
        assert_eq!(add_one_day("2025-01-31"), "2025-02-01");
        assert_eq!(add_one_day("2025-12-31"), "2026-01-01");
        assert_eq!(add_one_day("2024-02-28"), "2024-02-29"); // leap year
        assert_eq!(add_one_day("2024-02-29"), "2024-03-01"); // leap year
        assert_eq!(add_one_day("2025-02-28"), "2025-03-01"); // non-leap year
    }

    #[test]
    fn test_semantic_parity_preserves_where() {
        let transformer = create_transformer(&[("sales_data", "dt")]);
        let sql = "SELECT * FROM sales_data WHERE dt = '2025-01-15' AND country = 'JP'";

        let result = transformer.transform(sql).unwrap();

        // Both original predicates should be preserved
        assert!(result.transformed_sql.contains("dt = '2025-01-15'"));
        assert!(result.transformed_sql.contains("country = 'JP'"));
    }
}
