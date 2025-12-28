//! Predicate analysis: extracts date ranges from SQL WHERE clauses.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use sqlparser::ast::*;

use super::error::SqlTransformError;
use super::types::{DateBound, DateRange, TableReference};

/// Analyzes SQL predicates to extract date ranges for logical tables.
///
/// Looks for predicates on the time column of each logical table and
/// extracts the date range bounds for use in macro parameters.
pub struct PredicateAnalyzer {
    /// Map of dataset_id -> time_column_name
    time_columns: HashMap<String, String>,
}

impl PredicateAnalyzer {
    /// Create a new predicate analyzer.
    pub fn new(time_columns: HashMap<String, String>) -> Self {
        Self { time_columns }
    }

    /// Extract date ranges for all discovered tables.
    ///
    /// Analyzes the WHERE clause and JOIN conditions to find date
    /// predicates that apply to each table's time column.
    pub fn extract_date_ranges(
        &self,
        stmt: &Statement,
        tables: &[TableReference],
    ) -> Result<HashMap<String, DateRange>, SqlTransformError> {
        let mut ranges: HashMap<String, DateRange> = HashMap::new();

        // Initialize empty ranges for all tables
        for table in tables {
            ranges.insert(table.dataset_id.clone(), DateRange::default());
        }

        // Extract from the statement
        if let Statement::Query(query) = stmt {
            self.extract_from_query(query, tables, &mut ranges)?;
        }

        Ok(ranges)
    }

    /// Extract from a Query (handles CTEs and body).
    fn extract_from_query(
        &self,
        query: &Query,
        tables: &[TableReference],
        ranges: &mut HashMap<String, DateRange>,
    ) -> Result<(), SqlTransformError> {
        // Handle CTEs
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.extract_from_query(&cte.query, tables, ranges)?;
            }
        }

        // Handle main query body
        self.extract_from_set_expr(&query.body, tables, ranges)?;

        Ok(())
    }

    /// Extract from a SetExpr.
    fn extract_from_set_expr(
        &self,
        expr: &SetExpr,
        tables: &[TableReference],
        ranges: &mut HashMap<String, DateRange>,
    ) -> Result<(), SqlTransformError> {
        match expr {
            SetExpr::Select(select) => {
                self.extract_from_select(select, tables, ranges)?;
            }
            SetExpr::Query(query) => {
                self.extract_from_query(query, tables, ranges)?;
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.extract_from_set_expr(left, tables, ranges)?;
                self.extract_from_set_expr(right, tables, ranges)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Extract from a SELECT statement.
    ///
    /// Only extracts predicates for tables that are actually referenced
    /// in this SELECT's FROM clause (scope-aware extraction).
    fn extract_from_select(
        &self,
        select: &Select,
        tables: &[TableReference],
        ranges: &mut HashMap<String, DateRange>,
    ) -> Result<(), SqlTransformError> {
        // Collect table names/aliases that are actually in THIS SELECT's FROM clause
        let local_table_refs = self.collect_local_table_refs(select);
        
        // Filter to only tables that appear in this SELECT's FROM clause
        let scope_tables: Vec<_> = tables
            .iter()
            .filter(|t| {
                local_table_refs.contains(&t.dataset_id.to_lowercase())
                    || t.alias
                        .as_ref()
                        .map(|a| local_table_refs.contains(&a.to_lowercase()))
                        .unwrap_or(false)
            })
            .collect();

        // Extract from WHERE clause - only for tables in current scope
        if let Some(selection) = &select.selection {
            for table in &scope_tables {
                if let Some(time_col) = self.time_columns.get(&table.dataset_id) {
                    let range = self.extract_range_from_expr(
                        selection,
                        table.effective_name(),
                        time_col,
                    );
                    if !range.is_empty() {
                        if let Some(existing) = ranges.get_mut(&table.dataset_id) {
                            existing.merge(range);
                        }
                    }
                }
            }
        }

        // Extract from JOIN ON clauses - only for tables in current scope
        for twj in &select.from {
            for join in &twj.joins {
                if let Some(on_expr) = super::extract_join_on_expr(&join.join_operator) {
                    for table in &scope_tables {
                        if let Some(time_col) = self.time_columns.get(&table.dataset_id) {
                            let range = self.extract_range_from_expr(
                                on_expr,
                                table.effective_name(),
                                time_col,
                            );
                            if !range.is_empty() {
                                if let Some(existing) = ranges.get_mut(&table.dataset_id) {
                                    existing.merge(range);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Collect all table names and aliases from a SELECT's FROM clause.
    fn collect_local_table_refs(&self, select: &Select) -> HashSet<String> {
        let mut refs = HashSet::new();
        for twj in &select.from {
            self.collect_table_refs_from_factor(&twj.relation, &mut refs);
            for join in &twj.joins {
                self.collect_table_refs_from_factor(&join.relation, &mut refs);
            }
        }
        refs
    }

    /// Recursively collect table names from a TableFactor.
    fn collect_table_refs_from_factor(&self, factor: &TableFactor, refs: &mut HashSet<String>) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                // Add table name
                if let Some(last) = name.0.last() {
                    if let ObjectNamePart::Identifier(ident) = last {
                        refs.insert(ident.value.to_lowercase());
                    }
                }
                // Add alias if present
                if let Some(a) = alias {
                    refs.insert(a.name.value.to_lowercase());
                }
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.collect_table_refs_from_factor(&table_with_joins.relation, refs);
                for join in &table_with_joins.joins {
                    self.collect_table_refs_from_factor(&join.relation, refs);
                }
            }
            // Derived tables (subqueries) don't expose their inner tables to outer scope
            TableFactor::Derived { alias, .. } => {
                if let Some(a) = alias {
                    refs.insert(a.name.value.to_lowercase());
                }
            }
            _ => {}
        }
    }

    /// Extract date range from an expression.
    fn extract_range_from_expr(
        &self,
        expr: &Expr,
        table_name: &str,
        time_col: &str,
    ) -> DateRange {
        let mut range = DateRange::default();
        self.walk_expr(expr, table_name, time_col, &mut range);
        range
    }

    /// Recursively walk an expression looking for date predicates.
    fn walk_expr(
        &self,
        expr: &Expr,
        table_name: &str,
        time_col: &str,
        range: &mut DateRange,
    ) {
        match expr {
            // BETWEEN: dt BETWEEN '2025-01-01' AND '2025-12-31'
            Expr::Between {
                expr: col_expr,
                low,
                high,
                negated: false,
            } => {
                if self.is_time_column_ref(col_expr, table_name, time_col) {
                    if let Some(start) = self.extract_date_bound(low) {
                        range.start = Some(start);
                    }
                    if let Some(end) = self.extract_date_bound(high) {
                        range.end = Some(end);
                        range.end_inclusive = true; // BETWEEN is inclusive
                    }
                }
            }

            // Binary comparison: dt >= '2025-01-01', dt < '2025-12-31', etc.
            Expr::BinaryOp { left, op, right } => {
                // Handle AND: recurse into both sides
                if *op == BinaryOperator::And {
                    self.walk_expr(left, table_name, time_col, range);
                    self.walk_expr(right, table_name, time_col, range);
                    return;
                }

                // Check if left side is time column
                if self.is_time_column_ref(left, table_name, time_col) {
                    match op {
                        BinaryOperator::Gt | BinaryOperator::GtEq => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                range.start = Some(bound);
                            }
                        }
                        BinaryOperator::Lt => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                range.end = Some(bound);
                                range.end_inclusive = false;
                            }
                        }
                        BinaryOperator::LtEq => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                range.end = Some(bound);
                                range.end_inclusive = true;
                            }
                        }
                        BinaryOperator::Eq => {
                            // Single date equality: dt = '2025-01-01'
                            if let Some(bound) = self.extract_date_bound(right) {
                                range.start = Some(bound.clone());
                                range.end = Some(bound);
                                range.end_inclusive = true;
                            }
                        }
                        _ => {}
                    }
                }
                // Check if right side is time column (reversed comparison)
                else if self.is_time_column_ref(right, table_name, time_col) {
                    match op {
                        BinaryOperator::Lt | BinaryOperator::LtEq => {
                            // '2025-01-01' < dt  =>  dt > '2025-01-01'
                            if let Some(bound) = self.extract_date_bound(left) {
                                range.start = Some(bound);
                            }
                        }
                        BinaryOperator::Gt => {
                            // '2025-12-31' > dt  =>  dt < '2025-12-31'
                            if let Some(bound) = self.extract_date_bound(left) {
                                range.end = Some(bound);
                                range.end_inclusive = false;
                            }
                        }
                        BinaryOperator::GtEq => {
                            // '2025-12-31' >= dt  =>  dt <= '2025-12-31'
                            if let Some(bound) = self.extract_date_bound(left) {
                                range.end = Some(bound);
                                range.end_inclusive = true;
                            }
                        }
                        BinaryOperator::Eq => {
                            if let Some(bound) = self.extract_date_bound(left) {
                                range.start = Some(bound.clone());
                                range.end = Some(bound);
                                range.end_inclusive = true;
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Nested expression: (dt > '2025-01-01')
            Expr::Nested(inner) => {
                self.walk_expr(inner, table_name, time_col, range);
            }

            _ => {}
        }
    }

    /// Check if an expression references the time column.
    fn is_time_column_ref(&self, expr: &Expr, table_name: &str, time_col: &str) -> bool {
        match expr {
            // Simple identifier: dt
            Expr::Identifier(ident) => {
                ident.value.eq_ignore_ascii_case(time_col)
            }
            // Compound identifier: s.dt or schema.table.dt
            Expr::CompoundIdentifier(parts) => {
                if parts.len() >= 2 {
                    let col_name = &parts.last().unwrap().value;
                    let qualifier = &parts[parts.len() - 2].value;
                    col_name.eq_ignore_ascii_case(time_col) 
                        && qualifier.eq_ignore_ascii_case(table_name)
                } else if parts.len() == 1 {
                    parts[0].value.eq_ignore_ascii_case(time_col)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Extract a date bound from a value expression.
    /// Only extracts if the value looks like a valid date (YYYY-MM-DD format).
    fn extract_date_bound(&self, expr: &Expr) -> Option<DateBound> {
        match expr {
            // String literal: '2025-01-01'
            Expr::Value(val) => {
                match &val.value {
                    Value::SingleQuotedString(s) => {
                        if is_valid_date_format(s) {
                            Some(DateBound::Literal(s.clone()))
                        } else {
                            None // Invalid date format, don't extract
                        }
                    }
                    Value::Placeholder(p) => Some(DateBound::Parameter(p.clone())),
                    _ => None,
                }
            }
            // Typed string: DATE '2025-01-01'
            Expr::TypedString(ts) => {
                if matches!(ts.data_type, DataType::Date) {
                    // Extract string value from the typed string
                    match &ts.value.value {
                        Value::SingleQuotedString(s) => {
                            if is_valid_date_format(s) {
                                Some(DateBound::Literal(s.clone()))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            // Cast expression: CAST('2025-01-01' AS DATE)
            Expr::Cast { expr: inner, data_type, .. } => {
                if matches!(data_type, DataType::Date) {
                    self.extract_date_bound(inner)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Check if a string looks like a valid YYYY-MM-DD date format.
fn is_valid_date_format(s: &str) -> bool {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_sql;
    use crate::sql::discovery::TableDiscovery;
    use std::collections::HashSet;

    fn setup_analyzer(datasets: &[(&str, &str)]) -> PredicateAnalyzer {
        let time_columns: HashMap<String, String> = datasets
            .iter()
            .map(|(d, t)| (d.to_string(), t.to_string()))
            .collect();
        PredicateAnalyzer::new(time_columns)
    }

    fn discover_tables(sql: &str, datasets: &[&str]) -> (Statement, Vec<TableReference>) {
        let stmt = parse_sql(sql).unwrap();
        let registered: HashSet<String> = datasets.iter().map(|s| s.to_string()).collect();
        let tables = TableDiscovery::new(registered).discover(&stmt).unwrap();
        (stmt, tables)
    }

    #[test]
    fn test_extract_between() {
        let sql = "SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-12-31'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
        assert_eq!(range.end, Some(DateBound::Literal("2025-12-31".to_string())));
        assert!(range.end_inclusive);
    }

    #[test]
    fn test_extract_comparison_operators() {
        let sql = "SELECT * FROM sales_data WHERE dt >= '2025-01-01' AND dt < '2025-02-01'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
        assert_eq!(range.end, Some(DateBound::Literal("2025-02-01".to_string())));
        assert!(!range.end_inclusive);
    }

    #[test]
    fn test_extract_equality() {
        let sql = "SELECT * FROM sales_data WHERE dt = '2025-01-15'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-15".to_string())));
        assert_eq!(range.end, Some(DateBound::Literal("2025-01-15".to_string())));
        assert!(range.end_inclusive);
    }

    #[test]
    fn test_extract_with_alias() {
        let sql = "SELECT * FROM sales_data s WHERE s.dt >= '2025-01-01'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
    }

    #[test]
    fn test_extract_multiple_tables() {
        let sql = "SELECT * FROM sales_data s JOIN customer_data c ON s.cid = c.id \
                   WHERE s.dt >= '2025-01-01' AND c.created_at < '2025-06-01'";
        let (stmt, tables) = discover_tables(sql, &["sales_data", "customer_data"]);
        let analyzer = setup_analyzer(&[
            ("sales_data", "dt"),
            ("customer_data", "created_at"),
        ]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let sales_range = ranges.get("sales_data").unwrap();
        assert_eq!(sales_range.start, Some(DateBound::Literal("2025-01-01".to_string())));

        let customer_range = ranges.get("customer_data").unwrap();
        assert_eq!(customer_range.end, Some(DateBound::Literal("2025-06-01".to_string())));
    }

    #[test]
    fn test_extract_no_predicate() {
        let sql = "SELECT * FROM sales_data";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert!(range.is_empty());
    }

    #[test]
    fn test_extract_date_typed_string() {
        let sql = "SELECT * FROM sales_data WHERE dt = DATE '2025-01-15'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-15".to_string())));
    }

    #[test]
    fn test_extract_reversed_comparison() {
        let sql = "SELECT * FROM sales_data WHERE '2025-01-01' <= dt";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
    }

    #[test]
    fn test_extract_nested_expression() {
        let sql = "SELECT * FROM sales_data WHERE (dt >= '2025-01-01') AND (dt < '2025-02-01')";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
        assert_eq!(range.end, Some(DateBound::Literal("2025-02-01".to_string())));
    }

    #[test]
    fn test_scope_aware_cte_extraction() {
        // Outer query predicate should NOT apply to inner table
        let sql = "WITH recent AS (SELECT * FROM sales_data) SELECT * FROM recent WHERE dt = '2025-01-15'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        // sales_data is in the CTE, but the predicate dt = '2025-01-15' is in outer query
        // referencing 'recent', not 'sales_data'. Should NOT be extracted.
        let range = ranges.get("sales_data").unwrap();
        assert!(range.is_empty(), "Outer query predicate should not apply to CTE inner table");
    }

    #[test]
    fn test_scope_aware_cte_with_inner_predicate() {
        // Inner predicate should be extracted correctly
        let sql = "WITH recent AS (SELECT * FROM sales_data WHERE dt = '2025-01-01') SELECT * FROM recent WHERE dt = '2025-01-15'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        // sales_data has predicate in CTE, should extract '2025-01-01', not '2025-01-15'
        let range = ranges.get("sales_data").unwrap();
        assert_eq!(range.start, Some(DateBound::Literal("2025-01-01".to_string())));
    }

    #[test]
    fn test_invalid_date_format_ignored() {
        // Invalid date format should not be extracted
        let sql = "SELECT * FROM sales_data WHERE dt = 'not-a-date'";
        let (stmt, tables) = discover_tables(sql, &["sales_data"]);
        let analyzer = setup_analyzer(&[("sales_data", "dt")]);

        let ranges = analyzer.extract_date_ranges(&stmt, &tables).unwrap();

        // Invalid date should result in empty range (wide scan)
        let range = ranges.get("sales_data").unwrap();
        assert!(range.is_empty(), "Invalid date format should not be extracted");
    }
}
