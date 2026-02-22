//! Boolean expression analysis for date range extraction.
//!
//! This module implements relational algebra-aware predicate analysis.
//! It correctly handles:
//! - Per-table predicate isolation
//! - AND/OR boolean composition with interval algebra
//! - Cross-table predicates in JOINs
//! - Conservative extraction for semantic parity

use sqlparser::ast::*;
use std::collections::HashMap;

use super::interval::{DateBound, Interval};
use super::types::TableReference;

/// Analyzes Boolean expressions to extract date intervals per table.
///
/// Uses relational algebra principles:
/// - AND operations: predicates on same table → intersection
/// - OR operations: predicates on same table → union
/// - OR with cross-table predicates → forces wide scan (conservative)
pub struct BooleanExprAnalyzer {
    /// Map of dataset_id -> time_column_name
    time_columns: HashMap<String, String>,
}

impl BooleanExprAnalyzer {
    pub fn new(time_columns: HashMap<String, String>) -> Self {
        Self { time_columns }
    }

    /// Extract intervals for all tables from a Boolean expression.
    ///
    /// Returns a map of dataset_id -> Interval.
    pub fn extract_intervals(
        &self,
        expr: &Expr,
        tables: &[TableReference],
    ) -> HashMap<String, Interval> {
        let mut intervals = HashMap::new();

        for table in tables {
            let time_col = self.time_columns.get(&table.dataset_id);
            if time_col.is_none() {
                continue;
            }

            let interval = self.analyze_for_table(expr, table.effective_name(), time_col.unwrap());

            intervals.insert(table.dataset_id.clone(), interval);
        }

        intervals
    }

    /// Analyze a Boolean expression for a specific table.
    ///
    /// Returns an Interval that conservatively bounds the dates this table needs.
    /// Uses interval algebra to handle AND (∩) and OR (∪) correctly.
    fn analyze_for_table(&self, expr: &Expr, table_name: &str, time_col: &str) -> Interval {
        match expr {
            // AND: Intersection (tightening)
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let left_interval = self.analyze_for_table(left, table_name, time_col);
                let right_interval = self.analyze_for_table(right, table_name, time_col);

                // Key insight: If either side has no predicates for this table,
                // it returns Universe, and X ∩ Universe = X
                left_interval.intersect(&right_interval)
            }

            // OR: Union (widening)
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => {
                let left_interval = self.analyze_for_table(left, table_name, time_col);
                let right_interval = self.analyze_for_table(right, table_name, time_col);

                // Critical case: Cross-table OR
                // If one side has no predicates for THIS table (returns Universe),
                // then X ∪ Universe = Universe (forces wide scan)
                //
                // Example: WHERE sales.dt > '2025-01-01' OR customer.dt > '2025-06-01'
                // For sales table: [2025-01-01, +∞) ∪ Universe = Universe
                // This correctly identifies that we need ALL sales records.
                left_interval.union(&right_interval)
            }

            // BETWEEN
            Expr::Between {
                expr: col_expr,
                low,
                high,
                negated: false,
            } => {
                if self.is_time_column_ref(col_expr, table_name, time_col) {
                    if let (Some(start), Some(end)) =
                        (self.extract_date_bound(low), self.extract_date_bound(high))
                    {
                        // BETWEEN is inclusive, add one day to end
                        let exclusive_end = self.add_one_day(&end);
                        return Interval::from_bounds(Some(start), Some(exclusive_end));
                    }
                }
                Interval::universe() // Not applicable to this table
            }

            // Comparison operators
            Expr::BinaryOp { left, op, right } => {
                // Check if left side is our time column
                if self.is_time_column_ref(left, table_name, time_col) {
                    match op {
                        BinaryOperator::GtEq => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                return Interval::from_bounds(Some(bound), None);
                            }
                        }
                        BinaryOperator::Gt => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                // Exclusive lower bound: advance start by one day
                                let exclusive_start = self.add_one_day(&bound);
                                return Interval::from_bounds(Some(exclusive_start), None);
                            }
                        }
                        BinaryOperator::Lt => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                return Interval::from_bounds(None, Some(bound));
                            }
                        }
                        BinaryOperator::LtEq => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                // Convert <= to < by adding one day
                                let exclusive = self.add_one_day(&bound);
                                return Interval::from_bounds(None, Some(exclusive));
                            }
                        }
                        BinaryOperator::Eq => {
                            if let Some(bound) = self.extract_date_bound(right) {
                                return Interval::point(bound);
                            }
                        }
                        _ => {}
                    }
                }
                // Check if right side is our time column (reversed comparison)
                else if self.is_time_column_ref(right, table_name, time_col) {
                    match op {
                        BinaryOperator::LtEq => {
                            // '2025-01-01' <= dt  =>  dt >= '2025-01-01'
                            if let Some(bound) = self.extract_date_bound(left) {
                                return Interval::from_bounds(Some(bound), None);
                            }
                        }
                        BinaryOperator::Lt => {
                            // '2025-01-01' < dt  =>  dt > '2025-01-01'
                            if let Some(bound) = self.extract_date_bound(left) {
                                let exclusive_start = self.add_one_day(&bound);
                                return Interval::from_bounds(Some(exclusive_start), None);
                            }
                        }
                        BinaryOperator::Gt => {
                            // '2025-12-31' > dt  =>  dt < '2025-12-31'
                            if let Some(bound) = self.extract_date_bound(left) {
                                return Interval::from_bounds(None, Some(bound));
                            }
                        }
                        BinaryOperator::GtEq => {
                            // '2025-12-31' >= dt  =>  dt <= '2025-12-31'
                            if let Some(bound) = self.extract_date_bound(left) {
                                let exclusive = self.add_one_day(&bound);
                                return Interval::from_bounds(None, Some(exclusive));
                            }
                        }
                        BinaryOperator::Eq => {
                            if let Some(bound) = self.extract_date_bound(left) {
                                return Interval::point(bound);
                            }
                        }
                        _ => {}
                    }
                }

                Interval::universe() // Not applicable to this table
            }

            // Nested expressions
            Expr::Nested(inner) => self.analyze_for_table(inner, table_name, time_col),

            // All other expressions don't mention this table
            _ => Interval::universe(),
        }
    }

    /// Check if an expression references the time column of the target table.
    fn is_time_column_ref(&self, expr: &Expr, table_name: &str, time_col: &str) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(time_col),
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
    fn extract_date_bound(&self, expr: &Expr) -> Option<DateBound> {
        match expr {
            Expr::Value(val) => match &val.value {
                Value::SingleQuotedString(s) => {
                    if self.is_valid_date(s) {
                        Some(DateBound::Literal(s.clone()))
                    } else {
                        None
                    }
                }
                Value::Placeholder(p) => Some(DateBound::Parameter(p.clone())),
                _ => None,
            },
            Expr::TypedString(ts) => {
                if matches!(ts.data_type, DataType::Date) {
                    match &ts.value.value {
                        Value::SingleQuotedString(s) => {
                            if self.is_valid_date(s) {
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
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                if matches!(data_type, DataType::Date) {
                    self.extract_date_bound(inner)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Validate date format.
    fn is_valid_date(&self, s: &str) -> bool {
        use chrono::NaiveDate;
        NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
    }

    /// Add one day to a date bound (for inclusive → exclusive conversion).
    fn add_one_day(&self, bound: &DateBound) -> DateBound {
        match bound {
            DateBound::Literal(s) => {
                use chrono::NaiveDate;
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.succ_opt())
                    .map(|d| DateBound::Literal(d.format("%Y-%m-%d").to_string()))
                    .unwrap_or_else(|| bound.clone())
            }
            DateBound::Parameter(_) => bound.clone(), // Can't add to parameter
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_sql;
    use crate::sql::types::ScopeId;

    fn setup() -> BooleanExprAnalyzer {
        let mut time_columns = HashMap::new();
        time_columns.insert("sales_data".to_string(), "dt".to_string());
        time_columns.insert("customer_data".to_string(), "created_at".to_string());
        BooleanExprAnalyzer::new(time_columns)
    }

    fn make_table_ref(dataset_id: &str, alias: Option<&str>) -> TableReference {
        TableReference {
            dataset_id: dataset_id.to_string(),
            alias: alias.map(|s| s.to_string()),
            scope: ScopeId(0),
        }
    }

    fn extract_where_clause(sql: &str) -> Expr {
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => select.selection.unwrap(),
                _ => panic!("Expected SELECT"),
            },
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_simple_and() {
        let analyzer = setup();
        let expr =
            extract_where_clause("SELECT * FROM t WHERE dt >= '2025-01-01' AND dt < '2025-02-01'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-01".to_string())
        );
        assert_eq!(
            interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-02-01".to_string())
        );
    }

    #[test]
    fn test_and_tightening() {
        // WHERE dt >= '2025-01-01' AND dt < '2025-02-01' AND dt >= '2025-01-15'
        let analyzer = setup();
        let expr = extract_where_clause(
            "SELECT * FROM t WHERE dt >= '2025-01-01' AND dt < '2025-02-01' AND dt >= '2025-01-15'",
        );

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        // Should take max of starts: '2025-01-15'
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-15".to_string())
        );
        assert_eq!(
            interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-02-01".to_string())
        );
    }

    #[test]
    fn test_simple_or() {
        let analyzer = setup();
        let expr =
            extract_where_clause("SELECT * FROM t WHERE dt = '2025-01-01' OR dt = '2025-01-15'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        // Should union: ['2025-01-01', '2025-01-16') (covers both dates)
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-01".to_string())
        );
        assert_eq!(
            interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-01-16".to_string())
        );
    }

    #[test]
    fn test_cross_table_and() {
        // WHERE sales.dt >= '2025-01-01' AND customer.created_at < '2025-06-01'
        let analyzer = setup();
        let expr = extract_where_clause(
            "SELECT * FROM sales s, customer c WHERE s.dt >= '2025-01-01' AND c.created_at < '2025-06-01'",
        );

        let tables = vec![
            make_table_ref("sales_data", Some("s")),
            make_table_ref("customer_data", Some("c")),
        ];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        // Each table should extract only its own predicate
        let sales_interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            sales_interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-01".to_string())
        );
        assert!(sales_interval.end.is_none()); // Unbounded above

        let customer_interval = intervals.get("customer_data").unwrap();
        assert!(customer_interval.start.is_none()); // Unbounded below
        assert_eq!(
            customer_interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-06-01".to_string())
        );
    }

    #[test]
    fn test_cross_table_or() {
        // WHERE sales.dt = '2025-01-01' OR customer.created_at = '2025-06-01'
        // Critical test: OR across tables forces wide scan
        let analyzer = setup();
        let expr = extract_where_clause(
            "SELECT * FROM sales s, customer c WHERE s.dt = '2025-01-01' OR c.created_at = '2025-06-01'",
        );

        let tables = vec![
            make_table_ref("sales_data", Some("s")),
            make_table_ref("customer_data", Some("c")),
        ];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        // Both should be Universe (wide scan) because OR connects different tables
        let sales_interval = intervals.get("sales_data").unwrap();
        assert!(sales_interval.is_universe());

        let customer_interval = intervals.get("customer_data").unwrap();
        assert!(customer_interval.is_universe());
    }

    #[test]
    fn test_complex_boolean() {
        // WHERE (dt >= '2025-01-01' AND dt < '2025-01-31') OR (dt >= '2025-06-01' AND dt < '2025-06-30')
        let analyzer = setup();
        let expr = extract_where_clause(
            "SELECT * FROM t WHERE (dt >= '2025-01-01' AND dt < '2025-01-31') OR (dt >= '2025-06-01' AND dt < '2025-06-30')",
        );

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        // Union of [2025-01-01, 2025-01-31) and [2025-06-01, 2025-06-30)
        // Should be [2025-01-01, 2025-06-30) (covers both, plus gap in between)
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-01".to_string())
        );
        assert_eq!(
            interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-06-30".to_string())
        );
    }

    #[test]
    fn test_between() {
        let analyzer = setup();
        let expr =
            extract_where_clause("SELECT * FROM t WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        // BETWEEN is inclusive, should convert to [2025-01-01, 2025-02-01)
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-01".to_string())
        );
        assert_eq!(
            interval.end.as_ref().unwrap(),
            &DateBound::Literal("2025-02-01".to_string())
        );
    }

    #[test]
    fn test_no_predicate() {
        let analyzer = setup();
        let expr = extract_where_clause("SELECT * FROM t WHERE country = 'US'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert!(interval.is_universe()); // No date predicate → wide scan
    }

    #[test]
    fn test_contradictory_predicates() {
        // WHERE dt > '2025-12-31' AND dt < '2025-01-01'
        let analyzer = setup();
        let expr =
            extract_where_clause("SELECT * FROM t WHERE dt > '2025-12-31' AND dt < '2025-01-01'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert!(interval.is_empty()); // Contradiction → empty interval
    }

    #[test]
    fn test_gt_exclusive_lower_bound() {
        // WHERE dt > '2025-01-15'  →  lower bound should advance to 2025-01-16
        let analyzer = setup();
        let expr = extract_where_clause("SELECT * FROM t WHERE dt > '2025-01-15'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-16".to_string()),
            "Gt (exclusive) should advance start by one day"
        );
        assert!(interval.end.is_none());
    }

    #[test]
    fn test_gteq_inclusive_lower_bound() {
        // WHERE dt >= '2025-01-15'  →  lower bound should stay at 2025-01-15
        let analyzer = setup();
        let expr = extract_where_clause("SELECT * FROM t WHERE dt >= '2025-01-15'");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-15".to_string()),
            "GtEq (inclusive) should keep start unchanged"
        );
        assert!(interval.end.is_none());
    }

    #[test]
    fn test_reversed_lt_exclusive_lower_bound() {
        // WHERE '2025-01-15' < dt  =>  dt > '2025-01-15'  →  start should advance to 2025-01-16
        let analyzer = setup();
        let expr = extract_where_clause("SELECT * FROM t WHERE '2025-01-15' < dt");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-16".to_string()),
            "Reversed Lt (exclusive) should advance start by one day"
        );
    }

    #[test]
    fn test_reversed_lteq_inclusive_lower_bound() {
        // WHERE '2025-01-15' <= dt  =>  dt >= '2025-01-15'  →  start stays at 2025-01-15
        let analyzer = setup();
        let expr = extract_where_clause("SELECT * FROM t WHERE '2025-01-15' <= dt");

        let tables = vec![make_table_ref("sales_data", None)];
        let intervals = analyzer.extract_intervals(&expr, &tables);

        let interval = intervals.get("sales_data").unwrap();
        assert_eq!(
            interval.start.as_ref().unwrap(),
            &DateBound::Literal("2025-01-15".to_string()),
            "Reversed LtEq (inclusive) should keep start unchanged"
        );
    }
}
