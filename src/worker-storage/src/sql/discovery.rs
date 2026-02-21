//! Table discovery: traverses SQL AST to find logical table references.

use std::collections::HashSet;

use sqlparser::ast::*;

use super::error::SqlTransformError;
use super::types::{ScopeId, ScopeKind, TableReference};

/// Discovers logical table references in SQL AST.
///
/// Traverses the AST to find all table references that match registered
/// logical datasets. Tracks scope hierarchy for proper predicate matching.
pub struct TableDiscovery {
    /// Discovered table references
    tables: Vec<TableReference>,
    /// Scope stack for tracking nested queries
    scope_stack: Vec<ScopeId>,
    /// Current scope counter
    next_scope_id: usize,
    /// Registered dataset names to look for
    registered_datasets: HashSet<String>,
}

impl TableDiscovery {
    /// Create a new table discovery instance.
    pub fn new(registered_datasets: HashSet<String>) -> Self {
        Self {
            tables: Vec::new(),
            scope_stack: vec![ScopeId(0)], // Start with main query scope
            next_scope_id: 1,
            registered_datasets,
        }
    }

    /// Discover all logical table references in the statement.
    ///
    /// Returns a list of TableReference for each discovered logical table.
    pub fn discover(mut self, stmt: &Statement) -> Result<Vec<TableReference>, SqlTransformError> {
        self.visit_statement(stmt)?;
        Ok(self.tables)
    }

    /// Get current scope ID.
    fn current_scope(&self) -> ScopeId {
        *self.scope_stack.last().unwrap_or(&ScopeId(0))
    }

    /// Enter a new scope.
    fn enter_scope(&mut self, _kind: ScopeKind) -> ScopeId {
        let id = ScopeId(self.next_scope_id);
        self.next_scope_id += 1;
        self.scope_stack.push(id);
        id
    }

    /// Exit current scope.
    fn exit_scope(&mut self) {
        if self.scope_stack.len() > 1 {
            self.scope_stack.pop();
        }
    }

    /// Visit a statement.
    fn visit_statement(&mut self, stmt: &Statement) -> Result<(), SqlTransformError> {
        match stmt {
            Statement::Query(query) => self.visit_query(query),
            _ => Err(SqlTransformError::UnsupportedStatement(format!(
                "{:?}",
                std::mem::discriminant(stmt)
            ))),
        }
    }

    /// Visit a query (handles CTEs and body).
    fn visit_query(&mut self, query: &Query) -> Result<(), SqlTransformError> {
        // Handle CTEs (WITH clause) first
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.enter_scope(ScopeKind::Cte(cte.alias.name.value.clone()));
                self.visit_query(&cte.query)?;
                self.exit_scope();
            }
        }

        // Visit main query body
        self.visit_set_expr(&query.body)?;

        Ok(())
    }

    /// Visit a set expression (SELECT, UNION, etc.).
    fn visit_set_expr(&mut self, expr: &SetExpr) -> Result<(), SqlTransformError> {
        match expr {
            SetExpr::Select(select) => self.visit_select(select),
            SetExpr::Query(query) => {
                self.enter_scope(ScopeKind::Subquery);
                let result = self.visit_query(query);
                self.exit_scope();
                result
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(left)?;
                self.visit_set_expr(right)?;
                Ok(())
            }
            SetExpr::Values(_) => Ok(()), // VALUES clause, no tables
            SetExpr::Insert(_) => Err(SqlTransformError::UnsupportedStatement(
                "INSERT".to_string(),
            )),
            SetExpr::Update(_) => Err(SqlTransformError::UnsupportedStatement(
                "UPDATE".to_string(),
            )),
            SetExpr::Table(_) => Ok(()), // TABLE t syntax
            _ => Ok(()),                 // Handle any other variants
        }
    }

    /// Visit a SELECT statement.
    fn visit_select(&mut self, select: &Select) -> Result<(), SqlTransformError> {
        // Visit FROM clause
        for table_with_joins in &select.from {
            self.visit_table_with_joins(table_with_joins)?;
        }

        // Visit WHERE clause for subqueries
        if let Some(selection) = &select.selection {
            self.visit_expr_for_subqueries(selection)?;
        }

        // Visit SELECT list for subqueries
        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                self.visit_expr_for_subqueries(expr)?;
            }
        }

        // Visit HAVING clause for subqueries
        if let Some(having) = &select.having {
            self.visit_expr_for_subqueries(having)?;
        }

        Ok(())
    }

    /// Visit a table with its joins.
    fn visit_table_with_joins(&mut self, twj: &TableWithJoins) -> Result<(), SqlTransformError> {
        self.visit_table_factor(&twj.relation)?;

        for join in &twj.joins {
            self.visit_table_factor(&join.relation)?;

            // Check JOIN ON clause for subqueries
            if let Some(JoinConstraint::On(expr)) =
                super::extract_join_constraint(&join.join_operator)
            {
                self.visit_expr_for_subqueries(expr)?;
            }
        }

        Ok(())
    }

    /// Visit a table factor (table reference, subquery, or table function).
    fn visit_table_factor(&mut self, factor: &TableFactor) -> Result<(), SqlTransformError> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = extract_table_name(name);

                // Check if this is a registered logical dataset
                if self.registered_datasets.contains(&table_name) {
                    let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                    self.tables.push(TableReference {
                        dataset_id: table_name,
                        alias: alias_name,
                        scope: self.current_scope(),
                    });
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.enter_scope(ScopeKind::Subquery);
                self.visit_query(subquery)?;
                self.exit_scope();
            }
            TableFactor::TableFunction { .. } => {
                // Table functions like UNNEST - no logical tables
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.visit_table_with_joins(table_with_joins)?;
            }
            _ => {
                // Other table factors (PIVOT, UNPIVOT, etc.) - skip for now
            }
        }
        Ok(())
    }

    /// Visit an expression looking for subqueries.
    fn visit_expr_for_subqueries(&mut self, expr: &Expr) -> Result<(), SqlTransformError> {
        match expr {
            Expr::Subquery(query) => {
                self.enter_scope(ScopeKind::Subquery);
                self.visit_query(query)?;
                self.exit_scope();
            }
            Expr::InSubquery { subquery, .. } => {
                self.enter_scope(ScopeKind::Subquery);
                self.visit_query(subquery)?;
                self.exit_scope();
            }
            Expr::Exists { subquery, .. } => {
                self.enter_scope(ScopeKind::Subquery);
                self.visit_query(subquery)?;
                self.exit_scope();
            }
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr_for_subqueries(left)?;
                self.visit_expr_for_subqueries(right)?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.visit_expr_for_subqueries(expr)?;
            }
            Expr::Nested(inner) => {
                self.visit_expr_for_subqueries(inner)?;
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.visit_expr_for_subqueries(expr)?;
                self.visit_expr_for_subqueries(low)?;
                self.visit_expr_for_subqueries(high)?;
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.visit_expr_for_subqueries(op)?;
                }
                for case_when in conditions {
                    self.visit_expr_for_subqueries(&case_when.condition)?;
                    self.visit_expr_for_subqueries(&case_when.result)?;
                }
                if let Some(el) = else_result {
                    self.visit_expr_for_subqueries(el)?;
                }
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.visit_expr_for_subqueries(e)?;
                        }
                    }
                }
            }
            _ => {
                // Other expression types don't contain subqueries
            }
        }
        Ok(())
    }
}

/// Extract table name from ObjectName.
fn extract_table_name(name: &ObjectName) -> String {
    // Take the last part of the name (handles schema.table)
    name.0
        .last()
        .and_then(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_sql;

    fn make_datasets(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_discover_single_table() {
        let sql = "SELECT * FROM sales_data";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "sales_data");
        assert!(tables[0].alias.is_none());
    }

    #[test]
    fn test_discover_table_with_alias() {
        let sql = "SELECT s.* FROM sales_data AS s";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "sales_data");
        assert_eq!(tables[0].alias, Some("s".to_string()));
    }

    #[test]
    fn test_discover_join() {
        let sql = "SELECT * FROM sales_data s JOIN customer_data c ON s.cid = c.id";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data", "customer_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].dataset_id, "sales_data");
        assert_eq!(tables[1].dataset_id, "customer_data");
    }

    #[test]
    fn test_discover_unregistered_table() {
        let sql = "SELECT * FROM sales_data JOIN other_table ON 1=1";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]); // other_table not registered

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "sales_data");
    }

    #[test]
    fn test_discover_cte() {
        let sql = "WITH recent AS (SELECT * FROM sales_data WHERE dt > '2025-01-01') SELECT * FROM recent";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "sales_data");
    }

    #[test]
    fn test_discover_subquery_in_from() {
        let sql = "SELECT * FROM (SELECT * FROM sales_data) AS sub";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "sales_data");
    }

    #[test]
    fn test_discover_subquery_in_where() {
        let sql = "SELECT * FROM orders WHERE cid IN (SELECT id FROM customer_data)";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["customer_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].dataset_id, "customer_data");
    }

    #[test]
    fn test_discover_union() {
        let sql = "SELECT * FROM sales_data UNION ALL SELECT * FROM archive_data";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data", "archive_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_discover_no_tables() {
        let sql = "SELECT 1, 2, 3";
        let stmt = parse_sql(sql).unwrap();
        let datasets = make_datasets(&["sales_data"]);

        let tables = TableDiscovery::new(datasets).discover(&stmt).unwrap();

        assert!(tables.is_empty());
    }
}
