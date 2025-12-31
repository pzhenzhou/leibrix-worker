//! SQL parsing utilities.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::error::SqlTransformError;

/// Parse a SQL string into a single AST Statement.
///
/// Returns an error if:
/// - The SQL is syntactically invalid
/// - Multiple statements are provided (only single-statement queries supported)
pub fn parse_sql(sql: &str) -> Result<Statement, SqlTransformError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    if statements.is_empty() {
        return Err(SqlTransformError::ParseError("Empty SQL statement".to_string()));
    }

    if statements.len() > 1 {
        return Err(SqlTransformError::MultipleStatements);
    }

    Ok(statements.into_iter().next().unwrap())
}

/// Regenerate SQL string from an AST Statement.
///
/// Uses sqlparser's Display implementation which produces
/// normalized SQL (uppercase keywords, standard formatting).
pub fn to_sql(stmt: &Statement) -> String {
    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM sales_data WHERE dt = '2025-01-01'";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_with_join() {
        let sql = "SELECT s.*, c.name FROM sales s JOIN customers c ON s.cid = c.id";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_with_cte() {
        let sql = "WITH recent AS (SELECT * FROM sales WHERE dt > '2025-01-01') SELECT * FROM recent";
        let result = parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_empty_sql() {
        let sql = "";
        let result = parse_sql(sql);
        assert!(matches!(result, Err(SqlTransformError::ParseError(_))));
    }

    #[test]
    fn test_parse_multiple_statements() {
        let sql = "SELECT 1; SELECT 2";
        let result = parse_sql(sql);
        assert!(matches!(result, Err(SqlTransformError::MultipleStatements)));
    }

    #[test]
    fn test_roundtrip() {
        let sql = "SELECT a, b FROM t WHERE x = 1";
        let stmt = parse_sql(sql).unwrap();
        let regenerated = to_sql(&stmt);
        // Should parse back successfully
        let reparsed = parse_sql(&regenerated);
        assert!(reparsed.is_ok());
    }
}
