//! Stage SQL generation from [`LogicalPlan`] fragments.
//!
//! After stage cutting inserts exchange boundaries, each stage fragment is a
//! subtree of the original [`LogicalPlan`]. This module generates executable
//! SQL strings from those fragments — the SQL is sent to DuckDB via
//! `conn.query_arrow(sql)`.
//!
//! # Strategy
//!
//! Top-down reconstruction. Each [`LogicalPlan`] node maps to a SQL clause:
//!
//! | Node | SQL |
//! |------|-----|
//! | `Scan { table_factor }` | `FROM {table_factor}` |
//! | `ExchangeRead { alias }` | `FROM {alias}` |
//! | `Filter { predicate }` | `WHERE {predicate}` |
//! | `Project { items }` | `SELECT {items}` |
//! | `Aggregate { group_by, aggr_exprs }` | `SELECT {aggr_exprs} ... GROUP BY {group_by}` |
//! | `Sort { order_by }` | `ORDER BY {order_by}` |
//! | `Limit { limit, offset }` | `LIMIT {limit} OFFSET {offset}` |
//! | `Join { join_op }` | `{left} {join_op} {right}` |
//!
//! All sqlparser AST types implement `Display`, so generating SQL is
//! `format!("{}", expr)`. No custom SQL printer needed.
//!
//! See `docs/ldp_substrait_removal_design.md` §8.

use std::fmt::Write;

use sqlparser::ast::{Cte, Expr, OrderByExpr};

use super::logical_plan::{LogicalPlan, PlanContext};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate executable SQL from a [`LogicalPlan`] stage fragment.
///
/// `context` provides CTE definitions that may need to be prepended.
pub fn generate_stage_sql(plan: &LogicalPlan, context: &PlanContext) -> String {
    let mut sql = String::new();

    // Prepend CTEs if present.
    if !context.cte_definitions.is_empty() {
        write_cte_prefix(&mut sql, &context.cte_definitions);
    }

    // Generate the main SQL.
    generate_sql(plan, &mut sql);

    sql
}

/// Generate SQL without CTE context (for testing or simple cases).
pub fn generate_stage_sql_no_context(plan: &LogicalPlan) -> String {
    let mut sql = String::new();
    generate_sql(plan, &mut sql);
    sql
}

// ---------------------------------------------------------------------------
// CTE prefix
// ---------------------------------------------------------------------------

fn write_cte_prefix(sql: &mut String, ctes: &[Cte]) {
    sql.push_str("WITH ");
    for (i, cte) in ctes.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "{}", cte).unwrap();
    }
    sql.push(' ');
}

/// Returns true when the fragment ultimately reads from exchange data rather
/// than directly from local catalog tables.
fn is_exchange_backed_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ExchangeRead { .. } => true,
        LogicalPlan::SubqueryScan { input, .. } => is_exchange_backed_plan(input),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. } => is_exchange_backed_plan(input),
        LogicalPlan::Join { .. } | LogicalPlan::SetOp { .. } | LogicalPlan::Scan { .. } => false,
    }
}

/// Returns true when any child of this plan reads from exchange data.
/// Unlike `is_exchange_backed_plan`, this traverses into joins to detect
/// exchange reads on any branch of a multi-way join.
///
/// Only returns true for **bare** ExchangeRead nodes — those not wrapped
/// in a SubqueryScan with an alias. Aliased exchanges (via SubqueryScan)
/// preserve the original table qualifier and don't need dequalification.
fn plan_contains_bare_exchange(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ExchangeRead { .. } => true,
        // SubqueryScan wraps ExchangeRead with the original table alias,
        // so qualified references like `o.col` resolve correctly.
        LogicalPlan::SubqueryScan { .. } => false,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. } => plan_contains_bare_exchange(input),
        LogicalPlan::Join { left, right, .. } => {
            plan_contains_bare_exchange(left) || plan_contains_bare_exchange(right)
        }
        LogicalPlan::SetOp { left, right, .. } => {
            plan_contains_bare_exchange(left) || plan_contains_bare_exchange(right)
        }
        LogicalPlan::Scan { .. } => false,
    }
}

/// Dequalify ORDER BY expression.
/// This strips table qualifiers from column references so they match the column names
/// in the exchange output (which come from the SELECT list, not table references).
fn dequalify_order_by_expr(order_by: &OrderByExpr) -> OrderByExpr {
    let mut rewritten = order_by.clone();
    rewritten.expr = dequalify_expr(&rewritten.expr);
    rewritten
}

/// Strip table qualifiers from compound identifiers (e.g. `o.col` -> `col`).
///
/// This function recursively processes expression trees and removes table
/// qualifiers from column references. This is necessary when generating SQL
/// for exchange-backed plans where the table alias no longer exists.
fn dequalify_expr(expr: &Expr) -> Expr {
    match expr {
        // Core: qualified column reference → unqualified
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            Expr::Identifier(parts.last().cloned().unwrap())
        }

        // Binary operations (AND, OR, =, <, >, +, -, *, /, etc.)
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(dequalify_expr(left)),
            op: op.clone(),
            right: Box::new(dequalify_expr(right)),
        },

        // Unary operations (NOT, -, +)
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(dequalify_expr(expr)),
        },

        // Parenthesized expressions
        Expr::Nested(inner) => Expr::Nested(Box::new(dequalify_expr(inner))),

        // CAST(expr AS type)
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(dequalify_expr(expr)),
            data_type: data_type.clone(),
            format: format.clone(),
        },

        // Function calls: SUM(o.col), COUNT(o.col), etc.
        Expr::Function(func) => {
            let mut func = func.clone();
            func.args = match func.args {
                sqlparser::ast::FunctionArguments::List(mut arg_list) => {
                    arg_list.args = arg_list
                        .args
                        .into_iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(dequalify_expr(&e)),
                            ),
                            other => other,
                        })
                        .collect();
                    sqlparser::ast::FunctionArguments::List(arg_list)
                }
                other => other,
            };
            Expr::Function(func)
        }

        // IS NULL / IS NOT NULL
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(dequalify_expr(inner))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(dequalify_expr(inner))),

        // CASE WHEN ... THEN ... ELSE ... END
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => Expr::Case {
            case_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
            end_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
            operand: operand.as_ref().map(|e| Box::new(dequalify_expr(e))),
            conditions: conditions
                .iter()
                .map(|cw| sqlparser::ast::CaseWhen {
                    condition: dequalify_expr(&cw.condition),
                    result: dequalify_expr(&cw.result),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| Box::new(dequalify_expr(e))),
        },

        // BETWEEN low AND high
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(dequalify_expr(expr)),
            negated: *negated,
            low: Box::new(dequalify_expr(low)),
            high: Box::new(dequalify_expr(high)),
        },

        // IN (list) - e.g., col IN (1, 2, 3)
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(dequalify_expr(expr)),
            list: list.iter().map(dequalify_expr).collect(),
            negated: *negated,
        },

        // IN (subquery) - e.g., col IN (SELECT ...)
        // Note: We don't dequalify inside the subquery as it has its own scope
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(dequalify_expr(expr)),
            subquery: subquery.clone(),
            negated: *negated,
        },

        // LIKE / ILIKE patterns
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            any,
        } => Expr::Like {
            negated: *negated,
            expr: Box::new(dequalify_expr(expr)),
            pattern: Box::new(dequalify_expr(pattern)),
            escape_char: escape_char.clone(),
            any: *any,
        },
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            any,
        } => Expr::ILike {
            negated: *negated,
            expr: Box::new(dequalify_expr(expr)),
            pattern: Box::new(dequalify_expr(pattern)),
            escape_char: escape_char.clone(),
            any: *any,
        },

        // SIMILAR TO pattern
        Expr::SimilarTo {
            negated,
            expr,
            pattern,
            escape_char,
        } => Expr::SimilarTo {
            negated: *negated,
            expr: Box::new(dequalify_expr(expr)),
            pattern: Box::new(dequalify_expr(pattern)),
            escape_char: escape_char.clone(),
        },

        // EXTRACT(part FROM expr)
        Expr::Extract { field, expr, .. } => Expr::Extract {
            field: field.clone(),
            syntax: sqlparser::ast::ExtractSyntax::From,
            expr: Box::new(dequalify_expr(expr)),
        },

        // SUBSTRING(expr FROM start FOR length)
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => Expr::Substring {
            expr: Box::new(dequalify_expr(expr)),
            substring_from: substring_from.as_ref().map(|e| Box::new(dequalify_expr(e))),
            substring_for: substring_for.as_ref().map(|e| Box::new(dequalify_expr(e))),
            special: false,
            shorthand: false,
        },

        // TRIM([LEADING|TRAILING|BOTH] chars FROM expr)
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => Expr::Trim {
            expr: Box::new(dequalify_expr(expr)),
            trim_where: *trim_where,
            trim_what: trim_what.as_ref().map(|e| Box::new(dequalify_expr(e))),
            trim_characters: trim_characters
                .as_ref()
                .map(|chars| chars.iter().map(dequalify_expr).collect()),
        },

        // IS TRUE / IS FALSE / IS UNKNOWN
        Expr::IsTrue(inner) => Expr::IsTrue(Box::new(dequalify_expr(inner))),
        Expr::IsFalse(inner) => Expr::IsFalse(Box::new(dequalify_expr(inner))),
        Expr::IsNotTrue(inner) => Expr::IsNotTrue(Box::new(dequalify_expr(inner))),
        Expr::IsNotFalse(inner) => Expr::IsNotFalse(Box::new(dequalify_expr(inner))),
        Expr::IsUnknown(inner) => Expr::IsUnknown(Box::new(dequalify_expr(inner))),
        Expr::IsNotUnknown(inner) => Expr::IsNotUnknown(Box::new(dequalify_expr(inner))),

        // IS DISTINCT FROM / IS NOT DISTINCT FROM
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(dequalify_expr(left)),
            Box::new(dequalify_expr(right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(dequalify_expr(left)),
            Box::new(dequalify_expr(right)),
        ),

        // Tuple: (expr1, expr2, ...)
        Expr::Tuple(exprs) => Expr::Tuple(exprs.iter().map(dequalify_expr).collect()),

        // Default: return unchanged for literals, identifiers without qualifiers, etc.
        _ => expr.clone(),
    }
}

/// Strip table qualifiers from all expressions in a GROUP BY clause.
fn dequalify_group_by(group_by: &sqlparser::ast::GroupByExpr) -> sqlparser::ast::GroupByExpr {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            let dequalified = exprs.iter().map(dequalify_expr).collect();
            sqlparser::ast::GroupByExpr::Expressions(dequalified, modifiers.clone())
        }
        sqlparser::ast::GroupByExpr::All(v) => sqlparser::ast::GroupByExpr::All(v.clone()),
    }
}

// ---------------------------------------------------------------------------
// Core SQL generation (top-down recursive)
// ---------------------------------------------------------------------------

/// The top-level SQL generator dispatches based on the outermost node.
///
/// The key insight: SQL clauses are layered (SELECT ... FROM ... WHERE ...
/// GROUP BY ... ORDER BY ... LIMIT ...). We walk the plan top-down, collecting
/// clauses as we go, then emit them in SQL order.
fn generate_sql(plan: &LogicalPlan, sql: &mut String) {
    match plan {
        // Limit wraps Sort wraps ... — peel outer clauses first.
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            generate_sql(input, sql);
            if let Some(l) = limit {
                write!(sql, " LIMIT {}", l).unwrap();
            }
            if let Some(o) = offset {
                write!(sql, " OFFSET {}", o).unwrap();
            }
        }

        LogicalPlan::Sort { input, order_by } => {
            generate_sql(input, sql);
            if !order_by.is_empty() {
                sql.push_str(" ORDER BY ");
                for (i, expr) in order_by.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    let order_expr = if is_exchange_backed_plan(input) {
                        dequalify_order_by_expr(expr)
                    } else {
                        expr.clone()
                    };
                    write!(sql, "{}", order_expr).unwrap();
                }
            }
        }

        // Aggregate is a terminal SELECT (contains its own projection).
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            having,
            ..
        } => {
            let exchange_backed = is_exchange_backed_plan(input);

            sql.push_str("SELECT ");
            if exchange_backed {
                write_select_items_dequalified(sql, aggr_exprs);
            } else {
                write_select_items(sql, aggr_exprs);
            }

            // FROM clause from the input.
            sql.push_str(" FROM ");
            generate_from_fragment(input, sql);

            // WHERE clause (if input has a filter, it's already in the from fragment).

            // GROUP BY — only emit when there are grouping expressions.
            // GroupByExpr::Expressions([], _) formats as "GROUP BY " (no columns),
            // which DuckDB rejects with "syntax error at end of input".
            let has_group_by = match group_by {
                sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
                sqlparser::ast::GroupByExpr::All(_) => true,
            };
            if has_group_by {
                let group_by = if exchange_backed {
                    dequalify_group_by(group_by)
                } else {
                    group_by.clone()
                };
                write!(sql, " {}", group_by).unwrap();
            }

            // HAVING.
            if let Some(h) = having {
                let having_expr = if exchange_backed {
                    dequalify_expr(h)
                } else {
                    h.clone()
                };
                write!(sql, " HAVING {}", having_expr).unwrap();
            }
        }

        // Project is a terminal SELECT.
        LogicalPlan::Project { input, items } => {
            sql.push_str("SELECT ");
            if plan_contains_bare_exchange(input) {
                write_select_items_dequalified(sql, items);
            } else {
                write_select_items(sql, items);
            }
            sql.push_str(" FROM ");
            generate_from_fragment(input, sql);
        }

        // Window is treated like Project for SQL generation (§6.6).
        LogicalPlan::Window {
            input,
            window_exprs,
            ..
        } => {
            sql.push_str("SELECT ");
            write_select_items(sql, window_exprs);
            sql.push_str(" FROM ");
            generate_from_fragment(input, sql);
        }

        // SetOp: recurse into left and right.
        LogicalPlan::SetOp {
            left,
            right,
            op,
            set_quantifier,
        } => {
            generate_sql(left, sql);
            write!(sql, " {} ", op).unwrap();
            if !matches!(set_quantifier, sqlparser::ast::SetQuantifier::None) {
                write!(sql, "{} ", set_quantifier).unwrap();
            }
            generate_sql(right, sql);
        }

        // SubqueryScan at the top level: just generate the inner query.
        // The alias is meaningless outside a FROM clause. The (subquery) AS alias
        // form is only valid inside FROM — that case is handled by generate_from_fragment.
        LogicalPlan::SubqueryScan { input, .. } => {
            generate_sql(input, sql);
        }

        // Leaf nodes that appear as the outermost plan (unusual but possible).
        LogicalPlan::Scan { table_factor, .. } => {
            sql.push_str("SELECT * FROM ");
            write!(sql, "{}", table_factor).unwrap();
        }

        LogicalPlan::ExchangeRead { alias, .. } => {
            sql.push_str("SELECT * FROM ");
            sql.push_str(alias);
        }

        // Filter without a projection above it: emit SELECT * FROM ... WHERE ...
        LogicalPlan::Filter { input, predicate } => {
            sql.push_str("SELECT * FROM ");
            generate_from_fragment(input, sql);
            if plan_contains_bare_exchange(input) {
                write!(sql, " WHERE {}", dequalify_expr(predicate)).unwrap();
            } else {
                write!(sql, " WHERE {}", predicate).unwrap();
            }
        }

        // Join without a projection above it.
        LogicalPlan::Join { .. } => {
            sql.push_str("SELECT * FROM ");
            generate_from_fragment(plan, sql);
        }
    }
}

// ---------------------------------------------------------------------------
// FROM fragment generation
// ---------------------------------------------------------------------------

/// Generate the FROM fragment — everything that appears after `FROM`.
///
/// This handles scans, joins, filters (which become subqueries if nested
/// under a join), and exchange reads.
fn generate_from_fragment(plan: &LogicalPlan, sql: &mut String) {
    match plan {
        LogicalPlan::Scan { table_factor, .. } => {
            write!(sql, "{}", table_factor).unwrap();
        }

        LogicalPlan::ExchangeRead { alias, .. } => {
            sql.push_str(alias);
        }

        LogicalPlan::Join {
            left,
            right,
            join_op,
            ..
        } => {
            generate_from_fragment(left, sql);
            write!(sql, " {} ", format_join_type(join_op)).unwrap();
            generate_from_fragment(right, sql);
            // When either side of the join is an exchange read (without the
            // original table alias), qualified column references in the ON
            // clause would fail. Strip qualifiers so DuckDB resolves columns
            // by name against the flat exchange output.
            let has_exchange = plan_contains_bare_exchange(left) || plan_contains_bare_exchange(right);
            if has_exchange {
                write_join_constraint_dequalified(sql, join_op);
            } else {
                write_join_constraint(sql, join_op);
            }
        }

        // Filter under a FROM → WHERE clause.
        LogicalPlan::Filter { input, predicate } => {
            generate_from_fragment(input, sql);
            if plan_contains_bare_exchange(input) {
                write!(sql, " WHERE {}", dequalify_expr(predicate)).unwrap();
            } else {
                write!(sql, " WHERE {}", predicate).unwrap();
            }
        }

        // SubqueryScan in FROM position.
        LogicalPlan::SubqueryScan { input, alias } => {
            sql.push('(');
            generate_sql(input, sql);
            // TableAlias::Display already includes "AS " when explicit.
            write!(sql, ") {}", alias).unwrap();
        }

        // Other nodes in FROM position: wrap as subquery.
        other => {
            sql.push('(');
            generate_sql(other, sql);
            sql.push_str(") AS __subquery");
        }
    }
}

// ---------------------------------------------------------------------------
// Join formatting helpers
// ---------------------------------------------------------------------------

/// Format just the JOIN type keyword (without the constraint).
fn format_join_type(op: &sqlparser::ast::JoinOperator) -> &'static str {
    use sqlparser::ast::JoinOperator::*;
    match op {
        Join(_) | Inner(_) => "JOIN",
        Left(_) | LeftOuter(_) => "LEFT JOIN",
        Right(_) | RightOuter(_) => "RIGHT JOIN",
        FullOuter(_) => "FULL OUTER JOIN",
        CrossJoin(_) => "CROSS JOIN",
        Semi(_) => "SEMI JOIN",
        LeftSemi(_) => "LEFT SEMI JOIN",
        RightSemi(_) => "RIGHT SEMI JOIN",
        Anti(_) => "ANTI JOIN",
        LeftAnti(_) => "LEFT ANTI JOIN",
        RightAnti(_) => "RIGHT ANTI JOIN",
        CrossApply => "CROSS APPLY",
        OuterApply => "OUTER APPLY",
        AsOf { .. } => "ASOF JOIN",
        StraightJoin(_) => "STRAIGHT_JOIN",
    }
}

/// Write the JOIN constraint (ON/USING clause) if present.
fn write_join_constraint(sql: &mut String, op: &sqlparser::ast::JoinOperator) {
    if let Some(constraint) = super::extract_join_constraint(op) {
        write_constraint(sql, constraint, false);
    }
}

/// Write the JOIN constraint with table qualifiers stripped.
fn write_join_constraint_dequalified(sql: &mut String, op: &sqlparser::ast::JoinOperator) {
    if let Some(constraint) = super::extract_join_constraint(op) {
        write_constraint(sql, constraint, true);
    }
}

fn write_constraint(
    sql: &mut String,
    constraint: &sqlparser::ast::JoinConstraint,
    dequalify: bool,
) {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => {
            let expr = if dequalify {
                dequalify_expr(expr)
            } else {
                expr.clone()
            };
            write!(sql, " ON {}", expr).unwrap();
        }
        sqlparser::ast::JoinConstraint::Using(columns) => {
            sql.push_str(" USING(");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                write!(sql, "{}", col).unwrap();
            }
            sql.push(')');
        }
        sqlparser::ast::JoinConstraint::Natural => {}
        sqlparser::ast::JoinConstraint::None => {}
    }
}

// ---------------------------------------------------------------------------
// SELECT list formatting
// ---------------------------------------------------------------------------

fn write_select_items(sql: &mut String, items: &[sqlparser::ast::SelectItem]) {
    if items.is_empty() {
        sql.push('*');
        return;
    }
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        write!(sql, "{}", item).unwrap();
    }
}

/// Write SELECT items with table qualifiers stripped from column references.
fn write_select_items_dequalified(sql: &mut String, items: &[sqlparser::ast::SelectItem]) {
    if items.is_empty() {
        sql.push('*');
        return;
    }
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let dequalified = dequalify_select_item(item);
        write!(sql, "{}", dequalified).unwrap();
    }
}

/// Strip table qualifiers from expressions within a SELECT item while
/// preserving aliases.
fn dequalify_select_item(item: &sqlparser::ast::SelectItem) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(dequalify_expr(expr)),
        SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
            expr: dequalify_expr(expr),
            alias: alias.clone(),
        },
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::plan_builder::build_logical_plan;

    /// Parse SQL → build LogicalPlan → generate SQL → verify it parses again.
    fn round_trip(input_sql: &str) -> String {
        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, input_sql)
                .unwrap();
        let result = build_logical_plan(&stmts[0]).unwrap();
        let generated = generate_stage_sql(&result.plan, &result.context);

        // Verify the generated SQL is parseable.
        let reparsed = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            &generated,
        );
        assert!(
            reparsed.is_ok(),
            "Generated SQL failed to parse: {}\nError: {:?}",
            generated,
            reparsed.err()
        );

        generated
    }

    #[test]
    fn test_simple_select() {
        let sql = round_trip("SELECT id, name FROM users");
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("id"));
        assert!(sql.contains("name"));
        assert!(sql.contains("users"));
    }

    #[test]
    fn test_select_with_where() {
        let sql = round_trip("SELECT id FROM users WHERE age > 18");
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("age"));
    }

    #[test]
    fn test_aggregate() {
        let sql = round_trip("SELECT region, SUM(amount) FROM sales GROUP BY region");
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("SUM"));
        assert!(sql.contains("region"));
    }

    #[test]
    fn test_aggregate_with_having() {
        let sql = round_trip(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 100",
        );
        assert!(sql.contains("HAVING"));
    }

    #[test]
    fn test_order_by_limit() {
        let sql = round_trip("SELECT id FROM orders ORDER BY id LIMIT 10 OFFSET 5");
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT"));
        assert!(sql.contains("OFFSET"));
    }

    #[test]
    fn test_join() {
        let sql =
            round_trip("SELECT o.id, l.price FROM orders o JOIN lineitem l ON o.id = l.order_id");
        assert!(sql.contains("JOIN"));
        assert!(sql.contains("ON"));
    }

    #[test]
    fn test_union_all() {
        let sql = round_trip("SELECT id FROM t1 UNION ALL SELECT id FROM t2");
        assert!(sql.contains("UNION"));
        assert!(sql.contains("ALL"));
    }

    #[test]
    fn test_subquery() {
        let sql = round_trip("SELECT x FROM (SELECT id AS x FROM orders) AS sub");
        assert!(sql.contains("sub"));
    }

    #[test]
    fn test_exchange_read_sql() {
        let plan = LogicalPlan::ExchangeRead {
            exchange_id: 0,
            alias: "__exchange_0".to_string(),
        };
        let sql = generate_stage_sql_no_context(&plan);
        assert_eq!(sql, "SELECT * FROM __exchange_0");
    }

    #[test]
    fn test_cte_prefix() {
        let sql = round_trip("WITH cte AS (SELECT 1 AS x) SELECT x FROM cte");
        assert!(sql.contains("WITH"));
        assert!(sql.contains("cte"));
    }

    #[test]
    fn test_left_join() {
        let sql = round_trip("SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id");
        assert!(sql.contains("LEFT JOIN"), "SQL: {}", sql);
    }

    #[test]
    fn test_cross_join() {
        let sql = round_trip("SELECT * FROM t1, t2");
        // Implicit cross join → generates CROSS JOIN.
        assert!(sql.contains("CROSS JOIN") || sql.contains("t1") && sql.contains("t2"));
    }

    #[test]
    fn test_multiple_aggregates() {
        let sql = round_trip(
            "SELECT region, SUM(amount), COUNT(*), AVG(price) FROM sales GROUP BY region",
        );
        assert!(sql.contains("SUM"));
        assert!(sql.contains("COUNT"));
        assert!(sql.contains("AVG"));
    }

    #[test]
    fn test_window_function() {
        let sql = round_trip("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders");
        assert!(sql.contains("ROW_NUMBER"));
        assert!(sql.contains("OVER"));
    }

    #[test]
    fn test_join_using() {
        let sql = round_trip("SELECT * FROM t1 JOIN t2 USING (id)");
        assert!(sql.contains("JOIN"));
        assert!(sql.contains("USING"));
        assert!(sql.contains("id"));
    }

    #[test]
    fn test_fetch_first() {
        let sql = round_trip("SELECT id FROM orders FETCH FIRST 10 ROWS ONLY");
        // FETCH → LIMIT.
        assert!(sql.contains("LIMIT") || sql.contains("FETCH"));
    }

    #[test]
    fn test_complex_where_clause() {
        let sql = round_trip(
            "SELECT id FROM t WHERE (a > 10 AND b < 20) OR (c = 'test' AND d IS NOT NULL)",
        );
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("AND"));
        assert!(sql.contains("OR"));
    }
}
