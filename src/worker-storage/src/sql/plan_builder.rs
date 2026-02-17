//! SQL AST → [`LogicalPlan`] builder.
//!
//! Converts a parsed SQL `SELECT` statement into a [`LogicalPlan`] tree suitable
//! for LDP distribution planning. The builder preserves sqlparser AST expressions
//! for later SQL regeneration — it does not evaluate or type-check expressions.
//!
//! # Traversal Order
//!
//! For `Query`:
//! 1. `query.body` → `SetExpr::Select` → scan / join / filter / project
//! 2. `query.order_by` → wrap in `Sort`
//! 3. `query.limit_clause` / `query.fetch` → wrap in `Limit`
//! 4. `query.with` → record CTE definitions in `PlanContext`
//!
//! For `SetExpr::Select(select)`:
//! 1. `select.from` → build scan tree from `TableWithJoins` / `TableFactor` / `Join`
//! 2. `select.selection` → wrap in `Filter`
//! 3. `select.group_by` → wrap in `Aggregate`
//! 4. `select.projection` → `Window` + `Project` (if window functions) or `Project` only
//!
//! See `docs/ldp_substrait_removal_design.md` §6.

use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Cte, Expr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, OrderByKind, Query, Select, SelectItem, SetExpr,
    Statement, TableAlias, TableFactor, TableWithJoins,
};

use super::logical_plan::{ColumnRef, LogicalPlan, PlanBuildError, PlanContext};

/// Map of CTE name (lowercased) to its query AST, used for resolving CTE references
/// into SubqueryScan nodes during plan building.
type CteMap<'a> = HashMap<String, &'a Query>;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build result: a logical plan tree plus any CTE context.
#[derive(Clone, Debug)]
pub struct BuildResult {
    pub plan: LogicalPlan,
    pub context: PlanContext,
}

/// Convert a SQL statement into a [`LogicalPlan`] plus [`PlanContext`].
///
/// Only `SELECT` queries are supported. Returns [`PlanBuildError::UnsupportedStatement`]
/// for other statement types.
pub fn build_logical_plan(stmt: &Statement) -> Result<BuildResult, PlanBuildError> {
    match stmt {
        Statement::Query(query) => build_from_query(query),
        other => Err(PlanBuildError::UnsupportedStatement(
            statement_name(other).to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Query-level builders
// ---------------------------------------------------------------------------

fn build_from_query(query: &Query) -> Result<BuildResult, PlanBuildError> {
    build_from_query_with_ctes(query, &HashMap::new())
}

fn build_from_query_with_ctes(query: &Query, outer_ctes: &CteMap<'_>) -> Result<BuildResult, PlanBuildError> {
    // 1. Build CTE map: start with outer CTEs, add this query's CTEs.
    // CTEs are ordered so later CTEs can reference earlier ones.
    let mut cte_map = outer_ctes.clone();
    let cte_definitions: Vec<Cte> = query
        .with
        .as_ref()
        .map(|w| w.cte_tables.clone())
        .unwrap_or_default();

    for cte in &cte_definitions {
        cte_map.insert(cte.alias.name.value.to_lowercase(), &cte.query);
    }

    // 2. Build plan from query body, passing CTE map for resolution.
    let mut plan = build_set_expr(&query.body, &cte_map)?;

    // 3. ORDER BY → Sort.
    if let Some(ref order_by) = query.order_by {
        match &order_by.kind {
            OrderByKind::Expressions(exprs) if !exprs.is_empty() => {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: exprs.clone(),
                };
            }
            OrderByKind::All(_) => {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: vec![],
                };
            }
            _ => {} // No order-by expressions.
        }
    }

    // 4. LIMIT / OFFSET → Limit.
    let (limit_expr, offset_expr) = extract_limit_offset(query);
    if limit_expr.is_some() || offset_expr.is_some() {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: limit_expr,
            offset: offset_expr,
        };
    }

    Ok(BuildResult {
        plan,
        // Preserve all CTE definitions for downstream stage SQL generation.
        context: PlanContext::with_ctes(cte_definitions),
    })
}

/// Extract LIMIT and OFFSET expressions from a `Query`.
fn extract_limit_offset(query: &Query) -> (Option<Expr>, Option<Expr>) {
    use sqlparser::ast::LimitClause;

    let mut limit = None;
    let mut offset = None;

    if let Some(ref lc) = query.limit_clause {
        match lc {
            LimitClause::LimitOffset {
                limit: l,
                offset: o,
                ..
            } => {
                limit = l.clone();
                offset = o.as_ref().map(|off| off.value.clone());
            }
            LimitClause::OffsetCommaLimit {
                offset: o,
                limit: l,
            } => {
                limit = Some(l.clone());
                offset = Some(o.clone());
            }
        }
    }

    // FETCH FIRST N ROWS → treat as LIMIT N.
    if limit.is_none() {
        if let Some(ref fetch) = query.fetch {
            limit = fetch.quantity.clone();
        }
    }

    (limit, offset)
}

// ---------------------------------------------------------------------------
// SetExpr builders
// ---------------------------------------------------------------------------

fn build_set_expr(set_expr: &SetExpr, cte_map: &CteMap<'_>) -> Result<LogicalPlan, PlanBuildError> {
    match set_expr {
        SetExpr::Select(select) => build_select(select, cte_map),
        SetExpr::Query(query) => {
            let result = build_from_query_with_ctes(query, cte_map)?;
            Ok(result.plan)
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_plan = build_set_expr(left, cte_map)?;
            let right_plan = build_set_expr(right, cte_map)?;
            Ok(LogicalPlan::SetOp {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                op: *op,
                set_quantifier: *set_quantifier,
            })
        }
        SetExpr::Values(_) => Err(PlanBuildError::UnsupportedFeature(
            "VALUES clause".to_string(),
        )),
        other => Err(PlanBuildError::UnsupportedFeature(format!(
            "SetExpr variant: {:?}",
            std::mem::discriminant(other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// SELECT builder
// ---------------------------------------------------------------------------

fn build_select(select: &Select, cte_map: &CteMap<'_>) -> Result<LogicalPlan, PlanBuildError> {
    // 1. FROM → scan / join tree.
    let mut plan = build_from_clause(&select.from, cte_map)?;

    // 2. WHERE → Filter.
    if let Some(ref predicate) = select.selection {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // 3. GROUP BY → Aggregate.
    let has_group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        GroupByExpr::All(_) => true,
    };
    let has_aggregate_functions = select
        .projection
        .iter()
        .any(select_item_has_aggregate);

    if has_group_by || has_aggregate_functions {
        let group_keys = extract_group_keys(&select.group_by);
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            aggr_exprs: select.projection.clone(),
            having: select.having.clone(),
            group_keys,
        };
        // For aggregates, the projection is part of the Aggregate node.
        // No separate Project needed.
        return Ok(plan);
    }

    // 4. Projection → Window + Project or just Project.
    let has_window = select
        .projection
        .iter()
        .any(select_item_has_window);

    if has_window {
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            window_exprs: select.projection.clone(),
        };
    } else {
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            items: select.projection.clone(),
        };
    }

    Ok(plan)
}

// ---------------------------------------------------------------------------
// FROM clause builders
// ---------------------------------------------------------------------------

fn build_from_clause(from: &[TableWithJoins], cte_map: &CteMap<'_>) -> Result<LogicalPlan, PlanBuildError> {
    if from.is_empty() {
        return Err(PlanBuildError::UnsupportedFeature(
            "SELECT without FROM".to_string(),
        ));
    }

    // Build the first TableWithJoins.
    let mut plan = build_table_with_joins(&from[0], cte_map)?;

    // Multiple FROM items are implicit CROSS JOINs.
    for twj in &from[1..] {
        let right = build_table_with_joins(twj, cte_map)?;
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            join_op: JoinOperator::CrossJoin(JoinConstraint::None),
            left_keys: vec![],
            right_keys: vec![],
        };
    }

    Ok(plan)
}

fn build_table_with_joins(twj: &TableWithJoins, cte_map: &CteMap<'_>) -> Result<LogicalPlan, PlanBuildError> {
    let mut plan = build_table_factor(&twj.relation, cte_map)?;

    for join in &twj.joins {
        let right = build_table_factor(&join.relation, cte_map)?;

        // Collect table aliases/names from both sides for key attribution.
        let left_tables = collect_table_names(&plan);
        let right_tables = collect_table_names(&right);

        // Extract equi-join keys from the ON clause.
        let (left_keys, right_keys) =
            extract_join_keys_from_operator(&join.join_operator, &left_tables, &right_tables);

        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right),
            join_op: join.join_operator.clone(),
            left_keys,
            right_keys,
        };
    }

    Ok(plan)
}

fn build_table_factor(tf: &TableFactor, cte_map: &CteMap<'_>) -> Result<LogicalPlan, PlanBuildError> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);

            // Check if this table reference is a CTE. If so, resolve it as
            // a SubqueryScan so the annotator can see the actual base tables.
            // Only resolve CTEs whose body has a FROM clause (references tables).
            // Trivial CTEs like `SELECT 1 AS x` are left as opaque scans.
            if let Some(cte_query) = cte_map.get(&table_name.to_lowercase()) {
                if query_has_from(cte_query) {
                    let result = build_from_query_with_ctes(cte_query, cte_map)?;
                    let alias = alias.clone().unwrap_or(TableAlias {
                        name: Ident::new(&table_name),
                        columns: vec![],
                        explicit: true,
                    });
                    return Ok(LogicalPlan::SubqueryScan {
                        input: Box::new(result.plan),
                        alias,
                    });
                }
                // Trivial CTE without FROM: fall through to regular Scan.
                // The CTE definition will be stored in PlanContext for SQL generation.
            }

            Ok(LogicalPlan::Scan {
                table_name,
                alias: alias.clone(),
                table_factor: tf.clone(),
            })
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let result = build_from_query_with_ctes(subquery, cte_map)?;
            let alias = alias.clone().ok_or_else(|| {
                PlanBuildError::UnsupportedFeature("derived table without alias".to_string())
            })?;
            Ok(LogicalPlan::SubqueryScan {
                input: Box::new(result.plan),
                alias,
            })
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let plan = build_table_with_joins(table_with_joins, cte_map)?;
            match alias {
                Some(a) => Ok(LogicalPlan::SubqueryScan {
                    input: Box::new(plan),
                    alias: a.clone(),
                }),
                None => Ok(plan),
            }
        }
        TableFactor::Function {
            name, alias, ..
        } => {
            // Table-valued function, e.g., scan_orders(DATE '2024-02-01', DATE '9999-12-31') AS o.
            // Extract just the function name so metadata lookup works correctly
            // (the metadata module resolves `scan_X` → base table `X`).
            let table_name = object_name_to_string(name);
            Ok(LogicalPlan::Scan {
                table_name,
                alias: alias.clone(),
                table_factor: tf.clone(),
            })
        }
        other => {
            // TableFactor variants like UNNEST, Pivot, etc.
            // Treat as an opaque scan — the original TableFactor is preserved
            // for SQL regeneration. The planner treats it as EpochPartitioned
            // or Singleton depending on metadata.
            let table_name = format!("{}", other);
            Ok(LogicalPlan::Scan {
                table_name,
                alias: None,
                table_factor: other.clone(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Join key extraction
// ---------------------------------------------------------------------------

/// Extract equi-join keys from a `JoinOperator`'s constraint.
fn extract_join_keys_from_operator(
    join_op: &JoinOperator,
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
) -> (Vec<ColumnRef>, Vec<ColumnRef>) {
    let constraint = super::extract_join_constraint(join_op);
    match constraint {
        Some(JoinConstraint::On(expr)) => extract_equi_keys(expr, left_tables, right_tables),
        Some(JoinConstraint::Using(columns)) => {
            // USING(col1, col2) → both sides have same column name.
            let keys: Vec<ColumnRef> = columns
                .iter()
                .map(|obj_name| ColumnRef::unqualified(object_name_to_string(obj_name)))
                .collect();
            (keys.clone(), keys)
        }
        _ => (vec![], vec![]),
    }
}

/// Extract equi-join key pairs from an ON expression.
///
/// Handles chains of `a.col = b.col AND a.col2 = b.col2`.
/// Any non-equi predicate or unrecognized expression is ignored (conservative:
/// empty keys → Singleton requirement).
fn extract_equi_keys(
    expr: &Expr,
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
) -> (Vec<ColumnRef>, Vec<ColumnRef>) {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    extract_equi_keys_recursive(expr, left_tables, right_tables, &mut left_keys, &mut right_keys);

    (left_keys, right_keys)
}

fn extract_equi_keys_recursive(
    expr: &Expr,
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
    left_keys: &mut Vec<ColumnRef>,
    right_keys: &mut Vec<ColumnRef>,
) {
    match expr {
        // a.col = b.col
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l_ref), Some(r_ref)) =
                (expr_to_column_ref(left), expr_to_column_ref(right))
            {
                // Determine which side each column belongs to.
                let l_on_left = is_from_tables(&l_ref, left_tables);
                let r_on_right = is_from_tables(&r_ref, right_tables);
                let l_on_right = is_from_tables(&l_ref, right_tables);
                let r_on_left = is_from_tables(&r_ref, left_tables);

                if l_on_left && r_on_right {
                    left_keys.push(l_ref);
                    right_keys.push(r_ref);
                } else if l_on_right && r_on_left {
                    // Reversed: right.col = left.col
                    left_keys.push(r_ref);
                    right_keys.push(l_ref);
                }
                // If both on same side or unknown, skip (conservative).
            }
        }
        // expr1 AND expr2
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            extract_equi_keys_recursive(left, left_tables, right_tables, left_keys, right_keys);
            extract_equi_keys_recursive(right, left_tables, right_tables, left_keys, right_keys);
        }
        // Nested parenthesized expression.
        Expr::Nested(inner) => {
            extract_equi_keys_recursive(inner, left_tables, right_tables, left_keys, right_keys);
        }
        // Anything else (OR, function calls, etc.) → skip.
        _ => {}
    }
}

/// Try to extract a `ColumnRef` from an expression.
///
/// Handles:
/// - `Expr::Identifier(ident)` → unqualified column
/// - `Expr::CompoundIdentifier([table, column])` → qualified column
fn expr_to_column_ref(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(ident) => Some(ColumnRef::unqualified(&ident.value)),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(ColumnRef::qualified(
            &parts[0].value,
            &parts[1].value,
        )),
        // schema.table.column → use table.column
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let len = parts.len();
            Some(ColumnRef::qualified(
                &parts[len - 2].value,
                &parts[len - 1].value,
            ))
        }
        _ => None,
    }
}

/// Check if a `ColumnRef` belongs to one of the given table names/aliases.
fn is_from_tables(col: &ColumnRef, tables: &HashSet<String>) -> bool {
    match &col.table {
        Some(t) => tables.iter().any(|name| name.eq_ignore_ascii_case(t)),
        // Without a qualifier, we can't determine membership.
        None => false,
    }
}

// ---------------------------------------------------------------------------
// GROUP BY key extraction
// ---------------------------------------------------------------------------

/// Extract column references from a GROUP BY clause for distribution planning.
fn extract_group_keys(group_by: &GroupByExpr) -> Vec<ColumnRef> {
    match group_by {
        GroupByExpr::All(_) => {
            // GROUP BY ALL: DuckDB expands this at runtime.
            // We can't determine group keys statically → empty keys.
            // This triggers conservative Singleton requirement.
            vec![]
        }
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .filter_map(expr_to_column_ref)
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Window / aggregate detection
// ---------------------------------------------------------------------------

/// Check if a `SelectItem` contains a window function (has OVER clause).
fn select_item_has_window(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_has_window(expr)
        }
        _ => false,
    }
}

/// Check if an expression contains a window function (recursive).
fn expr_has_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => func.over.is_some(),
        // Check common compound expressions.
        Expr::BinaryOp { left, right, .. } => expr_has_window(left) || expr_has_window(right),
        Expr::UnaryOp { expr, .. } => expr_has_window(expr),
        Expr::Nested(inner) => expr_has_window(inner),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|e| expr_has_window(e))
                || conditions.iter().any(|c| expr_has_window(&c.condition) || expr_has_window(&c.result))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_has_window(e))
        }
        _ => false,
    }
}

/// Check if a `SelectItem` contains an aggregate function (without OVER → not window).
fn select_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_has_aggregate(expr)
        }
        _ => false,
    }
}

/// Check if an expression contains an aggregate function.
///
/// This uses a heuristic: functions named SUM, COUNT, AVG, MIN, MAX, etc.
/// without an OVER clause are aggregates. This is intentionally conservative.
fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            // Only aggregate if no OVER clause (otherwise it's a window function).
            if func.over.is_some() {
                return false;
            }
            is_aggregate_function_name(&func.name)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Nested(inner) => expr_has_aggregate(inner),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|e| expr_has_aggregate(e))
                || conditions.iter().any(|c| expr_has_aggregate(&c.condition) || expr_has_aggregate(&c.result))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_has_aggregate(e))
        }
        _ => false,
    }
}

/// Well-known SQL aggregate function names.
const AGGREGATE_FUNCTIONS: &[&str] = &[
    "sum",
    "count",
    "avg",
    "min",
    "max",
    "count_star",
    "group_concat",
    "string_agg",
    "array_agg",
    "bool_and",
    "bool_or",
    "bit_and",
    "bit_or",
    "bit_xor",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "covar_pop",
    "covar_samp",
    "corr",
    "percentile_cont",
    "percentile_disc",
    "median",
    "mode",
    "approx_count_distinct",
    "approx_quantile",
    "first",
    "last",
    "any_value",
    "list",
    "histogram",
    "argmin",
    "argmax",
    "arg_min",
    "arg_max",
    "arbitrary",
    "kurtosis",
    "skewness",
    "entropy",
    "mad",
    "quantile",
    "reservoir_quantile",
    "regr_avgx", "regr_avgy", "regr_count", "regr_intercept",
    "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
];

fn is_aggregate_function_name(name: &ObjectName) -> bool {
    let name_str = object_name_to_string(name);
    AGGREGATE_FUNCTIONS
        .iter()
        .any(|agg| agg.eq_ignore_ascii_case(&name_str))
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Collect all table names/aliases reachable from a plan subtree.
///
/// Used for join key attribution (determining which side of a join a column
/// reference belongs to).
fn collect_table_names(plan: &LogicalPlan) -> HashSet<String> {
    let mut names = HashSet::new();
    collect_table_names_recursive(plan, &mut names);
    names
}

fn collect_table_names_recursive(plan: &LogicalPlan, names: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan {
            table_name, alias, ..
        } => {
            names.insert(table_name.clone());
            if let Some(a) = alias {
                names.insert(a.name.value.clone());
            }
        }
        LogicalPlan::SubqueryScan { alias, .. } => {
            names.insert(alias.name.value.clone());
        }
        LogicalPlan::ExchangeRead { alias, .. } => {
            names.insert(alias.clone());
        }
        _ => {
            for child in plan.children() {
                collect_table_names_recursive(child, names);
            }
        }
    }
}

/// Check if a Query's body has a FROM clause (references tables).
fn query_has_from(query: &Query) -> bool {
    match &*query.body {
        SetExpr::Select(select) => !select.from.is_empty(),
        SetExpr::Query(inner) => query_has_from(inner),
        SetExpr::SetOperation { left, right, .. } => {
            // Both sides of a set operation should have FROM
            set_expr_has_from(left) || set_expr_has_from(right)
        }
        _ => false,
    }
}

fn set_expr_has_from(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => !select.from.is_empty(),
        SetExpr::Query(inner) => query_has_from(inner),
        _ => false,
    }
}

/// Convert an `ObjectName` to a simple string (last identifier part).
fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
            // Handle function-style parts, etc.
            other => format!("{}", other),
        })
        .unwrap_or_default()
}

/// Get a short human-readable name for a SQL statement type.
fn statement_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        _ => "UNKNOWN",
    }
}

/// Collect all columns referenced in a plan subtree.
///
/// Used during stage cutting to derive the upstream stage's SELECT list
/// (§8.4 in the design doc). Walks the plan and collects column references
/// from predicates, projections, join keys, group-by keys, etc.
pub fn collect_referenced_columns(plan: &LogicalPlan) -> Vec<ColumnRef> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    collect_refs_recursive(plan, &mut columns, &mut seen);
    columns
}

fn collect_refs_recursive(
    plan: &LogicalPlan,
    columns: &mut Vec<ColumnRef>,
    seen: &mut HashSet<ColumnRef>,
) {
    match plan {
        LogicalPlan::Filter { predicate, .. } => {
            collect_columns_from_expr(predicate, columns, seen);
        }
        LogicalPlan::Project { items, .. } => {
            for item in items {
                collect_columns_from_select_item(item, columns, seen);
            }
        }
        LogicalPlan::Aggregate {
            group_keys,
            aggr_exprs,
            having,
            ..
        } => {
            for key in group_keys {
                add_column_ref(key.clone(), columns, seen);
            }
            for item in aggr_exprs {
                collect_columns_from_select_item(item, columns, seen);
            }
            if let Some(h) = having {
                collect_columns_from_expr(h, columns, seen);
            }
        }
        LogicalPlan::Sort { order_by, .. } => {
            for expr in order_by {
                collect_columns_from_expr(&expr.expr, columns, seen);
            }
        }
        LogicalPlan::Join {
            left_keys,
            right_keys,
            join_op,
            ..
        } => {
            for key in left_keys {
                add_column_ref(key.clone(), columns, seen);
            }
            for key in right_keys {
                add_column_ref(key.clone(), columns, seen);
            }
            // Also collect from ON clause expression.
            if let Some(on_expr) = super::extract_join_on_expr(join_op) {
                collect_columns_from_expr(on_expr, columns, seen);
            }
        }
        LogicalPlan::Window { window_exprs, .. } => {
            for item in window_exprs {
                collect_columns_from_select_item(item, columns, seen);
            }
        }
        _ => {}
    }

    // Recurse into children.
    for child in plan.children() {
        collect_refs_recursive(child, columns, seen);
    }
}

fn collect_columns_from_select_item(
    item: &SelectItem,
    columns: &mut Vec<ColumnRef>,
    seen: &mut HashSet<ColumnRef>,
) {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            collect_columns_from_expr(expr, columns, seen);
        }
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
            // Wildcards: we can't enumerate specific columns.
            // The SQL generator will preserve * as-is.
        }
    }
}

fn collect_columns_from_expr(
    expr: &Expr,
    columns: &mut Vec<ColumnRef>,
    seen: &mut HashSet<ColumnRef>,
) {
    match expr {
        Expr::Identifier(ident) => {
            add_column_ref(ColumnRef::unqualified(&ident.value), columns, seen);
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            add_column_ref(
                ColumnRef::qualified(&parts[0].value, &parts[1].value),
                columns,
                seen,
            );
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let len = parts.len();
            add_column_ref(
                ColumnRef::qualified(&parts[len - 2].value, &parts[len - 1].value),
                columns,
                seen,
            );
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns_from_expr(left, columns, seen);
            collect_columns_from_expr(right, columns, seen);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_columns_from_expr(expr, columns, seen);
        }
        Expr::Nested(inner) => {
            collect_columns_from_expr(inner, columns, seen);
        }
        Expr::Function(func) => {
            // Collect columns from function arguments.
            if let FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(arg_expr)
                        | sqlparser::ast::FunctionArg::Named { arg: arg_expr, .. }
                        | sqlparser::ast::FunctionArg::ExprNamed { arg: arg_expr, .. } => {
                            match arg_expr {
                                sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                    collect_columns_from_expr(e, columns, seen);
                                }
                                sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => {}
                                sqlparser::ast::FunctionArgExpr::Wildcard => {}
                            }
                        }
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_columns_from_expr(op, columns, seen);
            }
            for cond in conditions {
                collect_columns_from_expr(&cond.condition, columns, seen);
                collect_columns_from_expr(&cond.result, columns, seen);
            }
            if let Some(e) = else_result {
                collect_columns_from_expr(e, columns, seen);
            }
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            collect_columns_from_expr(e, columns, seen);
        }
        Expr::InList { expr, list, .. } => {
            collect_columns_from_expr(expr, columns, seen);
            for e in list {
                collect_columns_from_expr(e, columns, seen);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_columns_from_expr(expr, columns, seen);
            collect_columns_from_expr(low, columns, seen);
            collect_columns_from_expr(high, columns, seen);
        }
        Expr::Cast { expr, .. } => {
            collect_columns_from_expr(expr, columns, seen);
        }
        _ => {
            // Other expression types: literals, subqueries, etc.
            // Subqueries are handled as opaque — they don't cross exchange boundaries.
        }
    }
}

fn add_column_ref(col: ColumnRef, columns: &mut Vec<ColumnRef>, seen: &mut HashSet<ColumnRef>) {
    if seen.insert(col.clone()) {
        columns.push(col);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{SetOperator, SetQuantifier};

    fn parse_and_build(sql: &str) -> BuildResult {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            sql,
        )
        .unwrap();
        build_logical_plan(&stmts[0]).unwrap()
    }

    #[test]
    fn test_simple_select() {
        let result = parse_and_build("SELECT id, name FROM users WHERE age > 18");
        assert!(matches!(result.plan, LogicalPlan::Project { .. }));

        // Project wraps Filter wraps Scan.
        if let LogicalPlan::Project { input, items, .. } = &result.plan {
            assert_eq!(items.len(), 2);
            assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
            if let LogicalPlan::Filter { input, .. } = input.as_ref() {
                assert!(matches!(input.as_ref(), LogicalPlan::Scan { .. }));
            }
        }
    }

    #[test]
    fn test_select_with_join() {
        let sql = "SELECT o.id, l.price FROM orders o JOIN lineitem l ON o.id = l.order_id";
        let result = parse_and_build(sql);

        if let LogicalPlan::Project { input, .. } = &result.plan {
            if let LogicalPlan::Join {
                left_keys,
                right_keys,
                ..
            } = input.as_ref()
            {
                assert_eq!(left_keys.len(), 1);
                assert_eq!(right_keys.len(), 1);
                assert_eq!(left_keys[0].column, "id");
                assert_eq!(right_keys[0].column, "order_id");
            } else {
                panic!("expected Join");
            }
        } else {
            panic!("expected Project");
        }
    }

    #[test]
    fn test_aggregate() {
        let sql = "SELECT region, SUM(amount) FROM sales GROUP BY region";
        let result = parse_and_build(sql);

        if let LogicalPlan::Aggregate { group_keys, .. } = &result.plan {
            assert_eq!(group_keys.len(), 1);
            assert_eq!(group_keys[0].column, "region");
        } else {
            panic!("expected Aggregate, got {:?}", result.plan.node_name());
        }
    }

    #[test]
    fn test_order_by_limit() {
        let sql = "SELECT id FROM orders ORDER BY id LIMIT 10";
        let result = parse_and_build(sql);

        assert!(matches!(result.plan, LogicalPlan::Limit { .. }));
        if let LogicalPlan::Limit { input, .. } = &result.plan {
            assert!(matches!(input.as_ref(), LogicalPlan::Sort { .. }));
        }
    }

    #[test]
    fn test_cte_extraction() {
        let sql = "WITH cte AS (SELECT 1 AS x) SELECT x FROM cte";
        let result = parse_and_build(sql);

        assert_eq!(result.context.cte_definitions.len(), 1);
        assert_eq!(result.context.cte_definitions[0].alias.name.value, "cte");
    }

    #[test]
    fn test_union() {
        let sql = "SELECT id FROM t1 UNION ALL SELECT id FROM t2";
        let result = parse_and_build(sql);

        assert!(matches!(result.plan, LogicalPlan::SetOp { .. }));
        if let LogicalPlan::SetOp {
            set_quantifier, op, ..
        } = &result.plan
        {
            assert!(matches!(op, SetOperator::Union));
            assert!(matches!(set_quantifier, SetQuantifier::All));
        }
    }

    #[test]
    fn test_subquery_from() {
        let sql = "SELECT x FROM (SELECT id AS x FROM orders) sub";
        let result = parse_and_build(sql);

        // Project → SubqueryScan → ... (inner select)
        if let LogicalPlan::Project { input, .. } = &result.plan {
            assert!(
                matches!(input.as_ref(), LogicalPlan::SubqueryScan { .. }),
                "expected SubqueryScan, got {:?}",
                input.node_name()
            );
        }
    }

    #[test]
    fn test_multiple_join_keys() {
        let sql = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b";
        let result = parse_and_build(sql);

        if let LogicalPlan::Project { input, .. } = &result.plan {
            if let LogicalPlan::Join {
                left_keys,
                right_keys,
                ..
            } = input.as_ref()
            {
                assert_eq!(left_keys.len(), 2);
                assert_eq!(right_keys.len(), 2);
            } else {
                panic!("expected Join");
            }
        }
    }

    #[test]
    fn test_collect_referenced_columns() {
        let sql = "SELECT o.id, l.price FROM orders o JOIN lineitem l ON o.id = l.order_id WHERE o.status = 'active'";
        let result = parse_and_build(sql);

        let columns = collect_referenced_columns(&result.plan);
        let col_names: Vec<&str> = columns.iter().map(|c| c.column.as_str()).collect();

        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"price"));
        assert!(col_names.contains(&"order_id"));
        assert!(col_names.contains(&"status"));
    }

    #[test]
    fn test_cross_join_implicit() {
        let sql = "SELECT * FROM t1, t2";
        let result = parse_and_build(sql);

        if let LogicalPlan::Project { input, .. } = &result.plan {
            if let LogicalPlan::Join {
                join_op,
                left_keys,
                right_keys,
                ..
            } = input.as_ref()
            {
                assert!(matches!(join_op, JoinOperator::CrossJoin(_)));
                assert!(left_keys.is_empty());
                assert!(right_keys.is_empty());
            } else {
                panic!("expected Join");
            }
        }
    }

    #[test]
    fn test_unsupported_statement() {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            "CREATE TABLE t (id INT)",
        )
        .unwrap();
        let result = build_logical_plan(&stmts[0]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PlanBuildError::UnsupportedStatement(_)
        ));
    }

    #[test]
    fn test_window_function() {
        let sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders";
        let result = parse_and_build(sql);

        // Window functions → Window node.
        assert!(
            matches!(result.plan, LogicalPlan::Window { .. }),
            "expected Window, got {:?}",
            result.plan.node_name()
        );
    }

    #[test]
    fn test_join_using_extracts_keys() {
        let sql = "SELECT * FROM t1 JOIN t2 USING (id, dt)";
        let result = parse_and_build(sql);

        if let LogicalPlan::Project { input, .. } = &result.plan {
            if let LogicalPlan::Join {
                left_keys,
                right_keys,
                ..
            } = input.as_ref()
            {
                assert_eq!(left_keys.len(), 2);
                assert_eq!(right_keys.len(), 2);
                assert_eq!(left_keys[0].column, "id");
                assert_eq!(right_keys[0].column, "id");
                assert_eq!(left_keys[1].column, "dt");
                assert_eq!(right_keys[1].column, "dt");
            } else {
                panic!("expected Join");
            }
        } else {
            panic!("expected Project");
        }
    }

    #[test]
    fn test_collect_referenced_columns_deduplicates() {
        let sql = "SELECT t.id, t.id FROM t WHERE t.id > 1";
        let result = parse_and_build(sql);
        let columns = collect_referenced_columns(&result.plan);

        let id_count = columns.iter().filter(|c| c.column == "id").count();
        assert_eq!(id_count, 1);
    }
}
