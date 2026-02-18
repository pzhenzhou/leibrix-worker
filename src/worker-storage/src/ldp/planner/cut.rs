//! Stage Cutting Module
//!
//! This module takes an `AnnotatedPlan` (logical plan with distribution annotations) and "cuts" it into
//! executable stages by detecting exchange boundaries. Each stage is a fragment of the overall plan that
//! executes on a set of workers.
//!
//! The cutting algorithm operates bottom-up, creating stages whenever an exchange operator is encountered.
//! Exchange inputs are replaced with `LogicalPlan::ExchangeRead` placeholders, which will be populated
//! at runtime by the executor.
//!
//! The output is an `LdpPlan` containing:
//! - Stages: Executable query fragments (SQL strings)
//! - Edges: Exchange connections between stages
//! - Coordinator: The entry point worker
//! - Query ID: For tracking and debugging

use crate::ldp::planner::annotate::AnnotatedPlan;
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::types::{
    Exchange, ExchangeId, LdpPlan, QueryId, Stage, StageId, StageInput, StageOutput,
};
use crate::sql::logical_plan::{LogicalPlan, PlanContext};
use crate::sql::stage_sql_gen::{generate_stage_sql, generate_stage_sql_no_context};
use sqlparser::ast::{Expr, Ident, OrderByExpr, SelectItem, TableAlias};
use std::collections::HashMap;

/// Extract column expressions from ORDER BY clauses.
/// This returns a list of expressions that need to be in the SELECT for the exchange to work.
fn extract_order_by_columns(order_by: &[OrderByExpr]) -> Vec<Expr> {
    let mut columns = Vec::new();
    for order_expr in order_by {
        // Extract the base column reference from the ORDER BY expression
        extract_column_from_expr(&order_expr.expr, &mut columns);
    }
    columns
}

/// Recursively extract column references from an expression.
fn extract_column_from_expr(expr: &Expr, columns: &mut Vec<Expr>) {
    match expr {
        // Simple identifier: e.g., "o_totalprice"
        Expr::Identifier(ident) => {
            columns.push(Expr::Identifier(ident.clone()));
        }
        // Qualified identifier: e.g., "o.o_totalprice" -> extract "o.o_totalprice"
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            // Convert back to compound identifier expression
            let new_parts = parts.clone();
            columns.push(Expr::CompoundIdentifier(new_parts));
        }
        // Binary expressions: e.g., "o.totalprice > 50000"
        Expr::BinaryOp { left, op: _, right } => {
            extract_column_from_expr(left, columns);
            extract_column_from_expr(right, columns);
        }
        // Unary expressions
        Expr::UnaryOp { expr, .. } => {
            extract_column_from_expr(expr, columns);
        }
        // Nested expressions
        Expr::Nested(inner) => {
            extract_column_from_expr(inner, columns);
        }
        // For other expression types (functions, CASE, etc.), extract all column references
        _ => {
            extract_all_columns_from_expr(expr, columns);
        }
    }
}

/// Extract the column name from an expression for use as an alias.
/// This is used to ensure ORDER BY columns are available in the exchange output
/// with names that match what downstream stages expect (after dequalification).
fn get_column_alias(expr: &Expr) -> String {
    match expr {
        // Simple identifier: e.g., "o_totalprice" -> "o_totalprice"
        Expr::Identifier(ident) => ident.value.clone(),
        // Qualified identifier: e.g., "o.o_totalprice" -> "o_totalprice" (dequalified)
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            // Take the last part (column name)
            parts.last().map(|p| p.value.clone()).unwrap_or_else(|| "col".to_string())
        }
        // For other expressions, return a placeholder
        _ => "order_by_col".to_string(),
    }
}

/// Extract all column references from any expression (fallback for complex expressions).
fn extract_all_columns_from_expr(expr: &Expr, columns: &mut Vec<Expr>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.push(Expr::Identifier(ident.clone()));
        }
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            columns.push(Expr::CompoundIdentifier(parts.clone()));
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_all_columns_from_expr(left, columns);
            extract_all_columns_from_expr(right, columns);
        }
        Expr::UnaryOp { expr, .. } => {
            extract_all_columns_from_expr(expr, columns);
        }
        Expr::Nested(inner) => {
            extract_all_columns_from_expr(inner, columns);
        }
        Expr::Case { operand, conditions, else_result, .. } => {
            // For CASE expressions, extract columns from all branches
            if let Some(op) = operand {
                extract_all_columns_from_expr(op, columns);
            }
            for case_when in conditions {
                extract_all_columns_from_expr(&case_when.condition, columns);
                extract_all_columns_from_expr(&case_when.result, columns);
            }
            if let Some(e) = else_result {
                extract_all_columns_from_expr(e, columns);
            }
        }
        Expr::Function(func) => {
            // Extract columns from function arguments
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        extract_all_columns_from_expr(e, columns);
                    }
                }
            }
        }
        Expr::Cast { expr, .. } => {
            extract_all_columns_from_expr(expr, columns);
        }
        Expr::InList { expr, list, .. } => {
            extract_all_columns_from_expr(expr, columns);
            for item in list {
                extract_all_columns_from_expr(item, columns);
            }
        }
        _ => {}
    }
}

/// Extract the output column name from a SelectItem for deduplication.
fn get_output_column_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                Some(parts.last().unwrap().value.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

/// Add ORDER BY columns to a LogicalPlan by wrapping it in a Project if needed.
/// This ensures that when the plan is executed and sent through an exchange,
/// the ORDER BY columns are available in the output.
fn add_columns_to_plan(mut plan: LogicalPlan, columns: Vec<Expr>) -> LogicalPlan {
    if columns.is_empty() {
        return plan;
    }

    // Check if the plan already has a Project; if so, add columns to it
    // Otherwise, wrap in a new Project
    match &mut plan {
        LogicalPlan::Project { items, input: _ } => {
            // Add the ORDER BY columns to the existing project, but skip duplicates.
            // A column is considered duplicate if an existing SelectItem already outputs
            // the same dequalified name (e.g., o_custkey already present as
            // `o1.o_custkey` or `o_custkey`).
            for col in columns {
                let alias = get_column_alias(&col);
                let already_exists = items.iter().any(|item| {
                    get_output_column_name(item)
                        .map(|name| name.eq_ignore_ascii_case(&alias))
                        .unwrap_or(false)
                });
                if !already_exists {
                    items.push(SelectItem::ExprWithAlias {
                        expr: col,
                        alias: Ident::new(alias),
                    });
                }
            }
            plan
        }
        LogicalPlan::Aggregate { aggr_exprs, input: _, .. } => {
            // Add the ORDER BY columns to the aggregate's SELECT
            for col in columns {
                let col_str = format!("{}", col);
                let already_exists = aggr_exprs.iter().any(|item| {
                    format!("{}", item).contains(&col_str)
                });
                if !already_exists {
                    aggr_exprs.push(SelectItem::UnnamedExpr(col));
                }
            }
            plan
        }
        LogicalPlan::Filter { input, .. } => {
            // For Filter, recurse into input
            *input = Box::new(add_columns_to_plan(*input.clone(), columns));
            plan
        }
        LogicalPlan::Join { .. } => {
            // For Join, wrap the Join in a Project that selects * plus the ORDER BY columns.
            // This ensures the exchange output has both original columns AND ORDER BY columns.
            let mut new_items = vec![SelectItem::Wildcard(Default::default())];  // SELECT *
            for col in columns {
                let alias = get_column_alias(&col);
                new_items.push(SelectItem::ExprWithAlias {
                    expr: col,
                    alias: Ident::new(alias),
                });
            }
            LogicalPlan::Project {
                items: new_items,
                input: Box::new(plan),
            }
        }
        LogicalPlan::Sort { input, .. } => {
            // For Sort, add columns to its input
            *input = Box::new(add_columns_to_plan(*input.clone(), columns));
            plan
        }
        LogicalPlan::Limit { input, .. } => {
            // For Limit, add columns to its input
            *input = Box::new(add_columns_to_plan(*input.clone(), columns));
            plan
        }
        LogicalPlan::Window { input, .. } => {
            // For Window, add columns to its input
            *input = Box::new(add_columns_to_plan(*input.clone(), columns));
            plan
        }
        LogicalPlan::SubqueryScan { input, .. } => {
            // For SubqueryScan, add columns to its input
            *input = Box::new(add_columns_to_plan(*input.clone(), columns));
            plan
        }
        _ => {
            // For other plan types (Scan, ExchangeRead), wrap in a new Project
            LogicalPlan::Project {
                items: columns
                    .into_iter()
                    .map(SelectItem::UnnamedExpr)
                    .collect(),
                input: Box::new(plan),
            }
        }
    }
}

/// Extract the Project items from a plan by walking through Limit/Sort wrappers.
/// Used to determine the original output columns when we need to strip extra ORDER BY columns.
fn extract_inner_projection_items(plan: &LogicalPlan) -> Option<Vec<SelectItem>> {
    match plan {
        LogicalPlan::Project { items, .. } => Some(items.clone()),
        LogicalPlan::Limit { input, .. } => extract_inner_projection_items(input),
        LogicalPlan::Sort { input, .. } => extract_inner_projection_items(input),
        _ => None,
    }
}

/// Check if an AnnotatedPlan or any of its descendants has an exchange_before set.
fn has_exchange_in_descendants(annotated: &AnnotatedPlan) -> bool {
    if annotated.exchange_before.is_some() {
        return true;
    }
    for child in &annotated.children {
        if has_exchange_in_descendants(child) {
            return true;
        }
    }
    false
}

/// Walk the annotated plan tree and add ORDER BY columns to the `.plan` field
/// of any child that has `exchange_before` set.  This ensures that when
/// `extract_stage_plan` later calls `cut_recursive(child)`, the child's plan
/// (which becomes the upstream stage's SQL) includes the extra columns.
///
/// This function only modifies the plan for nodes that are already Project or
/// Aggregate (where we can safely add columns without changing structure).
/// For other node types, we recurse to find Project/Aggregate descendants with exchanges.
fn add_order_by_columns_to_exchange_children(
    annotated: &mut AnnotatedPlan,
    columns: &[Expr],
) {
    for child in &mut annotated.children {
        if child.exchange_before.is_some() {
            // Only modify if it's safe to do so without changing structure
            // Project: add to items (safe)
            // Aggregate: add to aggr_exprs (safe)
            // Other: could wrap in Project - unsafe, skip
            let modified_plan = add_columns_to_plan_safe(&child.plan, columns.to_vec());
            if let Some(new_plan) = modified_plan {
                child.plan = new_plan;
            }
        } else {
            add_order_by_columns_to_exchange_children(child, columns);
        }
    }
}

/// Add columns to a plan, but only if it won't change the structure.
/// Returns Some(plan) if safe to modify, None if it would require wrapping.
fn add_columns_to_plan_safe(plan: &LogicalPlan, columns: Vec<Expr>) -> Option<LogicalPlan> {
    if columns.is_empty() {
        return None;
    }
    match plan {
        LogicalPlan::Project { .. } | LogicalPlan::Aggregate { .. } => {
            // Safe: can add columns without changing node type
            Some(add_columns_to_plan(plan.clone(), columns))
        }
        _ => {
            // Unsafe: would need to wrap in Project, changing structure
            // Recurse to find a safe target
            None
        }
    }
}

/// Entry point: cut an annotated plan into stages.
///
/// Returns an `LdpPlan` with stages, edges, and metadata.
pub fn cut_into_stages(
    root: &AnnotatedPlan,
    policy: &PlannerPolicy,
    query_id: &str,
    context: &PlanContext,
) -> LdpPlan {
    let mut ctx = CutContext::new(context.clone());
    let mut plan = LdpPlan {
        query_id: QueryId::from(query_id),
        coordinator: policy.coordinator.clone(),
        stages: Vec::new(),
        edges: Vec::new(),
        root_stage: StageId(0), // Updated below after cutting
    };

    // Recursively cut the plan – the returned stage_id is the root
    let final_stage_id = cut_recursive(root, &mut plan, &mut ctx, policy);
    plan.root_stage = final_stage_id;

    // Assign target workers to stages based on distribution annotations
    assign_stage_workers(&mut plan, root, &ctx);

    // The root stage produces the final output for the client.
    // It must use Stream output regardless of its distribution annotation,
    // because there is no downstream exchange that would consume partitioned data.
    if let Some(root_stage) = plan.stages.iter_mut().find(|s| s.stage_id == plan.root_stage) {
        root_stage.output = StageOutput::Stream;
    }

    plan
}

/// Context for tracking stage/exchange IDs during cutting.
struct CutContext {
    next_stage_id: StageId,
    next_exchange_id: ExchangeId,
    /// Map stage IDs to their distribution annotations for worker assignment
    stage_annotations: HashMap<StageId, crate::ldp::DistributionAnnotation>,
    /// CTE definitions from the original query, prepended to each stage SQL.
    plan_context: PlanContext,
}

impl CutContext {
    fn new(plan_context: PlanContext) -> Self {
        Self {
            next_stage_id: StageId(0),
            next_exchange_id: ExchangeId(0),
            stage_annotations: HashMap::new(),
            plan_context,
        }
    }

    fn alloc_stage_id(&mut self) -> StageId {
        self.next_stage_id.alloc_next()
    }

    fn alloc_exchange_id(&mut self) -> ExchangeId {
        self.next_exchange_id.alloc_next()
    }
}

/// Recursively cut the plan at exchange boundaries.
///
/// Returns the stage ID of the created stage.
///
/// **Important**: Exchange edges are *deferred* until this node's stage ID is
/// allocated. When a join has two children that both require exchanges, each
/// child's `cut_recursive` call can allocate intermediate stage IDs.  If we
/// eagerly created edges inside the child loop we would pre-allocate a
/// `to_stage` for each child separately, but only the last one would match
/// the actual stage ID — leaving earlier edges pointing at non-existent stages.
fn cut_recursive(
    annotated: &AnnotatedPlan,
    plan: &mut LdpPlan,
    ctx: &mut CutContext,
    policy: &PlannerPolicy,
) -> StageId {
    // Process children first (bottom-up)
    let mut child_plans_for_stage = Vec::new();

    // Collect pending edges: (exchange_id, from_stage, Exchange kind).
    // Edges are deferred because we don't know this node's stage_id yet.
    let mut pending_edges: Vec<(ExchangeId, StageId, Exchange)> = Vec::new();

    // If this node is a Sort (or Limit wrapping Sort), extract ORDER BY columns to add to exchange outputs
    let sort_order_by_columns = match &annotated.plan {
        LogicalPlan::Sort { order_by, .. } => {
            extract_order_by_columns(order_by)
        }
        LogicalPlan::Limit { input, .. } => {
            if let LogicalPlan::Sort { order_by, .. } = input.as_ref() {
                extract_order_by_columns(order_by)
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    };

    // Check if the current node is Sort and we need to add ORDER BY columns to exchange outputs
    let needs_order_by_columns = !sort_order_by_columns.is_empty();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // Cut here: child becomes a separate stage
            // If current node is Sort, we need to add ORDER BY columns to child's plan
            // to ensure they're included in the exchange output
            let mut child_to_process = child.clone();
            if needs_order_by_columns {
                child_to_process.plan = add_columns_to_plan(child.plan.clone(), sort_order_by_columns.clone());
            }

            let from_stage = cut_recursive(&child_to_process, plan, ctx, policy);
            let exchange_id = ctx.alloc_exchange_id();

            // Defer edge creation until we know our own stage_id
            pending_edges.push((exchange_id, from_stage, child.exchange_before.clone().unwrap()));

            // Replace child with ExchangeRead placeholder
            let exchange_table_name = format!("__exchange_{}", exchange_id);
            let exchange_read = LogicalPlan::ExchangeRead {
                exchange_id: exchange_id.0,
                alias: exchange_table_name,
            };
            if let Some(relation_alias) = derive_relation_alias(&child.plan) {
                child_plans_for_stage.push(LogicalPlan::SubqueryScan {
                    input: Box::new(exchange_read),
                    alias: relation_alias,
                });
            } else {
                child_plans_for_stage.push(exchange_read);
            }
        } else {
            // No exchange at this level: recursively process and include in current stage.
            // extract_stage_plan may itself encounter exchange boundaries deeper
            // in the tree; those edges also target *this* stage.
            // If we have ORDER BY columns to add AND there are exchanges in descendants,
            // walk the annotated tree and modify the `.plan` of any descendant that
            // has `exchange_before` set.
            let mut child_to_process = child.clone();
            if needs_order_by_columns && has_exchange_in_descendants(child) {
                add_order_by_columns_to_exchange_children(
                    &mut child_to_process,
                    &sort_order_by_columns,
                );
            }

            let (child_plan, child_edges) = extract_stage_plan(&child_to_process, plan, ctx, policy);
            pending_edges.extend(child_edges);
            child_plans_for_stage.push(child_plan);
        }
    }

    // Build the logical plan for this stage
    let mut stage_plan = reconstruct_plan_with_children(&annotated.plan, child_plans_for_stage);

    // If we added ORDER BY columns to exchange children, the coordinator stage may output
    // extra columns. Wrap the plan in a Project that restores the original output columns.
    if needs_order_by_columns {
        if let Some(original_items) = extract_inner_projection_items(&annotated.plan) {
            let wrapper_items: Vec<SelectItem> = original_items
                .iter()
                .filter_map(|item| {
                    get_output_column_name(item).map(|name| {
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(name)))
                    })
                })
                .collect();
            // Only wrap if we could resolve all column names
            if wrapper_items.len() == original_items.len() {
                stage_plan = LogicalPlan::Project {
                    items: wrapper_items,
                    input: Box::new(stage_plan),
                };
            }
        }
    }

    // Generate SQL for this stage.
    // Only include CTE definitions if the stage plan references CTE names.
    let stage_context = filter_ctes_for_stage(&stage_plan, &ctx.plan_context);
    let stage_sql = generate_stage_sql(&stage_plan, &stage_context);

    // Determine stage inputs by walking the plan for exchange reads
    let mut inputs: Vec<StageInput> = collect_exchange_reads(&stage_plan)
        .into_iter()
        .map(|(exchange_id, alias)| StageInput::ExchangeInput {
            exchange_id: ExchangeId(exchange_id),
            table_name: alias,
        })
        .collect();
    if has_local_scan(&stage_plan) {
        inputs.push(StageInput::LocalCatalog);
    }

    // Determine stage output format
    let output = determine_stage_output(annotated);

    // Allocate stage ID for this node (ONCE, after all children are processed)
    let stage_id = ctx.alloc_stage_id();

    // Now create all deferred exchange edges with the correct to_stage
    for (exchange_id, from_stage, kind) in pending_edges {
        let edge = crate::ldp::types::ExchangeEdge {
            exchange_id,
            from_stage,
            to_stage: stage_id,
            kind,
            partition_to_worker: Vec::new(), // Assigned later by assign_stage_workers
        };
        plan.edges.push(edge);
    }

    let stage = Stage {
        stage_id,
        stage_sql,
        inputs,
        output,
        target_workers: Vec::new(), // Assigned later
        limits: crate::ldp::types::StageLimits::default(),
    };

    // Record annotation for worker assignment.
    // If this node has an exchange_before, the annotation's distribution was
    // overwritten to the POST-exchange distribution (what the parent sees).
    // The stage executes BEFORE the exchange, so use the pre-exchange
    // distribution for correct worker targeting.
    let stage_annotation = if let Some(pre_dist) = &annotated.pre_exchange_distribution {
        let mut ann = annotated.annotation.clone();
        ann.distribution = pre_dist.clone();
        ann
    } else {
        annotated.annotation.clone()
    };
    ctx.stage_annotations.insert(stage_id, stage_annotation);

    plan.stages.push(stage);

    stage_id
}

/// Extract a logical plan for a stage, recursively replacing exchange children with placeholders.
///
/// Returns `(plan, pending_edges)` where `pending_edges` contains exchange edges whose
/// `to_stage` must be set by the caller (the stage that will contain this plan fragment).
fn extract_stage_plan(
    annotated: &AnnotatedPlan,
    plan: &mut LdpPlan,
    ctx: &mut CutContext,
    policy: &PlannerPolicy,
) -> (LogicalPlan, Vec<(ExchangeId, StageId, Exchange)>) {
    if annotated.children.is_empty() {
        return (annotated.plan.clone(), vec![]);
    }

    let mut new_children = Vec::new();
    let mut pending_edges: Vec<(ExchangeId, StageId, Exchange)> = Vec::new();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // Create exchange edge and placeholder
            let from_stage = cut_recursive(child, plan, ctx, policy);
            let exchange_id = ctx.alloc_exchange_id();

            // Defer edge creation – caller will set the correct to_stage
            pending_edges.push((exchange_id, from_stage, child.exchange_before.clone().unwrap()));

            // Create placeholder
            let exchange_table_name = format!("__exchange_{}", exchange_id);
            let placeholder = LogicalPlan::ExchangeRead {
                exchange_id: exchange_id.0,
                alias: exchange_table_name,
            };
            if let Some(relation_alias) = derive_relation_alias(&child.plan) {
                new_children.push(LogicalPlan::SubqueryScan {
                    input: Box::new(placeholder),
                    alias: relation_alias,
                });
            } else {
                new_children.push(placeholder);
            }
        } else {
            // Recursively extract; collect edges from deeper levels
            let (child_plan, child_edges) = extract_stage_plan(child, plan, ctx, policy);
            pending_edges.extend(child_edges);
            new_children.push(child_plan);
        }
    }

    (reconstruct_plan_with_children(&annotated.plan, new_children), pending_edges)
}

/// Reconstruct a LogicalPlan with new children.
///
/// This preserves the operator type but replaces child subplans.
fn reconstruct_plan_with_children(
    plan: &LogicalPlan,
    new_children: Vec<LogicalPlan>,
) -> LogicalPlan {
    use LogicalPlan::*;

    match plan {
        Scan {
            table_name,
            alias,
            table_factor,
        } => {
            // Scan has no children
            Scan {
                table_name: table_name.clone(),
                alias: alias.clone(),
                table_factor: table_factor.clone(),
            }
        }
        Filter { predicate, input: _ } => Filter {
            predicate: predicate.clone(),
            input: Box::new(new_children[0].clone()),
        },
        Project { items, input: _ } => Project {
            items: items.clone(),
            input: Box::new(new_children[0].clone()),
        },
        Aggregate {
            group_by,
            aggr_exprs,
            having,
            group_keys,
            input: _,
        } => Aggregate {
            group_by: group_by.clone(),
            aggr_exprs: aggr_exprs.clone(),
            having: having.clone(),
            group_keys: group_keys.clone(),
            input: Box::new(new_children[0].clone()),
        },
        Sort { order_by, input: _ } => Sort {
            order_by: order_by.clone(),
            input: Box::new(new_children[0].clone()),
        },
        Limit {
            limit,
            offset,
            input: _,
        } => Limit {
            limit: limit.clone(),
            offset: offset.clone(),
            input: Box::new(new_children[0].clone()),
        },
        Join {
            join_op,
            left: _,
            right: _,
            left_keys,
            right_keys,
        } => Join {
            join_op: join_op.clone(),
            left: Box::new(new_children[0].clone()),
            right: Box::new(new_children[1].clone()),
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
        },
        SetOp {
            op,
            set_quantifier,
            left: _,
            right: _,
        } => SetOp {
            op: *op,
            set_quantifier: *set_quantifier,
            left: Box::new(new_children[0].clone()),
            right: Box::new(new_children[1].clone()),
        },
        Window {
            window_exprs,
            input: _,
        } => Window {
            window_exprs: window_exprs.clone(),
            input: Box::new(new_children[0].clone()),
        },
        SubqueryScan { alias, input: _ } => SubqueryScan {
            alias: alias.clone(),
            input: Box::new(new_children[0].clone()),
        },
        ExchangeRead {
            exchange_id,
            alias,
        } => {
            // ExchangeRead has no children
            ExchangeRead {
                exchange_id: *exchange_id,
                alias: alias.clone(),
            }
        }
    }
}

/// Filter CTE definitions for a stage.
///
/// Returns the full CTE context if the stage plan references any CTE name,
/// otherwise returns an empty context. We include all CTEs rather than filtering
/// individually because CTEs may reference each other (e.g., CTE B references CTE A).
fn filter_ctes_for_stage(plan: &LogicalPlan, context: &PlanContext) -> PlanContext {
    if context.cte_definitions.is_empty() {
        return PlanContext::default();
    }

    let scan_names = collect_scan_names(plan);
    let any_cte_referenced = context.cte_definitions.iter().any(|cte| {
        scan_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&cte.alias.name.value))
    });

    if any_cte_referenced {
        context.clone()
    } else {
        PlanContext::default()
    }
}

/// Returns true when a plan contains only exchange reads — no local catalog scans.
/// Used to decide whether a SubqueryScan alias should be treated as a CTE reference.
fn is_purely_exchange_backed(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ExchangeRead { .. } => true,
        LogicalPlan::Scan { .. } => false,
        LogicalPlan::SubqueryScan { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Window { input, .. } => is_purely_exchange_backed(input),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SetOp { left, right, .. } => {
            is_purely_exchange_backed(left) && is_purely_exchange_backed(right)
        }
    }
}

/// Collect all table names from Scan nodes in a plan.
fn collect_scan_names(plan: &LogicalPlan) -> Vec<String> {
    let mut names = Vec::new();
    collect_scan_names_recursive(plan, &mut names);
    names
}

fn collect_scan_names_recursive(plan: &LogicalPlan, names: &mut Vec<String>) {
    use LogicalPlan::*;

    match plan {
        Scan { table_name, .. } => {
            names.push(table_name.clone());
        }
        ExchangeRead { .. } => {}
        Filter { input, .. }
        | Project { input, .. }
        | Aggregate { input, .. }
        | Sort { input, .. }
        | Limit { input, .. }
        | Window { input, .. } => collect_scan_names_recursive(input, names),
        SubqueryScan { alias, input } => {
            // Only treat the alias as a potential CTE reference when the input
            // contains at least one local scan.  If the subquery wraps an
            // ExchangeRead (the exchange-backed case produced by stage cutting),
            // the SubqueryScan alias is just a table alias for the exchange temp
            // table — it does NOT reference a CTE body that needs to be re-emitted.
            if !is_purely_exchange_backed(input) {
                names.push(alias.name.value.clone());
            }
            collect_scan_names_recursive(input, names);
        }
        Join { left, right, .. } | SetOp { left, right, .. } => {
            collect_scan_names_recursive(left, names);
            collect_scan_names_recursive(right, names);
        }
    }
}

/// Collect all ExchangeRead nodes from a plan (exchange_id, alias).
fn collect_exchange_reads(plan: &LogicalPlan) -> Vec<(u32, String)> {
    let mut result = Vec::new();
    collect_exchange_reads_recursive(plan, &mut result);
    result
}

fn collect_exchange_reads_recursive(plan: &LogicalPlan, result: &mut Vec<(u32, String)>) {
    use LogicalPlan::*;

    match plan {
        ExchangeRead { exchange_id, alias } => {
            result.push((*exchange_id, alias.clone()));
        }
        Scan { .. } => {}
        Filter { input, .. }
        | Project { input, .. }
        | Aggregate { input, .. }
        | Sort { input, .. }
        | Limit { input, .. }
        | Window { input, .. }
        | SubqueryScan { input, .. } => collect_exchange_reads_recursive(input, result),
        Join { left, right, .. } | SetOp { left, right, .. } => {
            collect_exchange_reads_recursive(left, result);
            collect_exchange_reads_recursive(right, result);
        }
    }
}

/// Derive a stable relation alias for a plan fragment.
///
/// This preserves original table aliases across exchange boundaries so join
/// predicates like `o.col = l.col` continue to bind after cutting.
fn derive_relation_alias(plan: &LogicalPlan) -> Option<TableAlias> {
    match plan {
        LogicalPlan::Scan {
            table_name, alias, ..
        } => alias.clone().or_else(|| {
            Some(TableAlias {
                name: Ident::new(table_name),
                columns: vec![],
                explicit: true,
            })
        }),
        LogicalPlan::SubqueryScan { alias, .. } => Some(alias.clone()),
        _ => None,
    }
}

/// Check if an AnnotatedPlan or any of its descendants has an exchange_before set.
/// This is used to detect nested exchanges (e.g., Sort -> Project -> Join, where Join has exchange_before).
#[allow(dead_code)]
fn annotated_plan_has_exchange(annotated: &AnnotatedPlan) -> bool {
    // Check if this node has an exchange
    if annotated.exchange_before.is_some() {
        return true;
    }

    // Recursively check children
    for child in &annotated.children {
        if annotated_plan_has_exchange(child) {
            return true;
        }
    }

    false
}

/// Check if a plan has any Scan nodes (indicates it reads from local catalog).
fn has_local_scan(plan: &LogicalPlan) -> bool {
    use LogicalPlan::*;

    match plan {
        Scan { .. } => true,
        ExchangeRead { .. } => false,
        Filter { input, .. }
        | Project { input, .. }
        | Aggregate { input, .. }
        | Sort { input, .. }
        | Limit { input, .. }
        | Window { input, .. }
        | SubqueryScan { input, .. } => has_local_scan(input),
        Join { left, right, .. } | SetOp { left, right, .. } => {
            has_local_scan(left) || has_local_scan(right)
        }
    }
}

/// Determine the output format for a stage.
fn determine_stage_output(annotated: &AnnotatedPlan) -> StageOutput {
    use crate::ldp::Distribution;

    // If the annotated node's output distribution is hash partitioned, mark the stage output
    // as partitioned to align with downstream consumers.
    if let Distribution::HashPartitioned { column_refs, .. } = &annotated.annotation.distribution {
        let partitions = annotated
            .exchange_before
            .as_ref()
            .and_then(|ex| {
                if let Exchange::HashPartition { partitions, .. } = ex {
                    Some(*partitions)
                } else {
                    None
                }
            })
            .unwrap_or_else(policy_default_partitions);

        return StageOutput::Partitioned {
            partitions,
            column_refs: column_refs.clone(),
        };
    }

    // If this annotated node has an exchange_before, it will produce partitioned output
    // for hash partition exchanges
    if let Some(Exchange::HashPartition {
            partitions,
            column_refs,
            ..
        }) = &annotated.exchange_before
    {
        return StageOutput::Partitioned {
            partitions: *partitions,
            column_refs: column_refs.clone(),
        };
    }

    // Default to stream output
    StageOutput::Stream
}

/// Default partitions helper (matches PlannerPolicy default).
fn policy_default_partitions() -> u32 {
    crate::ldp::planner::policy::PlannerPolicy::default().default_partitions
}

/// Assign target workers to stages based on distribution annotations.
fn assign_stage_workers(
    plan: &mut LdpPlan,
    _root_annotated: &AnnotatedPlan,
    ctx: &CutContext,
) {
    for stage in &mut plan.stages {
        if let Some(ann_ref) = ctx.stage_annotations.get(&stage.stage_id) {
            let mut workers = ann_ref.distribution.workers().to_vec();
            // Deduplicate workers: each worker should execute the stage only once.
            // Duplicates arise when metadata is unavailable and the planner fills
            // all partition slots with the coordinator.
            workers.sort();
            workers.dedup();
            stage.target_workers = workers;
        }

        // If no workers assigned (shouldn't happen), use coordinator
        if stage.target_workers.is_empty() {
            stage.target_workers = vec![plan.coordinator.clone()];
        }
    }

    // Also update partition_to_worker for hash partition exchanges
    for edge in &mut plan.edges {
        if let Exchange::HashPartition { partitions, .. } = &edge.kind {
            // For hash partition, assign partitions to available workers
            if let Some(to_stage) = plan.stages.iter().find(|s| s.stage_id == edge.to_stage) {
                let workers = &to_stage.target_workers;
                let mut partition_workers = Vec::with_capacity(*partitions as usize);

                for i in 0..*partitions {
                    // Round-robin assignment
                    let worker = &workers[i as usize % workers.len()];
                    partition_workers.push(worker.clone());
                }

                edge.partition_to_worker = partition_workers;
            }
        }
    }
}

/// Extract SQL for a stage plan (public helper).
///
/// This is a simpler version for cases where you just need the SQL string without full stage metadata.
pub fn extract_stage_sql(annotated: &AnnotatedPlan) -> String {
    let plan = extract_plan_with_placeholders(annotated, &mut 0);
    generate_stage_sql_no_context(&plan)
}

/// Extract LogicalPlan with exchange children replaced by ExchangeRead placeholders.
fn extract_plan_with_placeholders(
    annotated: &AnnotatedPlan,
    exchange_counter: &mut u32,
) -> LogicalPlan {
    if annotated.children.is_empty() {
        return annotated.plan.clone();
    }

    let mut new_children = Vec::new();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // Replace with placeholder
            let exchange_table_name = format!("__exchange_{}", *exchange_counter);
            let placeholder = LogicalPlan::ExchangeRead {
                exchange_id: *exchange_counter,
                alias: exchange_table_name,
            };
            *exchange_counter += 1;
            new_children.push(placeholder);
        } else {
            // Recursively process
            let child_plan = extract_plan_with_placeholders(child, exchange_counter);
            new_children.push(child_plan);
        }
    }

    reconstruct_plan_with_children(&annotated.plan, new_children)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::planner::annotate::AnnotatedPlan;
    use crate::ldp::planner::policy::PlannerPolicy;
    use crate::ldp::{Distribution, DistributionAnnotation, StatsSource};
    use crate::ldp::types::WorkerId;
    use crate::sql::logical_plan::{ColumnRef, LogicalPlan};
    use sqlparser::ast::Expr;

    /// Check if a plan has any ExchangeRead nodes (used only in tests).
    fn has_exchange_read(plan: &LogicalPlan) -> bool {
        use LogicalPlan::*;
        match plan {
            ExchangeRead { .. } => true,
            Scan { .. } => false,
            Filter { input, .. }
            | Project { input, .. }
            | Aggregate { input, .. }
            | Sort { input, .. }
            | Limit { input, .. }
            | Window { input, .. }
            | SubqueryScan { input, .. } => has_exchange_read(input),
            Join { left, right, .. } | SetOp { left, right, .. } => {
                has_exchange_read(left) || has_exchange_read(right)
            }
        }
    }

    fn test_policy() -> PlannerPolicy {
        PlannerPolicy::with_coordinator("coordinator")
    }

    fn create_scan_annotated(table: &str, workers: Vec<&str>) -> AnnotatedPlan {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, TableFactor};
        let plan = LogicalPlan::Scan {
            table_name: table.into(),
            alias: None,
            table_factor: TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table))]),
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
        };
        let workers: Vec<WorkerId> = workers.into_iter().map(WorkerId::from).collect();
        let dist = if workers.len() == 1 {
            Distribution::Singleton {
                worker: workers[0].clone(),
            }
        } else {
            Distribution::EpochPartitioned {
                workers: workers.clone(),
            }
        };

        AnnotatedPlan::new(
            plan,
            DistributionAnnotation::with_source(dist, 1000, 10000, StatsSource::Exact),
            vec![],
        )
    }

    fn create_filter_annotated(input: AnnotatedPlan) -> AnnotatedPlan {
        let plan = LogicalPlan::Filter {
            predicate: Expr::Value(sqlparser::ast::Value::Boolean(true).into()), // Dummy predicate
            input: Box::new(input.plan.clone()),
        };

        AnnotatedPlan::new(plan, input.annotation.clone(), vec![input])
    }

    #[test]
    fn test_simple_no_exchange() {
        let policy = test_policy();

        // Simple plan: Scan -> Filter (no exchange needed)
        let scan = create_scan_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(scan);

        let plan = cut_into_stages(&filter, &policy, "q1", &PlanContext::default());

        // Should be 1 stage, no exchanges
        assert_eq!(plan.stages.len(), 1);
        assert!(plan.edges.is_empty());
        assert_eq!(plan.stages[0].stage_id, StageId(0));
    }

    #[test]
    fn test_with_gather_exchange() {
        let policy = test_policy();

        // Scan (distributed) -> Sort (needs gather)
        let mut scan = create_scan_annotated("sales", vec!["w1", "w2"]);

        // Create sort that needs Singleton
        let sort_plan = LogicalPlan::Sort {
            order_by: vec![],
            input: Box::new(scan.plan.clone()),
        };

        // Mark exchange on the scan child
        scan.exchange_before = Some(Exchange::Gather {
            target: "coordinator".into(),
        });

        // Update scan's distribution to reflect post-exchange state
        scan.annotation.distribution = Distribution::Singleton {
            worker: "coordinator".into(),
        };

        let sort = AnnotatedPlan::new(
            sort_plan,
            DistributionAnnotation::with_source(
                Distribution::Singleton {
                    worker: "coordinator".into(),
                },
                1000,
                10000,
                StatsSource::Exact,
            ),
            vec![scan],
        );

        let plan = cut_into_stages(&sort, &policy, "q1", &PlanContext::default());

        // Should be 2 stages with 1 exchange
        assert_eq!(plan.stages.len(), 2, "Expected 2 stages");
        assert_eq!(plan.edges.len(), 1, "Expected 1 exchange edge");

        // Verify exchange edge
        let edge = &plan.edges[0];
        assert!(matches!(edge.kind, Exchange::Gather { .. }));
        assert_eq!(edge.from_stage, StageId(0));
        assert_eq!(edge.to_stage, StageId(1));
    }

    #[test]
    fn test_extract_stage_sql() {
        let scan = create_scan_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(scan);

        let sql = extract_stage_sql(&filter);
        assert!(!sql.is_empty());
        // SQL should contain table reference
        assert!(sql.to_lowercase().contains("sales"));
    }

    #[test]
    fn test_stage_inputs() {
        let policy = test_policy();
        let scan = create_scan_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(scan);

        let plan = cut_into_stages(&filter, &policy, "q1", &PlanContext::default());

        // Single stage should have LocalCatalog input
        assert_eq!(plan.stages[0].inputs.len(), 1);
        assert!(matches!(
            plan.stages[0].inputs[0],
            StageInput::LocalCatalog
        ));
    }

    #[test]
    fn test_stage_output_partitioned_for_hash_exchange() {
        let policy = test_policy();

        // Scan distributed across two workers, then hash partition
        let mut scan = create_scan_annotated("sales", vec!["w1", "w2"]);
        let col_ref = ColumnRef {
            table: Some("sales".to_string()),
            column: "region".into(),
        };
        scan.exchange_before = Some(Exchange::hash_partition(vec![col_ref.clone()], 2));
        scan.annotation.distribution = Distribution::HashPartitioned {
            column_refs: vec![col_ref.clone()],
            workers: vec!["w1".into(), "w2".into()],
        };

        let filter = create_filter_annotated(scan);
        let plan = cut_into_stages(&filter, &policy, "q1", &PlanContext::default());

        // Stage created for the scan should be partitioned
        let stage = plan
            .stages
            .iter()
            .find(|s| s.stage_id == StageId(0))
            .expect("stage 0 exists");

        match &stage.output {
            StageOutput::Partitioned {
                partitions,
                column_refs,
            } => {
                assert_eq!(*partitions, 2);
                assert_eq!(column_refs.len(), 1);
                assert_eq!(&column_refs[0], &col_ref);
            }
            _ => panic!("expected partitioned stage output"),
        }
    }

    #[test]
    fn test_has_exchange_read() {
        let scan = create_scan_annotated("sales", vec!["w1"]);
        assert!(!has_exchange_read(&scan.plan));

        let exchange_read = LogicalPlan::ExchangeRead {
            exchange_id: 0,
            alias: "__exchange_0".to_string(),
        };
        assert!(has_exchange_read(&exchange_read));

        let filter_with_exchange = LogicalPlan::Filter {
            predicate: Expr::Value(sqlparser::ast::Value::Boolean(true).into()),
            input: Box::new(exchange_read),
        };
        assert!(has_exchange_read(&filter_with_exchange));
    }

    #[test]
    fn test_has_local_scan() {
        let scan = create_scan_annotated("sales", vec!["w1"]);
        assert!(has_local_scan(&scan.plan));

        let exchange_read = LogicalPlan::ExchangeRead {
            exchange_id: 0,
            alias: "__exchange_0".to_string(),
        };
        assert!(!has_local_scan(&exchange_read));

        let filter_with_scan = LogicalPlan::Filter {
            predicate: Expr::Value(sqlparser::ast::Value::Boolean(true).into()),
            input: Box::new(scan.plan),
        };
        assert!(has_local_scan(&filter_with_scan));
    }
}
