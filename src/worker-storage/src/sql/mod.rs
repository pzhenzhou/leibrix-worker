//! SQL analysis and rewriting module for LogicalDatasetManager.
//!
//! This module provides transparent query transformation from logical dataset
//! names to table macro calls with date range parameters for epoch pruning.
//!
//! # Architecture
//!
//! The transformation process follows a four-phase algorithm:
//!
//! 1. **Parse Phase** ([`parser`]): Parse SQL into AST using `sqlparser-rs`
//! 2. **Discovery Phase** ([`discovery`]): Find logical table references in AST
//! 3. **Analysis Phase** ([`analyzer`]): Extract date range predicates
//! 4. **Transformation Phase** ([`transformer`]): Rewrite AST with macro calls
//!
//! # Key Design Principle: Semantic Parity
//!
//! To guarantee query results are exactly the same before and after conversion,
//! the transformer **preserves original WHERE predicates**. The macro parameters
//! act as optimization hints for epoch pruning, while the original predicates
//! serve as the final filter ensuring correctness.
//!
//! # Example
//!
//! ```rust
//! use worker_storage::sql::{SqlTransformer, RegisteredDataset};
//!
//! let mut transformer = SqlTransformer::new();
//! transformer.register_dataset(RegisteredDataset::new(
//!     "sales_data".to_string(),
//!     "dt".to_string(),
//! ));
//!
//! let result = transformer.transform(
//!     "SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'"
//! ).unwrap();
//!
//! // Transformed SQL uses macro call but preserves WHERE clause:
//! // SELECT * FROM scan_sales_data(DATE '2025-01-01', DATE '2025-02-01')
//! //   WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
//! assert!(result.transformed_sql.contains("scan_sales_data"));
//! ```
//!
//! # Supported SQL Patterns
//!
//! - Simple SELECT with single table
//! - JOINs (multiple tables with different time columns)
//! - CTEs (Common Table Expressions / WITH clause)
//! - Subqueries in FROM, WHERE, SELECT
//! - UNION / INTERSECT / EXCEPT operations
//! - Various date predicate formats:
//!   - `BETWEEN ... AND ...`
//!   - `>`, `>=`, `<`, `<=`, `=`
//!   - Reversed comparisons: `'2025-01-01' <= dt`
//!   - DATE literals: `DATE '2025-01-01'`
//!
//! # Limitations
//!
//! - Only SELECT queries are supported
//! - Complex date expressions (e.g., `dt = CURRENT_DATE - INTERVAL '7 days'`)
//!   are not extracted; wide scan is used instead
//! - Dynamic SQL (table names in variables) is not supported

mod admission;
mod analyzer;
mod boolean_analyzer;
mod discovery;
mod error;
mod interval;
mod parser;
mod transformer;
mod types;

use sqlparser::ast::{Expr, JoinConstraint, JoinOperator};

/// Extract constraint from JoinOperator (shared helper to avoid duplication).
pub(crate) fn extract_join_constraint(op: &JoinOperator) -> Option<&JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c) => Some(c),
        _ => None,
    }
}

/// Extract mutable constraint from JoinOperator (shared helper).
pub(crate) fn extract_join_constraint_mut(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c) => Some(c),
        _ => None,
    }
}

/// Extract ON expression from JoinOperator.
pub(crate) fn extract_join_on_expr(op: &JoinOperator) -> Option<&Expr> {
    match extract_join_constraint(op)? {
        JoinConstraint::On(expr) => Some(expr),
        _ => None,
    }
}

/// Extract mutable ON expression from JoinOperator.
pub(crate) fn extract_join_on_expr_mut(op: &mut JoinOperator) -> Option<&mut Expr> {
    match extract_join_constraint_mut(op)? {
        JoinConstraint::On(expr) => Some(expr),
        _ => None,
    }
}

// Re-export public types
pub use admission::{AdmissionController, AdmissionError, AdmissionResult};
pub use error::{SqlTransformError, TransformError};
pub use interval::Interval;
pub use transformer::SqlTransformer;
pub use types::{
    DateBound, DateRange, RegisteredDataset, ScopeId, ScopeKind, TableReference, TransformResult,
    WIDE_SCAN_END, WIDE_SCAN_START,
};

// Re-export parser utilities for testing
pub use parser::{parse_sql, to_sql};

// Re-export new interval algebra for testing
pub use boolean_analyzer::BooleanExprAnalyzer;
