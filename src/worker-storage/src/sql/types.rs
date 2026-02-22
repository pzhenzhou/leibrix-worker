//! Shared types for SQL analysis and transformation.

use std::collections::HashMap;

use super::interval::Interval;

/// Re-export the single canonical `DateBound` from the interval algebra module.
/// This eliminates the duplicate definition that previously existed here.
pub use super::interval::DateBound;

/// Unique identifier for a scope in the AST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId(pub usize);

/// The kind of scope in SQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeKind {
    /// Top-level query scope
    MainQuery,
    /// Common Table Expression (WITH clause)
    Cte(String),
    /// Subquery in FROM, WHERE, or SELECT
    Subquery,
}

/// A discovered table reference in the SQL AST.
#[derive(Debug, Clone)]
pub struct TableReference {
    /// The logical dataset name (e.g., "sales_data")
    pub dataset_id: String,
    /// The alias used in the query (e.g., "s" in "FROM sales_data AS s")
    pub alias: Option<String>,
    /// The scope where this table is referenced
    pub scope: ScopeId,
}

impl TableReference {
    /// Returns the effective name to use when matching predicates.
    /// Uses alias if present, otherwise the dataset_id.
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.dataset_id)
    }
}

/// Extracted date range for a table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DateRange {
    /// Start date (inclusive)
    pub start: Option<DateBound>,
    /// End date (for BETWEEN, this is inclusive; we convert to exclusive internally)
    pub end: Option<DateBound>,
    /// Whether the end bound was originally inclusive (BETWEEN) or exclusive (<)
    pub end_inclusive: bool,
}

impl DateRange {
    /// Create a new empty date range.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a date range with literal bounds.
    pub fn with_literals(start: Option<&str>, end: Option<&str>, end_inclusive: bool) -> Self {
        Self {
            start: start.map(|s| DateBound::Literal(s.to_string())),
            end: end.map(|e| DateBound::Literal(e.to_string())),
            end_inclusive,
        }
    }

    /// Check if this range has any bounds set.
    pub fn is_empty(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }

    /// Merge another range into this one, taking the tightest bounds.
    pub fn merge(&mut self, other: DateRange) {
        if self.start.is_none() {
            self.start = other.start;
        }
        if self.end.is_none() {
            self.end = other.end;
            self.end_inclusive = other.end_inclusive;
        }
    }
}

impl From<Interval> for DateRange {
    /// Convert a half-open `Interval` [start, end) into a `DateRange`.
    ///
    /// `Interval` is always half-open (end is exclusive), so
    /// `end_inclusive` is set to `false`.
    fn from(interval: Interval) -> Self {
        DateRange {
            start: interval.start,
            end: interval.end,
            end_inclusive: false, // Interval is always half-open [start, end)
        }
    }
}

/// Result of SQL transformation.
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// The original input SQL
    pub original_sql: String,
    /// The transformed SQL with macro calls
    pub transformed_sql: String,
    /// List of dataset IDs that were replaced with macros
    pub tables_replaced: Vec<String>,
    /// Extracted date ranges per dataset
    pub date_ranges: HashMap<String, DateRange>,
}

/// Configuration for a registered dataset.
#[derive(Debug, Clone)]
pub struct RegisteredDataset {
    /// The logical dataset identifier
    pub dataset_id: String,
    /// The name of the time partition column (e.g., "dt")
    pub time_column_name: String,
    /// The generated macro name (e.g., "scan_sales_data")
    pub macro_name: String,
}

impl RegisteredDataset {
    /// Create a new registered dataset with auto-generated macro name.
    pub fn new(dataset_id: String, time_column_name: String) -> Self {
        let macro_name = format!("scan_{}", dataset_id);
        Self {
            dataset_id,
            time_column_name,
            macro_name,
        }
    }

    /// Create with a custom macro name.
    pub fn with_macro_name(
        dataset_id: String,
        time_column_name: String,
        macro_name: String,
    ) -> Self {
        Self {
            dataset_id,
            time_column_name,
            macro_name,
        }
    }
}

/// Wide date range bounds for when no predicate is found.
pub const WIDE_SCAN_START: &str = "1970-01-01";
pub const WIDE_SCAN_END: &str = "9999-12-31";
