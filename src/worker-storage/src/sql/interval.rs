//! Interval algebra for date range extraction from SQL predicates.
//!
//! This module implements a formal interval algebra system based on
//! relational algebra semantics. It correctly handles:
//! - AND operations: intersection of intervals (tightening)
//! - OR operations: union of intervals (widening)
//! - Cross-table predicates in joins
//! - Conservative extraction for correctness

use chrono::NaiveDate;
use std::cmp::{Ordering, max, min};

/// A date bound used in interval endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateBound {
    /// A concrete date literal
    Literal(String),
    /// A parameter placeholder
    Parameter(String),
}


impl PartialOrd for DateBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DateBound {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by variant (Literal < Parameter for deterministic ordering)
        match (self, other) {
            (DateBound::Literal(a), DateBound::Literal(b)) => {
                // Compare literals by parsed date value
                match (NaiveDate::parse_from_str(a, "%Y-%m-%d"), NaiveDate::parse_from_str(b, "%Y-%m-%d")) {
                    (Ok(date_a), Ok(date_b)) => date_a.cmp(&date_b),
                    _ => a.cmp(b), // Fallback to string comparison for invalid dates
                }
            }
            (DateBound::Literal(_), DateBound::Parameter(_)) => Ordering::Less,
            (DateBound::Parameter(_), DateBound::Literal(_)) => Ordering::Greater,
            (DateBound::Parameter(a), DateBound::Parameter(b)) => a.cmp(b),
        }
    }
}

/// A half-open interval [start, end).
/// Represents the date range constraint for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval {
    /// Start date (inclusive), None means -∞
    pub start: Option<DateBound>,
    /// End date (exclusive), None means +∞
    pub end: Option<DateBound>,
}

impl Interval {
    /// Create an empty interval (contradiction, no valid dates).
    pub fn empty() -> Self {
        Self {
            start: Some(DateBound::Literal("9999-12-31".to_string())),
            end: Some(DateBound::Literal("1970-01-01".to_string())),
        }
    }

    /// Create the universe interval (-∞, +∞) that accepts all dates.
    pub fn universe() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// Create a point interval for a single date (inclusive).
    pub fn point(date: DateBound) -> Self {
        // Convert to [date, date+1) for half-open representation
        let next_date = match &date {
            DateBound::Literal(s) => {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.succ_opt())
                    .map(|d| DateBound::Literal(d.format("%Y-%m-%d").to_string()))
                    .unwrap_or_else(|| date.clone())
            }
            DateBound::Parameter(_) => date.clone(),
        };
        
        Self {
            start: Some(date),
            end: Some(next_date),
        }
    }

    /// Create an interval from bounds.
    pub fn from_bounds(start: Option<DateBound>, end: Option<DateBound>) -> Self {
        Self { start, end }
    }

    /// Check if this interval is empty (contradictory).
    /// Returns true only if we can definitively prove the interval is empty
    /// by comparing concrete literal dates. If parameters are involved,
    /// we conservatively return false (unknown at analysis time).
    pub fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Some(DateBound::Literal(s)), Some(DateBound::Literal(e))) => {
                // Only compare concrete dates
                match (NaiveDate::parse_from_str(s, "%Y-%m-%d"), 
                       NaiveDate::parse_from_str(e, "%Y-%m-%d")) {
                    (Ok(start_date), Ok(end_date)) => start_date >= end_date,
                    _ => false, // If we can't parse, assume not empty
                }
            }
            _ => false, // Unknown with parameters or unbounded
        }
    }

    /// Check if this is the universe interval.
    pub fn is_universe(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }

    /// Compute the intersection of two intervals (for AND operations).
    /// Returns the tightest interval that satisfies both constraints.
    pub fn intersect(&self, other: &Self) -> Self {
        // Empty ∩ X = Empty
        if self.is_empty() || other.is_empty() {
            return Self::empty();
        }

        // Compute max(start1, start2)
        let start = match (&self.start, &other.start) {
            (None, s) | (s, None) => s.clone(),
            (Some(s1), Some(s2)) => Some(max(s1, s2).clone()),
        };

        // Compute min(end1, end2)
        let end = match (&self.end, &other.end) {
            (None, e) | (e, None) => e.clone(),
            (Some(e1), Some(e2)) => Some(min(e1, e2).clone()),
        };

        Self { start, end }
    }

    /// Compute the union of two intervals (for OR operations).
    /// Returns the widest interval that covers both ranges.
    /// Note: This may include dates not in either original interval (conservative).
    pub fn union(&self, other: &Self) -> Self {
        // Empty ∪ X = X
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }

        // Universe ∪ X = Universe
        if self.is_universe() || other.is_universe() {
            return Self::universe();
        }

        // Compute min(start1, start2)
        let start = match (&self.start, &other.start) {
            (None, _) | (_, None) => None, // Either unbounded below → union unbounded
            (Some(s1), Some(s2)) => Some(min(s1, s2).clone()),
        };

        // Compute max(end1, end2)
        let end = match (&self.end, &other.end) {
            (None, _) | (_, None) => None, // Either unbounded above → union unbounded
            (Some(e1), Some(e2)) => Some(max(e1, e2).clone()),
        };

        Self { start, end }
    }

    /// Convert to conservative bounds for macro parameters.
    /// Returns (start, end) where start defaults to '1970-01-01' and end to '9999-12-31'.
    pub fn to_macro_params(&self) -> (String, String) {
        let start = match &self.start {
            Some(DateBound::Literal(s)) => s.clone(),
            Some(DateBound::Parameter(_)) => "1970-01-01".to_string(), // Conservative
            None => "1970-01-01".to_string(),
        };

        let end = match &self.end {
            Some(DateBound::Literal(e)) => e.clone(),
            Some(DateBound::Parameter(_)) => "9999-12-31".to_string(), // Conservative
            None => "9999-12-31".to_string(),
        };

        (start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(s: &str) -> DateBound {
        DateBound::Literal(s.to_string())
    }

    #[test]
    fn test_interval_empty() {
        let empty = Interval::empty();
        assert!(empty.is_empty());
        assert!(!empty.is_universe());
    }

    #[test]
    fn test_interval_universe() {
        let univ = Interval::universe();
        assert!(!univ.is_empty());
        assert!(univ.is_universe());
    }

    #[test]
    fn test_interval_point() {
        let point = Interval::point(lit("2025-01-15"));
        assert!(!point.is_empty());
        assert!(!point.is_universe());
        assert_eq!(point.start, Some(lit("2025-01-15")));
        assert_eq!(point.end, Some(lit("2025-01-16")));
    }

    #[test]
    fn test_intersect_basic() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-01")));
        let i2 = Interval::from_bounds(Some(lit("2025-01-15")), Some(lit("2025-03-01")));
        
        let result = i1.intersect(&i2);
        assert_eq!(result.start, Some(lit("2025-01-15"))); // max of starts
        assert_eq!(result.end, Some(lit("2025-02-01")));   // min of ends
    }

    #[test]
    fn test_intersect_disjoint() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-01-31")));
        let i2 = Interval::from_bounds(Some(lit("2025-06-01")), Some(lit("2025-06-30")));
        
        let result = i1.intersect(&i2);
        assert!(result.is_empty()); // Disjoint → empty
    }

    #[test]
    fn test_intersect_with_empty() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-01")));
        let empty = Interval::empty();
        
        let result = i1.intersect(&empty);
        assert!(result.is_empty());
    }

    #[test]
    fn test_intersect_with_universe() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-01")));
        let univ = Interval::universe();
        
        let result = i1.intersect(&univ);
        assert_eq!(result, i1); // X ∩ Universe = X
    }

    #[test]
    fn test_union_basic() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-01-31")));
        let i2 = Interval::from_bounds(Some(lit("2025-06-01")), Some(lit("2025-06-30")));
        
        let result = i1.union(&i2);
        assert_eq!(result.start, Some(lit("2025-01-01"))); // min of starts
        assert_eq!(result.end, Some(lit("2025-06-30")));   // max of ends
        // Note: This covers Feb-May even though they weren't in original intervals
    }

    #[test]
    fn test_union_overlapping() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-15")));
        let i2 = Interval::from_bounds(Some(lit("2025-01-15")), Some(lit("2025-03-01")));
        
        let result = i1.union(&i2);
        assert_eq!(result.start, Some(lit("2025-01-01")));
        assert_eq!(result.end, Some(lit("2025-03-01")));
    }

    #[test]
    fn test_union_with_empty() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-01")));
        let empty = Interval::empty();
        
        let result = i1.union(&empty);
        assert_eq!(result, i1); // X ∪ Empty = X
    }

    #[test]
    fn test_union_with_universe() {
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), Some(lit("2025-02-01")));
        let univ = Interval::universe();
        
        let result = i1.union(&univ);
        assert!(result.is_universe()); // X ∪ Universe = Universe
    }

    #[test]
    fn test_complex_intersection_chain() {
        // (dt >= '2025-01-01') AND (dt < '2025-02-01') AND (dt >= '2025-01-15')
        let i1 = Interval::from_bounds(Some(lit("2025-01-01")), None);
        let i2 = Interval::from_bounds(None, Some(lit("2025-02-01")));
        let i3 = Interval::from_bounds(Some(lit("2025-01-15")), None);
        
        let result = i1.intersect(&i2).intersect(&i3);
        assert_eq!(result.start, Some(lit("2025-01-15"))); // Tightest lower bound
        assert_eq!(result.end, Some(lit("2025-02-01")));   // Tightest upper bound
    }

    #[test]
    fn test_to_macro_params() {
        let interval = Interval::from_bounds(
            Some(lit("2025-01-01")),
            Some(lit("2025-02-01"))
        );
        
        let (start, end) = interval.to_macro_params();
        assert_eq!(start, "2025-01-01");
        assert_eq!(end, "2025-02-01");
    }

    #[test]
    fn test_to_macro_params_unbounded() {
        let interval = Interval::universe();
        
        let (start, end) = interval.to_macro_params();
        assert_eq!(start, "1970-01-01");
        assert_eq!(end, "9999-12-31");
    }

    #[test]
    fn test_datebound_ordering_literals() {
        let d1 = lit("2025-01-01");
        let d2 = lit("2025-01-15");
        let d3 = lit("2025-02-01");
        
        assert!(d1 < d2);
        assert!(d2 < d3);
        assert!(d1 < d3);
        assert_eq!(d1.cmp(&d1), Ordering::Equal);
    }

    #[test]
    fn test_datebound_ordering_parameters() {
        let p1 = DateBound::Parameter("?p1".to_string());
        let p2 = DateBound::Parameter("?p2".to_string());
        let p3 = DateBound::Parameter("?p1".to_string());
        
        // Different parameters must have consistent ordering
        assert_ne!(p1.cmp(&p2), Ordering::Equal);
        // Same parameter names are equal
        assert_eq!(p1.cmp(&p3), Ordering::Equal);
        // Ordering is consistent (if p1 < p2, then p2 > p1)
        assert_eq!(p1.cmp(&p2), p2.cmp(&p1).reverse());
    }

    #[test]
    fn test_datebound_ordering_mixed() {
        let lit_date = lit("2025-01-01");
        let param = DateBound::Parameter("?p1".to_string());
        
        // Literals always compare less than parameters (by design)
        assert!(lit_date < param);
        assert!(param > lit_date);
    }

    #[test]
    fn test_intersect_with_parameters() {
        // Simulate: (dt >= ?p1) AND (dt < '2025-02-01')
        let i1 = Interval::from_bounds(
            Some(DateBound::Parameter("?p1".to_string())),
            None
        );
        let i2 = Interval::from_bounds(
            None,
            Some(lit("2025-02-01"))
        );
        
        let result = i1.intersect(&i2);
        // Should preserve both bounds
        assert_eq!(result.start, Some(DateBound::Parameter("?p1".to_string())));
        assert_eq!(result.end, Some(lit("2025-02-01")));
        assert!(!result.is_empty());
    }

    #[test]
    fn test_intersect_different_parameters() {
        // (dt >= ?p1) AND (dt >= ?p2)
        let i1 = Interval::from_bounds(
            Some(DateBound::Parameter("?p1".to_string())),
            None
        );
        let i2 = Interval::from_bounds(
            Some(DateBound::Parameter("?p2".to_string())),
            None
        );
        
        let result = i1.intersect(&i2);
        // Should take max, which is now deterministic
        // Since "?p1" < "?p2" lexicographically, max is ?p2
        assert_eq!(result.start, Some(DateBound::Parameter("?p2".to_string())));
        assert_eq!(result.end, None);
    }

    #[test]
    fn test_union_with_parameters() {
        // (dt >= ?p1) OR (dt >= '2025-01-01')
        let i1 = Interval::from_bounds(
            Some(DateBound::Parameter("?p1".to_string())),
            None
        );
        let i2 = Interval::from_bounds(
            Some(lit("2025-01-01")),
            None
        );
        
        let result = i1.union(&i2);
        // Should take min, which is the literal (since Literal < Parameter)
        assert_eq!(result.start, Some(lit("2025-01-01")));
        assert_eq!(result.end, None);
    }

    #[test]
    fn test_max_min_correctness() {
        use std::cmp::{max, min};
        
        let d1 = lit("2025-01-01");
        let d2 = lit("2025-01-15");
        
        // Test max/min work correctly with fixed Ord
        assert_eq!(max(&d1, &d2), &d2);
        assert_eq!(min(&d1, &d2), &d1);
        
        // Test with parameters
        let p1 = DateBound::Parameter("?a".to_string());
        let p2 = DateBound::Parameter("?b".to_string());
        
        // Should be consistent and not treat as equal
        let max_result = max(&p1, &p2);
        let min_result = min(&p1, &p2);
        assert_ne!(max_result, min_result); // They're different parameters!
    }
}

