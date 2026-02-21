#![allow(dead_code)]

//! Assertion helpers for flight e2e tests.
//!
//! Thin wrappers over [`TestVerifier`] providing a flight-test-oriented API.
//! All functions panic on failure with a descriptive message so `cargo test`
//! output clearly identifies the failing assertion.

use arrow::array::{Array, Int32Array};
use arrow::record_batch::RecordBatch;
use worker_storage::ldp::testing::verifier::TestVerifier;

/// Assert that `flight` and `reference` batches contain the same rows,
/// independent of row order (rows are sorted before comparison).
pub fn assert_flight_matches_reference(flight: &[RecordBatch], reference: &[RecordBatch]) {
    TestVerifier::assert_results_equal(flight, reference, false).unwrap_or_else(|e| {
        TestVerifier::print_results(flight, "flight (actual)");
        TestVerifier::print_results(reference, "reference (expected)");
        panic!("flight result differs from reference (order-insensitive): {e}");
    });
}

/// Assert equality when the query has an explicit `ORDER BY` and row order matters.
pub fn assert_flight_matches_reference_ordered(flight: &[RecordBatch], reference: &[RecordBatch]) {
    TestVerifier::assert_results_equal(flight, reference, true).unwrap_or_else(|e| {
        TestVerifier::print_results(flight, "flight (actual)");
        TestVerifier::print_results(reference, "reference (expected)");
        panic!("flight result differs from reference (order-sensitive): {e}");
    });
}

/// Assert that both `flight` and `reference` represent an empty result set
/// (zero rows). Delegates to [`assert_flight_matches_reference`] so schema
/// consistency is verified when both sides carry batch metadata, and
/// empty-vs-empty is accepted as valid.
pub fn assert_flight_empty_matches_reference(flight: &[RecordBatch], reference: &[RecordBatch]) {
    // Row count check first for a clear error message.
    let flight_rows = TestVerifier::count_total_rows(flight);
    let reference_rows = TestVerifier::count_total_rows(reference);
    assert_eq!(
        flight_rows, 0,
        "expected empty flight result but got {flight_rows} rows"
    );
    assert_eq!(
        reference_rows, 0,
        "expected empty reference result but got {reference_rows} rows"
    );
    // Delegate to the standard comparator; after the verifier fix this
    // accepts empty-vs-empty as Ok(()).
    assert_flight_matches_reference(flight, reference);
}

/// Assert float-column results within `tolerance`.
/// Non-float columns are compared exactly. Order-independent.
pub fn assert_flight_approx(flight: &[RecordBatch], reference: &[RecordBatch], tolerance: f64) {
    TestVerifier::assert_results_approximately_equal(flight, reference, tolerance, false)
        .unwrap_or_else(|e| {
            TestVerifier::print_results(flight, "flight (actual)");
            TestVerifier::print_results(reference, "reference (expected)");
            panic!("flight float result differs from reference (tol={tolerance}): {e}");
        });
}

/// Assert that `result` is `Err` with `code == expected_code`.
pub fn assert_flight_error(
    result: Result<Vec<RecordBatch>, tonic::Status>,
    expected_code: tonic::Code,
) {
    match result {
        Err(status) => assert_eq!(
            status.code(),
            expected_code,
            "expected status {:?} but got {:?}: {}",
            expected_code,
            status.code(),
            status.message(),
        ),
        Ok(batches) => panic!(
            "expected Err({:?}) but got Ok with {} batch(es) ({} total rows)",
            expected_code,
            batches.len(),
            TestVerifier::count_total_rows(&batches),
        ),
    }
}

/// Assert that every value in the named `Int32` column across all `batches`
/// falls within the closed range `[min_id, max_id]`.
///
/// This is a golden-content assertion independent of the reference path,
/// used to verify epoch-pruning semantics against known fixture IDs.
pub fn assert_column_int_range(batches: &[RecordBatch], col_name: &str, min_id: i32, max_id: i32) {
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap_or_else(|_| {
            panic!(
                "column '{col_name}' not found in schema {:?}",
                batch.schema()
            )
        });
        let arr = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{col_name}' is not Int32"));
        for row in 0..arr.len() {
            if arr.is_null(row) {
                panic!("column '{col_name}' has unexpected NULL at row {row}");
            }
            let id = arr.value(row);
            assert!(
                id >= min_id && id <= max_id,
                "column '{col_name}' value {id} at row {row} is outside expected range [{min_id}, {max_id}]"
            );
        }
    }
}

/// Assert the total row count across all batches equals `expected`.
pub fn assert_flight_row_count(batches: &[RecordBatch], expected: usize) {
    let actual = TestVerifier::count_total_rows(batches);
    assert_eq!(actual, expected, "expected {expected} rows, got {actual}");
}
