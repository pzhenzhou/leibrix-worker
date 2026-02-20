//! Test result verifier for LDP end-to-end testing.
//!
//! This module provides utilities for comparing distributed execution results
//! with reference results to ensure correctness.

use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use std::sync::Arc;

/// Test result verifier for comparing distributed vs reference results.
pub struct TestVerifier;

impl TestVerifier {
    /// Compare two sets of record batches for equality.
    /// 
    /// This function compares the results of distributed execution with reference results
    /// to ensure they are equivalent. It handles floating-point precision differences
    /// and ignores row ordering by default.
    pub fn assert_results_equal(
        distributed: &[RecordBatch],
        reference: &[RecordBatch],
        check_ordering: bool,
    ) -> Result<(), VerificationError> {
        let dist_result = Self::concat_batches(distributed);
        let ref_result = Self::concat_batches(reference);

        // Treat empty-vs-empty as equal. Both sides produced no batches (e.g. a
        // query that returns zero rows via Arrow Flight / the reference engine).
        // If only one side is empty that is a row-count mismatch.
        let (dist_concat, ref_concat) = match (dist_result, ref_result) {
            (Err(VerificationError::EmptyResults), Err(VerificationError::EmptyResults)) => {
                return Ok(());
            }
            (Err(VerificationError::EmptyResults), Ok(ref_b)) => {
                return Err(VerificationError::RowCountMismatch {
                    distributed: 0,
                    reference: ref_b.num_rows(),
                });
            }
            (Ok(dist_b), Err(VerificationError::EmptyResults)) => {
                return Err(VerificationError::RowCountMismatch {
                    distributed: dist_b.num_rows(),
                    reference: 0,
                });
            }
            (Err(e), _) => return Err(e),
            (_, Err(e)) => return Err(e),
            (Ok(d), Ok(r)) => (d, r),
        };

        if dist_concat.num_rows() != ref_concat.num_rows() {
            return Err(VerificationError::RowCountMismatch {
                distributed: dist_concat.num_rows(),
                reference: ref_concat.num_rows(),
            });
        }

        if dist_concat.num_columns() != ref_concat.num_columns() {
            return Err(VerificationError::ColumnCountMismatch {
                distributed: dist_concat.num_columns(),
                reference: ref_concat.num_columns(),
            });
        }

        // Compare schemas
        if dist_concat.schema() != ref_concat.schema() {
            return Err(VerificationError::SchemaMismatch {
                distributed: dist_concat.schema().clone(),
                reference: ref_concat.schema().clone(),
            });
        }

        // Convert to rows for comparison, ignoring order by default
        let dist_rows = Self::convert_to_sorted_rows(&dist_concat, check_ordering)?;
        let ref_rows = Self::convert_to_sorted_rows(&ref_concat, check_ordering)?;

        if dist_rows.len() != ref_rows.len() {
            return Err(VerificationError::RowCountMismatch {
                distributed: dist_rows.len(),
                reference: ref_rows.len(),
            });
        }

        for (i, (dist_row, ref_row)) in dist_rows.iter().zip(ref_rows.iter()).enumerate() {
            if dist_row != ref_row {
                return Err(VerificationError::RowMismatch {
                    index: i,
                    distributed: format!("{:?}", dist_row),
                    reference: format!("{:?}", ref_row),
                });
            }
        }

        Ok(())
    }

    /// Compare two sets of record batches for approximate equality.
    /// 
    /// This function is useful for comparing results that may have minor floating-point
    /// precision differences. It compares numeric values within a tolerance.
    pub fn assert_results_approximately_equal(
        distributed: &[RecordBatch],
        reference: &[RecordBatch],
        tolerance: f64,
        check_ordering: bool,
    ) -> Result<(), VerificationError> {
        let dist_result = Self::concat_batches(distributed);
        let ref_result = Self::concat_batches(reference);

        // Mirror the empty-vs-empty logic from assert_results_equal.
        let (dist_concat, ref_concat) = match (dist_result, ref_result) {
            (Err(VerificationError::EmptyResults), Err(VerificationError::EmptyResults)) => {
                return Ok(());
            }
            (Err(VerificationError::EmptyResults), Ok(ref_b)) => {
                return Err(VerificationError::RowCountMismatch {
                    distributed: 0,
                    reference: ref_b.num_rows(),
                });
            }
            (Ok(dist_b), Err(VerificationError::EmptyResults)) => {
                return Err(VerificationError::RowCountMismatch {
                    distributed: dist_b.num_rows(),
                    reference: 0,
                });
            }
            (Err(e), _) => return Err(e),
            (_, Err(e)) => return Err(e),
            (Ok(d), Ok(r)) => (d, r),
        };

        if dist_concat.num_rows() != ref_concat.num_rows() {
            return Err(VerificationError::RowCountMismatch {
                distributed: dist_concat.num_rows(),
                reference: ref_concat.num_rows(),
            });
        }

        if dist_concat.num_columns() != ref_concat.num_columns() {
            return Err(VerificationError::ColumnCountMismatch {
                distributed: dist_concat.num_columns(),
                reference: ref_concat.num_columns(),
            });
        }

        // Sort both sides to a canonical row order when order-independence is
        // required, ensuring we always compare logically equivalent rows against
        // each other regardless of engine output order. This prevents the old
        // bug where positional column comparison paired wrong rows together when
        // the two sides happened to return rows in different orders.
        let (dist_aligned, ref_aligned) = if check_ordering {
            (dist_concat, ref_concat)
        } else {
            (
                Self::sort_batch_by_all_columns(&dist_concat)?,
                Self::sort_batch_by_all_columns(&ref_concat)?,
            )
        };

        // Compare cell by cell: float columns use tolerance, all others exact.
        for col_idx in 0..dist_aligned.num_columns() {
            for row_idx in 0..dist_aligned.num_rows() {
                Self::compare_cell_approximately(
                    dist_aligned.column(col_idx),
                    ref_aligned.column(col_idx),
                    row_idx,
                    col_idx,
                    tolerance,
                )?;
            }
        }

        Ok(())
    }

    /// Concatenate multiple record batches into a single batch for easier comparison.
    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, VerificationError> {
        if batches.is_empty() {
            return Err(VerificationError::EmptyResults);
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // Ensure all batches have the same schema
        let schema = batches[0].schema();
        for batch in batches.iter().skip(1) {
            if batch.schema() != schema {
                return Err(VerificationError::SchemaMismatch {
                    distributed: batch.schema().clone(),
                    reference: schema.clone(),
                });
            }
        }

        let columns: Result<Vec<_>, _> = (0..schema.fields().len())
            .map(|i| {
                let col_arrays: Vec<&dyn arrow::array::Array> = batches.iter().map(|batch| batch.column(i).as_ref()).collect();
                arrow::compute::concat(&col_arrays)
            })
            .collect();

        let concatenated_columns = columns.map_err(|e| VerificationError::ArrowError(e.to_string()))?;
        RecordBatch::try_new(schema, concatenated_columns).map_err(|e| VerificationError::ArrowError(e.to_string()))
    }

    /// Convert a record batch to sorted rows for comparison.
    fn convert_to_sorted_rows(
        batch: &RecordBatch,
        preserve_order: bool,
    ) -> Result<Vec<arrow::row::OwnedRow>, VerificationError> {
        let schema = batch.schema();

        // Create sort fields for all columns (we'll sort by all columns to normalize order)
        let sort_fields: Result<Vec<_>, _> = schema
            .fields()
            .iter()
            .map(|field| {
                // For floating point types, we might need special handling, but for now
                // we'll treat them as regular sortable fields
                Ok(SortField::new(field.data_type().clone()))
            })
            .collect();

        let sort_fields: Vec<_> = sort_fields.map_err(|e: ArrowError| VerificationError::ArrowError(e.to_string()))?;
        let converter = RowConverter::new(sort_fields).map_err(|e: ArrowError| VerificationError::ArrowError(e.to_string()))?;

        let rows = converter.convert_columns(batch.columns())?;
        
        if preserve_order {
            // Convert rows to owned
            Ok(rows.iter().map(|row| row.owned()).collect())
        } else {
            // Convert to owned and sort
            let mut owned_rows: Vec<_> = rows.iter().map(|row| row.owned()).collect();
            owned_rows.sort();
            Ok(owned_rows)
        }
    }

    /// Sort a `RecordBatch` by all columns lexicographically, producing a new
    /// batch with rows in a canonical order.
    ///
    /// This is used by [`Self::assert_results_approximately_equal`] to align
    /// rows from both sides before cell-by-cell comparison, so that logically
    /// equivalent result sets compare equal regardless of row order.
    fn sort_batch_by_all_columns(batch: &RecordBatch) -> Result<RecordBatch, VerificationError> {
        use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};

        if batch.num_rows() == 0 {
            return Ok(batch.clone());
        }

        let sort_columns: Vec<SortColumn> = batch
            .columns()
            .iter()
            .map(|col| SortColumn {
                values: Arc::clone(col),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
            .collect();

        let indices = lexsort_to_indices(&sort_columns, None)
            .map_err(|e| VerificationError::ArrowError(e.to_string()))?;

        let sorted_columns: Result<Vec<ArrayRef>, ArrowError> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();

        let sorted_columns =
            sorted_columns.map_err(|e| VerificationError::ArrowError(e.to_string()))?;
        RecordBatch::try_new(batch.schema(), sorted_columns)
            .map_err(|e| VerificationError::ArrowError(e.to_string()))
    }

    /// Compare a single cell from two arrays.
    ///
    /// - `Float32` / `Float64` columns: passes if `|a - b| <= tolerance`.
    /// - All other types: passes only on exact equality.
    /// - If exactly one of the two cells is `null`, returns a mismatch error.
    fn compare_cell_approximately(
        dist_col: &ArrayRef,
        ref_col: &ArrayRef,
        row_idx: usize,
        col_idx: usize,
        tolerance: f64,
    ) -> Result<(), VerificationError> {
        // Null handling: both null → equal; exactly one null → mismatch.
        match (dist_col.is_null(row_idx), ref_col.is_null(row_idx)) {
            (true, true) => return Ok(()),
            (true, false) | (false, true) => {
                return Err(VerificationError::ValueMismatch {
                    column_index: col_idx,
                    row_index: row_idx,
                    value1: Self::format_cell_value(dist_col, row_idx),
                    value2: Self::format_cell_value(ref_col, row_idx),
                });
            }
            (false, false) => {}
        }

        match dist_col.data_type() {
            DataType::Float32 => {
                let a = dist_col
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row_idx) as f64;
                let b = ref_col
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row_idx) as f64;
                if (a - b).abs() > tolerance {
                    return Err(VerificationError::ApproximateValueMismatch {
                        column_index: col_idx,
                        row_index: row_idx,
                        value1: a,
                        value2: b,
                        tolerance,
                    });
                }
            }
            DataType::Float64 => {
                let a = dist_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row_idx);
                let b = ref_col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row_idx);
                if (a - b).abs() > tolerance {
                    return Err(VerificationError::ApproximateValueMismatch {
                        column_index: col_idx,
                        row_index: row_idx,
                        value1: a,
                        value2: b,
                        tolerance,
                    });
                }
            }
            // All other types: slice the array to a single-row view and
            // compare exactly. Using a slice avoids downcasting against every
            // possible concrete Arrow array type.
            _ => {
                let dist_slice = dist_col.slice(row_idx, 1);
                let ref_slice = ref_col.slice(row_idx, 1);
                if dist_slice != ref_slice {
                    return Err(VerificationError::ValueMismatch {
                        column_index: col_idx,
                        row_index: row_idx,
                        value1: Self::format_cell_value(dist_col, row_idx),
                        value2: Self::format_cell_value(ref_col, row_idx),
                    });
                }
            }
        }

        Ok(())
    }

    /// Compare two columns for approximate equality.
    #[allow(dead_code)]
    fn compare_columns_approximately(
        col1: &ArrayRef,
        col2: &ArrayRef,
        tolerance: f64,
        col_idx: usize,
    ) -> Result<(), VerificationError> {
        if col1.len() != col2.len() {
            return Err(VerificationError::ColumnLengthMismatch {
                column_index: col_idx,
                len1: col1.len(),
                len2: col2.len(),
            });
        }

        match (col1.data_type(), col2.data_type()) {
            (DataType::Float32, DataType::Float32) => {
                let arr1 = col1.as_any().downcast_ref::<Float32Array>().unwrap();
                let arr2 = col2.as_any().downcast_ref::<Float32Array>().unwrap();
                
                for i in 0..arr1.len() {
                    if arr1.is_null(i) != arr2.is_null(i) {
                        return Err(VerificationError::ValueMismatch {
                            column_index: col_idx,
                            row_index: i,
                            value1: format!("{:?}", arr1.value(i)),
                            value2: format!("{:?}", arr2.value(i)),
                        });
                    }
                    if !arr1.is_null(i) {
                        let val1 = arr1.value(i) as f64;
                        let val2 = arr2.value(i) as f64;
                        if (val1 - val2).abs() > tolerance {
                            return Err(VerificationError::ApproximateValueMismatch {
                                column_index: col_idx,
                                row_index: i,
                                value1: val1,
                                value2: val2,
                                tolerance,
                            });
                        }
                    }
                }
            }
            (DataType::Float64, DataType::Float64) => {
                let arr1 = col1.as_any().downcast_ref::<Float64Array>().unwrap();
                let arr2 = col2.as_any().downcast_ref::<Float64Array>().unwrap();
                
                for i in 0..arr1.len() {
                    if arr1.is_null(i) != arr2.is_null(i) {
                        return Err(VerificationError::ValueMismatch {
                            column_index: col_idx,
                            row_index: i,
                            value1: format!("{:?}", arr1.value(i)),
                            value2: format!("{:?}", arr2.value(i)),
                        });
                    }
                    if !arr1.is_null(i) {
                        let val1 = arr1.value(i);
                        let val2 = arr2.value(i);
                        if (val1 - val2).abs() > tolerance {
                            return Err(VerificationError::ApproximateValueMismatch {
                                column_index: col_idx,
                                row_index: i,
                                value1: val1,
                                value2: val2,
                                tolerance,
                            });
                        }
                    }
                }
            }
            // For other types, do exact comparison
            _ => {
                // Cast both to the same type if needed and compare
                if col1 != col2 {
                    return Err(VerificationError::ColumnValuesMismatch {
                        column_index: col_idx,
                        message: "Columns have different values".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Count total rows across all batches.
    pub fn count_total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Print results for debugging purposes.
    pub fn print_results(batches: &[RecordBatch], label: &str) {
        println!("=== {} Results ===", label);
        for (batch_idx, batch) in batches.iter().enumerate() {
            println!("Batch {}: {} rows, {} cols", batch_idx, batch.num_rows(), batch.num_columns());
            for row_idx in 0..std::cmp::min(batch.num_rows(), 10) { // Print first 10 rows
                let mut row_str = String::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = Self::format_cell_value(col, row_idx);
                    row_str.push_str(&format!("{} ", value));
                }
                println!("  Row {}: {}", row_idx, row_str.trim());
            }
            if batch.num_rows() > 10 {
                println!("  ... ({} more rows)", batch.num_rows() - 10);
            }
        }
    }

    /// Format a cell value for display.
    fn format_cell_value(array: &ArrayRef, row_idx: usize) -> String {
        use arrow::array::*;
        
        if array.is_null(row_idx) {
            return "NULL".to_string();
        }

        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(row_idx).to_string()
            },
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(row_idx).to_string()
            },
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                format!("{:.6}", arr.value(row_idx))
            },
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                format!("{:.6}", arr.value(row_idx))
            },
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                format!("'{}'", arr.value(row_idx))
            },
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                arr.value(row_idx).to_string()
            },
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row_idx);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "INVALID_DATE".to_string());
                format!("DATE '{}'", date)
            },
            _ => format!("<{:?}>", array.data_type()),
        }
    }
}

/// Errors that can occur during test result verification.
#[derive(Debug, Clone, thiserror::Error)]
pub enum VerificationError {
    /// Arrow-related error during comparison.
    #[error("Arrow error: {0}")]
    ArrowError(String),
    /// Empty results provided for comparison.
    #[error("Empty results provided for comparison")]
    EmptyResults,
    /// Row count mismatch between distributed and reference results.
    #[error("Row count mismatch: distributed={distributed} vs reference={reference}")]
    RowCountMismatch { distributed: usize, reference: usize },
    /// Column count mismatch between distributed and reference results.
    #[error("Column count mismatch: distributed={distributed} vs reference={reference}")]
    ColumnCountMismatch { distributed: usize, reference: usize },
    /// Schema mismatch between distributed and reference results.
    #[error("Schema mismatch between results")]
    SchemaMismatch { 
        distributed: Arc<arrow::datatypes::Schema>, 
        reference: Arc<arrow::datatypes::Schema> 
    },
    /// Row content mismatch at a specific index.
    #[error("Row mismatch at index {index}: distributed='{distributed}' vs reference='{reference}'")]
    RowMismatch { 
        index: usize, 
        distributed: String, 
        reference: String 
    },
    /// Column length mismatch.
    #[error("Column {column_index} length mismatch: {len1} vs {len2}")]
    ColumnLengthMismatch { 
        column_index: usize, 
        len1: usize, 
        len2: usize 
    },
    /// Column values mismatch.
    #[error("Column {column_index} values mismatch: {message}")]
    ColumnValuesMismatch { 
        column_index: usize, 
        message: String 
    },
    /// Exact value mismatch.
    #[error("Value mismatch at [{column_index},{row_index}]: '{value1}' vs '{value2}'")]
    ValueMismatch { 
        column_index: usize, 
        row_index: usize, 
        value1: String, 
        value2: String 
    },
    /// Approximate value mismatch (beyond tolerance).
    #[error("Approximate value mismatch at [{column_index},{row_index}] beyond tolerance {tolerance}: {value1} vs {value2}")]
    ApproximateValueMismatch { 
        column_index: usize, 
        row_index: usize, 
        value1: f64, 
        value2: f64, 
        tolerance: f64 
    },
}

impl From<ArrowError> for VerificationError {
    fn from(error: ArrowError) -> Self {
        VerificationError::ArrowError(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch_with_ints(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
        ]));

        let array = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn create_test_batch_with_floats(values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
        ]));

        let array = Arc::new(Float64Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn create_test_batch_with_strings(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        let array = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_assert_results_equal_identical() {
        let batch1 = create_test_batch_with_ints(vec![1, 2, 3]);
        let batch2 = create_test_batch_with_ints(vec![1, 2, 3]);

        let result = TestVerifier::assert_results_equal(&[batch1], &[batch2], true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_assert_results_equal_different_order() {
        let batch1 = create_test_batch_with_ints(vec![1, 2, 3]);
        let batch2 = create_test_batch_with_ints(vec![3, 1, 2]); // Different order

        // Should pass when ordering is not checked
        let result = TestVerifier::assert_results_equal(
            std::slice::from_ref(&batch1),
            std::slice::from_ref(&batch2),
            false,
        );
        assert!(result.is_ok());

        // Should fail when ordering is checked
        let result = TestVerifier::assert_results_equal(&[batch1], &[batch2], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_assert_results_equal_different_values() {
        let batch1 = create_test_batch_with_ints(vec![1, 2, 3]);
        let batch2 = create_test_batch_with_ints(vec![1, 2, 4]); // Different last value

        let result = TestVerifier::assert_results_equal(&[batch1], &[batch2], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_assert_results_approximately_equal() {
        let batch1 = create_test_batch_with_floats(vec![1.0001, 2.0001, 3.0001]);
        let batch2 = create_test_batch_with_floats(vec![1.0002, 2.0002, 3.0002]); // Very close values

        // Should pass with tolerance of 0.001
        let result = TestVerifier::assert_results_approximately_equal(
            std::slice::from_ref(&batch1),
            std::slice::from_ref(&batch2),
            0.001,
            true,
        );
        assert!(result.is_ok());

        // Should fail with tighter tolerance
        let result = TestVerifier::assert_results_approximately_equal(&[batch1], &[batch2], 0.00005, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_concat_batches() {
        let batch1 = create_test_batch_with_ints(vec![1, 2]);
        let batch2 = create_test_batch_with_ints(vec![3, 4]);
        
        let result = TestVerifier::concat_batches(&[batch1, batch2]).unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.num_columns(), 1);

        let values = result.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
        assert_eq!(values.value(3), 4);
    }

    #[test]
    fn test_print_results() {
        let batch = create_test_batch_with_strings(vec!["hello", "world"]);
        TestVerifier::print_results(&[batch], "test");
        // Just checking this doesn't panic
    }

    // -----------------------------------------------------------------------
    // Issue-2 regression tests: approximate comparison must be order-independent
    // -----------------------------------------------------------------------

    /// Two float batches with the same values in *different row orders* must
    /// compare as equal in order-independent mode (check_ordering = false).
    #[test]
    fn test_approx_equal_order_independent_same_values() {
        // dist:      [3.0, 1.0, 2.0]
        // reference: [1.0, 2.0, 3.0]  — same set, different order
        let dist = create_test_batch_with_floats(vec![3.0, 1.0, 2.0]);
        let reference = create_test_batch_with_floats(vec![1.0, 2.0, 3.0]);

        let result = TestVerifier::assert_results_approximately_equal(
            &[dist],
            &[reference],
            0.001,
            false, // order-independent
        );
        assert!(
            result.is_ok(),
            "same float values in different order should pass: {result:?}"
        );
    }

    /// Two float batches whose values differ by more than the tolerance after
    /// row alignment must fail, even when check_ordering = false.
    #[test]
    fn test_approx_equal_order_independent_exceeds_tolerance() {
        // After sorting both: [1.0, 2.0, 3.0] vs [1.0, 2.0, 3.5]
        // The last pair differs by 0.5 > 0.1.
        let dist = create_test_batch_with_floats(vec![3.0, 1.0, 2.0]);
        let reference = create_test_batch_with_floats(vec![1.0, 2.0, 3.5]);

        let result = TestVerifier::assert_results_approximately_equal(
            &[dist],
            &[reference],
            0.1,
            false, // order-independent
        );
        assert!(
            result.is_err(),
            "values exceeding tolerance after alignment should fail"
        );
    }
}