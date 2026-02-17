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
        let dist_concat = Self::concat_batches(distributed)?;
        let ref_concat = Self::concat_batches(reference)?;

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
        let dist_concat = Self::concat_batches(distributed)?;
        let ref_concat = Self::concat_batches(reference)?;

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

        // For approximate comparison, we need to handle floating point values specially
        for col_idx in 0..dist_concat.num_columns() {
            let dist_col = dist_concat.column(col_idx);
            let ref_col = ref_concat.column(col_idx);

            Self::compare_columns_approximately(dist_col, ref_col, tolerance, col_idx)?;
        }

        // Also check row equivalence (for non-floating point cols mainly)
        let dist_rows = Self::convert_to_sorted_rows(&dist_concat, check_ordering)?;
        let ref_rows = Self::convert_to_sorted_rows(&ref_concat, check_ordering)?;

        if dist_rows.len() != ref_rows.len() {
            return Err(VerificationError::RowCountMismatch {
                distributed: dist_rows.len(),
                reference: ref_rows.len(),
            });
        }

        // For approximate comparison, we can't rely on row comparison for float cols
        // So we've already checked individual columns above

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

    /// Compare two columns for approximate equality.
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
#[derive(Debug, Clone)]
pub enum VerificationError {
    /// Arrow-related error during comparison.
    ArrowError(String),
    /// Empty results provided for comparison.
    EmptyResults,
    /// Row count mismatch between distributed and reference results.
    RowCountMismatch { distributed: usize, reference: usize },
    /// Column count mismatch between distributed and reference results.
    ColumnCountMismatch { distributed: usize, reference: usize },
    /// Schema mismatch between distributed and reference results.
    SchemaMismatch { 
        distributed: Arc<arrow::datatypes::Schema>, 
        reference: Arc<arrow::datatypes::Schema> 
    },
    /// Row content mismatch at a specific index.
    RowMismatch { 
        index: usize, 
        distributed: String, 
        reference: String 
    },
    /// Column length mismatch.
    ColumnLengthMismatch { 
        column_index: usize, 
        len1: usize, 
        len2: usize 
    },
    /// Column values mismatch.
    ColumnValuesMismatch { 
        column_index: usize, 
        message: String 
    },
    /// Exact value mismatch.
    ValueMismatch { 
        column_index: usize, 
        row_index: usize, 
        value1: String, 
        value2: String 
    },
    /// Approximate value mismatch (beyond tolerance).
    ApproximateValueMismatch { 
        column_index: usize, 
        row_index: usize, 
        value1: f64, 
        value2: f64, 
        tolerance: f64 
    },
}

impl std::fmt::Display for VerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerificationError::ArrowError(e) => write!(f, "Arrow error: {}", e),
            VerificationError::EmptyResults => write!(f, "Empty results provided for comparison"),
            VerificationError::RowCountMismatch { distributed, reference } => {
                write!(f, "Row count mismatch: distributed={} vs reference={}", distributed, reference)
            },
            VerificationError::ColumnCountMismatch { distributed, reference } => {
                write!(f, "Column count mismatch: distributed={} vs reference={}", distributed, reference)
            },
            VerificationError::SchemaMismatch { .. } => write!(f, "Schema mismatch between results"),
            VerificationError::RowMismatch { index, distributed, reference } => {
                write!(f, "Row mismatch at index {}: distributed='{}' vs reference='{}'", 
                       index, distributed, reference)
            },
            VerificationError::ColumnLengthMismatch { column_index, len1, len2 } => {
                write!(f, "Column {} length mismatch: {} vs {}", column_index, len1, len2)
            },
            VerificationError::ColumnValuesMismatch { column_index, message } => {
                write!(f, "Column {} values mismatch: {}", column_index, message)
            },
            VerificationError::ValueMismatch { column_index, row_index, value1, value2 } => {
                write!(f, "Value mismatch at [{},{}]: '{}' vs '{}'", column_index, row_index, value1, value2)
            },
            VerificationError::ApproximateValueMismatch { column_index, row_index, value1, value2, tolerance } => {
                write!(f, "Approximate value mismatch at [{},{}] beyond tolerance {}: {} vs {}", 
                       column_index, row_index, tolerance, value1, value2)
            },
        }
    }
}

impl std::error::Error for VerificationError {}

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
        let result = TestVerifier::assert_results_equal(&[batch1.clone()], &[batch2.clone()], false);
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
        let result = TestVerifier::assert_results_approximately_equal(&[batch1.clone()], &[batch2.clone()], 0.001, true);
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
}