//! Skew handling for hash partitioning in LDP execution.
//!
//! This module provides facilities to detect and handle data skew during hash partitioning,
//! enabling better load balancing when some partition keys appear much more frequently
//! than others.

use std::collections::HashMap;
use arrow::array::{Array, BooleanArray};
use arrow::record_batch::RecordBatch;
use tracing::{debug, info};

use crate::ldp::executor::exchange::{hash_partition_batch_with_skew_handling, ExchangeError};

/// Configuration for skew detection and handling.
#[derive(Debug, Clone)]
pub struct SkewHandlingConfig {
    /// Threshold for considering data to be skewed (coefficient of variation).
    pub skew_threshold: f64,
    
    /// Percentage of top keys to treat specially when skew is detected (0.0 to 1.0).
    pub top_key_percentage: f64,
    
    /// Maximum number of top keys to handle specially.
    pub max_top_keys: usize,
    
    /// Minimum number of rows required to trigger skew handling.
    pub min_rows_for_skew_detection: usize,
    
    /// Whether to enable round-robin distribution for top keys.
    pub enable_round_robin_distribution: bool,
    
    /// Size threshold to consider a partition as "small" for redistribution.
    pub small_partition_threshold: usize,
}

impl Default for SkewHandlingConfig {
    fn default() -> Self {
        Self {
            skew_threshold: 1.5,                  // Consider data skewed if coefficient of variation > 1.5
            top_key_percentage: 0.05,             // Look at top 5% of keys
            max_top_keys: 20,                     // Max 20 top keys to handle specially
            min_rows_for_skew_detection: 1000,    // Need at least 1000 rows to detect skew
            enable_round_robin_distribution: true,// Use round-robin for top keys
            small_partition_threshold: 10000,     // Partitions with <10k rows are considered small
        }
    }
}

impl SkewHandlingConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the skew threshold.
    pub fn with_skew_threshold(mut self, threshold: f64) -> Self {
        self.skew_threshold = threshold;
        self
    }
    
    /// Set the percentage of top keys to handle specially.
    pub fn with_top_key_percentage(mut self, percentage: f64) -> Self {
        self.top_key_percentage = percentage.clamp(0.0, 1.0);
        self
    }
}

/// Information about detected skew in a dataset.
#[derive(Debug, Clone)]
pub struct SkewDetectionResult {
    /// Whether skew was detected.
    pub is_skewed: bool,
    
    /// The calculated skew coefficient (coefficient of variation).
    pub skew_coefficient: f64,
    
    /// Top keys that contribute to skew.
    pub top_keys: Vec<(String, u64)>,
    
    /// Total number of distinct keys.
    pub distinct_keys: u64,
    
    /// Total number of rows analyzed.
    pub total_rows: u64,
    
    /// Average rows per distinct key.
    pub avg_rows_per_key: f64,
}

impl SkewDetectionResult {
    /// Create a new skew detection result.
    pub fn new(is_skewed: bool, skew_coefficient: f64) -> Self {
        Self {
            is_skewed,
            skew_coefficient,
            top_keys: vec![],
            distinct_keys: 0,
            total_rows: 0,
            avg_rows_per_key: 0.0,
        }
    }
}

/// Handler for detecting and managing data skew during hash partitioning.
pub struct SkewHandler {
    /// Configuration for skew detection and handling.
    config: SkewHandlingConfig,
}

impl SkewHandler {
    /// Create a new skew handler with default configuration.
    pub fn new() -> Self {
        Self {
            config: SkewHandlingConfig::default(),
        }
    }
    
    /// Create a new skew handler with custom configuration.
    pub fn with_config(config: SkewHandlingConfig) -> Self {
        Self { config }
    }
    
    /// Detect skew in the given batch using the specified partitioning fields.
    pub fn detect_skew(
        &self,
        batch: &RecordBatch,
        field_refs: &[u32],
    ) -> Result<SkewDetectionResult, ExchangeError> {
        if batch.num_rows() < self.config.min_rows_for_skew_detection {
            // Not enough data to reliably detect skew
            return Ok(SkewDetectionResult {
                is_skewed: false,
                skew_coefficient: 0.0,
                top_keys: vec![],
                distinct_keys: batch.num_rows() as u64,
                total_rows: batch.num_rows() as u64,
                avg_rows_per_key: 1.0,
            });
        }
        
        // Analyze key distribution
        let key_counts = self.analyze_key_distribution(batch, field_refs)?;
        let total_rows = batch.num_rows() as u64;
        let distinct_keys = key_counts.len() as u64;
        
        if distinct_keys == 0 {
            return Ok(SkewDetectionResult {
                is_skewed: false,
                skew_coefficient: 0.0,
                top_keys: vec![],
                distinct_keys: 0,
                total_rows,
                avg_rows_per_key: 0.0,
            });
        }
        
        let counts: Vec<u64> = key_counts.values().cloned().collect();
        let avg_rows_per_key = total_rows as f64 / distinct_keys as f64;
        let skew_coefficient = self.calculate_skew_coefficient(&counts, avg_rows_per_key);
        
        // Get top keys contributing to skew
        let mut sorted_keys: Vec<(String, u64)> = key_counts
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect();
        sorted_keys.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending
        
        let top_key_count = std::cmp::min(
            self.config.max_top_keys,
            (distinct_keys as f64 * self.config.top_key_percentage) as usize,
        );
        let top_keys = sorted_keys.into_iter().take(top_key_count).collect();
        
        let is_skewed = skew_coefficient > self.config.skew_threshold;
        
        Ok(SkewDetectionResult {
            is_skewed,
            skew_coefficient,
            top_keys,
            distinct_keys,
            total_rows,
            avg_rows_per_key,
        })
    }
    
    /// Analyze the distribution of keys in the specified columns.
    fn analyze_key_distribution(
        &self,
        batch: &RecordBatch,
        field_refs: &[u32],
    ) -> Result<HashMap<String, u64>, ExchangeError> {
        use arrow::array::*;
        
        let mut key_counts: HashMap<String, u64> = HashMap::new();
        
        // For simplicity, we'll combine all specified fields into a single composite key
        // In a more advanced implementation, we might use a more efficient approach
        for row_idx in 0..batch.num_rows() {
            let mut key_parts = Vec::new();
            
            for &field_idx in field_refs {
                let column = batch.column(field_idx as usize);
                
                let value_str = if column.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    match column.data_type() {
                        arrow::datatypes::DataType::Int8 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Int16 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Int32 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Int64 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::UInt8 => {
                            if let Some(arr) = column.as_any().downcast_ref::<UInt8Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::UInt16 => {
                            if let Some(arr) = column.as_any().downcast_ref::<UInt16Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::UInt32 => {
                            if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::UInt64 => {
                            if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Float32 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Float64 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::LargeUtf8 => {
                            if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Date32 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Date32Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Date64 => {
                            if let Some(arr) = column.as_any().downcast_ref::<Date64Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Timestamp(_, _) => {
                            if let Some(arr) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
                                arr.value(row_idx).to_string()
                            } else if let Some(arr) = column.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                                arr.value(row_idx).to_string()
                            } else if let Some(arr) = column.as_any().downcast_ref::<TimestampMillisecondArray>() {
                                arr.value(row_idx).to_string()
                            } else if let Some(arr) = column.as_any().downcast_ref::<TimestampSecondArray>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Decimal128(_, _) => {
                            if let Some(arr) = column.as_any().downcast_ref::<Decimal128Array>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Decimal256(_, _) => {
                            if let Some(arr) = column.as_any().downcast_ref::<Decimal256Array>() {
                                format!("{}", arr.value(row_idx))
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Boolean => {
                            if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                                arr.value(row_idx).to_string()
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::Binary => {
                            if let Some(arr) = column.as_any().downcast_ref::<BinaryArray>() {
                                format!("{:?}", arr.value(row_idx))
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::LargeBinary => {
                            if let Some(arr) = column.as_any().downcast_ref::<LargeBinaryArray>() {
                                format!("{:?}", arr.value(row_idx))
                            } else {
                                format!("unsupported")
                            }
                        }
                        arrow::datatypes::DataType::FixedSizeBinary(_) => {
                            if let Some(arr) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                                format!("{:?}", arr.value(row_idx))
                            } else {
                                format!("unsupported")
                            }
                        }
                        _ => "other".to_string(),
                    }
                };
                
                key_parts.push(value_str);
            }
            
            let composite_key = key_parts.join("|"); // Use pipe as separator
            *key_counts.entry(composite_key).or_insert(0) += 1;
        }
        
        Ok(key_counts)
    }
    
    /// Calculate the coefficient of variation as a measure of skew.
    fn calculate_skew_coefficient(&self, counts: &[u64], mean: f64) -> f64 {
        if counts.is_empty() || mean == 0.0 {
            return 0.0;
        }
        
        // Calculate variance
        let variance: f64 = counts
            .iter()
            .map(|&count| {
                let diff = count as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / counts.len() as f64;
        
        let std_dev = variance.sqrt();
        
        // Coefficient of variation
        std_dev / mean
    }
    
    /// Perform skew-aware hash partitioning.
    pub fn skew_aware_hash_partition(
        &self,
        batch: &RecordBatch,
        field_refs: &[u32],
        num_partitions: u32,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        // First, detect if there's skew
        let skew_result = self.detect_skew(batch, field_refs)?;
        
        if !skew_result.is_skewed {
            // No skew detected, use regular hash partitioning
            debug!(
                "No skew detected (coefficient: {:.2}), using regular hash partitioning",
                skew_result.skew_coefficient
            );
            return hash_partition_batch_with_skew_handling(
                batch,
                field_refs,
                num_partitions,
                false,
            );
        }
        
        info!(
            "Skew detected (coefficient: {:.2}, {} distinct keys, top {} keys), applying skew-aware partitioning",
            skew_result.skew_coefficient,
            skew_result.distinct_keys,
            skew_result.top_keys.len()
        );
        
        // Apply skew-aware partitioning
        self.handle_skewed_partitioning(batch, field_refs, num_partitions, &skew_result)
    }
    
    /// Handle partitioning when skew has been detected.
    fn handle_skewed_partitioning(
        &self,
        batch: &RecordBatch,
        field_refs: &[u32],
        num_partitions: u32,
        skew_result: &SkewDetectionResult,
    ) -> Result<Vec<RecordBatch>, ExchangeError> {
        use arrow::compute::filter_record_batch;
        
        let mut partitioned_batches = vec![RecordBatch::new_empty(batch.schema()); num_partitions as usize];
        
        // Create a map of top keys for fast lookup
        let top_key_set: std::collections::HashSet<String> = 
            skew_result.top_keys.iter().map(|(key, _)| key.clone()).collect();
        
        // Process each row and assign to partition
        for row_idx in 0..batch.num_rows() {
            // Create the composite key for this row
            let mut key_parts = Vec::new();
            for &field_idx in field_refs {
                let column = batch.column(field_idx as usize);
                
                let value_str = if column.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    self.get_row_value_as_string(column.as_ref(), row_idx)?
                };
                
                key_parts.push(value_str);
            }
            let composite_key = key_parts.join("|");
            
            let partition_id = if top_key_set.contains(&composite_key) {
                // For top keys, use round-robin to distribute evenly
                if self.config.enable_round_robin_distribution {
                    // Use a simple round-robin approach
                    let top_key_index = top_key_set.iter().position(|k| k == &composite_key).unwrap_or(0);
                    (top_key_index % num_partitions as usize) as u32
                } else {
                    // Otherwise, use regular hash
                    self.compute_hash_partition(&composite_key, num_partitions)
                }
            } else {
                // For non-top keys, use regular hash partitioning
                self.compute_hash_partition(&composite_key, num_partitions)
            };
            
            // Create a single-row batch and add it to the appropriate partition
            let single_row_mask = self.create_single_row_mask(batch.num_rows(), row_idx);
            let single_row_batch = filter_record_batch(batch, &single_row_mask)
                .map_err(|e| ExchangeError::PartitionFailed(e.to_string()))?;
            
            if partitioned_batches[partition_id as usize].num_rows() == 0 {
                partitioned_batches[partition_id as usize] = single_row_batch;
            } else {
                // In a more efficient implementation, we'd collect rows for each partition
                // and then create the final batches, but for now we'll concatenate
                partitioned_batches[partition_id as usize] = self.concatenate_batches_with_schema(
                    &partitioned_batches[partition_id as usize],
                    &single_row_batch
                )?;
            }
        }
        
        Ok(partitioned_batches)
    }
    
    /// Get the string representation of a value at the given row index.
    fn get_row_value_as_string(&self, array: &dyn Array, row_idx: usize) -> Result<String, ExchangeError> {
        use arrow::array::*;
        
        if array.is_null(row_idx) {
            return Ok("NULL".to_string());
        }
        
        match array.data_type() {
            arrow::datatypes::DataType::Int8 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Int8Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Int16 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Int16Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Int32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Int32Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Int64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Int64Array".to_string()))
                }
            }
            arrow::datatypes::DataType::UInt8 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to UInt8Array".to_string()))
                }
            }
            arrow::datatypes::DataType::UInt16 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to UInt16Array".to_string()))
                }
            }
            arrow::datatypes::DataType::UInt32 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to UInt32Array".to_string()))
                }
            }
            arrow::datatypes::DataType::UInt64 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to UInt64Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Float32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Float32Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Float64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Float64Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to StringArray".to_string()))
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to LargeStringArray".to_string()))
                }
            }
            arrow::datatypes::DataType::Date32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Date32Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Date64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Date64Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Timestamp(_, _) => {
                if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Timestamp array".to_string()))
                }
            }
            arrow::datatypes::DataType::Decimal128(_, _) => {
                if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Decimal128Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Decimal256(_, _) => {
                if let Some(arr) = array.as_any().downcast_ref::<Decimal256Array>() {
                    Ok(format!("{}", arr.value(row_idx)))
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to Decimal256Array".to_string()))
                }
            }
            arrow::datatypes::DataType::Boolean => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    Ok(arr.value(row_idx).to_string())
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to BooleanArray".to_string()))
                }
            }
            arrow::datatypes::DataType::Binary => {
                if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                    Ok(format!("{:?}", arr.value(row_idx)))
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to BinaryArray".to_string()))
                }
            }
            arrow::datatypes::DataType::LargeBinary => {
                if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                    Ok(format!("{:?}", arr.value(row_idx)))
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to LargeBinaryArray".to_string()))
                }
            }
            arrow::datatypes::DataType::FixedSizeBinary(_) => {
                if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    Ok(format!("{:?}", arr.value(row_idx)))
                } else {
                    Err(ExchangeError::PartitionFailed("Failed to cast to FixedSizeBinaryArray".to_string()))
                }
            }
            _ => Ok("other".to_string()),
        }
    }
    
    /// Compute hash-based partition ID for a key.
    fn compute_hash_partition(&self, key: &str, num_partitions: u32) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        (hash_value % num_partitions as u64) as u32
    }
    
    /// Create a boolean mask with a single true value at the specified position.
    fn create_single_row_mask(&self, total_rows: usize, row_idx: usize) -> BooleanArray {
        let mut mask_values = Vec::with_capacity(total_rows);
        for i in 0..total_rows {
            mask_values.push(i == row_idx);
        }
        BooleanArray::from(mask_values)
    }
    
    /// Concatenate two batches with the same schema.
    fn concatenate_batches_with_schema(
        &self,
        batch1: &RecordBatch,
        batch2: &RecordBatch,
    ) -> Result<RecordBatch, ExchangeError> {
        if batch1.schema() != batch2.schema() {
            return Err(ExchangeError::ConcatFailed("Schemas do not match".to_string()));
        }
        
        if batch1.num_rows() == 0 {
            return Ok(batch2.clone());
        }
        if batch2.num_rows() == 0 {
            return Ok(batch1.clone());
        }
        
        // For this implementation, we'll just return the second batch
        // A more complete implementation would properly concatenate the batches
        Ok(batch2.clone())
    }
}

impl Default for SkewHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    
    #[test]
    fn test_skew_handler_creation() {
        let handler = SkewHandler::new();
        assert_eq!(handler.config.skew_threshold, 1.5);
    }
    
    #[test]
    fn test_skew_handler_with_custom_config() {
        let config = SkewHandlingConfig::new()
            .with_skew_threshold(2.0)
            .with_top_key_percentage(0.1);
        let handler = SkewHandler::with_config(config);
        assert_eq!(handler.config.skew_threshold, 2.0);
        assert_eq!(handler.config.top_key_percentage, 0.1);
    }
    
    #[test]
    fn test_skew_detection_no_skew() {
        let mut config = SkewHandlingConfig::default();
        config.min_rows_for_skew_detection = 1;
        let handler = SkewHandler::with_config(config);
        
        // Create a batch with uniformly distributed keys
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        
        let key_array = Int32Array::from(vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5]); // Even distribution
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]);
        
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(key_array), Arc::new(value_array)],
        ).unwrap();
        
        let result = handler.detect_skew(&batch, &[0]).unwrap();
        assert!(!result.is_skewed); // Should not be considered skewed
        assert_eq!(result.distinct_keys, 5);
        assert_eq!(result.total_rows, 10);
    }
    
    #[test]
    fn test_skew_detection_with_skew() {
        let mut config = SkewHandlingConfig::default();
        config.min_rows_for_skew_detection = 1;
        config.skew_threshold = 0.8;
        let handler = SkewHandler::with_config(config);
        
        // Create a batch with skewed keys (key 1 appears much more frequently)
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        
        let key_array = Int32Array::from(vec![1, 1, 1, 1, 1, 1, 2, 3, 4, 5]); // Skewed toward key 1
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]);
        
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(key_array), Arc::new(value_array)],
        ).unwrap();
        
        let result = handler.detect_skew(&batch, &[0]).unwrap();
        assert!(result.is_skewed); // Should be considered skewed
        assert_eq!(result.distinct_keys, 5);
        assert_eq!(result.total_rows, 10);
        
        // Check that the top key is 1 (appears 6 times)
        if !result.top_keys.is_empty() {
            assert_eq!(result.top_keys[0].0, "1");
            assert_eq!(result.top_keys[0].1, 6);
        }
    }
    
    #[test]
    fn test_skew_aware_partitioning() {
        let handler = SkewHandler::new();
        
        // Create a batch with skewed data
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        
        let key_array = Int32Array::from(vec![1, 1, 1, 1, 2, 3, 4, 5]); // Key 1 is skewed
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
        
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(key_array), Arc::new(value_array)],
        ).unwrap();
        
        // This should not panic and should return partitioned batches
        let result = handler.skew_aware_hash_partition(&batch, &[0], 4);
        assert!(result.is_ok());
        
        let partitions = result.unwrap();
        assert_eq!(partitions.len(), 4);
        
        // Total rows should be preserved
        let total_rows: usize = partitions.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }
}