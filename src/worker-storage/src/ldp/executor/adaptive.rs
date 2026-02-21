//! Adaptive partitioning for LDP execution.
//!
//! This module provides facilities to adaptively choose partitioning strategies
//! based on data distribution characteristics, enabling better performance
//! for skewed data scenarios.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use tracing::debug;

use crate::ldp::executor::exchange::hash_partition_batch;

/// Statistics about data distribution for adaptive partitioning decisions.
#[derive(Debug, Clone)]
pub struct DataDistributionStats {
    /// Number of distinct values in the partitioning key column(s).
    pub distinct_values: u64,

    /// Total number of rows in the data.
    pub total_rows: u64,

    /// Percentage of distinct values relative to total rows.
    pub distinct_ratio: f64,

    /// Estimated skew factor (how unevenly distributed the data is).
    pub skew_factor: f64,

    /// Histogram of value frequencies.
    pub value_histogram: Vec<u64>,

    /// Top N most frequent values (for skew detection).
    pub top_values: Vec<(String, u64)>,
}

impl Default for DataDistributionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl DataDistributionStats {
    /// Create new distribution stats with default values.
    pub fn new() -> Self {
        Self {
            distinct_values: 0,
            total_rows: 0,
            distinct_ratio: 0.0,
            skew_factor: 1.0, // Perfectly uniform distribution
            value_histogram: vec![],
            top_values: vec![],
        }
    }
}

/// Adaptive partitioning strategy selector.
pub struct AdaptivePartitioner {
    /// Threshold for distinct values to use range partitioning vs hash partitioning.
    distinct_threshold: u64,

    /// Threshold for skew factor to trigger skew handling.
    skew_threshold: f64,

    /// Maximum number of partitions for adaptive selection.
    max_partitions: u32,
}

impl AdaptivePartitioner {
    /// Create a new adaptive partitioner with default settings.
    pub fn new() -> Self {
        Self {
            distinct_threshold: 1000, // Use range partitioning if < 1000 distinct values
            skew_threshold: 3.0,      // Consider data skewed if skew factor > 3.0
            max_partitions: 128,      // Maximum 128 partitions
        }
    }

    /// Analyze data distribution and return statistics.
    pub fn analyze_distribution(
        &self,
        batch: &RecordBatch,
        field_indices: &[u32],
    ) -> DataDistributionStats {
        if batch.num_rows() == 0 || field_indices.is_empty() {
            return DataDistributionStats::new();
        }

        let mut stats = DataDistributionStats::new();
        stats.total_rows = batch.num_rows() as u64;

        // For simplicity, we'll analyze the first field only
        // In a real implementation, we'd analyze combinations of fields
        if let Some(field_idx) = field_indices.first() {
            let column = batch.column(*field_idx as usize);
            stats = self.analyze_column_distribution(column, stats.total_rows);
        }

        stats
    }

    /// Analyze a single column's distribution.
    fn analyze_column_distribution(
        &self,
        column: &dyn Array,
        total_rows: u64,
    ) -> DataDistributionStats {
        use arrow::array::*;
        use std::collections::HashMap;

        let mut value_counts: HashMap<String, u64> = HashMap::new();
        let mut _null_count = 0;

        // Count occurrences of each value
        for i in 0..column.len() {
            if column.is_null(i) {
                _null_count += 1;
            } else {
                let value_str = match column.data_type() {
                    arrow::datatypes::DataType::Int32 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                            arr.value(i).to_string()
                        } else {
                            "unsupported".to_string()
                        }
                    }
                    arrow::datatypes::DataType::Int64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                            arr.value(i).to_string()
                        } else {
                            "unsupported".to_string()
                        }
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                            arr.value(i).to_string()
                        } else {
                            "unsupported".to_string()
                        }
                    }
                    arrow::datatypes::DataType::Float64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                            arr.value(i).to_string()
                        } else {
                            "unsupported".to_string()
                        }
                    }
                    _ => "other".to_string(),
                };

                *value_counts.entry(value_str).or_insert(0) += 1;
            }
        }

        let distinct_values = value_counts.len() as u64;
        let distinct_ratio = if total_rows > 0 {
            distinct_values as f64 / total_rows as f64
        } else {
            0.0
        };

        // Calculate skew factor using Gini coefficient approximation
        let skew_factor =
            calculate_skew_factor(&value_counts.values().cloned().collect::<Vec<_>>());

        // Get top values for skew analysis
        let mut top_values: Vec<(String, u64)> = value_counts.into_iter().collect();
        top_values.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

        // Take top 10 values
        top_values.truncate(10);

        DataDistributionStats {
            distinct_values,
            total_rows,
            distinct_ratio,
            skew_factor,
            value_histogram: vec![], // Could be populated with frequency buckets
            top_values,
        }
    }

    /// Determine the optimal partitioning strategy based on data distribution.
    pub fn determine_partition_strategy(
        &self,
        stats: &DataDistributionStats,
        current_num_partitions: u32,
    ) -> PartitionStrategy {
        if stats.distinct_values == 0 {
            // No data, use default strategy
            return PartitionStrategy::Hash(current_num_partitions);
        }

        // If we have very few distinct values, consider range partitioning
        if stats.distinct_values < self.distinct_threshold {
            return PartitionStrategy::Range {
                num_partitions: std::cmp::min(current_num_partitions, self.max_partitions),
            };
        }

        // If data is highly skewed, consider specialized strategies
        if stats.skew_factor > self.skew_threshold {
            return PartitionStrategy::SkewAware {
                num_partitions: std::cmp::min(current_num_partitions, self.max_partitions),
                top_values: stats.top_values.clone(),
            };
        }

        // Default to hash partitioning with adaptive partition count
        let adjusted_partitions = self.adjust_partition_count(stats, current_num_partitions);
        PartitionStrategy::Hash(adjusted_partitions)
    }

    /// Adjust partition count based on data characteristics.
    fn adjust_partition_count(&self, stats: &DataDistributionStats, current_count: u32) -> u32 {
        // Increase partitions for high-cardinality data
        if stats.distinct_ratio > 0.8 {
            return std::cmp::min(current_count * 2, self.max_partitions);
        }

        // Decrease partitions for low-cardinality data
        if stats.distinct_ratio < 0.1 {
            return std::cmp::max(current_count / 2, 1);
        }

        // For moderate cardinality, keep current count
        std::cmp::min(current_count, self.max_partitions)
    }

    /// Apply adaptive partitioning to a record batch.
    pub fn adaptive_partition(
        &self,
        batch: &RecordBatch,
        field_refs: &[u32],
        num_partitions: u32,
    ) -> Result<Vec<RecordBatch>, crate::ldp::executor::exchange::ExchangeError> {
        let stats = self.analyze_distribution(batch, field_refs);
        let strategy = self.determine_partition_strategy(&stats, num_partitions);

        debug!(
            "Adaptive partitioning: strategy={:?}, distinct_values={}, skew_factor={:.2}",
            strategy, stats.distinct_values, stats.skew_factor
        );

        match strategy {
            PartitionStrategy::Hash(partitions) => {
                hash_partition_batch(batch, field_refs, partitions)
            }
            PartitionStrategy::Range {
                num_partitions: partitions,
            } => {
                // For now, fall back to hash partitioning since range partitioning
                // requires additional logic to determine range boundaries
                hash_partition_batch(batch, field_refs, partitions)
            }
            PartitionStrategy::SkewAware {
                num_partitions: partitions,
                top_values,
            } => {
                // For now, fall back to hash partitioning since skew-aware partitioning
                // requires additional logic to handle top-skewed values specially
                debug!(
                    "Applying skew-aware partitioning with top values: {:?}",
                    top_values
                );
                hash_partition_batch(batch, field_refs, partitions)
            }
        }
    }
}

/// Different partitioning strategies that can be used adaptively.
#[derive(Debug, Clone)]
pub enum PartitionStrategy {
    /// Traditional hash partitioning.
    Hash(u32),

    /// Range-based partitioning (for low cardinality data).
    Range { num_partitions: u32 },

    /// Skew-aware partitioning that handles skewed data specially.
    SkewAware {
        num_partitions: u32,
        top_values: Vec<(String, u64)>,
    },
}

impl Default for AdaptivePartitioner {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate an approximation of the skew factor using the distribution of values.
fn calculate_skew_factor(counts: &[u64]) -> f64 {
    if counts.is_empty() {
        return 1.0; // Uniform distribution
    }

    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 1.0;
    }

    let mean = total as f64 / counts.len() as f64;
    if mean == 0.0 {
        return 1.0;
    }

    // Calculate variance-like measure
    let variance: f64 = counts
        .iter()
        .map(|&count| {
            let diff = count as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / counts.len() as f64;

    let std_dev = variance.sqrt();

    // Coefficient of variation as skew measure
    if std_dev > 0.0 {
        std_dev / mean
    } else {
        0.0 // Perfectly uniform
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_adaptive_partitioner_creation() {
        let partitioner = AdaptivePartitioner::new();
        assert_eq!(partitioner.distinct_threshold, 1000);
        assert_eq!(partitioner.skew_threshold, 3.0);
        assert_eq!(partitioner.max_partitions, 128);
    }

    #[test]
    fn test_data_distribution_analysis() {
        // Create a simple test batch with repeated values (skewed)
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let key_array = Int32Array::from(vec![1, 1, 1, 2, 3, 3, 4, 4, 4, 4]); // 4 distinct values in 10 rows
        let value_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(key_array), Arc::new(value_array)]).unwrap();

        let partitioner = AdaptivePartitioner::new();
        let stats = partitioner.analyze_distribution(&batch, &[0]); // Analyze first column

        assert_eq!(stats.total_rows, 10);
        assert_eq!(stats.distinct_values, 4);
        assert_eq!(stats.distinct_ratio, 0.4); // 4/10
        assert!(stats.skew_factor >= 0.0);
    }

    #[test]
    fn test_partition_strategy_determination() {
        let partitioner = AdaptivePartitioner::new();

        // Test with low cardinality data
        let low_card_stats = DataDistributionStats {
            distinct_values: 10,
            total_rows: 1000,
            distinct_ratio: 0.01,
            skew_factor: 1.0,
            value_histogram: vec![],
            top_values: vec![],
        };

        let strategy = partitioner.determine_partition_strategy(&low_card_stats, 16);
        match strategy {
            PartitionStrategy::Range { num_partitions } => {
                assert_eq!(num_partitions, 16);
            }
            _ => panic!("Expected Range partitioning for low cardinality data"),
        }

        // Test with high skew
        let mut high_skew_stats = DataDistributionStats::new();
        high_skew_stats.skew_factor = 5.0; // Above threshold
        high_skew_stats.top_values = vec![("popular".to_string(), 100)];

        let strategy = partitioner.determine_partition_strategy(&high_skew_stats, 16);
        match strategy {
            PartitionStrategy::SkewAware { num_partitions, .. } => {
                assert_eq!(num_partitions, 16);
            }
            _ => panic!("Expected SkewAware partitioning for high skew data"),
        }

        // Test with high cardinality data
        let high_card_stats = DataDistributionStats {
            distinct_values: 10000,
            total_rows: 10000,
            distinct_ratio: 1.0,
            skew_factor: 1.0,
            value_histogram: vec![],
            top_values: vec![],
        };

        let strategy = partitioner.determine_partition_strategy(&high_card_stats, 16);
        match strategy {
            PartitionStrategy::Hash(partitions) => {
                assert_eq!(partitions, 32); // Should increase partitions for high cardinality
            }
            _ => panic!("Expected Hash partitioning for high cardinality data"),
        }
    }

    #[test]
    fn test_skew_factor_calculation() {
        // Test with perfectly uniform distribution
        let uniform_counts = vec![10, 10, 10, 10]; // Equal counts
        let skew_uniform = calculate_skew_factor(&uniform_counts);
        assert!(skew_uniform < 0.1); // Should be low for uniform data

        // Test with highly skewed distribution
        let skewed_counts = vec![90, 5, 3, 2]; // One dominant value
        let skew_high = calculate_skew_factor(&skewed_counts);
        assert!(skew_high > skew_uniform); // Should be higher for skewed data
    }
}
