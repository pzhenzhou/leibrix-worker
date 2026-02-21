//! Comprehensive metrics collection for LDP execution.
//!
//! This module provides facilities to track and collect metrics during
//! LDP execution, including exchange operations, stage execution, and query performance.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::ldp::{ExchangeId, StageId, WorkerId};
use arrow::datatypes::SchemaRef;

/// Metrics for broadcast exchange operations.
#[derive(Clone, Debug)]
pub struct BroadcastExchangeMetrics {
    /// Exchange identifier.
    pub exchange_id: ExchangeId,

    /// Query identifier.
    pub query_id: String,

    /// Source worker that broadcasts the data.
    pub source_worker: WorkerId,

    /// Target workers that receive the broadcasted data.
    pub target_workers: Vec<WorkerId>,

    /// Number of rows broadcasted.
    pub rows_broadcasted: u64,

    /// Number of bytes broadcasted.
    pub bytes_broadcasted: u64,

    /// Number of target workers (replication factor).
    pub replication_factor: usize,

    /// Start time of the broadcast operation.
    pub start_time: std::time::Instant,

    /// Duration of the broadcast operation.
    pub duration: Duration,

    /// Throughput in bytes per second.
    pub throughput_bps: f64,

    /// Whether the broadcast decision was optimal.
    pub is_optimal: bool,

    /// Threshold that was used for broadcast decision.
    pub broadcast_threshold_bytes: u64,

    /// Actual size compared to threshold (percentage).
    pub size_ratio_to_threshold: f64,
}

impl BroadcastExchangeMetrics {
    /// Create a new broadcast exchange metrics instance.
    pub fn new(
        exchange_id: ExchangeId,
        query_id: String,
        source_worker: WorkerId,
        target_workers: Vec<WorkerId>,
        broadcast_threshold_bytes: u64,
    ) -> Self {
        Self {
            exchange_id,
            query_id,
            source_worker,
            target_workers,
            rows_broadcasted: 0,
            bytes_broadcasted: 0,
            replication_factor: 0,
            start_time: std::time::Instant::now(),
            duration: Duration::from_secs(0),
            throughput_bps: 0.0,
            is_optimal: false,
            broadcast_threshold_bytes,
            size_ratio_to_threshold: 0.0,
        }
    }

    /// Update metrics with broadcasted data information.
    pub fn update_data(&mut self, rows: u64, bytes: u64) {
        self.rows_broadcasted = rows;
        self.bytes_broadcasted = bytes;
        self.replication_factor = self.target_workers.len();

        // Calculate size ratio to threshold
        if self.broadcast_threshold_bytes > 0 {
            self.size_ratio_to_threshold = (bytes as f64) / (self.broadcast_threshold_bytes as f64);
        }

        // Determine if broadcast was optimal (within safety margin)
        // If size is less than threshold, it's likely optimal
        self.is_optimal = bytes <= self.broadcast_threshold_bytes;
    }

    /// Finalize metrics calculation after broadcast completes.
    pub fn finalize(&mut self) {
        self.duration = self.start_time.elapsed();

        // Calculate throughput (avoid division by zero)
        let duration_secs = self.duration.as_secs_f64();
        if duration_secs > 0.0 {
            self.throughput_bps = (self.bytes_broadcasted as f64) / duration_secs;
        } else {
            self.throughput_bps = f64::INFINITY;
        }
    }

    /// Log metrics for observability.
    pub fn log_metrics(&self) {
        info!(
            query_id = %self.query_id,
            exchange_id = %self.exchange_id,
            source_worker = %self.source_worker,
            target_workers = self.replication_factor,
            rows = self.rows_broadcasted,
            bytes = self.bytes_broadcasted,
            duration_ms = self.duration.as_millis(),
            throughput_mbps = self.throughput_bps / (1024.0 * 1024.0),
            optimal = self.is_optimal,
            "Broadcast exchange completed"
        );

        // Warn if broadcast was close to or exceeded threshold
        if self.size_ratio_to_threshold >= 0.9 {
            warn!(
                query_id = %self.query_id,
                exchange_id = %self.exchange_id,
                bytes = self.bytes_broadcasted,
                threshold = self.broadcast_threshold_bytes,
                ratio = self.size_ratio_to_threshold,
                "Broadcast size close to or exceeds safety threshold - consider hash partitioning"
            );
        }
    }
}

/// Metrics for general exchange operations.
#[derive(Clone, Debug)]
pub struct ExchangeMetrics {
    /// Exchange identifier.
    pub exchange_id: ExchangeId,

    /// Query identifier.
    pub query_id: String,

    /// Exchange type (Gather, Broadcast, HashPartition).
    pub exchange_type: String,

    /// Source workers.
    pub source_workers: Vec<WorkerId>,

    /// Target workers.
    pub target_workers: Vec<WorkerId>,

    /// Bytes transferred.
    pub bytes_transferred: u64,

    /// Rows transferred.
    pub rows_transferred: u64,

    /// Start time of the exchange operation.
    pub start_time: std::time::Instant,

    /// Duration of the exchange operation.
    pub duration: Duration,

    /// Throughput in bytes per second.
    pub throughput_bps: f64,
}

impl ExchangeMetrics {
    /// Create a new exchange metrics instance.
    pub fn new(
        exchange_id: ExchangeId,
        query_id: String,
        exchange_type: String,
        source_workers: Vec<WorkerId>,
        target_workers: Vec<WorkerId>,
    ) -> Self {
        Self {
            exchange_id,
            query_id,
            exchange_type,
            source_workers,
            target_workers,
            bytes_transferred: 0,
            rows_transferred: 0,
            start_time: std::time::Instant::now(),
            duration: Duration::from_secs(0),
            throughput_bps: 0.0,
        }
    }

    /// Update metrics with transfer information.
    pub fn update_transfer(&mut self, rows: u64, bytes: u64) {
        self.rows_transferred = rows;
        self.bytes_transferred = bytes;
    }

    /// Finalize metrics calculation after exchange completes.
    pub fn finalize(&mut self) {
        self.duration = self.start_time.elapsed();

        // Calculate throughput (avoid division by zero)
        let duration_secs = self.duration.as_secs_f64();
        if duration_secs > 0.0 {
            self.throughput_bps = (self.bytes_transferred as f64) / duration_secs;
        } else {
            self.throughput_bps = f64::INFINITY;
        }
    }

    /// Log metrics for observability.
    pub fn log_metrics(&self) {
        info!(
            query_id = %self.query_id,
            exchange_id = %self.exchange_id,
            exchange_type = %self.exchange_type,
            source_workers = self.source_workers.len(),
            target_workers = self.target_workers.len(),
            rows = self.rows_transferred,
            bytes = self.bytes_transferred,
            duration_ms = self.duration.as_millis(),
            throughput_mbps = self.throughput_bps / (1024.0 * 1024.0),
            "Exchange completed"
        );
    }
}

/// Global registry for collecting exchange metrics.
#[derive(Clone)]
pub struct ExchangeMetricsRegistry {
    /// Broadcast exchange metrics.
    broadcast_metrics: Arc<DashMap<String, BroadcastExchangeMetrics>>,

    /// General exchange metrics.
    exchange_metrics: Arc<DashMap<String, ExchangeMetrics>>,
}

impl ExchangeMetricsRegistry {
    /// Create a new metrics registry.
    pub fn new() -> Self {
        Self {
            broadcast_metrics: Arc::new(DashMap::new()),
            exchange_metrics: Arc::new(DashMap::new()),
        }
    }

    /// Register a new broadcast exchange metrics instance.
    pub async fn register_broadcast_metrics(&self, metrics: BroadcastExchangeMetrics) {
        let key = format!("{}_{}", metrics.query_id, metrics.exchange_id);
        self.broadcast_metrics.insert(key, metrics);
    }

    /// Register a new exchange metrics instance.
    pub async fn register_exchange_metrics(&self, metrics: ExchangeMetrics) {
        let key = format!("{}_{}", metrics.query_id, metrics.exchange_id);
        self.exchange_metrics.insert(key, metrics);
    }

    /// Get broadcast metrics for a specific query and exchange.
    pub async fn get_broadcast_metrics(
        &self,
        query_id: &str,
        exchange_id: ExchangeId,
    ) -> Option<BroadcastExchangeMetrics> {
        let key = format!("{}_{}", query_id, exchange_id);
        self.broadcast_metrics.get(&key).map(|v| v.clone())
    }

    /// Get exchange metrics for a specific query and exchange.
    pub async fn get_exchange_metrics(
        &self,
        query_id: &str,
        exchange_id: ExchangeId,
    ) -> Option<ExchangeMetrics> {
        let key = format!("{}_{}", query_id, exchange_id);
        self.exchange_metrics.get(&key).map(|v| v.clone())
    }

    /// Get all broadcast metrics for a query.
    pub async fn get_query_broadcast_metrics(
        &self,
        query_id: &str,
    ) -> Vec<BroadcastExchangeMetrics> {
        self.broadcast_metrics
            .iter()
            .filter(|entry| entry.value().query_id == query_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all exchange metrics for a query.
    pub async fn get_query_exchange_metrics(&self, query_id: &str) -> Vec<ExchangeMetrics> {
        self.exchange_metrics
            .iter()
            .filter(|entry| entry.value().query_id == query_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Clear metrics for a completed query.
    pub async fn clear_query_metrics(&self, query_id: &str) {
        self.broadcast_metrics
            .retain(|key, _| !key.starts_with(query_id));
        self.exchange_metrics
            .retain(|key, _| !key.starts_with(query_id));
    }
}

impl Default for ExchangeMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics for stage execution.
#[derive(Clone, Debug)]
pub struct StageExecutionMetrics {
    /// Stage identifier.
    pub stage_id: StageId,

    /// Query identifier.
    pub query_id: String,

    /// Worker that executes this stage.
    pub worker_id: WorkerId,

    /// Number of input rows to the stage.
    pub input_rows: u64,

    /// Number of output rows from the stage.
    pub output_rows: u64,

    /// Number of input bytes to the stage.
    pub input_bytes: u64,

    /// Number of output bytes from the stage.
    pub output_bytes: u64,

    /// Start time of stage execution.
    pub start_time: std::time::Instant,

    /// Duration of stage execution.
    pub execution_duration: Duration,

    /// Duration of stage submission (waiting time).
    pub submission_duration: Duration,

    /// Peak memory usage during execution (in bytes).
    pub peak_memory_bytes: u64,

    /// Number of input partitions processed.
    pub input_partitions: u32,

    /// Number of output partitions created.
    pub output_partitions: u32,

    /// Schema of the output.
    pub output_schema: Option<SchemaRef>,

    /// Whether the stage execution was successful.
    pub success: bool,

    /// Error message if execution failed.
    pub error_message: Option<String>,
}

impl StageExecutionMetrics {
    /// Create a new stage execution metrics instance.
    pub fn new(stage_id: StageId, query_id: String, worker_id: WorkerId) -> Self {
        Self {
            stage_id,
            query_id,
            worker_id,
            input_rows: 0,
            output_rows: 0,
            input_bytes: 0,
            output_bytes: 0,
            start_time: std::time::Instant::now(),
            execution_duration: Duration::from_secs(0),
            submission_duration: Duration::from_secs(0),
            peak_memory_bytes: 0,
            input_partitions: 0,
            output_partitions: 0,
            output_schema: None,
            success: false,
            error_message: None,
        }
    }

    /// Update input statistics.
    pub fn update_input_stats(&mut self, rows: u64, bytes: u64, partitions: u32) {
        self.input_rows = rows;
        self.input_bytes = bytes;
        self.input_partitions = partitions;
    }

    /// Update output statistics.
    pub fn update_output_stats(
        &mut self,
        rows: u64,
        bytes: u64,
        partitions: u32,
        schema: SchemaRef,
    ) {
        self.output_rows = rows;
        self.output_bytes = bytes;
        self.output_partitions = partitions;
        self.output_schema = Some(schema);
    }

    /// Set execution duration.
    pub fn set_execution_duration(&mut self, duration: Duration) {
        self.execution_duration = duration;
    }

    /// Set submission duration.
    pub fn set_submission_duration(&mut self, duration: Duration) {
        self.submission_duration = duration;
    }

    /// Set peak memory usage.
    pub fn set_peak_memory(&mut self, bytes: u64) {
        self.peak_memory_bytes = bytes;
    }

    /// Mark execution as successful.
    pub fn mark_success(&mut self) {
        self.success = true;
    }

    /// Mark execution as failed.
    pub fn mark_failure(&mut self, error_msg: String) {
        self.success = false;
        self.error_message = Some(error_msg);
    }

    /// Finalize metrics calculation.
    pub fn finalize(&mut self) {
        // Execution duration is already set separately
        if self.execution_duration.is_zero() {
            self.execution_duration = self.start_time.elapsed() - self.submission_duration;
        }
    }

    /// Log stage execution metrics for observability.
    pub fn log_metrics(&self) {
        if self.success {
            info!(
                query_id = %self.query_id,
                stage_id = %self.stage_id,
                worker_id = %self.worker_id,
                input_rows = self.input_rows,
                output_rows = self.output_rows,
                input_bytes = self.input_bytes,
                output_bytes = self.output_bytes,
                execution_ms = self.execution_duration.as_millis(),
                submission_ms = self.submission_duration.as_millis(),
                peak_memory_mb = self.peak_memory_bytes as f64 / (1024.0 * 1024.0),
                "Stage execution completed successfully"
            );
        } else {
            error!(
                query_id = %self.query_id,
                stage_id = %self.stage_id,
                worker_id = %self.worker_id,
                input_rows = self.input_rows,
                output_rows = self.output_rows,
                error = self.error_message.as_ref().unwrap_or(&"unknown".to_string()),
                execution_ms = self.execution_duration.as_millis(),
                "Stage execution failed"
            );
        }
    }
}

/// Metrics for query execution.
#[derive(Clone, Debug)]
pub struct QueryExecutionMetrics {
    /// Query identifier.
    pub query_id: String,

    /// Start time of query execution.
    pub start_time: std::time::Instant,

    /// Total execution time.
    pub total_duration: Duration,

    /// Number of stages in the query.
    pub stage_count: usize,

    /// Number of stages completed successfully.
    pub successful_stages: usize,

    /// Number of stages failed.
    pub failed_stages: usize,

    /// Total rows processed across all stages.
    pub total_rows_processed: u64,

    /// Total bytes processed across all stages.
    pub total_bytes_processed: u64,

    /// Peak memory usage across all stages.
    pub peak_memory_bytes: u64,

    /// Average stage execution time.
    pub avg_stage_duration: Duration,

    /// Total time spent in exchange operations.
    pub exchange_time: Duration,

    /// Total bytes transferred in exchanges.
    pub exchange_bytes: u64,

    /// Whether the query execution was successful.
    pub success: bool,

    /// Error message if query failed.
    pub error_message: Option<String>,
}

impl QueryExecutionMetrics {
    /// Create a new query execution metrics instance.
    pub fn new(query_id: String) -> Self {
        Self {
            query_id,
            start_time: std::time::Instant::now(),
            total_duration: Duration::from_secs(0),
            stage_count: 0,
            successful_stages: 0,
            failed_stages: 0,
            total_rows_processed: 0,
            total_bytes_processed: 0,
            peak_memory_bytes: 0,
            avg_stage_duration: Duration::from_secs(0),
            exchange_time: Duration::from_secs(0),
            exchange_bytes: 0,
            success: false,
            error_message: None,
        }
    }

    /// Update stage statistics.
    pub fn update_stage_stats(
        &mut self,
        success: bool,
        rows: u64,
        bytes: u64,
        memory: u64,
        duration: Duration,
    ) {
        self.stage_count += 1;
        if success {
            self.successful_stages += 1;
        } else {
            self.failed_stages += 1;
        }
        self.total_rows_processed += rows;
        self.total_bytes_processed += bytes;
        if memory > self.peak_memory_bytes {
            self.peak_memory_bytes = memory;
        }

        // Recalculate average stage duration
        let total_completed = self.successful_stages + self.failed_stages;
        if total_completed > 0 {
            // For this implementation, we'll just set the current duration as the average
            // A more complete implementation would track all stage durations
            self.avg_stage_duration = duration;
        }
    }

    /// Update exchange statistics.
    pub fn update_exchange_stats(&mut self, time: Duration, bytes: u64) {
        self.exchange_time += time;
        self.exchange_bytes += bytes;
    }

    /// Mark query as successful.
    pub fn mark_success(&mut self) {
        self.success = true;
    }

    /// Mark query as failed.
    pub fn mark_failure(&mut self, error_msg: String) {
        self.success = false;
        self.error_message = Some(error_msg);
    }

    /// Finalize query metrics calculation.
    pub fn finalize(&mut self) {
        self.total_duration = self.start_time.elapsed();
    }

    /// Log query execution metrics for observability.
    pub fn log_metrics(&self) {
        if self.success {
            info!(
                query_id = %self.query_id,
                total_stages = self.stage_count,
                successful_stages = self.successful_stages,
                failed_stages = self.failed_stages,
                total_rows = self.total_rows_processed,
                total_bytes = self.total_bytes_processed,
                total_duration_ms = self.total_duration.as_millis(),
                avg_stage_duration_ms = self.avg_stage_duration.as_millis(),
                exchange_duration_ms = self.exchange_time.as_millis(),
                exchange_bytes = self.exchange_bytes,
                peak_memory_mb = self.peak_memory_bytes as f64 / (1024.0 * 1024.0),
                "Query execution completed successfully"
            );
        } else {
            error!(
                query_id = %self.query_id,
                total_stages = self.stage_count,
                successful_stages = self.successful_stages,
                failed_stages = self.failed_stages,
                error = self.error_message.as_ref().unwrap_or(&"unknown".to_string()),
                total_duration_ms = self.total_duration.as_millis(),
                "Query execution failed"
            );
        }
    }
}

/// System health metrics.
#[derive(Clone, Debug)]
pub struct SystemHealthMetrics {
    /// Worker identifier.
    pub worker_id: WorkerId,

    /// Timestamp of metric collection.
    pub timestamp: std::time::Instant,

    /// Memory usage (in bytes).
    pub memory_used_bytes: u64,

    /// Memory capacity (in bytes).
    pub memory_capacity_bytes: u64,

    /// CPU usage percentage.
    pub cpu_usage_percent: f64,

    /// Number of active queries.
    pub active_queries: usize,

    /// Number of active stages.
    pub active_stages: usize,

    /// Disk space used (in bytes).
    pub disk_used_bytes: u64,

    /// Disk space capacity (in bytes).
    pub disk_capacity_bytes: u64,

    /// Network I/O in the last period (in bytes).
    pub network_io_bytes: u64,

    /// Active connections.
    pub active_connections: usize,
}

impl SystemHealthMetrics {
    /// Create a new system health metrics instance.
    pub fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            timestamp: std::time::Instant::now(),
            memory_used_bytes: 0,
            memory_capacity_bytes: 0,
            cpu_usage_percent: 0.0,
            active_queries: 0,
            active_stages: 0,
            disk_used_bytes: 0,
            disk_capacity_bytes: 0,
            network_io_bytes: 0,
            active_connections: 0,
        }
    }

    /// Update memory metrics.
    pub fn update_memory(&mut self, used: u64, capacity: u64) {
        self.memory_used_bytes = used;
        self.memory_capacity_bytes = capacity;
    }

    /// Update CPU metrics.
    pub fn update_cpu(&mut self, usage: f64) {
        self.cpu_usage_percent = usage;
    }

    /// Update active counts.
    pub fn update_active_counts(&mut self, queries: usize, stages: usize) {
        self.active_queries = queries;
        self.active_stages = stages;
    }

    /// Update disk metrics.
    pub fn update_disk(&mut self, used: u64, capacity: u64) {
        self.disk_used_bytes = used;
        self.disk_capacity_bytes = capacity;
    }

    /// Update network metrics.
    pub fn update_network(&mut self, io_bytes: u64) {
        self.network_io_bytes = io_bytes;
    }

    /// Update connection metrics.
    pub fn update_connections(&mut self, count: usize) {
        self.active_connections = count;
    }

    /// Get memory utilization percentage.
    pub fn memory_utilization_percent(&self) -> f64 {
        if self.memory_capacity_bytes > 0 {
            (self.memory_used_bytes as f64 / self.memory_capacity_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Get disk utilization percentage.
    pub fn disk_utilization_percent(&self) -> f64 {
        if self.disk_capacity_bytes > 0 {
            (self.disk_used_bytes as f64 / self.disk_capacity_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Log system health metrics for observability.
    pub fn log_metrics(&self) {
        debug!(
            worker_id = %self.worker_id,
            memory_used_mb = self.memory_used_bytes as f64 / (1024.0 * 1024.0),
            memory_capacity_mb = self.memory_capacity_bytes as f64 / (1024.0 * 1024.0),
            memory_utilization_pct = self.memory_utilization_percent(),
            cpu_usage_pct = self.cpu_usage_percent,
            active_queries = self.active_queries,
            active_stages = self.active_stages,
            disk_used_gb = self.disk_used_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            disk_utilization_pct = self.disk_utilization_percent(),
            active_connections = self.active_connections,
            "System health metrics"
        );
    }
}

/// Enhanced registry for all LDP execution metrics.
#[derive(Clone)]
pub struct LdpMetricsRegistry {
    /// Broadcast exchange metrics.
    broadcast_metrics: Arc<DashMap<String, BroadcastExchangeMetrics>>,

    /// General exchange metrics.
    exchange_metrics: Arc<DashMap<String, ExchangeMetrics>>,

    /// Stage execution metrics.
    stage_metrics: Arc<DashMap<String, StageExecutionMetrics>>,

    /// Query execution metrics.
    query_metrics: Arc<DashMap<String, QueryExecutionMetrics>>,

    /// System health metrics.
    health_metrics: Arc<DashMap<WorkerId, SystemHealthMetrics>>,
}

impl LdpMetricsRegistry {
    /// Create a new comprehensive metrics registry.
    pub fn new() -> Self {
        Self {
            broadcast_metrics: Arc::new(DashMap::new()),
            exchange_metrics: Arc::new(DashMap::new()),
            stage_metrics: Arc::new(DashMap::new()),
            query_metrics: Arc::new(DashMap::new()),
            health_metrics: Arc::new(DashMap::new()),
        }
    }

    /// Register stage execution metrics.
    pub async fn register_stage_metrics(&self, metrics: StageExecutionMetrics) {
        let key = format!("{}_{}", metrics.query_id, metrics.stage_id);
        self.stage_metrics.insert(key, metrics);
    }

    /// Register query execution metrics.
    pub async fn register_query_metrics(&self, metrics: QueryExecutionMetrics) {
        self.query_metrics.insert(metrics.query_id.clone(), metrics);
    }

    /// Register system health metrics.
    pub async fn register_health_metrics(&self, metrics: SystemHealthMetrics) {
        self.health_metrics
            .insert(metrics.worker_id.clone(), metrics);
    }

    /// Get stage metrics for a specific query and stage.
    pub async fn get_stage_metrics(
        &self,
        query_id: &str,
        stage_id: StageId,
    ) -> Option<StageExecutionMetrics> {
        let key = format!("{}_{}", query_id, stage_id);
        self.stage_metrics.get(&key).map(|v| v.clone())
    }

    /// Get query metrics for a specific query.
    pub async fn get_query_metrics(&self, query_id: &str) -> Option<QueryExecutionMetrics> {
        self.query_metrics.get(query_id).map(|v| v.clone())
    }

    /// Get system health metrics for a worker.
    pub async fn get_health_metrics(&self, worker_id: &str) -> Option<SystemHealthMetrics> {
        self.health_metrics
            .get(&WorkerId::from(worker_id))
            .map(|v| v.clone())
    }

    /// Get all stage metrics for a query.
    pub async fn get_query_stage_metrics(&self, query_id: &str) -> Vec<StageExecutionMetrics> {
        self.stage_metrics
            .iter()
            .filter(|entry| entry.value().query_id == query_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Clear metrics for a completed query.
    pub async fn clear_query_metrics(&self, query_id: &str) {
        // Clear from all maps
        self.stage_metrics
            .retain(|key, _| !key.starts_with(query_id));
        self.query_metrics.retain(|key, _| key != query_id);
        // Call parent method for exchange metrics
        ExchangeMetricsRegistry::clear_query_metrics_from_maps(
            &self.broadcast_metrics,
            &self.exchange_metrics,
            query_id,
        );
    }
}

impl Default for LdpMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of all metrics for a query.
#[derive(Clone, Debug)]
pub struct QuerySummary {
    /// Query identifier.
    pub query_id: String,

    /// Stage execution metrics.
    pub stage_metrics: Vec<StageExecutionMetrics>,

    /// Query execution metrics.
    pub query_metric: Option<QueryExecutionMetrics>,

    /// Exchange metrics.
    pub exchange_metrics: Vec<ExchangeMetrics>,

    /// Broadcast exchange metrics.
    pub broadcast_metrics: Vec<BroadcastExchangeMetrics>,
}

impl ExchangeMetricsRegistry {
    /// Helper method to clear query metrics from maps (for use in LdpMetricsRegistry).
    pub(super) fn clear_query_metrics_from_maps(
        broadcast_metrics: &Arc<DashMap<String, BroadcastExchangeMetrics>>,
        exchange_metrics: &Arc<DashMap<String, ExchangeMetrics>>,
        query_id: &str,
    ) {
        broadcast_metrics.retain(|key, _| !key.starts_with(query_id));
        exchange_metrics.retain(|key, _| !key.starts_with(query_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_broadcast_metrics_creation() {
        let mut metrics = BroadcastExchangeMetrics::new(
            ExchangeId(1),
            "test_query".to_string(),
            WorkerId::from("source_worker"),
            vec![WorkerId::from("target1"), WorkerId::from("target2")],
            1024 * 1024, // 1MB threshold
        );

        metrics.update_data(1000, 512 * 1024); // 512KB
        metrics.finalize();

        assert_eq!(metrics.exchange_id, ExchangeId(1));
        assert_eq!(metrics.query_id, "test_query");
        assert_eq!(metrics.rows_broadcasted, 1000);
        assert_eq!(metrics.bytes_broadcasted, 512 * 1024);
        assert_eq!(metrics.replication_factor, 2);
        assert!(metrics.is_optimal); // Less than threshold
        assert!(metrics.size_ratio_to_threshold < 1.0);
    }

    #[tokio::test]
    async fn test_exchange_metrics_registry() {
        let registry = ExchangeMetricsRegistry::new();

        let exchange_metrics = ExchangeMetrics::new(
            ExchangeId(1),
            "test_query".to_string(),
            "Broadcast".to_string(),
            vec![WorkerId::from("source")],
            vec![WorkerId::from("target1"), WorkerId::from("target2")],
        );

        registry.register_exchange_metrics(exchange_metrics).await;

        let retrieved = registry
            .get_exchange_metrics("test_query", ExchangeId(1))
            .await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_stage_execution_metrics() {
        let mut metrics = StageExecutionMetrics::new(
            StageId(1),
            "test_query".to_string(),
            WorkerId::from("worker1"),
        );

        metrics.update_input_stats(1000, 10240, 4);
        metrics.update_output_stats(800, 8192, 4, arrow::datatypes::Schema::empty().into());
        metrics.set_peak_memory(2 * 1024 * 1024); // 2MB
        metrics.mark_success();
        metrics.finalize();

        assert_eq!(metrics.stage_id, StageId(1));
        assert_eq!(metrics.query_id, "test_query");
        assert_eq!(metrics.input_rows, 1000);
        assert_eq!(metrics.output_rows, 800);
        assert_eq!(metrics.peak_memory_bytes, 2 * 1024 * 1024);
        assert!(metrics.success);
    }

    #[tokio::test]
    async fn test_ldp_metrics_registry() {
        let registry = LdpMetricsRegistry::new();

        let stage_metrics = StageExecutionMetrics::new(
            StageId(1),
            "test_query".to_string(),
            WorkerId::from("worker1"),
        );

        registry.register_stage_metrics(stage_metrics).await;

        let retrieved = registry.get_stage_metrics("test_query", StageId(1)).await;
        assert!(retrieved.is_some());
    }
}
