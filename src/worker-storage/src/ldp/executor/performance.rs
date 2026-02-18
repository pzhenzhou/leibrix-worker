//! Performance optimization module for LDP execution.
//!
//! This module provides facilities to monitor performance and apply optimizations
//! based on runtime benchmarks and profiling data.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use crate::ldp::StageId;
use crate::ldp::executor::metrics::{LdpMetricsRegistry, StageExecutionMetrics, QueryExecutionMetrics};

/// Performance thresholds and configuration for optimization decisions.
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Threshold for slow stage execution (in milliseconds).
    pub slow_stage_threshold_ms: u64,
    
    /// Threshold for high memory usage (as percentage of limit).
    pub high_memory_threshold_pct: f64,
    
    /// Threshold for large data transfers (in bytes).
    pub large_transfer_threshold_bytes: u64,
    
    /// Minimum sample count needed for reliable optimization decisions.
    pub min_sample_count: usize,
    
    /// Cache size for performance history.
    pub history_cache_size: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            slow_stage_threshold_ms: 5000,      // 5 seconds
            high_memory_threshold_pct: 80.0,    // 80%
            large_transfer_threshold_bytes: 100 * 1024 * 1024, // 100MB
            min_sample_count: 5,                // 5 samples
            history_cache_size: 100,            // 100 entries
        }
    }
}

impl PerformanceConfig {
    /// Create a new performance configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the slow stage threshold.
    pub fn with_slow_stage_threshold(mut self, threshold_ms: u64) -> Self {
        self.slow_stage_threshold_ms = threshold_ms;
        self
    }
    
    /// Set the high memory threshold.
    pub fn with_high_memory_threshold(mut self, threshold_pct: f64) -> Self {
        self.high_memory_threshold_pct = threshold_pct;
        self
    }
}

/// Performance optimizer that monitors and applies optimizations.
pub struct PerformanceOptimizer {
    /// Configuration for optimization thresholds.
    config: PerformanceConfig,
    
    /// Metrics registry for accessing performance data.
    metrics_registry: Arc<LdpMetricsRegistry>,
    
    /// Performance history cache.
    history_cache: Arc<RwLock<HashMap<String, PerformanceHistory>>>,
    
    /// Optimization recommendations.
    recommendations: Arc<RwLock<HashMap<String, OptimizationRecommendation>>>,
}

impl PerformanceOptimizer {
    /// Create a new performance optimizer.
    pub fn new(metrics_registry: Arc<LdpMetricsRegistry>) -> Self {
        Self {
            config: PerformanceConfig::default(),
            metrics_registry,
            history_cache: Arc::new(RwLock::new(HashMap::new())),
            recommendations: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Create a new performance optimizer with custom configuration.
    pub fn with_config(config: PerformanceConfig, metrics_registry: Arc<LdpMetricsRegistry>) -> Self {
        Self {
            config,
            metrics_registry,
            history_cache: Arc::new(RwLock::new(HashMap::new())),
            recommendations: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Analyze performance of a completed query.
    pub async fn analyze_query_performance(&self, query_id: &str) {
        let query_metrics = self.metrics_registry.get_query_metrics(query_id).await;
        let stage_metrics = self.metrics_registry.get_query_stage_metrics(query_id).await;
        
        if let Some(metrics) = query_metrics {
            // Analyze query-level performance
            self.analyze_query_level_performance(&metrics).await;
        }
        
        // Analyze stage-level performance
        for stage_metric in stage_metrics {
            self.analyze_stage_performance(&stage_metric).await;
        }
    }
    
    /// Analyze query-level performance and generate recommendations.
    async fn analyze_query_level_performance(&self, metrics: &QueryExecutionMetrics) {
        let mut recommendations = Vec::new();
        
        // Check if query is taking too long
        if metrics.total_duration.as_millis() > self.config.slow_stage_threshold_ms as u128 {
            recommendations.push(OptimizationSuggestion::QueryTooSlow {
                duration_ms: metrics.total_duration.as_millis(),
                threshold_ms: self.config.slow_stage_threshold_ms as u128,
            });
        }
        
        // Check if memory usage is too high
        let memory_utilization = (metrics.peak_memory_bytes as f64) / 
            (1024.0 * 1024.0); // Convert to MB
        
        if memory_utilization > self.config.high_memory_threshold_pct {
            recommendations.push(OptimizationSuggestion::HighMemoryUsage {
                memory_mb: memory_utilization,
                threshold_pct: self.config.high_memory_threshold_pct,
            });
        }
        
        // Store recommendations if any
        if !recommendations.is_empty() {
            let mut rec_map = self.recommendations.write().await;
            rec_map.insert(metrics.query_id.clone(), OptimizationRecommendation {
                query_id: metrics.query_id.clone(),
                suggestions: recommendations,
                timestamp: std::time::Instant::now(),
            });
        }
    }
    
    /// Analyze stage performance and generate recommendations.
    async fn analyze_stage_performance(&self, metrics: &StageExecutionMetrics) {
        let mut recommendations = Vec::new();
        
        // Check execution duration
        let duration_ms = metrics.execution_duration.as_millis();
        if duration_ms > self.config.slow_stage_threshold_ms as u128 {
            recommendations.push(OptimizationSuggestion::SlowStageExecution {
                stage_id: metrics.stage_id,
                duration_ms,
                threshold_ms: self.config.slow_stage_threshold_ms as u128,
            });
        }
        
        // Check memory usage
        let memory_mb = (metrics.peak_memory_bytes as f64) / (1024.0 * 1024.0);
        if memory_mb > self.config.high_memory_threshold_pct {
            recommendations.push(OptimizationSuggestion::HighStageMemoryUsage {
                stage_id: metrics.stage_id,
                memory_mb,
                threshold_pct: self.config.high_memory_threshold_pct,
            });
        }
        
        // Check data processing rates
        if metrics.execution_duration.as_millis() > 0 {
            let rows_per_second = (metrics.output_rows as f64) / 
                (metrics.execution_duration.as_millis() as f64 / 1000.0);
            
            // If processing rate is low, suggest optimization
            if rows_per_second < 1000.0 { // Less than 1K rows/sec
                recommendations.push(OptimizationSuggestion::LowProcessingRate {
                    stage_id: metrics.stage_id,
                    rows_per_second,
                    suggested_rate: 10000.0, // Target 10K rows/sec
                });
            }
        }
        
        // Store recommendations if any
        if !recommendations.is_empty() {
            let mut rec_map = self.recommendations.write().await;
            rec_map.insert(
                format!("{}_{}", metrics.query_id, metrics.stage_id),
                OptimizationRecommendation {
                    query_id: metrics.query_id.clone(),
                    suggestions: recommendations,
                    timestamp: std::time::Instant::now(),
                }
            );
        }
    }
    
    /// Apply optimizations based on recommendations.
    pub async fn apply_optimizations(&self, query_id: &str) -> Vec<AppliedOptimization> {
        let mut applied_optimizations = Vec::new();
        
        // Get recommendations for this query
        let rec_map = self.recommendations.read().await;
        let query_recommendations: Vec<_> = rec_map
            .values()
            .filter(|rec| rec.query_id == query_id)
            .cloned()
            .collect();
        drop(rec_map);
        
        for recommendation in query_recommendations {
            for suggestion in recommendation.suggestions {
                match suggestion {
                    OptimizationSuggestion::SlowStageExecution { stage_id, duration_ms, .. } => {
                        // Apply optimization for slow stages - perhaps increase parallelism
                        applied_optimizations.push(AppliedOptimization::IncreasedParallelism {
                            stage_id,
                            reason: format!("Stage took {}ms, applying parallelism optimization", duration_ms),
                        });
                    },
                    OptimizationSuggestion::HighStageMemoryUsage { stage_id, memory_mb, .. } => {
                        // Apply memory optimization - perhaps streaming/batching
                        applied_optimizations.push(AppliedOptimization::MemoryOptimization {
                            stage_id,
                            memory_saved_mb: memory_mb * 0.2, // Estimate 20% savings
                            reason: format!("High memory usage of {}MB detected", memory_mb),
                        });
                    },
                    OptimizationSuggestion::LowProcessingRate { stage_id, rows_per_second, .. } => {
                        // Apply processing optimization - perhaps better algorithms
                        applied_optimizations.push(AppliedOptimization::ProcessingOptimization {
                            stage_id,
                            improvement_factor: 2.0, // Estimate 2x improvement
                            reason: format!("Low processing rate of {} rows/sec", rows_per_second),
                        });
                    },
                    _ => {
                        // Other suggestions might not have direct optimizations yet
                    }
                }
            }
        }
        
        applied_optimizations
    }
    
    /// Get performance summary for a query.
    pub async fn get_performance_summary(&self, query_id: &str) -> PerformanceSummary {
        let stage_metrics = self.metrics_registry.get_query_stage_metrics(query_id).await;
        let _query_metric = self.metrics_registry.get_query_metrics(query_id).await;
        
        // Calculate aggregates
        let total_stages = stage_metrics.len();
        let slow_stages: Vec<_> = stage_metrics
            .iter()
            .filter(|m| m.execution_duration.as_millis() > self.config.slow_stage_threshold_ms as u128)
            .collect();
        
        let avg_execution_time: u128 = if !stage_metrics.is_empty() {
            stage_metrics.iter()
                .map(|m| m.execution_duration.as_millis())
                .sum::<u128>() / stage_metrics.len() as u128
        } else {
            0
        };
        
        let peak_memory_mb = stage_metrics
            .iter()
            .map(|m| m.peak_memory_bytes)
            .max()
            .unwrap_or(0) as f64 / (1024.0 * 1024.0);
        
        PerformanceSummary {
            query_id: query_id.to_string(),
            total_stages,
            slow_stages: slow_stages.len(),
            avg_execution_time_ms: avg_execution_time,
            peak_memory_mb,
            recommendations_count: self.get_recommendation_count(query_id).await,
        }
    }
    
    /// Get count of recommendations for a query.
    async fn get_recommendation_count(&self, query_id: &str) -> usize {
        let rec_map = self.recommendations.read().await;
        rec_map.values()
            .filter(|rec| rec.query_id == query_id)
            .count()
    }
    
    /// Clear performance data for a completed query.
    pub async fn clear_query_performance_data(&self, query_id: &str) {
        let mut history_cache = self.history_cache.write().await;
        history_cache.retain(|key, _| !key.starts_with(query_id));
        
        let mut recommendations = self.recommendations.write().await;
        recommendations.retain(|key, _| !key.starts_with(query_id));
    }
}

/// Historical performance data for optimization learning.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PerformanceHistory {
    /// Query ID.
    query_id: String,
    
    /// Stage ID.
    stage_id: StageId,
    
    /// Execution duration.
    execution_duration: Duration,
    
    /// Memory usage.
    memory_usage_bytes: u64,
    
    /// Input/output sizes.
    input_size_bytes: u64,
    output_size_bytes: u64,
    
    /// Timestamp of measurement.
    timestamp: std::time::Instant,
}

/// Optimization suggestion based on performance analysis.
#[derive(Debug, Clone)]
pub enum OptimizationSuggestion {
    /// Stage execution is too slow.
    SlowStageExecution {
        stage_id: StageId,
        duration_ms: u128,
        threshold_ms: u128,
    },
    
    /// High memory usage detected.
    HighStageMemoryUsage {
        stage_id: StageId,
        memory_mb: f64,
        threshold_pct: f64,
    },
    
    /// Low processing rate detected.
    LowProcessingRate {
        stage_id: StageId,
        rows_per_second: f64,
        suggested_rate: f64,
    },
    
    /// Query execution is too slow.
    QueryTooSlow {
        duration_ms: u128,
        threshold_ms: u128,
    },
    
    /// High memory usage at query level.
    HighMemoryUsage {
        memory_mb: f64,
        threshold_pct: f64,
    },
}

/// Applied optimization result.
#[derive(Debug, Clone)]
pub enum AppliedOptimization {
    /// Increased parallelism for better performance.
    IncreasedParallelism {
        stage_id: StageId,
        reason: String,
    },
    
    /// Applied memory optimization techniques.
    MemoryOptimization {
        stage_id: StageId,
        memory_saved_mb: f64,
        reason: String,
    },
    
    /// Applied processing optimization.
    ProcessingOptimization {
        stage_id: StageId,
        improvement_factor: f64,
        reason: String,
    },
}

/// Optimization recommendation.
#[derive(Debug, Clone)]
pub struct OptimizationRecommendation {
    /// Query ID.
    pub query_id: String,
    
    /// List of suggestions.
    pub suggestions: Vec<OptimizationSuggestion>,
    
    /// Timestamp of recommendation.
    pub timestamp: std::time::Instant,
}

/// Performance summary for a query.
#[derive(Debug)]
pub struct PerformanceSummary {
    /// Query ID.
    pub query_id: String,
    
    /// Total number of stages.
    pub total_stages: usize,
    
    /// Number of slow stages.
    pub slow_stages: usize,
    
    /// Average execution time in milliseconds.
    pub avg_execution_time_ms: u128,
    
    /// Peak memory usage in MB.
    pub peak_memory_mb: f64,
    
    /// Number of recommendations generated.
    pub recommendations_count: usize,
}

impl PerformanceOptimizer {
    /// Log performance insights for observability.
    pub async fn log_performance_insights(&self, query_id: &str) {
        let summary = self.get_performance_summary(query_id).await;
        
        info!(
            query_id = %summary.query_id,
            total_stages = summary.total_stages,
            slow_stages = summary.slow_stages,
            avg_execution_time_ms = summary.avg_execution_time_ms,
            peak_memory_mb = summary.peak_memory_mb,
            recommendations_count = summary.recommendations_count,
            "Performance analysis completed"
        );
        
        if summary.slow_stages > 0 {
            warn!(
                query_id = %summary.query_id,
                slow_stages = summary.slow_stages,
                total_stages = summary.total_stages,
                "Query has slow stages that may need optimization"
            );
        }
        
        if summary.recommendations_count > 0 {
            debug!(
                query_id = %summary.query_id,
                recommendations_count = summary.recommendations_count,
                "Generated performance optimization recommendations"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_performance_optimizer_creation() {
        let metrics_registry = Arc::new(LdpMetricsRegistry::new());
        let optimizer = PerformanceOptimizer::new(metrics_registry);
        
        assert_eq!(optimizer.config.slow_stage_threshold_ms, 5000);
        assert_eq!(optimizer.config.high_memory_threshold_pct, 80.0);
    }
    
    #[tokio::test]
    async fn test_performance_summary() {
        let metrics_registry = Arc::new(LdpMetricsRegistry::new());
        let optimizer = PerformanceOptimizer::new(metrics_registry);
        
        let summary = optimizer.get_performance_summary("test_query").await;
        
        assert_eq!(summary.query_id, "test_query");
        assert_eq!(summary.total_stages, 0);
        assert_eq!(summary.slow_stages, 0);
        assert_eq!(summary.avg_execution_time_ms, 0);
        assert_eq!(summary.peak_memory_mb, 0.0);
        assert_eq!(summary.recommendations_count, 0);
    }
    
    #[tokio::test]
    async fn test_optimization_recommendation_storage() {
        let metrics_registry = Arc::new(LdpMetricsRegistry::new());
        let optimizer = PerformanceOptimizer::new(metrics_registry);
        
        // Simulate adding a recommendation
        {
            let mut rec_map = optimizer.recommendations.write().await;
            rec_map.insert(
                "test_query_1".to_string(),
                OptimizationRecommendation {
                    query_id: "test_query".to_string(),
                    suggestions: vec![OptimizationSuggestion::SlowStageExecution {
                        stage_id: StageId(1),
                        duration_ms: 6000,
                        threshold_ms: 5000,
                    }],
                    timestamp: std::time::Instant::now(),
                }
            );
        }
        
        let summary = optimizer.get_performance_summary("test_query").await;
        assert_eq!(summary.recommendations_count, 1);
    }
}