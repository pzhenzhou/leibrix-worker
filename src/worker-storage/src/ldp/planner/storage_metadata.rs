//! Storage-backed metadata provider for LDP planning.
//!
//! Provides real epoch statistics and placement information by integrating
//! with the StorageEngine's memory stats and epoch information.

use crate::engine::storage_engine::{EpochMemoryStats, EpochView, MemoryStats, StorageEngine};
use crate::ldp::planner::metadata::{Metadata, TableScanStats};
use crate::ldp::{EpochStats, StatsSource, WorkerId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ============================================================================
// Epoch Placement Information
// ============================================================================

/// Information about an epoch's placement and time range.
#[derive(Clone, Debug)]
pub struct EpochPlacement {
    /// The epoch identifier.
    pub epoch_id: String,
    /// The dataset this epoch belongs to.
    pub dataset_id: String,
    /// The worker that holds this epoch.
    pub worker_id: WorkerId,
    /// Time range as (start_ms_inclusive, end_ms_exclusive).
    pub time_range: (u64, u64),
    /// Statistics (rows, bytes).
    pub stats: EpochStats,
}

impl EpochPlacement {
    /// Check if this epoch overlaps with a time range.
    pub fn overlaps_time_range(&self, start_ms: Option<u64>, end_ms: Option<u64>) -> bool {
        let (epoch_start, epoch_end) = self.time_range;

        // If no bounds given, all epochs match
        let query_start = start_ms.unwrap_or(0);
        let query_end = end_ms.unwrap_or(u64::MAX);

        // Overlap condition: epoch_start < query_end AND epoch_end > query_start
        epoch_start < query_end && epoch_end > query_start
    }
}

// ============================================================================
// Cluster Metadata - For Distributed Planning
// ============================================================================

/// Metadata provider for distributed LDP planning across multiple workers.
///
/// This is used by the coordinator to track epoch placement across all workers
/// in the cluster. It supports:
/// - Time-range based epoch filtering
/// - Worker placement lookup
/// - Aggregated statistics for table scans
#[derive(Clone, Debug, Default)]
pub struct ClusterMetadata {
    /// All known epoch placements: (dataset_id, epoch_id) -> placement.
    epochs: HashMap<(String, String), EpochPlacement>,
    /// Index: dataset_id -> list of epoch_ids (sorted by time).
    dataset_epochs: HashMap<String, Vec<String>>,
    /// Local worker ID (for local execution).
    local_worker_id: Option<WorkerId>,
}

impl ClusterMetadata {
    /// Create a new empty cluster metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a local worker ID.
    pub fn with_local_worker(worker_id: WorkerId) -> Self {
        Self {
            local_worker_id: Some(worker_id),
            ..Default::default()
        }
    }

    /// Set the local worker ID.
    pub fn set_local_worker(&mut self, worker_id: WorkerId) {
        self.local_worker_id = Some(worker_id);
    }

    /// Register an epoch with its placement information.
    pub fn register_epoch(&mut self, placement: EpochPlacement) {
        let key = (placement.dataset_id.clone(), placement.epoch_id.clone());

        // Add to dataset index
        self.dataset_epochs
            .entry(placement.dataset_id.clone())
            .or_default()
            .push(placement.epoch_id.clone());

        // Add placement
        self.epochs.insert(key, placement);
    }

    /// Register an epoch from EpochView and stats (for local storage integration).
    pub fn register_from_epoch_view(
        &mut self,
        dataset_id: &str,
        view: &EpochView,
        worker_id: &str,
        rows: u64,
        bytes: u64,
    ) {
        let placement = EpochPlacement {
            epoch_id: view.epoch_id.clone(),
            dataset_id: dataset_id.to_string(),
            worker_id: worker_id.to_string(),
            time_range: view.time_range,
            stats: EpochStats::exact(rows, bytes),
        };
        self.register_epoch(placement);
    }

    /// Get epochs for a dataset within a time range.
    ///
    /// # Arguments
    /// * `dataset_id` - The dataset to query.
    /// * `start_ms` - Optional start time (inclusive) in milliseconds.
    /// * `end_ms` - Optional end time (exclusive) in milliseconds.
    ///
    /// # Returns
    /// List of (epoch_id, worker_id) pairs for matching epochs.
    pub fn epochs_for(
        &self,
        dataset_id: &str,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Vec<(String, WorkerId)> {
        self.dataset_epochs
            .get(dataset_id)
            .map(|epoch_ids| {
                epoch_ids
                    .iter()
                    .filter_map(|epoch_id| {
                        let key = (dataset_id.to_string(), epoch_id.clone());
                        self.epochs.get(&key).and_then(|placement| {
                            if placement.overlaps_time_range(start_ms, end_ms) {
                                Some((epoch_id.clone(), placement.worker_id.clone()))
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the worker that holds a specific epoch.
    pub fn worker_for(&self, dataset_id: &str, epoch_id: &str) -> Option<WorkerId> {
        let key = (dataset_id.to_string(), epoch_id.to_string());
        self.epochs.get(&key).map(|p| p.worker_id.clone())
    }

    /// Get statistics for a specific epoch.
    pub fn stats_for(&self, dataset_id: &str, epoch_id: &str) -> Option<EpochStats> {
        let key = (dataset_id.to_string(), epoch_id.to_string());
        self.epochs.get(&key).map(|p| p.stats.clone())
    }

    /// Sort epochs by time for a dataset (call after bulk registration).
    pub fn sort_epochs_by_time(&mut self, dataset_id: &str) {
        if let Some(epoch_ids) = self.dataset_epochs.get_mut(dataset_id) {
            epoch_ids.sort_by(|a, b| {
                let key_a = (dataset_id.to_string(), a.clone());
                let key_b = (dataset_id.to_string(), b.clone());
                let time_a = self.epochs.get(&key_a).map(|p| p.time_range.0).unwrap_or(0);
                let time_b = self.epochs.get(&key_b).map(|p| p.time_range.0).unwrap_or(0);
                time_a.cmp(&time_b)
            });
        }
    }

    /// Get all datasets.
    pub fn datasets(&self) -> Vec<String> {
        self.dataset_epochs.keys().cloned().collect()
    }

    /// Get all workers that have epochs for a dataset.
    pub fn workers_for_dataset(&self, dataset_id: &str) -> Vec<WorkerId> {
        let mut workers = Vec::new();
        if let Some(epoch_ids) = self.dataset_epochs.get(dataset_id) {
            for epoch_id in epoch_ids {
                let key = (dataset_id.to_string(), epoch_id.clone());
                if let Some(placement) = self.epochs.get(&key) {
                    if !workers.contains(&placement.worker_id) {
                        workers.push(placement.worker_id.clone());
                    }
                }
            }
        }
        workers
    }

    /// Builder method to add an epoch.
    pub fn with_epoch(mut self, placement: EpochPlacement) -> Self {
        self.register_epoch(placement);
        self
    }

    /// Create from memory stats and epoch views (for local worker integration).
    pub fn from_memory_stats(
        memory_stats: &MemoryStats,
        epoch_views: &HashMap<String, (EpochView, u64, u64)>, // key -> (view, rows, bytes)
        worker_id: &str,
    ) -> Self {
        let mut metadata = Self::with_local_worker(worker_id.to_string());

        for stat in &memory_stats.epochs {
            let key = format!("{}__{}", stat.dataset_id, stat.epoch_id);
            if let Some((view, rows, bytes)) = epoch_views.get(&key) {
                metadata.register_from_epoch_view(
                    &stat.dataset_id,
                    view,
                    worker_id,
                    *rows,
                    *bytes,
                );
            } else {
                // Fallback: create placement from stats only (no time range)
                let placement = EpochPlacement {
                    epoch_id: stat.epoch_id.clone(),
                    dataset_id: stat.dataset_id.clone(),
                    worker_id: worker_id.to_string(),
                    time_range: (0, u64::MAX), // Unknown time range
                    stats: EpochStats::exact(stat.rows_count, stat.approx_bytes),
                };
                metadata.register_epoch(placement);
            }
        }

        // Sort all datasets by time
        let datasets: Vec<_> = metadata.datasets();
        for dataset_id in datasets {
            metadata.sort_epochs_by_time(&dataset_id);
        }

        metadata
    }
}

impl Metadata for ClusterMetadata {
    fn get_epoch_stats(&self, epoch_id: &str) -> Option<EpochStats> {
        // Try to find epoch in any dataset
        for ((dataset_id, eid), placement) in &self.epochs {
            if eid == epoch_id || format!("{}__{}", dataset_id, eid) == epoch_id {
                return Some(placement.stats.clone());
            }
        }
        None
    }

    fn get_epoch_worker(&self, epoch_id: &str) -> Option<WorkerId> {
        for ((dataset_id, eid), placement) in &self.epochs {
            if eid == epoch_id || format!("{}__{}", dataset_id, eid) == epoch_id {
                return Some(placement.worker_id.clone());
            }
        }
        None
    }

    fn get_epochs_for_table(
        &self,
        table_name: &str,
        start_epoch: Option<&str>,
        end_epoch: Option<&str>,
    ) -> Vec<(String, WorkerId)> {
        // Parse time range from epoch IDs if they're timestamps
        let start_ms = start_epoch.and_then(|s| parse_epoch_timestamp(s));
        let end_ms = end_epoch.and_then(|s| parse_epoch_timestamp(s));

        self.epochs_for(table_name, start_ms, end_ms)
    }
}

// ============================================================================
// Storage Engine Metadata Adapter
// ============================================================================

/// Async metadata provider that wraps a StorageEngine.
///
/// This adapter allows the LDP planner to access real epoch statistics
/// from the storage engine. It caches the stats for efficiency.
pub struct StorageEngineMetadata<E: StorageEngine> {
    /// The underlying storage engine.
    engine: Arc<E>,
    /// Cached cluster metadata (updated on refresh).
    cached_metadata: RwLock<ClusterMetadata>,
    /// Local worker ID.
    worker_id: WorkerId,
}

impl<E: StorageEngine + Send + Sync> StorageEngineMetadata<E> {
    /// Create a new storage engine metadata adapter.
    pub fn new(engine: Arc<E>, worker_id: WorkerId) -> Self {
        Self {
            engine,
            cached_metadata: RwLock::new(ClusterMetadata::with_local_worker(worker_id.clone())),
            worker_id,
        }
    }

    /// Refresh the cached metadata from the storage engine.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        let memory_stats = self.engine.memory_stats().await?;

        let mut metadata = ClusterMetadata::with_local_worker(self.worker_id.clone());

        // Group epochs by dataset for efficient epoch view lookup
        let mut datasets_to_refresh = std::collections::HashSet::new();
        for stat in &memory_stats.epochs {
            datasets_to_refresh.insert(stat.dataset_id.clone());
        }

        // Get epoch views for all datasets (includes time_range)
        let mut epoch_views_map = std::collections::HashMap::new();
        for dataset_id in datasets_to_refresh {
            match self.engine.list_epochs(dataset_id.clone()).await {
                Ok(views) => {
                    for view in views {
                        let key = (dataset_id.clone(), view.epoch_id.clone());
                        epoch_views_map.insert(key, view);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        dataset_id = %dataset_id,
                        error = %e,
                        "Failed to list epochs for dataset, will use default time range"
                    );
                }
            }
        }

        for stat in &memory_stats.epochs {
            // Get detailed stats from engine
            let epoch_stats = self
                .engine
                .get_epoch_stats(&stat.dataset_id, &stat.epoch_id)
                .await?;

            let stats = epoch_stats.unwrap_or_else(|| {
                EpochStats::exact(stat.rows_count, stat.approx_bytes)
            });

            // Get time_range from epoch view
            let key = (stat.dataset_id.clone(), stat.epoch_id.clone());
            let time_range = epoch_views_map
                .get(&key)
                .map(|view| view.time_range)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        dataset_id = %stat.dataset_id,
                        epoch_id = %stat.epoch_id,
                        "No epoch view found, using default time range (0, u64::MAX)"
                    );
                    (0, u64::MAX)
                });

            let placement = EpochPlacement {
                epoch_id: stat.epoch_id.clone(),
                dataset_id: stat.dataset_id.clone(),
                worker_id: self.worker_id.clone(),
                time_range,
                stats,
            };

            metadata.register_epoch(placement);
        }

        // Sort epochs by time
        let datasets: Vec<_> = metadata.datasets();
        for dataset_id in datasets {
            metadata.sort_epochs_by_time(&dataset_id);
        }

        *self.cached_metadata.write().await = metadata;
        Ok(())
    }

    /// Get the cached metadata (synchronous access).
    pub async fn get_metadata(&self) -> ClusterMetadata {
        self.cached_metadata.read().await.clone()
    }

    /// Get epochs for a dataset within a time range.
    pub async fn epochs_for(
        &self,
        dataset_id: &str,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Vec<(String, WorkerId)> {
        self.cached_metadata
            .read()
            .await
            .epochs_for(dataset_id, start_ms, end_ms)
    }

    /// Get the worker for an epoch.
    pub async fn worker_for(&self, dataset_id: &str, epoch_id: &str) -> Option<WorkerId> {
        self.cached_metadata
            .read()
            .await
            .worker_for(dataset_id, epoch_id)
    }

    /// Get stats for an epoch.
    pub async fn stats_for(&self, dataset_id: &str, epoch_id: &str) -> Option<EpochStats> {
        self.cached_metadata
            .read()
            .await
            .stats_for(dataset_id, epoch_id)
    }

    /// Get aggregated table scan stats.
    pub async fn get_table_scan_stats(&self, table_name: &str) -> TableScanStats {
        self.cached_metadata
            .read()
            .await
            .get_table_scan_stats(table_name)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse an epoch ID that might be a timestamp (format: {timestamp_ms}_{suffix}).
fn parse_epoch_timestamp(epoch_id: &str) -> Option<u64> {
    // Try to parse as plain timestamp
    if let Ok(ts) = epoch_id.parse::<u64>() {
        return Some(ts);
    }

    // Try format: {timestamp_ms}_{suffix}
    if let Some(ts_str) = epoch_id.split('_').next() {
        if let Ok(ts) = ts_str.parse::<u64>() {
            return Some(ts);
        }
    }

    None
}

/// Convert date string (YYYY-MM-DD) to milliseconds since epoch.
pub fn date_to_ms(date_str: &str) -> Option<u64> {
    use chrono::NaiveDate;
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .ok()
        .map(|date| {
            date.and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis() as u64
        })
}

/// Convert milliseconds since epoch to date string (YYYY-MM-DD).
pub fn ms_to_date(ms: u64) -> String {
    use chrono::{DateTime, Utc};
    let dt = DateTime::from_timestamp_millis(ms as i64).unwrap_or_default();
    dt.format("%Y-%m-%d").to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_placement(
        dataset_id: &str,
        epoch_id: &str,
        worker_id: &str,
        start_ms: u64,
        end_ms: u64,
        rows: u64,
        bytes: u64,
    ) -> EpochPlacement {
        EpochPlacement {
            epoch_id: epoch_id.to_string(),
            dataset_id: dataset_id.to_string(),
            worker_id: worker_id.to_string(),
            time_range: (start_ms, end_ms),
            stats: EpochStats::exact(rows, bytes),
        }
    }

    #[test]
    fn test_epoch_placement_overlaps() {
        let placement = create_test_placement(
            "sales",
            "e1",
            "w1",
            1000, // 1 second
            2000, // 2 seconds
            100,
            1000,
        );

        // Fully contained
        assert!(placement.overlaps_time_range(Some(500), Some(2500)));
        // Partial overlap at start
        assert!(placement.overlaps_time_range(Some(500), Some(1500)));
        // Partial overlap at end
        assert!(placement.overlaps_time_range(Some(1500), Some(2500)));
        // No overlap - before
        assert!(!placement.overlaps_time_range(Some(0), Some(1000)));
        // No overlap - after
        assert!(!placement.overlaps_time_range(Some(2000), Some(3000)));
        // No bounds - always overlaps
        assert!(placement.overlaps_time_range(None, None));
        // Only start bound
        assert!(placement.overlaps_time_range(Some(1500), None));
        // Only end bound
        assert!(placement.overlaps_time_range(None, Some(1500)));
    }

    #[test]
    fn test_cluster_metadata_epochs_for() {
        let mut metadata = ClusterMetadata::new();

        // Add epochs with different time ranges
        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e2", "w2", 2000, 3000, 200, 2000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e3", "w1", 3000, 4000, 150, 1500,
        ));

        // Query full range
        let epochs = metadata.epochs_for("sales", None, None);
        assert_eq!(epochs.len(), 3);

        // Query partial range
        let epochs = metadata.epochs_for("sales", Some(1500), Some(2500));
        assert_eq!(epochs.len(), 2); // e1 and e2 overlap

        // Query single epoch
        let epochs = metadata.epochs_for("sales", Some(2500), Some(2600));
        assert_eq!(epochs.len(), 1);
        assert_eq!(epochs[0].0, "e2");
    }

    #[test]
    fn test_cluster_metadata_worker_for() {
        let mut metadata = ClusterMetadata::new();

        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e2", "w2", 2000, 3000, 200, 2000,
        ));

        assert_eq!(metadata.worker_for("sales", "e1"), Some("w1".to_string()));
        assert_eq!(metadata.worker_for("sales", "e2"), Some("w2".to_string()));
        assert_eq!(metadata.worker_for("sales", "e3"), None);
    }

    #[test]
    fn test_cluster_metadata_stats_for() {
        let mut metadata = ClusterMetadata::new();

        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));

        let stats = metadata.stats_for("sales", "e1").unwrap();
        assert_eq!(stats.rows, 100);
        assert_eq!(stats.bytes, 1000);
        assert!(stats.stats_source.is_exact());
    }

    #[test]
    fn test_cluster_metadata_workers_for_dataset() {
        let mut metadata = ClusterMetadata::new();

        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e2", "w2", 2000, 3000, 200, 2000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e3", "w1", 3000, 4000, 150, 1500,
        ));

        let workers = metadata.workers_for_dataset("sales");
        assert_eq!(workers.len(), 2);
        assert!(workers.contains(&"w1".to_string()));
        assert!(workers.contains(&"w2".to_string()));
    }

    #[test]
    fn test_cluster_metadata_implements_metadata_trait() {
        let mut metadata = ClusterMetadata::new();

        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e2", "w2", 2000, 3000, 200, 2000,
        ));

        // Test via Metadata trait
        let scan_stats = metadata.get_table_scan_stats("sales");
        assert_eq!(scan_stats.rows, 300);
        assert_eq!(scan_stats.bytes, 3000);
        assert_eq!(scan_stats.workers.len(), 2);
        assert!(scan_stats.stats_source.is_exact());
    }

    #[test]
    fn test_parse_epoch_timestamp() {
        assert_eq!(parse_epoch_timestamp("1730419200000"), Some(1730419200000));
        assert_eq!(
            parse_epoch_timestamp("1730419200000_a7b3c2"),
            Some(1730419200000)
        );
        assert_eq!(parse_epoch_timestamp("not_a_timestamp"), None);
    }

    #[test]
    fn test_date_to_ms() {
        let ms = date_to_ms("2025-01-01").unwrap();
        assert!(ms > 0);

        // Invalid date
        assert!(date_to_ms("not-a-date").is_none());
    }

    #[test]
    fn test_ms_to_date() {
        // 2025-01-01 00:00:00 UTC
        let ms = 1735689600000u64;
        let date = ms_to_date(ms);
        assert_eq!(date, "2025-01-01");
    }

    #[test]
    fn test_cluster_metadata_sort_epochs() {
        let mut metadata = ClusterMetadata::new();

        // Add epochs out of order
        metadata.register_epoch(create_test_placement(
            "sales", "e3", "w1", 3000, 4000, 150, 1500,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e1", "w1", 1000, 2000, 100, 1000,
        ));
        metadata.register_epoch(create_test_placement(
            "sales", "e2", "w2", 2000, 3000, 200, 2000,
        ));

        metadata.sort_epochs_by_time("sales");

        let epochs = metadata.epochs_for("sales", None, None);
        // Should be sorted by time
        assert_eq!(epochs[0].0, "e1");
        assert_eq!(epochs[1].0, "e2");
        assert_eq!(epochs[2].0, "e3");
    }
}
