//! Metadata provider for LDP planning.
//!
//! Provides access to epoch statistics and placement information
//! needed for cost-based planning decisions.

use crate::ldp::{Distribution, EpochStats, StatsSource, WorkerId};
use dashmap::DashMap;

/// Optional closed time range `[start_unix_secs, end_unix_secs)`.
type TimeRange = Option<(u64, u64)>;

/// Trait for accessing metadata during LDP planning.
///
/// This abstracts over the actual metadata storage (could be in-memory,
/// from master service, etc.) to allow testing with mock implementations.
pub trait Metadata: Send + Sync {
    /// Get statistics for a specific epoch.
    fn get_epoch_stats(&self, epoch_id: &str) -> Option<EpochStats>;

    /// Get the worker that holds a specific epoch.
    fn get_epoch_worker(&self, epoch_id: &str) -> Option<WorkerId>;

    /// Get all epochs for a table within a time range.
    /// Returns (epoch_id, worker_id) pairs.
    fn get_epochs_for_table(
        &self,
        table_name: &str,
        start_epoch: Option<&str>,
        end_epoch: Option<&str>,
    ) -> Vec<(String, WorkerId)>;

    /// Get aggregated stats for all epochs of a table scan.
    fn get_table_scan_stats(&self, table_name: &str) -> TableScanStats {
        let epochs = self.get_epochs_for_table(table_name, None, None);
        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut workers = Vec::new();
        let mut all_exact = true;

        for (epoch_id, worker_id) in &epochs {
            if let Some(stats) = self.get_epoch_stats(epoch_id) {
                total_rows += stats.rows;
                total_bytes += stats.bytes;
                if stats.stats_source != StatsSource::Exact {
                    all_exact = false;
                }
            } else {
                all_exact = false;
            }

            if !workers.contains(worker_id) {
                workers.push(worker_id.clone());
            }
        }

        let stats_source = if all_exact && !epochs.is_empty() {
            StatsSource::Exact
        } else if !epochs.is_empty() {
            StatsSource::Estimated
        } else {
            StatsSource::Unknown
        };

        TableScanStats {
            rows: total_rows,
            bytes: total_bytes,
            workers,
            stats_source,
            distribution: None,
        }
    }
}

/// Aggregated statistics for a table scan across all relevant epochs.
#[derive(Clone, Debug, Default)]
pub struct TableScanStats {
    /// Total rows across all scanned epochs.
    pub rows: u64,
    /// Total bytes across all scanned epochs.
    pub bytes: u64,
    /// Workers that hold the epochs.
    pub workers: Vec<WorkerId>,
    /// Combined confidence level (Exact only if ALL epoch stats are exact).
    pub stats_source: StatsSource,
    /// Optional explicit distribution (e.g., Replicated, HashPartitioned)
    /// provided by control-plane metadata.
    pub distribution: Option<Distribution>,
}

impl TableScanStats {
    /// Convert to a Distribution annotation.
    pub fn to_distribution(&self) -> Distribution {
        if let Some(distribution) = &self.distribution {
            return distribution.clone();
        }

        if self.workers.len() == 1 {
            Distribution::Singleton {
                worker: self.workers[0].clone(),
            }
        } else {
            Distribution::EpochPartitioned {
                workers: self.workers.clone(),
            }
        }
    }
}

/// In-memory implementation of Metadata for testing and simple use cases.
///
/// Uses interior mutability (`DashMap`) so metadata can be registered after
/// the instance is shared behind `Arc` (e.g., in the coordinator).
pub struct InMemoryMetadata {
    /// epoch_id -> (stats, worker_id)
    epochs: DashMap<String, (EpochStats, WorkerId)>,
    /// table_name -> vec of (epoch_id, time_range)
    table_epochs: DashMap<String, Vec<(String, TimeRange)>>,
    /// table_name -> table-level scan stats from master/control plane
    table_stats: DashMap<String, TableScanStats>,
}

impl std::fmt::Debug for InMemoryMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryMetadata")
            .field("epochs", &self.epochs)
            .field("table_epochs", &self.table_epochs)
            .field("table_stats", &self.table_stats)
            .finish()
    }
}

impl Default for InMemoryMetadata {
    fn default() -> Self {
        Self {
            epochs: DashMap::new(),
            table_epochs: DashMap::new(),
            table_stats: DashMap::new(),
        }
    }
}

impl InMemoryMetadata {
    /// Create a new empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an epoch with its stats and worker.
    pub fn add_epoch(
        &self,
        epoch_id: impl Into<String>,
        table_name: impl Into<String>,
        stats: EpochStats,
        worker: WorkerId,
    ) {
        let epoch_id = epoch_id.into();
        let table_name = table_name.into();

        self.epochs.insert(epoch_id.clone(), (stats, worker));
        self.table_epochs
            .entry(table_name)
            .or_default()
            .push((epoch_id, None));
    }

    /// Register an epoch with time range for time-based filtering.
    pub fn add_epoch_with_time_range(
        &self,
        epoch_id: impl Into<String>,
        table_name: impl Into<String>,
        stats: EpochStats,
        worker: WorkerId,
        time_range: (u64, u64),
    ) {
        let epoch_id = epoch_id.into();
        let table_name = table_name.into();

        self.epochs.insert(epoch_id.clone(), (stats, worker));
        self.table_epochs
            .entry(table_name)
            .or_default()
            .push((epoch_id, Some(time_range)));
    }

    /// Builder method to add an epoch.
    pub fn with_epoch(
        self,
        epoch_id: impl Into<String>,
        table_name: impl Into<String>,
        stats: EpochStats,
        worker: WorkerId,
    ) -> Self {
        self.add_epoch(epoch_id, table_name, stats, worker);
        self
    }

    /// Builder method to add an epoch with time range.
    pub fn with_epoch_time_range(
        self,
        epoch_id: impl Into<String>,
        table_name: impl Into<String>,
        stats: EpochStats,
        worker: WorkerId,
        time_range: (u64, u64),
    ) -> Self {
        self.add_epoch_with_time_range(epoch_id, table_name, stats, worker, time_range);
        self
    }

    /// Sort epochs by time range start for a table.
    pub fn sort_epochs_by_time(&self, table_name: &str) {
        if let Some(mut epochs) = self.table_epochs.get_mut(table_name) {
            epochs.sort_by_key(|(_, time_range)| time_range.map(|(start, _)| start).unwrap_or(0));
        }
    }

    /// Register table scan statistics received from control plane.
    pub fn register_table_stats(
        &self,
        table_name: &str,
        workers: Vec<WorkerId>,
        distribution: Distribution,
        row_count: u64,
        byte_size: u64,
        stats_source: StatsSource,
    ) {
        self.table_stats.insert(
            table_name.to_string(),
            TableScanStats {
                rows: row_count,
                bytes: byte_size,
                workers,
                stats_source,
                distribution: Some(distribution),
            },
        );
    }
}

impl Metadata for InMemoryMetadata {
    fn get_epoch_stats(&self, epoch_id: &str) -> Option<EpochStats> {
        self.epochs.get(epoch_id).map(|entry| entry.0.clone())
    }

    fn get_epoch_worker(&self, epoch_id: &str) -> Option<WorkerId> {
        self.epochs.get(epoch_id).map(|entry| entry.1.clone())
    }

    fn get_epochs_for_table(
        &self,
        table_name: &str,
        start_epoch: Option<&str>,
        end_epoch: Option<&str>,
    ) -> Vec<(String, WorkerId)> {
        // Parse start/end as millisecond timestamps if possible
        let start_ms = start_epoch.and_then(|s| s.parse::<u64>().ok());
        let end_ms = end_epoch.and_then(|s| s.parse::<u64>().ok());

        // The SQL transformer rewrites table names to macro calls, e.g.
        // `orders` → `scan_orders(...)`.  The metadata stores epochs under the
        // original base table name.  If a direct lookup fails and the name
        // starts with `scan_`, fall back to the base name so the annotator
        // resolves the correct distribution.
        let resolved_name = if self.table_epochs.contains_key(table_name) {
            table_name.to_string()
        } else if let Some(base) = table_name.strip_prefix("scan_") {
            if self.table_epochs.contains_key(base) {
                base.to_string()
            } else {
                table_name.to_string()
            }
        } else {
            table_name.to_string()
        };

        self.table_epochs
            .get(&resolved_name)
            .map(|epoch_entries| {
                epoch_entries
                    .iter()
                    .filter_map(|(id, time_range)| {
                        // Check if this epoch matches the time range filter
                        let matches = match (time_range, start_ms, end_ms) {
                            // No time range on epoch or no filter -> include
                            (None, _, _) => true,
                            (_, None, None) => true,
                            // Have time range, check overlap
                            (Some((epoch_start, epoch_end)), query_start, query_end) => {
                                let qs = query_start.unwrap_or(0);
                                let qe = query_end.unwrap_or(u64::MAX);
                                // Overlap: epoch_start < query_end AND epoch_end > query_start
                                *epoch_start < qe && *epoch_end > qs
                            }
                        };

                        if matches {
                            self.epochs
                                .get(id)
                                .map(|entry| (id.clone(), entry.1.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_table_scan_stats(&self, table_name: &str) -> TableScanStats {
        // If we have registered table stats from control plane, use those.
        // Try direct lookup first; if the name starts with `scan_`, also
        // check the base name (the SQL transformer rewrites `orders` →
        // `scan_orders(...)`).
        if let Some(scan_stats) = self.table_stats.get(table_name) {
            return scan_stats.clone();
        }
        if let Some(base) = table_name.strip_prefix("scan_") {
            if let Some(scan_stats) = self.table_stats.get(base) {
                return scan_stats.clone();
            }
        }

        // Otherwise, aggregate from epoch metadata
        // (get_epochs_for_table already handles scan_ prefix resolution)
        let epochs = self.get_epochs_for_table(table_name, None, None);
        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut workers = Vec::new();
        let mut all_exact = true;

        for (epoch_id, worker_id) in &epochs {
            if let Some(stats) = self.get_epoch_stats(epoch_id) {
                total_rows += stats.rows;
                total_bytes += stats.bytes;
                if stats.stats_source != StatsSource::Exact {
                    all_exact = false;
                }
            } else {
                all_exact = false;
            }

            if !workers.contains(worker_id) {
                workers.push(worker_id.clone());
            }
        }

        let stats_source = if all_exact && !epochs.is_empty() {
            StatsSource::Exact
        } else if !epochs.is_empty() {
            StatsSource::Estimated
        } else {
            StatsSource::Unknown
        };

        TableScanStats {
            rows: total_rows,
            bytes: total_bytes,
            workers,
            stats_source,
            distribution: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_metadata() {
        let metadata = InMemoryMetadata::new()
            .with_epoch("e1", "sales", EpochStats::exact(1000, 10000), "w1".into())
            .with_epoch("e2", "sales", EpochStats::exact(2000, 20000), "w2".into())
            .with_epoch("e3", "products", EpochStats::exact(100, 1000), "w1".into());

        // Check epoch stats
        let stats = metadata.get_epoch_stats("e1").unwrap();
        assert_eq!(stats.rows, 1000);
        assert_eq!(stats.bytes, 10000);
        assert!(stats.stats_source.is_exact());

        // Check epoch worker
        assert_eq!(metadata.get_epoch_worker("e1"), Some("w1".into()));
        assert_eq!(metadata.get_epoch_worker("e2"), Some("w2".into()));

        // Check table epochs
        let sales_epochs = metadata.get_epochs_for_table("sales", None, None);
        assert_eq!(sales_epochs.len(), 2);

        // Check aggregated stats
        let scan_stats = metadata.get_table_scan_stats("sales");
        assert_eq!(scan_stats.rows, 3000);
        assert_eq!(scan_stats.bytes, 30000);
        assert_eq!(scan_stats.workers.len(), 2);
        assert!(scan_stats.stats_source.is_exact());
    }

    #[test]
    fn test_table_scan_stats_distribution() {
        let single_worker = TableScanStats {
            rows: 1000,
            bytes: 10000,
            workers: vec!["w1".into()],
            stats_source: StatsSource::Exact,
            distribution: None,
        };
        assert!(matches!(
            single_worker.to_distribution(),
            Distribution::Singleton { .. }
        ));

        let multi_worker = TableScanStats {
            rows: 1000,
            bytes: 10000,
            workers: vec!["w1".into(), "w2".into()],
            stats_source: StatsSource::Exact,
            distribution: None,
        };
        assert!(matches!(
            multi_worker.to_distribution(),
            Distribution::EpochPartitioned { .. }
        ));
    }

    #[test]
    fn test_time_range_filtering() {
        let metadata = InMemoryMetadata::new()
            // Epoch 1: time 1000-2000
            .with_epoch_time_range(
                "e1",
                "sales",
                EpochStats::exact(100, 1000),
                "w1".into(),
                (1000, 2000),
            )
            // Epoch 2: time 2000-3000
            .with_epoch_time_range(
                "e2",
                "sales",
                EpochStats::exact(200, 2000),
                "w2".into(),
                (2000, 3000),
            )
            // Epoch 3: time 3000-4000
            .with_epoch_time_range(
                "e3",
                "sales",
                EpochStats::exact(150, 1500),
                "w1".into(),
                (3000, 4000),
            );

        // Query all epochs (no filter)
        let all_epochs = metadata.get_epochs_for_table("sales", None, None);
        assert_eq!(all_epochs.len(), 3);

        // Query epochs overlapping time range 1500-2500
        let filtered = metadata.get_epochs_for_table("sales", Some("1500"), Some("2500"));
        assert_eq!(filtered.len(), 2); // e1 and e2 overlap

        // Query only e2 time range
        let filtered = metadata.get_epochs_for_table("sales", Some("2500"), Some("2600"));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "e2");

        // Query time range before all epochs
        let filtered = metadata.get_epochs_for_table("sales", Some("0"), Some("1000"));
        assert_eq!(filtered.len(), 0);

        // Query time range after all epochs
        let filtered = metadata.get_epochs_for_table("sales", Some("5000"), Some("6000"));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_mixed_epochs_with_and_without_time_range() {
        let metadata = InMemoryMetadata::new()
            // Epoch without time range (always matches)
            .with_epoch("e1", "sales", EpochStats::exact(100, 1000), "w1".into())
            // Epoch with time range
            .with_epoch_time_range(
                "e2",
                "sales",
                EpochStats::exact(200, 2000),
                "w2".into(),
                (2000, 3000),
            );

        // Query with time filter - e1 always matches, e2 only if overlaps
        let filtered = metadata.get_epochs_for_table("sales", Some("5000"), Some("6000"));
        // e1 has no time range so it matches, e2 doesn't overlap
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "e1");

        // Query overlapping e2's time range
        let filtered = metadata.get_epochs_for_table("sales", Some("2500"), Some("2600"));
        assert_eq!(filtered.len(), 2); // Both match
    }

    #[test]
    fn test_sort_epochs_by_time() {
        let metadata = InMemoryMetadata::new();

        // Add epochs out of order
        metadata.add_epoch_with_time_range(
            "e3",
            "sales",
            EpochStats::exact(150, 1500),
            "w1".into(),
            (3000, 4000),
        );
        metadata.add_epoch_with_time_range(
            "e1",
            "sales",
            EpochStats::exact(100, 1000),
            "w1".into(),
            (1000, 2000),
        );
        metadata.add_epoch_with_time_range(
            "e2",
            "sales",
            EpochStats::exact(200, 2000),
            "w2".into(),
            (2000, 3000),
        );

        // Sort by time
        metadata.sort_epochs_by_time("sales");

        // Verify order
        let epochs = metadata.get_epochs_for_table("sales", None, None);
        assert_eq!(epochs[0].0, "e1");
        assert_eq!(epochs[1].0, "e2");
        assert_eq!(epochs[2].0, "e3");
    }

    #[test]
    fn test_exact_stats_enables_registered_distribution() {
        let metadata = InMemoryMetadata::new();
        metadata.register_table_stats(
            "products",
            vec!["w1".into()],
            Distribution::Singleton {
                worker: "w1".into(),
            },
            10_000,
            5_000_000,
            StatsSource::Exact,
        );

        let scan_stats = metadata.get_table_scan_stats("products");
        assert_eq!(scan_stats.rows, 10_000);
        assert_eq!(scan_stats.bytes, 5_000_000);
        assert_eq!(scan_stats.stats_source, StatsSource::Exact);
        assert!(matches!(
            scan_stats.to_distribution(),
            Distribution::Singleton { .. }
        ));
    }

    #[test]
    fn test_unknown_stats_keeps_conservative_source() {
        let metadata = InMemoryMetadata::new();
        metadata.register_table_stats(
            "products",
            vec!["w1".into()],
            Distribution::Singleton {
                worker: "w1".into(),
            },
            0,
            0,
            StatsSource::Unknown,
        );

        let scan_stats = metadata.get_table_scan_stats("products");
        assert_eq!(scan_stats.rows, 0);
        assert_eq!(scan_stats.bytes, 0);
        assert_eq!(scan_stats.stats_source, StatsSource::Unknown);
    }
}
