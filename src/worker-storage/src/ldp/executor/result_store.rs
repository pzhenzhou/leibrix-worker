//! Stage result caching for distributed LDP execution.
//!
//! This module provides a `StageResultStore` that caches stage execution results
//! with TTL-based eviction for efficient retrieval via Arrow Flight DoGet.
//! Each worker maintains its own result store to serve results to other workers
//! and the coordinator.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;
use tracing::{debug, error};

use super::stage::StageExecutionStats;


/// Cached result of a stage execution.
#[derive(Debug)]
pub struct CachedStageResult {
    /// Output batches from the stage.
    pub batches: Vec<RecordBatch>,
    /// Execution statistics.
    pub stats: StageExecutionStats,
    /// When the result was created.
    pub created_at: Instant,
}

impl CachedStageResult {
    /// Create a new cached stage result.
    pub fn new(batches: Vec<RecordBatch>, stats: StageExecutionStats) -> Self {
        Self {
            batches,
            stats,
            created_at: Instant::now(),
        }
    }
}

/// Store for caching stage execution results with TTL-based eviction.
pub struct StageResultStore {
    /// Cache mapping (tenant_id, query_id, stage_id) to cached results.
    cache: RwLock<HashMap<(String, String, u32), CachedStageResult>>,
    /// Time-to-live for cached results.
    ttl: Duration,
    /// Cleanup interval for expired results.
    cleanup_interval: Duration,
}

impl StageResultStore {
    /// Create a new result store with the specified TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl,
            cleanup_interval: Duration::from_secs(60), // Cleanup every minute
        }
    }

    /// Create a new result store with a default TTL of 5 minutes.
    pub fn default() -> Self {
        Self::new(Duration::from_secs(300)) // 5 minutes
    }

    /// Register a stage result for later retrieval.
    pub async fn register_result(
        &self,
        tenant_id: String,
        query_id: String,
        stage_id: u32,
        batches: Vec<RecordBatch>,
        stats: StageExecutionStats,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = (tenant_id, query_id, stage_id);
        let result = CachedStageResult::new(batches, stats);

        {
            let mut cache = self.cache.write().await;
            cache.insert(key.clone(), result);
        }

        debug!(
            "Registered stage result: tenant={}, query={}, stage={}",
            key.0, key.1, key.2
        );

        Ok(())
    }

    /// Get a stage result by key.
    pub async fn get_result(
        &self,
        tenant_id: &str,
        query_id: &str,
        stage_id: u32,
    ) -> Result<Option<Vec<RecordBatch>>, Box<dyn std::error::Error + Send + Sync>> {
        let key = (tenant_id.to_string(), query_id.to_string(), stage_id);

        {
            let cache = self.cache.read().await;
            if let Some(result) = cache.get(&key) {
                // Check if result is expired
                if result.created_at.elapsed() > self.ttl {
                    debug!(
                        "Cached result expired: tenant={}, query={}, stage={}",
                        key.0, key.1, key.2
                    );
                    return Ok(None);
                }

                debug!(
                    "Retrieved cached result: tenant={}, query={}, stage={}",
                    key.0, key.1, key.2
                );
                return Ok(Some(result.batches.clone()));
            }
        }

        debug!(
            "No cached result found: tenant={}, query={}, stage={}",
            key.0, key.1, key.2
        );
        Ok(None)
    }

    /// Remove a specific result from the cache.
    pub async fn remove_result(
        &self,
        tenant_id: &str,
        query_id: &str,
        stage_id: u32,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let key = (tenant_id.to_string(), query_id.to_string(), stage_id);

        let mut cache = self.cache.write().await;
        let existed = cache.remove(&key).is_some();

        if existed {
            debug!(
                "Removed cached result: tenant={}, query={}, stage={}",
                key.0, key.1, key.2
            );
        }

        Ok(existed)
    }

    /// Remove all results for a specific query.
    pub async fn remove_query_results(
        &self,
        tenant_id: &str,
        query_id: &str,
    ) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let mut cache = self.cache.write().await;
        let mut removed_count = 0;

        let keys_to_remove: Vec<_> = cache
            .keys()
            .filter(|(t, q, _)| t == tenant_id && q == query_id)
            .cloned()
            .collect();

        for key in keys_to_remove {
            cache.remove(&key);
            removed_count += 1;
            debug!(
                "Removed cached result for query: tenant={}, query={}, stage={}",
                key.0, key.1, key.2
            );
        }

        Ok(removed_count)
    }

    /// Cleanup expired results from the cache.
    pub async fn cleanup_expired(&self) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let mut cache = self.cache.write().await;
        let now = Instant::now();
        let ttl = self.ttl;

        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|(_, result)| now.duration_since(result.created_at) > ttl)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &keys_to_remove {
            cache.remove(key);
            debug!(
                "Expired cached result: tenant={}, query={}, stage={}",
                key.0, key.1, key.2
            );
        }

        Ok(keys_to_remove.len() as u32)
    }

    /// Start background cleanup task.
    pub async fn start_cleanup_task(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cleanup_interval = self.cleanup_interval;
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cleanup_interval).await;
                
                match self.cleanup_expired().await {
                    Ok(count) => {
                        if count > 0 {
                            debug!("Cleaned up {} expired stage results", count);
                        }
                    }
                    Err(e) => {
                        error!("Error during stage result cleanup: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> Result<StageResultStoreStats, Box<dyn std::error::Error + Send + Sync>> {
        let cache = self.cache.read().await;
        let total_entries = cache.len();

        let mut valid_count = 0;
        let mut expired_count = 0;

        for (_, result) in cache.iter() {
            if result.created_at.elapsed() > self.ttl {
                expired_count += 1;
            } else {
                valid_count += 1;
            }
        }

        Ok(StageResultStoreStats {
            total_entries,
            valid_entries: valid_count,
            expired_entries: expired_count,
        })
    }
}

/// Statistics about the StageResultStore.
#[derive(Debug)]
pub struct StageResultStoreStats {
    /// Total number of entries in the cache.
    pub total_entries: usize,
    /// Number of valid (non-expired) entries.
    pub valid_entries: usize,
    /// Number of expired entries (would be removed on cleanup).
    pub expired_entries: usize,
}

impl Default for StageResultStore {
    fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_register_and_get_result() {
        let store = StageResultStore::new(Duration::from_secs(10));
        let batch = create_test_batch();
        let stats = StageExecutionStats {
            rows_produced: 3,
            bytes_produced: 100,
            execution_time_ms: 50,
        };

        // Register result
        store
            .register_result(
                "tenant-1".to_string(),
                "query-1".to_string(),
                1,
                vec![batch.clone()],
                stats.clone(),
            )
            .await
            .unwrap();

        // Retrieve result
        let retrieved = store
            .get_result("tenant-1", "query-1", 1)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_expired_result() {
        let store = StageResultStore::new(Duration::from_millis(10));
        let batch = create_test_batch();
        let stats = StageExecutionStats::default();

        // Register result
        store
            .register_result(
                "tenant-1".to_string(),
                "query-1".to_string(),
                1,
                vec![batch],
                stats,
            )
            .await
            .unwrap();

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Try to retrieve - should return None
        let retrieved = store.get_result("tenant-1", "query-1", 1).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_nonexistent_result() {
        let store = StageResultStore::new(Duration::from_secs(10));

        // Try to retrieve non-existent result
        let retrieved = store.get_result("tenant-1", "query-1", 1).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_remove_result() {
        let store = StageResultStore::new(Duration::from_secs(10));
        let batch = create_test_batch();
        let stats = StageExecutionStats::default();

        // Register result
        store
            .register_result(
                "tenant-1".to_string(),
                "query-1".to_string(),
                1,
                vec![batch],
                stats,
            )
            .await
            .unwrap();

        // Remove result
        let removed = store.remove_result("tenant-1", "query-1", 1).await.unwrap();
        assert!(removed);

        // Try to retrieve - should return None
        let retrieved = store.get_result("tenant-1", "query-1", 1).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_remove_query_results() {
        let store = StageResultStore::new(Duration::from_secs(10));
        let batch = create_test_batch();
        let stats = StageExecutionStats::default();

        // Register multiple results for the same query
        store
            .register_result(
                "tenant-1".to_string(),
                "query-1".to_string(),
                1,
                vec![batch.clone()],
                stats.clone(),
            )
            .await
            .unwrap();

        store
            .register_result(
                "tenant-1".to_string(),
                "query-1".to_string(),
                2,
                vec![batch],
                stats,
            )
            .await
            .unwrap();

        // Remove all results for the query
        let removed_count = store
            .remove_query_results("tenant-1", "query-1")
            .await
            .unwrap();
        assert_eq!(removed_count, 2);

        // Try to retrieve - should return None
        let retrieved1 = store.get_result("tenant-1", "query-1", 1).await.unwrap();
        let retrieved2 = store.get_result("tenant-1", "query-1", 2).await.unwrap();
        assert!(retrieved1.is_none());
        assert!(retrieved2.is_none());
    }
}