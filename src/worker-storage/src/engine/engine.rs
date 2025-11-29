use arrow::array::RecordBatch;
use futures_core::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use arrow::datatypes::Schema;
use thiserror::Error;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, StorageError>> + Send>>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Backend error ({backend}): {message}")]
    Backend {
        backend: &'static str,
        message: String,
    },

    #[error("Engine shutting down")]
    ShuttingDown,
}

#[derive(Clone, Debug)]
pub struct EpochMemoryStats {
    pub dataset_id: String,
    pub epoch_id: String,
    pub rows_count: u64,
    pub approx_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Sum of approx_loaded_bytes for all currently registered epochs.
    pub total_storage_bytes_estimate: u64,

    /// Per-epoch storage usage.
    pub epochs: Vec<EpochMemoryStats>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TableMetadata {
    pub table_name: String,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub create_at: u64,
    pub schema: Arc<Schema>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EpochView {
    pub epoch_id: String,
    pub table_name: String,
    pub time_range: (u64, u64),
    pub time_column_name: String,
    pub time_partition_value: String,
    pub dimension_values: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct EngineMetrics {
    pub total_batches_written: u64,
    pub total_rows_written: u64,
    pub total_flushes: u64,
    pub active_epochs: usize,
    pub committed_epochs: usize,
}

/// StorageEngine is responsible for:
/// - Creating immutable epoch tables
/// - Dropping epoch tables when they're no longer needed (e.g., TTL expired, memory pressure)
/// - Maintaining queryable epochs for a dataset
/// - Reporting memory usage for quota enforcement
///
///
/// Note: Table names are deterministically derived as `{dataset_id}__{epoch_id}` to ensure
/// consistency and prevent naming collisions.
pub trait StorageEngine: Send + Sync {
    /// create_epoch_table. Executes DDL to create epoch table and load Arrow data
    ///
    /// This method ingests data into the acceleration layer. It:
    /// 1. Creates a physical table with name `{dataset_id}__{epoch.epoch_id}`
    /// 2. Streams Arrow RecordBatches into the table (zero-copy ingestion where possible)
    /// 3. Returns metadata about the created table (row count, memory footprint, etc.)
    ///
    /// # Arguments
    /// * `dataset_id` - The logical dataset identifier; used as part of the table name
    /// * `epoch` - An `EpochView` containing the epoch ID, time range, and partition info
    /// * `arrow_stream` - A stream of Arrow RecordBatches to ingest
    fn create_epoch_table(
        &self,
        dataset_id: String,
        epoch: EpochView,
        arrow_stream: RecordBatchStream,
    ) -> impl Future<Output = anyhow::Result<TableMetadata>> + Send;

    /// Drops an epoch table and frees its memory.
    ///
    /// This method is called when:
    /// - An epoch's TTL has expired and should no longer be queryable
    /// - Memory pressure requires evicting data (LRU or benefit-driven eviction)
    /// - The dataset is being retired or replaced
    ///
    /// After successful completion, the physical table `{dataset_id}__{epoch_id}` is removed
    /// from the storage engine and all associated memory is freed.
    ///
    /// # Arguments
    /// * `dataset_id` - The logical dataset identifier; used to reconstruct table name
    /// * `epoch_id` - The epoch identifier; used to reconstruct table name
    ///
    /// # Returns
    /// Returns `Ok(())` on success. The table is guaranteed to be removed from storage.
    fn drop_epoch_table(
        &self,
        dataset_id: String,
        epoch_id: String,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Lists all epochs currently stored for a dataset.
    ///
    /// This method provides a snapshot of the active epochs in the storage engine. It's used for:
    /// - Validating dataset state after operations
    /// - Rebuilding the dataset view after crashes
    /// - Observability and debugging
    ///
    /// # Arguments
    /// * `dataset_id` - The logical dataset identifier
    fn list_epochs(
        &self,
        dataset_id: String,
    ) -> impl Future<Output = anyhow::Result<Vec<EpochView>>> + Send;

    /// memory_stats is called periodically to:
    /// - Monitor per-worker memory consumption against quota
    /// - Feed observability/metrics systems
    /// - Make admission control and eviction decisions
    ///
    ///  Returns `None` if memory statistics are not available (e.g., not yet initialized).
    fn memory_stats(&self) -> impl Future<Output = anyhow::Result<MemoryStats>> + Send;

    fn get_metrics(&self) -> impl Future<Output = anyhow::Result<EngineMetrics>> + Send;

    fn shutdown(self) -> impl Future<Output = anyhow::Result<()>> + Send;
}
