use super::helper::*;
use super::memory_duckdb_runtime::*;
use super::DuckDBConfig;
use super::SharedDatabase;
use crate::engine::storage_engine::{EngineMetrics, EpochView, TableMetadata};
use crate::ldp::{EpochStats, StatsSource};
use std::future::Future;
use std::sync::Arc;
use tokio::sync;
use tokio::sync::oneshot;
use tracing::{error, info};

/// An in-memory DuckDB storage engine for the acceleration layer.
///
/// Cloning produces a second handle to the **same** background engine thread.
/// All clones share the same `in_progress` / `committed` state via the channel.
#[derive(Clone)]
pub struct MemoryDuckDBEngine {
    com_tx: sync::mpsc::Sender<EngineCom>,
    shared_db: Arc<SharedDatabase>,
}

impl MemoryDuckDBEngine {
    /// Creates a new engine with a shared database.
    /// The shared database can be used by QueryEngine for read operations.
    pub fn new(config: DuckDBConfig, shared_db: Arc<SharedDatabase>) -> anyhow::Result<Self> {
        info!("MemoryDuckDBEngine starting with config: {:?}", config);

        // Get a pooled connection for the storage engine (writer)
        // This connection will be held for the lifetime of the engine
        let writer_conn = shared_db.get()?;

        let (tx, rx) = sync::mpsc::channel(config.channel_capacity);
        let thread_config = config.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = engine_main(thread_config, rx, writer_conn) {
                error!("Engine loop exited with error: {}", e);
            }
        });
        Ok(Self {
            com_tx: tx,
            shared_db,
        })
    }

    /// Creates a new engine with a fresh in-memory database.
    /// Convenience method that creates its own SharedDatabase.
    pub fn new_with_fresh_db(config: DuckDBConfig) -> anyhow::Result<Self> {
        let shared_db = Arc::new(SharedDatabase::new(&config)?);
        Self::new(config, shared_db)
    }

    pub fn with_defaults() -> anyhow::Result<Self> {
        Self::new_with_fresh_db(DuckDBConfig::default())
    }

    /// Returns the shared database handle.
    /// Use this to create a QueryEngine that reads from the same database.
    pub fn shared_database(&self) -> Arc<SharedDatabase> {
        self.shared_db.clone()
    }
}

impl crate::engine::storage_engine::StorageEngine for MemoryDuckDBEngine {
    fn create_epoch_table(
        &self,
        dataset_id: String,
        epoch: EpochView,
        mut arrow_stream: crate::engine::storage_engine::RecordBatchStream,
    ) -> impl Future<Output = anyhow::Result<TableMetadata>> + Send {
        let tx = self.com_tx.clone();
        async move {
            use futures_util::StreamExt;

            // Get the schema from the first batch
            let first_batch = arrow_stream
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("Arrow stream is empty"))??;
            let schema = first_batch.schema();

            // Send StartEpoch command
            let (done_tx, done_rx) = oneshot::channel();
            let key = epoch_key(&dataset_id, &epoch.epoch_id);
            tx.send(EngineCom::StartEpoch {
                dataset_id: dataset_id.clone(),
                epoch_view: epoch,
                schema,
                done: done_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            // Send the first batch
            tx.send(EngineCom::IngestBatch {
                key: key.clone(),
                batch: first_batch,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            // Stream remaining batches
            while let Some(batch_result) = arrow_stream.next().await {
                let batch = batch_result?;
                tx.send(EngineCom::IngestBatch {
                    key: key.clone(),
                    batch,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;
            }

            // Send FinishEpoch command
            tx.send(EngineCom::FinishEpoch { key })
                .await
                .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            // Wait for completion
            done_rx
                .await
                .map_err(|_| anyhow::anyhow!("Done channel closed"))?
        }
    }

    fn drop_epoch_table(
        &self,
        dataset_id: String,
        epoch_id: String,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        let tx = self.com_tx.clone();
        async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(EngineCom::DropEpoch {
                dataset_id,
                epoch_id,
                resp: resp_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("Response channel closed"))?
        }
    }

    fn list_epochs(
        &self,
        dataset_id: String,
    ) -> impl Future<Output = anyhow::Result<Vec<EpochView>>> + Send {
        let tx = self.com_tx.clone();
        async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(EngineCom::ListEpoch {
                dataset_id,
                resp: resp_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("Response channel closed"))?
        }
    }

    fn memory_stats(
        &self,
    ) -> impl Future<Output = anyhow::Result<crate::engine::storage_engine::MemoryStats>> + Send
    {
        let tx = self.com_tx.clone();
        async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(EngineCom::MemoryStats { resp: resp_tx })
                .await
                .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("Response channel closed"))?
        }
    }

    fn get_metrics(&self) -> impl Future<Output = anyhow::Result<EngineMetrics>> + Send {
        let tx = self.com_tx.clone();
        async move {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(EngineCom::GetMetrics { resp: resp_tx })
                .await
                .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("Response channel closed"))?
        }
    }

    fn get_epoch_stats(
        &self,
        dataset_id: &str,
        epoch_id: &str,
    ) -> impl Future<Output = anyhow::Result<Option<EpochStats>>> + Send {
        let tx = self.com_tx.clone();
        let dataset_id = dataset_id.to_string();
        let epoch_id = epoch_id.to_string();
        async move {
            // First get memory stats to find the epoch
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(EngineCom::MemoryStats { resp: resp_tx })
                .await
                .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

            let stats = resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("Response channel closed"))??;

            // Find the epoch in the stats
            let epoch_stat = stats
                .epochs
                .iter()
                .find(|e| e.dataset_id == dataset_id && e.epoch_id == epoch_id);

            Ok(epoch_stat.map(|e| EpochStats {
                rows: e.rows_count,
                bytes: e.approx_bytes,
                ndv: std::collections::HashMap::new(), // NDV not yet tracked
                stats_source: StatsSource::Exact,      // From ingestion metadata
            }))
        }
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.com_tx
            .send(EngineCom::Shutdown { resp: resp_tx })
            .await
            .map_err(|_| anyhow::anyhow!("Engine channel closed"))?;

        resp_rx
            .await
            .map_err(|_| anyhow::anyhow!("Response channel closed"))?
    }
}
