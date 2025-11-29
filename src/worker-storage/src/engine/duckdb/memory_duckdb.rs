use super::helper::*;
use super::memory_duckdb_runtime::*;
use super::DuckDBConfig;
use crate::engine::engine::{EngineMetrics, EpochView, TableMetadata};
use std::future::Future;
use tokio::sync;
use tokio::sync::oneshot;
use tracing::{error, info};

/// An in-memory DuckDB storage engine for the acceleration layer.
pub struct MemoryDuckDBEngine {
    com_tx: sync::mpsc::Sender<EngineCom>,
}

impl MemoryDuckDBEngine {
    pub fn new(config: DuckDBConfig) -> anyhow::Result<Self> {
        info!("MemoryDuckDBEngine starting with config: {:?}", config);
        let (tx, rx) = sync::mpsc::channel(config.channel_capacity);
        let thread_config = config.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = engine_main(thread_config, rx) {
                error!("Engine loop exited with error: {}", e);
            }
        });
        Ok(Self { com_tx: tx })
    }

    pub fn with_defaults() -> anyhow::Result<Self> {
        Self::new(DuckDBConfig::default())
    }
}


impl crate::engine::engine::StorageEngine for MemoryDuckDBEngine {
    fn create_epoch_table(
        &self,
        dataset_id: String,
        epoch: EpochView,
        mut arrow_stream: crate::engine::engine::RecordBatchStream,
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
    ) -> impl Future<Output = anyhow::Result<crate::engine::engine::MemoryStats>> + Send {
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

    fn shutdown(self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async move {
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
}
