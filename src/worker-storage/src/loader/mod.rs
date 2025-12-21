pub mod adapter;
pub mod types;
mod starrocks;

use self::adapter::SourceAdapter;
use self::types::{Catalog, LoadRequest};
use crate::engine::storage_engine::{StorageEngine, TableMetadata};
use std::sync::Arc;

pub struct DataLoader<E: StorageEngine> {
    engine: Arc<E>,
}

impl<E: StorageEngine> DataLoader<E> {
    pub fn new(engine: Arc<E>) -> Self {
        Self { engine }
    }

    pub async fn load_epoch(&self, request: LoadRequest) -> anyhow::Result<TableMetadata> {
        // 1. Select appropriate adapter based on request.source.catalog
        let adapter = self.get_adapter(&request.source.catalog)?;

        let data_source = Arc::new(request.source.clone());
        // 2. Fetch data stream
        let stream = adapter
            .stream_data(data_source, request.schema.clone())
            .await?;

        // 3. Load into engine
        self.engine
            .create_epoch_table(request.dataset_id, request.epoch_view, stream)
            .await
    }

    fn get_adapter(&self, catalog: &Catalog) -> anyhow::Result<Box<dyn SourceAdapter>> {
        match catalog {
            Catalog::Iceberg { .. } => {
                todo!("StarRocks adapter not implemented yet")
            }
            Catalog::StarRocks { .. } => {
                todo!("StarRocks adapter not implemented yet")
            }
            Catalog::Jdbc { .. } => {
                todo!("StarRocks adapter not implemented yet")
            }
        }
    }
}
