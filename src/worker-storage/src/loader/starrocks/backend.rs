use crate::engine::storage_engine::{RecordBatchStream, StorageError};
use crate::loader::adapter::SourceAdapter;
use crate::loader::types::{Catalog, DataSource, SourceError};
use arrow::datatypes::Schema;

use std::sync::Arc;

use super::adbc_client::StarRocksAdbcClient;
use super::jdbc_client::StarRocksJdbcClient;

#[derive(Clone)]
pub enum StarRocksBackend {
    Adbc(StarRocksAdbcClient),
    Jdbc(StarRocksJdbcClient),
}

impl SourceAdapter for StarRocksBackend {
    fn stream_data(
        &self,
        source: Arc<DataSource>,
        schema: Arc<Schema>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<RecordBatchStream, StorageError>> + Send>,
    > {
        match self {
            StarRocksBackend::Adbc(client) => client.stream_data(source, schema),
            StarRocksBackend::Jdbc(client) => client.stream_data(source, schema),
        }
    }
}

impl StarRocksBackend {
    pub async fn from_catalog(catalog: Catalog) -> anyhow::Result<Self> {
        match catalog {
            Catalog::StarRocks { .. } => {
                let client = StarRocksAdbcClient::from_catalog(catalog.clone(), None)?;
                Ok(StarRocksBackend::Adbc(client))
            }
            Catalog::Jdbc { .. } => {
                let client = StarRocksJdbcClient::from_catalog(catalog.clone()).await?;
                Ok(StarRocksBackend::Jdbc(client))
            }
            _ => Err(anyhow::anyhow!(SourceError::UnsupportedCatalog {
                catalog: format!("Unsupported catalog type: {:?}", catalog),
            })),
        }
    }
}
