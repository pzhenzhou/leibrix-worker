use super::types::DataSource;
use crate::engine::engine::{RecordBatchStream, StorageError};
use arrow::datatypes::Schema;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub trait SourceAdapter: Send + Sync {
    /// Returns a stream of Arrow RecordBatches from the source.
    fn stream_data(
        &self,
        source: Arc<DataSource>,
        schema: Arc<Schema>,
    ) -> Pin<Box<dyn Future<Output = Result<RecordBatchStream, StorageError>> + Send>>;
}
