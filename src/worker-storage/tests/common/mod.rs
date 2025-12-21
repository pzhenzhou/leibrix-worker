use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures_util::stream;
use std::collections::HashMap;
use std::sync::Arc;
use worker_storage::engine::duckdb::{DuckDBConfig, storage_engine_impl::MemoryDuckDBEngine};
use worker_storage::engine::storage_engine::{EpochView, RecordBatchStream, StorageError};

use std::sync::Once;

static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .with_test_writer() // important: routes to captured test output
            .init();
    });
}

/// Creates a lightweight test engine with reduced memory limits for faster tests
pub fn create_test_engine() -> MemoryDuckDBEngine {
    init_tracing();
    create_test_engine_with_memory(256) // 256 MB for tests
}

/// Creates a test engine with custom memory limit
pub fn create_test_engine_with_memory(memory_mb: u64) -> MemoryDuckDBEngine {
    let config = DuckDBConfig {
        memory_limit_mb: Some(memory_mb),
        flush_rows_threshold: 5_000,
        channel_capacity: 50,
        tmp_dir: None,
        max_identifiers: 100,
    };
    MemoryDuckDBEngine::new(config).expect("Failed to create test engine with custom memory")
}

/// Creates a simple test schema
pub fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]))
}

/// Creates a test RecordBatch with specified row count
pub fn create_test_batch(schema: Arc<Schema>, start_id: i64, row_count: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + row_count as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("name_{}", i)).collect();
    let values: Vec<i64> = ids.iter().map(|i| i * 10).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Creates a test Arrow stream from batches
pub fn create_test_stream(batches: Vec<RecordBatch>) -> RecordBatchStream {
    Box::pin(stream::iter(
        batches.into_iter().map(|b| Ok::<_, StorageError>(b)),
    ))
}

/// Creates a test EpochView
pub fn create_test_epoch(dataset_id: &str, epoch_id: &str) -> EpochView {
    EpochView {
        epoch_id: epoch_id.to_string(),
        table_name: format!("{}__{}", dataset_id, epoch_id),
        time_range: (1700000000, 1700086400), // Example timestamp range
        time_column_name: "dt".to_string(),
        time_partition_value: "2023-11-15".to_string(),
        dimension_values: HashMap::new(),
    }
}

/// Creates a test epoch with custom dimensions
#[allow(dead_code)]
pub fn create_test_epoch_with_dimensions(
    dataset_id: &str,
    epoch_id: &str,
    dimensions: HashMap<String, String>,
) -> EpochView {
    EpochView {
        epoch_id: epoch_id.to_string(),
        table_name: format!("{}__{}", dataset_id, epoch_id),
        time_range: (1700000000, 1700086400),
        time_column_name: "dt".to_string(),
        time_partition_value: "2023-11-15".to_string(),
        dimension_values: dimensions,
    }
}

/// Creates an empty stream (for error testing)
#[allow(dead_code)]
pub fn create_empty_stream() -> RecordBatchStream {
    Box::pin(stream::iter(Vec::<Result<RecordBatch, StorageError>>::new()))
}

/// Creates a stream that fails after N batches
#[allow(dead_code)]
pub fn create_failing_stream(
    successful_batches: Vec<RecordBatch>,
    error_message: &str,
) -> RecordBatchStream {
    let error = StorageError::Backend {
        backend: "test",
        message: error_message.to_string(),
    };

    let mut items: Vec<Result<RecordBatch, StorageError>> =
        successful_batches.into_iter().map(Ok).collect();
    items.push(Err(error));

    Box::pin(stream::iter(items))
}
