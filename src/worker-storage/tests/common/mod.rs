#![allow(dead_code)]

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures_util::stream;
use std::collections::HashMap;
use std::sync::Arc;
use worker_storage::engine::duckdb::DuckDBQueryEngine;
use worker_storage::engine::duckdb::{DuckDBConfig, SharedDatabase, storage_engine_impl::MemoryDuckDBEngine};
use worker_storage::engine::query_engine::QueryError;
use worker_storage::engine::storage_engine::{EpochView, RecordBatchStream, StorageError, StorageEngine};

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

/// Creates a shared in-memory database for tests
pub fn create_shared_database() -> Arc<SharedDatabase> {
    init_tracing();
    Arc::new(SharedDatabase::new(&DuckDBConfig::default()).expect("Failed to create shared database"))
}

/// Creates a lightweight test engine with reduced memory limits for faster tests
pub fn create_test_engine() -> MemoryDuckDBEngine {
    init_tracing();
    MemoryDuckDBEngine::with_defaults().expect("Failed to create test engine")
}

/// Creates a test engine with a shared database
pub fn create_test_engine_with_shared_db(shared_db: Arc<SharedDatabase>) -> MemoryDuckDBEngine {
    init_tracing();
    let config = DuckDBConfig {
        memory_limit_mb: Some(256),
        flush_rows_threshold: 5_000,
        channel_capacity: 50,
        tmp_dir: None,
        max_identifiers: 100,
        pool_size: 10,
        max_pool_size: 50,
    };
    MemoryDuckDBEngine::new(config, shared_db).expect("Failed to create test engine with shared db")
}

/// Creates a test engine with custom memory limit
pub fn create_test_engine_with_memory(memory_mb: u64) -> MemoryDuckDBEngine {
    let config = DuckDBConfig {
        memory_limit_mb: Some(memory_mb),
        flush_rows_threshold: 5_000,
        channel_capacity: 50,
        tmp_dir: None,
        max_identifiers: 100,
        pool_size: 10,
        max_pool_size: 50,
    };
    MemoryDuckDBEngine::new_with_fresh_db(config).expect("Failed to create test engine with custom memory")
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
        batches.into_iter().map(Ok::<_, StorageError>),
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

// ============================================================================
// QueryEngine Test Utilities
// ============================================================================

/// Creates a QueryEngine that shares the same database as the StorageEngine.
/// Uses the SharedDatabase handle for read-write separation.
pub fn create_test_query_engine(shared_db: Arc<SharedDatabase>, max_concurrent_queries: usize) -> DuckDBQueryEngine {
    DuckDBQueryEngine::new(shared_db, max_concurrent_queries)
}

/// Test fixture that sets up both StorageEngine and QueryEngine with pre-loaded data.
/// Both engines share the same in-memory database for read-write separation.
pub struct QueryEngineTestFixture {
    pub storage_engine: MemoryDuckDBEngine,
    pub query_engine: DuckDBQueryEngine,
    pub shared_db: Arc<SharedDatabase>,
    pub table_name: String,
    pub dataset_id: String,
    pub epoch_id: String,
    pub row_count: usize,
}

impl QueryEngineTestFixture {
    /// Creates a fixture with a single table containing the specified number of rows.
    pub async fn new(dataset_id: &str, epoch_id: &str, row_count: usize) -> Self {
        // Create a shared database
        let shared_db = create_shared_database();
        
        // Create storage engine (writer) using the shared database
        let storage_engine = create_test_engine_with_shared_db(shared_db.clone());
        
        // Create query engine (reader) using the same shared database
        let query_engine = create_test_query_engine(shared_db.clone(), 10);

        let schema = create_test_schema();
        let stream = create_test_stream(vec![create_test_batch(schema, 0, row_count)]);
        let epoch = create_test_epoch(dataset_id, epoch_id);
        let table_name = epoch.table_name.clone();

        storage_engine
            .create_epoch_table(dataset_id.to_string(), epoch, stream)
            .await
            .expect("Failed to create test table");

        Self {
            storage_engine,
            query_engine,
            shared_db,
            table_name,
            dataset_id: dataset_id.to_string(),
            epoch_id: epoch_id.to_string(),
            row_count,
        }
    }

    /// Creates a fixture with multiple tables for the same dataset.
    pub async fn with_multiple_epochs(dataset_id: &str, epoch_count: usize, rows_per_epoch: usize) -> Self {
        // Create a shared database
        let shared_db = create_shared_database();
        
        // Create storage engine (writer) using the shared database
        let storage_engine = create_test_engine_with_shared_db(shared_db.clone());
        
        // Create query engine (reader) using the same shared database
        let query_engine = create_test_query_engine(shared_db.clone(), 10);

        let schema = create_test_schema();
        let mut last_table_name = String::new();
        let mut last_epoch_id = String::new();

        for i in 1..=epoch_count {
            let epoch_id = format!("epoch_{:03}", i);
            let stream = create_test_stream(vec![create_test_batch(
                schema.clone(),
                (i * rows_per_epoch) as i64,
                rows_per_epoch,
            )]);
            let epoch = create_test_epoch(dataset_id, &epoch_id);
            last_table_name = epoch.table_name.clone();
            last_epoch_id = epoch_id.clone();

            storage_engine
                .create_epoch_table(dataset_id.to_string(), epoch, stream)
                .await
                .expect("Failed to create test table");
        }

        Self {
            storage_engine,
            query_engine,
            shared_db,
            table_name: last_table_name,
            dataset_id: dataset_id.to_string(),
            epoch_id: last_epoch_id,
            row_count: rows_per_epoch,
        }
    }
}

// ============================================================================
// Assertion Helpers - Reduce duplication in test assertions
// ============================================================================

/// Asserts that the result is a TableNotFound error.
pub fn assert_table_not_found<T: std::fmt::Debug>(result: Result<T, QueryError>) {
    assert!(result.is_err(), "Expected error but got success");
    let error = result.unwrap_err();
    assert!(
        matches!(error, QueryError::TableNotFound(_)),
        "Expected TableNotFound error, got: {:?}",
        error
    );
}

/// Asserts that a schema matches the expected test schema (id, name, value).
pub fn assert_test_schema(schema: &Schema) {
    assert_eq!(schema.fields().len(), 3, "Schema should have 3 fields");

    let id_field = schema.field(0);
    assert_eq!(id_field.name(), "id");
    assert_eq!(id_field.data_type(), &DataType::Int64);
    assert!(!id_field.is_nullable());

    let name_field = schema.field(1);
    assert_eq!(name_field.name(), "name");
    assert!(
        matches!(name_field.data_type(), DataType::Utf8 | DataType::LargeUtf8),
        "Expected Utf8 type for name field"
    );

    let value_field = schema.field(2);
    assert_eq!(value_field.name(), "value");
    assert_eq!(value_field.data_type(), &DataType::Int64);
}

/// Asserts that all table names in the list have the expected dataset prefix.
pub fn assert_tables_have_prefix(tables: &[String], dataset_id: &str) {
    let expected_prefix = format!("{}_", dataset_id);
    for table in tables {
        assert!(
            table.starts_with(&expected_prefix),
            "Table '{}' should start with prefix '{}'",
            table,
            expected_prefix
        );
    }
}

/// Asserts that metadata has valid basic fields.
pub fn assert_valid_metadata(
    metadata: &worker_storage::engine::storage_engine::TableMetadata,
    expected_table_name: &str,
    expected_rows: u64,
) {
    assert_eq!(metadata.table_name, expected_table_name);
    assert_eq!(metadata.total_rows, expected_rows, "Row count should match");
    assert!(metadata.total_bytes > 0, "Total bytes should be positive");
    assert!(metadata.create_at > 0, "Create timestamp should be set");
    assert_eq!(metadata.schema.fields().len(), 3, "Schema should have 3 fields");
}

/// Asserts that two schemas are equivalent.
pub fn assert_schemas_equal(schema1: &Schema, schema2: &Schema) {
    assert_eq!(
        schema1.fields().len(),
        schema2.fields().len(),
        "Field count should match"
    );
    for (i, field) in schema1.fields().iter().enumerate() {
        let other_field = schema2.field(i);
        assert_eq!(field.name(), other_field.name(), "Field names should match at index {}", i);
        assert_eq!(
            field.data_type(),
            other_field.data_type(),
            "Field types should match at index {}",
            i
        );
    }
}

/// Multi-dataset fixture for testing dataset isolation.
pub struct MultiDatasetFixture {
    pub storage_engine: MemoryDuckDBEngine,
    pub query_engine: DuckDBQueryEngine,
    pub shared_db: Arc<SharedDatabase>,
    pub datasets: Vec<(String, usize)>, // (dataset_id, epoch_count)
}

impl MultiDatasetFixture {
    /// Creates a fixture with multiple datasets, each with specified number of epochs.
    pub async fn new(dataset_configs: Vec<(&str, usize)>, rows_per_epoch: usize) -> Self {
        // Create a shared database
        let shared_db = create_shared_database();
        
        // Create storage engine (writer) using the shared database
        let storage_engine = create_test_engine_with_shared_db(shared_db.clone());
        
        // Create query engine (reader) using the same shared database
        let query_engine = create_test_query_engine(shared_db.clone(), 10);

        let schema = create_test_schema();
        let mut datasets = Vec::new();

        for (dataset_id, epoch_count) in &dataset_configs {
            for i in 1..=*epoch_count {
                let stream = create_test_stream(vec![create_test_batch(
                    schema.clone(),
                    (i * rows_per_epoch) as i64,
                    rows_per_epoch,
                )]);
                let epoch = create_test_epoch(dataset_id, &format!("epoch_{:03}", i));
                storage_engine
                    .create_epoch_table(dataset_id.to_string(), epoch, stream)
                    .await
                    .expect("Failed to create test table");
            }
            datasets.push((dataset_id.to_string(), *epoch_count));
        }

        Self {
            storage_engine,
            query_engine,
            shared_db,
            datasets,
        }
    }
}
