mod common;

use common::*;
use worker_storage::engine::query_engine::QueryEngine;

// ============================================================================
// get_table_schema Tests
// ============================================================================

#[tokio::test]
async fn test_get_table_schema_valid_table() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("schema_dataset", "epoch_001", 1000).await;

    // Act
    let schema = fixture
        .query_engine
        .get_table_schema(&fixture.table_name)
        .await
        .expect("get_table_schema should succeed for valid table");

    // Assert
    assert_test_schema(&schema);
}

#[tokio::test]
async fn test_get_table_schema_nonexistent_table() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("schema_dataset", "epoch_001", 100).await;

    // Act & Assert
    let result = fixture.query_engine.get_table_schema("nonexistent_table").await;
    assert_table_not_found(result);
}

#[tokio::test]
async fn test_get_table_schema_special_characters() {
    // Arrange: Dataset and epoch IDs that result in a table name with underscores
    let fixture = QueryEngineTestFixture::new("dataset_with_underscores", "epoch_v1_0", 100).await;

    // Act
    let result = fixture.query_engine.get_table_schema(&fixture.table_name).await;

    // Assert
    assert!(
        result.is_ok(),
        "get_table_schema should handle table names with special characters"
    );
}

// ============================================================================
// get_table_metadata Tests
// ============================================================================

#[tokio::test]
async fn test_get_table_metadata_valid_table() {
    // Arrange
    let row_count = 1000;
    let fixture = QueryEngineTestFixture::new("metadata_dataset", "epoch_001", row_count).await;

    // Act
    let metadata = fixture
        .query_engine
        .get_table_metadata(&fixture.table_name)
        .await
        .expect("get_table_metadata should succeed for valid table");

    // Assert
    assert_valid_metadata(&metadata, &fixture.table_name, row_count as u64);
}

#[tokio::test]
async fn test_get_table_metadata_nonexistent_table() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("metadata_dataset", "epoch_001", 100).await;

    // Act & Assert
    let result = fixture.query_engine.get_table_metadata("nonexistent_table").await;
    assert_table_not_found(result);
}

#[tokio::test]
async fn test_get_table_metadata_row_count_accuracy() {
    // Arrange: Create table with specific row counts and verify accuracy
    let test_cases = vec![100, 500, 1000, 1500];

    for expected_rows in test_cases {
        let fixture = QueryEngineTestFixture::new(
            &format!("accuracy_dataset_{}", expected_rows),
            "epoch_001",
            expected_rows,
        )
        .await;

        // Act
        let metadata = fixture
            .query_engine
            .get_table_metadata(&fixture.table_name)
            .await
            .expect("get_table_metadata should succeed");

        // Assert
        assert_eq!(
            metadata.total_rows, expected_rows as u64,
            "Row count should be exactly {} for dataset with {} rows",
            expected_rows, expected_rows
        );
    }
}

#[tokio::test]
async fn test_get_table_metadata_bytes_estimation() {
    // Arrange: Create tables with different sizes
    let small_fixture = QueryEngineTestFixture::new("bytes_dataset_small", "epoch_001", 100).await;
    let large_fixture = QueryEngineTestFixture::new("bytes_dataset_large", "epoch_001", 1000).await;

    // Act
    let small_metadata = small_fixture
        .query_engine
        .get_table_metadata(&small_fixture.table_name)
        .await
        .expect("get_table_metadata should succeed");

    let large_metadata = large_fixture
        .query_engine
        .get_table_metadata(&large_fixture.table_name)
        .await
        .expect("get_table_metadata should succeed");

    // Assert: Larger table should have more bytes
    assert!(
        large_metadata.total_bytes > small_metadata.total_bytes,
        "Larger table should have more bytes: {} vs {}",
        large_metadata.total_bytes,
        small_metadata.total_bytes
    );
}

// ============================================================================
// list_tables Tests
// ============================================================================

#[tokio::test]
async fn test_list_tables_single_epoch() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("list_dataset", "epoch_001", 100).await;

    // Act
    let result = fixture.query_engine.list_tables(&fixture.dataset_id).await;

    // Assert
    assert!(result.is_ok(), "list_tables should succeed");
    let tables = result.unwrap();
    assert_eq!(tables.len(), 1, "Should have exactly 1 table");
    assert_eq!(tables[0], fixture.table_name);
}

#[tokio::test]
async fn test_list_tables_multiple_epochs() {
    // Arrange
    let epoch_count = 5;
    let fixture =
        QueryEngineTestFixture::with_multiple_epochs("multi_epoch_dataset", epoch_count, 100).await;

    // Act
    let tables = fixture
        .query_engine
        .list_tables(&fixture.dataset_id)
        .await
        .expect("list_tables should succeed");

    // Assert
    assert_eq!(tables.len(), epoch_count, "Should have exactly {} tables", epoch_count);
    assert_tables_have_prefix(&tables, &fixture.dataset_id);
}

#[tokio::test]
async fn test_list_tables_empty_dataset() {
    // Arrange: Create a fixture with data, but query a different dataset
    let fixture = QueryEngineTestFixture::new("existing_dataset", "epoch_001", 100).await;

    // Act: Query for a dataset that doesn't exist
    let result = fixture.query_engine.list_tables("nonexistent_dataset").await;

    // Assert
    assert!(result.is_ok(), "list_tables should succeed even for empty dataset");
    let tables = result.unwrap();
    assert_eq!(tables.len(), 0, "Should return empty list for non-existent dataset");
}

#[tokio::test]
async fn test_list_tables_isolation_between_datasets() {
    // Arrange: Create tables for two different datasets using MultiDatasetFixture
    let fixture = MultiDatasetFixture::new(
        vec![("dataset_a", 3), ("dataset_b", 2)],
        100,
    )
    .await;

    // Act
    let tables_a = fixture.query_engine.list_tables("dataset_a").await.unwrap();
    let tables_b = fixture.query_engine.list_tables("dataset_b").await.unwrap();

    // Assert
    assert_eq!(tables_a.len(), 3, "dataset_a should have 3 tables");
    assert_eq!(tables_b.len(), 2, "dataset_b should have 2 tables");
    assert_tables_have_prefix(&tables_a, "dataset_a");
    assert_tables_have_prefix(&tables_b, "dataset_b");
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_metadata_consistency_across_calls() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("consistency_dataset", "epoch_001", 1000).await;

    // Act: Call metadata methods multiple times
    let schema1 = fixture
        .query_engine
        .get_table_schema(&fixture.table_name)
        .await
        .unwrap();
    let schema2 = fixture
        .query_engine
        .get_table_schema(&fixture.table_name)
        .await
        .unwrap();

    let metadata1 = fixture
        .query_engine
        .get_table_metadata(&fixture.table_name)
        .await
        .unwrap();
    let metadata2 = fixture
        .query_engine
        .get_table_metadata(&fixture.table_name)
        .await
        .unwrap();

    // Assert: Results should be consistent
    assert_eq!(schema1, schema2, "Schema should be consistent across calls");
    assert_eq!(
        metadata1.total_rows, metadata2.total_rows,
        "Row count should be consistent"
    );
    assert_eq!(
        metadata1.table_name, metadata2.table_name,
        "Table name should be consistent"
    );
}

#[tokio::test]
async fn test_schema_matches_metadata_schema() {
    // Arrange
    let fixture = QueryEngineTestFixture::new("schema_match_dataset", "epoch_001", 100).await;

    // Act
    let schema = fixture
        .query_engine
        .get_table_schema(&fixture.table_name)
        .await
        .unwrap();
    let metadata = fixture
        .query_engine
        .get_table_metadata(&fixture.table_name)
        .await
        .unwrap();

    // Assert
    assert_schemas_equal(&schema, &metadata.schema);
}
