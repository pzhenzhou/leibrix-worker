mod common;

use common::*;
use worker_storage::engine::engine::StorageEngine;

#[tokio::test]
async fn test_create_single_epoch_table() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();
    let batches = vec![
        create_test_batch(schema.clone(), 0, 100),
        create_test_batch(schema.clone(), 100, 100),
    ];
    let stream = create_test_stream(batches);
    let epoch = create_test_epoch("sales_data", "epoch_001");

    // Act
    let result = engine
        .create_epoch_table("sales_data".to_string(), epoch, stream)
        .await;

    // Assert
    assert!(
        result.is_ok(),
        "Failed to create epoch table: {:?}",
        result.err()
    );
    let metadata = result.unwrap();
    assert_eq!(metadata.total_rows, 200);
    assert!(metadata.total_bytes > 0);
    assert_eq!(metadata.table_name, "sales_data__epoch_001");
}

#[tokio::test]
async fn test_create_multiple_epochs_same_dataset() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Act & Assert - Create first epoch
    let stream1 = create_test_stream(vec![create_test_batch(schema.clone(), 0, 50)]);
    let epoch1 = create_test_epoch("dataset_a", "epoch_001");
    let result1 = engine
        .create_epoch_table("dataset_a".to_string(), epoch1, stream1)
        .await;
    assert!(result1.is_ok(), "Failed to create first epoch");

    // Act & Assert - Create second epoch
    let stream2 = create_test_stream(vec![create_test_batch(schema.clone(), 50, 75)]);
    let epoch2 = create_test_epoch("dataset_a", "epoch_002");
    let result2 = engine
        .create_epoch_table("dataset_a".to_string(), epoch2, stream2)
        .await;
    assert!(result2.is_ok(), "Failed to create second epoch");

    // Verify both epochs exist
    let epochs = engine.list_epochs("dataset_a".to_string()).await.unwrap();
    assert_eq!(epochs.len(), 2, "Should have exactly 2 epochs");
}

#[tokio::test]
async fn test_drop_epoch_table() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();
    let stream = create_test_stream(vec![create_test_batch(schema, 0, 100)]);
    let epoch = create_test_epoch("dataset_b", "epoch_001");

    engine
        .create_epoch_table("dataset_b".to_string(), epoch, stream)
        .await
        .unwrap();

    // Act
    let drop_result = engine
        .drop_epoch_table("dataset_b".to_string(), "epoch_001".to_string())
        .await;

    // Assert
    assert!(
        drop_result.is_ok(),
        "Failed to drop epoch: {:?}",
        drop_result.err()
    );

    // Verify epoch no longer exists
    let epochs = engine.list_epochs("dataset_b".to_string()).await.unwrap();
    assert_eq!(epochs.len(), 0, "No epochs should remain after drop");
}

#[tokio::test]
async fn test_list_epochs_multiple_datasets() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Create epochs for dataset_a
    for i in 1..=3 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 50)]);
        let epoch = create_test_epoch("dataset_a", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("dataset_a".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Create epochs for dataset_b
    for i in 1..=2 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 200, 50)]);
        let epoch = create_test_epoch("dataset_b", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("dataset_b".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Act
    let epochs_a = engine.list_epochs("dataset_a".to_string()).await.unwrap();
    let epochs_b = engine.list_epochs("dataset_b".to_string()).await.unwrap();

    // Assert
    assert_eq!(epochs_a.len(), 3, "dataset_a should have 3 epochs");
    assert_eq!(epochs_b.len(), 2, "dataset_b should have 2 epochs");
}

#[tokio::test]
async fn test_list_epochs_empty_dataset() {
    // Arrange
    let engine = create_test_engine();

    // Act
    let epochs = engine
        .list_epochs("nonexistent_dataset".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(epochs.len(), 0, "Should return empty list for non-existent dataset");
}

#[tokio::test]
async fn test_memory_stats_tracking() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Get initial stats
    let initial_stats = engine.memory_stats().await.unwrap();
    assert_eq!(initial_stats.epochs.len(), 0, "Should start with no epochs");
    assert_eq!(
        initial_stats.total_storage_bytes_estimate, 0,
        "Should start with 0 bytes"
    );

    // Act - Create two epochs
    let stream1 = create_test_stream(vec![create_test_batch(schema.clone(), 0, 1000)]);
    let epoch1 = create_test_epoch("dataset_c", "epoch_001");
    engine
        .create_epoch_table("dataset_c".to_string(), epoch1, stream1)
        .await
        .unwrap();

    let stream2 = create_test_stream(vec![create_test_batch(schema.clone(), 1000, 500)]);
    let epoch2 = create_test_epoch("dataset_c", "epoch_002");
    engine
        .create_epoch_table("dataset_c".to_string(), epoch2, stream2)
        .await
        .unwrap();

    // Assert
    let stats = engine.memory_stats().await.unwrap();
    assert_eq!(stats.epochs.len(), 2, "Should track 2 epochs");
    assert!(
        stats.total_storage_bytes_estimate > 0,
        "Should report non-zero bytes"
    );

    // Verify individual epoch stats
    let epoch_001_stats = stats
        .epochs
        .iter()
        .find(|e| e.epoch_id == "epoch_001")
        .expect("Should find epoch_001");
    assert_eq!(epoch_001_stats.rows_count, 1000);
    assert!(epoch_001_stats.approx_bytes > 0);

    let epoch_002_stats = stats
        .epochs
        .iter()
        .find(|e| e.epoch_id == "epoch_002")
        .expect("Should find epoch_002");
    assert_eq!(epoch_002_stats.rows_count, 500);
    assert!(epoch_002_stats.approx_bytes > 0);
}

#[tokio::test]
async fn test_memory_stats_after_drop() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Create two epochs
    let stream1 = create_test_stream(vec![create_test_batch(schema.clone(), 0, 1000)]);
    let epoch1 = create_test_epoch("dataset_d", "epoch_001");
    engine
        .create_epoch_table("dataset_d".to_string(), epoch1, stream1)
        .await
        .unwrap();

    let stream2 = create_test_stream(vec![create_test_batch(schema.clone(), 1000, 500)]);
    let epoch2 = create_test_epoch("dataset_d", "epoch_002");
    engine
        .create_epoch_table("dataset_d".to_string(), epoch2, stream2)
        .await
        .unwrap();

    let stats_before = engine.memory_stats().await.unwrap();
    let bytes_before = stats_before.total_storage_bytes_estimate;

    // Act - Drop one epoch
    engine
        .drop_epoch_table("dataset_d".to_string(), "epoch_001".to_string())
        .await
        .unwrap();

    // Assert
    let stats_after = engine.memory_stats().await.unwrap();
    assert_eq!(stats_after.epochs.len(), 1, "Should have 1 epoch remaining");
    assert!(
        stats_after.total_storage_bytes_estimate < bytes_before,
        "Total bytes should decrease after drop"
    );
}

#[tokio::test]
async fn test_engine_metrics() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Act - Create epoch with multiple batches
    let batches = vec![
        create_test_batch(schema.clone(), 0, 5000),
        create_test_batch(schema.clone(), 5000, 5000),
        create_test_batch(schema.clone(), 10000, 5000),
    ];
    let stream = create_test_stream(batches);
    let epoch = create_test_epoch("dataset_e", "epoch_001");

    engine
        .create_epoch_table("dataset_e".to_string(), epoch, stream)
        .await
        .unwrap();

    // Assert
    let metrics = engine.get_metrics().await.unwrap();
    assert!(
        metrics.total_rows_written >= 15000,
        "Should have written at least 15000 rows"
    );
    assert!(metrics.total_batches_written > 0, "Should have written batches");
    assert!(metrics.total_flushes > 0, "Should have performed flushes");
    assert_eq!(metrics.committed_epochs, 1, "Should have 1 committed epoch");
    assert_eq!(metrics.active_epochs, 0, "Should have no in-progress epochs");
}

#[tokio::test]
async fn test_large_batch_ingestion() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();

    // Create a large batch
    let large_batch = create_test_batch(schema.clone(), 0, 50000);
    let stream = create_test_stream(vec![large_batch]);
    let epoch = create_test_epoch("dataset_large", "epoch_001");

    // Act
    let result = engine
        .create_epoch_table("dataset_large".to_string(), epoch, stream)
        .await;

    // Assert
    assert!(result.is_ok(), "Should handle large batch successfully");
    let metadata = result.unwrap();
    assert_eq!(metadata.total_rows, 50000);
}

#[tokio::test]
async fn test_shutdown_gracefully() {
    // Arrange
    let engine = create_test_engine();
    let schema = create_test_schema();
    let stream = create_test_stream(vec![create_test_batch(schema, 0, 100)]);
    let epoch = create_test_epoch("dataset_f", "epoch_001");

    engine
        .create_epoch_table("dataset_f".to_string(), epoch, stream)
        .await
        .unwrap();

    // Act
    let shutdown_result = engine.shutdown().await;

    // Assert
    assert!(
        shutdown_result.is_ok(),
        "Shutdown failed: {:?}",
        shutdown_result.err()
    );
}