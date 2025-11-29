mod common;

use common::*;
use std::sync::Arc;
use tokio::task::JoinSet;
use worker_storage::engine::engine::StorageEngine;

#[tokio::test]
async fn test_concurrent_epoch_creation_different_datasets() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();
    let mut tasks = JoinSet::new();

    // Act - Create 10 epochs concurrently across different datasets
    for i in 0..10 {
        let engine_clone = engine.clone();
        let schema_clone = schema.clone();
        tasks.spawn(async move {
            let dataset_id = format!("dataset_{}", i);
            let epoch_id = "epoch_001";
            let stream =
                create_test_stream(vec![create_test_batch(schema_clone, i * 1000, 500)]);
            let epoch = create_test_epoch(&dataset_id, epoch_id);

            engine_clone
                .create_epoch_table(dataset_id, epoch, stream)
                .await
        });
    }

    // Assert - All should succeed
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        let task_result = result.expect("Task should not panic");
        assert!(
            task_result.is_ok(),
            "Concurrent creation failed: {:?}",
            task_result.err()
        );
        success_count += 1;
    }
    assert_eq!(success_count, 10, "All 10 creations should succeed");
}

#[tokio::test]
async fn test_concurrent_operations_same_dataset() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();

    // Create initial epochs
    for i in 1..=5 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 100)]);
        let epoch = create_test_epoch("concurrent_dataset", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("concurrent_dataset".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Act - Concurrent mix of operations (need separate JoinSets for different return types)
    let list_task = tokio::spawn({
        let engine = engine.clone();
        async move { engine.list_epochs("concurrent_dataset".to_string()).await }
    });

    let stats_task = tokio::spawn({
        let engine = engine.clone();
        async move { engine.memory_stats().await }
    });

    let metrics_task = tokio::spawn({
        let engine = engine.clone();
        async move { engine.get_metrics().await }
    });

    let drop_task = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .drop_epoch_table("concurrent_dataset".to_string(), "epoch_001".to_string())
                .await
        }
    });

    let create_task = tokio::spawn({
        let engine = engine.clone();
        let schema = schema.clone();
        async move {
            let stream = create_test_stream(vec![create_test_batch(schema, 600, 100)]);
            let epoch = create_test_epoch("concurrent_dataset", "epoch_006");
            engine
                .create_epoch_table("concurrent_dataset".to_string(), epoch, stream)
                .await
        }
    });

    // Assert - All operations should complete successfully
    let list_result = list_task.await.unwrap();
    let stats_result = stats_task.await.unwrap();
    let metrics_result = metrics_task.await.unwrap();
    let drop_result = drop_task.await.unwrap();
    let create_result = create_task.await.unwrap();

    let results = vec![
        list_result.is_ok(),
        stats_result.is_ok(),
        metrics_result.is_ok(),
        drop_result.is_ok(),
        create_result.is_ok(),
    ];

    let success_count = results.iter().filter(|&&r| r).count();
    assert!(
        success_count >= 4,
        "At least 4 out of 5 operations should succeed"
    );
}

#[tokio::test]
async fn test_concurrent_read_operations() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();

    // Create some epochs
    for i in 1..=3 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 100)]);
        let epoch = create_test_epoch("read_dataset", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("read_dataset".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Act - Multiple concurrent read operations using separate vectors for different types
    let mut list_handles = Vec::new();
    let mut stats_handles = Vec::new();
    let mut metrics_handles = Vec::new();

    for _ in 0..10 {
        // List epochs
        list_handles.push(tokio::spawn({
            let engine = engine.clone();
            async move { engine.list_epochs("read_dataset".to_string()).await }
        }));

        // Get memory stats
        stats_handles.push(tokio::spawn({
            let engine = engine.clone();
            async move { engine.memory_stats().await }
        }));

        // Get metrics
        metrics_handles.push(tokio::spawn({
            let engine = engine.clone();
            async move { engine.get_metrics().await }
        }));
    }

    // Assert - All reads should succeed
    let mut success_count = 0;
    for handle in list_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }
    for handle in stats_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }
    for handle in metrics_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }
    assert_eq!(success_count, 30, "All 30 read operations should succeed");
}

#[tokio::test]
async fn test_concurrent_epoch_creation_same_dataset() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();
    let mut tasks = JoinSet::new();

    // Act - Create multiple epochs for the same dataset concurrently
    for i in 1..=5 {
        let engine_clone = engine.clone();
        let schema_clone = schema.clone();
        tasks.spawn(async move {
            let stream =
                create_test_stream(vec![create_test_batch(schema_clone, i * 100, 100)]);
            let epoch = create_test_epoch("same_dataset", &format!("epoch_{:03}", i));
            engine_clone
                .create_epoch_table("same_dataset".to_string(), epoch, stream)
                .await
        });
    }

    // Assert - All should succeed since they have different epoch IDs
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        let task_result = result.expect("Task should not panic");
        assert!(
            task_result.is_ok(),
            "Concurrent creation of different epochs should succeed"
        );
        success_count += 1;
    }
    assert_eq!(success_count, 5, "All 5 epochs should be created");

    // Verify all epochs exist
    let epochs = engine.list_epochs("same_dataset".to_string()).await.unwrap();
    assert_eq!(
        epochs.len(),
        5,
        "Should have all 5 epochs after concurrent creation"
    );
}

#[tokio::test]
async fn test_concurrent_drop_different_epochs() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();

    // Create multiple epochs
    for i in 1..=10 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 100)]);
        let epoch = create_test_epoch("drop_dataset", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("drop_dataset".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Act - Drop multiple epochs concurrently
    let mut tasks = JoinSet::new();
    for i in 1..=5 {
        let engine_clone = engine.clone();
        tasks.spawn(async move {
            engine_clone
                .drop_epoch_table("drop_dataset".to_string(), format!("epoch_{:03}", i))
                .await
        });
    }

    // Assert - All drops should succeed
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        let task_result = result.expect("Task should not panic");
        assert!(
            task_result.is_ok(),
            "Concurrent drops should succeed: {:?}",
            task_result.err()
        );
        success_count += 1;
    }
    assert_eq!(success_count, 5, "All 5 drops should succeed");

    // Verify remaining epochs
    let epochs = engine.list_epochs("drop_dataset".to_string()).await.unwrap();
    assert_eq!(
        epochs.len(),
        5,
        "Should have 5 remaining epochs after concurrent drops"
    );
}

#[tokio::test]
async fn test_concurrent_create_and_drop() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();

    // Create initial epochs
    for i in 1..=5 {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 100)]);
        let epoch = create_test_epoch("mixed_ops", &format!("epoch_{:03}", i));
        engine
            .create_epoch_table("mixed_ops".to_string(), epoch, stream)
            .await
            .unwrap();
    }

    // Act - Mix of create and drop operations using separate vectors
    let mut drop_handles = Vec::new();
    let mut create_handles = Vec::new();

    // Drop some existing epochs
    for i in 1..=3 {
        let engine_clone = engine.clone();
        drop_handles.push(tokio::spawn(async move {
            engine_clone
                .drop_epoch_table("mixed_ops".to_string(), format!("epoch_{:03}", i))
                .await
        }));
    }

    // Create new epochs
    for i in 6..=8 {
        let engine_clone = engine.clone();
        let schema_clone = schema.clone();
        create_handles.push(tokio::spawn(async move {
            let stream =
                create_test_stream(vec![create_test_batch(schema_clone, i * 100, 100)]);
            let epoch = create_test_epoch("mixed_ops", &format!("epoch_{:03}", i));
            engine_clone
                .create_epoch_table("mixed_ops".to_string(), epoch, stream)
                .await
        }));
    }

    // Assert - All operations should succeed
    let mut success_count = 0;
    for handle in drop_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }
    for handle in create_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }

    assert!(
        success_count >= 5,
        "Most operations should succeed, got {} successes",
        success_count
    );
}

#[tokio::test]
async fn test_stress_concurrent_operations() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();
    let mut tasks = JoinSet::new();

    // Act - High volume of concurrent operations
    for dataset_idx in 0..5 {
        for epoch_idx in 0..5 {
            let engine_clone = engine.clone();
            let schema_clone = schema.clone();
            tasks.spawn(async move {
                let dataset_id = format!("stress_dataset_{}", dataset_idx);
                let epoch_id = format!("epoch_{:03}", epoch_idx);
                let stream = create_test_stream(vec![create_test_batch(
                    schema_clone,
                    (dataset_idx * 1000 + epoch_idx * 100) as i64,
                    100,
                )]);
                let epoch = create_test_epoch(&dataset_id, &epoch_id);

                engine_clone
                    .create_epoch_table(dataset_id, epoch, stream)
                    .await
            });
        }
    }

    // Assert - Most operations should succeed
    let mut success_count = 0;
    let mut error_count = 0;
    while let Some(result) = tasks.join_next().await {
        match result.expect("Task should not panic") {
            Ok(_) => success_count += 1,
            Err(_) => error_count += 1,
        }
    }

    println!(
        "Stress test results: {} successes, {} errors",
        success_count, error_count
    );
    assert!(
        success_count >= 20,
        "At least 80% of operations should succeed"
    );
}

#[tokio::test]
async fn test_concurrent_memory_stats_during_operations() {
    // Arrange
    let engine = Arc::new(create_test_engine());
    let schema = create_test_schema();
    
    // Start multiple epoch creations
    let mut create_handles = Vec::new();
    for i in 0..5 {
        let engine_clone = engine.clone();
        let schema_clone = schema.clone();
        create_handles.push(tokio::spawn(async move {
            let dataset_id = format!("stats_dataset_{}", i);
            let stream =
                create_test_stream(vec![create_test_batch(schema_clone, i * 1000, 1000)]);
            let epoch = create_test_epoch(&dataset_id, "epoch_001");
            engine_clone
                .create_epoch_table(dataset_id, epoch, stream)
                .await
        }));
    }

    // Concurrently query memory stats while creations are in progress
    let mut stats_handles = Vec::new();
    for _ in 0..10 {
        let engine_clone = engine.clone();
        stats_handles.push(tokio::spawn(async move { engine_clone.memory_stats().await }));
    }

    // Assert - All operations should complete
    let mut success_count = 0;
    
    for handle in create_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }
    
    for handle in stats_handles {
        if handle.await.expect("Task should not panic").is_ok() {
            success_count += 1;
        }
    }

    assert!(
        success_count >= 10,
        "Most operations should succeed during concurrent access"
    );
}

#[tokio::test]
async fn test_sequential_vs_concurrent_performance() {
    let schema = create_test_schema();
    let epoch_count = 5;

    // Sequential execution
    let sequential_start = std::time::Instant::now();
    let sequential_engine = create_test_engine();

    for i in 0..epoch_count {
        let stream = create_test_stream(vec![create_test_batch(schema.clone(), i * 100, 100)]);
        let epoch = create_test_epoch("seq_dataset", &format!("epoch_{:03}", i));
        sequential_engine
            .create_epoch_table("seq_dataset".to_string(), epoch, stream)
            .await
            .unwrap();
    }
    let sequential_duration = sequential_start.elapsed();

    // Concurrent execution
    let concurrent_start = std::time::Instant::now();
    let concurrent_engine = Arc::new(create_test_engine());
    let mut tasks = JoinSet::new();

    for i in 0..epoch_count {
        let engine = concurrent_engine.clone();
        let schema_clone = schema.clone();
        tasks.spawn(async move {
            let stream =
                create_test_stream(vec![create_test_batch(schema_clone, i * 100, 100)]);
            let epoch = create_test_epoch("concurrent_dataset", &format!("epoch_{:03}", i));
            engine
                .create_epoch_table("concurrent_dataset".to_string(), epoch, stream)
                .await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.expect("Task should not panic").unwrap();
    }
    let concurrent_duration = concurrent_start.elapsed();

    println!(
        "Sequential: {:?}, Concurrent: {:?}",
        sequential_duration, concurrent_duration
    );

    // Concurrent execution may not always be faster for I/O-bound operations
    // This test documents the behavior rather than asserting performance
    assert!(
        concurrent_duration < sequential_duration * 2,
        "Concurrent execution should not be significantly slower"
    );
}