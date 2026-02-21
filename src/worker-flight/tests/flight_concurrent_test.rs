//! Flight concurrency tests — multiple simultaneous clients against one server.
//!
//! Fires several `execute_via_flight` calls in parallel on independent tokio
//! tasks and asserts that:
//! 1. All queries complete without error.
//! 2. Every task returns the same result as the reference engine.
//! 3. The server handles concurrent requests without data corruption.
//!
//! `FlightServiceClient<Channel>` is `Clone`; each clone shares the
//! connection pool in the underlying tonic `Channel`.

mod common;

use std::sync::Arc;

use futures_util::future::try_join_all;

use common::{
    assertions::{assert_flight_matches_reference, assert_flight_row_count},
    data::DataSeeder,
    harness::FlightTestHarness,
    init_tracing,
    runner::{execute_reference, execute_via_flight},
};
const TENANT: &str = "test-tenant";

// ---------------------------------------------------------------------------
// 8 concurrent queries — same SQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_identical_queries() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT order_id, amount \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY order_id";

    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    // Fire 8 concurrent Flight calls.
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let mut client = h.client.clone();
            let sql = sql.to_string();
            tokio::spawn(async move { execute_via_flight(&mut client, &sql, TENANT).await })
        })
        .collect();

    let results = try_join_all(tasks).await.expect("tasks did not panic");

    for (i, result) in results.into_iter().enumerate() {
        let batches = result.unwrap_or_else(|e| panic!("task {i} failed: {e}"));
        assert_flight_matches_reference(&batches, &reference);
    }

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5 concurrent queries with different filters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_different_queries() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_standard().await;

    let queries: &[(&str, usize)] = &[
        ("SELECT * FROM orders WHERE order_date >= '2025-01-01' ORDER BY order_id", 20),
        ("SELECT * FROM orders WHERE order_date >= '2025-01-01' AND amount > 200.0 ORDER BY order_id", 13),
        ("SELECT order_id FROM orders WHERE order_date >= '2025-01-01' ORDER BY order_id LIMIT 5", 5),
        ("SELECT customer_id, COUNT(*) AS n FROM orders WHERE order_date >= '2025-01-01' GROUP BY customer_id ORDER BY customer_id", 5),
        ("SELECT * FROM orders WHERE order_date >= '2025-01-01' AND order_id = 99999", 0),
    ];

    let engine = Arc::clone(&h.query_engine);
    let transformer = Arc::clone(&h.sql_transformer);

    let tasks: Vec<_> = queries
        .iter()
        .map(|(sql, _expected_rows)| {
            let mut client = h.client.clone();
            let sql = sql.to_string();
            tokio::spawn(async move { execute_via_flight(&mut client, &sql, TENANT).await })
        })
        .collect();

    let results = try_join_all(tasks).await.expect("tasks did not panic");

    for ((sql, expected_rows), result) in queries.iter().zip(results.into_iter()) {
        let batches = result.unwrap_or_else(|e| panic!("query '{sql}' failed: {e}"));
        assert_flight_row_count(&batches, *expected_rows);
        // Always compare against the reference path, including empty results.
        // The verifier now accepts empty-vs-empty as a valid (equal) outcome.
        let reference = execute_reference(&engine, &transformer, sql).await;
        assert_flight_matches_reference(&batches, &reference);
    }

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// 16 concurrent queries — stress test the DuckDB connection pool
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_stress_pool() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT SUM(amount) AS total FROM orders WHERE order_date >= '2025-01-01'";

    // Fire 16 concurrent queries — more than the default pool idle size.
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let mut client = h.client.clone();
            let sql = sql.to_string();
            tokio::spawn(async move { execute_via_flight(&mut client, &sql, TENANT).await })
        })
        .collect();

    let results = try_join_all(tasks).await.expect("tasks did not panic");

    // All 16 must succeed and return exactly 1 aggregate row.
    for (i, result) in results.into_iter().enumerate() {
        let batches = result.unwrap_or_else(|e| panic!("stress task {i} failed: {e}"));
        assert_flight_row_count(&batches, 1);
    }

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Concurrency while seeding: queries overlap with a background seeder
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_queries_and_schema_discovery_concurrent() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_standard().await;

    // Mix of get_flight_info (metadata) and do_get (data) concurrent calls.
    let sql_data = "SELECT order_id FROM orders WHERE order_date >= '2025-01-01' ORDER BY order_id";
    let sql_agg = "SELECT COUNT(*) AS n FROM orders WHERE order_date >= '2025-01-01'";

    let tasks: Vec<_> = (0..6)
        .map(|i| {
            let mut client = h.client.clone();
            let sql = if i % 2 == 0 { sql_data } else { sql_agg }.to_string();
            tokio::spawn(async move { execute_via_flight(&mut client, &sql, TENANT).await })
        })
        .collect();

    let results = try_join_all(tasks)
        .await
        .expect("mixed tasks did not panic");

    for (i, result) in results.into_iter().enumerate() {
        result.unwrap_or_else(|e| panic!("mixed task {i} failed: {e}"));
    }

    h.shutdown().await;
}
