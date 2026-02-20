//! Flight multi-epoch tests — cross-epoch date-range pruning through Arrow Flight.
//!
//! Uses the two-epoch fixture (`seed_orders_multi_epoch`):
//!
//! | Epoch      | order_ids | Dates                      |
//! |------------|-----------|----------------------------|
//! | 20250101   | 1–10      | 2025-01-05 .. 2025-01-30   |
//! | 20250201   | 11–20     | 2025-02-03 .. 2025-02-25   |
//!
//! The SQL transformer uses Boolean interval algebra to derive which epochs
//! need to be scanned; the macro union-filters the epoch tables at runtime.
//! These tests verify the correct rows are returned for each date range.

mod common;

use common::{
    assertions::{
        assert_column_int_range, assert_flight_matches_reference,
        assert_flight_matches_reference_ordered, assert_flight_row_count,
    },
    data::DataSeeder,
    harness::FlightTestHarness,
    init_tracing,
    runner::{execute_reference, execute_via_flight},
};

const TENANT: &str = "test-tenant";

async fn seed_multi(h: &FlightTestHarness) {
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_orders_multi_epoch().await;
}

// ---------------------------------------------------------------------------
// Both epochs scanned
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_both_epochs_full_scan() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    // Predicate spans both epochs → 20 rows.
    let sql = "SELECT order_id FROM orders \
               WHERE order_date >= '2025-01-01' ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("both-epoch full scan");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Only January epoch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_only_january_epoch() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    // Upper bound before February → only Jan epoch rows (10).
    let sql = "SELECT order_id, order_date FROM orders \
               WHERE order_date >= '2025-01-01' AND order_date < '2025-02-01' \
               ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("Jan-only epoch scan");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 10);
    // Golden content: all returned order_ids must belong to the Jan epoch (1..=10).
    assert_column_int_range(&flight, "order_id", 1, 10);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Only February epoch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_only_february_epoch() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    // Lower bound from February → only Feb epoch rows (10).
    let sql = "SELECT order_id, order_date FROM orders \
               WHERE order_date >= '2025-02-01' \
               ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("Feb-only epoch scan");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 10);
    // Golden content: all returned order_ids must belong to the Feb epoch (11..=20).
    assert_column_int_range(&flight, "order_id", 11, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Aggregate across both epochs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_aggregate_across_both_epochs() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    // SUM(amount) per customer across both epochs.
    let sql = "SELECT customer_id, SUM(amount) AS total \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               GROUP BY customer_id \
               ORDER BY customer_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("cross-epoch aggregate");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference(&flight, &reference);
    assert_flight_row_count(&flight, 5); // 5 distinct customers

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Filter that falls inside one epoch's rows but spans epoch boundary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_narrow_range_within_january() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    // Only Jan rows with date >= 2025-01-20 (orders 6,7,8,9,10 = 5 rows).
    let sql = "SELECT order_id FROM orders \
               WHERE order_date >= '2025-01-20' AND order_date < '2025-02-01' \
               ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("narrow Jan range");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 5);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Epoch boundary: count rows per month
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_row_count_per_month() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_multi(&h).await;

    let sql = "SELECT EXTRACT(MONTH FROM order_date) AS month, COUNT(*) AS cnt \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               GROUP BY month \
               ORDER BY month";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("row count per month");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference(&flight, &reference);
    assert_flight_row_count(&flight, 2); // Jan (month=1) and Feb (month=2)

    h.shutdown().await;
}
