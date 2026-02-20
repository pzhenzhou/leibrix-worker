//! Flight JOIN tests — multi-table join queries through the full Flight pipeline.
//!
//! Validates that `orders JOIN customers` (registered epoch dataset + plain table)
//! and `orders JOIN customers JOIN regions` (3-way) produce correct results
//! when executed through Arrow Flight vs. the reference `DuckDBQueryEngine`.
//!
//! The SQL transformer rewrites only the registered `orders` table to its macro
//! form while leaving `customers` and `regions` untouched.

mod common;

use common::{
    assertions::{
        assert_flight_approx, assert_flight_matches_reference,
        assert_flight_matches_reference_ordered, assert_flight_row_count,
    },
    data::DataSeeder,
    harness::FlightTestHarness,
    init_tracing,
    runner::{execute_reference, execute_via_flight},
};

const TENANT: &str = "test-tenant";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Seed the standard orders + customers fixture.
async fn seed_standard(harness: &FlightTestHarness) {
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;
}

/// Seed standard + regions for 3-way join tests.
async fn seed_with_regions(harness: &FlightTestHarness) {
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;
    seeder.seed_regions();
}

// ---------------------------------------------------------------------------
// Inner join: orders × customers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_inner_join_orders_customers() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        SELECT o.order_id, o.amount, c.customer_name, c.region \
        FROM orders o \
        JOIN customers c ON o.customer_id = c.customer_id \
        WHERE o.order_date >= '2025-01-01' \
        ORDER BY o.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight inner join");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // All 20 orders have matching customers (101-105), so 20 rows.
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Left join: some customers may have no orders matching filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_left_join_customers_orders() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    // LEFT JOIN from customers → orders so that customers without orders
    // still appear. Since all 5 customers have at least one order, expect 20 rows.
    let sql = "\
        SELECT c.customer_id, c.customer_name, o.order_id, o.amount \
        FROM customers c \
        LEFT JOIN orders o ON c.customer_id = o.customer_id \
          AND o.order_date >= '2025-01-01' \
        ORDER BY c.customer_id, o.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight left join");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Aggregate join: SUM per customer name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_join_aggregate_sum_per_customer() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        SELECT c.customer_name, SUM(o.amount) AS total \
        FROM orders o \
        JOIN customers c ON o.customer_id = c.customer_id \
        WHERE o.order_date >= '2025-01-01' \
        GROUP BY c.customer_name \
        ORDER BY c.customer_name";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight join aggregate");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_approx(&flight, &reference, 0.01);
    assert_flight_row_count(&flight, 5); // 5 distinct customers

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Join with additional filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_join_with_region_filter() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    // Only US customers: Alice (101), Charlie (103).
    let sql = "\
        SELECT o.order_id, c.customer_name \
        FROM orders o \
        JOIN customers c ON o.customer_id = c.customer_id \
        WHERE o.order_date >= '2025-01-01' AND c.region = 'US' \
        ORDER BY o.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight join with region filter");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // customer 101 has orders {1,6,11,16} and 103 has {3,8,13,18} → 8 rows.
    assert_flight_row_count(&flight, 8);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// 3-way join: orders × customers × regions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_three_way_join() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_with_regions(&h).await;

    let sql = "\
        SELECT o.order_id, c.customer_name, r.continent \
        FROM orders o \
        JOIN customers c ON o.customer_id = c.customer_id \
        JOIN regions r ON c.region = r.region_name \
        WHERE o.order_date >= '2025-01-01' \
        ORDER BY o.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight 3-way join");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // Every order maps to a customer which maps to a region → 20 rows.
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Join with DISTINCT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_join_distinct_regions() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_with_regions(&h).await;

    let sql = "\
        SELECT DISTINCT c.region, r.continent \
        FROM orders o \
        JOIN customers c ON o.customer_id = c.customer_id \
        JOIN regions r ON c.region = r.region_name \
        WHERE o.order_date >= '2025-01-01'";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight join distinct");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference(&flight, &reference);
    // Distinct (region, continent) for customers: US/Americas, EU/Europe, APAC/Asia-Pacific → 3
    assert_flight_row_count(&flight, 3);

    h.shutdown().await;
}
