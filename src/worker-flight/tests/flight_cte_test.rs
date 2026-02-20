//! Flight CTE & subquery tests — advanced SQL patterns through the full Flight pipeline.
//!
//! Validates that Common Table Expressions (CTEs), scalar subqueries, and
//! derived tables produce correct results when executed as Arrow Flight RPCs.
//!
//! The SQL transformer descends into CTE bodies and subqueries, rewriting
//! any registered dataset references to their epoch-macro form.

mod common;

use common::{
    assertions::{
        assert_flight_error, assert_flight_matches_reference_ordered, assert_flight_row_count,
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

async fn seed_standard(harness: &FlightTestHarness) {
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;
}

// ---------------------------------------------------------------------------
// Simple CTE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cte_simple_filter() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        WITH big_orders AS ( \
            SELECT order_id, customer_id, amount \
            FROM orders \
            WHERE order_date >= '2025-01-01' AND amount > 300.0 \
        ) \
        SELECT order_id, amount FROM big_orders ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight CTE simple");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// CTE joined with a dimension table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cte_joined_with_customers() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        WITH recent_orders AS ( \
            SELECT order_id, customer_id, amount \
            FROM orders \
            WHERE order_date >= '2025-01-01' \
        ) \
        SELECT r.order_id, c.customer_name, r.amount \
        FROM recent_orders r \
        JOIN customers c ON r.customer_id = c.customer_id \
        ORDER BY r.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight CTE + JOIN");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Multiple CTEs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_ctes() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        WITH order_stats AS ( \
            SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total \
            FROM orders \
            WHERE order_date >= '2025-01-01' \
            GROUP BY customer_id \
        ), \
        enriched AS ( \
            SELECT s.customer_id, c.customer_name, s.order_count, s.total \
            FROM order_stats s \
            JOIN customers c ON s.customer_id = c.customer_id \
        ) \
        SELECT customer_name, order_count, total \
        FROM enriched \
        ORDER BY customer_name";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight multiple CTEs");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 5); // 5 customers

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Subquery in FROM (derived table)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_subquery_in_from() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        SELECT sub.order_id, sub.amount \
        FROM ( \
            SELECT order_id, amount \
            FROM orders \
            WHERE order_date >= '2025-01-01' AND amount > 400.0 \
        ) sub \
        ORDER BY sub.order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight subquery in FROM");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scalar subquery in SELECT list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scalar_subquery_in_select() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    let sql = "\
        SELECT order_id, amount, \
               (SELECT AVG(amount) FROM orders WHERE order_date >= '2025-01-01') AS avg_amount \
        FROM orders \
        WHERE order_date >= '2025-01-01' \
        ORDER BY order_id \
        LIMIT 5";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight scalar subquery");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 5);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Subquery in WHERE (correlated-style via IN)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_subquery_in_where() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed_standard(&h).await;

    // Find orders from customers in region 'EU' (Bob=102, Eve=105).
    let sql = "\
        SELECT order_id, customer_id, amount \
        FROM orders \
        WHERE order_date >= '2025-01-01' \
          AND customer_id IN (SELECT customer_id FROM customers WHERE region = 'EU') \
        ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("flight subquery in WHERE");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // Customer 102 orders: {2,7,12,17}, customer 105 orders: {5,10,15,20} → 8 rows.
    assert_flight_row_count(&flight, 8);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE — should be rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recursive_cte_is_rejected() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;

    // WITH RECURSIVE is not supported by the transformer.
    let sql = "\
        WITH RECURSIVE nums(n) AS ( \
            SELECT 1 \
            UNION ALL \
            SELECT n + 1 FROM nums WHERE n < 10 \
        ) \
        SELECT * FROM nums";

    let mut client = h.client.clone();
    let result = execute_via_flight(&mut client, sql, TENANT).await;

    // The transformer / admission control rejects recursive CTEs.
    // Mapped to InvalidArgument via map_transform_error_to_status.
    assert_flight_error(result, tonic::Code::InvalidArgument);

    h.shutdown().await;
}
