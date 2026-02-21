//! Flight window function tests — analytic SQL patterns through Arrow Flight.
//!
//! Exercises `ROW_NUMBER`, `RANK`, `SUM OVER`, `AVG OVER`, `LAG`, and
//! `NTILE` via the full `get_flight_info → do_get` round-trip.
//!
//! The SQL transformer is transparent to window functions: it rewrites the
//! logical `orders` table to a macro call and passes the rest through untouched.
//! Results are compared against the reference DuckDB engine on the same SQL.

mod common;

use common::{
    assertions::{
        assert_flight_matches_reference, assert_flight_matches_reference_ordered,
        assert_flight_row_count,
    },
    data::DataSeeder,
    harness::FlightTestHarness,
    init_tracing,
    runner::{execute_reference, execute_via_flight},
};

const TENANT: &str = "test-tenant";

async fn seed(h: &FlightTestHarness) {
    let seeder = DataSeeder::new(h.shared_db.clone(), h.sql_transformer.clone());
    seeder.seed_orders_single_epoch().await;
}

// ---------------------------------------------------------------------------
// ROW_NUMBER — global ranking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_row_number_global() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, amount, \
                      ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY rn";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("ROW_NUMBER global");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// ROW_NUMBER — partitioned by customer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_row_number_partitioned_by_customer() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, customer_id, amount, \
                      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY customer_id, rn";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("ROW_NUMBER partitioned");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // 5 customers × 4 orders each = 20 rows.
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// RANK — ties get same rank
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rank_by_amount() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, amount, \
                      RANK() OVER (ORDER BY amount DESC) AS rnk \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY rnk, order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("RANK by amount");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Running sum — SUM OVER PARTITION BY customer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_running_sum_per_customer() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, customer_id, amount, \
                      SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date \
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY customer_id, order_date";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("running sum");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// AVG OVER — rolling average
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_avg_over_all_orders() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    // Constant AVG across the entire partition (no ORDER BY in window = whole frame).
    let sql = "SELECT order_id, amount, \
                      AVG(amount) OVER () AS global_avg \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("AVG OVER all");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// LAG — access previous row's amount
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lag_previous_amount() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, amount, \
                      LAG(amount, 1) OVER (ORDER BY order_id) AS prev_amount \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("LAG previous amount");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// NTILE — divide into quartiles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ntile_quartiles() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "SELECT order_id, amount, \
                      NTILE(4) OVER (ORDER BY amount) AS quartile \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY quartile, order_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("NTILE quartiles");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // 20 rows across 4 quartiles.
    assert_flight_row_count(&flight, 20);

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Window function inside a CTE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_window_inside_cte() {
    init_tracing();
    let h = FlightTestHarness::start(TENANT).await;
    seed(&h).await;

    let sql = "\
        WITH ranked AS ( \
            SELECT order_id, customer_id, amount, \
                   RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk \
            FROM orders \
            WHERE order_date >= '2025-01-01' \
        ) \
        SELECT order_id, customer_id, amount \
        FROM ranked \
        WHERE rnk = 1 \
        ORDER BY customer_id";

    let mut client = h.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("window inside CTE");
    let reference = execute_reference(&h.query_engine, &h.sql_transformer, sql).await;

    assert_flight_matches_reference(&flight, &reference);
    // Top-ranked order per customer → at most 5 rows (one per customer).
    // All amounts are distinct per customer so exactly 5.
    assert_flight_row_count(&flight, 5);

    h.shutdown().await;
}
