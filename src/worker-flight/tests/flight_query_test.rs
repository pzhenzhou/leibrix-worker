//! Flight query execution tests.
//!
//! Each test seeds data, runs the same SQL through two paths:
//! 1. **Flight path**: `get_flight_info` → `do_get` (full gRPC round-trip)
//! 2. **Reference path**: `SqlTransformer` → `DuckDBQueryEngine::execute_query`
//!
//! Comparing the two paths is the primary correctness assertion.

mod common;

use worker_flight::ticket::FlightTicket;

use common::{
    assertions::{
        assert_flight_empty_matches_reference, assert_flight_error,
        assert_flight_matches_reference, assert_flight_matches_reference_ordered,
        assert_flight_row_count,
    },
    data::DataSeeder,
    harness::FlightTestHarness,
    init_tracing,
    runner::{do_get_with_ticket, execute_reference, execute_via_flight},
};

const TENANT: &str = "test-tenant";

// ---------------------------------------------------------------------------
// Happy-path queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_all_matches_reference() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT order_id, customer_id, amount, order_date \
               FROM orders WHERE order_date >= '2025-01-01' ORDER BY order_id";

    let mut client = harness.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("execute_via_flight failed");
    let reference = execute_reference(&harness.query_engine, &harness.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 20);

    harness.shutdown().await;
}

#[tokio::test]
async fn test_select_with_amount_filter_matches_reference() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT order_id, amount \
               FROM orders \
               WHERE order_date >= '2025-01-01' AND amount > 200.0 \
               ORDER BY order_id";

    let mut client = harness.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("execute_via_flight failed");
    let reference = execute_reference(&harness.query_engine, &harness.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    // 20 rows, those with amount > 200: ids {2,4,6,8,9,10,12,14,15,16,18,19,20} = 13
    assert_flight_row_count(&flight, 13);

    harness.shutdown().await;
}

#[tokio::test]
async fn test_aggregate_sum_by_customer_matches_reference() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT customer_id, SUM(amount) AS total_amount \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               GROUP BY customer_id \
               ORDER BY customer_id";

    let mut client = harness.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("execute_via_flight failed");
    let reference = execute_reference(&harness.query_engine, &harness.sql_transformer, sql).await;

    assert_flight_matches_reference(&flight, &reference);
    assert_flight_row_count(&flight, 5); // 5 distinct customer_ids (101-105)

    harness.shutdown().await;
}

#[tokio::test]
async fn test_empty_result_set_has_zero_rows() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    // order_id = 99999 never exists — should return zero rows.
    let sql = "SELECT * FROM orders WHERE order_date >= '2025-01-01' AND order_id = 99999";

    let mut client = harness.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("execute_via_flight failed");
    let reference = execute_reference(&harness.query_engine, &harness.sql_transformer, sql).await;

    assert_flight_row_count(&flight, 0);
    // Compare against reference: verifies the empty result is consistent
    // (same zero-row outcome on both paths, not just a missing error).
    assert_flight_empty_matches_reference(&flight, &reference);

    harness.shutdown().await;
}

#[tokio::test]
async fn test_select_limit_returns_correct_count() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    let sql = "SELECT order_id, amount \
               FROM orders \
               WHERE order_date >= '2025-01-01' \
               ORDER BY order_id \
               LIMIT 5";

    let mut client = harness.client.clone();
    let flight = execute_via_flight(&mut client, sql, TENANT)
        .await
        .expect("execute_via_flight failed");
    let reference = execute_reference(&harness.query_engine, &harness.sql_transformer, sql).await;

    assert_flight_matches_reference_ordered(&flight, &reference);
    assert_flight_row_count(&flight, 5);

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Error-path tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_wrong_tenant_in_ticket_is_permission_denied() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_standard().await;

    // Craft a ticket with a tenant that does NOT match the server's.
    let ticket = FlightTicket::query(
        "SELECT * FROM orders WHERE order_date >= '2025-01-01'".into(),
        "wrong-tenant".into(),
    )
    .encode()
    .expect("encode FlightTicket");

    let mut client = harness.client.clone();
    let result = do_get_with_ticket(&mut client, ticket).await;

    assert_flight_error(result, tonic::Code::PermissionDenied);

    harness.shutdown().await;
}

#[tokio::test]
async fn test_invalid_sql_returns_invalid_argument() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;

    // Completely garbled SQL — the transformer's parser rejects it.
    let sql = "THIS IS NOT VALID SQL!!!";

    let mut client = harness.client.clone();
    let result = execute_via_flight(&mut client, sql, TENANT).await;

    assert_flight_error(result, tonic::Code::InvalidArgument);

    harness.shutdown().await;
}

#[tokio::test]
async fn test_unregistered_table_returns_invalid_argument() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;

    // `ghost_table` is not a registered dataset so `get_flight_info` rejects
    // the query with "query does not reference any registered logical tables".
    let sql = "SELECT * FROM ghost_table";

    let mut client = harness.client.clone();
    let result = execute_via_flight(&mut client, sql, TENANT).await;

    assert_flight_error(result, tonic::Code::InvalidArgument);

    harness.shutdown().await;
}
