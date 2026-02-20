//! Flight metadata RPC tests.
//!
//! Validates `list_actions`, `list_flights`, `get_schema`, and `get_flight_info`
//! against a real `WorkerFlightService` backed by an in-memory DuckDB.

mod common;

use arrow_flight::{Criteria, FlightDescriptor};
use arrow_ipc::reader::FileReader;
use futures_util::TryStreamExt;
use std::io::Cursor;
use worker_flight::ticket::FlightTicket;

use common::{data::DataSeeder, harness::FlightTestHarness, init_tracing};

const TENANT: &str = "test-tenant";
const ORDERS_SQL: &str = "SELECT * FROM orders WHERE order_date >= '2025-01-01'";

// ---------------------------------------------------------------------------
// list_actions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_actions_returns_expected_actions() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let mut client = harness.client.clone();

    let stream = client
        .list_actions(arrow_flight::Empty {})
        .await
        .expect("list_actions RPC failed")
        .into_inner();

    let actions: Vec<_> = stream
        .try_collect()
        .await
        .expect("collect action stream");

    let names: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
    assert!(
        names.contains(&"health_check"),
        "expected health_check in actions, got {names:?}"
    );
    assert!(
        names.contains(&"submit_stage"),
        "expected submit_stage in actions, got {names:?}"
    );
    assert!(
        names.contains(&"cancel_query"),
        "expected cancel_query in actions, got {names:?}"
    );

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// list_flights
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_flights_returns_registered_dataset() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_orders_single_epoch().await;

    let mut client = harness.client.clone();
    let mut request = tonic::Request::new(Criteria::default());
    request
        .metadata_mut()
        .insert("x-tenant-id", TENANT.parse().unwrap());

    let stream = client
        .list_flights(request)
        .await
        .expect("list_flights RPC failed")
        .into_inner();

    let infos: Vec<_> = stream
        .try_collect()
        .await
        .expect("collect flight stream");

    assert_eq!(infos.len(), 1, "expected exactly 1 registered dataset");

    let desc = infos[0]
        .flight_descriptor
        .as_ref()
        .expect("missing descriptor");
    assert!(
        desc.path.iter().any(|p| p == "orders"),
        "expected path to contain 'orders', got {:?}",
        desc.path
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn test_list_flights_no_header_is_unauthenticated() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let mut client = harness.client.clone();

    // No x-tenant-id header — should fail.
    let err = client
        .list_flights(Criteria::default())
        .await
        .expect_err("expected Unauthenticated error");

    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "expected Unauthenticated, got {err:?}"
    );

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// get_schema
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_schema_returns_correct_column_names() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_orders_single_epoch().await;

    let mut client = harness.client.clone();
    let descriptor = FlightDescriptor::new_cmd(ORDERS_SQL.as_bytes().to_vec());
    let mut request = tonic::Request::new(descriptor);
    request
        .metadata_mut()
        .insert("x-tenant-id", TENANT.parse().unwrap());

    let schema_result = client
        .get_schema(request)
        .await
        .expect("get_schema RPC failed")
        .into_inner();

    // The service encodes the schema with arrow_ipc::writer::FileWriter,
    // so we decode with FileReader.
    let schema_bytes = schema_result.schema.to_vec();
    let reader =
        FileReader::try_new(Cursor::new(schema_bytes), None).expect("decode schema IPC file");
    let schema = reader.schema();

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec!["order_id", "customer_id", "amount", "order_date"],
        "unexpected schema fields"
    );

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// get_flight_info
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_flight_info_returns_query_ticket() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let seeder = DataSeeder::new(harness.shared_db.clone(), harness.sql_transformer.clone());
    seeder.seed_orders_single_epoch().await;

    let mut client = harness.client.clone();
    let descriptor = FlightDescriptor::new_cmd(ORDERS_SQL.as_bytes().to_vec());
    let mut request = tonic::Request::new(descriptor);
    request
        .metadata_mut()
        .insert("x-tenant-id", TENANT.parse().unwrap());

    let flight_info = client
        .get_flight_info(request)
        .await
        .expect("get_flight_info RPC failed")
        .into_inner();

    assert_eq!(
        flight_info.endpoint.len(),
        1,
        "expected 1 endpoint, got {}",
        flight_info.endpoint.len()
    );

    let ticket = flight_info.endpoint[0]
        .ticket
        .as_ref()
        .expect("endpoint has no ticket");

    let flight_ticket = FlightTicket::decode(ticket).expect("decode FlightTicket");
    assert!(
        matches!(flight_ticket, FlightTicket::Query(_)),
        "expected FlightTicket::Query, got {flight_ticket:?}"
    );

    if let FlightTicket::Query(ref q) = flight_ticket {
        assert_eq!(q.sql, ORDERS_SQL, "ticket SQL does not match original SQL");
        assert_eq!(q.tenant_id, TENANT, "ticket tenant does not match");
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn test_get_flight_info_no_header_is_unauthenticated() {
    init_tracing();
    let harness = FlightTestHarness::start(TENANT).await;
    let mut client = harness.client.clone();

    let descriptor = FlightDescriptor::new_cmd(ORDERS_SQL.as_bytes().to_vec());
    // No x-tenant-id header.
    let err = client
        .get_flight_info(descriptor)
        .await
        .expect_err("expected Unauthenticated error");

    assert_eq!(
        err.code(),
        tonic::Code::Unauthenticated,
        "expected Unauthenticated, got {err:?}"
    );

    harness.shutdown().await;
}
