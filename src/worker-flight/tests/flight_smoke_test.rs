//! Smoke tests — validate Stage 2 harness infrastructure end-to-end.
//!
//! If these pass, the server starts cleanly, the client connects, the
//! `health_check` action works, and graceful shutdown completes without
//! resource leaks. All higher-level tests in Stages 3–5 build on this.

mod common;

use arrow_flight::Action;
use bytes::Bytes;
use futures_util::StreamExt;

use common::harness::FlightTestHarness;

const TENANT: &str = "smoke-tenant";

/// Start the server, call health_check, assert the response body is `b"OK"`.
#[tokio::test]
async fn test_health_check_responds_ok() {
    let mut harness = FlightTestHarness::start(TENANT).await;

    let action = Action {
        r#type: "health_check".to_string(),
        body: Bytes::new(),
    };
    let mut stream = harness
        .client
        .do_action(tonic::Request::new(action))
        .await
        .expect("health_check RPC failed")
        .into_inner();

    let result = stream
        .next()
        .await
        .expect("expected at least one response item")
        .expect("stream error on health_check response");

    assert_eq!(
        result.body.as_ref(),
        b"OK",
        "health_check body should be b\"OK\""
    );

    harness.shutdown().await;
}

/// Start the server and immediately shut it down — no panics, no hangs.
#[tokio::test]
async fn test_server_starts_and_shuts_down_cleanly() {
    let harness = FlightTestHarness::start(TENANT).await;
    harness.shutdown().await;
}

/// Verify that the harness correctly reports a wrong-tenant error from
/// a metadata RPC. This validates the `x-tenant-id` header path.
#[tokio::test]
async fn test_wrong_tenant_header_is_rejected() {
    use arrow_flight::FlightDescriptor;

    let mut harness = FlightTestHarness::start(TENANT).await;

    let descriptor = FlightDescriptor::new_cmd(b"SELECT 1".to_vec());
    let mut request = tonic::Request::new(descriptor);
    request
        .metadata_mut()
        .insert("x-tenant-id", "wrong-tenant".parse().unwrap());

    let status = harness
        .client
        .get_flight_info(request)
        .await
        .expect_err("expected an error for wrong tenant");

    assert!(
        matches!(
            status.code(),
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
        ),
        "expected Unauthenticated or PermissionDenied, got {:?}: {}",
        status.code(),
        status.message()
    );

    harness.shutdown().await;
}
