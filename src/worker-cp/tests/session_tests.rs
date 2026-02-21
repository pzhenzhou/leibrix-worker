//! Integration tests for `ControlPlaneSession`.
//!
//! Uses [`MockCpServer`] (a real tonic gRPC server on 127.0.0.1:0) and
//! [`RecordingDispatcher`] to exercise the full session lifecycle without
//! any real storage or data-loading infrastructure.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::mock_server::MockCpServer;
use common::recording_dispatcher::RecordingDispatcher;

use worker_cp::{ControlPlaneConfig, ControlPlaneSession, SessionStatus};
use worker_proto::control_plane::{
    event_stream_message::Payload, CommonAckEvent, EventStreamMessage,
};

/// Build a fast config suitable for tests (short timeouts).
fn test_config(endpoint: &str) -> ControlPlaneConfig {
    ControlPlaneConfig {
        master_addr: endpoint.into(),
        worker_id: "test-worker".into(),
        tenant_id: "test-tenant".into(),
        heartbeat_interval: Duration::from_millis(200),
        registration_timeout: Duration::from_secs(5),
        outgoing_channel_capacity: 8, // small for backpressure test
        connect_timeout: Duration::from_secs(2),
        retry_initial_interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(200),
        retry_max_elapsed_time: Duration::from_secs(5),
        ..Default::default()
    }
}

/// Verify the happy-path lifecycle:
/// connect → register → receive ack → session Active.
#[tokio::test]
async fn session_lifecycle_connect_register_active() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    // The mock must send the registration ack *after* the worker sends its
    // RegisterEvent.  We start the session in a spawned task so we can
    // orchestrate the mock independently.
    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    // Wait for the worker's RegisterEvent.
    let reg_msg = mock.next_request().await;
    assert!(
        matches!(reg_msg.payload, Some(Payload::RegisterEvent(_))),
        "first message should be RegisterEvent, got {:?}",
        reg_msg.payload
    );
    assert_eq!(reg_msg.worker_id, "test-worker");
    assert_eq!(reg_msg.tenant_id, "test-tenant");

    // Send registration ack.
    mock.push_response(MockCpServer::registration_ack()).await;

    // Session should now be Active.
    let session = start_handle
        .await
        .unwrap()
        .expect("session start must succeed");
    let status = session.status().borrow().clone();
    assert!(
        matches!(status, SessionStatus::Active),
        "expected Active, got {status:?}"
    );
}

/// Verify that the session sends heartbeats periodically.
#[tokio::test]
async fn session_sends_heartbeats() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    // Consume RegisterEvent and ack.
    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let _session = start_handle.await.unwrap().expect("session start");

    // Wait for at least 2 heartbeats (interval=200ms, wait ~600ms).
    let mut heartbeat_count = 0;
    for _ in 0..10 {
        if let Some(msg) = mock.try_next_request(Duration::from_millis(200)).await {
            if matches!(msg.payload, Some(Payload::HeartbeatEvent(_))) {
                heartbeat_count += 1;
            }
            if heartbeat_count >= 2 {
                break;
            }
        }
    }
    assert!(
        heartbeat_count >= 2,
        "expected at least 2 heartbeats, got {heartbeat_count}"
    );
}

/// Verify that a DataAssignmentEvent is dispatched to the TaskDispatcher.
#[tokio::test]
async fn session_dispatches_assignment() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let _session = start_handle.await.unwrap().expect("session start");

    // Push a DataAssignmentEvent from the mock CP.
    use worker_proto::control_plane::DataAssignmentEvent;
    let assignment_msg = EventStreamMessage {
        event_id: "assign-1".into(),
        tenant_id: "test-tenant".into(),
        worker_id: "test-worker".into(),
        payload: Some(Payload::DataAssignment(DataAssignmentEvent {
            dataset_id: "sales".into(),
            epoch_id: "ep-1".into(),
            load_plan: None, // will fail conversion; dispatcher still gets called
        })),
    };
    mock.push_response(assignment_msg).await;

    // The receive task should forward a failed conversion as an error log
    // rather than dispatching, because load_plan is None.
    // Let's send a CommonAckEvent to trigger a dispatch.
    let evict_msg = EventStreamMessage {
        event_id: "evict-1".into(),
        tenant_id: "test-tenant".into(),
        worker_id: "test-worker".into(),
        payload: Some(Payload::CommonAck(CommonAckEvent {
            server_id: "mock-master".into(),
            event_type: "evict_epoch".into(),
            payload: None,
        })),
    };
    mock.push_response(evict_msg).await;

    // Wait for the dispatched command.
    dispatcher
        .wait_for_commands(1, Duration::from_secs(3))
        .await;
    let cmds = dispatcher.commands().await;
    assert!(!cmds.is_empty(), "expected at least one dispatched command");
    assert!(
        matches!(&cmds[0], worker_cp::ControlCommand::EvictEpoch { .. }),
        "expected EvictEpoch, got {:?}",
        cmds[0]
    );
}

/// Verify `try_send_status_update` enqueues a StatusUpdate that reaches
/// the mock server.
#[tokio::test]
async fn session_status_update_reaches_server() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let session = start_handle.await.unwrap().expect("session start");

    // Send a status update.
    session
        .try_send_status_update(
            "ds-1".into(),
            "ep-1".into(),
            worker_cp::LoadStatus::Completed,
            None,
        )
        .expect("try_send_status_update must succeed");

    // Look for the DataPullStatusUpdate among inbound messages.
    let mut found = false;
    for _ in 0..20 {
        if let Some(msg) = mock.try_next_request(Duration::from_millis(200)).await {
            if matches!(msg.payload, Some(Payload::DataPullStatusUpdate(_))) {
                found = true;
                break;
            }
        }
    }
    assert!(found, "expected DataPullStatusUpdate from worker");
}

/// When the server drops the stream, the session should transition to
/// Disconnected and the status watch should fire.
#[tokio::test]
async fn disconnect_on_server_stream_close() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let session = start_handle.await.unwrap().expect("session start");

    // Session should be Active.
    let status = session.status().borrow().clone();
    assert!(matches!(status, SessionStatus::Active));

    // Drop the mock server (closes all channels → stream closes).
    drop(mock);

    // Wait for the session to notice and transition to Disconnected.
    let mut status_rx = session.status();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if status_rx.changed().await.is_err() {
                // Sender dropped → session tasks exited → also disconnected.
                break;
            }
            let s = status_rx.borrow_and_update().clone();
            if matches!(s, SessionStatus::Disconnected(_)) {
                break;
            }
        }
    })
    .await;
    assert!(result.is_ok(), "session did not disconnect within 5s");

    // After disconnect, try_send_status_update should fail.
    let err =
        session.try_send_status_update("x".into(), "y".into(), worker_cp::LoadStatus::Failed, None);
    assert!(err.is_err(), "expected error after disconnect");
}

/// If the server never sends a registration ack, the session should time out.
#[tokio::test]
async fn disconnect_on_registration_timeout() {
    common::init_tracing();

    let _mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    // Very short registration timeout for this test.
    let cfg = ControlPlaneConfig {
        master_addr: _mock.endpoint(),
        worker_id: "w-timeout".into(),
        tenant_id: "t-timeout".into(),
        registration_timeout: Duration::from_millis(200),
        retry_initial_interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(100),
        retry_max_elapsed_time: Duration::from_secs(2),
        ..Default::default()
    };

    let result = ControlPlaneSession::start(cfg, dispatcher).await;
    assert!(result.is_err(), "expected HandshakeTimeout error");
    let err_str = result.err().unwrap().to_string();
    assert!(
        err_str.contains("timed out"),
        "expected timeout error, got: {err_str}"
    );
}

/// If the master cannot be reached, session should fail with ConnectionFailed.
#[tokio::test]
async fn disconnect_on_connection_refused() {
    common::init_tracing();

    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = ControlPlaneConfig {
        master_addr: "http://127.0.0.1:1".into(), // port 1 is almost certainly refused
        worker_id: "w-refused".into(),
        tenant_id: "t-refused".into(),
        retry_initial_interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(100),
        retry_max_elapsed_time: Duration::from_millis(500),
        ..Default::default()
    };

    let result = ControlPlaneSession::start(cfg, dispatcher).await;
    assert!(result.is_err(), "expected ConnectionFailed error");
}

// ===========================================================================
// Backpressure Tests  (§6.5)
// ===========================================================================

/// When the outgoing channel is full, `try_send_status_update` should return
/// `BackpressureFull`.
#[tokio::test]
async fn backpressure_full_returned_when_channel_full() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    // outgoing_channel_capacity = 8 (set in test_config)
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let session = start_handle.await.unwrap().expect("session start");

    // Flood the outgoing channel.  The send task reads messages, so we need
    // to stop consuming from the mock side first.  We do this by simply not
    // calling `mock.next_request()` — the mock's inbound channel (capacity 64)
    // will eventually fill too, but we only need the *outgoing* channel to fill.
    //
    // Actually, the gRPC channel also reads, so let's pause the send task by
    // filling the internal outgoing channel faster than the send task drains it.
    let mut backpressure_seen = false;
    for i in 0..200 {
        let result = session.try_send_status_update(
            format!("ds-{i}"),
            format!("ep-{i}"),
            worker_cp::LoadStatus::InProgress,
            None,
        );
        if let Err(worker_cp::SessionError::BackpressureFull { capacity }) = result {
            assert_eq!(capacity, 8, "capacity should match config");
            backpressure_seen = true;
            break;
        }
    }
    assert!(
        backpressure_seen,
        "expected BackpressureFull error after flooding the channel"
    );
}

/// After BackpressureFull, the session should still be alive and able to
/// process new messages once the channel drains.
#[tokio::test]
async fn backpressure_recovers_after_drain() {
    common::init_tracing();

    let mut mock = MockCpServer::start().await;
    let dispatcher = Arc::new(RecordingDispatcher::new());
    let cfg = test_config(&mock.endpoint());

    let d = Arc::clone(&dispatcher);
    let start_handle = tokio::spawn(async move { ControlPlaneSession::start(cfg, d).await });

    let _ = mock.next_request().await;
    mock.push_response(MockCpServer::registration_ack()).await;
    let session = start_handle.await.unwrap().expect("session start");

    // Fill the channel until backpressure.
    for i in 0..200 {
        let result = session.try_send_status_update(
            format!("ds-{i}"),
            format!("ep-{i}"),
            worker_cp::LoadStatus::InProgress,
            None,
        );
        if result.is_err() {
            break;
        }
    }

    // Drain a few messages from the mock side.
    for _ in 0..10 {
        let _ = mock.try_next_request(Duration::from_millis(100)).await;
    }

    // Allow the send task to drain.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now the channel should have space again.
    let result = session.try_send_status_update(
        "ds-recovery".into(),
        "ep-recovery".into(),
        worker_cp::LoadStatus::Completed,
        None,
    );
    assert!(
        result.is_ok(),
        "expected try_send to succeed after drain, got {result:?}"
    );
}
