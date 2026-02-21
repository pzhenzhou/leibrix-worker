#![allow(dead_code)]
//! Shared test infrastructure for `worker-cp` integration tests.
//!
//! Provides:
//! - [`MockCpServer`]: a `tonic` gRPC server implementing `ControlPlaneService`
//!   that captures inbound messages and allows tests to push outbound messages.
//! - [`RecordingDispatcher`]: a [`TaskDispatcher`] that records every
//!   `handle_command` call for assertion.

pub mod mock_server;
pub mod recording_dispatcher;

use std::sync::Once;

static INIT: Once = Once::new();

/// Call once per test binary to initialise `tracing_subscriber`.
pub fn init_tracing() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("worker_cp=debug,common=debug")
            .with_test_writer()
            .try_init();
    });
}
