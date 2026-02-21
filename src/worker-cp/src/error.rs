//! Session-level error types for `worker-cp`.
//!
//! # Design Rationale
//!
//! Errors are classified into distinct categories so the caller can react
//! appropriately without pattern-matching on strings:
//!
//! - [`SessionError::ConnectionFailed`] / [`SessionError::HandshakeTimeout`] —
//!   transient infra problems; can be retried at startup with exponential backoff.
//! - [`SessionError::RegistrationRejected`] — permanent configuration error;
//!   no retry is useful.
//! - [`SessionError::StreamClosed`] — the CP (or a new leader) is responsible
//!   for re-establishing contact; the session simply terminates.
//! - [`SessionError::BackpressureFull`] / [`SessionError::SessionClosed`] —
//!   local channel problems returned to callers of `try_send_status_update`.

use std::time::Duration;

/// All errors that can terminate a [`crate::session::ControlPlaneSession`]
/// or be returned from its public API.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The worker could not establish a TCP/TLS connection to the Control Plane.
    ///
    /// Returned during [`crate::session::ControlPlaneSession::start`] after all
    /// exponential-backoff retries are exhausted.
    #[error("failed to connect to control plane at `{addr}`: {source}")]
    ConnectionFailed {
        addr: String,
        #[source]
        source: tonic::transport::Error,
    },

    /// The Control Plane explicitly rejected the worker's registration.
    ///
    /// This is a permanent configuration error (e.g., unknown tenant, invalid
    /// worker ID). Retrying without fixing the configuration is pointless.
    #[error("registration rejected by control plane: {reason}")]
    RegistrationRejected { reason: String },

    /// The gRPC bidirectional stream was closed or produced an error.
    ///
    /// In HTTP/2 streaming this is a fatal stream-level event (RST_STREAM,
    /// GOAWAY, TCP disconnect). The session terminates and the CP or a new
    /// leader is expected to re-establish contact.
    #[error("control plane stream closed: {reason}")]
    StreamClosed { reason: String },

    /// The registration handshake did not complete within the configured timeout.
    #[error("registration handshake timed out after {timeout:?}")]
    HandshakeTimeout { timeout: Duration },

    /// The outgoing-event mpsc channel is full.
    ///
    /// The session is still alive. The caller may drop the event, retry after
    /// a short delay, or treat this as a non-fatal status-reporting failure.
    #[error("outgoing event channel is full (capacity: {capacity})")]
    BackpressureFull { capacity: usize },

    /// The outgoing-event mpsc channel is closed because the Send Task has exited.
    ///
    /// This means the session is no longer active. The caller should observe
    /// [`crate::types::SessionStatus::Disconnected`] via the watch channel.
    #[error("session is closed, cannot send events")]
    SessionClosed,
}
