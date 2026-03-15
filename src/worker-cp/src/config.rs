//! `worker-cp` configuration.
//!
//! [`ControlPlaneConfig`] consolidates every knob needed to connect to the
//! Control Plane and sustain the bidirectional stream.  All fields carry
//! sensible production-ready defaults so callers only need to override what
//! differs from the default.

use std::time::Duration;

/// Full configuration for the Control-Plane session.
///
/// # Quick-start
/// ```rust,ignore
/// let cfg = ControlPlaneConfig::new(
///     "http://cp-master:50051",
///     "worker-1",
///     "tenant-acme",
/// );
/// ```
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    // -------------------------------------------------------------------
    // Identity
    // -------------------------------------------------------------------
    /// gRPC endpoint of the Control Plane master, e.g. `"http://cp:50051"`.
    pub master_addr: String,

    /// Unique identifier for this worker process (e.g., `"worker-az1-02"`).
    pub worker_id: String,

    /// Tenant this worker is dedicated to.
    pub tenant_id: String,

    /// The worker's own advertised address (e.g., the Arrow Flight listen
    /// address).  This is what the CP and gateways will use to reach the
    /// worker for queries.  When `None`, falls back to `master_addr` (which
    /// is almost certainly wrong for production).
    pub worker_addr: Option<String>,

    // -------------------------------------------------------------------
    // Session behaviour
    // -------------------------------------------------------------------
    /// How often the worker sends a `Heartbeat` message upstream.
    ///
    /// Default: 10 seconds.
    pub heartbeat_interval: Duration,

    /// Maximum time to wait for the CP's acknowledgement after sending the
    /// registration `RegisterEvent`.
    ///
    /// Default: 10 seconds.
    pub registration_timeout: Duration,

    /// Capacity of the internal mpsc channel that buffers outgoing
    /// [`crate::types::OutgoingPayload`] messages.
    ///
    /// Default: 64 messages.
    pub outgoing_channel_capacity: usize,

    // -------------------------------------------------------------------
    // gRPC transport
    // -------------------------------------------------------------------
    /// Timeout for the initial TCP connection attempt.
    ///
    /// Default: 5 seconds.
    pub connect_timeout: Duration,

    /// Interval between HTTP/2 keep-alive PING frames.
    ///
    /// Default: 15 seconds.
    pub http2_keep_alive_interval: Duration,

    /// How long to wait for a keep-alive PONG before treating the connection
    /// as dead.
    ///
    /// Default: 5 seconds.
    pub keep_alive_timeout: Duration,

    /// Send keep-alive PINGs even when no streams are active.
    ///
    /// Default: `true` (ensures the channel stays alive between epochs).
    pub keep_alive_while_idle: bool,

    // -------------------------------------------------------------------
    // Reconnect backoff
    // -------------------------------------------------------------------
    /// Starting interval before the first reconnect attempt.
    ///
    /// Default: 500 ms.
    pub retry_initial_interval: Duration,

    /// Upper bound on the backoff interval.
    ///
    /// Default: 30 seconds.
    pub retry_max_interval: Duration,

    /// After this much total time has elapsed without a successful connection,
    /// the session gives up and reports
    /// [`crate::error::SessionError::ConnectionFailed`].
    ///
    /// Default: 5 minutes.
    pub retry_max_elapsed_time: Duration,

    // -------------------------------------------------------------------
    // TLS
    // -------------------------------------------------------------------
    /// Optional TLS configuration for the gRPC channel.
    ///
    /// When `None`, the channel uses plain HTTP/2.  When `Some`, the scheme
    /// in `master_addr` is automatically switched to `https://` and the
    /// provided `ClientTlsConfig` is applied to the tonic endpoint.
    pub tls: Option<tonic::transport::ClientTlsConfig>,
}

impl ControlPlaneConfig {
    /// Create a configuration with sane defaults.
    ///
    /// Override individual fields as needed before passing to
    /// `ControlPlaneSession::start`.
    pub fn new(
        master_addr: impl Into<String>,
        worker_id: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Self {
        Self {
            master_addr: master_addr.into(),
            worker_id: worker_id.into(),
            tenant_id: tenant_id.into(),
            ..Self::default()
        }
    }
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            master_addr: String::new(),
            worker_id: String::new(),
            tenant_id: String::new(),
            worker_addr: None,
            heartbeat_interval: Duration::from_secs(10),
            registration_timeout: Duration::from_secs(10),
            outgoing_channel_capacity: 64,
            connect_timeout: Duration::from_secs(5),
            http2_keep_alive_interval: Duration::from_secs(15),
            keep_alive_timeout: Duration::from_secs(5),
            keep_alive_while_idle: true,
            retry_initial_interval: Duration::from_millis(500),
            retry_max_interval: Duration::from_secs(30),
            retry_max_elapsed_time: Duration::from_secs(300),
            tls: None,
        }
    }
}
