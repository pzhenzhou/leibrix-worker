//! [`ControlPlaneSession`] — the public handle for the worker ↔ Control Plane
//! bidirectional session.
//!
//! # Lifecycle
//!
//! ```text
//! Connecting ──► Registering ──► Active ──► Disconnected
//! ```
//!
//! All state transitions are broadcast on the [`SessionStatus`] watch channel
//! returned by [`ControlPlaneSession::status`].
//!
//! # Stream Architecture
//!
//! After a successful call to [`ControlPlaneSession::start`] two background
//! tasks are alive:
//!
//! - **Send Task** – reads [`OutgoingPayload`] from the internal `mpsc` channel
//!   (plus a heartbeat timer) and writes [`EventStreamMessage`] to the gRPC
//!   request stream.
//! - **Receive Task** – reads [`EventStreamMessage`] from the gRPC response
//!   stream and dispatches them to the [`TaskDispatcher`].
//!
//! Both tasks share a [`CancellationToken`]. When either task terminates (due
//! to a stream error or an explicit cancellation) it fires the token so the
//! other task also shuts down cleanly.
//!
//! # Generic dispatcher
//!
//! [`ControlPlaneSession::start`] is generic over `D: TaskDispatcher + 'static`.
//! This lets internal tasks hold `Arc<D>` without erasing the concrete type,
//! matching the `impl Future + Send` style used by `StorageEngine` and
//! `QueryEngine` throughout the project (zero `Box<dyn Future>` overhead).

use backoff::ExponentialBackoff;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use worker_proto::control_plane::{
    control_plane_service_client::ControlPlaneServiceClient, event_stream_message::Payload,
    EventStreamMessage,
};

use crate::{
    config::ControlPlaneConfig,
    dispatch::TaskDispatcher,
    error::SessionError,
    proto_convert,
    types::{LoadStatus, OutgoingPayload, SessionStatus},
};

/// Handle to a live Control Plane session.
///
/// Produced by [`ControlPlaneSession::start`]. Cheaply cloneable status
/// observers via [`Self::status`]; non-blocking status updates via
/// [`Self::try_send_status_update`].
pub struct ControlPlaneSession {
    /// Sender side of the domain-level outgoing-event channel.
    outgoing_tx: mpsc::Sender<OutgoingPayload>,
    /// Watch receiver for observing the session lifecycle state.
    status_rx: watch::Receiver<SessionStatus>,
    /// Cached channel capacity used to populate [`SessionError::BackpressureFull`].
    channel_capacity: usize,
}

impl ControlPlaneSession {
    /// Connect to the Control Plane and start the bidirectional session.
    ///
    /// # Steps
    ///
    /// 1. Build a `tonic` transport channel to `config.master_addr` with
    ///    exponential-backoff retry on transient failures.
    /// 2. Open the `CoordinateWorker` bidirectional streaming RPC.
    /// 3. Send `RegisterEvent`; await `CommonAckEvent(registration_ack)`.
    /// 4. Spawn the Send Task and Receive Task.
    /// 5. Return the session handle.
    ///
    /// # Errors
    ///
    /// - [`SessionError::ConnectionFailed`] – TCP/TLS connection could not be
    ///   established after all backoff retries.
    /// - [`SessionError::StreamClosed`] – The gRPC stream could not be opened.
    /// - [`SessionError::HandshakeTimeout`] – The CP did not respond to
    ///   registration within `config.registration_timeout`.
    /// - [`SessionError::RegistrationRejected`] – The CP explicitly rejected
    ///   the worker's registration.
    /// # Generic parameter
    ///
    /// `D: TaskDispatcher + 'static` — lets the Receive Task own `Arc<D>`
    /// without erasing the concrete type (no `dyn Trait` boxing).  This is
    /// consistent with the `impl Future + Send` style used by
    /// [`StorageEngine`](crate::StorageEngine) and
    /// [`QueryEngine`](crate::QueryEngine) throughout the project.
    #[instrument(skip_all, fields(
        worker_id  = %config.worker_id,
        tenant_id  = %config.tenant_id,
        master_addr = %config.master_addr,
    ))]
    pub async fn start<D: TaskDispatcher + 'static>(
        config: ControlPlaneConfig,
        dispatcher: Arc<D>,
    ) -> Result<Self, SessionError> {
        let (status_tx, status_rx) = watch::channel(SessionStatus::Connecting);

        // ── 1. Build tonic channel with exponential-backoff retry ──────────
        info!("connecting to control plane");
        let channel = build_channel_with_backoff(&config, &status_tx).await?;
        debug!("gRPC channel established");

        // ── 2. Open the bidirectional stream ────────────────────────────────
        // `grpc_out_tx` is later handed to the Send Task; here we only use it
        // to send the registration message before the task is spawned.
        let (grpc_out_tx, grpc_out_rx) =
            mpsc::channel::<EventStreamMessage>(config.outgoing_channel_capacity);
        let mut client = ControlPlaneServiceClient::new(channel);
        let response = client
            .coordinate_worker(ReceiverStream::new(grpc_out_rx))
            .await
            .map_err(|s| SessionError::StreamClosed {
                reason: format!("failed to open CoordinateWorker stream: {s}"),
            })?;
        let mut response_stream = response.into_inner();
        debug!("bidirectional stream opened");

        // ── 3. Registration handshake ───────────────────────────────────────
        let _ = status_tx.send(SessionStatus::Registering);

        let register_msg = proto_convert::register_event(
            &config.worker_id,
            &config.tenant_id,
            &config.master_addr,
        );
        grpc_out_tx
            .send(register_msg)
            .await
            .map_err(|_| SessionError::StreamClosed {
                reason: "outgoing gRPC channel closed before registration could be sent"
                    .to_string(),
            })?;
        debug!("registration event sent");

        let timeout_dur = config.registration_timeout;
        let first_msg = tokio::time::timeout(timeout_dur, response_stream.message())
            .await
            .map_err(|_| SessionError::HandshakeTimeout {
                timeout: timeout_dur,
            })?
            .map_err(|s| SessionError::StreamClosed {
                reason: format!("stream error during registration: {s}"),
            })?
            .ok_or_else(|| SessionError::StreamClosed {
                reason: "server closed stream during registration handshake".to_string(),
            })?;

        match &first_msg.payload {
            Some(Payload::CommonAck(ack)) if ack.event_type == "registration_ack" => {
                info!(event_id = %first_msg.event_id, "registration acknowledged by CP");
            }
            Some(Payload::CommonAck(ack)) if ack.event_type.contains("reject") => {
                return Err(SessionError::RegistrationRejected {
                    reason: ack.event_type.clone(),
                });
            }
            unexpected => {
                // Some CP implementations may respond with a different ack type.
                // Log and continue rather than hard-failing to ease bring-up.
                warn!(?unexpected, event_id = %first_msg.event_id,
                      "unexpected first message from CP; treating as registration ack");
            }
        }

        let channel_capacity = config.outgoing_channel_capacity;
        let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingPayload>(channel_capacity);
        let cancel = CancellationToken::new();

        let _ = status_tx.send(SessionStatus::Active);
        info!("session is now active");

        // ── 5. Spawn Send Task and Receive Task ─────────────────────────────
        tokio::spawn(run_send_task(
            config.worker_id.clone(),
            config.tenant_id.clone(),
            outgoing_rx,
            grpc_out_tx,
            config.heartbeat_interval,
            cancel.clone(),
            status_tx.clone(),
        ));

        tokio::spawn(run_receive_task(
            response_stream,
            dispatcher,
            cancel,
            status_tx,
        ));

        Ok(Self {
            outgoing_tx,
            status_rx,
            channel_capacity,
        })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Returns a clone of the outgoing-event sender.
    ///
    /// Use this to wire up a [`crate::WorkerRuntimeDispatcher`] after the
    /// session has started (see [`crate::WorkerRuntimeDispatcher::init_sender`]).
    pub fn outgoing_sender(&self) -> mpsc::Sender<OutgoingPayload> {
        self.outgoing_tx.clone()
    }

    /// Enqueue a load-status update to be sent upstream — non-blocking.
    ///
    /// # Errors
    ///
    /// - [`SessionError::BackpressureFull`] — the outgoing channel is at
    ///   capacity; the caller may retry after a short delay.
    /// - [`SessionError::SessionClosed`] — the session has already terminated.
    pub fn try_send_status_update(
        &self,
        dataset_id: String,
        epoch_id: String,
        status: LoadStatus,
        error_message: Option<String>,
    ) -> Result<(), SessionError> {
        let payload = OutgoingPayload::StatusUpdate {
            dataset_id,
            epoch_id,
            status,
            error: error_message,
        };
        self.outgoing_tx.try_send(payload).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SessionError::BackpressureFull {
                capacity: self.channel_capacity,
            },
            mpsc::error::TrySendError::Closed(_) => SessionError::SessionClosed,
        })
    }

    /// Return a watch receiver that broadcasts [`SessionStatus`] transitions.
    ///
    /// Callers can `await` on `receiver.changed()` to be notified when the
    /// session transitions to [`SessionStatus::Disconnected`].
    pub fn status(&self) -> watch::Receiver<SessionStatus> {
        self.status_rx.clone()
    }
}

/// Build a tonic transport channel, retrying on transient failures using
/// exponential back-off.
///
/// Permanent errors (e.g., malformed `master_addr`) are surfaced immediately.
/// Transient errors (e.g., connection refused) are retried until
/// `config.retry_max_elapsed_time` is exceeded, at which point
/// [`SessionError::ConnectionFailed`] is returned with the last underlying
/// error.
async fn build_channel_with_backoff(
    config: &ControlPlaneConfig,
    _status_tx: &watch::Sender<SessionStatus>,
) -> Result<tonic::transport::Channel, SessionError> {
    // Normalize the address: tonic requires an absolute URI with a scheme.
    // Accept bare `host:port` (or `host`) and prepend the correct scheme.
    // When TLS is configured, force `https://`; otherwise use `http://`.
    let has_tls = config.tls.is_some();
    let addr = {
        let raw = config.master_addr.as_str();
        if raw.starts_with("http://") || raw.starts_with("https://") {
            config.master_addr.clone()
        } else if has_tls {
            format!("https://{raw}")
        } else {
            format!("http://{raw}")
        }
    };

    // Validate the address once upfront; a parse failure is permanent.
    let base_endpoint = tonic::transport::Endpoint::from_shared(addr.clone()).map_err(|e| {
        SessionError::ConnectionFailed {
            addr: addr.clone(),
            source: e,
        }
    })?;

    let endpoint = base_endpoint
        .connect_timeout(config.connect_timeout)
        .http2_keep_alive_interval(config.http2_keep_alive_interval)
        .keep_alive_timeout(config.keep_alive_timeout)
        .keep_alive_while_idle(config.keep_alive_while_idle);

    // Apply TLS config if provided.
    let endpoint = if let Some(tls) = config.tls.clone() {
        endpoint
            .tls_config(tls)
            .map_err(|e| SessionError::ConnectionFailed {
                addr: addr.clone(),
                source: e,
            })?
    } else {
        endpoint
    };

    let backoff = ExponentialBackoff {
        initial_interval: config.retry_initial_interval,
        max_interval: config.retry_max_interval,
        max_elapsed_time: Some(config.retry_max_elapsed_time),
        multiplier: 1.5,
        randomization_factor: 0.5,
        ..Default::default()
    };

    // `backoff::future::retry` owns the sleep schedule; no manual loop or
    // `tokio::time::sleep` needed here.  All connection failures are treated as
    // transient — the backoff's `max_elapsed_time` determines when we give up.
    backoff::future::retry(backoff, || {
        let endpoint = endpoint.clone();
        let addr = addr.clone();
        async move {
            endpoint.connect().await.map_err(|e| {
                warn!(addr = %addr, error = %e, "connection attempt failed, will retry");
                backoff::Error::transient(e)
            })
        }
    })
    .await
    .map_err(|e| SessionError::ConnectionFailed {
        addr: addr.clone(),
        source: e,
    })
}

/// Background task that reads incoming events from the gRPC response stream
/// and dispatches them to the [`TaskDispatcher`].
///
/// # Shutdown
///
/// Exits when:
/// - The gRPC response stream closes or yields an error → fires `cancel`.
/// - `cancel` is fired by the Send Task → exits cleanly.
///
/// In both cases, [`SessionStatus::Disconnected`] is published on `status_tx`.
async fn run_receive_task<D: TaskDispatcher + 'static>(
    mut response_stream: tonic::codec::Streaming<EventStreamMessage>,
    dispatcher: Arc<D>,
    cancel: CancellationToken,
    status_tx: watch::Sender<SessionStatus>,
) {
    debug!("receive task started");

    let disconnect = |status_tx: watch::Sender<SessionStatus>, reason: String| {
        warn!(reason = %reason, "receive task: stream closed");
        let _ = status_tx.send(SessionStatus::Disconnected(Arc::new(
            SessionError::StreamClosed { reason },
        )));
    };

    loop {
        tokio::select! {
            biased;

            // Graceful shutdown takes priority over new messages.
            _ = cancel.cancelled() => {
                debug!("receive task stopping (cancel)");
                return;
            }

            result = response_stream.message() => {
                match result {
                    Err(status) => {
                        cancel.cancel();
                        disconnect(status_tx, format!("gRPC stream error: {status}"));
                        return;
                    }
                    Ok(None) => {
                        cancel.cancel();
                        disconnect(status_tx, "server closed the stream".to_string());
                        return;
                    }
                    Ok(Some(msg)) => {
                        handle_inbound(msg, &dispatcher).await;
                    }
                }
            }
        }
    }
}

/// Decode and dispatch a single inbound [`EventStreamMessage`].
///
/// Conversion errors are logged-and-skipped; they must never crash the receive
/// loop or cancel the session.
#[inline]
async fn handle_inbound<D: TaskDispatcher>(msg: EventStreamMessage, dispatcher: &Arc<D>) {
    let event_id = msg.event_id.clone();
    match msg.payload {
        Some(Payload::DataAssignment(ev)) => {
            match crate::proto_convert::assignment_from_proto(ev) {
                Ok(assignment) => {
                    debug!(%event_id, dataset_id = %assignment.dataset_id,
                           epoch_id = %assignment.epoch_id, "dispatching assignment");
                    dispatcher
                        .handle_command(crate::types::ControlCommand::Assignment(Box::new(
                            assignment,
                        )))
                        .await;
                }
                Err(e) => {
                    error!(%event_id, error = %e, "failed to convert DataAssignmentEvent; skipping");
                }
            }
        }
        Some(Payload::CommonAck(ack)) => {
            let command = crate::proto_convert::command_from_ack(ack);
            debug!(%event_id, ?command, "dispatching command");
            dispatcher.handle_command(command).await;
        }
        // HeartbeatEvent / DataPullStatusUpdate / RegisterEvent arriving
        // inbound are CP-initiated, currently no-op on the worker side.
        Some(other) => {
            debug!(%event_id, payload_variant = ?std::mem::discriminant(&other),
                   "ignoring unsupported inbound payload variant");
        }
        None => {
            warn!(%event_id, "received EventStreamMessage with empty payload; skipping");
        }
    }
}

/// Background task that writes outgoing events to the gRPC request stream.
///
/// # Sources (via `select!`)
///
/// 1. `outgoing_rx` — domain payloads from [`ControlPlaneSession::try_send_status_update`].
/// 2. Heartbeat `interval` — fires every `heartbeat_interval`.
/// 3. `cancel` — gracefully drains the channel then exits.
///
/// # Shutdown
///
/// When `cancel` fires the task performs a best-effort drain of any pending
/// messages that arrived before cancellation, then exits.  If any gRPC write
/// fails (stream dead), the task cancels the shared token so the Receive Task
/// also shuts down.
async fn run_send_task(
    worker_id: String,
    tenant_id: String,
    mut outgoing_rx: mpsc::Receiver<OutgoingPayload>,
    grpc_tx: mpsc::Sender<EventStreamMessage>,
    heartbeat_interval: std::time::Duration,
    cancel: CancellationToken,
    status_tx: watch::Sender<SessionStatus>,
) {
    debug!("send task started");

    // Start the heartbeat interval after one full period (no immediate tick).
    let start = tokio::time::Instant::now() + heartbeat_interval;
    let mut hb_interval = tokio::time::interval_at(start, heartbeat_interval);
    // Miss a tick rather than burst on resume.
    hb_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Sends `msg` on `grpc_tx`. Returns `false` if the stream is dead.
    macro_rules! write_proto {
        ($msg:expr) => {{
            if grpc_tx.send($msg).await.is_err() {
                warn!("send task: gRPC write channel closed; stream is dead");
                cancel.cancel();
                let _ = status_tx.send(SessionStatus::Disconnected(Arc::new(
                    SessionError::StreamClosed {
                        reason: "gRPC request channel closed".to_string(),
                    },
                )));
                return;
            }
        }};
    }

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                debug!("send task stopping (cancel); draining outgoing channel");
                // Best-effort flush: forward any already-queued messages before exit.
                while let Ok(payload) = outgoing_rx.try_recv() {
                    let msg = crate::proto_convert::wrap_outgoing(&worker_id, &tenant_id, payload);
                    // Ignore errors during shutdown drain — stream may already be gone.
                    let _ = grpc_tx.try_send(msg);
                }
                return;
            }

            maybe_payload = outgoing_rx.recv() => {
                match maybe_payload {
                    Some(payload) => {
                        let msg = crate::proto_convert::wrap_outgoing(&worker_id, &tenant_id, payload);
                        debug!(event_id = %msg.event_id, "send task: forwarding status update");
                        write_proto!(msg);
                    }
                    None => {
                        // All senders dropped — session handle was dropped.
                        debug!("send task: outgoing channel closed; stopping");
                        cancel.cancel();
                        let _ = status_tx.send(SessionStatus::Disconnected(Arc::new(
                            SessionError::SessionClosed,
                        )));
                        return;
                    }
                }
            }

            _ = hb_interval.tick() => {
                let msg = crate::proto_convert::wrap_outgoing(
                    &worker_id,
                    &tenant_id,
                    OutgoingPayload::Heartbeat,
                );
                debug!(event_id = %msg.event_id, "send task: sending heartbeat");
                write_proto!(msg);
            }
        }
    }
}
