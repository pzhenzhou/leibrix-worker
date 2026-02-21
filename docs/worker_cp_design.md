# Worker Control Plane (worker-cp) Design

## 1. Overview

The `worker-cp` crate is the control plane communication module for the Leibrix Worker. It manages the bidirectional
gRPC stream (`CoordinateWorker`) between the Worker and the Control Plane (Master). It acts as the control interface for
the worker, handling registration, heartbeats, data assignments, and status reporting.

This document outlines the high-level design, core abstractions, ownership model, error handling strategy, and
performance considerations for implementing `worker-cp` in Rust.

## 2. Core Responsibilities

- **Session Management**: Establish a bidirectional gRPC stream with the Master. If the connection is lost, terminate
  the session with an error (the CP is responsible for re-establishing contact via a new leader).
- **Lifecycle Management**: Handle worker registration (`RegisterEvent`) upon startup and periodic health reporting (
  `HeartbeatEvent`).
- **Task Dispatching**: Receive `DataAssignmentEvent` from the Master and dispatch them to the `worker-storage` data
  loader without blocking the control stream.
- **Status Reporting**: Report the progress and outcome of data loading tasks via `DataPullStatusUpdateEvent` (e.g.,
  `IN_PROGRESS`, `COMPLETED`, `FAILED`).
- **Command Handling**: Process `CommonAckEvent` for control commands like `evict_epoch` or `drain_command`.

## 3. Core Abstractions & Architecture

The architecture is designed around asynchronous message passing to ensure the gRPC stream remains highly responsive.

### 3.1. `ControlPlaneSession`

The main entry point and public API for the `worker-cp` crate. "Session" reflects the bidirectional, symmetric nature of
the gRPC stream — the worker both receives active notifications from the CP and reports status back.

- **Ownership**: Owned by the main worker runtime (e.g., in `worker-cli`).
- **Responsibilities**:
    - Initialize the gRPC channel using `tonic` (acting as the network-level client to dial the Master).
    - Perform the initial handshake (sending `RegisterEvent` and waiting for `CommonAckEvent` with `registration_ack`).
    - Spawn the background Send Task and Receive Task.
    - Provide a thread-safe handle (via `mpsc::Sender`) to enqueue outgoing events to the Master.
    - Expose a `SessionStatus` channel (`tokio::sync::watch`) so the worker runtime can observe session health (e.g.,
      detect when the stream has been torn down and the session is no longer active).

### 3.2. Send Task & Receive Task

The `EventLoop` is not a single loop but two independent `tokio::spawn` tasks that share ownership of a
`CancellationToken`. This separation ensures that a blocked or failing write path never stalls the processing of
incoming events.

- **Receive Task**: Continuously awaits messages from the gRPC `ResponseStream`. When an event (e.g.,
  `DataAssignmentEvent`) arrives, it immediately calls the `TaskDispatcher`. This task never blocks on network sends. If
  the `ResponseStream` returns `None` (server closed the stream) or an error, it cancels the shared `CancellationToken`
  which signals the Send Task to shut down, and then publishes `SessionStatus::Disconnected` via the `watch` channel.

- **Send Task**: Uses `tokio::select!` over three sources:
    1. The internal `mpsc::Receiver` (outgoing events from `DataLoader` tasks).
    2. A `tokio::time::interval` tick for heartbeats (no separate `HeartbeatManager` component needed — the interval is
       an implementation detail of this task).
    3. The `CancellationToken` (for graceful shutdown).

  When the Send Task reads a message from any source, it writes it to the gRPC `RequestStream`. If the write fails, the
  stream is dead — it cancels the shared `CancellationToken` (which signals the Receive Task to stop) and publishes
  `SessionStatus::Disconnected`. **No backoff retry is attempted on the stream itself**, because a gRPC stream write
  failure means the HTTP/2 stream is broken and cannot be reused.

### 3.3. `TaskDispatcher` Trait

An abstraction to decouple the gRPC event handling from the actual data loading work (`worker-storage`).

- **Ownership**: Implemented by the worker runtime and passed to `worker-cp` as an `Arc<dyn TaskDispatcher>`.
- **Proto Boundary**: The `TaskDispatcher` trait does **not** expose raw proto types. The Receive Task converts
  `DataAssignmentEvent` and `CommonAckEvent` into domain types (e.g., `LoadAssignment`, `ControlCommand`) before calling
  the dispatcher. This conversion happens in a single `proto_convert.rs` module, following the project's "Proto Boundary
  as Conversion Firewall" principle.
- **Data Loading Concurrency Limit**: The dispatcher should use a `tokio::sync::Semaphore` to cap the number of *
  *concurrent `DataLoader::load_epoch` operations** (not the event dispatch rate). The Receive Task always dispatches
  events immediately (spawn and return), but each spawned task must `semaphore.acquire()` before calling `load_epoch`.
  This prevents memory and CPU exhaustion if the Master sends many assignments in a burst. The bounded `mpsc` channel
  only limits the outgoing status-update path and has no effect on incoming dispatch throughput.

## 4. Proto Conversion Boundary

Following the project's design principles, a `proto_convert.rs` module in `worker-cp` handles all conversions between
proto types and domain types:

```rust
// worker-cp/src/proto_convert.rs

/// Domain type for a data loading assignment (converted from DataAssignmentEvent + LoadPlan)
pub struct LoadAssignment {
    pub dataset_id: String,    // Future: DatasetId newtype
    pub epoch_id: String,      // Matches existing EpochId = String
    pub load_plan: DomainLoadPlan,
}

/// Domain type for control commands (converted from CommonAckEvent)
pub enum ControlCommand {
    EvictEpoch { dataset_id: String, epoch_id: String, reason: String },
    Drain,
    // ... other commands
}

/// Builds an EventStreamMessage envelope with event_id, tenant_id, worker_id.
/// The session owns these identity fields and stamps every outgoing message.
pub(crate) fn wrap_outgoing(
    worker_id: &str,
    tenant_id: &str,
    payload: OutgoingPayload,
) -> EventStreamMessage { ... }
```

**Note on Newtypes**: `WorkerId` already exists in `worker-storage/src/ldp/types.rs`. `TenantId` and `DatasetId` do not
exist yet in the codebase (both are raw `String`). The `worker-cp` implementation should initially use `String` for
consistency with the rest of the codebase, and introduce newtypes as a follow-up refactor across the workspace.

## 5. Ownership and Concurrency Model

```text
                                 [Master (Control Plane)]
                                       ^      |
                                       |      | gRPC Bidirectional Stream
                                       |      v
┌──────────────────────────────────────|──────|──────────────────────────────────────┐
│                                      |      |                                      │
│  ┌─────────────────────────────┐     |      |    ┌──────────────────────────────┐  │
│  │       Send Task             │<────┘      └───>│       Receive Task           │  │
│  │ select! {                   │                  │ loop {                       │  │
│  │   msg from mpsc::Receiver,  │                  │   match stream.next() {     │  │
│  │   tick from interval,       │  CancellationTkn │     assignment => dispatch, │  │
│  │   cancel from token,        │<────────────────>│     command => handle,      │  │
│  │ }                           │                  │     None/Err => cancel,     │  │
│  └─────────────────────────────┘                  │   }                         │  │
│               ^                                   │ }                           │  │
│               | (mpsc::Receiver)                  └──────────────────────────────┘  │
└───────────────|────────────────────────────────────────────────|────────────────────┘
                |                                                |
                | Outgoing Events                                | Incoming Events
                |                                                v
     ┌──────────┴──────────┐                           [TaskDispatcher]
     │                     │                (proto_convert → domain types)
     │  DataLoader Tasks   │                                    |
     │  HeartbeatInterval  │                         Semaphore-bounded spawn
     │                     │                                    |
     └─────────────────────┘                                    v
                                                      [DataLoader Task]
                                                   (calls worker-storage)
```

**Key invariants**:

1. The Receive Task and Send Task run as independent `tokio::spawn` tasks, linked only by a shared `CancellationToken`
   and a `watch::Sender<SessionStatus>`.
2. If either task detects a fatal stream error, it cancels the token, which causes the other task to shut down
   gracefully.
3. The `TaskDispatcher` is called synchronously within the Receive Task (it must NOT block — it spawns a `tokio` task
   and returns immediately).
4. Spawned `DataLoader` tasks send `DataPullStatusUpdateEvent` back to the Send Task's queue via a cloned
   `mpsc::Sender`.

## 6. Error Handling Strategy

### 6.0. No Dead Letter Queue

A dead letter queue (DLQ) is **not** needed for this design. The rationale by direction:

**Incoming events (CP → worker):**

- If `load_epoch` fails, the spawned task reports a `FAILED` status update back to the CP via the outgoing channel. The
  CP owns assignment state and can re-issue the assignment if appropriate.
- If proto conversion fails for a malformed event (e.g., unrecognized `CommonAckEvent.event_type`), the Receive Task
  logs the error at `warn` level and **drops the event**. There is no meaningful retry — the event is malformed and
  re-processing it would produce the same error.
- If `dispatch_assignment` itself fails synchronously (e.g., the dispatcher panics or returns an error), the Receive
  Task catches the error, logs it, and **continues processing the next event from the stream**. This is critical: a
  single bad event must never terminate the Receive Task or block subsequent events.

**Outgoing events (worker → CP):**

- If the session dies, pending status updates in the mpsc channel are lost. A DLQ would have no destination to replay to
  until the CP re-establishes a new session. When reconnected, the CP can reconcile state by querying the worker's
  loaded epochs (via `StorageEngine::list_epochs`).

**Status report failure after successful `load_epoch`:**

A critical edge case: `load_epoch` succeeds (data is loaded into DuckDB), but the status report back to the CP fails.
Two sub-cases:

- **Channel full (`BackpressureFull`)**: The session is still alive, the outgoing queue is temporarily saturated. The
  spawned task should retry `try_send` a few times with a short delay (e.g., 3 retries, 100ms apart). If still full, log
  the unreported success at `warn` level and drop the status update. The data remains queryable in DuckDB.
- **Channel closed (`SessionClosed`)**: The session is dead, no destination to send to. Log the unreported success at
  `warn` level and give up. The data remains loaded in DuckDB.

In both cases, a **state divergence** exists: the worker has the data loaded, but the CP doesn't know. This is resolved
via **reconciliation on reconnect**: when the CP re-establishes a new session, the worker reports its currently loaded
epochs (via `StorageEngine::list_epochs`). The CP then updates its internal state to match reality. This reconciliation
should be part of the registration handshake for the new session.

In summary, error handling never blocks normal processing: incoming dispatch errors are logged and skipped, outgoing
load failures are reported back to the CP as `FAILED` status, and session-level failures terminate the session cleanly.

### 6.1. Error Classification

Errors are classified into distinct categories with different handling strategies:

| Error Category           | Example                                                               | Handling                                                                                                                  |
|--------------------------|-----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| **Connection Error**     | Can't reach master, DNS failure, TLS handshake                        | Session terminates with `SessionError::ConnectionFailed`. Worker runtime decides next step.                               |
| **Registration Error**   | Master rejects the worker (e.g., unknown tenant)                      | Session terminates with `SessionError::RegistrationRejected`. No retry — this is a configuration error.                   |
| **Stream Closure**       | Master closed the stream, network partition, HTTP/2 GOAWAY/RST_STREAM | Session terminates with `SessionError::StreamClosed`. The CP (or a new leader) will re-establish contact.                 |
| **Local Channel Full**   | The outgoing `mpsc` channel is at capacity                            | `send_status_update()` returns `SessionError::BackpressureFull`. The caller can decide to drop, buffer locally, or retry. |
| **Local Channel Closed** | The Send Task has exited (session is dead)                            | `send_status_update()` returns `SessionError::SessionClosed`. The caller knows the session is no longer active.           |

### 6.2. Why No Retry on Stream Write Failure

In HTTP/2 bidirectional streaming (which gRPC uses), a write failure means the underlying stream is broken:

- `RST_STREAM` — peer reset the stream
- `GOAWAY` — peer is shutting down
- TCP disconnect — network partition

None of these are transient at the *stream level*. You cannot retry a write on the same `tonic::Streaming<T>` — you need
a new stream (which means a new `CoordinateWorker` call and re-registration). Since the design states that the CP is
responsible for reconnecting to the worker via a new leader, **the session simply terminates on any stream write failure
**.

### 6.3. Retry at Session Establishment

The only place exponential backoff retry makes sense is during **initial connection and registration** (before the
stream is active). If `tonic::transport::Channel::connect()` fails or the registration handshake times out,
`ControlPlaneSession::start()` can retry with backoff before returning an error to the caller.

### 6.4. Error Type Definition

```rust
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("failed to connect to control plane at {addr}: {source}")]
    ConnectionFailed {
        addr: String,
        #[source]
        source: tonic::transport::Error,
    },

    #[error("registration rejected by control plane: {reason}")]
    RegistrationRejected { reason: String },

    #[error("control plane stream closed: {reason}")]
    StreamClosed { reason: String },

    #[error("outgoing event channel is full (capacity: {capacity})")]
    BackpressureFull { capacity: usize },

    #[error("session is closed, cannot send events")]
    SessionClosed,

    #[error("registration handshake timed out after {timeout:?}")]
    HandshakeTimeout { timeout: std::time::Duration },
}
```

## 7. gRPC Transport Configuration

The `CoordinateWorker` bidirectional stream is the sole network interface for `worker-cp`. Its `tonic` transport
configuration is critical for connection health detection, timeout handling, and message size safety. All values below
are defaults that should be overridable via `ControlPlaneConfig`.

### 7.1. Connection Management

```rust
tonic::transport::Channel::from_shared(master_addr) ?
// TCP connect timeout — fail fast if the master is unreachable.
.connect_timeout(Duration::from_secs(5))
// Total request timeout — does NOT apply to streaming RPCs.
// For CoordinateWorker (a long-lived stream), this should be None / omitted.
// Timeout is managed per-phase: registration handshake has its own timeout (§7.3).
// .timeout(...)  — intentionally omitted for streaming
.connect()
.await?;
```

**Key point**: `tonic::Channel::timeout()` sets a per-request deadline, which is unsuitable for a long-lived
bidirectional stream. The stream itself has no timeout — liveness is managed by HTTP/2 keep-alive and application-level
heartbeats.

### 7.2. HTTP/2 Keep-Alive

HTTP/2 keep-alive pings are the primary mechanism for detecting dead connections (network partition, silent peer crash,
load balancer idle timeout).

```rust
tonic::transport::Channel::from_shared(master_addr) ?
// Send a keep-alive ping every 15 seconds if the connection is idle.
.http2_keep_alive_interval(Duration::from_secs(15))
// If no ping response within 5 seconds, consider the connection dead.
.keep_alive_timeout(Duration::from_secs(5))
// Send keep-alive even if no active streams (important for detecting
// network partitions during idle periods between events).
.keep_alive_while_idle(true)
.connect()
.await?;
```

| Parameter                   | Default | Rationale                                                                                                                                                                             |
|-----------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `http2_keep_alive_interval` | 15s     | Frequent enough to detect dead connections within ~20s. Not so frequent as to waste bandwidth on a control plane channel.                                                             |
| `keep_alive_timeout`        | 5s      | If the peer is alive, it should respond to pings quickly. 5s accounts for network jitter.                                                                                             |
| `keep_alive_while_idle`     | `true`  | The control plane stream may be idle for extended periods (no assignments, heartbeats handled separately). Without this, a load balancer could silently drop the idle TCP connection. |

### 7.3. Timeout Strategy

Since the stream is long-lived, timeouts are applied per-phase rather than per-connection:

| Phase                     | Timeout                      | Mechanism                                                                                           |
|---------------------------|------------------------------|-----------------------------------------------------------------------------------------------------|
| TCP connect               | 5s                           | `Channel::connect_timeout()`                                                                        |
| Registration handshake    | 10s                          | `tokio::time::timeout()` wrapping the `RegisterEvent` → `CommonAckEvent(registration_ack)` exchange |
| Heartbeat liveness        | Configured by CP (e.g., 30s) | CP monitors heartbeat arrivals. Worker sends heartbeats at `heartbeat_interval` (default 10s).      |
| Dead connection detection | ~20s                         | HTTP/2 keep-alive (15s interval + 5s timeout)                                                       |
| Data load assignment      | No timeout on worker side    | CP sets its own assignment deadline. Worker reports `COMPLETED` or `FAILED` as fast as it can.      |

### 7.4. Message Size Limits

Control plane messages are small (metadata only, no data payloads), but `LoadPlan.arrow_schema` is a serialized Arrow
schema in bytes which could be non-trivial for wide tables.

```rust
tonic::transport::Channel::from_shared(master_addr) ?
// Default is 4MB, which is sufficient for control plane events.
// Arrow schemas for tables with hundreds of columns fit within ~100KB.
// No need to increase beyond the default.
// .max_decoding_message_size(4 * 1024 * 1024)  // 4MB default, kept as-is
// .max_encoding_message_size(4 * 1024 * 1024)   // 4MB default, kept as-is
.connect()
.await?;
```

| Parameter                   | Default             | Rationale                                                                                                                                                                 |
|-----------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `max_decoding_message_size` | 4MB (tonic default) | Sufficient. Largest expected message is `DataAssignmentEvent` with `LoadPlan` containing an Arrow schema. Even wide tables (500+ columns) produce schemas well under 1MB. |
| `max_encoding_message_size` | 4MB (tonic default) | Sufficient. Largest outgoing message is `DataPullStatusUpdateEvent` which is a few hundred bytes.                                                                         |

**If the default is exceeded**, tonic returns a `Status::resource_exhausted` error, which the Receive Task treats as a
malformed event (log and skip), or the Send Task treats as a fatal stream error (terminate session).

### 7.5. Retry at Session Establishment

Exponential backoff is applied only at session establishment (before the stream is active). Once the stream is open,
write failures are fatal (see §6.2).

```rust
// Pseudocode for ControlPlaneSession::start()
let channel = backoff::retry(ExponentialBackoff::default (), | | async {
Channel::from_shared(config.master_addr.clone()) ?
.connect_timeout(Duration::from_secs(5))
.http2_keep_alive_interval(Duration::from_secs(15))
.keep_alive_timeout(Duration::from_secs(5))
.keep_alive_while_idle(true)
.connect()
.await
.map_err(backoff::Error::transient)
}).await?;
```

| Parameter        | Default | Rationale                                                                                    |
|------------------|---------|----------------------------------------------------------------------------------------------|
| Initial interval | 500ms   | Fast first retry in case of a transient network glitch.                                      |
| Max interval     | 30s     | Avoid thundering herd; cap the backoff ceiling.                                              |
| Max elapsed time | 5min    | If the master is down for >5 minutes, give up and let the worker runtime handle the failure. |
| Multiplier       | 2.0     | Standard doubling.                                                                           |

### 7.6. Configuration Struct

All gRPC transport parameters are consolidated in `ControlPlaneConfig`:

```rust
pub struct ControlPlaneConfig {
    pub master_addr: String,
    pub worker_id: String,
    pub tenant_id: String,

    // Session behavior
    pub heartbeat_interval: Duration,           // default: 10s
    pub registration_timeout: Duration,         // default: 10s
    pub outgoing_channel_capacity: usize,       // default: 64

    // Transport
    pub connect_timeout: Duration,              // default: 5s
    pub http2_keep_alive_interval: Duration,    // default: 15s
    pub keep_alive_timeout: Duration,           // default: 5s

    // Retry (session establishment only)
    pub retry_initial_interval: Duration,       // default: 500ms
    pub retry_max_interval: Duration,           // default: 30s
    pub retry_max_elapsed_time: Duration,       // default: 5min
}
```

## 8. Performance Considerations

- **Non-blocking I/O**: The gRPC tasks (Send/Receive) must never block the tokio runtime. All heavy lifting (data
  loading, DuckDB interactions) must be offloaded to separate tasks.
- **`spawn_blocking` for DuckDB**: `DataLoader::load_epoch` ultimately calls into embedded DuckDB, which performs
  blocking I/O and CPU-bound computation. The dispatcher implementation should use `tokio::task::spawn_blocking` (or run
  DuckDB work on a dedicated thread pool) to avoid starving the async runtime.
- **Backpressure**: Use a bounded `mpsc` channel for outgoing messages. The capacity should be modest (e.g.,
  `mpsc::channel(64)`) given the low throughput of control plane events. The `send_status_update` method should use
  `try_send` (returning `BackpressureFull`) rather than blocking `send`, so the caller retains control.
- **Data Loading Concurrency Limit**: The `TaskDispatcher` implementation should use a `tokio::sync::Semaphore` to cap *
  *concurrent `DataLoader::load_epoch` operations** (e.g., 4–8 concurrent loads). This is distinct from event dispatch
  throughput — the Receive Task always dispatches events immediately (spawn and return), but each spawned task must
  `semaphore.acquire()` before calling `load_epoch`. Without this, a burst of `DataAssignmentEvent`s from the Master
  could start dozens of concurrent loads, each pulling data from StarRocks/Iceberg and writing into DuckDB, exhausting
  memory. Note: the bounded `mpsc` channel (capacity 64) only limits the **outgoing** status-update path; it has no
  effect on how many incoming assignments are being actively loaded.

## 8. Best Rust Implementation Practices

Following the project's design principles (Effective Rust & A Philosophy of Software Design):

- **Type Safety (Newtypes)**: Reuse `WorkerId` from `worker-storage/src/ldp/types.rs`. Introduce `TenantId` and
  `DatasetId` as newtypes in a follow-up refactor (they are currently raw `String` across the entire codebase).
- **Proto Boundary as Conversion Firewall**: All proto ↔ domain conversions happen in `proto_convert.rs`. The
  `TaskDispatcher` trait and `ControlPlaneSession` public API never expose proto-generated types.
- **Error Handling**: Use `thiserror` with structured `SessionError` variants (see §6.4). No stringly-typed errors.
- **Graceful Shutdown**: Use `tokio_util::sync::CancellationToken` shared between the Send Task and Receive Task. When
  the worker runtime shuts down, it cancels the token. The Send Task drains any pending messages from the channel before
  closing the stream.
- **Session Status Observability**: Use `tokio::sync::watch` to broadcast `SessionStatus` (e.g., `Registering`,
  `Active`, `Disconnected(SessionError)`) to the worker runtime. This avoids polling or callback patterns.
- **Observability**: Integrate `tracing`. Log every incoming and outgoing `EventStreamMessage` at `debug`/`trace` level,
  including the `event_id` for distributed tracing correlation.

## 9. `SessionStatus` State Machine

```text
  ┌──────────────┐     RegisterEvent sent      ┌──────────────┐
  │ Connecting   │ ──────────────────────────>  │ Registering  │
  └──────────────┘                              └──────────────┘
         |                                             |
         | connect() fails                             | CommonAckEvent(registration_ack)
         v                                             v
  ┌──────────────┐                              ┌──────────────┐
  │ Disconnected │ <─── stream error ────────── │   Active     │
  │  (error)     │                              │              │
  └──────────────┘                              └──────────────┘
```

```rust
pub enum SessionStatus {
    Connecting,
    Registering,
    Active,
    Disconnected(SessionError),
}
```

## 10. Interface Design (Draft)

```rust
// worker-cp/src/session.rs

use tokio::sync::{mpsc, watch};
use std::sync::Arc;

pub struct ControlPlaneConfig {
    pub master_addr: String,
    pub worker_id: String,
    pub tenant_id: String,
    pub heartbeat_interval: std::time::Duration,
    pub outgoing_channel_capacity: usize,        // default: 64
    pub registration_timeout: std::time::Duration, // default: 10s
}

pub struct ControlPlaneSession {
    /// Channel to enqueue outgoing events to the Send Task.
    tx: mpsc::Sender<OutgoingPayload>,
    /// Observable session status (Connecting → Registering → Active → Disconnected).
    status_rx: watch::Receiver<SessionStatus>,
}

impl ControlPlaneSession {
    /// Connects to the Master, registers the worker, and starts background tasks.
    /// Retries the initial connection with exponential backoff.
    /// Returns an error if registration is rejected or times out after all retries.
    pub async fn start(
        config: ControlPlaneConfig,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Result<Self, SessionError> {
        // 1. Connect to master via tonic (with backoff retry)
        // 2. Open CoordinateWorker bidirectional stream
        // 3. Send RegisterEvent, await CommonAckEvent(registration_ack)
        // 4. Spawn Send Task and Receive Task with shared CancellationToken
        // 5. Return session handle
        todo!()
    }

    /// Attempts to enqueue a status update. Non-blocking.
    /// Returns BackpressureFull if the channel is at capacity.
    /// Returns SessionClosed if the session has terminated.
    pub fn try_send_status_update(
        &self,
        dataset_id: String,
        epoch_id: String,
        status: LoadStatus,
        error_message: Option<String>,
    ) -> Result<(), SessionError> {
        todo!()
    }

    /// Returns a clone of the status receiver for observing session health.
    pub fn status(&self) -> watch::Receiver<SessionStatus> {
        self.status_rx.clone()
    }
}

/// Domain types for outgoing events (not proto types).
pub enum OutgoingPayload {
    StatusUpdate { dataset_id: String, epoch_id: String, status: LoadStatus, error: Option<String> },
    Heartbeat,
}

pub enum LoadStatus { InProgress, Completed, Failed }

/// Trait implemented by the worker runtime to handle incoming CP events.
/// All methods receive domain types, never proto types.
#[async_trait::async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Called when a new data assignment is received.
    /// MUST NOT block. Should spawn a bounded task to handle the load.
    async fn dispatch_assignment(&self, assignment: LoadAssignment);

    /// Called when a control command is received (e.g., evict_epoch, drain).
    async fn handle_command(&self, command: ControlCommand);
}
```