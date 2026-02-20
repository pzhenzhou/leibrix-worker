# Worker Control Plane (worker-cp) Design

## 1. Overview

The `worker-cp` crate is the control plane communication module for the Leibrix Worker. It is responsible for managing the bidirectional gRPC communication between the Worker and the Control Plane (Master). It acts as the control interface for the worker, handling registration, heartbeats, data assignments, and status reporting.

This document outlines the high-level design, core abstractions, ownership model, and performance considerations for implementing `worker-cp` in Rust.

## 2. Core Responsibilities

- **Connection Management**: Establish and maintain a bidirectional gRPC stream (`CoordinateWorker`) with the Master. Handle event sending failures with exponential backoff, and abort the session if the underlying connection is lost.
- **Lifecycle Management**: Handle worker registration (`RegisterEvent`) upon startup and periodic health reporting (`HeartbeatEvent`).
- **Task Dispatching**: Receive `DataAssignmentEvent` from the Master and dispatch them to the `worker-storage` data loader without blocking the control stream.
- **Status Reporting**: Report the progress and outcome of data loading tasks via `DataPullStatusUpdateEvent` (e.g., `IN_PROGRESS`, `COMPLETED`, `FAILED`).
- **Command Handling**: Process `CommonAckEvent` for control commands like `evict_epoch` or `drain_command`.

## 3. Core Abstractions & Architecture

The architecture is designed around asynchronous message passing to ensure the gRPC stream remains highly responsive.

### 3.1. `ControlPlaneSession`
The main entry point and public API for the `worker-cp` crate. Because the gRPC stream is bidirectional and symmetric (the worker both receives active notifications and reports status), "Session" is a more accurate semantic name than "Client". It acts autonomously to maintain the session, send heartbeats, and dispatch incoming events.
- **Ownership**: Owned by the main worker runtime (e.g., in `worker-cli`).
- **Responsibilities**:
  - Initialize the gRPC channel using `tonic` (acting as the network-level client to dial the Master).
  - Perform the initial handshake (sending `RegisterEvent` and waiting for `CommonAckEvent` with `registration_ack`).
  - Spawn the background `EventLoop`.
  - Provide a thread-safe handle (via `mpsc::Sender`) to enqueue outgoing events to the Master.

### 3.2. `EventLoop` (The Reactor)
A background asynchronous task that manages the active bidirectional stream.
- **Ownership**: Runs independently in a `tokio::spawn` task.
- **Responsibilities**:
  - **Multiplexing (Send Task)**: Read outgoing events from an internal `mpsc::Receiver` and write them to the gRPC `RequestStream`.
  - **Demultiplexing (Receive Task)**: Read incoming events from the gRPC `ResponseStream` and route them to the appropriate handlers.
  - **Event Sending Resilience**: If sending an event fails (e.g., transient stream buffer issues), the `Send Task` retries using an exponential backoff algorithm before giving up. Crucially, this retry logic is isolated to the `Send Task` and does **not block** the `Receive Task`.
  - **Connection Loss Handling**: If the bidirectional stream is lost or closed by the server, the `EventLoop` should terminate and return an error directly to the worker runtime. It does not attempt to reconnect, as the CP (or a new leader) manages worker status and the worker runtime will handle the lifecycle.

### 3.3. `TaskDispatcher` Trait
An abstraction to decouple the gRPC event loop from the actual data loading work (`worker-storage`).
- **Ownership**: Implemented by the worker runtime and passed to `worker-cp` as an `Arc<dyn TaskDispatcher>`.
- **Responsibilities**:
  - Receive `DataAssignmentEvent`.
  - Spawn asynchronous tasks to execute `DataLoader::load_epoch`.
  - Handle `CommonAckEvent` commands (e.g., triggering cache eviction).

### 3.4. `HeartbeatManager`
A dedicated component to ensure timely heartbeats.
- **Ownership**: Can be integrated directly into the `EventLoop` using `tokio::select!` with a `tokio::time::interval`.
- **Responsibilities**: Periodically inject `HeartbeatEvent` into the outgoing message queue.

## 4. Ownership and Concurrency Model

To ensure high performance and avoid blocking the gRPC stream, `worker-cp` uses a message-passing architecture based on `tokio::sync::mpsc`. To handle the requirement that **event sending failures must not block the processing of incoming events**, the `EventLoop` explicitly splits the bidirectional stream into two independent asynchronous tasks.

```text
                                  [Master (Control Plane)]
                                        ^      |
                                        |      | gRPC Bidirectional Stream
                                        |      v
┌───────────────────────────────────────|──────|───────────────────────────────────────┐
│ EventLoop (Reactor)                   |      |                                       │
│                                       |      |                                       │
│  ┌──────────────────────────────┐     |      |    ┌───────────────────────────────┐  │
│  │       Send Task              │<────┘      └───>│        Receive Task           │  │
│  │ (Handles outgoing events &   │                 │ (Handles incoming events &    │  │
│  │  exponential backoff retries)│                 │  dispatches them immediately) │  │
│  └──────────────────────────────┘                 └───────────────────────────────┘  │
│                 ^                                                 |                  │
│                 | (mpsc::Receiver)                                |                  │
└─────────────────|─────────────────────────────────────────────────|──────────────────┘
                  |                                                 |
                  | Outgoing Events (Status, Heartbeat)             | Incoming Events (Assignments)
                  |                                                 v
           [HeartbeatManager]                             [TaskDispatcher]
                  |                                                 |
                  |                                           Spawn tokio task
                  |                                                 |
                  |                                                 v
                  └─────────────────────────────────────── [DataLoader Task]
                                                        (calls worker-storage)
```

1. **Split Stream Architecture**: The `EventLoop` does not use a single `tokio::select!` loop. Instead, it splits the gRPC stream into a `RequestStream` (write half) and a `ResponseStream` (read half). It then spawns two separate `tokio` tasks:
   - **Receive Task**: Continuously awaits messages from the `ResponseStream`. When an event (e.g., `DataAssignmentEvent`) arrives, it immediately calls the `TaskDispatcher`. This task never blocks on network sends.
   - **Send Task**: Continuously reads from the internal `mpsc::Receiver` (which collects events from the `DataLoader` and `HeartbeatManager`). It attempts to write these events to the `RequestStream`.
2. **Error Handling & Retries (Send Task)**: If the `Send Task` encounters a transient error while writing to the gRPC stream, it enters an exponential backoff loop *internally within the Send Task*. Because the `Receive Task` is running concurrently in a separate `tokio` task, the worker continues to receive and process new assignments from the Master without interruption.
3. **TaskDispatcher**: When the `Receive Task` gets a `DataAssignmentEvent`, it calls the `TaskDispatcher`. The dispatcher must **not** block. It should immediately spawn a new `tokio` task for the assignment.
4. **Data Loading Task**: The spawned task calls `DataLoader::load_epoch`. Upon completion (success or failure), it uses a cloned `mpsc::Sender` to send a `DataPullStatusUpdateEvent` back to the `Send Task`'s queue.

## 5. Performance Considerations

- **Non-blocking I/O**: The gRPC event loop must never block. All heavy lifting (data loading, DuckDB interactions) must be offloaded to separate `tokio` tasks.
- **Backpressure**: Use bounded `mpsc` channels (e.g., `mpsc::channel(1024)`) for outgoing messages. If the Master is slow to consume, the channel will exert backpressure, preventing unbounded memory growth.
- **Connection Pooling & Keep-Alive**: Configure the `tonic` channel with HTTP/2 keep-alive pings to detect dead connections quickly and prevent load balancers from dropping idle connections.
- **Zero-Copy Deserialization**: Leverage `tonic` and `prost` optimizations. The `EventStreamMessage` uses `oneof`, which is efficient for memory allocation.

## 6. Best Rust Implementation Practices

Following the project's design principles (Effective Rust & A Philosophy of Software Design):

- **Type Safety (Newtypes)**: Use newtypes for IDs (e.g., `WorkerId`, `TenantId`, `DatasetId`, `EpochId`) within the `worker-cp` domain logic, converting to/from raw strings only at the protobuf boundary (`proto_convert.rs`).
- **State Machine**: Represent the connection state explicitly using Rust enums (e.g., `enum ConnectionState { Disconnected, Registering, Active(Stream) }`) to make invalid states unrepresentable.
- **Error Handling**: Use `thiserror` for defining specific control plane errors (`CpError::ConnectionFailed`, `CpError::RegistrationRejected`). Avoid stringly-typed errors.
- **Graceful Shutdown**: Use `tokio_util::sync::CancellationToken`. When the worker shuts down, the token is cancelled. The `EventLoop` should catch this, attempt to send any pending status updates, and cleanly close the gRPC stream.
- **Retry Logic**: Use the `backoff` crate (e.g., `ExponentialBackoff`) specifically for **event sending failures** over the active stream. If the underlying gRPC connection to the CP node is lost, do not spin-loop reconnects; instead, **return an error directly** and let the worker runtime dictate the next steps (e.g., restart or wait for CP leader to re-establish state).
- **Bidirectional Stream Handling**: Since `CoordinateWorker` is a bidirectional stream, the read and write halves must be managed concurrently. A failure in either the receiving or sending direction should gracefully tear down the other half and terminate the session. However, transient send failures (which trigger backoff retries) must be isolated to the `Send Task` so that the `Receive Task` can continue processing incoming events while the send path recovers.
- **Observability**: Integrate `tracing`. Every incoming and outgoing `EventStreamMessage` should be logged at the `debug` or `trace` level, including the `event_id` for distributed tracing correlation.

## 7. Interface Design (Draft)

```rust
// worker-cp/src/client.rs

use tokio::sync::mpsc;
use worker_proto::proto::control_plane::{EventStreamMessage, DataPullStatusUpdateEvent};
use std::sync::Arc;

/// Configuration for the Control Plane connection
pub struct ControlPlaneConfig {
    pub master_addr: String,
    pub worker_id: String, // Should be a Newtype in actual implementation
    pub tenant_id: String, // Should be a Newtype in actual implementation
    pub heartbeat_interval: std::time::Duration,
}

/// The main session handle used by the worker runtime to interact with the Control Plane
pub struct ControlPlaneSession {
    // Channel to send messages to the background event loop
    tx: mpsc::Sender<EventStreamMessage>,
}

impl ControlPlaneSession {
    /// Connects to the Master, registers the worker, and starts the background event loop.
    pub async fn start(
        config: ControlPlaneConfig, 
        dispatcher: Arc<dyn TaskDispatcher>
    ) -> anyhow::Result<Self> {
        // 1. Establish gRPC connection via tonic
        // 2. Send RegisterEvent and await CommonAckEvent
        // 3. Spawn EventLoop task
        // 4. Return session handle
        todo!()
    }

    /// Enqueues a status update to be sent to the Master.
    pub async fn send_status_update(&self, update: DataPullStatusUpdateEvent) -> anyhow::Result<()> {
        // Wrap in EventStreamMessage and send to self.tx
        todo!()
    }
}

/// Trait implemented by the worker runtime to handle incoming Master events
#[async_trait::async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Called when a new data assignment is received.
    /// MUST NOT block. Should spawn a task to handle the load.
    async fn dispatch_assignment(&self, assignment: worker_proto::proto::control_plane::DataAssignmentEvent);
    
    /// Called when a control command is received.
    async fn handle_command(&self, command: worker_proto::proto::control_plane::CommonAckEvent);
}
```

## 8. Integration with `worker-cli`

In `worker-cli/src/main.rs`, the initialization sequence will look like:

1. Initialize `StorageEngine` and `DataLoader`.
2. Create a `WorkerRuntimeDispatcher` that implements `TaskDispatcher`. It holds an `Arc<DataLoader>` and a clone of the `ControlPlaneSession`'s sender (to report status).
3. Call `ControlPlaneSession::start(config, Arc::new(dispatcher))`.
4. The main thread waits on a shutdown signal (Ctrl+C) or a session error.
5. On shutdown, trigger the `CancellationToken` to gracefully stop the `EventLoop` and `DataLoader` tasks.
