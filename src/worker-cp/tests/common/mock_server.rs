//! Mock Control Plane gRPC server for integration tests.
//!
//! # Usage
//!
//! ```ignore
//! let mock = MockCpServer::start().await;
//! // mock.addr() gives the endpoint URL for ControlPlaneConfig
//! // mock.push_response(msg) sends a message to the connected worker
//! // mock.next_request().await receives the next worker message
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

use worker_proto::control_plane::{
    control_plane_service_server::{ControlPlaneService, ControlPlaneServiceServer},
    event_stream_message::Payload,
    CommonAckEvent, EventStreamMessage,
};

/// A mock CP server that captures all inbound messages and lets tests inject
/// outbound messages.
pub struct MockCpServer {
    /// The address the server is listening on.
    addr: SocketAddr,
    /// Send outbound messages (CP → worker) on this channel.
    response_tx: mpsc::Sender<EventStreamMessage>,
    /// Receive inbound messages (worker → CP) from this channel.
    request_rx: mpsc::Receiver<EventStreamMessage>,
    /// Server task handle.
    _server_handle: tokio::task::JoinHandle<()>,
}

impl MockCpServer {
    /// Start a mock gRPC server on an ephemeral port.
    ///
    /// The server is configured to:
    /// 1. Accept a single `CoordinateWorker` stream.
    /// 2. Forward every inbound worker message to `request_rx`.
    /// 3. Send every message pushed via `response_tx` back to the worker.
    pub async fn start() -> Self {
        // Channels for test ↔ gRPC service communication.
        // response: test pushes messages that the mock sends to the worker.
        let (response_tx, response_rx) = mpsc::channel::<EventStreamMessage>(64);
        // request: the mock forwards worker messages to the test.
        let (request_tx, request_rx) = mpsc::channel::<EventStreamMessage>(64);

        let svc = MockCpService {
            response_rx: Arc::new(tokio::sync::Mutex::new(Some(response_rx))),
            request_tx,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");

        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ControlPlaneServiceServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("mock server crashed");
        });

        // Give the server a moment to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Self {
            addr,
            response_tx,
            request_rx,
            _server_handle: server_handle,
        }
    }

    /// The endpoint URL to use in `ControlPlaneConfig::master_addr`.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Push a message from the mock CP to the worker.
    pub async fn push_response(&self, msg: EventStreamMessage) {
        self.response_tx.send(msg).await.expect("push_response");
    }

    /// Receive the next message sent by the worker.
    pub async fn next_request(&mut self) -> EventStreamMessage {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.request_rx.recv())
            .await
            .expect("timed out waiting for worker request")
            .expect("request channel closed")
    }

    /// Try to receive the next message, returning `None` if nothing arrived
    /// within `timeout`.
    pub async fn try_next_request(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<EventStreamMessage> {
        tokio::time::timeout(timeout, self.request_rx.recv())
            .await
            .ok()
            .flatten()
    }

    /// Build a registration-ack response message.
    pub fn registration_ack() -> EventStreamMessage {
        EventStreamMessage {
            event_id: "ack-reg-1".into(),
            tenant_id: String::new(),
            worker_id: String::new(),
            payload: Some(Payload::CommonAck(CommonAckEvent {
                server_id: "mock-master".into(),
                event_type: "registration_ack".into(),
                payload: None,
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation
// ---------------------------------------------------------------------------

struct MockCpService {
    /// The test-controlled outbound channel.  Wrapped in Mutex<Option<>> so we
    /// can take it once when the stream is opened (only one stream per test).
    response_rx: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<EventStreamMessage>>>>,
    /// Forward worker messages to the test.
    request_tx: mpsc::Sender<EventStreamMessage>,
}

#[async_trait]
impl ControlPlaneService for MockCpService {
    type CoordinateWorkerStream = ReceiverStream<Result<EventStreamMessage, Status>>;

    async fn coordinate_worker(
        &self,
        request: Request<tonic::Streaming<EventStreamMessage>>,
    ) -> Result<Response<Self::CoordinateWorkerStream>, Status> {
        let mut inbound = request.into_inner();
        let request_tx = self.request_tx.clone();

        // Take the response_rx — only one call expected per test.
        let mut response_rx = self
            .response_rx
            .lock()
            .await
            .take()
            .expect("CoordinateWorker called more than once on mock server");

        // Channel that carries the outbound gRPC stream messages.
        let (out_tx, out_rx) = mpsc::channel::<Result<EventStreamMessage, Status>>(64);

        // Inbound forwarding task: worker → test
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                if request_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Outbound forwarding task: test → worker
        tokio::spawn(async move {
            while let Some(msg) = response_rx.recv().await {
                if out_tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
            // When response_tx is dropped by the test (or explicitly), the
            // stream closes, causing the worker's receive task to detect a
            // disconnect.
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}
