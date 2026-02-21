//! Domain types for the worker–control-plane protocol.
//!
//! These types are the **only** representation of protocol concepts visible
//! outside `worker-cp`. No `prost`-generated structures cross this boundary.
//! All conversions happen inside [`crate::proto_convert`].

use crate::error::SessionError;
use std::collections::HashMap;
use std::sync::Arc;

/// A half-open time interval `[start_inclusive_ns, end_exclusive_ns)` in
/// nanoseconds since the Unix epoch.
///
/// The nano-second precision preserves the full fidelity of
/// `google.protobuf.Timestamp`. Use [`Self::start_secs`] / [`Self::end_secs`]
/// for coarser comparisons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRange {
    pub start_inclusive_ns: i64,
    pub end_exclusive_ns: i64,
}

impl TimeRange {
    /// Start boundary expressed in whole seconds since the Unix epoch.
    #[inline]
    pub fn start_secs(&self) -> i64 {
        self.start_inclusive_ns / 1_000_000_000
    }

    /// End boundary (exclusive) expressed in whole seconds since the Unix epoch.
    #[inline]
    pub fn end_secs(&self) -> i64 {
        self.end_exclusive_ns / 1_000_000_000
    }
}

/// Worker-side mirror of `common.ConnectionPoolOptions`.
#[derive(Debug, Clone)]
pub struct PoolOptions {
    pub min_connections: u32,
    pub max_connections: u32,
    /// Timeout for establishing a new connection, in milliseconds.
    pub connect_timeout_ms: u64,
    /// Maximum idle duration for a connection before it is closed, in milliseconds.
    pub idle_timeout_ms: u64,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 16,
            connect_timeout_ms: 5_000,
            idle_timeout_ms: 600_000,
        }
    }
}

/// Identifies the upstream catalog type and how to connect to it.
///
/// Mirrors `common.Catalog` but in idiomatic Rust without proto baggage.
#[derive(Debug, Clone)]
pub enum CatalogInfo {
    Iceberg {
        uri: String,
    },
    StarRocks {
        catalog_name: String,
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
        max_concurrency: Option<u32>,
        pool_options: Option<PoolOptions>,
    },
    Jdbc {
        uri: String,
        driver: String,
        pool_options: Option<PoolOptions>,
    },
}

/// Fully qualified upstream table location.
///
/// Mirrors `common.DataSource` without the optional `partition` / `filter`
/// fields, which are not relevant at assignment time.
#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    pub catalog: CatalogInfo,
    pub database: String,
    pub table: String,
    pub version: Option<String>,
}

/// Everything required to load a single epoch of data.
///
/// This is the domain representation of a `DataAssignmentEvent`. Phase 4 will
/// add a `fn into_load_request(self, ...) -> LoadRequest` converter.
#[derive(Debug, Clone)]
pub struct LoadAssignment {
    /// Logical dataset identifier (e.g., `"sales"`, `"orders"`).
    pub dataset_id: String,

    /// Epoch identifier within the dataset.
    pub epoch_id: String,

    /// Opaque plan identifier assigned by the Control Plane.
    pub plan_id: String,

    /// The DuckDB table name the worker should materialise the epoch into.
    pub destination_table_name: String,

    /// Description of where to pull the data from.
    pub source: DataSourceInfo,

    /// Arrow IPC-encoded schema bytes. Use `arrow_schema::Schema::try_from(&schema_ref)`.
    pub arrow_schema_bytes: Vec<u8>,

    /// Temporal filter for this epoch (if the source supports predicate pushdown).
    pub time_range: Option<TimeRange>,

    /// The partition-key time column name (e.g., `"dt"`).
    pub time_column_name: String,

    /// A string-encoded partition value that identifies this epoch inside the
    /// epoch registry (e.g., `"2025-01"`).
    pub time_partition_value: String,

    /// Additional dimension values from `ResolvedPartition.dimension_values`.
    pub dimension_values: HashMap<String, String>,
}

/// Lifecycle phase of an epoch tracked by the dispatcher.
///
/// Transitions:
/// ```text
/// Loading ──► Active ──► Evicting ──► (removed from map)
/// ```
///
/// The phase prevents races between a concurrent load and an eviction
/// command that arrives before the load finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochPhase {
    /// Data is being ingested from the upstream source.
    Loading,
    /// Data is fully loaded and queryable.
    Active,
    /// The CP requested eviction; waiting for in-flight operations to drain
    /// before the table is dropped.
    Evicting,
}

/// Composite key for the per-epoch phase map.
///
/// Uses `(dataset_id, epoch_id)` as the DashMap key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EpochKey {
    pub dataset_id: String,
    pub epoch_id: String,
}

impl EpochKey {
    pub fn new(dataset_id: impl Into<String>, epoch_id: impl Into<String>) -> Self {
        Self {
            dataset_id: dataset_id.into(),
            epoch_id: epoch_id.into(),
        }
    }
}

impl std::fmt::Display for EpochKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset_id, self.epoch_id)
    }
}

/// Commands sent *by the Control Plane* to the worker, or synthesised
/// internally from a `DataAssignmentEvent`.
///
/// All inbound CP events flow through [`crate::dispatch::TaskDispatcher::handle_command`]
/// via this enum, giving implementors a single dispatch surface.
#[derive(Debug, Clone)]
pub enum ControlCommand {
    /// Load a new epoch of data into the in-memory storage engine.
    ///
    /// Converted from a `DataAssignmentEvent` by the Receive Task.
    Assignment(Box<LoadAssignment>),
    /// Evict a previously loaded epoch from memory.
    EvictEpoch {
        dataset_id: String,
        epoch_id: String,
        reason: String,
    },
    /// Gracefully drain pending load operations before shutdown.
    Drain,
    /// An unrecognised `event_type`. Logged; otherwise ignored.
    Unknown { event_type: String },
}

/// Status emitted by the worker when a load operation finishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStatus {
    InProgress,
    Completed,
    Failed,
}

/// Payload variants the worker may send upstream.
#[derive(Debug, Clone)]
pub enum OutgoingPayload {
    /// Report the outcome of a `LoadAssignment`.
    StatusUpdate {
        dataset_id: String,
        epoch_id: String,
        status: LoadStatus,
        /// Human-readable error detail, present only when `status == Failed`.
        error: Option<String>,
    },
    /// Periodic liveness signal.
    Heartbeat,
}

/// Observable state of the [`crate::session::ControlPlaneSession`].
///
/// Consumers receive updates via a `tokio::sync::watch::Receiver<SessionStatus>`.
/// `Arc<SessionError>` is used because `SessionError` is not `Clone` (it may
/// wrap a `tonic::transport::Error`).
#[derive(Debug, Clone)]
pub enum SessionStatus {
    /// Establishing a gRPC connection to the Control Plane.
    Connecting,
    /// Connection established; waiting for the CP to acknowledge registration.
    Registering,
    /// Fully operational: events are flowing in both directions.
    Active,
    /// The session has terminated.
    Disconnected(Arc<SessionError>),
}
