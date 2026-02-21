//! Worker Control Plane library.
//!
//! Re-exports proto types from worker-proto for backward compatibility.

//! `worker-cp` — Control Plane communication crate.
//!
//! Exposes:
//! - [`ControlPlaneSession`]: the live session handle (connect, register,
//!   dispatch events).
//! - [`ControlPlaneConfig`]: all knobs for connecting to and communicating with
//!   the Control Plane master.
//! - [`TaskDispatcher`]: the trait the rest of the worker implements to receive
//!   epoch assignments and lifecycle commands.
//! - Domain types ([`LoadAssignment`], [`OutgoingPayload`], …) used across the
//!   boundary.
//!
//! All `prost`-generated proto types are hidden inside [`proto_convert`] and
//! never exposed to callers.

pub mod config;
pub mod dispatch;
pub mod error;
pub mod runtime_dispatcher;
pub mod session;
pub mod types;

pub(crate) mod proto_convert;

pub use config::ControlPlaneConfig;
pub use dispatch::TaskDispatcher;
pub use error::SessionError;
pub use runtime_dispatcher::WorkerRuntimeDispatcher;
pub use session::ControlPlaneSession;
pub use types::{
    CatalogInfo, ControlCommand, DataSourceInfo, EpochKey, EpochPhase, LoadAssignment, LoadStatus,
    OutgoingPayload, PoolOptions, SessionStatus, TimeRange,
};
