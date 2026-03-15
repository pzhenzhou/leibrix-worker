//! [`TaskDispatcher`] trait — the integration boundary between `worker-cp`
//! and the rest of the worker (storage, loader, LDP).
//!
//! # Contract
//!
//! `handle_command` **must return promptly** for all variants.  In particular,
//! [`ControlCommand::Assignment`] must spawn a background task rather than
//! awaiting the full load inline, since the Receive Task calls this method
//! sequentially and a slow call would stall all subsequent inbound events.
//!
//! # Concurrency guard
//!
//! Implementations are responsible for limiting the number of concurrent
//! `load_epoch` operations.  The recommended approach (documented in
//! `docs/worker_cp_design.md §8`) is to guard entry to the actual load with a
//! `tokio::sync::Semaphore`, allowing the Receive Task to return immediately
//! while the semaphore keeps memory / CPU pressure bounded.
//!
//! # Async style
//!
//! This trait uses `-> impl Future<Output = …> + Send` (RPITIT), the same
//! style as `StorageEngine` and `QueryEngine` in `worker-storage`.  This
//! avoids the `Box<dyn Future>` heap allocation that `#[async_trait]` incurs
//! on every call.  Call sites hold an `Arc<D>` where `D: TaskDispatcher`
//! rather than `Arc<dyn TaskDispatcher>`, so the trait is deliberately
//! not object-safe.

use crate::types::{ControlCommand, EpochKey};
use std::future::Future;

/// Single-method dispatcher consumed by the Receive Task.
///
/// All inbound Control Plane events arrive as a [`ControlCommand`] variant:
///
/// | Proto payload        | Variant                        |
/// |----------------------|--------------------------------|
/// | `DataAssignmentEvent`| [`ControlCommand::Assignment`] |
/// | `CommonAckEvent`     | `EvictEpoch` / `Drain` / …     |
///
/// Keeping a single entry point means implementations decide internally how
/// urgency, concurrency, and error handling differ across variants — the
/// framework imposes no policy.
///
/// # Async style
///
/// Uses `-> impl Future<Output = …> + Send` (RPITIT), consistent with
/// [`crate::StorageEngine`] / [`crate::QueryEngine`].  Call-sites use
/// `Arc<D>` where `D: TaskDispatcher` to avoid `dyn Trait` boxing.
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a command from the Control Plane.
    ///
    /// **MUST return promptly** for every variant — spawn background tasks
    /// for anything that involves I/O or long computation.
    fn handle_command(&self, command: ControlCommand) -> impl Future<Output = ()> + Send;

    /// Returns a list of all epochs currently loaded and queryable.
    ///
    /// Used for reconnect reconciliation: after re-establishing a session with
    /// the control plane, the worker reports all loaded epochs so the CP can
    /// update its state to match reality.
    fn list_loaded_epochs(&self) -> Vec<EpochKey>;
}
