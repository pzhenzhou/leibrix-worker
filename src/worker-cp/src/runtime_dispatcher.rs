//! Concrete [`TaskDispatcher`] that bridges `worker-cp` to `worker-storage`.
//!
//! [`WorkerRuntimeDispatcher`] receives domain events from the Receive Task
//! and routes them to the appropriate `worker-storage` component:
//!
//! - [`LoadAssignment`] → semaphore-gated [`DataLoader::load_epoch`]
//! - [`ControlCommand::EvictEpoch`] → [`StorageEngine::drop_epoch_table`]
//! - [`ControlCommand::Drain`] / [`ControlCommand::Unknown`] → log only
//!
//! # Concurrency model
//!
//! `dispatch_assignment` **always returns immediately** — it spawns a
//! `tokio::spawn` task for the actual load.  Inside that task the call must
//! first acquire one permit from a shared [`Semaphore`] before touching
//! DuckDB, thereby capping the number of concurrent in-memory ingestion
//! operations without blocking the Receive Task.
//!
//! # Status reporting
//!
//! `InProgress` is reported before `load_epoch` is called; `Completed` or
//! `Failed` is reported when it finishes.  If the outgoing channel is
//! temporarily full, `try_send` is retried [`STATUS_RETRY_COUNT`] times with
//! [`STATUS_RETRY_DELAY`] between attempts before giving up (see design doc
//! §6.0).

use crate::dispatch::TaskDispatcher;
use crate::types::{
    CatalogInfo, ControlCommand, DataSourceInfo, EpochKey, EpochPhase, LoadAssignment, LoadStatus,
    OutgoingPayload, PoolOptions,
};
use anyhow::Context as _;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use dashmap::DashMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{mpsc, Notify, RwLock, Semaphore};
use tracing::{debug, error, info, instrument, warn};
use worker_storage::engine::storage_engine::{EpochView, StorageEngine};
use worker_storage::loader::types::{Catalog, ConnectionPoolOptions, DataSource, LoadRequest};
use worker_storage::loader::DataLoader;
use worker_storage::sql::RegisteredDataset;
use worker_storage::sql::SqlTransformer;

/// How many times to retry `try_send` before dropping a status update.
const STATUS_RETRY_COUNT: usize = 3;

/// Pause between `try_send` retries when the outgoing channel is full.
const STATUS_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

/// Bridges control-plane events to the storage layer.
///
/// # Construction
///
/// ```ignore
/// let dispatcher = WorkerRuntimeDispatcher::new(
///     data_loader,
///     storage_engine,
///     /* max_concurrent_loads = */ 4,
///     sql_transformer,
/// );
/// ```
pub struct WorkerRuntimeDispatcher<E: StorageEngine> {
    data_loader: Arc<DataLoader<E>>,
    /// Held separately so `handle_command` can call `drop_epoch_table` without
    /// going through DataLoader internals.
    storage_engine: Arc<E>,
    /// Limits concurrent `load_epoch` calls.
    semaphore: Arc<Semaphore>,
    /// Per-epoch lifecycle phase.  Used for two-phase eviction:
    /// if an eviction arrives while a load is in-flight, the phase is set to
    /// `Evicting` and the load task itself drops the table when it finishes.
    epoch_phases: Arc<DashMap<EpochKey, EpochPhase>>,
    /// SQL transformer for dataset registration after successful epoch loads.
    /// When a new dataset is loaded for the first time, the dispatcher
    /// registers it with the transformer so queries can reference it.
    sql_transformer: Arc<RwLock<SqlTransformer>>,
    /// Channel back to the Send Task for status updates.
    ///
    /// Initialised via [`Self::init_sender`] after the session is started.  The
    /// split is necessary because the dispatcher is constructed before the
    /// session (which internally creates the outgoing channel), yet the session
    /// needs the dispatcher to handle inbound events.
    ///
    /// Stored behind `Arc` so that `handle_command` can clone the `Arc` and
    /// call `.get()` **lazily** inside the spawned load task — by which time
    /// `init_sender` is guaranteed to have run.  Capturing `.get().cloned()`
    /// eagerly at command-entry time would race against `init_sender` and
    /// could yield `None` for assignments that arrive immediately after
    /// registration.
    outgoing_tx: Arc<OnceLock<mpsc::Sender<OutgoingPayload>>>,
    /// When true, new assignments are rejected (drain mode is active).
    draining: Arc<AtomicBool>,
    /// Notified each time a load task completes (used by `wait_for_drain`).
    drain_notify: Arc<Notify>,
}

impl<E: StorageEngine + 'static> WorkerRuntimeDispatcher<E> {
    /// Create a new dispatcher.
    ///
    /// `max_concurrent_loads` controls the `Semaphore` capacity — at most
    /// that many `DataLoader::load_epoch` calls will execute simultaneously.
    /// A value of `4` is a reasonable default for most configurations.
    ///
    /// Call [`Self::init_sender`] immediately after
    /// [`ControlPlaneSession::start`] returns to wire up status reporting.
    pub fn new(
        data_loader: Arc<DataLoader<E>>,
        storage_engine: Arc<E>,
        max_concurrent_loads: usize,
        sql_transformer: Arc<RwLock<SqlTransformer>>,
    ) -> Self {
        Self {
            data_loader,
            storage_engine,
            semaphore: Arc::new(Semaphore::new(max_concurrent_loads)),
            epoch_phases: Arc::new(DashMap::new()),
            sql_transformer,
            outgoing_tx: Arc::new(OnceLock::new()),
            draining: Arc::new(AtomicBool::new(false)),
            drain_notify: Arc::new(Notify::new()),
        }
    }

    /// Read-only snapshot of the current epoch-phase map.
    ///
    /// Useful for tests and diagnostics.
    pub fn epoch_phases(&self) -> &DashMap<EpochKey, EpochPhase> {
        &self.epoch_phases
    }

    /// Returns a list of all epochs currently in the Active phase.
    ///
    /// Used for reconnect reconciliation: after re-establishing a session with
    /// the control plane, the worker reports all loaded epochs so the CP can
    /// update its state to match reality.
    pub fn list_loaded_epochs(&self) -> Vec<EpochKey> {
        self.epoch_phases
            .iter()
            .filter(|entry| *entry.value() == EpochPhase::Active)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Wire up the outgoing status channel after the session has started.
    ///
    /// Must be called exactly once.  Panics in debug builds if called a second
    /// time (the `OnceLock` contract).
    pub fn init_sender(&self, sender: mpsc::Sender<OutgoingPayload>) {
        self.outgoing_tx
            .set(sender)
            .expect("init_sender called more than once");
    }

    /// Returns `true` if drain mode is active (new assignments are rejected).
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Wait for all in-flight loads to complete.
    ///
    /// Returns immediately if no loads are in progress.  The caller should
    /// call this after the Drain command is handled to ensure a graceful
    /// shutdown before terminating the worker.
    pub async fn wait_for_drain(&self) {
        loop {
            // Count epochs currently in Loading phase
            let loading_count = self
                .epoch_phases
                .iter()
                .filter(|entry| *entry.value() == EpochPhase::Loading)
                .count();
            if loading_count == 0 {
                info!("drain complete: no in-flight loads");
                return;
            }
            info!(loading_count, "drain waiting for in-flight loads to complete");
            self.drain_notify.notified().await;
        }
    }
}

impl<E: StorageEngine + 'static> TaskDispatcher for WorkerRuntimeDispatcher<E> {
    #[instrument(skip_all, fields(command = ?std::mem::discriminant(&command)))]
    fn handle_command(&self, command: ControlCommand) -> impl Future<Output = ()> + Send {
        let data_loader = Arc::clone(&self.data_loader);
        let storage_engine = Arc::clone(&self.storage_engine);
        let semaphore = Arc::clone(&self.semaphore);
        let epoch_phases = Arc::clone(&self.epoch_phases);
        let sql_transformer = Arc::clone(&self.sql_transformer);
        let draining = Arc::clone(&self.draining);
        let drain_notify = Arc::clone(&self.drain_notify);
        // Capture the Arc — *not* `.get().cloned()` — so every use-site
        // resolves the sender lazily.  This eliminates the race where a
        // command arrives before `init_sender` is called (possible because
        // the Receive Task starts running while `ControlPlaneSession::start`
        // is still on the stack) and the spawned task would hold a stale
        // `None` even after initialization completes.
        let outgoing_tx = Arc::clone(&self.outgoing_tx);

        async move {
            match command {
                // ── Assignment: semaphore-gated background load ────────────
                ControlCommand::Assignment(assignment) => {
                    // Reject new assignments if draining
                    if draining.load(Ordering::Acquire) {
                        warn!(
                            dataset_id = %assignment.dataset_id,
                            epoch_id = %assignment.epoch_id,
                            "rejecting assignment: drain mode active"
                        );
                        if let Some(tx) = outgoing_tx.get() {
                            report_status(
                                tx,
                                assignment.dataset_id.clone(),
                                assignment.epoch_id.clone(),
                                LoadStatus::Failed,
                                Some("worker is draining".to_string()),
                            )
                            .await;
                        }
                        return;
                    }

                    let dataset_id = assignment.dataset_id.clone();
                    let epoch_id = assignment.epoch_id.clone();
                    let time_column_name = assignment.time_column_name.clone();
                    let key = EpochKey::new(&dataset_id, &epoch_id);

                    // Mark the epoch as Loading (or skip if already Evicting)
                    if let Some(existing) = epoch_phases.get(&key) {
                        if *existing == EpochPhase::Evicting {
                            warn!(%dataset_id, %epoch_id,
                                  "epoch already being evicted; ignoring assignment");
                            return;
                        }
                    }
                    epoch_phases.insert(key.clone(), EpochPhase::Loading);

                    let request = match into_load_request(*assignment) {
                        Ok(r) => r,
                        Err(e) => {
                            error!(
                                %dataset_id, %epoch_id, error = %e,
                                "LoadAssignment → LoadRequest conversion failed; reporting FAILED"
                            );
                            epoch_phases.remove(&key);
                            if let Some(tx) = outgoing_tx.get() {
                                report_status(
                                    tx,
                                    dataset_id,
                                    epoch_id,
                                    LoadStatus::Failed,
                                    Some(e.to_string()),
                                )
                                .await;
                            }
                            return;
                        }
                    };

                    tokio::spawn(async move {
                        let _permit = match semaphore.acquire().await {
                            Ok(p) => p,
                            Err(_) => {
                                warn!(%dataset_id, %epoch_id, "semaphore closed; dropping assignment");
                                epoch_phases.remove(&key);
                                return;
                            }
                        };

                        info!(%dataset_id, %epoch_id, "epoch load starting");
                        if let Some(tx) = outgoing_tx.get() {
                            report_status(
                                tx,
                                dataset_id.clone(),
                                epoch_id.clone(),
                                LoadStatus::InProgress,
                                None,
                            )
                            .await;
                        }

                        match data_loader.load_epoch(request).await {
                            Ok(meta) => {
                                info!(%dataset_id, %epoch_id,
                                      rows = meta.total_rows, bytes = meta.total_bytes,
                                      "epoch load completed");

                                // Two-phase eviction: if the CP sent EvictEpoch
                                // while we were loading, drain now.
                                let should_evict = epoch_phases
                                    .get(&key)
                                    .map(|p| *p == EpochPhase::Evicting)
                                    .unwrap_or(false);

                                if should_evict {
                                    info!(%dataset_id, %epoch_id,
                                          "eviction requested during load; dropping table");
                                    if let Err(e) = storage_engine
                                        .drop_epoch_table(dataset_id.clone(), epoch_id.clone())
                                        .await
                                    {
                                        error!(%dataset_id, %epoch_id, error = %e,
                                               "deferred drop_epoch_table failed");
                                    } else {
                                        // Update the scan macro to remove the dropped epoch table.
                                        // The time_column_name is available from the original assignment.
                                        if !time_column_name.is_empty() {
                                            if let Err(e) = storage_engine
                                                .update_dataset_macro(
                                                    dataset_id.clone(),
                                                    time_column_name.clone(),
                                                )
                                                .await
                                            {
                                                warn!(
                                                    %dataset_id,
                                                    error = %e,
                                                    "failed to update dataset scan macro after deferred eviction"
                                                );
                                            }
                                        }
                                    }
                                    epoch_phases.remove(&key);
                                } else {
                                    // Register the dataset with the SQL transformer so
                                    // queries can reference it by logical name.
                                    if !time_column_name.is_empty() {
                                        let dataset = RegisteredDataset::new(
                                            dataset_id.clone(),
                                            time_column_name.clone(),
                                        );
                                        sql_transformer.write().await.register_dataset(dataset);
                                        info!(%dataset_id, "dataset registered with SQL transformer");

                                        // Update the DuckDB scan macro to include all epoch tables
                                        // for this dataset. This enables the SQL transformer's
                                        // rewritten queries (e.g., scan_orders(...)) to work.
                                        match storage_engine
                                            .update_dataset_macro(
                                                dataset_id.clone(),
                                                time_column_name.clone(),
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                epoch_phases.insert(key, EpochPhase::Active);
                                                if let Some(tx) = outgoing_tx.get() {
                                                    report_status(
                                                        tx,
                                                        dataset_id,
                                                        epoch_id,
                                                        LoadStatus::Completed,
                                                        None,
                                                    )
                                                    .await;
                                                }
                                            }
                                            Err(e) => {
                                                // Macro update failed - data is not queryable.
                                                // Report FAILED to CP to avoid state divergence.
                                                error!(
                                                    %dataset_id,
                                                    %epoch_id,
                                                    error = %e,
                                                    "failed to update dataset scan macro; reporting load as failed"
                                                );
                                                epoch_phases.remove(&key);
                                                if let Some(tx) = outgoing_tx.get() {
                                                    report_status(
                                                        tx,
                                                        dataset_id,
                                                        epoch_id,
                                                        LoadStatus::Failed,
                                                        Some(format!("macro update failed: {}", e)),
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    } else {
                                        // No time column - dataset not registered but still queryable
                                        // via raw table name.
                                        epoch_phases.insert(key, EpochPhase::Active);
                                        if let Some(tx) = outgoing_tx.get() {
                                            report_status(
                                                tx,
                                                dataset_id,
                                                epoch_id,
                                                LoadStatus::Completed,
                                                None,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(%dataset_id, %epoch_id, error = %e, "epoch load failed");
                                epoch_phases.remove(&key);
                                if let Some(tx) = outgoing_tx.get() {
                                    report_status(
                                        tx,
                                        dataset_id,
                                        epoch_id,
                                        LoadStatus::Failed,
                                        Some(e.to_string()),
                                    )
                                    .await;
                                }
                            }
                        }
                        // Notify drain waiter that a load task completed
                        drain_notify.notify_one();
                    });
                }

                ControlCommand::EvictEpoch {
                    dataset_id,
                    epoch_id,
                    reason,
                } => {
                    let key = EpochKey::new(&dataset_id, &epoch_id);
                    let current_phase = epoch_phases.get(&key).map(|p| *p);

                    match current_phase {
                        Some(EpochPhase::Loading) => {
                            // Load is in-flight — mark as Evicting so the load
                            // task will drain + drop the table when it finishes.
                            info!(%dataset_id, %epoch_id, %reason,
                                  "epoch is loading; deferring eviction until load completes");
                            epoch_phases.insert(key, EpochPhase::Evicting);
                        }
                        Some(EpochPhase::Evicting) => {
                            debug!(%dataset_id, %epoch_id, "duplicate evict; already draining");
                        }
                        Some(EpochPhase::Active) | None => {
                            // Active or unknown — drop immediately.
                            info!(%dataset_id, %epoch_id, %reason, "evicting epoch");
                            epoch_phases.remove(&key);
                            if let Err(e) = storage_engine
                                .drop_epoch_table(dataset_id.clone(), epoch_id.clone())
                                .await
                            {
                                error!(%dataset_id, %epoch_id, error = %e,
                                       "drop_epoch_table failed");
                            } else {
                                // Update the scan macro to remove the dropped epoch table.
                                // Get the time column from the registered dataset.
                                let time_column = sql_transformer
                                    .read()
                                    .await
                                    .get_dataset(&dataset_id)
                                    .map(|d| d.time_column_name.clone());

                                if let Some(time_col) = time_column {
                                    if let Err(e) = storage_engine
                                        .update_dataset_macro(dataset_id.clone(), time_col)
                                        .await
                                    {
                                        warn!(
                                            %dataset_id,
                                            error = %e,
                                            "failed to update dataset scan macro after eviction"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Drain ─────────────────────────────────────────────────
                ControlCommand::Drain => {
                    // Set drain flag to reject new assignments.
                    // The caller should call `wait_for_drain()` to wait for
                    // in-flight loads to complete before shutting down.
                    let was_draining = draining.swap(true, Ordering::AcqRel);
                    if was_draining {
                        info!("received Drain command; already draining");
                    } else {
                        let loading_count = epoch_phases
                            .iter()
                            .filter(|e| *e.value() == EpochPhase::Loading)
                            .count();
                        info!(loading_count, "received Drain command; drain mode activated");
                    }
                }

                // ── Unknown ───────────────────────────────────────────────
                ControlCommand::Unknown { event_type } => {
                    warn!(%event_type, "received unknown control command; ignoring");
                }
            }
        }
    }

    fn list_loaded_epochs(&self) -> Vec<EpochKey> {
        self.epoch_phases
            .iter()
            .filter(|entry| *entry.value() == EpochPhase::Active)
            .map(|entry| entry.key().clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// LoadAssignment → LoadRequest conversion
// ---------------------------------------------------------------------------

fn into_load_request(assignment: LoadAssignment) -> anyhow::Result<LoadRequest> {
    let schema = decode_arrow_schema(&assignment.arrow_schema_bytes).with_context(|| {
        format!(
            "failed to decode Arrow schema for epoch {}/{}",
            assignment.dataset_id, assignment.epoch_id
        )
    })?;

    // Map the domain TimeRange (nanoseconds) to the `(u64, u64)` tuple that
    // EpochView expects.  Default to [0, MAX) when no time range is present so
    // the table macro generates no temporal predicate guard.
    let time_range = match &assignment.time_range {
        Some(tr) => (tr.start_inclusive_ns as u64, tr.end_exclusive_ns as u64),
        None => (0, u64::MAX),
    };

    let epoch_view = EpochView {
        epoch_id: assignment.epoch_id,
        table_name: assignment.destination_table_name,
        time_range,
        time_column_name: assignment.time_column_name.clone(),
        time_partition_value: assignment.time_partition_value.clone(),
        dimension_values: assignment.dimension_values,
    };

    // Build a SQL filter from the resolved partition value so the source
    // adapter can push down the time predicate to StarRocks/Iceberg.
    // Example: "dt = '2025-01-15'"
    //
    // Only construct the filter when *both* fields are non-empty.  Assignments
    // that omit `resolved_partition` arrive with empty strings for both; the
    // unconditional `Some(format!(...))` would produce `" = ''"`, which is
    // invalid SQL and causes the load to fail instead of degrading to an
    // unfiltered read.
    let filter = match (
        assignment.time_column_name.as_str(),
        assignment.time_partition_value.as_str(),
    ) {
        ("", _) | (_, "") => None,
        (col, val) => Some(format!("{col} = '{val}'")),
    };

    let source = into_data_source(assignment.source, filter);

    Ok(LoadRequest {
        dataset_id: assignment.dataset_id,
        epoch_view,
        source,
        schema,
    })
}

fn into_data_source(info: DataSourceInfo, filter: Option<String>) -> DataSource {
    DataSource {
        catalog: into_catalog(info.catalog),
        database: info.database,
        table: info.table,
        filter,
        version: info.version,
    }
}

fn into_catalog(info: CatalogInfo) -> Catalog {
    match info {
        CatalogInfo::Iceberg { uri } => Catalog::Iceberg { uri },
        CatalogInfo::StarRocks {
            catalog_name,
            host,
            port,
            database,
            username,
            password,
            max_concurrency,
            pool_options,
        } => Catalog::StarRocks {
            catalog: catalog_name,
            host,
            port: port as usize,
            database,
            user: username,
            password,
            max_concurrency: max_concurrency.map(|n| n as usize),
            pool_options: pool_options.map(into_pool_options),
        },
        CatalogInfo::Jdbc {
            uri,
            driver,
            pool_options,
        } => Catalog::Jdbc {
            uri,
            driver,
            pool_options: pool_options.map(into_pool_options),
        },
    }
}

fn into_pool_options(opts: PoolOptions) -> ConnectionPoolOptions {
    ConnectionPoolOptions {
        max_connections: Some(opts.max_connections),
        min_connections: Some(opts.min_connections),
        acquire_timeout_ms: None,
        connect_timeout_ms: Some(opts.connect_timeout_ms),
        idle_timeout_ms: Some(opts.idle_timeout_ms),
        max_lifetime_ms: None,
        batch_size: None,
    }
}

/// Decode Arrow IPC stream bytes into an [`Arc<Schema>`].
///
/// The CP serialises the schema as an Arrow IPC stream (a Schema message
/// header optionally followed by zero or more RecordBatch messages and an
/// EOS marker).  [`StreamReader::try_new`] reads and returns the schema from
/// the stream header without consuming any record batches, making it safe for
/// "schema-only" payloads as well as full streams.
fn decode_arrow_schema(bytes: &[u8]) -> anyhow::Result<Arc<Schema>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .context("Arrow IPC stream reader initialisation failed")?;
    Ok(reader.schema())
}

/// Attempt to enqueue a status update up to [`STATUS_RETRY_COUNT`] times.
///
/// On `TrySendError::Full` the task sleeps for [`STATUS_RETRY_DELAY`] and
/// retries.  On `TrySendError::Closed` (session dead) or after all retries
/// are exhausted, the update is dropped with a warning.  The data remains
/// loaded in DuckDB; divergence is resolved on reconnect via epoch
/// reconciliation (see design doc §6.0).
async fn report_status(
    tx: &mpsc::Sender<OutgoingPayload>,
    dataset_id: String,
    epoch_id: String,
    status: LoadStatus,
    error: Option<String>,
) {
    let payload = OutgoingPayload::StatusUpdate {
        dataset_id: dataset_id.clone(),
        epoch_id: epoch_id.clone(),
        status,
        error,
    };

    for attempt in 0..STATUS_RETRY_COUNT {
        match tx.try_send(payload.clone()) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if attempt + 1 < STATUS_RETRY_COUNT {
                    tokio::time::sleep(STATUS_RETRY_DELAY).await;
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(
                    %dataset_id,
                    %epoch_id,
                    ?status,
                    "session closed; status update dropped"
                );
                return;
            }
        }
    }

    warn!(
        %dataset_id,
        %epoch_id,
        ?status,
        "outgoing channel full after {} retries; status update dropped",
        STATUS_RETRY_COUNT
    );
}
