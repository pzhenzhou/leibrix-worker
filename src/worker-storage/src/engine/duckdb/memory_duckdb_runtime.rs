use super::helper::*;
use super::DuckDBConfig;
use crate::engine::storage_engine::*;
use anyhow::{bail, Context};
use arrow::datatypes::Schema;
use arrow::record_batch;
use duckdb::{params, Connection, DuckdbConnectionManager};
use r2d2::PooledConnection;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

/// Internal state for an epoch currently being ingested.
/// Accumulates batches until flush threshold is reached.
struct EpochInProgress {
    #[allow(dead_code)]
    dataset_id: String,
    epoch_view: EpochView,
    pending_batches: Vec<record_batch::RecordBatch>,
    pending_rows: u64,
    total_rows_committed: u64,
    /// First flush error encountered for this epoch, if any.
    /// Set by `on_ingest_batch` on flush failure so the error is reported
    /// to the caller via `done` in `on_finish_epoch` without terminating
    /// the engine or affecting other in-progress epochs.
    first_error: Option<anyhow::Error>,
    done: oneshot::Sender<anyhow::Result<TableMetadata>>,
}

struct EngineState {
    conn: PooledConnection<DuckdbConnectionManager>,
    config: DuckDBConfig,
    committed: HashMap<String, (EpochView, TableMetadata)>,
    in_progress: HashMap<String, EpochInProgress>,
    metrics: EngineMetrics,
    shutdown: bool,
}

pub enum EngineCom {
    StartEpoch {
        dataset_id: String,
        epoch_view: EpochView,
        schema: Arc<Schema>,
        done: oneshot::Sender<anyhow::Result<TableMetadata>>,
    },
    IngestBatch {
        key: String,
        batch: record_batch::RecordBatch,
    },
    FinishEpoch {
        key: String,
    },
    DropEpoch {
        dataset_id: String,
        epoch_id: String,
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
    ListEpoch {
        dataset_id: String,
        resp: oneshot::Sender<anyhow::Result<Vec<EpochView>>>,
    },
    MemoryStats {
        resp: oneshot::Sender<anyhow::Result<crate::engine::storage_engine::MemoryStats>>,
    },
    GetMetrics {
        resp: oneshot::Sender<anyhow::Result<EngineMetrics>>,
    },
    Shutdown {
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
}

pub fn engine_main(
    config: DuckDBConfig,
    mut rx: sync::mpsc::Receiver<EngineCom>,
    db_conn: PooledConnection<DuckdbConnectionManager>,
) -> anyhow::Result<()> {
    // Configure the connection
    if let Some(mem_limit) = config.memory_limit_mb {
        db_conn
            .execute(&format!("SET memory_limit='{} MB'", mem_limit), params![])
            .context("Failed to set memory limit")?;

        info!("DuckDB memory limit set to {} MB", mem_limit);
    }

    if let Some(tmp_dir) = &config.tmp_dir {
        std::fs::create_dir_all(tmp_dir).context("Failed to create tmp dir")?;
        db_conn
            .execute(
                &format!("SET temp_directory='{}'", tmp_dir.display()),
                params![],
            )
            .context("Failed to set temp directory")?;

        info!(
            "DuckDB set temp directory for potential disk spilling {}",
            tmp_dir.display()
        );
    }
    info!("DuckDB Memory Engine started");
    let mut state = EngineState {
        conn: db_conn,
        config,
        committed: HashMap::new(),
        in_progress: HashMap::new(),
        metrics: EngineMetrics {
            total_batches_written: 0,
            total_rows_written: 0,
            total_flushes: 0,
            active_epochs: 0,
            committed_epochs: 0,
        },
        shutdown: false,
    };
    loop {
        match rx.blocking_recv() {
            Some(EngineCom::StartEpoch {
                dataset_id,
                epoch_view,
                schema,
                done,
            }) => {
                on_start_epoch(&mut state, dataset_id, epoch_view, schema, done);
            }
            Some(EngineCom::IngestBatch {
                key: dataset_id,
                batch,
            }) => {
                // on_ingest_batch records flush errors per-epoch via first_error
                // and always returns Ok(()); only truly unexpected engine-level
                // failures would produce an Err, which we log as non-fatal.
                if let Err(e) = on_ingest_batch(&mut state, dataset_id, batch) {
                    warn!(error = ?e, "Unexpected error in on_ingest_batch (non-fatal, other epochs unaffected)");
                }
            }
            Some(EngineCom::FinishEpoch { key }) => {
                // on_finish_epoch reports per-epoch errors through the done
                // oneshot and always returns Ok(()); only unexpected engine-level
                // failures produce an Err.
                if let Err(e) = on_finish_epoch(&mut state, key) {
                    warn!(error = ?e, "Unexpected error in on_finish_epoch (non-fatal, other epochs unaffected)");
                }
            }
            Some(EngineCom::DropEpoch {
                dataset_id,
                epoch_id,
                resp,
            }) => {
                let res = on_drop_epoch(&mut state, dataset_id, epoch_id);
                if let Err(e) = &res {
                    warn!(error = ?e, "Failed to drop epoch");
                }
                let _ = resp.send(res);
            }
            Some(EngineCom::ListEpoch { dataset_id, resp }) => {
                let rs = on_list_epochs(&mut state, dataset_id);
                let _ = resp.send(rs);
            }
            Some(EngineCom::MemoryStats { resp }) => {
                let state = on_memory_stats(&mut state);
                let _ = resp.send(Ok(state));
            }
            Some(EngineCom::GetMetrics { resp }) => {
                let _ = resp.send(Ok(state.metrics.clone()));
            }
            Some(EngineCom::Shutdown { resp }) => {
                info!("Shutdown requested - cleaning up in-progress epochs");
                state.shutdown = true;

                // Fail all in-progress epochs
                for (key, ep) in state.in_progress.drain() {
                    let _ = ep.done.send(Err(StorageError::ShuttingDown.into()));
                    warn!(key = %key, "Aborted in-progress epoch due to shutdown");
                }

                let _ = resp.send(Ok(()));
                break;
            }
            None => {
                warn!("Command channel closed - shutting down engine");
                break;
            }
        }

        // Update metrics after each command (cheap operation)
        state.metrics.active_epochs = state.in_progress.len();
        state.metrics.committed_epochs = state.committed.len();
    }
    info!("Engine thread shutting down gracefully");
    Ok(())
}

/// on_start_epoch init EpochInProgress state
fn on_start_epoch(
    state: &mut EngineState,
    dataset_id: String,
    epoch_view: EpochView,
    schema: Arc<Schema>,
    done: oneshot::Sender<anyhow::Result<TableMetadata>>,
) {
    if state.shutdown {
        let _ = done.send(Err(StorageError::ShuttingDown.into()));
        return;
    }
    let epoch_key = epoch_key(&dataset_id, &epoch_view.epoch_id);

    if state.in_progress.contains_key(&epoch_key) || state.committed.contains_key(&epoch_key) {
        let _ = done.send(Err(StorageError::InvalidArgument(format!(
            "Epoch table {} for dataset {} already exists.",
            epoch_view.epoch_id, dataset_id
        ))
        .into()));
        return;
    }
    // if create table fails, send error back
    if let Err(e) = create_table_from_arrow_schema(&state.conn, &epoch_view.table_name, &schema) {
        let _ = done.send(Err(e));
        return;
    }

    state.in_progress.insert(
        epoch_key,
        EpochInProgress {
            dataset_id: dataset_id.clone(),
            epoch_view,
            pending_batches: Vec::new(),
            pending_rows: 0,
            total_rows_committed: 0,
            first_error: None,
            done,
        },
    );
}

fn on_ingest_batch(
    state: &mut EngineState,
    key: String,
    batch: record_batch::RecordBatch,
) -> anyhow::Result<()> {
    let ep = match state.in_progress.get_mut(&key) {
        Some(ep) => ep,
        None => {
            warn!(key = %key, "IngestBatch for non-existent epoch — ignoring");
            return Ok(());
        }
    };

    // If a previous flush already failed, discard incoming batches for this
    // epoch rather than accumulating data that can never be committed.
    if ep.first_error.is_some() {
        return Ok(());
    }

    let num_rows = batch.num_rows() as u64;
    ep.pending_rows += num_rows;
    ep.pending_batches.push(batch);

    // Flush if threshold reached
    if ep.pending_rows >= state.config.flush_rows_threshold {
        if let Err(e) = flush_pending_batches(&state.conn, &mut state.metrics, ep)
            .with_context(|| format!("Failed to flush pending batches for epoch {}", key))
        {
            // Record the error on this epoch only. Clear buffered batches so
            // subsequent IngestBatch messages are cheaply discarded above.
            ep.pending_batches.clear();
            ep.pending_rows = 0;
            error!(
                key = %key,
                error = ?e,
                "Flush failed for epoch — recording error, other epochs unaffected"
            );
            ep.first_error = Some(e);
        }
    }
    Ok(())
}

fn on_finish_epoch(state: &mut EngineState, key: String) -> anyhow::Result<()> {
    let mut ep = match state.in_progress.remove(&key) {
        Some(ep) => ep,
        None => {
            warn!(key = %key, "FinishEpoch for non-existent epoch — ignoring");
            return Ok(());
        }
    };

    // Fast-path: a previous flush already recorded an error for this epoch.
    if let Some(e) = ep.first_error.take() {
        error!(key = %key, error = ?e, "Epoch ingestion failed (flush error recorded earlier) — reporting to caller");
        let _ = ep.done.send(Err(e));
        return Ok(());
    }

    // Flush any remaining buffered batches; treat failure as a per-epoch error.
    if let Err(e) = flush_pending_batches(&state.conn, &mut state.metrics, &mut ep)
        .with_context(|| format!("Failed to flush final batches for epoch {}", key))
    {
        error!(key = %key, error = ?e, "Final flush failed for epoch — reporting to caller, other epochs unaffected");
        let _ = ep.done.send(Err(e));
        return Ok(());
    }

    if ep.total_rows_committed == 0 {
        let _ = ep.done.send(Err(StorageError::InvalidArgument(
            "Arrow stream for epoch is empty".to_string(),
        )
        .into()));
        return Ok(());
    }

    // Get schema from DuckDB table; treat failure as a per-epoch error.
    let schema = match query_table_schema(&state.conn, &ep.epoch_view.table_name).with_context(
        || {
            format!(
                "Failed to query schema for table {}",
                ep.epoch_view.table_name
            )
        },
    ) {
        Ok(s) => s,
        Err(e) => {
            error!(key = %key, error = ?e, "Schema query failed after flush — reporting to caller, other epochs unaffected");
            let _ = ep.done.send(Err(e));
            return Ok(());
        }
    };

    // Get actual table size from DuckDB
    let approx_bytes = query_table_size(&state.conn, &ep.epoch_view.table_name)
        .unwrap_or_else(|_| estimate_table_bytes(ep.total_rows_committed, &schema));

    let create_at = now_secs();

    let metadata = TableMetadata {
        table_name: ep.epoch_view.table_name.clone(),
        total_rows: ep.total_rows_committed,
        total_bytes: approx_bytes,
        create_at,
        schema,
    };

    info!(
        key = %key,
        rows = ep.total_rows_committed,
        bytes = approx_bytes,
        "Epoch ingestion completed successfully"
    );

    // Store both EpochView and TableMetadata
    state
        .committed
        .insert(key, (ep.epoch_view, metadata.clone()));

    let _ = ep.done.send(Ok(metadata));
    Ok(())
}

fn on_drop_epoch(
    state: &mut EngineState,
    dataset_id: String,
    epoch_id: String,
) -> anyhow::Result<()> {
    let key = epoch_key(&dataset_id, &epoch_id);
    if state.in_progress.contains_key(&key) {
        bail!(
            "Cannot drop epoch {} for dataset {} while ingestion is in progress",
            epoch_id,
            dataset_id
        );
    }

    let (epoch_view, _metadata) = state.committed.remove(&key).ok_or_else(|| {
        StorageError::InvalidArgument(format!(
            "Epoch {} for dataset {} not found",
            epoch_id, dataset_id
        ))
    })?;

    let drop_sql = format!(
        "DROP TABLE IF EXISTS {}",
        escape_ident(&epoch_view.table_name)
    );
    state
        .conn
        .execute(&drop_sql, params![])
        .with_context(|| format!("Failed to drop table {}", epoch_view.table_name))?;

    info!(key = %key, table = %epoch_view.table_name, "Dropped epoch table");
    Ok(())
}

fn on_list_epochs(state: &mut EngineState, dataset_id: String) -> anyhow::Result<Vec<EpochView>> {
    let prefix = format!("{}__{}", dataset_id, "");
    let epochs = state
        .committed
        .iter()
        .filter_map(|(key, (epoch_view, _metadata))| {
            if key.starts_with(&prefix) {
                Some(epoch_view.clone())
            } else {
                None
            }
        })
        .collect();

    Ok(epochs)
}

fn on_memory_stats(state: &mut EngineState) -> MemoryStats {
    let epochs: Vec<EpochMemoryStats> = state
        .committed
        .iter()
        .map(|(key, (view, metadata))| {
            let parts: Vec<&str> = key.split("__").collect();
            EpochMemoryStats {
                dataset_id: parts.first().unwrap_or(&"").to_string(),
                epoch_id: view.epoch_id.clone(),
                rows_count: metadata.total_rows,
                approx_bytes: metadata.total_bytes,
            }
        })
        .collect();

    let total_storage_bytes_estimate = epochs.iter().map(|e| e.approx_bytes).sum();

    MemoryStats {
        total_storage_bytes_estimate,
        epochs,
    }
}

fn flush_pending_batches(
    conn: &Connection,
    metrics: &mut EngineMetrics,
    ep: &mut EpochInProgress,
) -> anyhow::Result<()> {
    if ep.pending_batches.is_empty() {
        return Ok(());
    }

    let mut appender = conn
        .appender(&ep.epoch_view.table_name)
        .with_context(|| format!("Failed to create appender for {}", ep.epoch_view.table_name))?;

    // Append all pending batches
    for batch in ep.pending_batches.drain(..) {
        let num_rows = batch.num_rows(); // Capture before move
        appender.append_record_batch(batch).with_context(|| {
            format!(
                "Failed to append batch with {} rows to {}",
                num_rows, ep.epoch_view.table_name
            )
        })?;
    }

    // Flush the appender to commit the data
    appender
        .flush()
        .context("Failed to flush appender to DuckDB")?;

    // Update metrics and state only after successful flush
    metrics.total_batches_written += 1;
    metrics.total_rows_written += ep.pending_rows;
    metrics.total_flushes += 1;

    ep.total_rows_committed += ep.pending_rows;
    ep.pending_rows = 0;

    Ok(())
}
