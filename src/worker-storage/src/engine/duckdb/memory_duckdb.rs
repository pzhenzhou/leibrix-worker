use crate::engine::duckdb::{DuckDBEngineConfig, arrow_type_to_duckdb_type};
use crate::engine::engine::{EngineMetrics, EpochView, StorageError, TableMetadata};
use anyhow::Context;
use arrow::datatypes::Schema;
use arrow::record_batch;
use duckdb::{Connection, params};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync;
use tokio::sync::oneshot;
use tracing::{error, info};

/// Internal state for an epoch currently being ingested.
/// Accumulates batches until flush threshold is reached.
struct EpochInProgress {
    dataset_id: String,
    epoch_view: EpochView,
    pending_batches: Vec<record_batch::RecordBatch>,
    pending_rows: u64,
    total_rows_commited: u64,
    done: oneshot::Sender<anyhow::Result<TableMetadata>>,
    first_error: Option<anyhow::Error>,
}

struct EngineState {
    conn: Connection,
    config: DuckDBEngineConfig,
    commited: HashMap<String, (EpochView, TableMetadata)>,
    in_progress: HashMap<String, EpochInProgress>,
    metrics: EngineMetrics,
    first_error: Option<anyhow::Error>,
    shutdown: bool,
}

enum EngineCom {
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
        resp: oneshot::Sender<anyhow::Result<crate::engine::engine::MemoryStats>>,
    },
    GetMetrics {
        resp: oneshot::Sender<anyhow::Result<EngineMetrics>>,
    },
    Shutdown {
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
}

/// An in-memory DuckDB storage engine for the acceleration layer.
pub struct MemoryDuckDBEngine {
    com_tx: sync::mpsc::Sender<EngineCom>,
}

impl MemoryDuckDBEngine {
    pub fn new(config: DuckDBEngineConfig) -> anyhow::Result<Self> {
        info!("MemoryDuckDBEngine starting with config: {:?}", config);
        let (tx, rx) = sync::mpsc::channel(config.channel_capacity);
        let thread_config = config.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = start_engine_loop(thread_config, rx) {
                error!("Engine loop exited with error: {}", e);
            }
        });
        Ok(Self { com_tx: tx })
    }

    pub fn with_defaults() -> anyhow::Result<Self> {
        Self::new(DuckDBEngineConfig::default())
    }
}

fn epoch_key(dataset_id: &str, epoch_id: &str) -> String {
    format!("{}__{}", dataset_id, epoch_id)
}

fn escape_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn flush_pending_batches(state: &mut EngineState, ep: &mut EpochInProgress) -> anyhow::Result<()> {
    if ep.pending_batches.is_empty() {
        return Ok(());
    }

    let mut appender = state
        .conn
        .appender(&ep.epoch_view.table_name)
        .with_context(|| format!("Failed to create appender for {}", ep.epoch_view.table_name))?;

    // Append all pending batches
    for batch in &ep.pending_batches {
        appender
            .append_record_batch(batch.clone())
            .with_context(|| format!("Failed to append batch to {}", ep.epoch_view.table_name))?;
    }

    // Flush the appender to commit the data
    appender.flush().context("Failed to flush appender")?;

    // Update metrics only after successful flush
    state.metrics.total_batches_written += ep.pending_batches.len() as u64;
    state.metrics.total_rows_written += ep.pending_rows;
    state.metrics.total_flushes += 1;

    ep.pending_rows = 0;
    ep.pending_batches.clear();

    Ok(())
}

fn create_table_from_arrow_schema(
    conn: &Connection,
    table_name: &str,
    schema: &Schema,
) -> anyhow::Result<()> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let duckdb_type = arrow_type_to_duckdb_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("{} {}{}", escape_ident(field.name()), duckdb_type, nullable)
        })
        .collect::<Vec<String>>()
        .join(", ");
    let create_table_sql = format!("CREATE TABLE {} ({})", escape_ident(table_name), columns);
    conn.execute(&create_table_sql, params![])
        .with_context(|| {
            format!(
                "Failed to create table {} with schema {:?}",
                table_name, schema
            )
        })?;
    info!("Created table {} with schema {:?}", table_name, columns);
    Ok(())
}

fn start_engine_loop(
    config: DuckDBEngineConfig,
    mut rx: sync::mpsc::Receiver<EngineCom>,
) -> anyhow::Result<()> {
    // The connection is not thread-safe, so owned by single thread
    let db_conn = duckdb::Connection::open_in_memory().context("Failed to open db")?;
    if let Some(mem_limit) = config.memory_limit_mb {
        db_conn
            .execute(&format!("SET memory_limit='{} MB'", mem_limit), params![])
            .context("Failed to set memory limit")?;

        info!("DuckDB memory limit set to {} MB", mem_limit);
    }

    if let Some(parent) = config.tmp_dir.as_ref().and_then(|d| d.parent()) {
        std::fs::create_dir_all(parent).context("Failed to create tmp dir parent")?;
        db_conn
            .execute(
                &format!("SET temp_directory='{}'", parent.display()),
                params![],
            )
            .context("Failed to set temp directory")?;

        info!(
            "DuckDB set temp director for potential disk spilling {}",
            parent.display()
        );
    }
    info!("DuckDB Memory Engine started");
    let mut state = EngineState {
        conn: db_conn,
        config,
        commited: HashMap::new(),
        in_progress: HashMap::new(),
        metrics: EngineMetrics {
            total_batches_written: 0,
            total_rows_written: 0,
            total_flushes: 0,
            active_epochs: 0,
            committed_epochs: 0,
        },
        first_error: None,
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
                on_ingest_batch(&mut state, dataset_id, batch);
            }
            Some(EngineCom::FinishEpoch { key: dataset_id }) => {
                on_finish_epoch(&mut state, dataset_id);
            }
            Some(EngineCom::DropEpoch {
                dataset_id,
                epoch_id,
                resp,
            }) => {
                on_drop_epoch(&mut state, dataset_id, epoch_id, resp);
            }
            Some(EngineCom::ListEpoch { dataset_id, resp }) => {
                on_list_epochs(&mut state, dataset_id, resp);
            }
            Some(EngineCom::MemoryStats { resp }) => {
                on_memory_stats(&mut state, resp);
            }
            Some(EngineCom::GetMetrics { resp }) => {
                on_get_metrics(&mut state, resp);
            }
            Some(EngineCom::Shutdown { resp }) => {
                state.shutdown = true;
                let _ = resp.send(Ok(()));
                break;
            }
            None => {
                break;
            }
        }
    }

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

    if state.in_progress.contains_key(&epoch_key) || state.commited.contains_key(&epoch_key) {
        let _ = done.send(Err(StorageError::InvalidArgument(format!(
            "Epoch table {} for dataset {} already exists.",
            epoch_view.epoch_id, dataset_id
        ))
        .into()));
        return;
    }

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
            total_rows_commited: 0,
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
    let ep = state.in_progress.get_mut(&key).ok_or_else(|| {
        StorageError::InvalidArgument(format!("AppendBatch for non-existent epoch: {}", key))
    })?;

    if ep.first_error.is_some() {
        return Ok(());
    }

    ep.pending_rows += batch.num_rows() as u64;
    ep.pending_batches.push(batch);

    unimplemented!()
}

fn on_finish_epoch(state: &mut EngineState, dataset_id: String) {
    unimplemented!()
}

fn on_drop_epoch(
    state: &mut EngineState,
    dataset_id: String,
    epoch_id: String,
    resp: oneshot::Sender<anyhow::Result<()>>,
) {
    unimplemented!()
}

fn on_list_epochs(
    state: &mut EngineState,
    dataset_id: String,
    resp: oneshot::Sender<anyhow::Result<Vec<EpochView>>>,
) {
    unimplemented!()
}

fn on_memory_stats(
    state: &mut EngineState,
    resp: oneshot::Sender<anyhow::Result<crate::engine::engine::MemoryStats>>,
) {
    unimplemented!()
}

fn on_get_metrics(state: &mut EngineState, resp: oneshot::Sender<anyhow::Result<EngineMetrics>>) {
    unimplemented!()
}

fn append_pending_batches(key: String, state: &mut EngineState) -> anyhow::Result<()> {
    unimplemented!()
}

impl crate::engine::engine::StorageEngine for MemoryDuckDBEngine {
    fn create_epoch_table(
        &self,
        dataset_id: String,
        epoch: EpochView,
        mut arrow_stream: crate::engine::engine::RecordBatchStream,
    ) -> impl Future<Output = anyhow::Result<TableMetadata>> + Send {
        async { panic!("not implemented") }
    }

    fn drop_epoch_table(
        &self,
        dataset_id: String,
        epoch_id: String,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { panic!("not implemented") }
    }

    fn list_epochs(
        &self,
        dataset_id: String,
    ) -> impl Future<Output = anyhow::Result<Vec<EpochView>>> + Send {
        async { panic!("not implemented") }
    }

    fn memory_stats(
        &self,
    ) -> impl Future<Output = anyhow::Result<crate::engine::engine::MemoryStats>> + Send {
        async { panic!("not implemented") }
    }

    fn get_metrics(&self) -> impl Future<Output = anyhow::Result<EngineMetrics>> + Send {
        async { panic!("not implemented") }
    }

    fn shutdown(self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { panic!("not implemented") }
    }
}
