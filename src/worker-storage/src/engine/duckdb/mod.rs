use std::path::PathBuf;

mod helper;
pub mod storage_engine_impl;
mod memory_duckdb_runtime;
mod query_engine_impl;

/// Configuration for the DuckDB storage engine.
#[derive(Debug, Clone)]
pub struct DuckDBConfig {
    pub memory_limit_mb: Option<u64>,
    pub flush_rows_threshold: u64,
    pub channel_capacity: usize,
    pub tmp_dir: Option<PathBuf>,
    pub max_identifiers: usize,
}

impl Default for DuckDBConfig {
    fn default() -> Self {
        Self {
            memory_limit_mb: Some(1024), // 1 GB
            flush_rows_threshold: 10_000,
            channel_capacity: 100,
            tmp_dir: None,
            max_identifiers: 1000,
        }
    }
}
