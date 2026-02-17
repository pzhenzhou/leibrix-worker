//! Epoch macro helpers for test infrastructure.
//!
//! This module provides utility functions for creating and managing
//! DuckDB table macros that enable epoch-based time range filtering
//! for testing purposes.

use crate::ldp::testing::cluster::TestWorker;
use crate::ldp::types::WorkerId;
use duckdb::{Connection, Error as DuckDBError};
use std::collections::HashMap;
use thiserror::Error;

/// Placeholder for LogicalDatasetManager (not yet implemented).
/// In production, this would manage logical dataset to physical table mappings.
pub struct LogicalDatasetManager {
    datasets: tokio::sync::RwLock<HashMap<String, Vec<String>>>,
}

impl LogicalDatasetManager {
    pub fn new() -> Self {
        Self {
            datasets: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_dataset_tables(&self, dataset_name: &str) -> Vec<String> {
        let datasets = self.datasets.read().await;
        datasets.get(dataset_name).cloned().unwrap_or_default()
    }

    pub async fn register_table(&self, dataset_name: &str, table_name: String) {
        let mut datasets = self.datasets.write().await;
        datasets
            .entry(dataset_name.to_string())
            .or_insert_with(Vec::new)
            .push(table_name);
    }
}

impl Default for LogicalDatasetManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Error type for macro operations.
#[derive(Error, Debug)]
pub enum MacroError {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] DuckDBError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("General error: {0}")]
    General(String),
}

impl From<anyhow::Error> for MacroError {
    fn from(err: anyhow::Error) -> Self {
        MacroError::General(err.to_string())
    }
}

/// Create a table macro for an epoch-partitioned dataset.
///
/// This function creates a DuckDB macro that unions all epoch tables for a dataset
/// and applies time-based filtering based on the provided date column.
///
/// # Arguments
/// * `conn` - DuckDB connection to create the macro on
/// * `dataset_name` - Name of the dataset (used in macro name)
/// * `date_column` - Name of the date/time column for filtering
/// * `epoch_tables` - List of epoch table names to include in the union
///
/// # Example
/// ```sql
/// -- Creates macro scan_orders(start_date, end_date)
/// -- Usage: SELECT * FROM scan_orders('2022-01-01', '2022-02-01')
/// ```
pub fn create_epoch_table_macro(
    conn: &Connection,
    dataset_name: &str,
    date_column: &str,
    epoch_tables: &[String],
) -> Result<(), MacroError> {
    if epoch_tables.is_empty() {
        // Create an empty macro that returns no rows
        let macro_sql = format!(
            r#"
            CREATE OR REPLACE MACRO scan_{}(start_date, end_date) AS TABLE (
                SELECT * FROM (VALUES()) AS t LIMIT 0
            )
            "#,
            dataset_name
        );
        conn.execute_batch(&macro_sql)?;
        return Ok(());
    }

    // Create UNION ALL clauses for each epoch table
    let union_clauses: Vec<String> = epoch_tables
        .iter()
        .map(|table| format!("SELECT * FROM \"{}\"", table)) // Quote table names to handle special chars
        .collect();

    let union_sql = union_clauses.join(" UNION ALL ");

    let macro_sql = format!(
        r#"
        CREATE OR REPLACE MACRO scan_{}(start_date, end_date) AS TABLE (
            SELECT * FROM (
                {}
            ) WHERE CAST({} AS DATE) >= CAST(start_date AS DATE) 
                  AND CAST({} AS DATE) < CAST(end_date AS DATE)
        )
        "#,
        dataset_name, union_sql, date_column, date_column
    );

    conn.execute_batch(&macro_sql)?;
    Ok(())
}

/// Register all epoch tables and create macros for a dataset across a test cluster.
///
/// This function collects all epoch tables across workers in the cluster and creates
/// a macro on the coordinator that unions them together with time-based filtering.
pub async fn register_dataset_with_macros<M: crate::ldp::planner::metadata::Metadata>(
    _coordinator: &crate::ldp::executor::coordinator::LdpCoordinator<M>,
    dataset_manager: &LogicalDatasetManager,
    dataset_name: &str,
    _date_column: &str,
) -> Result<(), MacroError> {
    // For now, this is a placeholder since coordinator doesn't expose DuckDB connection
    // In a real implementation, we would get the connection and create macros

    // Get all registered epoch tables for this dataset from the dataset manager
    let _epoch_tables = dataset_manager.get_dataset_tables(dataset_name).await;

    // TODO: Create the macro using the collected epoch tables
    // This requires coordinator to expose its DuckDB connection or provide a macro registration API

    Ok(())
}

/// Helper function to create epoch macros on a test worker's DuckDB instance.
///
/// This is useful for setting up individual workers with the appropriate macros
/// during test cluster initialization.
pub async fn register_worker_macros(
    worker: &TestWorker,
    dataset_name: &str,
    _date_column: &str,
) -> Result<(), MacroError> {
    // TODO: Implement proper macro registration once QueryEngine API is finalized
    // This requires:
    // 1. Getting a DuckDB connection from the worker's query engine
    // 2. Querying for tables matching the dataset pattern
    // 3. Building and executing a CREATE MACRO statement

    // For now, just log that we're skipping this
    // (we don't have access to the dataset manager here)
    tracing::debug!(
        "Skipping macro registration for worker {} dataset {}",
        worker.worker_id,
        dataset_name
    );

    Ok(())
}

/// Convenience function to set up all epoch macros for a test cluster.
///
/// This function iterates through all workers in the cluster and sets up
/// the appropriate epoch macros for the given dataset.
pub async fn setup_cluster_epoch_macros(
    workers: &HashMap<WorkerId, TestWorker>,
    dataset_name: &str,
    date_column: &str,
) -> Result<(), MacroError> {
    for (worker_id, worker) in workers {
        println!("Setting up epoch macros for worker: {}", worker_id);
        register_worker_macros(worker, dataset_name, date_column).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::duckdb::DuckDBConfig;

    #[tokio::test]
    async fn test_create_simple_epoch_macro() {
        let config = DuckDBConfig::default();
        let db = crate::engine::duckdb::SharedDatabase::new(&config).unwrap();
        let conn = db.get().unwrap();

        // Create a test table
        conn.execute_batch("CREATE TABLE test_dataset__epoch_001 (id INTEGER, dt DATE)")
            .unwrap();
        conn.execute_batch("INSERT INTO test_dataset__epoch_001 VALUES (1, '2022-01-01')")
            .unwrap();

        // Create the macro
        let result = create_epoch_table_macro(
            &conn,
            "test_dataset",
            "dt",
            &["test_dataset__epoch_001".to_string()],
        );

        assert!(result.is_ok());

        // Test that the macro works
        let mut stmt = conn
            .prepare("SELECT * FROM scan_test_dataset('2022-01-01', '2022-02-01')")
            .unwrap();
        let rows: Vec<(i32, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1);
    }

    #[tokio::test]
    async fn test_create_empty_epoch_macro() {
        let config = DuckDBConfig::default();
        let db = crate::engine::duckdb::SharedDatabase::new(&config).unwrap();
        let conn = db.get().unwrap();

        // Create an empty macro
        let result = create_epoch_table_macro(&conn, "empty_dataset", "dt", &[]);

        assert!(result.is_ok());

        // Test that the macro works (should return no rows)
        let mut stmt = conn
            .prepare("SELECT * FROM scan_empty_dataset('2022-01-01', '2022-02-01')")
            .unwrap();
        let rows: Vec<i32> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 0);
    }
}
