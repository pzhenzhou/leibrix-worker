//! Test data loader for LDP end-to-end testing.
//!
//! This module provides utilities for loading test data into the cluster,
//! including standard datasets like orders and customers for join tests.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use rand::Rng;

use super::cluster::TestCluster;
use crate::engine::duckdb::arrow_utils::arrow_type_to_duckdb;
use crate::ldp::WorkerId;

/// Parameters for loading a single epoch into a worker's DuckDB instance.
pub struct LoadEpochParams {
    /// Worker to receive the data.
    pub worker_id: String,
    /// Name of the epoch table to create (e.g. `orders__epoch_0`).
    pub epoch_table_name: String,
    /// Base table name (reserved for future filtering logic).
    pub base_table_name: String,
    /// Record batches to load.
    pub data: Vec<RecordBatch>,
    /// Start of the epoch time range (reserved for future filtering).
    pub start_date: NaiveDate,
    /// End of the epoch time range (reserved for future filtering).
    pub end_date: NaiveDate,
    /// Date column name (reserved for future filtering).
    pub date_column: String,
}

/// Distribution specification for loading data across workers.
#[derive(Debug, Clone)]
pub struct DataDistribution {
    /// Worker ID and the number of rows to place on that worker
    pub placements: Vec<(String, usize)>,
}

impl DataDistribution {
    /// Create a distribution with specified row counts per worker
    /// Example: [("w1", 1000), ("w2", 1000), ("w3", 3000)]
    pub fn explicit(placements: Vec<(&str, usize)>) -> Self {
        Self {
            placements: placements
                .iter()
                .map(|(id, count)| (id.to_string(), *count))
                .collect(),
        }
    }

    /// Create a round-robin distribution across N workers
    pub fn round_robin(worker_ids: Vec<&str>, total_rows: usize) -> Self {
        let workers_count = worker_ids.len();
        let rows_per_worker = total_rows / workers_count;
        let remainder = total_rows % workers_count;

        let mut placements = Vec::new();
        for (i, worker_id) in worker_ids.iter().enumerate() {
            let extra = if i < remainder { 1 } else { 0 };
            placements.push((worker_id.to_string(), rows_per_worker + extra));
        }

        Self { placements }
    }

    /// Create a distribution where all data is on a single worker
    pub fn singleton(worker_id: &str, total_rows: usize) -> Self {
        Self {
            placements: vec![(worker_id.to_string(), total_rows)],
        }
    }

    /// Total number of rows across all workers
    pub fn total_rows(&self) -> usize {
        self.placements.iter().map(|(_, count)| count).sum()
    }

    /// Get worker IDs that have data
    pub fn workers(&self) -> Vec<String> {
        self.placements.iter().map(|(id, _)| id.clone()).collect()
    }
}

/// Epoch specification for time-partitioned data
#[derive(Debug, Clone)]
pub struct EpochSpec {
    pub epoch_id: String,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub worker_id: String,
    pub row_count: usize,
}

/// Table loading specification
#[derive(Debug, Clone)]
pub struct TableLoadSpec {
    /// Table name (e.g., "lineitem", "orders")
    pub table_name: String,

    /// Dataset ID for epoch registration (e.g., "ds_lineitem")
    pub dataset_id: String,

    /// Date column name for epoch partitioning (e.g., "l_shipdate")
    pub date_column: String,

    /// Epoch specifications (distribution + time ranges)
    pub epochs: Vec<EpochSpec>,
}

impl TableLoadSpec {
    /// Create a new table load specification
    pub fn new(
        table_name: impl Into<String>,
        dataset_id: impl Into<String>,
        date_column: impl Into<String>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            dataset_id: dataset_id.into(),
            date_column: date_column.into(),
            epochs: Vec::new(),
        }
    }

    /// Add an epoch to this table
    pub fn with_epoch(
        mut self,
        epoch_id: impl Into<String>,
        start_date: NaiveDate,
        end_date: NaiveDate,
        worker_id: impl Into<String>,
        row_count: usize,
    ) -> Self {
        self.epochs.push(EpochSpec {
            epoch_id: epoch_id.into(),
            start_date,
            end_date,
            worker_id: worker_id.into(),
            row_count,
        });
        self
    }

    /// Get all workers that have epochs for this table
    pub fn workers(&self) -> Vec<String> {
        self.epochs
            .iter()
            .map(|e| e.worker_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Total row count across all epochs
    pub fn total_rows(&self) -> usize {
        self.epochs.iter().map(|e| e.row_count).sum()
    }
}

/// Test data generator and loader for LDP testing.
pub struct TestDataLoader {
    cluster: Arc<TestCluster>,
}

impl TestDataLoader {
    /// Create a new test data loader for the given cluster.
    pub fn new(cluster: Arc<TestCluster>) -> Self {
        Self { cluster }
    }

    /// Load a table with specified epoch distribution
    ///
    /// This is the main entry point for loading distributed test data.
    /// It handles:
    /// 1. Generating and loading data to each worker's DuckDB instance
    /// 2. Creating table macros for epoch pruning
    /// 3. Registering epoch metadata in the cluster metadata
    ///
    /// # Arguments
    /// * `spec` - Table load specification with epoch distribution
    /// * `data_generator` - Function that generates RecordBatches given row count and date range
    pub async fn load_table_with_epochs<F>(
        &self,
        spec: &TableLoadSpec,
        mut data_generator: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(usize, NaiveDate, NaiveDate) -> Vec<RecordBatch>,
    {
        println!(
            "Loading table '{}' with {} epochs",
            spec.table_name,
            spec.epochs.len()
        );

        // 1. Generate data for each epoch and load to workers
        for epoch_spec in &spec.epochs {
            println!(
                "  Epoch {}: {} rows on worker {} (date range: {} to {})",
                epoch_spec.epoch_id,
                epoch_spec.row_count,
                epoch_spec.worker_id,
                epoch_spec.start_date,
                epoch_spec.end_date
            );

            // Generate data for this epoch with date constraints
            let epoch_data = data_generator(
                epoch_spec.row_count,
                epoch_spec.start_date,
                epoch_spec.end_date,
            );

            // Create epoch table name: {dataset_id}__{epoch_id}
            let epoch_table_name = format!("{}__{}", spec.dataset_id, epoch_spec.epoch_id);

            // Load data to worker's DuckDB instance
            self.load_epoch_to_worker(LoadEpochParams {
                worker_id: epoch_spec.worker_id.clone(),
                epoch_table_name: epoch_table_name.clone(),
                base_table_name: spec.table_name.clone(),
                data: epoch_data,
                start_date: epoch_spec.start_date,
                end_date: epoch_spec.end_date,
                date_column: spec.date_column.clone(),
            })
            .await?;
        }

        // 2. Register table macro for epoch pruning on each worker
        self.register_table_macro(spec).await?;

        // 3. Register epoch metadata in cluster metadata
        self.register_epoch_metadata(spec).await?;

        println!(
            "✓ Loaded table '{}': {} total rows across {} workers",
            spec.table_name,
            spec.total_rows(),
            spec.workers().len()
        );

        Ok(())
    }

    /// Load a single epoch to a specific worker
    async fn load_epoch_to_worker(
        &self,
        params: LoadEpochParams,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let LoadEpochParams {
            worker_id,
            epoch_table_name,
            base_table_name: _base_table_name,
            data,
            start_date: _start_date,
            end_date: _end_date,
            date_column: _date_column,
        } = params;
        let worker_id = worker_id.as_str();
        let epoch_table_name = epoch_table_name.as_str();
        if data.is_empty() {
            return Err(format!("No data provided for epoch table {}", epoch_table_name).into());
        }

        let _worker = self
            .cluster
            .get_worker(worker_id)
            .ok_or_else(|| format!("Worker {} not found", worker_id))?;

        // Create the epoch table in DuckDB
        let schema = data[0].schema();
        let create_sql = self.generate_create_table_sql(epoch_table_name, &schema);

        self.cluster
            .execute_query_on_worker(worker_id, &create_sql)
            .await?;

        // Insert data
        for batch in &data {
            self.insert_batch_to_worker(worker_id, epoch_table_name, batch)
                .await?;
        }

        // Verify data was loaded
        let count_sql = format!("SELECT COUNT(*) FROM {}", epoch_table_name);
        let result = self
            .cluster
            .execute_query_on_worker(worker_id, &count_sql)
            .await?;

        if !result.is_empty() && result[0].num_rows() > 0 {
            let count_array = result[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = count_array.value(0);
            println!(
                "    Verified: {} rows loaded to {}",
                count, epoch_table_name
            );
        }

        Ok(())
    }

    /// Register table macro for epoch pruning
    ///
    /// Creates: scan_{table_name}(start_date, end_date) that unions all epochs
    /// and filters by date range for epoch pruning
    async fn register_table_macro(
        &self,
        spec: &TableLoadSpec,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Group epochs by worker
        let mut epochs_by_worker: HashMap<String, Vec<&EpochSpec>> = HashMap::new();

        for epoch in &spec.epochs {
            epochs_by_worker
                .entry(epoch.worker_id.clone())
                .or_default()
                .push(epoch);
        }

        // Register macro on each worker that has data
        for (worker_id, epochs) in epochs_by_worker {
            let _worker = self
                .cluster
                .get_worker(&worker_id)
                .ok_or_else(|| format!("Worker {} not found", worker_id))?;

            // Build UNION ALL query for all epochs on this worker
            let mut union_parts = Vec::new();
            for epoch in epochs {
                let epoch_table_name = format!("{}__{}", spec.dataset_id, epoch.epoch_id);
                union_parts.push(format!(
                    "SELECT * FROM {} WHERE {} >= start_date AND {} < end_date",
                    epoch_table_name, spec.date_column, spec.date_column
                ));
            }

            let macro_body = if union_parts.len() > 1 {
                union_parts.join(" UNION ALL ")
            } else {
                union_parts[0].clone()
            };

            // Create table macro
            // Note: DuckDB macro parameters don't use $ prefix
            let macro_sql = format!(
                "CREATE OR REPLACE MACRO scan_{}(start_date, end_date) AS TABLE ({})",
                spec.table_name, macro_body
            );

            println!(
                "    Registering macro on {}: scan_{}",
                worker_id, spec.table_name
            );

            self.cluster
                .execute_query_on_worker(&worker_id, &macro_sql)
                .await?;
        }

        Ok(())
    }

    /// Register epoch metadata in cluster metadata for LDP planning
    ///
    /// This allows the planner to know:
    /// - Which workers have which epochs
    /// - Row counts and byte sizes for cost estimation
    /// - Time ranges for epoch pruning
    async fn register_epoch_metadata(
        &self,
        spec: &TableLoadSpec,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::ldp::EpochStats;

        // Register each epoch table name in the dataset manager AND planner metadata
        for epoch in &spec.epochs {
            // Create epoch table name: {dataset_id}__{epoch_id}
            let epoch_table_name = format!("{}__{}", spec.dataset_id, epoch.epoch_id);

            println!(
                "    Registering epoch metadata: {} on {}",
                epoch.epoch_id, epoch.worker_id
            );

            // Register the epoch table with the dataset manager
            // This makes the table visible for macro creation
            self.cluster
                .dataset_manager
                .register_table(&spec.table_name, epoch_table_name)
                .await;

            // Register with InMemoryMetadata so the planner knows distribution
            let start_ms = epoch
                .start_date
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp_millis() as u64)
                .unwrap_or(0);
            let end_ms = epoch
                .end_date
                .and_hms_opt(23, 59, 59)
                .map(|dt| dt.and_utc().timestamp_millis() as u64)
                .unwrap_or(u64::MAX);
            let estimated_bytes = epoch.row_count as u64 * 100; // rough estimate

            self.cluster.metadata.add_epoch_with_time_range(
                &epoch.epoch_id,
                &spec.table_name,
                EpochStats::exact(epoch.row_count as u64, estimated_bytes),
                WorkerId::from(epoch.worker_id.clone()),
                (start_ms, end_ms),
            );
        }

        Ok(())
    }

    /// Generate CREATE TABLE SQL from Arrow schema
    fn generate_create_table_sql(&self, table_name: &str, schema: &Schema) -> String {
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| {
                let col_name = field.name();
                let col_type = arrow_type_to_duckdb(field.data_type())
                    .unwrap_or_else(|_| "VARCHAR".to_string());
                format!("{} {}", col_name, col_type)
            })
            .collect();

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name,
            columns.join(", ")
        )
    }

    /// Generate orders test data with the specified count.
    pub fn generate_orders(count: usize) -> Vec<RecordBatch> {
        let mut rng = rand::thread_rng();

        // Generate data
        let mut order_ids = Vec::with_capacity(count);
        let mut customer_ids = Vec::with_capacity(count);
        let mut amounts = Vec::with_capacity(count);
        let mut dates = Vec::with_capacity(count);

        for i in 0..count {
            order_ids.push(i as i32);
            customer_ids.push((i % 100) as i32); // 100 customers
            amounts.push(rng.gen_range(10.0..1000.0));

            // Generate dates in a range (e.g., last year)
            let day_offset = rng.gen_range(0..365);
            let base_date = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
            let date = base_date + chrono::Duration::days(day_offset as i64);
            dates.push(date);
        }

        // Create arrays
        let order_id_array = Arc::new(Int32Array::from(order_ids));
        let customer_id_array = Arc::new(Int32Array::from(customer_ids));
        let amount_array = Arc::new(Float64Array::from(amounts));
        let date_array = Arc::new(Date32Array::from(
            dates
                .iter()
                .map(|d| d.num_days_from_ce() - 719163)
                .collect::<Vec<i32>>(), // Convert to days since Unix epoch
        ));

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("order_date", DataType::Date32, false),
        ]));

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![order_id_array, customer_id_array, amount_array, date_array],
        )
        .expect("Failed to create record batch");

        vec![batch]
    }

    /// Generate customers test data with the specified count.
    pub fn generate_customers(count: usize) -> Vec<RecordBatch> {
        let mut customer_ids = Vec::with_capacity(count);
        let mut names = Vec::with_capacity(count);
        let mut cities = Vec::with_capacity(count);

        for i in 0..count {
            customer_ids.push(i as i32);
            names.push(format!("Customer_{}", i));
            cities.push(format!("City_{}", i % 10)); // 10 cities
        }

        // Create arrays
        let customer_id_array = Arc::new(Int32Array::from(customer_ids));
        let name_array = Arc::new(StringArray::from(names));
        let city_array = Arc::new(StringArray::from(cities));

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]));

        // Create record batch
        let batch = RecordBatch::try_new(schema, vec![customer_id_array, name_array, city_array])
            .expect("Failed to create record batch");

        vec![batch]
    }

    /// Generate products test data with the specified count.
    pub fn generate_products(count: usize) -> Vec<RecordBatch> {
        let mut product_ids = Vec::with_capacity(count);
        let mut names = Vec::with_capacity(count);
        let mut categories = Vec::with_capacity(count);
        let mut prices = Vec::with_capacity(count);

        for i in 0..count {
            product_ids.push(i as i32);
            names.push(format!("Product_{}", i));
            categories.push(format!("Category_{}", i % 5)); // 5 categories
            prices.push((i as f64) * 10.0 + 5.0);
        }

        // Create arrays
        let product_id_array = Arc::new(Int32Array::from(product_ids));
        let name_array = Arc::new(StringArray::from(names));
        let category_array = Arc::new(StringArray::from(categories));
        let price_array = Arc::new(Float64Array::from(prices));

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int32, false),
            Field::new("product_name", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![product_id_array, name_array, category_array, price_array],
        )
        .expect("Failed to create record batch");

        vec![batch]
    }

    /// Load orders data distributed across all workers in the cluster.
    pub async fn load_orders_distributed(
        &self,
        orders: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_ids = self.cluster.worker_ids();
        if worker_ids.is_empty() {
            return Err("No workers in cluster".into());
        }

        // Distribute batches across workers in round-robin fashion
        for (i, batch) in orders.iter().enumerate() {
            let worker_id = &worker_ids[i % worker_ids.len()];

            // Create table if it doesn't exist
            self.cluster.execute_query_on_worker(
                worker_id,
                "CREATE TABLE IF NOT EXISTS orders (order_id INTEGER, customer_id INTEGER, amount DOUBLE, order_date DATE)"
            ).await?;

            // Insert the batch data
            // Note: For simplicity, we're using DuckDB's insert mechanism
            // In a real scenario, this would involve more sophisticated batch loading
            self.insert_batch_to_worker(worker_id, "orders", batch)
                .await?;
        }

        Ok(())
    }

    /// Load customers data to a specific worker.
    pub async fn load_customers_on_worker(
        &self,
        worker_id: &str,
        customers: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self
            .cluster
            .worker_ids()
            .iter()
            .any(|w| w.as_ref() == worker_id)
        {
            return Err(format!("Worker {} not found in cluster", worker_id).into());
        }

        for batch in &customers {
            // Create table if it doesn't exist
            self.cluster.execute_query_on_worker(
                worker_id,
                "CREATE TABLE IF NOT EXISTS customers (customer_id INTEGER, customer_name VARCHAR, city VARCHAR)"
            ).await?;

            // Insert the batch data
            self.insert_batch_to_worker(worker_id, "customers", batch)
                .await?;
        }

        Ok(())
    }

    /// Load products data distributed across all workers in the cluster.
    pub async fn load_products_distributed(
        &self,
        products: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_ids = self.cluster.worker_ids();
        if worker_ids.is_empty() {
            return Err("No workers in cluster".into());
        }

        // Distribute batches across workers in round-robin fashion
        for (i, batch) in products.iter().enumerate() {
            let worker_id = &worker_ids[i % worker_ids.len()];

            // Create table if it doesn't exist
            self.cluster.execute_query_on_worker(
                worker_id,
                "CREATE TABLE IF NOT EXISTS products (product_id INTEGER, product_name VARCHAR, category VARCHAR, price DOUBLE)"
            ).await?;

            // Insert the batch data
            self.insert_batch_to_worker(worker_id, "products", batch)
                .await?;
        }

        Ok(())
    }

    /// Helper function to insert a batch into a worker's table.
    async fn insert_batch_to_worker(
        &self,
        worker_id: impl AsRef<str>,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_id = worker_id.as_ref();
        // Convert batch to individual row inserts for simplicity
        // In a real scenario, we'd use bulk insert mechanisms
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = self.format_column_value(col, row_idx)?;
                values.push(value);
            }

            let values_str = values.join(", ");
            let sql = format!("INSERT INTO {} VALUES ({})", table_name, values_str);

            self.cluster
                .execute_query_on_worker(worker_id, &sql)
                .await?;
        }

        Ok(())
    }

    /// Format a column value for SQL insertion.
    fn format_column_value(
        &self,
        array: &ArrayRef,
        index: usize,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::*;

        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(arr.value(index).to_string())
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(arr.value(index).to_string())
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(arr.value(index).to_string())
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(format!("'{}'", arr.value(index)))
            }
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(index);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).ok_or_else(
                    || Box::<dyn std::error::Error + Send + Sync>::from("Invalid date"),
                )?;
                Ok(format!("DATE '{}'", date))
            }
            _ => Err(format!("Unsupported data type: {:?}", array.data_type()).into()),
        }
    }

    /// Generate and load standard test data into the cluster.
    /// This creates orders and customers tables distributed appropriately.
    pub async fn load_standard_test_data(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Loading standard test data...");

        // Generate orders data (distributed across workers)
        let orders = Self::generate_orders(1000);
        self.load_orders_distributed(orders).await?;
        println!("Loaded orders data (1000 records) distributed across workers");

        // Generate customers data (could be replicated or on specific workers)
        let customers = Self::generate_customers(100);
        // Load customers on first worker for simplicity (in real test scenarios,
        // you might want to replicate this table across all workers for broadcast joins)
        let worker_ids = self.cluster.worker_ids();
        let first_worker = worker_ids.first().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from("No workers available")
        })?;
        self.load_customers_on_worker(first_worker.as_ref(), customers)
            .await?;
        println!(
            "Loaded customers data (100 records) on worker {}",
            first_worker
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_generate_orders() {
        let orders = TestDataLoader::generate_orders(5);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].num_rows(), 5);
        assert_eq!(orders[0].num_columns(), 4);

        // Check schema
        let schema = orders[0].schema();
        assert_eq!(schema.field(0).name(), "order_id");
        assert_eq!(schema.field(1).name(), "customer_id");
        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(schema.field(3).name(), "order_date");
    }

    #[tokio::test]
    async fn test_generate_customers() {
        let customers = TestDataLoader::generate_customers(3);
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].num_rows(), 3);
        assert_eq!(customers[0].num_columns(), 3);

        // Check schema
        let schema = customers[0].schema();
        assert_eq!(schema.field(0).name(), "customer_id");
        assert_eq!(schema.field(1).name(), "customer_name");
        assert_eq!(schema.field(2).name(), "city");
    }

    #[tokio::test]
    async fn test_data_loader_creation() {
        // Test the data loader methods individually without a full cluster
        let orders = TestDataLoader::generate_orders(10);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].num_rows(), 10);
    }
}
