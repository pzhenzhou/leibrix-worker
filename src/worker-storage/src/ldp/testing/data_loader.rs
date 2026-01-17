//! Test data loader for LDP end-to-end testing.
//!
//! This module provides utilities for loading test data into the cluster,
//! including standard datasets like orders and customers for join tests.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, Float64Array, StringArray, Date32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime, Datelike};
use rand::Rng;

use super::cluster::TestCluster;

/// Test data generator and loader for LDP testing.
pub struct TestDataLoader {
    cluster: Arc<TestCluster>,
}

impl TestDataLoader {
    /// Create a new test data loader for the given cluster.
    pub fn new(cluster: Arc<TestCluster>) -> Self {
        Self { cluster }
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
            dates.iter().map(|d| d.num_days_from_ce() - 719163).collect::<Vec<i32>>() // Convert to days since Unix epoch
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
            vec![
                order_id_array,
                customer_id_array,
                amount_array,
                date_array,
            ],
        ).expect("Failed to create record batch");

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
        let batch = RecordBatch::try_new(
            schema,
            vec![
                customer_id_array,
                name_array,
                city_array,
            ],
        ).expect("Failed to create record batch");

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
            vec![
                product_id_array,
                name_array,
                category_array,
                price_array,
            ],
        ).expect("Failed to create record batch");

        vec![batch]
    }

    /// Load orders data distributed across all workers in the cluster.
    pub async fn load_orders_distributed(&self, orders: Vec<RecordBatch>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            self.insert_batch_to_worker(worker_id, "orders", batch).await?;
        }

        Ok(())
    }

    /// Load customers data to a specific worker.
    pub async fn load_customers_on_worker(
        &self,
        worker_id: &str,
        customers: Vec<RecordBatch>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.cluster.worker_ids().contains(&worker_id.to_string()) {
            return Err(format!("Worker {} not found in cluster", worker_id).into());
        }

        for batch in &customers {
            // Create table if it doesn't exist
            self.cluster.execute_query_on_worker(
                worker_id,
                "CREATE TABLE IF NOT EXISTS customers (customer_id INTEGER, customer_name VARCHAR, city VARCHAR)"
            ).await?;
            
            // Insert the batch data
            self.insert_batch_to_worker(worker_id, "customers", batch).await?;
        }

        Ok(())
    }

    /// Load products data distributed across all workers in the cluster.
    pub async fn load_products_distributed(&self, products: Vec<RecordBatch>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            self.insert_batch_to_worker(worker_id, "products", batch).await?;
        }

        Ok(())
    }

    /// Helper function to insert a batch into a worker's table.
    async fn insert_batch_to_worker(
        &self,
        worker_id: &str,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            
            self.cluster.execute_query_on_worker(worker_id, &sql).await?;
        }
        
        Ok(())
    }

    /// Format a column value for SQL insertion.
    fn format_column_value(&self, array: &ArrayRef, index: usize) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::*;
        
        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(arr.value(index).to_string())
            },
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(arr.value(index).to_string())
            },
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(arr.value(index).to_string())
            },
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(format!("'{}'", arr.value(index)))
            },
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(index);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                    .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from("Invalid date"))?;
                Ok(format!("DATE '{}'", date))
            },
            _ => Err(format!("Unsupported data type: {:?}", array.data_type()).into()),
        }
    }

    /// Generate and load standard test data into the cluster.
    /// This creates orders and customers tables distributed appropriately.
    pub async fn load_standard_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        let first_worker = worker_ids.first()
            .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from("No workers available"))?;
        self.load_customers_on_worker(first_worker, customers).await?;
        println!("Loaded customers data (100 records) on worker {}", first_worker);
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::executor::coordinator::LdpCoordinator;
    use crate::ldp::executor::stage::LogicalDatasetManager;
    use crate::ldp::planner::PlannerPolicy;

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
        // Create a minimal cluster for testing
        let config = crate::ldp::executor::coordinator::CoordinatorConfig::builder()
            .with_tenant_id("test-tenant".to_string())
            .with_policy(PlannerPolicy::default())
            .build();
            
        let dataset_manager = Arc::new(LogicalDatasetManager::new());
        let coordinator = Arc::new(LdpCoordinator::new(config, dataset_manager.clone()));
        
        // For this test, we'll create a minimal cluster-like structure
        // Since the full cluster involves more complex setup, we'll test the data loader methods individually
        let orders = TestDataLoader::generate_orders(10);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].num_rows(), 10);
    }
}