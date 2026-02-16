//! Test cluster infrastructure for LDP end-to-end testing.
//!
//! This module provides a TestCluster that simulates a multi-worker environment
//! for testing distributed LDP execution.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use crate::engine::duckdb::arrow_utils::register_arrow_batches_persistent;
use crate::ldp::executor::coordinator::LdpCoordinator;
use crate::ldp::executor::stage::LocalStageExecutor;
use crate::ldp::planner::PlannerPolicy;
use crate::engine::duckdb::DuckDBQueryEngine;
use crate::ldp::testing::macro_helpers::LogicalDatasetManager;

/// Test cluster for simulating distributed LDP execution.
pub struct TestCluster {
    /// Workers in the cluster.
    pub workers: HashMap<String, TestWorker>,
    /// Coordinator for distributed execution.
    pub coordinator: Arc<LdpCoordinator<crate::ldp::planner::metadata::InMemoryMetadata>>,
    /// Planner metadata (shared with coordinator for registration after construction).
    pub metadata: Arc<crate::ldp::planner::metadata::InMemoryMetadata>,
    /// Dataset manager for logical table operations.
    pub dataset_manager: Arc<LogicalDatasetManager>,
    /// Cluster-wide configuration.
    pub config: TestClusterConfig,
}

/// Configuration for the test cluster.
#[derive(Debug, Clone)]
pub struct TestClusterConfig {
    /// Tenant ID for the test cluster.
    pub tenant_id: String,
    /// Number of workers in the cluster.
    pub worker_count: usize,
    /// Planner policy for LDP planning.
    pub policy: PlannerPolicy,
    /// Ports for each worker.
    pub ports: Vec<u16>,
}

impl Default for TestClusterConfig {
    fn default() -> Self {
        Self {
            tenant_id: "test-tenant".to_string(),
            worker_count: 3,
            policy: PlannerPolicy::default(),
            ports: vec![8815, 8816, 8817],
        }
    }
}

impl TestClusterConfig {
    /// Create a new config with the specified number of workers.
    pub fn with_workers(mut self, count: usize) -> Self {
        self.worker_count = count;
        // Adjust ports based on worker count
        self.ports = (0..count)
            .map(|i| 8815 + i as u16)
            .collect();
        self
    }

    /// Create a new config with the specified tenant ID.
    pub fn with_tenant(mut self, tenant_id: String) -> Self {
        self.tenant_id = tenant_id;
        self
    }

    /// Create a new config with the specified planner policy.
    pub fn with_policy(mut self, policy: PlannerPolicy) -> Self {
        self.policy = policy;
        self
    }
}

/// A test worker in the cluster.
pub struct TestWorker {
    /// Worker ID.
    pub worker_id: String,
    /// Local stage executor for this worker.
    pub executor: Arc<LocalStageExecutor>,
    /// DuckDB query engine for this worker.
    pub query_engine: Arc<DuckDBQueryEngine>,
    /// Port this worker is running on (for future Flight integration).
    pub port: u16,
}

impl TestWorker {
    /// Create a new test worker.
    pub fn new(worker_id: String, port: u16) -> Self {
        // Create a shared database
        let config = crate::engine::duckdb::DuckDBConfig::default();
        let shared_db = Arc::new(
            crate::engine::duckdb::SharedDatabase::new(&config)
                .expect("Failed to create shared database")
        );
        
        let query_engine = Arc::new(
            DuckDBQueryEngine::new(shared_db, 16) // 16 concurrent queries
        );
        let executor = Arc::new(LocalStageExecutor::new());

        Self {
            worker_id,
            executor,
            query_engine,
            port,
        }
    }

    /// Get the worker's endpoint for Flight connections.
    pub fn endpoint(&self) -> String {
        format!("http://localhost:{}", self.port)
    }
}

impl TestCluster {
    /// Create a new test cluster with the given configuration.
    pub async fn new(config: TestClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = config;
        let mut workers = HashMap::new();

        // Create workers
        for i in 0..config.worker_count {
            let worker_id = format!("w{}", i + 1);
            let port = config.ports[i];
            let worker = TestWorker::new(worker_id, port);
            workers.insert(worker.worker_id.clone(), worker);
        }

        // Ensure coordinator is a concrete worker for deterministic local execution.
        if config.policy.coordinator.is_empty() {
            config.policy.coordinator = "w1".to_string();
        }

        // Create dataset manager
        let dataset_manager = Arc::new(LogicalDatasetManager::new());

        // Create coordinator using cluster policy (including coordinator worker).
        // Use only 1 retry in tests to avoid 60+ second hangs on deterministic failures.
        let coordinator_config = crate::ldp::executor::coordinator::CoordinatorConfig::new(
            config.tenant_id.clone(),
        )
        .with_policy(config.policy.clone())
        .with_retries(1);
        let metadata = Arc::new(crate::ldp::planner::metadata::InMemoryMetadata::new());
        
        let coordinator = Arc::new(
            crate::ldp::executor::coordinator::LdpCoordinator::new(coordinator_config, metadata.clone())
                .map_err(|e| format!("Failed to create coordinator: {:?}", e))?
        );

        let cluster = Self {
            workers,
            coordinator,
            metadata,
            dataset_manager,
            config,
        };

        // Wire worker databases into the coordinator so stage SQL can read local catalogs.
        for (worker_id, worker) in &cluster.workers {
            cluster.coordinator.register_worker_database(
                worker_id.clone(),
                worker.query_engine.shared_db.clone(),
            );
        }

        info!(
            "Created test cluster with {} workers and coordinator",
            cluster.config.worker_count
        );

        Ok(cluster)
    }

    /// Get a worker by ID.
    pub fn get_worker(&self, worker_id: &str) -> Option<&TestWorker> {
        self.workers.get(worker_id)
    }

    /// Get mutable reference to a worker by ID.
    pub fn get_worker_mut(&mut self, worker_id: &str) -> Option<&mut TestWorker> {
        self.workers.get_mut(worker_id)
    }

    /// Get all worker IDs.
    pub fn worker_ids(&self) -> Vec<String> {
        self.workers.keys().cloned().collect()
    }

    /// Execute a query using the coordinator.
    pub async fn execute_query(
        &self,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Box<dyn std::error::Error>> {
        // Use the coordinator's execute_query API which handles the full pipeline
        let result = self.coordinator.execute_query(sql).await
            .map_err(|e| format!("Query execution failed: {:?}", e))?;
        Ok(result.batches)
    }

    /// Execute a query using a specific worker (for reference comparison).
    pub async fn execute_query_on_worker(
        &self,
        worker_id: &str,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let worker = self
            .workers
            .get(worker_id)
            .ok_or_else(|| format!("Worker {} not found", worker_id))?;

        // Execute query using the worker's query engine's shared database
        let shared_db = worker.query_engine.shared_db.clone();
        let sql_owned = sql.to_string();
        
        let batches = tokio::task::spawn_blocking(move || -> Result<Vec<arrow::record_batch::RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
            // Get connection inside spawn_blocking to avoid Send issues
            let conn = shared_db.get()?;
            let mut stmt = conn.prepare(&sql_owned)?;
            let arrow_batches = stmt.query_arrow([])?;
            
            let mut result = Vec::new();
            for batch in arrow_batches {
                result.push(batch);
            }
            Ok(result)
        }).await.map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn std::error::Error + Send + Sync>)??;
        
        Ok(batches)
    }

    /// Load data to a specific worker.
    pub async fn load_data_to_worker(
        &self,
        worker_id: &str,
        table_name: &str,
        data: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker = self
            .workers
            .get(worker_id)
            .ok_or_else(|| format!("Worker {} not found", worker_id))?;

        let shared_db = worker.query_engine.shared_db.clone();
        let table_name_owned = table_name.to_string();
        
        // Register the table in the worker's DuckDB instance
        tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let conn = shared_db.get()?;
            register_arrow_batches_persistent(&conn, &table_name_owned, &data)?;
            Ok(())
        }).await??;

        Ok(())
    }

    /// Load data distributed across workers.
    pub async fn load_distributed_data(
        &self,
        table_name: &str,
        data_chunks: Vec<(String, Vec<arrow::record_batch::RecordBatch>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (worker_id, data) in data_chunks {
            self.load_data_to_worker(&worker_id, table_name, data).await?;
        }
        Ok(())
    }

    /// Shutdown the cluster and clean up resources.
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down test cluster");
        Ok(())
    }
}

impl TestCluster {
    /// Create a builder for constructing a test cluster.
    pub fn builder() -> TestClusterBuilder {
        TestClusterBuilder::new()
    }
}

/// Builder for TestCluster.
pub struct TestClusterBuilder {
    config: TestClusterConfig,
}

impl TestClusterBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: TestClusterConfig::default(),
        }
    }

    /// Set the number of workers in the cluster.
    pub fn workers(mut self, count: usize) -> Self {
        self.config = self.config.with_workers(count);
        self
    }

    /// Set the tenant ID for the cluster.
    pub fn tenant_id(mut self, tenant_id: String) -> Self {
        self.config = self.config.with_tenant(tenant_id);
        self
    }

    /// Set the planner policy for the cluster.
    pub fn policy(mut self, policy: PlannerPolicy) -> Self {
        self.config = self.config.with_policy(policy);
        self
    }

    /// Set the ports for the workers.
    pub fn ports(mut self, ports: Vec<u16>) -> Self {
        self.config.worker_count = ports.len();
        self.config.ports = ports;
        self
    }

    /// Build the test cluster.
    pub async fn build(self) -> Result<TestCluster, Box<dyn std::error::Error>> {
        TestCluster::new(self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                std::sync::Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "David", "Eve",
                ])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_cluster_creation() {
        let cluster = TestCluster::builder()
            .workers(3)
            .tenant_id("test-tenant".to_string())
            .build()
            .await
            .unwrap();

        assert_eq!(cluster.worker_ids().len(), 3);
        assert!(cluster.get_worker("w1").is_some());
        assert!(cluster.get_worker("w2").is_some());
        assert!(cluster.get_worker("w3").is_some());
        assert!(cluster.get_worker("w4").is_none());
    }

    #[tokio::test]
    async fn test_cluster_load_and_query() {
        let cluster = TestCluster::builder()
            .workers(2)
            .tenant_id("test-tenant".to_string())
            .build()
            .await
            .unwrap();

        let batch = create_test_batch();

        // Load data to worker w1
        cluster
            .load_data_to_worker("w1", "test_table", vec![batch])
            .await
            .unwrap();

        // Query the data
        let results = cluster
            .execute_query_on_worker("w1", "SELECT COUNT(*) FROM test_table")
            .await
            .unwrap();

        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_cluster_builder_pattern() {
        let config = TestClusterConfig::default()
            .with_workers(2)
            .with_tenant("builder-test".to_string());

        assert_eq!(config.worker_count, 2);
        assert_eq!(config.tenant_id, "builder-test");
        assert_eq!(config.ports, vec![8815, 8816]);
    }
}