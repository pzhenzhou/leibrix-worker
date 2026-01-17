//! DuckDB connection pooling for high-performance concurrent access.
//!
//! This module provides a connection pool specifically designed for DuckDB
//! to handle concurrent query execution efficiently.

use std::sync::Arc;
use std::time::Duration;

use duckdb::Connection;
use deadpool::managed::{Manager, Pool, PoolConfig, QueueMode, RecycleResult};
use tokio::sync::Semaphore;
use tracing::warn;

/// Configuration for the DuckDB connection pool.
#[derive(Debug, Clone)]
pub struct DuckDbPoolConfig {
    /// Maximum number of connections in the pool.
    pub max_size: usize,
    /// Initial number of connections to establish.
    pub initial_size: usize,
    /// Connection timeout in seconds.
    pub connection_timeout: Duration,
    /// Statement timeout in seconds.
    pub statement_timeout: Duration,
    /// Memory limit for each connection (in MB).
    pub memory_limit_mb: Option<u64>,
    /// Whether to enable query logging.
    pub enable_logging: bool,
}

impl Default for DuckDbPoolConfig {
    fn default() -> Self {
        Self {
            max_size: 32,
            initial_size: 4,
            connection_timeout: Duration::from_secs(30),
            statement_timeout: Duration::from_secs(60),
            memory_limit_mb: Some(1024), // 1GB default
            enable_logging: false,
        }
    }
}

impl DuckDbPoolConfig {
    /// Create a new config with the specified maximum pool size.
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Create a new config with the specified memory limit in MB.
    pub fn with_memory_limit(mut self, limit_mb: u64) -> Self {
        self.memory_limit_mb = Some(limit_mb);
        self
    }

    /// Create a new config with the specified statement timeout.
    pub fn with_statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }
}

/// Manager for DuckDB connections in the pool.
struct DuckDbConnectionManager {
    config: DuckDbPoolConfig,
}

impl DuckDbConnectionManager {
    /// Create a new connection manager with the given configuration.
    async fn new(config: DuckDbPoolConfig) -> anyhow::Result<Self> {
        Ok(Self { config })
    }
}

impl Manager for DuckDbConnectionManager {
    type Type = Connection;
    type Error = duckdb::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = Connection::open_in_memory()?;
        
        // Apply memory limits if specified
        if let Some(memory_limit_mb) = self.config.memory_limit_mb {
            let _ = conn.execute(&format!("SET max_memory = '{}MB'", memory_limit_mb), []);
        }
        
        // Set statement timeout
        let _ = conn.execute(
            &format!("SET statement_timeout = '{}s'", self.config.statement_timeout.as_secs()), 
            []
        );
        
        Ok(conn)
    }

    async fn recycle(&self, obj: &mut Self::Type, _metrics: &deadpool::managed::Metrics) -> RecycleResult<Self::Error> {
        // Reset connection state before recycling
        if let Err(e) = obj.execute_batch("RESET ALL") {
            warn!("Failed to reset connection: {}", e);
            return Err(deadpool::managed::RecycleError::Message("Reset failed".into()));
        }
        
        Ok(())
    }
}

/// DuckDB connection pool for concurrent access.
pub struct DuckDbConnectionPool {
    pool: Pool<DuckDbConnectionManager>,
    /// Semaphore to limit concurrent queries (separate from connection pool)
    query_semaphore: Arc<Semaphore>,
    config: DuckDbPoolConfig,
}

impl DuckDbConnectionPool {
    /// Create a new connection pool with the given configuration.
    pub async fn new(config: DuckDbPoolConfig) -> anyhow::Result<Self> {
        let manager = DuckDbConnectionManager::new(config.clone()).await?;
        let pool_config = PoolConfig {
            max_size: config.max_size,
            timeouts: deadpool::managed::Timeouts::default(),
            queue_mode: QueueMode::Fifo,
        };

        let pool = Pool::builder(manager)
            .config(pool_config)
            .build()?;

        // Pre-populate the pool with initial connections
        for _ in 0..config.initial_size {
            if let Ok(conn) = pool.get().await {
                drop(conn); // Return to pool
            }
        }

        let semaphore = Arc::new(Semaphore::new(config.max_size));

        Ok(Self {
            pool,
            query_semaphore: semaphore,
            config,
        })
    }

    /// Get a connection from the pool.
    pub async fn acquire(&self) -> anyhow::Result<DuckDbPooledConnection> {
        let permit = self.query_semaphore.clone().acquire_owned().await
            .map_err(|e| anyhow::anyhow!("Failed to acquire query permit: {}", e))?;

        let conn = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))?;

        Ok(DuckDbPooledConnection {
            conn: Some(conn),
            _permit: permit,
            config: self.config.clone(),
        })
    }

    /// Execute a Substrait plan using a connection from the pool.
    /// 
    /// **Note**: This is a placeholder. Full Substrait execution requires integration
    /// with DuckDB's Substrait extension.
    pub async fn execute_substrait(
        &self,
        _substrait_plan: &[u8],
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        anyhow::bail!("Substrait execution not yet integrated with connection pool")
    }

    /// Get current pool status.
    pub async fn status(&self) -> deadpool::managed::Status {
        self.pool.status()
    }

    /// Get configuration.
    pub fn config(&self) -> &DuckDbPoolConfig {
        &self.config
    }
}

/// A pooled DuckDB connection that automatically returns to the pool when dropped.
pub struct DuckDbPooledConnection {
    conn: Option<deadpool::managed::Object<DuckDbConnectionManager>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    config: DuckDbPoolConfig,
}

impl DuckDbPooledConnection {
    /// Get a reference to the underlying DuckDB connection.
    pub fn as_conn(&self) -> &Connection {
        self.conn.as_ref().expect("Connection should exist")
    }

    /// Get a mutable reference to the underlying DuckDB connection.
    pub fn as_conn_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("Connection should exist")
    }

    /// Execute a raw SQL query.
    pub fn execute(&mut self, sql: &str, params: &[&dyn duckdb::ToSql]) -> Result<usize, duckdb::Error> {
        self.as_conn_mut().execute(sql, params)
    }

    /// Prepare and execute a statement, returning rows that can be collected.
    pub fn prepare_execute<T, F>(
        &mut self,
        sql: &str,
        mapper: F,
    ) -> Result<Vec<T>, duckdb::Error>
    where
        F: FnMut(&duckdb::Row) -> Result<T, duckdb::Error>,
    {
        let mut stmt = self.as_conn_mut().prepare(sql)?;
        let rows = stmt.query_map([], mapper)?;
        rows.collect()
    }

    /// Get the configuration used for this connection.
    pub fn config(&self) -> &DuckDbPoolConfig {
        &self.config
    }
}

impl std::ops::Deref for DuckDbPooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.as_conn()
    }
}

impl std::ops::DerefMut for DuckDbPooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_conn_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_creation() {
        let config = DuckDbPoolConfig::default();
        let pool = DuckDbConnectionPool::new(config).await.unwrap();
        
        assert_eq!(pool.status().await.size, 4); // initial size
        assert_eq!(pool.config().max_size, 32);
    }

    #[tokio::test]
    async fn test_connection_acquisition() {
        let config = DuckDbPoolConfig::default().with_max_size(2);
        let pool = DuckDbConnectionPool::new(config).await.unwrap();
        
        // Acquire one connection
        let conn1 = pool.acquire().await.unwrap();
        assert!(conn1.execute("CREATE TABLE test (id INTEGER)", []).is_ok());
        
        // Acquire another connection
        let conn2 = pool.acquire().await.unwrap();
        assert!(conn2.execute("INSERT INTO test VALUES (1)", []).is_ok());
        
        // Try to acquire a third connection (should work as we return first one)
        drop(conn1);
        let conn3 = pool.acquire().await.unwrap();
        assert!(conn3.execute("SELECT * FROM test", []).is_ok());
    }

    #[tokio::test]
    async fn test_basic_query() {
        let config = DuckDbPoolConfig::default();
        let pool = DuckDbConnectionPool::new(config).await.unwrap();
        
        let mut conn = pool.acquire().await.unwrap();
        conn.execute("CREATE TABLE numbers (id INTEGER, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO numbers VALUES (1, 'one'), (2, 'two')", [])
            .unwrap();
        
        let results: Vec<i32> = conn.prepare_execute("SELECT id FROM numbers ORDER BY id", |row| {
            row.get(0)
        }).unwrap();
        
        assert_eq!(results, vec![1, 2]);
    }
}