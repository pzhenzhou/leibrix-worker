use std::path::PathBuf;
use std::time::Duration;
use duckdb::{Connection, DuckdbConnectionManager};
use r2d2::{Pool, PooledConnection};
use anyhow::Context;

mod helper;
pub mod pool;
pub mod storage_engine_impl;
mod memory_duckdb_runtime;
pub mod query_engine_impl;
pub mod substrait;

// Re-export commonly used types
pub use query_engine_impl::DuckDBQueryEngine;

/// A shared DuckDB database with connection pooling for high-performance concurrent access.
/// 
/// Design: Read-write separation with connection pool using in-memory database.
/// - One SharedDatabase instance is created and wrapped in Arc
/// - StorageEngine (writer) and QueryEngine (reader) share the same instance
/// - Uses r2d2 connection pool with DuckdbConnectionManager::memory()
/// - All pooled connections automatically share the same in-memory database
/// 
/// Performance Benefits:
/// - Connection reuse eliminates per-query connection overhead
/// - Pool bounds maximum connections to prevent resource exhaustion
/// - Prepared statement caches are preserved across queries
/// - Thread-safe concurrent access with MVCC isolation
/// 
/// Lifetime Management:
/// - Pool maintains connections for the lifetime of SharedDatabase
/// - In-memory database persists as long as the pool exists
/// - When SharedDatabase is dropped, pool closes all connections and database is freed
pub struct SharedDatabase {
    /// Logical name for debugging/logging (not used for connection)
    db_name: String,
    /// Connection pool - all connections share the same in-memory database
    pool: Pool<DuckdbConnectionManager>,
}

impl SharedDatabase {
    /// Creates a new shared database with connection pooling using config.
    /// 
    /// Uses DuckDB's in-memory database with connection pooling.
    /// All connections from the pool automatically share the same database instance.
    /// Use `Arc<SharedDatabase>` to share between StorageEngine and QueryEngine.
    pub fn new(config: &DuckDBConfig) -> anyhow::Result<Self> {
        // Create in-memory connection manager
        // All pooled connections will share the same in-memory database instance
        let manager = DuckdbConnectionManager::memory()
            .context("Failed to create in-memory connection manager")?;
        
        // Apply pool size from config, capped by max_pool_size
        let pool_size = config.pool_size.min(config.max_pool_size);
        
        // Keep half the pool warm, with a minimum of 1 to maintain database lifetime
        let min_idle = (pool_size / 2).max(1);
        
        // Build connection pool with timeouts and lifecycle management
        let pool = Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(min_idle))
            .connection_timeout(Duration::from_secs(30))      // Max wait for connection from pool
            .idle_timeout(Some(Duration::from_secs(600)))     // Close idle connections after 10 min
            .max_lifetime(Some(Duration::from_secs(1800)))    // Recycle connections after 30 min
            .build(manager)
            .context("Failed to create connection pool")?;
        
        // Generate a unique identifier for debugging/logging purposes
        let db_name = format!(":memory:leibrix_db_{}_{}", 
            std::process::id(), 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        
        Ok(Self { db_name, pool })
    }

    /// Gets a connection from the pool.
    /// 
    /// This is the primary method for obtaining connections.
    /// Returns a pooled connection that is automatically returned to the pool on drop.
    /// 
    /// Thread-safe: Pool handles concurrent access.
    /// The connection uses MVCC for isolation from other concurrent operations.
    pub fn get(&self) -> anyhow::Result<PooledConnection<DuckdbConnectionManager>> {
        self.pool.get()
            .map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))
    }

    /// Creates a new dedicated connection (not from pool).
    /// 
    /// ⚠️  WARNING: For in-memory databases created with `memory()`, this creates a NEW,
    /// separate in-memory database instance that does NOT share data with the pool!
    /// 
    /// Only use this if you specifically need an isolated database (e.g., for testing).
    /// For normal operations, use `get()` to obtain a pooled connection instead.
    /// 
    /// For StorageEngine's writer connection, consider using a pooled connection
    /// with a longer checkout time, or use `get()` and keep it for the duration needed.
    pub fn connect(&self) -> anyhow::Result<Connection> {
        // Creates a separate in-memory database - not shared with the pool!
        Connection::open(":memory:")
            .context("Failed to create dedicated in-memory connection")
    }
    
    /// Returns the database name for debugging purposes.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Returns current pool statistics for monitoring.
    pub fn pool_state(&self) -> PoolStats {
        let state = self.pool.state();
        PoolStats {
            connections: state.connections,
            idle_connections: state.idle_connections,
            max_size: self.pool.max_size(),
        }
    }
}

// Pool<DuckdbConnectionManager> is Send + Sync, so SharedDatabase is automatically Send + Sync

/// Pool statistics for monitoring connection pool health.
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    /// Total number of connections currently open
    pub connections: u32,
    /// Number of idle connections available for reuse
    pub idle_connections: u32,
    /// Maximum pool size configured
    pub max_size: u32,
}

/// Configuration for the DuckDB storage engine.
#[derive(Debug, Clone)]
pub struct DuckDBConfig {
    pub memory_limit_mb: Option<u64>,
    pub flush_rows_threshold: u64,
    pub channel_capacity: usize,
    pub tmp_dir: Option<PathBuf>,
    pub max_identifiers: usize,
    /// Connection pool size for QueryEngine
    pub pool_size: u32,
    /// Maximum allowed pool size (caps pool_size)
    pub max_pool_size: u32,
}

impl Default for DuckDBConfig {
    fn default() -> Self {
        Self {
            memory_limit_mb: Some(1024), // 1 GB
            flush_rows_threshold: 10_000,
            channel_capacity: 100,
            tmp_dir: None,
            max_identifiers: 1000,
            pool_size: 32,              // Increased for better concurrency
            max_pool_size: 128,         // Higher ceiling for burst capacity
        }
    }
}

impl DuckDBConfig {
    /// Creates a config with a custom pool size.
    /// The pool size will be automatically capped by max_pool_size.
    pub fn with_pool_size(mut self, size: u32) -> Self {
        self.pool_size = size.min(self.max_pool_size);
        self
    }
    
    /// Creates a config with custom memory limit in MB.
    pub fn with_memory_limit(mut self, limit_mb: u64) -> Self {
        self.memory_limit_mb = Some(limit_mb);
        self
    }
}
