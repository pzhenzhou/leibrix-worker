//! Basic Flight server example with DuckDB backend.
//!
//! This example demonstrates:
//! 1. Setting up a DuckDB query engine
//! 2. Creating and loading sample data into epoch tables
//! 3. Registering logical datasets with SQL transformer
//! 4. Starting an Arrow Flight server
//!
//! Run with: `cargo run --example basic_server`

use std::sync::Arc;

use worker_flight::{FlightServerBuilder, FlightServerConfig};
use worker_storage::engine::duckdb::{DuckDBConfig, DuckDBQueryEngine, SharedDatabase};
use worker_storage::sql::{RegisteredDataset, SqlTransformer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 Starting Arrow Flight server example...\n");

    // 1. Create in-memory DuckDB database
    println!("📊 Creating DuckDB database...");
    let db_config = DuckDBConfig::default();
    let shared_db = Arc::new(SharedDatabase::new(&db_config)?);
    println!("✓ Database created: {}", shared_db.db_name());

    // 2. Load sample data into epoch tables
    println!("\n📥 Loading sample data...");
    {
        let conn = shared_db.get()?;

        // Create an epoch table for sales_data
        conn.execute(
            "CREATE TABLE sales_data__epoch_20250101 (
                id INTEGER,
                dt DATE,
                product VARCHAR,
                amount DECIMAL(10, 2),
                country VARCHAR
            )",
            [],
        )?;

        conn.execute(
            "INSERT INTO sales_data__epoch_20250101 VALUES
                (1, '2025-01-01', 'Widget', 100.50, 'US'),
                (2, '2025-01-01', 'Gadget', 250.00, 'UK'),
                (3, '2025-01-01', 'Widget', 150.75, 'JP')",
            [],
        )?;

        // Create table macro
        conn.execute(
            "CREATE OR REPLACE MACRO scan_sales_data(start_dt DATE, end_excl DATE) AS TABLE (
                SELECT * FROM sales_data__epoch_20250101
                WHERE (DATE '2025-01-01' >= start_dt AND DATE '2025-01-01' < end_excl)
                  AND dt = DATE '2025-01-01'
            )",
            [],
        )?;

        println!("✓ Loaded 3 rows into sales_data__epoch_20250101");
    }

    // 3. Create query engine
    println!("\n🔧 Creating query engine...");
    let query_engine = Arc::new(DuckDBQueryEngine::new(shared_db, 32));
    println!("✓ Query engine ready with 32 max concurrent queries");

    // 4. Register logical datasets with SQL transformer
    println!("\n📝 Registering logical datasets...");
    let mut sql_transformer = SqlTransformer::new();
    sql_transformer.register_dataset(RegisteredDataset::new(
        "sales_data".to_string(),
        "dt".to_string(),
    ));
    let sql_transformer = Arc::new(sql_transformer);
    println!("✓ Registered logical dataset: sales_data");

    // 5. Configure Flight server
    let config = FlightServerConfig {
        bind_addr: "127.0.0.1:8815".parse()?,
        tls_config: None, // No TLS for this example
        max_message_size: 16 * 1024 * 1024,
        concurrency_limit: 100,
    };

    println!("\n🛫 Starting Arrow Flight server...");
    println!("   Address: {}", config.bind_addr);
    println!("   Tenant: test-tenant");
    println!("   Max message size: {} MB", config.max_message_size / 1024 / 1024);
    println!();
    println!("📡 Server ready to accept connections!");
    println!();
    println!("Example client connections:");
    println!("  Python: pyarrow.flight.FlightClient.connect('grpc://127.0.0.1:8815')");
    println!("  Rust:   FlightClient::new(Channel::from_static('http://127.0.0.1:8815'))");
    println!();
    println!("Try queries like:");
    println!("  SELECT * FROM sales_data WHERE dt = '2025-01-01'");
    println!("  SELECT country, SUM(amount) FROM sales_data WHERE dt = '2025-01-01' GROUP BY country");
    println!();
    println!("Press Ctrl+C to stop the server");
    println!();

    // 6. Start the server (this will block until Ctrl+C)
    FlightServerBuilder::new(
        config,
        query_engine,
        sql_transformer,
        "test-tenant".to_string(),
    )
    .run_with_shutdown(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        println!("\n\n🛑 Shutting down server...");
    })
    .await?;

    println!("✓ Server stopped");
    Ok(())
}


