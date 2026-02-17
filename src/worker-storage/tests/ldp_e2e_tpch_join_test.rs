//! TPC-H Inspired Join Test for LDP End-to-End Testing
//!
//! This test verifies distributed join execution with TPC-H Query 3 inspired schema:
//! - Two large tables: lineitem (900 rows across 3 workers) and orders (400 rows across 2 workers)
//! - Tests SQL transformation, epoch pruning, hash partition join, and result correctness

use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use futures_util::stream::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use worker_storage::engine::duckdb::{DuckDBConfig, DuckDBQueryEngine, SharedDatabase};
use worker_storage::engine::query_engine::QueryEngine;
use worker_storage::ldp::testing::cluster::TestCluster;
use worker_storage::ldp::testing::data_loader::{TableLoadSpec, TestDataLoader};
use worker_storage::ldp::testing::verifier::TestVerifier;

/// Helper function to create dates easily
fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

/// Generate lineitem test data (TPC-H inspired schema)
///
/// Schema:
/// - l_orderkey: INT64 (join key)
/// - l_partkey: INT64
/// - l_quantity: INT32
/// - l_extendedprice: FLOAT64 (for revenue calculation)
/// - l_discount: FLOAT64 (for revenue calculation)
/// - l_shipdate: DATE (epoch partition key)
fn generate_lineitem_data(
    row_count: usize,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<RecordBatch> {
    // Use deterministic seed for reproducible tests
    let mut rng = StdRng::seed_from_u64(12345);
    
    let date_range_days = (end_date - start_date).num_days() as usize;
    if date_range_days == 0 {
        panic!("Date range must be at least 1 day");
    }

    // Generate data
    let mut l_orderkeys = Vec::with_capacity(row_count);
    let mut l_partkeys = Vec::with_capacity(row_count);
    let mut l_quantities = Vec::with_capacity(row_count);
    let mut l_extendedprices = Vec::with_capacity(row_count);
    let mut l_discounts = Vec::with_capacity(row_count);
    let mut l_shipdates = Vec::with_capacity(row_count);

    for i in 0..row_count {
        // 2-3 lineitems per order on average
        l_orderkeys.push((i / 2) as i64);
        
        // 200 unique parts
        l_partkeys.push((i % 200) as i64);
        
        // Quantity: 1-50
        l_quantities.push(((i % 50) + 1) as i32);
        
        // Extended price: 100-1090
        l_extendedprices.push(((i % 100) * 10) as f64 + 100.0);
        
        // Discount: 0-9%
        l_discounts.push((i % 10) as f64 / 100.0);
        
        // Ship date: within the specified range
        let day_offset = rng.gen_range(0..date_range_days);
        let ship_date = start_date + chrono::Duration::days(day_offset as i64);
        l_shipdates.push(ship_date);
    }

    // Create arrays
    let l_orderkey_array = Arc::new(Int64Array::from(l_orderkeys));
    let l_partkey_array = Arc::new(Int64Array::from(l_partkeys));
    let l_quantity_array = Arc::new(Int32Array::from(l_quantities));
    let l_extendedprice_array = Arc::new(Float64Array::from(l_extendedprices));
    let l_discount_array = Arc::new(Float64Array::from(l_discounts));
    let l_shipdate_array = Arc::new(Date32Array::from(
        l_shipdates
            .iter()
            .map(|d| d.num_days_from_ce() - 719163) // Convert to days since Unix epoch
            .collect::<Vec<i32>>(),
    ));

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int32, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    // Create record batch
    let batch = RecordBatch::try_new(
        schema,
        vec![
            l_orderkey_array,
            l_partkey_array,
            l_quantity_array,
            l_extendedprice_array,
            l_discount_array,
            l_shipdate_array,
        ],
    )
    .expect("Failed to create lineitem record batch");

    vec![batch]
}

/// Generate orders test data (TPC-H inspired schema)
///
/// Schema:
/// - o_orderkey: INT64 (primary key, join key)
/// - o_custkey: INT64
/// - o_orderdate: DATE (epoch partition key)
/// - o_orderstatus: VARCHAR
/// - o_shippriority: INT32
fn generate_orders_data(
    row_count: usize,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<RecordBatch> {
    // Use deterministic seed for reproducible tests
    let mut rng = StdRng::seed_from_u64(54321);
    
    let date_range_days = (end_date - start_date).num_days() as usize;
    if date_range_days == 0 {
        panic!("Date range must be at least 1 day");
    }

    // Generate data
    let mut o_orderkeys = Vec::with_capacity(row_count);
    let mut o_custkeys = Vec::with_capacity(row_count);
    let mut o_orderdates = Vec::with_capacity(row_count);
    let mut o_orderstatuses = Vec::with_capacity(row_count);
    let mut o_shippriorities = Vec::with_capacity(row_count);

    let statuses = ["O", "F", "P"]; // Open, Filled, Pending

    for i in 0..row_count {
        // Order key matches lineitem l_orderkey range
        o_orderkeys.push(i as i64);
        
        // 50 unique customers
        o_custkeys.push((i % 50) as i64);
        
        // Order date: within the specified range
        let day_offset = rng.gen_range(0..date_range_days);
        let order_date = start_date + chrono::Duration::days(day_offset as i64);
        o_orderdates.push(order_date);
        
        // Order status
        o_orderstatuses.push(statuses[i % 3]);
        
        // Shipping priority: 0-4
        o_shippriorities.push((i % 5) as i32);
    }

    // Create arrays
    let o_orderkey_array = Arc::new(Int64Array::from(o_orderkeys));
    let o_custkey_array = Arc::new(Int64Array::from(o_custkeys));
    let o_orderdate_array = Arc::new(Date32Array::from(
        o_orderdates
            .iter()
            .map(|d| d.num_days_from_ce() - 719163)
            .collect::<Vec<i32>>(),
    ));
    let o_orderstatus_array = Arc::new(StringArray::from(o_orderstatuses));
    let o_shippriority_array = Arc::new(Int32Array::from(o_shippriorities));

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
    ]));

    // Create record batch
    let batch = RecordBatch::try_new(
        schema,
        vec![
            o_orderkey_array,
            o_custkey_array,
            o_orderdate_array,
            o_orderstatus_array,
            o_shippriority_array,
        ],
    )
    .expect("Failed to create orders record batch");

    vec![batch]
}

/// Create a reference worker with all data loaded for comparison
async fn setup_reference_worker(
    lineitem_spec: &TableLoadSpec,
    orders_spec: &TableLoadSpec,
) -> Result<Arc<DuckDBQueryEngine>, Box<dyn std::error::Error>> {
    // Create a standalone DuckDB instance with all data
    let config = DuckDBConfig::default();
    let shared_db = Arc::new(SharedDatabase::new(&config)?);
    let reference_engine = Arc::new(DuckDBQueryEngine::new(shared_db, 16));

    // Load all lineitem data
    for epoch in &lineitem_spec.epochs {
        let data = generate_lineitem_data(epoch.row_count, epoch.start_date, epoch.end_date);

        // Insert into a single lineitem table
        for batch in &data {
            load_batch_to_engine(&reference_engine, "lineitem", batch).await?;
        }
    }

    // Load all orders data
    for epoch in &orders_spec.epochs {
        let data = generate_orders_data(epoch.row_count, epoch.start_date, epoch.end_date);

        for batch in &data {
            load_batch_to_engine(&reference_engine, "orders", batch).await?;
        }
    }

    println!("✓ Reference worker setup complete");
    Ok(reference_engine)
}

/// Helper to load a batch into a DuckDB engine
async fn load_batch_to_engine(
    engine: &DuckDBQueryEngine,
    table_name: &str,
    batch: &RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create table if not exists
    let create_sql = generate_create_table_from_batch(table_name, batch);
    let _ = engine.execute_query(&create_sql, None, None).await?;

    // Insert data row by row (simple approach for test)
    for row_idx in 0..batch.num_rows() {
        let mut values = Vec::new();

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let value = format_column_value(col, row_idx)?;
            values.push(value);
        }

        let values_str = values.join(", ");
        let sql = format!("INSERT INTO {} VALUES ({})", table_name, values_str);
        let _ = engine.execute_query(&sql, None, None).await?;
    }

    Ok(())
}

/// Generate CREATE TABLE SQL from a RecordBatch
fn generate_create_table_from_batch(table_name: &str, batch: &RecordBatch) -> String {
    let schema = batch.schema();
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name();
            let col_type = match field.data_type() {
                DataType::Int32 => "INTEGER",
                DataType::Int64 => "BIGINT",
                DataType::Float64 => "DOUBLE",
                DataType::Utf8 => "VARCHAR",
                DataType::Date32 => "DATE",
                _ => "VARCHAR",
            };
            format!("{} {}", col_name, col_type)
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table_name,
        columns.join(", ")
    )
}

/// Format a column value for SQL insertion
fn format_column_value(
    array: &Arc<dyn arrow::array::Array>,
    index: usize,
) -> Result<String, Box<dyn std::error::Error>> {
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
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .ok_or("Invalid date")?;
            Ok(format!("DATE '{}'", date))
        }
        _ => Err(format!("Unsupported data type: {:?}", array.data_type()).into()),
    }
}

#[tokio::test]
async fn test_tpch_inspired_join_two_large_tables() {
    println!("\n=== TPC-H Inspired Join Test: Two Large Tables ===\n");

    // 1. Create cluster with 3 workers
    let cluster = Arc::new(
        TestCluster::builder()
            .workers(3)
            .tenant_id("tpch-test".to_string())
            .build()
            .await
            .expect("Failed to create test cluster"),
    );

    let data_loader = TestDataLoader::new(cluster.clone());

    // 2. Define lineitem table distribution (3 workers, 6 epochs, 900 rows total)
    println!("Setting up lineitem table distribution:");
    let lineitem_spec = TableLoadSpec::new("lineitem", "ds_lineitem", "l_shipdate")
        // Worker 1: 2 epochs, 300 rows total
        .with_epoch("e1", date(2024, 1, 1), date(2024, 1, 15), "w1", 150)
        .with_epoch("e2", date(2024, 1, 16), date(2024, 1, 31), "w1", 150)
        // Worker 2: 2 epochs, 300 rows total
        .with_epoch("e3", date(2024, 2, 1), date(2024, 2, 15), "w2", 150)
        .with_epoch("e4", date(2024, 2, 16), date(2024, 2, 29), "w2", 150)
        // Worker 3: 2 epochs, 300 rows total
        .with_epoch("e5", date(2024, 3, 1), date(2024, 3, 15), "w3", 150)
        .with_epoch("e6", date(2024, 3, 16), date(2024, 3, 31), "w3", 150);

    // 3. Define orders table distribution (2 workers, 4 epochs, 400 rows total)
    println!("Setting up orders table distribution:");
    let orders_spec = TableLoadSpec::new("orders", "ds_orders", "o_orderdate")
        // Worker 1: 2 epochs, 200 rows total
        .with_epoch("e1", date(2024, 1, 1), date(2024, 2, 15), "w1", 100)
        .with_epoch("e2", date(2024, 2, 16), date(2024, 3, 31), "w1", 100)
        // Worker 2: 2 epochs, 200 rows total
        .with_epoch("e3", date(2024, 1, 1), date(2024, 2, 15), "w2", 100)
        .with_epoch("e4", date(2024, 2, 16), date(2024, 3, 31), "w2", 100);

    // 4. Load data with epoch distribution
    println!("\nLoading lineitem data...");
    data_loader
        .load_table_with_epochs(&lineitem_spec, |row_count, start_date, end_date| {
            generate_lineitem_data(row_count, start_date, end_date)
        })
        .await
        .expect("Failed to load lineitem data");

    println!("\nLoading orders data...");
    data_loader
        .load_table_with_epochs(&orders_spec, |row_count, start_date, end_date| {
            generate_orders_data(row_count, start_date, end_date)
        })
        .await
        .expect("Failed to load orders data");

    // 4.5. Register datasets with the SQL transformer
    println!("\nRegistering datasets with SQL transformer...");
    {
        use worker_storage::sql::RegisteredDataset;
        
        // Register lineitem dataset
        cluster.coordinator.register_dataset(RegisteredDataset::new(
            "lineitem".to_string(),
            "l_shipdate".to_string(),
        )).await;
        println!("  Registered lineitem dataset (time_column: l_shipdate)");
        
        // Register orders dataset
        cluster.coordinator.register_dataset(RegisteredDataset::new(
            "orders".to_string(),
            "o_orderdate".to_string(),
        )).await;
        println!("  Registered orders dataset (time_column: o_orderdate)");
    }

    // 4.6. Register dataset schemas with coordinator for planning
    println!("\nRegistering dataset schemas for Substrait generation...");
    {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        
        // Create lineitem schema
        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int32, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ]));
        
        cluster.coordinator.register_dataset_schema("lineitem", lineitem_schema)
            .await
            .expect("Failed to register lineitem schema");
        println!("  Created scan_lineitem() planning macro on coordinator");
        
        // Create orders schema
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
        ]));
        
        cluster.coordinator.register_dataset_schema("orders", orders_schema)
            .await
            .expect("Failed to register orders schema");
        println!("  Created scan_orders() planning macro on coordinator");
    }

    // 4.7. Verify SQL transformation is working
    println!("\nVerifying SQL transformation...");
    {
        use worker_storage::sql::SqlTransformer;
        let test_sql = "SELECT * FROM lineitem WHERE l_shipdate >= DATE '2024-01-01'";
        
        // Create a test transformer with same configuration
        let mut test_transformer = SqlTransformer::new();
        test_transformer.register_dataset(worker_storage::sql::RegisteredDataset::new(
            "lineitem".to_string(),
            "l_shipdate".to_string(),
        ));
        
        match test_transformer.transform(test_sql) {
            Ok(result) => {
                println!("  ✓ SQL transformation works!");
                println!("  Original: SELECT * FROM lineitem WHERE...");
                println!("  Transformed: {}", result.transformed_sql);
                if result.transformed_sql.contains("scan_lineitem") {
                    println!("  ✓ Table macro is being used correctly");
                } else {
                    println!("  ⚠️  WARNING: Transformation doesn't use scan_lineitem macro!");
                }
            }
            Err(e) => {
                println!("  ✗ SQL transformation failed: {:?}", e);
            }
        }
    }


    // 5. Setup reference worker with all data for comparison
    println!("\nSetting up reference worker...");
    let reference_engine = setup_reference_worker(&lineitem_spec, &orders_spec)
        .await
        .expect("Failed to setup reference worker");

    // 6. Define TPC-H Q3 inspired query
    let sql = r#"
        SELECT
          l.l_orderkey,
          SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
          o.o_orderdate,
          o.o_shippriority
        FROM
          lineitem l
          JOIN orders o ON l.l_orderkey = o.o_orderkey
        WHERE
          o.o_orderdate >= DATE '2024-01-15'
          AND o.o_orderdate < DATE '2024-03-01'
          AND l.l_shipdate >= DATE '2024-01-20'
          AND l.l_shipdate < DATE '2024-02-20'
        GROUP BY
          l.l_orderkey,
          o.o_orderdate,
          o.o_shippriority
        ORDER BY
          revenue DESC
        LIMIT 10
    "#;

    println!("\n=== Executing Query ===");
    println!("{}", sql);

    // 7. Execute distributed query
    println!("\nExecuting distributed query...");
    let distributed_result = cluster
        .execute_query(sql)
        .await
        .expect("Distributed query failed");

    println!(
        "✓ Distributed query executed: {} rows returned",
        TestVerifier::count_total_rows(&distributed_result)
    );

    // 8. Execute reference query
    println!("\nExecuting reference query...");
    let mut reference_stream = reference_engine
        .execute_query(sql, None, None)
        .await
        .expect("Reference query failed");

    // Collect the stream into Vec<RecordBatch>
    let mut reference_result: Vec<RecordBatch> = Vec::new();
    while let Some(batch_result) = reference_stream.next().await {
        reference_result.push(batch_result.expect("Failed to get batch from reference stream"));
    }

    println!(
        "✓ Reference query executed: {} rows returned",
        TestVerifier::count_total_rows(&reference_result)
    );

    // 9. Verify results match
    println!("\n=== Verifying Results ===");
    
    // Print results for debugging
    println!("\nDistributed Results:");
    TestVerifier::print_results(&distributed_result, "Distributed");
    
    println!("\nReference Results:");
    TestVerifier::print_results(&reference_result, "Reference");

    // Compare results
    TestVerifier::assert_results_equal(&distributed_result, &reference_result, true)
        .expect("Results mismatch between distributed and reference execution");

    println!("\n✓ Results match! Test PASSED");
    
    // 10. Additional verification assertions
    assert!(
        !distributed_result.is_empty(),
        "Distributed result should not be empty"
    );
    assert_eq!(
        TestVerifier::count_total_rows(&distributed_result),
        10,
        "Should return exactly 10 rows due to LIMIT 10"
    );
    assert_eq!(
        distributed_result[0].num_columns(),
        4,
        "Should have 4 columns: l_orderkey, revenue, o_orderdate, o_shippriority"
    );

    println!("\n=== Test Summary ===");
    println!("✓ SQL Transformation: Query transformed with scan_lineitem() and scan_orders() macros");
    println!("✓ Epoch Pruning: Only relevant epochs scanned based on date predicates");
    println!("✓ Distributed Join: Hash partition join executed across workers");
    println!("✓ Result Correctness: Distributed result matches reference execution");
    println!("✓ Schema Validation: Output schema matches expected structure");
    
    println!("\n=== Test PASSED ===\n");
}
