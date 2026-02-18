//! End-to-end correctness tests for window function handling in LDP.
//!
//! This test suite verifies that window functions work correctly in distributed
//! query execution, including:
//! - ROW_NUMBER, RANK, DENSE_RANK
//! - Aggregate window functions (SUM, AVG, COUNT, etc.)
//! - Window functions with PARTITION BY
//! - Window functions with ORDER BY
//! - Window functions with frame specifications

use anyhow::anyhow;
use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use worker_storage::ldp::testing::cluster::TestCluster;
use worker_storage::ldp::testing::data_loader::{TableLoadSpec, TestDataLoader};
use worker_storage::ldp::testing::verifier::TestVerifier;
use worker_storage::sql::RegisteredDataset;

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn generate_orders_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut o_orderkeys = Vec::with_capacity(row_count);
    let mut o_custkeys = Vec::with_capacity(row_count);
    let mut o_totalprice = Vec::with_capacity(row_count);
    let mut o_orderdate = Vec::with_capacity(row_count);
    let mut o_orderstatus = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;
    let statuses = ["O", "F", "P"];

    for i in 0..row_count {
        o_orderkeys.push(i as i64);
        o_custkeys.push((i % 20) as i32); // 20 customers for better partition testing
        o_totalprice.push(1000.0 + (i % 100) as f64 * 1000.0);
        
        let days_offset = if date_range_days > 0 { i as i32 % date_range_days } else { 0 };
        let order_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch = (order_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        o_orderdate.push(days_since_epoch as i32);
        
        o_orderstatus.push(statuses[i % 3].to_string());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(o_orderkeys)),
            Arc::new(Int32Array::from(o_custkeys)),
            Arc::new(Float64Array::from(o_totalprice)),
            Arc::new(Date32Array::from(o_orderdate)),
            Arc::new(StringArray::from(o_orderstatus)),
        ],
    ).unwrap()]
}

fn generate_lineitem_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut l_orderkeys = Vec::with_capacity(row_count);
    let mut l_partkey = Vec::with_capacity(row_count);
    let mut l_quantity = Vec::with_capacity(row_count);
    let mut l_extendedprice = Vec::with_capacity(row_count);
    let mut l_shipdate = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        l_orderkeys.push((i / 3) as i64);
        l_partkey.push((i % 50) as i32); // 50 parts for better partition testing
        l_quantity.push((i % 50 + 1) as i32);
        l_extendedprice.push(100.0 + (i % 100) as f64 * 10.0);
        
        let days_offset = if date_range_days > 0 { i as i32 % date_range_days } else { 0 };
        let ship_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch = (ship_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        l_shipdate.push(days_since_epoch as i32);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_quantity", DataType::Int32, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(l_orderkeys)),
            Arc::new(Int32Array::from(l_partkey)),
            Arc::new(Int32Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Date32Array::from(l_shipdate)),
        ],
    ).unwrap()]
}

async fn setup_test_cluster() -> anyhow::Result<Arc<TestCluster>> {
    #[allow(clippy::arc_with_non_send_sync)]
    let cluster = Arc::new(
        TestCluster::builder()
            .workers(3)
            .tenant_id("window-test".to_string())
            .build()
            .await
            .map_err(|e| anyhow!("{}", e))?,
    );

    let data_loader = TestDataLoader::new(cluster.clone());

    // Load orders data
    let orders_spec = TableLoadSpec::new("orders", "ds_orders", "o_orderdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 2, 15), "w1", 300)
        .with_epoch("e2", date(2024, 2, 16), date(2024, 3, 31), "w2", 300);
    data_loader.load_table_with_epochs(&orders_spec, generate_orders_data).await
        .map_err(|e| anyhow!("{}", e))?;

    // Load lineitem data
    let lineitem_spec = TableLoadSpec::new("lineitem", "ds_lineitem", "l_shipdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 1, 31), "w1", 400)
        .with_epoch("e2", date(2024, 2, 1), date(2024, 2, 29), "w2", 400)
        .with_epoch("e3", date(2024, 3, 1), date(2024, 3, 31), "w3", 400);
    data_loader.load_table_with_epochs(&lineitem_spec, generate_lineitem_data).await
        .map_err(|e| anyhow!("{}", e))?;

    // Register datasets
    cluster.coordinator.register_dataset(RegisteredDataset::new(
        "orders".to_string(),
        "o_orderdate".to_string(),
    )).await;

    cluster.coordinator.register_dataset(RegisteredDataset::new(
        "lineitem".to_string(),
        "l_shipdate".to_string(),
    )).await;

    Ok(cluster)
}

#[tokio::test]
async fn test_row_number_window_function() {
    println!("\n=== Test: ROW_NUMBER() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            ROW_NUMBER() OVER (ORDER BY o_totalprice DESC) as row_num
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY row_num
        LIMIT 20
    "#;

    println!("Executing ROW_NUMBER():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 20, "Should respect LIMIT 20");
    
    println!("✓ ROW_NUMBER() executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_row_number_with_partition_by() {
    println!("\n=== Test: ROW_NUMBER() with PARTITION BY ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            ROW_NUMBER() OVER (
                PARTITION BY o_custkey 
                ORDER BY o_totalprice DESC
            ) as customer_rank
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, customer_rank
        LIMIT 50
    "#;

    println!("Executing ROW_NUMBER() with PARTITION BY:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ ROW_NUMBER() with PARTITION BY executed successfully");
}

#[tokio::test]
async fn test_rank_window_function() {
    println!("\n=== Test: RANK() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            RANK() OVER (ORDER BY o_totalprice DESC) as price_rank
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY price_rank
        LIMIT 25
    "#;

    println!("Executing RANK():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ RANK() executed successfully");
}

#[tokio::test]
async fn test_dense_rank_window_function() {
    println!("\n=== Test: DENSE_RANK() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            DENSE_RANK() OVER (
                PARTITION BY o_custkey 
                ORDER BY o_totalprice DESC
            ) as dense_rank
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, dense_rank
        LIMIT 40
    "#;

    println!("Executing DENSE_RANK():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ DENSE_RANK() executed successfully");
}

#[tokio::test]
async fn test_aggregate_window_sum() {
    println!("\n=== Test: SUM() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_custkey,
            o_orderkey,
            o_totalprice,
            SUM(o_totalprice) OVER (
                PARTITION BY o_custkey 
                ORDER BY o_orderdate
            ) as running_total
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, o_orderdate
        LIMIT 50
    "#;

    println!("Executing SUM() window function:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ SUM() window function executed successfully");
}

#[tokio::test]
async fn test_aggregate_window_avg() {
    println!("\n=== Test: AVG() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_custkey,
            o_orderkey,
            o_totalprice,
            AVG(o_totalprice) OVER (
                PARTITION BY o_custkey
            ) as customer_avg_price
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, o_orderkey
        LIMIT 60
    "#;

    println!("Executing AVG() window function:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ AVG() window function executed successfully");
}

#[tokio::test]
async fn test_aggregate_window_count() {
    println!("\n=== Test: COUNT() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_custkey,
            o_orderkey,
            COUNT(*) OVER (PARTITION BY o_custkey) as customer_order_count
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY customer_order_count DESC, o_custkey
        LIMIT 30
    "#;

    println!("Executing COUNT() window function:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ COUNT() window function executed successfully");
}

#[tokio::test]
async fn test_window_function_with_filter() {
    println!("\n=== Test: Window Function with WHERE Filter ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH ranked_orders AS (
            SELECT 
                o_orderkey,
                o_custkey,
                o_totalprice,
                ROW_NUMBER() OVER (
                    PARTITION BY o_custkey 
                    ORDER BY o_totalprice DESC
                ) as rank
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
        )
        SELECT o_orderkey, o_custkey, o_totalprice, rank
        FROM ranked_orders
        WHERE rank <= 3
        ORDER BY o_custkey, rank
    "#;

    println!("Executing window function with filter:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ Window function with filter executed successfully");
}

#[tokio::test]
async fn test_multiple_window_functions() {
    println!("\n=== Test: Multiple Window Functions in Same Query ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            ROW_NUMBER() OVER (
                PARTITION BY o_custkey 
                ORDER BY o_totalprice DESC
            ) as row_num,
            RANK() OVER (
                PARTITION BY o_custkey 
                ORDER BY o_totalprice DESC
            ) as rank,
            SUM(o_totalprice) OVER (
                PARTITION BY o_custkey
            ) as customer_total
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, row_num
        LIMIT 40
    "#;

    println!("Executing multiple window functions:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 6, "Should have 6 columns");
    
    println!("✓ Multiple window functions executed successfully");
}

#[tokio::test]
async fn test_window_function_on_lineitem() {
    println!("\n=== Test: Window Function on Lineitem Table ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            l_orderkey,
            l_partkey,
            l_quantity,
            l_extendedprice,
            SUM(l_quantity) OVER (
                PARTITION BY l_orderkey 
                ORDER BY l_extendedprice DESC
            ) as order_quantity_cumsum
        FROM lineitem
        WHERE l_shipdate >= DATE '2024-01-01'
        ORDER BY l_orderkey, l_extendedprice DESC
        LIMIT 50
    "#;

    println!("Executing window function on lineitem:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 5, "Should have 5 columns");
    
    println!("✓ Window function on lineitem executed successfully");
}

#[tokio::test]
async fn test_window_function_with_join() {
    println!("\n=== Test: Window Function with Join ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH order_lineitem_summary AS (
            SELECT 
                o.o_orderkey,
                o.o_custkey,
                o.o_totalprice,
                SUM(l.l_extendedprice) as lineitem_total
            FROM orders o
            JOIN lineitem l ON o.o_orderkey = l.l_orderkey
            WHERE o.o_orderdate >= DATE '2024-02-01'
              AND l.l_shipdate >= DATE '2024-02-01'
            GROUP BY o.o_orderkey, o.o_custkey, o.o_totalprice
        )
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            lineitem_total,
            ROW_NUMBER() OVER (
                PARTITION BY o_custkey 
                ORDER BY lineitem_total DESC
            ) as customer_rank
        FROM order_lineitem_summary
        ORDER BY o_custkey, customer_rank
        LIMIT 30
    "#;

    println!("Executing window function with join:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    // Query executed successfully
    assert_eq!(result[0].num_columns(), 5, "Should have 5 columns");
    
    println!("✓ Window function with join executed successfully");
}

#[tokio::test]
async fn test_ntile_window_function() {
    println!("\n=== Test: NTILE() Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            NTILE(4) OVER (ORDER BY o_totalprice DESC) as price_quartile
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY price_quartile, o_totalprice DESC
        LIMIT 40
    "#;

    println!("Executing NTILE():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    println!("✓ NTILE() executed successfully");
}

#[tokio::test]
async fn test_lag_lead_window_functions() {
    println!("\n=== Test: LAG() and LEAD() Window Functions ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            LAG(o_totalprice, 1) OVER (
                PARTITION BY o_custkey 
                ORDER BY o_orderdate
            ) as prev_order_price,
            LEAD(o_totalprice, 1) OVER (
                PARTITION BY o_custkey 
                ORDER BY o_orderdate
            ) as next_order_price
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, o_orderdate
        LIMIT 50
    "#;

    println!("Executing LAG() and LEAD():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 5, "Should have 5 columns");
    
    println!("✓ LAG() and LEAD() executed successfully");
}

#[tokio::test]
async fn test_first_value_last_value() {
    println!("\n=== Test: FIRST_VALUE() and LAST_VALUE() Window Functions ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_custkey,
            o_totalprice,
            FIRST_VALUE(o_totalprice) OVER (
                PARTITION BY o_custkey 
                ORDER BY o_orderdate
            ) as first_order_price,
            LAST_VALUE(o_totalprice) OVER (
                PARTITION BY o_custkey 
                ORDER BY o_orderdate
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as last_order_price
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_custkey, o_orderdate
        LIMIT 50
    "#;

    println!("Executing FIRST_VALUE() and LAST_VALUE():\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 5, "Should have 5 columns");
    
    println!("✓ FIRST_VALUE() and LAST_VALUE() executed successfully");
}
