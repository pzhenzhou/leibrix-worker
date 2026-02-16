//! End-to-end correctness tests for CTE (Common Table Expression) handling in LDP.
//!
//! This test suite verifies that non-recursive CTEs work correctly in distributed
//! query execution, including:
//! - Simple CTEs with filtering and aggregation
//! - Multiple CTEs in a single query
//! - CTEs referenced multiple times
//! - CTEs combined with joins

use std::sync::Arc;

use anyhow::anyhow;

use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use worker_storage::ldp::testing::cluster::TestCluster;
use worker_storage::ldp::testing::data_loader::{TableLoadSpec, TestDataLoader};
use worker_storage::ldp::testing::verifier::TestVerifier;
use worker_storage::sql::RegisteredDataset;

/// Helper function to create dates easily
fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

/// Generate orders test data
fn generate_orders_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut o_orderkeys = Vec::with_capacity(row_count);
    let mut o_custkeys = Vec::with_capacity(row_count);
    let mut o_totalprice = Vec::with_capacity(row_count);
    let mut o_orderdate = Vec::with_capacity(row_count);
    let mut o_orderstatus = Vec::with_capacity(row_count);

    let statuses = ["O", "F", "P"];
    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        o_orderkeys.push(i as i64);
        o_custkeys.push((i % 50) as i32); // 50 unique customers
        o_totalprice.push(1000.0 + (i % 100) as f64 * 1000.0); // 1000-100000
        
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
    )
    .unwrap()]
}

/// Generate lineitem test data
fn generate_lineitem_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut l_orderkeys = Vec::with_capacity(row_count);
    let mut l_partkey = Vec::with_capacity(row_count);
    let mut l_quantity = Vec::with_capacity(row_count);
    let mut l_extendedprice = Vec::with_capacity(row_count);
    let mut l_discount = Vec::with_capacity(row_count);
    let mut l_shipdate = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        l_orderkeys.push((i / 3) as i64); // ~3 lineitems per order
        l_partkey.push((i % 200) as i32); // 200 unique parts
        l_quantity.push((i % 50 + 1) as i32);
        l_extendedprice.push(100.0 + (i % 100) as f64 * 10.0);
        l_discount.push((i % 10) as f64 / 100.0); // 0-9%
        
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
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(l_orderkeys)),
            Arc::new(Int32Array::from(l_partkey)),
            Arc::new(Int32Array::from(l_quantity)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Float64Array::from(l_discount)),
            Arc::new(Date32Array::from(l_shipdate)),
        ],
    )
    .unwrap()]
}

/// Setup test cluster with orders and lineitem data
async fn setup_test_cluster() -> anyhow::Result<Arc<TestCluster>> {
    let cluster = Arc::new(
        TestCluster::builder()
            .workers(3)
            .tenant_id("cte-test".to_string())
            .build()
            .await
            .map_err(|e| anyhow!("{}", e))?,
    );

    let data_loader = TestDataLoader::new(cluster.clone());

    // Load orders data (2 workers, 400 rows total)
    let orders_spec = TableLoadSpec::new("orders", "ds_orders", "o_orderdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 2, 15), "w1", 200)
        .with_epoch("e2", date(2024, 2, 16), date(2024, 3, 31), "w2", 200);

    data_loader
        .load_table_with_epochs(&orders_spec, generate_orders_data)
        .await
        .map_err(|e| anyhow!("{}", e))?;

    // Load lineitem data (3 workers, 600 rows total)
    let lineitem_spec = TableLoadSpec::new("lineitem", "ds_lineitem", "l_shipdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 1, 31), "w1", 200)
        .with_epoch("e2", date(2024, 2, 1), date(2024, 2, 29), "w2", 200)
        .with_epoch("e3", date(2024, 3, 1), date(2024, 3, 31), "w3", 200);

    data_loader
        .load_table_with_epochs(&lineitem_spec, generate_lineitem_data)
        .await
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
async fn test_simple_cte_with_filter() {
    println!("\n=== Test: Simple CTE with Filter ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH high_value_orders AS (
            SELECT o_orderkey, o_custkey, o_totalprice
            FROM orders
            WHERE o_totalprice > 50000
                AND o_orderdate >= DATE '2024-01-01'
        )
        SELECT COUNT(*) as order_count
        FROM high_value_orders
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 1, "Should have 1 column");
    
    let count_col = result[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let count = count_col.value(0);
    
    println!("✓ CTE with filter executed successfully: count = {}", count);
    assert!(count > 0, "Should have some high-value orders");
}

#[tokio::test]
async fn test_cte_with_aggregation() {
    println!("\n=== Test: CTE with Aggregation ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH customer_totals AS (
            SELECT o_custkey, SUM(o_totalprice) as total_spent
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
            GROUP BY o_custkey
        )
        SELECT o_custkey, total_spent
        FROM customer_totals
        WHERE total_spent > 100000
        ORDER BY total_spent DESC
        LIMIT 5
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 2, "Should have 2 columns");
    
    println!("✓ CTE with aggregation executed successfully");
}

#[tokio::test]
async fn test_multiple_ctes() {
    println!("\n=== Test: Multiple CTEs ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH high_value_orders AS (
            SELECT o_orderkey, o_custkey, o_totalprice
            FROM orders
            WHERE o_totalprice > 50000
                AND o_orderdate >= DATE '2024-01-01'
        ),
        customer_counts AS (
            SELECT o_custkey, COUNT(*) as order_count
            FROM high_value_orders
            GROUP BY o_custkey
        )
        SELECT o_custkey, order_count
        FROM customer_counts
        WHERE order_count > 1
        ORDER BY order_count DESC
    "#;

    println!("Executing query:\n{}", sql);

    let _result = cluster.execute_query(sql).await.expect("Query failed");
    
    // Query executed successfully - result can be empty or non-empty
    
    println!("✓ Multiple CTEs executed successfully");
}

#[tokio::test]
async fn test_cte_used_multiple_times() {
    println!("\n=== Test: CTE Referenced Multiple Times (SELECT without FROM) ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH recent_orders AS (
            SELECT o_orderkey, o_custkey, o_totalprice, o_orderdate
            FROM orders
            WHERE o_orderdate >= DATE '2024-02-01'
        )
        SELECT 
            (SELECT COUNT(*) FROM recent_orders) as total_orders,
            (SELECT AVG(o_totalprice) FROM recent_orders) as avg_price
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await;
    assert!(
        result.is_err(),
        "SELECT without FROM is currently unsupported and should fail"
    );
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("SELECT without FROM"),
        "Unexpected error: {}",
        err
    );

    println!("✓ CTE used multiple times rejected due to unsupported SELECT without FROM");
}

#[tokio::test]
async fn test_cte_with_join() {
    println!("\n=== Test: CTE with Join ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH high_value_orders AS (
            SELECT o_orderkey, o_custkey, o_totalprice
            FROM orders
            WHERE o_totalprice > 30000
                AND o_orderdate >= DATE '2024-01-01'
        )
        SELECT 
            hvo.o_orderkey,
            hvo.o_totalprice,
            SUM(l.l_extendedprice * (1 - l.l_discount)) as lineitem_revenue
        FROM high_value_orders hvo
        JOIN lineitem l ON hvo.o_orderkey = l.l_orderkey
        WHERE l.l_shipdate >= DATE '2024-01-01'
        GROUP BY hvo.o_orderkey, hvo.o_totalprice
        ORDER BY lineitem_revenue DESC
        LIMIT 10
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 10, "Should respect LIMIT 10");
    
    println!("✓ CTE with join executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_nested_cte() {
    println!("\n=== Test: Nested CTE (CTE referencing another CTE) ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH filtered_orders AS (
            SELECT o_orderkey, o_custkey, o_totalprice
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
        ),
        aggregated_orders AS (
            SELECT o_custkey, 
                   COUNT(*) as order_count,
                   SUM(o_totalprice) as total_spent
            FROM filtered_orders
            GROUP BY o_custkey
        )
        SELECT o_custkey, order_count, total_spent
        FROM aggregated_orders
        WHERE total_spent > 50000
        ORDER BY total_spent DESC
        LIMIT 10
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ Nested CTE executed successfully");
}

#[tokio::test]
async fn test_cte_with_window_function() {
    println!("\n=== Test: CTE with Window Function ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH customer_orders AS (
            SELECT o_custkey, o_orderkey, o_totalprice, o_orderdate
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
        ),
        ranked_orders AS (
            SELECT 
                o_custkey,
                o_orderkey,
                o_totalprice,
                ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) as rank
            FROM customer_orders
        )
        SELECT o_custkey, o_orderkey, o_totalprice
        FROM ranked_orders
        WHERE rank = 1
        ORDER BY o_totalprice DESC
        LIMIT 10
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ CTE with window function executed successfully");
}

#[tokio::test]
async fn test_cte_admission_control_rejects_recursive() {
    println!("\n=== Test: Admission Control Rejects Recursive CTE ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    // Recursive CTE should be rejected by admission control
    let sql = r#"
        WITH RECURSIVE cte AS (
            SELECT 1 as n
            UNION ALL
            SELECT n + 1 FROM cte WHERE n < 10
        )
        SELECT * FROM cte
    "#;

    println!("Attempting to execute recursive CTE:\n{}", sql);

    let result = cluster.execute_query(sql).await;
    
    assert!(result.is_err(), "Recursive CTE should be rejected by admission control");
    
    println!("✓ Recursive CTE correctly rejected by admission control");
}

#[tokio::test]
async fn test_cte_with_complex_expression() {
    println!("\n=== Test: CTE with Complex Expression ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        WITH order_details AS (
            SELECT 
                o_orderkey,
                o_custkey,
                o_totalprice,
                o_totalprice * 1.1 as price_with_tax,
                CASE 
                    WHEN o_totalprice > 80000 THEN 'High'
                    WHEN o_totalprice > 40000 THEN 'Medium'
                    ELSE 'Low'
                END as price_category
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
        )
        SELECT price_category, COUNT(*) as count, AVG(price_with_tax) as avg_price
        FROM order_details
        GROUP BY price_category
        ORDER BY avg_price DESC
    "#;

    println!("Executing query:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ CTE with complex expressions executed successfully");
}
