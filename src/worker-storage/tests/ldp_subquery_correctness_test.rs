//! End-to-end correctness tests for subquery handling in LDP.
//!
//! This test suite verifies that various subquery types work correctly in distributed
//! query execution, including:
//! - Subqueries in WHERE clause (IN, EXISTS, NOT IN, NOT EXISTS)
//! - Scalar subqueries in SELECT
//! - Subqueries in FROM clause (derived tables)
//! - Correlated subqueries

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

fn generate_orders_data(
    row_count: usize,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<RecordBatch> {
    let mut o_orderkeys = Vec::with_capacity(row_count);
    let mut o_custkeys = Vec::with_capacity(row_count);
    let mut o_totalprice = Vec::with_capacity(row_count);
    let mut o_orderdate = Vec::with_capacity(row_count);
    let mut o_orderstatus = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;
    let statuses = ["O", "F", "P"];

    for i in 0..row_count {
        o_orderkeys.push(i as i64);
        o_custkeys.push((i % 50) as i32);
        o_totalprice.push(1000.0 + (i % 100) as f64 * 1000.0);

        let days_offset = if date_range_days > 0 {
            i as i32 % date_range_days
        } else {
            0
        };
        let order_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch =
            (order_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
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

fn generate_lineitem_data(
    row_count: usize,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<RecordBatch> {
    let mut l_orderkeys = Vec::with_capacity(row_count);
    let mut l_partkey = Vec::with_capacity(row_count);
    let mut l_quantity = Vec::with_capacity(row_count);
    let mut l_extendedprice = Vec::with_capacity(row_count);
    let mut l_shipdate = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        l_orderkeys.push((i / 3) as i64);
        l_partkey.push((i % 200) as i32);
        l_quantity.push((i % 50 + 1) as i32);
        l_extendedprice.push(100.0 + (i % 100) as f64 * 10.0);

        let days_offset = if date_range_days > 0 {
            i as i32 % date_range_days
        } else {
            0
        };
        let ship_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch =
            (ship_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
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
    )
    .unwrap()]
}

fn generate_customer_data(row_count: usize) -> Vec<RecordBatch> {
    let mut c_custkeys = Vec::with_capacity(row_count);
    let mut c_names = Vec::with_capacity(row_count);
    let mut c_acctbal = Vec::with_capacity(row_count);

    for i in 0..row_count {
        c_custkeys.push(i as i32);
        c_names.push(format!("Customer#{}", i));
        c_acctbal.push(1000.0 + (i % 100) as f64 * 100.0);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(c_custkeys)),
            Arc::new(StringArray::from(c_names)),
            Arc::new(Float64Array::from(c_acctbal)),
        ],
    )
    .unwrap()]
}

#[allow(clippy::arc_with_non_send_sync)]
async fn setup_test_cluster() -> anyhow::Result<Arc<TestCluster>> {
    let cluster = Arc::new(
        TestCluster::builder()
            .workers(3)
            .tenant_id("subquery-test".to_string())
            .build()
            .await
            .map_err(|e| anyhow!("{}", e))?,
    );

    let data_loader = TestDataLoader::new(cluster.clone());

    // Load orders data
    let orders_spec = TableLoadSpec::new("orders", "ds_orders", "o_orderdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 2, 15), "w1", 200)
        .with_epoch("e2", date(2024, 2, 16), date(2024, 3, 31), "w2", 200);
    data_loader
        .load_table_with_epochs(&orders_spec, generate_orders_data)
        .await
        .map_err(|e| anyhow!("{}", e))?;

    // Load lineitem data
    let lineitem_spec = TableLoadSpec::new("lineitem", "ds_lineitem", "l_shipdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 1, 31), "w1", 300)
        .with_epoch("e2", date(2024, 2, 1), date(2024, 2, 29), "w2", 300)
        .with_epoch("e3", date(2024, 3, 1), date(2024, 3, 31), "w3", 300);
    data_loader
        .load_table_with_epochs(&lineitem_spec, generate_lineitem_data)
        .await
        .map_err(|e| anyhow!("{}", e))?;

    // Load customer data to ALL workers (dimension table needs to be replicated)
    let customers = generate_customer_data(50);
    for worker_id in ["w1", "w2", "w3"] {
        cluster
            .load_data_to_worker(worker_id, "customer", customers.clone())
            .await
            .map_err(|e| anyhow!("{}", e))?;
    }

    // Register datasets
    cluster
        .coordinator
        .register_dataset(RegisteredDataset::new(
            "orders".to_string(),
            "o_orderdate".to_string(),
        ))
        .await;

    cluster
        .coordinator
        .register_dataset(RegisteredDataset::new(
            "lineitem".to_string(),
            "l_shipdate".to_string(),
        ))
        .await;

    Ok(cluster)
}

#[tokio::test]
async fn test_in_clause_with_subquery() {
    println!("\n=== Test: IN Clause with Subquery ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_custkey IN (
              SELECT DISTINCT c_custkey 
              FROM customer 
              WHERE c_acctbal > 5000
          )
        ORDER BY o_totalprice DESC
        LIMIT 20
    "#;

    println!("Executing IN clause with subquery:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 20, "Should respect LIMIT 20");

    println!(
        "✓ IN clause with subquery executed successfully: {} rows",
        row_count
    );
}

#[tokio::test]
async fn test_not_in_clause_with_subquery() {
    println!("\n=== Test: NOT IN Clause with Subquery ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_custkey NOT IN (
              SELECT c_custkey 
              FROM customer 
              WHERE c_acctbal < 3000
          )
        ORDER BY o_totalprice DESC
        LIMIT 20
    "#;

    println!("Executing NOT IN clause with subquery:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ NOT IN clause with subquery executed successfully");
}

#[tokio::test]
async fn test_exists_clause() {
    println!("\n=== Test: EXISTS Clause ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders o
        WHERE o_orderdate >= DATE '2024-01-01'
          AND EXISTS (
              SELECT 1 
              FROM lineitem l 
              WHERE l.l_orderkey = o.o_orderkey
                AND l.l_shipdate >= DATE '2024-01-01'
                AND l.l_extendedprice > 500
          )
        ORDER BY o_totalprice DESC
        LIMIT 15
    "#;

    println!("Executing EXISTS clause:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ EXISTS clause executed successfully");
}

#[tokio::test]
async fn test_not_exists_clause() {
    println!("\n=== Test: NOT EXISTS Clause ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders o
        WHERE o_orderdate >= DATE '2024-01-01'
          AND NOT EXISTS (
              SELECT 1 
              FROM lineitem l 
              WHERE l.l_orderkey = o.o_orderkey
                AND l.l_shipdate < DATE '2024-01-15'
          )
        ORDER BY o_orderkey
        LIMIT 20
    "#;

    println!("Executing NOT EXISTS clause:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ NOT EXISTS clause executed successfully");
}

#[tokio::test]
async fn test_scalar_subquery_in_select() {
    println!("\n=== Test: Scalar Subquery in SELECT ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_orderkey,
            o_totalprice,
            (SELECT COUNT(*) 
             FROM lineitem l 
             WHERE l.l_orderkey = o.o_orderkey
               AND l.l_shipdate >= DATE '2024-01-01') as lineitem_count
        FROM orders o
        WHERE o_orderdate >= DATE '2024-01-01'
        ORDER BY o_totalprice DESC
        LIMIT 25
    "#;

    println!("Executing scalar subquery in SELECT:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 25, "Should respect LIMIT 25");

    println!(
        "✓ Scalar subquery in SELECT executed successfully: {} rows",
        row_count
    );
}

#[tokio::test]
async fn test_subquery_in_from_clause() {
    println!("\n=== Test: Subquery in FROM Clause (Derived Table) ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            avg_price_range,
            COUNT(*) as order_count
        FROM (
            SELECT 
                CASE 
                    WHEN o_totalprice > 70000 THEN 'High'
                    WHEN o_totalprice > 35000 THEN 'Medium'
                    ELSE 'Low'
                END as avg_price_range
            FROM orders
            WHERE o_orderdate >= DATE '2024-01-01'
        ) AS price_categories
        GROUP BY avg_price_range
        ORDER BY order_count DESC
    "#;

    println!("Executing subquery in FROM clause:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 2, "Should have 2 columns");

    println!("✓ Subquery in FROM clause executed successfully");
}

#[tokio::test]
async fn test_nested_subqueries() {
    println!("\n=== Test: Nested Subqueries ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_totalprice
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_custkey IN (
              SELECT c_custkey 
              FROM customer
              WHERE c_acctbal > (
                  SELECT AVG(c_acctbal) * 1.5
                  FROM customer
              )
          )
        ORDER BY o_totalprice DESC
        LIMIT 15
    "#;

    println!("Executing nested subqueries:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 2, "Should have 2 columns");

    println!("✓ Nested subqueries executed successfully");
}

#[tokio::test]
async fn test_correlated_subquery_simple() {
    println!("\n=== Test: Simple Correlated Subquery ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders o
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_totalprice > (
              SELECT AVG(o2.o_totalprice)
              FROM orders o2
              WHERE o2.o_custkey = o.o_custkey
                AND o2.o_orderdate >= DATE '2024-01-01'
          )
        ORDER BY o_totalprice DESC
        LIMIT 20
    "#;

    println!("Executing correlated subquery:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ Correlated subquery executed successfully");
}

#[tokio::test]
async fn test_subquery_with_aggregation() {
    println!("\n=== Test: Subquery with Aggregation ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o_custkey,
            COUNT(*) as order_count,
            SUM(o_totalprice) as total_spent
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_orderkey IN (
              SELECT l_orderkey
              FROM lineitem
              WHERE l_shipdate >= DATE '2024-01-01'
              GROUP BY l_orderkey
              HAVING SUM(l_extendedprice) > 1000
          )
        GROUP BY o_custkey
        ORDER BY total_spent DESC
        LIMIT 10
    "#;

    println!("Executing subquery with aggregation:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ Subquery with aggregation executed successfully");
}

#[tokio::test]
async fn test_multiple_subqueries_in_where() {
    println!("\n=== Test: Multiple Subqueries in WHERE ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_custkey, o_totalprice
        FROM orders o
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_custkey IN (
              SELECT c_custkey 
              FROM customer 
              WHERE c_acctbal > 5000
          )
          AND o_orderkey IN (
              SELECT l_orderkey
              FROM lineitem
              WHERE l_shipdate >= DATE '2024-01-01'
                AND l_extendedprice > 300
          )
        ORDER BY o_totalprice DESC
        LIMIT 15
    "#;

    println!("Executing multiple subqueries in WHERE:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");

    println!("✓ Multiple subqueries in WHERE executed successfully");
}

#[tokio::test]
async fn test_subquery_with_union() {
    println!("\n=== Test: Subquery with UNION ===\n");

    let cluster = setup_test_cluster().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT o_orderkey, o_totalprice
        FROM orders
        WHERE o_orderdate >= DATE '2024-01-01'
          AND o_custkey IN (
              SELECT c_custkey FROM customer WHERE c_acctbal > 7000
              UNION
              SELECT c_custkey FROM customer WHERE c_acctbal < 2000
          )
        ORDER BY o_totalprice DESC
        LIMIT 20
    "#;

    println!("Executing subquery with UNION:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");

    // Query executed successfully
    assert_eq!(result[0].num_columns(), 2, "Should have 2 columns");

    println!("✓ Subquery with UNION executed successfully");
}
