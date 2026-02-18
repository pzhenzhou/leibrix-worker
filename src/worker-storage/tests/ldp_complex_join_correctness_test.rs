//! End-to-end correctness tests for complex join operations in LDP.
//!
//! This test suite verifies that various join types work correctly in distributed
//! query execution, including:
//! - Multi-way joins (3+ tables)
//! - Self-joins
//! - Outer joins (LEFT, RIGHT, FULL)
//! - Cross joins
//! - Joins with complex predicates

use anyhow::anyhow;
use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;

use worker_storage::ldp::testing::cluster::TestCluster;
use worker_storage::ldp::testing::data_loader::{TableLoadSpec, TestDataLoader};
use worker_storage::ldp::testing::verifier::TestVerifier;
use worker_storage::ldp::{Distribution, StatsSource};
use worker_storage::sql::RegisteredDataset;

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

/// Generate orders data
fn generate_orders_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut o_orderkeys = Vec::with_capacity(row_count);
    let mut o_custkeys = Vec::with_capacity(row_count);
    let mut o_totalprice = Vec::with_capacity(row_count);
    let mut o_orderdate = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        o_orderkeys.push(i as i64);
        o_custkeys.push((i % 50) as i32);
        o_totalprice.push(1000.0 + (i % 100) as f64 * 1000.0);
        
        let days_offset = if date_range_days > 0 { i as i32 % date_range_days } else { 0 };
        let order_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch = (order_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        o_orderdate.push(days_since_epoch as i32);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(o_orderkeys)),
            Arc::new(Int32Array::from(o_custkeys)),
            Arc::new(Float64Array::from(o_totalprice)),
            Arc::new(Date32Array::from(o_orderdate)),
        ],
    ).unwrap()]
}

/// Generate lineitem data
fn generate_lineitem_data(row_count: usize, start_date: NaiveDate, end_date: NaiveDate) -> Vec<RecordBatch> {
    let mut l_orderkeys = Vec::with_capacity(row_count);
    let mut l_partkey = Vec::with_capacity(row_count);
    let mut l_suppkey = Vec::with_capacity(row_count);
    let mut l_extendedprice = Vec::with_capacity(row_count);
    let mut l_shipdate = Vec::with_capacity(row_count);

    let date_range_days = (end_date - start_date).num_days() as i32;

    for i in 0..row_count {
        l_orderkeys.push((i / 3) as i64); // ~3 lineitems per order
        l_partkey.push((i % 200) as i32);
        l_suppkey.push((i % 50) as i32);
        l_extendedprice.push(100.0 + (i % 100) as f64 * 10.0);
        
        let days_offset = if date_range_days > 0 { i as i32 % date_range_days } else { 0 };
        let ship_date = start_date + chrono::Duration::days(days_offset as i64);
        let days_since_epoch = (ship_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        l_shipdate.push(days_since_epoch as i32);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(l_orderkeys)),
            Arc::new(Int32Array::from(l_partkey)),
            Arc::new(Int32Array::from(l_suppkey)),
            Arc::new(Float64Array::from(l_extendedprice)),
            Arc::new(Date32Array::from(l_shipdate)),
        ],
    ).unwrap()]
}

/// Generate customer data (dimension table, not time-partitioned)
fn generate_customer_data(row_count: usize) -> Vec<RecordBatch> {
    let mut c_custkeys = Vec::with_capacity(row_count);
    let mut c_names = Vec::with_capacity(row_count);
    let mut c_nationkey = Vec::with_capacity(row_count);

    for i in 0..row_count {
        c_custkeys.push(i as i32);
        c_names.push(format!("Customer#{}", i));
        c_nationkey.push((i % 25) as i32); // 25 nations
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(c_custkeys)),
            Arc::new(StringArray::from(c_names)),
            Arc::new(Int32Array::from(c_nationkey)),
        ],
    ).unwrap()]
}

/// Generate part data (dimension table)
fn generate_part_data(row_count: usize) -> Vec<RecordBatch> {
    let mut p_partkeys = Vec::with_capacity(row_count);
    let mut p_names = Vec::with_capacity(row_count);
    let mut p_retailprice = Vec::with_capacity(row_count);

    for i in 0..row_count {
        p_partkeys.push(i as i32);
        p_names.push(format!("Part#{}", i));
        p_retailprice.push(100.0 + (i % 100) as f64 * 10.0);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(p_partkeys)),
            Arc::new(StringArray::from(p_names)),
            Arc::new(Float64Array::from(p_retailprice)),
        ],
    ).unwrap()]
}

/// Generate supplier data (dimension table)
fn generate_supplier_data(row_count: usize) -> Vec<RecordBatch> {
    let mut s_suppkeys = Vec::with_capacity(row_count);
    let mut s_names = Vec::with_capacity(row_count);
    let mut s_nationkey = Vec::with_capacity(row_count);

    for i in 0..row_count {
        s_suppkeys.push(i as i32);
        s_names.push(format!("Supplier#{}", i));
        s_nationkey.push((i % 25) as i32);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int32, false),
    ]));

    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(s_suppkeys)),
            Arc::new(StringArray::from(s_names)),
            Arc::new(Int32Array::from(s_nationkey)),
        ],
    ).unwrap()]
}

async fn setup_test_cluster_with_all_tables() -> anyhow::Result<Arc<TestCluster>> {
    #[allow(clippy::arc_with_non_send_sync)]
    let cluster = Arc::new(
        TestCluster::builder()
            .workers(3)
            .tenant_id("complex-join-test".to_string())
            .build()
            .await
            .map_err(|e| anyhow!("{}", e))?,
    );

    let data_loader = TestDataLoader::new(cluster.clone());

    // Load fact tables (time-partitioned)
    let orders_spec = TableLoadSpec::new("orders", "ds_orders", "o_orderdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 2, 15), "w1", 200)
        .with_epoch("e2", date(2024, 2, 16), date(2024, 3, 31), "w2", 200);
    data_loader.load_table_with_epochs(&orders_spec, generate_orders_data).await
        .map_err(|e| anyhow!("{}", e))?;

    let lineitem_spec = TableLoadSpec::new("lineitem", "ds_lineitem", "l_shipdate")
        .with_epoch("e1", date(2024, 1, 1), date(2024, 1, 31), "w1", 300)
        .with_epoch("e2", date(2024, 2, 1), date(2024, 2, 29), "w2", 300)
        .with_epoch("e3", date(2024, 3, 1), date(2024, 3, 31), "w3", 300);
    data_loader.load_table_with_epochs(&lineitem_spec, generate_lineitem_data).await
        .map_err(|e| anyhow!("{}", e))?;

    // Replicate dimension tables to all workers so every stage can access them
    let worker_ids = cluster.worker_ids();
    let customers = generate_customer_data(50);
    for wid in &worker_ids {
        cluster.load_data_to_worker(wid, "customer", customers.clone()).await
            .map_err(|e| anyhow!("{}", e))?;
    }

    let parts = generate_part_data(200);
    for wid in &worker_ids {
        cluster.load_data_to_worker(wid, "part", parts.clone()).await
            .map_err(|e| anyhow!("{}", e))?;
    }

    let suppliers = generate_supplier_data(50);
    for wid in &worker_ids {
        cluster.load_data_to_worker(wid, "supplier", suppliers.clone()).await
            .map_err(|e| anyhow!("{}", e))?;
    }

    // Register dimension table metadata as Replicated
    cluster.metadata.register_table_stats(
        "customer",
        worker_ids.clone(),
        Distribution::Replicated { workers: worker_ids.clone() },
        50, 5_000,
        StatsSource::Exact,
    );
    cluster.metadata.register_table_stats(
        "part",
        worker_ids.clone(),
        Distribution::Replicated { workers: worker_ids.clone() },
        200, 20_000,
        StatsSource::Exact,
    );
    cluster.metadata.register_table_stats(
        "supplier",
        worker_ids.clone(),
        Distribution::Replicated { workers: worker_ids.clone() },
        50, 5_000,
        StatsSource::Exact,
    );

    // Register time-partitioned datasets
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
async fn test_three_way_join() {
    println!("\n=== Test: Three-Way Join (Orders-Lineitem-Customer) ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            c.c_name,
            SUM(l.l_extendedprice) as total_price
        FROM orders o
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        JOIN customer c ON o.o_custkey = c.c_custkey
        WHERE o.o_orderdate >= DATE '2024-01-15'
          AND l.l_shipdate >= DATE '2024-01-15'
        GROUP BY o.o_orderkey, c.c_name
        ORDER BY total_price DESC
        LIMIT 20
    "#;

    println!("Executing three-way join:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 20, "Should respect LIMIT 20");
    
    println!("✓ Three-way join executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_four_way_join() {
    println!("\n=== Test: Four-Way Join (Orders-Lineitem-Part-Supplier) ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            p.p_name,
            s.s_name,
            l.l_extendedprice
        FROM orders o
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        JOIN part p ON l.l_partkey = p.p_partkey
        JOIN supplier s ON l.l_suppkey = s.s_suppkey
        WHERE o.o_orderdate >= DATE '2024-02-01'
          AND l.l_shipdate >= DATE '2024-02-01'
          AND p.p_retailprice > 500
        ORDER BY l.l_extendedprice DESC
        LIMIT 15
    "#;

    println!("Executing four-way join:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count > 0, "Four-way join should produce rows (matching data exists across tables)");
    assert!(row_count <= 15, "Should respect LIMIT 15");
    
    println!("✓ Four-way join executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_self_join() {
    println!("\n=== Test: Self-Join (Find related orders from same customer) ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o1.o_orderkey as order1,
            o2.o_orderkey as order2,
            o1.o_custkey,
            o1.o_totalprice as price1,
            o2.o_totalprice as price2
        FROM orders o1
        JOIN orders o2 ON o1.o_custkey = o2.o_custkey
        WHERE o1.o_orderkey < o2.o_orderkey
          AND o1.o_orderdate >= DATE '2024-01-01'
          AND o2.o_orderdate >= DATE '2024-01-01'
        ORDER BY o1.o_custkey, o1.o_orderkey
        LIMIT 10
    "#;

    println!("Executing self-join:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    // Query executed successfully
    assert_eq!(result[0].num_columns(), 5, "Should have 5 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    println!("✓ Self-join executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_left_outer_join() {
    println!("\n=== Test: LEFT OUTER JOIN ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            o.o_totalprice,
            l.l_orderkey as lineitem_orderkey,
            l.l_extendedprice
        FROM orders o
        LEFT JOIN lineitem l ON o.o_orderkey = l.l_orderkey 
            AND l.l_shipdate >= DATE '2024-01-01'
        WHERE o.o_orderdate >= DATE '2024-01-01'
          AND o.o_orderdate < DATE '2024-02-01'
        ORDER BY o.o_orderkey
        LIMIT 50
    "#;

    println!("Executing LEFT OUTER JOIN:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    println!("✓ LEFT OUTER JOIN executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_right_outer_join() {
    println!("\n=== Test: RIGHT OUTER JOIN ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            o.o_totalprice,
            l.l_orderkey,
            l.l_extendedprice
        FROM orders o
        RIGHT JOIN lineitem l ON o.o_orderkey = l.l_orderkey
            AND o.o_orderdate >= DATE '2024-01-01'
        WHERE l.l_shipdate >= DATE '2024-01-01'
          AND l.l_shipdate < DATE '2024-02-01'
        ORDER BY l.l_orderkey
        LIMIT 50
    "#;

    println!("Executing RIGHT OUTER JOIN:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    println!("✓ RIGHT OUTER JOIN executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_full_outer_join() {
    println!("\n=== Test: FULL OUTER JOIN ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            l.l_orderkey,
            COALESCE(o.o_totalprice, 0) as order_price,
            COALESCE(l.l_extendedprice, 0) as lineitem_price
        FROM orders o
        FULL OUTER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE (o.o_orderdate >= DATE '2024-03-01' OR o.o_orderdate IS NULL)
          AND (l.l_shipdate >= DATE '2024-03-01' OR l.l_shipdate IS NULL)
        LIMIT 100
    "#;

    println!("Executing FULL OUTER JOIN:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 4, "Should have 4 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    println!("✓ FULL OUTER JOIN executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_join_with_complex_predicate() {
    println!("\n=== Test: Join with Complex Predicate ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            l.l_extendedprice,
            o.o_totalprice
        FROM orders o
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATE '2024-01-01'
          AND l.l_shipdate >= DATE '2024-01-01'
          AND l.l_extendedprice > (o.o_totalprice / 10)
        ORDER BY l.l_extendedprice DESC
        LIMIT 20
    "#;

    println!("Executing join with complex predicate:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ Join with complex predicate executed successfully");
}

#[tokio::test]
async fn test_cross_join_with_filter() {
    println!("\n=== Test: Cross Join with Filter (Cartesian Product) ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    // Cross join with filter to keep result size manageable
    let sql = r#"
        SELECT 
            o.o_orderkey,
            c.c_custkey,
            c.c_name
        FROM orders o
        CROSS JOIN customer c
        WHERE o.o_orderdate >= DATE '2024-03-01'
          AND o.o_custkey = c.c_custkey
        LIMIT 30
    "#;

    println!("Executing cross join with filter:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    let row_count = TestVerifier::count_total_rows(&result);
    assert!(row_count <= 30, "Should respect LIMIT 30");
    
    println!("✓ Cross join executed successfully: {} rows", row_count);
}

#[tokio::test]
async fn test_multiple_joins_with_aggregation() {
    println!("\n=== Test: Multiple Joins with Aggregation ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            c.c_name,
            COUNT(DISTINCT o.o_orderkey) as order_count,
            SUM(l.l_extendedprice) as total_revenue
        FROM customer c
        JOIN orders o ON c.c_custkey = o.o_custkey
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATE '2024-01-01'
          AND l.l_shipdate >= DATE '2024-01-01'
        GROUP BY c.c_name
        HAVING COUNT(DISTINCT o.o_orderkey) > 2
        ORDER BY total_revenue DESC
        LIMIT 10
    "#;

    println!("Executing multiple joins with aggregation:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    // Query executed successfully
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ Multiple joins with aggregation executed successfully");
}

#[tokio::test]
async fn test_join_with_case_expression() {
    println!("\n=== Test: Join with CASE Expression ===\n");

    let cluster = setup_test_cluster_with_all_tables().await.expect("Failed to setup cluster");

    let sql = r#"
        SELECT 
            o.o_orderkey,
            l.l_extendedprice,
            CASE 
                WHEN o.o_totalprice > 50000 THEN 'High Value'
                WHEN o.o_totalprice > 20000 THEN 'Medium Value'
                ELSE 'Low Value'
            END as order_category
        FROM orders o
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderdate >= DATE '2024-02-01'
          AND l.l_shipdate >= DATE '2024-02-01'
        ORDER BY o.o_totalprice DESC
        LIMIT 25
    "#;

    println!("Executing join with CASE expression:\n{}", sql);

    let result = cluster.execute_query(sql).await.expect("Query failed");
    
    assert!(!result.is_empty(), "Result should not be empty");
    assert_eq!(result[0].num_columns(), 3, "Should have 3 columns");
    
    println!("✓ Join with CASE expression executed successfully");
}
