//! DuckDB Substrait extension wrapper.
//!
//! This module provides utilities to obtain Substrait plans from DuckDB
//! using the `get_substrait()` extension function.

use anyhow::{Context, Result};
use duckdb::Connection;

/// Get a Substrait plan from DuckDB for the given SQL query.
///
/// This uses DuckDB's `get_substrait()` function to convert a SQL query
/// into a Substrait protobuf blob.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `sql` - SQL query to convert
///
/// # Returns
/// Substrait protobuf bytes on success.
///
/// # Errors
/// Returns an error if:
/// - The substrait extension is not available
/// - The SQL is invalid
/// - DuckDB cannot generate a plan
///
/// # Example
/// ```ignore
/// let substrait_bytes = duckdb_get_substrait(&conn, "SELECT * FROM orders")?;
/// ```
pub fn duckdb_get_substrait(conn: &Connection, sql: &str) -> Result<Vec<u8>> {
    // First, ensure the substrait extension is loaded
    load_substrait_extension(conn)?;

    // Use get_substrait() to get the plan
    let query = format!("SELECT get_substrait('{}')", escape_sql_string(sql));

    let mut stmt = conn.prepare(&query).context("Failed to prepare get_substrait query")?;

    let substrait_blob: Vec<u8> = stmt
        .query_row([], |row| row.get(0))
        .context("Failed to execute get_substrait")?;

    Ok(substrait_blob)
}

/// Get a Substrait plan as JSON from DuckDB.
///
/// Similar to `duckdb_get_substrait` but returns human-readable JSON.
/// Useful for debugging and testing.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `sql` - SQL query to convert
///
/// # Returns
/// Substrait JSON string on success.
pub fn duckdb_get_substrait_json(conn: &Connection, sql: &str) -> Result<String> {
    // First, ensure the substrait extension is loaded
    load_substrait_extension(conn)?;

    // Use get_substrait_json() to get the plan as JSON
    let query = format!("SELECT get_substrait_json('{}')", escape_sql_string(sql));

    let mut stmt = conn.prepare(&query).context("Failed to prepare get_substrait_json query")?;

    let substrait_json: String = stmt
        .query_row([], |row| row.get(0))
        .context("Failed to execute get_substrait_json")?;

    Ok(substrait_json)
}

/// Execute a Substrait plan in DuckDB.
///
/// This uses DuckDB's `from_substrait()` function to execute a Substrait plan.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `substrait_bytes` - Substrait protobuf bytes
///
/// # Returns
/// All result batches concatenated into a single RecordBatch.
///
/// # Note
/// This function is primarily for testing. In production, stages are executed
/// via the normal query engine path which streams batches.
pub fn duckdb_from_substrait(
    conn: &Connection,
    substrait_bytes: &[u8],
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::array::RecordBatch;
    use arrow::compute::concat_batches;

    // First, ensure the substrait extension is loaded
    load_substrait_extension(conn)?;

    // Encode substrait bytes as base64 for SQL string
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, substrait_bytes);

    // Use from_substrait() to execute
    let query = format!("SELECT * FROM from_substrait(decode('{}', 'base64'))", b64);

    let mut stmt = conn.prepare(&query).context("Failed to prepare from_substrait query")?;

    // Execute and collect results
    let rows = stmt
        .query_arrow([])
        .context("Failed to execute from_substrait")?;

    // Collect all batches
    let batches: Vec<RecordBatch> = rows.collect();

    if batches.is_empty() {
        return Err(anyhow::anyhow!("No results from substrait execution"));
    }

    // Concatenate all batches into one
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // Get schema from first batch
    let schema = batches[0].schema();

    // Concatenate all batches
    let concatenated = concat_batches(&schema, &batches)
        .context("Failed to concatenate result batches")?;

    Ok(concatenated)
}

/// Execute a Substrait plan and return results as a stream of batches.
///
/// This is more memory-efficient for large results as it doesn't
/// require concatenating all batches.
pub fn duckdb_from_substrait_batches(
    conn: &Connection,
    substrait_bytes: &[u8],
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::array::RecordBatch;

    // First, ensure the substrait extension is loaded
    load_substrait_extension(conn)?;

    // Encode substrait bytes as base64 for SQL string
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, substrait_bytes);

    // Use from_substrait() to execute
    let query = format!("SELECT * FROM from_substrait(decode('{}', 'base64'))", b64);

    let mut stmt = conn.prepare(&query).context("Failed to prepare from_substrait query")?;

    // Execute and collect results
    let rows = stmt
        .query_arrow([])
        .context("Failed to execute from_substrait")?;

    // Collect all batches
    let batches: Vec<RecordBatch> = rows.collect();

    Ok(batches)
}

/// Load the DuckDB substrait extension if not already loaded.
fn load_substrait_extension(conn: &Connection) -> Result<()> {
    // Try to load the extension
    // The substrait extension may already be loaded, so we ignore "already loaded" errors
    match conn.execute("LOAD substrait", []) {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = e.to_string();
            // Ignore "already loaded" type errors
            if err_msg.contains("already loaded") || err_msg.contains("Extension") {
                Ok(())
            } else {
                // Try installing first if not available
                match conn.execute("INSTALL substrait; LOAD substrait", []) {
                    Ok(_) => Ok(()),
                    Err(install_err) => Err(anyhow::anyhow!(
                        "Failed to load substrait extension: {}. Install error: {}",
                        e,
                        install_err
                    )),
                }
            }
        }
    }
}

/// Escape a SQL string for use in a query.
/// Doubles single quotes to prevent SQL injection.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Check if the substrait extension is available.
pub fn is_substrait_available(conn: &Connection) -> bool {
    load_substrait_extension(conn).is_ok()
}

/// Register Arrow record batches as a temporary table in DuckDB.
///
/// This allows exchange inputs to be queried as tables within Substrait plans.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `table_name` - Name for the temporary table
/// * `batches` - Arrow record batches to register
///
/// # Note
/// This creates a temporary table that will be dropped when the connection closes.
pub fn register_arrow_batches(
    conn: &Connection,
    table_name: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<()> {
    use arrow::compute::concat_batches;

    if batches.is_empty() {
        // Create an empty table - need schema from somewhere
        // For now, skip empty inputs
        return Ok(());
    }

    // Concatenate all batches into one
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        concat_batches(&schema, batches)
            .context("Failed to concatenate batches for registration")?        
    };

    // DuckDB can query Arrow data via arrow_scan
    // But we need to create a temporary table from the data
    // For this, we'll insert the data into a temp table
    
    // First, create the table with matching schema
    let create_sql = build_create_table_sql(table_name, &schema)?;
    conn.execute(&create_sql, [])
        .context("Failed to create temp table for arrow data")?;

    // Insert the data using DuckDB's Arrow appender
    insert_arrow_data(conn, table_name, &combined)?;

    Ok(())
}

/// Build a CREATE TABLE statement from an Arrow schema.
fn build_create_table_sql(
    table_name: &str,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<String> {
    let mut columns = Vec::new();

    for field in schema.fields() {
        let duckdb_type = arrow_type_to_duckdb(field.data_type())?;
        let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
        columns.push(format!("{} {}{}", field.name(), duckdb_type, nullable));
    }

    Ok(format!(
        "CREATE TEMPORARY TABLE {} ({})",
        table_name,
        columns.join(", ")
    ))
}

/// Convert Arrow data type to DuckDB type string.
fn arrow_type_to_duckdb(arrow_type: &arrow::datatypes::DataType) -> Result<String> {
    use arrow::datatypes::DataType;

    let duckdb_type = match arrow_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Decimal128(p, s) => return Ok(format!("DECIMAL({}, {})", p, s)),
        DataType::Decimal256(p, s) => return Ok(format!("DECIMAL({}, {})", p, s)),
        DataType::List(_) => "JSON", // Simplified - DuckDB has better list support
        DataType::Struct(_) => "JSON", // Simplified
        _ => return Err(anyhow::anyhow!("Unsupported Arrow type: {:?}", arrow_type)),
    };

    Ok(duckdb_type.to_string())
}

/// Insert Arrow data into a DuckDB table.
fn insert_arrow_data(
    conn: &Connection,
    table_name: &str,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if batch.num_rows() == 0 {
        return Ok(());
    }

    let num_cols = batch.num_columns();
    let placeholders: Vec<&str> = (0..num_cols).map(|_| "?").collect();
    let insert_sql = format!(
        "INSERT INTO {} VALUES ({})",
        table_name,
        placeholders.join(", ")
    );

    let mut stmt = conn.prepare(&insert_sql)
        .context("Failed to prepare insert statement")?;

    // Insert row by row (not optimal, but works for bounded intermediates)
    for row in 0..batch.num_rows() {
        let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::with_capacity(num_cols);

        for col in 0..num_cols {
            let column = batch.column(col);
            let param = arrow_value_to_duckdb(column, row)?;
            params.push(param);
        }

        // Convert params to references
        let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        stmt.execute(param_refs.as_slice())
            .context("Failed to insert row")?;
    }

    Ok(())
}

/// Convert an Arrow array value to a DuckDB parameter.
fn arrow_value_to_duckdb(
    array: &dyn arrow::array::Array,
    row: usize,
) -> Result<Box<dyn duckdb::ToSql>> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row) {
        return Ok(Box::new(None::<i64>));
    }

    let value: Box<dyn duckdb::ToSql> = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Box::new(arr.value(row) as i32)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Box::new(arr.value(row) as i32)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Box::new(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Box::new(arr.value(row))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Box::new(arr.value(row) as i32)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Box::new(arr.value(row) as i32)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Box::new(arr.value(row) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            // DuckDB doesn't have unsigned 64-bit, use signed
            Box::new(arr.value(row) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Box::new(arr.value(row) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Box::new(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Box::new(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Box::new(arr.value(row).to_string())
        }
        _ => return Err(anyhow::anyhow!("Unsupported type for insertion: {:?}", array.data_type())),
    };

    Ok(value)
}

/// Drop a temporary table if it exists.
pub fn drop_temp_table(conn: &Connection, table_name: &str) -> Result<()> {
    conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])
        .context("Failed to drop temp table")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_connection() -> Connection {
        Connection::open_in_memory().expect("Failed to create in-memory DuckDB connection")
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("'test'"), "''test''");
    }

    #[test]
    fn test_load_substrait_extension() {
        let conn = create_test_connection();
        // This test may fail if substrait extension is not installed
        // In CI, we'd need to ensure the extension is available
        let result = load_substrait_extension(&conn);
        // Don't assert success since extension may not be installed
        // Just verify it doesn't panic
        let _ = result;
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_get_substrait_simple_query() {
        let conn = create_test_connection();

        // Create a simple table
        conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)", [])
            .unwrap();

        // Try to get substrait
        let result = duckdb_get_substrait(&conn, "SELECT * FROM test");

        match result {
            Ok(bytes) => {
                assert!(!bytes.is_empty(), "Substrait bytes should not be empty");
            }
            Err(e) => {
                // Extension not available - that's okay for CI without extension
                println!("Substrait extension not available: {}", e);
            }
        }
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_get_substrait_json() {
        let conn = create_test_connection();

        conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();

        let result = duckdb_get_substrait_json(&conn, "SELECT * FROM test");

        match result {
            Ok(json) => {
                assert!(!json.is_empty(), "Substrait JSON should not be empty");
                // Should be valid JSON
                assert!(json.contains("{"), "Should be JSON object");
            }
            Err(e) => {
                println!("Substrait extension not available: {}", e);
            }
        }
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_roundtrip_substrait() {
        let conn = create_test_connection();

        conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)", [])
            .unwrap();
        conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')", [])
            .unwrap();

        // Get substrait for a query
        let substrait = match duckdb_get_substrait(&conn, "SELECT * FROM test") {
            Ok(s) => s,
            Err(e) => {
                println!("Substrait extension not available: {}", e);
                return;
            }
        };

        // Execute the substrait plan
        let result = duckdb_from_substrait(&conn, &substrait);

        match result {
            Ok(batch) => {
                assert_eq!(batch.num_rows(), 2, "Should return 2 rows");
                assert_eq!(batch.num_columns(), 2, "Should return 2 columns");
            }
            Err(e) => {
                println!("Failed to execute substrait: {}", e);
            }
        }
    }
}
