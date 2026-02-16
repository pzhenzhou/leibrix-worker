//! Arrow ↔ DuckDB utility functions.
//!
//! These utilities handle materializing Arrow `RecordBatch` data into DuckDB
//! temporary tables and cleaning them up. They are Substrait-independent and
//! used for registering exchange inputs within the LDP executor.
//!
//! # Lifecycle
//!
//! 1. **Register**: `register_arrow_batches(conn, table_name, batches)` creates
//!    a DuckDB temporary table from Arrow data.
//! 2. **Query**: The stage executor runs SQL against the temp table via
//!    `conn.query_arrow(sql)`.
//! 3. **Cleanup**: `drop_temp_table(conn, table_name)` removes the temp table.

use anyhow::{Context, Result};
use duckdb::Connection;

use arrow::array::*;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Register Arrow record batches as a temporary table in DuckDB.
///
/// This allows exchange inputs to be queried as tables within stage SQL.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `table_name` - Name for the temporary table (e.g., `"__exchange_0"`)
/// * `batches` - Arrow record batches to register
///
/// # Note
/// Creates a temporary table that will be dropped when the connection closes,
/// or explicitly via [`drop_temp_table`].
pub fn register_arrow_batches(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<()> {
    register_arrow_batches_with_options(conn, table_name, batches, true, true)
}

/// Register Arrow record batches as a regular (non-temporary) table in DuckDB.
///
/// This is primarily used by test setup code that needs table data to persist
/// beyond the lifetime of a single connection.
pub fn register_arrow_batches_persistent(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<()> {
    register_arrow_batches_with_options(conn, table_name, batches, false, true)
}

/// Drop a temporary table if it exists.
pub fn drop_temp_table(conn: &Connection, table_name: &str) -> Result<()> {
    conn.execute(
        &format!("DROP TABLE IF EXISTS {}", quote_ident(table_name)),
        [],
    )
    .context("Failed to drop temp table")?;
    Ok(())
}

/// Escape a SQL string by doubling single quotes.
pub fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build a `CREATE TEMPORARY TABLE` statement from an Arrow schema.
fn build_create_table_sql(
    table_name: &str,
    schema: &arrow::datatypes::SchemaRef,
    temporary: bool,
) -> Result<String> {
    let mut columns = Vec::new();

    for field in schema.fields() {
        let duckdb_type = arrow_type_to_duckdb(field.data_type())?;
        let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
        columns.push(format!(
            "{} {}{}",
            quote_ident(field.name()),
            duckdb_type,
            nullable
        ));
    }

    let table_kind = if temporary { "TEMPORARY " } else { "" };
    Ok(format!(
        "CREATE {}TABLE {} ({})",
        table_kind,
        quote_ident(table_name),
        columns.join(", ")
    ))
}

/// Convert an Arrow `DataType` to a DuckDB type string.
fn arrow_type_to_duckdb(arrow_type: &DataType) -> Result<String> {
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
        DataType::List(_) => "JSON",
        DataType::Struct(_) => "JSON",
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported Arrow type: {:?}",
                arrow_type
            ))
        }
    };

    Ok(duckdb_type.to_string())
}

/// Insert Arrow data into a DuckDB table row-by-row.
///
/// Not optimal, but works for bounded intermediates (exchange data between stages).
fn insert_arrow_data(
    conn: &Connection,
    table_name: &str,
    batch: &RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let num_cols = batch.num_columns();
    let placeholders: Vec<&str> = (0..num_cols).map(|_| "?").collect();
    let insert_sql = format!(
        "INSERT INTO {} VALUES ({})",
        quote_ident(table_name),
        placeholders.join(", ")
    );

    let mut stmt = conn
        .prepare(&insert_sql)
        .context("Failed to prepare insert statement")?;

    for row in 0..batch.num_rows() {
        let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::with_capacity(num_cols);

        for col in 0..num_cols {
            let column = batch.column(col);
            let param = arrow_value_to_duckdb(column, row)?;
            params.push(param);
        }

        let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        stmt.execute(param_refs.as_slice())
            .context("Failed to insert row")?;
    }

    Ok(())
}

/// Convert an Arrow array value at `row` to a boxed `dyn duckdb::ToSql`.
fn arrow_value_to_duckdb(
    array: &dyn Array,
    row: usize,
) -> Result<Box<dyn duckdb::ToSql>> {
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
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| anyhow::anyhow!("Invalid Date32 value: {}", days))?;
            // Insert as ISO string; DuckDB will coerce to DATE.
            Box::new(date.to_string())
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row);
            let dt = chrono::DateTime::from_timestamp_millis(millis)
                .ok_or_else(|| anyhow::anyhow!("Invalid Date64 value: {}", millis))?;
            Box::new(dt.naive_utc().to_string())
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported type for insertion: {:?}",
                array.data_type()
            ))
        }
    };

    Ok(value)
}

/// Register Arrow record batches with configurable table scope and lifecycle.
fn register_arrow_batches_with_options(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
    temporary: bool,
    replace_if_exists: bool,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    // Concatenate all batches into one.
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        concat_batches(&schema, batches)
            .context("Failed to concatenate batches for registration")?
    };

    if replace_if_exists {
        conn.execute(
            &format!("DROP TABLE IF EXISTS {}", quote_ident(table_name)),
            [],
        )
        .with_context(|| format!("Failed to drop existing table '{}'", table_name))?;
    }

    // Create the table with matching schema.
    let create_sql = build_create_table_sql(table_name, &schema, temporary)?;
    conn.execute(&create_sql, []).with_context(|| {
        format!(
            "Failed to create {}table '{}' for arrow data",
            if temporary { "temp " } else { "" },
            table_name
        )
    })?;

    // Insert the data.
    insert_arrow_data(conn, table_name, &combined)
        .with_context(|| format!("Failed to insert arrow data into table '{}'", table_name))?;

    Ok(())
}

/// Quote a SQL identifier for DuckDB.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("'test'"), "''test''");
    }

    #[test]
    fn test_arrow_type_to_duckdb() {
        assert_eq!(arrow_type_to_duckdb(&DataType::Int32).unwrap(), "INTEGER");
        assert_eq!(arrow_type_to_duckdb(&DataType::Utf8).unwrap(), "VARCHAR");
        assert_eq!(arrow_type_to_duckdb(&DataType::Boolean).unwrap(), "BOOLEAN");
        assert_eq!(arrow_type_to_duckdb(&DataType::Float64).unwrap(), "DOUBLE");
        assert_eq!(
            arrow_type_to_duckdb(&DataType::Decimal128(10, 2)).unwrap(),
            "DECIMAL(10, 2)"
        );
    }

    #[test]
    fn test_build_create_table_sql() {
        use arrow::datatypes::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let sql = build_create_table_sql("test_table", &schema, true).unwrap();
        assert!(sql.contains("CREATE TEMPORARY TABLE"));
        assert!(sql.contains("id INTEGER NOT NULL"));
        assert!(sql.contains("name VARCHAR"));
    }

    #[test]
    fn test_register_and_drop() {
        use arrow::datatypes::{Field, Schema};

        let conn = Connection::open_in_memory().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();

        // Register.
        register_arrow_batches(&conn, "__exchange_0", &[batch]).unwrap();

        // Query.
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM __exchange_0").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 3);

        // Drop.
        drop_temp_table(&conn, "__exchange_0").unwrap();
    }

    #[test]
    fn test_register_empty_batches() {
        let conn = Connection::open_in_memory().unwrap();
        // Should not error.
        register_arrow_batches(&conn, "__empty", &[]).unwrap();
    }
}
