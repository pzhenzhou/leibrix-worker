//! Arrow ↔ DuckDB utility functions.
//!
//! These utilities handle materializing Arrow `RecordBatch` data into DuckDB
//! temporary tables and cleaning them up. They are Substrait-independent and
//! used for registering exchange inputs within the LDP executor.
//!
//! # Ingestion mechanism
//!
//! Data is loaded via the DuckDB native `Appender` API together with
//! `Appender::append_record_batch`, which is enabled by the `appender-arrow`
//! Cargo feature compiled into `worker-storage`.  Each `RecordBatch` is handed
//! to DuckDB as an Arrow C Data Interface stream, so data is ingested as a
//! columnar bulk transfer rather than row-by-row.  A final `flush()` call
//! commits the buffered data before the appender is dropped.
//!
//! # Lifecycle
//!
//! 1. **Register**: `register_arrow_batches(conn, table_name, batches)` creates
//!    a DuckDB temporary table and bulk-loads the batches via the appender.
//! 2. **Query**: The stage executor runs SQL against the temp table via
//!    `conn.query_arrow(sql)`.
//! 3. **Cleanup**: `drop_temp_table(conn, table_name)` removes the temp table.

use anyhow::{Context, Result};
use duckdb::Connection;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Register Arrow record batches as a temporary table in DuckDB.
///
/// This allows exchange inputs to be queried as tables within stage SQL.
/// Internally it issues `CREATE TEMPORARY TABLE` DDL (schema derived from the
/// first batch) and then bulk-loads the data via the DuckDB native `Appender`
/// API (`conn.appender()` + `append_record_batch`), which passes each
/// `RecordBatch` through the Arrow C Data Interface for columnar bulk ingestion.
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `table_name` - Name for the temporary table (e.g., `"__exchange_0"`)
/// * `batches` - Arrow record batches to register
///
/// # Note
/// The table is temporary: it will be dropped when the connection closes,
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
pub fn arrow_type_to_duckdb(arrow_type: &DataType) -> Result<String> {
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
        _ => return Err(anyhow::anyhow!("Unsupported Arrow type: {:?}", arrow_type)),
    };

    Ok(duckdb_type.to_string())
}

/// Bulk-load Arrow record batches into an existing DuckDB table using the
/// native `Appender` API.
///
/// Requires the `appender-arrow` Cargo feature (already enabled in
/// `worker-storage`).  Each `RecordBatch` is handed to DuckDB as an Arrow C
/// Data Interface stream, so data is ingested in columnar bulk rather than
/// row-by-row.  A final `flush()` call commits the buffered data before the
/// appender is dropped.
fn append_arrow_batches(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<()> {
    let mut appender = conn
        .appender(table_name)
        .with_context(|| format!("Failed to create appender for '{}'", table_name))?;

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        appender
            .append_record_batch(batch.clone())
            .with_context(|| {
                format!(
                    "Failed to append batch ({} rows) to '{}'",
                    batch.num_rows(),
                    table_name
                )
            })?;
    }

    appender
        .flush()
        .context("Failed to flush appender to DuckDB")?;

    Ok(())
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

    let schema = batches[0].schema();

    if replace_if_exists {
        conn.execute(
            &format!("DROP TABLE IF EXISTS {}", quote_ident(table_name)),
            [],
        )
        .with_context(|| format!("Failed to drop existing table '{}'", table_name))?;
    }

    // Create the table with a schema derived from the first batch.
    let create_sql = build_create_table_sql(table_name, &schema, temporary)?;
    conn.execute(&create_sql, []).with_context(|| {
        format!(
            "Failed to create {}table '{}' for arrow data",
            if temporary { "temp " } else { "" },
            table_name
        )
    })?;

    // Bulk-load data via the native Arrow appender.
    append_arrow_batches(conn, table_name, batches)
        .with_context(|| format!("Failed to append arrow data into table '{}'", table_name))?;

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
    use arrow::array::*;
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
        // Field names are double-quoted by quote_ident to handle special chars and reserved words
        assert!(sql.contains("\"id\" INTEGER NOT NULL"));
        assert!(sql.contains("\"name\" VARCHAR"));
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
