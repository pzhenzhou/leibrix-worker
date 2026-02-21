use anyhow::Context;
use arrow::datatypes::{DataType, Schema};
use duckdb::{params, Connection};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

pub fn epoch_key(dataset_id: &str, epoch_id: &str) -> String {
    format!("{}__{}", dataset_id, epoch_id)
}

pub fn escape_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn estimate_table_bytes(row_count: u64, schema: &Schema) -> u64 {
    let row_size: u64 = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Boolean => 1,
            DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64 => 8,
            DataType::Utf8 | DataType::LargeUtf8 => 32,
            DataType::Binary | DataType::LargeBinary => 64,
            DataType::Timestamp(_, _) | DataType::Date32 => 8,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => 16,
            _ => 16,
        })
        .sum();
    row_count.saturating_mul(row_size)
}

pub fn query_table_row_count(conn: &Connection, table_name: &str) -> anyhow::Result<u64> {
    let query = format!("SELECT COUNT(*) FROM {}", escape_ident(table_name));

    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to prepare COUNT query for {}", table_name))?;

    let count: i64 = stmt
        .query_row([], |row| row.get(0))
        .with_context(|| format!("Failed to execute COUNT query for {}", table_name))?;

    Ok(count as u64)
}

pub fn query_table_size(conn: &Connection, table_name: &str) -> anyhow::Result<u64> {
    let query = format!(
        "SELECT SUM(column_size) as total_bytes FROM storage_info('{}')",
        escape_ident(table_name)
    );

    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to prepare size query for {}", table_name))?;

    let result = stmt
        .query_row([], |row| {
            let val: Option<i64> = row.get(0)?;
            Ok(val.map(|v| v as u64))
        })
        .map_err(|e| match e {
            duckdb::Error::QueryReturnedNoRows => {
                anyhow::anyhow!("No size information available for {}", table_name)
            }
            e => anyhow::anyhow!("Failed to execute size query for {}: {}", table_name, e),
        })?;

    result.ok_or_else(|| anyhow::anyhow!("No size information available for {}", table_name))
}

pub fn query_table_schema(conn: &Connection, table_name: &str) -> anyhow::Result<Arc<Schema>> {
    let query = format!("DESCRIBE {}", escape_ident(table_name));
    let mut stmt = conn
        .prepare(&query)
        .with_context(|| format!("Failed to prepare DESCRIBE query for {}", table_name))?;

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?, // column_name
                row.get::<_, String>(1)?, // column_type
                row.get::<_, String>(2)?, // null
            ))
        })
        .with_context(|| format!("Failed to execute DESCRIBE query for {}", table_name))?;

    let mut fields = Vec::new();
    for row_result in rows {
        let (name, type_str, null_str) = row_result?;
        let nullable = null_str == "YES";
        let data_type = duckdb_type_str_to_arrow_type(&type_str)?;
        fields.push(arrow::datatypes::Field::new(name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub fn create_table_from_arrow_schema(
    conn: &Connection,
    table_name: &str,
    schema: &Schema,
) -> anyhow::Result<()> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let duckdb_type = arrow_type_to_duckdb_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("{} {}{}", escape_ident(field.name()), duckdb_type, nullable)
        })
        .collect::<Vec<String>>()
        .join(", ");
    let create_table_sql = format!("CREATE TABLE {} ({})", escape_ident(table_name), columns);
    conn.execute(&create_table_sql, params![])
        .with_context(|| {
            format!(
                "Failed to create table {} with schema {:?}",
                table_name, schema
            )
        })?;
    info!("Created table {} with schema {:?}", table_name, columns);
    Ok(())
}
fn duckdb_type_str_to_arrow_type(type_str: &str) -> anyhow::Result<DataType> {
    let type_str_upper = type_str.to_uppercase();
    // Handle types that might have parameters like DECIMAL(10, 2)
    let base_type = type_str_upper
        .split('(')
        .next()
        .unwrap_or(&type_str_upper)
        .trim();

    Ok(match base_type {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INTEGER" | "INT" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "UTINYINT" => DataType::UInt8,
        "USMALLINT" => DataType::UInt16,
        "UINTEGER" => DataType::UInt32,
        "UBIGINT" => DataType::UInt64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" => DataType::Utf8,
        "BLOB" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        "TIMESTAMP" | "TIMESTAMPTZ" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "JSON" => DataType::Utf8, // Represent JSON as string for now
        _ => {
            // Default to VARCHAR for unknown types
            warn!("Unknown DuckDB type: {}, defaulting to VARCHAR", type_str);
            DataType::Utf8
        }
    })
}

fn arrow_type_to_duckdb_type(arrow_type: &DataType) -> &'static str {
    match arrow_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 => "FLOAT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Duration(_) => "INTERVAL",
        DataType::Decimal128(_precision, _scale) => {
            // DuckDB's DECIMAL type
            // Note: This creates a string that needs to be handled carefully
            // For simplicity, we'll use VARCHAR as fallback if needed
            // In production, you'd want to format this properly
            "DECIMAL(38, 10)" // Default precision/scale
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "JSON",
        DataType::Struct(_) => "STRUCT",
        DataType::Map(_, _) => "MAP",
        _ => "VARCHAR",
    }
}
