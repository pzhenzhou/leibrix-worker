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
        .map(|field| -> anyhow::Result<String> {
            let duckdb_type = super::arrow_utils::arrow_type_to_duckdb(field.data_type())
                .map_err(|e| anyhow::anyhow!("Unsupported type for '{}': {}", field.name(), e))?;
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!("{} {}{}", escape_ident(field.name()), duckdb_type, nullable))
        })
        .collect::<anyhow::Result<Vec<String>>>()?
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
        "DECIMAL" | "HUGEINT" => {
            // Parse DECIMAL(precision, scale) from the original type string.
            if let Some(params) = type_str_upper
                .strip_prefix("DECIMAL(")
                .and_then(|s| s.strip_suffix(')'))
            {
                let parts: Vec<&str> = params.split(',').collect();
                if parts.len() == 2 {
                    if let (Ok(p), Ok(s)) = (
                        parts[0].trim().parse::<u8>(),
                        parts[1].trim().parse::<i8>(),
                    ) {
                        return Ok(DataType::Decimal128(p, s));
                    }
                }
            }
            DataType::Decimal128(38, 10)
        }
        _ => {
            // Default to VARCHAR for unknown types
            warn!("Unknown DuckDB type: {}, defaulting to VARCHAR", type_str);
            DataType::Utf8
        }
    })
}

