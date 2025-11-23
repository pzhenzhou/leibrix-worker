use arrow::datatypes::DataType;
use std::path::PathBuf;

pub mod memory_duckdb;

/// Configuration for the DuckDB storage engine.
#[derive(Debug, Clone)]
pub struct DuckDBEngineConfig {
    pub memory_limit_mb: Option<u64>,
    pub flush_rows_threshold: u64,
    pub channel_capacity: usize,
    pub tmp_dir: Option<PathBuf>,
    pub max_identifiers: usize,
}

impl Default for DuckDBEngineConfig {
    fn default() -> Self {
        Self {
            memory_limit_mb: Some(1024), // 1 GB
            flush_rows_threshold: 10_000,
            channel_capacity: 100,
            tmp_dir: None,
            max_identifiers: 1000,
        }
    }
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
        DataType::Decimal128(precision, scale) => {
            // DuckDB's DECIMAL type
            // Note: This creates a string that needs to be handled carefully
            // For simplicity, we'll use VARCHAR as fallback if needed
            // In production, you'd want to format this properly
            return "DECIMAL(38, 10)"; // Default precision/scale
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "JSON",
        DataType::Struct(_) => "STRUCT",
        DataType::Map(_, _) => "MAP",
        _ => "VARCHAR",
    }
}
