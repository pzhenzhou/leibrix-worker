use crate::loader::types::{Catalog, DataSource};
use arrow::datatypes::Schema;
use std::sync::Arc;

mod adbc_client;
pub mod backend;
mod jdbc_client;

#[allow(unused_imports)]
pub use backend::StarRocksBackend;

pub fn select_text(source: Arc<DataSource>, schema: Arc<Schema>) -> String {
    let column_list = schema
        .fields()
        .iter()
        .map(|f| format!("`{}`", f.name()))
        .collect::<Vec<String>>()
        .join(", ");

    let full_table_name = match &source.catalog {
        Catalog::StarRocks { catalog, .. } => {
            format!("{}.`{}`.`{}`", catalog, source.database, source.table)
        }
        Catalog::Jdbc { .. } => {
            format!("`{}`.`{}`", source.database, source.table)
        }
        _ => format!("`{}`.`{}`", source.database, source.table),
    };

    let mut sql = format!(
        "SELECT {} FROM {}",
        column_list, full_table_name
    );

    if let Some(filter) = &source.filter {
        sql.push_str(&format!(" WHERE {}", filter));
    }
    sql
}
