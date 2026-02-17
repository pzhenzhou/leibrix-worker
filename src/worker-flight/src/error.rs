//! Flight-specific error mapping.
//!
//! Maps internal QueryError and SqlTransformError types to tonic Status
//! for proper Flight protocol error responses.

use tonic::Status;
use worker_storage::engine::query_engine::QueryError;
use worker_storage::sql::SqlTransformError;

/// Maps a QueryError to an appropriate tonic Status.
///
/// | QueryError | Status |
/// |------------|--------|
/// | Timeout | deadline_exceeded |
/// | DuckDB | internal |
/// | Sql | invalid_argument |
/// | TableNotFound | not_found |
/// | TenantValidation | permission_denied |
/// | Internal | internal |
pub fn map_query_error_to_status(err: QueryError) -> Status {
    match err {
        QueryError::Timeout(secs) => {
            Status::deadline_exceeded(format!("Query timeout after {}s", secs))
        }
        QueryError::DuckDB(msg) => Status::internal(format!("Database error: {}", msg)),
        QueryError::Sql(msg) => Status::invalid_argument(format!("SQL error: {}", msg)),
        QueryError::TableNotFound(table) => {
            Status::not_found(format!("Table not found: {}", table))
        }
        QueryError::TenantValidation(msg) => {
            Status::permission_denied(format!("Tenant validation failed: {}", msg))
        }
        QueryError::Internal(msg) => Status::internal(format!("Internal error: {}", msg)),
    }
}

/// Maps a SqlTransformError to an appropriate tonic Status.
///
/// All SQL transformation errors are mapped to invalid_argument since they
/// represent issues with the client's query syntax or semantics.
pub fn map_transform_error_to_status(err: SqlTransformError) -> Status {
    Status::invalid_argument(format!("SQL transformation failed: {}", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn test_timeout_maps_to_deadline_exceeded() {
        let err = QueryError::Timeout(30);
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::DeadlineExceeded);
    }

    #[test]
    fn test_duckdb_maps_to_internal() {
        let err = QueryError::DuckDB("connection failed".to_string());
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn test_sql_maps_to_invalid_argument() {
        let err = QueryError::Sql("syntax error".to_string());
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn test_table_not_found_maps_to_not_found() {
        let err = QueryError::TableNotFound("missing_table".to_string());
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::NotFound);
    }

    #[test]
    fn test_tenant_validation_maps_to_permission_denied() {
        let err = QueryError::TenantValidation("wrong tenant".to_string());
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::PermissionDenied);
    }

    #[test]
    fn test_internal_maps_to_internal() {
        let err = QueryError::Internal("unexpected".to_string());
        let status = map_query_error_to_status(err);
        assert_eq!(status.code(), Code::Internal);
    }
}
