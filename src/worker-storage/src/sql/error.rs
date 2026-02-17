//! Error types for SQL transformation.

use thiserror::Error;

use super::admission::AdmissionError;

/// Errors that can occur during SQL transformation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SqlTransformError {
    /// Failed to parse the SQL statement.
    #[error("Failed to parse SQL: {0}")]
    ParseError(String),

    /// Multiple SQL statements were provided (only single statements supported).
    #[error("Multiple statements not supported; expected single SELECT query")]
    MultipleStatements,

    /// The SQL statement type is not supported for transformation.
    #[error("Unsupported SQL statement type: {0}")]
    UnsupportedStatement(String),

    /// A referenced dataset is not registered.
    #[error("Dataset not registered: {0}")]
    DatasetNotRegistered(String),

    /// Failed to transform the SQL for some reason.
    #[error("Failed to transform SQL: {0}")]
    TransformFailed(String),

    /// Internal error during AST manipulation.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<sqlparser::parser::ParserError> for SqlTransformError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        SqlTransformError::ParseError(err.to_string())
    }
}

/// Combined error for admission and transformation.
///
/// Used by `admit_and_transform` to report either admission rejection
/// or transformation failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransformError {
    /// Query rejected by admission control.
    #[error("Admission rejected: {0}")]
    Admission(#[from] AdmissionError),

    /// Query failed during transformation.
    #[error("Transform failed: {0}")]
    Transform(#[from] SqlTransformError),
}
