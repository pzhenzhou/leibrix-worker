//! Ticket encoding and decoding for Arrow Flight protocol.
//!
//! This module provides serialization/deserialization for query tickets
//! used in the Arrow Flight DoGet RPC.

use arrow_flight::Ticket;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Query ticket containing SQL query and execution parameters.
///
/// This is serialized to JSON and embedded in the Arrow Flight Ticket
/// for the DoGet RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTicket {
    /// SQL query to execute (against logical table names)
    pub sql: String,

    /// Tenant ID for validation
    pub tenant_id: String,

    /// Optional memory limit in megabytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<usize>,

    /// Optional query timeout in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

impl QueryTicket {
    /// Create a new query ticket.
    pub fn new(sql: String, tenant_id: String) -> Self {
        Self {
            sql,
            tenant_id,
            memory_limit_mb: None,
            timeout_secs: None,
        }
    }

    /// Set memory limit in megabytes.
    pub fn with_memory_limit(mut self, limit_mb: usize) -> Self {
        self.memory_limit_mb = Some(limit_mb);
        self
    }

    /// Set query timeout in seconds.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    /// Encode the ticket to Arrow Flight Ticket format (JSON bytes).
    pub fn encode(&self) -> Result<Ticket, TicketError> {
        let json = serde_json::to_vec(self).map_err(TicketError::SerializationFailed)?;
        Ok(Ticket { ticket: json.into() })
    }

    /// Decode a QueryTicket from Arrow Flight Ticket.
    pub fn decode(ticket: &Ticket) -> Result<Self, TicketError> {
        serde_json::from_slice(&ticket.ticket).map_err(TicketError::DeserializationFailed)
    }
}

/// Errors that can occur during ticket encoding/decoding.
#[derive(Debug, Error)]
pub enum TicketError {
    #[error("Failed to serialize ticket: {0}")]
    SerializationFailed(#[source] serde_json::Error),

    #[error("Failed to deserialize ticket: {0}")]
    DeserializationFailed(#[source] serde_json::Error),
}

impl From<TicketError> for tonic::Status {
    fn from(err: TicketError) -> Self {
        tonic::Status::invalid_argument(format!("Invalid ticket: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticket_roundtrip() {
        let ticket = QueryTicket::new(
            "SELECT * FROM sales_data".to_string(),
            "tenant-123".to_string(),
        )
        .with_memory_limit(512)
        .with_timeout(30);

        let encoded = ticket.encode().unwrap();
        let decoded = QueryTicket::decode(&encoded).unwrap();

        assert_eq!(decoded.sql, "SELECT * FROM sales_data");
        assert_eq!(decoded.tenant_id, "tenant-123");
        assert_eq!(decoded.memory_limit_mb, Some(512));
        assert_eq!(decoded.timeout_secs, Some(30));
    }

    #[test]
    fn test_ticket_without_optional_fields() {
        let ticket = QueryTicket::new("SELECT 1".to_string(), "tenant-456".to_string());

        let encoded = ticket.encode().unwrap();
        let decoded = QueryTicket::decode(&encoded).unwrap();

        assert_eq!(decoded.sql, "SELECT 1");
        assert_eq!(decoded.tenant_id, "tenant-456");
        assert_eq!(decoded.memory_limit_mb, None);
        assert_eq!(decoded.timeout_secs, None);
    }

    #[test]
    fn test_invalid_ticket_returns_error() {
        let invalid_ticket = Ticket {
            ticket: b"not valid json".to_vec().into(),
        };

        let result = QueryTicket::decode(&invalid_ticket);
        assert!(result.is_err());
    }
}
