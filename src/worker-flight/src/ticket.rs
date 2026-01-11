//! Ticket encoding and decoding for Arrow Flight protocol.
//!
//! This module provides serialization/deserialization for tickets
//! used in the Arrow Flight DoGet RPC.
//!
//! # Ticket Types
//! - `QueryTicket`: SQL query execution (existing)
//! - `StageResultTicket`: LDP stage output retrieval (new)
//! - `FlightTicket`: Unified enum for dispatching in DoGet

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

// ============================================================================
// Stage Result Ticket (LDP)
// ============================================================================

/// Ticket for retrieving LDP stage execution results.
///
/// After a stage is submitted via DoAction, the response contains a ticket
/// that can be used to retrieve the stage output via DoGet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageResultTicket {
    /// Tenant ID for validation
    pub tenant_id: String,

    /// Query ID this stage belongs to
    pub query_id: String,

    /// Stage ID within the query
    pub stage_id: u32,

    /// Partition index for partitioned outputs (None for stream output)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
}

impl StageResultTicket {
    /// Create a new stage result ticket for stream output.
    pub fn new(tenant_id: String, query_id: String, stage_id: u32) -> Self {
        Self {
            tenant_id,
            query_id,
            stage_id,
            partition: None,
        }
    }

    /// Create a ticket for a specific partition.
    pub fn for_partition(
        tenant_id: String,
        query_id: String,
        stage_id: u32,
        partition: u32,
    ) -> Self {
        Self {
            tenant_id,
            query_id,
            stage_id,
            partition: Some(partition),
        }
    }

    /// Encode the ticket to Arrow Flight Ticket format (JSON bytes).
    pub fn encode(&self) -> Result<Ticket, TicketError> {
        let json = serde_json::to_vec(self).map_err(TicketError::SerializationFailed)?;
        Ok(Ticket { ticket: json.into() })
    }

    /// Decode a StageResultTicket from Arrow Flight Ticket.
    pub fn decode(ticket: &Ticket) -> Result<Self, TicketError> {
        serde_json::from_slice(&ticket.ticket).map_err(TicketError::DeserializationFailed)
    }
}

// ============================================================================
// Unified Flight Ticket
// ============================================================================

/// Unified ticket type for dispatching in DoGet.
///
/// This enum allows the Flight service to handle both SQL queries
/// and LDP stage result retrieval through the same DoGet endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FlightTicket {
    /// SQL query execution
    #[serde(rename = "query")]
    Query(QueryTicket),

    /// LDP stage result retrieval
    #[serde(rename = "stage_result")]
    StageResult(StageResultTicket),
}

impl FlightTicket {
    /// Create a query ticket.
    pub fn query(sql: String, tenant_id: String) -> Self {
        FlightTicket::Query(QueryTicket::new(sql, tenant_id))
    }

    /// Create a stage result ticket.
    pub fn stage_result(tenant_id: String, query_id: String, stage_id: u32) -> Self {
        FlightTicket::StageResult(StageResultTicket::new(tenant_id, query_id, stage_id))
    }

    /// Create a stage result ticket for a partition.
    pub fn stage_result_partition(
        tenant_id: String,
        query_id: String,
        stage_id: u32,
        partition: u32,
    ) -> Self {
        FlightTicket::StageResult(StageResultTicket::for_partition(
            tenant_id, query_id, stage_id, partition,
        ))
    }

    /// Encode the ticket to Arrow Flight Ticket format.
    pub fn encode(&self) -> Result<Ticket, TicketError> {
        let json = serde_json::to_vec(self).map_err(TicketError::SerializationFailed)?;
        Ok(Ticket { ticket: json.into() })
    }

    /// Decode a FlightTicket from Arrow Flight Ticket.
    ///
    /// This first tries to decode as the unified format. If that fails,
    /// it falls back to trying QueryTicket for backward compatibility.
    pub fn decode(ticket: &Ticket) -> Result<Self, TicketError> {
        // Try unified format first
        if let Ok(flight_ticket) = serde_json::from_slice::<FlightTicket>(&ticket.ticket) {
            return Ok(flight_ticket);
        }

        // Fallback: try legacy QueryTicket format
        if let Ok(query_ticket) = serde_json::from_slice::<QueryTicket>(&ticket.ticket) {
            return Ok(FlightTicket::Query(query_ticket));
        }

        // Neither worked
        Err(TicketError::DeserializationFailed(
            serde_json::from_slice::<FlightTicket>(&ticket.ticket).unwrap_err(),
        ))
    }

    /// Get the tenant ID from any ticket type.
    pub fn tenant_id(&self) -> &str {
        match self {
            FlightTicket::Query(q) => &q.tenant_id,
            FlightTicket::StageResult(s) => &s.tenant_id,
        }
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

    // ========================================================================
    // StageResultTicket Tests
    // ========================================================================

    #[test]
    fn test_stage_result_ticket_roundtrip() {
        let ticket = StageResultTicket::new(
            "tenant-123".to_string(),
            "query-456".to_string(),
            5,
        );

        let encoded = ticket.encode().unwrap();
        let decoded = StageResultTicket::decode(&encoded).unwrap();

        assert_eq!(decoded.tenant_id, "tenant-123");
        assert_eq!(decoded.query_id, "query-456");
        assert_eq!(decoded.stage_id, 5);
        assert_eq!(decoded.partition, None);
    }

    #[test]
    fn test_stage_result_ticket_with_partition() {
        let ticket = StageResultTicket::for_partition(
            "tenant-123".to_string(),
            "query-456".to_string(),
            3,
            7,
        );

        let encoded = ticket.encode().unwrap();
        let decoded = StageResultTicket::decode(&encoded).unwrap();

        assert_eq!(decoded.stage_id, 3);
        assert_eq!(decoded.partition, Some(7));
    }

    // ========================================================================
    // FlightTicket Tests
    // ========================================================================

    #[test]
    fn test_flight_ticket_query_roundtrip() {
        let ticket = FlightTicket::query(
            "SELECT * FROM test".to_string(),
            "tenant-123".to_string(),
        );

        let encoded = ticket.encode().unwrap();
        let decoded = FlightTicket::decode(&encoded).unwrap();

        match decoded {
            FlightTicket::Query(q) => {
                assert_eq!(q.sql, "SELECT * FROM test");
                assert_eq!(q.tenant_id, "tenant-123");
            }
            _ => panic!("Expected Query variant"),
        }
    }

    #[test]
    fn test_flight_ticket_stage_result_roundtrip() {
        let ticket = FlightTicket::stage_result(
            "tenant-123".to_string(),
            "query-789".to_string(),
            42,
        );

        let encoded = ticket.encode().unwrap();
        let decoded = FlightTicket::decode(&encoded).unwrap();

        match decoded {
            FlightTicket::StageResult(s) => {
                assert_eq!(s.tenant_id, "tenant-123");
                assert_eq!(s.query_id, "query-789");
                assert_eq!(s.stage_id, 42);
                assert_eq!(s.partition, None);
            }
            _ => panic!("Expected StageResult variant"),
        }
    }

    #[test]
    fn test_flight_ticket_stage_result_partition_roundtrip() {
        let ticket = FlightTicket::stage_result_partition(
            "tenant-1".to_string(),
            "q-1".to_string(),
            1,
            15,
        );

        let encoded = ticket.encode().unwrap();
        let decoded = FlightTicket::decode(&encoded).unwrap();

        match decoded {
            FlightTicket::StageResult(s) => {
                assert_eq!(s.partition, Some(15));
            }
            _ => panic!("Expected StageResult variant"),
        }
    }

    #[test]
    fn test_flight_ticket_backward_compatibility() {
        // Legacy QueryTicket format (without "type" field)
        let legacy_ticket = QueryTicket::new(
            "SELECT 1".to_string(),
            "legacy-tenant".to_string(),
        );
        let encoded = legacy_ticket.encode().unwrap();

        // FlightTicket::decode should handle legacy format
        let decoded = FlightTicket::decode(&encoded).unwrap();

        match decoded {
            FlightTicket::Query(q) => {
                assert_eq!(q.sql, "SELECT 1");
                assert_eq!(q.tenant_id, "legacy-tenant");
            }
            _ => panic!("Expected Query variant for legacy ticket"),
        }
    }

    #[test]
    fn test_flight_ticket_tenant_id() {
        let query_ticket = FlightTicket::query("SELECT 1".to_string(), "t1".to_string());
        assert_eq!(query_ticket.tenant_id(), "t1");

        let stage_ticket = FlightTicket::stage_result("t2".to_string(), "q".to_string(), 0);
        assert_eq!(stage_ticket.tenant_id(), "t2");
    }
}
