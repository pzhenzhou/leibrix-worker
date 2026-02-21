#![allow(dead_code)]

//! Query execution helpers shared across all flight e2e tests.
//!
//! Two primary functions — both accept the same logical SQL string:
//!
//! - [`execute_via_flight`]: full e2e path → `get_flight_info` (applies
//!   SQL transformation) → `do_get` → Arrow IPC decode.
//! - [`execute_reference`]: applies the same `SqlTransformer`, then calls
//!   `QueryEngine::execute_query` directly.
//!
//! Comparing the two outputs is the primary correctness check for all tests.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use futures_util::TryStreamExt;
use tonic::transport::Channel;

use worker_storage::engine::duckdb::DuckDBQueryEngine;
use worker_storage::engine::query_engine::QueryEngine;
use worker_storage::sql::SqlTransformer;

/// Execute `sql` through the full Arrow Flight pipeline:
/// `get_flight_info` (applies transformation) → `do_get` → Arrow IPC decode.
///
/// The `x-tenant-id` gRPC metadata header is added automatically.
///
/// Returns `Err(tonic::Status)` if any RPC step fails, enabling error-path
/// tests to assert the exact status code without panicking.
pub async fn execute_via_flight(
    client: &mut FlightServiceClient<Channel>,
    sql: &str,
    tenant_id: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    // 1. get_flight_info applies the SQL transformer and returns a ticket
    //    containing the (potentially rewritten) SQL.
    let descriptor = FlightDescriptor::new_cmd(sql.as_bytes().to_vec());
    let mut info_request = tonic::Request::new(descriptor);
    info_request.metadata_mut().insert(
        "x-tenant-id",
        tenant_id.parse().expect("valid header value"),
    );

    let flight_info = client.get_flight_info(info_request).await?.into_inner();

    let ticket = flight_info
        .endpoint
        .into_iter()
        .next()
        .and_then(|ep| ep.ticket)
        .ok_or_else(|| tonic::Status::internal("no endpoint/ticket in FlightInfo"))?;

    // 2. do_get with the ticket and decode the Arrow IPC stream.
    do_get_with_ticket(client, ticket).await
}

/// Issue `do_get` with a pre-built `arrow_flight::Ticket` and collect batches.
///
/// Use this for error-path tests where you need to bypass `get_flight_info`
/// and craft the ticket manually (e.g., wrong tenant, malformed SQL in ticket).
pub async fn do_get_with_ticket(
    client: &mut FlightServiceClient<Channel>,
    ticket: arrow_flight::Ticket,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let response = client.do_get(tonic::Request::new(ticket)).await?;

    // Map tonic::Status errors to FlightError so FlightRecordBatchStream can
    // decode the Arrow IPC stream produced by FlightDataEncoderBuilder.
    let stream = response
        .into_inner()
        .map_err(|e| FlightError::ExternalError(Box::new(e)));

    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(stream)
        .try_collect()
        .await
        .map_err(|e| tonic::Status::internal(format!("flight decode error: {e}")))?;

    Ok(batches)
}

/// Execute `sql` directly on the reference `DuckDBQueryEngine`.
///
/// Applies the same `SqlTransformer` as the Flight service so both the flight
/// path and this reference path execute logically equivalent SQL.
pub async fn execute_reference(
    engine: &Arc<DuckDBQueryEngine>,
    transformer: &tokio::sync::RwLock<SqlTransformer>,
    sql: &str,
) -> Vec<RecordBatch> {
    let transformed = transformer
        .read()
        .await
        .transform(sql)
        .unwrap_or_else(|e| panic!("SQL transformer failed for reference query '{sql}': {e}"));

    let stream = engine
        .execute_query(&transformed.transformed_sql, None, None)
        .await
        .unwrap_or_else(|e| panic!("reference execute_query failed for '{sql}': {e}"));

    stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|e| panic!("reference stream collection failed for '{sql}': {e}"))
}
