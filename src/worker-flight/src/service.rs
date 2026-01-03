//! Arrow Flight query service implementation.
//!
//! This module implements the FlightService trait for SQL query execution
//! over the Arrow Flight protocol.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures_util::stream::{self, BoxStream};
use futures_util::TryStreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, instrument, warn};

use worker_storage::engine::query_engine::QueryEngine;
use worker_storage::sql::SqlTransformer;

use crate::error::{map_query_error_to_status, map_transform_error_to_status};
use crate::ticket::QueryTicket;

/// Arrow Flight query service that provides SQL query execution over Arrow Flight protocol.
///
/// Generic over `Q: QueryEngine` to enable:
/// - Zero-cost abstraction (static dispatch)
/// - Easy testing with mock implementations
/// - Type safety without runtime overhead
///
/// This follows Rust best practices by using generics instead of trait objects,
/// since `QueryEngine` uses RPITIT (Return Position Impl Trait In Traits) which
/// is not object-safe.
pub struct WorkerFlightService<Q>
where
    Q: QueryEngine,
{
    query_engine: Arc<Q>,
    sql_transformer: Arc<SqlTransformer>,
    /// Tenant ID bound at startup (shared-nothing architecture)
    tenant_id: String,
}

impl<Q> WorkerFlightService<Q>
where
    Q: QueryEngine,
{
    /// Create a new FlightQueryService.
    ///
    /// # Arguments
    /// * `query_engine` - The query engine to execute SQL queries
    /// * `sql_transformer` - Transforms logical tables to macro calls
    /// * `tenant_id` - Tenant ID for this worker (validated on each request)
    pub fn new(
        query_engine: Arc<Q>,
        sql_transformer: Arc<SqlTransformer>,
        tenant_id: String,
    ) -> Self {
        Self {
            query_engine,
            sql_transformer,
            tenant_id,
        }
    }

    /// Validate that the request tenant matches this worker's tenant.
    fn validate_tenant(&self, request_tenant: &str) -> Result<(), Status> {
        if request_tenant != self.tenant_id {
            return Err(Status::permission_denied(format!(
                "Tenant ID mismatch: expected '{}', got '{}'",
                self.tenant_id, request_tenant
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl<Q> FlightService for WorkerFlightService<Q>
where
    Q: QueryEngine + 'static,
{
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    /// Handshake is not required for this service.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake is not implemented"))
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    /// List available logical datasets.
    ///
    /// Returns FlightInfo for each registered logical dataset.
    #[instrument(skip(self, _request), level = "debug")]
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        debug!("list_flights called");

        // Get registered logical dataset IDs from the transformer
        let dataset_ids: Vec<String> = self
            .sql_transformer
            .registered_dataset_ids()
            .into_iter()
            .collect();

        // Create FlightInfo for each dataset
        let flight_infos: Vec<Result<FlightInfo, Status>> = dataset_ids
            .into_iter()
            .map(|dataset_id| {
                Ok(FlightInfo::new().with_descriptor(FlightDescriptor::new_path(vec![dataset_id])))
            })
            .collect();

        let stream = stream::iter(flight_infos);
        Ok(Response::new(Box::pin(stream)))
    }
    /// Get flight info for a SQL query.
    ///
    /// The FlightDescriptor.cmd contains the SQL query against logical table names.
    /// Returns schema and endpoint information for subsequent DoGet calls.
    #[instrument(skip(self, request), level = "debug")]
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        debug!(?descriptor, "get_flight_info called");

        // Extract SQL from command
        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid SQL encoding: {}", e)))?;

        if sql.is_empty() {
            return Err(Status::invalid_argument("Empty SQL query"));
        }

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed"
        );

        // Extract schema from the first logical table that was replaced
        let schema = if !transform_result.tables_replaced.is_empty() {
            let first_table = &transform_result.tables_replaced[0];
            debug!(table = %first_table, "Extracting schema from first logical table");
            
            self.query_engine
                .get_table_schema(first_table)
                .await
                .map_err(map_query_error_to_status)?
        } else {
            // No logical tables found in the query
            // This could happen for queries like "SELECT 1" or queries against non-registered tables
            return Err(Status::invalid_argument(
                "Cannot determine schema: query does not reference any registered logical tables. \
                 Use get_schema with a query containing logical table references."
            ));
        };

        debug!(fields = schema.fields().len(), "Schema extracted successfully");

        // Create a ticket with the original SQL (transformation happens at DoGet time)
        let ticket = QueryTicket::new(sql.clone(), self.tenant_id.clone());
        let encoded_ticket = ticket
            .encode()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Build FlightInfo with endpoint and schema
        let endpoint = FlightEndpoint::new().with_ticket(encoded_ticket);

        let flight_info = FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Failed to set schema in FlightInfo: {}", e)))?;

        Ok(Response::new(flight_info))
    }
    /// Poll flight info is not supported.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }
    /// Get schema for a query (delegates to get_flight_info).
    #[instrument(skip(self, request), level = "debug")]
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        debug!(?descriptor, "get_schema called");

        // Extract SQL from command
        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid SQL encoding: {}", e)))?;

        if sql.is_empty() {
            return Err(Status::invalid_argument("Empty SQL query"));
        }

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed for get_schema"
        );

        // Extract schema from the first logical table
        let schema = if !transform_result.tables_replaced.is_empty() {
            let first_table = &transform_result.tables_replaced[0];
            debug!(table = %first_table, "Extracting schema from first logical table");
            
            self.query_engine
                .get_table_schema(first_table)
                .await
                .map_err(map_query_error_to_status)?
        } else {
            // No logical tables found in the query
            return Err(Status::invalid_argument(
                "Cannot determine schema: query does not reference any registered logical tables"
            ));
        };

        debug!(fields = schema.fields().len(), "Schema extracted successfully");

        // Encode schema to Arrow IPC format
        let schema_bytes = {
            let mut buf = Vec::new();
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &schema)
                .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?;
            writer.finish()
                .map_err(|e| Status::internal(format!("Failed to finish schema encoding: {}", e)))?;
            buf
        };

        Ok(Response::new(SchemaResult {
            schema: schema_bytes.into(),
        }))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Execute a query and stream results as FlightData.
    ///
    /// The Ticket contains a serialized QueryTicket with the SQL query.
    #[instrument(skip(self, request), level = "debug")]
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        debug!("do_get called");

        // Decode the ticket
        let query_ticket = QueryTicket::decode(&ticket)?;
        debug!(sql = %query_ticket.sql, tenant = %query_ticket.tenant_id, "Decoded ticket");

        // Validate tenant
        self.validate_tenant(&query_ticket.tenant_id)?;

        // Transform SQL: logical tables -> macro calls
        let transform_result = self
            .sql_transformer
            .transform(&query_ticket.sql)
            .map_err(map_transform_error_to_status)?;

        debug!(
            original = %query_ticket.sql,
            transformed = %transform_result.transformed_sql,
            tables_replaced = ?transform_result.tables_replaced,
            "SQL transformed for execution"
        );

        // Execute the transformed query
        let timeout = query_ticket.timeout_secs.map(Duration::from_secs);
        let result_stream = self
            .query_engine
            .execute_query(
                &transform_result.transformed_sql,
                query_ticket.memory_limit_mb,
                timeout,
            )
            .await
            .map_err(map_query_error_to_status)?;

        // Convert QueryResultStream to FlightData stream
        // The stream yields Result<RecordBatch, QueryError>
        // FlightDataEncoder expects impl Stream<Item = Result<RecordBatch, FlightError>>
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(result_stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    /// DoPut is not supported (read-only query service).
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "DoPut is not supported; this is a read-only query service",
        ))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// DoExchange is not supported.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    /// Execute custom actions.
    ///
    /// Supported actions:
    /// - `health_check`: Returns service health status
    /// - `cancel_query`: Cancel a running query (TODO)
    #[instrument(skip(self, request), level = "debug")]
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        debug!(action_type = %action.r#type, "do_action called");

        match action.r#type.as_str() {
            "health_check" => {
                let result = arrow_flight::Result {
                    body: b"OK".to_vec().into(),
                };
                let stream = stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "cancel_query" => {
                // TODO: Implement query cancellation
                warn!("cancel_query action not yet implemented");
                Err(Status::unimplemented("cancel_query is not yet implemented"))
            }
            _ => {
                warn!(action_type = %action.r#type, "Unknown action type");
                Err(Status::invalid_argument(format!(
                    "Unknown action type: {}",
                    action.r#type
                )))
            }
        }
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    /// List available actions.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "health_check".to_string(),
                description: "Check service health".to_string(),
            }),
            Ok(ActionType {
                r#type: "cancel_query".to_string(),
                description: "Cancel a running query (not yet implemented)".to_string(),
            }),
        ];

        let stream = stream::iter(actions);
        Ok(Response::new(Box::pin(stream)))
    }
}
