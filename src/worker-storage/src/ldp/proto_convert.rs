//! Proto conversion layer for LDP types.
//!
//! Provides bidirectional conversion between Rust LDP types (`ldp::types::*`)
//! and proto-generated types (`worker_proto::proto::ldp::*`).
//!
//! This module enables:
//! - Serializing LDP plans for transmission over Arrow Flight
//! - Deserializing SubmitStageRequest from Flight DoAction
//! - Converting between internal and wire formats

use crate::ldp::types::{
    Distribution, Exchange, ExchangeEdge, ExchangeId, LdpPlan, QueryId, Stage, StageId, StageInput,
    StageLimits, StageOutput, WorkerId,
};
use crate::sql::logical_plan::ColumnRef;
use prost::Message;
use worker_proto::proto::ldp as proto;

// ============================================================================
// Error Types
// ============================================================================

/// Error during proto conversion.
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("Missing required field: {0}")]
    MissingField(&'static str),

    #[error("Invalid enum variant for {0}")]
    InvalidVariant(&'static str),

    #[error("Proto encoding error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Proto decoding error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Custom error: {0}")]
    Custom(String),
}

pub type ConversionResult<T> = Result<T, ConversionError>;

// ============================================================================
// ColumnRef Conversion Helpers
// ============================================================================

fn column_refs_to_proto(refs: &[ColumnRef]) -> Vec<proto::ColumnRef> {
    refs.iter()
        .map(|cr| proto::ColumnRef {
            table: cr.table.clone(),
            column: cr.column.clone(),
        })
        .collect()
}

fn proto_to_column_refs(refs: &[proto::ColumnRef]) -> Vec<ColumnRef> {
    refs.iter()
        .map(|cr| ColumnRef {
            table: cr.table.clone(),
            column: cr.column.clone(),
        })
        .collect()
}

// ============================================================================
// Distribution Conversion
// ============================================================================

impl From<&Distribution> for proto::Distribution {
    fn from(dist: &Distribution) -> Self {
        let distribution_type = match dist {
            Distribution::Singleton { worker } => {
                proto::distribution::DistributionType::Singleton(proto::SingletonDistribution {
                    worker_id: worker.0.clone(),
                })
            }
            Distribution::HashPartitioned {
                column_refs,
                workers,
            } => proto::distribution::DistributionType::HashPartitioned(
                proto::HashPartitionedDistribution {
                    column_refs: column_refs_to_proto(column_refs),
                    worker_ids: workers.iter().map(|w| w.0.clone()).collect(),
                },
            ),
            Distribution::EpochPartitioned { workers } => {
                proto::distribution::DistributionType::EpochPartitioned(
                    proto::EpochPartitionedDistribution {
                        worker_ids: workers.iter().map(|w| w.0.clone()).collect(),
                    },
                )
            }
            Distribution::Replicated { workers } => {
                proto::distribution::DistributionType::Replicated(proto::ReplicatedDistribution {
                    worker_ids: workers.iter().map(|w| w.0.clone()).collect(),
                })
            }
        };

        proto::Distribution {
            distribution_type: Some(distribution_type),
        }
    }
}

impl TryFrom<&proto::Distribution> for Distribution {
    type Error = ConversionError;

    fn try_from(proto: &proto::Distribution) -> ConversionResult<Self> {
        match &proto.distribution_type {
            Some(proto::distribution::DistributionType::Singleton(s)) => {
                Ok(Distribution::Singleton {
                    worker: WorkerId::from(s.worker_id.clone()),
                })
            }
            Some(proto::distribution::DistributionType::HashPartitioned(h)) => {
                Ok(Distribution::HashPartitioned {
                    column_refs: proto_to_column_refs(&h.column_refs),
                    workers: h.worker_ids.iter().cloned().map(WorkerId::from).collect(),
                })
            }
            Some(proto::distribution::DistributionType::EpochPartitioned(e)) => {
                Ok(Distribution::EpochPartitioned {
                    workers: e.worker_ids.iter().cloned().map(WorkerId::from).collect(),
                })
            }
            Some(proto::distribution::DistributionType::Replicated(r)) => {
                Ok(Distribution::Replicated {
                    workers: r.worker_ids.iter().cloned().map(WorkerId::from).collect(),
                })
            }
            None => Err(ConversionError::MissingField("distribution_type")),
        }
    }
}

// ============================================================================
// Exchange Conversion
// ============================================================================

impl From<&Exchange> for proto::Exchange {
    fn from(exchange: &Exchange) -> Self {
        let exchange_type = match exchange {
            Exchange::Gather { target } => {
                proto::exchange::ExchangeType::Gather(proto::GatherExchange {
                    target_worker_id: target.0.clone(),
                })
            }
            Exchange::Broadcast { targets } => {
                proto::exchange::ExchangeType::Broadcast(proto::BroadcastExchange {
                    target_worker_ids: targets.iter().map(|w| w.0.clone()).collect(),
                })
            }
            Exchange::HashPartition {
                column_refs,
                partitions,
            } => proto::exchange::ExchangeType::HashPartition(proto::HashPartitionExchange {
                column_refs: column_refs_to_proto(column_refs),
                partitions: *partitions,
            }),
        };

        proto::Exchange {
            exchange_type: Some(exchange_type),
        }
    }
}

impl TryFrom<&proto::Exchange> for Exchange {
    type Error = ConversionError;

    fn try_from(proto: &proto::Exchange) -> ConversionResult<Self> {
        match &proto.exchange_type {
            Some(proto::exchange::ExchangeType::Gather(g)) => Ok(Exchange::Gather {
                target: WorkerId::from(g.target_worker_id.clone()),
            }),
            Some(proto::exchange::ExchangeType::Broadcast(b)) => Ok(Exchange::Broadcast {
                targets: b
                    .target_worker_ids
                    .iter()
                    .cloned()
                    .map(WorkerId::from)
                    .collect(),
            }),
            Some(proto::exchange::ExchangeType::HashPartition(h)) => Ok(Exchange::HashPartition {
                column_refs: proto_to_column_refs(&h.column_refs),
                partitions: h.partitions,
            }),
            None => Err(ConversionError::MissingField("exchange_type")),
        }
    }
}

// ============================================================================
// StageInput Conversion
// ============================================================================

impl From<&StageInput> for proto::StageInput {
    fn from(input: &StageInput) -> Self {
        let input_type = match input {
            StageInput::LocalCatalog => {
                proto::stage_input::InputType::LocalCatalog(proto::LocalCatalogInput {})
            }
            StageInput::ExchangeInput {
                exchange_id,
                table_name,
            } => proto::stage_input::InputType::Exchange(proto::ExchangeInput {
                exchange_id: exchange_id.0,
                table_name: table_name.clone(),
            }),
        };

        proto::StageInput {
            input_type: Some(input_type),
        }
    }
}

impl TryFrom<&proto::StageInput> for StageInput {
    type Error = ConversionError;

    fn try_from(proto: &proto::StageInput) -> ConversionResult<Self> {
        match &proto.input_type {
            Some(proto::stage_input::InputType::LocalCatalog(_)) => Ok(StageInput::LocalCatalog),
            Some(proto::stage_input::InputType::Exchange(e)) => Ok(StageInput::ExchangeInput {
                exchange_id: ExchangeId(e.exchange_id),
                table_name: e.table_name.clone(),
            }),
            None => Err(ConversionError::MissingField("input_type")),
        }
    }
}

// ============================================================================
// StageOutput Conversion
// ============================================================================

impl From<&StageOutput> for proto::StageOutput {
    fn from(output: &StageOutput) -> Self {
        let output_type = match output {
            StageOutput::Stream => proto::stage_output::OutputType::Stream(proto::StreamOutput {}),
            StageOutput::Partitioned {
                partitions,
                column_refs,
            } => proto::stage_output::OutputType::Partitioned(proto::PartitionedOutput {
                partitions: *partitions,
                column_refs: column_refs_to_proto(column_refs),
            }),
        };

        proto::StageOutput {
            output_type: Some(output_type),
        }
    }
}

impl TryFrom<&proto::StageOutput> for StageOutput {
    type Error = ConversionError;

    fn try_from(proto: &proto::StageOutput) -> ConversionResult<Self> {
        match &proto.output_type {
            Some(proto::stage_output::OutputType::Stream(_)) => Ok(StageOutput::Stream),
            Some(proto::stage_output::OutputType::Partitioned(p)) => Ok(StageOutput::Partitioned {
                partitions: p.partitions,
                column_refs: proto_to_column_refs(&p.column_refs),
            }),
            None => Err(ConversionError::MissingField("output_type")),
        }
    }
}

// ============================================================================
// StageLimits Conversion
// ============================================================================

impl From<&StageLimits> for proto::StageLimits {
    fn from(limits: &StageLimits) -> Self {
        proto::StageLimits {
            max_bytes_output: limits.max_bytes_output,
            max_rows_output: limits.max_rows_output,
            timeout_ms: limits.timeout_ms,
            memory_bytes: limits.memory_bytes,
        }
    }
}

impl From<&proto::StageLimits> for StageLimits {
    fn from(proto: &proto::StageLimits) -> Self {
        StageLimits {
            max_bytes_output: proto.max_bytes_output,
            max_rows_output: proto.max_rows_output,
            timeout_ms: proto.timeout_ms,
            memory_bytes: proto.memory_bytes,
        }
    }
}

// ============================================================================
// Stage Conversion
// ============================================================================

impl From<&Stage> for proto::Stage {
    fn from(stage: &Stage) -> Self {
        proto::Stage {
            stage_id: stage.stage_id.0,
            target_worker_ids: stage.target_workers.iter().map(|w| w.0.clone()).collect(),
            inputs: stage.inputs.iter().map(proto::StageInput::from).collect(),
            output: Some(proto::StageOutput::from(&stage.output)),
            stage_sql: stage.stage_sql.clone(),
            limits: Some(proto::StageLimits::from(&stage.limits)),
        }
    }
}

impl TryFrom<&proto::Stage> for Stage {
    type Error = ConversionError;

    fn try_from(proto: &proto::Stage) -> ConversionResult<Self> {
        let inputs: ConversionResult<Vec<StageInput>> =
            proto.inputs.iter().map(StageInput::try_from).collect();

        let output = proto
            .output
            .as_ref()
            .map(StageOutput::try_from)
            .transpose()?
            .unwrap_or_default();

        let limits = proto
            .limits
            .as_ref()
            .map(StageLimits::from)
            .unwrap_or_default();

        Ok(Stage {
            stage_id: StageId(proto.stage_id),
            target_workers: proto
                .target_worker_ids
                .iter()
                .cloned()
                .map(WorkerId::from)
                .collect(),
            inputs: inputs?,
            output,
            stage_sql: proto.stage_sql.clone(),
            limits,
        })
    }
}

// ============================================================================
// ExchangeEdge Conversion
// ============================================================================

impl From<&ExchangeEdge> for proto::ExchangeEdge {
    fn from(edge: &ExchangeEdge) -> Self {
        proto::ExchangeEdge {
            exchange_id: edge.exchange_id.0,
            kind: Some(proto::Exchange::from(&edge.kind)),
            from_stage_id: edge.from_stage.0,
            to_stage_id: edge.to_stage.0,
            partition_to_worker: edge
                .partition_to_worker
                .iter()
                .map(|w| w.0.clone())
                .collect(),
        }
    }
}

impl TryFrom<&proto::ExchangeEdge> for ExchangeEdge {
    type Error = ConversionError;

    fn try_from(proto: &proto::ExchangeEdge) -> ConversionResult<Self> {
        let kind = proto
            .kind
            .as_ref()
            .ok_or(ConversionError::MissingField("kind"))?;

        Ok(ExchangeEdge {
            exchange_id: ExchangeId(proto.exchange_id),
            kind: Exchange::try_from(kind)?,
            from_stage: StageId(proto.from_stage_id),
            to_stage: StageId(proto.to_stage_id),
            partition_to_worker: proto
                .partition_to_worker
                .iter()
                .cloned()
                .map(WorkerId::from)
                .collect(),
        })
    }
}

// ============================================================================
// LdpPlan Conversion
// ============================================================================

impl From<&LdpPlan> for proto::LdpPlan {
    fn from(plan: &LdpPlan) -> Self {
        proto::LdpPlan {
            query_id: plan.query_id.0.clone(),
            coordinator_worker_id: plan.coordinator.0.clone(),
            stages: plan.stages.iter().map(proto::Stage::from).collect(),
            edges: plan.edges.iter().map(proto::ExchangeEdge::from).collect(),
        }
    }
}

impl TryFrom<&proto::LdpPlan> for LdpPlan {
    type Error = ConversionError;

    fn try_from(proto: &proto::LdpPlan) -> ConversionResult<Self> {
        let stages: ConversionResult<Vec<Stage>> =
            proto.stages.iter().map(Stage::try_from).collect();

        let edges: ConversionResult<Vec<ExchangeEdge>> =
            proto.edges.iter().map(ExchangeEdge::try_from).collect();

        let stages = stages?;
        let root_stage = stages.last().map(|s| s.stage_id).unwrap_or_default();

        Ok(LdpPlan {
            query_id: QueryId::from(proto.query_id.clone()),
            coordinator: WorkerId::from(proto.coordinator_worker_id.clone()),
            stages,
            edges: edges?,
            root_stage,
        })
    }
}

// ============================================================================
// Serialization Helpers
// ============================================================================

/// Serialize an LdpPlan to proto bytes.
pub fn serialize_ldp_plan(plan: &LdpPlan) -> ConversionResult<Vec<u8>> {
    let proto_plan = proto::LdpPlan::from(plan);
    let mut buf = Vec::with_capacity(proto_plan.encoded_len());
    proto_plan.encode(&mut buf)?;
    Ok(buf)
}

/// Deserialize an LdpPlan from proto bytes.
pub fn deserialize_ldp_plan(bytes: &[u8]) -> ConversionResult<LdpPlan> {
    let proto_plan = proto::LdpPlan::decode(bytes)?;
    LdpPlan::try_from(&proto_plan)
}

/// Serialize a SubmitStageRequest to proto bytes.
///
/// Exchange inputs are serialized as Arrow IPC streams for efficient transmission.
pub fn serialize_submit_stage_request(
    tenant_id: &str,
    query_id: &str,
    stage_id: StageId,
    stage_sql: &str,
    limits: &StageLimits,
    exchange_inputs: &std::collections::HashMap<String, Vec<arrow::record_batch::RecordBatch>>,
) -> ConversionResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    use std::io::Cursor;

    // Serialize each exchange input table to Arrow IPC format
    let mut serialized_inputs = Vec::new();

    for (table_name, batches) in exchange_inputs {
        if batches.is_empty() {
            // Register table with empty schema placeholder
            serialized_inputs.push(proto::ExchangeInputRegistration {
                table_name: table_name.clone(),
                arrow_ipc_stream: vec![],
            });
            continue;
        }

        // Serialize batches to Arrow IPC stream
        let schema = batches[0].schema();
        let mut buffer = Cursor::new(Vec::new());

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(|e| {
                ConversionError::Custom(format!("Failed to create Arrow IPC writer: {}", e))
            })?;

            for batch in batches {
                writer.write(batch).map_err(|e| {
                    ConversionError::Custom(format!("Failed to write batch: {}", e))
                })?;
            }

            writer.finish().map_err(|e| {
                ConversionError::Custom(format!("Failed to finish Arrow IPC stream: {}", e))
            })?;
        }

        serialized_inputs.push(proto::ExchangeInputRegistration {
            table_name: table_name.clone(),
            arrow_ipc_stream: buffer.into_inner(),
        });
    }

    let request = proto::SubmitStageRequest {
        tenant_id: tenant_id.to_string(),
        query_id: query_id.to_string(),
        stage_id: stage_id.0,
        stage_sql: stage_sql.to_string(),
        limits: Some(proto::StageLimits::from(limits)),
        exchange_inputs: serialized_inputs,
    };

    let mut buf = Vec::with_capacity(request.encoded_len());
    request.encode(&mut buf)?;
    Ok(buf)
}

/// Deserialize a SubmitStageRequest from proto bytes.
pub fn deserialize_submit_stage_request(
    bytes: &[u8],
) -> ConversionResult<proto::SubmitStageRequest> {
    Ok(proto::SubmitStageRequest::decode(bytes)?)
}

/// Serialize a SubmitStageResponse to proto bytes.
pub fn serialize_submit_stage_response(
    success: bool,
    error_message: &str,
    result_ticket: &[u8],
    stats: Option<(u64, u64, u64)>, // (rows, bytes, time_ms)
) -> ConversionResult<Vec<u8>> {
    let response = proto::SubmitStageResponse {
        success,
        error_message: error_message.to_string(),
        result_ticket: result_ticket.to_vec(),
        stats: stats.map(|(rows, bytes, time_ms)| proto::StageExecutionStats {
            rows_produced: rows,
            bytes_produced: bytes,
            execution_time_ms: time_ms,
        }),
    };

    let mut buf = Vec::with_capacity(response.encoded_len());
    response.encode(&mut buf)?;
    Ok(buf)
}

/// Deserialize a SubmitStageResponse from proto bytes.
pub fn deserialize_submit_stage_response(
    bytes: &[u8],
) -> ConversionResult<proto::SubmitStageResponse> {
    Ok(proto::SubmitStageResponse::decode(bytes)?)
}

/// Deserialize exchange inputs from Arrow IPC streams.
///
/// Returns a HashMap mapping table names to Arrow RecordBatches.
/// This is used on the worker side to reconstruct exchange input data.
pub fn deserialize_exchange_inputs(
    exchange_inputs: &[proto::ExchangeInputRegistration],
) -> ConversionResult<std::collections::HashMap<String, Vec<arrow::record_batch::RecordBatch>>> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let mut result = std::collections::HashMap::new();

    for input_reg in exchange_inputs {
        if input_reg.arrow_ipc_stream.is_empty() {
            // Empty stream - register empty batches
            result.insert(input_reg.table_name.clone(), vec![]);
            continue;
        }

        // Deserialize Arrow IPC stream
        let cursor = Cursor::new(&input_reg.arrow_ipc_stream);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            ConversionError::DecodeError(prost::DecodeError::new(format!(
                "Failed to create Arrow IPC reader: {}",
                e
            )))
        })?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                ConversionError::DecodeError(prost::DecodeError::new(format!(
                    "Failed to read batch: {}",
                    e
                )))
            })?;
            batches.push(batch);
        }

        result.insert(input_reg.table_name.clone(), batches);
    }

    Ok(result)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distribution_roundtrip() {
        let distributions = vec![
            Distribution::Singleton {
                worker: WorkerId::from("w1"),
            },
            Distribution::HashPartitioned {
                column_refs: vec![
                    ColumnRef::unqualified("col0"),
                    ColumnRef::unqualified("col1"),
                ],
                workers: vec![WorkerId::from("w1"), WorkerId::from("w2")],
            },
            Distribution::EpochPartitioned {
                workers: vec![WorkerId::from("w1")],
            },
            Distribution::Replicated {
                workers: vec![WorkerId::from("w1"), WorkerId::from("w2")],
            },
        ];

        for dist in distributions {
            let proto_dist = proto::Distribution::from(&dist);
            let roundtrip = Distribution::try_from(&proto_dist).unwrap();
            assert_eq!(dist, roundtrip);
        }
    }

    #[test]
    fn test_exchange_roundtrip() {
        let exchanges = vec![
            Exchange::Gather {
                target: WorkerId::from("coordinator"),
            },
            Exchange::Broadcast {
                targets: vec![WorkerId::from("w1"), WorkerId::from("w2")],
            },
            Exchange::HashPartition {
                column_refs: vec![ColumnRef::unqualified("col0")],
                partitions: 16,
            },
        ];

        for exchange in exchanges {
            let proto_exchange = proto::Exchange::from(&exchange);
            let roundtrip = Exchange::try_from(&proto_exchange).unwrap();
            assert_eq!(exchange, roundtrip);
        }
    }

    #[test]
    fn test_stage_input_roundtrip() {
        let inputs = vec![
            StageInput::LocalCatalog,
            StageInput::ExchangeInput {
                exchange_id: ExchangeId(42),
                table_name: "__exchange_42".to_string(),
            },
        ];

        for input in inputs {
            let proto_input = proto::StageInput::from(&input);
            let roundtrip = StageInput::try_from(&proto_input).unwrap();
            assert_eq!(input, roundtrip);
        }
    }

    #[test]
    fn test_stage_output_roundtrip() {
        let outputs = vec![
            StageOutput::Stream,
            StageOutput::Partitioned {
                partitions: 8,
                column_refs: vec![
                    ColumnRef::unqualified("col0"),
                    ColumnRef::unqualified("col1"),
                ],
            },
        ];

        for output in outputs {
            let proto_output = proto::StageOutput::from(&output);
            let roundtrip = StageOutput::try_from(&proto_output).unwrap();
            assert_eq!(output, roundtrip);
        }
    }

    #[test]
    fn test_stage_limits_roundtrip() {
        let limits = StageLimits {
            max_bytes_output: 1024 * 1024,
            max_rows_output: 10000,
            timeout_ms: 30000,
            memory_bytes: 512 * 1024 * 1024,
        };

        let proto_limits = proto::StageLimits::from(&limits);
        let roundtrip = StageLimits::from(&proto_limits);
        assert_eq!(limits, roundtrip);
    }

    #[test]
    fn test_stage_roundtrip() {
        let stage = Stage {
            stage_id: StageId(1),
            target_workers: vec![WorkerId::from("w1"), WorkerId::from("w2")],
            inputs: vec![
                StageInput::LocalCatalog,
                StageInput::ExchangeInput {
                    exchange_id: ExchangeId(0),
                    table_name: "__exchange_0".to_string(),
                },
            ],
            output: StageOutput::Partitioned {
                partitions: 4,
                column_refs: vec![ColumnRef::unqualified("col0")],
            },
            stage_sql: "SELECT col0 FROM __exchange_0".to_string(),
            limits: StageLimits::default(),
        };

        let proto_stage = proto::Stage::from(&stage);
        let roundtrip = Stage::try_from(&proto_stage).unwrap();

        assert_eq!(stage.stage_id, roundtrip.stage_id);
        assert_eq!(stage.target_workers, roundtrip.target_workers);
        assert_eq!(stage.inputs, roundtrip.inputs);
        assert_eq!(stage.output, roundtrip.output);
        assert_eq!(stage.stage_sql, roundtrip.stage_sql);
    }

    #[test]
    fn test_ldp_plan_roundtrip() {
        let mut plan = LdpPlan::new(QueryId::from("query-123"), WorkerId::from("coordinator"));
        plan.stages
            .push(Stage::new(StageId(0), "SELECT * FROM t1".to_string()));
        plan.stages.push(Stage::new(
            StageId(1),
            "SELECT * FROM __exchange_0".to_string(),
        ));
        plan.edges.push(ExchangeEdge {
            exchange_id: ExchangeId(0),
            kind: Exchange::Gather {
                target: WorkerId::from("coordinator"),
            },
            from_stage: StageId(0),
            to_stage: StageId(1),
            partition_to_worker: vec![],
        });
        plan.root_stage = StageId(1);

        let bytes = serialize_ldp_plan(&plan).unwrap();
        let roundtrip = deserialize_ldp_plan(&bytes).unwrap();

        assert_eq!(plan.query_id, roundtrip.query_id);
        assert_eq!(plan.coordinator, roundtrip.coordinator);
        assert_eq!(plan.stages.len(), roundtrip.stages.len());
        assert_eq!(plan.edges.len(), roundtrip.edges.len());
    }

    #[test]
    fn test_submit_stage_request_roundtrip() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let limits = StageLimits::default();

        // Create a simple RecordBatch for exchange input
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut exchange_inputs = std::collections::HashMap::new();
        exchange_inputs.insert("__exchange_0".to_string(), vec![batch]);

        let bytes = serialize_submit_stage_request(
            "tenant-1",
            "query-123",
            StageId(5),
            "SELECT * FROM table1",
            &limits,
            &exchange_inputs,
        )
        .unwrap();

        let request = deserialize_submit_stage_request(&bytes).unwrap();
        assert_eq!(request.tenant_id, "tenant-1");
        assert_eq!(request.query_id, "query-123");
        assert_eq!(request.stage_id, 5);
        assert_eq!(request.stage_sql, "SELECT * FROM table1");
        assert_eq!(request.exchange_inputs.len(), 1);
    }

    #[test]
    fn test_submit_stage_response_roundtrip() {
        let bytes =
            serialize_submit_stage_response(true, "", &[7, 8, 9], Some((1000, 4096, 50))).unwrap();

        let response = deserialize_submit_stage_response(&bytes).unwrap();
        assert!(response.success);
        assert_eq!(response.result_ticket, vec![7, 8, 9]);
        assert_eq!(response.stats.unwrap().rows_produced, 1000);
    }
}
