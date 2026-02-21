//! Proto-boundary conversion module.
//!
//! **All** translation between `prost`-generated types and the domain types in
//! [`crate::types`] lives here.  No other module in `worker-cp` may import
//! `worker_proto` types.
//!
//! # Design Rationale
//!
//! A single conversion firewall prevents proto-generated types from leaking
//! into business logic.  This makes it trivial to update proto definitions
//! without touching any code outside this file.

use crate::types::{
    CatalogInfo, ControlCommand, DataSourceInfo, LoadAssignment, LoadStatus, OutgoingPayload,
    PoolOptions, TimeRange,
};
use std::collections::HashMap;
use worker_proto::common;
use worker_proto::control_plane::{
    data_pull_status_update_event::Status as ProtoStatus, event_stream_message::Payload,
    CommonAckEvent, DataAssignmentEvent, DataPullStatusUpdateEvent, EventStreamMessage,
    HeartbeatEvent, RegisterEvent, Worker,
};

// ---------------------------------------------------------------------------
// Public error type (crate-internal)
// ---------------------------------------------------------------------------

/// Error returned when a proto message is structurally invalid or missing a
/// required field.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConversionError {
    #[error("missing required field `{field}` in `{message_type}`")]
    MissingField {
        message_type: &'static str,
        field: &'static str,
    },
}

// ---------------------------------------------------------------------------
// Inbound: proto → domain
// ---------------------------------------------------------------------------

/// Convert a [`DataAssignmentEvent`] into a [`LoadAssignment`].
///
/// Returns `Err(ConversionError::MissingField)` if any required nested message
/// is absent, rather than panicking.
pub(crate) fn assignment_from_proto(
    ev: DataAssignmentEvent,
) -> Result<LoadAssignment, ConversionError> {
    let plan = ev.load_plan.ok_or(ConversionError::MissingField {
        message_type: "DataAssignmentEvent",
        field: "load_plan",
    })?;

    let source_proto = plan.source.ok_or(ConversionError::MissingField {
        message_type: "LoadPlan",
        field: "source",
    })?;

    let source = data_source_from_proto(source_proto)?;

    let (time_range, time_column_name, time_partition_value, dimension_values) =
        if let Some(rp) = plan.resolved_partition {
            let tr = rp.time_range.map(time_range_from_proto).transpose()?;
            (
                tr,
                rp.time_column_name,
                rp.time_partition_value,
                rp.dimension_values,
            )
        } else {
            (None, String::new(), String::new(), HashMap::new())
        };

    Ok(LoadAssignment {
        dataset_id: ev.dataset_id,
        epoch_id: ev.epoch_id,
        plan_id: plan.plan_id,
        destination_table_name: plan.destination_table_name,
        source,
        arrow_schema_bytes: plan.arrow_schema,
        time_range,
        time_column_name,
        time_partition_value,
        dimension_values,
    })
}

/// Convert a [`CommonAckEvent`] into a [`ControlCommand`].
///
/// Unknown `event_type` values are silently mapped to [`ControlCommand::Unknown`].
pub(crate) fn command_from_ack(ack: CommonAckEvent) -> ControlCommand {
    let fields: HashMap<String, String> = ack
        .payload
        .map(|s| extract_string_fields(&s.fields))
        .unwrap_or_default();

    match ack.event_type.as_str() {
        "evict_epoch" => ControlCommand::EvictEpoch {
            dataset_id: fields.get("dataset_id").cloned().unwrap_or_default(),
            epoch_id: fields.get("epoch_id").cloned().unwrap_or_default(),
            reason: fields.get("reason").cloned().unwrap_or_default(),
        },
        "drain_command" => ControlCommand::Drain,
        other => ControlCommand::Unknown {
            event_type: other.to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
// Outbound: domain → proto
// ---------------------------------------------------------------------------

/// Wrap an [`OutgoingPayload`] into an [`EventStreamMessage`] ready to be sent.
///
/// A fresh `event_id` (UUID v4) is stamped on every message.
pub(crate) fn wrap_outgoing(
    worker_id: &str,
    tenant_id: &str,
    payload: OutgoingPayload,
) -> EventStreamMessage {
    let event_id = uuid::Uuid::new_v4().to_string();
    let proto_payload = match payload {
        OutgoingPayload::Heartbeat => Payload::HeartbeatEvent(HeartbeatEvent {}),
        OutgoingPayload::StatusUpdate {
            dataset_id,
            epoch_id,
            status,
            error,
        } => Payload::DataPullStatusUpdate(DataPullStatusUpdateEvent {
            dataset_id,
            epoch_id,
            status: load_status_to_proto(status) as i32,
            error_message: error.unwrap_or_default(),
        }),
    };

    EventStreamMessage {
        event_id,
        tenant_id: tenant_id.to_string(),
        worker_id: worker_id.to_string(),
        payload: Some(proto_payload),
    }
}

/// Build the initial registration `EventStreamMessage`.
pub(crate) fn register_event(worker_id: &str, tenant_id: &str, addr: &str) -> EventStreamMessage {
    let event_id = uuid::Uuid::new_v4().to_string();
    let mut labels = std::collections::HashMap::new();
    labels.insert("tenant_id".to_string(), tenant_id.to_string());
    EventStreamMessage {
        event_id,
        tenant_id: tenant_id.to_string(),
        worker_id: worker_id.to_string(),
        payload: Some(Payload::RegisterEvent(RegisterEvent {
            worker: Some(Worker {
                node_id: worker_id.to_string(),
                addr: addr.to_string(),
                labels,
            }),
        })),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn data_source_from_proto(ds: common::DataSource) -> Result<DataSourceInfo, ConversionError> {
    let catalog_proto = ds.catalog.ok_or(ConversionError::MissingField {
        message_type: "DataSource",
        field: "catalog",
    })?;
    let catalog = catalog_from_proto(catalog_proto)?;
    Ok(DataSourceInfo {
        catalog,
        database: ds.database,
        table: ds.table,
        version: ds.version,
    })
}

fn catalog_from_proto(c: common::Catalog) -> Result<CatalogInfo, ConversionError> {
    use worker_proto::proto::common::catalog::CatalogType;

    let ct = c.catalog_type.ok_or(ConversionError::MissingField {
        message_type: "Catalog",
        field: "catalog_type",
    })?;

    match ct {
        CatalogType::Iceberg(ic) => Ok(CatalogInfo::Iceberg { uri: ic.uri }),
        CatalogType::Starrocks(sr) => Ok(CatalogInfo::StarRocks {
            catalog_name: sr.catalog_name,
            host: sr.host,
            port: sr.port as u16,
            database: sr.database,
            username: sr.username,
            password: sr.password,
            max_concurrency: if sr.max_concurrency > 0 {
                Some(sr.max_concurrency as u32)
            } else {
                None
            },
            pool_options: sr.pool_options.map(pool_options_from_proto),
        }),
        CatalogType::Jdbc(jc) => Ok(CatalogInfo::Jdbc {
            uri: jc.uri,
            driver: jc.driver,
            pool_options: jc.pool_options.map(pool_options_from_proto),
        }),
    }
}

fn pool_options_from_proto(p: common::ConnectionPoolOptions) -> PoolOptions {
    PoolOptions {
        min_connections: p.min_connections.unwrap_or(1),
        max_connections: p.max_connections.unwrap_or(16),
        connect_timeout_ms: p.connect_timeout_ms.unwrap_or(5_000),
        idle_timeout_ms: p.idle_timeout_ms.unwrap_or(600_000),
    }
}

fn time_range_from_proto(tr: common::TimeRange) -> Result<TimeRange, ConversionError> {
    let start = tr.start_inclusive.ok_or(ConversionError::MissingField {
        message_type: "TimeRange",
        field: "start_inclusive",
    })?;
    let end = tr.end_exclusive.ok_or(ConversionError::MissingField {
        message_type: "TimeRange",
        field: "end_exclusive",
    })?;
    Ok(TimeRange {
        start_inclusive_ns: start.seconds * 1_000_000_000 + start.nanos as i64,
        end_exclusive_ns: end.seconds * 1_000_000_000 + end.nanos as i64,
    })
}

fn load_status_to_proto(s: LoadStatus) -> ProtoStatus {
    match s {
        LoadStatus::InProgress => ProtoStatus::InProgress,
        LoadStatus::Completed => ProtoStatus::Completed,
        LoadStatus::Failed => ProtoStatus::Failed,
    }
}

/// Extract `String`-valued fields from a `google.protobuf.Struct`.
fn extract_string_fields(
    fields: &HashMap<String, worker_proto::proto::google::protobuf::Value>,
) -> HashMap<String, String> {
    use worker_proto::proto::google::protobuf::value::Kind;
    fields
        .iter()
        .filter_map(|(k, v)| {
            if let Some(Kind::StringValue(s)) = &v.kind {
                Some((k.clone(), s.clone()))
            } else {
                None
            }
        })
        .collect()
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use worker_proto::proto::google::protobuf;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a minimal valid `DataAssignmentEvent` for testing.
    fn minimal_assignment_event() -> DataAssignmentEvent {
        use worker_proto::control_plane::{LoadPlan, ResolvedPartition};
        use worker_proto::proto::common::{
            catalog::CatalogType, Catalog, DataSource, IcebergCatalog, TimeRange,
        };

        let catalog = Catalog {
            catalog_type: Some(CatalogType::Iceberg(IcebergCatalog {
                uri: "s3://bucket/warehouse".into(),
            })),
        };
        let source = DataSource {
            catalog: Some(catalog),
            database: "analytics_db".into(),
            table: "sales".into(),
            partition: None,
            version: Some("snap-42".into()),
        };

        let start = protobuf::Timestamp {
            seconds: 1_700_000_000,
            nanos: 500,
        };
        let end = protobuf::Timestamp {
            seconds: 1_700_100_000,
            nanos: 0,
        };

        let mut dim = std::collections::HashMap::new();
        dim.insert("region".into(), "US".into());

        let resolved = ResolvedPartition {
            time_range: Some(TimeRange {
                start_inclusive: Some(start),
                end_exclusive: Some(end),
            }),
            time_column_name: "dt".into(),
            time_partition_value: "2025-01-15".into(),
            dimension_values: dim,
        };

        // Arrow IPC schema bytes are tested separately — use empty vec here
        // (will fail decode in runtime_dispatcher, but proto_convert only
        // copies the bytes transparently).
        DataAssignmentEvent {
            dataset_id: "sales".into(),
            epoch_id: "epoch-42".into(),
            load_plan: Some(LoadPlan {
                plan_id: "plan-1".into(),
                source: Some(source),
                destination_table_name: "sales__epoch_42".into(),
                arrow_schema: vec![],
                resolved_partition: Some(resolved),
            }),
        }
    }

    fn make_ack(event_type: &str, fields: &[(&str, &str)]) -> CommonAckEvent {
        let mut map = std::collections::HashMap::new();
        for (k, v) in fields {
            map.insert(
                k.to_string(),
                protobuf::Value {
                    kind: Some(protobuf::value::Kind::StringValue(v.to_string())),
                },
            );
        }
        CommonAckEvent {
            server_id: "master-1".into(),
            event_type: event_type.into(),
            payload: Some(protobuf::Struct { fields: map }),
        }
    }

    // -----------------------------------------------------------------------
    // assignment_from_proto
    // -----------------------------------------------------------------------

    #[test]
    fn assignment_from_proto_happy_path() {
        let ev = minimal_assignment_event();
        let la = assignment_from_proto(ev).expect("conversion must succeed");

        assert_eq!(la.dataset_id, "sales");
        assert_eq!(la.epoch_id, "epoch-42");
        assert_eq!(la.plan_id, "plan-1");
        assert_eq!(la.destination_table_name, "sales__epoch_42");
        assert_eq!(la.time_column_name, "dt");
        assert_eq!(la.time_partition_value, "2025-01-15");
        assert_eq!(la.dimension_values.get("region").unwrap(), "US");
        assert!(la.arrow_schema_bytes.is_empty());

        // Verify source fields.
        match &la.source.catalog {
            CatalogInfo::Iceberg { uri } => assert_eq!(uri, "s3://bucket/warehouse"),
            other => panic!("expected Iceberg, got {other:?}"),
        }
        assert_eq!(la.source.database, "analytics_db");
        assert_eq!(la.source.table, "sales");
        assert_eq!(la.source.version.as_deref(), Some("snap-42"));

        // Verify time range nanosecond conversion.
        let tr = la.time_range.expect("time_range must be Some");
        assert_eq!(tr.start_inclusive_ns, 1_700_000_000 * 1_000_000_000 + 500);
        assert_eq!(tr.end_exclusive_ns, 1_700_100_000 * 1_000_000_000);
    }

    #[test]
    fn assignment_from_proto_missing_load_plan() {
        let ev = DataAssignmentEvent {
            dataset_id: "x".into(),
            epoch_id: "y".into(),
            load_plan: None,
        };
        let err = assignment_from_proto(ev).unwrap_err();
        assert!(err.to_string().contains("load_plan"), "{err}");
    }

    #[test]
    fn assignment_from_proto_missing_source() {
        let mut ev = minimal_assignment_event();
        ev.load_plan.as_mut().unwrap().source = None;
        let err = assignment_from_proto(ev).unwrap_err();
        assert!(err.to_string().contains("source"), "{err}");
    }

    #[test]
    fn assignment_from_proto_missing_catalog() {
        use worker_proto::proto::common::DataSource;
        let mut ev = minimal_assignment_event();
        ev.load_plan.as_mut().unwrap().source = Some(DataSource {
            catalog: None,
            database: "db".into(),
            table: "tbl".into(),
            partition: None,
            version: None,
        });
        let err = assignment_from_proto(ev).unwrap_err();
        assert!(err.to_string().contains("catalog"), "{err}");
    }

    #[test]
    fn assignment_from_proto_no_resolved_partition() {
        let mut ev = minimal_assignment_event();
        ev.load_plan.as_mut().unwrap().resolved_partition = None;
        let la = assignment_from_proto(ev).expect("conversion should succeed");
        assert!(la.time_range.is_none());
        assert!(la.time_column_name.is_empty());
        assert!(la.time_partition_value.is_empty());
        assert!(la.dimension_values.is_empty());
    }

    // -----------------------------------------------------------------------
    // command_from_ack
    // -----------------------------------------------------------------------

    #[test]
    fn command_from_ack_evict_epoch() {
        let ack = make_ack(
            "evict_epoch",
            &[
                ("dataset_id", "sales"),
                ("epoch_id", "ep-1"),
                ("reason", "ttl_expired"),
            ],
        );
        match command_from_ack(ack) {
            ControlCommand::EvictEpoch {
                dataset_id,
                epoch_id,
                reason,
            } => {
                assert_eq!(dataset_id, "sales");
                assert_eq!(epoch_id, "ep-1");
                assert_eq!(reason, "ttl_expired");
            }
            other => panic!("expected EvictEpoch, got {other:?}"),
        }
    }

    #[test]
    fn command_from_ack_drain() {
        let ack = make_ack("drain_command", &[]);
        assert!(matches!(command_from_ack(ack), ControlCommand::Drain));
    }

    #[test]
    fn command_from_ack_unknown() {
        let ack = make_ack("some_future_command", &[("foo", "bar")]);
        match command_from_ack(ack) {
            ControlCommand::Unknown { event_type } => {
                assert_eq!(event_type, "some_future_command");
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // wrap_outgoing
    // -----------------------------------------------------------------------

    #[test]
    fn wrap_outgoing_heartbeat() {
        let msg = wrap_outgoing("w-1", "t-1", OutgoingPayload::Heartbeat);
        assert_eq!(msg.worker_id, "w-1");
        assert_eq!(msg.tenant_id, "t-1");
        assert!(!msg.event_id.is_empty());
        assert!(matches!(
            msg.payload,
            Some(Payload::HeartbeatEvent(HeartbeatEvent {}))
        ));
    }

    #[test]
    fn wrap_outgoing_status_completed() {
        let msg = wrap_outgoing(
            "w-1",
            "t-1",
            OutgoingPayload::StatusUpdate {
                dataset_id: "ds".into(),
                epoch_id: "ep".into(),
                status: LoadStatus::Completed,
                error: None,
            },
        );
        match msg.payload {
            Some(Payload::DataPullStatusUpdate(ev)) => {
                assert_eq!(ev.dataset_id, "ds");
                assert_eq!(ev.epoch_id, "ep");
                assert_eq!(ev.status, ProtoStatus::Completed as i32);
                assert!(ev.error_message.is_empty());
            }
            other => panic!("expected DataPullStatusUpdate, got {other:?}"),
        }
    }

    #[test]
    fn wrap_outgoing_status_failed_with_error() {
        let msg = wrap_outgoing(
            "w-1",
            "t-1",
            OutgoingPayload::StatusUpdate {
                dataset_id: "ds".into(),
                epoch_id: "ep".into(),
                status: LoadStatus::Failed,
                error: Some("disk full".into()),
            },
        );
        match msg.payload {
            Some(Payload::DataPullStatusUpdate(ev)) => {
                assert_eq!(ev.status, ProtoStatus::Failed as i32);
                assert_eq!(ev.error_message, "disk full");
            }
            other => panic!("expected DataPullStatusUpdate, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // register_event
    // -----------------------------------------------------------------------

    #[test]
    fn register_event_populates_envelope() {
        let msg = register_event("w-2", "t-acme", "http://cp:50051");
        assert_eq!(msg.worker_id, "w-2");
        assert_eq!(msg.tenant_id, "t-acme");
        assert!(!msg.event_id.is_empty());

        match msg.payload {
            Some(Payload::RegisterEvent(re)) => {
                let worker = re.worker.expect("worker field must be present");
                assert_eq!(worker.node_id, "w-2");
                assert_eq!(worker.addr, "http://cp:50051");
                assert_eq!(worker.labels.get("tenant_id").unwrap(), "t-acme");
            }
            other => panic!("expected RegisterEvent, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // StarRocks catalog conversion
    // -----------------------------------------------------------------------

    #[test]
    fn starrocks_catalog_round_trip() {
        use worker_proto::proto::common::{
            catalog::CatalogType, Catalog, ConnectionPoolOptions as ProtoPoolOpts, StarRocksCatalog,
        };
        let proto = Catalog {
            catalog_type: Some(CatalogType::Starrocks(StarRocksCatalog {
                catalog_name: "sr_cat".into(),
                host: "sr-host".into(),
                port: 9030,
                database: "sr_db".into(),
                username: "root".into(),
                password: "secret".into(),
                max_concurrency: 8,
                pool_options: Some(ProtoPoolOpts {
                    max_connections: Some(32),
                    min_connections: Some(2),
                    connect_timeout_ms: Some(3000),
                    idle_timeout_ms: Some(120_000),
                    acquire_timeout_ms: None,
                    max_lifetime_ms: None,
                    batch_size: None,
                }),
            })),
        };
        let info = catalog_from_proto(proto).expect("conversion must succeed");
        match info {
            CatalogInfo::StarRocks {
                catalog_name,
                host,
                port,
                database,
                username,
                password,
                max_concurrency,
                pool_options,
            } => {
                assert_eq!(catalog_name, "sr_cat");
                assert_eq!(host, "sr-host");
                assert_eq!(port, 9030);
                assert_eq!(database, "sr_db");
                assert_eq!(username, "root");
                assert_eq!(password, "secret");
                assert_eq!(max_concurrency, Some(8));
                let po = pool_options.unwrap();
                assert_eq!(po.max_connections, 32);
                assert_eq!(po.min_connections, 2);
                assert_eq!(po.connect_timeout_ms, 3000);
                assert_eq!(po.idle_timeout_ms, 120_000);
            }
            other => panic!("expected StarRocks, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // JDBC catalog conversion
    // -----------------------------------------------------------------------

    #[test]
    fn jdbc_catalog_round_trip() {
        use worker_proto::proto::common::{catalog::CatalogType, Catalog, JdbcCatalog};
        let proto = Catalog {
            catalog_type: Some(CatalogType::Jdbc(JdbcCatalog {
                uri: "jdbc:mysql://host:3306/db".into(),
                driver: "com.mysql.cj.jdbc.Driver".into(),
                pool_options: None,
            })),
        };
        let info = catalog_from_proto(proto).expect("conversion must succeed");
        match info {
            CatalogInfo::Jdbc {
                uri,
                driver,
                pool_options,
            } => {
                assert_eq!(uri, "jdbc:mysql://host:3306/db");
                assert_eq!(driver, "com.mysql.cj.jdbc.Driver");
                assert!(pool_options.is_none());
            }
            other => panic!("expected Jdbc, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // time_range_from_proto
    // -----------------------------------------------------------------------

    #[test]
    fn time_range_converts_correctly() {
        use worker_proto::proto::common::TimeRange;

        let proto = TimeRange {
            start_inclusive: Some(protobuf::Timestamp {
                seconds: 100,
                nanos: 250,
            }),
            end_exclusive: Some(protobuf::Timestamp {
                seconds: 200,
                nanos: 750,
            }),
        };
        let tr = time_range_from_proto(proto).unwrap();
        assert_eq!(tr.start_inclusive_ns, 100 * 1_000_000_000 + 250);
        assert_eq!(tr.end_exclusive_ns, 200 * 1_000_000_000 + 750);
        assert_eq!(tr.start_secs(), 100);
        assert_eq!(tr.end_secs(), 200);
    }

    #[test]
    fn time_range_missing_start_fails() {
        use worker_proto::proto::common::TimeRange;
        let proto = TimeRange {
            start_inclusive: None,
            end_exclusive: Some(protobuf::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
        };
        let err = time_range_from_proto(proto).unwrap_err();
        assert!(err.to_string().contains("start_inclusive"), "{err}");
    }

    // -----------------------------------------------------------------------
    // load_status_to_proto
    // -----------------------------------------------------------------------

    #[test]
    fn load_status_mapping() {
        assert_eq!(
            load_status_to_proto(LoadStatus::InProgress) as i32,
            ProtoStatus::InProgress as i32
        );
        assert_eq!(
            load_status_to_proto(LoadStatus::Completed) as i32,
            ProtoStatus::Completed as i32
        );
        assert_eq!(
            load_status_to_proto(LoadStatus::Failed) as i32,
            ProtoStatus::Failed as i32
        );
    }

    // -----------------------------------------------------------------------
    // extract_string_fields
    // -----------------------------------------------------------------------

    #[test]
    fn extract_string_fields_ignores_non_string() {
        let mut fields = HashMap::new();
        fields.insert(
            "good".into(),
            protobuf::Value {
                kind: Some(protobuf::value::Kind::StringValue("ok".into())),
            },
        );
        fields.insert(
            "number".into(),
            protobuf::Value {
                kind: Some(protobuf::value::Kind::NumberValue(42.0)),
            },
        );
        fields.insert(
            "null".into(),
            protobuf::Value {
                kind: Some(protobuf::value::Kind::NullValue(0)),
            },
        );
        let result = extract_string_fields(&fields);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("good").unwrap(), "ok");
    }
}
