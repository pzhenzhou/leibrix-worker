//! Leibrix Distributed Plan (LDP) module.
//!
//! This module implements distributed query execution for bounded, in-memory analytics.
//!
//! # Design Principles
//! - Work directly with Substrait as the logical plan representation
//! - No custom IR - traverse and annotate Substrait directly
//! - Single unified algorithm driven by distribution property enforcement
//!
//! # Key Components
//! - `types`: Core data structures (Distribution, Exchange, Stage, LdpPlan)
//! - `planner`: LDP generation (annotate_and_enforce, cut_into_stages)
//! - `substrait`: Utilities for working with Substrait plans
//! - `executor`: LDP execution (LdpExecutor, ExchangeRuntime)

pub mod executor;
pub mod planner;
pub mod proto_convert;
pub mod substrait;
pub mod testing;
pub mod types;

// Re-export key planner types
pub use planner::{
    annotate_and_enforce, cut_into_stages, extract_stage_substrait, plan_ldp,
    plan_ldp_from_rel, plan_ldp_from_substrait, AnnotatedRel, AnnotationResult,
    ExchangeDecision, InMemoryMetadata, Metadata, PipelineError, PlannerPolicy,
    PlanningError, RejectReason, TableScanStats,
};
pub use types::*;

// Re-export executor types
pub use executor::{
    concat_record_batches, execute_local, DistributedExchangeRuntime, ExecutionError, ExchangeError,
    ExchangeRuntime, FlightStageExecutor, LdpExecutor, LdpFlightClient, LocalStageExecutor,
    RemoteTicket, StageExecutionError, StageExecutionStats, StageExecutor, StageResult, StageTicket,
    StageTickets, WorkerConnection, WorkerConnectionPool,
};

// Re-export proto conversion types
pub use proto_convert::{
    deserialize_exchange_inputs, deserialize_ldp_plan, deserialize_submit_stage_request,
    deserialize_submit_stage_response, serialize_ldp_plan, serialize_submit_stage_request,
    serialize_submit_stage_response, ConversionError, ConversionResult,
};
