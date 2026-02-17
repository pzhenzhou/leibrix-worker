//! LDP Planner module.
//!
//! This module contains the unified planning algorithm for generating
//! Leibrix Distributed Plans from Substrait.
//!
//! # Key Components
//! - `policy`: Planner configuration and thresholds
//! - `metadata`: Access to epoch statistics and placement
//! - `storage_metadata`: Real metadata from StorageEngine integration
//! - `requirements`: Distribution requirements per operator type
//! - `exchange`: Exchange selection logic
//! - `annotate`: Main annotation and enforcement algorithm
//! - `cut`: Stage cutting at exchange boundaries
//! - `pipeline`: End-to-end SQL → LDP conversion

pub mod annotate;
pub mod cut;
pub mod exchange;
pub mod inspect;
pub mod metadata;
pub mod pipeline;
pub mod policy;
pub mod requirements;
pub mod storage_metadata;

pub use annotate::{annotate_logical_plan, AnnotatedPlan, AnnotationResult, PlanningError};
pub use cut::{cut_into_stages, extract_stage_sql};
pub use exchange::{determine_exchange, ExchangeDecision, RejectReason};
pub use inspect::PlanInspector;
pub use metadata::{InMemoryMetadata, Metadata, TableScanStats};
pub use pipeline::{plan_ldp, plan_ldp_from_logical_plan, PipelineError};
pub use policy::*;
pub use requirements::{
    all_satisfied, get_logical_plan_requirements, satisfies, unsatisfied_requirements,
};
pub use storage_metadata::{
    date_to_ms, ms_to_date, ClusterMetadata, EpochPlacement, StorageEngineMetadata,
};
