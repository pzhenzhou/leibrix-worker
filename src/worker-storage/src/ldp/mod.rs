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
//! - `substrait`: Utilities for working with Substrait plans
//! - `planner`: LDP generation (annotate_and_enforce, cut_into_stages)
//! - `executor`: LDP execution (LdpExecutor, ExchangeRuntime)

pub mod types;

// TODO: Uncomment as modules are implemented
// pub mod substrait;
// pub mod planner;
// pub mod executor;

pub use types::*;
