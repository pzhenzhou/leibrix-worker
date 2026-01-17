//! Testing infrastructure for LDP (Leibrix Distributed Plan).
//!
//! This module provides utilities for end-to-end testing of distributed
//! query execution, including test cluster management, data loading,
//! and result verification.

pub mod cluster;
pub mod data_loader;
pub mod macro_helpers;
pub mod verifier;