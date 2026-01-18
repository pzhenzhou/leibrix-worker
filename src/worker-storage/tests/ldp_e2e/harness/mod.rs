//! Test harness infrastructure for LDP end-to-end testing.
//!
//! This module provides the core testing infrastructure including:
//! - TestCluster: Multi-worker cluster management
//! - TestDataLoader: Data generation and distribution
//! - TestVerifier: Result verification utilities

pub mod cluster;
pub mod data_loader;
pub mod verifier;

// Re-export commonly used types
pub use cluster::{TestCluster, TestClusterConfig, TestWorker};
pub use data_loader::{DataDistribution, EpochSpec, TableLoadSpec, TestDataLoader};
pub use verifier::{TestVerifier, VerificationError};
