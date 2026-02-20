//! Shared test infrastructure for worker-flight e2e tests.
//!
//! This module provides reusable helpers so individual test files remain thin
//! scenario declarations with no duplicated setup, execution, or assertion logic.
//!
//! # Submodules
//!
//! - [`harness`] — `FlightTestHarness`: server lifecycle (start / shutdown / client access)
//! - [`data`]    — `DataSeeder`: deterministic epoch-table + macro + dataset seeding
//! - [`runner`]  — `execute_via_flight` / `execute_reference`: query execution helpers
//! - [`assertions`] — Thin wrappers over `TestVerifier` for flight-vs-reference comparison

pub mod assertions;
pub mod data;
pub mod harness;
pub mod runner;

use std::sync::Once;

static INIT_TRACING: Once = Once::new();

/// Initialise `tracing_subscriber` exactly once across all test threads.
///
/// Uses the `with_test_writer()` adapter so output is captured by `cargo test`
/// and only shown for failing tests (unless `--nocapture` is passed).
pub fn init_tracing() {
    INIT_TRACING.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .init();
    });
}
