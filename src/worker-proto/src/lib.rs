//! Protocol buffer definitions for Leibrix Worker.
//!
//! This crate contains the generated Rust code from all proto files,
//! providing type-safe access to protocol messages.

pub mod proto;

// Re-export proto modules at crate root for convenience
pub use proto::common;
pub use proto::control_plane;
pub use proto::ldp;
