//! Substrait utilities for LDP planning.
//!
//! This module provides utilities for working directly with Substrait proto types.
//! NO custom IR - we work directly with `substrait::proto::Rel`.
//!
//! # Key Functions
//! - `get_substrait_children()` - Extract child Rels from any Rel
//! - `extract_grouping_field_refs()` - Get field refs from AggregateRel
//! - `extract_join_key_refs()` - Get join key field refs from JoinRel
//! - `reconstruct_rel_with_children()` - Rebuild Rel with new children
//! - `create_read_rel()` - Create a ReadRel for exchange input
//! - `serialize_rel_to_substrait()` - Serialize Rel to bytes

pub mod children;
pub mod helpers;
pub mod reconstruct;

pub use children::*;
pub use helpers::*;
pub use reconstruct::*;
