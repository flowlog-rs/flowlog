//! Shared common utilities for FlowLog Datalog programs.

pub mod config;
pub mod formatter;

// Re-export main types for backwards compatibility
pub use config::{get_example_files, Config, ExecutionMode};
pub use formatter::TestResult;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes a derived fingerprint by hashing all identifying inputs together.
///
/// NOTE: Uses `DefaultHasher` which is deterministic within a build but not
/// guaranteed stable across Rust versions.
pub fn compute_fp<T: Hash>(t: T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}
