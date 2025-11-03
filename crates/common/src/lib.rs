//! Shared common utilities for Macaron Datalog programs.

pub mod args;
pub mod formatter;

// Re-export main types for backwards compatibility
pub use args::{get_example_files, Args};
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
