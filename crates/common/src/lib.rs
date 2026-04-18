//! Shared common utilities for FlowLog Datalog programs.

pub mod config;
pub mod diag;
pub mod formatter;
pub mod macros;
pub mod source;

// Re-export main types for backwards compatibility
pub use config::{get_example_files, Config, ExecutionMode};
pub use diag::{emit, BoxError, Diagnostic, InternalError};
pub use formatter::TestResult;
pub use macros::INTERN_MAX_RETRIES;
pub use source::{FileId, SourceMap, Span};

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

/// Parse a `TokenStream` into a `syn::File` and pretty-print it via `prettyplease`.
pub fn pretty_print(ts: proc_macro2::TokenStream) -> String {
    let ast: syn::File = syn::parse2(ts).expect("valid token stream");
    prettyplease::unparse(&ast)
}
