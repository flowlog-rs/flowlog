//! Shared common utilities for FlowLog Datalog programs.

mod config;
mod diag;
mod formatter;
mod macros;
mod source;

// External API — consumed by flowlog-compiler and integration tests.
pub use config::{get_example_files, Config, ExecutionMode};
pub use diag::{emit, emit_and_exit, BoxError, Diagnostic, InternalError, BUG_URL};
pub use formatter::SECTION_BAR;
pub use macros::INTERN_MAX_RETRIES;
pub use source::{FileId, SourceMap, Span};

// Intra-crate shortcuts.
pub(crate) use diag::{labels, primary_label, secondary_label};
pub(crate) use formatter::SUBSECTION_BAR;
pub(crate) use source::Ignored;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes a derived fingerprint by hashing all identifying inputs together.
///
/// NOTE: Uses `DefaultHasher` which is deterministic within a build but not
/// guaranteed stable across Rust versions.
pub(crate) fn compute_fp<T: Hash>(t: T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// Parse a `TokenStream` into a `syn::File` and pretty-print it via `prettyplease`.
pub fn pretty_print(ts: proc_macro2::TokenStream) -> String {
    let ast: syn::File = syn::parse2(ts).expect("valid token stream");
    prettyplease::unparse(&ast)
}
