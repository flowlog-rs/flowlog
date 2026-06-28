//! Library-mode build-flow errors.
//!
//! [`BuildError`] covers infrastructure and input-validation failures
//! raised while driving a `build.rs` compilation. Pipeline-stage errors
//! (parse / stratify / plan / codegen) flow through this crate as their
//! own [`Diagnostic`] types inside a [`flowlog_common::BoxError`]; they
//! don't round-trip through `BuildError`.

use std::io;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::{Diagnostic, FileId};
use thiserror::Error;

/// Infrastructure failure raised by the library-mode build flow.
///
/// Surfaces filesystem I/O against `$OUT_DIR`, `OUT_DIR` lookup failures,
/// and input-validation rejections (malformed program paths) the builder
/// catches before handing off to the pipeline. Distinct from:
///
/// - [`crate::CodegenError`] — user-source errors caught during codegen.
/// - [`flowlog_common::InternalError`] — compiler invariant violations
///   that render as "please file a bug" ICEs.
///
/// [`Diagnostic::is_internal`] returns `false` for every `BuildError`
/// variant, so a top-level renderer (e.g. [`flowlog_common::emit_and_exit`])
/// exits with code `1` and omits the bug-report note — the user fixes
/// their build setup, not their `.dl` program.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    /// An [`io::Error`] surfaced by the build pipeline. Covers genuine
    /// I/O (writes to `$OUT_DIR`, `OUT_DIR` lookup) as well as
    /// input-validation rejections wrapped as
    /// [`io::ErrorKind::InvalidInput`] — e.g. a program path with no
    /// usable file stem or non-UTF-8 bytes.
    #[error("{0}")]
    Io(#[from] io::Error),
}

impl Diagnostic for BuildError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        CsDiagnostic::error().with_message(self.to_string())
    }
}
