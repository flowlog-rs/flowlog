//! Diagnostic trait, `BoxError` alias, and the renderer.
//!
//! Each pipeline stage defines its own error enum and implements
//! [`Diagnostic`] for it. A blanket `From` impl boxes any `Diagnostic`
//! into a [`BoxError`], so `?` threads stage-specific errors through a
//! function that returns `Result<_, BoxError>`. The [`emit`] helper
//! renders a `BoxError` to a writer via `codespan-reporting`.

use std::error::Error as StdError;
use std::io;

use codespan_reporting::diagnostic::{Diagnostic as CsDiagnostic, Label};

use crate::source::{FileId, SourceMap, Span};

/// Canonical bug-report URL for [`InternalError`] — shared by every stage
/// so all "please file a bug" notes point at the same tracker.
pub const BUG_URL: &str = "https://github.com/flowlog-rs/flowlog/issues/new";

/// Build a primary label for `span`, or `None` if the span is dummy.
///
/// Dummy spans come from synthesized AST nodes; attaching a label to one
/// would point the renderer at a bogus file offset. Returning `None` lets
/// callers `flatten()` them away.
pub fn primary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::primary(span.file, span.range()))
}

/// Build a secondary label for `span`, or `None` if the span is dummy.
pub fn secondary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::secondary(span.file, span.range()))
}

/// Wrap [`primary_label`] with a message into a `Vec<Label<FileId>>`
/// ready for `CsDiagnostic::with_labels`. Yields an empty `Vec` for
/// dummy spans so variants with optional spans fall through cleanly.
pub fn labels(span: Span, msg: impl Into<String>) -> Vec<Label<FileId>> {
    primary_label(span)
        .map(|l| l.with_message(msg.into()))
        .into_iter()
        .collect()
}

/// Error types that can be rendered as a source-annotated diagnostic.
///
/// [`to_diagnostic`] maps variant data into a `codespan-reporting`
/// [`CsDiagnostic`] carrying a message and zero or more `Label`s that
/// point at `Span`s in the [`SourceMap`]. [`is_internal`] distinguishes
/// compiler bugs from user errors; the CLI uses it to pick an exit code
/// and the renderer uses it to append a "file a bug" note.
///
/// [`to_diagnostic`]: Diagnostic::to_diagnostic
/// [`is_internal`]: Diagnostic::is_internal
pub trait Diagnostic: StdError + Send + Sync + 'static {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId>;

    fn is_internal(&self) -> bool {
        false
    }
}

/// A heap-allocated [`Diagnostic`] — the top-level error type returned by
/// pipeline entry points. Constructed automatically from any `Diagnostic`
/// via the blanket `From` impl below.
pub type BoxError = Box<dyn Diagnostic>;

impl<E: Diagnostic> From<E> for BoxError {
    fn from(e: E) -> Self {
        Box::new(e)
    }
}

/// An invariant violation inside the compiler (as opposed to a user error).
///
/// Implements [`Diagnostic`] with `is_internal() == true` and renders as
/// `internal compiler error at stage X: <detail>` plus a "file a bug at
/// <url>" note.
#[derive(Debug)]
pub struct InternalError {
    pub stage: &'static str,
    pub detail: String,
    pub bug_url: &'static str,
}

impl InternalError {
    pub fn new(stage: &'static str, detail: impl Into<String>, bug_url: &'static str) -> Self {
        Self {
            stage,
            detail: detail.into(),
            bug_url,
        }
    }
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "internal compiler error at stage `{}`: {}",
            self.stage, self.detail
        )
    }
}

impl StdError for InternalError {}

impl Diagnostic for InternalError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        CsDiagnostic::bug()
            .with_message(format!(
                "internal compiler error at stage `{}`: {}",
                self.stage, self.detail
            ))
            .with_notes(vec![format!("please file a bug at {}", self.bug_url)])
    }

    fn is_internal(&self) -> bool {
        true
    }
}

/// Render `err` to `writer` as a source-annotated diagnostic.
///
/// Writes plain (uncolored) output; pass `&mut std::io::stderr()` for CLI
/// use or a `&mut Vec<u8>` to capture the rendered string. For CLI
/// binaries prefer [`emit_and_exit`], which additionally handles TTY
/// color detection and exit-code mapping.
pub fn emit(err: &BoxError, sources: &SourceMap, writer: &mut dyn io::Write) -> io::Result<()> {
    let diag = err.to_diagnostic();
    let config = codespan_reporting::term::Config::default();
    codespan_reporting::term::emit_to_io_write(writer, &config, sources, &diag)
        .map_err(io::Error::other)
}

/// Render `err` to stderr as a colored, source-annotated diagnostic and
/// exit the process.
///
/// Exit code is `2` for internal compiler errors ([`Diagnostic::is_internal`])
/// and `1` otherwise. Colors auto-disable when stderr is not a TTY.
/// Intended for CLI binaries whose response to any pipeline failure is
/// "print and give up"; library callers should use [`emit`] directly and
/// propagate the error.
pub fn emit_and_exit(err: impl Into<BoxError>, sources: &SourceMap) -> ! {
    use codespan_reporting::term::termcolor::{ColorChoice, StandardStream};
    let boxed: BoxError = err.into();
    let code = if boxed.is_internal() { 2 } else { 1 };
    let writer = StandardStream::stderr(ColorChoice::Auto);
    let diag = boxed.to_diagnostic();
    let config = codespan_reporting::term::Config::default();
    let _ =
        codespan_reporting::term::emit_to_write_style(&mut writer.lock(), &config, sources, &diag);
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;
    use codespan_reporting::diagnostic::Label;

    #[derive(Debug)]
    struct DemoError {
        span: crate::source::Span,
        msg: &'static str,
    }

    impl std::fmt::Display for DemoError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.msg)
        }
    }

    impl StdError for DemoError {}

    impl Diagnostic for DemoError {
        fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
            CsDiagnostic::error()
                .with_message(self.msg)
                .with_labels(vec![Label::primary(self.span.file, self.span.range())])
        }
    }

    #[test]
    fn question_mark_boxes_stage_error() {
        fn inner() -> Result<(), DemoError> {
            Err(DemoError {
                span: crate::source::Span::new(FileId(0), 0, 1),
                msg: "inner",
            })
        }
        fn outer() -> Result<(), BoxError> {
            inner()?;
            Ok(())
        }
        let err = outer().unwrap_err();
        assert_eq!(err.to_string(), "inner");
        assert!(!err.is_internal());
    }

    #[test]
    fn emit_renders_user_error_with_source_label() {
        let mut sm = SourceMap::new();
        let f = sm.add("demo.dl".into(), "abc def ghi".into());
        let err: BoxError = DemoError {
            span: crate::source::Span::new(f, 4, 7),
            msg: "bad token",
        }
        .into();

        let mut buf: Vec<u8> = Vec::new();
        emit(&err, &sm, &mut buf).unwrap();
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("bad token"), "got: {out}");
        assert!(out.contains("demo.dl"), "got: {out}");
        assert!(out.contains("def"), "got: {out}");
    }

    #[test]
    fn emit_renders_internal_error_as_bug() {
        let sm = SourceMap::new();
        let err: BoxError =
            InternalError::new("codegen", "missing fingerprint", "https://example/bugs").into();
        assert!(err.is_internal());

        let mut buf: Vec<u8> = Vec::new();
        emit(&err, &sm, &mut buf).unwrap();
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("bug"), "got: {out}");
        assert!(out.contains("codegen"), "got: {out}");
        assert!(out.contains("https://example/bugs"), "got: {out}");
    }
}
