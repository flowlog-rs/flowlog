//! Diagnostic trait, `BoxError` alias, and the renderer.
//!
//! Each stage implements [`Diagnostic`] for its error type; a blanket `From`
//! boxes it into [`BoxError`] for `?`, and [`emit`] renders it via
//! `codespan-reporting`.

use std::error::Error as StdError;
use std::io;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use codespan_reporting::diagnostic::Label;

use crate::source::FileId;
use crate::source::SourceMap;
use crate::source::Span;

/// Canonical bug-report URL for [`InternalError`] — shared by every stage
/// so all "please file a bug" notes point at the same tracker.
pub const BUG_URL: &str = "https://github.com/flowlog-rs/flowlog/issues/new";

/// Build a primary label for `span`, or `None` if the span is dummy.
pub fn primary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::primary(span.file, span.range()))
}

/// Build a secondary label for `span`, or `None` if the span is dummy.
pub fn secondary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::secondary(span.file, span.range()))
}

/// Primary label for `span` carrying `msg`, as a `Vec` for
/// `CsDiagnostic::with_labels`. Empty for dummy spans.
pub fn labels(span: Span, msg: impl Into<String>) -> Vec<Label<FileId>> {
    primary_label(span)
        .map(|l| l.with_message(msg.into()))
        .into_iter()
        .collect()
}

/// Error types that can be rendered as a source-annotated diagnostic.
///
/// `to_diagnostic` builds the `codespan-reporting` diagnostic; `is_internal`
/// marks compiler bugs (vs. user errors) to drive exit code and the bug note.
pub trait Diagnostic: StdError + Send + Sync + 'static {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId>;

    fn is_internal(&self) -> bool {
        false
    }
}

/// Heap-allocated [`Diagnostic`] — the error type returned by pipeline entry
/// points.
pub type BoxError = Box<dyn Diagnostic>;

impl<E: Diagnostic> From<E> for BoxError {
    fn from(e: E) -> Self {
        Box::new(e)
    }
}

/// An invariant violation inside the compiler (as opposed to a user error).
/// Renders as an `internal compiler error` with a "file a bug" note.
#[derive(Debug)]
pub struct InternalError {
    pub(crate) stage: &'static str,
    pub(crate) detail: String,
    pub(crate) bug_url: &'static str,
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

/// Render `err` to `writer` as a plain (uncolored) source-annotated
/// diagnostic. CLI binaries usually want [`emit_and_exit`] instead.
pub fn emit(err: &BoxError, sources: &SourceMap, writer: &mut dyn io::Write) -> io::Result<()> {
    let diag = err.to_diagnostic();
    let config = codespan_reporting::term::Config::default();
    codespan_reporting::term::emit_to_io_write(writer, &config, sources, &diag)
        .map_err(io::Error::other)
}

/// Render `err` to stderr (colored when stderr is a TTY) and exit: code `2`
/// for internal errors, `1` otherwise. For CLI binaries; libraries should use
/// [`emit`] and propagate instead.
pub fn emit_and_exit(err: impl Into<BoxError>, sources: &SourceMap) -> ! {
    use codespan_reporting::term::termcolor::ColorChoice;
    use codespan_reporting::term::termcolor::StandardStream;
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
    use codespan_reporting::diagnostic::Label;

    use super::*;

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
