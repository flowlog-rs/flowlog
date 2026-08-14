//! Diagnostic trait, `BoxError` alias, and the renderer.
//!
//! Each stage implements [`Diagnostic`] for its error type; a blanket `From`
//! boxes it into [`BoxError`] for `?`, and [`emit`] renders it via
//! `codespan-reporting`.

use std::io;
use std::process;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use codespan_reporting::diagnostic::Label;
use codespan_reporting::term;

use crate::error::FlowlogError;
use crate::error::InternalError;
use crate::source::FileId;
use crate::source::SourceMap;
use crate::source::Span;

/// Build a primary label for `span`, or `None` if the span is dummy.
pub fn primary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::primary(span.file, span.range()))
}

/// Build a secondary label for `span`, or `None` if the span is dummy.
pub fn secondary_label(span: Span) -> Option<Label<FileId>> {
    (!span.is_dummy()).then(|| Label::secondary(span.file, span.range()))
}

/// Build a primary label for `span` carrying `msg`, as a `Vec` for
/// `CsDiagnostic::with_labels`. Empty for dummy spans.
pub fn labels(span: Span, msg: impl Into<String>) -> Vec<Label<FileId>> {
    primary_label(span)
        .map(|l| l.with_message(msg.into()))
        .into_iter()
        .collect()
}

/// Error types that can be rendered as a source-annotated diagnostic.
///
/// Extends [`FlowlogError`] for the errors that have a span to point at; an
/// error without one implements that trait alone.
pub trait Diagnostic: FlowlogError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId>;
}

/// Heap-allocated [`Diagnostic`]: the error type returned by pipeline
/// entry points.
pub type BoxError = Box<dyn Diagnostic>;

impl<E: Diagnostic> From<E> for BoxError {
    fn from(e: E) -> Self {
        Box::new(e)
    }
}

impl Diagnostic for InternalError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        CsDiagnostic::bug()
            .with_message(self.to_string())
            .with_notes(vec![format!("please file a bug at {}", self.bug_url())])
    }
}

/// Render `err` to `writer` as a plain (uncolored) source-annotated
/// diagnostic. CLI binaries usually want [`emit_and_exit`] instead.
pub fn emit(err: &BoxError, sources: &SourceMap, writer: &mut dyn io::Write) -> io::Result<()> {
    let diag = err.to_diagnostic();
    let config = term::Config::default();
    term::emit_to_io_write(writer, &config, sources, &diag).map_err(io::Error::other)
}

/// Render `err` to stderr (colored when stderr is a TTY) and exit: code `2`
/// for internal errors, `1` otherwise. For CLI binaries; libraries should use
/// [`emit`] and propagate instead.
pub fn emit_and_exit(err: impl Into<BoxError>, sources: &SourceMap) -> ! {
    use term::termcolor::ColorChoice;
    use term::termcolor::StandardStream;
    let boxed: BoxError = err.into();
    let code = if boxed.is_internal() { 2 } else { 1 };
    let writer = StandardStream::stderr(ColorChoice::Auto);
    let diag = boxed.to_diagnostic();
    let config = term::Config::default();
    let _ = term::emit_to_write_style(&mut writer.lock(), &config, sources, &diag);
    process::exit(code);
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::fmt;

    use codespan_reporting::diagnostic::Label;

    use super::*;

    #[derive(Debug)]
    struct DemoError {
        span: Span,
        msg: &'static str,
    }

    impl fmt::Display for DemoError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    impl FlowlogError for DemoError {}

    #[test]
    fn question_mark_boxes_stage_error() {
        fn inner() -> Result<(), DemoError> {
            Err(DemoError {
                span: Span::new(FileId(0), 0, 1),
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
            span: Span::new(f, 4, 7),
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
