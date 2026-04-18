//! Parse errors and grammar-contract internal errors.
//!
//! `ParseError` covers failures reachable from a user-authored `.dl` program:
//! syntax errors, duplicate declarations, references to undeclared relations,
//! broken include directives, and so on. Each variant carries a [`Span`] so
//! the renderer can point at the offending source.
//!
//! [`grammar_bug`] produces an [`InternalError`] for Pest grammar contracts
//! that should hold by construction (e.g. an `atom` rule always has an inner
//! `relation_name`). Those aren't user errors, but they still need to surface
//! as a structured diagnostic rather than a SIGABRT.

use std::path::PathBuf;

use codespan_reporting::diagnostic::{Diagnostic as CsDiagnostic, Label};
use common::diag::{Diagnostic, InternalError};
use common::source::{FileId, Span};
use thiserror::Error;

#[allow(dead_code)]
const BUG_URL: &str = "https://github.com/flowlog-rs/flowlog/issues/new";

/// Which `.decl`-style directive is being reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveKind {
    Input,
    Output,
    PrintSize,
}

impl std::fmt::Display for DirectiveKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DirectiveKind::Input => ".input",
            DirectiveKind::Output => ".output",
            DirectiveKind::PrintSize => ".printsize",
        })
    }
}

fn primary(span: Span) -> Label<FileId> {
    Label::primary(span.file, span.range())
}

fn secondary(span: Span) -> Label<FileId> {
    Label::secondary(span.file, span.range())
}

/// Errors raised while parsing a FlowLog program.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ParseError {
    /// Pest rejected the input with a grammar error.
    #[error("syntax error: {message}")]
    Syntax { span: Span, message: String },

    /// Two `.decl` declarations share a name (or case-colliding raw names).
    #[error("duplicate declaration of relation `{name}`")]
    DuplicateDecl {
        span: Span,
        prior: Span,
        name: String,
    },

    /// Two directives of the same kind target the same relation.
    #[error("duplicate {kind} directive for relation `{name}`")]
    DuplicateDirective {
        span: Span,
        prior: Span,
        kind: DirectiveKind,
        name: String,
    },

    /// A directive names a relation that was never `.decl`-d.
    #[error("{kind} directive references undeclared relation `{name}`")]
    UndeclaredInDirective {
        span: Span,
        kind: DirectiveKind,
        name: String,
    },

    /// A loop's `iterative [...]` list names a relation that was never `.decl`-d.
    #[error("iterative list references undeclared relation `{name}`")]
    UndeclaredInIterativeList { span: Span, name: String },

    /// A loop's `until`/`while` condition names a relation that was never `.decl`-d.
    #[error("loop condition references undeclared relation `{name}`")]
    UndeclaredLoopCondition { span: Span, name: String },

    /// A `loop` / `fixpoint` block appeared outside `extend-*` mode.
    #[error("`loop`/`fixpoint` blocks require `--mode extend-batch` or `extend-inc`")]
    LoopBlockInStandardMode { span: Span },

    /// A loop's until-condition names a relation with nonzero arity.
    #[error(
        "loop condition relation `{name}` must be nullary, but is declared with arity {arity}"
    )]
    NonNullaryLoopCondition {
        span: Span,
        name: String,
        arity: usize,
    },

    /// A UDF call uses `_` as an argument; wildcards aren't allowed in UDF args.
    #[error("`_` placeholder is not allowed in arguments to UDF `{udf_name}`")]
    PlaceholderInUdf { span: Span, udf_name: String },

    /// An `.include` directive's target could not be opened.
    #[error("failed to read included file `{}`: {source}", path.display())]
    IncludeIo {
        span: Span,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// An `.include` chain cycles back to a file already being loaded.
    #[error("circular include of `{}`", path.display())]
    CircularInclude {
        span: Span,
        path: PathBuf,
        /// Files currently being loaded, outer-most first.
        chain: Vec<PathBuf>,
    },

    /// A grammar contract the Pest grammar should have made unreachable. Not a
    /// user error; reported as an internal compiler bug.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl Diagnostic for ParseError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        if let ParseError::Internal(e) = self {
            return e.to_diagnostic();
        }
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            ParseError::DuplicateDecl { span, prior, .. } => base.with_labels(vec![
                primary(*span).with_message("redeclared here"),
                secondary(*prior).with_message("first declared here"),
            ]),

            ParseError::DuplicateDirective { span, prior, .. } => base.with_labels(vec![
                primary(*span).with_message("duplicate directive"),
                secondary(*prior).with_message("first directive here"),
            ]),

            ParseError::UndeclaredInDirective { span, name, .. } => base
                .with_labels(vec![primary(*span)])
                .with_notes(vec![format!(
                    "add a `.decl {name}(...)` before this directive"
                )]),

            ParseError::UndeclaredInIterativeList { span, name } => base
                .with_labels(vec![primary(*span)])
                .with_notes(vec![format!(
                    "either `.decl {name}(...)` it, or drop `{name}` from the iterative list"
                )]),

            ParseError::UndeclaredLoopCondition { span, name } => base
                .with_labels(vec![primary(*span)])
                .with_notes(vec![format!(
                    "declare `{name}` as a nullary relation with `.decl {name}()` and derive it inside the loop"
                )]),

            ParseError::CircularInclude { span, chain, .. } => {
                let mut diag = base.with_labels(vec![primary(*span)]);
                if !chain.is_empty() {
                    let shown: Vec<String> = chain.iter().map(|p| p.display().to_string()).collect();
                    diag = diag.with_notes(vec![format!("include chain: {}", shown.join(" → "))]);
                }
                diag
            }

            ParseError::Syntax { span, .. }
            | ParseError::LoopBlockInStandardMode { span }
            | ParseError::NonNullaryLoopCondition { span, .. }
            | ParseError::PlaceholderInUdf { span, .. }
            | ParseError::IncludeIo { span, .. } => base.with_labels(vec![primary(*span)]),

            ParseError::Internal(_) => unreachable!("handled above"),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, ParseError::Internal(_))
    }
}

/// Produce a `ParseError::Internal` for a Pest grammar-contract violation.
///
/// Use this at sites where an `.expect` would otherwise fire on an inner
/// token that the grammar guarantees — e.g. `"atom_rule always contains
/// relation_name"`. If such a site ever trips, it's a FlowLog bug, not a
/// user error.
#[allow(dead_code)]
pub(crate) fn grammar_bug(detail: impl Into<String>) -> ParseError {
    ParseError::Internal(InternalError::new("parser", detail, BUG_URL))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::diag::{emit, BoxError};
    use common::source::SourceMap;

    fn make_sm_with(text: &str) -> (SourceMap, FileId) {
        let mut sm = SourceMap::new();
        let f = sm.add("t.dl".into(), text.into());
        (sm, f)
    }

    fn render(err: ParseError, sm: &SourceMap) -> String {
        let err: BoxError = err.into();
        let mut buf: Vec<u8> = Vec::new();
        emit(&err, sm, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn duplicate_decl_labels_both_sites() {
        let (sm, f) = make_sm_with(".decl Foo(x: int)\n.decl Foo(y: int)\n");
        let out = render(
            ParseError::DuplicateDecl {
                span: Span::new(f, 24, 27),
                prior: Span::new(f, 6, 9),
                name: "Foo".into(),
            },
            &sm,
        );
        assert!(out.contains("duplicate declaration"), "got: {out}");
        assert!(out.contains("redeclared here"), "got: {out}");
        assert!(out.contains("first declared here"), "got: {out}");
    }

    #[test]
    fn undeclared_in_directive_includes_help_note() {
        let (sm, f) = make_sm_with(".input Bar(filename=\"b.csv\")\n");
        let out = render(
            ParseError::UndeclaredInDirective {
                span: Span::new(f, 7, 10),
                kind: DirectiveKind::Input,
                name: "Bar".into(),
            },
            &sm,
        );
        assert!(out.contains(".input"), "got: {out}");
        assert!(out.contains("undeclared"), "got: {out}");
        assert!(out.contains("add a `.decl Bar"), "got: {out}");
    }

    #[test]
    fn internal_variant_renders_bug_note() {
        let (sm, _) = make_sm_with("");
        let out = render(grammar_bug("ghosts in the AST"), &sm);
        assert!(out.contains("bug"), "got: {out}");
        assert!(out.contains("ghosts in the AST"), "got: {out}");
        assert!(out.contains(BUG_URL), "got: {out}");
    }
}
