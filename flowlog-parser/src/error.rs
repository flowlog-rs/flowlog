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

use std::fmt;
use std::io;
use std::path::PathBuf;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use codespan_reporting::diagnostic::Label;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use flowlog_common::Span;
use flowlog_common::primary_label;
use flowlog_common::secondary_label;
use thiserror::Error;

use crate::Rule;

/// Which `.decl`-style directive is being reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveKind {
    Input,
    Output,
    PrintSize,
}

impl fmt::Display for DirectiveKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DirectiveKind::Input => ".input",
            DirectiveKind::Output => ".output",
            DirectiveKind::PrintSize => ".printsize",
        })
    }
}

/// Build the `[primary, secondary]` label pair for a "duplicate X, first
/// declared at Y" style diagnostic. Dummy spans (no source position) drop
/// out instead of pointing at a bogus file.
fn dup_labels(span: Span, prior: Span, here: &str, first: &str) -> Vec<Label<FileId>> {
    [
        primary_label(span).map(|l| l.with_message(here)),
        secondary_label(prior).map(|l| l.with_message(first)),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Single-element label vec for diagnostics that only point at one span.
/// Returns an empty vec for dummy spans rather than fabricating a location.
fn primary_only(span: Span) -> Vec<Label<FileId>> {
    primary_label(span).into_iter().collect()
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

    /// Two `.extern fn` declarations share a name.
    #[error("duplicate declaration of extern function `{name}`")]
    DuplicateExternFn {
        span: Span,
        prior: Span,
        name: String,
    },

    /// Two attributes in one `.decl` share a name (or case-colliding raw names).
    #[error("duplicate attribute `{name}` in relation `{relation}`")]
    DuplicateAttribute {
        span: Span,
        prior: Span,
        relation: String,
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

    /// A relation carries both `.output` and `.printsize`. Both write
    /// to the same `<RawName>.csv` path, so the second would silently
    /// clobber the first — rejected up-front. Use one or the other.
    #[error(
        "relation `{name}` has both `.output` and `.printsize`; \
         both write `{name}.csv` — pick one"
    )]
    OutputAndPrintsizeConflict { span: Span, name: String },

    /// A loop's `iterative [...]` list names a relation that was never `.decl`-d.
    #[error("iterative list references undeclared relation `{name}`")]
    UndeclaredInIterativeList { span: Span, name: String },

    /// A loop's `until`/`while` condition names a relation that was never `.decl`-d.
    #[error("loop condition references undeclared relation `{name}`")]
    UndeclaredLoopCondition { span: Span, name: String },

    /// A rule head or body atom names a relation that was never `.decl`-d.
    #[error("rule references undeclared relation `{name}`")]
    UndeclaredInRule { span: Span, name: String },

    /// A ground fact names a relation that was never `.decl`-d.
    #[error("fact references undeclared relation `{name}`")]
    UndeclaredInFact { span: Span, name: String },

    /// A `loop` / `fixpoint` block appeared outside `extend-*` mode.
    #[error("`loop`/`fixpoint` blocks require `--mode extend-batch` or `extend-inc`")]
    LoopBlockInStandardMode { span: Span },

    /// A loop's until-condition names a relation with nonzero arity.
    #[error("loop condition relation `{name}` must be nullary, but is declared with arity {arity}")]
    NonNullaryLoopCondition {
        span: Span,
        name: String,
        arity: usize,
    },

    /// A built-in call passes the wrong number of arguments. Carries the
    /// keyword string instead of the enum to keep this error layer
    /// independent of `crate::logic::BuiltinOperator`.
    #[error("built-in `{op}` expects {expected} argument(s) but got {found}")]
    BuiltinArity {
        span: Span,
        op: &'static str,
        expected: usize,
        found: usize,
    },

    /// An `.include` directive's target could not be opened.
    #[error("failed to read included file `{}`: {source}", path.display())]
    IncludeIo {
        span: Span,
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// An `.include` chain cycles back to a file already being loaded.
    #[error("circular include of `{}`", path.display())]
    CircularInclude {
        span: Span,
        path: PathBuf,
        /// Files currently being loaded, outer-most first.
        chain: Vec<PathBuf>,
    },

    /// Two `.type` declarations share a name.
    #[error("duplicate `.type` declaration of `{name}`")]
    DuplicateTypeDecl {
        span: Span,
        prior: Span,
        name: String,
    },

    /// `.type X = Y` (or `<:`) where `Y` is undeclared.
    #[error("`.type {name} = ...` references unknown type `{parent}`")]
    UnknownTypeParent {
        span: Span,
        name: String,
        parent: String,
    },

    /// `.decl R(x: T)` where `T` is undeclared.
    #[error("attribute references unknown type `{name}`")]
    UnknownAttributeType { span: Span, name: String },

    /// `.type T = ( f: U, … )` where field type `U` is undeclared.
    #[error("tuple type `{tuple}` field `{field}` references unknown type `{field_type}`")]
    TupleFieldUnknownType {
        span: Span,
        tuple: String,
        field: String,
        field_type: String,
    },

    /// `.type T = ( …, f: T, … )` — a tuple that references its own type.
    /// Recursive tuples (cons-lists / trees) are not supported.
    #[error("tuple type `{name}` is recursive; recursive tuples are not supported")]
    RecursiveTuple { span: Span, name: String },

    /// `.type X <: Y` where `Y` is a tuple type. Tuples are not subtypeable.
    #[error("`.type {name} <: {parent}` — tuples cannot be subtyped")]
    SubtypeOfTuple {
        span: Span,
        name: String,
        parent: String,
    },

    /// `.type T <: ( … )` — an inline tuple RHS declared with `<:`. A tuple
    /// definition is its own kind of `.type` and must use `=`.
    #[error("`.type {name} <: ( … )` — a tuple type must be defined with `=`, not `<:`")]
    TupleSubtypeDecl { span: Span, name: String },

    /// `.input R` where `R` has a tuple-typed column. Tuples are constructed
    /// by rules, never read from EDB facts.
    #[error("`.input {name}` is not allowed: relation `{name}` has a tuple-typed column")]
    TupleInInput { span: Span, name: String },

    /// `.init c = Foo<...>` where `Foo` was never declared as a `.comp`.
    #[error("unknown component `{name}`")]
    UnknownComponent { span: Span, name: String },

    /// `.comp A : B { ... }` where the inheritance chain cycles back to `A`.
    #[error("circular component inheritance involving `{name}`")]
    CircularInheritance { span: Span, name: String },

    /// `.init c = Foo<...>` passes a different number of type arguments
    /// than `Foo`'s `.comp` declaration accepts.
    #[error("component `{name}` expects {expected} type argument(s) but got {found}")]
    ComponentArityMismatch {
        span: Span,
        name: String,
        expected: usize,
        found: usize,
    },

    /// A dotted reference like `cfg.X` appears in a component body but
    /// `cfg` is neither a nested init nor a bound type-parameter.
    #[error("unresolved qualified reference `{path}`")]
    UnresolvedQualifiedRef { span: Span, path: String },

    /// `overridable` keyword on a top-level `.decl`. The keyword only
    /// makes sense inside a `.comp` body where a subcomponent might
    /// supply an `.override`.
    #[error("`overridable` is only allowed on a `.decl` inside a `.comp` body")]
    OverridableOutsideComp { span: Span, name: String },

    /// `.override Foo` in a subcomponent, but no `.decl Foo` was
    /// inherited from any parent component.
    #[error("override of undeclared relation `{name}`")]
    OverrideUnknownRelation { span: Span, name: String },

    /// `.override Foo` in a subcomponent, but the inherited `.decl Foo`
    /// is not marked `overridable`.
    #[error("override of non-overridable relation `{name}`")]
    OverrideOfNonOverridable {
        span: Span,
        prior: Span,
        name: String,
    },

    /// Subcomponent has `.override Foo` and also redeclares `.decl Foo`.
    /// Override only applies to *inherited* relations, so a local
    /// redeclaration would shadow the inherited decl and leave nothing
    /// for `.override` to target.
    #[error("override of non-inherited relation `{name}`")]
    OverrideRedeclaresRelation {
        span: Span,
        prior: Span,
        name: String,
    },

    /// `.plan (...)` appears without an immediately-preceding rule clause
    /// to attach to.
    #[error("`.plan` has no preceding rule to attach to")]
    PlanOrphan { span: Span },

    /// `.plan` index count does not match the rule's positive-atom count.
    #[error("`.plan` expects {expected} index(es) (one per positive body atom) but got {found}")]
    PlanArityMismatch {
        span: Span,
        expected: usize,
        found: usize,
    },

    /// `.plan` references an index outside `1..=positive_atom_count`.
    #[error("`.plan` index {index} is out of range (valid: 1..={max})")]
    PlanIndexOutOfRange {
        span: Span,
        index: usize,
        max: usize,
    },

    /// `.plan` lists the same index twice — must be a permutation.
    #[error("`.plan` lists positive-atom index {index} more than once")]
    PlanDuplicateIndex { span: Span, index: usize },

    /// An equality assignment `v = expr` grounds `v`, but `v` is then used as
    /// an argument of a negated atom with a non-trivial (arithmetic / function)
    /// right-hand side. FlowLog can substitute a variable or constant into a
    /// negation, but not an arbitrary expression (atom arguments are not
    /// expressions), so this form is rejected rather than silently mishandled.
    #[error(
        "assignment-bound variable `{var}` cannot be used in a negated atom with a computed value"
    )]
    AssignmentVarInNegation { span: Span, var: String },

    /// Assignment desugaring emptied a rule's body, but the head could not be
    /// reduced to constants (an unbound head variable, or a non-integer
    /// expression). The planner requires at least one positive atom, so the
    /// rule is rejected here rather than panicking downstream.
    #[error("rule body reduces to nothing but its head is not a constant fact")]
    GroundRuleNotConst { span: Span },

    /// A grammar contract the Pest grammar should have made unreachable. Not a
    /// user error; reported as an internal compiler bug.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl ParseError {
    /// Construct a [`ParseError::Syntax`] from a Pest error, anchoring the
    /// span to `file`.
    pub fn syntax_from_pest(err: &pest::error::Error<Rule>, file: FileId) -> Self {
        use pest::error::InputLocation;
        let (start, end) = match err.location {
            InputLocation::Pos(p) => (p as u32, p as u32),
            InputLocation::Span((s, e)) => (s as u32, e as u32),
        };
        ParseError::Syntax {
            span: Span::new(file, start, end),
            message: err.variant.message().into_owned(),
        }
    }
}

impl Diagnostic for ParseError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        if let ParseError::Internal(e) = self {
            return e.to_diagnostic();
        }
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            ParseError::DuplicateDecl { span, prior, .. }
            | ParseError::DuplicateExternFn { span, prior, .. } => {
                base.with_labels(dup_labels(*span, *prior, "redeclared here", "first declared here"))
            }

            ParseError::DuplicateDirective { span, prior, .. } => base.with_labels(dup_labels(
                *span,
                *prior,
                "duplicate directive",
                "first directive here",
            )),

            ParseError::OutputAndPrintsizeConflict { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "remove either the `.output` or the `.printsize` directive for this relation"
                        .to_string(),
                ]),

            ParseError::DuplicateAttribute { span, prior, .. } => base.with_labels(dup_labels(
                *span,
                *prior,
                "duplicate attribute here",
                "first declared here",
            )),

            ParseError::UndeclaredInDirective { span, name, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "add a `.decl {name}(...)` before this directive"
                )]),

            ParseError::UndeclaredInIterativeList { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "either `.decl {name}(...)` it, or drop `{name}` from the iterative list"
                )]),

            ParseError::UndeclaredLoopCondition { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "declare `{name}` as a nullary relation with `.decl {name}()` and derive it inside the loop"
                )]),

            ParseError::UndeclaredInRule { span, name }
            | ParseError::UndeclaredInFact { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "add a matching `.decl {name}(...)` declaration, or remove the reference"
                )]),

            ParseError::CircularInclude { span, chain, .. } => {
                let mut diag = base.with_labels(primary_only(*span));
                if !chain.is_empty() {
                    let shown: Vec<String> = chain.iter().map(|p| p.display().to_string()).collect();
                    diag = diag.with_notes(vec![format!("include chain: {}", shown.join(" → "))]);
                }
                diag
            }

            ParseError::DuplicateTypeDecl { span, prior, .. } => base.with_labels(dup_labels(
                *span,
                *prior,
                "redeclared here",
                "first declared here",
            )),

            ParseError::UnknownTypeParent { span, parent, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "declare `{parent}` with a `.type {parent} = ...` (or `<:`) earlier in the program"
                )]),

            ParseError::UnknownAttributeType { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "either use a built-in primitive or add `.type {name} = ...`"
                )]),

            ParseError::UnknownComponent { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "declare `{name}` with a `.comp {name} {{ ... }}` block"
                )]),

            ParseError::CircularInheritance { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "`.comp {name}` inherits transitively from itself; break the cycle"
                )]),

            ParseError::ComponentArityMismatch { span, .. } => {
                base.with_labels(primary_only(*span))
            }

            ParseError::UnresolvedQualifiedRef { span, path } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "the first segment of `{path}` must be either a nested `.init` instance in this component or a bound type-parameter"
                )]),

            ParseError::OverridableOutsideComp { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "remove `overridable` from this top-level `.decl {name}`, or move the declaration inside a `.comp` body"
                )]),

            ParseError::OverrideUnknownRelation { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "no inherited `.decl {name}(...) overridable` was found in any parent component"
                )]),

            ParseError::OverrideOfNonOverridable { span, prior, name } => base.with_labels(dup_labels(
                *span,
                *prior,
                "override target is not `overridable`",
                "declared without `overridable` here",
            )).with_notes(vec![format!(
                "add `overridable` to the parent `.decl {name}` to allow this override"
            )]),

            ParseError::OverrideRedeclaresRelation { span, prior, name } => base.with_labels(dup_labels(
                *span,
                *prior,
                "`.override` here",
                "relation redeclared in this comp here",
            )).with_notes(vec![format!(
                "`.override {name}` may only target an inherited relation; drop the local `.decl {name}` from this comp"
            )]),

            ParseError::PlanOrphan { span } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "place `.plan (...)` immediately after the rule it pins".into(),
                ]),

            ParseError::PlanArityMismatch { span, expected, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "supply exactly {expected} 1-based index(es), one per positive body atom"
                )]),

            ParseError::PlanIndexOutOfRange { span, max, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "use a 1-based index in 1..={max} (the rule has {max} positive atom(s))"
                )]),

            ParseError::PlanDuplicateIndex { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "`.plan` must be a permutation: each positive-atom index appears exactly once"
                        .into(),
                ]),

            ParseError::TupleFieldUnknownType {
                span, field_type, ..
            } => base.with_labels(primary_only(*span)).with_notes(vec![format!(
                "declare `{field_type}` earlier (a built-in primitive or a `.type`)"
            )]),

            ParseError::RecursiveTuple { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "`{name}` references its own type; recursive tuples (cons-lists / trees) are not supported"
                )]),

            ParseError::SubtypeOfTuple { span, parent, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "`{parent}` is a tuple type; use `=` to alias it instead of `<:` to subtype it"
                )]),

            ParseError::TupleSubtypeDecl { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "tuples cannot be subtyped; define the tuple with `=`".into(),
                ]),

            ParseError::TupleInInput { span, name } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "tuples are constructed by rules, not read from facts; remove `.input {name}` \
                     or change the column to a non-tuple type"
                )]),

            ParseError::Syntax { span, .. }
            | ParseError::LoopBlockInStandardMode { span }
            | ParseError::NonNullaryLoopCondition { span, .. }
            | ParseError::BuiltinArity { span, .. }
            | ParseError::AssignmentVarInNegation { span, .. }
            | ParseError::GroundRuleNotConst { span }
            | ParseError::IncludeIo { span, .. } => base.with_labels(primary_only(*span)),

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
pub fn grammar_bug(detail: impl Into<String>) -> ParseError {
    ParseError::Internal(InternalError::new("parser", detail, BUG_URL))
}

#[cfg(test)]
mod tests {
    use flowlog_common::BoxError;
    use flowlog_common::SourceMap;
    use flowlog_common::emit;

    use super::*;

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
    fn duplicate_extern_fn_labels_both_sites() {
        let (sm, f) =
            make_sm_with(".extern fn foo(x: int64) -> int64\n.extern fn foo(y: int64) -> int64\n");
        let out = render(
            ParseError::DuplicateExternFn {
                span: Span::new(f, 34, 67),
                prior: Span::new(f, 0, 33),
                name: "foo".into(),
            },
            &sm,
        );
        assert!(
            out.contains("duplicate declaration of extern function"),
            "got: {out}"
        );
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
