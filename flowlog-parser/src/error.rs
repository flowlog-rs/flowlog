//! Parse errors and grammar-contract internal errors.
//!
//! `ParseError` covers failures reachable from a user-authored `.dl` program
//! across every pipeline stage: syntax errors, duplicate declarations,
//! references to undeclared relations, broken include directives, and the
//! semantic half: type, subtype, and cast errors raised by the checker.
//! Each variant carries a [`Span`] so the renderer can point at the
//! offending source.
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
use flowlog_common::labels;
use flowlog_common::primary_label;
use flowlog_common::secondary_label;
use thiserror::Error;

use crate::AggregationOperator;
use crate::ArithmeticOperator;
use crate::BuiltinOperator;
use crate::ComparisonOperator;
use crate::DataType;
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
    /// clobber the first; rejected up-front. Use one or the other.
    #[error(
        "relation `{name}` has both `.output` and `.printsize`; \
         both write `{name}.csv`; pick one"
    )]
    OutputAndPrintsizeConflict { span: Span, name: String },

    /// A rule head or body atom names a relation that was never `.decl`-d.
    #[error("rule references undeclared relation `{name}`")]
    UndeclaredInRule { span: Span, name: String },

    /// A ground fact names a relation that was never `.decl`-d.
    #[error("fact references undeclared relation `{name}`")]
    UndeclaredInFact { span: Span, name: String },

    /// A built-in call passes the wrong number of arguments. Carries the
    /// keyword as the user spelled it.
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

    /// `.type T = ( f: U, ... )` where field type `U` is undeclared.
    #[error("tuple type `{tuple}` field `{field}` references unknown type `{field_type}`")]
    TupleFieldUnknownType {
        span: Span,
        tuple: String,
        field: String,
        field_type: String,
    },

    /// `.type T = ( ..., f: T, ... )`: a tuple that references its own type.
    /// Recursive tuples (cons-lists / trees) are not supported.
    #[error("tuple type `{name}` is recursive; recursive tuples are not supported")]
    RecursiveTuple { span: Span, name: String },

    /// `.type X <: Y` where `Y` is a tuple type. Tuples are not subtypeable.
    #[error("`.type {name} <: {parent}`: tuples cannot be subtyped")]
    SubtypeOfTuple {
        span: Span,
        name: String,
        parent: String,
    },

    /// `.type T <: ( ... )`: an inline tuple RHS declared with `<:`. A tuple
    /// definition is its own kind of `.type` and must use `=`.
    #[error("`.type {name} <: ( ... )`: a tuple type must be defined with `=`, not `<:`")]
    TupleSubtypeDecl { span: Span, name: String },

    /// `.input R` where `R` has a tuple-typed column. Tuples are constructed
    /// by rules, never read from EDB facts.
    #[error("`.input {name}` is not allowed: relation `{name}` has a tuple-typed column")]
    TupleInInput { span: Span, name: String },

    /// `.input R(IO="...")` naming storage no reader implements. The set is
    /// closed, so a misspelling would otherwise leave the relation with no
    /// startup facts and no complaint.
    #[error("unknown `.input` IO `{io}`")]
    UnknownInputIo { span: Span, io: String },

    /// `.input`/`.output R(delimiter="...")` that names no single byte to
    /// split cells on, or names one the line reader has already consumed.
    #[error("delimiter must be one ASCII character, not \"{}\"", .value.escape_debug())]
    InvalidDelimiter { span: Span, value: String },

    /// `.output R(IO="...")` naming a sink FlowLog does not write.
    #[error("unknown `.output` IO `{io}`")]
    UnknownOutputIo { span: Span, io: String },

    /// `.output R(order_by="...")` that names no column of `relation`, or
    /// spells a clause the sink cannot read.
    #[error("invalid `order_by` for relation `{relation}`: {reason}")]
    InvalidOrderBy {
        span: Span,
        relation: String,
        reason: String,
    },

    /// `.output R(limit="...")` whose value is not a row count.
    #[error("invalid `limit` `{value}` for relation `{relation}`")]
    InvalidLimit {
        span: Span,
        relation: String,
        value: String,
    },

    /// `.output R(limit=...)` with no `order_by`, which leaves which rows
    /// survive up to the order they were derived in.
    #[error("`limit` on relation `{relation}` needs an `order_by`")]
    LimitWithoutOrderBy { span: Span, relation: String },

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

    /// `.plan` lists the same index twice; must be a permutation.
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

    /// Assignment substitution emptied a rule's body, but the head could not be
    /// reduced to constants (an unbound head variable, or a non-integer
    /// expression). The planner requires at least one positive atom, so the rule
    /// is rejected during constant folding rather than panicking downstream.
    #[error("rule body reduces to nothing but its head is not a constant fact")]
    GroundRuleNotConst { span: Span },

    /// A `_` placeholder alone in parentheses. `(_)` is neither grouping
    /// (a placeholder is not an expression) nor a tuple (no comma).
    #[error("`_` cannot be grouped: `(_)` is neither a tuple nor an expression")]
    GroupedPlaceholder { span: Span },

    /// A string token is not a valid Rust string literal. FlowLog strings
    /// follow Rust syntax (quoted with Rust's escape alphabet, or raw);
    /// unknown escapes are errors, unlike Souffle's pass-through.
    #[error("invalid string literal: {reason}")]
    InvalidStringLiteral { span: Span, reason: String },

    // --- Semantic (type-check) errors ---
    /// A variable is bound to one type and later reused with another.
    #[error("variable `{var}` bound as `{first_ty:?}` but used as `{later_ty:?}`")]
    TypeMismatch {
        var: String,
        first_ty: DataType,
        first_span: Span,
        later_ty: DataType,
        later_span: Span,
    },

    /// Two factors of a single arithmetic expression have different types.
    #[error("mixed types in arithmetic expression: `{left:?}` and `{right:?}`")]
    ArithmeticTypeMismatch {
        span: Span,
        left: DataType,
        right: DataType,
    },

    /// An arithmetic operator applied to an incompatible type.
    #[error("arithmetic operator `{op}` is not allowed on `{ty:?}`")]
    ArithmeticOpNotAllowed {
        span: Span,
        op: ArithmeticOperator,
        ty: DataType,
    },

    /// The two sides of a comparison have different types.
    #[error("comparison sides disagree: `{left:?}` {op} `{right:?}`")]
    ComparisonTypeMismatch {
        span: Span,
        op: ComparisonOperator,
        left: DataType,
        right: DataType,
    },

    /// An ordering comparison (`<`, `<=`, `>`, `>=`) applied to a type
    /// with no natural order (`Bool`).
    #[error("comparison operator `{op}` is not allowed on `{ty:?}`")]
    ComparisonOpNotAllowed {
        span: Span,
        op: ComparisonOperator,
        ty: DataType,
    },

    /// A constant in an atom or head position doesn't fit the declared
    /// column family (`5.0` into `Int32`, `"x"` into `Bool`, ...).
    #[error("literal `{literal}` does not fit column type `{expected:?}`")]
    LiteralColumnMismatch {
        span: Span,
        literal: String,
        expected: DataType,
    },

    /// A numeric literal's value does not fit the width its context pins
    /// it to (`300` into an `int8` column).
    #[error("literal `{literal}` is out of range for `{target}`")]
    LiteralOutOfRange {
        span: Span,
        literal: String,
        target: DataType,
    },

    /// A UDF call has no matching `.extern fn` declaration.
    #[error("call to undeclared UDF `{name}`")]
    UndeclaredUdf { span: Span, name: String },

    /// A UDF call passes the wrong number of arguments.
    #[error("UDF `{name}` expects {expected} argument(s) but got {found}")]
    UdfArity {
        span: Span,
        name: String,
        expected: usize,
        found: usize,
    },

    /// A UDF argument's type doesn't match the declared parameter.
    #[error("UDF `{name}` parameter `{param}` expects `{expected:?}` but got `{found:?}`")]
    UdfArgType {
        span: Span,
        name: String,
        param: String,
        expected: DataType,
        found: DataType,
    },

    /// A built-in argument's type isn't in the parameter's allowed set.
    /// Arity is enforced earlier by [`ParseError::BuiltinArity`](crate::ParseError),
    /// so the typechecker only worries about per-arg type fit. `expected` is the
    /// set of accepted types (one element for a fixed param, several for a
    /// polymorphic one like `to_string`).
    #[error("built-in `{op}` argument {arg_index} expects `{expected:?}` but got `{found:?}`")]
    BuiltinArgType {
        span: Span,
        op: BuiltinOperator,
        arg_index: usize,
        expected: Vec<DataType>,
        found: DataType,
    },

    /// `ord(s)` was used without `--str-intern`. `ord` returns the
    /// symbol's intern key, which only exists when strings are
    /// interned; there's no collision-free fallback to use otherwise.
    #[error("built-in `ord` requires `--str-intern` to be enabled")]
    OrdRequiresStrIntern { span: Span },

    /// A `_` placeholder appears in a tuple *construct* (`x = (a, _)`).
    /// Placeholders are only meaningful when destructuring.
    #[error("`_` placeholder is not allowed when constructing a tuple")]
    TuplePlaceholderInConstruct { span: Span },

    /// A tuple destructure (`(a, b) = x`) doesn't match `x`'s type: `x` is
    /// not a tuple, or the pattern has more fields than the tuple.
    #[error("invalid tuple destructure: {detail}")]
    TupleDestructure { span: Span, detail: String },

    /// A tuple construct (`(e0, ...)`) doesn't match the declared tuple type;
    /// wrong field count or a field whose value type doesn't fit.
    #[error("invalid tuple construct: {detail}")]
    TupleConstruct { span: Span, detail: String },

    /// `sum` / `avg` / `min` / `max` applied to a non-numeric input.
    #[error("aggregation `{op:?}` requires a numeric input but got `{ty:?}`")]
    AggregationInputNotNumeric {
        span: Span,
        op: AggregationOperator,
        ty: DataType,
    },

    /// The declared output type of an aggregation contradicts the
    /// operator's contract.
    #[error("aggregation `{op:?}` cannot produce result of type `{declared:?}`")]
    AggregationOutputType {
        span: Span,
        op: AggregationOperator,
        declared: DataType,
    },

    /// A head column's type disagrees with the relation's `.decl`.
    #[error("head column {col} of `{rel}` expects `{expected:?}` but produces `{found:?}`")]
    HeadColumnType {
        span: Span,
        rel: String,
        col: usize,
        expected: DataType,
        found: DataType,
    },

    /// A head's arity disagrees with the relation's `.decl`.
    #[error("head `{rel}` expects arity {expected} but got {found}")]
    HeadArity {
        span: Span,
        rel: String,
        expected: usize,
        found: usize,
    },

    /// Sibling subtypes joined at the same variable (no meet).
    #[error(
        "variable `{var}` declared as `{first_ty}` but later used as `{later_ty}` (no common subtype)"
    )]
    SubtypeMismatch {
        var: String,
        first_ty: String,
        first_span: Span,
        later_ty: String,
        later_span: Span,
    },

    /// Comparison operands with no common subtype; e.g. `x = y` where
    /// `x: UserId` and `y: ProductId` are siblings of `number`.
    #[error("comparison operands have incompatible subtypes: `{left_ty}` and `{right_ty}`")]
    ComparisonSubtypeMismatch {
        span: Span,
        left_ty: String,
        right_ty: String,
    },

    /// Narrowing in a head column without an explicit `as()`.
    #[error(
        "head column {col} of `{rel}` expects `{expected}` but receives `{found}` (use `as(expr, {expected})` to narrow)"
    )]
    HeadSubtypeMismatch {
        span: Span,
        rel: String,
        col: usize,
        expected: String,
        found: String,
    },

    /// `as(expr, T)` where source and target have different primitive roots.
    #[error("illegal cast: cannot cast `{from}` to `{to}` (different primitive roots)")]
    IllegalCast {
        span: Span,
        from: String,
        to: String,
    },

    /// `as(expr, T)` where `T` is undeclared.
    #[error("unknown cast target type `{name}`")]
    UnknownCastType { span: Span, name: String },

    /// A rule head references a variable never bound by a positive body
    /// atom. Valid syntax, but the variable has no value at evaluation time.
    #[error("unknown head variable `{var}`")]
    UnknownHeadVariable {
        head_span: Span,
        rule_span: Span,
        var: String,
    },

    /// A single rule head contains more than one aggregation argument.
    /// FlowLog's evaluator materializes at most one aggregation per head.
    #[error("rule head for `{rel}` contains {count} aggregations; at most one is allowed")]
    MultipleAggregationsInHead {
        head_span: Span,
        rule_span: Span,
        rel: String,
        count: usize,
    },

    /// A grammar contract the Pest grammar should have made unreachable. Not a
    /// user error; reported as an internal compiler bug.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl ParseError {
    /// Construct a [`ParseError::Syntax`] from a Pest error, anchoring the
    /// span to `file`.
    pub(crate) fn syntax_from_pest(err: &pest::error::Error<Rule>, file: FileId) -> Self {
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
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            // An internal error renders as its own bug-report diagnostic; every
            // other variant is a user diagnostic built on `base`.
            ParseError::Internal(e) => e.to_diagnostic(),

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
                    diag = diag.with_notes(vec![format!("include chain: {}", shown.join(" -> "))]);
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

            ParseError::UnknownInputIo { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "`IO=` selects the storage: \"file\" reads a delimited text file, \
                     \"command\" takes `put` tuples only, \"sqlite\" reads a database"
                        .into(),
                ]),

            ParseError::InvalidDelimiter { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "a delimiter is one ASCII character, written as a string literal: \
                     a tab is \"\\t\". A newline cannot delimit cells because the reader \
                     consumes it to end the line"
                        .into(),
                ]),

            ParseError::UnknownOutputIo { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "`IO=` selects the storage: \"file\" writes a delimited text file, \
                     \"sqlite\" writes a database table. Use the compiler's output \
                     directory to choose where rows go"
                        .into(),
                ]),

            ParseError::InvalidOrderBy { span, relation, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![format!(
                    "an `order_by` lists columns of `{relation}` by name, each optionally \
                     followed by ASC or DESC: order_by=\"b DESC, a\""
                )]),

            ParseError::InvalidLimit { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "a `limit` is a row count, written as a string literal: limit=\"10\"".into(),
                ]),

            ParseError::LimitWithoutOrderBy { span, .. } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "add an `order_by` so the rows that survive the limit are the same \
                     on every run"
                        .into(),
                ]),

            ParseError::Syntax { span, .. }
            | ParseError::BuiltinArity { span, .. }
            | ParseError::AssignmentVarInNegation { span, .. }
            | ParseError::GroundRuleNotConst { span }
            | ParseError::IncludeIo { span, .. } => base.with_labels(primary_only(*span)),
            ParseError::TypeMismatch {
                var,
                first_ty,
                first_span,
                later_ty,
                later_span,
            } => {
                let mut label_vec = Vec::new();
                if let Some(l) = primary_label(*later_span) {
                    label_vec.push(l.with_message(format!("`{var}` used as `{later_ty:?}` here")));
                }
                if let Some(l) = secondary_label(*first_span) {
                    label_vec
                        .push(l.with_message(format!("`{var}` first bound as `{first_ty:?}`")));
                }
                base.with_labels(label_vec).with_notes(vec![
                    "a variable's type is fixed by its first positive-atom occurrence; \
                     all later uses must agree"
                        .into(),
                ])
            }

            ParseError::ArithmeticTypeMismatch { span, left, right } => {
                base.with_labels(labels(
                    *span,
                    format!("`{left:?}` and `{right:?}` cannot be combined"),
                ))
            }

            ParseError::ArithmeticOpNotAllowed { span, op, ty } => base
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "numeric operators (`+`, `-`, `*`, `/`, `%`) require numeric factors; \
                     `cat` requires strings; `Bool` has no arithmetic"
                        .into(),
                ]),

            ParseError::ComparisonTypeMismatch {
                span, left, right, ..
            } => base.with_labels(labels(
                *span,
                format!("`{left:?}` cannot be compared with `{right:?}`"),
            )),

            ParseError::ComparisonOpNotAllowed { span, op, ty } => base
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "ordering comparisons (`<`, `<=`, `>`, `>=`) require numeric or \
                     string operands; `=` and `!=` work on any matching types"
                        .into(),
                ]),

            ParseError::UndeclaredUdf { span, name } => base
                .with_labels(labels(*span, format!("`{name}` is never declared")))
                .with_notes(vec![format!(
                    "add a matching `.extern fn {name}(...): ...` declaration, \
                     or remove the call"
                )]),

            ParseError::UdfArity {
                span,
                name,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{name}` expects {expected} argument(s), got {found}"),
            )),

            ParseError::UdfArgType {
                span,
                name,
                param,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{name}` param `{param}`: expected `{expected:?}`, got `{found:?}`"),
            )),

            ParseError::BuiltinArgType {
                span,
                op,
                arg_index,
                expected,
                found,
            } => {
                let expected = expected
                    .iter()
                    .map(|t| format!("{t:?}"))
                    .collect::<Vec<_>>()
                    .join(" | ");
                base.with_labels(labels(
                    *span,
                    format!(
                        "built-in `{op}` arg {arg_index}: expected `{expected}`, got `{found:?}`"
                    ),
                ))
            }

            ParseError::OrdRequiresStrIntern { span } => base
                .with_labels(labels(*span, "`ord` used here"))
                .with_notes(vec![
                    "ord returns the symbol's intern key: a unique per-string \
                     integer that only exists when strings are interned. Compile \
                     with `--str-intern` (binary mode) or `.string_intern(true)` \
                     (library mode) to use it."
                        .into(),
                ]),

            ParseError::TuplePlaceholderInConstruct { span } => base
                .with_labels(labels(*span, "`_` placeholder here"))
                .with_notes(vec![
                    "a `_` can only ignore a component when destructuring a bound \
                     tuple (`(a, _) = x`); a construct must supply every field."
                        .into(),
                ]),

            ParseError::TupleDestructure { span, detail }
            | ParseError::TupleConstruct { span, detail } => {
                base.with_labels(labels(*span, detail.clone()))
            }

            ParseError::AggregationInputNotNumeric { span, op, ty } => {
                base.with_labels(labels(
                    *span,
                    format!("`{op:?}` requires a numeric column but found `{ty:?}`"),
                ))
            }

            ParseError::AggregationOutputType { span, op, declared } => {
                base.with_labels(labels(
                    *span,
                    format!("declared as `{declared:?}`, incompatible with `{op:?}`"),
                ))
            }

            ParseError::HeadColumnType {
                span,
                rel,
                col,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{rel}` column {col} expects `{expected:?}`, got `{found:?}`"),
            )),

            ParseError::HeadArity {
                span,
                rel,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{rel}` expects {expected} column(s), got {found}"),
            )),

            ParseError::LiteralColumnMismatch {
                span,
                literal,
                expected,
            } => base.with_labels(labels(
                *span,
                format!("`{literal}` does not fit `{expected:?}`"),
            )),

            ParseError::GroupedPlaceholder { span } => base
                .with_labels(primary_only(*span))
                .with_notes(vec![
                    "a 1-tuple that ignores its component is `(_,)`; grouping needs an expression"
                        .into(),
                ]),

            ParseError::InvalidStringLiteral { span, reason } => {
                base.with_labels(labels(*span, reason.clone()))
            }

            ParseError::LiteralOutOfRange {
                span,
                literal,
                target,
            } => base.with_labels(labels(
                *span,
                format!("`{literal}` is out of range for `{target}`"),
            )),

            ParseError::SubtypeMismatch {
                var,
                first_ty,
                first_span,
                later_ty,
                later_span,
            } => {
                let mut label_vec = Vec::new();
                if let Some(l) = primary_label(*later_span) {
                    label_vec.push(l.with_message(format!("`{var}` used as `{later_ty}` here")));
                }
                if let Some(l) = secondary_label(*first_span) {
                    label_vec.push(l.with_message(format!("`{var}` first bound as `{first_ty}`")));
                }
                base.with_labels(label_vec).with_notes(vec![
                    "sibling subtypes of the same primitive are intentionally incompatible; \
                     wrap one side with `as(expr, OtherType)` if you really mean to join them"
                        .into(),
                ])
            }

            ParseError::ComparisonSubtypeMismatch {
                span,
                left_ty,
                right_ty,
            } => base
                .with_labels(labels(
                    *span,
                    format!("`{left_ty}` and `{right_ty}` have no common subtype"),
                ))
                .with_notes(vec![
                    "wrap one side with `as(expr, OtherType)` to assert they should compare".into(),
                ]),

            ParseError::HeadSubtypeMismatch {
                span,
                rel,
                col,
                expected,
                found,
            } => base
                .with_labels(labels(
                    *span,
                    format!("`{rel}` column {col} expects `{expected}`, found `{found}`"),
                ))
                .with_notes(vec![
                    "head columns allow implicit widening (subtype -> parent), \
                     but narrowing (parent -> subtype) requires `as(expr, TargetType)`"
                        .into(),
                ]),

            ParseError::IllegalCast { span, from, to } => base
                .with_labels(labels(*span, format!("`{from}` cannot be cast to `{to}`")))
                .with_notes(vec![
                    "`as()` only casts within the same primitive root \
                     (e.g. between two `<: number` subtypes)"
                        .into(),
                ]),

            ParseError::UnknownCastType { span, name } => base
                .with_labels(labels(*span, format!("`{name}` is not a declared type")))
                .with_notes(vec![format!(
                    "use a built-in primitive or add `.type {name} = ...` (or `<:`)"
                )]),

            ParseError::UnknownHeadVariable {
                head_span,
                rule_span,
                var,
            } => base
                .with_labels(dup_labels(
                    *head_span,
                    *rule_span,
                    &format!("`{var}` is referenced here but never bound by a positive body atom"),
                    "in this rule",
                ))
                .with_notes(vec![
                    "every variable in the rule head must appear in a positive body \
                     atom so its value is determined during evaluation"
                        .into(),
                ]),

            ParseError::MultipleAggregationsInHead {
                head_span,
                rule_span,
                ..
            } => base
                .with_labels(dup_labels(
                    *head_span,
                    *rule_span,
                    "multiple aggregations declared here",
                    "in this rule",
                ))
                .with_notes(vec![
                    "split the head into multiple rules, each producing a separate \
                     relation, if you need several aggregated columns"
                        .into(),
                ]),

        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, ParseError::Internal(_))
    }
}

/// Produce a `ParseError::Internal` for a violated internal invariant: an
/// "impossible" state an earlier stage should have guaranteed.
///
/// Use this instead of an `.expect`/`panic!` at sites the grammar or an
/// earlier pass makes unreachable: a Pest token the grammar guarantees
/// (`"atom_rule always contains relation_name"`), or a type-check invariant
/// (`"subtype pass: atom already declared"`). If such a site ever trips, it's
/// a FlowLog bug, not a user error.
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

    /// A grammar failure is mapped to `ParseError::Syntax` by `syntax_from_pest`.
    #[test]
    fn syntax_from_pest_maps_grammar_failure_to_syntax() {
        use pest::Parser as _;

        let err = crate::FlowLogParser::parse(Rule::main_grammar, ".decl edge(x: number")
            .expect_err("a malformed `.decl` should fail the grammar");
        assert!(matches!(
            ParseError::syntax_from_pest(&err, FileId::new(0)),
            ParseError::Syntax { .. }
        ));
    }

    #[test]
    fn unknown_head_variable_labels_head_and_rule() {
        let (sm, f) = make_sm_with("Out(x) :- Edge(y, z).\n");
        let out = render(
            ParseError::UnknownHeadVariable {
                head_span: Span::new(f, 0, 6),
                rule_span: Span::new(f, 0, 21),
                var: "x".into(),
            },
            &sm,
        );
        assert!(out.contains("unknown head variable `x`"), "got: {out}");
        assert!(out.contains("never bound"), "got: {out}");
        assert!(out.contains("in this rule"), "got: {out}");
    }

    #[test]
    fn multiple_aggregations_labels_head_and_rule() {
        let (sm, f) = make_sm_with("Totals(sum(a), count(b)) :- Orders(a, b).\n");
        let out = render(
            ParseError::MultipleAggregationsInHead {
                head_span: Span::new(f, 0, 24),
                rule_span: Span::new(f, 0, 41),
                rel: "Totals".into(),
                count: 2,
            },
            &sm,
        );
        assert!(out.contains("contains 2 aggregations"), "got: {out}");
        assert!(out.contains("at most one is allowed"), "got: {out}");
        assert!(
            out.contains("multiple aggregations declared here"),
            "got: {out}"
        );
    }
}
