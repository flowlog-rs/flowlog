//! User-facing type errors raised by [`check_program`](super::check_program).
//!
//! Each variant carries the spans needed to render a
//! [`codespan_reporting`] diagnostic that points at the offending site
//! (and, where helpful, a secondary site such as the original binding).

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use thiserror::Error;

use crate::common::{
    BUG_URL, Diagnostic, FileId, InternalError, Span, labels, primary_label, secondary_label,
};
use crate::parser::{
    AggregationOperator, ArithmeticOperator, BuiltinOperator, ComparisonOperator, DataType,
};

#[derive(Debug, Error)]
pub enum TypeCheckError {
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

    /// A built-in argument's type doesn't match the declared parameter.
    /// Arity is enforced earlier by [`ParseError::BuiltinArity`](super::super::parser::ParseError),
    /// so the typechecker only worries about per-arg type fit.
    #[error("built-in `{op}` argument {arg_index} expects `{expected:?}` but got `{found:?}`")]
    BuiltinArgType {
        span: Span,
        op: BuiltinOperator,
        arg_index: usize,
        expected: DataType,
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

    /// A tuple destructure (`(a, b) = x`) doesn't match `x`'s type — `x` is
    /// not a tuple, or the pattern has more fields than the tuple.
    #[error("invalid tuple destructure: {detail}")]
    TupleDestructure { span: Span, detail: String },

    /// A tuple construct (`(e0, …)`) doesn't match the declared tuple type —
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

    /// Comparison operands with no common subtype — e.g. `x = y` where
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

    /// Type-checker invariant violation, rendered as a "please file a bug" ICE.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl TypeCheckError {
    pub(super) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("typechecker", detail, BUG_URL))
    }
}

impl Diagnostic for TypeCheckError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            TypeCheckError::TypeMismatch {
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

            TypeCheckError::ArithmeticTypeMismatch { span, left, right } => {
                base.with_labels(labels(
                    *span,
                    format!("`{left:?}` and `{right:?}` cannot be combined"),
                ))
            }

            TypeCheckError::ArithmeticOpNotAllowed { span, op, ty } => base
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "numeric operators (`+`, `-`, `*`, `/`, `%`) require numeric factors; \
                     `cat` requires strings; `Bool` has no arithmetic"
                        .into(),
                ]),

            TypeCheckError::ComparisonTypeMismatch {
                span, left, right, ..
            } => base.with_labels(labels(
                *span,
                format!("`{left:?}` cannot be compared with `{right:?}`"),
            )),

            TypeCheckError::ComparisonOpNotAllowed { span, op, ty } => base
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "ordering comparisons (`<`, `<=`, `>`, `>=`) require numeric or \
                     string operands; `=` and `!=` work on any matching types"
                        .into(),
                ]),

            TypeCheckError::UndeclaredUdf { span, name } => base
                .with_labels(labels(*span, format!("`{name}` is never declared")))
                .with_notes(vec![format!(
                    "add a matching `.extern fn {name}(...): ...` declaration, \
                     or remove the call"
                )]),

            TypeCheckError::UdfArity {
                span,
                name,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{name}` expects {expected} argument(s), got {found}"),
            )),

            TypeCheckError::UdfArgType {
                span,
                name,
                param,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{name}` param `{param}`: expected `{expected:?}`, got `{found:?}`"),
            )),

            TypeCheckError::BuiltinArgType {
                span,
                op,
                arg_index,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!(
                    "built-in `{op}` arg {arg_index}: expected `{expected:?}`, got `{found:?}`"
                ),
            )),

            TypeCheckError::OrdRequiresStrIntern { span } => base
                .with_labels(labels(*span, "`ord` used here"))
                .with_notes(vec![
                    "ord returns the symbol's intern key — a unique per-string \
                     integer that only exists when strings are interned. Compile \
                     with `--str-intern` (binary mode) or `.string_intern(true)` \
                     (library mode) to use it."
                        .into(),
                ]),

            TypeCheckError::TuplePlaceholderInConstruct { span } => base
                .with_labels(labels(*span, "`_` placeholder here"))
                .with_notes(vec![
                    "a `_` can only ignore a component when destructuring a bound \
                     tuple (`(a, _) = x`); a construct must supply every field."
                        .into(),
                ]),

            TypeCheckError::TupleDestructure { span, detail }
            | TypeCheckError::TupleConstruct { span, detail } => {
                base.with_labels(labels(*span, detail.clone()))
            }

            TypeCheckError::AggregationInputNotNumeric { span, op, ty } => {
                base.with_labels(labels(
                    *span,
                    format!("`{op:?}` requires a numeric column but found `{ty:?}`"),
                ))
            }

            TypeCheckError::AggregationOutputType { span, op, declared } => {
                base.with_labels(labels(
                    *span,
                    format!("declared as `{declared:?}`, incompatible with `{op:?}`"),
                ))
            }

            TypeCheckError::HeadColumnType {
                span,
                rel,
                col,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{rel}` column {col} expects `{expected:?}`, got `{found:?}`"),
            )),

            TypeCheckError::HeadArity {
                span,
                rel,
                expected,
                found,
            } => base.with_labels(labels(
                *span,
                format!("`{rel}` expects {expected} column(s), got {found}"),
            )),

            TypeCheckError::LiteralColumnMismatch {
                span,
                literal,
                expected,
            } => base.with_labels(labels(
                *span,
                format!("`{literal}` does not fit `{expected:?}`"),
            )),

            TypeCheckError::SubtypeMismatch {
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
                    "sibling subtypes of the same primitive are intentionally incompatible — \
                     wrap one side with `as(expr, OtherType)` if you really mean to join them"
                        .into(),
                ])
            }

            TypeCheckError::ComparisonSubtypeMismatch {
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

            TypeCheckError::HeadSubtypeMismatch {
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
                    "head columns allow implicit widening (subtype → parent), \
                     but narrowing (parent → subtype) requires `as(expr, TargetType)`"
                        .into(),
                ]),

            TypeCheckError::IllegalCast { span, from, to } => base
                .with_labels(labels(*span, format!("`{from}` cannot be cast to `{to}`")))
                .with_notes(vec![
                    "`as()` only casts within the same primitive root \
                     (e.g. between two `<: number` subtypes)"
                        .into(),
                ]),

            TypeCheckError::UnknownCastType { span, name } => base
                .with_labels(labels(*span, format!("`{name}` is not a declared type")))
                .with_notes(vec![format!(
                    "use a built-in primitive or add `.type {name} = ...` (or `<:`)"
                )]),

            TypeCheckError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, TypeCheckError::Internal(_))
    }
}
