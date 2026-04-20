//! User-facing type errors raised by [`check_program`](super::check_program).
//!
//! Each variant carries the spans needed to render a
//! [`codespan_reporting`] diagnostic that points at the offending site
//! (and, where helpful, a secondary site such as the original binding).

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use common::diag::{labels, primary_label, secondary_label, Diagnostic, InternalError, BUG_URL};
use common::source::{FileId, Span};
use parser::{AggregationOperator, ArithmeticOperator, ComparisonOperator, DataType};
use thiserror::Error;

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

    /// Type-checker invariant violation, rendered as a "please file a bug" ICE.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl TypeCheckError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
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

            TypeCheckError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, TypeCheckError::Internal(_))
    }
}
