//! Catalog errors — user-facing safety / type violations and
//! catalog-internal ICEs.

use std::fmt;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use common::diag::{labels, primary_label, secondary_label, Diagnostic, InternalError, BUG_URL};
use common::source::{FileId, Span};
use parser::{AggregationOperator, ArithmeticOperator, ComparisonOperator, DataType};
use thiserror::Error;

/// Which body predicate carried an unsafe variable. Only affects the
/// rendered wording; all three kinds share the same range-restriction rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsafePredicateKind {
    Negation,
    Comparison,
    FnCall,
}

impl fmt::Display for UnsafePredicateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negation => write!(f, "negated atom"),
            Self::Comparison => write!(f, "comparison"),
            Self::FnCall => write!(f, "function call"),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CatalogError {
    /// A variable in a negated atom, comparison, or function call never
    /// appears in a positive body atom (Datalog¬ range-restriction).
    #[error("unsafe variable `{var}` in {kind} `{predicate}`")]
    UnsafeVariable {
        kind: UnsafePredicateKind,
        /// Display form of the offending predicate.
        predicate: String,
        predicate_span: Span,
        rule_span: Span,
        var: String,
    },

    /// A variable is used at two positions with disagreeing types
    /// (e.g. a join where the two atoms declare different column types).
    #[error("variable `{var}` bound as `{first_ty:?}` but used as `{later_ty:?}`")]
    TypeMismatch {
        var: String,
        first_ty: DataType,
        first_span: Span,
        later_ty: DataType,
        later_span: Span,
    },

    /// Mixed types inside a single arithmetic expression.
    #[error("mixed types in arithmetic expression: `{left:?}` and `{right:?}`")]
    ArithmeticTypeMismatch {
        span: Span,
        left: DataType,
        right: DataType,
    },

    /// An arithmetic operator applied to an incompatible type
    /// (e.g. `+` on `String`, `cat` on numeric, any op on `Bool`).
    #[error("arithmetic operator `{op}` is not allowed on `{ty:?}`")]
    ArithmeticOpNotAllowed {
        span: Span,
        op: ArithmeticOperator,
        ty: DataType,
    },

    /// `left op right` where `left` and `right` have disagreeing types.
    #[error("comparison sides disagree: `{left:?}` {op} `{right:?}`")]
    ComparisonTypeMismatch {
        span: Span,
        op: ComparisonOperator,
        left: DataType,
        right: DataType,
    },

    /// Ordering comparison (`<`, `<=`, `>`, `>=`) on a type with no
    /// natural order (`Bool`).
    #[error("comparison operator `{op}` is not allowed on `{ty:?}`")]
    ComparisonOpNotAllowed {
        span: Span,
        op: ComparisonOperator,
        ty: DataType,
    },

    /// A rule references a UDF with no matching `.extern fn` declaration.
    #[error("call to undeclared UDF `{name}`")]
    UndeclaredUdf { span: Span, name: String },

    /// UDF call has the wrong arity.
    #[error("UDF `{name}` expects {expected} argument(s) but got {found}")]
    UdfArity {
        span: Span,
        name: String,
        expected: usize,
        found: usize,
    },

    /// UDF call argument type disagrees with the declared parameter type.
    #[error("UDF `{name}` parameter `{param}` expects `{expected:?}` but got `{found:?}`")]
    UdfArgType {
        span: Span,
        name: String,
        param: String,
        expected: DataType,
        found: DataType,
    },

    /// An aggregation's input expression isn't numeric (for
    /// `sum`/`avg`/`min`/`max`).
    #[error("aggregation `{op:?}` requires a numeric input but got `{ty:?}`")]
    AggregationInputNotNumeric {
        span: Span,
        op: AggregationOperator,
        ty: DataType,
    },

    /// The declared output column type for an aggregation disagrees with
    /// the operator's contract.
    #[error("aggregation `{op:?}` cannot produce result of type `{declared:?}`")]
    AggregationOutputType {
        span: Span,
        op: AggregationOperator,
        declared: DataType,
    },

    /// A head column's type disagrees with the declared relation column.
    #[error("head column {col} of `{rel}` expects `{expected:?}` but produces `{found:?}`")]
    HeadColumnType {
        span: Span,
        rel: String,
        col: usize,
        expected: DataType,
        found: DataType,
    },

    /// Head arity disagrees with the relation's `.decl`.
    #[error("head `{rel}` expects arity {expected} but got {found}")]
    HeadArity {
        span: Span,
        rel: String,
        expected: usize,
        found: usize,
    },

    /// Catalog-internal invariant violation (typically a planner/catalog
    /// contract break). Rendered as a "please file a bug" ICE.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CatalogError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("catalog", detail, BUG_URL))
    }
}

impl Diagnostic for CatalogError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            CatalogError::UnsafeVariable {
                predicate_span,
                rule_span,
                var,
                ..
            } => {
                let mut label_vec = Vec::new();
                if let Some(l) = primary_label(*predicate_span) {
                    label_vec
                        .push(l.with_message(format!("`{var}` is never bound in a positive atom")));
                }
                if let Some(l) = secondary_label(*rule_span) {
                    label_vec.push(l.with_message("in this rule"));
                }
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(label_vec)
                    .with_notes(vec![
                        "every variable in a body predicate must also appear in a \
                         positive atom, so the set of tuples it ranges over is finite"
                            .into(),
                    ])
            }

            CatalogError::TypeMismatch {
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
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(label_vec)
                    .with_notes(vec![
                        "a variable's type is fixed by its first positive-atom occurrence; \
                         all later uses must agree"
                            .into(),
                    ])
            }

            CatalogError::ArithmeticTypeMismatch { span, left, right } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{left:?}` and `{right:?}` cannot be combined"),
                )),

            CatalogError::ArithmeticOpNotAllowed { span, op, ty } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "numeric operators (`+`, `-`, `*`, `/`, `%`) require numeric factors; \
                     `cat` requires strings; `Bool` columns have no arithmetic"
                        .into(),
                ]),

            CatalogError::ComparisonTypeMismatch {
                span, left, right, ..
            } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{left:?}` cannot be compared with `{right:?}`"),
                )),

            CatalogError::ComparisonOpNotAllowed { span, op, ty } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(*span, format!("`{op}` cannot apply to `{ty:?}`")))
                .with_notes(vec![
                    "ordering comparisons (`<`, `<=`, `>`, `>=`) require numeric or \
                     string operands; `==` and `!=` work on any matching types"
                        .into(),
                ]),

            CatalogError::UndeclaredUdf { span, name } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(*span, format!("`{name}` is never declared")))
                .with_notes(vec![format!(
                    "add a matching `.extern fn {name}(...): ...` declaration, \
                     or remove the call"
                )]),

            CatalogError::UdfArity {
                span,
                name,
                expected,
                found,
            } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{name}` expects {expected} argument(s), got {found}"),
                )),

            CatalogError::UdfArgType {
                span,
                name,
                param,
                expected,
                found,
            } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{name}` param `{param}`: expected `{expected:?}`, got `{found:?}`"),
                )),

            CatalogError::AggregationInputNotNumeric { span, op, ty } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{op:?}` requires a numeric column but found `{ty:?}`"),
                )),

            CatalogError::AggregationOutputType { span, op, declared } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("declared as `{declared:?}`, incompatible with `{op:?}`"),
                )),

            CatalogError::HeadColumnType {
                span,
                rel,
                col,
                expected,
                found,
            } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{rel}` column {col} expects `{expected:?}`, got `{found:?}`"),
                )),

            CatalogError::HeadArity {
                span,
                rel,
                expected,
                found,
            } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{rel}` expects {expected} column(s), got {found}"),
                )),

            CatalogError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CatalogError::Internal(_))
    }
}
