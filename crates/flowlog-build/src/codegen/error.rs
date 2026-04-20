//! Codegen errors — user-facing type violations and codegen-internal ICEs.
//!
//! User sites (aggregation type mismatch, arithmetic on a boolean, a call
//! to an undeclared UDF) surface as dedicated [`CodegenError`] variants
//! carrying a [`Span`]. Invariant violations (missing fingerprint, a
//! match arm the planner shouldn't have produced) wrap via
//! [`CodegenError::internal`] and render as `please file a bug` ICEs.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use common::diag::{labels, Diagnostic, InternalError, BUG_URL};
use common::source::{FileId, Span};
use parser::{AggregationOperator, DataType};
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CodegenError {
    /// A rule head uses an aggregation whose column type violates the
    /// operator's type contract (e.g. `sum` over a string column).
    #[error(
        "aggregation `{op:?}` cannot produce result of type `{agg_type:?}`: \
         the column must be numeric"
    )]
    AggregationTypeMismatch {
        op: AggregationOperator,
        agg_type: DataType,
        /// Rule head that introduced the aggregation.
        span: Span,
    },

    /// A rule body calls a UDF with no matching `.extern fn` declaration.
    #[error("call to undeclared UDF `{name}`")]
    UndeclaredUdf {
        name: String,
        /// Enclosing rule — the planner drops per-call-site spans, so the
        /// finest location we can cite is the rule itself.
        span: Span,
    },

    /// An arithmetic expression applies a numeric operator to a `Bool`
    /// column.
    #[error("arithmetic operations are not supported on `Bool` columns")]
    ArithmeticOnBool {
        /// Enclosing rule.
        span: Span,
    },

    /// Codegen invariant violation, rendered as an ICE.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CodegenError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("codegen", detail, BUG_URL))
    }
}

impl Diagnostic for CodegenError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            CodegenError::AggregationTypeMismatch { op, agg_type, span } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    format!("`{op:?}` here requires a numeric column but found `{agg_type:?}`"),
                ))
                .with_notes(vec![
                    "every aggregation in FlowLog requires a numeric column: \
                     `count` produces an integer, while `sum`/`avg`/`min`/`max` \
                     require the aggregated column itself to be numeric"
                        .into(),
                ]),

            CodegenError::UndeclaredUdf { name, span } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(*span, format!("`{name}` is never declared")))
                .with_notes(vec![format!(
                    "add a matching `.extern fn {name}(...): ...` declaration, \
                     or remove the call from the rule body"
                )]),

            CodegenError::ArithmeticOnBool { span } => CsDiagnostic::error()
                .with_message(self.to_string())
                .with_labels(labels(
                    *span,
                    "this expression mixes arithmetic with a boolean",
                ))
                .with_notes(vec![
                    "convert the boolean to an integer first, or restructure the \
                     rule so the boolean is consumed by a comparison instead of \
                     an arithmetic operator"
                        .into(),
                ]),

            CodegenError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CodegenError::Internal(_))
    }
}
