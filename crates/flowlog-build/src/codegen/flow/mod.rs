//! Flow analysis — per-stratum transformation / aggregation codegen.

pub(super) mod non_recursive;
pub(super) mod recursive;
pub(super) mod transformation;

use common::source::Span;
use parser::{AggregationOperator, DataType};

use crate::codegen::error::CodegenError;

/// Check that `agg_type` — the declared output column for an aggregation
/// — is one of the numeric types the runtime can emit. Runs once per
/// aggregation at the flow boundary, so downstream token emitters never
/// encounter a non-numeric type.
pub(super) fn check_aggregation_type(
    op: AggregationOperator,
    agg_type: DataType,
    span: Span,
) -> Result<(), CodegenError> {
    if agg_type.is_numeric() {
        Ok(())
    } else {
        Err(CodegenError::AggregationTypeMismatch { op, agg_type, span })
    }
}
