//! Rewrites transformation expressions and output metadata for chosen layouts.
//!
//! `rewrite` validates input compatibility, rewrites the operator itself, then
//! reconnects its inputs. Prepared output metadata retains the original query
//! body and fingerprint; `expression_rewrite` applies slot mappings to the
//! output expressions and comparison trees.

use std::collections::HashMap;
use std::sync::Arc;

use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::JoinOrientation;
use super::expression_rewrite;
use super::upstream_rewrite;
use super::upstream_rewrite::RewrittenCollection;
use super::validation;
use crate::catalog::ArithmeticPos;
use crate::planner::Collection;
use crate::planner::Constraints;
use crate::planner::PlanError;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

/// Applies one transformation's choices without changing its identity or scope.
/// Returns an internal error for incompatible layouts or invalid references.
pub(super) fn rewrite(
    transformation: &mut Transformation,
    output_rewrite: &RewrittenCollection,
    join_orientation: JoinOrientation,
    rewritten_outputs: &HashMap<u64, RewrittenCollection>,
) -> Result<(), PlanError> {
    // Resolve the old sides before changing either the flow or its connections;
    // a swapped join must not remap its slots against already swapped inputs.
    let input_rewrites =
        validation::transformation(transformation, join_orientation, rewritten_outputs)?;
    rewrite_self(
        transformation,
        output_rewrite,
        join_orientation,
        &input_rewrites,
    )?;
    upstream_rewrite::rewrite_transformation(transformation, rewritten_outputs, join_orientation)
}

/// Keeps lineage and the canonical query body, permuting only output columns.
/// The order must already be a validated permutation within each block.
/// Returns an internal error for a column outside its original block.
pub(super) fn prepare_output(
    collection: &Arc<Collection>,
    column_order: AttributeLayout,
) -> Result<RewrittenCollection, PlanError> {
    let (key_arity, value_arity) = collection.arity();
    if column_order == AttributeLayout::new(key_arity, value_arity) {
        return Ok(RewrittenCollection::new(
            Arc::clone(collection),
            column_order,
        ));
    }
    // Arrangement IDs flatten the original key and value. The two metadata
    // representations keep separate blocks, so values need local positions.
    let value_order: Vec<usize> = column_order
        .value()
        .iter()
        .map(|&column| {
            column.checked_sub(key_arity).ok_or_else(|| {
                PlanError::internal(format!(
                    "value reorder of {collection} reads key column {column}",
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let mut rewritten_collection = collection.as_ref().clone();
    let layout = rewritten_collection.kv_layout_mut();
    reorder_columns(layout.key_mut(), column_order.key())?;
    reorder_columns(layout.value_mut(), &value_order)?;
    let canonical = rewritten_collection.canonical_mut();
    reorder_columns(canonical.key_mut(), column_order.key())?;
    reorder_columns(canonical.value_mut(), &value_order)?;
    Ok(RewrittenCollection::new(
        Arc::new(rewritten_collection),
        column_order,
    ))
}

/// Reorders one metadata block without changing its arity or expressions.
/// Returns an internal error for a wrong length or an absent original column.
fn reorder_columns(columns: &mut [ArithmeticPos], column_order: &[usize]) -> Result<(), PlanError> {
    if columns.len() != column_order.len() {
        return Err(PlanError::internal(format!(
            "column reorder has {} positions, expected {} columns",
            column_order.len(),
            columns.len(),
        )));
    }
    let reordered: Vec<ArithmeticPos> = column_order
        .iter()
        .map(|&column| {
            columns.get(column).cloned().ok_or_else(|| {
                PlanError::internal(format!(
                    "column reorder reads column {column}, expected fewer than {}",
                    columns.len(),
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    columns.clone_from_slice(&reordered);
    Ok(())
}

/// Rewrites flow slots and output metadata for the new representations.
/// `input_rewrites` must follow original side order. Input collections stay
/// unchanged here; their later rewrite must use the same join orientation.
/// Returns an internal error for an invalid slot, including a nested one.
fn rewrite_self(
    transformation: &mut Transformation,
    output_rewrite: &RewrittenCollection,
    join_orientation: JoinOrientation,
    input_rewrites: &[RewrittenCollection],
) -> Result<(), PlanError> {
    let remap_input_slot = |argument| match argument {
        TransformationArgument::KV((is_key, index)) => Ok(TransformationArgument::KV((
            is_key,
            input_rewrites[0].remap_slot(is_key, index)?,
        ))),
        TransformationArgument::Jn((is_left, is_key, index)) => {
            let input_rewrite = input_rewrites.get(usize::from(!is_left)).ok_or_else(|| {
                PlanError::internal("join expression reads a missing right input")
            })?;
            let rewritten_is_left = if join_orientation == JoinOrientation::Swapped {
                !is_left
            } else {
                is_left
            };
            Ok(TransformationArgument::Jn((
                rewritten_is_left,
                is_key,
                input_rewrite.remap_slot(is_key, index)?,
            )))
        }
    };
    let (output_collection, flow) = match transformation {
        Transformation::RowToRow { output, flow, .. }
        | Transformation::RowToKv { output, flow, .. }
        | Transformation::KvToRow { output, flow, .. }
        | Transformation::KvToKv { output, flow, .. }
        | Transformation::JnToRow { output, flow, .. }
        | Transformation::JnToKv { output, flow, .. }
        | Transformation::NJnToRow { output, flow, .. }
        | Transformation::NJnToKv { output, flow, .. }
        | Transformation::AggToRow { output, flow, .. }
        | Transformation::AggToKv { output, flow, .. } => (output, flow),
    };
    match flow {
        TransformationFlow::KVToKV {
            key,
            value,
            constraints,
            compares,
        } => {
            let constant_equalities = constraints
                .constant_eq_constraints()
                .iter()
                .map(|(argument, constant)| Ok((remap_input_slot(*argument)?, constant.clone())))
                .collect::<Result<Vec<_>, PlanError>>()?;
            let variable_equalities = constraints
                .variable_eq_constraints()
                .iter()
                .map(|(left, right)| Ok((remap_input_slot(*left)?, remap_input_slot(*right)?)))
                .collect::<Result<Vec<_>, PlanError>>()?;
            *constraints = Constraints::new(constant_equalities, variable_equalities);
            expression_rewrite::rewrite_output(
                key,
                value,
                compares,
                output_rewrite.column_order(),
                &remap_input_slot,
            )?;
        }
        TransformationFlow::JnToKV {
            key,
            value,
            compares,
        } => {
            expression_rewrite::rewrite_output(
                key,
                value,
                compares,
                output_rewrite.column_order(),
                &remap_input_slot,
            )?;
        }
        TransformationFlow::Aggregate {
            target,
            key,
            value,
            compares,
            ..
        } => {
            if let Some(target) = target {
                *target = remap_input_slot(*target)?;
            }
            // Only the target reads matches. Output V0 is the aggregate answer,
            // not the first value column of the matches arrangement.
            let remap_answer_slot = |argument| match argument {
                TransformationArgument::KV((true, index)) => Ok(TransformationArgument::KV((
                    true,
                    input_rewrites[0].remap_slot(/*is_key*/ true, index)?,
                ))),
                TransformationArgument::KV((false, 0)) => Ok(argument),
                TransformationArgument::KV((false, _)) | TransformationArgument::Jn(_) => Err(
                    PlanError::internal(format!("invalid aggregate answer slot {argument:?}")),
                ),
            };
            expression_rewrite::rewrite_output(
                key,
                value,
                compares,
                output_rewrite.column_order(),
                &remap_answer_slot,
            )?;
        }
    }
    *output_collection = Arc::clone(output_rewrite.collection());
    Ok(())
}
