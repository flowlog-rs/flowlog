//! Output-expression ordering and recursive input-slot rewriting.
//!
//! One slot mapping applies to arithmetic, calls, tuples and comparisons.
//! Operator-specific input coordinates and join direction stay with the
//! transformation rewrite that supplies this mapping.

use std::sync::Arc;

use super::super::arrangement_graph::AttributeLayout;
use crate::planner::ArithmeticArgument;
use crate::planner::ComparisonExprArgument;
use crate::planner::FactorArgument;
use crate::planner::PlanError;
use crate::planner::TransformationArgument;

/// Permutes existing output expressions and remaps every input reference.
/// Projection cannot bring back columns absent from these expressions.
/// Requires a validated output order within the original key/value blocks.
/// Returns an internal error if the slot mapping rejects an input reference.
pub(super) fn rewrite_output(
    key_expressions: &mut Arc<Vec<ArithmeticArgument>>,
    value_expressions: &mut Arc<Vec<ArithmeticArgument>>,
    comparisons: &mut [ComparisonExprArgument],
    output_order: &AttributeLayout,
    remap_slot: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    let key_arity = key_expressions.len();
    let reorder_expressions = |expressions: &[ArithmeticArgument],
                               original_columns: &[usize],
                               block_offset: usize| {
        original_columns
            .iter()
            .map(|&original_column| {
                let mut rewritten_expression = expressions[original_column - block_offset].clone();
                rewrite_expression(&mut rewritten_expression, remap_slot)?;
                Ok(rewritten_expression)
            })
            .collect::<Result<Vec<_>, PlanError>>()
    };
    *key_expressions = Arc::new(reorder_expressions(key_expressions, output_order.key(), 0)?);
    *value_expressions = Arc::new(reorder_expressions(
        value_expressions,
        output_order.value(),
        key_arity,
    )?);
    for comparison in comparisons {
        rewrite_expression(comparison.left_mut(), remap_slot)?;
        rewrite_expression(comparison.right_mut(), remap_slot)?;
    }
    Ok(())
}

/// Descends through arithmetic, calls and tuples, preserving their operators.
fn rewrite_expression(
    expression: &mut ArithmeticArgument,
    remap_slot: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    rewrite_factor(&mut expression.init, remap_slot)?;
    for (_, operand) in &mut expression.rest {
        rewrite_factor(operand, remap_slot)?;
    }
    Ok(())
}

/// Applies the same slot mapping inside every expression shape.
fn rewrite_factor(
    operand: &mut FactorArgument,
    remap_slot: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    match operand {
        FactorArgument::Var(argument) => *argument = remap_slot(*argument)?,
        FactorArgument::Const(_) => {}
        FactorArgument::FnCall { args, .. }
        | FactorArgument::Builtin { args, .. }
        | FactorArgument::Tuple { fields: args } => {
            for argument in args {
                rewrite_expression(argument, remap_slot)?;
            }
        }
        FactorArgument::Group(argument)
        | FactorArgument::TupleProj {
            tuple: argument, ..
        } => rewrite_expression(argument, remap_slot)?,
    }
    Ok(())
}
