//! Physical reordering for maps, joins, antijoins and aggregations.
//! Transformation slots, expressions and collection metadata are rewritten
//! together; sorting boundaries retain their key/value membership.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use super::arrangement_graph::Arities;
use super::arrangement_graph::JoinOrientation;
use crate::planner::ArithmeticArgument;
use crate::planner::BindingOperation;
use crate::planner::CanonicalForm;
use crate::planner::Collection;
use crate::planner::ComparisonExprArgument;
use crate::planner::Constraints;
use crate::planner::FactorArgument;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::RelationBindingPlan;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

// =============================================================================
// Input
// =============================================================================

/// A rewritten producer and its old-slot to new-slot correspondence.
#[derive(Debug, Clone)]
pub(super) struct Input {
    pub(super) collection: Arc<Collection>,
    pub(super) order: Arities,
}

impl Input {
    /// Borrows a rewritten producer, or keeps an unchanged input's own view.
    #[must_use]
    fn resolve<'producers>(
        input: &Arc<Collection>,
        rewritten: &'producers HashMap<u64, Self>,
    ) -> Cow<'producers, Self> {
        rewritten
            .get(&input.fingerprint())
            .map(Cow::Borrowed)
            .unwrap_or_else(|| {
                let (key, value) = input.arity();
                Cow::Owned(Self {
                    collection: Arc::clone(input),
                    order: Arities::new(key, value),
                })
            })
    }

    /// Returns the new slot for an original block index. Invalid references
    /// are plan errors, including references hidden inside expressions.
    fn slot(&self, is_key: bool, index: usize) -> Result<usize, PlanError> {
        let columns = if is_key {
            &self.order.key
        } else {
            &self.order.value
        };
        let field = if is_key {
            index
        } else {
            self.order.key.len() + index
        };
        columns
            .iter()
            .position(|&column| column == field)
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "collection 0x{:016x} has no {} slot {index} under {:?}",
                    self.collection.fingerprint(),
                    if is_key { "key" } else { "value" },
                    self.order,
                ))
            })
    }
}

// =============================================================================
// Binding operation
// =============================================================================

/// Sets the internal comparator of a dedup or declared aggregation. Its
/// inputs and logical output schema are preserved. The order must already
/// be validated against the binding's sortable columns.
pub(super) fn binding(binding: &mut RelationBindingPlan, order: &Arities) {
    let (BindingOperation::Dedup { order: columns }
    | BindingOperation::Aggregation { order: columns }) = &mut binding.operation;
    columns.clone_from(&order.key);
}

// =============================================================================
// Transformation
// =============================================================================

/// Applies one output order and compensates for its inputs' changed orders.
/// Aggregate output expressions read the synthetic answer row; their target
/// alone reads the matches input's value block.
/// Requires a block-preserving output order and a legal join orientation.
/// Returns an internal error for incompatible keys or invalid slot references.
pub(super) fn transformation(
    step: &mut Transformation,
    order: &Arities,
    orientation: JoinOrientation,
    rewritten: &HashMap<u64, Input>,
) -> Result<(), PlanError> {
    let inputs: Vec<Cow<'_, Input>> = match step {
        Transformation::RowToRow { input, .. }
        | Transformation::RowToKv { input, .. }
        | Transformation::KvToRow { input, .. }
        | Transformation::KvToKv { input, .. } => {
            vec![Input::resolve(input, rewritten)]
        }
        Transformation::JnToRow {
            input: (left, right),
            ..
        }
        | Transformation::JnToKv {
            input: (left, right),
            ..
        }
        | Transformation::NJnToRow {
            input: (left, right),
            ..
        }
        | Transformation::NJnToKv {
            input: (left, right),
            ..
        } => {
            vec![
                Input::resolve(left, rewritten),
                Input::resolve(right, rewritten),
            ]
        }
        Transformation::AggToRow {
            input: (matches, groups),
            ..
        }
        | Transformation::AggToKv {
            input: (matches, groups),
            ..
        } => {
            let mut inputs = vec![Input::resolve(matches, rewritten)];
            inputs.extend(
                groups
                    .as_ref()
                    .map(|groups| Input::resolve(groups, rewritten)),
            );
            inputs
        }
    };
    if let [left, right] = inputs.as_slice()
        && left.order.key != right.order.key
    {
        return Err(PlanError::internal(format!(
            "{} matches incompatible key orders {:?} and {:?}",
            step.operation_name(),
            left.order.key,
            right.order.key,
        )));
    }
    let map = |argument| match argument {
        TransformationArgument::KV((is_key, index)) => Ok(TransformationArgument::KV((
            is_key,
            inputs[0].slot(is_key, index)?,
        ))),
        TransformationArgument::Jn((is_left, is_key, index)) => {
            let input = inputs.get(usize::from(!is_left)).ok_or_else(|| {
                PlanError::internal("join expression reads a missing right input")
            })?;
            let side = if orientation == JoinOrientation::Swapped {
                !is_left
            } else {
                is_left
            };
            Ok(TransformationArgument::Jn((
                side,
                is_key,
                input.slot(is_key, index)?,
            )))
        }
    };
    let flow = match step {
        Transformation::RowToRow { flow, .. }
        | Transformation::RowToKv { flow, .. }
        | Transformation::KvToRow { flow, .. }
        | Transformation::KvToKv { flow, .. }
        | Transformation::JnToRow { flow, .. }
        | Transformation::JnToKv { flow, .. }
        | Transformation::NJnToRow { flow, .. }
        | Transformation::NJnToKv { flow, .. }
        | Transformation::AggToRow { flow, .. }
        | Transformation::AggToKv { flow, .. } => flow,
    };
    match flow {
        TransformationFlow::KVToKV {
            key,
            value,
            constraints,
            compares,
        } => {
            let constants = constraints
                .constant_eq_constraints()
                .iter()
                .map(|(argument, constant)| Ok((map(*argument)?, constant.clone())))
                .collect::<Result<Vec<_>, PlanError>>()?;
            let variables = constraints
                .variable_eq_constraints()
                .iter()
                .map(|(left, right)| Ok((map(*left)?, map(*right)?)))
                .collect::<Result<Vec<_>, PlanError>>()?;
            *constraints = Constraints::new(constants, variables);
            remap_output(key, value, compares, order, &map)?;
        }
        TransformationFlow::JnToKV {
            key,
            value,
            compares,
        } => {
            remap_output(key, value, compares, order, &map)?;
        }
        TransformationFlow::Aggregate {
            target,
            key,
            value,
            compares,
            ..
        } => {
            if let Some(target) = target {
                *target = map(*target)?;
            }
            let answer = |argument| match argument {
                TransformationArgument::KV((true, index)) => {
                    Ok(TransformationArgument::KV((
                        true,
                        inputs[0].slot(/*is_key*/ true, index)?,
                    )))
                }
                TransformationArgument::KV((false, 0)) => Ok(argument),
                TransformationArgument::KV((false, _)) | TransformationArgument::Jn(_) => Err(
                    PlanError::internal(format!("invalid aggregate answer slot {argument:?}")),
                ),
            };
            remap_output(key, value, compares, order, &answer)?;
        }
    }
    let replace = |input: &mut Arc<Collection>, index: usize| {
        *input = Arc::clone(&inputs[index].collection);
    };
    let output = match step {
        Transformation::RowToRow { input, output, .. }
        | Transformation::RowToKv { input, output, .. }
        | Transformation::KvToRow { input, output, .. }
        | Transformation::KvToKv { input, output, .. } => {
            replace(input, 0);
            output
        }
        Transformation::JnToRow {
            input: (left, right),
            output,
            ..
        }
        | Transformation::JnToKv {
            input: (left, right),
            output,
            ..
        }
        | Transformation::NJnToRow {
            input: (left, right),
            output,
            ..
        }
        | Transformation::NJnToKv {
            input: (left, right),
            output,
            ..
        } => {
            replace(left, 0);
            replace(right, 1);
            if orientation == JoinOrientation::Swapped {
                std::mem::swap(left, right);
            }
            output
        }
        Transformation::AggToRow {
            input: (matches, groups),
            output,
            ..
        }
        | Transformation::AggToKv {
            input: (matches, groups),
            output,
            ..
        } => {
            replace(matches, 0);
            if let Some(groups) = groups {
                replace(groups, 1);
            }
            output
        }
    };
    let value_order: Vec<usize> = order
        .value
        .iter()
        .map(|&column| column - order.key.len())
        .collect();
    *output = Arc::new(collection(output, &order.key, &value_order));
    Ok(())
}

/// Permutes output expressions and rewrites references, without introducing
/// a runtime map. Projection keeps only its own output expressions.
fn remap_output(
    key: &mut Arc<Vec<ArithmeticArgument>>,
    value: &mut Arc<Vec<ArithmeticArgument>>,
    compares: &mut [ComparisonExprArgument],
    order: &Arities,
    map: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    let columns = key.len();
    let mut new_key = Vec::with_capacity(key.len());
    let mut new_value = Vec::with_capacity(value.len());
    for &field in &order.key {
        let mut expr = key[field].clone();
        expression(&mut expr, map)?;
        new_key.push(expr);
    }
    for &field in &order.value {
        let mut expr = value[field - columns].clone();
        expression(&mut expr, map)?;
        new_value.push(expr);
    }
    *key = Arc::new(new_key);
    *value = Arc::new(new_value);
    for compare in compares {
        expression(&mut compare.left, map)?;
        expression(&mut compare.right, map)?;
    }
    Ok(())
}

// =============================================================================
// Collection and expression coordinates
// =============================================================================

/// Keeps collection identity and query body while reordering both the local
/// layout and canonical columns. Orders use indices within each original
/// block and have already been validated against its membership.
#[must_use]
fn collection(input: &Collection, key: &[usize], value: &[usize]) -> Collection {
    let mut output = input.clone();
    output.kv_layout = KeyValueLayout::new(
        key.iter()
            .map(|&column| input.kv_layout.key()[column].clone())
            .collect(),
        value
            .iter()
            .map(|&column| input.kv_layout.value()[column].clone())
            .collect(),
    );
    let form: &mut CanonicalForm = &mut output.canonical;
    form.key = key
        .iter()
        .map(|&column| input.canonical.key[column].clone())
        .collect();
    form.value = value
        .iter()
        .map(|&column| input.canonical.value[column].clone())
        .collect();
    output
}

/// Rewrites every slot recursively, preserving constants and operators.
fn expression(
    expr: &mut ArithmeticArgument,
    map: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    factor(&mut expr.init, map)?;
    for (_, operand) in &mut expr.rest {
        factor(operand, map)?;
    }
    Ok(())
}

/// Descends through calls and tuples so nested slots follow the same mapping
/// as top-level columns.
fn factor(
    operand: &mut FactorArgument,
    map: &impl Fn(TransformationArgument) -> Result<TransformationArgument, PlanError>,
) -> Result<(), PlanError> {
    match operand {
        FactorArgument::Var(argument) => *argument = map(*argument)?,
        FactorArgument::Const(_) => {}
        FactorArgument::FnCall { args, .. }
        | FactorArgument::Builtin { args, .. }
        | FactorArgument::Tuple { fields: args } => {
            for argument in args {
                expression(argument, map)?;
            }
        }
        FactorArgument::Group(argument)
        | FactorArgument::TupleProj {
            tuple: argument, ..
        } => expression(argument, map)?,
    }
    Ok(())
}
