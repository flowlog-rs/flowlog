//! Permutes physical fields after planning, preserving logical column uses.

use std::sync::Arc;

use super::ArithmeticArgument;
use super::Constraints;
use super::FactorArgument;
use super::Transformation;
use super::TransformationArgument;
use super::TransformationFlow;

fn arithmetic(
    value: &mut ArithmeticArgument,
    remap: &impl Fn(TransformationArgument) -> TransformationArgument,
) {
    factor(&mut value.init, remap);
    for (_, value) in &mut value.rest {
        factor(value, remap);
    }
}

fn factor(
    value: &mut FactorArgument,
    remap: &impl Fn(TransformationArgument) -> TransformationArgument,
) {
    match value {
        FactorArgument::Var(slot) => *slot = remap(*slot),
        FactorArgument::Const(_) => (),
        FactorArgument::FnCall { args, .. }
        | FactorArgument::Builtin { args, .. }
        | FactorArgument::Tuple { fields: args } => {
            for arg in args {
                arithmetic(arg, remap);
            }
        }
        FactorArgument::Group(arg) | FactorArgument::TupleProj { tuple: arg, .. } => {
            arithmetic(arg, remap);
        }
    }
}

impl Transformation {
    /// Returns this step with physical columns permuted within K and V.
    ///
    /// # Panics
    ///
    /// Panics if an order is not a complete permutation of its input or
    /// output columns. Callers must preserve each key prefix and give both
    /// join inputs the same key permutation.
    pub fn experiment_permute(&self, inputs: &[Vec<usize>], output: &[usize], swap: bool) -> Self {
        let arities = if self.is_unary() {
            vec![self.unary_input().arity()]
        } else {
            vec![self.binary_input().0.arity(), self.binary_input().1.arity()]
        };
        let anti = matches!(self, Self::NJnToRow { .. } | Self::NJnToKv { .. });
        let unary_side = usize::from(anti);
        let position = |side: usize, key: bool, index: usize| {
            let keys = arities[side].0;
            let old = if key { index } else { keys + index };
            let new = inputs[side]
                .iter()
                .position(|&column| column == old)
                .unwrap();
            (new < keys, if new < keys { new } else { new - keys })
        };
        let aggregate = matches!(self, Self::AggToRow { .. } | Self::AggToKv { .. });
        let remap = |slot| match slot {
            TransformationArgument::KV((false, index)) if aggregate => {
                TransformationArgument::KV((false, index))
            }
            TransformationArgument::KV((key, index)) => {
                let (key, index) = position(unary_side, key, index);
                TransformationArgument::KV((key, index))
            }
            TransformationArgument::Jn((left, key, index)) => {
                let (key, index) = position(usize::from(!left), key, index);
                TransformationArgument::Jn((left ^ swap, key, index))
            }
        };
        let mut result = self.clone();
        let flow = match &mut result {
            Self::AggToRow { flow, .. }
            | Self::AggToKv { flow, .. }
            | Self::RowToRow { flow, .. }
            | Self::RowToKv { flow, .. }
            | Self::KvToRow { flow, .. }
            | Self::KvToKv { flow, .. } => flow,
            Self::JnToRow { input, flow, .. }
            | Self::JnToKv { input, flow, .. }
            | Self::NJnToRow { input, flow, .. }
            | Self::NJnToKv { input, flow, .. } => {
                if swap {
                    std::mem::swap(&mut input.0, &mut input.1);
                }
                flow
            }
        };
        let output_keys = self.output().arity().0;
        match flow {
            TransformationFlow::KVToKV {
                key,
                value,
                constraints,
                compares,
            } => {
                let fields: Vec<_> = key.iter().chain(value.iter()).cloned().collect();
                *key = Arc::new(
                    output[..output_keys]
                        .iter()
                        .map(|&index| {
                            let mut value = fields[index].clone();
                            arithmetic(&mut value, &remap);
                            value
                        })
                        .collect(),
                );
                *value = Arc::new(
                    output[output_keys..]
                        .iter()
                        .map(|&index| {
                            let mut value = fields[index].clone();
                            arithmetic(&mut value, &remap);
                            value
                        })
                        .collect(),
                );
                *constraints = Constraints::new(
                    constraints
                        .constant_eq_constraints()
                        .iter()
                        .map(|(slot, value)| (remap(*slot), value.clone()))
                        .collect(),
                    constraints
                        .variable_eq_constraints()
                        .iter()
                        .map(|(left, right)| (remap(*left), remap(*right)))
                        .collect(),
                );
                for compare in compares {
                    arithmetic(&mut compare.left, &remap);
                    arithmetic(&mut compare.right, &remap);
                }
            }
            TransformationFlow::JnToKV {
                key,
                value,
                compares,
            }
            | TransformationFlow::Aggregate {
                key,
                value,
                compares,
                ..
            } => {
                let fields: Vec<_> = key.iter().chain(value.iter()).cloned().collect();
                *key = Arc::new(
                    output[..output_keys]
                        .iter()
                        .map(|&index| {
                            let mut value = fields[index].clone();
                            arithmetic(&mut value, &remap);
                            value
                        })
                        .collect(),
                );
                *value = Arc::new(
                    output[output_keys..]
                        .iter()
                        .map(|&index| {
                            let mut value = fields[index].clone();
                            arithmetic(&mut value, &remap);
                            value
                        })
                        .collect(),
                );
                for compare in compares {
                    arithmetic(&mut compare.left, &remap);
                    arithmetic(&mut compare.right, &remap);
                }
            }
        }
        if let TransformationFlow::Aggregate {
            target: Some(slot), ..
        } = flow
        {
            let (key, index) = match *slot {
                TransformationArgument::KV((key, index)) => position(0, key, index),
                _ => unreachable!(),
            };
            *slot = TransformationArgument::KV((key, index));
        }
        result
    }
}
