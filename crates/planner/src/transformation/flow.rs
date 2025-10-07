//! Transformation flows used during query planning.
//!
//! A TransformationFlow represents how data is transformed as it flows
//! through different stages of query execution, including filtering,
//! projection, and joins.

use super::KeyValueLayout;
use crate::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument, TransformationArgument,
};
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos};
use parser::ConstType;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::trace;

/// Represents data transformation flows in query execution.
///
/// TransformationFlow defines how data is transformed as it flows through
/// different stages of query execution, including filtering, projection,
/// and joins.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub enum TransformationFlow {
    /// Single relation transformations: filtering, projection.
    /// Example: `((x), y) -> ((), x, y)` with filter `x > 0`.
    ///
    /// Note:
    /// - Arithmetic expressions are only allowed in the last transformation
    ///   before output or aggregation.
    KVToKV {
        /// Output key expressions
        key: Arc<Vec<ArithmeticArgument>>,
        /// Output value expressions
        value: Arc<Vec<ArithmeticArgument>>,
        /// Equality constraints (e.g., `x = 5`)
        constraints: Constraints,
        /// Comparison filters (e.g., `x > y`)
        compares: Vec<ComparisonExprArgument>,
    },

    /// Join operations between two relations.
    /// Example: `((x), y) + ((x), z) -> ((), x, y, z)`
    ///
    /// Note:
    /// - TODO: Only argument-level joins are considered. Any arithmetic inside
    ///   a join key (e.g., `x + 1`) will be handled in the future.
    JnToKV {
        /// Output key expressions
        key: Arc<Vec<ArithmeticArgument>>,
        /// Output value expressions
        value: Arc<Vec<ArithmeticArgument>>,
        /// Join filters
        compares: Vec<ComparisonExprArgument>,
    },
}

// ========================
// Constructors
// ========================
impl TransformationFlow {
    /// Creates a KVToKV transformation flow from input/output expressions and constraints.
    pub fn kv_to_kv(
        input_kv_layout: &KeyValueLayout,
        output_kv_layout: &KeyValueLayout,
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        // Map input expressions to transformation arguments
        let input_expr_map = Self::kv_argument_flow_map(input_kv_layout);

        // Generate output key/value arguments
        let flow_key_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.key());
        let flow_value_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.value());

        // Process constant and variable equality constraints via helpers
        let flow_const_args =
            Self::build_const_eq_constraints(&input_expr_map, const_eq_constraints);
        let flow_var_eq_args = Self::build_var_eq_constraints(&input_expr_map, var_eq_constraints);

        // Process comparison constraints
        let flow_compares = Self::build_compare_arguments(&input_expr_map, compare_exprs);

        Self::KVToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            constraints: Constraints::new(flow_const_args, flow_var_eq_args),
            compares: flow_compares,
        }
    }

    /// Creates a JnToKV transformation flow from input/output expressions and join conditions.
    pub fn join_to_kv(
        input_left_kv_layout: &KeyValueLayout,
        input_right_kv_layout: &KeyValueLayout,
        output_kv_layout: &KeyValueLayout,
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        // Map input expressions to transformation arguments
        let input_expr_map =
            Self::jn_argument_flow_map(input_left_kv_layout, input_right_kv_layout);

        // Generate output key/value arguments
        let flow_key_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.key());
        let flow_value_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.value());

        // Process comparison constraints
        let flow_compares = Self::build_compare_arguments(&input_expr_map, compare_exprs);

        Self::JnToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            compares: flow_compares,
        }
    }
}

// ========================
// Getters
// ========================
impl TransformationFlow {
    /// Returns the constraints for flows that support them.
    pub fn constraints(&self) -> &Constraints {
        match self {
            Self::KVToKV { constraints, .. } => constraints,
            Self::JnToKV { .. } => {
                panic!("Planner error: TransformationFlow::constraints is not supported for JnToKV")
            }
        }
    }

    /// Returns the comparison filters for flows that support them.
    pub fn compares(&self) -> &Vec<ComparisonExprArgument> {
        match self {
            Self::KVToKV { compares, .. } => compares,
            Self::JnToKV { compares, .. } => compares,
        }
    }

    /// Returns the output key expressions.
    pub fn key(&self) -> &Arc<Vec<ArithmeticArgument>> {
        match self {
            Self::KVToKV { key, .. } => key,
            Self::JnToKV { key, .. } => key,
        }
    }

    /// Returns the output value expressions.
    pub fn value(&self) -> &Arc<Vec<ArithmeticArgument>> {
        match self {
            Self::KVToKV { value, .. } => value,
            Self::JnToKV { value, .. } => value,
        }
    }
}

// ========================
// Helper Functions
// ========================
impl TransformationFlow {
    /// Creates a new flow with join arguments flipped from left to right side.
    pub fn jn_flip(&self) -> Self {
        match self {
            Self::JnToKV {
                key,
                value,
                compares,
            } => Self::JnToKV {
                key: Arc::new(key.iter().map(|arg| arg.jn_flip()).collect()),
                value: Arc::new(value.iter().map(|arg| arg.jn_flip()).collect()),
                compares: compares.iter().map(|comp| comp.jn_flip()).collect(),
            },
            _ => panic!("Planner error: TransformationFlow::jn_flip can only be called on JnToKV"),
        }
    }

    /// Checks if this flow has any constraints or filters applied.
    pub fn is_constrained(&self) -> bool {
        match self {
            Self::KVToKV {
                constraints,
                compares,
                ..
            } => !constraints.is_empty() || !compares.is_empty(),
            Self::JnToKV { compares, .. } => !compares.is_empty(),
        }
    }

    /// Checks if the output key is empty (indicating flat tuple output).
    pub fn is_key_empty(&self) -> bool {
        match self {
            Self::KVToKV { key, .. } => key.is_empty(),
            Self::JnToKV { key, .. } => key.is_empty(),
        }
    }
}

// ========================
// Private Helper Methods
// ========================
impl TransformationFlow {
    /// Creates a mapping from arithmetic position expressions to key-value
    /// transformation arguments for operator flows.
    fn kv_argument_flow_map(
        kv_layout: &KeyValueLayout,
    ) -> HashMap<ArithmeticPos, TransformationArgument> {
        kv_layout
            .key()
            .iter()
            .enumerate()
            .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((false, id))))
            .chain(
                kv_layout
                    .value()
                    .iter()
                    .enumerate()
                    .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((true, id)))),
            )
            .collect()
    }

    /// Creates a mapping from arithmetic position expressions to join
    /// transformation arguments for join flows.
    fn jn_argument_flow_map(
        jn_left_layout: &KeyValueLayout,
        jn_right_layout: &KeyValueLayout,
    ) -> HashMap<ArithmeticPos, TransformationArgument> {
        jn_left_layout
            .key()
            .iter()
            .enumerate()
            .map(|(id, expr)| (expr.clone(), TransformationArgument::Jn((false, false, id))))
            .chain(
                jn_left_layout.value().iter().enumerate().map(|(id, expr)| {
                    (expr.clone(), TransformationArgument::Jn((false, true, id)))
                }),
            )
            .chain(
                jn_right_layout.key().iter().enumerate().map(|(id, expr)| {
                    (expr.clone(), TransformationArgument::Jn((true, false, id)))
                }),
            )
            .chain(
                jn_right_layout
                    .value()
                    .iter()
                    .enumerate()
                    .map(|(id, expr)| (expr.clone(), TransformationArgument::Jn((true, true, id)))),
            )
            .collect()
    }

    /// Composes output arithmetic expressions from available input chunks.
    ///
    /// This method handles both direct expression lookup and factor-by-factor
    /// construction when the complete expression isn't found in the input map.
    fn flow_over_exprs(
        input_exprs_map: &HashMap<ArithmeticPos, TransformationArgument>,
        output_exprs: &[ArithmeticPos],
    ) -> Vec<ArithmeticArgument> {
        trace!(
            "flow_over_exprs: input_exprs_map = {:?}, output_exprs = {:?}",
            input_exprs_map,
            output_exprs
        );

        output_exprs
            .iter()
            .map(|expr| {
                // First try to find the entire expression in the map
                if let Some(&trans_arg) = input_exprs_map.get(expr) {
                    // Found the complete expression - create a simple variable reference
                    ArithmeticArgument {
                        init: FactorArgument::Var(trans_arg),
                        rest: vec![],
                    }
                } else {
                    // Not found as complete expression - build factor by factor
                    // Find the init factor
                    let init_factor = match expr.init() {
                        catalog::FactorPos::Var(sig) => {
                            // Look up the single-var expression directly
                            let key = ArithmeticPos::from_var_signature(*sig);
                            let trans_arg = input_exprs_map.get(&key).copied().unwrap_or_else(|| {
                                panic!(
                                    "Planner error: missing init variable signature {:?} in input expression map",
                                    sig
                                )
                            });
                            FactorArgument::Var(trans_arg)
                        }
                        catalog::FactorPos::Const(c) => FactorArgument::Const(c.clone()),
                    };

                    // Find the rest factors
                    let rest_factors: Vec<(parser::ArithmeticOperator, FactorArgument)> = expr
                        .rest()
                        .iter()
                        .map(|(op, factor)| {
                            let factor_arg = match factor {
                                catalog::FactorPos::Var(sig) => {
                                    // Look up the single-var expression directly
                                    let key = ArithmeticPos::from_var_signature(*sig);
                                    let trans_arg = input_exprs_map.get(&key).copied().unwrap_or_else(|| {
                                        panic!(
                                            "Planner error: missing rest variable signature {:?} in input expression map",
                                            sig
                                        )
                                    });
                                    FactorArgument::Var(trans_arg)
                                }
                                catalog::FactorPos::Const(c) => FactorArgument::Const(c.clone()),
                            };
                            (op.clone(), factor_arg)
                        })
                        .collect();

                    // Create ArithmeticArgument using the operators and factors
                    ArithmeticArgument {
                        init: init_factor,
                        rest: rest_factors,
                    }
                }
            })
            .collect()
    }

    /// Helper to construct constant equality constraints: (var = const)
    fn build_const_eq_constraints(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
    ) -> Vec<(TransformationArgument, ConstType)> {
        let const_exprs: Vec<ArithmeticPos> = const_eq_constraints
            .iter()
            .map(|(signature, _)| ArithmeticPos::from_var_signature(*signature))
            .collect();

        TransformationArgument::from_arithmetic_arguments(Self::flow_over_exprs(
            input_expr_map,
            &const_exprs,
        ))
        .into_iter()
        .zip(
            const_eq_constraints
                .iter()
                .map(|(_, constant)| constant.clone()),
        )
        .collect()
    }

    /// Helper to construct variable equality constraints: (var = var)
    fn build_var_eq_constraints(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
    ) -> Vec<(TransformationArgument, TransformationArgument)> {
        let (left_exprs, right_exprs): (Vec<_>, Vec<_>) = var_eq_constraints
            .iter()
            .map(|(left_sig, right_sig)| {
                (
                    ArithmeticPos::from_var_signature(*left_sig),
                    ArithmeticPos::from_var_signature(*right_sig),
                )
            })
            .unzip();

        let left_args = TransformationArgument::from_arithmetic_arguments(Self::flow_over_exprs(
            input_expr_map,
            &left_exprs,
        ));
        let right_args = TransformationArgument::from_arithmetic_arguments(Self::flow_over_exprs(
            input_expr_map,
            &right_exprs,
        ));

        left_args.into_iter().zip(right_args).collect()
    }

    /// Helper to construct comparison arguments from input expression map and comparison positions.
    fn build_compare_arguments(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        compare_exprs: &[ComparisonExprPos],
    ) -> Vec<ComparisonExprArgument> {
        compare_exprs
            .iter()
            .map(|comp| {
                let (left_exprs, right_exprs): (Vec<_>, Vec<_>) = (
                    comp.left()
                        .signatures()
                        .iter()
                        .map(|&signature| ArithmeticPos::from_var_signature(*signature))
                        .collect(),
                    comp.right()
                        .signatures()
                        .iter()
                        .map(|&signature| ArithmeticPos::from_var_signature(*signature))
                        .collect(),
                );

                let left_trans_args = TransformationArgument::from_arithmetic_arguments(
                    Self::flow_over_exprs(input_expr_map, &left_exprs),
                );
                let right_trans_args = TransformationArgument::from_arithmetic_arguments(
                    Self::flow_over_exprs(input_expr_map, &right_exprs),
                );

                ComparisonExprArgument::from_comparison_expr(
                    comp,
                    &left_trans_args,
                    &right_trans_args,
                )
            })
            .collect()
    }
}

impl fmt::Display for TransformationFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KVToKV {
                key,
                value,
                constraints,
                compares,
            } => {
                let filters_str = match (constraints.is_empty(), compares.is_empty()) {
                    (true, true) => String::new(),
                    (false, true) => format!(" if {}", constraints),
                    (true, false) => format!(
                        " if {}",
                        compares
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    (false, false) => format!(
                        " if {} and {}",
                        constraints,
                        compares
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };

                let key_str = key
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let value_str = value
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(
                    f,
                    "key:({}), value:({}), filter:({})",
                    key_str, value_str, filters_str
                )
            }

            Self::JnToKV {
                key,
                value,
                compares,
            } => {
                let filters_str = if compares.is_empty() {
                    String::new()
                } else {
                    format!(
                        " if {}",
                        compares
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };

                let key_str = key
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let value_str = value
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(
                    f,
                    "key:({}), value:({}), filter:({})",
                    key_str, value_str, filters_str
                )
            }
        }
    }
}
