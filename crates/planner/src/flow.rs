//! Transformation flows for query planning in Macaron Datalog programs.

use crate::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument, TransformationArgument,
};
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos};
use parser::{AggregationOperator, ConstType};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Represents data transformation flows in query execution.
///
/// TransformationFlow defines how data is transformed as it flows through
/// different stages of query execution, including filtering, projection,
/// joins, and aggregations.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub enum TransformationFlow {
    /// Single relation transformations: filtering, projection.
    /// Example: `((x), y) -> ((), x, y)` with filter `x > 0`.
    ///
    /// Note:
    /// - Arithmetic expressions are only allowed in the last head transformation
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
    /// - Only argument-level joins are considered. Any arithmetic inside
    ///   a join key (e.g., `x + 1`) will be handled in the future.
    JnToKV {
        /// Output key expressions
        key: Arc<Vec<ArithmeticArgument>>,
        /// Output value expressions
        value: Arc<Vec<ArithmeticArgument>>,
        /// Join filters
        compares: Vec<ComparisonExprArgument>,
    },

    /// Aggregation operations on a relation.
    /// Example: `(x, y) -> (x, SUM(y))`.
    ///
    /// Note:
    /// - Only argument-level aggregation is considered. Any arithmetic inside
    ///   an aggregation (e.g., `SUM(x + 1)`) should be handled by a preceding
    ///   KVToKV transformation.
    /// - Both the input and output of this operator are rows.
    AggToRow {
        /// Output row expressions
        field: TransformationArgument,
        /// Grouping keys
        operator: AggregationOperator,
    },
}

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
            _ => panic!("Planner error: jn_flip() can only be called on JnToKV flows"),
        }
    }

    /// Returns the constraints for flows that support them.
    pub fn constraints(&self) -> &Constraints {
        match self {
            Self::KVToKV { constraints, .. } => constraints,
            Self::JnToKV { .. } => panic!("Planner error: constraints() called on JnToKV flow"),
            Self::AggToRow { .. } => panic!("Planner error: constraints() called on AggToRow flow"),
        }
    }

    /// Returns the comparison filters for flows that support them.
    pub fn compares(&self) -> &Vec<ComparisonExprArgument> {
        match self {
            Self::KVToKV { compares, .. } => compares,
            Self::JnToKV { compares, .. } => compares,
            Self::AggToRow { .. } => panic!("Planner error: compares() called on AggToRow flow"),
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
            Self::AggToRow { .. } => false, // Aggregation flows don't have constraints
        }
    }

    /// Checks if the output key is empty (indicating flat tuple output).
    pub fn is_key_empty(&self) -> bool {
        match self {
            Self::KVToKV { key, .. } => key.is_empty(),
            Self::JnToKV { key, .. } => key.is_empty(),
            Self::AggToRow { .. } => true, // Aggregation always produces flat rows
        }
    }

    /// Creates a KVToKV transformation flow from input/output expressions and constraints.
    pub fn kv_to_kv(
        input_key_exprs: &[ArithmeticPos],
        input_value_exprs: &[ArithmeticPos],
        output_key_exprs: &[ArithmeticPos],
        output_value_exprs: &[ArithmeticPos],
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        let input_expr_map = Self::kv_argument_flow_map(input_key_exprs, input_value_exprs);

        let flow_key_args = Self::flow_over_exprs(
            &input_expr_map,
            output_key_exprs,
            "TransformationFlow::kv_to_kv key",
        );
        let flow_value_args = Self::flow_over_exprs(
            &input_expr_map,
            output_value_exprs,
            "TransformationFlow::kv_to_kv value",
        );
        // Process constant and variable equality constraints via helpers
        let flow_const_args = Self::build_const_eq_constraints(
            &input_expr_map,
            const_eq_constraints,
            "(TransformationFlow::kv_to_kv)",
        );

        let flow_var_eq_args = Self::build_var_eq_constraints(
            &input_expr_map,
            var_eq_constraints,
            "(TransformationFlow::kv_to_kv)",
        );

        // Process comparison constraints
        let flow_compares = Self::build_compare_arguments(
            &input_expr_map,
            compare_exprs,
            "(TransformationFlow::kv_to_kv)",
        );

        Self::KVToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            constraints: Constraints::new(flow_const_args, flow_var_eq_args),
            compares: flow_compares,
        }
    }

    /// Creates a JnToKV transformation flow from input/output expressions and join conditions.
    pub fn join_to_kv(
        input_left_key_exprs: &[ArithmeticPos],
        input_left_value_exprs: &[ArithmeticPos],
        input_right_value_exprs: &[ArithmeticPos],
        output_key_exprs: &[ArithmeticPos],
        output_value_exprs: &[ArithmeticPos],
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        // Map left-side inputs to join transformation arguments
        let left_expr_map =
            Self::kv_argument_flow_map(input_left_key_exprs, input_left_value_exprs)
                .into_iter()
                .map(|(signature, trace)| {
                    let join_trace = match trace {
                        TransformationArgument::KV((key_or_value, id)) => {
                            TransformationArgument::Jn((false, key_or_value, id))
                        }
                        _ => panic!(
                            "TransformationFlow::Jn expects kv in left input: {:?}",
                            trace
                        ),
                    };
                    (signature, join_trace)
                });

        // Map right-side inputs to join transformation arguments (only values, no keys)
        let right_expr_map = Self::kv_argument_flow_map(&vec![], input_right_value_exprs) // Self::kv_argument_flow_map(input_right_key_signatures, input_right_value_signatures)
            .into_iter()
            .map(|(signature, trace)| {
                let join_trace = match trace {
                    TransformationArgument::KV((key_or_value, id)) => {
                        TransformationArgument::Jn((true, key_or_value, id))
                    }
                    _ => panic!(
                        "TransformationFlow::join_to_kv expects kv in right input: {:?}",
                        trace
                    ),
                };
                (signature, join_trace)
            });

        // Combine left and right expression maps
        let input_expr_map = left_expr_map.chain(right_expr_map).collect();

        let flow_key_args = Self::flow_over_exprs(
            &input_expr_map,
            output_key_exprs,
            "TransformationFlow::join_to_kv key",
        );
        let flow_value_args = Self::flow_over_exprs(
            &input_expr_map,
            output_value_exprs,
            "TransformationFlow::join_to_kv value",
        );

        // Process comparison constraints
        let flow_compares = Self::build_compare_arguments(
            &input_expr_map,
            compare_exprs,
            "(TransformationFlow::join_to_kv)",
        );

        Self::JnToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            compares: flow_compares,
        }
    }

    /// Creates an AggToRow transformation flow for aggregation operations.
    pub fn agg_to_row(
        input_value_exprs: &[ArithmeticPos],
        output_field_expr: &ArithmeticPos,
        operator: AggregationOperator,
    ) -> Self {
        // For aggregation, we guarantee that the output expression should directly
        // be found in the input value expressions, so we iterate and find the index
        let field_index = input_value_exprs
            .iter()
            .position(|input_expr| input_expr == output_field_expr)
            .unwrap_or_else(|| {
                panic!(
                    "Planner error: TransformationFlow::agg_to_row - output field {:?} not found in input expressions {:?}",
                    output_field_expr, input_value_exprs
                )
            });

        // Manually create the transformation argument for the aggregation field
        let field = TransformationArgument::KV((true, field_index));

        Self::AggToRow { field, operator }
    }
}

// ========================
// Internal helper methods
// ========================
impl TransformationFlow {
    /// Creates a mapping from arithmetic position expressions to transformation arguments for operator flows.
    fn kv_argument_flow_map(
        key_exprs: &[ArithmeticPos],
        value_exprs: &[ArithmeticPos],
    ) -> HashMap<ArithmeticPos, TransformationArgument> {
        key_exprs
            .iter()
            .enumerate()
            .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((false, id))))
            .chain(
                value_exprs
                    .iter()
                    .enumerate()
                    .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((true, id)))),
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
        context: &str,
    ) -> Vec<ArithmeticArgument> {
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
                            let key = ArithmeticPos::from_var_signature(sig.clone());
                            let trans_arg = input_exprs_map.get(&key).copied().unwrap_or_else(|| {
                                panic!(
                                    "Planner error: {} - init variable signature {:?} not found in input expressions map",
                                    context, sig
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
                                    let key = ArithmeticPos::from_var_signature(sig.clone());
                                    let trans_arg = input_exprs_map.get(&key).copied().unwrap_or_else(|| {
                                        panic!(
                                            "Planner error: {} - rest variable signature {:?} not found in input expressions map",
                                            context, sig
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
        context: &str,
    ) -> Vec<(TransformationArgument, ConstType)> {
        let const_exprs: Vec<ArithmeticPos> = const_eq_constraints
            .iter()
            .map(|(signature, _)| ArithmeticPos::from_var_signature(signature.clone()))
            .collect();

        TransformationArgument::from_arithmetic_arguments(
            Self::flow_over_exprs(input_expr_map, &const_exprs, &format!("{} const", context)),
            &format!("{} constant constraints", context),
        )
        .into_iter()
        .zip(
            const_eq_constraints
                .iter()
                .map(|(_, constant)| constant.clone()),
        )
        .collect::<Vec<(TransformationArgument, ConstType)>>()
    }

    /// Helper to construct variable equality constraints: (var = var)
    fn build_var_eq_constraints(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
        context: &str,
    ) -> Vec<(TransformationArgument, TransformationArgument)> {
        let left_exprs: Vec<ArithmeticPos> = var_eq_constraints
            .iter()
            .map(|(left_signature, _)| ArithmeticPos::from_var_signature(left_signature.clone()))
            .collect();

        let right_exprs: Vec<ArithmeticPos> = var_eq_constraints
            .iter()
            .map(|(_, right_signature)| ArithmeticPos::from_var_signature(right_signature.clone()))
            .collect();

        let left_args = TransformationArgument::from_arithmetic_arguments(
            Self::flow_over_exprs(
                input_expr_map,
                &left_exprs,
                &format!("{} var left", context),
            ),
            &format!("{} variable equality (left)", context),
        );
        let right_args = TransformationArgument::from_arithmetic_arguments(
            Self::flow_over_exprs(
                input_expr_map,
                &right_exprs,
                &format!("{} var right", context),
            ),
            &format!("{} variable equality (right)", context),
        );

        left_args
            .into_iter()
            .zip(right_args.into_iter())
            .collect::<Vec<(TransformationArgument, TransformationArgument)>>()
    }

    /// Helper to construct comparison arguments from input expression map and comparison positions.
    fn build_compare_arguments(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        compare_exprs: &[ComparisonExprPos],
        context: &str,
    ) -> Vec<ComparisonExprArgument> {
        compare_exprs
            .iter()
            .map(|comp| {
                let left_exprs = comp
                    .left()
                    .signatures()
                    .iter()
                    .map(|&signature| ArithmeticPos::from_var_signature(signature.clone()))
                    .collect::<Vec<_>>();
                let right_exprs = comp
                    .right()
                    .signatures()
                    .iter()
                    .map(|&signature| ArithmeticPos::from_var_signature(signature.clone()))
                    .collect::<Vec<_>>();

                let left_trans_args = TransformationArgument::from_arithmetic_arguments(
                    Self::flow_over_exprs(
                        input_expr_map,
                        &left_exprs,
                        &format!("{} compare left", context),
                    ),
                    &format!("{} (left)", context),
                );

                let right_trans_args = TransformationArgument::from_arithmetic_arguments(
                    Self::flow_over_exprs(
                        input_expr_map,
                        &right_exprs,
                        &format!("{} compare right", context),
                    ),
                    &format!("{} (right)", context),
                );

                ComparisonExprArgument::from_comparison_expr(
                    comp,
                    &left_trans_args,
                    &right_trans_args,
                )
            })
            .collect::<Vec<ComparisonExprArgument>>()
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
                            .map(|comp| comp.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    (false, false) => format!(
                        " if {} and {}",
                        constraints,
                        compares
                            .iter()
                            .map(|comp| comp.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };

                if key.is_empty() {
                    write!(
                        f,
                        "|({}) {}|",
                        value
                            .iter()
                            .map(|transformation_argument| format!("{}", transformation_argument))
                            .collect::<Vec<String>>()
                            .join(", "),
                        filters_str
                    )
                } else {
                    write!(
                        f,
                        "|({}: {}) {}|",
                        key.iter()
                            .map(|transformation_argument| format!("{}", transformation_argument))
                            .collect::<Vec<String>>()
                            .join(", "),
                        value
                            .iter()
                            .map(|transformation_argument| format!("{}", transformation_argument))
                            .collect::<Vec<String>>()
                            .join(", "),
                        filters_str
                    )
                }
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
                            .map(|comp| comp.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };

                if key.is_empty() {
                    write!(
                        f,
                        "|({}) {}|",
                        value
                            .iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        filters_str
                    )
                } else {
                    write!(
                        f,
                        "|({}: {}) {}|",
                        key.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        value
                            .iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        filters_str
                    )
                }
            }

            Self::AggToRow { field, operator } => {
                write!(f, "|Agg {} ({})|", operator, field)
            }
        }
    }
}
