//! Transformation flows used during query planning.
//!
//! A `TransformationFlow` represents how data is transformed as it flows
//! through different stages of query execution, including filtering,
//! projection, and joins. This module provides the core abstractions
//! for building and executing data transformation pipelines.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::trace;

use super::KeyValueLayout;
use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, ComparisonExprPos, FactorPos, FnCallPredicatePos,
    JoinPredicates, KvPredicates,
};
use flowlog_parser::ConstType;
use crate::planner::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument,
    FnCallPredicateArgument, TransformationArgument,
};

/// Represents data transformation flows in query execution.
///
/// TransformationFlow defines how data is transformed as it flows through
/// different stages of query execution, including filtering, projection,
/// and joins.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub(crate) enum TransformationFlow {
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
        /// Boolean UDF predicate filters (e.g., `is_valid(x)`)
        fn_call_preds: Vec<FnCallPredicateArgument>,
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
        /// Boolean UDF predicate filters (e.g., `is_valid(x)`)
        fn_call_preds: Vec<FnCallPredicateArgument>,
    },
}

// ========================
// Constructors
// ========================
impl TransformationFlow {
    /// Creates a KVToKV transformation flow from input/output expressions and constraints.
    ///
    /// This constructor builds a transformation that processes a single input relation,
    /// applying filters, projections, and potentially reordering the output key-value layout.
    ///
    /// # Arguments
    ///
    /// * `input_kv_layout` - The key-value layout of the input relation
    /// * `output_kv_layout` - The desired key-value layout for the output  
    /// * `const_eq_constraints` - Constant equality filters (e.g., `x = 42`)
    /// * `var_eq_constraints` - Variable equality filters (e.g., `x = y`)
    /// * `compare_exprs` - Comparison filters (e.g., `x < y`, `x >= 10`)
    ///
    /// # Returns
    ///
    /// A new `KVToKV` transformation flow that can transform input data according
    /// to the specified layout and constraints.
    pub(crate) fn kv_to_kv(
        input_kv_layout: &KeyValueLayout,
        output_kv_layout: &KeyValueLayout,
        predicates: &KvPredicates,
    ) -> Self {
        // Map input expressions to transformation arguments
        let input_expr_map = Self::kv_argument_flow_map(input_kv_layout);

        // Generate output key/value arguments
        let flow_key_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.key());
        let flow_value_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.value());

        // Process constant and variable equality constraints via helpers
        let flow_const_args =
            Self::build_const_eq_constraints(&input_expr_map, &predicates.const_eq);
        let flow_var_eq_args = Self::build_var_eq_constraints(&input_expr_map, &predicates.var_eq);

        // Process comparison constraints
        let flow_compares =
            Self::build_compare_arguments(&input_expr_map, &predicates.compare_exprs);

        // Process fn_call predicate constraints
        let flow_fn_call_preds =
            Self::build_fn_call_arguments(&input_expr_map, &predicates.fn_call_preds);

        Self::KVToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            constraints: Constraints::new(flow_const_args, flow_var_eq_args),
            compares: flow_compares,
            fn_call_preds: flow_fn_call_preds,
        }
    }

    /// Creates a JnToKV transformation flow from input/output expressions and join conditions.
    ///
    /// This constructor builds a transformation that combines two input relations through
    /// a join operation, producing a single output relation with the specified layout.
    ///
    /// # Arguments
    ///
    /// * `input_left_kv_layout` - The key-value layout of the left input relation
    /// * `input_right_kv_layout` - The key-value layout of the right input relation
    /// * `output_kv_layout` - The desired key-value layout for the joined output
    /// * `compare_exprs` - Comparison filters to apply during the join
    ///
    /// # Returns
    ///
    /// A new `JnToKV` transformation flow that can join the two input relations
    /// according to their shared keys and produce the specified output layout.
    pub(crate) fn join_to_kv(
        input_left_kv_layout: &KeyValueLayout,
        input_right_kv_layout: &KeyValueLayout,
        output_kv_layout: &KeyValueLayout,
        predicates: &JoinPredicates,
    ) -> Self {
        // Map input expressions to transformation arguments
        let input_expr_map =
            Self::jn_argument_flow_map(input_left_kv_layout, input_right_kv_layout);

        // Generate output key/value arguments
        let flow_key_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.key());
        let flow_value_args = Self::flow_over_exprs(&input_expr_map, output_kv_layout.value());

        // Process comparison constraints
        let flow_compares =
            Self::build_compare_arguments(&input_expr_map, &predicates.compare_exprs);

        // Process fn_call predicate constraints
        let flow_fn_call_preds =
            Self::build_fn_call_arguments(&input_expr_map, &predicates.fn_call_preds);

        Self::JnToKV {
            key: Arc::new(flow_key_args),
            value: Arc::new(flow_value_args),
            compares: flow_compares,
            fn_call_preds: flow_fn_call_preds,
        }
    }
}

// ========================
// Getters
// ========================
impl TransformationFlow {
    /// Returns the output key expressions.
    pub(crate) fn key(&self) -> &Arc<Vec<ArithmeticArgument>> {
        match self {
            Self::KVToKV { key, .. } => key,
            Self::JnToKV { key, .. } => key,
        }
    }

    /// Returns the output value expressions.
    pub(crate) fn value(&self) -> &Arc<Vec<ArithmeticArgument>> {
        match self {
            Self::KVToKV { value, .. } => value,
            Self::JnToKV { value, .. } => value,
        }
    }

    /// Returns the constraints for flows that support them.
    ///
    /// # Panics
    ///
    /// Panics if called on a `JnToKV` flow, which doesn't support constraints.
    pub(crate) fn constraints(&self) -> &Constraints {
        match self {
            Self::KVToKV { constraints, .. } => constraints,
            Self::JnToKV { .. } => {
                panic!("Planner error: TransformationFlow::constraints is not supported for JnToKV")
            }
        }
    }

    /// Returns the comparison filters for flows that support them.
    pub(crate) fn compares(&self) -> &Vec<ComparisonExprArgument> {
        match self {
            Self::KVToKV { compares, .. } => compares,
            Self::JnToKV { compares, .. } => compares,
        }
    }

    /// Returns the boolean UDF predicate filters.
    pub(crate) fn fn_call_preds(&self) -> &Vec<FnCallPredicateArgument> {
        match self {
            Self::KVToKV { fn_call_preds, .. } => fn_call_preds,
            Self::JnToKV { fn_call_preds, .. } => fn_call_preds,
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
            .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((true, id))))
            .chain(
                kv_layout
                    .value()
                    .iter()
                    .enumerate()
                    .map(|(id, expr)| (expr.clone(), TransformationArgument::KV((false, id)))),
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
            .map(|(id, expr)| (expr.clone(), TransformationArgument::Jn((true, true, id))))
            .chain(
                jn_left_layout.value().iter().enumerate().map(|(id, expr)| {
                    (expr.clone(), TransformationArgument::Jn((true, false, id)))
                }),
            )
            .chain(
                jn_right_layout.key().iter().enumerate().map(|(id, expr)| {
                    (expr.clone(), TransformationArgument::Jn((false, true, id)))
                }),
            )
            .chain(
                jn_right_layout
                    .value()
                    .iter()
                    .enumerate()
                    .map(|(id, expr)| {
                        (expr.clone(), TransformationArgument::Jn((false, false, id)))
                    }),
            )
            .collect()
    }

    /// Composes output arithmetic expressions from available input chunks.
    ///
    /// This method is the core expression transformation logic. It attempts to map
    /// output expressions to input transformation arguments, handling both direct
    /// expression lookup and factor-by-factor construction when needed.
    ///
    /// # Arguments
    ///
    /// * `input_exprs_map` - Map from input arithmetic positions to transformation arguments
    /// * `output_exprs` - List of output expressions to be constructed
    ///
    /// # Returns
    ///
    /// Vector of `ArithmeticArgument`s representing the transformed expressions
    ///
    /// # Panics
    ///
    /// Panics if a required variable signature is not found in the input expression map,
    /// indicating an inconsistency in the transformation setup.
    fn flow_over_exprs(
        input_exprs_map: &HashMap<ArithmeticPos, TransformationArgument>,
        output_exprs: &[ArithmeticPos],
    ) -> Vec<ArithmeticArgument> {
        trace!(
            "flow_over_exprs: input_exprs_map = {:?}, output_exprs = {:?}",
            input_exprs_map, output_exprs
        );

        output_exprs
            .iter()
            .map(|expr| {
                // Fast path: the entire expression matches an input directly,
                // so emit a single variable reference.
                if let Some(&trans_arg) = input_exprs_map.get(expr) {
                    return ArithmeticArgument {
                        init: FactorArgument::Var(trans_arg),
                        rest: vec![],
                    };
                }

                // Otherwise rebuild the expression factor by factor. Both
                // call-style arms (FnCall, Builtin) re-resolve each
                // sub-expression's variable signatures against the input
                // expression map, so we share the walk.
                let lower_call_args = |args: &[ArithmeticPos]| -> Vec<ArithmeticArgument> {
                    args.iter()
                        .map(|a| {
                            let var_args: Vec<_> = a
                                .signatures()
                                .iter()
                                .map(|sig| {
                                    let key = ArithmeticPos::from_var_signature(**sig);
                                    *input_exprs_map.get(&key).unwrap_or_else(|| {
                                        panic!(
                                            "Planner error: missing call-arg signature {:?}",
                                            sig
                                        )
                                    })
                                })
                                .collect();
                            ArithmeticArgument::from_arithmeticpos(a, &var_args)
                        })
                        .collect()
                };
                let resolve_factor = |factor: &FactorPos| -> FactorArgument {
                    match factor {
                        FactorPos::Var(sig) => {
                            let key = ArithmeticPos::from_var_signature(*sig);
                            let trans_arg = input_exprs_map.get(&key).copied().unwrap_or_else(|| {
                                panic!(
                                    "Planner error: missing variable signature {:?} in input expression map",
                                    sig
                                )
                            });
                            FactorArgument::Var(trans_arg)
                        }
                        FactorPos::Const(c) => FactorArgument::Const(c.clone()),
                        FactorPos::FnCall { name, args } => FactorArgument::FnCall {
                            name: name.clone(),
                            args: lower_call_args(args),
                        },
                        FactorPos::Builtin { op, args } => FactorArgument::Builtin {
                            op: *op,
                            args: lower_call_args(args),
                        },
                        FactorPos::Group(a) => {
                            // Reuse the call-arg lowering: a grouped expression
                            // resolves its inner signatures the same way.
                            let inner = lower_call_args(std::slice::from_ref(a))
                                .pop()
                                .expect("group lowering yields exactly one arithmetic");
                            FactorArgument::Group(Box::new(inner))
                        }
                        FactorPos::Tuple { fields } => FactorArgument::Tuple {
                            fields: lower_call_args(fields),
                        },
                        FactorPos::TupleProj { tuple, index } => {
                            let inner = lower_call_args(std::slice::from_ref(tuple))
                                .pop()
                                .expect("proj lowering yields exactly one arithmetic");
                            FactorArgument::TupleProj {
                                tuple: Box::new(inner),
                                index: *index,
                            }
                        }
                    }
                };

                let init = resolve_factor(expr.init());
                let rest = expr
                    .rest()
                    .iter()
                    .map(|(op, factor)| (op.clone(), resolve_factor(factor)))
                    .collect();
                ArithmeticArgument { init, rest }
            })
            .collect()
    }

    /// Resolves a sequence of variable signatures into the corresponding
    /// `TransformationArgument`s by routing each through `flow_over_exprs`
    /// and the arithmetic→transformation conversion.
    fn signatures_to_trans_args<'a, I>(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        signatures: I,
    ) -> Vec<TransformationArgument>
    where
        I: IntoIterator<Item = &'a AtomArgumentSignature>,
    {
        let exprs: Vec<ArithmeticPos> = signatures
            .into_iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();
        TransformationArgument::from_arithmetic_arguments(Self::flow_over_exprs(
            input_expr_map,
            &exprs,
        ))
    }

    /// Helper to construct constant equality constraints: (var = const)
    fn build_const_eq_constraints(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
    ) -> Vec<(TransformationArgument, ConstType)> {
        let trans_args = Self::signatures_to_trans_args(
            input_expr_map,
            const_eq_constraints.iter().map(|(sig, _)| sig),
        );
        trans_args
            .into_iter()
            .zip(const_eq_constraints.iter().map(|(_, c)| c.clone()))
            .collect()
    }

    /// Helper to construct variable equality constraints: (var = var)
    fn build_var_eq_constraints(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
    ) -> Vec<(TransformationArgument, TransformationArgument)> {
        let left_args = Self::signatures_to_trans_args(
            input_expr_map,
            var_eq_constraints.iter().map(|(left, _)| left),
        );
        let right_args = Self::signatures_to_trans_args(
            input_expr_map,
            var_eq_constraints.iter().map(|(_, right)| right),
        );
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
                let left_args =
                    Self::signatures_to_trans_args(input_expr_map, comp.left().signatures());
                let right_args =
                    Self::signatures_to_trans_args(input_expr_map, comp.right().signatures());
                ComparisonExprArgument::from_comparison_expr(comp, &left_args, &right_args)
            })
            .collect()
    }

    /// Helper to construct fn_call predicate arguments from input expression map and fn_call positions.
    fn build_fn_call_arguments(
        input_expr_map: &HashMap<ArithmeticPos, TransformationArgument>,
        fn_call_preds: &[FnCallPredicatePos],
    ) -> Vec<FnCallPredicateArgument> {
        fn_call_preds
            .iter()
            .map(|fc| {
                let per_arg_trans: Vec<Vec<TransformationArgument>> = fc
                    .args()
                    .iter()
                    .map(|arg_pos| {
                        Self::signatures_to_trans_args(input_expr_map, arg_pos.signatures())
                    })
                    .collect();
                FnCallPredicateArgument::from_fn_call_pos(fc, &per_arg_trans)
            })
            .collect()
    }
}

impl fmt::Display for TransformationFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Comma-joined `to_string` of each item, used for K:(...), V:(...) and
        // for the inner pieces of F:(...).
        fn join_display<T: fmt::Display>(items: &[T]) -> String {
            items
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        }

        // Render the standard `K:(...), V:(...), F:(if ... and ...)` triple,
        // omitting any section whose source is empty.
        fn format_kv_parts(
            key: &[ArithmeticArgument],
            value: &[ArithmeticArgument],
            filter_parts: &[String],
        ) -> String {
            let mut parts: Vec<String> = Vec::new();
            if !key.is_empty() {
                parts.push(format!("K:({})", join_display(key)));
            }
            if !value.is_empty() {
                parts.push(format!("V:({})", join_display(value)));
            }
            if !filter_parts.is_empty() {
                parts.push(format!("F:(if {})", filter_parts.join(" and ")));
            }
            parts.join(", ")
        }

        match self {
            Self::KVToKV {
                key,
                value,
                constraints,
                compares,
                fn_call_preds,
            } => {
                let mut filter_parts: Vec<String> = Vec::new();
                if !constraints.is_empty() {
                    filter_parts.push(constraints.to_string());
                }
                if !compares.is_empty() {
                    filter_parts.push(join_display(compares));
                }
                if !fn_call_preds.is_empty() {
                    filter_parts.push(join_display(fn_call_preds));
                }
                write!(f, "{}", format_kv_parts(key, value, &filter_parts))
            }

            Self::JnToKV {
                key,
                value,
                compares,
                fn_call_preds,
            } => {
                let mut filter_parts: Vec<String> = Vec::new();
                if !compares.is_empty() {
                    filter_parts.push(join_display(compares));
                }
                if !fn_call_preds.is_empty() {
                    filter_parts.push(join_display(fn_call_preds));
                }
                write!(f, "{}", format_kv_parts(key, value, &filter_parts))
            }
        }
    }
}
