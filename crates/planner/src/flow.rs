use crate::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument, TransformationArgument,
};
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos, FactorPos};
use parser::{AggregationOperator, ConstType};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Represents data transformation flows in query execution.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub enum TransformationFlow {
    /// Single relation transformations: filtering, projection.
    /// Example: `((x), y) -> ((), x + 1, y * 2)` with filter `x > 0`
    KVToKV {
        /// Output key expressions (e.g., `[x + 1]`)
        key: Arc<Vec<ArithmeticArgument>>,
        /// Output value expressions (e.g., `[y * 2, x]`)
        value: Arc<Vec<ArithmeticArgument>>,
        /// Equality constraints (e.g., `x + 1 >= 5`)
        constraints: Constraints,
        /// Comparison filters (e.g., `x + 2 > y`)
        compares: Vec<ComparisonExprArgument>,
    },

    /// Join operations between two relations.
    /// Example: `((x), y) + ((x), z) -> ((), x, y, z)`
    JnToKV {
        /// Output key expressions
        key: Arc<Vec<ArithmeticArgument>>,
        /// Output value expressions
        value: Arc<Vec<ArithmeticArgument>>,
        /// Join filters
        compares: Vec<ComparisonExprArgument>,
    },

    /// Aggregation operations on a relation.
    /// Example: `(x, y) -> (x, SUM(y))`
    /// Notice:
    /// 1. we only consider argument level aggregation, any
    ///    arithmetic inside aggregation (e.g., SUM(x + 1)) should
    ///    be handled in the KVToKV transformation before the aggregation.
    /// 2. we assume the input and output of this operator are both rows.
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

    /// Creates a mapping from arithmetic position expression to transformation arguments for operator flows.
    fn kv_argument_flow_map(
        key_signatures: &[ArithmeticPos],
        value_signatures: &[ArithmeticPos],
    ) -> HashMap<ArithmeticPos, TransformationArgument> {
        key_signatures
            .iter()
            .enumerate()
            .map(|(id, signature)| (signature.clone(), TransformationArgument::KV((false, id))))
            .chain(
                value_signatures.iter().enumerate().map(|(id, signature)| {
                    (signature.clone(), TransformationArgument::KV((true, id)))
                }),
            )
            .collect()
    }

    /// Compose output arithmetic expressions from available input chunks.
    ///
    /// Given a map of available arithmetic positions to their transformation arguments,
    /// build each requested output arithmetic by concatenating existing chunks.
    /// Panics with context if no valid decomposition exists.
    fn flow_over_signatures(
        input_signature_map: &HashMap<ArithmeticPos, TransformationArgument>,
        output_signatures: &[ArithmeticPos],
        context: &str,
    ) -> Vec<ArithmeticArgument> {
        output_signatures
            .iter()
            .map(|target| {
                Self::compose_from_chunks(target, input_signature_map).unwrap_or_else(|e| {
                    panic!(
                        "Planner error: {}, failed to compose {:?} from inputs: {} ({} inputs)",
                        context,
                        target,
                        e,
                        input_signature_map.len()
                    )
                })
            })
            .collect()
    }

    /// Compose a target arithmetic from available exact chunks.
    ///
    /// Simplified: Just look up the target directly in the input map.
    fn compose_from_chunks(
        target: &ArithmeticPos,
        input_map: &HashMap<ArithmeticPos, TransformationArgument>,
    ) -> Result<ArithmeticArgument, String> {
        // Direct lookup - if target exists in input map, use it directly
        if let Some(arg) = input_map.get(target) {
            let init = FactorArgument::Var(*arg);
            let rest = Vec::new(); // Single chunk, no additional parts
            return Ok(ArithmeticArgument { init, rest });
        }

        Err("no exact match found in input map".to_string())
    }

    /// Build a map from single-variable inputs to their transformation arguments.
    fn build_var_map(
        input_signature_map: &HashMap<ArithmeticPos, TransformationArgument>,
    ) -> HashMap<AtomArgumentSignature, TransformationArgument> {
        input_signature_map
            .iter()
            .filter_map(|(arith, arg)| {
                if arith.is_var() {
                    if let FactorPos::Var(sig) = arith.init() {
                        Some((*sig, *arg))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
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
        let input_signature_map = Self::kv_argument_flow_map(input_key_exprs, input_value_exprs);

        let flow_key_signatures = Self::flow_over_signatures(
            &input_signature_map,
            output_key_exprs,
            "TransformationFlow::kv_to_kv key",
        );
        let flow_value_signatures = Self::flow_over_signatures(
            &input_signature_map,
            output_value_exprs,
            "TransformationFlow::kv_to_kv value",
        );

        // Build var map from available single-variable inputs
        let var_map = Self::build_var_map(&input_signature_map);

        // Process constant constraints: var equals const
        let flow_const_signatures = const_eq_constraints
            .iter()
            .map(|(sig, c)| {
                let arg = *var_map.get(sig).unwrap_or_else(|| {
                    panic!(
                        "Planner error: TransformationFlow::kv_to_kv const - variable signature {} not found in inputs",
                        sig
                    )
                });
                (arg, c.clone())
            })
            .collect();

        // Process variable equality constraints: var equals var
        let flow_var_eq_signatures = var_eq_constraints
            .iter()
            .map(|(l, r)| {
                let la = *var_map.get(l).unwrap_or_else(|| {
                    panic!(
                        "Planner error: TransformationFlow::kv_to_kv var left - variable signature {} not found in inputs",
                        l
                    )
                });
                let ra = *var_map.get(r).unwrap_or_else(|| {
                    panic!(
                        "Planner error: TransformationFlow::kv_to_kv var right - variable signature {} not found in inputs",
                        r
                    )
                });
                (la, ra)
            })
            .collect();

        // Process comparison constraints
        let flow_compare_signatures = compare_exprs
            .iter()
            .map(|comp| {
                let left_arg = Self::compose_from_chunks(comp.left(), &input_signature_map)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Planner error: TransformationFlow::kv_to_kv compare left - failed to compose {:?}: {}",
                            comp.left(), e
                        )
                    });
                let right_arg = Self::compose_from_chunks(comp.right(), &input_signature_map)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Planner error: TransformationFlow::kv_to_kv compare right - failed to compose {:?}: {}",
                            comp.right(), e
                        )
                    });

                ComparisonExprArgument::new(comp.operator().clone(), left_arg, right_arg)
            })
            .collect();

        Self::KVToKV {
            key: Arc::new(flow_key_signatures),
            value: Arc::new(flow_value_signatures),
            constraints: Constraints::new(flow_const_signatures, flow_var_eq_signatures),
            compares: flow_compare_signatures,
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
        let left_signature_map =
            Self::kv_argument_flow_map(input_left_key_exprs, input_left_value_exprs)
                .into_iter()
                .map(|(signature, trace)| {
                    let join_trace = match trace {
                        TransformationArgument::KV((key_or_value, id)) => {
                            TransformationArgument::Jn((false, key_or_value, id))
                        }
                        _ => panic!(
                            "Planner error: TransformationFlow::Jn expects kv in left input: {:?}",
                            trace
                        ),
                    };
                    (signature, join_trace)
                });

        // Map right-side inputs to join transformation arguments (only values, no keys)
        let right_signature_map = Self::kv_argument_flow_map(&[], input_right_value_exprs)
            .into_iter()
            .map(|(signature, trace)| {
                let join_trace = match trace {
                    TransformationArgument::KV((key_or_value, id)) => {
                        TransformationArgument::Jn((true, key_or_value, id))
                    }
                    _ => panic!(
                        "Planner error: TransformationFlow::join_to_kv expects kv in right input: {:?}",
                        trace
                    ),
                };
                (signature, join_trace)
            });

        // Combine left and right signature maps
        let input_signature_map = left_signature_map.chain(right_signature_map).collect();

        let flow_key_signatures = Self::flow_over_signatures(
            &input_signature_map,
            output_key_exprs,
            "TransformationFlow::join_to_kv key",
        );
        let flow_value_signatures = Self::flow_over_signatures(
            &input_signature_map,
            output_value_exprs,
            "TransformationFlow::join_to_kv value",
        );

        // Process comparison constraints
        let flow_compare_signatures = compare_exprs
            .iter()
            .map(|comp| {
                let left_arg = Self::compose_from_chunks(comp.left(), &input_signature_map)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Planner error: TransformationFlow::join_to_kv compare left - failed to compose {:?}: {}",
                            comp.left(), e
                        )
                    });
                let right_arg = Self::compose_from_chunks(comp.right(), &input_signature_map)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Planner error: TransformationFlow::join_to_kv compare right - failed to compose {:?}: {}",
                            comp.right(), e
                        )
                    });

                ComparisonExprArgument::new(comp.operator().clone(), left_arg, right_arg)
            })
            .collect();

        Self::JnToKV {
            key: Arc::new(flow_key_signatures),
            value: Arc::new(flow_value_signatures),
            compares: flow_compare_signatures,
        }
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
