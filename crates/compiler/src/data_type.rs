//! Type inference system for FlowLog compiler.
//!
//! This module builds and maintains a global mapping from relation fingerprints
//! to their key and value data types. It also provides utilities to infer the
//! data types of expressions used in transformations, and to generate Rust type
//! tokens corresponding to FlowLog data types.

use super::Compiler;
use parser::{AggregationOperator, ArithmeticOperator, ConstType, DataType};
use planner::{
    ArithmeticArgument, FactorArgument, StratumPlanner, TransformationArgument, TransformationFlow,
};
use proc_macro2::TokenStream;
use quote::quote;

// ============================================================================
// Global type inference (fingerprint -> (key_types, value_types))
// ============================================================================
impl Compiler {
    /// Build the global fingerprint-to-type map for *all* relations (EDBs + IDBs).
    ///
    /// "Global" means the types are stable across strata. (Local recursion strata
    /// may introduce new identifiers, but those refer back to these global types.)
    pub(super) fn make_global_type_map(&mut self) {
        self.global_fp_to_type = self
            .program
            .relations()
            .iter()
            .map(|rel| (rel.fingerprint(), (Vec::new(), rel.data_type())))
            .collect();
    }

    /// Verify (or infer+insert) the global type mapping for a transformation output.
    ///
    /// - `left_fingerprint` is always present.
    /// - `right_fingerprint` is present only for binary transformations (e.g., join, njoin).
    /// - `output_fingerprint` is the fingerprint of the produced collection.
    pub(super) fn verify_and_infer_global_type(
        &mut self,
        left_fingerprint: u64,
        right_fingerprint: Option<u64>,
        output_fingerprint: u64,
        flow: &TransformationFlow,
        stratum: &StratumPlanner,
    ) {
        let head_to_idb_map = stratum.head_to_idb_map();
        let idb_to_aggregation_map = stratum.idb_to_aggregation_map();

        let left_type = self.find_global_type(left_fingerprint);
        let right_type = right_fingerprint.map(|rf| self.find_global_type(rf));

        self.assert_join_key_type_compat(
            left_fingerprint,
            left_type,
            right_fingerprint,
            right_type,
        );

        let key_types: Vec<Option<DataType>> = flow
            .key()
            .iter()
            .map(|expr| self.infer_expr_type(expr, left_type, right_type))
            .collect();

        let value_types: Vec<Option<DataType>> = flow
            .value()
            .iter()
            .map(|expr| self.infer_expr_type(expr, left_type, right_type))
            .collect();

        // If this head feeds into a declared IDB, resolve to the IDB fingerprint
        // so we verify/insert against the IDB's declared types directly.
        let resolved_idb_fp = head_to_idb_map.get(&output_fingerprint).copied();
        let output_fingerprint = resolved_idb_fp.unwrap_or(output_fingerprint);

        // If the resolved IDB has an aggregation, validate the aggregation
        // position according to the operator's type contract instead of requiring
        // an exact match between the pre-aggregation expression type and the
        // post-aggregation declared type.
        let agg_info = idb_to_aggregation_map.get(&output_fingerprint);

        self.global_fp_to_type
            .entry(output_fingerprint)
            .and_modify(|existing| {
                let key_len = existing.0.len();

                // Check whether position `flat_i` (in key++value space) satisfies
                // the aggregation operator's type contract.
                let agg_type_ok = |flat_i: usize, decl: &DataType, inf: &DataType| -> bool {
                    let Some((op, pos, _)) = agg_info else {
                        return false;
                    };
                    if flat_i != *pos {
                        return false;
                    }
                    match op {
                        // count(T) → any input type; output must be integer
                        AggregationOperator::Count => decl.is_integer(),
                        // sum(T), avg(T), min(T), max(T) → input and output must
                        // match and both be numeric
                        AggregationOperator::Sum
                        | AggregationOperator::Avg
                        | AggregationOperator::Min
                        | AggregationOperator::Max => inf == decl && decl.is_numeric(),
                    }
                };

                let types_match =
                    |declared: &[DataType], inferred: &[Option<DataType>], offset: usize| {
                        declared.len() == inferred.len()
                            && declared.iter().zip(inferred).enumerate().all(
                                |(i, (decl, inf))| {
                                    inf.as_ref()
                                        .is_none_or(|t| t == decl || agg_type_ok(offset + i, decl, t))
                                },
                            )
                    };
                assert!(
                    types_match(&existing.0, &key_types, 0)
                        && types_match(&existing.1, &value_types, key_len),
                    "Compiler error: type mismatch for fingerprint 0x{:016x}",
                    output_fingerprint
                );
            })
            .or_insert_with(|| {
                let keys = key_types
                    .into_iter()
                    .map(|t| t.expect("Compiler error: cannot infer type for all-constant position for intermediate relation without IDB declaration"))
                    .collect();
                let vals = value_types
                    .into_iter()
                    .map(|t| t.expect("Compiler error: cannot infer type for all-constant position for intermediate relation without IDB declaration"))
                    .collect();
                (keys, vals)
            });
    }

    /// Assert that in a join operator the *key* types agree between inputs.
    fn assert_join_key_type_compat(
        &self,
        left_fp: u64,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_fp: Option<u64>,
        right_type: Option<&(Vec<DataType>, Vec<DataType>)>,
    ) {
        if let (Some(rf), Some(rt)) = (right_fp, right_type) {
            assert!(
                left_type.0 == rt.0,
                "Compiler error: key type mismatch for fingerprints 0x{:016x} and 0x{:016x}",
                left_fp,
                rf
            );
        }
    }

    /// Infer the `DataType` of a single factor.
    fn infer_factor_type(
        &self,
        factor: &FactorArgument,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_type: Option<&(Vec<DataType>, Vec<DataType>)>,
    ) -> Option<DataType> {
        match factor {
            // KV variables always refer to the left input's key/value slots.
            FactorArgument::Var(TransformationArgument::KV((is_key, idx))) => {
                let src = if *is_key { &left_type.0 } else { &left_type.1 };
                src.get(*idx).cloned()
            }
            // Join variables can reference either side; right side must exist.
            FactorArgument::Var(TransformationArgument::Jn((is_left, is_key, idx))) => {
                let base = if *is_left {
                    left_type
                } else {
                    right_type
                        .expect("Compiler error: right input type missing for join transformation")
                };
                let src = if *is_key { &base.0 } else { &base.1 };
                src.get(*idx).cloned()
            }
            FactorArgument::Const(ConstType::Int(_)) => None,
            FactorArgument::Const(ConstType::Float(_)) => None,
            FactorArgument::Const(ConstType::Text(_)) => Some(DataType::String),
            FactorArgument::Const(ConstType::Bool(_)) => Some(DataType::Bool),
            FactorArgument::FnCall { name, args } => {
                let ext = self
                    .program
                    .udfs()
                    .iter()
                    .find(|e| e.name() == name)
                    .unwrap_or_else(|| panic!("Compiler error: UDF '{}' not declared", name));

                // 1. Assert argument count.
                assert_eq!(
                    args.len(),
                    ext.params().len(),
                    "Compiler error: UDF '{}' expects {} args but got {}",
                    name,
                    ext.params().len(),
                    args.len(),
                );

                // 2. Assert each argument type matches the parameter type.
                for (i, (arg, param)) in args.iter().zip(ext.params()).enumerate() {
                    let arg_type = self.infer_expr_type(arg, left_type, right_type);
                    match arg_type {
                        Some(at) => assert_eq!(
                            at,
                            *param.data_type(),
                            "Compiler error: UDF '{}' param {} ('{}') expects {:?} but got {:?}",
                            name, i, param.name(), param.data_type(), at,
                        ),
                        None => {
                            // Numeric constants infer as None (polymorphic).
                            // Still reject if the param expects a non-numeric type.
                            assert!(
                                param.data_type().is_numeric(),
                                "Compiler error: UDF '{}' param {} ('{}') expects {:?} but got a numeric literal",
                                name, i, param.name(), param.data_type(),
                            );
                        }
                    }
                }

                // 3. Return type.
                Some(ext.ret_type())
            }
        }
    }

    /// Infer the `DataType` of a single arithmetic expression.
    ///
    /// All factors (init + rest) must agree on the same data type.
    pub(super) fn infer_expr_type(
        &self,
        expr: &ArithmeticArgument,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_type: Option<&(Vec<DataType>, Vec<DataType>)>,
    ) -> Option<DataType> {
        let mut inferred: Option<DataType> = None;

        // Init factor.
        if let Some(dt) = self.infer_factor_type(&expr.init, left_type, right_type) {
            inferred = Some(dt);
        }

        // Remaining factors must match.
        for (op, factor) in &expr.rest {
            if let Some(dt) = self.infer_factor_type(factor, left_type, right_type) {
                match inferred {
                    None => inferred = Some(dt),
                    Some(existing) => assert!(
                        existing == dt,
                        "Compiler error: mixed data types in arithmetic expression: {:?} vs {:?}",
                        existing,
                        dt
                    ),
                }
            }

            // Validate operator-type compatibility.
            if let Some(dt) = inferred {
                if dt == DataType::Bool {
                    panic!("Compiler error: arithmetic operations are not supported on Bool type");
                } else if dt == DataType::String {
                    assert!(
                        matches!(op, ArithmeticOperator::Cat),
                        "Compiler error: arithmetic operator {:?} is not allowed on string type, use 'cat'",
                        op
                    );
                } else {
                    assert!(
                        !matches!(op, ArithmeticOperator::Cat),
                        "Compiler error: 'cat' operator is not allowed on numeric type {:?}",
                        dt
                    );
                }
            }
        }

        inferred
    }

    /// Look up the `(key_types, value_types)` for a fingerprint.
    pub(crate) fn find_global_type(&self, fingerprint: u64) -> &(Vec<DataType>, Vec<DataType>) {
        self.global_fp_to_type.get(&fingerprint).unwrap_or_else(|| {
            panic!(
                "Compiler error: input type missing for fingerprint 0x{:016x}",
                fingerprint
            )
        })
    }
}

// ============================================================================
// Token helpers: DataType -> Rust type tokens
// ============================================================================

/// Construct the Rust type token for a row with the given input types:
/// - `[]` => `()`
/// - `[T]` => `(T,)`
/// - `[T1, T2, ...]` => `(T1, T2, ...)`
///
/// When `string_intern` is true, `DataType::String` maps to `Spur`;
/// otherwise it maps to `String`.
pub(super) fn type_tokens(input_types: &[DataType], string_intern: bool) -> TokenStream {
    let tys: Vec<TokenStream> = input_types
        .iter()
        .map(|dt| match dt {
            DataType::Int8 => quote! { i8 },
            DataType::Int16 => quote! { i16 },
            DataType::Int32 => quote! { i32 },
            DataType::Int64 => quote! { i64 },
            DataType::UInt8 => quote! { u8 },
            DataType::UInt16 => quote! { u16 },
            DataType::UInt32 => quote! { u32 },
            DataType::UInt64 => quote! { u64 },
            DataType::Float32 => quote! { OrderedFloat<f32> },
            DataType::Float64 => quote! { OrderedFloat<f64> },
            DataType::String => {
                if string_intern {
                    quote! { Spur }
                } else {
                    quote! { String }
                }
            }
            DataType::Bool => quote! { bool },
        })
        .collect();

    match tys.len() {
        0 => quote! { () },
        1 => {
            let t0 = &tys[0];
            quote! { ( #t0, ) }
        }
        _ => quote! { ( #(#tys),* ) },
    }
}
