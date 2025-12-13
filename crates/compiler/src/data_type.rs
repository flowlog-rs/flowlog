//! Type inference system for FlowLog compiler.
//!
//! This module builds and maintains a global mapping from relation fingerprints
//! to their key and value data types. It also provides utilities to infer the
//! data types of expressions used in transformations, and to generate Rust type
//! tokens corresponding to FlowLog data types.

use proc_macro2::TokenStream;
use quote::quote;

use super::Compiler;
use parser::{ConstType, DataType};
use planner::{ArithmeticArgument, FactorArgument, TransformationArgument, TransformationFlow};

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
            .edbs()
            .into_iter()
            .chain(self.program.idbs())
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
    ) {
        let left_type = self.find_global_type(left_fingerprint);
        let right_type = right_fingerprint.map(|rf| self.find_global_type(rf));

        self.assert_join_key_type_compat(
            left_fingerprint,
            left_type,
            right_fingerprint,
            right_type,
        );

        let key_types: Vec<DataType> = flow
            .key()
            .iter()
            .map(|expr| Self::infer_expr_type(expr, left_type, right_type))
            .collect();

        let value_types: Vec<DataType> = flow
            .value()
            .iter()
            .map(|expr| Self::infer_expr_type(expr, left_type, right_type))
            .collect();

        self.global_fp_to_type
            .entry(output_fingerprint)
            .and_modify(|existing| {
                assert!(
                    existing.0 == key_types && existing.1 == value_types,
                    "Compiler error: type mismatch for fingerprint 0x{:016x}",
                    output_fingerprint
                );
            })
            .or_insert((key_types, value_types));
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

    /// Infer the `DataType` of a single arithmetic expression.
    ///
    /// All factors (init + rest) must agree on the same data type.
    fn infer_expr_type(
        expr: &ArithmeticArgument,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_type: Option<&(Vec<DataType>, Vec<DataType>)>,
    ) -> DataType {
        fn type_of_factor(
            factor: &FactorArgument,
            left_type: &(Vec<DataType>, Vec<DataType>),
            right_type: Option<&(Vec<DataType>, Vec<DataType>)>,
        ) -> Option<DataType> {
            match factor {
                // KV variables always refer to the left input’s key/value slots.
                FactorArgument::Var(TransformationArgument::KV((is_key, idx))) => {
                    let src = if *is_key { &left_type.0 } else { &left_type.1 };
                    src.get(*idx).cloned()
                }

                // Join variables can reference either side; right side must exist.
                FactorArgument::Var(TransformationArgument::Jn((is_left, is_key, idx))) => {
                    let base = if *is_left {
                        left_type
                    } else {
                        right_type.expect(
                            "Compiler error: right input type missing for join transformation",
                        )
                    };
                    let src = if *is_key { &base.0 } else { &base.1 };
                    src.get(*idx).cloned()
                }

                // Consts are fully typed.
                FactorArgument::Const(ConstType::Integer(_)) => Some(DataType::Integer),
                FactorArgument::Const(ConstType::Text(_)) => Some(DataType::String),
            }
        }

        let mut inferred: Option<DataType> = None;

        // Init factor.
        if let Some(dt) = type_of_factor(&expr.init, left_type, right_type) {
            inferred = Some(dt);
        }

        // Remaining factors must match.
        for (_op, factor) in &expr.rest {
            if let Some(dt) = type_of_factor(factor, left_type, right_type) {
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
        }

        inferred.expect(
            "Compiler error: unable to infer data type for arithmetic expression (no factors)",
        )
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
pub(super) fn type_tokens(input_types: &[DataType]) -> TokenStream {
    let tys: Vec<TokenStream> = input_types
        .iter()
        .map(|dt| match dt {
            DataType::Integer => quote! { i32 },
            DataType::String => quote! { String },
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
