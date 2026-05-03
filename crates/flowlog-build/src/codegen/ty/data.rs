//! Intermediate-fingerprint type registry + Rust-type-token emission.
//!
//! Type *checking* lives in the [`typechecker`] crate; this module only
//! (a) seeds declared types from `.decl`s, (b) propagates them through
//! planner transformations to the intermediate fingerprints codegen
//! emits, and (c) emits the Rust tuple types for both the internal
//! (DD-facing) and user-facing shapes.

use proc_macro2::TokenStream;
use quote::quote;

use crate::parser::DataType;
use crate::planner::{
    ArithmeticArgument, FactorArgument, StratumPlanner, TransformationArgument, TransformationFlow,
};

use crate::codegen::{CodeGen, CodegenError};

/// `(key_types, value_types)` — a relation's shape in key++value form.
pub(crate) type KvTypes = (Vec<DataType>, Vec<DataType>);

// ==================================================
// Fingerprint → KvTypes registry
// ==================================================

impl CodeGen {
    /// Seed the registry from every declared relation.
    pub(crate) fn make_global_data_type_map(&mut self) {
        self.global_fp_to_type = self
            .program
            .relations()
            .iter()
            .map(|rel| (rel.fingerprint(), (Vec::new(), rel.data_type())))
            .collect();
    }

    /// A missing fingerprint is a planner/codegen contract violation.
    pub(crate) fn find_global_data_type(&self, fingerprint: u64) -> Result<&KvTypes, CodegenError> {
        self.global_fp_to_type.get(&fingerprint).ok_or_else(|| {
            CodegenError::internal(format!(
                "input type missing for fingerprint 0x{fingerprint:016x}"
            ))
        })
    }

    /// Column type at `agg_pos` in the key++value layout of `idb_fp`.
    pub(crate) fn agg_column_type(
        &self,
        idb_fp: u64,
        agg_pos: usize,
    ) -> Result<DataType, CodegenError> {
        let (keys, vals) = self.find_global_data_type(idb_fp)?;
        keys.iter()
            .chain(vals)
            .nth(agg_pos)
            .copied()
            .ok_or_else(|| {
                CodegenError::internal(format!(
                    "aggregation position {agg_pos} out of bounds for \
                 relation fingerprint 0x{idb_fp:016x}"
                ))
            })
    }

    /// Propagate types through `flow` and register the output shape
    /// under `output_fingerprint` (or its IDB fingerprint, if this head
    /// feeds one).
    pub(crate) fn record_transformation_output_type(
        &mut self,
        left_fingerprint: u64,
        right_fingerprint: Option<u64>,
        output_fingerprint: u64,
        flow: &TransformationFlow,
        stratum: &StratumPlanner,
    ) -> Result<(), CodegenError> {
        let output_fingerprint = stratum
            .head_to_idb_map()
            .get(&output_fingerprint)
            .copied()
            .unwrap_or(output_fingerprint);

        // IDB relations are seeded from their `.decl` in
        // `make_global_data_type_map`, and that declared shape is
        // authoritative. For aggregation rules in particular, the
        // transformation flow carries the *pre-aggregation* column types
        // (e.g. `count(n)` where `n: String` flows a `String` value),
        // which don't match the IDB's `.decl`-declared output type.
        if self.global_fp_to_type.contains_key(&output_fingerprint) {
            return Ok(());
        }

        let left_type = self.find_global_data_type(left_fingerprint)?.clone();
        let right_type = right_fingerprint
            .map(|rf| self.find_global_data_type(rf))
            .transpose()?
            .cloned();

        let resolve =
            |expr: &ArithmeticArgument| self.infer_expr_type(expr, &left_type, right_type.as_ref());
        let keys = flow.key().iter().map(&resolve).collect::<Result<_, _>>()?;
        let vals = flow
            .value()
            .iter()
            .map(&resolve)
            .collect::<Result<_, _>>()?;

        self.global_fp_to_type
            .insert(output_fingerprint, (keys, vals));
        Ok(())
    }

    /// Type of the expression's initial factor — post-typecheck every
    /// factor has a concrete type, and an arithmetic expression's factors
    /// all unify to the same type, so the first one is authoritative.
    pub(crate) fn infer_expr_type(
        &self,
        expr: &ArithmeticArgument,
        left_type: &KvTypes,
        right_type: Option<&KvTypes>,
    ) -> Result<DataType, CodegenError> {
        self.infer_factor_type(expr.init(), left_type, right_type)
    }

    fn infer_factor_type(
        &self,
        factor: &FactorArgument,
        left_type: &KvTypes,
        right_type: Option<&KvTypes>,
    ) -> Result<DataType, CodegenError> {
        match factor {
            FactorArgument::Var(TransformationArgument::KV((is_key, idx))) => {
                slot(left_type, *is_key).get(*idx).copied().ok_or_else(|| {
                    CodegenError::internal(format!(
                        "KV slot out of bounds: is_key={is_key}, idx={idx}, \
                     left shape=({}, {})",
                        left_type.0.len(),
                        left_type.1.len()
                    ))
                })
            }
            FactorArgument::Var(TransformationArgument::Jn((is_left, is_key, idx))) => {
                let base = if *is_left {
                    left_type
                } else {
                    right_type.ok_or_else(|| {
                        CodegenError::internal(
                            "join factor references right input but no right type is bound"
                                .to_string(),
                        )
                    })?
                };
                slot(base, *is_key).get(*idx).copied().ok_or_else(|| {
                    CodegenError::internal(format!(
                        "join slot out of bounds: is_left={is_left}, is_key={is_key}, idx={idx}"
                    ))
                })
            }
            FactorArgument::Const(c) => c.data_type().ok_or_else(|| {
                CodegenError::internal(format!(
                    "polymorphic const {c:?} reached codegen; typechecker should have pinned it"
                ))
            }),
            FactorArgument::FnCall { name, .. } => self
                .program
                .udfs()
                .iter()
                .find(|e| e.name() == name)
                .map(|e| e.ret_type())
                .ok_or_else(|| CodegenError::internal(format!("UDF `{name}` not declared"))),
        }
    }
}

fn slot(tp: &KvTypes, is_key: bool) -> &[DataType] {
    if is_key {
        &tp.0
    } else {
        &tp.1
    }
}

// ==================================================
// DataType → Rust type tokens
// ==================================================

/// Internal tuple type (DD's in-memory shape): `f32`/`f64` become
/// `OrderedFloat<_>`, `String` becomes `Spur` under interning.
pub fn data_type_tokens(input_types: &[DataType], string_intern: bool) -> TokenStream {
    tuple_tokens(
        input_types
            .iter()
            .map(|dt| internal_column_tokens(dt, string_intern)),
    )
}

/// User-facing tuple type: `f32` / `String` regardless of interning.
/// The engine converts on insert / drain.
pub(crate) fn user_tuple_tokens(input_types: &[DataType]) -> TokenStream {
    tuple_tokens(input_types.iter().map(user_column_tokens))
}

pub(crate) fn user_column_tokens(dt: &DataType) -> TokenStream {
    match *dt {
        DataType::Int8 => quote! { i8 },
        DataType::Int16 => quote! { i16 },
        DataType::Int32 => quote! { i32 },
        DataType::Int64 => quote! { i64 },
        DataType::UInt8 => quote! { u8 },
        DataType::UInt16 => quote! { u16 },
        DataType::UInt32 => quote! { u32 },
        DataType::UInt64 => quote! { u64 },
        DataType::Float32 => quote! { f32 },
        DataType::Float64 => quote! { f64 },
        DataType::String => quote! { String },
        DataType::Bool => quote! { bool },
    }
}

fn internal_column_tokens(dt: &DataType, string_intern: bool) -> TokenStream {
    match *dt {
        DataType::Float32 => quote! { OrderedFloat<f32> },
        DataType::Float64 => quote! { OrderedFloat<f64> },
        DataType::String if string_intern => quote! { Spur },
        _ => user_column_tokens(dt),
    }
}

/// Wrap per-column tokens as `()`, `(T,)`, or `(T1, T2, …)`.
pub(crate) fn tuple_tokens<I: IntoIterator<Item = TokenStream>>(cols: I) -> TokenStream {
    let tys: Vec<TokenStream> = cols.into_iter().collect();
    match tys.as_slice() {
        [] => quote! { () },
        [t0] => quote! { ( #t0, ) },
        _ => quote! { ( #(#tys),* ) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Config;
    use crate::parser::{ArithmeticOperator, ConstType, Program};

    fn make_codegen() -> CodeGen {
        CodeGen::new(Config::default(), Program::default())
    }

    #[test]
    fn infer_factor_type_concrete_literal_returns_its_type() {
        let cg = make_codegen();
        let empty: KvTypes = (vec![], vec![]);
        assert_eq!(
            cg.infer_factor_type(&FactorArgument::Const(ConstType::Int32(42)), &empty, None)
                .unwrap(),
            DataType::Int32
        );
        assert_eq!(
            cg.infer_factor_type(&FactorArgument::Const(ConstType::Bool(true)), &empty, None)
                .unwrap(),
            DataType::Bool
        );
    }

    #[test]
    fn infer_expr_type_picks_first_concrete_factor() {
        let cg = make_codegen();
        let expr = ArithmeticArgument {
            init: FactorArgument::Const(ConstType::Int64(0)),
            rest: vec![(
                ArithmeticOperator::Plus,
                FactorArgument::Var(TransformationArgument::KV((false, 0))),
            )],
        };
        let left_type: KvTypes = (vec![], vec![DataType::Int64]);
        assert_eq!(
            cg.infer_expr_type(&expr, &left_type, None).unwrap(),
            DataType::Int64
        );
    }

    /// Aggregation rules feed a transformation flow whose column types
    /// match the *pre-reduce* input (e.g. `count(n: String)` flows a
    /// `String`), not the IDB's declared output. Inferring from that flow
    /// would overwrite the authoritative `.decl` shape and break later
    /// codegen (see `agg_count_string` e2e).
    #[test]
    fn record_transformation_output_type_preserves_declared_idb_shape() {
        use crate::planner::Constraints;
        use std::sync::Arc;

        let mut cg = make_codegen();
        // IDB's declared shape: e.g. `DeptHeadcount(d: int32, cnt: int32)`.
        let declared = (vec![DataType::Int32], vec![DataType::Int32]);
        cg.global_fp_to_type.insert(0x1, declared.clone());

        // Pre-aggregation input: `(d: Int32, n: String)`. If the
        // short-circuit is removed, the flow below would resolve its
        // value column as `String` from this input, and overwrite the
        // declared IDB shape at fp 0x1.
        cg.global_fp_to_type
            .insert(0x2, (vec![], vec![DataType::Int32, DataType::String]));
        let flow = TransformationFlow::KVToKV {
            key: Arc::new(vec![ArithmeticArgument {
                init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
                rest: vec![],
            }]),
            value: Arc::new(vec![ArithmeticArgument {
                init: FactorArgument::Var(TransformationArgument::KV((false, 1))),
                rest: vec![],
            }]),
            constraints: Constraints::new(vec![], vec![]),
            compares: vec![],
            fn_call_preds: vec![],
        };
        let stratum = StratumPlanner::default();

        cg.record_transformation_output_type(0x2, None, 0x1, &flow, &stratum)
            .expect("short-circuit on registered output must not error");

        assert_eq!(cg.global_fp_to_type.get(&0x1), Some(&declared));
    }

    /// Rust distinguishes `(T)` (a parenthesized type) from `(T,)` (a
    /// 1-tuple). `tuple_tokens` must emit the trailing comma for the
    /// singleton case or every 1-column IDB silently gets the wrong
    /// type in the generated project. Also pins the 0-arity and n-arity
    /// branches so a refactor can't accidentally collapse the `match`.
    #[test]
    fn tuple_tokens_arity_dispatch_keeps_singleton_comma() {
        // Arity 0 → unit type `()`.
        assert_eq!(tuple_tokens(std::iter::empty()).to_string(), "()");

        // Arity 1 → `(T,)` — the comma is the whole point.
        let single = tuple_tokens(std::iter::once(quote! { i32 })).to_string();
        let single_norm: String = single.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(
            single_norm, "(i32 ,)",
            "singleton tuple must carry trailing comma; `(i32)` would be a \
             parenthesized type, not a 1-tuple"
        );

        // Arity 2+ → standard comma-separated tuple without trailing comma.
        let pair = tuple_tokens(vec![quote! { i32 }, quote! { String }]).to_string();
        let pair_norm: String = pair.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(pair_norm, "(i32 , String)");
    }
}
