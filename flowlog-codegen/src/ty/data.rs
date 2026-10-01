//! Data types: the registry of each collection's key and value types, and
//! their Rust type tokens.
//!
//! Type checking happens earlier, in flowlog-parser's typecheck pass. This
//! module seeds each relation's types from its `.decl`, carries them through
//! the planner's transformations to every intermediate fingerprint, and
//! lowers them to Rust tuple types in two shapes: the internal one the
//! dataflow holds and the one users see.

use flowlog_parser::DataType;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::TransformationArgument;
use flowlog_planner::planner::TransformationFlow;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

/// `(key_types, value_types)`: a relation's shape in key++value form.
pub(crate) type KvTypes = (Vec<DataType>, Vec<DataType>);

// =============================================================================
// Fingerprint -> KvTypes registry
// =============================================================================

impl Codegen {
    /// Seeds the registry with every declared relation's types.
    pub(crate) fn seed_global_types(&mut self) {
        self.global_fp_to_type = self
            .program
            .relations()
            .iter()
            .map(|rel| (rel.fingerprint(), (Vec::new(), rel.data_type())))
            .collect();
    }

    /// Returns the key and value types recorded for `fingerprint`, or an
    /// internal error when none are: every collection codegen reads was
    /// recorded when it was built.
    pub(crate) fn find_global_type(&self, fingerprint: u64) -> Result<&KvTypes, CodegenError> {
        self.global_fp_to_type.get(&fingerprint).ok_or_else(|| {
            CodegenError::internal(format!(
                "input type missing for fingerprint 0x{fingerprint:016x}"
            ))
        })
    }

    /// Returns the type of column `agg_pos` in `idb_fp`'s key++value layout.
    pub(crate) fn agg_column_type(
        &self,
        idb_fp: u64,
        agg_pos: usize,
    ) -> Result<DataType, CodegenError> {
        let (keys, vals) = self.find_global_type(idb_fp)?;
        keys.iter()
            .chain(vals)
            .nth(agg_pos)
            .cloned()
            .ok_or_else(|| {
                CodegenError::internal(format!(
                    "aggregation position {agg_pos} out of bounds for \
                 relation fingerprint 0x{idb_fp:016x}"
                ))
            })
    }

    /// Records the key and value types `flow` produces from its inputs under
    /// `output_fingerprint`, the collection's own identity: any
    /// transformation may read it, a shared one included. Relations are
    /// seeded from their `.decl` and never registered here, so a head's
    /// flow type, which for `count(n)` is the type of `n`, never stands
    /// in for the declared type of the relation it feeds.
    pub(crate) fn record_output_type(
        &mut self,
        left_fingerprint: u64,
        right_fingerprint: Option<u64>,
        output_fingerprint: u64,
        flow: &TransformationFlow,
    ) -> Result<(), CodegenError> {
        let left_type = self.find_global_type(left_fingerprint)?.clone();
        let right_type = right_fingerprint
            .map(|rf| self.find_global_type(rf))
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

    /// Returns an expression's type, which is its first factor's: after
    /// typecheck every factor has a concrete type, and an arithmetic
    /// expression's factors all unify to one type.
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
                slot(left_type, *is_key).get(*idx).cloned().ok_or_else(|| {
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
                            "join factor references right input but no right type is bound",
                        )
                    })?
                };
                slot(base, *is_key).get(*idx).cloned().ok_or_else(|| {
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
            FactorArgument::Builtin { op, .. } => Ok(op.ret_type()),
            FactorArgument::Group(a) => self.infer_expr_type(a, left_type, right_type),
            FactorArgument::Tuple { fields } => {
                let dts = fields
                    .iter()
                    .map(|f| self.infer_expr_type(f, left_type, right_type))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataType::FixedTuple(dts))
            }
            FactorArgument::TupleProj { tuple, index } => {
                match self.infer_expr_type(tuple, left_type, right_type)? {
                    DataType::FixedTuple(fields) => fields.get(*index).cloned().ok_or_else(|| {
                        CodegenError::internal(format!(
                            "tuple projection index {index} out of bounds (arity {})",
                            fields.len()
                        ))
                    }),
                    other => Err(CodegenError::internal(format!(
                        "tuple projection of a non-tuple type {other:?}"
                    ))),
                }
            }
        }
    }

    /// Returns `true` if every projected column has the type of the input
    /// column at its position: the output row then has the input row's Rust
    /// tuple type, the type precondition for an in-place rewrite.
    pub(crate) fn row_projection_preserves_type(
        &self,
        args: &[ArithmeticArgument],
        input_type: &KvTypes,
    ) -> Result<bool, CodegenError> {
        let row = &input_type.1;
        if args.len() != row.len() {
            return Ok(false);
        }
        for (arg, expected) in args.iter().zip(row) {
            if self.infer_expr_type(arg, input_type, None)? != *expected {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Returns the key types of `tp` when `is_key`, else its value types.
fn slot(tp: &KvTypes, is_key: bool) -> &[DataType] {
    if is_key { &tp.0 } else { &tp.1 }
}

// =============================================================================
// DataType -> Rust type tokens
// =============================================================================

/// Returns the internal tuple type the dataflow holds, each column lowered
/// by `internal_column_tokens`.
///
/// # Panics
///
/// Panics on an unpinned literal type; see `user_column_tokens`.
pub(crate) fn internal_tuple_tokens(input_types: &[DataType], string_intern: bool) -> TokenStream {
    tuple_tokens(
        input_types
            .iter()
            .map(|dt| internal_column_tokens(dt, string_intern)),
    )
}

/// Returns the tuple type users see, each column lowered by
/// `user_column_tokens`; the engine converts on insert and drain.
///
/// # Panics
///
/// Panics on an unpinned literal type; see `user_column_tokens`.
pub fn user_tuple_tokens(input_types: &[DataType]) -> TokenStream {
    tuple_tokens(input_types.iter().map(user_column_tokens))
}

/// Returns a column's user-facing Rust type: its scalar type as declared,
/// whatever the interning, and a tuple column as a nested tuple.
///
/// # Panics
///
/// Panics on `IntLit` or `FloatLit`: the typechecker pins every literal
/// before codegen.
pub(crate) fn user_column_tokens(dt: &DataType) -> TokenStream {
    match dt {
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
        DataType::FixedTuple(fields) => user_tuple_tokens(fields),
        DataType::IntLit | DataType::FloatLit => {
            unreachable!("unpinned literal type reached codegen; the typechecker pins all literals")
        }
    }
}

/// Returns `true` if every column lowers to a `Copy` Rust type (see
/// [`internal_column_tokens`]): a raw `String` leaf is the only non-`Copy`
/// lowering, and interning replaces those with `Spur` keys.
pub(crate) fn row_is_copy(types: &[DataType], string_intern: bool) -> bool {
    string_intern
        || !types
            .iter()
            .any(|dt| dt.any_scalar(&|l| matches!(l, DataType::String)))
}

/// Returns a column's internal Rust type: a float wrapped in `OrderedFloat`
/// so it orders totally, a string as its `Spur` key under interning, and
/// every other scalar as its user-facing type.
///
/// # Panics
///
/// Panics on an unpinned literal type; see [`user_column_tokens`].
pub(crate) fn internal_column_tokens(dt: &DataType, string_intern: bool) -> TokenStream {
    match dt {
        DataType::Float32 => quote! { ::flowlog_runtime::ordered_float::OrderedFloat<f32> },
        DataType::Float64 => quote! { ::flowlog_runtime::ordered_float::OrderedFloat<f64> },
        DataType::String if string_intern => quote! { ::flowlog_runtime::lasso::Spur },
        DataType::FixedTuple(fields) => internal_tuple_tokens(fields, string_intern),
        DataType::String
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Bool
        | DataType::IntLit
        | DataType::FloatLit => user_column_tokens(dt),
    }
}

/// Returns `cols` as a tuple: `()`, `(T,)`, or `(T1, T2, ...)`. A single
/// column keeps its trailing comma, since `(T)` is a parenthesized `T`, not
/// a 1-tuple.
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
    use std::sync::Arc;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::Constant;
    use flowlog_planner::planner::Constraints;
    use flowlog_planner::planner::TransformationArgument::Jn;
    use flowlog_planner::planner::TransformationArgument::KV;
    use rstest::rstest;

    use super::*;

    /// A code generator over an empty program; parsing is the only way to
    /// build a `Program`.
    fn codegen() -> Codegen {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("empty program parses");
        Codegen::new(config, program)
    }

    fn arg(init: FactorArgument) -> ArithmeticArgument {
        ArithmeticArgument {
            init,
            rest: Vec::new(),
        }
    }

    // --- Type inference ---

    /// Inference reads the left input `(Int64 | String)` and the right input
    /// `(| Bool)`.
    // Cases: factor, type.
    #[rstest]
    #[case::literal(
        FactorArgument::Const(Constant::new(DataType::Int32, "42")),
        DataType::Int32
    )]
    #[case::left_key(FactorArgument::Var(KV((true, 0))), DataType::Int64)]
    #[case::left_value(FactorArgument::Var(KV((false, 0))), DataType::String)]
    #[case::right_value(FactorArgument::Var(Jn((false, false, 0))), DataType::Bool)]
    #[case::tuple(
        FactorArgument::Tuple { fields: vec![arg(FactorArgument::Var(KV((true, 0)))), arg(FactorArgument::Var(Jn((false, false, 0))))] },
        DataType::FixedTuple(vec![DataType::Int64, DataType::Bool])
    )]
    #[case::projection(
        FactorArgument::TupleProj {
            tuple: Box::new(arg(FactorArgument::Tuple { fields: vec![arg(FactorArgument::Var(KV((true, 0)))), arg(FactorArgument::Var(KV((false, 0))))] })),
            index: 1,
        },
        DataType::String
    )]
    fn a_factor_infers_its_type(#[case] factor: FactorArgument, #[case] expected: DataType) {
        let left: KvTypes = (vec![DataType::Int64], vec![DataType::String]);
        let right: KvTypes = (Vec::new(), vec![DataType::Bool]);
        let ty = codegen()
            .infer_factor_type(&factor, &left, Some(&right))
            .expect("typed factor");
        assert_eq!(ty, expected);
    }

    #[test]
    fn an_expression_has_its_first_factors_type() {
        let expr = ArithmeticArgument {
            init: FactorArgument::Const(Constant::new(DataType::Int64, "0")),
            rest: vec![(
                ArithmeticOperator::Plus,
                FactorArgument::Var(KV((false, 0))),
            )],
        };
        let left: KvTypes = (Vec::new(), vec![DataType::Int64]);
        assert_eq!(
            codegen()
                .infer_expr_type(&expr, &left, None)
                .expect("typed"),
            DataType::Int64
        );
    }

    /// A head's flow carries the pre-reduce column types, `count(n:
    /// String)` flows a `String`, so its shape goes under the head's own
    /// fingerprint and the relation it feeds keeps the shape its `.decl`
    /// seeded (see `agg_count_string` e2e).
    #[test]
    fn record_transformation_output_type_registers_the_head_not_the_relation() {
        let mut cg = codegen();
        // The relation's declared shape: `DeptHeadcount(d: int32, cnt: int32)`.
        let declared = (vec![DataType::Int32], vec![DataType::Int32]);
        cg.global_fp_to_type.insert(0x1, declared.clone());
        // The head's input: `(d: Int32, n: String)`.
        cg.global_fp_to_type
            .insert(0x2, (vec![], vec![DataType::Int32, DataType::String]));
        let flow = TransformationFlow::KVToKV {
            key: Arc::new(vec![arg(FactorArgument::Var(KV((false, 0))))]),
            value: Arc::new(vec![arg(FactorArgument::Var(KV((false, 1))))]),
            constraints: Constraints::new(vec![], vec![]),
            compares: vec![],
        };

        cg.record_output_type(0x2, None, 0x3, &flow)
            .expect("the head's inputs are registered");

        assert_eq!(
            cg.global_fp_to_type.get(&0x3),
            Some(&(vec![DataType::Int32], vec![DataType::String]))
        );
        assert_eq!(cg.global_fp_to_type.get(&0x1), Some(&declared));
    }

    /// The input row is `(Int32, String)`.
    // Cases: projected columns, preserves type.
    #[rstest]
    #[case::identity(vec![KV((false, 0)), KV((false, 1))], true)]
    #[case::swapped(vec![KV((false, 1)), KV((false, 0))], false)]
    #[case::narrower(vec![KV((false, 0))], false)]
    fn a_row_projection_preserves_type_only_column_for_column(
        #[case] columns: Vec<TransformationArgument>,
        #[case] expected: bool,
    ) {
        let args: Vec<_> = columns
            .into_iter()
            .map(|column| arg(FactorArgument::Var(column)))
            .collect();
        let input: KvTypes = (Vec::new(), vec![DataType::Int32, DataType::String]);
        assert_eq!(
            codegen()
                .row_projection_preserves_type(&args, &input)
                .expect("typed"),
            expected
        );
    }

    // --- Rust type tokens ---

    // Cases: column, string_intern, internal type, user type.
    #[rstest]
    #[case::int(DataType::Int32, false, quote! { i32 }, quote! { i32 })]
    #[case::float(DataType::Float64, false, quote! { ::flowlog_runtime::ordered_float::OrderedFloat<f64> }, quote! { f64 })]
    #[case::string(DataType::String, false, quote! { String }, quote! { String })]
    #[case::interned_string(
        DataType::String,
        true,
        quote! { ::flowlog_runtime::lasso::Spur },
        quote! { String }
    )]
    #[case::tuple(
        DataType::FixedTuple(vec![DataType::Float32, DataType::String]),
        true,
        quote! { (::flowlog_runtime::ordered_float::OrderedFloat<f32>, ::flowlog_runtime::lasso::Spur) },
        quote! { (f32, String) }
    )]
    fn a_column_lowers_to_its_internal_and_user_types(
        #[case] column: DataType,
        #[case] string_intern: bool,
        #[case] internal: TokenStream,
        #[case] user: TokenStream,
    ) {
        assert_eq!(
            (
                internal_column_tokens(&column, string_intern).to_string(),
                user_column_tokens(&column).to_string()
            ),
            (internal.to_string(), user.to_string())
        );
    }

    // Cases: columns, string_intern, copy.
    #[rstest]
    #[case::numbers(vec![DataType::Int32, DataType::Float64], false, true)]
    #[case::raw_string(vec![DataType::Int32, DataType::String], false, false)]
    #[case::nested_raw_string(vec![DataType::FixedTuple(vec![DataType::String])], false, false)]
    #[case::interned_string(vec![DataType::String], true, true)]
    fn only_a_raw_string_makes_a_row_not_copy(
        #[case] columns: Vec<DataType>,
        #[case] string_intern: bool,
        #[case] expected: bool,
    ) {
        assert_eq!(row_is_copy(&columns, string_intern), expected);
    }

    // Cases: columns, tuple.
    #[rstest]
    #[case::unit(vec![], quote! { () })]
    #[case::single(vec![quote! { i32 }], quote! { (i32,) })]
    #[case::pair(vec![quote! { i32 }, quote! { String }], quote! { (i32, String) })]
    fn tuple_tokens_keep_the_singleton_comma(
        #[case] columns: Vec<TokenStream>,
        #[case] expected: TokenStream,
    ) {
        assert_eq!(tuple_tokens(columns).to_string(), expected.to_string());
    }
}
