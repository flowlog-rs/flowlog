//! Comparison predicates: a rule's comparisons as the predicate a row,
//! key-value, or join closure filters on. One public function per closure
//! shape; all three share [`CodeGen::comparison`], which lowers one
//! comparison, and the string constraints `match` and `contains` below it.

use flowlog_parser::ComparisonOperator;
use flowlog_parser::DataType;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::ComparisonExprArgument;
use flowlog_planner::planner::FactorArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::ty::data::KvTypes;

impl CodeGen {
    /// Returns a row closure's comparison predicate, or `None` when there is
    /// no comparison. Each variable reads from the row pattern's `fields`.
    pub(crate) fn row_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        fields: &[Ident],
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<Option<TokenStream>, CodegenError> {
        self.compare_predicate(comps, string_intern, (input_type, None), |cg, arg| {
            cg.build_row_args_arithmetic_expr(arg, fields, string_intern)
        })
    }

    /// Returns a key-value closure's comparison predicate, or `None` when
    /// there is no comparison. Each variable reads from the `(k, v)`
    /// parameters.
    pub(crate) fn kv_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<Option<TokenStream>, CodegenError> {
        self.compare_predicate(comps, string_intern, (input_type, None), |cg, arg| {
            cg.build_kv_args_arithmetic_expr(arg, string_intern)
        })
    }

    /// Returns a join closure's comparison predicate, or `None` when there
    /// is no comparison. Each variable reads from the `(k, lv, rv)`
    /// parameters.
    pub(crate) fn join_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        left_type: &KvTypes,
        right_type: &KvTypes,
    ) -> Result<Option<TokenStream>, CodegenError> {
        self.compare_predicate(
            comps,
            string_intern,
            (left_type, Some(right_type)),
            |cg, arg| cg.build_join_args_arithmetic_expr(arg, string_intern),
        )
    }

    /// Returns every comparison joined with `&&`, or `None` when there is
    /// none.
    fn compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        types: (&KvTypes, Option<&KvTypes>),
        operand: impl Fn(&mut Self, &ArithmeticArgument) -> Result<TokenStream, CodegenError>,
    ) -> Result<Option<TokenStream>, CodegenError> {
        let parts = comps
            .iter()
            .map(|c| {
                self.comparison(
                    c.operator(),
                    c.left(),
                    c.right(),
                    string_intern,
                    types,
                    &operand,
                )
            })
            .collect::<Result<Vec<_>, CodegenError>>()?;
        Ok((!parts.is_empty()).then(|| quote! { #( #parts )&&* }))
    }

    /// Returns one comparison `left op right`. `operand` lowers each side;
    /// `types` are the input types (the left and, for a join, the right)
    /// its variables read.
    fn comparison(
        &mut self,
        op: &ComparisonOperator,
        left: &ArithmeticArgument,
        right: &ArithmeticArgument,
        string_intern: bool,
        (left_type, right_type): (&KvTypes, Option<&KvTypes>),
        operand: &impl Fn(&mut Self, &ArithmeticArgument) -> Result<TokenStream, CodegenError>,
    ) -> Result<TokenStream, CodegenError> {
        // Either side may be an arithmetic expression, so both lower through
        // the arithmetic builders, `.clone()` on each variable included;
        // constraint.rs compares bare bindings because its sides are only
        // variables and constants.
        let l = operand(self, left)?;
        let r = operand(self, right)?;
        let infix = match op {
            ComparisonOperator::Equal => quote! { == },
            ComparisonOperator::NotEqual => quote! { != },
            ComparisonOperator::GreaterThan => quote! { > },
            ComparisonOperator::GreaterEqualThan => quote! { >= },
            ComparisonOperator::LessThan => quote! { < },
            ComparisonOperator::LessEqualThan => quote! { <= },
            ComparisonOperator::Contains { negated } => {
                return Ok(contains_predicate(*negated, &l, &r, string_intern));
            }
            ComparisonOperator::Match { negated } => {
                return Ok(match_predicate(*negated, left, &l, &r, string_intern));
            }
        };
        // An interned string's key orders by insertion, not by text, so an
        // ordering between two strings compares their resolved text.
        // Equality on keys is exact and stays on the keys.
        let resolve = string_intern
            && op.is_ordering()
            && self.infer_expr_type(left, left_type, right_type)? == DataType::String
            && self.infer_expr_type(right, left_type, right_type)? == DataType::String;
        Ok(if resolve {
            quote! {
                ::flowlog_runtime::intern::resolve(#l)
                    #infix ::flowlog_runtime::intern::resolve(#r)
            }
        } else {
            quote! { (#l) #infix (#r) }
        })
    }
}

/// Returns `contains`: `true` when the `haystack` string contains the
/// `needle` string, or the opposite when `negated`.
fn contains_predicate(
    negated: bool,
    needle: &TokenStream,
    haystack: &TokenStream,
    string_intern: bool,
) -> TokenStream {
    let neg = negation(negated);
    let needle = as_str(needle, string_intern);
    let haystack = as_str(haystack, string_intern);
    quote! { #neg (#haystack).contains(#needle) }
}

/// Returns `match`: `true` when the `pattern` regular expression matches the
/// whole `haystack` string, or the opposite when `negated`. A pattern that
/// fails to compile matches nothing. `pattern_arg` is the pattern before
/// lowering; a string literal compiles once rather than on every row.
fn match_predicate(
    negated: bool,
    pattern_arg: &ArithmeticArgument,
    pattern: &TokenStream,
    haystack: &TokenStream,
    string_intern: bool,
) -> TokenStream {
    let neg = negation(negated);
    let haystack = as_str(haystack, string_intern);
    // FlowLog requires a full match, while regex searches by default.
    // Anchoring gives both literal and computed patterns the same
    // full-match semantics.
    if let FactorArgument::Const(c) = pattern_arg.init()
        && c.ty() == &DataType::String
        && pattern_arg.rest().is_empty()
    {
        let anchored = format!("^(?:{})$", c.text());
        quote! {{
            static RE: ::std::sync::LazyLock<
                Option<::flowlog_runtime::regex::Regex>,
            > = ::std::sync::LazyLock::new(|| {
                ::flowlog_runtime::regex::Regex::new(#anchored).ok()
            });
            #neg RE.as_ref().is_some_and(|re| re.is_match(#haystack))
        }}
    } else {
        let pattern = as_str(pattern, string_intern);
        quote! {
            #neg ::flowlog_runtime::regex::Regex::new(&format!("^(?:{})$", #pattern))
                .map_or(false, |re| re.is_match(#haystack))
        }
    }
}

/// Returns the prefix `!` when `negated`, otherwise nothing.
fn negation(negated: bool) -> TokenStream {
    // A prefix `!` rather than `!( ... )` around the whole predicate:
    // the predicate lands in an `if` condition, where the outer
    // parentheses would trip the `unused_parens` lint.
    if negated {
        quote! { ! }
    } else {
        quote! {}
    }
}

/// Returns a string operand as a `&str`, resolving an interned key.
fn as_str(operand: &TokenStream, string_intern: bool) -> TokenStream {
    if string_intern {
        quote! { ::flowlog_runtime::intern::resolve(#operand) }
    } else {
        quote! { (#operand).as_str() }
    }
}

// Tests drive the private `CodeGen::comparison`: a `ComparisonExprArgument`
// is built only inside flowlog-planner, so the public functions can be
// reached only with no comparison at all.
#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_parser::Constant;
    use flowlog_planner::planner::TransformationArgument::KV;
    use rstest::rstest;

    use super::*;

    /// A code generator over an empty program; parsing is the only way to
    /// build a `Program`.
    fn codegen() -> CodeGen {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("empty program parses");
        CodeGen::new(config, program)
    }

    fn arg(init: FactorArgument) -> ArithmeticArgument {
        ArithmeticArgument {
            init,
            rest: Vec::new(),
        }
    }

    /// Lowers `v.0 op v.1` in a key-value closure whose two value columns
    /// have type `ty`.
    fn kv_comparison(
        op: ComparisonOperator,
        right: ArithmeticArgument,
        ty: DataType,
        string_intern: bool,
    ) -> String {
        let input_type: KvTypes = (Vec::new(), vec![ty.clone(), ty]);
        codegen()
            .comparison(
                &op,
                &arg(FactorArgument::Var(KV((false, 0)))),
                &right,
                string_intern,
                (&input_type, None),
                &|cg, arg| cg.build_kv_args_arithmetic_expr(arg, string_intern),
            )
            .expect("kv comparison")
            .to_string()
    }

    #[test]
    fn no_comparison_is_no_predicate() {
        let input_type: KvTypes = (Vec::new(), Vec::new());
        let pred = codegen()
            .kv_compare_predicate(&[], false, &input_type)
            .expect("nothing to lower");
        assert!(pred.is_none());
    }

    // Cases: operator, infix.
    #[rstest]
    #[case(ComparisonOperator::Equal, quote! { == })]
    #[case(ComparisonOperator::NotEqual, quote! { != })]
    #[case(ComparisonOperator::GreaterThan, quote! { > })]
    #[case(ComparisonOperator::GreaterEqualThan, quote! { >= })]
    #[case(ComparisonOperator::LessThan, quote! { < })]
    #[case(ComparisonOperator::LessEqualThan, quote! { <= })]
    fn a_numeric_comparison_is_its_infix_operator(
        #[case] op: ComparisonOperator,
        #[case] infix: TokenStream,
    ) {
        assert_eq!(
            kv_comparison(
                op,
                arg(FactorArgument::Var(KV((false, 1)))),
                DataType::Int32,
                false
            ),
            quote! { (v.0.clone()) #infix (v.1.clone()) }.to_string()
        );
    }

    /// Interned keys order by insertion, so only an ordering resolves them.
    // Cases: operator, expression.
    #[rstest]
    #[case(
        ComparisonOperator::LessThan,
        quote! {
            ::flowlog_runtime::intern::resolve(v.0.clone())
                < ::flowlog_runtime::intern::resolve(v.1.clone())
        }
    )]
    #[case(ComparisonOperator::Equal, quote! { (v.0.clone()) == (v.1.clone()) })]
    fn an_interned_string_ordering_compares_the_text(
        #[case] op: ComparisonOperator,
        #[case] expected: TokenStream,
    ) {
        assert_eq!(
            kv_comparison(
                op,
                arg(FactorArgument::Var(KV((false, 1)))),
                DataType::String,
                true
            ),
            expected.to_string()
        );
    }

    #[test]
    fn a_negated_contains_tests_the_right_side_for_the_left() {
        assert_eq!(
            kv_comparison(
                ComparisonOperator::Contains { negated: true },
                arg(FactorArgument::Var(KV((false, 1)))),
                DataType::String,
                false
            ),
            quote! {
                ! ((v.1.clone()).as_str()).contains((v.0.clone()).as_str())
            }
            .to_string()
        );
    }

    /// A literal pattern compiles once, anchored, into a static.
    #[test]
    fn a_literal_match_pattern_compiles_once() {
        let input_type: KvTypes = (Vec::new(), vec![DataType::String]);
        let pattern = arg(FactorArgument::Const(Constant::new(
            DataType::String,
            "a.*",
        )));
        let tokens = codegen()
            .comparison(
                &ComparisonOperator::Match { negated: false },
                &pattern,
                &arg(FactorArgument::Var(KV((false, 0)))),
                false,
                (&input_type, None),
                &|cg, arg| cg.build_kv_args_arithmetic_expr(arg, false),
            )
            .expect("match comparison");
        assert_eq!(
            tokens.to_string(),
            quote! {{
                static RE: ::std::sync::LazyLock<
                    Option<::flowlog_runtime::regex::Regex>,
                > = ::std::sync::LazyLock::new(|| {
                    ::flowlog_runtime::regex::Regex::new("^(?:a.*)$").ok()
                });
                RE.as_ref().is_some_and(|re| re.is_match((v.0.clone()).as_str()))
            }}
            .to_string()
        );
    }

    /// A pattern read from a column compiles, anchored, on every row.
    #[test]
    fn a_computed_match_pattern_compiles_per_row() {
        assert_eq!(
            kv_comparison(
                ComparisonOperator::Match { negated: false },
                arg(FactorArgument::Var(KV((false, 1)))),
                DataType::String,
                false
            ),
            quote! {
                ::flowlog_runtime::regex::Regex::new(&format!("^(?:{})$", (v.0.clone()).as_str()))
                    .map_or(false, |re| re.is_match((v.1.clone()).as_str()))
            }
            .to_string()
        );
    }
}
