//! UDF calls: a `.extern fn` call as a call into the user's `udf` module,
//! with each argument converted to the declared parameter type.

use flowlog_parser::DataType;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns a call to `udf::<name>`, each argument passed as an owned
    /// value of its declared parameter type.
    ///
    /// With `string_intern` set, a string argument resolves from its interned
    /// key to an owned `String`, and a string result is interned back, so the
    /// user's function sees and returns plain strings either way.
    ///
    /// Returns an internal error when no `.extern fn` declares `name` or the
    /// call's argument count differs from the declaration's: the typechecker
    /// checks both.
    pub(super) fn fncall_to_token<F>(
        &mut self,
        name: &str,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        let udf = self
            .program
            .udfs()
            .iter()
            .find(|f| f.name() == name)
            .ok_or_else(|| CodegenError::internal(format!("UDF `{name}` not declared")))?;
        let param_types: Vec<DataType> =
            udf.params().iter().map(|p| p.data_type().clone()).collect();
        let returns_string = udf.ret_type() == DataType::String;
        if param_types.len() != args.len() {
            return Err(CodegenError::internal(format!(
                "UDF `{name}` takes {} arguments, got {}",
                param_types.len(),
                args.len()
            )));
        }

        let arg_tokens = args
            .iter()
            .zip(&param_types)
            .map(|(arg, ty)| {
                let token = self.arithmetic_to_token(arg, string_intern, resolve_var)?;
                // `token` is already an owned value: every arithmetic
                // lowering clones the variables it reads.
                Ok(if string_intern && *ty == DataType::String {
                    quote! { ::flowlog_runtime::intern::resolve(#token).to_string() }
                } else {
                    token
                })
            })
            .collect::<Result<Vec<_>, CodegenError>>()?;

        let fn_ident = format_ident!("{}", name);
        let call = quote! { udf::#fn_ident(#(#arg_tokens),*) };
        Ok(if string_intern && returns_string {
            quote! { ::flowlog_runtime::intern::intern(&#call) }
        } else {
            call
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::TransformationArgument::KV;
    use rstest::rstest;
    use syn::Index;

    use super::*;
    use crate::test_harness::codegen;

    /// The value column `v.<idx>`, as a whole argument.
    fn value(idx: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(KV((false, idx))),
            rest: Vec::new(),
        }
    }

    // Cases: declaration, string_intern, call.
    #[rstest]
    #[case::numbers(
        ".extern fn f(a: int32, b: int32) -> int32",
        false,
        quote! { udf::f(v.0.clone(), v.1.clone()) }
    )]
    #[case::strings(
        ".extern fn f(a: string, b: int32) -> string",
        false,
        quote! { udf::f(v.0.clone(), v.1.clone()) }
    )]
    #[case::interned_strings(
        ".extern fn f(a: string, b: int32) -> string",
        true,
        quote! {
            ::flowlog_runtime::intern::intern(&udf::f(
                ::flowlog_runtime::intern::resolve(v.0.clone()).to_string(),
                v.1.clone()
            ))
        }
    )]
    #[case::interned_number_result(
        ".extern fn f(a: string, b: int32) -> int32",
        true,
        quote! {
            udf::f(
                ::flowlog_runtime::intern::resolve(v.0.clone()).to_string(),
                v.1.clone()
            )
        }
    )]
    fn a_udf_call_passes_and_returns_plain_values(
        #[case] declaration: &str,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        let call = codegen(declaration)
            .fncall_to_token(
                "f",
                &[value(0), value(1)],
                string_intern,
                &|arg| match arg {
                    KV((false, idx)) => {
                        let i = Index::from(*idx);
                        Ok(quote! { v.#i.clone() })
                    }
                    other => Err(CodegenError::internal(format!("unexpected {other:?}"))),
                },
            )
            .expect("declared UDF");
        assert_eq!(call.to_string(), expected.to_string());
    }
}
