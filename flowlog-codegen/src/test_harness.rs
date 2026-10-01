//! Helpers for this crate's tests: a code generator over a program given as
//! a source string, and renderers that turn generated tokens into the
//! strings the tests compare.

use flowlog_common::Config;
use flowlog_common::SourceMap;
use proc_macro2::TokenStream;

use crate::Codegen;

/// Returns a code generator over `source`, its types and idents seeded and
/// every relation's mutability recorded from its declaration, as the strata
/// would record it.
///
/// # Panics
///
/// Panics when the program does not parse.
pub(crate) fn codegen(source: &str) -> Codegen {
    let mut config = Config::default();
    let program = flowlog_parser::test_harness::parse(source, &mut SourceMap::new(), &mut config)
        .unwrap_or_else(|e| panic!("test program does not parse: {e:?}"));
    let mut codegen = Codegen::new(config, program);
    codegen.seed_global_types();
    codegen.seed_global_idents();
    codegen.global_fp_to_mutability = codegen
        .program
        .edbs()
        .into_iter()
        .chain(codegen.program.idbs())
        .map(|rel| (rel.fingerprint(), rel.input_mutability()))
        .collect();
    codegen
}

/// Returns `tokens` rendered as source, after checking that they parse as
/// a Rust file: a fragment that does not is a codegen bug whatever its text
/// says.
///
/// # Panics
///
/// Panics when `tokens` do not parse.
pub(crate) fn rendered(tokens: TokenStream) -> String {
    syn::parse2::<syn::File>(tokens.clone()).expect("generated code is valid Rust");
    tokens.to_string()
}

/// Returns each of `tokens` rendered as source, for comparing a list of
/// fragments against its expectation in one assertion.
pub(crate) fn strings<I>(tokens: I) -> Vec<String>
where
    I: IntoIterator,
    I::Item: ToString,
{
    tokens.into_iter().map(|t| t.to_string()).collect()
}
