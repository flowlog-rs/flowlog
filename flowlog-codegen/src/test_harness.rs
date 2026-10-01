//! Helpers for this crate's tests: a code generator over a program given as
//! a source string.

use flowlog_common::Config;
use flowlog_common::SourceMap;

use crate::Codegen;

/// Returns a code generator over `source`, its idents seeded and every
/// relation's mutability recorded from its declaration, as the strata would
/// record it.
///
/// # Panics
///
/// Panics when the program does not parse.
pub(crate) fn codegen(source: &str) -> Codegen {
    let mut config = Config::default();
    let program = flowlog_parser::test_harness::parse(source, &mut SourceMap::new(), &mut config)
        .unwrap_or_else(|e| panic!("test program does not parse: {e:?}"));
    let mut codegen = Codegen::new(config, program);
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
