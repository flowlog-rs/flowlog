//! The program's boundary. [`relation`] declares each relation, [`input`]
//! owns the inputs' loaders, collections, and handles, and [`output`] the
//! inspectors and emitters the outputs go through; [`gen_relations`] bundles
//! what a frontend includes as its relation module.

pub(crate) mod input;
pub(crate) mod output;
pub(crate) mod relation;

use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::quote;

use crate::CodegenError;
use crate::io::input::gen_inputs_container;
use crate::io::relation::gen_relation_declaration;

/// Returns the relation module a frontend includes: a `Relation`
/// declaration for every input and output, then the `Inputs` container of
/// the inputs' loaders.
///
/// The generated code names nothing from its parent module: each loader
/// spells the time of its own scope. Interned facts use the runtime's
/// string pool.
pub fn gen_relations(program: &Program, string_intern: bool) -> Result<TokenStream, CodegenError> {
    let edbs = program.edbs();
    let outputs = program.idbs();
    let declarations = edbs
        .iter()
        .copied()
        .chain(outputs.into_iter().filter(|output| {
            !edbs
                .iter()
                .any(|input| input.fingerprint() == output.fingerprint())
        }))
        .map(|relation| gen_relation_declaration(program, relation, string_intern))
        .collect::<Result<Vec<_>, _>>()?;
    let inputs = gen_inputs_container(program, string_intern);
    Ok(quote! {
        #(#declarations)*
        #inputs
    })
}

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::rendered;

    /// Renders the relation module of `source`'s program.
    fn generate(source: &str) -> String {
        rendered(gen_relations(&program(source), false).expect("generate relations"))
    }

    #[test]
    fn a_relation_that_is_both_input_and_output_is_declared_once() {
        let generated = generate(".decl Edge(id: int32)\n.input Edge\n.output Edge\n");
        let declaration = quote! { impl ::flowlog_runtime::io::Relation for Reledge }.to_string();
        assert_eq!(generated.matches(&declaration).count(), 1, "{generated}");
    }

    /// Every loader spells the time of its own scope, so the module names
    /// nothing from its parent, whatever its inputs.
    // Cases: program.
    #[rstest]
    #[case::static_input(".decl A(x: int32)\n.input A\n.output A\n")]
    #[case::mutable_input(".decl A(x: int32) mutable\n.input A\n.output A\n")]
    fn a_relation_module_imports_nothing_from_its_parent(#[case] source: &str) {
        let generated = generate(source);
        assert!(!generated.contains("super"), "{generated}");
    }
}
