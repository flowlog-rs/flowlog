//! The program's boundary. [`relation`] declares each relation, [`input`]
//! owns the inputs' loaders, collections, and handles, and [`output`] the
//! inspectors and emitters the outputs go through;
//! [`Codegen::gen_relations`] bundles what a frontend includes as its
//! relation module.

pub(crate) mod input;
pub(crate) mod output;
pub(crate) mod relation;

use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::input::gen_inputs_container;
use crate::io::relation::gen_relation_declaration;

impl Codegen {
    /// Returns the relation module a frontend includes: a `Relation`
    /// declaration for every input and output, then the `Inputs` container
    /// of the inputs' loaders. Each declaration carries the mutability
    /// `generate` planned for its relation, so it runs after `generate`.
    ///
    /// The generated code expects its parent module to supply `Ts`; interned
    /// facts use the runtime's string pool.
    pub fn gen_relations(&self) -> Result<TokenStream, CodegenError> {
        let string_intern = self.config.str_intern_enabled();
        let edbs = self.program.edbs();
        let outputs = self.program.idbs();
        let declarations = edbs
            .iter()
            .copied()
            .chain(outputs.into_iter().filter(|output| {
                !edbs
                    .iter()
                    .any(|input| input.fingerprint() == output.fingerprint())
            }))
            .map(|relation| {
                gen_relation_declaration(
                    &self.program,
                    relation,
                    self.mutability(relation.fingerprint())?,
                    string_intern,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let inputs = gen_inputs_container(&edbs, string_intern);
        Ok(quote! {
            use super::*;
            #(#declarations)*
            #inputs
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    /// Renders the relation module of `source`'s program.
    fn generate(source: &str) -> String {
        rendered(codegen(source).gen_relations().expect("generate relations"))
    }

    #[test]
    fn a_relation_that_is_both_input_and_output_is_declared_once() {
        let generated = generate(".decl Edge(id: int32)\n.input Edge\n.output Edge\n");
        let declaration = quote! { impl ::flowlog_runtime::io::Relation for Reledge }.to_string();
        assert_eq!(generated.matches(&declaration).count(), 1, "{generated}");
    }
}
