//! Top-level item collection and declaration resolution. Declarations resolve
//! separately, once component expansion has registered all member types.

use std::collections::HashMap;

use super::Assembler;
use crate::Node;
use crate::Rule;
use crate::ast::FlowLogRule;
use crate::declaration::CompDecl;
use crate::declaration::ExternFn;
use crate::declaration::Relation;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::types::TypeRegistry;

impl Assembler {
    /// Collects component definitions, external functions, facts, and
    /// directives. Leaves rules, instances, and declarations unresolved.
    pub(super) fn collect(root: Node<'_>, type_registry: TypeRegistry) -> Result<Self, ParseError> {
        let mut assembler = Self::new(type_registry);
        let mut udf_spans = HashMap::new();

        for node in root.children() {
            match node.rule() {
                Rule::extern_fn => {
                    let ext = ExternFn::from_parsed_rule(node, &assembler.type_registry)?;
                    if let Some(prior) = udf_spans.insert(ext.name().to_string(), ext.span()) {
                        return Err(ParseError::DuplicateExternFn {
                            span: ext.span(),
                            prior,
                            name: ext.name().to_string(),
                        });
                    }
                    assembler.udfs.push(ext);
                }
                Rule::comp_decl => {
                    let comp = CompDecl::from_parsed_rule(node)?;
                    assembler.components.insert(comp.name.clone(), comp);
                }
                Rule::input_directive => assembler.input_directives.push(node.lower()?),
                Rule::output_directive => assembler.output_directives.push(node.lower()?),
                Rule::printsize_directive => assembler.printsize_directives.push(node.lower()?),
                Rule::fact => {
                    let head = node.children().lower_next("fact head")?;
                    assembler.raw_facts.push(FlowLogRule::new(head, vec![]));
                }
                Rule::include_directive => {
                    return Err(grammar_bug(
                        "includes must be resolved before program assembly",
                    ));
                }
                Rule::type_alias_decl
                | Rule::declaration
                | Rule::init_decl
                | Rule::rule
                | Rule::EOI => {}
                other => return Err(grammar_bug(format!("unexpected top-level node: {other:?}"))),
            }
        }

        Ok(assembler)
    }

    /// Resolves top-level attributes against the completed type registry.
    /// Keeps top-level declarations before component declarations.
    pub(super) fn resolve_declarations(&mut self, root: Node<'_>) -> Result<(), ParseError> {
        let declarations = root
            .children()
            .filter(|node| node.rule() == Rule::declaration)
            .map(|node| Relation::from_parsed_rule_with_registry(node, &self.type_registry))
            .collect::<Result<Vec<_>, _>>()?;
        self.relations.splice(0..0, declarations);
        Ok(())
    }
}
