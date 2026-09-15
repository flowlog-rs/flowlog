//! Program assembly and phase ordering. `collect` lowers top-level items;
//! `inline` expands rules and component instances. `substitute` removes
//! assignments; `validate` checks declarations, directives, and references.

mod collect;
mod inline;
mod substitute;
mod validate;

use std::collections::HashMap;

use flowlog_common::FileId;
use pest::Parser;

use crate::FlowLogParser;
use crate::Node;
use crate::Rule;
use crate::ast::FlowLogRule;
use crate::declaration::CompDecl;
use crate::declaration::ExternFn;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::Relation;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::program::InlineFact;
use crate::program::Program;
use crate::types::TypeRegistry;

/// Assembly state shared by top-level collection and component expansion.
/// Consumed by `finish` to produce a validated [`Program`].
#[derive(Debug)]
struct Assembler {
    type_registry: TypeRegistry,
    components: HashMap<String, CompDecl>,
    relations: Vec<Relation>,
    rules: Vec<FlowLogRule>,
    udfs: Vec<ExternFn>,
    // Raw heads retain source spellings for undeclared-relation diagnostics.
    raw_facts: Vec<FlowLogRule>,
    input_directives: Vec<InputDirective>,
    output_directives: Vec<OutputDirective>,
    printsize_directives: Vec<PrintSizeDirective>,
}

impl Assembler {
    fn new(type_registry: TypeRegistry) -> Self {
        Self {
            type_registry,
            components: HashMap::new(),
            relations: Vec::new(),
            rules: Vec::new(),
            udfs: Vec::new(),
            raw_facts: Vec::new(),
            input_directives: Vec::new(),
            output_directives: Vec::new(),
            printsize_directives: Vec::new(),
        }
    }

    /// Normalizes and validates the expanded program before folding raw facts.
    fn finish(mut self) -> Result<Program, ParseError> {
        validate::validate_declarations(&self.relations)?;
        validate::apply_directives(
            &mut self.relations,
            self.input_directives,
            self.output_directives,
            self.printsize_directives,
        )?;
        inline::normalize_dots(&mut self.relations, &mut self.rules, &mut self.raw_facts);
        substitute::substitute_assignments(&mut self.rules)?;
        validate::validate_relation_references(&self.relations, &self.rules, &self.raw_facts)?;

        let mut facts = HashMap::new();
        for raw_fact in self.raw_facts {
            let (name, fact) = InlineFact::from_rule(&raw_fact)?;
            facts.entry(name).or_insert_with(Vec::new).push(fact);
        }
        Ok(Program {
            relations: self.relations,
            rules: self.rules,
            udfs: self.udfs,
            facts,
            type_registry: self.type_registry,
        })
    }
}

/// Parses source with includes already resolved and assembles its program.
/// Component rules retain their `.init`'s position among top-level rules.
pub(super) fn collect_program(source: &str, file: FileId) -> Result<Program, ParseError> {
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, source)
        .map_err(|e| ParseError::syntax_from_pest(&e, file))?;
    let root = pairs
        .next()
        .ok_or_else(|| grammar_bug("no parsed rule found"))?;

    let registry = TypeRegistry::from_type_declarations(root.clone(), file)?;
    let root = Node::new(root, file);
    let mut assembler = Assembler::collect(root.clone(), registry)?;
    assembler.expand(root.clone())?;
    // Component member types must exist before top-level attributes resolve.
    assembler.resolve_declarations(root)?;
    assembler.finish()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::assert_err;

    #[test]
    fn collect_program_rejects_duplicate_decl() {
        // `Edge` collides with `edge`: declaration names are case-insensitive.
        assert_err!(
            collect_program(
                ".decl edge(x: number)\n.decl Edge(y: number)\n",
                FileId::new(0),
            ),
            ParseError::DuplicateDecl { .. }
        );
    }

    #[test]
    fn collect_program_rejects_duplicate_extern_fn() {
        assert_err!(
            collect_program(
                ".extern fn hash(x: int64) -> int64\n.extern fn hash(y: int64) -> int64\n",
                FileId::new(0),
            ),
            ParseError::DuplicateExternFn { .. }
        );
    }

    #[rstest]
    fn top_level_decl_resolves_component_subtype(#[values(true, false)] declaration_first: bool) {
        let declaration = ".decl Out(x: c.Context)";
        let component = ".comp C { .type Context <: symbol }\n.init c = C";
        let source = if declaration_first {
            format!("{declaration}\n{component}")
        } else {
            format!("{component}\n{declaration}")
        };
        let program = collect_program(&source, FileId::new(0)).unwrap();
        let attribute = &program.relations()[0].attributes()[0];
        assert_eq!(attribute.data_type(), &crate::DataType::String);
        assert_eq!(
            Some(attribute.declared_id()),
            program.type_registry.lookup("c.Context")
        );
        assert_ne!(
            Some(attribute.declared_id()),
            program.type_registry.lookup("symbol")
        );
    }

    #[test]
    fn top_level_declarations_keep_their_order_before_component_declarations() {
        let program = collect_program(
            ".decl Before(x: number)\n\
             .comp C { .decl Inner(x: number) }\n.init c = C\n\
             .decl After(x: number)",
            FileId::new(0),
        )
        .unwrap();
        let names: Vec<_> = program.relations().iter().map(Relation::name).collect();
        assert_eq!(names, ["before", "after", "c\u{b7}inner"]);
    }

    #[test]
    fn collect_program_rejects_duplicate_component_decl() {
        assert_err!(
            collect_program(
                ".comp C { .decl R(x: number) }\n.init c = C\n.init c = C",
                FileId::new(0),
            ),
            ParseError::DuplicateDecl { .. }
        );
    }

    #[test]
    fn duplicate_outputs_from_sibling_components_are_rejected() {
        assert_err!(
            collect_program(
                ".comp Source { .decl R(x: number) }\n\
                 .comp Writer { .output source.R }\n\
                 .init source = Source\n\
                 .init a = Writer\n\
                 .init b = Writer",
                FileId::new(0),
            ),
            ParseError::DuplicateDirective { .. }
        );
    }

    #[test]
    fn init_rules_keep_source_order() {
        let program = collect_program(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output b
            .comp C {
              .decl s(x: number)
              .decl t(x: number)
              t(X) :- s(X).
            }
            .comp Empty { }
            a(X) :- b(X).
            .init c = C
            .init empty = Empty
            b(X) :- a(X).
            .init d = C
            ",
            FileId::new(0),
        )
        .unwrap();
        let heads: Vec<&str> = program.rules().iter().map(|r| r.head().name()).collect();
        assert_eq!(heads, ["a", "c\u{b7}t", "b", "d\u{b7}t"]);
    }
}
