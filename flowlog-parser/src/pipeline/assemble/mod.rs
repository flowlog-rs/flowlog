//! The assemble stage: build a [`Program`] from a fully-included source
//! string.
//!
//! [`collect_program`] parses the source and walks the top-level nodes (one
//! match arm per construct), then runs the assembly sub-phases in a fixed
//! order: component inlining, directive application, conflict validation, dot
//! normalization, assignment substitution, reference validation, fact
//! extraction. `validate` holds the checks; `inline` and `substitute` the
//! rewrites.

mod inline;
mod substitute;
mod validate;

use std::collections::HashMap;
use std::mem;

use flowlog_common::FileId;
use flowlog_common::Span;
use pest::Parser;

use crate::FlowLogParser;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::ast::FlowLogRule;
use crate::ast::Head;
use crate::declaration::CompDecl;
use crate::declaration::ExternFn;
use crate::declaration::InitDecl;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::Relation;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::program::InlineFact;
use crate::program::Program;
use crate::segment::LoopBlock;
use crate::segment::Segment;
use crate::types::TypeRegistry;

/// Seal `pending` rules into a `Segment::Plain` appended to `out`, emptying
/// `pending`. Does nothing when `pending` is empty.
#[inline]
fn flush_rules(pending: &mut Vec<FlowLogRule>, out: &mut Vec<Segment>) {
    if !pending.is_empty() {
        out.push(Segment::Plain(mem::take(pending)));
    }
}

/// Parse a fully-included source string (after [`resolve_includes`] has run)
/// and assemble it into a [`Program`].
///
/// Rules and loop/fixpoint blocks become [`Segment`]s in source order: each
/// loop or fixpoint block is its own segment, and every run of plain rules
/// between blocks (or trailing after the last) is sealed into one
/// `Segment::Plain`.
pub(super) fn collect_program(
    source: &str,
    extended: bool,
    file: FileId,
) -> Result<Program, ParseError> {
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, source)
        .map_err(|e| ParseError::syntax_from_pest(&e, file))?;
    let parsed_rule = pairs
        .next()
        .ok_or_else(|| grammar_bug("no parsed rule found"))?;

    // Resolve `.type` decls first so a forward-referenced
    // `.decl R(x: NodeId)` works.
    let mut type_registry = TypeRegistry::from_type_declarations(parsed_rule.clone(), file)?;

    let mut relations: Vec<Relation> = Vec::new();
    let mut decl_spans: HashMap<String, (String, Span)> = HashMap::new();
    let mut input_directives: Vec<InputDirective> = Vec::new();
    let mut output_directives: Vec<OutputDirective> = Vec::new();
    let mut printsize_directives: Vec<PrintSizeDirective> = Vec::new();
    let mut udfs: Vec<ExternFn> = Vec::new();
    let mut udf_spans: HashMap<String, Span> = HashMap::new();
    let mut raw_facts: Vec<FlowLogRule> = Vec::new();
    let mut current_rules: Vec<FlowLogRule> = Vec::new();
    let mut segments: Vec<Segment> = Vec::new();
    let mut comps: HashMap<String, CompDecl> = HashMap::new();
    // Each `.init` is recorded with the `segments.len()` value at the
    // point it appeared, so the inliner's output rules splice in at
    // that exact position: preserving source-order use-before-def.
    let mut inits_at_pos: Vec<(InitDecl, usize)> = Vec::new();

    for node in parsed_rule.into_inner() {
        let node_rule = node.as_rule();
        match node_rule {
            // --- Schema ---
            Rule::declaration => {
                let rel = Relation::from_parsed_rule_with_registry(
                    Node::new(node, file),
                    &type_registry,
                )?;
                if let Some((_prev_raw, prior)) = decl_spans.get(rel.name()) {
                    return Err(ParseError::DuplicateDecl {
                        span: rel.span(),
                        prior: *prior,
                        name: rel.raw_name().to_string(),
                    });
                }
                decl_spans.insert(
                    rel.name().to_string(),
                    (rel.raw_name().to_string(), rel.span()),
                );
                relations.push(rel);
            }
            Rule::extern_fn => {
                let ext = ExternFn::from_parsed_rule(Node::new(node, file), &type_registry)?;
                if let Some(prior) = udf_spans.get(ext.name()) {
                    return Err(ParseError::DuplicateExternFn {
                        span: ext.span(),
                        prior: *prior,
                        name: ext.name().to_string(),
                    });
                }
                udf_spans.insert(ext.name().to_string(), ext.span());
                udfs.push(ext);
            }
            Rule::type_alias_decl => {} // handled by TypeRegistry::from_type_declarations
            Rule::comp_decl => {
                let comp = CompDecl::from_parsed_rule(Node::new(node, file))?;
                comps.insert(comp.name.clone(), comp);
            }
            Rule::init_decl => {
                // Flush any pending rules before recording the init's
                // position so the position points just after them.
                flush_rules(&mut current_rules, &mut segments);
                let init = InitDecl::from_parsed_rule(Node::new(node, file))?;
                inits_at_pos.push((init, segments.len()));
            }

            // --- I/O directives ---
            Rule::input_directive => {
                input_directives.push(InputDirective::from_parsed_rule(Node::new(node, file))?)
            }
            Rule::output_directive => {
                output_directives.push(OutputDirective::from_parsed_rule(Node::new(node, file))?)
            }
            Rule::printsize_directive => printsize_directives
                .push(PrintSizeDirective::from_parsed_rule(Node::new(node, file))?),

            // --- Rules and loop/fixpoint blocks ---
            // A rule carries its own trailing `.plan`; there is no separate
            // plan node to track.
            Rule::rule => {
                current_rules.extend(FlowLogRule::expand_from_parsed_rule(node, file)?);
            }
            Rule::loop_block => {
                let block = LoopBlock::from_parsed_rule(Node::new(node, file))?;
                if !extended {
                    return Err(ParseError::LoopBlockInStandardMode { span: block.span() });
                }
                flush_rules(&mut current_rules, &mut segments);
                segments.push(Segment::Loop(block));
            }
            Rule::fixpoint_block => {
                let block = LoopBlock::from_parsed_rule(Node::new(node, file))?;
                if !extended {
                    return Err(ParseError::LoopBlockInStandardMode { span: block.span() });
                }
                flush_rules(&mut current_rules, &mut segments);
                segments.push(Segment::Fixpoint(block));
            }

            // --- Ground facts ---
            Rule::fact => {
                let head_node = node
                    .into_inner()
                    .next()
                    .ok_or_else(|| grammar_bug("fact missing head"))?;
                raw_facts.push(FlowLogRule::new(
                    Head::from_parsed_rule(Node::new(head_node, file))?,
                    vec![],
                ));
            }

            // include_directive nodes should never appear here: all
            // `.include` lines were replaced with their file contents by
            // `resolve_includes` before this source was parsed.
            Rule::include_directive => {
                return Err(grammar_bug(
                    "unexpected include_directive in parsed tree; includes should have been resolved before parsing",
                ));
            }

            _ => {}
        }
    }

    // Seal any rules that trail after the last loop block.
    flush_rules(&mut current_rules, &mut segments);

    // Expand `.comp` / `.init` into prefixed primitive forms.
    // Each init's emitted rules splice into `segments` at the
    // position recorded when the `.init` was parsed; rules
    // referencing the init's relations must appear *after* the
    // `.init` in source order, otherwise the stratifier catches
    // them as forward references.
    // Every top-level `.init` is visible to every other at global
    // scope, so a rule inside one instance may reference a sibling's
    // relations (e.g. `basic.SubtypeOf`). Map each global instance
    // name to its prefix (which, at global scope, is the name itself).
    let global_instances: HashMap<String, String> = inits_at_pos
        .iter()
        .map(|(init, _)| (init.instance.to_lowercase(), init.instance.clone()))
        .collect();
    let global_decls: HashMap<String, String> = HashMap::new();
    let mut shift = 0usize;
    for (init, pos) in inits_at_pos {
        let mut out = inline::InlinerOutput::default();
        inline::inline_one(
            "",
            &global_instances,
            &global_decls,
            init,
            &mut comps,
            &mut out,
            &mut type_registry,
        )?;
        for rel in out.relations {
            if let Some((_prev_raw, prior)) = decl_spans.get(rel.name()) {
                return Err(ParseError::DuplicateDecl {
                    span: rel.span(),
                    prior: *prior,
                    name: rel.raw_name().to_string(),
                });
            }
            decl_spans.insert(
                rel.name().to_string(),
                (rel.raw_name().to_string(), rel.span()),
            );
            relations.push(rel);
        }
        raw_facts.extend(out.facts);
        // Comp-internal directives that targeted an enclosing/global
        // relation are applied with the top-level directives, against
        // the full relation set.
        input_directives.extend(out.input_directives);
        output_directives.extend(out.output_directives);
        printsize_directives.extend(out.printsize_directives);
        if !out.rules.is_empty() {
            segments.insert(pos + shift, Segment::Plain(out.rules));
            shift += 1;
        }
    }

    validate::apply_directives(
        &mut relations,
        input_directives,
        output_directives,
        printsize_directives,
    )?;
    validate::validate_output_printsize_exclusion(&relations)?;
    validate::validate_loop_conditions(&segments, &relations)?;

    inline::normalize_dots(&mut relations, &mut segments, &mut raw_facts);

    // Eliminate equality assignments (`v = expr`) by substitution before the
    // catalog/planner, which ground variables only through positive atoms. An
    // all-assignment rule is left body-less for the fold stage to materialize
    // as a fact.
    substitute::substitute_assignments(&mut segments)?;

    validate::validate_relation_references(&relations, &segments, &raw_facts)?;

    let mut program = Program {
        relations,
        segments,
        udfs,
        type_registry,
        facts: HashMap::new(),
    };
    for fact in raw_facts {
        extract_fact(&mut program, fact)?;
    }

    Ok(program)
}

/// Insert a ground-tuple fact into `program.facts`, keyed by its relation name.
fn extract_fact(program: &mut Program, fact_rule: FlowLogRule) -> Result<(), ParseError> {
    let (rel_name, fact) = InlineFact::from_rule(&fact_rule)?;
    program.facts.entry(rel_name).or_default().push(fact);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_err;
    // `DuplicateDecl` / `DuplicateExternFn` / `LoopBlockInStandardMode` are
    // produced inline while assembling the program, so `collect_program` is
    // their producing function.

    #[test]
    fn collect_program_rejects_duplicate_decl() {
        // `Edge` collides with `edge`: declaration names are case-insensitive.
        assert_err!(
            collect_program(
                ".decl edge(x: number)\n.decl Edge(y: number)\n",
                true,
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
                true,
                FileId::new(0),
            ),
            ParseError::DuplicateExternFn { .. }
        );
    }

    #[test]
    fn collect_program_rejects_loop_block_in_standard_mode() {
        // extended = false -> `loop`/`fixpoint` blocks require Extended mode.
        assert_err!(
            collect_program(
                ".decl edge(x: number, y: number)\n.output edge\nfixpoint { edge(X, Y) :- edge(X, Y). }\n",
                false,
                FileId::new(0),
            ),
            ParseError::LoopBlockInStandardMode { .. }
        );
    }

    /// Parse and assemble `src` in extended mode, panicking on failure.
    fn assembled(src: &str) -> Program {
        collect_program(src, true, FileId::new(0)).expect("assembly should succeed")
    }

    /// Rules and loop blocks land in `segments` in source order, with a
    /// fixpoint block as its own segment between the surrounding rule groups.
    #[test]
    fn segments_preserve_source_order_with_fixpoint_isolated() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output c
            a(X) :- b(X).
            fixpoint { b(X) :- a(X). }
            c(X) :- a(X).
            ",
        );
        let items = program.segments();
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 1));
        assert!(matches!(&items[1], Segment::Fixpoint(_)));
        assert!(matches!(&items[2], Segment::Plain(r) if r.len() == 1));
    }

    /// Consecutive top-level rules collapse into a single `Plain` segment
    /// before a loop barrier.
    #[test]
    fn multiple_rules_before_loop_form_one_segment() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(1) :- b(1).
            a(2) :- b(2).
            fixpoint { a(X) :- b(X). }
            ",
        );
        let items = program.segments();
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 2));
        assert!(matches!(&items[1], Segment::Fixpoint(_)));
    }

    /// Rules emitted by a `.init` splice into the segment sequence at the
    /// position the `.init` held in source order, between the surrounding
    /// rule segments.
    #[test]
    fn init_rules_splice_at_source_position() {
        let program = assembled(
            "
            .decl a(x: number)
            .decl b(x: number)
            .output b
            .comp C {
              .decl s(x: number)
              .decl t(x: number)
              t(X) :- s(X).
            }
            a(X) :- b(X).
            .init c = C
            b(X) :- a(X).
            ",
        );
        let heads: Vec<Vec<&str>> = program
            .segments()
            .iter()
            .map(|seg| seg.as_rules().iter().map(|r| r.head().name()).collect())
            .collect();
        assert_eq!(
            heads,
            vec![vec!["a"], vec!["c\u{b7}t"], vec!["b"]],
            "init-emitted rules must sit between the surrounding rule segments"
        );
    }
}
