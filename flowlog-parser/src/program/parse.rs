use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::mem;
use std::path::Path;
use std::path::PathBuf;

use flowlog_common::FileId;
use flowlog_common::SourceMap;
use flowlog_common::Span;
use pest::Parser;
use pest::iterators::Pair;
use tracing::debug;
use tracing::info;

use super::Program;
use super::include::resolve_includes;
use crate::FlowLogParser;
use crate::Lexeme;
use crate::Rule;
use crate::declaration::CompDecl;
use crate::declaration::ExternFn;
use crate::declaration::InitDecl;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::RawTypeOp;
use crate::declaration::Relation;
use crate::declaration::split_type_alias;
use crate::desugar;
use crate::error::DirectiveKind;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::inliner;
use crate::logic::FlowLogRule;
use crate::logic::Head;
use crate::logic::LoopBlock;
use crate::logic::Predicate;
use crate::logic::consume_plan_directive;
use crate::primitive::TypeRegistry;
use crate::segment::Segment;

// =============================================================================
// Type registry pre-pass
// =============================================================================

/// Register every top-level `.type` declaration. Define-before-use:
/// a `.type` can only reference primitives or types declared earlier
/// in source. Cycles (including `.type X = X`) surface as
/// `UnknownTypeParent` because the parent isn't registered when the
/// child is processed.
///
/// `.type` declarations *inside* `.comp` bodies are intentionally
/// skipped here — the inliner registers per-instance prefixed types
/// during expansion.
fn build_type_registry(parsed_rule: Pair<Rule>, file: FileId) -> Result<TypeRegistry, ParseError> {
    let mut registry = TypeRegistry::new();
    for node in parsed_rule.into_inner() {
        if node.as_rule() != Rule::type_alias_decl {
            continue;
        }
        let (name, op, parent, span) = split_type_alias(node, file)?;
        match op {
            RawTypeOp::Subtype => {
                registry.register_subtype(&name, &parent, span)?;
            }
            RawTypeOp::Alias => {
                registry.register_alias(&name, &parent, span)?;
            }
            RawTypeOp::Tuple(fields) => {
                registry.register_tuple(&name, &fields, span)?;
            }
        }
    }
    Ok(registry)
}

// =============================================================================
// Parsing helpers
// =============================================================================

/// Seal `pending` rules into a `Segment::Plain` and append it to `out`.
///
/// Called before every loop-block barrier and after the last top-level item.
/// Does nothing if `pending` is empty.
#[inline]
fn flush_rules(pending: &mut Vec<FlowLogRule>, out: &mut Vec<Segment>) {
    if !pending.is_empty() {
        out.push(Segment::Plain(mem::take(pending)));
    }
}

/// Replace `.` with `·` (U+00B7) in every dotted relation name.
/// `·` is in Unicode's XID_Continue (so `c·holds` is a valid Rust
/// 2021 identifier) but the FlowLog grammar's `identifier` is ASCII
/// only, so it can never appear in a user-written `.decl` —
/// collisions with user names are impossible by construction.
fn normalize_inliner_dots(
    relations: &mut [Relation],
    segments: &mut [Segment],
    raw_facts: &mut [FlowLogRule],
) {
    for rel in relations.iter_mut() {
        if rel.name().contains('.') {
            let renamed = rel.raw_name().replace('.', INLINER_SEP);
            rel.set_name(renamed);
        }
    }
    for_each_rule_mut(segments, normalize_rule_dots);
    for fact in raw_facts.iter_mut() {
        normalize_rule_dots(fact);
    }
}

fn normalize_rule_dots(rule: &mut FlowLogRule) {
    let head = rule.head_mut();
    if head.name().contains('.') {
        head.set_name(head.name().replace('.', INLINER_SEP));
    }
    for pred in rule.rhs_mut() {
        if let Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) = pred
            && a.name().contains('.')
        {
            a.set_name(a.name().replace('.', INLINER_SEP));
        }
    }
}

/// Inliner-produced relation-name separator. Replaces user-written
/// `.` after inlining. See [`normalize_inliner_dots`].
const INLINER_SEP: &str = "·";

/// Apply `f` to every rule in every segment, including rules nested
/// inside loop/fixpoint blocks.
fn for_each_rule_mut<F>(segments: &mut [Segment], mut f: F)
where
    F: FnMut(&mut FlowLogRule),
{
    for seg in segments.iter_mut() {
        let rules: &mut [FlowLogRule] = match seg {
            Segment::Plain(rs) => rs.as_mut_slice(),
            Segment::Loop(b) | Segment::Fixpoint(b) => b.rules_mut(),
        };
        for rule in rules {
            f(rule);
        }
    }
}

fn check_duplicate_directives<T>(
    dirs: &[T],
    kind: DirectiveKind,
    name_of: impl Fn(&T) -> &str,
    span_of: impl Fn(&T) -> Span,
) -> Result<(), ParseError> {
    let mut seen: HashMap<&str, Span> = HashMap::new();
    for d in dirs {
        let name = name_of(d);
        let span = span_of(d);
        if let Some(prior) = seen.get(name) {
            return Err(ParseError::DuplicateDirective {
                span,
                prior: *prior,
                kind,
                name: name.to_string(),
            });
        }
        seen.insert(name, span);
    }
    Ok(())
}

// =============================================================================
// Parsing internals
// =============================================================================

impl Program {
    /// Parse a program from a file, resolving `.include` directives recursively.
    ///
    /// `.include "name.dl"` is resolved by trying, in order:
    /// 1. The including file's own directory (always tried first).
    /// 2. Each entry in `include_dirs` — extra search dirs tried after the
    ///    file's own directory. Pass `&[]` for none.
    ///
    /// Source text is loaded into `sm` so later diagnostics can cite it.
    ///
    /// If `extended` is `false`, rejects programs that contain any `loop`
    /// blocks — those require Extended Datalog mode (`--mode extend-batch`
    /// or `--mode extend-inc`).
    pub fn parse(
        path: &str,
        extended: bool,
        include_dirs: &[&Path],
        sm: &mut SourceMap,
    ) -> Result<Self, ParseError> {
        let file_path = PathBuf::from(path);
        let root_file = sm
            .load(&file_path)
            .map_err(|source| ParseError::IncludeIo {
                span: Span::DUMMY,
                path: file_path.clone(),
                source,
            })?;

        let base_dir: PathBuf = file_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));

        let mut in_progress = HashSet::new();
        let mut completed = HashSet::new();
        in_progress.insert(fs::canonicalize(&file_path).unwrap_or_else(|_| file_path.clone()));
        let combined = resolve_includes(
            sm.text(root_file).to_string(),
            root_file,
            &base_dir,
            include_dirs,
            &mut in_progress,
            &mut completed,
            sm,
        )?;

        // Re-register the combined text as the authoritative "file" we pass
        // to `from_parsed_rule` below. Spans then point into this combined
        // source; individual include files already live in `sm` for file
        // I/O errors but aren't the positions Pest reports against.
        let combined_file = sm.add(file_path.clone(), combined);

        let mut pairs = FlowLogParser::parse(Rule::main_grammar, sm.text(combined_file))
            .map_err(|e| ParseError::syntax_from_pest(&e, combined_file))?;
        let root = pairs
            .next()
            .ok_or_else(|| grammar_bug("no parsed rule found"))?;

        let mut program = Self::collect_program(root, extended, combined_file)?;
        program.prune_dead_components();
        // Materialize orphan references only *after* pruning, so the empty-fact
        // entries we add can't disable pruning's "no outputs/facts ⇒ keep all"
        // shortcut, and so we never materialize a relation that pruning dropped.
        program.materialize_orphan_relations();

        debug!("\n{}", program);
        info!("Successfully parsed program from '{}'.", path);

        Ok(program)
    }

    /// Build a [`Program`] from the top-level parse-tree node of an already
    /// fully-inlined source string (i.e. after [`resolve_includes`] has run).
    ///
    /// # Segment construction
    ///
    /// Rules and loop blocks are collected into [`Segment`]s in source order:
    ///
    /// - Consecutive `rule` nodes are accumulated in `current_rules`.
    /// - A `loop_block` is a *flush barrier*: preceding rules are sealed into a
    ///   `Segment::Plain`, then the loop is appended.
    /// - After the loop, rules resume accumulating into a fresh buffer.
    /// - Trailing rules after the last loop are sealed at the end.
    fn collect_program(
        parsed_rule: Pair<Rule>,
        extended: bool,
        file: FileId,
    ) -> Result<Self, ParseError> {
        // Resolve `.type` decls first so a forward-referenced
        // `.decl R(x: NodeId)` works.
        let mut type_registry = build_type_registry(parsed_rule.clone(), file)?;

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
        // that exact position — preserving source-order use-before-def.
        let mut inits_at_pos: Vec<(InitDecl, usize)> = Vec::new();
        // Cleared by any intervening non-rule node so a later `.plan`
        // can't silently attach to an earlier rule clause.
        let mut plan_target_start: Option<usize> = None;

        for node in parsed_rule.into_inner() {
            let node_rule = node.as_rule();
            match node_rule {
                // ── Schema ────────────────────────────────────────────────────
                Rule::declaration => {
                    let rel = Relation::from_parsed_rule_with_registry(node, file, &type_registry)?;
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
                    let ext = ExternFn::from_parsed_rule(node, file, &type_registry)?;
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
                Rule::type_alias_decl => {} // handled by build_type_registry
                Rule::comp_decl => {
                    let comp = CompDecl::from_parsed_rule(node, file)?;
                    comps.insert(comp.name.clone(), comp);
                }
                Rule::init_decl => {
                    // Flush any pending rules before recording the init's
                    // position so the position points just after them.
                    flush_rules(&mut current_rules, &mut segments);
                    let init = InitDecl::from_parsed_rule(node, file)?;
                    inits_at_pos.push((init, segments.len()));
                }

                // ── I/O directives ────────────────────────────────────────────
                Rule::input_directive => {
                    input_directives.push(InputDirective::from_parsed_rule(node, file)?)
                }
                Rule::output_directive => {
                    output_directives.push(OutputDirective::from_parsed_rule(node, file)?)
                }
                Rule::printsize_directive => {
                    printsize_directives.push(PrintSizeDirective::from_parsed_rule(node, file)?)
                }

                // ── Rules and loop/fixpoint blocks ───────────────────────────
                Rule::rule => {
                    let start = current_rules.len();
                    current_rules.extend(FlowLogRule::expand_from_parsed_rule(node, file)?);
                    plan_target_start = Some(start);
                }
                Rule::plan_directive => {
                    consume_plan_directive(node, file, &mut current_rules, &mut plan_target_start)?;
                }
                Rule::loop_block => {
                    let block = LoopBlock::from_parsed_rule(node, file)?;
                    if !extended {
                        return Err(ParseError::LoopBlockInStandardMode { span: block.span() });
                    }
                    flush_rules(&mut current_rules, &mut segments);
                    segments.push(Segment::Loop(block));
                }
                Rule::fixpoint_block => {
                    let block = LoopBlock::from_parsed_rule(node, file)?;
                    if !extended {
                        return Err(ParseError::LoopBlockInStandardMode { span: block.span() });
                    }
                    flush_rules(&mut current_rules, &mut segments);
                    segments.push(Segment::Fixpoint(block));
                }

                // ── Ground facts ──────────────────────────────────────────────
                Rule::fact => {
                    let head_node = node
                        .into_inner()
                        .next()
                        .ok_or_else(|| grammar_bug("fact missing head"))?;
                    raw_facts.push(FlowLogRule::new(
                        Head::from_parsed_rule(head_node, file)?,
                        vec![],
                    ));
                }

                // include_directive nodes should never appear here — all
                // `.include` lines were replaced with their file contents by
                // `resolve_includes` before this source was parsed.
                Rule::include_directive => {
                    return Err(grammar_bug(
                        "unexpected include_directive in parsed tree; includes should have been resolved before parsing",
                    ));
                }

                _ => {}
            }
            if !matches!(node_rule, Rule::rule | Rule::plan_directive) {
                plan_target_start = None;
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
            let mut out = inliner::InlinerOutput::default();
            inliner::inline_one(
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

        Self::apply_directives(
            &mut relations,
            input_directives,
            output_directives,
            printsize_directives,
        )?;
        Self::validate_output_printsize_exclusion(&relations)?;
        Self::validate_loop_conditions(&segments, &relations)?;

        normalize_inliner_dots(&mut relations, &mut segments, &mut raw_facts);

        // Eliminate equality assignments (`v = expr`) by substitution before the
        // catalog/planner, which ground variables only through positive atoms.
        // Constant-only rules become inline facts.
        desugar::desugar_equality_assignments(&mut segments, &mut raw_facts)?;

        let mut program = Self {
            relations,
            segments,
            udfs,
            type_registry,
            ..Self::default()
        };
        for fact in raw_facts {
            program.extract_fact(fact)?;
        }

        program.validate_relation_references()?;

        Ok(program)
    }

    /// Materialize *orphan* relations as empty inputs.
    ///
    /// A relation that is declared and referenced in some rule body, yet has no
    /// producing rule, no `.input`, and no inline facts, denotes the empty
    /// relation under Soufflé semantics (a positive reference yields nothing; a
    /// negative reference is always satisfied). The stratifier already tolerates
    /// such references; here we give them an empty inline-fact entry so codegen
    /// emits an empty collection for them instead of referencing an undefined
    /// binding.
    fn materialize_orphan_relations(&mut self) {
        let mut produced: HashSet<String> = HashSet::new();
        let mut referenced: HashSet<String> = HashSet::new();
        for segment in &self.segments {
            let rules: &[FlowLogRule] = match segment {
                Segment::Plain(rules) => rules,
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            };
            for rule in rules {
                produced.insert(rule.head().name().to_string());
                for pred in rule.rhs() {
                    if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
                        referenced.insert(atom.name().to_string());
                    }
                }
            }
        }

        let orphans: Vec<String> = self
            .relations
            .iter()
            .filter(|rel| {
                let name = rel.name();
                referenced.contains(name)
                    && !produced.contains(name)
                    && !self.facts.contains_key(name)
                    && !rel.has_input()
            })
            .map(|rel| rel.name().to_string())
            .collect();
        for name in orphans {
            self.facts.entry(name).or_default();
        }
    }

    /// Reject any rule head, body atom, or ground fact whose relation
    /// name has no matching `.decl`. Mirrors the check directives already
    /// do via [`ParseError::UndeclaredInDirective`]; covering the rule and
    /// fact paths here lets the typechecker assume every reference is
    /// declared.
    fn validate_relation_references(&self) -> Result<(), ParseError> {
        let declared: HashSet<&str> = self.relations.iter().map(|r| r.name()).collect();

        for segment in &self.segments {
            let rules: &[FlowLogRule] = match segment {
                Segment::Plain(rules) => rules,
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            };
            for rule in rules {
                let head = rule.head();
                if !declared.contains(head.name()) {
                    return Err(ParseError::UndeclaredInRule {
                        span: head.span(),
                        name: head.name().to_string(),
                    });
                }
                for pred in rule.rhs() {
                    if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred
                        && !declared.contains(atom.name())
                    {
                        return Err(ParseError::UndeclaredInRule {
                            span: atom.span(),
                            name: atom.name().to_string(),
                        });
                    }
                }
            }
        }

        for (rel_name, tuples) in &self.facts {
            if !declared.contains(rel_name.as_str()) {
                let span = tuples.first().map(|(s, _)| *s).unwrap_or(Span::DUMMY);
                return Err(ParseError::UndeclaredInFact {
                    span,
                    name: rel_name.clone(),
                });
            }
        }

        Ok(())
    }

    /// Apply `.input`, `.output`, and `.printsize` directives to `relations`.
    ///
    /// Each directive kind is checked for duplicates first, then the matching
    /// relation is updated in place. Errors if a directive names a relation with
    /// no corresponding `.decl`, or if one appears twice in the same kind.
    fn apply_directives(
        relations: &mut [Relation],
        input_directives: Vec<InputDirective>,
        output_directives: Vec<OutputDirective>,
        printsize_directives: Vec<PrintSizeDirective>,
    ) -> Result<(), ParseError> {
        check_duplicate_directives(
            &input_directives,
            DirectiveKind::Input,
            |d| d.relation_name(),
            |d| d.span(),
        )?;
        check_duplicate_directives(
            &output_directives,
            DirectiveKind::Output,
            |d| d.relation_name(),
            |d| d.span(),
        )?;
        check_duplicate_directives(
            &printsize_directives,
            DirectiveKind::PrintSize,
            |d| d.relation_name(),
            |d| d.span(),
        )?;

        for d in input_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => {
                    // Tuples are constructed by rules, never read from EDB
                    // facts — reject `.input` on a tuple-column relation here
                    // rather than panicking the fact-reader codegen later.
                    if rel.data_type().iter().any(|dt| dt.is_tuple()) {
                        return Err(ParseError::TupleInInput {
                            span: d.span(),
                            name: rel.raw_name().to_string(),
                        });
                    }
                    rel.set_input_params(d.parameters().clone());
                }
                None => {
                    return Err(ParseError::UndeclaredInDirective {
                        span: d.span(),
                        kind: DirectiveKind::Input,
                        name: d.relation_name().to_string(),
                    });
                }
            }
        }
        for d in output_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => {
                    rel.set_output(true);
                    if !d.parameters().is_empty() {
                        rel.set_output_params(d.parameters().clone())?;
                    }
                }
                None => {
                    return Err(ParseError::UndeclaredInDirective {
                        span: d.span(),
                        kind: DirectiveKind::Output,
                        name: d.relation_name().to_string(),
                    });
                }
            }
        }
        for d in printsize_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_printsize(true),
                None => {
                    return Err(ParseError::UndeclaredInDirective {
                        span: d.span(),
                        kind: DirectiveKind::PrintSize,
                        name: d.relation_name().to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Reject `.output R` + `.printsize R` on the same relation. Both
    /// target `<RawName>.csv`, so the second would silently clobber
    /// the first. Runs AFTER both the inliner and [`Self::apply_directives`]
    /// have populated `relations`, so it catches the conflict whether
    /// the directives came from the top level, from inside a `.comp`,
    /// or one from each.
    fn validate_output_printsize_exclusion(relations: &[Relation]) -> Result<(), ParseError> {
        for rel in relations {
            if rel.output() && rel.printsize() {
                return Err(ParseError::OutputAndPrintsizeConflict {
                    span: rel.span(),
                    name: rel.raw_name().to_string(),
                });
            }
        }
        Ok(())
    }

    /// Validate that every relation name appearing in a loop condition has a
    /// corresponding `.decl` declaration and is nullary (arity 0).
    ///
    /// Loop condition relations act as boolean flags: a non-empty `rel()` means
    /// the condition holds.  Non-nullary relations are rejected here so the
    /// compiler can assume `Collection<G, ()>` without an extra `.map(|_| ())`.
    fn validate_loop_conditions(
        items: &[Segment],
        relations: &[Relation],
    ) -> Result<(), ParseError> {
        let declared: HashSet<&str> = relations.iter().map(|r| r.name()).collect();

        for item in items {
            let Some(block) = item.as_loop() else {
                continue;
            };

            for directive in block.iterative_relations() {
                let name = directive.name();
                if !declared.contains(name) {
                    return Err(ParseError::UndeclaredInIterativeList {
                        span: block.span(),
                        name: name.to_string(),
                    });
                }
            }

            let Some(cond) = block.condition() else {
                continue;
            };

            let Some(until_group) = cond.until_part() else {
                continue;
            };

            for rel in until_group.relations() {
                let name = rel.name();
                if !declared.contains(name) && !declared.contains(name.to_lowercase().as_str()) {
                    return Err(ParseError::UndeclaredLoopCondition {
                        span: block.span(),
                        name: name.to_string(),
                    });
                }

                let decl = relations
                    .iter()
                    .find(|r| r.name() == name || r.name() == name.to_lowercase().as_str())
                    .ok_or_else(|| grammar_bug("already confirmed declared above"))?;
                if decl.arity() != 0 {
                    return Err(ParseError::NonNullaryLoopCondition {
                        span: block.span(),
                        name: decl.raw_name().to_string(),
                        arity: decl.arity(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Insert a ground-tuple fact into `self.facts`, preserving the head
    /// span so the typechecker can cite the offending source position.
    fn extract_fact(&mut self, fact_rule: FlowLogRule) -> Result<(), ParseError> {
        let rel_name = fact_rule.head().name().to_string();
        let span = fact_rule.head().span();
        let tuple = fact_rule.extract_constants_from_head()?;
        self.facts.entry(rel_name).or_default().push((span, tuple));
        Ok(())
    }
}
