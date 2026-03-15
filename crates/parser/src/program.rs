//! FlowLog program representation and manipulation.
//!
//! A [`Program`] is the central data structure produced by the parser.  It holds
//! everything needed to evaluate a FlowLog Datalog program:
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`Relation`] declarations | Schema: name, attribute types, EDB/IDB role |
//! | [`Segment`]s | Rules and loop blocks in source order |
//! | UDF declarations | External scalar functions (`.extern fn`) |
//! | Inline facts | Ground tuples written directly in source (`rel(1, 2).`) |
//!
//! # File loading and include resolution
//!
//! [`Program::parse`] is the entry point for file-based loading.  It first
//! resolves all `.include "path"` directives at the text level (see
//! [`resolve_includes`]), producing a single combined source string, then parses
//! that string in one pass.  This keeps the parser itself simple — it never has
//! to merge two partially-parsed programs.
//!
//! # Segment model
//!
//! Rules are grouped into [`Segment::Plain`] segments separated by
//! [`Segment::Loop`] barriers.  The stratifier processes segments in source
//! order and treats each loop block as a hard boundary between strata groups.

use super::{
    declaration::{ExternFn, InputDirective, OutputDirective, PrintSizeDirective, Relation},
    logic::{Arithmetic, AtomArg, Factor, FlowLogRule, FnCall, Head, LoopBlock, Predicate},
    segment::Segment,
    ConstType, FlowLogParser, Lexeme, Rule,
};
use pest::{iterators::Pair, Parser};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::{fmt, fs};
use tracing::{debug, info, warn};

// =============================================================================
// Program
// =============================================================================

/// A fully-parsed FlowLog program.
///
/// Construct one with [`Program::parse`] (file path) or, in tests, via the
/// [`Lexeme`] impl on an already-parsed pest node.
///
/// ```ignore
/// let program = Program::parse("path/to/program.fl");
/// println!("{}", program);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Program {
    /// All relation declarations (`.decl`), in source order.
    relations: Vec<Relation>,
    /// Ordered sequence of [`Segment`]s (plain rule groups and loop blocks).
    ///
    /// This is the primary representation consumed by the stratifier.
    /// Source order is preserved exactly, including across included files.
    segments: Vec<Segment>,
    /// External scalar UDF declarations (`.extern fn`).
    udfs: Vec<ExternFn>,
    /// Inline ground facts, keyed by relation name.
    ///
    /// Each value is a list of constant tuples, one per `rel(c1, c2, ...).` fact.
    facts: HashMap<String, Vec<Vec<ConstType>>>,
}

// =============================================================================
// Display
// =============================================================================

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=============================================")?;
        writeln!(f, "FlowLog DATALOG PROGRAM")?;
        writeln!(f, "=============================================")?;
        writeln!(f)?;

        if !self.relations.is_empty() {
            writeln!(f, "Relations")?;
            writeln!(f, "---------------------------------------------")?;
            for rel in &self.relations {
                writeln!(f, "{}", rel)?;
            }
            writeln!(f)?;
        }

        if !self.udfs.is_empty() {
            writeln!(f, "Extern Functions")?;
            writeln!(f, "---------------------------------------------")?;
            for udf in &self.udfs {
                writeln!(f, "{}", udf)?;
            }
            writeln!(f)?;
        }

        if !self.segments.is_empty() {
            writeln!(f, "Program (source order)")?;
            writeln!(f, "---------------------------------------------")?;
            for (i, item) in self.segments.iter().enumerate() {
                match item {
                    Segment::Plain(rules) => {
                        writeln!(f, "[Segment {}]", i)?;
                        for rule in rules {
                            writeln!(f, "  {}", rule)?;
                        }
                    }
                    Segment::Loop(block) => {
                        writeln!(f, "[Loop {}]", i)?;
                        writeln!(f, "  {}", block)?;
                    }
                }
            }
            writeln!(f)?;
        }

        if !self.facts.is_empty() {
            writeln!(f, "Facts")?;
            writeln!(f, "---------------------------------------------")?;
            for (rel_name, facts) in &self.facts {
                for vals in facts {
                    let values = vals
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    writeln!(f, "{}({}).", rel_name, values)?;
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Public API
// =============================================================================

impl Program {
    /// Parse a program from a file, resolving `.include` directives recursively.
    ///
    /// Included files are resolved relative to the including file's directory.
    /// Panics on I/O errors, parse errors, or circular includes.
    #[must_use]
    pub fn parse(path: &str) -> Self {
        let file_path = Path::new(path);
        let source = fs::read_to_string(file_path)
            .unwrap_or_else(|_| panic!("Parser error: failed to read '{}'", path));
        let base_dir = file_path.parent().unwrap_or(Path::new("."));

        let mut in_progress = HashSet::new();
        let mut completed = HashSet::new();
        let combined = resolve_includes(&source, base_dir, &mut in_progress, &mut completed);

        let mut pairs = FlowLogParser::parse(Rule::main_grammar, &combined)
            .unwrap_or_else(|e| panic!("Parser error in '{}': {}", path, e));
        let root = pairs.next().expect("Parser error: no parsed rule found");

        let mut program = Self::collect_program(root);
        program.prune_dead_components();

        debug!("\n{}", program);
        info!("Successfully parsed program from '{}'.", path);

        program
    }

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// EDB relations (those with input parameters).
    #[must_use]
    #[inline]
    pub fn edbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.is_edb()).collect()
    }

    /// Ordered EDB relation names (sorted lexicographically).
    #[must_use]
    pub fn edb_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .relations
            .iter()
            .filter(|rel| rel.is_edb())
            .map(|rel| rel.name().to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Deduplicated EDB relation fingerprints.
    #[must_use]
    pub fn edb_fingerprints(&self) -> HashSet<u64> {
        self.relations
            .iter()
            .filter(|rel| rel.is_edb())
            .map(|rel| rel.fingerprint())
            .collect()
    }

    /// IDB relations (those annotated with `.output` or `.printsize`).
    ///
    /// Returned in declaration order.
    #[must_use]
    #[inline]
    pub fn idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_output_printsize())
            .collect()
    }

    /// Ordered program items (rule segments and loop blocks in source order).
    ///
    /// This is the primary representation for the stratifier.  It processes
    /// items in order and treats each `Segment::Loop` as a hard barrier.
    #[must_use]
    #[inline]
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// All top-level rules, flattened across all `Segment::Plain` segments.
    /// Does NOT include rules inside loop blocks.
    ///
    /// This is provided for backward-compatible access by callers that do not
    /// yet understand loop blocks.  Prefer [`segments`] for loop-aware processing.
    #[must_use]
    pub fn rules(&self) -> Vec<&FlowLogRule> {
        self.segments
            .iter()
            .flat_map(|item| item.as_rules())
            .collect()
    }

    /// Look up a rule by its global source-order ID.
    ///
    /// # Panics
    /// Panics if `rid` is out of bounds.
    #[must_use]
    pub fn rule(&self, rid: usize) -> &FlowLogRule {
        let mut offset = 0;
        for seg in &self.segments {
            let rules: &[FlowLogRule] = match seg {
                Segment::Plain(rules) => rules,
                Segment::Loop(block) => block.rules(),
            };
            if rid < offset + rules.len() {
                return &rules[rid - offset];
            }
            offset += rules.len();
        }
        panic!("Parser error: rule ID {rid} out of bounds");
    }

    /// Inline facts (ground tuples).
    #[must_use]
    #[inline]
    pub fn facts(&self) -> &HashMap<String, Vec<Vec<ConstType>>> {
        &self.facts
    }

    /// External UDF declarations.
    #[must_use]
    #[inline]
    pub fn udfs(&self) -> &[ExternFn] {
        &self.udfs
    }
}

// =============================================================================
// Include resolution
// =============================================================================

/// Recursively inline all `.include "path"` directives in `source`, returning
/// a single combined source string ready to be parsed in one pass.
///
/// Rather than scanning the raw text (which would require re-implementing
/// comment awareness), this function uses pest to parse `source` and locate
/// `include_directive` nodes.  The combined source is built by interleaving the
/// verbatim source spans between directives with the recursively resolved
/// content of each included file.
///
/// Two sets provide cycle detection:
///
/// - `in_progress` — files on the current DFS stack.  Re-encountering one means
///   a circular include and panics.
/// - `completed` — files fully inlined in a prior branch.  Re-encountering one
///   (diamond include) is silently skipped so its content is not duplicated.
fn resolve_includes(
    source: &str,
    base_dir: &Path,
    in_progress: &mut HashSet<PathBuf>,
    completed: &mut HashSet<PathBuf>,
) -> String {
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, source)
        .unwrap_or_else(|e| panic!("Parser error: {}", e));
    let root = pairs.next().expect("Parser error: no parsed rule found");

    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize; // byte offset into `source` of the last consumed position

    for node in root.into_inner() {
        if node.as_rule() != Rule::include_directive {
            continue;
        }

        let span = node.as_span();

        // Append all source text between the previous directive and this one.
        out.push_str(&source[cursor..span.start()]);
        cursor = span.end();

        // The grammar child is the `string` token; its text includes the quotes.
        let path_node = node
            .into_inner()
            .next()
            .expect("Parser error: include directive missing path");
        let raw = path_node.as_str().trim_matches('"');
        let full_path = base_dir.join(raw);
        let canonical = fs::canonicalize(&full_path).unwrap_or_else(|_| {
            panic!(
                "Parser error: cannot resolve include path '{}'",
                full_path.display()
            )
        });

        if in_progress.contains(&canonical) {
            panic!(
                "Parser error: circular include detected — '{}' is already being loaded",
                full_path.display()
            );
        }
        if completed.contains(&canonical) {
            warn!("Skipping duplicate include '{}'.", full_path.display());
            continue;
        }

        debug!("Including '{}'.", full_path.display());
        let included_source = fs::read_to_string(&full_path)
            .unwrap_or_else(|_| panic!("Parser error: failed to read '{}'", full_path.display()));
        let included_base = full_path.parent().unwrap_or(Path::new("."));

        in_progress.insert(canonical.clone());
        let inlined = resolve_includes(&included_source, included_base, in_progress, completed);
        in_progress.remove(&canonical);
        completed.insert(canonical);

        if out.chars().last().is_some_and(|c| !c.is_whitespace()) {
            out.push('\n');
        }
        out.push_str(&inlined);
        if out.chars().last().is_some_and(|c| !c.is_whitespace()) {
            out.push('\n');
        }
    }

    // Append any remaining source after the last include directive.
    out.push_str(&source[cursor..]);
    out
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
        out.push(Segment::Plain(std::mem::take(pending)));
    }
}

// =============================================================================
// Parsing internals
// =============================================================================

impl Program {
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
    fn collect_program(parsed_rule: Pair<Rule>) -> Self {
        let mut relations: Vec<Relation> = Vec::new();
        let mut input_directives: Vec<InputDirective> = Vec::new();
        let mut output_directives: Vec<OutputDirective> = Vec::new();
        let mut printsize_directives: Vec<PrintSizeDirective> = Vec::new();
        let mut udfs: Vec<ExternFn> = Vec::new();
        let mut raw_facts: Vec<FlowLogRule> = Vec::new();
        let mut current_rules: Vec<FlowLogRule> = Vec::new();
        let mut segments: Vec<Segment> = Vec::new();

        for node in parsed_rule.into_inner() {
            match node.as_rule() {
                // ── Schema ────────────────────────────────────────────────────
                Rule::declaration => relations.push(Relation::from_parsed_rule(node)),
                Rule::extern_fn => udfs.push(ExternFn::from_parsed_rule(node)),

                // ── I/O directives ────────────────────────────────────────────
                Rule::input_directive => {
                    input_directives.push(InputDirective::from_parsed_rule(node))
                }
                Rule::output_directive => {
                    output_directives.push(OutputDirective::from_parsed_rule(node))
                }
                Rule::printsize_directive => {
                    printsize_directives.push(PrintSizeDirective::from_parsed_rule(node))
                }

                // ── Rules and loop blocks ─────────────────────────────────────
                Rule::rule => current_rules.push(FlowLogRule::from_parsed_rule(node)),
                Rule::loop_block => {
                    flush_rules(&mut current_rules, &mut segments);
                    segments.push(Segment::Loop(LoopBlock::from_parsed_rule(node)));
                }

                // ── Ground facts ──────────────────────────────────────────────
                Rule::fact => {
                    let head_node = node
                        .into_inner()
                        .next()
                        .expect("Parser error: fact missing head");
                    raw_facts.push(FlowLogRule::new(
                        Head::from_parsed_rule(head_node),
                        vec![],
                        false,
                    ));
                }

                // include_directive nodes should never appear here — all
                // `.include` lines were replaced with their file contents by
                // `resolve_includes` before this source was parsed.
                Rule::include_directive => {
                    panic!("Parser error: unexpected include_directive in parsed tree; includes should have been resolved before parsing")
                }

                _ => {}
            }
        }

        // Seal any rules that trail after the last loop block.
        flush_rules(&mut current_rules, &mut segments);

        Self::apply_directives(
            &mut relations,
            input_directives,
            output_directives,
            printsize_directives,
        );
        Self::reclassify_udf_predicates(&mut segments, &udfs);
        Self::validate_loop_conditions(&segments, &relations);

        let mut program = Self {
            relations,
            segments,
            udfs,
            ..Self::default()
        };
        for fact in raw_facts {
            program.extract_fact(fact);
        }

        program
    }

    /// Apply `.input`, `.output`, and `.printsize` directives to `relations`.
    ///
    /// Each directive kind is validated for duplicates first, then the matching
    /// relation is updated in place.  Panics if a directive names a relation that
    /// has no corresponding `.decl`, or if the same relation appears twice in the
    /// same directive kind.
    fn apply_directives(
        relations: &mut [Relation],
        input_directives: Vec<InputDirective>,
        output_directives: Vec<OutputDirective>,
        printsize_directives: Vec<PrintSizeDirective>,
    ) {
        // Reject duplicate directives within each kind before applying any of them,
        // so errors are reported early and the state remains consistent.
        fn check_duplicates<T>(directives: &[T], name_fn: impl Fn(&T) -> &str, kind: &str) {
            let mut seen = HashSet::new();
            for d in directives {
                let name = name_fn(d);
                if !seen.insert(name) {
                    panic!("Parser error: duplicate .{kind} directive for relation '{name}'");
                }
            }
        }
        check_duplicates(&input_directives, |d| d.relation_name(), "input");
        check_duplicates(&output_directives, |d| d.relation_name(), "output");
        check_duplicates(&printsize_directives, |d| d.relation_name(), "printsize");

        // Apply each directive, panicking if the target relation is undeclared.
        for d in input_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_input_params(d.parameters().clone()),
                None => panic!(
                    "Parser error: .input directive for undeclared relation '{}' — \
                     declare it with .decl first",
                    d.relation_name()
                ),
            }
        }
        for d in output_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_output(true),
                None => panic!(
                    "Parser error: .output directive for undeclared relation '{}' — \
                     declare it with .decl first",
                    d.relation_name()
                ),
            }
        }
        for d in printsize_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_printsize(true),
                None => panic!(
                    "Parser error: .printsize directive for undeclared relation '{}' — \
                     declare it with .decl first",
                    d.relation_name()
                ),
            }
        }
    }

    /// Validate that every relation name appearing in a loop condition has a
    /// corresponding `.decl` declaration and is nullary (arity 0).
    ///
    /// Loop condition relations act as boolean flags: a non-empty `rel()` means
    /// the condition holds.  Non-nullary relations are rejected here so the
    /// compiler can assume `Collection<G, ()>` without an extra `.map(|_| ())`.
    fn validate_loop_conditions(items: &[Segment], relations: &[Relation]) {
        let declared: HashSet<&str> = relations.iter().map(|r| r.name()).collect();

        for item in items {
            let Some(block) = item.as_loop() else {
                continue;
            };

            let Some(cond) = block.condition() else {
                continue;
            };

            let Some(stop_group) = cond.stop_part() else {
                continue;
            };

            for rel in stop_group.relations() {
                let name = rel.name.as_str();
                // Relation names are stored lowercase internally (Relation::new
                // lowercases them), so compare case-insensitively.
                if !declared.contains(name) && !declared.contains(name.to_lowercase().as_str()) {
                    panic!(
                        "Parser error: loop condition references undeclared relation '{}'. \
                         Declare it with .decl first.",
                        name
                    );
                }

                // Loop condition relations must be nullary (boolean flag).
                let decl = relations
                    .iter()
                    .find(|r| r.name() == name || r.name() == name.to_lowercase().as_str())
                    .expect("already confirmed declared above");
                if decl.arity() != 0 {
                    panic!(
                        "Parser error: loop condition relation '{}' must be nullary \
                         (declared as '.decl {}()' with no arguments). \
                         Got arity {}.",
                        name,
                        decl.name(),
                        decl.arity()
                    );
                }
            }
        }
    }

    /// Reclassify body atoms whose name matches an `.extern fn` as [`FnCallPredicate`].
    ///
    /// PEG grammars resolve `name(args...)` as `atom` before `fn_call_expr` since
    /// they share identical syntax. The distinction is semantic (declared as a
    /// relation vs. an extern function), so we correct it here after all
    /// declarations have been collected.
    ///
    /// Processes rules in all items — both top-level rule segments and rules
    /// inside loop blocks.
    fn reclassify_udf_predicates(items: &mut [Segment], udfs: &[ExternFn]) {
        let udf_names: HashSet<&str> = udfs.iter().map(ExternFn::name).collect();
        if udf_names.is_empty() {
            return;
        }

        for item in items.iter_mut() {
            match item {
                Segment::Plain(rules) => Self::reclassify_rules(rules, &udf_names),
                Segment::Loop(block) => Self::reclassify_rules(block.rules_mut(), &udf_names),
            }
        }
    }

    /// Rewrite rules in `rules` whose body atoms reference a UDF name.
    ///
    /// See [`Self::reclassify_udf_predicates`] for the rationale.
    fn reclassify_rules(rules: &mut [FlowLogRule], udf_names: &HashSet<&str>) {
        for rule in rules.iter_mut() {
            let needs_rewrite = rule.rhs().iter().any(|p| {
                matches!(
                    p,
                    Predicate::PositiveAtomPredicate(a) | Predicate::NegativeAtomPredicate(a)
                    if udf_names.contains(a.name())
                )
            });
            if !needs_rewrite {
                continue;
            }

            let new_rhs = rule
                .rhs()
                .iter()
                .map(|pred| match pred {
                    Predicate::PositiveAtomPredicate(atom)
                    | Predicate::NegativeAtomPredicate(atom)
                        if udf_names.contains(atom.name()) =>
                    {
                        let args = atom
                            .arguments()
                            .iter()
                            .map(|a| match a {
                                AtomArg::Var(v) => Arithmetic::new(Factor::Var(v.clone()), vec![]),
                                AtomArg::Const(c) => {
                                    Arithmetic::new(Factor::Const(c.clone()), vec![])
                                }
                                AtomArg::Placeholder => panic!(
                                    "Parser error: placeholder '_' not allowed in UDF call '{}'",
                                    atom.name()
                                ),
                            })
                            .collect();
                        Predicate::FnCallPredicate(FnCall::new(
                            atom.name().to_string(),
                            args,
                            matches!(pred, Predicate::NegativeAtomPredicate(_)),
                        ))
                    }
                    other => other.clone(),
                })
                .collect();

            *rule = FlowLogRule::new(rule.head().clone(), new_rhs, rule.is_planning());
        }
    }

    /// Insert a ground-tuple fact into `self.facts`.
    fn extract_fact(&mut self, fact_rule: FlowLogRule) {
        let rel_name = fact_rule.head().name().to_string();
        let tuple = fact_rule.extract_constants_from_head();
        self.facts.entry(rel_name).or_default().push(tuple);
    }
}

// =============================================================================
// Dead-code elimination
// =============================================================================

/// Sentinel used in the dependency map to represent a predicate that has no
/// rule deriving it anywhere in the program — e.g. a pure `.input` relation
/// or any otherwise externally-satisfied predicate.  No rule index exists for
/// such predicates in the flattened rule list, so this is stored instead to
/// signal that DFS traversal should stop here.
const NO_TOP_LEVEL_RULE_ID: usize = usize::MAX;

impl Program {
    /// Compute the transitive closure of dependencies needed by outputs + facts.
    ///
    /// Operates on all rules across all segments (plain and loop-internal),
    /// flattened in source order.
    ///
    /// Returns `(needed_rule_indices, needed_predicate_names)` where indices are
    /// into the unified flat rule list.
    #[must_use]
    fn identify_needed_components(&self) -> (HashSet<usize>, HashSet<String>) {
        // Flatten all rules (plain and loop-internal) in source order.
        let all_rules: Vec<&FlowLogRule> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) => block.rules(),
            })
            .collect();

        let mut needed_preds: HashSet<String> = self
            .idbs()
            .into_iter()
            .map(|d| d.name().to_string())
            .collect();

        // Fact relations are always needed.
        needed_preds.extend(self.facts.keys().cloned());

        // If no outputs and no facts, keep everything.
        if needed_preds.is_empty() {
            let all_indices = (0..all_rules.len()).collect();
            let all_preds = self
                .relations
                .iter()
                .map(|d| d.name().to_string())
                .collect();
            return (all_indices, all_preds);
        }

        // Map: head name -> rule indices that derive it.
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in all_rules.iter().enumerate() {
            head_to_rules
                .entry(r.head().name().to_string())
                .or_default()
                .push(i);
        }

        // Seed: rules that define already-needed predicates.
        let mut needed_rules: HashSet<usize> = needed_preds
            .iter()
            .flat_map(|p| head_to_rules.get(p).into_iter().flatten().copied())
            .collect();

        // Build a dependency map: rule index → [(dep_rule_index, predicate_name)].
        // For predicates with no defining rule, [`NO_TOP_LEVEL_RULE_ID`] is stored as the index.
        let dep_map: HashMap<usize, Vec<(usize, String)>> = all_rules
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let deps = r
                    .rhs()
                    .iter()
                    .filter_map(|pred| match pred {
                        Predicate::PositiveAtomPredicate(a)
                        | Predicate::NegativeAtomPredicate(a) => Some(a.name()),
                        _ => None,
                    })
                    .flat_map(|atom_name| {
                        if let Some(ids) = head_to_rules.get(atom_name) {
                            ids.iter()
                                .map(|&id| (id, atom_name.to_string()))
                                .collect::<Vec<_>>()
                        } else {
                            // No top-level rule derives this predicate.
                            vec![(NO_TOP_LEVEL_RULE_ID, atom_name.to_string())]
                        }
                    })
                    .collect();
                (i, deps)
            })
            .collect();

        // DFS traversal.
        let mut processed: HashSet<usize> = HashSet::new();
        let mut stack: Vec<usize> = needed_rules.iter().copied().collect();

        while let Some(rule_id) = stack.pop() {
            if !processed.insert(rule_id) {
                continue;
            }
            for &(dep_rule_id, ref pred_name) in dep_map.get(&rule_id).into_iter().flatten() {
                needed_preds.insert(pred_name.clone());
                if dep_rule_id != NO_TOP_LEVEL_RULE_ID && !processed.contains(&dep_rule_id) {
                    needed_rules.insert(dep_rule_id);
                    stack.push(dep_rule_id);
                }
            }
        }

        (needed_rules, needed_preds)
    }

    /// Remove dead rules and relations in place, logging what was dropped.
    fn prune_dead_components(&mut self) {
        let (needed_rules, needed_preds) = self.identify_needed_components();

        let dead_relations: Vec<_> = self
            .relations
            .iter()
            .filter(|d| !needed_preds.contains(d.name()))
            .map(|d| d.name().to_string())
            .collect();
        if !dead_relations.is_empty() {
            warn!("Dead relations: {}", dead_relations.join(", "));
        }

        // Log dead rules across all segments.
        let dead_rules: Vec<_> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) => block.rules(),
            })
            .enumerate()
            .filter(|(i, _)| !needed_rules.contains(i))
            .map(|(i, r)| format!("#{}: {}", i, r))
            .collect();
        if !dead_rules.is_empty() {
            warn!("Dead rules: {}", dead_rules.join(", "));
        }

        self.relations.retain(|d| needed_preds.contains(d.name()));

        // Filter dead rules from all segments; drop any segment that becomes empty.
        let mut global_idx = 0usize;
        let new_items: Vec<Segment> = self
            .segments
            .drain(..)
            .filter_map(|item| match item {
                Segment::Plain(rules) => {
                    let filtered: Vec<FlowLogRule> = rules
                        .into_iter()
                        .filter(|_| {
                            let keep = needed_rules.contains(&global_idx);
                            global_idx += 1;
                            keep
                        })
                        .collect();
                    if filtered.is_empty() {
                        None
                    } else {
                        Some(Segment::Plain(filtered))
                    }
                }
                Segment::Loop(mut block) => {
                    block.rules_mut().retain(|_| {
                        let keep = needed_rules.contains(&global_idx);
                        global_idx += 1;
                        keep
                    });
                    if block.rules().is_empty() {
                        None
                    } else {
                        Some(Segment::Loop(block))
                    }
                }
            })
            .collect();
        self.segments = new_items;

        self.facts
            .retain(|rel, _| needed_preds.contains(rel.as_str()));
    }
}

// =============================================================================
// Lexeme trait
// =============================================================================

impl Lexeme for Program {
    /// Build a program from an already-inlined parse-tree node.
    ///
    /// For file-based parsing with include resolution use [`Program::parse`],
    /// which calls [`resolve_includes`] before invoking this.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        Self::collect_program(parsed_rule)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Lexeme, Rule};
    use pest::Parser;

    fn loop_blocks(program: &Program) -> Vec<&LoopBlock> {
        program
            .segments()
            .iter()
            .filter_map(|s| s.as_loop())
            .collect()
    }

    fn parse_program(src: &str) -> Program {
        let mut pairs = FlowLogParser::parse(Rule::main_grammar, src)
            .unwrap_or_else(|e| panic!("parse error: {e}"));
        Program::from_parsed_rule(pairs.next().unwrap())
    }

    #[test]
    fn ordering_preserved() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output c
            a(X) :- b(X).
            loop { b(X) :- a(X). }
            c(X) :- a(X).
        ";
        let program = parse_program(src);
        let items = program.segments();
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 1));
        assert!(matches!(&items[1], Segment::Loop(_)));
        assert!(matches!(&items[2], Segment::Plain(r) if r.len() == 1));
    }

    #[test]
    fn multiple_rules_before_loop_form_one_segment() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(1) :- b(1).
            a(2) :- b(2).
            loop { }
        ";
        let program = parse_program(src);
        let items = program.segments();
        // Two rules collapse into one segment, then the loop.
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 2));
        assert!(matches!(&items[1], Segment::Loop(_)));
    }

    #[test]
    fn rules_method_flattens_segments() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(X) :- b(X).
            loop { }
            a(1) :- b(1).
        ";
        let program = parse_program(src);
        // rules() flattens top-level rules only (excludes loop internals).
        assert_eq!(program.rules().len(), 2);
    }

    #[test]
    fn loop_block_with_declared_relation() {
        let src = "
            .decl done()
            .decl edge(x: number, y: number)
            .output done
            loop stop { done } { done() :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    fn loop_pure_fixpoint_needs_no_declaration() {
        let src = "
            .decl edge(x: number, y: number)
            .output edge
            loop { edge(1, 2) :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    fn loop_iter_needs_no_declaration() {
        let src = "
            .decl edge(x: number, y: number)
            .output edge
            loop continue { iter <= 9 } { edge(1, 2) :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    #[should_panic(expected = "loop condition references undeclared relation 'done'")]
    fn loop_bool_relation_undeclared_panics() {
        parse_program("loop stop { done } { }");
    }

    #[test]
    #[should_panic(expected = "loop condition relation 'done' must be nullary")]
    fn loop_relation_non_nullary_panics() {
        let src = "
            .decl done(x: number)
            .output done
            loop stop { done } { done(1) :- done(1). }
        ";
        parse_program(src);
    }
}
