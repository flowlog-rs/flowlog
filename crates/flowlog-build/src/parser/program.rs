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
//! [`Segment::Loop`] / [`Segment::Fixpoint`] barriers.  The stratifier
//! processes segments in source order and treats each loop or fixpoint block
//! as a hard boundary between strata groups.

use super::{
    ConstType, FlowLogParser, Lexeme, Rule,
    declaration::{ExternFn, InputDirective, OutputDirective, PrintSizeDirective, Relation},
    error::{DirectiveKind, ParseError, grammar_bug},
    logic::{Arithmetic, AtomArg, Factor, FlowLogRule, FnCall, Head, LoopBlock, Predicate},
    primitive::TypeRegistry,
    segment::Segment,
    span_of, type_ref_name,
};
use crate::common::{FileId, SourceMap, Span};
use pest::{Parser, iterators::Pair};
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
    /// Inline ground facts, keyed by relation name. Each entry is a list
    /// of `(span, tuple)` — the head span of the source `rel(c1, ...).`
    /// fact plus its constant columns.
    facts: HashMap<String, Vec<(Span, Vec<ConstType>)>>,
    /// Built during parsing, consulted by the typechecker for subtype
    /// rules, ignored downstream (subtypes are compile-time-only).
    type_registry: TypeRegistry,
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
                    Segment::Loop(block) | Segment::Fixpoint(block) => {
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
                for (_, vals) in facts {
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
    /// Source text is loaded into `sm` so later diagnostics can cite it.
    ///
    /// If `extended` is `false`, rejects programs that contain any `loop`
    /// blocks — those require Extended Datalog mode (`--mode extend-batch`
    /// or `--mode extend-inc`).
    ///
    /// For an include search path use [`Self::parse_with_includes`].
    pub fn parse(path: &str, extended: bool, sm: &mut SourceMap) -> Result<Self, ParseError> {
        Self::parse_with_includes(path, extended, &[], sm)
    }

    /// Parse a Datalog program with extra include search directories.
    ///
    /// `.include "name.dl"` is resolved by trying:
    /// 1. The parent file's directory (always tried first).
    /// 2. Each entry in `include_dirs`, in order.
    ///
    /// `parse(path, extended, sm)` is equivalent to
    /// `parse_with_includes(path, extended, &[], sm)`.
    pub fn parse_with_includes(
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

        debug!("\n{}", program);
        info!("Successfully parsed program from '{}'.", path);

        Ok(program)
    }

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub(crate) fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// EDB relations available before rule evaluation starts.
    ///
    /// This is the union of:
    /// - file-backed relations declared with `.input`
    /// - relations with inline ground facts such as `rel(1, 2).`
    ///
    /// A relation may belong to both subsets.
    #[must_use]
    pub fn edbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| self.is_edb_relation(rel))
            .collect()
    }

    #[cfg(test)]
    #[must_use]
    #[inline]
    pub(crate) fn file_backed_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_file_backed())
            .collect()
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn inline_fact_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| self.has_inline_facts(rel.name()))
            .collect()
    }

    /// Ordered EDB relation names (sorted lexicographically).
    #[must_use]
    pub(crate) fn edb_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .edbs()
            .iter()
            .map(|rel| rel.name().to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Deduplicated EDB relation fingerprints.
    #[must_use]
    pub(crate) fn edb_fingerprints(&self) -> HashSet<u64> {
        self.edbs().iter().map(|rel| rel.fingerprint()).collect()
    }

    /// IDB relations (those annotated with `.output` or `.printsize`).
    ///
    /// Returned in declaration order.
    #[must_use]
    #[inline]
    pub(crate) fn idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_output_printsize())
            .collect()
    }

    /// IDB relations annotated with `.output`, in declaration order.
    #[must_use]
    #[inline]
    pub fn output_idbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.output()).collect()
    }

    /// IDB relations annotated with `.printsize`, in declaration order.
    #[must_use]
    #[inline]
    pub fn printsize_idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.printsize())
            .collect()
    }

    /// Ordered program items (rule segments and loop blocks in source order).
    ///
    /// This is the primary representation for the stratifier.  It processes
    /// items in order and treats each `Segment::Loop` as a hard barrier.
    #[must_use]
    #[inline]
    pub(crate) fn segments(&self) -> &[Segment] {
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

    /// Mutable version of [`segments`](Self::segments).
    pub(crate) fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }

    /// Mutable access to inline ground facts — only used by the typechecker's
    /// lowering pass to rewrite polymorphic literals to their concrete
    /// declared types.
    pub(crate) fn facts_mut(&mut self) -> &mut HashMap<String, Vec<(Span, Vec<ConstType>)>> {
        &mut self.facts
    }

    /// Look up a rule by its global source-order ID.
    ///
    /// # Panics
    /// Panics if `rid` is out of bounds.
    #[must_use]
    pub(crate) fn rule(&self, rid: usize) -> &FlowLogRule {
        let mut offset = 0;
        for seg in &self.segments {
            let rules: &[FlowLogRule] = match seg {
                Segment::Plain(rules) => rules,
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
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
    pub fn facts(&self) -> &HashMap<String, Vec<(Span, Vec<ConstType>)>> {
        &self.facts
    }

    /// Whether the named relation has any inline ground facts.
    #[must_use]
    #[inline]
    pub(crate) fn has_inline_facts(&self, relation_name: &str) -> bool {
        self.facts.contains_key(relation_name)
    }

    /// External UDF declarations.
    #[must_use]
    #[inline]
    pub(crate) fn udfs(&self) -> &[ExternFn] {
        &self.udfs
    }

    /// Split-borrow used by the subtype pass: registry is read while
    /// segments are mutated in place. Going through a method lets the
    /// borrow checker see the two fields are disjoint.
    #[inline]
    pub(crate) fn registry_and_segments_mut(&mut self) -> (&TypeRegistry, &mut [Segment]) {
        (&self.type_registry, &mut self.segments)
    }

    #[inline]
    fn is_edb_relation(&self, rel: &Relation) -> bool {
        rel.has_input() || self.has_inline_facts(rel.name())
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
    source: String,
    source_file: FileId,
    base_dir: &Path,
    include_dirs: &[&Path],
    in_progress: &mut HashSet<PathBuf>,
    completed: &mut HashSet<PathBuf>,
    sm: &mut SourceMap,
) -> Result<String, ParseError> {
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, &source)
        .map_err(|e| ParseError::syntax_from_pest(&e, source_file))?;
    let root = pairs
        .next()
        .ok_or_else(|| grammar_bug("no parsed rule found"))?;

    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize; // byte offset into `source` of the last consumed position

    for node in root.into_inner() {
        if node.as_rule() != Rule::include_directive {
            continue;
        }

        let span = node.as_span();
        let directive_span = Span::new(source_file, span.start() as u32, span.end() as u32);

        // Append all source text between the previous directive and this one.
        out.push_str(&source[cursor..span.start()]);
        cursor = span.end();

        // The grammar child is the `string` token; its text includes the quotes.
        let path_node = node
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("include directive missing path"))?;
        let raw = path_node.as_str().trim_matches('"');

        let full_path = resolve_one_include(raw, base_dir, include_dirs);
        let canonical = fs::canonicalize(&full_path).unwrap_or_else(|_| full_path.clone());

        if in_progress.contains(&canonical) {
            return Err(ParseError::CircularInclude {
                span: directive_span,
                path: full_path.clone(),
                chain: in_progress.iter().cloned().collect(),
            });
        }
        if completed.contains(&canonical) {
            warn!("Skipping duplicate include '{}'.", full_path.display());
            continue;
        }

        debug!("Including '{}'.", full_path.display());
        let included_file = sm
            .load(&full_path)
            .map_err(|source| ParseError::IncludeIo {
                span: directive_span,
                path: full_path.clone(),
                source,
            })?;
        let included_source = sm.text(included_file).to_string();
        let included_base = full_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        in_progress.insert(canonical.clone());
        let inlined = resolve_includes(
            included_source,
            included_file,
            &included_base,
            include_dirs,
            in_progress,
            completed,
            sm,
        )?;
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
    Ok(out)
}

/// Resolve `raw` (the include path string) by checking the parent directory
/// first, then each entry in `include_dirs` in order. Returns the first
/// existing file. Falls back to `base_dir.join(raw)` (the original behavior)
/// if nothing matches — `resolve_includes` will then surface the I/O error
/// with the resolved path so error messages stay informative.
fn resolve_one_include(raw: &str, base_dir: &Path, include_dirs: &[&Path]) -> PathBuf {
    let parent_relative = base_dir.join(raw);
    if parent_relative.exists() {
        return parent_relative;
    }
    for dir in include_dirs {
        let candidate = dir.join(raw);
        if candidate.exists() {
            return candidate;
        }
    }
    parent_relative
}

// =============================================================================
// Type registry pre-pass
// =============================================================================

/// Register every `.type` declaration. Define-before-use: a `.type` can
/// only reference primitives or types declared earlier in source. Cycles
/// (including `.type X = X`) surface as `UnknownTypeParent` because the
/// parent isn't registered when the child is processed.
fn build_type_registry(parsed_rule: Pair<Rule>, file: FileId) -> Result<TypeRegistry, ParseError> {
    let mut registry = TypeRegistry::new();
    for node in parsed_rule.into_inner() {
        if node.as_rule() != Rule::type_alias_decl {
            continue;
        }
        let span = span_of(&node, file);
        let mut inner = node.into_inner();
        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("type_alias_decl missing name"))?
            .as_str()
            .to_string();
        let is_subtype = inner
            .next()
            .ok_or_else(|| grammar_bug("type_alias_decl missing operator"))?
            .into_inner()
            .next()
            .map(|p| p.as_rule() == Rule::subtype_op)
            .ok_or_else(|| grammar_bug("type_decl_op missing inner op"))?;
        let parent = type_ref_name(
            &inner
                .next()
                .ok_or_else(|| grammar_bug("type_alias_decl missing parent type_ref"))?,
        );
        if is_subtype {
            registry.register_subtype(&name, &parent, span)?;
        } else {
            registry.register_alias(&name, &parent, span)?;
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
        out.push(Segment::Plain(std::mem::take(pending)));
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
        let type_registry = build_type_registry(parsed_rule.clone(), file)?;

        let mut relations: Vec<Relation> = Vec::new();
        // Local to the parse loop; not threaded into the returned `Program`.
        let mut decl_spans: HashMap<String, (String, Span)> = HashMap::new();
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
                    udfs.push(ExternFn::from_parsed_rule(node, file, &type_registry)?)
                }
                Rule::type_alias_decl => {} // handled by build_type_registry

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
                    current_rules.extend(FlowLogRule::expand_from_parsed_rule(node, file)?);
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
        }

        // Seal any rules that trail after the last loop block.
        flush_rules(&mut current_rules, &mut segments);

        Self::apply_directives(
            &mut relations,
            input_directives,
            output_directives,
            printsize_directives,
        )?;
        Self::reclassify_udf_predicates(&mut segments, &udfs)?;
        Self::validate_loop_conditions(&segments, &relations)?;

        let mut program = Self {
            relations,
            segments,
            udfs,
            type_registry,
            ..Self::default()
        };
        for fact in raw_facts {
            program.extract_fact(fact);
        }

        program.validate_relation_references()?;

        Ok(program)
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
    /// Each directive kind is validated for duplicates first, then the matching
    /// relation is updated in place.  Panics if a directive names a relation that
    /// has no corresponding `.decl`, or if the same relation appears twice in the
    /// same directive kind.
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
                Some(rel) => rel.set_input_params(d.parameters().clone()),
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
                        name: name.to_string(),
                        arity: decl.arity(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Reclassify body atoms whose name matches an `.extern fn` as [`FnCall`].
    ///
    /// PEG grammars resolve `name(args...)` as `atom` before `fn_call_expr` since
    /// they share identical syntax. The distinction is semantic (declared as a
    /// relation vs. an extern function), so we correct it here after all
    /// declarations have been collected.
    ///
    /// Processes rules in all items — both top-level rule segments and rules
    /// inside loop blocks.
    fn reclassify_udf_predicates(
        items: &mut [Segment],
        udfs: &[ExternFn],
    ) -> Result<(), ParseError> {
        let udf_names: HashSet<&str> = udfs.iter().map(ExternFn::name).collect();
        if udf_names.is_empty() {
            return Ok(());
        }

        for item in items.iter_mut() {
            match item {
                Segment::Plain(rules) => Self::reclassify_rules(rules, &udf_names)?,
                Segment::Loop(block) | Segment::Fixpoint(block) => {
                    Self::reclassify_rules(block.rules_mut(), &udf_names)?
                }
            }
        }
        Ok(())
    }

    /// Rewrite rules in `rules` whose body atoms reference a UDF name.
    ///
    /// See [`Self::reclassify_udf_predicates`] for the rationale.
    fn reclassify_rules(
        rules: &mut [FlowLogRule],
        udf_names: &HashSet<&str>,
    ) -> Result<(), ParseError> {
        for rule in rules.iter_mut() {
            let needs_rewrite = rule.rhs().iter().any(|p| {
                matches!(
                    p,
                    Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a)
                    if udf_names.contains(a.name())
                )
            });
            if !needs_rewrite {
                continue;
            }

            let mut new_rhs = Vec::with_capacity(rule.rhs().len());
            for pred in rule.rhs() {
                new_rhs.push(match pred {
                    Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom)
                        if udf_names.contains(atom.name()) =>
                    {
                        let mut args = Vec::with_capacity(atom.arguments().len());
                        for a in atom.arguments() {
                            match a {
                                AtomArg::Var(v) => {
                                    args.push(Arithmetic::new(Factor::Var(v.clone()), vec![]));
                                }
                                AtomArg::Const(c) => {
                                    args.push(Arithmetic::new(Factor::Const(c.clone()), vec![]));
                                }
                                AtomArg::Placeholder => {
                                    return Err(ParseError::PlaceholderInUdf {
                                        span: atom.span(),
                                        udf_name: atom.name().to_string(),
                                    });
                                }
                            }
                        }
                        Predicate::FnCall(
                            FnCall::new(
                                atom.name().to_string(),
                                args,
                                matches!(pred, Predicate::NegativeAtom(_)),
                            )
                            .with_span(atom.span()),
                        )
                    }
                    other => other.clone(),
                });
            }

            *rule = FlowLogRule::new(rule.head().clone(), new_rhs);
        }
        Ok(())
    }

    /// Insert a ground-tuple fact into `self.facts`, preserving the head
    /// span so the typechecker can cite the offending source position.
    fn extract_fact(&mut self, fact_rule: FlowLogRule) {
        let rel_name = fact_rule.head().name().to_string();
        let span = fact_rule.head().span();
        let tuple = fact_rule.extract_constants_from_head();
        self.facts.entry(rel_name).or_default().push((span, tuple));
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
    /// Returns `((needed_rule_indices, needed_predicate_names), underived_idb_names)`.
    #[must_use]
    fn identify_needed_components(&self) -> ((HashSet<usize>, HashSet<String>), HashSet<String>) {
        // Flatten all rules (plain and loop-internal) in source order.
        let all_rules: Vec<&FlowLogRule> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            })
            .collect();

        let mut needed_preds: HashSet<String> = self
            .idbs()
            .into_iter()
            .map(|d| d.name().to_string())
            .collect();

        // Fact relations are always needed.
        needed_preds.extend(self.facts.keys().cloned());

        // Relations referenced by loop until conditions must be retained even if
        // they are not outputs. They are semantically live because the loop
        // controller reads them to decide termination.
        needed_preds.extend(
            self.segments
                .iter()
                .filter_map(Segment::as_loop)
                .flat_map(|block| {
                    block
                        .condition()
                        .and_then(|cond| cond.until_part())
                        .into_iter()
                        .flat_map(|stop| stop.relations().map(|rel| rel.name().to_string()))
                }),
        );

        // If no outputs and no facts, keep everything.
        if needed_preds.is_empty() {
            let all_indices = (0..all_rules.len()).collect();
            let all_preds = self
                .relations
                .iter()
                .map(|d| d.name().to_string())
                .collect();
            return ((all_indices, all_preds), HashSet::new());
        }

        // Map: head name -> rule indices that derive it.
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in all_rules.iter().enumerate() {
            head_to_rules
                .entry(r.head().name().to_string())
                .or_default()
                .push(i);
        }

        // Remove IDB relations that are declared (with .output/.printsize) but
        // never derived by any rule and have no facts.  They will always be
        // empty, so keeping them only causes downstream codegen errors.
        let input_relations: HashSet<String> = self
            .relations
            .iter()
            .filter(|r| r.has_input())
            .map(|r| r.name().to_string())
            .collect();
        let underived: Vec<String> = needed_preds
            .iter()
            .filter(|p| {
                !head_to_rules.contains_key(p.as_str())
                    && !self.facts.contains_key(p.as_str())
                    && !input_relations.contains(p.as_str())
            })
            .cloned()
            .collect();
        for name in &underived {
            needed_preds.remove(name);
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
                        Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => Some(a.name()),
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

        let underived: HashSet<String> = underived.into_iter().collect();
        ((needed_rules, needed_preds), underived)
    }

    /// Remove dead rules and relations in place, logging what was dropped.
    fn prune_dead_components(&mut self) {
        let ((needed_rules, needed_preds), underived) = self.identify_needed_components();

        // Collect dead relations (unreachable) and dead rules for a single
        // structured warning so the output is easy to scan.
        let dead_relations: Vec<_> = self
            .relations
            .iter()
            .filter(|d| !needed_preds.contains(d.name()) && !underived.contains(d.name()))
            .map(|d| d.name().to_string())
            .collect();

        let dead_rules: Vec<_> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            })
            .enumerate()
            .filter(|(i, _)| !needed_rules.contains(i))
            .map(|(i, r)| format!("#{}: {}", i, r))
            .collect();

        if !underived.is_empty() || !dead_relations.is_empty() || !dead_rules.is_empty() {
            let mut parts = Vec::new();
            if !underived.is_empty() {
                let mut sorted: Vec<_> = underived.iter().collect();
                sorted.sort();
                parts.push(format!(
                    "  underived IDBs (declared but no rules): {}",
                    sorted
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !dead_relations.is_empty() {
                parts.push(format!(
                    "  unreachable relations: {}",
                    dead_relations.join(", ")
                ));
            }
            if !dead_rules.is_empty() {
                parts.push(format!("  unreachable rules: {}", dead_rules.join(", ")));
            }
            warn!("Pruned dead components:\n{}", parts.join("\n"));
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
                Segment::Fixpoint(mut block) => {
                    block.rules_mut().retain(|_| {
                        let keep = needed_rules.contains(&global_idx);
                        global_idx += 1;
                        keep
                    });
                    if block.rules().is_empty() {
                        None
                    } else {
                        Some(Segment::Fixpoint(block))
                    }
                }
            })
            .collect();
        self.segments = new_items;

        self.facts
            .retain(|rel, _| needed_preds.contains(rel.as_str()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::DataType;
    use std::io::Write;

    fn loop_blocks(program: &Program) -> Vec<&LoopBlock> {
        program
            .segments()
            .iter()
            .filter_map(|s| s.as_loop())
            .collect()
    }

    fn parse_program(src: &str) -> Program {
        parse_program_result(src).expect("parse failed")
    }

    fn parse_program_result(src: &str) -> Result<Program, ParseError> {
        let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(src.as_bytes())
            .expect("failed to write temp file");
        let mut sm = SourceMap::new();
        Program::parse(&tmp.path().to_string_lossy(), true, &mut sm)
    }

    #[test]
    fn decl_case_collision_rejected() {
        let err = parse_program_result(
            "
            .decl edge(x: number)
            .decl Edge(y: number)
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::DuplicateDecl { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn attr_case_collision_rejected() {
        let err = parse_program_result(".decl edge(x: number, X: number)").unwrap_err();
        assert!(
            matches!(err, ParseError::DuplicateAttribute { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn ordering_preserved() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output c
            a(X) :- b(X).
            fixpoint { b(X) :- a(X). }
            c(X) :- a(X).
        ";
        let program = parse_program(src);
        let items = program.segments();
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 1));
        assert!(matches!(&items[1], Segment::Fixpoint(_)));
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
            fixpoint { a(X) :- b(X). }
        ";
        let program = parse_program(src);
        let items = program.segments();
        // Two rules collapse into one segment, then the fixpoint.
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], Segment::Plain(r) if r.len() == 2));
        assert!(matches!(&items[1], Segment::Fixpoint(_)));
    }

    #[test]
    fn rules_method_flattens_segments() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .output a
            a(X) :- b(X).
            fixpoint { }
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
            loop until { done } { done() :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    fn fixpoint_needs_no_declaration() {
        let src = "
            .decl edge(x: number, y: number)
            .output edge
            fixpoint { edge(1, 2) :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    fn loop_while_needs_no_declaration() {
        let src = "
            .decl edge(x: number, y: number)
            .output edge
            loop while { @it <= 9 } { edge(1, 2) :- edge(1, 2). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
    }

    #[test]
    fn iterative_declared_passes() {
        let src = "
            .decl edge(x: number, y: number)
            .decl active_edge(x: number, y: number)
            .output active_edge
            fixpoint { .iterative active_edge  active_edge(X, Y) :- edge(X, Y). }
        ";
        let program = parse_program(src);
        assert_eq!(loop_blocks(&program).len(), 1);
        assert_eq!(loop_blocks(&program)[0].iterative_relations().len(), 1);
    }

    #[test]
    fn dead_code_elimination_keeps_loop_until_relations() {
        let src = "
            .decl edge(x: number, y: number)
            .decl keep()
            .decl dead()
            .output edge

            edge(1, 2).

            loop until { keep } {
                keep() :- edge(1, 2).
            }

            dead() :- edge(2, 3).
        ";
        let program = parse_program(src);

        assert!(program.relations().iter().any(|rel| rel.name() == "keep"));
        assert!(!program.relations().iter().any(|rel| rel.name() == "dead"));
    }

    #[test]
    fn edb_subsets_track_file_backed_inline_and_overlap_relations() {
        let src = "
            .decl file_only(x: number)
            .decl fact_only(x: number)
            .decl both(x: number)
            .decl out(x: number)
            .input file_only(IO=\"file\", filename=\"file_only.csv\", delimiter=\",\")
            .input both(IO=\"file\", filename=\"both.csv\", delimiter=\",\")
            .output out

            fact_only(1).
            both(2).

            out(X) :- file_only(X).
            out(X) :- fact_only(X).
            out(X) :- both(X).
        ";
        let program = parse_program(src);

        let mut edbs = program
            .edbs()
            .into_iter()
            .map(|rel| rel.name().to_string())
            .collect::<Vec<_>>();
        edbs.sort_unstable();

        let mut file_backed = program
            .file_backed_relations()
            .into_iter()
            .map(|rel| rel.name().to_string())
            .collect::<Vec<_>>();
        file_backed.sort_unstable();

        let mut inline_facts = program
            .inline_fact_relations()
            .into_iter()
            .map(|rel| rel.name().to_string())
            .collect::<Vec<_>>();
        inline_facts.sort_unstable();

        assert_eq!(edbs, vec!["both", "fact_only", "file_only"]);
        assert_eq!(file_backed, vec!["both", "file_only"]);
        assert_eq!(inline_facts, vec!["both", "fact_only"]);
    }

    #[test]
    fn multi_head_rule_expands() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output b
            .output c
            b(X); c(X) :- a(X).
        ";
        let program = parse_program(src);
        let rules = program.rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].head().name(), "b");
        assert_eq!(rules[1].head().name(), "c");
        // Both share the same body
        assert_eq!(rules[0].rhs().len(), 1);
        assert_eq!(rules[1].rhs().len(), 1);
    }

    #[test]
    fn multi_head_rule_in_fixpoint() {
        let src = "
            .decl a(x: number, y: number)
            .decl b(x: number, y: number)
            .decl c(x: number, y: number)
            .output b
            .output c
            fixpoint { b(X, Y); c(X, Y) :- a(X, Y). }
        ";
        let program = parse_program(src);
        let blocks = loop_blocks(&program);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].rules().len(), 2);
        assert_eq!(blocks[0].rules()[0].head().name(), "b");
        assert_eq!(blocks[0].rules()[1].head().name(), "c");
    }

    #[test]
    fn multi_body_rule_expands() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output c
            c(X) :- a(X); b(X).
        ";
        let program = parse_program(src);
        let rules = program.rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].head().name(), "c");
        assert_eq!(rules[1].head().name(), "c");
        assert_eq!(rules[0].rhs()[0].name(), "a");
        assert_eq!(rules[1].rhs()[0].name(), "b");
    }

    #[test]
    fn multi_head_multi_body_expands() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl d(x: number)
            .output c
            .output d
            c(X); d(X) :- a(X); b(X).
        ";
        let program = parse_program(src);
        let rules = program.rules();
        // 2 heads × 2 bodies = 4 rules
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0].head().name(), "c");
        assert_eq!(rules[0].rhs()[0].name(), "a");
        assert_eq!(rules[1].head().name(), "c");
        assert_eq!(rules[1].rhs()[0].name(), "b");
        assert_eq!(rules[2].head().name(), "d");
        assert_eq!(rules[2].rhs()[0].name(), "a");
        assert_eq!(rules[3].head().name(), "d");
        assert_eq!(rules[3].rhs()[0].name(), "b");
    }

    /// Diamond include: `root` includes `left` and `right`, both include
    /// `leaf`. The `completed` set in `resolve_includes` must prevent
    /// `leaf` from being inlined twice — otherwise `.decl leaf_rel` would
    /// appear twice and fail with `DuplicateDecl`. This guards the warn-
    /// and-skip branch at the `completed.contains` check.
    #[test]
    fn diamond_include_dedups_leaf() {
        let dir = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, body: &str| {
            std::fs::write(dir.path().join(name), body).expect("write");
        };
        write(
            "leaf.dl",
            ".decl leaf_rel(x: number)\n.output leaf_rel\nleaf_rel(1).\n",
        );
        write("left.dl", ".include \"leaf.dl\"\n");
        write("right.dl", ".include \"leaf.dl\"\n");
        write("root.dl", ".include \"left.dl\"\n.include \"right.dl\"\n");

        let mut sm = SourceMap::new();
        let program = Program::parse(&dir.path().join("root.dl").to_string_lossy(), true, &mut sm)
            .expect("diamond include should succeed with dedup");

        let rels: Vec<_> = program
            .relations()
            .iter()
            .filter(|r| r.name() == "leaf_rel")
            .collect();
        assert_eq!(rels.len(), 1, "leaf_rel inlined twice");
    }

    /// Chained aliases: `A = B = C = number`. All four resolve to int32.
    #[test]
    fn type_alias_chain_resolves_to_root() {
        let src = "
            .type C = number
            .type B = C
            .type A = B
            .decl R(x: A, y: B, z: C)
            .output R
            R(1, 2, 3).
        ";
        let program = parse_program(src);
        let r = program
            .relations()
            .iter()
            .find(|r| r.name() == "r")
            .unwrap();
        assert_eq!(
            r.data_type(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32]
        );
    }

    /// UDF reclassification must preserve negation: `!my_udf(x)` parses
    /// first as `NegativeAtom`, then `reclassify_udf_predicates`
    /// rewrites it to `FnCall` with `is_negated = true`. A bug
    /// dropping the flag would turn a negated filter into a positive one
    /// — wrong semantics, no compile error.
    #[test]
    fn negated_udf_reclassification_preserves_negation() {
        let src = "
            .decl edge(x: number, y: number)
            .decl out(x: number, y: number)
            .output out
            .extern fn cost(x: number) -> number
            out(X, Y) :- edge(X, Y), !cost(X).
        ";
        let program = parse_program(src);
        let rule = program.rules()[0];
        let fn_call = rule
            .rhs()
            .iter()
            .find_map(|p| match p {
                Predicate::FnCall(fc) => Some(fc),
                _ => None,
            })
            .expect("udf body atom should be reclassified to FnCall");
        assert!(
            fn_call.is_negated(),
            "negation lost during reclassification"
        );
        assert_eq!(fn_call.name(), "cost");
    }
}
