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
    declaration::{
        CompDecl, ExternFn, InitDecl, InputDirective, OutputDirective, PrintSizeDirective,
        RawTypeOp, Relation, split_type_alias,
    },
    error::{DirectiveKind, ParseError, grammar_bug},
    inliner,
    logic::{FlowLogRule, Head, LoopBlock, Predicate, consume_plan_directive},
    primitive::TypeRegistry,
    segment::Segment,
};
use flowlog_common::{FileId, SourceMap, Span};
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
    /// Filenames of relations marked `.output` whose `.decl` was
    /// pruned out of `relations` (and out of the dataflow) because
    /// they had no rules and no facts.
    ///
    /// Each entry is the relation's
    /// [`Relation::output_file_name`] result — usually
    /// `<RawName>.csv`, or the user-supplied `filename=` parameter.
    /// They produce no tuples at runtime, but Soufflé's contract is
    /// that every `.output R` materialises a file — empty for an
    /// empty relation — so downstream readers that iterate declared
    /// outputs don't fail on a missing path. The compiler's I/O sink
    /// uses this list to `File::create` a zero-byte file per entry,
    /// with no buffer / inspector / drain attached.
    ///
    /// Distinct from [`Self::output_idbs`], which lists relations
    /// that ARE in the dataflow and emit through the normal drain.
    empty_output_files: Vec<String>,
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
        // Materialize orphan references only *after* pruning, so the empty-fact
        // entries we add can't disable pruning's "no outputs/facts ⇒ keep all"
        // shortcut, and so we never materialize a relation that pruning dropped.
        program.materialize_orphan_relations();

        debug!("\n{}", program);
        info!("Successfully parsed program from '{}'.", path);

        Ok(program)
    }

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// Look up a declared relation by fingerprint.
    ///
    /// This is the bridge from codegen's fingerprint world back to the
    /// declaration — primarily so human-facing output (profiler labels,
    /// diagnostics) can show the user's original spelling
    /// ([`Relation::raw_name`]) instead of the canonical internal name.
    /// Linear scan: callers are codegen-time, never on the data path.
    #[must_use]
    pub fn relation_by_fingerprint(&self, fp: u64) -> Option<&Relation> {
        self.relations.iter().find(|rel| rel.fingerprint() == fp)
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
    pub fn file_backed_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_file_backed())
            .collect()
    }

    #[cfg(test)]
    #[must_use]
    pub fn inline_fact_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| self.has_inline_facts(rel.name()))
            .collect()
    }

    /// Ordered EDB relation names (sorted lexicographically).
    #[must_use]
    pub fn edb_names(&self) -> Vec<String> {
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
    pub fn edb_fingerprints(&self) -> HashSet<u64> {
        self.edbs().iter().map(|rel| rel.fingerprint()).collect()
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

    /// IDB relations annotated with `.output`, in declaration order.
    #[must_use]
    #[inline]
    pub fn output_idbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.output()).collect()
    }

    /// Filenames (relative to `-D <outdir>`) of `.output`-marked
    /// relations whose `.decl` was pruned out of the dataflow (no
    /// rules, no facts). The compiler touches an empty file per
    /// entry. See [`Self::empty_output_files`] field doc for the
    /// full rationale.
    #[must_use]
    #[inline]
    pub fn empty_output_files(&self) -> &[String] {
        &self.empty_output_files
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

    /// Mutable version of [`segments`](Self::segments).
    pub fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }

    /// Mutable access to inline ground facts — only used by the typechecker's
    /// lowering pass to rewrite polymorphic literals to their concrete
    /// declared types.
    pub fn facts_mut(&mut self) -> &mut HashMap<String, Vec<(Span, Vec<ConstType>)>> {
        &mut self.facts
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
    pub fn has_inline_facts(&self, relation_name: &str) -> bool {
        self.facts.contains_key(relation_name)
    }

    /// External UDF declarations.
    #[must_use]
    #[inline]
    pub fn udfs(&self) -> &[ExternFn] {
        &self.udfs
    }

    /// Split-borrow used by the subtype pass: registry is read while
    /// segments are mutated in place. Going through a method lets the
    /// borrow checker see the two fields are disjoint.
    #[inline]
    pub fn registry_and_segments_mut(&mut self) -> (&TypeRegistry, &mut [Segment]) {
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
        out.push(Segment::Plain(std::mem::take(pending)));
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
/// inside loop/fixpoint blocks. Shared by the dot-normalization pass
/// and the UDF reclassification pass.
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
        super::desugar::desugar_equality_assignments(&mut segments, &mut raw_facts)?;

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
            .map(|d| d.raw_name().to_string())
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
                // Display-only: show the user's spelling (`underived`
                // keeps canonical names for the membership tests above).
                let mut sorted: Vec<&str> = self
                    .relations
                    .iter()
                    .filter(|r| underived.contains(r.name()))
                    .map(Relation::raw_name)
                    .collect();
                sorted.sort_unstable();
                parts.push(format!(
                    "  underived IDBs (declared but no rules): {}",
                    sorted.join(", ")
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

        // Capture `.output`-marked relations that prune is about to drop
        // (declared but never derived). They stay out of the dataflow,
        // but the compiler still writes an empty file for each —
        // Soufflé's "every .output produces a file" contract. Filenames
        // honor any user-supplied `filename=` param, falling back to
        // `<RawName>.csv`.
        self.empty_output_files = self
            .relations
            .iter()
            .filter(|d| d.output() && !needed_preds.contains(d.name()))
            .map(Relation::output_file_name)
            .collect();

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
    use crate::ComparisonOperator;
    use crate::DataType;
    use crate::HeadArg;
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

    fn find_relation<'a>(program: &'a Program, name: &str) -> &'a Relation {
        program
            .relations()
            .iter()
            .find(|r| r.name() == name)
            .unwrap_or_else(|| panic!("relation `{name}` not found"))
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
    fn duplicate_extern_fn_rejected() {
        let err = parse_program_result(
            "
            .extern fn f(x: int64) -> int64
            .extern fn f(y: int64) -> int64
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::DuplicateExternFn { .. }),
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

    /// `.input Edge` with no parens / no params is accepted and
    /// resolves to Soufflé defaults: file-backed, `Edge.facts`, TAB.
    #[test]
    fn bare_input_directive_uses_souffle_defaults() {
        let src = "
            .decl Edge(a: symbol, b: symbol)
            .input Edge
        ";
        let program = parse_program(src);
        let edge = find_relation(&program, "edge");
        assert!(edge.has_input(), "bare .input attaches params");
        assert!(edge.is_file_backed(), "absent IO= defaults to file");
        assert_eq!(edge.input_file_name(), "Edge.facts");
        assert_eq!(edge.input_delimiter(), "\t");
    }

    #[test]
    fn multi_head_rule_expands() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .output b
            .output c
            b(X), c(X) :- a(X).
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
            fixpoint { b(X, Y), c(X, Y) :- a(X, Y). }
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
    fn disjunction_arm_can_be_a_conjunction() {
        // `(A, B ; C, D)` means `(A AND B) OR (C AND D)` — each arm
        // of a `;` may itself be a comma-separated conjunction.
        let src = "
            .decl a(x: number) .decl b(x: number)
            .decl c(x: number) .decl d(x: number)
            .decl r(x: number)
            .output r
            r(X) :- ( a(X), b(X) ; c(X), d(X) ).
        ";
        let program = parse_program(src);
        let rules = program.rules();
        assert_eq!(rules.len(), 2);
        let bodies: Vec<Vec<&str>> = rules
            .iter()
            .map(|r| r.rhs().iter().map(|p| p.name()).collect())
            .collect();
        assert!(bodies.contains(&vec!["a", "b"]));
        assert!(bodies.contains(&vec!["c", "d"]));
    }

    #[test]
    fn nested_disjunctions_cross_product() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl d(x: number)
            .decl r(x: number)
            .output r
            r(X) :- ( a(X) ; b(X) ), ( c(X) ; d(X) ).
        ";
        let program = parse_program(src);
        let rules = program.rules();
        assert_eq!(rules.len(), 4);
        let bodies: Vec<(&str, &str)> = rules
            .iter()
            .map(|r| (r.rhs()[0].name(), r.rhs()[1].name()))
            .collect();
        assert!(bodies.contains(&("a", "c")));
        assert!(bodies.contains(&("a", "d")));
        assert!(bodies.contains(&("b", "c")));
        assert!(bodies.contains(&("b", "d")));
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
            c(X), d(X) :- a(X); b(X).
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
        let r = find_relation(&program, "r");
        assert_eq!(
            r.data_type(),
            vec![DataType::Int32, DataType::Int32, DataType::Int32]
        );
    }

    // =============================================================
    // `.override` / `overridable`
    // =============================================================
    //
    // The inliner replaces user-written `.` with `·` (U+00B7) in
    // prefixed relation names, so a `.init s = Sub` produces facts
    // keyed by `s·foo` rather than `s.foo`. The tests below assert
    // against the post-inliner form.

    /// Extract the first-column integer values from `program`'s facts
    /// for `rel`. Used by the override tests, which assert by tuple
    /// value rather than by parsed-AST structure.
    fn fact_numbers(program: &Program, rel: &str) -> Vec<i64> {
        program
            .facts()
            .get(rel)
            .unwrap_or_else(|| panic!("no facts for `{rel}`"))
            .iter()
            .map(|(_, tuple)| match &tuple[0] {
                ConstType::Int(n) => *n,
                other => panic!("expected number in `{rel}`, got {other:?}"),
            })
            .collect()
    }

    /// `.output R` and `.printsize R` on the same relation conflict —
    /// both target `R.csv`, the second would silently clobber the
    /// first. Reject at parse time.
    #[test]
    fn output_and_printsize_on_same_relation_rejected() {
        let err = parse_program_result(
            "
            .decl R(x: number)
            R(1).
            .output R
            .printsize R
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::OutputAndPrintsizeConflict { ref name, .. } if name == "R"),
            "expected OutputAndPrintsizeConflict, got {err:?}"
        );
    }

    /// Order-independent: `.printsize R` before `.output R` is the
    /// same conflict and must also be rejected.
    #[test]
    fn output_and_printsize_rejected_regardless_of_order() {
        let err = parse_program_result(
            "
            .decl R(x: number)
            R(1).
            .printsize R
            .output R
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::OutputAndPrintsizeConflict { .. }),
            "expected OutputAndPrintsizeConflict, got {err:?}"
        );
    }

    /// Comp-internal directives bypass `apply_directives` (the
    /// inliner sets the flags directly), so the conflict check must
    /// run AFTER both passes. This pins that — without the post-pass
    /// validator the conflict would slip through and produce two
    /// writers racing on the same `c.R.csv` file.
    #[test]
    fn output_and_printsize_inside_comp_rejected() {
        let err = parse_program_result(
            "
            .comp C {
              .decl Src(x: number)
              .decl R(x: number)
              Src(1).
              R(x) :- Src(x).
              .output R
              .printsize R
            }
            .init c = C
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::OutputAndPrintsizeConflict { .. }),
            "expected OutputAndPrintsizeConflict for comp-internal pair, got {err:?}"
        );
    }

    /// `.output R` with no rules, no facts, and no body references
    /// is recorded in [`Program::empty_output_files`] (the
    /// compiler-side sink uses this to touch an empty file) and
    /// pruned from `relations` (so codegen doesn't try to emit a
    /// buffer for a non-existent dataflow node).
    #[test]
    fn empty_output_recorded_and_pruned_from_dataflow() {
        let program = parse_program(
            "
            .decl Nothing(x: symbol)
            .decl Src(x: symbol)
            .decl Out(x: symbol)
            Src(\"v\").
            Out(x) :- Src(x).
            .output Nothing
            .output Out
            ",
        );
        // `Nothing` is pruned from output_idbs (no rules, no facts,
        // unreferenced by any body atom).
        assert!(
            program.output_idbs().iter().all(|r| r.name() != "nothing"),
            "empty `.output` should be pruned from output_idbs, got: {:?}",
            program
                .output_idbs()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>()
        );
        // …but its filename survives in the touch list.
        assert_eq!(program.empty_output_files(), &["Nothing.csv"]);
        // `Out` flows through the normal drain path.
        assert!(program.output_idbs().iter().any(|r| r.name() == "out"));
    }

    /// `.output R(filename="custom.tsv")` overrides the default
    /// `<RawName>.csv`. The override flows all the way to the
    /// touch-file list when the relation is also pruned.
    #[test]
    fn empty_output_filename_param_honored() {
        let program = parse_program(
            "
            .decl Nothing(x: symbol)
            .output Nothing(filename=\"custom.tsv\")
            // companion derived rel to keep the dataflow non-empty so codegen works
            .decl Filled(x: symbol)
            Filled(\"v\").
            .decl Out(x: symbol)
            Out(x) :- Filled(x).
            .output Out
            ",
        );
        assert_eq!(program.empty_output_files(), &["custom.tsv"]);
    }

    /// Infix `cat` is gone — `a cat b` MUST NOT parse as a string
    /// concatenation. A future re-introduction would silently change
    /// the grammar; this test pins that.
    #[test]
    fn infix_cat_no_longer_parses() {
        let res = parse_program_result(
            "
            .decl A(x: symbol)
            .decl B(x: symbol)
            .decl C(x: symbol)
            A(\"hi\").
            B(\"there\").
            C(x cat y) :- A(x), B(y).
            ",
        );
        assert!(
            res.is_err(),
            "infix `cat` should be a parse error, but parsed: {res:?}"
        );
    }

    /// Inliner normalizes dotted instance names (`c.R` → `c·R`) on
    /// `name` for Rust ident safety, but leaves `raw_name` carrying
    /// the original literal-dot form — that's what the I/O sinks use
    /// for Soufflé-style filenames (`c.R.csv`, not `c·R.csv`).
    #[test]
    fn inlined_relation_raw_name_keeps_literal_dot() {
        let src = "
            .comp C {
              .decl R(x: symbol)
              .decl S(x: symbol)
              R(x) :- S(x).
              .output R
            }
            .init c = C
        ";
        let program = parse_program(src);
        let r = find_relation(&program, "c·r");
        assert_eq!(r.name(), "c·r");
        assert_eq!(r.raw_name(), "c.R");
    }

    // =============================================================
    // Qualified member types of nested / type-param-bound `.init`s
    // =============================================================

    /// Facet A: an attribute typed `instance.Member` resolves even when
    /// the nested `.init` that supplies `Member` is declared *after* the
    /// `.decl` in the comp body — attribute-type resolution is independent
    /// of textual order within the comp body (Soufflé-compatible).
    #[test]
    fn member_type_resolves_when_nested_init_follows_decl() {
        let src = "
            .type Value = symbol
            .comp Cfg { .type Context = symbol }
            .comp Analysis<Configuration> {
              .decl RunningThread(ctx:configuration.Context, v:Value)
              .init configuration = Configuration
            }
            .init mainAnalysis = Analysis<Cfg>
        ";
        let program = parse_program(src);
        let r = find_relation(&program, "mainanalysis·runningthread");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
    }

    /// Facet B: a base component declares relations over
    /// `configuration.Member` where `configuration` is the eventual
    /// instance of that component itself (no local `.init`), and the
    /// member `.type` is supplied by a concrete subtype. When the
    /// outermost `.init` binds the subtype, `configuration.Member`
    /// resolves to the subtype's `.type`.
    #[test]
    fn self_referential_member_type_from_concrete_subtype() {
        let src = "
            .type Value = symbol
            .type Invo = symbol
            .comp AbstractConfiguration {
              .decl ContextRequest(ctx:configuration.Context, invo:Invo)
            }
            .comp Analysis<Configuration> {
              .init configuration = Configuration
              .decl RunningThread(ctx:configuration.Context, v:Value)
            }
            .comp ConcreteConfiguration : AbstractConfiguration {
              .type Context = symbol
            }
            .init mainAnalysis = Analysis<ConcreteConfiguration>
        ";
        let program = parse_program(src);
        let req = find_relation(&program, "mainanalysis·configuration·contextrequest");
        assert_eq!(req.data_type(), vec![DataType::String, DataType::String]);
        let thread = find_relation(&program, "mainanalysis·runningthread");
        assert_eq!(thread.data_type(), vec![DataType::String, DataType::String]);
    }

    /// A component-local `.type` alias used as a *bare* (unqualified)
    /// attribute type within the same component must resolve, matching
    /// top-level `.type` alias behaviour. The alias is registered under
    /// the instance prefix, so a bare use-site must resolve against the
    /// instance-local alias table.
    #[test]
    fn comp_local_type_alias_resolves_as_attr_type() {
        let src = "
            .comp C {
              .type MethodType = symbol
              .decl R(mt:MethodType, i:number)
            }
            .init c = C
        ";
        let program = parse_program(src);
        let r = find_relation(&program, "c·r");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::Int32]);
    }

    /// A *bare* member type declared by a concrete subtype resolves in an
    /// inherited base-component `.decl`. Inheritance flattens the base body
    /// and the subtype's `.type` into one comp body, so the subtype's alias
    /// is local — the same mechanism as a plain component-local alias.
    #[test]
    fn bare_member_type_from_concrete_subtype_resolves() {
        let src = "
            .type Invo = symbol
            .comp AbstractConfiguration {
              .decl ContextRequest(ctx:Context, invo:Invo)
            }
            .comp ConcreteConfiguration : AbstractConfiguration {
              .type Context = symbol
            }
            .init c = ConcreteConfiguration
        ";
        let program = parse_program(src);
        let r = find_relation(&program, "c·contextrequest");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
    }

    /// A rule inside one component may reference a relation of a *sibling*
    /// instance declared in the enclosing (global) scope. `basic` and
    /// `main` are both global `.init`s; `basic.SubtypeOf` inside `main`'s
    /// body resolves to the global `basic·subtypeof` relation (Soufflé
    /// sibling/enclosing-scope visibility).
    #[test]
    fn sibling_instance_relation_ref_resolves() {
        let src = "
            .comp Lib { .decl SubtypeOf(a:symbol, b:symbol) }
            .init basic = Lib
            .comp Analysis {
              .decl R(x:symbol)
              R(x) :- basic.SubtypeOf(x, _).
            }
            .init main = Analysis
        ";
        let program = parse_program(src);
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "main·r")
            .expect("main·r rule");
        let body: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert!(
            body.contains(&"basic·subtypeof"),
            "sibling ref should resolve to basic·subtypeof, got {body:?}"
        );
    }

    /// `R(t) :- A(x), t = x + 1.` — the assignment grounds `t`, which is
    /// substituted into the head as an arithmetic expression; the equality
    /// literal is dropped, leaving only the positive atom.
    #[test]
    fn equality_assignment_arith_substituted_into_head() {
        let program = parse_program(
            "
            .decl A(x:number)
            .decl R(t:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            R(t) :- A(x), t = x + 1.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        assert_eq!(rule.rhs().len(), 1, "equality literal should be dropped");
        assert!(matches!(rule.rhs()[0], Predicate::PositiveAtom(_)));
        assert!(
            matches!(rule.head().head_arguments()[0], HeadArg::Arith(_)),
            "head arg should carry the substituted arithmetic"
        );
    }

    /// `R(y) :- A(x), y = x.` — a pure aliasing assignment collapses the head
    /// argument back to the source variable.
    #[test]
    fn equality_assignment_alias_substituted_into_head() {
        let program = parse_program(
            "
            .decl A(x:symbol)
            .decl R(y:symbol)
            .input A(IO=\"file\",filename=\"A.csv\")
            R(y) :- A(x), y = x.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        assert_eq!(rule.rhs().len(), 1);
        match &rule.head().head_arguments()[0] {
            HeadArg::Var(v) => assert_eq!(v, "x"),
            other => panic!("expected aliased Var(x), got {other:?}"),
        }
    }

    /// `P(t) :- t = "boolean".` — an empty-body rule whose head is fully ground
    /// after substitution is lowered to an inline fact.
    #[test]
    fn equality_assignment_const_only_becomes_fact() {
        let program = parse_program(
            "
            .decl P(t:symbol)
            P(t) :- t = \"boolean\".
            .output P
            ",
        );
        assert!(
            program.facts().contains_key("p"),
            "const-only assignment rule should become a fact"
        );
        assert_eq!(program.facts()["p"].len(), 1);
        assert!(
            program.rules().iter().all(|r| r.head().name() != "p"),
            "no derivation rule should remain for the fact relation"
        );
    }

    /// `R(x,t) :- A(x), B(t), t = x.` — `t` is already bound by `B`, so the
    /// equality is a genuine filter and must be left in the body, not treated
    /// as an assignment.
    #[test]
    fn equality_between_bound_columns_is_kept_as_filter() {
        let program = parse_program(
            "
            .decl A(x:number)
            .decl B(t:number)
            .decl R(x:number, t:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            .input B(IO=\"file\",filename=\"B.csv\")
            R(x, t) :- A(x), B(t), t = x.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        let compares = rule
            .rhs()
            .iter()
            .filter(|p| matches!(p, Predicate::Compare(_)))
            .count();
        assert_eq!(compares, 1, "filter equality must be preserved");
    }

    /// A relation that is declared and referenced by a live rule but never
    /// produced (no rule, no `.input`, no facts) is materialized as an empty
    /// inline-fact relation so codegen emits an empty collection for it.
    /// An `.input`-backed relation is NOT an orphan — its collection comes
    /// from the fact file.
    #[test]
    fn orphan_relation_referenced_by_live_rule_is_materialized_empty() {
        let program = parse_program(
            "
            .decl O(x:symbol)
            .decl I(x:symbol)
            .input I(IO=\"file\",filename=\"I.csv\")
            .decl R(x:symbol)
            R(x) :- O(x), I(x).
            .output R
            ",
        );
        assert!(
            program.facts().contains_key("o"),
            "orphan relation should be materialized"
        );
        assert!(
            program.facts()["o"].is_empty(),
            "materialized orphan must be empty"
        );
        assert!(
            !program.facts().contains_key("i"),
            ".input relation must not be materialized as an orphan"
        );
    }

    /// Substituting an assignment of a *computed* expression into a negated
    /// atom is rejected: a negated atom argument can only be a bare variable or
    /// constant, never an arithmetic expression.
    #[test]
    fn equality_assignment_into_negation_with_arith_errors() {
        let err = parse_program_result(
            "
            .decl A(x:number)
            .decl B(t:number)
            .decl R(x:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            .input B(IO=\"file\",filename=\"B.csv\")
            R(x) :- A(x), !B(t), t = x + 1.
            .output R
            ",
        )
        .expect_err("computed value into negated atom should error");
        assert!(
            matches!(err, ParseError::AssignmentVarInNegation { .. }),
            "expected AssignmentVarInNegation, got {err:?}"
        );
    }

    /// A rule whose body desugars away entirely must still leave the segment:
    /// ground integer arithmetic in the head is folded (`x = 1 + 2` → fact
    /// `P(3)`), and anything unfoldable is rejected with
    /// [`ParseError::GroundRuleNotConst`] instead of reaching the planner,
    /// which panics on zero-positive-atom rules.
    #[test]
    fn assignment_only_rule_folds_or_rejects() {
        // Foldable integer expression → inline fact P(3).
        let program = parse_program(
            "
            .decl P(x:number)
            P(x) :- x = 1 + 2.
            .output P
            ",
        );
        assert!(program.facts().contains_key("p"));
        assert!(program.rules().iter().all(|r| r.head().name() != "p"));

        // Unfoldable (builtin call) → rejected, not handed to the planner.
        let err = parse_program_result(
            "
            .decl P(s:symbol)
            P(s) :- s = cat(\"a\", \"b\").
            .output P
            ",
        )
        .expect_err("builtin in ground head should be rejected");
        assert!(
            matches!(err, ParseError::GroundRuleNotConst { .. }),
            "expected GroundRuleNotConst, got {err:?}"
        );

        // Unbound head variable with emptied body → same rejection.
        let err = parse_program_result(
            "
            .decl P(x:number)
            P(x) :- y = 1.
            .output P
            ",
        )
        .expect_err("unbound head var in ground rule should be rejected");
        assert!(
            matches!(err, ParseError::GroundRuleNotConst { .. }),
            "expected GroundRuleNotConst, got {err:?}"
        );

        // Division by zero refuses to fold → rejected, not miscomputed.
        let err = parse_program_result(
            "
            .decl P(x:number)
            P(x) :- x = 1 / 0.
            .output P
            ",
        )
        .expect_err("division by zero should be rejected");
        assert!(matches!(err, ParseError::GroundRuleNotConst { .. }));

        // Folding recurses through groups: (1 + 2) * 3 → fact P(9).
        let program = parse_program(
            "
            .decl P(x:number)
            P(x) :- x = (1 + 2) * 3.
            .output P
            ",
        );
        assert!(program.facts().contains_key("p"));
    }

    /// Chained assignments resolve to a fixpoint regardless of source order
    /// (`b = a + 2` is discovered only after `a = x + 1` grounds `a`), and the
    /// resolved values substitute into remaining comparison filters.
    #[test]
    fn chained_assignments_resolve_and_substitute_into_filters() {
        let program = parse_program(
            "
            .decl A(x:number)
            .decl R(a:number, b:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            R(a, b) :- A(x), b = a + 2, a = x + 1, b < 10.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        // Both assignments eliminated; only the `< 10` filter survives, and it
        // no longer mentions the assignment variables.
        let mut compare_vars = Vec::new();
        let mut compares = 0;
        for pred in rule.rhs() {
            if let Predicate::Compare(e) = pred {
                compares += 1;
                compare_vars.extend(e.left().vars().into_iter().cloned());
                compare_vars.extend(e.right().vars().into_iter().cloned());
            }
        }
        assert_eq!(compares, 1, "only the filter comparison remains");
        assert!(
            compare_vars.iter().all(|v| v == "x"),
            "assignment vars must be fully substituted away: {rule}"
        );
    }

    /// A multi-term assignment value spliced into a factor slot is wrapped in
    /// a `Group`, preserving fold order: `z = x * y` with `y := a - b` must
    /// mean `x * (a - b)`, not `(x * a) - b`. Same inside aggregation args.
    #[test]
    fn multi_term_substitution_wraps_in_group() {
        use crate::Factor;

        let program = parse_program(
            "
            .decl A(x:number, a:number, b:number)
            .decl R(z:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            R(z) :- A(x, a, b), y = a - b, z = x * y.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        let [HeadArg::Arith(arith)] = rule.head().head_arguments() else {
            panic!("expected one arithmetic head arg");
        };
        let (_, factor) = &arith.rest()[0];
        assert!(
            matches!(factor, Factor::Group(inner) if !inner.rest().is_empty()),
            "substituted multi-term value must be group-wrapped: {arith}"
        );

        // Aggregation arguments take the same substitution path.
        let program = parse_program(
            "
            .decl A(g:number, x:number)
            .decl S(g:number, s:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            S(g, sum(t)) :- A(g, x), t = x + 1.
            .output S
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "s")
            .expect("s rule");
        let agg = rule
            .head()
            .head_arguments()
            .iter()
            .find_map(|a| match a {
                HeadArg::Aggregation(agg) => Some(agg),
                _ => None,
            })
            .expect("aggregation head arg");
        assert!(
            !agg.arithmetic().vars().iter().any(|v| *v == "t"),
            "assignment var must be substituted inside the aggregation"
        );
    }

    /// The desugar pass also runs inside `fixpoint`/`loop` blocks.
    #[test]
    fn assignment_inside_fixpoint_desugared() {
        let program = parse_program(
            "
            .decl A(x:number)
            .decl R(t:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            fixpoint {
              R(t) :- A(x), t = x + 1.
              R(t) :- R(x), t = x + 1, x < 5.
            }
            .output R
            ",
        );
        for rule in program.rules() {
            let assignments = rule
                .rhs()
                .iter()
                .filter(|p| matches!(p, Predicate::Compare(e) if *e.operator() == ComparisonOperator::Equal))
                .count();
            assert_eq!(assignments, 0, "assignments inside blocks must desugar");
        }
    }

    /// Parentheses around a single factor are transparent: a grouped constant
    /// assignment `t = ("boolean")` lowers to a fact exactly like the bare
    /// form, a grouped assignment variable `(t) = x` is still recognized as
    /// an assignment, and a grouped bare variable substituted into a negated
    /// atom is accepted (it is not computed arithmetic).
    #[test]
    fn single_factor_groups_are_transparent_to_desugar() {
        // Grouped constant → inline fact (not an empty-bodied rule).
        let program = parse_program(
            "
            .decl P(t:symbol)
            P(t) :- t = (\"boolean\").
            .output P
            ",
        );
        assert!(
            program.facts().contains_key("p"),
            "grouped const-only assignment rule should become a fact"
        );

        // Grouped assignment variable → recognized and eliminated.
        let program = parse_program(
            "
            .decl A(x:number)
            .decl R(t:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            R(t) :- A(x), (t) = x.
            .output R
            ",
        );
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "r")
            .expect("r rule");
        assert!(
            !rule
                .rhs()
                .iter()
                .any(|p| matches!(p, Predicate::Compare(_))),
            "grouped assignment must be eliminated, not left as a filter"
        );

        // Grouped bare variable into a negated atom → fine, not arithmetic.
        parse_program(
            "
            .decl A(x:number)
            .decl B(t:number)
            .decl R(x:number)
            .input A(IO=\"file\",filename=\"A.csv\")
            .input B(IO=\"file\",filename=\"B.csv\")
            R(x) :- A(x), !B(t), t = (x).
            .output R
            ",
        );
    }

    /// An `.output`/`.input` directive *inside* a component may target a
    /// relation declared in the enclosing (global) scope. The directive
    /// resolver falls through to the global relation set — the same scope
    /// fall-through rule-body references already use.
    #[test]
    fn comp_directive_targets_global_relation() {
        let src = "
            .decl G(x:symbol)
            G(\"a\").
            .comp C {
              .decl L(x:symbol)
              L(x) :- G(x).
              .output G(IO=\"file\",filename=\"G.csv\",delimiter=\"\\t\")
            }
            .init c = C
        ";
        let program = parse_program(src);
        let g = find_relation(&program, "g");
        assert!(
            g.output(),
            ".output of a global relation from inside a comp should apply"
        );
    }

    /// Spec test 1: `.output Foo` drops parent's ground facts.
    #[test]
    fn override_drops_parent_facts() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
              Foo(2).
            }
            .comp Sub : Base {
              .override Foo
              Foo(10).
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = parse_program(src);
        assert_eq!(fact_numbers(&program, "s·foo"), vec![10]);
    }

    /// Spec test 2: override replaces a derived rule, not just facts.
    #[test]
    fn override_drops_parent_derived_rule() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              .decl Seed(x: number)
              Foo(x) :- Seed(x).
            }
            .comp Sub : Base {
              .override Foo
              Foo(x) :- Seed(x), x > 5.
            }
            .init s = Sub
            .input s.Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")
            .output s.Foo
        ";
        let program = parse_program(src);
        let rules: Vec<_> = program
            .rules()
            .into_iter()
            .filter(|r| r.head().name() == "s·foo")
            .collect();
        assert_eq!(rules.len(), 1, "exactly one s·foo rule survives");
        // The surviving rule must carry the `x > 5` filter.
        let has_compare = rules[0]
            .rhs()
            .iter()
            .any(|p| matches!(p, Predicate::Compare(_)));
        assert!(has_compare, "override's filtered rule should survive");
    }

    /// Spec test 3: `overridable` without `.override` is a no-op.
    #[test]
    fn overridable_without_override_keeps_parent_facts() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base { }
            .init s = Sub
            .output s.Foo
        ";
        let program = parse_program(src);
        let tuples = program.facts().get("s·foo").expect("s·foo facts");
        assert_eq!(tuples.len(), 1);
    }

    /// Spec test 4: `.override` of a non-`overridable` parent is an error.
    #[test]
    fn override_of_non_overridable_errors() {
        let src = "
            .comp Base { .decl Foo(x: number)  Foo(1). }
            .comp Sub : Base { .override Foo  Foo(10). }
            .init s = Sub
            .output s.Foo
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(err, ParseError::OverrideOfNonOverridable { .. }),
            "got {err:?}"
        );
    }

    /// Spec test 5: `.override` of an unknown relation name is an error.
    #[test]
    fn override_unknown_relation_errors() {
        let src = "
            .comp Base { .decl Foo(x: number) overridable  Foo(1). }
            .comp Sub : Base { .override Bar  Foo(10). }
            .init s = Sub
            .output s.Foo
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(err, ParseError::OverrideUnknownRelation { .. }),
            "got {err:?}"
        );
    }

    /// Spec test 6: diamond inheritance — Mid's override wins through to Bot.
    #[test]
    fn override_propagates_through_inheritance_chain() {
        let src = "
            .comp Top  { .decl Foo(x: number) overridable  Foo(1). }
            .comp Mid1 : Top { .override Foo  Foo(2). }
            .comp Bot  : Mid1 { }
            .init b = Bot
            .output b.Foo
        ";
        let program = parse_program(src);
        assert_eq!(fact_numbers(&program, "b·foo"), vec![2]);
    }

    /// Spec test 7: parametric — `.comp X<T>` declares `Foo(x: T) overridable`,
    /// subcomponent overrides it; type substitution must still flow through.
    #[test]
    fn override_parametric_type_substitution() {
        let src = "
            .comp Base<T> {
              .decl Foo(x: T) overridable
              Foo(0).
            }
            .comp Sub<T> : Base<T> {
              .override Foo
              Foo(42).
            }
            .init s = Sub<number>
            .output s.Foo
        ";
        let program = parse_program(src);
        assert_eq!(fact_numbers(&program, "s·foo"), vec![42]);
    }

    /// Spec test 8: `.override` with zero replacement rules drops the
    /// parent's derivations and leaves nothing in their place. (FlowLog
    /// then prunes the empty relation downstream — Soufflé would keep
    /// it; either way the override successfully cleared the parent's
    /// rules, which is what this test guards.)
    #[test]
    fn override_to_empty_drops_parent_derivations() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base {
              .override Foo
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = parse_program(src);
        assert!(program.facts().get("s·foo").is_none());
        assert!(program.rules().iter().all(|r| r.head().name() != "s·foo"));
    }

    /// Spec test 9: `.override` at the top level is a syntax error.
    #[test]
    fn override_outside_comp_is_syntax_error() {
        let src = "
            .decl Foo(x: number)
            .override Foo
            .output Foo
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(matches!(err, ParseError::Syntax { .. }), "got {err:?}");
    }

    /// Spec test 10: subcomponent that both `.override`s AND re-`.decl`s
    /// the relation must fail with `OverrideRedeclaresRelation`.
    #[test]
    fn override_redeclaration_errors() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base {
              .override Foo
              .decl Foo(x: number)
              Foo(10).
            }
            .init s = Sub
            .output s.Foo
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(err, ParseError::OverrideRedeclaresRelation { .. }),
            "got {err:?}"
        );
    }

    /// Spec test 11: two `.override Foo` directives are redundant but
    /// not an error — they collapse to a single override.
    #[test]
    fn double_override_is_accepted() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base {
              .override Foo
              .override Foo
              Foo(10).
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = parse_program(src);
        let tuples = program.facts().get("s·foo").expect("s·foo facts");
        assert_eq!(tuples.len(), 1);
    }

    /// `overridable` outside a `.comp` body must be rejected with a
    /// dedicated error rather than silently accepted.
    #[test]
    fn overridable_outside_comp_errors() {
        let src = ".decl Foo(x: number) overridable\n.output Foo\n";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(err, ParseError::OverridableOutsideComp { .. }),
            "got {err:?}"
        );
    }

    // =============================================================
    // `.plan` directive
    // =============================================================

    /// Source-level body order is permuted in the parsed AST so that
    /// downstream (planner, optimizer) sees the hinted order as if the
    /// user had written the body that way directly.
    #[test]
    fn plan_reorders_positive_atoms() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), b(X), c(X).
            .plan (3, 1, 2)
        ";
        let program = parse_program(src);
        let rule = program.rules()[0];
        let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    /// Soufflé's `.plan N:(…)` form is accepted as an alias: the
    /// leading version index is stripped and the permutation is
    /// applied just like the native form.
    #[test]
    fn plan_souffle_form_applies_permutation() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), b(X), c(X).
            .plan 1:(3, 1, 2)
        ";
        let program = parse_program(src);
        let rule = program.rules()[0];
        let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    /// `.plan` indices are positive-atom-only. Negations stay in their
    /// original global slots; the positive-atom permutation slides under
    /// them.
    #[test]
    fn plan_skips_negations_and_pins_only_positives() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl d(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), !d(X), b(X), c(X).
            .plan (3, 1, 2)
        ";
        let program = parse_program(src);
        let rule = program.rules()[0];
        let labelled: Vec<String> = rule
            .rhs()
            .iter()
            .map(|p| match p {
                Predicate::PositiveAtom(a) => a.name().to_string(),
                Predicate::NegativeAtom(a) => format!("!{}", a.name()),
                other => format!("{other}"),
            })
            .collect();
        // Positive slots [a, b, c] permute to [c, a, b]; `!d` keeps slot 1.
        assert_eq!(labelled, vec!["c", "!d", "a", "b"]);
    }

    /// `.plan` with no preceding rule clause is rejected, not silently
    /// dropped or attached to something earlier.
    #[test]
    fn plan_without_preceding_rule_errors() {
        let src = "
            .decl a(x: number)
            .output a
            .plan (1)
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(matches!(err, ParseError::PlanOrphan { .. }), "got {err:?}");
    }

    /// Index count must equal the positive-atom count.
    #[test]
    fn plan_arity_mismatch_errors() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), b(X).
            .plan (1, 2, 3)
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(
                err,
                ParseError::PlanArityMismatch {
                    expected: 2,
                    found: 3,
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    /// Index 0 or > positive-atom-count is rejected.
    #[test]
    fn plan_index_out_of_range_errors() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), b(X).
            .plan (1, 3)
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(
                err,
                ParseError::PlanIndexOutOfRange {
                    index: 3,
                    max: 2,
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    /// Each index must appear exactly once.
    #[test]
    fn plan_duplicate_index_errors() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl h(x: number)
            .output h
            h(X) :- a(X), b(X).
            .plan (1, 1)
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(
            matches!(err, ParseError::PlanDuplicateIndex { index: 1, .. }),
            "got {err:?}"
        );
    }

    /// `.plan` is honored inside `fixpoint { ... }` blocks.
    #[test]
    fn plan_inside_fixpoint_block() {
        let src = "
            .decl a(x: number)
            .decl b(x: number)
            .decl c(x: number)
            .decl h(x: number)
            .output h
            fixpoint {
                h(X) :- a(X), b(X), c(X).
                .plan (3, 1, 2)
            }
        ";
        let program = parse_program(src);
        let block = loop_blocks(&program)[0];
        let names: Vec<&str> = block.rules()[0].rhs().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    /// `.plan` inside a `.comp` body permutes the rule's positive atoms
    /// at parse time, so the inlined / prefixed rule reaches the planner
    /// already in hint order.
    #[test]
    fn plan_inside_comp_body() {
        let src = "
            .comp C {
              .decl A(x: number)
              .decl B(x: number)
              .decl D(x: number)
              .decl H(x: number)
              H(X) :- A(X), B(X), D(X).
              .plan (3, 1, 2)
            }
            .init c = C
            .output c.H
        ";
        let program = parse_program(src);
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "c·h")
            .expect("instantiated H rule");
        let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["c·d", "c·a", "c·b"]);
        assert!(rule.plan_pinned(), "plan_pinned should survive inlining");
    }

    /// `.plan` orphan inside a `.comp` is still rejected.
    #[test]
    fn plan_orphan_inside_comp_errors() {
        let src = "
            .comp C {
              .decl A(x: number)
              .plan (1)
            }
            .init c = C
            .output c.A
        ";
        let err = parse_program_result(src).unwrap_err();
        assert!(matches!(err, ParseError::PlanOrphan { .. }), "got {err:?}");
    }
}
