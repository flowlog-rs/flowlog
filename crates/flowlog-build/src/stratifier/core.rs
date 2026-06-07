//! Core stratification logic for FlowLog programs.
//!
//! See the crate-level documentation for an overview of strata, recursion, and
//! the Extended Datalog mode.

use crate::common::Span;
use crate::parser::{
    AggregationOperator, FlowLogRule, HeadArg, IterativeDirective, LoopCondition, Predicate,
    Program, Segment,
};
use crate::stratifier::dependency_graph::DependencyGraph;
use crate::stratifier::error::StratifyError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tracing::{debug, info, warn};

// =============================================================================
// Stratifier
// =============================================================================

/// Evaluation plan for a FlowLog program, partitioned into ordered strata.
///
/// Each stratum is a slice of rule IDs that must be evaluated as a unit after
/// all preceding strata have been fully computed.  The stratifier identifies
/// two kinds of strata:
///
/// - **Non-recursive** — rules with no mutual dependency cycle; evaluated in a
///   single pass.
/// - **Recursive** — rules forming a strongly-connected component (SCC), or a
///   single rule that references its own head; evaluated recursively until a
///   fixpoint or until condition is reached.
///
/// ## Loop blocks
///
/// A `loop` block in the source program is always a **single recursive
/// stratum** with an associated [`LoopCondition`] that controls when iteration
/// stops.  All rules inside the block iterate together — there is exactly one
/// recursive stratum per `loop` block, with no further internal stratification.
/// Loop blocks act as hard evaluation barriers: the stratifier cannot move
/// rules across their boundaries.
///
/// ## Extended Datalog mode
///
/// When `extended = true` is passed to [`Stratifier::from_program`], the
/// program is validated under *Extended Datalog* semantics: any recursive
/// dependency found in plain (non-loop) rules is a hard error.  All recursion
/// must be expressed explicitly via `loop` blocks.  In standard mode
/// (`extended = false`) recursion in plain rules is allowed and handled
/// implicitly, matching classic stratified-Datalog semantics.
///
/// ## Relation metadata
///
/// After stratification, three sets of relation fingerprints are precomputed
/// for each stratum:
///
/// | Metadata | Meaning |
/// |----------|---------|
/// | *recursive* | Self-referential relations that grow monotonically each round. |
/// | *leave* | Relations whose values must be retained after the stratum finishes, because a later stratum or an IDB output needs them. |
/// | *available* | Relations that are already fully computed before this stratum begins (EDBs + leaves from all prior strata). |
#[derive(Debug, Clone)]
pub struct Stratifier {
    /// The original program being stratified.
    program: Program,

    /// Rule IDs per stratum in evaluation order.
    /// Each inner `Vec<usize>` is one stratum; IDs are global source-order indices.
    stratum: Vec<Vec<usize>>,

    /// `true` iff the corresponding stratum is recursive (parallel with `stratum`).
    is_recursive_stratum_bitmap: Vec<bool>,

    /// The loop condition for each stratum.
    ///
    /// `Some` only for loop-derived strata; `None` for SCC-derived plain strata.
    /// Parallel with `stratum`.
    stratum_loop_condition: Vec<Option<LoopCondition>>,

    /// Accumulative recursive relations per stratum.
    ///
    /// Recursive relations (head ∩ body) that are NOT marked `.iterative`.
    /// Use `Variable::new` + concat-with-self feedback semantics.
    /// Non-empty only for recursive strata.  Parallel with `stratum`.
    stratum_accumulate_recursive_relation: Vec<Vec<u64>>,

    /// Iterative recursive relations per stratum.
    ///
    /// Recursive relations (head ∩ body) that ARE marked `.iterative`.
    /// Use `Variable::new_from` + replace-only feedback semantics.
    /// Non-empty only for loop-derived strata with an `iterative` annotation.
    /// Parallel with `stratum`.
    stratum_iterative_recursive_relation: Vec<Vec<u64>>,

    /// Relation fingerprints that must be preserved after a stratum finishes.
    ///
    /// A head relation is in the leave set if it is referenced by any later
    /// stratum or is annotated as an IDB output (`.output` / `.printsize`).
    /// Parallel with `stratum`.
    stratum_leave_relation: Vec<Vec<u64>>,

    /// Relations that are fully computed before a stratum begins.
    ///
    /// Always includes all EDB relations. For stratum *i*, this is the union
    /// of the leave sets of strata *0 … i-1*.  Parallel with `stratum`.
    stratum_available_relations: Vec<HashSet<u64>>,
}

// =============================================================================
// Public API
// =============================================================================

impl Stratifier {
    /// User-facing spelling for the relation behind `fp`, for diagnostics —
    /// falls back to the given canonical name when the fingerprint has no
    /// declaration (synthesized relations).
    pub(crate) fn display_name(&self, fp: u64, canonical: &str) -> String {
        self.program
            .relation_by_fingerprint(fp)
            .map(|r| r.raw_name().to_string())
            .unwrap_or_else(|| canonical.to_string())
    }

    /// Returns `true` if stratum `idx` is recursive.
    #[must_use]
    pub(crate) fn is_recursive_stratum(&self, idx: usize) -> bool {
        self.is_recursive_stratum_bitmap[idx]
    }

    /// Returns the loop condition for a loop-derived stratum, or `None` for a
    /// plain SCC-derived stratum.
    #[must_use]
    pub(crate) fn loop_condition(&self, idx: usize) -> Option<&LoopCondition> {
        self.stratum_loop_condition[idx].as_ref()
    }

    /// Returns each stratum as a slice of rule references (resolved from IDs).
    ///
    /// Primarily useful for display and tests; prefer index-based accessors for
    /// performance-sensitive paths.
    #[must_use]
    pub fn stratum(&self) -> Vec<Vec<&FlowLogRule>> {
        self.stratum
            .iter()
            .map(|s| s.iter().map(|&rid| self.program.rule(rid)).collect())
            .collect()
    }

    /// Accumulative recursive relations for stratum `idx`.
    ///
    /// Recursive relations (head ∩ body) not marked `.iterative`.
    /// Empty for non-recursive strata.
    #[must_use]
    pub(crate) fn stratum_accumulate_recursive_relation(&self, idx: usize) -> &[u64] {
        &self.stratum_accumulate_recursive_relation[idx]
    }

    /// Iterative recursive relations for stratum `idx`.
    ///
    /// Recursive relations (head ∩ body) explicitly marked `.iterative`.
    /// Empty for non-loop strata or loops without an `iterative` annotation.
    #[must_use]
    pub(crate) fn stratum_iterative_recursive_relation(&self, idx: usize) -> &[u64] {
        &self.stratum_iterative_recursive_relation[idx]
    }

    /// Relation fingerprints whose values must be retained after stratum `idx`.
    #[must_use]
    pub(crate) fn stratum_leave_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_leave_relation[idx]
    }

    /// Relations that are fully computed before stratum `idx` begins.
    ///
    /// Always includes EDB relations and the leave sets of all earlier strata.
    #[must_use]
    pub(crate) fn stratum_available_relations(&self, idx: usize) -> &HashSet<u64> {
        &self.stratum_available_relations[idx]
    }
}

// =============================================================================
// Construction
// =============================================================================

impl Stratifier {
    /// Stratify a program, returning an ordered evaluation plan.
    ///
    /// Processes [`Segment`]s in source order:
    ///
    /// - **`Segment::Plain`** — stratified via SCC detection + merging.
    ///   Cross-segment references are treated as already-computed EDB from
    ///   prior segments.  In Extended Datalog mode (`extended = true`), any
    ///   recursive SCC here is a hard error.
    ///
    /// - **`Segment::Loop`** — always becomes exactly one recursive stratum
    ///   tagged with its [`LoopCondition`].  All rules inside the block
    ///   iterate together; no further internal stratification is performed.
    ///
    /// # Errors
    ///
    /// Returns a [`StratifyError`] when the program is structurally invalid:
    /// recursion outside a `loop`/`fixpoint` block in extended mode, a forward
    /// reference across a loop barrier, an empty recursive stratum, a
    /// malformed `.iterative` directive, or an unreachable loop condition.
    pub fn from_program(program: &Program, extended: bool) -> Result<Self, StratifyError> {
        let mut strata: Vec<Vec<usize>> = Vec::new();
        let mut bitmap: Vec<bool> = Vec::new();
        let mut loop_conditions: Vec<Option<LoopCondition>> = Vec::new();
        // Per-stratum iterative directives, preserved because the parser's
        // dead-code pass may drop the underlying relation from
        // `program.relations()` before diagnostics need its name.
        let mut iterative_rels: Vec<Vec<IterativeDirective>> = Vec::new();
        let mut id_offset = 0usize;

        let segments = program.segments();
        let mut i = 0;
        while i < segments.len() {
            match &segments[i] {
                Segment::Plain(_) => {
                    // Coalesce the maximal run of consecutive `Plain` segments
                    // and stratify it as one unit. Component instances splice
                    // into their own segment per `.init`, so a relation may be
                    // defined in a later instance than it is referenced. One
                    // SCC problem per run makes instance order irrelevant —
                    // matching Soufflé's global stratification — while
                    // `Loop`/`Fixpoint` barriers still bound each run.
                    let mut run: Vec<&[FlowLogRule]> = Vec::new();
                    let mut total = 0usize;
                    while let Some(Segment::Plain(rules)) = segments.get(i) {
                        run.push(rules);
                        total += rules.len();
                        i += 1;
                    }
                    let (seg_strata, seg_bitmap) = if let [rules] = run[..] {
                        // Single segment: stratify the slice directly.
                        Self::stratify_segment(rules, id_offset, extended)?
                    } else {
                        let combined: Vec<FlowLogRule> =
                            run.iter().flat_map(|r| r.iter().cloned()).collect();
                        Self::stratify_segment(&combined, id_offset, extended)?
                    };
                    let n = seg_strata.len();
                    strata.extend(seg_strata);
                    bitmap.extend(seg_bitmap);
                    loop_conditions.extend(std::iter::repeat_n(None, n));
                    iterative_rels.extend(std::iter::repeat_with(Vec::new).take(n));
                    id_offset += total;
                }
                Segment::Loop(block) | Segment::Fixpoint(block) => {
                    // A loop/fixpoint block is always exactly one recursive stratum.
                    // No SCC analysis is performed inside: all rules iterate
                    // together under the block's loop condition.
                    let rules = block.rules();
                    let rule_count = rules.len();

                    // Every negative edge inside a loop block is negation
                    // through recursion — the whole block is one recursive
                    // stratum, so no filter is needed.
                    let dep_graph = DependencyGraph::from_rules(rules);
                    Self::warn_negation_edges(&dep_graph, rules, id_offset, |_, _| true);

                    strata.push((id_offset..id_offset + rule_count).collect());
                    bitmap.push(true);
                    loop_conditions.push(block.condition().cloned());
                    iterative_rels.push(block.iterative_relations().to_vec());
                    id_offset += rule_count;
                    i += 1;
                }
            }
        }

        let mut instance = Self {
            program: program.clone(),
            stratum: strata,
            is_recursive_stratum_bitmap: bitmap,
            stratum_loop_condition: loop_conditions,
            stratum_accumulate_recursive_relation: Vec::new(),
            stratum_iterative_recursive_relation: Vec::new(),
            stratum_leave_relation: Vec::new(),
            stratum_available_relations: Vec::new(),
        };

        // Rules within a stratum are parallel (no inter-dependencies), so their
        // SCC-traversal order is incidental. Sort by source position so every
        // downstream consumer — plan trees, diagnostics, logs — sees rules in
        // the order the user wrote them. `program.rule()` is O(segments) per
        // lookup; precompute once so the comparator stays O(1).
        let rule_starts: Vec<u32> = program
            .segments()
            .iter()
            .flat_map(|seg| match seg {
                Segment::Plain(rules) => rules.iter().map(|r| r.span().start).collect::<Vec<_>>(),
                Segment::Loop(block) | Segment::Fixpoint(block) => {
                    block.rules().iter().map(|r| r.span().start).collect()
                }
            })
            .collect();
        for stratum in &mut instance.stratum {
            stratum.sort_by_key(|&rid| rule_starts[rid]);
        }

        instance.build_stratum_metadata(&iterative_rels)?;
        instance.validate_forward_references()?;
        instance.validate_recursive_strata()?;
        instance.validate_loop_conditions()?;
        instance.warn_aggregation();

        debug!("\n{}", instance);
        info!(
            "Successfully stratified program: produced {} strata ({} recursive)",
            instance.stratum.len(),
            instance
                .is_recursive_stratum_bitmap
                .iter()
                .filter(|&&b| b)
                .count()
        );

        Ok(instance)
    }

    /// Stratify a single `Segment::Plain` slice of rules.
    ///
    /// Rule IDs are 0-based local indices within the slice; on return they are
    /// shifted to global IDs by adding `id_offset`.
    ///
    /// Steps:
    /// 1. Build the intra-segment dependency graph.
    /// 2. Compute SCCs (Kosaraju's algorithm).
    /// 3. In Extended Datalog mode, reject any recursive SCC.
    /// 4. Emit negation-in-recursive-stratum warnings.
    /// 5. Merge dependency-free non-recursive SCCs into a single wider stratum
    ///    to reduce evaluation passes.
    fn stratify_segment(
        rules: &[FlowLogRule],
        id_offset: usize,
        extended: bool,
    ) -> Result<(Vec<Vec<usize>>, Vec<bool>), StratifyError> {
        if rules.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let dep_graph = DependencyGraph::from_rules(rules);
        let dep_map = dep_graph.dependency_map();
        let (sccs, scc_bitmap, scc_id) = Self::compute_sccs(dep_map);

        // Extended Datalog mode: recursion in plain rules is forbidden.
        // Every recursive SCC must be inside an explicit `loop` block.
        if extended {
            for (scc, &is_rec) in sccs.iter().zip(scc_bitmap.iter()) {
                if is_rec {
                    let offending: Vec<(usize, Span)> = scc
                        .iter()
                        .map(|&local| (local + id_offset, rules[local].span()))
                        .collect();
                    return Err(StratifyError::RecursionOutsideLoop {
                        rules: offending,
                        hint: "wrap these rules in `fixpoint { ... }` or another loop form",
                    });
                }
            }
        }

        // Warn when negation appears on a back-edge within a recursive SCC.
        // The filter keeps only edges where both ends are in the same recursive SCC.
        Self::warn_negation_edges(&dep_graph, rules, id_offset, |src, dst| {
            scc_id.get(&src) == scc_id.get(&dst)
                && scc_id.get(&src).is_some_and(|&idx| scc_bitmap[idx])
        });

        let (merged_sccs, merged_bitmap) = Self::merge_strata(sccs, scc_bitmap, dep_map);

        // Shift local IDs to global IDs.
        let global_strata = merged_sccs
            .into_iter()
            .map(|s| s.into_iter().map(|local| local + id_offset).collect())
            .collect();

        Ok((global_strata, merged_bitmap))
    }
}

// =============================================================================
// SCC computation (Kosaraju's algorithm)
// =============================================================================

impl Stratifier {
    /// Compute SCCs of the dependency graph using Kosaraju's two-pass DFS.
    ///
    /// Returns `(sccs, recursive_bitmap, scc_id_map)` where:
    /// - `sccs` — each SCC as a list of local rule IDs.
    /// - `recursive_bitmap` — `true` when the corresponding SCC has a cycle
    ///   (more than one rule, or a single rule with a self-dependency).
    /// - `scc_id_map` — maps each rule ID to its SCC index; used for
    ///   negation-through-recursion warnings.
    fn compute_sccs(
        dep_map: &HashMap<usize, HashSet<usize>>,
    ) -> (Vec<Vec<usize>>, Vec<bool>, HashMap<usize, usize>) {
        let n = dep_map.len();

        // Pass 1: DFS on the original graph to record nodes in reverse finish order.
        let mut order = Vec::with_capacity(n);
        let mut visited = vec![false; n];
        for &id in dep_map.keys().sorted() {
            Self::dfs_order(dep_map, &mut visited, &mut order, id);
        }
        order.reverse();

        // Pass 2: DFS on the transposed graph in reverse-finish order.
        // Each DFS tree rooted in this pass is one SCC.
        let transpose = Self::transpose(dep_map);
        let mut assigned = vec![false; n];
        let mut sccs: Vec<Vec<usize>> = Vec::new();
        for node in order {
            if !assigned[node] {
                let mut scc = Vec::new();
                Self::dfs_assign(&transpose, &mut assigned, &mut scc, node);
                sccs.push(scc);
            }
        }

        // Classify each SCC as recursive or not, and build the scc_id lookup.
        let mut scc_id: HashMap<usize, usize> = HashMap::new();
        let mut bitmap: Vec<bool> = Vec::new();
        for (idx, scc) in sccs.iter().enumerate() {
            let is_rec = scc.len() > 1
                || dep_map
                    .get(&scc[0])
                    .is_some_and(|deps| deps.contains(&scc[0]));
            bitmap.push(is_rec);
            for &rid in scc {
                scc_id.insert(rid, idx);
            }
        }

        (sccs, bitmap, scc_id)
    }

    /// Merge phase: collapse dependency-free non-recursive SCCs into a single
    /// wider stratum to reduce the total number of evaluation passes.
    ///
    /// Recursive SCCs are never merged — each keeps its own fixpoint boundary.
    ///
    /// The algorithm repeatedly peels off all SCCs that have no unresolved
    /// dependency (i.e. no dependency on a rule still in `pending`), batches
    /// the non-recursive ones into one combined stratum, and emits each
    /// recursive one as its own stratum.
    fn merge_strata(
        strata: Vec<Vec<usize>>,
        bitmap: Vec<bool>,
        dep_map: &HashMap<usize, HashSet<usize>>,
    ) -> (Vec<Vec<usize>>, Vec<bool>) {
        let mut merged: Vec<Vec<usize>> = Vec::new();
        let mut merged_bitmap: Vec<bool> = Vec::new();
        let mut pending: Vec<(Vec<usize>, bool)> = strata.into_iter().zip(bitmap).collect();

        while !pending.is_empty() {
            let remaining: HashSet<usize> = pending
                .iter()
                .flat_map(|(s, _)| s.iter().copied())
                .collect();

            let mut still: Vec<(Vec<usize>, bool)> = Vec::new();
            let mut batch_non_rec: Vec<usize> = Vec::new();
            let mut batch_rec: Vec<(Vec<usize>, bool)> = Vec::new();

            for (s, is_rec) in pending {
                // A stratum has a pending dependency if any of its rules depend on
                // a rule that is still unresolved and not inside the same stratum.
                let has_pending_dep = s.iter().any(|rid| {
                    dep_map.get(rid).is_some_and(|deps| {
                        deps.iter().any(|d| remaining.contains(d) && !s.contains(d))
                    })
                });
                if has_pending_dep {
                    still.push((s, is_rec));
                } else if is_rec {
                    batch_rec.push((s, is_rec));
                } else {
                    batch_non_rec.extend(s);
                }
            }
            pending = still;

            if !batch_non_rec.is_empty() {
                merged.push(batch_non_rec);
                merged_bitmap.push(false);
            }
            for (s, is_rec) in batch_rec {
                merged.push(s);
                merged_bitmap.push(is_rec);
            }
        }

        (merged, merged_bitmap)
    }

    /// Build the transpose (reverse adjacency map) of `dep_map`.
    fn transpose(dep_map: &HashMap<usize, HashSet<usize>>) -> HashMap<usize, HashSet<usize>> {
        let mut out: HashMap<usize, HashSet<usize>> =
            dep_map.keys().map(|&k| (k, HashSet::new())).collect();
        for (&src, dsts) in dep_map {
            for &dst in dsts {
                out.entry(dst).or_default().insert(src);
            }
        }
        out
    }

    /// DFS pass 1 (Kosaraju): visit `node` and push it onto `order` in
    /// post-order (i.e. after all descendants).
    fn dfs_order(
        dep_map: &HashMap<usize, HashSet<usize>>,
        visited: &mut [bool],
        order: &mut Vec<usize>,
        node: usize,
    ) {
        if visited[node] {
            return;
        }
        visited[node] = true;
        if let Some(children) = dep_map.get(&node) {
            for &c in children {
                Self::dfs_order(dep_map, visited, order, c);
            }
        }
        order.push(node);
    }

    /// Emit a warning for each negative edge `(src, dst)` that satisfies
    /// `include(src, dst)`.
    ///
    /// Shared by plain-rule stratification (filter: same recursive SCC) and
    /// loop-block processing (filter: always true — every edge is recursive).
    fn warn_negation_edges(
        dep_graph: &DependencyGraph,
        rules: &[FlowLogRule],
        id_offset: usize,
        include: impl Fn(usize, usize) -> bool,
    ) {
        for &(src, dst) in dep_graph.negative_edges() {
            if !include(src, dst) {
                continue;
            }
            if src == dst {
                warn!(
                    "Negation in recursive stratum (rule {} negates itself): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}",
                    src + id_offset,
                    src + id_offset,
                    rules[src]
                );
            } else {
                warn!(
                    "Negation in recursive stratum (rule {} negates rule {}): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}\n  Rule {}: {}",
                    src + id_offset,
                    dst + id_offset,
                    src + id_offset,
                    rules[src],
                    dst + id_offset,
                    rules[dst]
                );
            }
        }
    }

    /// DFS pass 2 (Kosaraju): collect all nodes reachable from `node` in the
    /// transposed graph into `scc`.
    fn dfs_assign(
        transpose: &HashMap<usize, HashSet<usize>>,
        assigned: &mut [bool],
        scc: &mut Vec<usize>,
        node: usize,
    ) {
        if assigned[node] {
            return;
        }
        assigned[node] = true;
        scc.push(node);
        if let Some(parents) = transpose.get(&node) {
            for &p in parents {
                Self::dfs_assign(transpose, assigned, scc, p);
            }
        }
    }
}

// =============================================================================
// Stratum metadata
// =============================================================================

impl Stratifier {
    /// Populate recursive, leave and available relations after the strata list is finalised.
    fn build_stratum_metadata(
        &mut self,
        iterative_rels_per_stratum: &[Vec<IterativeDirective>],
    ) -> Result<(), StratifyError> {
        // Precompute head and body-atom fingerprint sets per stratum.
        let mut heads_per_stratum: Vec<HashSet<u64>> = Vec::new();
        let mut body_atoms_per_stratum: Vec<HashSet<u64>> = Vec::new();

        for stratum in &self.stratum {
            let heads: HashSet<u64> = stratum
                .iter()
                .map(|&rid| self.program.rule(rid).head().head_fingerprint())
                .collect();
            heads_per_stratum.push(heads);

            let body_atoms: HashSet<u64> = stratum
                .iter()
                .flat_map(|&rid| {
                    self.program.rule(rid).rhs().iter().filter_map(|p| match p {
                        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                            Some(atom.fingerprint())
                        }
                        _ => None,
                    })
                })
                .collect();
            body_atoms_per_stratum.push(body_atoms);
        }

        let n = self.stratum.len();

        // IDB fingerprints — always kept in the leave set so outputs remain
        // accessible after the dataflow even if no later stratum reads them.
        let idb_fp_set: HashSet<u64> = self
            .program
            .idbs()
            .into_iter()
            .map(|r| r.fingerprint())
            .collect();

        // For each stratum, accumulate body atoms referenced by all *strictly
        // later* strata in reverse order so we can answer in one forward pass.
        let mut later_union: HashSet<u64> = HashSet::new();
        let mut later_body_atoms_per_stratum: Vec<HashSet<u64>> = vec![HashSet::new(); n];
        for i in (0..n).rev() {
            later_body_atoms_per_stratum[i] = later_union.clone();
            later_union.extend(body_atoms_per_stratum[i].iter().copied());
        }

        // Build per-stratum recursive and leave sets.
        for i in 0..n {
            let heads = &heads_per_stratum[i];
            let body_atoms = &body_atoms_per_stratum[i];
            let later_body_atoms = &later_body_atoms_per_stratum[i];

            // Recursive relations: heads that also appear in the body of the same stratum.
            // Split by iterative annotation: iterative fps go to iterative_recursive,
            // the rest go to accumulate_recursive.
            if self.is_recursive_stratum(i) {
                let iterative_rels = &iterative_rels_per_stratum[i];

                // Validate every declared-iterative relation before the split.
                let recursive_fps: HashSet<u64> = heads.intersection(body_atoms).copied().collect();
                for directive in iterative_rels {
                    let fp = directive.fp();
                    if !heads.contains(&fp) {
                        return Err(StratifyError::IterativeNotInLoopHead {
                            rel: self.display_name(fp, directive.name()),
                            decl_span: directive.span(),
                        });
                    }
                    if !recursive_fps.contains(&fp) {
                        return Err(StratifyError::IterativeNotRecursive {
                            rel: self.display_name(fp, directive.name()),
                            decl_span: directive.span(),
                        });
                    }
                }

                let iterative_fps: HashSet<u64> =
                    iterative_rels.iter().map(IterativeDirective::fp).collect();
                let mut accumulate: Vec<u64> = Vec::new();
                let mut iterative: Vec<u64> = Vec::new();
                for fp in heads.intersection(body_atoms).copied() {
                    if iterative_fps.contains(&fp) {
                        iterative.push(fp);
                    } else {
                        accumulate.push(fp);
                    }
                }
                self.stratum_accumulate_recursive_relation.push(accumulate);
                self.stratum_iterative_recursive_relation.push(iterative);
            } else {
                self.stratum_accumulate_recursive_relation.push(Vec::new());
                self.stratum_iterative_recursive_relation.push(Vec::new());
            };

            // Leave relations: head relations needed by later strata or outputs.
            let leave: Vec<u64> = heads
                .iter()
                .filter(|fp| later_body_atoms.contains(fp) || idb_fp_set.contains(fp))
                .copied()
                .collect();
            self.stratum_leave_relation.push(leave);
        }

        // Available relations: EDBs ∪ leaves from all preceding strata.
        let edb_fps = self.program.edb_fingerprints();
        let mut accumulated: HashSet<u64> = HashSet::new();
        for leave in &self.stratum_leave_relation {
            let mut available = accumulated.clone();
            available.extend(&edb_fps);
            self.stratum_available_relations.push(available);
            accumulated.extend(leave);
        }
        Ok(())
    }

    /// Validate that no stratum references an IDB relation defined only in a later stratum.
    ///
    /// Each body atom in stratum *i* must be either:
    /// - an EDB relation (always available), or
    /// - in `stratum_available_relations[i]` (derived and left by a prior stratum), or
    /// - a head of the same stratum (recursive self-reference).
    ///
    /// A body atom that fails all three is a forward reference: the relation is
    /// only defined in a later segment and will be empty when stratum *i* runs,
    /// silently producing wrong results.
    fn validate_forward_references(&self) -> Result<(), StratifyError> {
        let edb_fps = self.program.edb_fingerprints();

        // Fingerprints of every relation produced by some rule head anywhere in
        // the program. A body atom whose relation is neither an EDB nor any
        // rule's head is an *orphan*: declared but never populated. Soufflé
        // treats such a relation as simply empty (the referencing rule yields
        // nothing), so this is not a forward reference — only a relation that
        // *is* defined, but in a later stratum, qualifies.
        let defined_fps: HashSet<u64> = self
            .stratum
            .iter()
            .flatten()
            .map(|&rid| self.program.rule(rid).head().head_fingerprint())
            .collect();

        for (i, stratum) in self.stratum.iter().enumerate() {
            let available = &self.stratum_available_relations[i];
            let heads: HashSet<u64> = stratum
                .iter()
                .map(|&rid| self.program.rule(rid).head().head_fingerprint())
                .collect();

            for &rid in stratum {
                let rule = self.program.rule(rid);
                for predicate in rule.rhs() {
                    let (fp, atom_span) = match predicate {
                        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                            (atom.fingerprint(), atom.span())
                        }
                        _ => continue,
                    };
                    if edb_fps.contains(&fp)
                        || available.contains(&fp)
                        || heads.contains(&fp)
                        || !defined_fps.contains(&fp)
                    {
                        continue;
                    }
                    let rel_name = self.display_name(fp, "<unknown>");
                    // Fall back to the rule's span if the atom has no recorded
                    // position (synthesized atoms, dummies in tests).
                    let span = if atom_span.is_dummy() {
                        rule.span()
                    } else {
                        atom_span
                    };
                    return Err(StratifyError::ForwardReference {
                        rule: rid,
                        span,
                        rel: rel_name.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Validate that every recursive stratum has at least one recursive relation.
    ///
    /// A recursive stratum with an empty recursive-relation set is structurally
    /// invalid: the compiler's iterative scope requires at least one feedback
    /// variable to wire up.  For loop blocks this arises when every rule inside
    /// the block is non-self-referential (no head relation ever appears as a body
    /// atom), making iteration pointless.
    fn validate_recursive_strata(&self) -> Result<(), StratifyError> {
        for (idx, is_rec) in self.is_recursive_stratum_bitmap.iter().enumerate() {
            if *is_rec
                && self.stratum_accumulate_recursive_relation[idx].is_empty()
                && self.stratum_iterative_recursive_relation[idx].is_empty()
            {
                let rules: Vec<(usize, Span)> = self.stratum[idx]
                    .iter()
                    .map(|&rid| (rid, self.program.rule(rid).span()))
                    .collect();
                return Err(StratifyError::RecursiveStratumEmpty {
                    stratum: idx + 1,
                    rules,
                });
            }
        }
        Ok(())
    }

    /// Validate that every relation in a loop until condition:
    ///   1. is derived by at least one rule inside the loop body, and
    ///   2. transitively depends on a recursive relation in the same stratum.
    ///
    /// An until relation that is never derived or is independent of the recursive
    /// computation can never change across iterations and is rejected.
    fn validate_loop_conditions(&self) -> Result<(), StratifyError> {
        for (idx, cond_opt) in self.stratum_loop_condition.iter().enumerate() {
            let Some(cond) = cond_opt else {
                continue;
            };
            let Some(until_group) = cond.until_part() else {
                continue;
            };

            let stratum_rules: Vec<_> = self.stratum[idx]
                .iter()
                .map(|&rid| self.program.rule(rid).clone())
                .collect();
            let dep_graph = DependencyGraph::from_rules(&stratum_rules);

            let local_head_fp: Vec<u64> = stratum_rules
                .iter()
                .map(|r| r.head().head_fingerprint())
                .collect();
            let heads: HashSet<u64> = local_head_fp.iter().copied().collect();

            // Recursive relations: heads that also appear in a body within the stratum.
            let body_fps: HashSet<u64> = stratum_rules
                .iter()
                .flat_map(|r| {
                    r.rhs().iter().filter_map(|p| match p {
                        Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => {
                            Some(a.fingerprint())
                        }
                        _ => None,
                    })
                })
                .collect();
            let recursive_fps: HashSet<u64> = heads.intersection(&body_fps).copied().collect();

            // Rule indices whose head is a recursive relation.
            let recursive_rule_ids: HashSet<usize> = local_head_fp
                .iter()
                .enumerate()
                .filter(|(_, fp)| recursive_fps.contains(fp))
                .map(|(i, _)| i)
                .collect();

            for rel in until_group.relations() {
                let (rel_name, fp, span) = (rel.name(), rel.fp(), rel.span());

                if !heads.contains(&fp) {
                    return Err(StratifyError::LoopConditionNotDerived {
                        rel: self.display_name(fp, rel_name),
                        span,
                    });
                }

                // BFS from the until relation's rules through the dependency graph
                // to verify it transitively reaches a recursive relation.
                let seed: Vec<usize> = local_head_fp
                    .iter()
                    .enumerate()
                    .filter(|(_, h)| **h == fp)
                    .map(|(i, _)| i)
                    .collect();
                if !self.reaches_recursive(&dep_graph, &seed, &recursive_rule_ids) {
                    return Err(StratifyError::LoopConditionNotRecursive {
                        rel: self.display_name(fp, rel_name),
                        span,
                    });
                }
            }
        }
        Ok(())
    }

    /// Returns `true` if any rule in `seeds` transitively depends on a rule in
    /// `targets` via the given dependency graph.
    fn reaches_recursive(
        &self,
        dep_graph: &DependencyGraph,
        seeds: &[usize],
        targets: &HashSet<usize>,
    ) -> bool {
        let mut visited = HashSet::new();
        let mut stack: Vec<usize> = seeds.to_vec();
        while let Some(cur) = stack.pop() {
            if !visited.insert(cur) {
                continue;
            }
            if let Some(deps) = dep_graph.dependency_map().get(&cur) {
                for &dep in deps {
                    if targets.contains(&dep) {
                        return true;
                    }
                    stack.push(dep);
                }
            }
        }
        false
    }

    /// Emit warnings for non-monotone aggregation operators in recursive strata.
    ///
    /// `min` and `max` are monotone and safe in a fixpoint loop.  `sum`,
    /// `count`, and `avg` accumulate across iterations and will never stabilise,
    /// so the fixpoint may never be reached.
    fn warn_aggregation(&self) {
        for (idx, stratum) in self.stratum.iter().enumerate() {
            if !self.is_recursive_stratum(idx) {
                continue;
            }
            for &rid in stratum {
                let rule = self.program.rule(rid);
                for arg in rule.head().head_arguments() {
                    if let HeadArg::Aggregation(agg) = arg {
                        match agg.operator() {
                            AggregationOperator::Sum
                            | AggregationOperator::Count
                            | AggregationOperator::Avg => {
                                warn!(
                                    "`{}` in recursive stratum #{} (rule {}): \
                                     not monotone; the fixpoint may never converge.\n  \
                                     Rule {}: {}",
                                    agg.operator(),
                                    idx + 1,
                                    rid,
                                    rid,
                                    rule
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Display
// =============================================================================

impl fmt::Display for Stratifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\nStratum:")?;
        writeln!(f, "{}", "-".repeat(45))?;

        let fp2name: HashMap<u64, String> = self
            .program
            .relations()
            .iter()
            .map(|r| (r.fingerprint(), r.name().to_string()))
            .collect();
        let fmt_fps = |fps: &[u64]| -> String {
            let mut names: Vec<String> = fps
                .iter()
                .map(|fp| {
                    fp2name
                        .get(fp)
                        .cloned()
                        .unwrap_or_else(|| format!("0x{:016x}", fp))
                })
                .collect();
            names.sort();
            names.dedup();
            names.join(", ")
        };

        for (idx, stratum) in self.stratum.iter().enumerate() {
            let recursive = self.is_recursive_stratum(idx);
            let label = if let Some(cond) = &self.stratum_loop_condition[idx] {
                format!("loop: {}", cond)
            } else if recursive {
                "recursive".to_string()
            } else {
                "non-recursive".to_string()
            };
            let ids = stratum.iter().sorted().map(|r| r.to_string()).join(", ");
            writeln!(f, "#{} [{}] [{}]", idx + 1, label, ids)?;

            if recursive {
                let acc = &self.stratum_accumulate_recursive_relation[idx];
                let itr = &self.stratum_iterative_recursive_relation[idx];
                if !acc.is_empty() {
                    writeln!(f, "  accumulate: [{}]", fmt_fps(acc))?;
                }
                if !itr.is_empty() {
                    writeln!(f, "  iterative:  [{}]", fmt_fps(itr))?;
                }
            }
            writeln!(
                f,
                "  leave: [{}]",
                fmt_fps(&self.stratum_leave_relation[idx])
            )?;

            for &rid in stratum {
                writeln!(f, "{}", self.program.rule(rid))?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tracing_test::traced_test;

    fn parse_program(source: &str) -> Program {
        use crate::common::SourceMap;
        let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        let mut sm = SourceMap::new();
        Program::parse(&tmp.path().to_string_lossy(), true, &mut sm).expect("parse failed")
    }

    /// Each `.init` splices its component instance into its own `Plain`
    /// segment, so instance `a` negating `b.Keep` — produced by a *later*
    /// instance — is a forward reference across segments. Coalescing the
    /// Plain run before SCC stratification makes instance order irrelevant,
    /// matching Soufflé's global stratification.
    #[test]
    fn coalesced_plain_run_stratifies_cross_instance_forward_reference() {
        let src = "\
            .decl In(x: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .comp A {\n\
              .decl Out(x: int32)\n\
              Out(x) :- In(x), !b.Keep(x).\n\
            }\n\
            .comp B {\n\
              .decl Keep(x: int32)\n\
              Keep(x) :- In(x).\n\
            }\n\
            .init a = A\n\
            .init b = B\n\
            .output a.Out\n";
        Stratifier::from_program(&parse_program(src), false)
            .expect("cross-instance forward reference must stratify");
    }

    /// A(x,y) :- Edge(x,y), !B(x,y).
    /// B(x,y) :- A(x,y).
    /// A and B form a recursive cycle and A negates B → stratified negation warning.
    #[test]
    #[traced_test]
    fn warns_negation_through_recursion() {
        let src = "\
            .decl Edge(a: int32, b: int32)\n\
            .decl A(a: int32, b: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            A(x, y) :- Edge(x, y), !B(x, y).\n\
            B(x, y) :- A(x, y).\n\
            .output A\n\
            .output B\n";
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// A(x,y) :- Edge(x,y), !A(x,y).
    /// Self-negation → stratified negation warning.
    #[test]
    #[traced_test]
    fn warns_self_negation() {
        let src = "\
            .decl Edge(a: int32, b: int32)\n\
            .decl A(a: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            A(x, y) :- Edge(x, y), !A(x, y).\n\
            .output A\n";
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// Running(x, sum(cost)) :- Running(x, prev), Edge(x, y, cost).
    /// sum in the head of a recursive rule → non-monotone aggregation warning.
    #[test]
    #[traced_test]
    fn warns_sum_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32, cost: int32)\n\
            .decl Running(x: int32, total: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Running(x, sum(cost)) :- Edge(x, y, cost).\n\
            Running(x, sum(cost)) :- Running(x, prev), Edge(x, y, cost).\n\
            .output Running\n";
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(logs_contain("`sum` in recursive stratum"));
    }

    /// Best(x, min(cost)) :- Best(x, b), Edge(x, y, cost).
    /// min is monotone → no fixpoint warning emitted.
    #[test]
    #[traced_test]
    fn no_warn_min_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32, cost: int32)\n\
            .decl Best(x: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Best(x, min(cost)) :- Edge(x, y, cost).\n\
            Best(x, min(cost)) :- Best(x, b), Edge(x, y, cost).\n\
            .output Best\n";
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(!logs_contain("fixpoint may never converge"));
    }

    /// A `loop` block becomes exactly one recursive stratum tagged with its condition.
    #[test]
    fn loop_block_is_single_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert_eq!(s.stratum.len(), 1);
        // A `loop {}` block always becomes one recursive stratum; the condition
        // is None because fixpoint is implicit (no explicit loop condition).
        assert!(s.is_recursive_stratum(0));
    }

    /// Plain rules before and after a loop block are stratified independently
    /// from the loop stratum, yielding at least three strata total.
    #[test]
    fn segments_stratified_independently() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            .output Reach\n\
            A(x) :- Edge(x, y).\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n\
            Out(x) :- A(x).\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert!(s.stratum.len() >= 3);
        // The loop block is the only recursive stratum.
        let loop_idx = (0..s.stratum.len())
            .find(|&i| s.is_recursive_stratum(i))
            .expect("no loop stratum");
        assert!(s.is_recursive_stratum(loop_idx));
    }

    /// Negation inside a `loop` block is always negation-through-recursion →
    /// warning, because the whole block is one recursive stratum.
    #[test]
    #[traced_test]
    fn warns_negation_in_loop_block() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            .output B\n\
            fixpoint {\n\
                A(x, y) :- Edge(x, y), !B(x, y).\n\
                B(x, y) :- A(x, y).\n\
            }\n";
        Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// Extended Datalog mode: recursive rules inside a `loop` block are valid.
    #[test]
    fn extended_mode_loop_recursion_ok() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert_eq!(s.stratum.len(), 1);
        assert!(s.is_recursive_stratum(0));
    }

    /// Inline fact-only relations are EDBs and must be available to the very
    /// first stratum just like file-backed `.input` relations.
    #[test]
    fn inline_fact_relations_are_available_before_first_stratum() {
        let src = "\
            .decl Param(x: int32)\n\
            .decl Out(x: int32)\n\
            Param(1).\n\
            Out(x) :- Param(x).\n\
            .output Out\n";
        let program = parse_program(src);
        let param_fp = program
            .relations()
            .iter()
            .find(|r| r.name() == "param")
            .expect("param relation missing")
            .fingerprint();

        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

        assert!(
            s.stratum_available_relations(0).contains(&param_fp),
            "inline fact relation should be available before the first stratum"
        );
    }

    /// k-core-like loop: `active_edge` and `degree` are iterative (declared),
    /// `removed` is accumulative.  After stratification the two sets must be
    /// split correctly.
    #[test]
    fn loop_iterative_split_correctly() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl active_edge(x: int32, y: int32)\n\
            .decl degree(x: int32, d: int32)\n\
            .decl removed(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output removed\n\
            fixpoint {\n\
                .iterative active_edge\n\
                .iterative degree\n\
                active_edge(x, y) :- edge(x, y), !removed(x), !removed(y).\n\
                degree(x, count(y)) :- active_edge(x, y).\n\
                removed(x) :- degree(x, d), d < 2.\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");

        // Should be exactly one stratum (the fixpoint block).
        assert_eq!(s.stratum.len(), 1);
        assert!(s.is_recursive_stratum(0));

        let acc = s.stratum_accumulate_recursive_relation(0);
        let itr = s.stratum_iterative_recursive_relation(0);

        // active_edge and degree are explicitly iterative.
        assert_eq!(itr.len(), 2, "active_edge and degree should be iterative");
        // removed feeds back (it appears in active_edge's body) → recursive,
        // but not declared iterative → accumulative.
        assert_eq!(acc.len(), 1, "removed should be accumulative");
        // Iterative and accumulative sets must be disjoint.
        let itr_set: HashSet<u64> = itr.iter().copied().collect();
        let acc_set: HashSet<u64> = acc.iter().copied().collect();
        assert!(
            itr_set.is_disjoint(&acc_set),
            "iterative and accumulative sets must be disjoint"
        );
    }

    /// Helper: look up a relation's fingerprint by name.
    fn fp_of(program: &Program, name: &str) -> u64 {
        program
            .relations()
            .iter()
            .find(|r| r.name() == name)
            .unwrap_or_else(|| panic!("relation `{name}` missing"))
            .fingerprint()
    }

    /// Leave set for stratum N must contain a head relation consumed by any
    /// *later* stratum. If `later_body_atoms_per_stratum` accumulation breaks,
    /// intermediate relations get dropped and codegen silently loses data.
    #[test]
    fn leave_set_includes_relation_consumed_by_later_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Mid(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            Mid(x, y) :- Edge(x, y).\n\
            Out(x) :- Mid(x, y).\n";
        let program = parse_program(src);
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

        let mid_fp = fp_of(&program, "mid");
        // Mid is NOT an .output, so its presence in leave[0] must come from
        // the later-body-atom branch, not the idb branch.
        assert!(
            s.stratum_leave_relation(0).contains(&mid_fp),
            "mid should be retained for stratum 1 to consume"
        );
    }

    /// Leave set for the last stratum must contain any `.output` relation it
    /// heads, even with no later consumer. Guards the `idb_fp_set` branch of
    /// the leave-set computation — a bug there would drop outputs from the
    /// persisted set.
    #[test]
    fn leave_set_includes_idb_even_with_no_later_consumer() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Final(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Final\n\
            Final(x, y) :- Edge(x, y).\n";
        let program = parse_program(src);
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

        let final_fp = fp_of(&program, "final");
        let last = s.stratum.len() - 1;
        assert!(
            s.stratum_leave_relation(last).contains(&final_fp),
            "output relation must stay in leave set of its stratum"
        );
    }

    /// `stratum_available_relations(i)` = EDBs ∪ leave[0..i). Across three
    /// strata, stratum 2's available set must contain relations produced by
    /// both stratum 0 and stratum 1. Guards the `accumulated.extend(leave)`
    /// accumulator in `build_stratum_metadata`.
    #[test]
    fn available_set_accumulates_leaves_across_strata() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            A(x, y) :- Edge(x, y).\n\
            B(x, y) :- A(x, y).\n\
            Out(x) :- A(x, y), B(x, y).\n";
        let program = parse_program(src);
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

        assert!(s.stratum.len() >= 3, "expected at least 3 strata");
        let a_fp = fp_of(&program, "a");
        let b_fp = fp_of(&program, "b");
        let last = s.stratum.len() - 1;
        let available = s.stratum_available_relations(last);
        assert!(
            available.contains(&a_fp),
            "A's leave from stratum 0 missing"
        );
        assert!(
            available.contains(&b_fp),
            "B's leave from stratum 1 missing"
        );
    }

    /// Negation across *non-recursive* strata must not trigger the recursive-
    /// stratum negation warning. A regression that broadens the trigger would
    /// silently spam warnings on every cross-stratum `!B(...)` the user writes.
    #[test]
    #[traced_test]
    fn no_warn_on_non_recursive_negation() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl B(x: int32)\n\
            .decl A(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            B(x) :- Edge(x, y).\n\
            A(x) :- Edge(x, y), !B(x).\n";
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(
            !logs_contain("Negation in recursive stratum"),
            "non-recursive negation should not fire the recursive-stratum warning"
        );
    }
}
