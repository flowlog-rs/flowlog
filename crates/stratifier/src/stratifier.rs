//! Core stratification logic for FlowLog programs.
//!
//! See the crate-level documentation for an overview of strata, recursion, and
//! the Extended Datalog mode.

use crate::dependency_graph::DependencyGraph;
use itertools::Itertools;
use parser::{
    AggregationOperator, FlowLogRule, HeadArg, LoopCondition, Predicate, Program, Segment,
};
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
///   fixpoint or stop condition is reached.
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
/// | *available* | Relations that are already fully computed before this stratum begins (EDB + leaves from all prior strata). |
#[derive(Debug, Clone)]
pub struct Stratifier {
    /// The original program being stratified.
    program: Program,

    /// Rule IDs per stratum in evaluation order.
    /// Each inner `Vec<usize>` is one stratum; IDs are global source-order indices.
    stratum: Vec<Vec<usize>>,

    /// `true` iff the corresponding stratum is recursive (parallel with `stratum`).
    is_recursive_stratum_bitmap: Vec<bool>,

    /// The stop condition for each stratum.
    ///
    /// `Some` only for loop-derived strata; `None` for SCC-derived plain strata.
    /// Parallel with `stratum`.
    stratum_loop_condition: Vec<Option<LoopCondition>>,

    /// Recursive relations per stratum.
    ///
    /// Head relations that also appear in the body of the same stratum.
    /// Non-empty only for recursive strata.  Parallel with `stratum`.
    stratum_recursive_relation: Vec<Vec<u64>>,

    /// Relation fingerprints that must be preserved after a stratum finishes.
    ///
    /// A head relation is in the leave set if it is referenced by any later
    /// stratum or is annotated as an IDB output (`.output` / `.printsize`).
    /// Parallel with `stratum`.
    stratum_leave_relation: Vec<Vec<u64>>,

    /// Relations that are fully computed before a stratum begins.
    ///
    /// Always includes all EDB relations.  For stratum *i*, this is the union
    /// of the leave sets of strata *0 … i-1*.  Parallel with `stratum`.
    stratum_available_relations: Vec<HashSet<u64>>,
}

// =============================================================================
// Public API
// =============================================================================

impl Stratifier {
    /// The original program that was stratified.
    #[must_use]
    pub fn program(&self) -> &Program {
        &self.program
    }

    /// A parallel bitmap indicating which strata are recursive.
    ///
    /// `bitmap[i]` is `true` when stratum *i* contains a dependency cycle and
    /// requires recursive evaluation.
    #[must_use]
    pub fn is_recursive_stratum_bitmap(&self) -> &[bool] {
        &self.is_recursive_stratum_bitmap
    }

    /// Returns `true` if stratum `idx` is recursive.
    #[must_use]
    pub fn is_recursive_stratum(&self, idx: usize) -> bool {
        self.is_recursive_stratum_bitmap[idx]
    }

    /// Returns the stop condition for a loop-derived stratum, or `None` for a
    /// plain SCC-derived stratum.
    #[must_use]
    pub fn loop_condition(&self, idx: usize) -> Option<&LoopCondition> {
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

    /// Recursive relations for stratum `idx`.
    ///
    /// Head relations that also appear in the body of the same stratum.
    /// Empty for non-recursive strata.
    #[must_use]
    pub fn stratum_recursive_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_recursive_relation[idx]
    }

    /// Relation fingerprints whose values must be retained after stratum `idx`.
    #[must_use]
    pub fn stratum_leave_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_leave_relation[idx]
    }

    /// Relations that are fully computed before stratum `idx` begins.
    ///
    /// Always includes EDB relations and the leave sets of all earlier strata.
    #[must_use]
    pub fn stratum_available_relations(&self, idx: usize) -> &HashSet<u64> {
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
    /// # Panics
    ///
    /// Panics if `extended = true` and a recursive dependency is detected
    /// outside a `loop` block.
    #[must_use]
    pub fn from_program(program: &Program, extended: bool) -> Self {
        let mut strata: Vec<Vec<usize>> = Vec::new();
        let mut bitmap: Vec<bool> = Vec::new();
        let mut loop_conditions: Vec<Option<LoopCondition>> = Vec::new();
        let mut id_offset = 0usize;

        for seg in program.segments() {
            match seg {
                Segment::Plain(rules) => {
                    let (seg_strata, seg_bitmap) =
                        Self::stratify_segment(rules, id_offset, extended);
                    let n = seg_strata.len();
                    strata.extend(seg_strata);
                    bitmap.extend(seg_bitmap);
                    loop_conditions.extend(std::iter::repeat_n(None, n));
                    id_offset += rules.len();
                }
                Segment::Loop(block) => {
                    // A loop block is always exactly one recursive stratum.
                    // No SCC analysis is performed inside: all rules iterate
                    // together under the block's stop condition.
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
                    id_offset += rule_count;
                }
            }
        }

        let mut instance = Self {
            program: program.clone(),
            stratum: strata,
            is_recursive_stratum_bitmap: bitmap,
            stratum_loop_condition: loop_conditions,
            stratum_recursive_relation: Vec::new(),
            stratum_leave_relation: Vec::new(),
            stratum_available_relations: Vec::new(),
        };

        instance.build_stratum_metadata();
        instance.validate_forward_references();
        instance.validate_recursive_strata();
        instance.validate_loop_conditions();
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

        instance
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
    ) -> (Vec<Vec<usize>>, Vec<bool>) {
        if rules.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let dep_graph = DependencyGraph::from_rules(rules);
        let dep_map = dep_graph.dependency_map();
        let (sccs, scc_bitmap, scc_id) = Self::compute_sccs(dep_map);

        // Extended Datalog mode: recursion in plain rules is forbidden.
        // Every recursive SCC must be inside an explicit `loop` block.
        if extended {
            for (scc, &is_rec) in sccs.iter().zip(scc_bitmap.iter()) {
                if is_rec {
                    let rule_ids: Vec<String> = scc
                        .iter()
                        .map(|&local| (local + id_offset).to_string())
                        .collect();
                    panic!(
                        "Extended Datalog error: recursive rules must be inside an explicit \
                         `loop` block, but recursion was found in plain rules: [{}]\n  \
                         Hint: wrap these rules in `loop {{ ... }}` or another loop form.",
                        rule_ids.join(", ")
                    );
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

        (global_strata, merged_bitmap)
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
    fn build_stratum_metadata(&mut self) {
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
                        Predicate::PositiveAtomPredicate(atom)
                        | Predicate::NegativeAtomPredicate(atom) => Some(atom.fingerprint()),
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
            if self.is_recursive_stratum(i) {
                let recursive: Vec<u64> = heads.intersection(body_atoms).copied().collect();
                self.stratum_recursive_relation.push(recursive);
            } else {
                self.stratum_recursive_relation.push(Vec::new());
            };

            // Leave relations: head relations needed by later strata or outputs.
            let leave: Vec<u64> = heads
                .iter()
                .filter(|fp| later_body_atoms.contains(fp) || idb_fp_set.contains(fp))
                .copied()
                .collect();
            self.stratum_leave_relation.push(leave);
        }

        // Available relations: EDB ∪ leaves from all preceding strata.
        let edb_fps = self.program.edb_fingerprints();
        let mut accumulated: HashSet<u64> = HashSet::new();
        for leave in &self.stratum_leave_relation {
            let mut available = accumulated.clone();
            available.extend(&edb_fps);
            self.stratum_available_relations.push(available);
            accumulated.extend(leave);
        }
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
    fn validate_forward_references(&self) {
        let fp_to_name: HashMap<u64, &str> = self
            .program
            .relations()
            .iter()
            .map(|r| (r.fingerprint(), r.name()))
            .collect();
        let edb_fps = self.program.edb_fingerprints();

        for (i, stratum) in self.stratum.iter().enumerate() {
            let available = &self.stratum_available_relations[i];
            let heads: HashSet<u64> = stratum
                .iter()
                .map(|&rid| self.program.rule(rid).head().head_fingerprint())
                .collect();

            for &rid in stratum {
                let rule = self.program.rule(rid);
                for predicate in rule.rhs() {
                    let fp = match predicate {
                        Predicate::PositiveAtomPredicate(atom)
                        | Predicate::NegativeAtomPredicate(atom) => atom.fingerprint(),
                        _ => continue,
                    };
                    if edb_fps.contains(&fp) || available.contains(&fp) || heads.contains(&fp) {
                        continue;
                    }
                    let rel_name = fp_to_name.get(&fp).copied().unwrap_or("<unknown>");
                    panic!(
                        "Stratifier error: rule {} in stratum #{} references relation `{}`, \
                         which is not yet defined at this point in the program.\n  \
                         Rule: {}\n  \
                         Hint: `{}` appears to be defined in a later segment. \
                         Move the rule after the segment that derives `{}`.",
                        rid,
                        i + 1,
                        rel_name,
                        rule,
                        rel_name,
                        rel_name,
                    );
                }
            }
        }
    }

    /// Validate that every recursive stratum has at least one recursive relation.
    ///
    /// A recursive stratum with an empty recursive-relation set is structurally
    /// invalid: the compiler's iterative scope requires at least one feedback
    /// variable to wire up.  For loop blocks this arises when every rule inside
    /// the block is non-self-referential (no head relation ever appears as a body
    /// atom), making iteration pointless.
    fn validate_recursive_strata(&self) {
        for (idx, is_rec) in self.is_recursive_stratum_bitmap.iter().enumerate() {
            if *is_rec && self.stratum_recursive_relation[idx].is_empty() {
                let rule_ids: Vec<String> = self.stratum[idx]
                    .iter()
                    .map(|&rid| rid.to_string())
                    .collect();
                panic!(
                    "Stratifier error: recursive stratum #{} has no recursive relations \
                     (no head relation appears as a body atom in the same stratum).\n  \
                     Rules: [{}]\n  \
                     Hint: a `loop` block must contain at least one rule whose head \
                     relation also appears in a body atom within the same block.",
                    idx + 1,
                    rule_ids.join(", ")
                );
            }
        }
    }

    /// Validate that every relation referenced in a loop block's stop condition
    /// is derived by at least one rule inside the loop body.
    ///
    /// A relation that appears as a head in the loop is guaranteed to change
    /// as the loop progresses (either directly or via the recursive IDBs it
    /// depends on), making it a valid termination signal.  A relation that is
    /// never derived inside the loop is static and cannot meaningfully control
    /// termination — this is rejected as a hard error.
    fn validate_loop_conditions(&self) {
        for (idx, condition) in self.stratum_loop_condition.iter().enumerate() {
            let Some(cond) = condition else { continue };
            let heads: HashSet<u64> = self.stratum[idx]
                .iter()
                .map(|&rid| self.program.rule(rid).head().head_fingerprint())
                .collect();

            let Some(stop_group) = cond.stop_part() else {
                continue;
            };
            for rel in stop_group.relations() {
                let (rel_name, fp) = (rel.name.as_str(), rel.fp);
                if !heads.contains(&fp) {
                    panic!(
                        "Stratifier error: loop condition in stratum #{} references relation \
                         `{}`, which has no rule inside the loop body that derives it.\n  \
                         Hint: the stop-condition relation must appear as the head of at least \
                         one rule inside the loop block.",
                        idx + 1,
                        rel_name
                    );
                }
            }
        }
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
                writeln!(
                    f,
                    "  recursive: [{}]",
                    fmt_fps(&self.stratum_recursive_relation[idx])
                )?;
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
        let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        Program::parse(&tmp.path().to_string_lossy(), true)
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
        let _ = Stratifier::from_program(&parse_program(src), false);
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
        let _ = Stratifier::from_program(&parse_program(src), false);
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
        let _ = Stratifier::from_program(&parse_program(src), false);
        assert!(logs_contain("`sum` in recursive stratum"));
    }

    /// Reachable(x, count(y)) :- Reachable(x, n), Edge(x, y).
    /// count in the head of a recursive rule → non-monotone aggregation warning.
    #[test]
    #[traced_test]
    fn warns_count_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reachable(x: int32, n: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Reachable(x, count(y)) :- Edge(x, y).\n\
            Reachable(x, count(y)) :- Reachable(x, n), Edge(x, y).\n\
            .output Reachable\n";
        let _ = Stratifier::from_program(&parse_program(src), false);
        assert!(logs_contain("`count` in recursive stratum"));
    }

    /// Score(x, AVG(v)) :- Score(x, s), Base(x, v).
    /// avg in the head of a recursive rule → non-monotone aggregation warning.
    #[test]
    #[traced_test]
    fn warns_avg_in_recursive_stratum() {
        let src = "\
            .decl Base(x: int32, v: int32)\n\
            .decl Score(x: int32, s: int32)\n\
            .input Base(IO=\"file\", filename=\"Base.csv\", delimiter=\",\")\n\
            Score(x, AVG(v)) :- Base(x, v).\n\
            Score(x, AVG(v)) :- Score(x, s), Base(x, v).\n\
            .output Score\n";
        let _ = Stratifier::from_program(&parse_program(src), false);
        assert!(logs_contain("`average` in recursive stratum"));
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
        let _ = Stratifier::from_program(&parse_program(src), false);
        assert!(!logs_contain("fixpoint may never converge"));
    }

    /// Best(x, max(cost)) :- Best(x, b), Edge(x, y, cost).
    /// max is monotone → no fixpoint warning emitted.
    #[test]
    #[traced_test]
    fn no_warn_max_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32, cost: int32)\n\
            .decl Best(x: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Best(x, max(cost)) :- Edge(x, y, cost).\n\
            Best(x, max(cost)) :- Best(x, b), Edge(x, y, cost).\n\
            .output Best\n";
        let _ = Stratifier::from_program(&parse_program(src), false);
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
            loop {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s = Stratifier::from_program(&parse_program(src), true);
        assert_eq!(s.stratum.len(), 1);
        // A `loop {}` block always becomes one recursive stratum; the condition
        // is None because fixpoint is implicit (no explicit stop condition).
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
            loop {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n\
            Out(x) :- A(x).\n";
        let s = Stratifier::from_program(&parse_program(src), true);
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
            loop {\n\
                A(x, y) :- Edge(x, y), !B(x, y).\n\
                B(x, y) :- A(x, y).\n\
            }\n";
        let _ = Stratifier::from_program(&parse_program(src), true);
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
            loop {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s = Stratifier::from_program(&parse_program(src), true);
        assert_eq!(s.stratum.len(), 1);
        assert!(s.is_recursive_stratum(0));
    }

    /// Extended Datalog mode: recursive rules in a plain segment are a hard error.
    #[test]
    #[should_panic(expected = "Extended Datalog error")]
    fn extended_mode_plain_recursion_errors() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x, y) :- Edge(x, y).\n\
            Reach(x, z) :- Edge(x, y), Reach(y, z).\n";
        let _ = Stratifier::from_program(&parse_program(src), true);
    }

    /// A plain-segment rule that references a relation only defined inside a
    /// later `loop` block is a forward reference → stratifier hard error.
    #[test]
    #[should_panic(expected = "Stratifier error: rule 0 in stratum #1 references relation `b`")]
    fn forward_reference_across_loop_barrier_errors() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            A(x, y) :- B(x, y).\n\
            loop {\n\
                B(x, y) :- Edge(x, y).\n\
                B(x, z) :- Edge(x, y), B(y, z).\n\
            }\n";
        let _ = Stratifier::from_program(&parse_program(src), false);
    }

    /// A `loop` block where no head relation appears as a body atom has no
    /// recursive relations → stratifier hard error.
    #[test]
    #[should_panic(expected = "Stratifier error: recursive stratum #1 has no recursive relations")]
    fn loop_block_with_no_recursive_relations_errors() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            loop {\n\
                A(x, y) :- Edge(x, y).\n\
            }\n";
        let _ = Stratifier::from_program(&parse_program(src), false);
    }
}
