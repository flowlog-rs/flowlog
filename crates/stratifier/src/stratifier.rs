//! Stratification for FlowLog Datalog programs.

use crate::dependency_graph::DependencyGraph;
use itertools::Itertools;
use parser::{AggregationOperator, FlowLogRule, HeadArg, Predicate, Program};
use std::collections::{HashMap, HashSet};
use std::fmt;
use tracing::{debug, info, warn};

/// Stratify a program into group of rules.
///
/// Each stratum is a set of rule IDs that can be evaluated parallelyas a unit after all
/// preceding strata have been computed. A stratum is marked recursive when it
/// corresponds to a strongly connected component (SCC) with more than one rule
/// or a single rule with a self‑dependency.
#[derive(Debug, Clone)]
pub struct Stratifier {
    /// The original program being stratified.
    program: Program,

    /// Underlying dependency graph (rule -> rules it depends on).
    dependency_graph: DependencyGraph,

    /// Rule IDs per stratum in evaluation order
    stratum: Vec<Vec<usize>>,

    /// True iff corresponding stratum is recursive
    is_recursive_stratum_bitmap: Vec<bool>,

    /// Mapping of stratum IDs to their iterative relations (rule IDs) (recursive strata only).
    stratum_iterative_relation: Vec<Vec<u64>>,

    /// Mapping of stratum IDs to their leave relations (rule IDs) required by later stratum.
    stratum_leave_relation: Vec<Vec<u64>>,

    /// Mapping of stratum IDs to the relations available before planning that stratum.
    stratum_available_relations: Vec<HashSet<u64>>,
}

impl Stratifier {
    /// The original (owned) program being stratified.
    #[must_use]
    pub fn program(&self) -> &Program {
        &self.program
    }

    /// Bitmap indicating which strata are recursive (parallel with `strata`).
    #[must_use]
    pub fn is_recursive_stratum_bitmap(&self) -> &[bool] {
        &self.is_recursive_stratum_bitmap
    }

    /// Whether the given stratum (by index) is recursive.
    #[must_use]
    pub fn is_recursive_stratum(&self, idx: usize) -> bool {
        self.is_recursive_stratum_bitmap[idx]
    }

    /// Return stratum as rule references instead of IDs (helper for display/tests).
    #[must_use]
    pub fn stratum(&self) -> Vec<Vec<&FlowLogRule>> {
        let mut out = Vec::with_capacity(self.stratum.len());
        for s in &self.stratum {
            out.push(s.iter().map(|&rid| &self.program.rules()[rid]).collect());
        }
        out
    }

    /// Return iterative relations.
    #[must_use]
    pub fn stratum_iterative_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_iterative_relation[idx]
    }

    /// Return leave relations.
    #[must_use]
    pub fn stratum_leave_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_leave_relation[idx]
    }

    /// Return relations available prior to the given stratum (union of leaves from earlier strata).
    #[must_use]
    pub fn stratum_available_relations(&self, idx: usize) -> &HashSet<u64> {
        &self.stratum_available_relations[idx]
    }

    /// Build strata by computing SCCs then merging independent non‑recursive strata.
    ///
    /// Algorithm outline:
    /// 1. Build rule dependency graph (already polarity-agnostic).
    /// 2. Run Kosaraju to obtain SCCs (gives base strata + recursion detection).
    /// 3. Mark recursive strata (size > 1 or self loop).
    /// 4. Iteratively merge all currently dependency‑free non‑recursive strata
    ///    into a single wider stratum to reduce evaluation passes.
    #[must_use]
    pub fn from_program(program: &Program) -> Self {
        let dependency_graph = DependencyGraph::from_program(program);
        let dep_map = dependency_graph.dependency_map();

        // Kosaraju step 1: order by finish time.
        let mut order = Vec::with_capacity(dep_map.len());
        let mut visited = vec![false; dep_map.len()];
        for &rule_id in dep_map.keys() {
            Self::dfs_order(dep_map, &mut visited, &mut order, rule_id);
        }
        order.reverse();

        // Transpose graph.
        let transpose = Self::transpose(dep_map);

        // Kosaraju step 2: assign SCCs on transposed graph in reverse finish order.
        let mut assigned = vec![false; dep_map.len()];
        let mut sccs: Vec<Vec<usize>> = Vec::new();
        for node in order {
            if !assigned[node] {
                let mut scc = Vec::new();
                Self::dfs_assign(&transpose, &mut assigned, &mut scc, node);
                sccs.push(scc);
            }
        }

        // Topological order of SCCs (already in reverse postorder of transpose if we reverse again):
        // We currently have SCCs in evaluation order (dependencies before dependents) because
        // we processed original order reversed; keep as-is.

        // Identify recursion (multi-node SCC or self-loop) and collect strata.
        let mut scc_id = vec![0usize; dep_map.len()];
        let mut strata: Vec<Vec<usize>> = Vec::new();
        let mut recursive_bitmap: Vec<bool> = Vec::new();
        for (idx, scc) in sccs.into_iter().enumerate() {
            let is_recursive = scc.len() > 1 || {
                let r = scc[0];
                dep_map.get(&r).is_some_and(|deps| deps.contains(&r))
            };
            for &rule_id in &scc {
                scc_id[rule_id] = idx;
            }
            strata.push(scc);
            recursive_bitmap.push(is_recursive);
        }

        // Warn: negation through recursion (stratified negation).
        // A single scan over negative edges using the precomputed scc_id array.
        for &(src, dst) in dependency_graph.negative_edges() {
            if scc_id[src] == scc_id[dst] && recursive_bitmap[scc_id[src]] {
                let src_rule = &program.rules()[src];
                if src == dst {
                    warn!(
                        "Negation in recursive stratum (rule {} negates itself): \
                         negation is not monotone; the fixpoint may never converge.\n  \
                         Rule {}: {}",
                        src, src, src_rule
                    );
                } else {
                    let dst_rule = &program.rules()[dst];
                    warn!(
                        "Negation in recursive stratum (rule {} negates rule {}): \
                         negation is not monotone; the fixpoint may never converge.\n  \
                         Rule {}: {}\n  Rule {}: {}",
                        src, dst, src, src_rule, dst, dst_rule
                    );
                }
            }
        }

        // Merge phase: repeatedly take all remaining strata with no external
        // dependencies. Merge all non‑recursive ones together; keep recursive
        // ones separate to preserve fixpoint boundaries.
        let mut merged: Vec<Vec<usize>> = Vec::new();
        let mut merged_bitmap: Vec<bool> = Vec::new();
        let mut pending = strata.into_iter().zip(recursive_bitmap).collect::<Vec<_>>();

        while !pending.is_empty() {
            // Partition strata by whether they still depend on *remaining* strata.
            let mut batch_non_recursive: Vec<usize> = Vec::new();
            let mut batch_recursive: Vec<(Vec<usize>, bool)> = Vec::new();

            let remaining_ids: HashSet<usize> = pending
                .iter()
                .flat_map(|(s, _)| s.iter().copied())
                .collect();

            let mut still: Vec<(Vec<usize>, bool)> = Vec::new();
            for (s, is_rec) in pending.into_iter() {
                // Check if this stratum depends on any rule that remains in another (unprocessed) stratum.
                let external_dep_exists = s.iter().any(|rid| {
                    dep_map.get(rid).is_some_and(|deps| {
                        deps.iter()
                            .any(|d| remaining_ids.contains(d) && !s.contains(d))
                    })
                });
                if external_dep_exists {
                    still.push((s, is_rec));
                } else if is_rec {
                    batch_recursive.push((s, is_rec));
                } else {
                    batch_non_recursive.extend(s);
                }
            }
            pending = still;

            if !batch_non_recursive.is_empty() {
                merged.push(batch_non_recursive);
                merged_bitmap.push(false);
            }
            for (s, is_rec) in batch_recursive {
                merged.push(s);
                merged_bitmap.push(is_rec);
            }
        }

        let mut instance = Self {
            program: program.clone(),
            dependency_graph,
            stratum: merged,
            is_recursive_stratum_bitmap: merged_bitmap,
            stratum_iterative_relation: Vec::new(),
            stratum_leave_relation: Vec::new(),
            stratum_available_relations: Vec::new(),
        };

        instance.build_stratum_metadata();

        // Warn: non-monotone aggregation (sum/count/avg) in a recursive stratum.
        // min/max are monotone and safe; sum/count/avg accumulate across iterations
        // and will never stabilise, so the fixpoint cannot be reached.
        for (idx, stratum) in instance.stratum.iter().enumerate() {
            if !instance.is_recursive_stratum(idx) {
                continue;
            }
            for &rid in stratum {
                let rule = &instance.program.rules()[rid];
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

        // Debug info print
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

    /// Build the reverse adjacency map (transpose of the dependency graph).
    fn transpose(
        rule_dependency_map: &HashMap<usize, HashSet<usize>>,
    ) -> HashMap<usize, HashSet<usize>> {
        let mut out: HashMap<usize, HashSet<usize>> = rule_dependency_map
            .keys()
            .map(|&k| (k, HashSet::new()))
            .collect();
        for (&src, dests) in rule_dependency_map.iter() {
            for &dst in dests {
                out.entry(dst).or_default().insert(src);
            }
        }
        out
    }

    /// DFS used in the first pass of Kosaraju to record finishing order.
    fn dfs_order(
        rule_dependency_map: &HashMap<usize, HashSet<usize>>,
        visited: &mut [bool],
        order: &mut Vec<usize>,
        node: usize,
    ) {
        if visited[node] {
            return;
        }
        visited[node] = true;
        if let Some(children) = rule_dependency_map.get(&node) {
            for &c in children {
                Self::dfs_order(rule_dependency_map, visited, order, c);
            }
        }
        order.push(node);
    }

    /// DFS on the transposed graph to collect one SCC.
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

    fn build_stratum_metadata(&mut self) {
        // Precompute heads and body atoms per stratum.
        let mut heads_per_stratum = Vec::new();
        let mut body_atoms_per_stratum = Vec::new();

        for stratum in &self.stratum {
            // Heads in this stratum
            let heads: HashSet<u64> = stratum
                .iter()
                .map(|rid| self.program.rules()[*rid].head().head_fingerprint())
                .collect();
            heads_per_stratum.push(heads);

            // Body atoms in this stratum
            let body_atoms: HashSet<u64> = stratum
                .iter()
                .flat_map(|rid| {
                    let rule = &self.program.rules()[*rid];
                    rule.rhs().iter().filter_map(|p| match p {
                        Predicate::PositiveAtomPredicate(atom)
                        | Predicate::NegativeAtomPredicate(atom) => Some(atom.fingerprint()),
                        _ => None,
                    })
                })
                .collect();
            body_atoms_per_stratum.push(body_atoms);
        }

        let n = self.stratum.len();

        // Precompute fingerprints of IDB relations (annotated with .output or .printsize).
        // These are always retained in the leave set so they remain accessible after the dataflow,
        // even if no later stratum references them.
        // Internal relations only needed within a stratum are excluded; they are kept by `later_body_atoms`.
        let idb_fp_set: HashSet<u64> = self
            .program
            .idbs()
            .into_iter()
            .map(|r| r.fingerprint())
            .collect();
        // For each stratum, compute the set of body atoms referenced by any STRICTLY later stratum.
        let mut later_union: HashSet<u64> = HashSet::new();
        let mut later_body_atoms_per_stratum: Vec<HashSet<u64>> = vec![HashSet::new(); n];

        for i in (0..n).rev() {
            // For stratum i, the "later" set is what we've accumulated so far.
            later_body_atoms_per_stratum[i] = later_union.clone();
            // Include current stratum's body atoms for earlier strata to see.
            later_union.extend(body_atoms_per_stratum[i].iter().copied());
        }

        // Build iterative and leave relations per stratum.
        for i in 0..n {
            let heads = &heads_per_stratum[i];
            let body_atoms = &body_atoms_per_stratum[i];
            let later_body_atoms = &later_body_atoms_per_stratum[i];

            // Iterative relations only for recursive strata: heads that appear in bodies within the same stratum.
            if self.is_recursive_stratum(i) {
                let iterative: Vec<u64> = heads.intersection(body_atoms).copied().collect();
                self.stratum_iterative_relation.push(iterative);
            } else {
                self.stratum_iterative_relation.push(Vec::new());
            }

            // Leave relations: heads from this stratum that are referenced by any later stratum.
            // Additionally retain heads that correspond to IDB relations even if not referenced later.
            let leave: Vec<u64> = heads
                .iter()
                .filter(|fp| later_body_atoms.contains(fp) || idb_fp_set.contains(fp))
                .copied()
                .collect();
            self.stratum_leave_relation.push(leave);
        }

        // Build available relations per stratum by accumulating leaves from previous strata
        // and always including original EDB relation fingerprints (facts available from the start).
        let mut accumulated: HashSet<u64> = HashSet::new();
        for leave in &self.stratum_leave_relation {
            let mut available_relations = accumulated.clone();
            available_relations.extend(self.program.edb_fingerprints().iter().copied());
            self.stratum_available_relations.push(available_relations);
            accumulated.extend(leave.iter().copied());
        }
    }
}

impl fmt::Display for Stratifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.dependency_graph)?;

        writeln!(f, "\nStratum:")?;
        writeln!(f, "{}", "-".repeat(45))?;

        // Build a lookup from relation fingerprint to relation name for nicer printing.
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
            write!(
                f,
                "#{} [{}] ",
                idx + 1,
                if recursive {
                    "recursive"
                } else {
                    "non-recursive"
                }
            )?;
            let ids = stratum.iter().sorted().map(|r| r.to_string()).join(", ");
            writeln!(f, "[{}]", ids)?;

            let iters = &self.stratum_iterative_relation[idx];
            let leaves = &self.stratum_leave_relation[idx];
            if recursive {
                writeln!(f, "  iterative: [{}]", fmt_fps(iters))?;
            }
            writeln!(f, "  leave: [{}]", fmt_fps(leaves))?;

            for rid in stratum {
                writeln!(f, "{}", self.program.rules()[*rid])?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tracing_test::traced_test;

    /// Write a Datalog source string to a temporary file and parse it.
    fn parse_program(source: &str) -> Program {
        let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        Program::parse(&tmp.path().to_string_lossy())
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
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
        let _ = Stratifier::from_program(&parse_program(src));
        assert!(!logs_contain("fixpoint may never converge"));
    }
}
