//! Stratification for Macaron Datalog programs.

use crate::dependency_graph::DependencyGraph;
use itertools::Itertools;
use parser::{logic::MacaronRule, Program};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Stratify a program into rule layers.
///
/// Each stratum is a set of rule IDs that can be evaluated as a unit after all
/// preceding strata have been computed. A stratum is marked recursive when it
/// corresponds to a strongly connected component (SCC) with more than one rule
/// or a single rule with a self‑dependency.
#[derive(Debug, Clone)]
pub struct Stratifier {
    program: Program,
    dependency_graph: DependencyGraph,

    // rule IDs per stratum in evaluation order
    stratum: Vec<Vec<usize>>,
    // true iff corresponding stratum is recursive
    is_recursive_stratum_bitmap: Vec<bool>,

    // Mapping of stratum IDs to their entered relations (rule IDs).
    // Later in planner, we will determine exact data collection needs for dynamic transformations.
    stratum_enter_relation: Vec<Vec<u64>>,
    // Mapping of stratum IDs to their iterative relations (rule IDs) (recursive strata only).
    stratum_iterative_relation: Vec<Vec<u64>>,
}

impl Stratifier {
    /// The original (owned) program being stratified.
    #[must_use]
    pub fn program(&self) -> &Program {
        &self.program
    }

    /// Underlying dependency graph (rule -> rules it depends on).
    #[must_use]
    pub fn dependency_graph(&self) -> &DependencyGraph {
        &self.dependency_graph
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
    pub fn stratum(&self) -> Vec<Vec<&MacaronRule>> {
        let mut out = Vec::with_capacity(self.stratum.len());
        for s in &self.stratum {
            out.push(s.iter().map(|&rid| &self.program.rules()[rid]).collect());
        }
        out
    }

    /// Return enter relations.
    #[must_use]
    pub fn stratum_enter_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_enter_relation[idx]
    }

    /// Return iterative relations.
    #[must_use]
    pub fn stratum_iterative_relation(&self, idx: usize) -> &Vec<u64> {
        &self.stratum_iterative_relation[idx]
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
        let mut strata: Vec<Vec<usize>> = Vec::new();
        let mut recursive_bitmap: Vec<bool> = Vec::new();
        for scc in sccs {
            let is_recursive = scc.len() > 1 || {
                let r = scc[0];
                dep_map.get(&r).is_some_and(|deps| deps.contains(&r))
            };
            strata.push(scc);
            recursive_bitmap.push(is_recursive);
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
            stratum_enter_relation: Vec::new(),
            stratum_iterative_relation: Vec::new(),
        };

        instance.build_recursive_metadata();
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

    fn build_recursive_metadata(&mut self) {
        // Relations already computed before the current recursive stratum (EDBs + previous strata heads).
        let mut input_relations: HashSet<u64> = self
            .program
            .edbs()
            .iter()
            .map(|r| r.fingerprint())
            .collect();

        for stratum in self.stratum.iter() {
            let mut enter_rels: HashSet<u64> = HashSet::new();
            let mut iterative_rels: HashSet<u64> = HashSet::new();
            let mut exit_rels: HashSet<u64> = HashSet::new();

            for &rid in stratum {
                let rule = &self.program.rules()[rid];

                // Extract atom fingerprints from RHS predicates.
                for atom_fp in rule.rhs().iter().filter_map(|p| match p {
                    parser::logic::Predicate::PositiveAtomPredicate(atom)
                    | parser::logic::Predicate::NegatedAtomPredicate(atom) => {
                        Some(atom.fingerprint())
                    }
                    _ => None,
                }) {
                    if input_relations.contains(&atom_fp) {
                        // Relation originating outside current recursive stratum – enter relation.
                        enter_rels.insert(atom_fp);
                    }
                    if rule.head().head_fingerprint() == atom_fp {
                        // Derived inside the recursive stratum – iterative relation.
                        iterative_rels.insert(atom_fp);
                    }
                }

                // Head relation produced by this rule – exit relation.
                exit_rels.insert(rule.head().head_fingerprint());
            }

            // Newly produced relations become available to later strata.
            input_relations.extend(&exit_rels);

            self.stratum_enter_relation
                .push(enter_rels.into_iter().collect());
            self.stratum_iterative_relation
                .push(iterative_rels.into_iter().collect());
        }
    }
}

impl fmt::Display for Stratifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                        .unwrap_or_else(|| format!("0x{:x}", fp))
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

            let enters = &self.stratum_enter_relation[idx];
            let iters = &self.stratum_iterative_relation[idx];

            writeln!(f, "  enter:     [{}]", fmt_fps(enters))?;
            if recursive {
                writeln!(f, "  iterative: [{}]", fmt_fps(iters))?;
            }

            for rid in stratum {
                writeln!(f, "{}", self.program.rules()[*rid])?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}
