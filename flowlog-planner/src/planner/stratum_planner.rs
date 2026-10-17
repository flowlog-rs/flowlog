//! Stratum planner that plans a stratum (a group of rules).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::mem;

use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use tracing::debug;
use tracing::trace;

use crate::catalog::Catalog;
use crate::optimizer::Optimizer;
use crate::planner::PlanError;
use crate::planner::RulePlanner;
use crate::planner::Transformation;
use crate::stratifier::Stratum;

mod dedup;

/// Planned transformations and execution metadata for one stratum.
///
/// Work is shared across its rules, and with the preludes of the strata
/// before it, by canonical form: a collection equal to an earlier one is
/// that collection, and a join whose rows another collection already
/// holds becomes a map over it. Recursive plans separate work that runs
/// once from work repeated at each iteration.
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// One planner per rule; these own the raw transformation infos.
    rule_planners: Vec<RulePlanner>,

    /// Whether the stratum is recursive.
    is_recursive: bool,

    /// Deduplicated transformations awaiting recursive partitioning.
    transformations: Vec<Transformation>,

    /// Transformations that depend only on EDB inputs; computed once.
    non_recursive_transformations: Vec<Transformation>,

    /// Transformations that touch IDB inputs; re-run during recursion.
    recursive_transformations: Vec<Transformation>,

    /// Fingerprints of collections that enter recursion.
    recursion_enter_collections: Vec<u64>,

    /// Fingerprints of the collections that feed back into the recursion.
    recursion_feedback_collections: Vec<u64>,

    /// Fingerprints of collections that exit recursion.
    recursion_leave_collections: Vec<u64>,

    /// The collections whose rows each relation of this stratum unions:
    /// one per rule as planned, fewer once sharing lets one collection
    /// stand for several heads. Codegen concatenates them per relation,
    /// and merges a lattice relation's union by its declared column.
    idb_to_heads_map: HashMap<u64, Vec<u64>>,

    /// Atom fingerprints unioned across every rule's rhs. Computed once at
    /// stratum construction so codegen can tell which transformation inputs
    /// are named atoms without re-walking the rule planners.
    atom_fps: HashSet<u64>,

    /// The mutability of every relation the stratum reads or produces,
    /// keyed by fingerprint.
    mutabilities: BTreeMap<u64, Mutability>,
}

impl StratumPlanner {
    /// Builds a stratum planner from a stratum, sharing work with
    /// `preludes`, the non-recursive transformations of the strata before
    /// it in order.
    pub(crate) fn from_stratum(
        program: &Program,
        stratified: &Stratum,
        optimizer: &mut Optimizer,
        plan_graph: &mut Option<PlanGraph>,
        preludes: &[Transformation],
    ) -> Result<Self, PlanError> {
        let rules = program.rules();
        let stratum: Vec<FlowLogRule> = stratified
            .rule_ids()
            .iter()
            .map(|&rule_id| rules[rule_id].clone())
            .collect();
        let is_recursive = stratified.is_recursive();
        let mut catalogs = Vec::with_capacity(stratum.len());
        let mut rule_planners = Vec::with_capacity(stratum.len());

        trace!("New Stratum");

        // Phase 1 plans each rule's body to one atom.
        for (i, rule) in stratum.iter().enumerate() {
            let mut catalog = Catalog::from_rule(rule, &BTreeSet::new())?;
            let mut planner = RulePlanner::new(rule.clone(), stratified.recursive_relations());
            planner.plan_body(&mut catalog, optimizer)?;
            debug!(
                "rule[{i}] planned: {} transformations",
                planner.transformation_infos().len()
            );
            trace!("rule[{i}] planned:\n{catalog}");
            catalogs.push(catalog);
            rule_planners.push(planner);
        }

        // Phase 2 pushes folded original filters down the finished plans.
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.pushdown(catalog)?;
        }

        // Phase 3 combines transformations before final output alignment.
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.fuse(catalog.original_atom_fingerprints())?;
        }

        // Phase 4 aligns each final output with its rule head and row format.
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter_mut()) {
            planner.post(catalog)?;
        }

        // Phase 5 materializes each rule's pipeline and derives every
        // collection's canonical form and mutability.
        for planner in rule_planners.iter_mut() {
            planner.materialize(&|fp| stratified.mutability(fp))?;
        }

        // Debug info for per-rule plan trees
        rule_planners.iter().for_each(|rp| {
            debug!("{}", rp);
        });

        // Profiling: record rule logic profiles if enabled
        with_plan_graph(plan_graph, |plan_graph| {
            for rule_planner in rule_planners.iter() {
                plan_graph.insert_rule(
                    rule_planner.rule().to_string(),
                    rule_planner
                        .transformations()
                        .iter()
                        .map(|tx| {
                            let inputs = if tx.is_unary() {
                                (tx.unary_input().fingerprint(), None)
                            } else {
                                let (left, right) = tx.binary_input();
                                (left.fingerprint(), Some(right.fingerprint()))
                            };
                            (inputs, tx.output().fingerprint())
                        })
                        .collect(),
                );
            }
        });

        // Phase 6 shares work across rules, and with earlier preludes, by
        // canonical form. Heads stay even when nothing in the stratum reads
        // them: codegen unions them into their relations.
        let atom_fps: HashSet<u64> = rule_planners
            .iter()
            .flat_map(RulePlanner::rhs_atom_fps)
            .collect();
        // Each rule's last transformation is its head; dedup rewrites the
        // map as it merges heads or serves one from another collection.
        let mut idb_to_heads_map: HashMap<u64, Vec<u64>> = HashMap::new();
        for (catalog, planner) in catalogs.iter().zip(&rule_planners) {
            if let Some(head) = planner.transformations().last() {
                idb_to_heads_map
                    .entry(catalog.head_idb_fingerprint())
                    .or_default()
                    .push(head.output().fingerprint());
            }
        }
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            recursion_feedback_collections: stratified.recursive_relations().to_vec(),
            recursion_leave_collections: stratified.leave_relations().to_vec(),
            idb_to_heads_map,
            atom_fps,
            mutabilities: stratified.mutabilities().clone(),
            ..Self::default()
        };
        stratum_planner.dedup_transformations(preludes)?;

        // Phase 7 separates recursive operations and builds their metadata.
        stratum_planner.identify_recursive_transformations(is_recursive);
        stratum_planner
            .build_recursion_enter_collections(stratified.available_relations(), preludes);

        // Debug info for non-recursive vs recursive transformations.
        debug!("\n{}", stratum_planner);

        Ok(stratum_planner)
    }
}

// =========================================================================
// Getters
// =========================================================================
impl StratumPlanner {
    /// Get non-recursive transformations that depend only on EDBs.
    /// These transformations can be computed once outside recursion.
    #[inline]
    pub fn non_recursive_transformations(&self) -> &[Transformation] {
        &self.non_recursive_transformations
    }

    /// Get dynamic transformations that depend on IDB collections.
    /// These transformations must be re-evaluated during recursion.
    #[inline]
    pub fn recursive_transformations(&self) -> &[Transformation] {
        &self.recursive_transformations
    }

    /// Whether `tx` belongs to this stratum's recursive partition, i.e. its
    /// tokens are emitted inside the `iterate` scope (`Product<_, _>` time).
    /// Derived from the same partition the emitters iterate, so codegen cannot
    /// desync the emission scope from the call site.
    pub fn is_recursive_transformation(&self, tx: &Transformation) -> bool {
        self.recursive_transformations.contains(tx)
    }

    /// Get fingerprints of collections that enter recursion.
    #[inline]
    pub fn recursion_enter_collections(&self) -> &[u64] {
        &self.recursion_enter_collections
    }

    /// Get fingerprints of the collections that feed back into the recursion.
    #[inline]
    pub fn recursion_feedback_collections(&self) -> &[u64] {
        &self.recursion_feedback_collections
    }

    /// Get fingerprints of collections that leave recursion.
    #[inline]
    pub fn recursion_leave_collections(&self) -> &[u64] {
        &self.recursion_leave_collections
    }

    /// Output relation fingerprints produced by this stratum.
    #[inline]
    pub fn output_relations(&self) -> HashSet<u64> {
        self.idb_to_heads_map.keys().cloned().collect()
    }

    /// Get the mapping from each IDB fingerprint to per-rule head fingerprints.
    #[inline]
    pub fn idb_to_heads_map(&self) -> &HashMap<u64, Vec<u64>> {
        &self.idb_to_heads_map
    }

    /// Map of atom fingerprint to `"name(arg1, ..., argN)"` label for every
    /// positive/negative atom on any rule's rhs in this stratum. Used by
    /// codegen to annotate operator names with the EDB atom they consume so
    /// the profiler/visualizer can show `[Row -> KV] K:(V0) arc(x, y)` without
    /// any downstream knowledge of atoms.
    #[inline]
    pub fn atom_fps(&self) -> &HashSet<u64> {
        &self.atom_fps
    }

    /// Check if this stratum is recursive.
    #[inline]
    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    /// Returns the mutability of a relation this stratum reads or produces,
    /// or `None` for a relation it does not touch.
    ///
    /// A relation it reads has its final value. A relation it produces has
    /// the value of everything produced for it up to this stratum, so a
    /// relation whose rules span strata can have a lower value in an earlier
    /// one. Every head of a recursive stratum has the same value.
    #[must_use]
    pub fn mutability(&self, relation_fp: u64) -> Option<Mutability> {
        self.mutabilities.get(&relation_fp).copied()
    }

    /// Test-only: per-rule transformations before cross-rule dedup.
    #[cfg(test)]
    pub(crate) fn rule_planners(&self) -> &[RulePlanner] {
        &self.rule_planners
    }
}

impl fmt::Display for StratumPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", SECTION_BAR)?;

        let stratum_name = if self.is_recursive {
            "Recursive Stratum"
        } else {
            "Non-Recursive Stratum"
        };
        writeln!(f, "[{stratum_name}] {} rules", self.rule_planners.len())?;
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        writeln!(f, "Rules:")?;
        for (idx, rule_planner) in self.rule_planners.iter().enumerate() {
            writeln!(f, "  ({:>2}) {:?}", idx, rule_planner.rule())?;
        }
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        writeln!(
            f,
            "Non-recursive transformations ({}):",
            self.non_recursive_transformations.len()
        )?;
        for (idx, tx) in self.non_recursive_transformations.iter().enumerate() {
            write!(f, "  [N{:>3}] {}", idx, tx)?;
        }
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        if self.is_recursive {
            writeln!(
                f,
                "Recursive transformations ({}):",
                self.recursive_transformations.len()
            )?;
            for (idx, tx) in self.recursive_transformations.iter().enumerate() {
                write!(f, "  [R{:>3}] {}", idx, tx)?;
            }
        } else {
            writeln!(f, "(Non-recursive stratum: no recursive transformations)")?;
        }

        writeln!(f, "{}", SECTION_BAR)
    }
}

// =========================================================================
// Recursive/Non-Recursive Separation
// =========================================================================
impl StratumPlanner {
    /// Splits transformations into one-time and per-iteration work.
    ///
    /// In recursive strata, a transformation is recursive when it consumes a
    /// stratum output or another recursive transformation.
    fn identify_recursive_transformations(&mut self, is_recursive: bool) {
        if !is_recursive {
            self.non_recursive_transformations = mem::take(&mut self.transformations);
            debug!(
                "Non-recursive stratum: all {} transformations are non-recursive",
                self.non_recursive_transformations.len()
            );
            return;
        }

        let mut dynamic_fingerprints: HashSet<u64> =
            self.idb_to_heads_map.keys().copied().collect();

        let mut dynamic_indices = HashSet::new();

        for (i, transformation) in self.transformations.iter().enumerate() {
            let consumes_dynamic = if transformation.is_unary() {
                let input_fp = transformation.unary_input().fingerprint();
                dynamic_fingerprints.contains(&input_fp)
            } else {
                let (left, right) = transformation.binary_input();
                dynamic_fingerprints.contains(&left.fingerprint())
                    || dynamic_fingerprints.contains(&right.fingerprint())
            };

            if consumes_dynamic {
                dynamic_indices.insert(i);
                dynamic_fingerprints.insert(transformation.output().fingerprint());
            }
        }

        for (i, transformation) in mem::take(&mut self.transformations).into_iter().enumerate() {
            if dynamic_indices.contains(&i) {
                self.recursive_transformations.push(transformation);
            } else {
                self.non_recursive_transformations.push(transformation);
            }
        }

        debug!(
            "Recursive stratum: separated {} non-recursive, {} recursive transformations (total: {})",
            self.non_recursive_transformations.len(),
            self.recursive_transformations.len(),
            self.non_recursive_transformations.len() + self.recursive_transformations.len()
        );
    }
}

// =========================================================================
// Metadata Mappings
// =========================================================================
impl StratumPlanner {
    /// Build the fingerprint of collections that enter recursion: the
    /// relations available to the stratum, its own prelude, and the
    /// preludes of the strata before it.
    fn build_recursion_enter_collections(
        &mut self,
        available_relations: &HashSet<u64>,
        preludes: &[Transformation],
    ) {
        let mut recursion_input_fps: HashSet<u64> = HashSet::new();
        let mut recursion_output_fps: HashSet<u64> = HashSet::new();
        let mut available_fps = available_relations.clone();

        for tx in self.non_recursive_transformations.iter().chain(preludes) {
            available_fps.insert(tx.output().fingerprint());
        }

        for tx in &self.recursive_transformations {
            if tx.is_unary() {
                recursion_input_fps.insert(tx.unary_input().fingerprint());
            } else {
                let (left, right) = tx.binary_input();
                recursion_input_fps.insert(left.fingerprint());
                recursion_input_fps.insert(right.fingerprint());
            }
            recursion_output_fps.insert(tx.output().fingerprint());
        }

        // A value enters recursion only when no repeated transformation
        // produces it.
        self.recursion_enter_collections = recursion_input_fps
            .difference(&recursion_output_fps)
            .filter(|fp| available_fps.contains(fp))
            .copied()
            .collect()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;

    use super::*;
    use crate::test_harness::program_planner;

    /// A planned stratum carries the stratifier's value for each relation it
    /// reads or produces, and none for a relation it does not touch.
    #[test]
    fn stratum_reports_the_mutability_of_what_it_touches() {
        let pp = program_planner(
            "
            .decl E(x: int32) mutable
            .input E
            .decl S(x: int32)
            .input S
            .decl A(x: int32)
            A(x) :- S(x).
            .decl B(x: int32)
            B(x) :- A(x), E(x).
            .output B
            ",
        );
        let producing = |name: &str| {
            pp.strata()
                .iter()
                .find(|stratum| stratum.idb_to_heads_map().contains_key(&compute_fp(name)))
                .expect("a stratum produces the relation")
        };
        let (a, b) = (producing("a"), producing("b"));
        let values = |stratum: &StratumPlanner| {
            ["a", "b", "e", "s"].map(|name| stratum.mutability(compute_fp(name)))
        };

        assert_eq!(
            values(a),
            [
                Some(Mutability::Static),
                None,
                None,
                Some(Mutability::Static)
            ]
        );
        assert_eq!(
            values(b),
            [
                Some(Mutability::Static),
                Some(Mutability::Mutable),
                Some(Mutability::Mutable),
                None
            ]
        );
    }
}
