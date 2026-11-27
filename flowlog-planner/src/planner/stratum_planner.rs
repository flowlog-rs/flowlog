//! Stratum planner that plans a stratum (a group of rules).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::iter;
use std::mem;
use std::sync::Arc;

use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::Atom;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use tracing::debug;
use tracing::trace;

use crate::catalog::Catalog;
use crate::optimizer::Optimizer;
use crate::planner::Collection;
use crate::planner::PlanError;
use crate::planner::RulePlanner;
use crate::planner::Transformation;
use crate::stratifier::Stratum;

mod binding;
mod dedup;

pub use binding::RelationBinding;

/// Each relation's rule head outputs, in concatenation order, while
/// sharing may still merge or replace them; they become its binding's
/// contributors once its seed is known.
type RuleHeadOutputs = BTreeMap<u64, Vec<Arc<Collection>>>;

/// Planned transformations and execution metadata for one stratum.
///
/// Work is shared across its rules, and with the preludes of the strata
/// before it, by canonical form: a collection equal to an earlier one is
/// that collection, and a join whose rows another collection already
/// holds becomes a map over it. Recursive plans separate work that runs
/// once from work repeated at each iteration, and every plan separates the
/// static work outside its loop from the rest.
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// One planner per rule; these own the raw transformation infos.
    rule_planners: Vec<RulePlanner>,

    /// Whether the stratum is recursive.
    is_recursive: bool,

    /// Deduplicated transformations awaiting recursive partitioning.
    transformations: Vec<Transformation>,

    /// Transformations outside any loop whose output is static; computed
    /// once.
    non_recursive_static_transformations: Vec<Transformation>,

    /// Transformations outside any loop whose output is not static.
    non_recursive_non_static_transformations: Vec<Transformation>,

    /// Transformations that touch IDB inputs; re-run during recursion.
    recursive_transformations: Vec<Transformation>,

    /// The collections that enter recursion, each at the mutability it has
    /// outside the loop.
    recursion_enter_collections: Vec<Arc<Collection>>,

    /// The collections that feed back into the recursion: each feedback
    /// relation's binding's collapse.
    recursion_feedback_collections: Vec<Arc<Collection>>,

    /// The static collections that the non-static part reads, each once,
    /// in the order of its first reader.
    non_static_enter_collections: Vec<Arc<Collection>>,

    /// Fingerprints of the relations that leave the recursion: heads of
    /// this stratum, so each is bound in `relation_bindings`.
    recursion_leave_relations: Vec<u64>,

    /// Each relation's binding. Keyed in fingerprint order, the order
    /// codegen emits the relations in.
    relation_bindings: BTreeMap<u64, RelationBinding>,

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
    /// it in order. Every relation binding includes its earlier seed from
    /// `bound_relations`, or its entered outer binding inside recursion.
    ///
    /// # Errors
    ///
    /// Returns errors from rule planning or sharing, including internal
    /// errors for inconsistent metadata or dependencies.
    pub(super) fn from_stratum(
        program: &Program,
        stratified: &Stratum,
        optimizer: &mut Optimizer,
        plan_graph: &mut Option<PlanGraph>,
        preludes: &[Transformation],
        declarations: &HashMap<u64, &Relation>,
        bound_relations: &HashMap<u64, Mutability>,
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
        let mut mutabilities = stratified.mutabilities().clone();
        let mut rule_head_outputs = RuleHeadOutputs::new();

        trace!("New Stratum");

        // Phase 1 plans each rule's body to one atom. A body that reads no
        // rows reads the unit instead, a static relation.
        for (i, rule) in stratum.iter().enumerate() {
            let rule = match rule.to_unit_reading() {
                Some(rule) => {
                    mutabilities.insert(Atom::unit_fingerprint(), Mutability::Static);
                    rule
                }
                None => rule.clone(),
            };
            let mut catalog = Catalog::from_rule(&rule, &BTreeSet::new())?;
            let mut planner = RulePlanner::new(rule, stratified.recursive_relations());
            planner.plan_body(&mut catalog, optimizer)?;
            planner.debug_assert_wired_by_content("plan_body");
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
            planner.debug_assert_wired_by_content("pushdown");
        }

        // Phase 3 combines transformations before final output alignment.
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.fuse(catalog.original_atom_fingerprints())?;
            planner.debug_assert_wired_by_content("fuse");
        }

        // Phase 4 aligns each final output with its rule head and row format.
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter_mut()) {
            planner.post(catalog)?;
            planner.debug_assert_wired_by_content("post");
        }

        // Phase 5 materializes each rule's pipeline and derives every
        // collection's canonical form and mutability.
        for planner in rule_planners.iter_mut() {
            planner.materialize(&|fp| mutabilities.get(&fp).copied())?;
            Transformation::debug_assert_named_by_content(planner.transformations(), "materialize");
        }

        // Debug info for per-rule plan trees
        rule_planners.iter().for_each(|rp| {
            debug!("{}", rp);
        });

        // Phase 6 shares work across rules, and with earlier preludes, by
        // canonical form. Heads stay even when nothing in the stratum reads
        // them: codegen unions them into their relations.
        let atom_fps: HashSet<u64> = rule_planners
            .iter()
            .flat_map(RulePlanner::rhs_atom_fps)
            .collect();
        // Each rule's last transformation is its head; sharing rewrites the
        // bindings as it merges heads or serves one from another collection.
        let heads: Vec<Transformation> = rule_planners
            .iter()
            .map(|planner| {
                planner.transformations().last().cloned().ok_or_else(|| {
                    PlanError::internal(format!(
                        "rule {} has no head transformation after post and materialization",
                        planner.rule()
                    ))
                })
            })
            .collect::<Result<_, PlanError>>()?;
        for (catalog, head) in catalogs.iter().zip(&heads) {
            rule_head_outputs
                .entry(catalog.head_idb_fingerprint())
                .or_default()
                .push(Arc::clone(head.output()));
        }
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            recursion_leave_relations: stratified.leave_relations().to_vec(),
            atom_fps,
            mutabilities,
            ..Self::default()
        };
        stratum_planner.dedup_transformations(preludes, &mut rule_head_outputs)?;

        // Profiling: record each rule as written (not its unit-reading
        // rewrite) with the transformations that produce its head after
        // sharing. Recorded before, a rule would name collections sharing
        // removed, which no plan node carries; recorded later, the
        // transformations would be partitioned away by phase 7.
        with_plan_graph(plan_graph, |plan_graph| {
            for ((rule, catalog), head) in stratum.iter().zip(&catalogs).zip(&heads) {
                let head =
                    Self::standing_for(&rule_head_outputs, catalog.head_idb_fingerprint(), head);
                let plan = stratum_planner.producers_of(head, preludes);
                plan_graph.insert_rule(rule.to_string(), plan);
            }
        });

        // Phase 7 classifies each transformation by where it runs and builds
        // the recursion's metadata.
        stratum_planner.classify_transformations(is_recursive, &rule_head_outputs);
        stratum_planner.build_recursion_enter_collections(
            stratified.available_relations(),
            preludes,
            declarations,
            bound_relations,
        );

        // Phase 8 builds the bindings after recursion-entry metadata is
        // complete, so a recursive binding's seed is only an input available
        // inside the loop.
        let entered: HashSet<u64> = stratum_planner
            .recursion_enter_collections
            .iter()
            .map(|collection| collection.fingerprint())
            .collect();
        for (relation, contributors) in rule_head_outputs {
            let declaration = declarations.get(&relation).ok_or_else(|| {
                PlanError::internal(format!(
                    "relation binding 0x{relation:016x} has no declaration"
                ))
            })?;
            let seed = bound_relations
                .get(&relation)
                .filter(|_| !is_recursive || entered.contains(&relation))
                .map(|&mutability| Arc::new(Collection::from_relation(declaration, mutability)));
            stratum_planner.relation_bindings.insert(
                relation,
                RelationBinding::new(declaration, seed, contributors),
            );
        }
        for relation in stratified.recursive_relations() {
            if let Some(binding) = stratum_planner.relation_bindings.get(relation) {
                let collapse = Arc::clone(binding.collapse());
                stratum_planner
                    .recursion_feedback_collections
                    .push(collapse);
            }
        }

        // Phase 9 lists the static collections the non-static part reads,
        // after the transformations are classified and the bindings built:
        // it reads both.
        stratum_planner.build_non_static_enter_collections();

        debug!("\n{}", stratum_planner);

        Ok(stratum_planner)
    }

    /// The preludes, then this stratum's own transformations: everything
    /// computed by the time this stratum's heads are, producers first.
    /// Complete between sharing and phase 7, while `transformations` holds
    /// this stratum's whole pipeline.
    fn pipeline<'a>(
        &'a self,
        preludes: &'a [Transformation],
    ) -> impl Iterator<Item = &'a Transformation> {
        preludes.iter().chain(&self.transformations)
    }

    /// The fingerprint of the collection standing for `head`'s output
    /// after sharing, among `relation`'s rule head outputs: its own when
    /// it was kept, else the one `merge_equal` made it. Those outputs are
    /// all rows, and `merge_equal` leaves one collection of each form and
    /// shape, so the output with `head`'s form is unique.
    fn standing_for(
        rule_head_outputs: &RuleHeadOutputs,
        relation: u64,
        head: &Transformation,
    ) -> u64 {
        // Searched among the relation's head outputs, not the whole
        // pipeline: an identity map over an unkeyed server has the
        // server's form and shape too (`cover_bodies`), and the server
        // comes first.
        rule_head_outputs
            .get(&relation)
            .and_then(|outputs| {
                outputs
                    .iter()
                    .find(|output| output.canonical() == head.output().canonical())
            })
            .expect("a relation's head outputs hold every head of its rules, merged or kept")
            .fingerprint()
    }

    /// The transformations producing `collection`, transitively: its
    /// producer, the producers of that one's inputs, and so on down to the
    /// relations, in the pipeline's order. Each is recorded once as
    /// `((input, second input of a join), output)` fingerprints.
    fn producers_of(
        &self,
        collection: u64,
        preludes: &[Transformation],
    ) -> Vec<((u64, Option<u64>), u64)> {
        let producers: HashMap<u64, &Transformation> = self
            .pipeline(preludes)
            .map(|tx| (tx.output().fingerprint(), tx))
            .collect();
        let mut reached: HashSet<u64> = HashSet::new();
        let mut pending = vec![collection];
        while let Some(fingerprint) = pending.pop() {
            let Some(tx) = producers.get(&fingerprint) else {
                continue;
            };
            if reached.insert(fingerprint) {
                pending.extend(tx.input_fingerprints());
            }
        }
        self.pipeline(preludes)
            .filter(|tx| reached.contains(&tx.output().fingerprint()))
            .map(|tx| {
                let inputs = if tx.is_unary() {
                    (tx.unary_input().fingerprint(), None)
                } else {
                    let (left, right) = tx.binary_input();
                    (left.fingerprint(), Some(right.fingerprint()))
                };
                (inputs, tx.output().fingerprint())
            })
            .collect()
    }
}

// =========================================================================
// Getters
// =========================================================================
impl StratumPlanner {
    /// Returns the transformations outside any loop, in the order they
    /// run: the static ones, then the non-static ones.
    #[must_use = "consume the iterator to read the transformations"]
    pub fn non_recursive_transformations(
        &self,
    ) -> impl Iterator<Item = &Transformation> + Clone + '_ {
        self.non_recursive_static_transformations
            .iter()
            .chain(&self.non_recursive_non_static_transformations)
    }

    /// Returns the transformations outside any loop whose output is
    /// static, in planned order. A static transformation reads only static
    /// collections, so the static transformations of every stratum, taken
    /// in stratum order, read nothing non-static.
    #[must_use]
    pub fn non_recursive_static_transformations(&self) -> &[Transformation] {
        &self.non_recursive_static_transformations
    }

    /// Returns the transformations outside any loop whose output is not
    /// static, in planned order.
    #[must_use]
    pub fn non_recursive_non_static_transformations(&self) -> &[Transformation] {
        &self.non_recursive_non_static_transformations
    }

    /// Get dynamic transformations that depend on IDB collections.
    /// These transformations must be re-evaluated during recursion.
    #[inline]
    pub fn recursive_transformations(&self) -> &[Transformation] {
        &self.recursive_transformations
    }

    /// Returns the collections that enter the recursion, in fingerprint
    /// order, each at the mutability it has outside the loop. A relation
    /// the recursion rebinds enters as its earlier binding, which can be
    /// static though the recursion is not.
    #[must_use]
    pub fn recursion_enter_collections(&self) -> &[Arc<Collection>] {
        &self.recursion_enter_collections
    }

    /// Returns the static collections that the stratum's non-static part
    /// reads, each once, in the order of its first reader: the inputs of
    /// its non-static transformations outside the loop, then the seed and
    /// rule results its non-static relation bindings union outside a loop,
    /// or what enters its loop when the loop is non-static and a relation
    /// leaves it.
    #[must_use]
    pub fn non_static_enter_collections(&self) -> &[Arc<Collection>] {
        &self.non_static_enter_collections
    }

    /// The collections that feed back into the recursion, in their
    /// relations' fingerprint order: each the collapse of its relation's
    /// binding, which a loop variable holds.
    #[inline]
    #[must_use]
    pub fn recursion_feedback_collections(&self) -> &[Arc<Collection>] {
        &self.recursion_feedback_collections
    }

    /// The bindings of the relations that leave the recursion, in
    /// fingerprint order: each leaves the loop as its
    /// [`collapse`](RelationBinding::collapse), which it then maps into its
    /// relation.
    #[must_use = "consume the iterator to read the leaving bindings"]
    pub fn recursion_leave_bindings(&self) -> impl Iterator<Item = &RelationBinding> + '_ {
        self.recursion_leave_relations
            .iter()
            .filter_map(|relation_fp| self.relation_bindings.get(relation_fp))
    }

    /// Output relation fingerprints produced by this stratum.
    #[must_use = "consume the iterator to read the produced relations"]
    pub fn output_relations(&self) -> impl Iterator<Item = u64> + '_ {
        self.relation_bindings.keys().copied()
    }

    /// Each relation's binding, in fingerprint order.
    #[must_use]
    pub fn relation_bindings(&self) -> &BTreeMap<u64, RelationBinding> {
        &self.relation_bindings
    }

    /// Returns the relation bindings whose output is static, in
    /// fingerprint order. A recursive stratum's bindings share one
    /// mutability, so they are all static or none is.
    #[must_use]
    pub fn static_relation_bindings(&self) -> Vec<&RelationBinding> {
        let bindings = self.relation_bindings.values();
        bindings
            .filter(|binding| binding.output().mutability().is_static())
            .collect()
    }

    /// Returns the relation bindings whose output is not static, in
    /// fingerprint order.
    #[must_use]
    pub fn non_static_relation_bindings(&self) -> Vec<&RelationBinding> {
        let bindings = self.relation_bindings.values();
        bindings
            .filter(|binding| !binding.output().mutability().is_static())
            .collect()
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

    /// Returns `true` if a rule of this stratum reads the unit
    /// ([`Atom::UNIT_NAME`]).
    #[must_use]
    pub fn reads_unit(&self) -> bool {
        self.atom_fps.contains(&Atom::unit_fingerprint())
    }

    /// Returns the mutability of a relation this stratum reads or produces,
    /// or `None` for a relation it does not touch.
    ///
    /// A relation it reads has its final value. A relation it produces has
    /// the value of everything produced for it up to this stratum, so a
    /// relation whose rules span strata can have a lower value in an earlier
    /// one. Recursive relation bindings all have the same value.
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
            self.non_recursive_transformations().count()
        )?;
        for (idx, tx) in self.non_recursive_transformations().enumerate() {
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
// Transformation Classification
// =========================================================================
impl StratumPlanner {
    /// Classifies each transformation by where it runs: inside the
    /// stratum's loop, or outside any loop, and there by whether its output
    /// is static.
    ///
    /// In recursive strata, a transformation is recursive when it consumes a
    /// relation in `produced` or another recursive transformation.
    fn classify_transformations(&mut self, is_recursive: bool, produced: &RuleHeadOutputs) {
        let mut dynamic_indices: HashSet<usize> = HashSet::new();
        if is_recursive {
            let mut dynamic_fingerprints: HashSet<u64> = produced.keys().copied().collect();
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
        }

        for (i, transformation) in mem::take(&mut self.transformations).into_iter().enumerate() {
            if dynamic_indices.contains(&i) {
                self.recursive_transformations.push(transformation);
            } else if transformation.output().mutability().is_static() {
                let (first, second) = transformation.inputs();
                debug_assert!(
                    first.mutability().is_static()
                        && second.is_none_or(|input| input.mutability().is_static()),
                    "an output is as mutable as its most mutable input, yet static \
                     {transformation} reads a non-static collection"
                );
                self.non_recursive_static_transformations
                    .push(transformation);
            } else {
                self.non_recursive_non_static_transformations
                    .push(transformation);
            }
        }

        debug!(
            "Separated {} static, {} non-static and {} recursive transformations",
            self.non_recursive_static_transformations.len(),
            self.non_recursive_non_static_transformations.len(),
            self.recursive_transformations.len(),
        );
    }
}

// =========================================================================
// Metadata Mappings
// =========================================================================
impl StratumPlanner {
    /// Builds the static collections that the non-static part reads, from
    /// the classified transformations, the relation bindings and the
    /// recursion's entering collections, all built before it.
    fn build_non_static_enter_collections(&mut self) {
        let mut read: Vec<Arc<Collection>> = Vec::new();
        for tx in &self.non_recursive_non_static_transformations {
            let (first, second) = tx.inputs();
            read.extend(iter::once(first).chain(second).cloned());
        }
        let bindings: Vec<&RelationBinding> = self
            .relation_bindings
            .values()
            .filter(|binding| !binding.output().mutability().is_static())
            .collect();
        if !self.is_recursive {
            for binding in bindings {
                read.extend(binding.input_seed().cloned().map(Arc::new));
                read.extend(binding.input_contributors().iter().cloned());
            }
        } else if !bindings.is_empty() && self.recursion_leave_bindings().next().is_some() {
            // A loop no relation leaves is not computed, so nothing enters
            // it.
            read.extend(self.recursion_enter_collections.iter().cloned());
        }

        // A vector keeps the order of first readers; the set tells a
        // collection already listed without scanning it.
        let mut entering: Vec<Arc<Collection>> = Vec::new();
        let mut listed: HashSet<u64> = HashSet::new();
        for collection in read {
            if collection.mutability().is_static() && listed.insert(collection.fingerprint()) {
                entering.push(collection);
            }
        }
        self.non_static_enter_collections = entering;
    }

    /// Build the fingerprint of collections that enter recursion: the
    /// relations available to the stratum, its own prelude, and the
    /// preludes of the strata before it. They are in fingerprint order, the
    /// order codegen enters them in.
    fn build_recursion_enter_collections(
        &mut self,
        available_relations: &HashSet<u64>,
        preludes: &[Transformation],
        declarations: &HashMap<u64, &Relation>,
        bound_relations: &HashMap<u64, Mutability>,
    ) {
        let mut recursion_inputs: BTreeMap<u64, &Arc<Collection>> = BTreeMap::new();
        let mut recursion_output_fps: BTreeSet<u64> = BTreeSet::new();
        let mut available_fps = available_relations.clone();

        for tx in self.non_recursive_transformations().chain(preludes) {
            available_fps.insert(tx.output().fingerprint());
        }

        for tx in &self.recursive_transformations {
            let (first, second) = tx.inputs();
            for input in iter::once(first).chain(second) {
                recursion_inputs.insert(input.fingerprint(), input);
            }
            recursion_output_fps.insert(tx.output().fingerprint());
        }

        // A value enters recursion only when no repeated transformation
        // produces it. The loop reads a relation it rebinds at the loop's
        // mutability, but the relation enters as its binding so far.
        let entering = recursion_inputs
            .into_iter()
            .filter(|(fp, _)| !recursion_output_fps.contains(fp) && available_fps.contains(fp));
        self.recursion_enter_collections = entering
            .map(
                |(fp, input)| match (declarations.get(&fp), bound_relations.get(&fp)) {
                    (Some(declaration), Some(&mutability)) => {
                        Arc::new(Collection::from_relation(declaration, mutability))
                    }
                    (Some(_), None) | (None, Some(_)) | (None, None) => Arc::clone(input),
                },
            )
            .collect();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::stratifier::Stratifier;
    use crate::test_harness::program_planner;

    /// Plans the first stratum of `source`, with every input bound.
    fn plan_first_stratum(source: &str) -> Result<StratumPlanner, PlanError> {
        let program = program(source);
        let stratifier = Stratifier::from_program(&program).expect("stratified");
        let declarations: HashMap<u64, &Relation> = program
            .relations()
            .iter()
            .map(|relation| (relation.fingerprint(), relation))
            .collect();
        let inputs: HashMap<u64, Mutability> = program
            .edbs()
            .into_iter()
            .map(|relation| (relation.fingerprint(), relation.input_mutability()))
            .collect();
        StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[0],
            &mut Optimizer::new(),
            &mut None,
            &[],
            &declarations,
            &inputs,
        )
    }

    #[test]
    fn stratum_construction_resolves_the_existing_input_seed() {
        let stratum = plan_first_stratum(
            r#"
            .decl E(x: int32)
            .input E
            .decl R(x: int32)
            .input R
            .output R
            R(x) :- E(x).
            "#,
        )
        .expect("planned");
        assert_eq!(
            stratum.relation_bindings()[&compute_fp("r")]
                .input_seed()
                .map(|seed| seed.fingerprint()),
            Some(compute_fp("r"))
        );
    }

    /// Returns a program joining a `left`-column relation to a
    /// `right`-column one on their first columns, then to `Link` on the
    /// second columns of both, so the first join carries the other
    /// `left - 2` and `right - 2` columns and one join key: a row of
    /// `left + right - 3` columns.
    fn join_of_widths(left: usize, right: usize) -> String {
        let decl = |width: usize| -> String {
            let columns: Vec<String> = (0..width).map(|i| format!("c{i}: int32")).collect();
            columns.join(", ")
        };
        let side = |prefix: &str, width: usize| -> String {
            let vars: Vec<String> = (1..width).map(|i| format!("{prefix}{i}")).collect();
            format!("a0, {}", vars.join(", "))
        };
        let sum: Vec<String> = (2..left)
            .map(|i| format!("a{i}"))
            .chain((2..right).map(|i| format!("b{i}")))
            .collect();
        format!(
            ".decl L({left_decl})\n.input L\n.decl R({right_decl})\n.input R\n\
             .decl Link(x: int32, y: int32)\n.input Link\n\
             .decl Out(s: int32)\n.output Out\n\
             Out({sum}) :- L({left_side}), R({right_side}), Link(a1, b1).\n",
            left_decl = decl(left),
            right_decl = decl(right),
            sum = sum.join(" + "),
            left_side = side("a", left),
            right_side = side("b", right),
        )
    }

    /// A row may hold exactly the limit: the widest row the plan carries is
    /// the `left + right - 3` columns of the first join.
    #[rstest]
    #[case::under_the_limit(33, 33, 63)]
    #[case::at_the_limit(33, 34, 64)]
    fn a_rule_carrying_a_row_within_the_limit_plans(
        #[case] left: usize,
        #[case] right: usize,
        #[case] expected: usize,
    ) {
        let planner = plan_first_stratum(&join_of_widths(left, right)).expect("plans");
        let widest = planner
            .non_recursive_transformations()
            .map(|transformation| {
                let (keys, values) = transformation.output().arity();
                keys.max(values)
            })
            .max();
        assert_eq!(widest, Some(expected));
    }

    #[test]
    fn a_rule_carrying_more_columns_than_a_row_holds_is_refused() {
        let result = plan_first_stratum(&join_of_widths(34, 34));
        assert!(
            matches!(result, Err(PlanError::RowTooWide { width: 65, .. })),
            "{:?}",
            result.err()
        );
    }

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
                .find(|stratum| stratum.relation_bindings().contains_key(&compute_fp(name)))
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

    /// A body that reads no rows reads the unit instead, which the stratum
    /// records as a static relation it reads; a body with rows does not.
    // Cases: rule, whether it reads the unit.
    #[rstest]
    #[case::negation("M(0) :- !On().", true)]
    #[case::rows("M(0) :- B(0, _), !On().", false)]
    fn only_a_body_without_rows_reads_the_static_unit(
        #[case] rule: &str,
        #[case] reads_unit: bool,
    ) {
        let pp = program_planner(&format!(
            "
            .decl On()
            .input On
            .decl B(x: int32, y: int32)
            .input B
            .decl M(x: int32)
            .output M
            {rule}
            "
        ));
        let [stratum] = pp.strata() else {
            panic!("one rule plans one stratum");
        };

        assert_eq!(stratum.reads_unit(), reads_unit);
        assert_eq!(
            stratum.mutability(Atom::unit_fingerprint()),
            reads_unit.then_some(Mutability::Static)
        );
    }

    /// A body of only a negation derives its head from an antijoin of the
    /// unit, which keeps the unit's one row exactly when the negated atom
    /// matches nothing.
    #[test]
    fn a_negation_only_body_antijoins_the_unit() {
        let pp = program_planner(
            "
            .decl On()
            .input On
            .decl M(x: int32)
            .output M
            M(0) :- !On().
            ",
        );
        let [stratum] = pp.strata() else {
            panic!("one rule plans one stratum");
        };
        let head = stratum
            .non_recursive_transformations()
            .last()
            .expect("the rule derives its head");

        assert_eq!(head.profile_operation_name(), "AntiJoin");
    }

    #[test]
    fn binding_contributors_are_the_producing_heads_themselves() {
        let plan = program_planner(
            ".decl A(a:symbol,b:int32,c:int64)\n.input A\n\
             .decl B(a:symbol,b:int32,c:int64)\n.input B\n\
             .decl Out(c:int64,a:symbol,b:int32)\n.output Out\n\
             Out(c,a,b) :- A(a,b,c).\nOut(c,a,b) :- B(a,b,c).\n",
        );
        let stratum = &plan.strata()[0];
        let contributors = stratum.relation_bindings()[&compute_fp("out")].input_contributors();
        assert_eq!(contributors.len(), 2);
        for contributor in contributors {
            let head = stratum
                .non_recursive_transformations()
                .find(|step| step.output().fingerprint() == contributor.fingerprint())
                .expect("each contributor is a head of this stratum");
            assert!(Arc::ptr_eq(contributor, head.output()));
        }
    }

    #[test]
    fn shared_rule_results_are_referenced_directly_by_each_binding() {
        let plan = program_planner(
            ".decl A(a:int32,b:int32)\n.input A\n\
             .decl Out(a:int32,b:int32)\n.output Out\n\
             .decl Other(a:int32,b:int32)\n.output Other\n\
             Out(b,a) :- A(a,b).\nOther(b,a) :- A(a,b).\n",
        );
        let bindings = plan.strata()[0].relation_bindings();
        let [out] = bindings[&compute_fp("out")].input_contributors() else {
            panic!("Out has one head");
        };
        let [other] = bindings[&compute_fp("other")].input_contributors() else {
            panic!("Other has one head");
        };
        assert!(Arc::ptr_eq(out, other));
    }

    #[test]
    fn a_recursive_seed_keeps_its_outer_static_weight() {
        let plan = program_planner(
            ".decl Base(x:int32,y:int32)\n.input Base\n\
             .decl Edge(x:int32,y:int32) mutable\n.input Edge\n\
             .decl Reach(x:int32,y:int32)\n.output Reach\n\
             Reach(x,y) :- Base(x,y).\nReach(x,z) :- Reach(x,y), Edge(y,z).\n",
        );
        let recursive = plan
            .strata()
            .iter()
            .find(|stratum| stratum.is_recursive())
            .expect("Reach recurses over Edge");
        let seed = recursive.relation_bindings()[&compute_fp("reach")]
            .input_seed()
            .expect("the base rule's binding seeds the recursion");
        assert_eq!(seed.mutability(), Mutability::Static);
    }

    #[test]
    fn a_recursive_binding_keeps_its_outer_seed_apart_from_feedback() {
        let plan = program_planner(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let [base, recursive] = plan.strata() else {
            panic!("Reach has a base stratum and a recursive stratum");
        };
        let relation = compute_fp("reach");
        assert_eq!(base.relation_bindings()[&relation].input_seed(), None);
        let binding = &recursive.relation_bindings()[&relation];
        assert_eq!(
            binding.input_seed().map(|seed| seed.fingerprint()),
            Some(relation)
        );
        assert!(
            !binding
                .input_contributor_fingerprints()
                .any(|contributor| contributor == relation)
        );
        assert!(
            recursive
                .recursion_feedback_collections()
                .iter()
                .any(|collection| collection.fingerprint() == relation)
        );
    }

    /// A loop variable holds its relation as the binding collapses it, so
    /// the feedback collection is that collapse itself.
    #[test]
    fn a_feedback_collection_is_its_bindings_collapse() {
        let plan = program_planner(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let [_, recursive] = plan.strata() else {
            panic!("Reach has a base stratum and a recursive stratum");
        };
        let binding = &recursive.relation_bindings()[&compute_fp("reach")];
        let [feedback] = recursive.recursion_feedback_collections() else {
            panic!("only Reach feeds back");
        };
        assert!(Arc::ptr_eq(feedback, binding.collapse()));
    }

    /// `Hop` is bound in the loop it shares with `Reach`, but nothing past
    /// that loop reads it, so only `Reach` leaves.
    #[test]
    fn only_a_relation_read_past_its_loop_leaves_it() {
        let plan = program_planner(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Hop(x: int32, y: int32)
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Hop(x,z) :- Reach(x,y), Arc(y,z).
            Reach(x,z) :- Hop(x,z).
            "#,
        );
        let recursive = plan
            .strata()
            .iter()
            .find(|stratum| stratum.is_recursive())
            .expect("Reach and Hop recurse");
        assert!(
            recursive
                .relation_bindings()
                .contains_key(&compute_fp("hop"))
        );
        let leaving: Vec<u64> = recursive
            .recursion_leave_bindings()
            .map(|binding| binding.output().fingerprint())
            .collect();
        assert_eq!(leaving, [compute_fp("reach")]);
    }

    #[test]
    fn a_later_non_recursive_binding_keeps_the_earlier_binding() {
        let plan = program_planner(
            r#"
            .decl E(x: int32)
            .input E
            .decl F(x: int32)
            .input F
            .decl G(x: int32)
            .input G
            .decl Aux(x: int32)
            .decl R(x: int32)
            .output R
            Aux(x) :- G(x).
            R(x) :- E(x).
            R(x) :- Aux(x), F(x).
            "#,
        );
        let [base, later] = plan.strata() else {
            panic!("Aux separates the two rules for R into two strata");
        };
        let relation = compute_fp("r");
        assert!(!base.is_recursive());
        assert!(!later.is_recursive());
        assert_eq!(base.relation_bindings()[&relation].input_seed(), None);
        let binding = &later.relation_bindings()[&relation];
        assert_eq!(
            binding.input_seed().map(|seed| seed.fingerprint()),
            Some(relation)
        );
        assert_eq!(binding.input_contributors().len(), 1);
        assert!(
            !binding
                .input_contributor_fingerprints()
                .any(|contributor| contributor == relation)
        );
    }

    /// Static links, mutable edges, and a rule over each: one stratum.
    const RULES_OF_BOTH_KINDS: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Path(x: int32, y: int32)
        .output Path
        .decl Hop(x: int32, y: int32)
        .output Hop
        Path(x, y) :- Link(x, y).
        Hop(x, y) :- Edge(x, y).
    ";

    /// A non-static rule, which keys the static links for its join.
    const STATIC_STEP_OF_A_NON_STATIC_RULE: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Hop(x: int32, z: int32)
        .output Hop
        Hop(x, z) :- Edge(x, y), Link(y, z).
    ";

    /// A loop over mutable edges, which keys the static links outside it.
    /// Its base rule is the stratum before it.
    const NON_STATIC_LOOP: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Reach(x: int32, y: int32)
        .output Reach
        Reach(x, y) :- Edge(x, y).
        Reach(x, z) :- Reach(x, y), Link(y, z).
    ";

    /// A loop over static links alone. Its base rule is the stratum before
    /// it.
    const STATIC_LOOP: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Path(x: int32, y: int32)
        .output Path
        Path(x, y) :- Link(x, y).
        Path(x, z) :- Path(x, y), Link(y, z).
    ";

    /// Each step outside a loop and each relation binding is static
    /// exactly when its output is, so a non-static rule still has static
    /// steps, and a loop's bindings are all of the loop's mutability.
    // Cases: program, stratum, static steps, static relations, non-static
    // steps, non-static relations.
    #[rstest]
    #[case::rules_of_both_kinds(RULES_OF_BOTH_KINDS, 0, 1, &["path"], 1, &["hop"])]
    #[case::static_step_of_a_non_static_rule(
        STATIC_STEP_OF_A_NON_STATIC_RULE,
        0,
        1,
        &[],
        2,
        &["hop"]
    )]
    #[case::non_static_loop(NON_STATIC_LOOP, 1, 1, &[], 0, &["reach"])]
    #[case::static_loop(STATIC_LOOP, 1, 1, &["path"], 0, &[])]
    fn a_stratum_splits_its_steps_and_bindings_by_mutability(
        #[case] source: &str,
        #[case] stratum: usize,
        #[case] static_steps: usize,
        #[case] static_relations: &[&str],
        #[case] non_static_steps: usize,
        #[case] non_static_relations: &[&str],
    ) {
        let planner = program_planner(source);
        let stratum = &planner.strata()[stratum];
        let relations = |bindings: Vec<&RelationBinding>| -> Vec<u64> {
            let outputs = bindings.iter().map(|binding| binding.output());
            outputs.map(|output| output.fingerprint()).collect()
        };
        let fingerprints = |relations: &[&str]| -> Vec<u64> {
            relations.iter().copied().map(compute_fp).collect()
        };
        let mutabilities = |steps: &[Transformation]| -> Vec<Mutability> {
            let outputs = steps.iter().map(Transformation::output);
            outputs.map(|output| output.mutability()).collect()
        };

        assert_eq!(
            mutabilities(stratum.non_recursive_static_transformations()),
            vec![Mutability::Static; static_steps]
        );
        let non_static = mutabilities(stratum.non_recursive_non_static_transformations());
        assert_eq!(non_static.len(), non_static_steps);
        assert!(!non_static.contains(&Mutability::Static), "{non_static:?}");
        assert_eq!(
            relations(stratum.static_relation_bindings()),
            fingerprints(static_relations)
        );
        assert_eq!(
            relations(stratum.non_static_relation_bindings()),
            fingerprints(non_static_relations)
        );
    }

    /// A loop over mutable edges whose relation is first bound from static
    /// links, in the stratum before it.
    const NON_STATIC_LOOP_OVER_A_STATIC_SEED: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Reach(x: int32, y: int32)
        .output Reach
        Reach(x, y) :- Link(x, y).
        Reach(x, z) :- Reach(x, y), Edge(y, z).
    ";

    /// A relation its loop rebinds enters as its earlier binding, static or
    /// not as that binding is, though the loop is non-static either way.
    // Cases: program, the mutability `Reach` enters at.
    #[rstest]
    #[case::static_seed(NON_STATIC_LOOP_OVER_A_STATIC_SEED, Mutability::Static)]
    #[case::non_static_seed(NON_STATIC_LOOP, Mutability::Mutable)]
    fn a_rebound_relation_enters_its_loop_with_its_earlier_mutability(
        #[case] source: &str,
        #[case] entered: Mutability,
    ) {
        let planner = program_planner(source);
        let recursive = &planner.strata()[1];
        let entering = recursive.recursion_enter_collections().iter();
        let reach: Vec<Mutability> = entering
            .filter(|collection| collection.fingerprint() == compute_fp("reach"))
            .map(|collection| collection.mutability())
            .collect();

        assert_eq!(reach, [entered]);
        assert!(recursive.static_relation_bindings().is_empty());
    }

    /// A rule over static links and one over mutable edges, both deriving
    /// one relation.
    const UNION_OF_BOTH_KINDS: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Both(x: int32, y: int32)
        .output Both
        Both(x, y) :- Link(x, y).
        Both(x, y) :- Edge(x, y).
    ";

    /// The non-static part reads a static collection where a non-static
    /// step outside the loop reads it, where a non-static relation unions
    /// it, and where it enters a non-static loop; a stratum with no
    /// non-static part, or one that reads nothing static, reads none.
    // Cases: program, stratum, keyed collections read, row collections
    // read.
    #[rstest]
    #[case::join_input(STATIC_STEP_OF_A_NON_STATIC_RULE, 0, 1, 0)]
    #[case::union_input(UNION_OF_BOTH_KINDS, 0, 0, 1)]
    #[case::loop_input(NON_STATIC_LOOP, 1, 1, 0)]
    #[case::loop_seed(NON_STATIC_LOOP_OVER_A_STATIC_SEED, 1, 0, 1)]
    #[case::static_loop(STATIC_LOOP, 1, 0, 0)]
    #[case::separate_rules(RULES_OF_BOTH_KINDS, 0, 0, 0)]
    fn the_non_static_part_reads_each_static_collection_it_uses(
        #[case] source: &str,
        #[case] stratum: usize,
        #[case] keyed: usize,
        #[case] rows: usize,
    ) {
        let planner = program_planner(source);
        let entering = planner.strata()[stratum].non_static_enter_collections();
        let is_keyed = |collection: &&Arc<Collection>| collection.arity().0 > 0;

        assert_eq!(entering.iter().filter(is_keyed).count(), keyed);
        assert_eq!(entering.len() - keyed, rows);
        assert!(
            entering
                .iter()
                .all(|collection| collection.mutability().is_static())
        );
    }
}
