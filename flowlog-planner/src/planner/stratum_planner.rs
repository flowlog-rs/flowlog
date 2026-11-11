//! Stratum planner that plans a stratum (a group of rules).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::mem;
use std::sync::Arc;

use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::Atom;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
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

pub(super) use binding::BindingDeclaration;
pub use binding::BindingOperation;
pub use binding::RelationBindingPlan;

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

    /// Work independent of this stratum's feedback, including earlier IDBs.
    non_recursive_transformations: Vec<Transformation>,

    /// Work that transitively depends on this stratum's feedback.
    recursive_transformations: Vec<Transformation>,

    /// Fingerprints of collections that enter recursion.
    recursion_enter_collections: Vec<u64>,

    /// Fingerprints of the collections that feed back into the recursion.
    recursion_feedback_collections: Vec<u64>,

    /// Fingerprints of collections that exit recursion.
    recursion_leave_collections: Vec<u64>,

    /// Each relation's inputs, seed, and dedup or declared aggregation.
    /// Keyed in fingerprint order, the order codegen emits the relations in.
    relation_bindings: BTreeMap<u64, RelationBindingPlan>,

    /// Atom fingerprints unioned across every rule's rhs. Computed once at
    /// stratum construction so codegen can tell which transformation inputs
    /// are named atoms without re-walking the rule planners.
    atom_fps: HashSet<u64>,

    /// The mutability of every relation the stratum reads or produces,
    /// keyed by fingerprint.
    mutabilities: BTreeMap<u64, Mutability>,
}

impl StratumPlanner {
    #[must_use]
    pub(super) fn non_recursive_transformations_mut(&mut self) -> &mut Vec<Transformation> {
        &mut self.non_recursive_transformations
    }

    /// Allows column rewrites without changing feedback-dependent membership.
    #[must_use]
    pub(super) fn recursive_transformations_mut(&mut self) -> &mut [Transformation] {
        &mut self.recursive_transformations
    }

    /// Returns a binding for rewriting, preserving the set of bound relations.
    #[must_use]
    pub(super) fn relation_binding_mut(
        &mut self,
        relation: u64,
    ) -> Option<&mut RelationBindingPlan> {
        self.relation_bindings.get_mut(&relation)
    }

    #[must_use]
    pub(super) fn recursion_enter_collections_mut(&mut self) -> &mut Vec<u64> {
        &mut self.recursion_enter_collections
    }

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
        declarations: &HashMap<u64, BindingDeclaration<'_>>,
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
            planner.materialize(&|fp| mutabilities.get(&fp).copied())?;
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
        let mut rule_outputs: BTreeMap<u64, Vec<Arc<Collection>>> = BTreeMap::new();
        for (catalog, head) in catalogs.iter().zip(&heads) {
            rule_outputs
                .entry(catalog.head_idb_fingerprint())
                .or_default()
                .push(Arc::clone(head.output()));
        }
        let relation_bindings: BTreeMap<u64, RelationBindingPlan> = rule_outputs
            .into_iter()
            .map(|(relation, inputs)| {
                let declaration = declarations.get(&relation).ok_or_else(|| {
                    PlanError::internal(format!(
                        "relation binding 0x{relation:016x} has no declaration"
                    ))
                })?;
                Ok((
                    relation,
                    RelationBindingPlan::new(declaration.operation().clone(), inputs),
                ))
            })
            .collect::<Result<_, PlanError>>()?;
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            recursion_feedback_collections: stratified.recursive_relations().to_vec(),
            recursion_leave_collections: stratified.leave_relations().to_vec(),
            relation_bindings,
            atom_fps,
            mutabilities,
            ..Self::default()
        };
        stratum_planner.dedup_transformations(preludes)?;

        // Profiling: record each rule as written (not its unit-reading
        // rewrite) with the transformations that produce its head after
        // sharing. Recorded before, a rule would name collections sharing
        // removed, which no plan node carries; recorded later, the
        // transformations would be partitioned away by phase 7.
        with_plan_graph(plan_graph, |plan_graph| {
            for (rule, head) in stratum.iter().zip(&heads) {
                let head = stratum_planner.standing_for(head, preludes);
                let plan = stratum_planner.producers_of(head, preludes);
                plan_graph.insert_rule(rule.to_string(), plan);
            }
        });

        // Phase 7 separates recursive operations and builds their metadata.
        stratum_planner.identify_recursive_transformations(is_recursive);
        stratum_planner
            .build_recursion_enter_collections(stratified.available_relations(), preludes);

        // Phase 8 resolves seeds after recursion-entry metadata is complete, so
        // recursive bindings only reference inputs available inside the loop.
        // Finish before returning so the plan needs no later binding pass.
        let entered: HashSet<u64> = stratum_planner
            .recursion_enter_collections
            .iter()
            .copied()
            .collect();
        for (&relation, binding) in &mut stratum_planner.relation_bindings {
            if is_recursive && !entered.contains(&relation) {
                continue;
            }
            if let Some(&mutability) = bound_relations.get(&relation) {
                let declaration = declarations.get(&relation).ok_or_else(|| {
                    PlanError::internal(format!("seed 0x{relation:016x} has no declaration"))
                })?;
                *binding.seed_mut() = Some(Arc::new(Collection::from_relation(
                    declaration.relation(),
                    mutability,
                )));
            }
        }

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
    /// after sharing: the one output in the pipeline with its form and
    /// shape, its own when it was kept. Sharing merges only collections of
    /// one form and shape and keeps exactly one of each (`merge_equal`), so
    /// the output is unique; it exists for every head, which a relation
    /// unions.
    fn standing_for(&self, head: &Transformation, preludes: &[Transformation]) -> u64 {
        // Found by form rather than tracked through `alias`: tracking would
        // keep per-rule state the stratum never reads after construction.
        self.pipeline(preludes)
            .find(|tx| {
                tx.output().canonical() == head.output().canonical()
                    && tx.need_arrange() == head.need_arrange()
            })
            .expect("sharing keeps one collection of every form and shape a relation unions")
            .output()
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

    /// Returns `true` if `tx` runs inside this stratum's recursive scope,
    /// rather than in its one-time prelude.
    #[must_use]
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
    #[must_use = "consume the iterator to read the produced relations"]
    pub fn output_relations(&self) -> impl Iterator<Item = u64> + '_ {
        self.relation_bindings.keys().copied()
    }

    /// Each relation's planned inputs, seed, and binding operation, in
    /// fingerprint order.
    #[must_use]
    pub fn relation_bindings(&self) -> &BTreeMap<u64, RelationBindingPlan> {
        &self.relation_bindings
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
            self.relation_bindings.keys().copied().collect();

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
    /// preludes of the strata before it. They are in fingerprint order, the
    /// order codegen enters them in.
    fn build_recursion_enter_collections(
        &mut self,
        available_relations: &HashSet<u64>,
        preludes: &[Transformation],
    ) {
        let mut recursion_input_fps: BTreeSet<u64> = BTreeSet::new();
        let mut recursion_output_fps: BTreeSet<u64> = BTreeSet::new();
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
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::stratifier::Stratifier;
    use crate::test_harness::program_planner;

    #[test]
    fn stratum_construction_resolves_the_existing_input_seed() {
        let program = program(
            r#"
            .decl E(x: int32)
            .input E
            .decl R(x: int32)
            .input R
            .output R
            R(x) :- E(x).
            "#,
        );
        let stratifier = Stratifier::from_program(&program).expect("stratified");
        let declarations: HashMap<u64, BindingDeclaration<'_>> = program
            .relations()
            .iter()
            .map(|relation| (relation.fingerprint(), BindingDeclaration::new(relation)))
            .collect();
        let stratum = StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[0],
            &mut Optimizer::new(),
            &mut None,
            &[],
            &declarations,
            &program
                .edbs()
                .into_iter()
                .map(|relation| (relation.fingerprint(), relation.input_mutability()))
                .collect(),
        )
        .expect("planned");
        assert_eq!(
            stratum.relation_bindings()[&compute_fp("r")]
                .seed()
                .map(|seed| seed.fingerprint()),
            Some(compute_fp("r"))
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
}
