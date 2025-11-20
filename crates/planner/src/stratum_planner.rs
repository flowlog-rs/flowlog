//! Stratum planner that plans a stratum (a group of rules).

use std::collections::{HashMap, HashSet};

use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;
use parser::{AggregationOperator, HeadArg};
use tracing::debug;

use crate::{RulePlanner, Transformation, TransformationInfo};

/// Planner for a single stratum (a group of parallel rules).
///
/// A stratum groups rules that can be evaluated parallelly together.
/// The planner owns per-rule planners, deduplicates the
/// generated transformation graphs, separates non-recursive (EDB-only) work from
/// recursive (IDB-dependent) work, and records metadata (enter/leave collections,
/// aggregations) that generator/executor stages need to run the stratum efficiently.
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// One planner per rule; these own the raw transformation infos.
    rule_planners: Vec<RulePlanner>,

    /// Whether the stratum is recursive.
    is_recursive: bool,

    /// All deduplicated transformations before recursive/non-recursive separation.
    /// Prefer `non_recursive_transformations` and `recursive_transformations` afterwards.
    transformations: Vec<Transformation>,

    /// Transformations that depend only on EDB inputs; computed once.
    non_recursive_transformations: Vec<Transformation>,

    /// Transformations that touch IDB inputs; re-run during recursion.
    recursive_transformations: Vec<Transformation>,

    /// Fingerprints of collections that enter recursion.
    recursion_enter_collections: Vec<u64>,

    /// Fingerprints of collections updated within recursion.
    recursion_iterative_collections: Vec<u64>,

    /// Fingerprints of collections that exit recursion.
    recursion_leave_collections: Vec<u64>,

    /// Map each stratum output relation to the rule head IDBs that feed it.
    /// Enables the generator to locate the materialized results per rule.
    output_to_idb_map: HashMap<u64, Vec<u64>>,

    /// Aggregation metadata keyed by final output fingerprint.
    /// Only populated for rules whose heads contain an aggregation argument.
    /// Values are `(AggregationOperator, output_position)` tuples.
    output_to_aggregation_map: HashMap<u64, (AggregationOperator, usize)>,
}

impl StratumPlanner {
    /// Build a stratum planner from a stratum.
    #[must_use]
    pub fn from_rules(
        stratum: &[MacaronRule],
        optimizer: &mut Optimizer,
        is_recursive: bool,
        iterative_relation: &[u64],
        leave_relation: &[u64],
    ) -> Self {
        let mut catalogs = Vec::with_capacity(stratum.len());
        let mut rule_planners = Vec::with_capacity(stratum.len());

        // Phase 1: Initialize catalogs and run prepare phase
        // to apply local filters/semijoin/comparison before core join planning
        for (i, rule) in stratum.iter().enumerate() {
            let mut catalog = Catalog::from_rule(rule);
            debug!("{}", "-".repeat(40));
            debug!("rule[{i}] init:\n{catalog}");
            debug!("{}", "-".repeat(40));

            let mut planner = RulePlanner::new();
            planner.prepare(&mut catalog);

            debug!(
                "rule[{i}] prepare: {} transformations",
                planner.transformation_infos().len()
            );
            debug!("rule[{i}] after prepare:\n{catalog}");
            debug!("{}", "-".repeat(40));

            catalogs.push(catalog);
            rule_planners.push(planner);
        }

        // Phase 2: Core planning with optimizer guidance
        // this phase may introduce exponential blowup in intermediate results if not guided properly
        while !catalogs.iter().all(|c| c.is_planned()) {
            let planning_status: Vec<_> = catalogs.iter().map(|c| c.is_planned()).collect();
            let join_decisions = optimizer.plan_stratum(&catalogs, planning_status);

            for ((planner, catalog), join_decision) in rule_planners
                .iter_mut()
                .zip(catalogs.iter_mut())
                .zip(join_decisions)
            {
                if let Some(join_tuple_index) = join_decision {
                    planner.core(catalog, join_tuple_index);
                }
            }
        }

        // Phase 3: Fusion
        // to combine transformations and optimize execution
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.fuse(catalog.original_atom_fingerprints());
        }

        // Phase 4: Post-processing
        // align final output to the rule head (vars and arithmetic)
        // to apply final adjustments after fusion, e.g. convert to row type
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter_mut()) {
            planner.post(catalog);
        }

        // Phase 5: Materialize deduplicated transformations
        // this phase also do sharing optimization across rules
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            transformations: Vec::new(),
            non_recursive_transformations: Vec::new(),
            recursive_transformations: Vec::new(),
            recursion_enter_collections: Vec::new(),
            recursion_iterative_collections: iterative_relation.to_vec(),
            recursion_leave_collections: leave_relation.to_vec(),
            output_to_idb_map: HashMap::new(),
            output_to_aggregation_map: HashMap::new(),
        };
        stratum_planner.materialize_transformations();

        // Phase 6: Recursive split and metadata mappings
        // this phase to factoring optimizations
        stratum_planner.build_output_to_idb_map(&catalogs);
        stratum_planner.identify_recursive_transformations(is_recursive);
        stratum_planner.build_recursion_enter_collections();
        stratum_planner.build_output_to_aggregation_map(&catalogs);
        stratum_planner
    }
}

// =========================================================================
// Getters
// =========================================================================
impl StratumPlanner {
    /// Read-only access to all deduplicated executable transformations.
    /// Note: Prefer using `non_recursive_transformations()` and
    /// `recursive_transformations()` for recursive strata.
    #[inline]
    pub fn transformations(&self) -> &[Transformation] {
        &self.transformations
    }

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

    /// Get fingerprints of collections that enter recursion.
    #[inline]
    pub fn recursion_enter_collections(&self) -> &[u64] {
        &self.recursion_enter_collections
    }

    /// Get fingerprints of collections that are iterative (i.e., updated during recursion).
    #[inline]
    pub fn recursion_iterative_collections(&self) -> &[u64] {
        &self.recursion_iterative_collections
    }

    /// Get fingerprints of collections that leave recursion.
    #[inline]
    pub fn recursion_leave_collections(&self) -> &[u64] {
        &self.recursion_leave_collections
    }

    /// Output relation fingerprints produced by this stratum.
    #[inline]
    pub fn output_relations(&self) -> HashSet<u64> {
        self.output_to_idb_map.keys().cloned().collect()
    }

    /// Get the mapping from each rule output relation to corresponding head IDBs.
    #[inline]
    pub fn output_to_idb_map(&self) -> &HashMap<u64, Vec<u64>> {
        &self.output_to_idb_map
    }

    /// Get the mapping from rule output relation to corresponding aggregation.
    /// Returns tuples of (AggregationOperator, position in output relation).
    #[inline]
    pub fn output_to_aggregation_map(&self) -> &HashMap<u64, (AggregationOperator, usize)> {
        &self.output_to_aggregation_map
    }

    /// Check if this stratum is recursive.
    #[inline]
    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }
}

// =========================================================================
// Sharing Optimization
// =========================================================================
impl StratumPlanner {
    /// Deduplicate transformation infos across all rules and build executable transformations.
    fn materialize_transformations(&mut self) {
        let unique_infos = self.deduplicate_transformation_infos();

        self.transformations = unique_infos
            .iter()
            .map(|&info| match info {
                TransformationInfo::KVToKV { .. } => Transformation::kv_to_kv(info),
                TransformationInfo::JoinToKV { .. } => Transformation::join(info),
                TransformationInfo::AntiJoinToKV { .. } => Transformation::antijoin(info),
            })
            .collect();
    }

    /// Flatten and deduplicate transformation infos from all rules.
    /// Returns references to unique transformation infos (first occurrence wins).
    fn deduplicate_transformation_infos(&self) -> Vec<&TransformationInfo> {
        let mut seen = HashSet::new();
        let mut unique_infos = Vec::new();

        for planner in &self.rule_planners {
            for info in planner.transformation_infos() {
                if seen.insert(info) {
                    unique_infos.push(info);
                }
            }
        }

        unique_infos
    }
}

// =========================================================================
// Recursive/Non-Recursive Separation
// =========================================================================
impl StratumPlanner {
    /// Identify non-recursive and recursive transformations for factoring optimization.
    ///
    /// Non-recursive transformations depend only on EDB (base) relations and can be
    /// computed once outside of recursive evaluation loops.
    ///
    /// Recursive transformations depend on IDB (derived) relations and must be
    /// re-evaluated during recursive fixed-point computation.
    ///
    /// # Algorithm
    ///
    /// - Non-recursive strata: All transformations are non-recursive since no recursion occurs.
    /// - Recursive strata: Use left-to-right propagation:
    ///   1. Start with IDB fingerprints (head relations produced in this stratum)
    ///   2. For each transformation from left to right:
    ///      - If any input fingerprint is dynamic, mark transformation as dynamic
    ///      - Add the transformation's output fingerprint to the dynamic set
    ///   3. Remaining transformations are non-recursive
    fn identify_recursive_transformations(&mut self, is_recursive: bool) {
        if !is_recursive {
            // Non-recursive stratum: all transformations are non-recursive
            self.non_recursive_transformations
                .extend(self.transformations.iter().cloned());
            debug!(
                "Non-recursive stratum: all {} transformations are non-recursive",
                self.transformations.len()
            );
            return;
        }

        // Recursive stratum: propagate dynamic dependencies

        // Step 1: Initialize with output relations fingerprints.
        let mut dynamic_fingerprints: HashSet<u64> =
            self.output_to_idb_map.keys().copied().collect();

        // Step 2: Left-to-right propagation through transformations
        let mut dynamic_indices = HashSet::new();

        for (i, transformation) in self.transformations.iter().enumerate() {
            // Check if this transformation consumes any dynamic fingerprints
            let consumes_dynamic = if transformation.is_unary() {
                let input_fp = transformation.unary_input().fingerprint();
                dynamic_fingerprints.contains(&input_fp)
            } else {
                let (left, right) = transformation.binary_input();
                dynamic_fingerprints.contains(&left.fingerprint())
                    || dynamic_fingerprints.contains(&right.fingerprint())
            };

            if consumes_dynamic {
                // Mark as dynamic and propagate output fingerprint
                dynamic_indices.insert(i);
                dynamic_fingerprints.insert(transformation.output().fingerprint());
            }
        }

        // Step 3: Separate transformations into non-recursive and recursive vectors
        for (i, transformation) in self.transformations.iter().enumerate() {
            if dynamic_indices.contains(&i) {
                self.recursive_transformations.push(transformation.clone());
            } else {
                self.non_recursive_transformations
                    .push(transformation.clone());
            }
        }

        debug!(
            "Recursive stratum: separated {} non-recursive, {} recursive transformations (total: {})",
            self.non_recursive_transformations.len(),
            self.recursive_transformations.len(),
            self.transformations.len()
        );
    }
}

// =========================================================================
// Metadata Mappings
// =========================================================================
impl StratumPlanner {
    /// Build the fingerprint of collections that enter recursion.
    pub fn build_recursion_enter_collections(&mut self) {
        // Build sets of input/output fingerprints touched by recursion transformations.
        let mut recursion_input_fps: HashSet<u64> = HashSet::new();
        let mut recursion_output_fps: HashSet<u64> = HashSet::new();

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

        // Inputs that never appear as outputs of any recursion transformation are the
        // entering collections for the recursion.
        self.recursion_enter_collections = recursion_input_fps
            .difference(&recursion_output_fps)
            .copied()
            .collect()
    }

    /// Build the mapping from each final output collection fingerprint to the rule head IDB fingerprints
    /// that produce it. Multiple rules may contribute to the same output relation.
    ///
    /// Note:
    /// 1. Due to sharing, not every rule has a real output fingerprint -> head IDB mapping, though it may occur
    ///    in the `output_to_idb_map`.
    fn build_output_to_idb_map(&mut self, catalogs: &[Catalog]) {
        for (rule_idx, catalog) in catalogs.iter().enumerate() {
            let head_idb_fp = catalog.head_idb_fingerprint();
            if let Some(final_info) = self.rule_planners[rule_idx].transformation_infos().last() {
                let output_fp = final_info.output_info_fp();
                self.output_to_idb_map
                    .entry(head_idb_fp)
                    .or_default()
                    .push(output_fp);
            }
        }
    }

    /// Build the mapping from each final output collection fingerprint to its aggregation requirement.
    /// Ensures consistent aggregation operator and position for a given output relation if multiple rules map to it.
    ///
    /// Note:
    /// 1. Not every output relation has an aggregation.
    /// 2. Due to sharing, not every rule has a real output fingerprint -> aggregation mapping, though it may occur
    ///    in the `output_to_aggregation_map`.
    fn build_output_to_aggregation_map(&mut self, catalogs: &[Catalog]) {
        for (rule_idx, catalog) in catalogs.iter().enumerate() {
            if let Some((pos, op)) =
                catalog
                    .head_arguments()
                    .iter()
                    .enumerate()
                    .find_map(|(i, arg)| match arg {
                        HeadArg::Aggregation(agg) => Some((i, *agg.operator())),
                        _ => None,
                    })
            {
                let final_info = self.rule_planners[rule_idx]
                    .transformation_infos()
                    .last()
                    .unwrap();
                let output_fp = final_info.output_info_fp();
                match self.output_to_aggregation_map.get(&output_fp) {
                    Some(&(existing_op, existing_pos)) => {
                        if existing_op != op || existing_pos != pos {
                            panic!(
                                    "Planner error: inconsistent aggregation for output fingerprint {:#018x}, found {:?} at position {} but expected {:?} at position {}",
                                    output_fp, op, pos, existing_op, existing_pos
                                );
                        }
                    }
                    None => {
                        self.output_to_aggregation_map.insert(output_fp, (op, pos));
                    }
                }
            }
        }
    }
}
