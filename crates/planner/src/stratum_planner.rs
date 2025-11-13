//! Stratum planner that plans a stratum (a group of rules).

use std::collections::{HashMap, HashSet};

use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;
use parser::{AggregationOperator, HeadArg};
use tracing::debug;

use crate::{RulePlanner, Transformation, TransformationInfo};

/// Planner for a single stratum (a group of rules).
///
/// A stratum contains one or more rules that can be evaluated together.
/// The planner processes all rules in the stratum, deduplicates shared
/// transformations, and produces executable transformation steps.
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// Per-rule planners that own the transformation infos.
    rule_planners: Vec<RulePlanner>,

    /// Indicator of recursiveness for this stratum.
    is_recursive: bool,

    /// Temporary storage for all deduplicated transformations during analysis.
    /// After static/dynamic separation, prefer using static_transformations and dynamic_transformations.
    transformations: Vec<Transformation>,

    /// Static transformations that depend only on EDBs.
    /// These transformations can be computed once outside recursion loops.
    static_transformations: Vec<Transformation>,

    /// Dynamic transformations that depend on IDB collections.
    /// These transformations must be re-evaluated during recursion.
    dynamic_transformations: Vec<Transformation>,

    /// Fingerprints of collections that enter recursion.
    dynamic_enter_collections: Vec<u64>,

    /// Fingerprints of collections that are iterative (i.e., updated during recursion).
    dynamic_iterative_collections: Vec<u64>,

    /// Mapping from stratum each output relation to rule head IDBs.
    /// This allows the generator to find where each rule's result is stored.
    output_to_idb_map: HashMap<u64, Vec<u64>>,

    /// Mapping from stratum output relation to aggregation requirement.
    /// Only contains entries for rules with aggregation in their heads.
    /// Tuple format: (AggregationOperator, position in output relation)
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
    ) -> Self {
        let mut catalogs = Vec::with_capacity(stratum.len());
        let mut rule_planners = Vec::with_capacity(stratum.len());

        // Phase 1: Initialize catalogs and run prepare phase
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

        // Phase 3: Fusion and post-processing
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.fuse(catalog.original_atom_fingerprints());
        }

        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter_mut()) {
            planner.post(catalog);
        }

        // Phase 4: Materialize deduplicated transformations
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            transformations: Vec::new(),
            static_transformations: Vec::new(),
            dynamic_transformations: Vec::new(),
            dynamic_enter_collections: Vec::new(),
            dynamic_iterative_collections: iterative_relation.to_vec(),
            output_to_idb_map: HashMap::new(),
            output_to_aggregation_map: HashMap::new(),
        };
        stratum_planner.materialize_transformations();
        stratum_planner.build_output_to_idb_map(&catalogs);
        stratum_planner.identify_static_transformations(is_recursive);
        stratum_planner.build_dynamic_enter_collections();
        stratum_planner.build_output_to_aggregation_map(&catalogs);
        stratum_planner
    }
}

// =========================================================================
// Getters
// =========================================================================
impl StratumPlanner {
    /// Read-only access to per-rule transformation infos as borrowed slices.
    #[inline]
    pub fn transformation_infos(&self) -> Vec<&[TransformationInfo]> {
        self.rule_planners
            .iter()
            .map(|planner| planner.transformation_infos().as_slice())
            .collect()
    }

    /// Read-only access to all deduplicated executable transformations.
    /// Note: Prefer using static_transformations() and dynamic_transformations()
    /// for better optimization in recursive scenarios.
    #[inline]
    pub fn transformations(&self) -> &[Transformation] {
        &self.transformations
    }

    /// Get static transformations that depend only on EDBs.
    /// These transformations can be computed once outside recursion loops.
    #[inline]
    pub fn static_transformations(&self) -> &[Transformation] {
        &self.static_transformations
    }

    /// Get dynamic transformations that depend on IDB collections.
    /// These transformations must be re-evaluated during recursion.
    #[inline]
    pub fn dynamic_transformations(&self) -> &[Transformation] {
        &self.dynamic_transformations
    }

    /// Get fingerprints of collections that enter recursion.
    #[inline]
    pub fn dynamic_enter_collections(&self) -> &[u64] {
        &self.dynamic_enter_collections
    }

    /// Get fingerprints of collections that are iterative (i.e., updated during recursion).
    #[inline]
    pub fn dynamic_iterative_collections(&self) -> &[u64] {
        &self.dynamic_iterative_collections
    }

    /// Get the mapping from stratum each output relation to rule head IDBs.
    #[inline]
    pub fn output_to_idb_map(&self) -> &HashMap<u64, Vec<u64>> {
        &self.output_to_idb_map
    }

    /// Get the mapping from stratum output relation to aggregation requirement.
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

    /// Output relation fingerprints produced by this stratum.
    #[inline]
    pub fn output_relations(&self) -> Vec<u64> {
        self.output_to_idb_map.keys().cloned().collect()
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
// Static/Dynamic Separation
// =========================================================================
impl StratumPlanner {
    /// Identify static and dynamic transformations for optimization.
    ///
    /// Static transformations depend only on EDB (base) relations and can be computed
    /// once outside of recursive evaluation loops.
    ///
    /// Dynamic transformations depend on IDB (derived) relations and must be
    /// re-evaluated during recursive fixed-point computation.
    ///
    /// # Algorithm
    ///
    /// - Non-recursive strata: All transformations are static since no recursion occurs.
    /// - Recursive strata: Use left-to-right propagation:
    ///   1. Start with IDB fingerprints (head relations produced in this stratum)
    ///   2. For each transformation from left to right:
    ///      - If any input fingerprint is dynamic, mark transformation as dynamic
    ///      - Add the transformation's output fingerprint to the dynamic set
    ///   3. Remaining transformations are static
    fn identify_static_transformations(&mut self, is_recursive: bool) {
        // Clear any previous results
        self.static_transformations.clear();
        self.dynamic_transformations.clear();

        if !is_recursive {
            // Non-recursive stratum: all transformations are static
            self.static_transformations
                .extend(self.transformations.iter().cloned());
            debug!(
                "Non-recursive stratum: all {} transformations are static",
                self.transformations.len()
            );
            return;
        }

        // Recursive stratum: propagate dynamic dependencies

        // Step 1: Initialize with IDB fingerprints (head relations).
        // Values in `output_to_idb_map` are Vec<u64>; flatten them into the set.
        let mut dynamic_fingerprints: HashSet<u64> = self
            .output_to_idb_map
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();

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

        // Step 3: Separate transformations into static and dynamic vectors
        for (i, transformation) in self.transformations.iter().enumerate() {
            if dynamic_indices.contains(&i) {
                self.dynamic_transformations.push(transformation.clone());
            } else {
                self.static_transformations.push(transformation.clone());
            }
        }

        debug!(
            "Recursive stratum: separated {} static, {} dynamic transformations (total: {})",
            self.static_transformations.len(),
            self.dynamic_transformations.len(),
            self.transformations.len()
        );
    }
}

// =========================================================================
// Metadata Mappings
// =========================================================================
impl StratumPlanner {
    /// Build the fingerprint of collections that enter resursion.
    pub fn build_dynamic_enter_collections(&mut self) {
        // Build sets of input/output fingerprints touched by dynamic transformations.
        let mut dynamic_input_fps: HashSet<u64> = HashSet::new();
        let mut dynamic_output_fps: HashSet<u64> = HashSet::new();

        // Iterate by reference to avoid moving out of `self.dynamic_transformations`.
        for tx in &self.dynamic_transformations {
            if tx.is_unary() {
                dynamic_input_fps.insert(tx.unary_input().fingerprint());
            } else {
                let (left, right) = tx.binary_input();
                dynamic_input_fps.insert(left.fingerprint());
                dynamic_input_fps.insert(right.fingerprint());
            }
            dynamic_output_fps.insert(tx.output().fingerprint());
        }

        // Inputs that never appear as outputs of any dynamic transformation are the
        // entering collections for the dynamic (recursive) portion.
        self.dynamic_enter_collections = dynamic_input_fps
            .difference(&dynamic_output_fps)
            .copied()
            .collect()
    }

    /// Build the mapping from each final output collection fingerprint to the rule head IDB fingerprints
    /// that produce it. Multiple rules may contribute to the same output relation.
    fn build_output_to_idb_map(&mut self, catalogs: &[Catalog]) {
        for (rule_idx, catalog) in catalogs.iter().enumerate() {
            let head_idb_fp = catalog.head_idb_fingerprint();
            if let Some(final_info) = self.rule_planners[rule_idx].transformation_infos().last() {
                let output_fp = final_info.output_info_fp();
                self.output_to_idb_map
                    .entry(output_fp)
                    .or_insert_with(Vec::new)
                    .push(head_idb_fp);
            }
        }
    }

    /// Build the mapping from each final output collection fingerprint to its aggregation requirement.
    /// Ensures consistent aggregation operator and position for a given output relation if multiple rules map to it.
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
                if let Some(final_info) = self.rule_planners[rule_idx].transformation_infos().last()
                {
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
}
