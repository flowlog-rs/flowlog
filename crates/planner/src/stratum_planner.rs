//! Stratum planner that plans a stratum (a group of rules).

use std::collections::HashSet;

use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;
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

    /// Deduplicated executable transformations for the entire stratum.
    transformations: Vec<Transformation>,
}

impl StratumPlanner {
    /// Build a stratum planner from a stratum.
    #[must_use]
    pub fn from_rules(rules: &[MacaronRule], optimizer: &mut Optimizer) -> Self {
        let mut catalogs = Vec::with_capacity(rules.len());
        let mut rule_planners = Vec::with_capacity(rules.len());

        // Phase 1: Initialize catalogs and run prepare phase
        for (i, rule) in rules.iter().enumerate() {
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
            transformations: Vec::new(),
        };
        stratum_planner.materialize_transformations();
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

    /// Read-only access to the deduplicated executable transformations.
    #[inline]
    pub fn transformations(&self) -> &[Transformation] {
        &self.transformations
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
