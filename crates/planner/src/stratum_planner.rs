//! Stratum planner that plans a stratum (a group of rules).

use std::collections::HashSet;

use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;
use tracing::debug;

use crate::{RulePlanner, Transformation, TransformationInfo};

/// Planner for a single stratum (a group of rules).
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// Per-rule transformations, preserving rule grouping and order.
    pub rule_transformation_infos: Vec<Vec<TransformationInfo>>,

    pub real_transformations: Vec<Transformation>, // reserved for future use
}

impl StratumPlanner {
    /// Build a `StratumPlanner` directly from a slice of rules.
    ///
    /// Steps:
    /// - For each rule, build its catalog and run per-rule `prepare`.
    /// - Repeatedly run optimizer passes until all catalogs are planned (`core`).
    /// - Perform `fuse` and `post` per rule to finalize transformations.
    #[must_use]
    pub fn from_rules(rules: &[MacaronRule], optimizer: &mut Optimizer) -> Self {
        let mut rule_transformation_infos: Vec<Vec<TransformationInfo>> =
            Vec::with_capacity(rules.len());
        let mut catalogs: Vec<Catalog> = Vec::with_capacity(rules.len());
        let mut rps: Vec<RulePlanner> = Vec::with_capacity(rules.len());

        for (i, rule) in rules.iter().enumerate() {
            let mut catalog = Catalog::from_rule(rule);
            debug!("{}", "-".repeat(40));
            debug!("rule[{i}] init:\n{catalog}");
            debug!("{}", "-".repeat(40));

            let mut rp = RulePlanner::new();
            rp.prepare(&mut catalog);

            let txs = rp.transformation_infos(); // (clone; your API returns Vec)
            debug!("rule[{i}] prepare: {} transformations", txs.len());
            debug!("rule[{i}] after prepare:\n{catalog}");
            debug!("{}", "-".repeat(40));

            catalogs.push(catalog);
            rps.push(rp);
        }

        while !catalogs.iter().all(|c| c.is_planned()) {
            let is_planned = catalogs.iter().map(|c| c.is_planned()).collect::<Vec<_>>();
            let first_joins = optimizer.plan_stratum(&catalogs, is_planned);
            rps.iter_mut()
                .zip(catalogs.iter_mut())
                .zip(first_joins.into_iter())
                .for_each(|((rp, catalog), first_join)| {
                    if let Some(join_tuple_index) = first_join {
                        rp.core(catalog, join_tuple_index);
                    }
                });
        }

        rps.iter_mut()
            .zip(catalogs.iter())
            .for_each(|(rp, catalog)| rp.fuse(catalog.original_atom_fingerprints()));

        rps.iter_mut()
            .zip(catalogs.iter_mut())
            .for_each(|(rp, catalog)| rp.post(catalog));

        for rp in rps.into_iter() {
            rule_transformation_infos.push(rp.transformation_infos().clone());
        }

        Self {
            rule_transformation_infos,
            ..Default::default()
        }
    }

    /// Read-only access to per-rule transformations.
    #[inline]
    pub fn rule_transformation_infos(&self) -> &[Vec<TransformationInfo>] {
        &self.rule_transformation_infos
    }

    /// Read-only access to all transformations.
    #[inline]
    pub fn real_transformations(&self) -> &[Transformation] {
        &self.real_transformations
    }

    /// Flatten all per-rule transformation infos, enable sharing by removing duplicates
    /// (preserving the first occurrence), and build concrete `real_transformations`.
    pub fn materialize_shared_transformations(&mut self) {
        let unique_infos = self.flatten_unique_infos();

        // Build real transformations from unique infos
        self.real_transformations = unique_infos
            .iter()
            .map(|info| match info {
                TransformationInfo::KVToKV { .. } => Transformation::kv_to_kv(info),
                TransformationInfo::JoinToKV { .. } => Transformation::join(info),
                TransformationInfo::AntiJoinToKV { .. } => Transformation::antijoin(info),
            })
            .collect();
    }

    /// Flatten per-rule infos and return a unique list (first occurrence wins).
    fn flatten_unique_infos(&self) -> Vec<TransformationInfo> {
        let mut seen: HashSet<TransformationInfo> = HashSet::new();
        let mut unique_infos: Vec<TransformationInfo> = Vec::new();

        self.rule_transformation_infos
            .iter()
            .flat_map(|group| group.iter())
            .for_each(|info| {
                if seen.insert(info.clone()) {
                    unique_infos.push(info.clone());
                }
            });

        unique_infos
    }
}
