//! Stratum planner that plans a stratum (a group of rules).

use crate::{RulePlanner, TransformationInfo};
use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;
use tracing::debug;

/// Planner for a single stratum (a group of rules).
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// Per-rule transformations, preserving rule grouping and order.
    pub rule_transformation_infos: Vec<Vec<TransformationInfo>>,

    pub rule_real_transformations: Vec<Vec<String>>, // reserved for future use
}

impl StratumPlanner {
    /// Build a `StratumPlanner` directly from a vector/slice of rules.
    ///
    /// Steps:
    /// - For each rule, build its catalog and run per-rule `prepare`.
    /// - Run a single optimizer pass over the prepared catalogs to obtain plan trees
    ///   (currently unused but executed to keep the flow consistent).
    #[must_use]
    pub fn from_rules(rules: &[MacaronRule], optimizer: &mut Optimizer) -> Self {
        let mut rule_transformation_infos: Vec<Vec<TransformationInfo>> =
            Vec::with_capacity(rules.len());
        let mut catalogs: Vec<Catalog> = Vec::with_capacity(rules.len());

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

            rule_transformation_infos.push(txs);
            catalogs.push(catalog);
        }

        while !catalogs.iter().all(|c| c.is_planned()) {
            let is_planned = catalogs.iter().map(|c| c.is_planned()).collect::<Vec<_>>();
            let first_joins = optimizer.plan_stratum(&catalogs, is_planned);
        }

        debug!("{}", "-".repeat(40));
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
}
