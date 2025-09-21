//! Stratum planner that plans a group of rules (a stratum).

use catalog::Catalog;
use optimizer::Optimizer;
use parser::logic::MacaronRule;

use crate::{fake_transformation::FakeTransformation, rule_planner::RulePlanner};

/// Planner for a single stratum (group of rules).
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// Per-rule transformations, preserving rule grouping and order.
    pub per_rule_transformations: Vec<Vec<FakeTransformation>>,
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
        let mut per_rule_transformations: Vec<Vec<FakeTransformation>> =
            Vec::with_capacity(rules.len());
        let mut catalogs: Vec<Catalog> = Vec::with_capacity(rules.len());

        // Phase 1: per-rule prepare
        for rule in rules {
            let mut catalog = Catalog::from_rule(rule);

            let mut rp = RulePlanner::new();
            rp.prepare(&mut catalog);
            // rp.core(&mut catalog); // reserved for future core planning

            let per_rule = rp.fake_transformations();
            per_rule_transformations.push(per_rule);
            catalogs.push(catalog);
        }

        // Phase 2: single optimizer pass for the whole stratum
        let catalog_refs: Vec<&Catalog> = catalogs.iter().collect();
        let _plan_trees = optimizer.plan_stratum(catalog_refs);

        Self {
            per_rule_transformations,
        }
    }

    /// Read-only access to per-rule transformations.
    #[inline]
    pub fn per_rule(&self) -> &[Vec<FakeTransformation>] {
        &self.per_rule_transformations
    }
}
