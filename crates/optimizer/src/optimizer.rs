//! Optimizer for Macaron Datalog programs.
use crate::plan_tree::PlanTree;
use catalog::rule::Catalog;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Optimizer {
    relation_cardinality: HashMap<u64, u64>, // future: temporary use u64
}

impl Optimizer {
    /// Create an empty optimizer instance.
    pub fn new() -> Self {
        Self {
            relation_cardinality: HashMap::new(),
        }
    }

    /// Get stored cardinality (if present) for a relation.
    pub fn get_cardinality(&self, rel_fp: u64) -> Option<u64> {
        self.relation_cardinality.get(&rel_fp).copied()
    }

    /// Set a single relation cardinality.
    pub fn set_cardinality(&mut self, rel_fp: u64, card: u64) {
        self.relation_cardinality.insert(rel_fp, card);
    }

    /// Plan a stratum producing while building one `PlanTree` per rule.
    /// This function returns a vector of optional tuple indices, one per rule in the stratum.
    /// If a rule is already planned (is_planned = true), its entry is `None`.
    /// Otherwise, it contains `Some((first_join_tuple_index))`.
    pub fn plan_stratum(&self, catalogs: &[Catalog]) -> Vec<Option<(usize, usize)>> {
        catalogs
            .iter()
            .map(|catalog| {
                if catalog.is_planned() {
                    None
                } else {
                    // TODO: future work to return join tuple based on built join tree
                    //       while also considering cardinalities.
                    let plan_tree = PlanTree::from_catalog(catalog);
                    Some(plan_tree.get_first_join_tuple_index())
                }
            })
            .collect()
    }
}
