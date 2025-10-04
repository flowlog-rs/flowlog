//! Optimizer for Macaron Datalog programs.
use crate::plan_tree::PlanTree;
use catalog::rule::Catalog;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Optimizer {
    relation_cardinality: HashMap<String, u64>, // future: temporary use u64
}

impl Optimizer {
    /// Create an empty optimizer instance.
    pub fn new() -> Self {
        Self {
            relation_cardinality: HashMap::new(),
        }
    }

    /// Get stored cardinality (if present) for a relation.
    pub fn get_cardinality(&self, rel: &str) -> Option<u64> {
        self.relation_cardinality.get(rel).copied()
    }

    /// Set a single relation cardinality.
    pub fn set_cardinality(&mut self, rel: &str, card: u64) {
        self.relation_cardinality.insert(rel.to_owned(), card);
    }

    /// Plan a stratum producing one `PlanTree` per rule.
    pub fn plan_stratum(
        &self,
        catalogs: &[Catalog],
        is_planned: Vec<bool>,
    ) -> Vec<Option<(usize, usize)>> {
        catalogs
            .iter()
            .zip(is_planned)
            .map(|(catalog, is_planned)| {
                if is_planned {
                    None
                } else {
                    let plan_tree = PlanTree::from_catalog(catalog);
                    Some(plan_tree.get_first_join_tuple_index())
                }
            })
            .collect()
    }
}
