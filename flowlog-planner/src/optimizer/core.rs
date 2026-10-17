//! Optimizer for FlowLog Datalog programs.
use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::optimizer::plan_tree::PlanTree;
use crate::planner::PlanError;

/// The optimizer's decision on a rule's next step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlanDecision {
    /// Join these two positive atoms, by catalog index.
    Join((usize, usize)),
    /// Take this aggregate, by catalog index: plan its braces and append
    /// its value to the atom covering it.
    Aggregate(usize),
}

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

    /// Returns the next step of one rule, `None` once it is planned: an
    /// aggregate as soon as the atoms cover one, else the next join.
    ///
    /// An aggregate is taken before any join so that it reads the atoms
    /// after every cheaper fold, and a filter folded later still reaches
    /// its braces through pushdown.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the rule has neither an aggregate to
    /// take nor two atoms to join.
    pub(crate) fn plan_rule(&self, catalog: &Catalog) -> Result<Option<PlanDecision>, PlanError> {
        if catalog.is_planned() {
            return Ok(None);
        }
        if let Some(index) = Self::ready_aggregate(catalog) {
            return Ok(Some(PlanDecision::Aggregate(index)));
        }
        // TODO: future work to return join tuple based on built join tree
        //       while also considering cardinalities.
        let plan_tree = PlanTree::from_catalog(catalog)?;
        Ok(Some(PlanDecision::Join(
            plan_tree.get_first_join_tuple_index(),
        )))
    }

    /// Returns the first aggregate the rule can take: one reading no
    /// variable of the body, or one some positive atom covers.
    fn ready_aggregate(catalog: &Catalog) -> Option<usize> {
        catalog
            .aggregate_supersets()
            .iter()
            .zip(catalog.aggregate_variables())
            .position(|(supersets, variables)| !supersets.is_empty() || variables.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_harness::catalog;

    /// Shared declarations for the decision tests.
    const DECLS: &str = "\
        .decl A(a: int32, b: int32)\n\
        .decl B(a: int32, b: int32)\n\
        .decl Out(x: int32, y: int32)\n\
        .decl Total(c: int32)\n\
        .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
        .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
        .output Out\n\
        .output Total\n";

    /// An aggregate is taken as soon as an atom covers it, or at once when
    /// it reads nothing from the body; otherwise the next join; nothing
    /// once one atom is left. The rules are written as prepare leaves
    /// them, with no filter left to fold.
    // Cases: rule, decision.
    #[rstest]
    #[case(
        "Out(x, c) :- A(x, a), c = count : B(x, _).",
        Some(PlanDecision::Aggregate(0))
    )]
    #[case("Total(c) :- c = count : B(_, _).", Some(PlanDecision::Aggregate(0)))]
    #[case(
        "Out(x, c) :- A(x, a), B(y, b), c = count : B(x, y).",
        Some(PlanDecision::Join((0, 1)))
    )]
    #[case("Out(x, y) :- A(x, a), B(y, b).", Some(PlanDecision::Join((0, 1))))]
    #[case("Out(x, y) :- A(x, y).", None)]
    fn an_aggregate_is_taken_as_soon_as_an_atom_covers_it(
        #[case] rule: &str,
        #[case] expected: Option<PlanDecision>,
    ) {
        let catalog = catalog(&format!("{DECLS}{rule}\n"));
        assert_eq!(Optimizer::new().plan_rule(&catalog).unwrap(), expected);
    }
}
