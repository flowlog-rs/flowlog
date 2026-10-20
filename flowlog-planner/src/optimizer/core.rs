//! Optimizer for FlowLog Datalog programs.
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::optimizer::join_order;
use crate::optimizer::join_order::AtomKeys;
use crate::optimizer::join_order::JoinKey;
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
    /// aggregate as soon as the atoms cover one, else the next join as
    /// [`join_order::next_join`] picks it.
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
        let atoms = catalog.core_atom_number()?;
        let pair = join_order::next_join(&Self::atom_keys(catalog)?).ok_or_else(|| {
            PlanError::internal(format!(
                "optimizer needs at least two core atoms, found {atoms} in {}",
                catalog.rule()
            ))
        })?;
        Ok(Some(PlanDecision::Join(pair)))
    }

    /// Returns each positive atom's join keys: its variables and, for each
    /// atom it shares no variable with, the spanning equalities equijoin
    /// fusion will key the pair on. Fusion does not fire on a pair that
    /// shares a variable, where an equality stays a filter.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the catalog's equality metadata is
    /// inconsistent.
    fn atom_keys(catalog: &Catalog) -> Result<AtomKeys, PlanError> {
        let variables = catalog.positive_atom_variables();
        let mut keys: Vec<BTreeSet<JoinKey>> = variables
            .iter()
            .map(|vars| vars.iter().cloned().map(JoinKey::Variable).collect())
            .collect();
        for a in 0..variables.len() {
            for b in a + 1..variables.len() {
                if !variables[a].is_disjoint(&variables[b]) {
                    continue;
                }
                for (comparison, ..) in catalog.equijoin_keys_for_pair(a, b)? {
                    keys[a].insert(JoinKey::Equality(comparison));
                    keys[b].insert(JoinKey::Equality(comparison));
                }
            }
        }
        Ok(AtomKeys::new(keys))
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

    /// A body written leaves first is joined hub first: the two leaves
    /// share only `v`, which the hub also holds, so they are not neighbors
    /// in any join tree and joining them first would match on `v` alone.
    #[test]
    fn a_join_never_skips_the_atom_between_two_leaves() {
        let catalog = catalog(
            "\
            .decl B(d: int32, v: int32, u: int32)\n\
            .decl C(k: int32, v: int32, n: int32)\n\
            .decl A(u: int32, k: int32, v: int32)\n\
            .decl Out(d: int32, n: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(d, n) :- B(d, v, u), C(k, v, n), A(u, k, v).\n",
        );
        assert_eq!(
            Optimizer::new().plan_rule(&catalog).unwrap(),
            Some(PlanDecision::Join((2, 0)))
        );
    }

    /// Atoms that share no variable are never joined while another pair
    /// is linked, whether by a shared variable or by an equality that
    /// equijoin fusion turns into a key.
    // Cases: rule, decision.
    #[rstest]
    #[case::cycle_written_corners_first(
        "Out(x, z) :- R(x, y), R(z, w), R(y, z), R(w, x).",
        PlanDecision::Join((2, 0))
    )]
    #[case::equality_links_first_and_last(
        "Out(x, y) :- S(x), S(y), S(z), z = x + 1.",
        PlanDecision::Join((2, 0))
    )]
    #[case::cross_product_waits_for_the_rest(
        "Out(a, x) :- S(a), R(x, y), R(y, z).",
        PlanDecision::Join((1, 2))
    )]
    fn a_join_avoids_a_cross_product_while_atoms_are_linked(
        #[case] rule: &str,
        #[case] expected: PlanDecision,
    ) {
        let catalog = catalog(&format!(
            "\
            .decl R(a: int32, b: int32)\n\
            .decl S(a: int32)\n\
            .decl Out(a: int32, b: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Out\n\
            {rule}\n"
        ));
        assert_eq!(
            Optimizer::new().plan_rule(&catalog).unwrap(),
            Some(expected)
        );
    }
}
