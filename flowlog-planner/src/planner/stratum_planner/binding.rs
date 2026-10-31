//! Relation bindings: their rule results, earlier seeds, and operations.

use flowlog_parser::Relation;

// =============================================================================
// BindingOperation
// =============================================================================

/// The operation applied after a relation's inputs are unioned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingOperation {
    /// Sorts and dedups whole rows in `order`, then restores their logical
    /// column positions. `order` is a permutation of every row column.
    Dedup { order: Vec<usize> },
    /// Merges the declared min/max column by every other column. Its
    /// idempotent merge consumes the union without a whole-row dedup.
    /// `order` lists every grouping column in physical key order; the
    /// merged result restores logical column positions.
    Aggregation { order: Vec<usize> },
}

// =============================================================================
// RelationBindingPlan
// =============================================================================

/// The plan for binding a relation's result within one stratum.
///
/// A rule head describes the rows produced by one rule. A relation binding
/// combines the results of all rules for that relation in this stratum,
/// preserving any earlier input or result for the same relation.
///
/// The plan contains three parts:
///
/// - [`inputs`](Self::inputs): the rule-result collection fingerprints after
///   sharing, in concatenation order. Several rules can share one input.
/// - [`seed`](Self::seed): an optional earlier binding of the relation,
///   supplied by an input declaration or an earlier stratum. Inside recursion,
///   it refers to the entered outer binding, separately from the feedback
///   variable. A completed stratum has already resolved whether a seed exists.
/// - [`operation`](Self::operation): whole-row [`BindingOperation::Dedup`]
///   with its field permutation, or
///   [`BindingOperation::Aggregation`] for a declared min/max column. This
///   selects the relation's operation; aggregates inside rule bodies belong
///   to those rules' plans.
///
/// The seed, when present, precedes the rule results in their union. The
/// selected operation applies to that union, including when there is only
/// one input. Declared aggregation consumes the union directly, without a
/// separate whole-row dedup.
#[derive(Debug)]
pub struct RelationBindingPlan {
    pub(in crate::planner) inputs: Vec<u64>,
    pub(in crate::planner) seed: Option<u64>,
    pub(in crate::planner) operation: BindingOperation,
}

impl RelationBindingPlan {
    #[must_use]
    pub(super) fn new(declaration: &Relation, inputs: Vec<u64>) -> Self {
        let operation = if let Some((target, _)) = declaration.lattice() {
            BindingOperation::Aggregation {
                order: (0..declaration.arity())
                    .filter(|&column| column != target)
                    .collect(),
            }
        } else {
            BindingOperation::Dedup {
                order: (0..declaration.arity()).collect(),
            }
        };
        Self {
            inputs,
            seed: None,
            operation,
        }
    }

    /// Rule-result fingerprints after sharing, in concatenation order.
    #[must_use]
    pub fn inputs(&self) -> &[u64] {
        &self.inputs
    }

    /// The earlier outer binding unioned before the rule results. Inside
    /// recursion this is the entered seed, not the feedback variable.
    #[must_use]
    pub fn seed(&self) -> Option<u64> {
        self.seed
    }

    #[must_use]
    pub fn operation(&self) -> &BindingOperation {
        &self.operation
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::program_planner;

    // Cases: declaration fields, binding operation.
    #[rstest]
    #[case::ordinary("a: int32, b: symbol", BindingOperation::Dedup { order: vec![0, 1] })]
    #[case::minimum("a: int32, b: min<int32>", BindingOperation::Aggregation { order: vec![0] })]
    #[case::maximum("a: int32, b: max<int32>", BindingOperation::Aggregation { order: vec![0] })]
    fn a_binding_selects_its_operation_from_the_declaration(
        #[case] fields: &str,
        #[case] expected: BindingOperation,
    ) {
        let program = program(&format!(".decl R({fields})\n.input R\n.output R\n"));
        let binding = RelationBindingPlan::new(&program.relations()[0], vec![0x11]);
        assert_eq!(binding.operation(), &expected);
    }

    #[test]
    fn a_recursive_binding_keeps_its_outer_seed_apart_from_feedback() {
        let plan = program_planner(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let [base, recursive] = plan.strata() else {
            panic!("Reach has a base stratum and a recursive stratum");
        };
        let relation = compute_fp("reach");
        assert_eq!(base.relation_bindings()[&relation].seed(), None);
        let binding = &recursive.relation_bindings()[&relation];
        assert_eq!(binding.seed(), Some(relation));
        assert!(!binding.inputs().contains(&relation));
        assert!(
            recursive
                .recursion_feedback_collections()
                .contains(&relation)
        );
    }

    #[test]
    fn a_later_non_recursive_binding_keeps_the_earlier_binding() {
        let plan = program_planner(
            r#"
            .decl E(x: int32)
            .input E
            .decl F(x: int32)
            .input F
            .decl G(x: int32)
            .input G
            .decl Aux(x: int32)
            .decl R(x: int32)
            .output R
            Aux(x) :- G(x).
            R(x) :- E(x).
            R(x) :- Aux(x), F(x).
            "#,
        );
        let [base, later] = plan.strata() else {
            panic!("Aux separates the two rules for R into two strata");
        };
        let relation = compute_fp("r");
        assert!(!base.is_recursive());
        assert!(!later.is_recursive());
        assert_eq!(base.relation_bindings()[&relation].seed(), None);
        let binding = &later.relation_bindings()[&relation];
        assert_eq!(binding.seed(), Some(relation));
        assert_eq!(binding.inputs().len(), 1);
        assert!(!binding.inputs().contains(&relation));
    }

    #[test]
    fn a_binding_unions_its_existing_input_relation() {
        let plan = program_planner(
            r#"
            .decl E(a: int32)
            .input E
            .decl R(a: int32)
            .input R
            .output R
            R(a) :- E(a).
            "#,
        );
        assert_eq!(
            plan.strata()[0].relation_bindings()[&compute_fp("r")].seed(),
            Some(compute_fp("r"))
        );
    }
}
