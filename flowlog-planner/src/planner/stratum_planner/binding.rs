//! Relation bindings: their rule results, earlier seeds, and operations.

use std::sync::Arc;

use flowlog_parser::Relation;

use crate::planner::Collection;

// =============================================================================
// BindingOperation
// =============================================================================

/// The operation applied after a relation's inputs are unioned. Each
/// `order` names declared columns, `order[i]` being the column held at
/// position `i`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingOperation {
    /// Dedups whole rows held in `order`, a permutation of every declared
    /// column.
    Dedup { order: Vec<usize> },
    /// Merges the declared min/max column by every other column, the group
    /// key holding those columns in `order`. Its idempotent merge consumes
    /// the union without a whole-row dedup.
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
/// - [`inputs`](Self::inputs): rule-result collections in concatenation order.
///   Each producer must emit the binding operation's input format directly.
///   An input is the producer transformation's own output handle, so the
///   binding and the transformation agree by identity, not by fingerprint
///   lookup.
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
    pub(super) inputs: Vec<Arc<Collection>>,
    pub(super) seed: Option<Arc<Collection>>,
    pub(super) operation: BindingOperation,
}

impl RelationBindingPlan {
    /// Plans `declaration`'s binding in declaration column order: a dedup
    /// of every column, or a min/max merge grouped by every other column.
    #[must_use]
    pub(super) fn new(declaration: &Relation, inputs: Vec<Arc<Collection>>) -> Self {
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

    /// Rule results after sharing, in concatenation order, each the
    /// producing transformation's own output at its mutability.
    #[must_use]
    pub fn inputs(&self) -> &[Arc<Collection>] {
        &self.inputs
    }

    /// The fingerprints of [`inputs`](Self::inputs), in the same order.
    pub fn input_fingerprints(&self) -> impl Iterator<Item = u64> + '_ {
        self.inputs.iter().map(|input| input.fingerprint())
    }

    /// The earlier outer binding unioned before the rule results. Inside
    /// recursion this is the entered seed, not the feedback variable.
    #[must_use]
    pub fn seed(&self) -> Option<&Collection> {
        self.seed.as_deref()
    }

    #[must_use]
    pub fn operation(&self) -> &BindingOperation {
        &self.operation
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::Mutability;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::program_planner;

    // Cases: declaration fields, binding operation.
    #[rstest]
    #[case::ordinary("a: int32, b: symbol", BindingOperation::Dedup { order: vec![0, 1] })]
    #[case::minimum("a: int32, b: min<int32>", BindingOperation::Aggregation { order: vec![0] })]
    #[case::maximum("a: int32, b: max<int32>", BindingOperation::Aggregation { order: vec![0] })]
    #[case::leading_lattice(
        "a: min<int32>, b: int32, c: int32",
        BindingOperation::Aggregation { order: vec![1, 2] }
    )]
    fn a_binding_selects_its_operation_from_the_declaration(
        #[case] fields: &str,
        #[case] expected: BindingOperation,
    ) {
        let program = program(&format!(".decl R({fields})\n.input R\n.output R\n"));
        let binding = RelationBindingPlan::new(&program.relations()[0], Vec::new());
        assert_eq!(binding.operation(), &expected);
    }

    #[test]
    fn binding_inputs_are_the_producing_heads_themselves() {
        let plan = program_planner(
            ".decl A(a:symbol,b:int32,c:int64)\n.input A\n\
             .decl B(a:symbol,b:int32,c:int64)\n.input B\n\
             .decl Out(c:int64,a:symbol,b:int32)\n.output Out\n\
             Out(c,a,b) :- A(a,b,c).\nOut(c,a,b) :- B(a,b,c).\n",
        );
        let stratum = &plan.strata()[0];
        let inputs = stratum.relation_bindings()[&compute_fp("out")].inputs();
        assert_eq!(inputs.len(), 2);
        for input in inputs {
            let head = stratum
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == input.fingerprint())
                .expect("each input is a head of this stratum");
            assert!(Arc::ptr_eq(input, head.output()));
        }
    }

    #[test]
    fn shared_rule_results_are_referenced_directly_by_each_binding() {
        let plan = program_planner(
            ".decl A(a:int32,b:int32)\n.input A\n\
             .decl Out(a:int32,b:int32)\n.output Out\n\
             .decl Other(a:int32,b:int32)\n.output Other\n\
             Out(b,a) :- A(a,b).\nOther(b,a) :- A(a,b).\n",
        );
        let bindings = plan.strata()[0].relation_bindings();
        let [out] = bindings[&compute_fp("out")].inputs() else {
            panic!("Out has one head");
        };
        let [other] = bindings[&compute_fp("other")].inputs() else {
            panic!("Other has one head");
        };
        assert!(Arc::ptr_eq(out, other));
    }

    #[test]
    fn a_recursive_seed_keeps_its_outer_static_weight() {
        let plan = program_planner(
            ".decl Base(x:int32,y:int32)\n.input Base\n\
             .decl Edge(x:int32,y:int32) mutable\n.input Edge\n\
             .decl Reach(x:int32,y:int32)\n.output Reach\n\
             Reach(x,y) :- Base(x,y).\nReach(x,z) :- Reach(x,y), Edge(y,z).\n",
        );
        let recursive = plan
            .strata()
            .iter()
            .find(|stratum| stratum.is_recursive())
            .expect("Reach recurses over Edge");
        let seed = recursive.relation_bindings()[&compute_fp("reach")]
            .seed()
            .expect("the base rule's binding seeds the recursion");
        assert_eq!(seed.mutability(), Mutability::Static);
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
        assert_eq!(
            binding.seed().map(|seed| seed.fingerprint()),
            Some(relation)
        );
        assert!(!binding.input_fingerprints().any(|input| input == relation));
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
        assert_eq!(
            binding.seed().map(|seed| seed.fingerprint()),
            Some(relation)
        );
        assert_eq!(binding.inputs().len(), 1);
        assert!(!binding.input_fingerprints().any(|input| input == relation));
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
            plan.strata()[0].relation_bindings()[&compute_fp("r")]
                .seed()
                .map(|seed| seed.fingerprint()),
            Some(compute_fp("r"))
        );
    }
}
