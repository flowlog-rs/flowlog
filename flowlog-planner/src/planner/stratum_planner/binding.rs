//! Relation bindings: their rule results, earlier seeds, and operations.

use std::sync::Arc;

use flowlog_parser::Relation;

use crate::planner::Collection;
#[cfg(any(test, feature = "_test-harness"))]
use crate::planner::PlanError;

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
// BindingDeclaration
// =============================================================================

/// A declared relation and the input contract selected for its bindings.
/// Rule heads use this contract before materialization and sharing.
#[derive(Debug)]
pub(in crate::planner) struct BindingDeclaration<'program> {
    pub(in crate::planner) relation: &'program Relation,
    pub(in crate::planner) operation: BindingOperation,
}

impl<'program> BindingDeclaration<'program> {
    /// Uses logical row order for dedup, or all non-lattice columns as the
    /// grouping key for a declared min/max merge.
    #[must_use]
    pub(in crate::planner) fn new(relation: &'program Relation) -> Self {
        let operation = if let Some((target, _)) = relation.lattice() {
            BindingOperation::Aggregation {
                order: (0..relation.arity())
                    .filter(|&column| column != target)
                    .collect(),
            }
        } else {
            BindingOperation::Dedup {
                order: (0..relation.arity()).collect(),
            }
        };
        Self {
            relation,
            operation,
        }
    }

    /// Selects an explicit whole-row dedup order. Rejects invalid
    /// permutations and declared min/max relations.
    #[cfg(any(test, feature = "_test-harness"))]
    pub(in crate::planner) fn try_new(
        relation: &'program Relation,
        order: &[usize],
    ) -> Result<Self, PlanError> {
        let mut declaration = Self::new(relation);
        let BindingOperation::Dedup { order: selected } = &mut declaration.operation else {
            return Err(PlanError::internal(format!(
                "whole-row order selected for aggregation relation `{}`",
                relation.name(),
            )));
        };
        let mut sorted = order.to_vec();
        sorted.sort_unstable();
        if !sorted.iter().copied().eq(0..relation.arity()) {
            return Err(PlanError::internal(format!(
                "relation `{}` has binding order {order:?}, expected {} columns",
                relation.name(),
                relation.arity(),
            )));
        }
        *selected = order.to_vec();
        Ok(declaration)
    }
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
/// - [`seed`](Self::seed): an optional earlier binding,
///   supplied by an input declaration or an earlier stratum. Inside recursion,
///   it refers to the entered outer binding, separately from the
///   feedback variable. A completed stratum has already resolved whether a
///   seed exists.
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
    pub(in crate::planner) inputs: Vec<Arc<Collection>>,
    pub(in crate::planner) seed: Option<Arc<Collection>>,
    pub(in crate::planner) operation: BindingOperation,
}

impl RelationBindingPlan {
    #[must_use]
    pub(super) fn new(operation: BindingOperation, inputs: Vec<Arc<Collection>>) -> Self {
        Self {
            inputs,
            seed: None,
            operation,
        }
    }

    /// Rule results after sharing, in concatenation order. Each input keeps
    /// its producer's weight until the union performs any required lift.
    #[must_use]
    pub fn inputs(&self) -> &[Arc<Collection>] {
        &self.inputs
    }

    /// Returns the producer identities consumed directly by the binding.
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
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::program_planner;

    // Cases: duplicate, missing and out-of-range columns.
    #[rstest]
    #[case(&[0, 0])]
    #[case(&[0])]
    #[case(&[0, 2])]
    fn binding_declarations_reject_invalid_orders(#[case] order: &[usize]) {
        let program = program(".decl R(a:int32,b:symbol)\n.input R\n.output R\n");
        let error = BindingDeclaration::try_new(&program.relations()[0], order).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("binding order"));
    }

    #[test]
    fn declared_aggregation_rejects_a_whole_row_dedup_order() {
        let program = program(".decl R(k:int32,v:min<int32>)\n.input R\n.output R\n");
        let error = BindingDeclaration::try_new(&program.relations()[0], &[1, 0]).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("aggregation relation `r`"));
    }

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
        let declaration = BindingDeclaration::new(&program.relations()[0]);
        let binding = RelationBindingPlan::new(declaration.operation, Vec::new());
        assert_eq!(binding.operation(), &expected);
    }

    #[test]
    fn binding_inputs_reference_rule_results_directly() {
        let plan = program_planner(
            ".decl A(a:symbol,b:int32,c:int64)\n.input A\n\
             .decl B(a:symbol,b:int32,c:int64)\n.input B\n\
             .decl Out(c:int64,a:symbol,b:int32)\n.output Out\n\
             Out(c,a,b) :- A(a,b,c).\nOut(c,a,b) :- B(a,b,c).\n",
        );
        let binding = &plan.strata()[0].relation_bindings()[&compute_fp("out")];
        assert_eq!(binding.inputs().len(), 2);
        for input in binding.inputs() {
            let head = plan.strata()[0]
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == input.fingerprint())
                .unwrap();
            assert!(Arc::ptr_eq(input, head.output()));
            assert_eq!(head.flow().to_string(), "V:(V2, V0, V1)");
        }
        assert_ne!(
            binding.inputs()[0].fingerprint(),
            binding.inputs()[1].fingerprint()
        );
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
        let out = &bindings[&compute_fp("out")].inputs()[0];
        let other = &bindings[&compute_fp("other")].inputs()[0];
        assert_eq!(out.fingerprint(), other.fingerprint());
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
            .unwrap();
        let binding = &recursive.relation_bindings()[&compute_fp("reach")];
        let seed = binding.seed().unwrap();
        assert_eq!(seed.fingerprint(), compute_fp("reach"));
        assert_eq!(seed.mutability(), flowlog_parser::Mutability::Static);
        assert_eq!(
            binding.inputs()[0].mutability(),
            flowlog_parser::Mutability::Mutable
        );
        assert!(
            recursive
                .recursive_transformations()
                .iter()
                .any(|step| { step.output().fingerprint() == binding.inputs()[0].fingerprint() })
        );
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
