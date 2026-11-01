//! Relation bindings: their rule results, earlier seeds, and operations.

use std::sync::Arc;

use flowlog_common::compute_fp;
use flowlog_parser::Relation;

use crate::planner::Collection;
use crate::planner::PlanError;
#[cfg(feature = "_test-harness")]
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

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
/// - [`inputs`](Self::inputs): row projections over the shared rule results,
///   in concatenation order. Every projection has a distinct output identity.
///   Their output rows must share the binding operation's input format.
/// - [`seed`](Self::seed): an optional projection of an earlier binding,
///   supplied by an input declaration or an earlier stratum. Inside recursion,
///   its input refers to the entered outer binding, separately from the
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
    pub(in crate::planner) inputs: Vec<Transformation>,
    pub(in crate::planner) seed: Option<Transformation>,
    pub(in crate::planner) operation: BindingOperation,
}

impl RelationBindingPlan {
    pub(super) fn try_new(
        declaration: &Relation,
        inputs: Vec<Arc<Collection>>,
    ) -> Result<Self, PlanError> {
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
        let columns: Vec<usize> = (0..declaration.arity()).collect();
        let inputs = inputs
            .into_iter()
            .map(|input| {
                let output = compute_fp((
                    "binding_input",
                    declaration.fingerprint(),
                    input.fingerprint(),
                ));
                Transformation::row_projection(input, output, &columns)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            inputs,
            seed: None,
            operation,
        })
    }

    /// Pure row projections after sharing, in concatenation order. They
    /// preserve each input's weight; the union performs any required lift.
    #[must_use]
    pub fn inputs(&self) -> &[Transformation] {
        &self.inputs
    }

    /// Returns source fingerprints before the binding's input projections.
    #[must_use = "consume the iterator to read the input fingerprints"]
    pub fn input_fingerprints(&self) -> impl Iterator<Item = u64> + '_ {
        self.inputs
            .iter()
            .map(|projection| projection.unary_input().fingerprint())
    }

    /// The earlier outer binding unioned before the rule results. Inside
    /// recursion this is the entered seed, not the feedback variable.
    #[must_use]
    pub fn seed(&self) -> Option<&Transformation> {
        self.seed.as_ref()
    }

    #[must_use]
    pub fn operation(&self) -> &BindingOperation {
        &self.operation
    }
}

#[cfg(feature = "_test-harness")]
impl StratumPlanner {
    pub(in crate::planner) fn set_binding_order_for_test(
        &mut self,
        relation: u64,
        order: &[usize],
    ) {
        let Some(binding) = self.relation_bindings.get_mut(&relation) else {
            return;
        };
        let BindingOperation::Dedup { order: current } = &mut binding.operation else {
            panic!("the test relation must use whole-row dedup");
        };
        let mut sorted = order.to_vec();
        sorted.sort_unstable();
        assert!(
            sorted.iter().copied().eq(0..current.len()),
            "the test order must contain every relation column exactly once",
        );
        *current = order.to_vec();
        let projections = binding.inputs.iter_mut().chain(binding.seed.iter_mut());
        for projection in projections {
            *projection = Transformation::row_projection(
                Arc::clone(projection.unary_input()),
                projection.output().fingerprint(),
                order,
            )
            .expect("a binding projection reads its complete logical source row");
        }
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
        let binding = RelationBindingPlan::try_new(&program.relations()[0], Vec::new()).unwrap();
        assert_eq!(binding.operation(), &expected);
    }

    #[test]
    fn binding_inputs_are_identity_projections_with_distinct_outputs() {
        let plan = program_planner(
            ".decl A(a:symbol,b:int32,c:int64)\n.input A\n\
             .decl B(a:symbol,b:int32,c:int64)\n.input B\n\
             .decl Out(c:int64,a:symbol,b:int32)\n.output Out\n\
             Out(c,a,b) :- A(a,b,c).\nOut(c,a,b) :- B(a,b,c).\n",
        );
        let binding = &plan.strata()[0].relation_bindings()[&compute_fp("out")];
        assert_eq!(binding.inputs().len(), 2);
        for projection in binding.inputs() {
            let Transformation::RowToRow {
                input,
                output,
                flow,
            } = projection
            else {
                panic!("binding inputs are row projections");
            };
            assert_ne!(input.fingerprint(), output.fingerprint());
            assert_eq!(flow.to_string(), "V:(V0, V1, V2)");
            assert_eq!(input.canonical(), output.canonical());
            assert_eq!(input.mutability(), output.mutability());
        }
        assert_ne!(
            binding.inputs()[0].output().fingerprint(),
            binding.inputs()[1].output().fingerprint(),
        );
    }

    #[test]
    fn shared_rule_results_keep_independent_binding_projections() {
        let plan = program_planner(
            ".decl A(a:int32,b:int32)\n.input A\n\
             .decl Out(a:int32,b:int32)\n.output Out\n\
             .decl Other(a:int32,b:int32)\n.output Other\n\
             Out(b,a) :- A(a,b).\nOther(b,a) :- A(a,b).\n",
        );
        let bindings = plan.strata()[0].relation_bindings();
        let out = &bindings[&compute_fp("out")].inputs()[0];
        let other = &bindings[&compute_fp("other")].inputs()[0];
        assert_eq!(
            out.unary_input().fingerprint(),
            other.unary_input().fingerprint()
        );
        assert_ne!(out.output().fingerprint(), other.output().fingerprint());
    }

    #[test]
    fn a_recursive_seed_projection_keeps_its_outer_static_weight() {
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
        assert_eq!(seed.unary_input().fingerprint(), compute_fp("reach"));
        assert_eq!(
            seed.output().mutability(),
            flowlog_parser::Mutability::Static
        );
        assert_eq!(
            binding.inputs()[0].output().mutability(),
            flowlog_parser::Mutability::Mutable
        );
        assert!(recursive.is_recursive_transformation(seed));
        assert!(recursive.is_recursive_transformation(&binding.inputs()[0]));
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
            binding.seed().map(|seed| seed.unary_input().fingerprint()),
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
            binding.seed().map(|seed| seed.unary_input().fingerprint()),
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
                .map(|seed| seed.unary_input().fingerprint()),
            Some(compute_fp("r"))
        );
    }
}
