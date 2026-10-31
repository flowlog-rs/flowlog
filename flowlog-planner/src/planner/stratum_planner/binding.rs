//! Relation bindings: their rule results, earlier seeds, and operations.

use std::collections::HashMap;
use std::sync::Arc;

use flowlog_parser::Relation;

use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;
use crate::planner::TransformationFlow;

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
/// The plan contains the following metadata:
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
/// - [`input_order`](Self::input_order): physical field orders already emitted
///   by exclusive rule-result producers. Other inputs and every seed remain
///   in logical order and need conversion when the binding's order differs.
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
    input_orders: HashMap<u64, Vec<usize>>,
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
            input_orders: HashMap::new(),
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

    /// Returns the physical-to-logical column order of a fused rule result.
    /// `None` means that the result still uses logical column order.
    #[must_use]
    pub fn input_order(&self, fingerprint: u64) -> Option<&[usize]> {
        self.input_orders.get(&fingerprint).map(Vec::as_slice)
    }
}

impl StratumPlanner {
    /// Folds a binding's dedup order into exclusive row-producing heads.
    /// Sharing must be complete, and `downstream` must contain every later
    /// stratum, so no other consumer can observe a changed row schema.
    /// Seeds and shared inputs retain logical order. Existing fused inputs
    /// can be reordered again without composing the same permutation twice.
    ///
    /// Returns an internal error when a selected order is not a permutation
    /// of its head's columns.
    pub(crate) fn permute_binding_inputs(&mut self, downstream: &[Self]) -> Result<(), PlanError> {
        let mut readers: HashMap<u64, usize> = HashMap::new();
        let mut producers: HashMap<u64, usize> = HashMap::new();
        let plans = std::iter::once(&*self).chain(downstream);
        for plan in plans {
            let steps = plan
                .non_recursive_transformations
                .iter()
                .chain(&plan.recursive_transformations);
            for step in steps {
                *producers.entry(step.output().fingerprint()).or_default() += 1;
                for input in step.input_fingerprints() {
                    *readers.entry(input).or_default() += 1;
                }
            }
            for binding in plan.relation_bindings.values() {
                let inputs = binding
                    .seed
                    .into_iter()
                    .chain(binding.inputs.iter().copied());
                for input in inputs {
                    *readers.entry(input).or_default() += 1;
                }
            }
            // Entering a prelude result exposes it to a separate scope even
            // when a later map aliases that result instead of reading it.
            for &input in &plan.recursion_enter_collections {
                *readers.entry(input).or_default() += 1;
            }
        }
        let steps = self
            .non_recursive_transformations
            .iter_mut()
            .chain(&mut self.recursive_transformations);
        let mut heads: HashMap<u64, &mut Transformation> = steps
            .filter(|step| !step.need_arrange())
            .map(|step| (step.output().fingerprint(), step))
            .collect();
        for binding in self.relation_bindings.values_mut() {
            let BindingOperation::Dedup { order } = &binding.operation else {
                continue;
            };
            for &input in &binding.inputs {
                if readers.get(&input) != Some(&1) || producers.get(&input) != Some(&1) {
                    continue;
                }
                let Some(head) = heads.get_mut(&input) else {
                    continue;
                };
                let columns = head.output().arity().1;
                let mut sorted = order.clone();
                sorted.sort_unstable();
                if !sorted.iter().copied().eq(0..columns) {
                    return Err(PlanError::internal(format!(
                        "head 0x{input:016x} has dedup order {order:?}, expected {columns} columns"
                    )));
                }
                let previous = binding.input_orders.get(&input);
                if previous.is_some_and(|previous| previous == order)
                    || (previous.is_none() && order.iter().copied().eq(0..columns))
                {
                    continue;
                }
                // Only this binding reads the head. Keep its identity while
                // changing expressions and metadata together; no reader needs
                // a compensating projection before the union.
                let positions: Vec<usize> = order
                    .iter()
                    .map(|&column| {
                        previous.map_or(column, |previous| {
                            previous
                                .iter()
                                .position(|&field| field == column)
                                .expect("recorded head order is a complete permutation")
                        })
                    })
                    .collect();
                permute_head(head, &positions);
                if order.iter().copied().eq(0..columns) {
                    binding.input_orders.remove(&input);
                } else {
                    binding.input_orders.insert(input, order.clone());
                }
            }
        }
        Ok(())
    }
}

/// Permutes a validated row output, keeping its expression types, local layout
/// and canonical columns in the same order. Inputs and filters are unchanged.
fn permute_head(head: &mut Transformation, positions: &[usize]) {
    let (flow, output) = match head {
        Transformation::RowToRow { flow, output, .. }
        | Transformation::KvToRow { flow, output, .. }
        | Transformation::JnToRow { flow, output, .. }
        | Transformation::NJnToRow { flow, output, .. }
        | Transformation::AggToRow { flow, output, .. } => (flow, output),
        Transformation::RowToKv { .. }
        | Transformation::KvToKv { .. }
        | Transformation::JnToKv { .. }
        | Transformation::NJnToKv { .. }
        | Transformation::AggToKv { .. } => unreachable!("only row outputs can be binding heads"),
    };
    let (TransformationFlow::KVToKV { value, .. }
    | TransformationFlow::JnToKV { value, .. }
    | TransformationFlow::Aggregate { value, .. }) = flow;
    *value = Arc::new(
        positions
            .iter()
            .map(|&column| value[column].clone())
            .collect(),
    );
    let mut collection = (**output).clone();
    collection.kv_layout = KeyValueLayout::new(
        Vec::new(),
        positions
            .iter()
            .map(|&column| collection.kv_layout.value()[column].clone())
            .collect(),
    );
    collection.canonical.value = positions
        .iter()
        .map(|&column| collection.canonical.value[column].clone())
        .collect();
    *output = Arc::new(collection);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::optimizer::Optimizer;
    use crate::planner::FactorArgument;
    use crate::planner::TransformationArgument;
    use crate::stratifier::Stratifier;
    use crate::test_harness::program_planner;

    /// Builds editable plans through normal sharing before selecting a
    /// non-default binding order, which the public planner does not choose yet.
    fn planned(source: &str) -> Vec<StratumPlanner> {
        let program = program(source);
        let stratifier = Stratifier::from_program(&program).unwrap();
        let declarations: HashMap<_, _> = program
            .relations()
            .iter()
            .map(|relation| (relation.fingerprint(), relation))
            .collect();
        let mut optimizer = Optimizer::new();
        let mut bound: HashSet<u64> = program.edb_fingerprints();
        let mut preludes = Vec::new();
        let mut strata = Vec::new();
        for stratum in stratifier.strata() {
            let plan = StratumPlanner::from_stratum(
                &program,
                stratum,
                &mut optimizer,
                &mut None,
                &preludes,
                &declarations,
                &bound,
            )
            .unwrap();
            bound.extend(plan.output_relations());
            preludes.extend(plan.non_recursive_transformations().iter().cloned());
            strata.push(plan);
        }
        strata
    }

    const TWO_HEADS: &str = r#"
        .decl A(a: symbol, b: int32, c: int64)
        .input A
        .decl B(a: symbol, b: int32, c: int64)
        .input B
        .decl Out(c: int64, a: symbol, b: int32)
        .output Out
        Out(c,a,b) :- A(a,b,c).
        Out(c,a,b) :- B(a,b,c).
    "#;

    #[test]
    fn every_exclusive_head_emits_the_binding_order_before_union() {
        let mut strata = planned(TWO_HEADS);
        let plan = &mut strata[0];
        let relation = compute_fp("out");
        plan.relation_bindings.get_mut(&relation).unwrap().operation = BindingOperation::Dedup {
            order: vec![2, 0, 1],
        };

        plan.permute_binding_inputs(&[]).unwrap();

        let binding = &plan.relation_bindings[&relation];
        assert_eq!(binding.inputs().len(), 2);
        for &input in binding.inputs() {
            assert_eq!(binding.input_order(input), Some(&[2, 0, 1][..]));
            let head = plan
                .non_recursive_transformations()
                .iter()
                .find(|step| step.output().fingerprint() == input)
                .unwrap();
            let slots: Vec<FactorArgument> = head
                .flow()
                .value()
                .iter()
                .map(|expr| expr.init().clone())
                .collect();
            assert_eq!(
                slots,
                [
                    FactorArgument::Var(TransformationArgument::KV((false, 1))),
                    FactorArgument::Var(TransformationArgument::KV((false, 2))),
                    FactorArgument::Var(TransformationArgument::KV((false, 0))),
                ]
            );
            assert!(head.output().kv_layout.key().is_empty());
            assert_eq!(head.output().kv_layout.value().len(), 3);
        }
    }

    #[test]
    fn changing_a_fused_order_is_idempotent_and_can_restore_logical_rows() {
        let mut strata = planned(TWO_HEADS);
        let plan = &mut strata[0];
        let relation = compute_fp("out");
        plan.relation_bindings.get_mut(&relation).unwrap().operation = BindingOperation::Dedup {
            order: vec![2, 0, 1],
        };
        plan.permute_binding_inputs(&[]).unwrap();
        let once = plan.non_recursive_transformations().to_vec();
        plan.permute_binding_inputs(&[]).unwrap();
        assert_eq!(plan.non_recursive_transformations(), once);

        plan.relation_bindings.get_mut(&relation).unwrap().operation = BindingOperation::Dedup {
            order: vec![0, 1, 2],
        };
        plan.permute_binding_inputs(&[]).unwrap();

        let binding = &plan.relation_bindings[&relation];
        for &input in binding.inputs() {
            assert_eq!(binding.input_order(input), None);
        }
        for head in plan.non_recursive_transformations() {
            let slots: Vec<FactorArgument> = head
                .flow()
                .value()
                .iter()
                .map(|expr| expr.init().clone())
                .collect();
            assert_eq!(
                slots,
                [
                    FactorArgument::Var(TransformationArgument::KV((false, 2))),
                    FactorArgument::Var(TransformationArgument::KV((false, 0))),
                    FactorArgument::Var(TransformationArgument::KV((false, 1))),
                ]
            );
        }
    }

    #[test]
    fn a_head_shared_by_another_relation_keeps_logical_order() {
        let mut strata = planned(
            r#"
            .decl A(a: int32, b: int32)
            .input A
            .decl Out(a: int32, b: int32)
            .output Out
            .decl Other(a: int32, b: int32)
            .output Other
            Out(b,a) :- A(a,b).
            Other(b,a) :- A(a,b).
            "#,
        );
        let plan = &mut strata[0];
        let relation = compute_fp("out");
        let input = plan.relation_bindings[&relation].inputs()[0];
        assert_eq!(
            plan.relation_bindings[&compute_fp("other")].inputs(),
            &[input]
        );
        let before = plan.non_recursive_transformations().to_vec();
        plan.relation_bindings.get_mut(&relation).unwrap().operation =
            BindingOperation::Dedup { order: vec![1, 0] };

        plan.permute_binding_inputs(&[]).unwrap();

        assert_eq!(plan.non_recursive_transformations(), before);
        assert_eq!(plan.relation_bindings[&relation].input_order(input), None);
    }

    #[test]
    fn a_head_read_in_a_later_stratum_keeps_logical_order() {
        let mut strata = planned(
            r#"
            .decl R(k: int32, a: int32)
            .input R
            .decl S(k: int32, b: int32, c: int32)
            .input S
            .decl T(z: int32)
            .input T
            .decl Wide(a: int32, b: int32, c: int32)
            .output Wide
            .decl Mid(z: int32)
            .decl Late(b: int32, c: int32, z: int32)
            .output Late
            Wide(a,b,c) :- R(k,a), S(k,b,c).
            Mid(z) :- T(z).
            Late(b,c,z) :- R(k,_), S(k,b,c), Mid(z).
            "#,
        );
        let (earlier, later) = strata.split_at_mut(1);
        let plan = &mut earlier[0];
        let relation = compute_fp("wide");
        let input = plan.relation_bindings[&relation].inputs()[0];
        assert!(later.iter().any(|plan| {
            plan.non_recursive_transformations()
                .iter()
                .any(|step| step.input_fingerprints().contains(&input))
        }));
        let before = plan.non_recursive_transformations().to_vec();
        plan.relation_bindings.get_mut(&relation).unwrap().operation = BindingOperation::Dedup {
            order: vec![2, 0, 1],
        };

        plan.permute_binding_inputs(later).unwrap();

        assert_eq!(plan.non_recursive_transformations(), before);
        assert_eq!(plan.relation_bindings[&relation].input_order(input), None);
    }

    #[test]
    fn recursive_heads_can_be_permuted_without_changing_the_entered_seed() {
        let mut strata = planned(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let plan = &mut strata[1];
        let relation = compute_fp("reach");
        plan.relation_bindings.get_mut(&relation).unwrap().operation =
            BindingOperation::Dedup { order: vec![1, 0] };

        plan.permute_binding_inputs(&[]).unwrap();

        let binding = &plan.relation_bindings[&relation];
        assert_eq!(binding.seed(), Some(relation));
        assert_eq!(binding.input_order(relation), None);
        for &input in binding.inputs() {
            assert_eq!(binding.input_order(input), Some(&[1, 0][..]));
        }
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
