//! Applying selected layouts and join directions across stratum plans.
//!
//! [`Selection`] owns the choices and validates their plan locations and
//! key/value membership. [`apply`] coordinates dependency-ordered traversal,
//! keeps outer and recursive producer mappings separate, updates relation
//! binding orders, and propagates shared producer changes to downstream
//! readers while preserving downstream choices.
//! [`super::layout_permutation`] performs each operator's field and
//! expression remapping.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use super::arrangement_graph::Arities;
use super::arrangement_graph::JoinOrientation;
use super::arrangement_graph::PhysicalArrangement;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationId;
use super::layout_permutation;
use super::layout_permutation::Input;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

// =============================================================================
// Selection
// =============================================================================

/// Owned solver choices, detached from the graph's borrowed feed expressions.
/// Field IDs name positions before this rewrite, key columns first.
#[derive(Debug)]
pub(super) struct Selection {
    pub(super) layouts: BTreeMap<PhysicalArrangement, Arities>,
    pub(super) joins: BTreeMap<TransformationId, JoinOrientation>,
}

impl Selection {
    /// Rejects orders outside their original K/V blocks or changes to frozen
    /// downstream choices. Every selected node must name an existing boundary.
    fn validate(
        &self,
        stratum: &StratumPlanner,
        downstream: &[StratumPlanner],
    ) -> Result<(), PlanError> {
        for (node, selected) in &self.layouts {
            let (index, scope) = match node {
                PhysicalArrangement::Arrangement { stratum, scope, .. }
                | PhysicalArrangement::Dedup { stratum, scope, .. }
                | PhysicalArrangement::Aggregation { stratum, scope, .. } => (*stratum, *scope),
            };
            let plan = plan_at(index, stratum, downstream)?;
            if matches!(
                node,
                PhysicalArrangement::Dedup { .. } | PhysicalArrangement::Aggregation { .. }
            ) && scope
                != if plan.is_recursive() {
                    Scope::Recursive
                } else {
                    Scope::NonRecursive
                }
            {
                return Err(PlanError::internal(format!(
                    "binding layout selects the wrong scope: {node:?}"
                )));
            }
            let expected = match node {
                PhysicalArrangement::Arrangement { transformation, .. } => {
                    let step = steps(plan, scope).get(*transformation).ok_or_else(|| {
                        PlanError::internal(format!("layout selects missing step {node:?}"))
                    })?;
                    if !step.need_arrange() {
                        return Err(PlanError::internal(format!(
                            "layout selects a row-only step {node:?}"
                        )));
                    }
                    let (key, value) = step.output().arity();
                    Arities::new(key, value)
                }
                PhysicalArrangement::Dedup { relation, .. }
                | PhysicalArrangement::Aggregation { relation, .. } => {
                    let binding = plan.relation_bindings().get(relation).ok_or_else(|| {
                        PlanError::internal(format!("layout selects missing binding {node:?}"))
                    })?;
                    let expected_node = PhysicalArrangement::relation_binding(
                        *relation,
                        binding.operation(),
                        index,
                        scope,
                    );
                    if *node != expected_node {
                        return Err(PlanError::internal(format!(
                            "layout selects {node:?}, expected {expected_node:?}"
                        )));
                    }
                    let (BindingOperation::Dedup { order }
                    | BindingOperation::Aggregation { order }) = binding.operation();
                    Arities {
                        key: order.clone(),
                        value: Vec::new(),
                    }
                }
            };
            let mut key = selected.key.clone();
            let mut value = selected.value.clone();
            let mut expected_key = expected.key.clone();
            let mut expected_value = expected.value.clone();
            key.sort_unstable();
            value.sort_unstable();
            expected_key.sort_unstable();
            expected_value.sort_unstable();
            if key != expected_key || value != expected_value {
                return Err(PlanError::internal(format!(
                    "layout {selected:?} does not permute the blocks {expected:?} of {node:?}"
                )));
            }
            if index != 0 && *selected != expected {
                return Err(PlanError::internal(format!(
                    "layout changes frozen downstream boundary {node:?}"
                )));
            }
        }
        for (id, orientation) in &self.joins {
            let plan = plan_at(id.stratum, stratum, downstream)?;
            let step = steps(plan, id.scope)
                .get(id.transformation)
                .ok_or_else(|| {
                    PlanError::internal(format!("orientation selects missing step {id:?}"))
                })?;
            if !matches!(
                step,
                Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
            ) {
                return Err(PlanError::internal(format!(
                    "orientation selects a step other than an ordinary join: {id:?}"
                )));
            }
            if id.stratum != 0 && *orientation != JoinOrientation::Original {
                return Err(PlanError::internal(format!(
                    "orientation changes frozen downstream join {id:?}"
                )));
            }
        }
        Ok(())
    }
}

/// Resolves graph numbering: downstream plans are stored in reverse order.
fn plan_at<'plan>(
    index: usize,
    stratum: &'plan StratumPlanner,
    downstream: &'plan [StratumPlanner],
) -> Result<&'plan StratumPlanner, PlanError> {
    if index == 0 {
        return Ok(stratum);
    }
    downstream.iter().rev().nth(index - 1).ok_or_else(|| {
        PlanError::internal(format!("layout selects missing downstream stratum {index}"))
    })
}

/// Resolves the scope-local numbering used by graph transformation IDs.
#[must_use]
fn steps(plan: &StratumPlanner, scope: Scope) -> &[Transformation] {
    match scope {
        Scope::NonRecursive => plan.non_recursive_transformations(),
        Scope::Recursive => plan.recursive_transformations(),
    }
}

// =============================================================================
// Plan rewriting
// =============================================================================

/// Applies a legal selection and remaps all readers, including shared readers
/// in downstream strata. Union, seed and feedback rows keep logical positions;
/// binding operations restore those positions after their physical sort.
/// Downstream orders and join directions remain fixed.
///
/// Returns an internal error for invalid choices, unmatched join key orders,
/// or invalid slot references. No objective is evaluated here: costs belong
/// to the solver and are independent of applying a layout.
pub(super) fn apply(
    stratum: &mut StratumPlanner,
    downstream: &mut [StratumPlanner],
    selection: Selection,
) -> Result<(), PlanError> {
    selection.validate(stratum, downstream)?;
    let mut outer = HashMap::new();
    rewrite_stratum(stratum, 0, &selection, &mut outer)?;
    let evaluation_order = downstream.iter_mut().rev().enumerate();
    for (index, plan) in evaluation_order {
        rewrite_stratum(plan, index + 1, &selection, &mut outer)?;
    }
    Ok(())
}

/// Rewrites prelude and loop steps with separate producer registries, so
/// loop-local arrangements cannot replace similarly named outer producers.
fn rewrite_stratum(
    plan: &mut StratumPlanner,
    stratum: usize,
    selection: &Selection,
    outer: &mut HashMap<u64, Input>,
) -> Result<(), PlanError> {
    rewrite_steps(
        &mut plan.non_recursive_transformations,
        stratum,
        Scope::NonRecursive,
        selection,
        outer,
    )?;
    if plan.is_recursive() && !plan.recursion_leave_collections().is_empty() {
        let mut recursive = outer.clone();
        rewrite_steps(
            &mut plan.recursive_transformations,
            stratum,
            Scope::Recursive,
            selection,
            &mut recursive,
        )?;
    }
    // A relation's physical order is internal to its binding operator; all
    // its consumers, including its next iteration, still read logical rows.
    if stratum == 0 {
        for (node, layout) in &selection.layouts {
            let relation = match node {
                PhysicalArrangement::Dedup {
                    stratum: 0,
                    relation,
                    ..
                }
                | PhysicalArrangement::Aggregation {
                    stratum: 0,
                    relation,
                    ..
                } => *relation,
                PhysicalArrangement::Arrangement { .. }
                | PhysicalArrangement::Dedup { .. }
                | PhysicalArrangement::Aggregation { .. } => continue,
            };
            let binding = plan.relation_bindings.get_mut(&relation).ok_or_else(|| {
                PlanError::internal(format!("selected binding 0x{relation:016x} disappeared"))
            })?;
            layout_permutation::binding(binding, layout);
        }
    }
    Ok(())
}

/// Rewrites dependencies before their readers. Row-only outputs retain their
/// schema, while keyed outputs apply the selected within-block permutations.
fn rewrite_steps(
    steps: &mut [Transformation],
    stratum: usize,
    scope: Scope,
    selection: &Selection,
    rewritten: &mut HashMap<u64, Input>,
) -> Result<(), PlanError> {
    let indexed = steps.iter_mut().enumerate();
    for (transformation, step) in indexed {
        let node = PhysicalArrangement::Arrangement {
            stratum,
            scope,
            transformation,
        };
        let (key, value) = step.output().arity();
        let order = selection
            .layouts
            .get(&node)
            .cloned()
            .unwrap_or_else(|| Arities::new(key, value));
        let id = TransformationId {
            stratum,
            scope,
            transformation,
        };
        let orientation = selection
            .joins
            .get(&id)
            .copied()
            .unwrap_or(JoinOrientation::Original);
        layout_permutation::transformation(step, &order, orientation, rewritten)?;
        rewritten.insert(
            step.output().fingerprint(),
            Input {
                collection: Arc::clone(step.output()),
                order,
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::Program;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::optimizer::Optimizer;
    use crate::planner::FactorArgument;
    use crate::planner::TransformationArgument;
    use crate::planner::TransformationFlow;
    use crate::planner::arity_order::initialize::initialize;
    use crate::stratifier::Stratifier;

    const JOIN: &str = r#"
        .decl R(k0: int32, k1: int32, a: int32, b: int32)
        .input R
        .decl S(k0: int32, k1: int32, c: int32, d: int32)
        .input S
        .decl Out(a: int32, b: int32, c: int32, d: int32)
        .output Out
        Out(a,b,c,d) :- R(k0,k1,a,b), S(k0,k1,c,d).
    "#;

    /// Builds editable strata through the normal sharing and seed resolution
    /// path, before applying any test-selected physical order.
    fn planned(source: &str) -> (Program, Vec<StratumPlanner>) {
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
        (program, strata)
    }

    fn selection(
        stratum: &StratumPlanner,
        downstream: &[StratumPlanner],
        program: &Program,
    ) -> Selection {
        let graph = initialize(stratum, &[], downstream, program);
        Selection {
            layouts: graph.nodes,
            joins: graph.joins,
        }
    }

    /// Reads a projection/join's plain output slots from supplied physical
    /// tuples. The expected rows below are logical query answers, independent
    /// of how the rewriter represents a permutation.
    fn output_values(
        flow: &TransformationFlow,
        left: &[i32],
        right: &[i32],
        keys: usize,
    ) -> Vec<i32> {
        flow.key()
            .iter()
            .chain(flow.value().iter())
            .map(|expression| {
                let FactorArgument::Var(argument) = expression.init else {
                    panic!("this query only projects columns");
                };
                assert!(expression.rest.is_empty());
                match argument {
                    TransformationArgument::KV((is_key, index)) => {
                        left[if is_key { index } else { keys + index }]
                    }
                    TransformationArgument::Jn((is_left, is_key, index)) => {
                        let input = if is_left { left } else { right };
                        input[if is_key { index } else { keys + index }]
                    }
                }
            })
            .collect()
    }

    #[test]
    fn permuted_join_inputs_and_swapped_sides_preserve_logical_answers() {
        let (program, mut strata) = planned(JOIN);
        let plan = &mut strata[0];
        let mut chosen = selection(plan, &[], &program);
        for (node, order) in &mut chosen.layouts {
            if matches!(node, PhysicalArrangement::Arrangement { .. }) {
                assert_eq!((order.key.len(), order.value.len()), (2, 2));
                order.key.reverse();
                order.value.reverse();
            }
        }
        let (id, direction) = chosen.joins.first_key_value().unwrap();
        let join_index = id.transformation;
        assert_eq!(*direction, JoinOrientation::Original);
        *chosen.joins.values_mut().next().unwrap() = JoinOrientation::Swapped;
        let join = &plan.non_recursive_transformations()[join_index];
        let (left, right) = join.binary_input();
        let old_left = left.fingerprint();
        let old_right = right.fingerprint();
        let old_form = join.output().canonical().clone();

        apply(plan, &mut [], chosen).unwrap();

        let join = &plan.non_recursive_transformations()[join_index];
        let (left, right) = join.binary_input();
        assert_eq!(
            (left.fingerprint(), right.fingerprint()),
            (old_right, old_left)
        );
        assert_eq!(join.output().canonical(), &old_form);
        assert_eq!(
            output_values(join.flow(), &[20, 10, 4, 3], &[20, 10, 2, 1], 2),
            [1, 2, 3, 4]
        );
    }

    // Cases: duplicated column, field crossing K/V, missing value column.
    #[rstest]
    #[case(vec![0, 0], vec![2, 3])]
    #[case(vec![0, 2], vec![1, 3])]
    #[case(vec![0, 1], vec![2])]
    fn invalid_layouts_are_rejected_before_rewriting(
        #[case] key: Vec<usize>,
        #[case] value: Vec<usize>,
    ) {
        let (program, mut strata) = planned(JOIN);
        let plan = &mut strata[0];
        let before = plan.to_string();
        let mut chosen = selection(plan, &[], &program);
        let order = chosen
            .layouts
            .iter_mut()
            .find_map(|(node, order)| {
                matches!(node, PhysicalArrangement::Arrangement { .. }).then_some(order)
            })
            .unwrap();
        *order = Arities { key, value };

        let error = apply(plan, &mut [], chosen).unwrap_err();

        assert!(matches!(&error, PlanError::Internal(_)));
        assert!(error.to_string().contains("does not permute the blocks"));
        assert_eq!(plan.to_string(), before);
    }

    #[test]
    fn incompatible_join_key_orders_are_rejected() {
        let (program, mut strata) = planned(JOIN);
        let plan = &mut strata[0];
        let mut chosen = selection(plan, &[], &program);
        let order = chosen
            .layouts
            .iter_mut()
            .find_map(|(node, order)| {
                matches!(node, PhysicalArrangement::Arrangement { .. }).then_some(order)
            })
            .unwrap();
        order.key.reverse();

        let error = apply(plan, &mut [], chosen).unwrap_err();

        assert!(matches!(&error, PlanError::Internal(_)));
        assert!(error.to_string().contains("incompatible key orders"));
    }

    #[test]
    fn a_projection_over_a_reordered_shared_join_does_not_restore_dropped_fields() {
        let (program, mut strata) = planned(
            r#"
            .decl R(k: int32, a: int32, b: int32)
            .input R
            .decl S(k: int32, c: int32)
            .input S
            .decl T(c: int32, z: int32)
            .input T
            .decl U(c: int32, w: int32)
            .input U
            .decl Wide(a: int32, b: int32, z: int32)
            .decl Narrow(a: int32, w: int32)
            .output Wide
            .output Narrow
            Wide(a,b,z) :- R(k,a,b), S(k,c), T(c,z).
            Narrow(a,w) :- R(k,a,_), S(k,c), U(c,w).
        "#,
        );
        let plan = &mut strata[0];
        let (index, producer) = plan
            .non_recursive_transformations()
            .iter()
            .enumerate()
            .find(|(_, step)| {
                step.need_arrange()
                    && step.output().arity() == (1, 2)
                    && plan.non_recursive_transformations().iter().any(|reader| {
                        reader.is_map()
                            && reader.unary_input().fingerprint() == step.output().fingerprint()
                    })
            })
            .expect("the wide partial join serves the narrow rule's projection");
        let fingerprint = producer.output().fingerprint();
        let consumer = plan
            .non_recursive_transformations()
            .iter()
            .find(|step| step.is_map() && step.unary_input().fingerprint() == fingerprint)
            .unwrap();
        let retained = consumer.output().arity();
        assert!(retained.0 + retained.1 < 3);
        let mut chosen = selection(plan, &[], &program);
        let node = PhysicalArrangement::Arrangement {
            stratum: 0,
            scope: Scope::NonRecursive,
            transformation: index,
        };
        chosen.layouts.get_mut(&node).unwrap().value.reverse();

        apply(plan, &mut [], chosen).unwrap();

        let consumer = plan
            .non_recursive_transformations()
            .iter()
            .find(|step| step.is_map() && step.unary_input().fingerprint() == fingerprint)
            .unwrap();
        assert_eq!(consumer.output().arity(), retained);
        assert_eq!(
            output_values(consumer.flow(), &[10, 30, 20], &[], 1),
            [10, 20]
        );
    }

    #[test]
    fn aggregate_answers_keep_their_own_value_slot() {
        let (program, mut strata) = planned(
            r#"
            .decl Node(a: int32, b: int32)
            .input Node
            .decl Edge(a: int32, b: int32, v: int32, w: int32)
            .input Edge
            .decl Out(a: int32, b: int32, s: int32)
            .output Out
            Out(a,b,s) :- Node(a,b), s = sum v : { Edge(a,b,v,w) }.
        "#,
        );
        let plan = &mut strata[0];
        let mut chosen = selection(plan, &[], &program);
        for (node, order) in &mut chosen.layouts {
            if matches!(node, PhysicalArrangement::Arrangement { .. }) {
                order.key.reverse();
                order.value.reverse();
            }
        }
        let aggregate = plan
            .non_recursive_transformations()
            .iter()
            .find(|step| matches!(step.flow(), TransformationFlow::Aggregate { .. }))
            .unwrap();
        let TransformationFlow::Aggregate { target, .. } = aggregate.flow() else {
            unreachable!()
        };
        assert_eq!(*target, Some(TransformationArgument::KV((false, 0))));

        apply(plan, &mut [], chosen).unwrap();

        let aggregate = plan
            .non_recursive_transformations()
            .iter()
            .find(|step| matches!(step.flow(), TransformationFlow::Aggregate { .. }))
            .unwrap();
        let TransformationFlow::Aggregate { target, .. } = aggregate.flow() else {
            unreachable!()
        };
        assert_eq!(*target, Some(TransformationArgument::KV((false, 1))));
        assert_eq!(
            output_values(aggregate.flow(), &[20, 10, 42], &[], 2),
            [10, 20, 42]
        );
    }

    #[test]
    fn a_recursive_binding_keeps_its_seed_union_and_feedback() {
        let (program, mut strata) = planned(
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
        let binding = &plan.relation_bindings()[&relation];
        let inputs = binding.inputs().to_vec();
        let seed = binding.seed();
        let feedback = plan.recursion_feedback_collections().to_vec();
        let mut chosen = selection(plan, &[], &program);
        let node = PhysicalArrangement::Dedup {
            stratum: 0,
            scope: Scope::Recursive,
            relation,
        };
        chosen.layouts.get_mut(&node).unwrap().key = vec![1, 0];

        apply(plan, &mut [], chosen).unwrap();

        let binding = &plan.relation_bindings()[&relation];
        assert_eq!(
            binding.operation(),
            &BindingOperation::Dedup { order: vec![1, 0] }
        );
        assert_eq!(binding.inputs(), inputs);
        assert_eq!(binding.seed(), seed);
        assert_eq!(plan.recursion_feedback_collections(), feedback);
        assert_eq!(seed, Some(relation));
    }

    #[test]
    fn shared_downstream_joins_keep_their_selected_output_layouts() {
        let (program, mut strata) = planned(
            r#"
            .decl R(k: int32, a: int32)
            .input R
            .decl S(k: int32, b: int32, c: int32)
            .input S
            .decl Wide(a: int32, b: int32, c: int32)
            .decl Mid(b: int32, c: int32)
            .decl Out(b: int32, c: int32, z: int32)
            .output Wide
            .output Out
            Wide(a,b,c) :- R(k,a), S(k,b,c).
            Mid(b,c) :- R(k,_), S(k,b,c).
            Out(b,c,z) :- S(k,b,c), Mid(k,z).
        "#,
        );
        let mut later = vec![strata.pop().unwrap()];
        let plan = &mut strata[0];
        let candidate = plan
            .non_recursive_transformations()
            .iter()
            .enumerate()
            .find(|(_, step)| {
                step.need_arrange()
                    && step.output().arity() == (1, 2)
                    && later[0]
                        .non_recursive_transformations()
                        .iter()
                        .any(|consumer| {
                            consumer
                                .input_fingerprints()
                                .contains(&step.output().fingerprint())
                        })
            })
            .unwrap();
        let (index, producer) = candidate;
        let source = producer.output().fingerprint();
        let consumer = later[0]
            .non_recursive_transformations()
            .iter()
            .find(|step| {
                matches!(step, Transformation::JnToRow { .. })
                    && step.input_fingerprints().contains(&source)
            })
            .unwrap();
        let retained = consumer.output().arity();
        let (left, _) = consumer.binary_input();
        let source_is_left = left.fingerprint() == source;
        let downstream_orders = selection(&later[0], &[], &program).layouts;
        let mut chosen = selection(plan, &later, &program);
        let node = PhysicalArrangement::Arrangement {
            stratum: 0,
            scope: Scope::NonRecursive,
            transformation: index,
        };
        chosen.layouts.get_mut(&node).unwrap().value = vec![2, 1];

        apply(plan, &mut later, chosen).unwrap();

        let consumer = later[0]
            .non_recursive_transformations()
            .iter()
            .find(|step| {
                matches!(step, Transformation::JnToRow { .. })
                    && step.input_fingerprints().contains(&source)
            })
            .unwrap();
        assert_eq!(consumer.output().arity(), retained);
        let (left_rows, right_rows) = if source_is_left {
            (&[10, 30, 20][..], &[10, 40][..])
        } else {
            (&[10, 40][..], &[10, 30, 20][..])
        };
        assert_eq!(
            output_values(consumer.flow(), left_rows, right_rows, 1),
            [20, 30, 40]
        );
        let (left, right) = consumer.binary_input();
        let shared = if source_is_left { left } else { right };
        assert!(Arc::ptr_eq(
            shared,
            plan.non_recursive_transformations()[index].output()
        ));
        assert_eq!(
            selection(&later[0], &[], &program).layouts,
            downstream_orders
        );
    }

    #[test]
    fn frozen_downstream_orders_cannot_be_changed_by_an_upstream_selection() {
        let (program, mut strata) = planned(
            r#"
            .decl E(a: int32, b: int32)
            .input E
            .decl Mid(a: int32, b: int32)
            .decl Out(a: int32, b: int32)
            .output Out
            Mid(a,b) :- E(a,b).
            Out(a,b) :- Mid(a,b).
        "#,
        );
        let mut later = vec![strata.pop().unwrap()];
        let plan = &mut strata[0];
        let mut chosen = selection(plan, &later, &program);
        let node = PhysicalArrangement::Dedup {
            stratum: 1,
            scope: Scope::NonRecursive,
            relation: compute_fp("out"),
        };
        chosen.layouts.get_mut(&node).unwrap().key.reverse();

        let error = apply(plan, &mut later, chosen).unwrap_err();

        assert!(matches!(&error, PlanError::Internal(_)));
        assert!(error.to_string().contains("frozen downstream boundary"));
    }

    // Cases: declared columns, rule columns, original and selected key orders.
    #[rstest]
    #[case("a: min<int32>, b: int32, c: int32", "a,b,c", &[1, 2], &[2, 1])]
    #[case("a: int32, b: min<int32>, c: int32", "a,b,c", &[0, 2], &[2, 0])]
    #[case("a: int32, b: int32, c: min<int32>", "a,b,c", &[0, 1], &[1, 0])]
    #[case("a: min<int32>", "a", &[], &[])]
    fn initialized_aggregation_orders_exclude_the_answer_column(
        #[case] fields: &str,
        #[case] arguments: &str,
        #[case] expected: &[usize],
        #[case] selected: &[usize],
    ) {
        let plain = fields.replace("min<int32>", "int32");
        let (program, mut strata) = planned(&format!(
            ".decl E({plain})\n.input E\n.decl R({fields})\n.output R\nR({arguments}) :- E({arguments})."
        ));
        let plan = &mut strata[0];
        let mut chosen = selection(plan, &[], &program);
        let node = PhysicalArrangement::Aggregation {
            stratum: 0,
            scope: Scope::NonRecursive,
            relation: compute_fp("r"),
        };
        let order = chosen.layouts.get_mut(&node).unwrap();
        assert_eq!(order.key, expected);
        assert!(order.value.is_empty());
        order.key.reverse();

        apply(plan, &mut [], chosen).unwrap();

        assert_eq!(
            plan.relation_bindings()[&compute_fp("r")].operation(),
            &BindingOperation::Aggregation {
                order: selected.to_vec()
            }
        );
    }
}
