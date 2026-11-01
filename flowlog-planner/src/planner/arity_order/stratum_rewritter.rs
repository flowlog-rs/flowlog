//! Applying selected layouts and join directions across stratum plans.
//!
//! [`apply`] validates choices through [`super::layout_validation`], then
//! visits producers before readers with separate outer and recursive mappings.
//! [`super::layout_permutation`] remaps each operator's fields and expressions.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use flowlog_parser::Program;

use super::arrangement_graph::Arities;
use super::arrangement_graph::JoinOrientation;
use super::arrangement_graph::PhysicalArrangement;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationId;
use super::layout_permutation;
use super::layout_permutation::Input;
use super::layout_validation;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

// =============================================================================
// Selection
// =============================================================================

/// Owned solver choices, detached from the graph's borrowed feed expressions.
#[derive(Debug)]
pub(super) struct Selection {
    pub(super) layouts: BTreeMap<PhysicalArrangement, Arities>,
    pub(super) joins: BTreeMap<TransformationId, JoinOrientation>,
}

// =============================================================================
// Plan rewriting
// =============================================================================

/// Applies a legal selection in evaluation order and remaps all shared
/// readers. Binding input projections follow their physical dedup order;
/// completed bindings and feedback rows retain logical column positions.
/// Fixed aggregation exits retain declaration order.
///
/// Returns an internal error for invalid choices, unmatched join key orders,
/// or invalid slot references. Costs belong to the solver.
pub(super) fn apply(
    plan: &mut [StratumPlanner],
    program: &Program,
    selection: Selection,
) -> Result<(), PlanError> {
    layout_validation::validate(&selection, plan, program)?;
    let mut outer_producer_map = HashMap::new();
    let evaluation_order = plan.iter_mut().enumerate();
    for (stratum_index, strata) in evaluation_order {
        rewrite_stratum(strata, stratum_index, &selection, &mut outer_producer_map)?;
    }
    Ok(())
}

/// Keeps loop-local producer changes separate from outer seed sources.
fn rewrite_stratum(
    strata: &mut StratumPlanner,
    stratum_index: usize,
    selection: &Selection,
    outer_producer_map: &mut HashMap<u64, Input>,
) -> Result<(), PlanError> {
    rewrite_steps(
        &mut strata.non_recursive_transformations,
        stratum_index,
        Scope::NonRecursive,
        selection,
        outer_producer_map,
    )?;
    if strata.is_recursive() && strata.recursion_leave_collections().is_empty() {
        return Ok(());
    }
    let recursive_producer_map = if strata.is_recursive() {
        // Inner layouts must not overwrite producers read by outer seeds.
        let mut recursive_producer_map = outer_producer_map.clone();
        rewrite_steps(
            &mut strata.recursive_transformations,
            stratum_index,
            Scope::Recursive,
            selection,
            &mut recursive_producer_map,
        )?;
        Some(recursive_producer_map)
    } else {
        None
    };
    let scope = if strata.is_recursive() {
        Scope::Recursive
    } else {
        Scope::NonRecursive
    };
    let input_producer_map = recursive_producer_map
        .as_ref()
        .unwrap_or(outer_producer_map);
    for (&relation_fingerprint, binding) in &mut strata.relation_bindings {
        if let Some(seed) = &mut binding.seed {
            let (key, value) = seed.output().arity();
            layout_permutation::transformation(
                seed,
                &Arities::new(key, value),
                JoinOrientation::Original,
                outer_producer_map,
            )?;
        }
        for projection in &mut binding.inputs {
            let (key, value) = projection.output().arity();
            layout_permutation::transformation(
                projection,
                &Arities::new(key, value),
                JoinOrientation::Original,
                input_producer_map,
            )?;
        }
        let node = PhysicalArrangement::relation_binding(
            relation_fingerprint,
            binding.operation(),
            stratum_index,
            scope,
        );
        if let Some(layout) = selection.layouts.get(&node) {
            layout_permutation::binding(binding, layout)?;
        }
    }
    Ok(())
}

/// Rewrites dependencies before their readers. Row-only outputs retain their
/// schema, while keyed outputs apply the selected within-block permutations.
fn rewrite_steps(
    steps: &mut [Transformation],
    stratum_index: usize,
    scope: Scope,
    selection: &Selection,
    rewritten: &mut HashMap<u64, Input>,
) -> Result<(), PlanError> {
    let indexed = steps.iter_mut().enumerate();
    for (transformation_index, step) in indexed {
        let node = PhysicalArrangement::Arrangement {
            stratum_index,
            scope,
            transformation_index,
        };
        let (key, value) = step.output().arity();
        let order = selection
            .layouts
            .get(&node)
            .cloned()
            .unwrap_or_else(|| Arities::new(key, value));
        let id = TransformationId::Rule {
            stratum_index,
            scope,
            transformation_index,
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
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::Mutability;
    use flowlog_parser::Program;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::optimizer::Optimizer;
    use crate::planner::BindingOperation;
    use crate::planner::FactorArgument;
    use crate::planner::TransformationArgument;
    use crate::planner::TransformationFlow;
    use crate::planner::arity_order::arrangement_graph::Producer;
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
        let mut bound: HashMap<u64, Mutability> = program
            .edbs()
            .into_iter()
            .map(|declaration| (declaration.fingerprint(), declaration.input_mutability()))
            .collect();
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
            for relation_fingerprint in plan.output_relations() {
                bound.insert(
                    relation_fingerprint,
                    plan.mutability(relation_fingerprint).unwrap(),
                );
            }
            preludes.extend(plan.non_recursive_transformations().iter().cloned());
            strata.push(plan);
        }
        (program, strata)
    }

    fn selection(plan: &[StratumPlanner], program: &Program) -> Selection {
        let graph = initialize(plan, program).unwrap();
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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[0];
        for (node, order) in &mut chosen.layouts {
            if matches!(node, PhysicalArrangement::Arrangement { .. }) {
                assert_eq!((order.key.len(), order.value.len()), (2, 2));
                order.key.reverse();
                order.value.reverse();
            }
        }
        let (id, direction) = chosen.joins.first_key_value().unwrap();
        let TransformationId::Rule {
            transformation_index: join_index,
            ..
        } = *id
        else {
            panic!("join choices identify rule transformations");
        };
        assert_eq!(*direction, JoinOrientation::Original);
        *chosen.joins.values_mut().next().unwrap() = JoinOrientation::Swapped;
        let join = &plan.non_recursive_transformations()[join_index];
        let (left, right) = join.binary_input();
        let old_left = left.fingerprint();
        let old_right = right.fingerprint();
        let old_form = join.output().canonical().clone();

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[0];
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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[0];
        let before = plan.to_string();
        let order = chosen
            .layouts
            .iter_mut()
            .find_map(|(node, order)| {
                matches!(node, PhysicalArrangement::Arrangement { .. }).then_some(order)
            })
            .unwrap();
        *order = Arities { key, value };

        let error = apply(&mut strata, &program, chosen).unwrap_err();

        assert!(matches!(&error, PlanError::Internal(_)));
        assert!(error.to_string().contains("does not permute the blocks"));
        assert_eq!(strata[0].to_string(), before);
    }

    #[test]
    fn incompatible_join_key_orders_are_rejected() {
        let (program, mut strata) = planned(JOIN);
        let mut chosen = selection(&strata, &program);
        let order = chosen
            .layouts
            .iter_mut()
            .find_map(|(node, order)| {
                matches!(node, PhysicalArrangement::Arrangement { .. }).then_some(order)
            })
            .unwrap();
        order.key.reverse();

        let error = apply(&mut strata, &program, chosen).unwrap_err();

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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[0];
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
        let node = PhysicalArrangement::Arrangement {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            transformation_index: index,
        };
        chosen.layouts.get_mut(&node).unwrap().value.reverse();

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[0];
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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[0];
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

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[0];
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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[1];
        let relation = compute_fp("reach");
        let binding = &plan.relation_bindings()[&relation];
        let inputs: Vec<u64> = binding.input_fingerprints().collect();
        let projection_outputs: Vec<u64> = binding
            .inputs()
            .iter()
            .map(|projection| projection.output().fingerprint())
            .collect();
        let seed = binding.seed().unwrap().unary_input().fingerprint();
        let seed_output = binding.seed().unwrap().output().fingerprint();
        let feedback = plan.recursion_feedback_collections().to_vec();
        let node = PhysicalArrangement::Dedup {
            stratum_index: 1,
            scope: Scope::Recursive,
            relation_fingerprint: relation,
        };
        chosen.layouts.get_mut(&node).unwrap().key = vec![1, 0];

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[1];
        let binding = &plan.relation_bindings()[&relation];
        assert_eq!(
            binding.operation(),
            &BindingOperation::Dedup { order: vec![1, 0] }
        );
        assert_eq!(binding.input_fingerprints().collect::<Vec<_>>(), inputs);
        assert_eq!(
            binding
                .inputs()
                .iter()
                .map(|projection| projection.output().fingerprint())
                .collect::<Vec<_>>(),
            projection_outputs
        );
        let seed_projection = binding.seed().unwrap();
        assert_eq!(seed_projection.unary_input().fingerprint(), seed);
        assert_eq!(seed_projection.output().fingerprint(), seed_output);
        assert_eq!(
            output_values(seed_projection.flow(), &[10, 20], &[], 0),
            [20, 10]
        );
        for projection in binding.inputs() {
            assert_eq!(
                output_values(projection.flow(), &[10, 20], &[], 0),
                [20, 10]
            );
        }
        assert_eq!(plan.recursion_feedback_collections(), feedback);
        assert_eq!(seed, relation);
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
        let mut chosen = selection(&strata, &program);
        let plan = &strata[0];
        let candidate = plan
            .non_recursive_transformations()
            .iter()
            .enumerate()
            .find(|(_, step)| {
                step.need_arrange()
                    && step.output().arity() == (1, 2)
                    && strata[1]
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
        let consumer = strata[1]
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
        let mut unchanged_orders = chosen.layouts.clone();
        let node = PhysicalArrangement::Arrangement {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            transformation_index: index,
        };
        chosen.layouts.get_mut(&node).unwrap().value = vec![2, 1];

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[0];
        let consumer = strata[1]
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
        let mut rewritten_orders = selection(&strata, &program).layouts;
        unchanged_orders.remove(&node);
        rewritten_orders.remove(&node);
        assert_eq!(rewritten_orders, unchanged_orders);
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
        let mut chosen = selection(&strata, &program);
        let node = PhysicalArrangement::Aggregation {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            relation_fingerprint: compute_fp("r"),
        };
        let order = chosen.layouts.get_mut(&node).unwrap();
        assert_eq!(order.key, expected);
        assert!(order.value.is_empty());
        order.key.reverse();

        apply(&mut strata, &program, chosen).unwrap();

        let plan = &strata[0];
        assert_eq!(
            plan.relation_bindings()[&compute_fp("r")].operation(),
            &BindingOperation::Aggregation {
                order: selected.to_vec()
            }
        );
    }

    #[test]
    fn dedup_reordering_composes_every_rule_and_seed_projection() {
        let (program, mut plan) = planned(
            r#"
            .decl A(a: int32, b: symbol, c: int64)
            .input A
            .decl B(a: int32, b: symbol, c: int64)
            .input B
            .decl R(c: int64, a: int32, b: symbol)
            .input R
            .output R
            R(c,a,b) :- A(a,b,c).
            R(c,a,b) :- B(a,b,c).
            "#,
        );
        let relation_fingerprint = compute_fp("r");
        let original_inputs: Vec<u64> = plan[0].relation_bindings()[&relation_fingerprint]
            .input_fingerprints()
            .collect();
        let original_steps = plan[0].non_recursive_transformations().to_vec();
        let node = PhysicalArrangement::Dedup {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            relation_fingerprint,
        };
        let mut chosen = selection(&plan, &program);
        chosen.layouts.get_mut(&node).unwrap().key = vec![2, 0, 1];
        apply(&mut plan, &program, chosen).unwrap();

        let binding = &plan[0].relation_bindings()[&relation_fingerprint];
        let projections = binding.inputs().iter().chain(binding.seed());
        for projection in projections {
            assert_eq!(
                output_values(projection.flow(), &[10, 20, 30], &[], 0),
                [30, 10, 20]
            );
        }

        let mut chosen = selection(&plan, &program);
        chosen.layouts.get_mut(&node).unwrap().key = vec![1, 2, 0];
        apply(&mut plan, &program, chosen).unwrap();

        let binding = &plan[0].relation_bindings()[&relation_fingerprint];
        assert_eq!(binding.inputs().len(), 2);
        assert_eq!(
            binding.input_fingerprints().collect::<Vec<_>>(),
            original_inputs
        );
        assert_eq!(plan[0].non_recursive_transformations(), original_steps);
        let projections = binding.inputs().iter().chain(binding.seed());
        for projection in projections {
            assert_eq!(
                output_values(projection.flow(), &[10, 20, 30], &[], 0),
                [20, 30, 10]
            );
            let expected = Transformation::row_projection(
                Arc::clone(projection.unary_input()),
                projection.output().fingerprint(),
                &[1, 2, 0],
            )
            .unwrap();
            assert_eq!(projection, &expected);
        }
    }

    // Cases: input mutability, declared aggregate.
    #[rstest]
    #[case(" append", "min")]
    #[case(" mutable", "min")]
    #[case(" append", "max")]
    #[case(" mutable", "max")]
    fn signed_aggregation_reordering_preserves_the_value_column_and_input_rows(
        #[case] input_mutability_suffix: &str,
        #[case] aggregate: &str,
    ) {
        let (program, mut plan) = planned(&format!(
            ".decl E(a:int32,b:int32,c:int32){input_mutability_suffix}\n.input E\n\
             .decl R(a:int32,b:{aggregate}<int32>,c:int32)\n.output R\n\
             R(a,b,c) :- E(a,b,c).\n",
        ));
        let node = PhysicalArrangement::Aggregation {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            relation_fingerprint: compute_fp("r"),
        };
        let mut chosen = selection(&plan, &program);
        assert_eq!(
            chosen.layouts[&node],
            Arities {
                key: vec![0, 2],
                value: vec![1]
            }
        );
        chosen.layouts.get_mut(&node).unwrap().key = vec![2, 0];

        apply(&mut plan, &program, chosen).unwrap();

        let binding = &plan[0].relation_bindings()[&compute_fp("r")];
        assert_eq!(
            binding.operation(),
            &BindingOperation::Aggregation { order: vec![2, 0] }
        );
        assert_eq!(
            output_values(binding.inputs()[0].flow(), &[10, 20, 30], &[], 0),
            [10, 20, 30]
        );
        assert_eq!(
            selection(&plan, &program).layouts[&node],
            Arities {
                key: vec![2, 0],
                value: vec![1]
            }
        );
    }

    #[test]
    fn recursive_aggregation_reordering_keeps_the_outer_declaration_order() {
        let (program, mut plan) = planned(
            r#"
            .decl Seed(a: int32, b: int32, c: int32)
            .input Seed
            .decl Edge(src: int32, dst: int32, w: int32)
            .input Edge
            .decl R(a: int32, b: min<int32>, c: int32)
            .decl Out(a: int32, b: int32, c: int32)
            .output Out
            R(a,b,c) :- Seed(a,b,c).
            R(to,b+w,c) :- R(from,b,c), Edge(from,to,w).
            Out(a,b,c) :- R(a,b,c).
            "#,
        );
        let relation_fingerprint = compute_fp("r");
        let inner = PhysicalArrangement::Aggregation {
            stratum_index: 1,
            scope: Scope::Recursive,
            relation_fingerprint,
        };
        let exit = PhysicalArrangement::AggregationExit {
            stratum_index: 1,
            relation_fingerprint,
        };
        let mut chosen = selection(&plan, &program);
        chosen.layouts.get_mut(&inner).unwrap().key = vec![2, 0];

        apply(&mut plan, &program, chosen).unwrap();

        let graph = initialize(&plan, &program).unwrap();
        assert_eq!(
            graph.nodes[&inner],
            Arities {
                key: vec![2, 0],
                value: vec![]
            }
        );
        assert_eq!(
            graph.nodes[&exit],
            Arities {
                key: vec![0, 2],
                value: vec![]
            }
        );
        let out = &plan[2].relation_bindings()[&compute_fp("out")];
        let out_edge = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum_index: 2,
                        scope: Scope::NonRecursive,
                        relation_fingerprint: compute_fp("out"),
                    }
            })
            .unwrap();
        assert!(matches!(&out_edge.input, Producer::Union(parts)
            if matches!(parts.as_slice(), [Producer::Arrangement(node)] if node == &exit)));
        assert_eq!(
            output_values(out.inputs()[0].flow(), &[10, 20, 30], &[], 0),
            [10, 20, 30]
        );
    }
}
