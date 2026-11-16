//! Applies a layout assignment to existing plan nodes.
//!
//! `validation` checks arrangement choices and shared binding inputs.
//! `transformation_rewrite` updates operator outputs and metadata, delegating
//! expression trees to `expression_rewrite`. `binding_rewrite` aligns binding
//! inputs and seeds; both use `upstream_rewrite` to reconnect consumers.

mod binding_rewrite;
mod expression_rewrite;
mod transformation_rewrite;
mod upstream_rewrite;
mod validation;

use std::collections::HashMap;

use flowlog_parser::Program;

use self::upstream_rewrite::RewrittenCollection;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::ArrangementOrigin;
use super::arrangement_graph::AttributeLayout;
use super::arrangement_graph::JoinOrientation;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationLocation;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

/// Layout choices addressed by their locations in the original plan.
/// Graph feed borrows are released before these choices modify that plan.
#[derive(Debug)]
pub(super) struct LayoutAssignment {
    /// Physical key/value column orders for sorting boundaries. Row feeds do
    /// not choose independently: they must emit their binding's input order.
    arrangement_layouts: HashMap<ArrangementOrigin, AttributeLayout>,
    /// Left/right choices for ordinary joins; other operators keep their sides.
    join_orientations: HashMap<TransformationLocation, JoinOrientation>,
}

impl LayoutAssignment {
    /// Detaches choices from the graph, releasing its original-plan borrows.
    #[must_use]
    pub(super) fn from_graph(graph: ArrangementGraph<'_>) -> Self {
        Self {
            arrangement_layouts: graph
                .arrangements()
                .iter()
                .map(|node| (node.origin(), node.layout().clone()))
                .collect(),
            join_orientations: graph
                .feeds()
                .iter()
                .filter_map(|feed| {
                    feed.join_orientation()
                        .map(|orientation| (feed.location(), orientation))
                })
                .collect(),
        }
    }
}

/// Preserves plan identities and dependencies while applying column orders.
/// Shared binding-input requirements are validated before any node is changed.
/// Returns an internal error for incompatible choices or unresolved producers.
pub(super) fn apply(
    plan: &mut [StratumPlanner],
    program: &Program,
    assignment: LayoutAssignment,
) -> Result<(), PlanError> {
    let input_layouts = validation::assignment(&assignment, plan, program)?;
    let output_rewrites = prepare_output_rewrites(plan, &assignment, &input_layouts.feed_layouts)?;
    let mut outer_outputs: HashMap<u64, RewrittenCollection> = HashMap::new();
    let strata = plan.iter_mut().enumerate();
    for (stratum_index, stratum) in strata {
        let mut recursive_outputs = HashMap::new();
        for scope in [Scope::NonRecursive, Scope::Recursive] {
            let (transformations, available_outputs): (
                &mut [Transformation],
                &mut HashMap<u64, RewrittenCollection>,
            ) = match scope {
                Scope::NonRecursive => (
                    stratum.non_recursive_transformations_mut(),
                    &mut outer_outputs,
                ),
                Scope::Recursive => {
                    // A loop inherits outer producers, but its own versions
                    // must not escape and replace later outer-scope reads.
                    recursive_outputs = outer_outputs.clone();
                    (
                        stratum.recursive_transformations_mut(),
                        &mut recursive_outputs,
                    )
                }
            };
            let indexed_transformations = transformations.iter_mut().enumerate();
            for (transformation_index, transformation) in indexed_transformations {
                let location =
                    TransformationLocation::new(stratum_index, scope, transformation_index);
                let join_orientation = assignment
                    .join_orientations
                    .get(&location)
                    .copied()
                    .unwrap_or(JoinOrientation::Original);
                transformation_rewrite::rewrite(
                    transformation,
                    &output_rewrites[&location],
                    join_orientation,
                    available_outputs,
                )?;
                available_outputs.insert(
                    transformation.output().fingerprint(),
                    output_rewrites[&location].clone(),
                );
            }
        }
        let (scope, rule_outputs) = if stratum.is_recursive() {
            (Scope::Recursive, &recursive_outputs)
        } else {
            (Scope::NonRecursive, &outer_outputs)
        };
        let relations: Vec<u64> = stratum.output_relations().collect();
        for relation_fingerprint in relations {
            let binding = stratum
                .relation_binding_mut(relation_fingerprint)
                .expect("relation comes from this stratum's bindings");
            let arrangement_origin = ArrangementOrigin::from_binding(
                relation_fingerprint,
                binding.operation(),
                stratum_index,
                scope,
            );
            let Some(selected_layout) = assignment.arrangement_layouts.get(&arrangement_origin)
            else {
                continue;
            };
            if let Some(seed_projection) = binding_rewrite::rewrite(
                binding,
                selected_layout,
                rule_outputs,
                &outer_outputs,
                input_layouts
                    .seed_orders
                    .get(&arrangement_origin)
                    .map(Vec::as_slice),
                stratum_index,
                relation_fingerprint,
            )? {
                // The conversion reads the earlier binding before feedback
                // shadows it, so it runs outside this loop.
                if stratum.is_recursive() {
                    stratum
                        .recursion_enter_collections_mut()
                        .push(seed_projection.output().fingerprint());
                }
                stratum
                    .non_recursive_transformations_mut()
                    .push(seed_projection);
            }
        }
        let replaced_seeds: Vec<u64> = stratum
            .recursion_feedback_collections()
            .iter()
            .copied()
            .filter(|fp| {
                !stratum
                    .relation_bindings()
                    .values()
                    .any(|binding| binding.seed().is_some_and(|seed| seed.fingerprint() == *fp))
            })
            .collect();
        stratum
            .recursion_enter_collections_mut()
            .retain(|fp| !replaced_seeds.contains(fp));
    }
    Ok(())
}

/// Prepares output metadata independently of consumer traversal order.
/// Locations keep repeated fingerprints in separate scopes independent.
/// Returns an internal error for an invalid output-column permutation.
fn prepare_output_rewrites(
    plan: &[StratumPlanner],
    assignment: &LayoutAssignment,
    binding_feed_layouts: &HashMap<TransformationLocation, AttributeLayout>,
) -> Result<HashMap<TransformationLocation, RewrittenCollection>, PlanError> {
    let mut output_rewrites = HashMap::new();
    let strata = plan.iter().enumerate();
    for (stratum_index, stratum) in strata {
        let scopes = [
            (Scope::NonRecursive, stratum.non_recursive_transformations()),
            (Scope::Recursive, stratum.recursive_transformations()),
        ];
        for (scope, transformations) in scopes {
            let indexed_transformations = transformations.iter().enumerate();
            for (transformation_index, transformation) in indexed_transformations {
                let location =
                    TransformationLocation::new(stratum_index, scope, transformation_index);
                let (key_arity, value_arity) = transformation.output().arity();
                let output_order = assignment
                    .arrangement_layouts
                    .get(&ArrangementOrigin::Transformation(location))
                    .or_else(|| binding_feed_layouts.get(&location))
                    .cloned()
                    .unwrap_or_else(|| AttributeLayout::new(key_arity, value_arity));
                validation::output_layout(transformation, &output_order)?;
                output_rewrites.insert(
                    location,
                    transformation_rewrite::prepare_output(transformation.output(), output_order)?,
                );
            }
        }
    }
    Ok(output_rewrites)
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
    use crate::planner::Transformation;
    use crate::planner::TransformationArgument;
    use crate::planner::TransformationFlow;
    use crate::planner::arity_order::arrangement_graph::Scope;
    use crate::planner::arity_order::initialization::build_graph;
    use crate::stratifier::Stratifier;

    /// Builds editable strata through the normal sharing and seed resolution
    /// path, before applying any test-selected physical order.
    #[must_use]
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

    /// Reads a projection/join's plain output slots from supplied physical
    /// tuples. The expected rows below are logical query answers, independent
    /// of how the rewriter represents a permutation.
    #[must_use]
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

    #[must_use]
    fn chosen(plan: &[StratumPlanner], program: &Program) -> LayoutAssignment {
        LayoutAssignment::from_graph(build_graph(plan, program).unwrap())
    }

    #[must_use]
    fn boundary(index: usize) -> ArrangementOrigin {
        ArrangementOrigin::Transformation(TransformationLocation::new(
            0,
            Scope::NonRecursive,
            index,
        ))
    }

    const JOIN: &str = r#"
        .decl R(k0:int32,k1:int32,a:int32,b:int32)
        .input R
        .decl S(k0:int32,k1:int32,c:int32,d:int32)
        .input S
        .decl Out(a:int32,b:int32,c:int32,d:int32)
        .output Out
        Out(a,b,c,d) :- R(k0,k1,a,b), S(k0,k1,c,d).
    "#;

    #[test]
    fn rewriting_join_slots_preserves_identity_after_reordering_and_side_swap() {
        let (program, mut plan) = planned(JOIN);
        let fingerprints: Vec<u64> = plan[0]
            .non_recursive_transformations()
            .iter()
            .map(|step| step.output().fingerprint())
            .collect();
        let mut assignment = chosen(&plan, &program);
        for (origin, layout) in &mut assignment.arrangement_layouts {
            if matches!(origin, ArrangementOrigin::Transformation(_)) {
                layout.key_mut().reverse();
                layout.value_mut().reverse();
            }
        }
        *assignment.join_orientations.values_mut().next().unwrap() = JoinOrientation::Swapped;
        apply(&mut plan, &program, assignment).unwrap();
        let rewritten_outputs: Vec<u64> = plan[0]
            .non_recursive_transformations()
            .iter()
            .map(|step| step.output().fingerprint())
            .collect();
        assert_eq!(rewritten_outputs, fingerprints);
        let join = plan[0]
            .non_recursive_transformations()
            .iter()
            .find(|step| matches!(step, Transformation::JnToRow { .. }))
            .unwrap();
        assert!(
            join.binary_input()
                .0
                .canonical()
                .to_string()
                .contains("atoms(s)")
        );
        assert_eq!(
            output_values(join.flow(), &[2, 1, 21, 20], &[2, 1, 11, 10], 2),
            [10, 11, 20, 21]
        );
    }

    #[test]
    fn binding_inputs_align_arithmetic_and_every_union_feed_with_one_seed_conversion() {
        let (program, mut plan) = planned(
            r#"
            .decl E(c:int32,a:int32,b:int32)
            .input E
            .decl B(a:int32,b:int32,c:int32)
            .input B
            .decl R(a:int32,b:int32,c:int32)
            .input R
            .output R
            R(a+1,b,c) :- E(c,a,b).
            R(a,b,c) :- B(a,b,c).
        "#,
        );
        let count = plan[0].non_recursive_transformations().len();
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
            })
            .unwrap()
            .key_mut()
            .copy_from_slice(&[2, 0, 1]);
        apply(&mut plan, &program, assignment).unwrap();
        let binding = &plan[0].relation_bindings()[&compute_fp("r")];
        assert_eq!(
            binding.operation(),
            &BindingOperation::Dedup {
                order: vec![2, 0, 1]
            }
        );
        let steps = plan[0].non_recursive_transformations();
        assert_eq!(steps.len(), count + 1);
        let arithmetic = steps
            .iter()
            .find(|step| step.unary_input().fingerprint() == compute_fp("e"))
            .unwrap();
        assert_eq!(arithmetic.flow().to_string(), "V:(V0, V1 + 1, V2)");
        for input in binding.inputs() {
            let producer = steps
                .iter()
                .find(|step| step.output().fingerprint() == input.fingerprint())
                .unwrap();
            assert!(Arc::ptr_eq(input, producer.output()));
        }
        let seed = steps
            .iter()
            .find(|step| step.output().fingerprint() == binding.seed().unwrap().fingerprint())
            .unwrap();
        assert_eq!(seed.unary_input().fingerprint(), compute_fp("r"));
        assert_eq!(seed.flow().to_string(), "V:(V2, V0, V1)");
        assert_eq!(
            seed.output().canonical().to_string(),
            "atoms(r) not() eq() where() key() value(0.2, 0.0, 0.1)",
        );
    }

    #[test]
    fn a_rewritten_shared_projection_keeps_only_its_retained_columns() {
        let (program, mut plan) = planned(
            r#"
            .decl R(k:int32,a:int32,b:int32)
            .input R
            .decl S(k:int32,c:int32)
            .input S
            .decl T(c:int32,z:int32)
            .input T
            .decl U(c:int32,w:int32)
            .input U
            .decl Wide(a:int32,b:int32,z:int32)
            .decl Narrow(a:int32,w:int32)
            .output Wide
            .output Narrow
            Wide(a,b,z) :- R(k,a,b), S(k,c), T(c,z).
            Narrow(a,w) :- R(k,a,_), S(k,c), U(c,w).
        "#,
        );
        let steps = plan[0].non_recursive_transformations();
        let (index, producer) = steps
            .iter()
            .enumerate()
            .find(|(_, step)| {
                step.need_arrange()
                    && step.output().arity() == (1, 2)
                    && steps.iter().any(|reader| {
                        reader.is_map()
                            && reader.unary_input().fingerprint() == step.output().fingerprint()
                    })
            })
            .unwrap();
        let consumer = steps
            .iter()
            .position(|step| {
                step.is_map() && step.unary_input().fingerprint() == producer.output().fingerprint()
            })
            .unwrap();
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&boundary(index))
            .unwrap()
            .value_mut()
            .reverse();
        apply(&mut plan, &program, assignment).unwrap();
        let consumer = &plan[0].non_recursive_transformations()[consumer];
        assert_eq!(consumer.output().arity(), (1, 1));
        assert_eq!(
            output_values(consumer.flow(), &[10, 30, 20], &[], 1),
            [10, 20]
        );
    }

    #[test]
    fn aggregate_target_reads_matches_while_its_output_reads_the_answer() {
        let (program, mut plan) = planned(
            r#"
            .decl Node(a:int32,b:int32)
            .input Node
            .decl Edge(a:int32,b:int32,v:int32,w:int32)
            .input Edge
            .decl Out(a:int32,b:int32,s:int32)
            .output Out
            Out(a,b,s) :- Node(a,b), s = sum v : { Edge(a,b,v,w) }.
        "#,
        );
        let mut assignment = chosen(&plan, &program);
        for (origin, layout) in &mut assignment.arrangement_layouts {
            if matches!(origin, ArrangementOrigin::Transformation(_)) {
                layout.key_mut().reverse();
                layout.value_mut().reverse();
            }
        }
        apply(&mut plan, &program, assignment).unwrap();
        let aggregate = plan[0]
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

    // Cases: upstream order, downstream order, expected conversion flow.
    #[rstest]
    #[case::both_original(vec![0,1,2], vec![0,1,2], None)]
    #[case::only_downstream_reordered(
        vec![0,1,2], vec![2,0,1], Some("V:(V2, V0, V1)")
    )]
    #[case::only_upstream_reordered(
        vec![2,0,1], vec![0,1,2], Some("V:(V0, V1, V2)")
    )]
    #[case::both_reordered(vec![2,0,1], vec![2,0,1], None)]
    fn relation_seeds_convert_only_when_physical_orders_differ(
        #[case] upstream_order: Vec<usize>,
        #[case] downstream_order: Vec<usize>,
        #[case] expected_flow: Option<&str>,
    ) {
        let (program, mut plan) = planned(
            r#"
            .decl Arc(x:int32,y:int32,z:int32)
            .input Arc
            .decl Reach(x:int32,y:int32,z:int32)
            .output Reach
            Reach(x,y,z) :- Arc(x,y,z).
            Reach(x,y,z) :- Reach(y,x,z).
        "#,
        );
        let relation = compute_fp("reach");
        let mut assignment = chosen(&plan, &program);
        for (stratum_index, scope, order) in [
            (0, Scope::NonRecursive, &upstream_order),
            (1, Scope::Recursive, &downstream_order),
        ] {
            assignment
                .arrangement_layouts
                .get_mut(&ArrangementOrigin::Dedup {
                    stratum_index,
                    scope,
                    relation_fingerprint: relation,
                })
                .unwrap()
                .key_mut()
                .copy_from_slice(order);
        }
        let prelude_length = plan[1].non_recursive_transformations().len();
        apply(&mut plan, &program, assignment).unwrap();
        let seed = plan[1].relation_bindings()[&relation].seed().unwrap();
        if let Some(expected_flow) = expected_flow {
            assert_eq!(
                plan[1].non_recursive_transformations().len(),
                prelude_length + 1
            );
            let conversion = plan[1].non_recursive_transformations().last().unwrap();
            assert_eq!(conversion.output().fingerprint(), seed.fingerprint());
            assert_eq!(conversion.unary_input().fingerprint(), relation);
            assert_eq!(conversion.flow().to_string(), expected_flow);
        } else {
            assert_eq!(seed.fingerprint(), relation);
            assert_eq!(
                plan[1].non_recursive_transformations().len(),
                prelude_length
            );
        }
        assert_eq!(plan[1].recursion_enter_collections(), [seed.fingerprint()]);
    }

    #[test]
    fn recursive_dedup_rewriting_keeps_seed_outside_feedback() {
        let (program, mut plan) = planned(
            r#"
            .decl Arc(x:int32,y:int32)
            .input Arc
            .decl Reach(x:int32,y:int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
        "#,
        );
        let outer_input = *plan[1]
            .recursion_enter_collections()
            .iter()
            .find(|&&fp| fp != compute_fp("reach"))
            .unwrap();
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&ArrangementOrigin::Dedup {
                stratum_index: 1,
                scope: Scope::Recursive,
                relation_fingerprint: compute_fp("reach"),
            })
            .unwrap()
            .key_mut()
            .reverse();
        apply(&mut plan, &program, assignment).unwrap();
        let binding = &plan[1].relation_bindings()[&compute_fp("reach")];
        let seed = binding.seed().unwrap().fingerprint();
        let conversion = plan[1]
            .non_recursive_transformations()
            .iter()
            .find(|step| step.output().fingerprint() == seed)
            .unwrap();
        assert_eq!(conversion.unary_input().fingerprint(), compute_fp("reach"));
        assert_eq!(conversion.flow().to_string(), "V:(V1, V0)");
        assert_eq!(plan[1].recursion_enter_collections(), [outer_input, seed]);
        assert!(
            plan[1]
                .recursive_transformations()
                .iter()
                .any(|step| step.input_fingerprints() == [compute_fp("reach")])
        );
        assert_ne!(binding.inputs()[0].fingerprint(), seed);
    }

    // Cases: malformed key fields, malformed value fields.
    #[rstest]
    #[case::duplicate(vec![0,0], vec![2,3])]
    #[case::cross_block(vec![0,2], vec![1,3])]
    #[case::missing(vec![0,1], vec![2])]
    fn invalid_layouts_are_rejected_before_rewriting(
        #[case] key: Vec<usize>,
        #[case] value: Vec<usize>,
    ) {
        let (program, mut plan) = planned(JOIN);
        let before = plan[0].non_recursive_transformations().to_vec();
        let mut assignment = chosen(&plan, &program);
        let layout = assignment
            .arrangement_layouts
            .iter_mut()
            .find(|(origin, _)| matches!(origin, ArrangementOrigin::Transformation(_)))
            .unwrap()
            .1;
        *layout = AttributeLayout::from_columns(key, value);
        let error = apply(&mut plan, &program, assignment).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("does not permute the blocks"));
        assert_eq!(plan[0].non_recursive_transformations(), before);
    }

    #[test]
    fn equal_fingerprints_keep_independent_recursive_and_outer_layouts() {
        let (program, mut plan) = planned(
            r#"
            .decl subset(x:int32,y:int32,z:int32)
            .input subset
            .decl loan_issued_at(x:int32,y:int32,z:int32)
            .input loan_issued_at
            .decl origin_live_on_entry(x:int32,y:int32)
            .input origin_live_on_entry
            .decl origin_contains_loan_on_entry(x:int32,y:int32,z:int32)
            .decl loan_live_at(x:int32,y:int32)
            .output loan_live_at
            origin_contains_loan_on_entry(origin,loan,point) :-
                loan_issued_at(loan,origin,point).
            origin_contains_loan_on_entry(origin2,loan,point) :-
                origin_contains_loan_on_entry(origin1,loan,point),
                subset(origin1,origin2,point).
            loan_live_at(loan,point) :-
                origin_contains_loan_on_entry(origin,loan,point),
                origin_live_on_entry(origin,point).
        "#,
        );
        let relation = compute_fp("origin_contains_loan_on_entry");
        let recursive_stratum = plan.iter().position(StratumPlanner::is_recursive).unwrap();
        let (recursive_index, recursive_producer) = plan[recursive_stratum]
            .recursive_transformations()
            .iter()
            .enumerate()
            .find(|(_, step)| {
                matches!(step, Transformation::RowToKv { .. })
                    && step.unary_input().fingerprint() == relation
                    && step.output().arity() == (2, 1)
            })
            .unwrap();
        let shared_fingerprint = recursive_producer.output().fingerprint();
        let (outer_stratum, outer_index) = plan
            .iter()
            .enumerate()
            .skip(recursive_stratum + 1)
            .find_map(|(stratum_index, stratum)| {
                stratum
                    .non_recursive_transformations()
                    .iter()
                    .position(|step| step.output().fingerprint() == shared_fingerprint)
                    .map(|index| (stratum_index, index))
            })
            .unwrap();
        let mut assignment = chosen(&plan, &program);
        for (origin, layout) in &mut assignment.arrangement_layouts {
            if let ArrangementOrigin::Transformation(location) = origin
                && location.stratum_index() == recursive_stratum
                && layout.key().len() == 2
            {
                layout.key_mut().reverse();
            }
        }

        apply(&mut plan, &program, assignment).unwrap();

        let recursive = &plan[recursive_stratum].recursive_transformations()[recursive_index];
        let outer = &plan[outer_stratum].non_recursive_transformations()[outer_index];
        assert_eq!(recursive.output().fingerprint(), shared_fingerprint);
        assert_eq!(outer.output().fingerprint(), shared_fingerprint);
        assert_eq!(recursive.flow().to_string(), "K:(V2, V0), V:(V1)");
        assert_eq!(outer.flow().to_string(), "K:(V0, V2), V:(V1)");
        let recursive_join = plan[recursive_stratum]
            .recursive_transformations()
            .iter()
            .find(|step| {
                !step.is_unary() && step.input_fingerprints().contains(&shared_fingerprint)
            })
            .unwrap();
        let outer_join = plan[outer_stratum]
            .non_recursive_transformations()
            .iter()
            .find(|step| {
                !step.is_unary() && step.input_fingerprints().contains(&shared_fingerprint)
            })
            .unwrap();
        for (consumer, producer) in [(recursive_join, recursive), (outer_join, outer)] {
            let (left, right) = consumer.binary_input();
            let input = if left.fingerprint() == shared_fingerprint {
                left
            } else {
                right
            };
            assert!(Arc::ptr_eq(input, producer.output()));
        }
        assert!(!Arc::ptr_eq(recursive.output(), outer.output()));
        assert_eq!(
            output_values(recursive_join.flow(), &[30, 10, 20], &[30, 10, 40], 2),
            [40, 20, 30]
        );
        assert_eq!(
            output_values(outer_join.flow(), &[40, 30], &[40, 30, 20], 2),
            [20, 30]
        );
    }

    #[test]
    fn shared_heads_cannot_request_conflicting_physical_rows() {
        let (program, mut plan) = planned(
            r#"
            .decl A(a:int32,b:int32)
            .input A
            .decl R(a:int32,b:int32)
            .output R
            .decl S(a:int32,b:int32)
            .output S
            R(a,b) :- A(a,b).
            S(a,b) :- A(a,b).
        "#,
        );
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
            })
            .unwrap()
            .key_mut()
            .reverse();
        let error = apply(&mut plan, &program, assignment).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("incompatible binding layouts"));
    }

    // Cases: input mutability, static exit fold.
    #[rstest]
    #[case::static_inputs("", true)]
    #[case::append_inputs(" append", false)]
    #[case::mutable_inputs(" mutable", false)]
    fn declared_aggregation_reorders_grouping_keys_and_keeps_logical_readers(
        #[case] mutability: &str,
        #[case] has_exit: bool,
    ) {
        let (program, mut plan) = planned(&format!(
            r#"
            .decl Seed(a:int32,b:int32,c:int32){mutability}
            .input Seed
            .decl Edge(src:int32,dst:int32,w:int32){mutability}
            .input Edge
            .decl R(a:int32,b:min<int32>,c:int32)
            R(a,b,c) :- Seed(a,b,c).
            R(to,b+w,c) :- R(from,b,c), Edge(from,to,w).
            .decl Out(a:int32,b:int32,c:int32)
            .output Out
            Out(a,b,c) :- R(a,b,c).
        "#
        ));
        let before: Vec<Transformation> = plan[2].non_recursive_transformations().to_vec();
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&ArrangementOrigin::Aggregation {
                stratum_index: 1,
                scope: Scope::Recursive,
                relation_fingerprint: compute_fp("r"),
            })
            .unwrap()
            .key_mut()
            .reverse();
        let exit = assignment
            .arrangement_layouts
            .get(&ArrangementOrigin::AggregationExit {
                stratum_index: 1,
                relation_fingerprint: compute_fp("r"),
            });
        assert_eq!(exit.is_some(), has_exit);
        if let Some(exit) = exit {
            assert_eq!(exit.key(), [0, 2]);
        }
        apply(&mut plan, &program, assignment).unwrap();
        let binding = &plan[1].relation_bindings()[&compute_fp("r")];
        assert_eq!(
            binding.operation(),
            &BindingOperation::Aggregation { order: vec![2, 0] }
        );
        assert_eq!(binding.seed().unwrap().fingerprint(), compute_fp("r"));
        let after = plan[2].non_recursive_transformations();
        assert_eq!(after.len(), before.len());
        for (old, new) in before.iter().zip(after) {
            assert_eq!(new.output().fingerprint(), old.output().fingerprint());
            assert_eq!(new.input_fingerprints(), old.input_fingerprints());
            assert_eq!(new.flow(), old.flow());
            assert_eq!(new.output().canonical(), old.output().canonical());
        }
    }

    #[test]
    fn join_inputs_must_choose_the_same_key_order() {
        let (program, mut plan) = planned(JOIN);
        let mut assignment = chosen(&plan, &program);
        let first = assignment
            .arrangement_layouts
            .iter_mut()
            .find(|(origin, _)| matches!(origin, ArrangementOrigin::Transformation(_)))
            .unwrap()
            .1;
        first.key_mut().reverse();
        let error = apply(&mut plan, &program, assignment).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("incompatible join key orders"));
    }

    #[test]
    fn unchanged_choices_preserve_flows_and_canonical_forms_across_shared_strata() {
        let (program, mut plan) = planned(
            r#"
            .decl Arc(x:int32,y:int32)
            .input Arc
            .decl Reach(x:int32,y:int32)
            .decl Reverse(x:int32,y:int32)
            .decl Out(x:int32,y:int32)
            .output Out
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            Reverse(y,x) :- Reach(x,y).
            Out(x,y) :- Reverse(x,y), !Arc(x,y).
        "#,
        );
        let before: Vec<Transformation> = plan
            .iter()
            .flat_map(|stratum| {
                stratum
                    .non_recursive_transformations()
                    .iter()
                    .chain(stratum.recursive_transformations())
            })
            .cloned()
            .collect();
        let assignment = chosen(&plan, &program);
        apply(&mut plan, &program, assignment).unwrap();
        let after: Vec<&Transformation> = plan
            .iter()
            .flat_map(|stratum| {
                stratum
                    .non_recursive_transformations()
                    .iter()
                    .chain(stratum.recursive_transformations())
            })
            .collect();
        assert_eq!(after.len(), before.len());
        for (old, new) in before.iter().zip(after) {
            assert_eq!(new.flow(), old.flow());
            assert_eq!(new.output().canonical(), old.output().canonical());
            assert_eq!(new.output().mutability(), old.output().mutability());
        }
    }

    #[test]
    fn unchanged_layouts_reuse_resolved_seeds_and_recursive_boundaries() {
        let (program, mut plan) = planned(
            r#"
            .decl Edge(x:int32,y:int32)
            .input Edge
            .decl R(x:int32,y:int32)
            .input R
            .output R
            R(x,y) :- Edge(x,y).
            R(x,z) :- R(x,y), Edge(y,z).
        "#,
        );
        let relation = compute_fp("r");
        let earlier_seed = plan[1]
            .relation_binding_mut(relation)
            .unwrap()
            .seed_mut()
            .clone()
            .unwrap();
        let assignment = chosen(&plan, &program);
        apply(&mut plan, &program, assignment).unwrap();
        let seed = plan[1].relation_bindings()[&relation].seed().unwrap();
        assert!(std::ptr::eq(seed, earlier_seed.as_ref()));
        assert_eq!(plan[1].recursion_feedback_collections(), [relation]);
        assert_eq!(plan[1].recursion_leave_collections(), [relation]);
    }

    #[test]
    fn an_existing_seed_projection_emits_the_new_binding_order_directly() {
        let (program, mut plan) = planned(
            r#"
            .decl E(c:int32,a:int32,b:int32)
            .input E
            .decl R(a:int32,b:int32,c:int32)
            .input R
            .output R
            R(a,b,c) :- E(c,a,b).
        "#,
        );
        let relation = compute_fp("r");
        let binding_origin = ArrangementOrigin::Dedup {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            relation_fingerprint: relation,
        };
        let mut first_assignment = chosen(&plan, &program);
        first_assignment
            .arrangement_layouts
            .get_mut(&binding_origin)
            .unwrap()
            .key_mut()
            .copy_from_slice(&[1, 2, 0]);
        apply(&mut plan, &program, first_assignment).unwrap();
        assert_eq!(plan[0].non_recursive_transformations().len(), 2);
        let mut assignment = chosen(&plan, &program);
        assignment
            .arrangement_layouts
            .get_mut(&binding_origin)
            .unwrap()
            .key_mut()
            .copy_from_slice(&[2, 0, 1]);
        apply(&mut plan, &program, assignment).unwrap();
        let seed = plan[0].relation_bindings()[&relation].seed().unwrap();
        let steps = plan[0].non_recursive_transformations();
        assert_eq!(
            steps.len(),
            2,
            "the existing seed projection must be reused"
        );
        let conversion = steps
            .iter()
            .find(|step| step.output().fingerprint() == seed.fingerprint())
            .unwrap();
        assert_eq!(conversion.unary_input().fingerprint(), relation);
        assert_eq!(conversion.flow().to_string(), "V:(V2, V0, V1)");
    }
}
