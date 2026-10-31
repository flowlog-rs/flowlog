//! Arrangement provenance across evaluation and recursion scopes.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use flowlog_parser::Program;

use super::arrangement_graph::Arities;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::Edge;
use super::arrangement_graph::JoinOrientation;
use super::arrangement_graph::PhysicalArrangement;
use super::arrangement_graph::Producer;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationId;
use crate::planner::ArithmeticArgument;
use crate::planner::BindingOperation;
use crate::planner::FactorArgument;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

type Bindings<'plan> = BTreeMap<u64, Producer<'plan>>;

// =============================================================================
// Graph initialization
// =============================================================================

/// Preserves arrangement identities across sharing and scope entry, and
/// composes projections within feeds up to materialized layout boundaries.
/// Outputs of `upstream` are external inputs to this graph. `downstream`
/// is ordered from the last stratum to the nearest one.
#[must_use]
pub(super) fn initialize<'plan>(
    stratum: &'plan StratumPlanner,
    upstream: &[StratumPlanner],
    downstream: &'plan [StratumPlanner],
    program: &Program,
) -> ArrangementGraph<'plan> {
    let mut graph = ArrangementGraph::default();
    // A relation can gain rule results after being bound by an earlier
    // stratum. Preserve that binding even if no body reads the relation.
    let earlier_relations = upstream.iter().flat_map(StratumPlanner::output_relations);
    let mut bindings: Bindings<'plan> = program
        .edb_fingerprints()
        .into_iter()
        .chain(earlier_relations)
        .map(|fingerprint| (fingerprint, Producer::External(fingerprint)))
        .collect();
    initialize_stratum(0, stratum, program, &mut bindings, &mut graph);
    let evaluation_order = downstream.iter().rev().enumerate();
    for (index, next) in evaluation_order {
        initialize_stratum(index + 1, next, program, &mut bindings, &mut graph);
    }
    graph
}

/// Adds one stratum's arrangement graph without changing its plan.
/// `bindings` maps available outer collection fingerprints to their
/// producers. It gains prelude results and non-recursive relation results.
/// From a recursive body, only results leaving the loop are published.
///
/// Each relation binding is one dedup or aggregation node fed by its rule
/// results and any earlier seed. Recursive reads reference that binding,
/// forming a graph cycle rather than unrolling iterations. The seed
/// retains its outer producer even when feedback names the same relation.
fn initialize_stratum<'plan>(
    index: usize,
    stratum: &'plan StratumPlanner,
    program: &Program,
    bindings: &mut Bindings<'plan>,
    graph: &mut ArrangementGraph<'plan>,
) {
    // Relation bindings and loop entries can read prelude results, so resolve
    // those producers before replacing any earlier relation binding.
    initialize_transformations(
        index,
        Scope::NonRecursive,
        stratum.non_recursive_transformations(),
        bindings,
        None,
        graph,
    );

    if !stratum.is_recursive() {
        // All results read the incoming bindings and the completed prelude.
        // Publish them together so map iteration cannot change lineage.
        let mut relation_results: Bindings<'plan> = BTreeMap::new();
        for declaration in program.relations() {
            let relation = declaration.fingerprint();
            let Some(binding) = stratum.relation_bindings().get(&relation) else {
                continue;
            };
            let node = PhysicalArrangement::relation_binding(
                relation,
                binding.operation(),
                index,
                Scope::NonRecursive,
            );
            let mut parts: Vec<Producer<'plan>> = binding
                .seed()
                .map(|seed| resolve_producer(seed, bindings, None))
                .into_iter()
                .collect();
            parts.extend(
                binding
                    .inputs()
                    .iter()
                    .map(|&fingerprint| resolve_producer(fingerprint, bindings, None)),
            );
            graph
                .nodes
                .insert(node.clone(), binding_arities(binding.operation()));
            graph.edges.push(Edge {
                input: Producer::Union(parts),
                output: node.clone(),
            });
            relation_results.insert(relation, Producer::Arrangement(node));
        }
        bindings.extend(relation_results);
        return;
    }

    // Codegen omits an unobservable loop but still emits its prelude.
    if stratum.recursion_leave_collections().is_empty() {
        return;
    }

    let entered: Bindings<'plan> = stratum
        .recursion_enter_collections()
        .iter()
        .map(|&fingerprint| (fingerprint, resolve_producer(fingerprint, bindings, None)))
        .collect();
    // Keep the seed bindings separate: feedback must shadow body reads
    // of R without replacing the outer R that feeds the binding's union.
    let mut recursive_bindings = entered.clone();
    let mut relation_results: Bindings<'plan> = BTreeMap::new();
    // Reserve binding identities before the body so its feedback sources
    // can reference nodes whose incoming edges have not been filled yet.
    for declaration in program.relations() {
        let relation = declaration.fingerprint();
        let Some(binding) = stratum.relation_bindings().get(&relation) else {
            continue;
        };
        let node = PhysicalArrangement::relation_binding(
            relation,
            binding.operation(),
            index,
            Scope::Recursive,
        );
        graph
            .nodes
            .insert(node.clone(), binding_arities(binding.operation()));
        relation_results.insert(relation, Producer::Arrangement(node));
    }
    // A recursive feed references the binding arrangement directly; neither
    // entering a scope nor feeding back rows builds another arrangement.
    for &relation in stratum.recursion_feedback_collections() {
        if let Some(binding) = relation_results.get(&relation) {
            recursive_bindings.insert(relation, binding.clone());
        }
    }
    initialize_transformations(
        index,
        Scope::Recursive,
        stratum.recursive_transformations(),
        &mut recursive_bindings,
        Some(bindings),
        graph,
    );
    for declaration in program.relations() {
        let relation = declaration.fingerprint();
        let Some(binding) = stratum.relation_bindings().get(&relation) else {
            continue;
        };
        let node = PhysicalArrangement::relation_binding(
            relation,
            binding.operation(),
            index,
            Scope::Recursive,
        );
        let mut parts: Vec<Producer<'plan>> = binding
            .seed()
            .and_then(|seed| entered.get(&seed))
            .cloned()
            .into_iter()
            .collect();
        parts.extend(binding.inputs().iter().map(|&fingerprint| {
            resolve_producer(fingerprint, &recursive_bindings, Some(bindings))
        }));
        graph.edges.push(Edge {
            input: Producer::Union(parts),
            output: node,
        });
    }
    // Loop-local arrangements must not alias later arrangements of the
    // completed rows, so only results leaving the loop become outer bindings.
    for &relation in stratum.recursion_leave_collections() {
        if let Some(binding) = relation_results.get(&relation) {
            bindings.insert(relation, binding.clone());
        }
    }
}

/// Keeps binding field IDs in logical row coordinates: codegen restores
/// those coordinates after sorting and before feedback or scope exit.
#[must_use]
fn binding_arities(operation: &BindingOperation) -> Arities {
    let (BindingOperation::Dedup { order } | BindingOperation::Aggregation { order }) = operation;
    Arities {
        key: order.clone(),
        value: Vec::new(),
    }
}

/// Publishes each step's producer in dependency order. Keyed outputs become
/// graph nodes; row outputs remain feed expressions with projections composed
/// across row-only steps. Binary sources retain their left-to-right order.
fn initialize_transformations<'plan>(
    stratum: usize,
    scope: Scope,
    transformations: &'plan [Transformation],
    bindings: &mut Bindings<'plan>,
    outer: Option<&Bindings<'plan>>,
    graph: &mut ArrangementGraph<'plan>,
) {
    let steps = transformations.iter().enumerate();
    for (transformation, step) in steps {
        if matches!(
            step,
            Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
        ) {
            graph.joins.insert(
                TransformationId {
                    stratum,
                    scope,
                    transformation,
                },
                JoinOrientation::Original,
            );
        }
        let inputs = step.input_fingerprints();
        let sources: Vec<Producer<'plan>> = inputs
            .into_iter()
            .map(|fingerprint| resolve_producer(fingerprint, bindings, outer))
            .collect();
        let expression = fuse_projections(
            TransformationId {
                stratum,
                scope,
                transformation,
            },
            step,
            sources,
        );
        let output = if step.need_arrange() {
            let (key, value) = step.output().arity();
            let node = PhysicalArrangement::Arrangement {
                stratum,
                scope,
                transformation,
            };
            graph.nodes.insert(node.clone(), Arities::new(key, value));
            graph.edges.push(Edge {
                input: expression,
                output: node.clone(),
            });
            Producer::Arrangement(node)
        } else {
            expression
        };
        bindings.insert(step.output().fingerprint(), output);
    }
}

/// Resolves local bindings before outer bindings. Unresolved collections
/// remain external leaves instead of introducing arrangement nodes.
#[must_use]
fn resolve_producer<'plan>(
    fingerprint: u64,
    bindings: &Bindings<'plan>,
    outer: Option<&Bindings<'plan>>,
) -> Producer<'plan> {
    bindings
        .get(&fingerprint)
        .or_else(|| outer.and_then(|bindings| bindings.get(&fingerprint)))
        .cloned()
        .unwrap_or(Producer::External(fingerprint))
}

// =============================================================================
// Projection fusion
// =============================================================================

/// Collapses pure projection chains within a feed. Physical arrangements,
/// filters and computed expressions stop fusion. Shared feeds stay intact.
#[must_use]
fn fuse_projections<'plan>(
    id: TransformationId,
    step: &'plan Transformation,
    mut sources: Vec<Producer<'plan>>,
) -> Producer<'plan> {
    let mut step = Cow::Borrowed(step);
    while step.is_map() {
        let [
            Producer::Transformation {
                step: previous,
                sources: previous_sources,
                ..
            },
        ] = sources.as_mut_slice()
        else {
            break;
        };
        let (input, row_input) = match previous.as_ref() {
            Transformation::RowToRow { input, .. } => (input, true),
            Transformation::KvToRow { input, .. } => (input, false),
            Transformation::RowToKv { .. }
            | Transformation::KvToKv { .. }
            | Transformation::JnToRow { .. }
            | Transformation::JnToKv { .. }
            | Transformation::NJnToRow { .. }
            | Transformation::NJnToKv { .. }
            | Transformation::AggToRow { .. }
            | Transformation::AggToKv { .. } => break,
        };
        let Some(flow) = compose_projection(step.flow(), previous.flow()) else {
            break;
        };
        let input = Arc::clone(input);
        let output = Arc::clone(step.output());
        let arranged_output = step.need_arrange();
        step = Cow::Owned(match (row_input, arranged_output) {
            (true, false) => Transformation::RowToRow {
                input,
                output,
                flow,
            },
            (true, true) => Transformation::RowToKv {
                input,
                output,
                flow,
            },
            (false, false) => Transformation::KvToRow {
                input,
                output,
                flow,
            },
            (false, true) => Transformation::KvToKv {
                input,
                output,
                flow,
            },
        });
        sources = mem::take(previous_sources);
    }
    Producer::Transformation { id, step, sources }
}

/// Composes two column-only maps over the earlier input. Returns `None` if
/// either map has a predicate or computed expression, if a flow is not a map,
/// or if a projected column has no source position.
#[must_use]
fn compose_projection(
    projection: &TransformationFlow,
    previous: &TransformationFlow,
) -> Option<TransformationFlow> {
    let TransformationFlow::KVToKV {
        key,
        value,
        constraints,
        compares,
    } = projection
    else {
        return None;
    };
    let TransformationFlow::KVToKV {
        key: previous_key,
        value: previous_value,
        constraints: previous_constraints,
        compares: previous_compares,
    } = previous
    else {
        return None;
    };
    if !constraints.is_empty()
        || !compares.is_empty()
        || !previous_constraints.is_empty()
        || !previous_compares.is_empty()
    {
        return None;
    }
    let column_only = |argument: &ArithmeticArgument| {
        argument.rest.is_empty()
            && matches!(
                argument.init,
                FactorArgument::Var(TransformationArgument::KV(_))
            )
    };
    if !previous_key
        .iter()
        .chain(previous_value.iter())
        .all(column_only)
    {
        return None;
    }
    let resolve_column = |argument: &ArithmeticArgument| {
        if !argument.rest.is_empty() {
            return None;
        }
        let FactorArgument::Var(TransformationArgument::KV((is_key, index))) = &argument.init
        else {
            return None;
        };
        let fields = if *is_key {
            previous_key
        } else {
            previous_value
        };
        fields.get(*index).cloned()
    };
    let key: Option<Vec<ArithmeticArgument>> = key.iter().map(resolve_column).collect();
    let value: Option<Vec<ArithmeticArgument>> = value.iter().map(resolve_column).collect();
    Some(TransformationFlow::KVToKV {
        key: Arc::new(key?),
        value: Arc::new(value?),
        constraints: constraints.clone(),
        compares: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::planner::Constraints;
    use crate::planner::ProgramPlanner;

    /// Projection fusion changes graph feeds, not planned transformations,
    /// so refusal to fuse cannot be observed through the returned plan.
    // Cases: predicates, output expression.
    #[rstest]
    #[case::equality_filter(
        Constraints::new(
            vec![(TransformationArgument::KV((false, 0)), Constant::new(DataType::Int32, "1"))],
            vec![],
        ),
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
            rest: vec![],
        }
    )]
    #[case::arithmetic(
        Constraints::new(vec![], vec![]),
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
            rest: vec![(ArithmeticOperator::Plus, FactorArgument::Const(Constant::new(DataType::Int32, "1")))],
        }
    )]
    #[case::constant(
        Constraints::new(vec![], vec![]),
        ArithmeticArgument {
            init: FactorArgument::Const(Constant::new(DataType::Int32, "1")),
            rest: vec![],
        }
    )]
    fn predicates_and_expressions_stop_projection_composition(
        #[case] predicates: Constraints,
        #[case] expression: ArithmeticArgument,
    ) {
        let plain = TransformationFlow::KVToKV {
            key: Arc::new(vec![]),
            value: Arc::new(vec![ArithmeticArgument {
                init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
                rest: vec![],
            }]),
            constraints: Constraints::new(vec![], vec![]),
            compares: vec![],
        };
        let blocked = TransformationFlow::KVToKV {
            key: Arc::new(vec![]),
            value: Arc::new(vec![expression]),
            constraints: predicates,
            compares: vec![],
        };

        assert!(compose_projection(&plain, &blocked).is_none());
        assert!(compose_projection(&blocked, &plain).is_none());
    }

    /// Sharing reads S0's (b,c) stream to build S1's (c) index. Fuse that
    /// path while preserving the (b,c) feed into Mid's dedup.
    #[test]
    fn cross_stratum_projections_compose_without_changing_shared_feeds() {
        let program = program(
            r#"
            .decl R(k: int32, a: int32)
            .input R(IO="file", filename="R.csv")
            .decl S(k: int32, b: int32, c: int32)
            .input S(IO="file", filename="S.csv")
            .decl Wide(a: int32, b: int32, c: int32)
            .decl Mid(b: int32, c: int32)
            .decl Out(c: int32, z: int32)
            .output Wide
            .output Out
            Wide(a,b,c) :- R(k,a), S(k,b,c).
            Mid(b,c) :- R(k,_), S(k,b,c).
            Out(c,z) :- R(k,_), S(k,_,c), Mid(c,z).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).expect("planned");
        let [upstream, downstream] = plan.strata() else {
            panic!("the Mid dependency separates two strata");
        };
        let graph = initialize(upstream, &[], std::slice::from_ref(downstream), &program);
        let index = graph
            .edges
            .iter()
            .find(|edge| {
                matches!(
                    edge.output,
                    PhysicalArrangement::Arrangement { stratum: 1, .. }
                ) && matches!(
                    &edge.input,
                    Producer::Transformation { step, .. } if step.output().is_k_only()
                )
            })
            .expect("the downstream c index");
        let Producer::Transformation { step, sources, .. } = &index.input else {
            panic!("the c index has a projection feed");
        };
        assert_eq!(step.unary_input().arity(), (0, 3));
        assert_eq!(
            step.flow().key().as_slice(),
            &[ArithmeticArgument {
                init: FactorArgument::Var(TransformationArgument::KV((false, 2))),
                rest: Vec::new(),
            }],
        );
        assert!(step.flow().value().is_empty());
        let [Producer::Transformation { step: join, .. }] = sources.as_slice() else {
            panic!("the projection reads the shared join directly");
        };
        assert!(matches!(join.as_ref(), Transformation::JnToRow { .. }));

        let mid = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum: 0,
                        scope: Scope::NonRecursive,
                        relation: compute_fp("mid"),
                    }
            })
            .expect("Mid's dedup");
        let Producer::Union(parts) = &mid.input else {
            panic!("Mid's binding retains its union feeds");
        };
        let [Producer::Transformation { step, .. }] = parts.as_slice() else {
            panic!("Mid has one projection feed");
        };
        assert_eq!(
            step.flow().value().as_slice(),
            &[
                ArithmeticArgument {
                    init: FactorArgument::Var(TransformationArgument::KV((false, 1))),
                    rest: Vec::new(),
                },
                ArithmeticArgument {
                    init: FactorArgument::Var(TransformationArgument::KV((false, 2))),
                    rest: Vec::new(),
                },
            ],
        );
    }

    #[test]
    fn dedup_arities_follow_the_order_written_back_to_the_stratum() {
        let program = program(
            r#"
            .decl E(a: int32, b: int32, c: int32)
            .input E
            .decl R(a: int32, b: int32, c: int32)
            .output R
            R(a,b,c) :- E(a,b,c).
            "#,
        );
        let stratifier = crate::stratifier::Stratifier::from_program(&program).expect("stratified");
        let mut optimizer = crate::optimizer::Optimizer::new();
        let declarations: std::collections::HashMap<u64, &flowlog_parser::Relation> = program
            .relations()
            .iter()
            .map(|relation| (relation.fingerprint(), relation))
            .collect();
        let mut stratum = StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[0],
            &mut optimizer,
            &mut None,
            &[],
            &declarations,
            &program.edb_fingerprints(),
        )
        .expect("planned");
        stratum
            .relation_bindings
            .get_mut(&compute_fp("r"))
            .unwrap()
            .operation = BindingOperation::Dedup {
            order: vec![2, 0, 1],
        };
        let graph = initialize(&stratum, &[], &[], &program);
        assert_eq!(
            graph.nodes[&PhysicalArrangement::Dedup {
                stratum: 0,
                scope: Scope::NonRecursive,
                relation: compute_fp("r"),
            }],
            Arities {
                key: vec![2, 0, 1],
                value: vec![]
            },
        );
    }

    #[test]
    fn a_later_stratums_binding_keeps_earlier_contributions() {
        let program = program(
            r#"
            .decl E(x: int32)
            .input E(IO="file", filename="E.csv")
            .decl F(x: int32)
            .input F(IO="file", filename="F.csv")
            .decl G(x: int32)
            .input G(IO="file", filename="G.csv")
            .decl Aux(x: int32)
            .decl R(x: int32)
            .output R
            Aux(x) :- G(x).
            R(x) :- E(x).
            R(x) :- Aux(x), F(x).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).expect("planned");
        let [upstream, downstream] = plan.strata() else {
            panic!("the Aux dependency separates two strata");
        };
        let graph = initialize(downstream, std::slice::from_ref(upstream), &[], &program);
        let binding = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum: 0,
                        scope: Scope::NonRecursive,
                        relation: compute_fp("r"),
                    }
            })
            .expect("R's later dedup");
        let Producer::Union(parts) = &binding.input else {
            panic!("R's binding retains its union feeds");
        };
        let [
            Producer::External(seed),
            Producer::Transformation { step, .. },
        ] = parts.as_slice()
        else {
            panic!("R's binding must union its earlier binding with the new join");
        };
        assert_eq!(*seed, compute_fp("r"));
        assert!(matches!(step.as_ref(), Transformation::JnToRow { .. }));
    }

    #[test]
    fn recursive_feedback_references_the_binding_without_replacing_its_seed() {
        let program = program(
            r#"
            .decl Arc(x: int32, y: int32)
            .input Arc
            .decl Reach(x: int32, y: int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).expect("planned");
        let [base, recursive] = plan.strata() else {
            panic!("Reach has a base stratum and a recursive stratum");
        };
        let graph = initialize(recursive, std::slice::from_ref(base), &[], &program);
        let relation = compute_fp("reach");
        let expected = PhysicalArrangement::Dedup {
            stratum: 0,
            scope: Scope::Recursive,
            relation,
        };
        let binding = graph
            .edges
            .iter()
            .find(|edge| edge.output == expected)
            .expect("Reach's recursive binding");
        let Producer::Union(parts) = &binding.input else {
            panic!("Reach's binding retains its seed and rule results");
        };
        assert!(matches!(parts.first(), Some(Producer::External(seed)) if *seed == relation));
        assert!(graph.edges.iter().any(|edge| {
            matches!(
                &edge.input,
                Producer::Transformation { sources, .. }
                    if sources.iter().any(|source| {
                        matches!(source, Producer::Arrangement(node) if node == &expected)
                    })
            )
        }));
    }

    #[test]
    fn projection_fusion_stops_at_a_dedup_arrangement() {
        let program = program(
            r#"
            .decl R(a: int32, b: int32, c: int32)
            .input R(IO="file", filename="R.csv")
            .decl P(b: int32, c: int32)
            .decl Out(c: int32)
            .output Out
            P(b,c) :- R(_,b,c).
            Out(c) :- P(_,c).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).expect("planned");
        let [upstream, downstream] = plan.strata() else {
            panic!("the P dependency separates two strata");
        };
        let graph = initialize(upstream, &[], std::slice::from_ref(downstream), &program);
        let out = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum: 1,
                        scope: Scope::NonRecursive,
                        relation: compute_fp("out"),
                    }
            })
            .expect("Out's dedup");
        let Producer::Union(parts) = &out.input else {
            panic!("Out's binding retains its union feeds");
        };
        let [Producer::Transformation { step, sources, .. }] = parts.as_slice() else {
            panic!("Out has one projection feed");
        };
        assert_eq!(step.unary_input().arity(), (0, 2));
        assert!(matches!(
            sources.as_slice(),
            [Producer::Arrangement(PhysicalArrangement::Dedup {
                stratum: 0,
                scope: Scope::NonRecursive,
                relation,
                ..
            })] if *relation == compute_fp("p")
        ));
    }
}
