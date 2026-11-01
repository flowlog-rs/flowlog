//! Arrangement provenance across evaluation and recursion scopes.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;

use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;

use super::arrangement_graph::Arities;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::Edge;
use super::arrangement_graph::JoinOrientation;
use super::arrangement_graph::PhysicalArrangement;
use super::arrangement_graph::Producer;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationId;
use super::transformation_fusion::fuse_projection_feeds;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::RelationBindingPlan;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

type CollectionProducerMap<'plan> = BTreeMap<u64, Producer<'plan>>;

// =============================================================================
// Graph initialization
// =============================================================================

/// Initializes every stratum once in evaluation order, preserving shared
/// producers, recursive feedback and the separate outer seed. External leaves
/// are declared program inputs. Projection fusion stays within feeds and stops
/// at materialized layout boundaries.
///
/// # Errors
///
/// Returns an internal error for missing declaration or mutability metadata,
/// a planned aggregation without a declared min/max column, or an unresolved
/// collection producer.
pub(super) fn initialize<'plan>(
    plan: &'plan [StratumPlanner],
    program: &Program,
) -> Result<ArrangementGraph<'plan>, PlanError> {
    let relation_declaration_map: HashMap<u64, &Relation> = program
        .relations()
        .iter()
        .map(|declaration| (declaration.fingerprint(), declaration))
        .collect();
    let mut graph = ArrangementGraph::default();
    let mut collection_producer_map: CollectionProducerMap<'plan> = program
        .edb_fingerprints()
        .into_iter()
        .map(|fingerprint| (fingerprint, Producer::External(fingerprint)))
        .collect();
    let evaluation_order = plan.iter().enumerate();
    for (stratum_index, strata) in evaluation_order {
        initialize_stratum(
            stratum_index,
            strata,
            &relation_declaration_map,
            &mut collection_producer_map,
            &mut graph,
        )?;
    }
    fuse_projection_feeds(&mut graph);
    Ok(graph)
}

/// Adds a stratum's boundaries and feeds, publishing its completed outer
/// results. Recursive reads retain inner producers, and seeds retain their
/// earlier outer producers even when feedback uses the same relation name.
fn initialize_stratum<'plan>(
    stratum_index: usize,
    strata: &'plan StratumPlanner,
    relation_declaration_map: &HashMap<u64, &Relation>,
    collection_producer_map: &mut CollectionProducerMap<'plan>,
    graph: &mut ArrangementGraph<'plan>,
) -> Result<(), PlanError> {
    // Seeds and entered inputs can read prelude results, so resolve their
    // producers before replacing any earlier relation binding.
    initialize_transformations(
        stratum_index,
        Scope::NonRecursive,
        strata.non_recursive_transformations(),
        collection_producer_map,
        None,
        graph,
    )?;

    // An unobservable loop is omitted by codegen, but its prelude still runs.
    if strata.is_recursive() && strata.recursion_leave_collections().is_empty() {
        return Ok(());
    }
    let scope = if strata.is_recursive() {
        Scope::Recursive
    } else {
        Scope::NonRecursive
    };
    // Reserve binding identities before reading feedback to represent cycles
    // without unrolling the recursive body.
    let mut binding_producer_map: CollectionProducerMap<'plan> = BTreeMap::new();
    for (&relation_fingerprint, binding) in strata.relation_bindings() {
        let declaration = relation_declaration_map
            .get(&relation_fingerprint)
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "binding 0x{relation_fingerprint:016x} has no declaration"
                ))
            })?;
        let mutability = strata.mutability(relation_fingerprint).ok_or_else(|| {
            PlanError::internal(format!(
                "binding 0x{relation_fingerprint:016x} has no mutability"
            ))
        })?;
        let node = PhysicalArrangement::relation_binding(
            relation_fingerprint,
            binding.operation(),
            stratum_index,
            scope,
        );
        graph.nodes.insert(
            node.clone(),
            binding_arities(binding.operation(), declaration, mutability)?,
        );
        binding_producer_map.insert(relation_fingerprint, Producer::Arrangement(node));
    }

    if !strata.is_recursive() {
        for (&relation_fingerprint, binding) in strata.relation_bindings() {
            graph.edges.push(Edge {
                input: binding_feed(
                    stratum_index,
                    scope,
                    relation_fingerprint,
                    binding,
                    collection_producer_map,
                    collection_producer_map,
                    None,
                )?,
                output: PhysicalArrangement::relation_binding(
                    relation_fingerprint,
                    binding.operation(),
                    stratum_index,
                    scope,
                ),
            });
        }
        // Publish together after resolving every union against earlier
        // bindings, so a relation cannot accidentally replace its own seed.
        collection_producer_map.extend(binding_producer_map);
        return Ok(());
    }

    let entered_producer_map: CollectionProducerMap<'plan> = strata
        .recursion_enter_collections()
        .iter()
        .map(|&fingerprint| {
            Ok((
                fingerprint,
                resolve_producer(fingerprint, collection_producer_map, None)?,
            ))
        })
        .collect::<Result<_, PlanError>>()?;
    // Feedback shadows body reads of R, while R's seed retains the entered
    // outer producer. Neither scope entry nor feedback builds an arrangement.
    let mut recursive_collection_producer_map = entered_producer_map.clone();
    for &relation_fingerprint in strata.recursion_feedback_collections() {
        if let Some(producer) = binding_producer_map.get(&relation_fingerprint) {
            recursive_collection_producer_map.insert(relation_fingerprint, producer.clone());
        }
    }
    initialize_transformations(
        stratum_index,
        Scope::Recursive,
        strata.recursive_transformations(),
        &mut recursive_collection_producer_map,
        Some(collection_producer_map),
        graph,
    )?;
    for (&relation_fingerprint, binding) in strata.relation_bindings() {
        graph.edges.push(Edge {
            input: binding_feed(
                stratum_index,
                scope,
                relation_fingerprint,
                binding,
                &entered_producer_map,
                &recursive_collection_producer_map,
                Some(collection_producer_map),
            )?,
            output: PhysicalArrangement::relation_binding(
                relation_fingerprint,
                binding.operation(),
                stratum_index,
                scope,
            ),
        });
    }

    // A static min/max loop accumulates answers from several iterations.
    // Codegen folds them once more after leaving, in declaration key order;
    // that fixed outer boundary must not replace the inner feedback producer.
    for &relation_fingerprint in strata.recursion_leave_collections() {
        let Some(producer) = binding_producer_map.get(&relation_fingerprint) else {
            continue;
        };
        let is_static_aggregation = strata.mutability(relation_fingerprint)
            == Some(Mutability::Static)
            && matches!(
                strata.relation_bindings()[&relation_fingerprint].operation(),
                BindingOperation::Aggregation { .. }
            );
        let producer = if is_static_aggregation {
            let declaration = relation_declaration_map[&relation_fingerprint];
            let Some((aggregate_column, _)) = declaration.lattice() else {
                return Err(PlanError::internal(format!(
                    "aggregation exit 0x{relation_fingerprint:016x} has no declared min/max column"
                )));
            };
            let node = PhysicalArrangement::AggregationExit {
                stratum_index,
                relation_fingerprint,
            };
            graph.nodes.insert(
                node.clone(),
                Arities {
                    key: (0..declaration.arity())
                        .filter(|&column| column != aggregate_column)
                        .collect(),
                    value: Vec::new(),
                },
            );
            graph.edges.push(Edge {
                input: producer.clone(),
                output: node.clone(),
            });
            Producer::Arrangement(node)
        } else {
            producer.clone()
        };
        collection_producer_map.insert(relation_fingerprint, producer);
    }
    Ok(())
}

/// Includes each binding input's ordinary row projection in its feed. Seed
/// sources resolve separately from rule sources when feedback shadows a name.
fn binding_feed<'plan>(
    stratum_index: usize,
    scope: Scope,
    relation_fingerprint: u64,
    binding: &'plan RelationBindingPlan,
    seed_producer_map: &CollectionProducerMap<'plan>,
    input_producer_map: &CollectionProducerMap<'plan>,
    outer_producer_map: Option<&CollectionProducerMap<'plan>>,
) -> Result<Producer<'plan>, PlanError> {
    let mut feeds = Vec::new();
    if let Some(projection) = binding.seed() {
        let source = resolve_producer(
            projection.unary_input().fingerprint(),
            seed_producer_map,
            None,
        )?;
        feeds.push(Producer::Transformation {
            id: TransformationId::BindingSeed {
                stratum_index,
                scope,
                relation_fingerprint,
            },
            step: Cow::Borrowed(projection),
            sources: vec![source].into(),
        });
    }
    let rule_feeds = binding.inputs().iter().enumerate();
    for (input_index, projection) in rule_feeds {
        let source = resolve_producer(
            projection.unary_input().fingerprint(),
            input_producer_map,
            outer_producer_map,
        )?;
        feeds.push(Producer::Transformation {
            id: TransformationId::BindingInput {
                stratum_index,
                scope,
                relation_fingerprint,
                input_index,
            },
            step: Cow::Borrowed(projection),
            sources: vec![source].into(),
        });
    }
    Ok(Producer::Union(feeds))
}

/// Keeps binding IDs in logical relation coordinates. Static min/max stores
/// contributions in weights, while signed merges arrange both key and value.
pub(super) fn binding_arities(
    operation: &BindingOperation,
    declaration: &Relation,
    mutability: Mutability,
) -> Result<Arities, PlanError> {
    match operation {
        BindingOperation::Dedup { order } => Ok(Arities {
            key: order.clone(),
            value: Vec::new(),
        }),
        BindingOperation::Aggregation { order } => {
            let Some((aggregate_column, _)) = declaration.lattice() else {
                return Err(PlanError::internal(format!(
                    "aggregation of {} has no declared min/max column",
                    declaration.name(),
                )));
            };
            Ok(Arities {
                key: order.clone(),
                value: match mutability {
                    Mutability::Static => Vec::new(),
                    Mutability::Append | Mutability::Mutable => vec![aggregate_column],
                },
            })
        }
    }
}

/// Publishes each step's producer in dependency order. Keyed outputs become
/// graph nodes; row outputs remain feed expressions for projection fusion.
/// Binary sources retain their left-to-right order.
fn initialize_transformations<'plan>(
    stratum_index: usize,
    scope: Scope,
    transformations: &'plan [Transformation],
    collection_producer_map: &mut CollectionProducerMap<'plan>,
    outer_producer_map: Option<&CollectionProducerMap<'plan>>,
    graph: &mut ArrangementGraph<'plan>,
) -> Result<(), PlanError> {
    let steps = transformations.iter().enumerate();
    for (transformation_index, step) in steps {
        if matches!(
            step,
            Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
        ) {
            graph.joins.insert(
                TransformationId::Rule {
                    stratum_index,
                    scope,
                    transformation_index,
                },
                JoinOrientation::Original,
            );
        }
        let input_fingerprints = step.input_fingerprints();
        let sources: Vec<Producer<'plan>> = input_fingerprints
            .into_iter()
            .map(|fingerprint| {
                resolve_producer(fingerprint, collection_producer_map, outer_producer_map)
            })
            .collect::<Result<_, _>>()?;
        let expression = Producer::Transformation {
            id: TransformationId::Rule {
                stratum_index,
                scope,
                transformation_index,
            },
            step: Cow::Borrowed(step),
            sources: sources.into(),
        };
        let output_producer = if step.need_arrange() {
            let (key_arity, value_arity) = step.output().arity();
            let node = PhysicalArrangement::Arrangement {
                stratum_index,
                scope,
                transformation_index,
            };
            graph
                .nodes
                .insert(node.clone(), Arities::new(key_arity, value_arity));
            graph.edges.push(Edge {
                input: expression,
                output: node.clone(),
            });
            Producer::Arrangement(node)
        } else {
            expression
        };
        collection_producer_map.insert(step.output().fingerprint(), output_producer);
    }
    Ok(())
}

/// Resolves a collection's producer from the local map before the outer map.
/// Missing producers indicate an incomplete plan or dependency order.
fn resolve_producer<'plan>(
    fingerprint: u64,
    collection_producer_map: &CollectionProducerMap<'plan>,
    outer_producer_map: Option<&CollectionProducerMap<'plan>>,
) -> Result<Producer<'plan>, PlanError> {
    collection_producer_map
        .get(&fingerprint)
        .or_else(|| {
            outer_producer_map
                .and_then(|collection_producer_map| collection_producer_map.get(&fingerprint))
        })
        .cloned()
        .ok_or_else(|| {
            PlanError::internal(format!(
                "collection 0x{fingerprint:016x} has no producer in the local or outer scope"
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::planner::ArithmeticArgument;
    use crate::planner::FactorArgument;
    use crate::planner::ProgramPlanner;
    use crate::planner::TransformationArgument;

    #[test]
    fn missing_internal_producers_are_rejected_instead_of_becoming_external() {
        let program = program(
            r#"
            .decl E(x: int32)
            .input E
            .decl P(x: int32)
            .decl Out(x: int32)
            .output Out
            P(x) :- E(x).
            Out(x) :- P(x).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        assert_eq!(plan.strata().len(), 2);

        let error = initialize(&plan.strata()[1..], &program).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert_eq!(
            error.to_string(),
            format!(
                "internal compiler error at stage `planner`: collection 0x{:016x} \
                 has no producer in the local or outer scope",
                compute_fp("p"),
            ),
        );
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
        assert_eq!(plan.strata().len(), 2);
        let graph = initialize(plan.strata(), &program).unwrap();
        let arrangement_edge = graph
            .edges
            .iter()
            .find(|edge| {
                matches!(
                    edge.output,
                    PhysicalArrangement::Arrangement {
                        stratum_index: 1,
                        ..
                    }
                ) && matches!(
                    &edge.input,
                    Producer::Transformation { step, .. } if step.output().is_k_only()
                )
            })
            .expect("the downstream c index");
        let Producer::Transformation { step, sources, .. } = &arrangement_edge.input else {
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
        let [Producer::Transformation { step: join, .. }] = sources.as_ref() else {
            panic!("the projection reads the shared join directly");
        };
        assert!(matches!(join.as_ref(), Transformation::JnToRow { .. }));

        let mid = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum_index: 0,
                        scope: Scope::NonRecursive,
                        relation_fingerprint: compute_fp("mid"),
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
            .input R
            .output R
            R(a,b,c) :- E(a,b,c).
            "#,
        );
        let stratifier = crate::stratifier::Stratifier::from_program(&program).expect("stratified");
        let mut optimizer = crate::optimizer::Optimizer::new();
        let relation_declaration_map: std::collections::HashMap<u64, &flowlog_parser::Relation> =
            program
                .relations()
                .iter()
                .map(|declaration| (declaration.fingerprint(), declaration))
                .collect();
        let mut strata = StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[0],
            &mut optimizer,
            &mut None,
            &[],
            &relation_declaration_map,
            &program
                .edbs()
                .into_iter()
                .map(|declaration| (declaration.fingerprint(), declaration.input_mutability()))
                .collect(),
        )
        .expect("planned");
        let binding = strata.relation_bindings.get_mut(&compute_fp("r")).unwrap();
        binding.operation = BindingOperation::Dedup {
            order: vec![2, 0, 1],
        };
        let projections = binding.seed.iter_mut().chain(&mut binding.inputs);
        for projection in projections {
            *projection = Transformation::row_projection(
                Arc::clone(projection.unary_input()),
                projection.output().fingerprint(),
                &[2, 0, 1],
            )
            .unwrap();
        }
        let graph = initialize(std::slice::from_ref(&strata), &program).unwrap();
        assert_eq!(
            graph.nodes[&PhysicalArrangement::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
            }],
            Arities {
                key: vec![2, 0, 1],
                value: vec![]
            },
        );
        let edge = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum_index: 0,
                        scope: Scope::NonRecursive,
                        relation_fingerprint: compute_fp("r"),
                    }
            })
            .unwrap();
        let Producer::Union(parts) = &edge.input else {
            panic!("R keeps both its seed and rule projection");
        };
        let [
            Producer::Transformation {
                id: seed,
                step: seed_step,
                sources: seed_sources,
            },
            Producer::Transformation {
                id: rule,
                step: rule_step,
                sources: rule_sources,
            },
        ] = parts.as_slice()
        else {
            panic!("both non-default projections remain explicit feeds");
        };
        assert_eq!(
            *seed,
            TransformationId::BindingSeed {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
            }
        );
        assert_eq!(
            *rule,
            TransformationId::BindingInput {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
                input_index: 0,
            }
        );
        assert_eq!(seed_step.flow().to_string(), "V:(V2, V0, V1)");
        assert_eq!(rule_step.flow().to_string(), "V:(V2, V0, V1)");
        assert!(
            matches!(seed_sources.as_ref(), [Producer::External(source)] if *source == compute_fp("r"))
        );
        assert!(
            matches!(rule_sources.as_ref(), [Producer::External(source)] if *source == compute_fp("e"))
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
        assert_eq!(plan.strata().len(), 2);
        let graph = initialize(plan.strata(), &program).unwrap();
        let binding = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum_index: 1,
                        scope: Scope::NonRecursive,
                        relation_fingerprint: compute_fp("r"),
                    }
            })
            .expect("R's later dedup");
        let Producer::Union(parts) = &binding.input else {
            panic!("R's binding retains its union feeds");
        };
        let [
            Producer::Arrangement(PhysicalArrangement::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: seed,
            }),
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
        assert_eq!(plan.strata().len(), 2);
        let graph = initialize(plan.strata(), &program).unwrap();
        let relation_fingerprint = compute_fp("reach");
        let expected = PhysicalArrangement::Dedup {
            stratum_index: 1,
            scope: Scope::Recursive,
            relation_fingerprint,
        };
        let binding = graph
            .edges
            .iter()
            .find(|edge| edge.output == expected)
            .expect("Reach's recursive binding");
        let Producer::Union(parts) = &binding.input else {
            panic!("Reach's binding retains its seed and rule results");
        };
        assert!(matches!(
            parts.first(),
            Some(Producer::Arrangement(PhysicalArrangement::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: seed,
            })) if *seed == relation_fingerprint
        ));
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
        assert_eq!(plan.strata().len(), 2);
        let graph = initialize(plan.strata(), &program).unwrap();
        let out = graph
            .edges
            .iter()
            .find(|edge| {
                edge.output
                    == PhysicalArrangement::Dedup {
                        stratum_index: 1,
                        scope: Scope::NonRecursive,
                        relation_fingerprint: compute_fp("out"),
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
            sources.as_ref(),
            [Producer::Arrangement(PhysicalArrangement::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint,
                ..
            })] if *relation_fingerprint == compute_fp("p")
        ));
    }

    // Cases: input mutability declaration, aggregate, arrangement value fields.
    #[rstest]
    #[case::static_min("", "min", vec![])]
    #[case::append_min(" append", "min", vec![1])]
    #[case::mutable_min(" mutable", "min", vec![1])]
    #[case::static_max("", "max", vec![])]
    #[case::append_max(" append", "max", vec![1])]
    #[case::mutable_max(" mutable", "max", vec![1])]
    fn aggregation_values_follow_the_stratums_inferred_mutability(
        #[case] input_mutability_suffix: &str,
        #[case] aggregate: &str,
        #[case] expected_value: Vec<usize>,
    ) {
        let program = program(&format!(
            ".decl E(a:int32,b:int32,c:int32){input_mutability_suffix}\n.input E\n\
             .decl R(a:int32,b:{aggregate}<int32>,c:int32)\n.output R\n\
             R(a,b,c) :- E(a,b,c).\n",
        ));
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        assert_eq!(plan.strata().len(), 1);
        let graph = initialize(plan.strata(), &program).unwrap();
        let node = PhysicalArrangement::Aggregation {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            relation_fingerprint: compute_fp("r"),
        };
        assert_eq!(
            graph.nodes[&node],
            Arities {
                key: vec![0, 2],
                value: expected_value
            }
        );
        assert!(
            !graph
                .nodes
                .keys()
                .any(|node| matches!(node, PhysicalArrangement::AggregationExit { .. }))
        );
    }

    // Cases: input mutability, fixed outer fold, inner value fields.
    #[rstest]
    #[case::static_inputs("", true, vec![])]
    #[case::append_inputs(" append", false, vec![1])]
    #[case::mutable_inputs(" mutable", false, vec![1])]
    fn recursive_aggregations_publish_the_outer_fold_only_for_static_inputs(
        #[case] input_mutability_suffix: &str,
        #[case] expected_fold: bool,
        #[case] expected_value: Vec<usize>,
    ) {
        let program = program(&format!(
            ".decl Seed(a:int32,b:int32,c:int32){input_mutability_suffix}\n.input Seed\n\
             .decl Edge(src:int32,dst:int32,w:int32){input_mutability_suffix}\n.input Edge\n\
             .decl R(a:int32,b:min<int32>,c:int32)\n\
             R(a,b,c) :- Seed(a,b,c).\n\
             R(to,b+w,c) :- R(from,b,c), Edge(from,to,w).\n\
             .decl Out(a:int32,b:int32,c:int32)\n.output Out\n\
             Out(a,b,c) :- R(a,b,c).\n",
        ));
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        assert_eq!(plan.strata().len(), 3);
        let graph = initialize(plan.strata(), &program).unwrap();
        let inner = PhysicalArrangement::Aggregation {
            stratum_index: 1,
            scope: Scope::Recursive,
            relation_fingerprint: compute_fp("r"),
        };
        let exit = PhysicalArrangement::AggregationExit {
            stratum_index: 1,
            relation_fingerprint: compute_fp("r"),
        };
        assert_eq!(
            graph.nodes[&inner],
            Arities {
                key: vec![0, 2],
                value: expected_value
            }
        );
        assert_eq!(graph.nodes.contains_key(&exit), expected_fold);
        let completed = if expected_fold {
            assert_eq!(
                graph.nodes[&exit],
                Arities {
                    key: vec![0, 2],
                    value: vec![]
                }
            );
            let edge = graph.edges.iter().find(|edge| edge.output == exit).unwrap();
            assert!(matches!(&edge.input, Producer::Arrangement(node) if node == &inner));
            &exit
        } else {
            &inner
        };
        let out = graph
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
        assert!(matches!(
            &out.input,
            Producer::Union(parts) if matches!(parts.as_slice(), [Producer::Arrangement(node)] if node == completed)
        ));
        assert!(graph.edges.iter().any(|edge| {
            matches!(&edge.input, Producer::Transformation { sources, .. }
                if sources.iter().any(|source| matches!(source, Producer::Arrangement(node) if node == &inner)))
        }));
    }

    #[test]
    fn a_static_aggregation_exit_keeps_declaration_order_after_inner_reordering() {
        let program = program(
            r#"
            .decl Seed(a: int32, b: int32, c: int32)
            .input Seed
            .decl Edge(src: int32, dst: int32, w: int32)
            .input Edge
            .decl R(a: int32, b: min<int32>, c: int32)
            .output R
            R(a,b,c) :- Seed(a,b,c).
            R(to,b+w,c) :- R(from,b,c), Edge(from,to,w).
            "#,
        );
        let stratifier = crate::stratifier::Stratifier::from_program(&program).unwrap();
        assert_eq!(stratifier.strata().len(), 2);
        let mut optimizer = crate::optimizer::Optimizer::new();
        let relation_declaration_map: HashMap<u64, &Relation> = program
            .relations()
            .iter()
            .map(|declaration| (declaration.fingerprint(), declaration))
            .collect();
        let mut bound_relations: HashMap<u64, Mutability> = program
            .edbs()
            .into_iter()
            .map(|declaration| (declaration.fingerprint(), declaration.input_mutability()))
            .collect();
        let relation_fingerprint = compute_fp("r");
        let seed_strata = StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[0],
            &mut optimizer,
            &mut None,
            &[],
            &relation_declaration_map,
            &bound_relations,
        )
        .unwrap();
        bound_relations.insert(relation_fingerprint, Mutability::Static);
        let mut strata = StratumPlanner::from_stratum(
            &program,
            &stratifier.strata()[1],
            &mut optimizer,
            &mut None,
            &[],
            &relation_declaration_map,
            &bound_relations,
        )
        .unwrap();
        strata
            .relation_bindings
            .get_mut(&relation_fingerprint)
            .unwrap()
            .operation = BindingOperation::Aggregation { order: vec![2, 0] };

        let plan = [seed_strata, strata];
        let graph = initialize(&plan, &program).unwrap();
        let inner = PhysicalArrangement::Aggregation {
            stratum_index: 1,
            scope: Scope::Recursive,
            relation_fingerprint,
        };
        let exit = PhysicalArrangement::AggregationExit {
            stratum_index: 1,
            relation_fingerprint,
        };
        assert_eq!(
            graph.nodes[&inner],
            Arities {
                key: vec![2, 0],
                value: vec![],
            }
        );
        assert_eq!(
            graph.nodes[&exit],
            Arities {
                key: vec![0, 2],
                value: vec![],
            }
        );
        let edge = graph.edges.iter().find(|edge| edge.output == exit).unwrap();
        assert!(matches!(&edge.input, Producer::Arrangement(node) if node == &inner));
        assert!(graph.edges.iter().any(|edge| {
            matches!(&edge.input, Producer::Transformation { sources, .. }
                if sources.iter().any(|source| matches!(source, Producer::Arrangement(node) if node == &inner)))
        }));
    }

    #[test]
    fn a_stratum_chain_records_each_binding_and_feed_once() {
        let mut source = ".decl E(x:int32)\n.input E\n".to_string();
        for index in 0..32 {
            source.push_str(&format!(".decl R{index}(x:int32)\n"));
            let input = if index == 0 {
                "E".to_string()
            } else {
                format!("R{}", index - 1)
            };
            source.push_str(&format!("R{index}(x) :- {input}(x).\n"));
        }
        source.push_str(".output R31\n");
        let program = program(&source);
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        assert_eq!(plan.strata().len(), 32);
        let graph = initialize(plan.strata(), &program).unwrap();
        assert_eq!(graph.nodes.len(), 32);
        assert_eq!(graph.edges.len(), 32);
        let feeds = graph.edges.iter().enumerate();
        for (index, edge) in feeds {
            let Producer::Union(parts) = &edge.input else {
                panic!("each binding has one rule-result feed");
            };
            if index == 0 {
                assert!(
                    matches!(parts.as_slice(), [Producer::External(source)] if *source == compute_fp("e"))
                );
            } else {
                assert!(
                    matches!(parts.as_slice(), [Producer::Arrangement(PhysicalArrangement::Dedup {
                    stratum_index, scope: Scope::NonRecursive, relation_fingerprint,
                })] if *stratum_index == index - 1 && *relation_fingerprint == compute_fp(format!("r{}", index - 1)))
                );
            }
        }
    }
}
