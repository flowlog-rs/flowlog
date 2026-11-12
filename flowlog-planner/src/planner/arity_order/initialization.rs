//! Whole-program arrangement graph construction and projection fusion.
//!
//! `initialize_stratum` registers each stratum's bindings and feeds with
//! their scoped producers. Once every stratum is present, `fuse_feed` composes
//! feeds without changing the original plans.

mod fuse_feed;
mod initialize_stratum;

use std::collections::BTreeMap;
use std::collections::HashMap;

use flowlog_parser::Atom;
use flowlog_parser::Program;
use flowlog_parser::Relation;

use self::fuse_feed::fuse_feed;
use self::initialize_stratum::build_stratum;
use super::arrangement_graph::Arrangement;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::ArrangementOrigin;
use super::arrangement_graph::AttributeLayout;
use super::arrangement_graph::Source;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;

type CollectionProducerMap = BTreeMap<u64, Source>;

/// Builds one graph with every stratum in evaluation order. Shared collections
/// retain one source ID, feedback references reserved arrangements, and seeds
/// retain their earlier sources. Projection fusion owns its rewritten graph
/// transformations, leaving the original plans unchanged.
///
/// # Errors
///
/// Returns an internal error for missing declaration or mutability metadata,
/// a planned aggregation without a declared min/max column, or an unresolved
/// collection producer.
pub(super) fn build_graph<'plan>(
    plan: &'plan [StratumPlanner],
    program: &Program,
) -> Result<ArrangementGraph<'plan>, PlanError> {
    let relation_declaration_map: HashMap<u64, &Relation> = program
        .relations()
        .iter()
        .map(|declaration| (declaration.fingerprint(), declaration))
        .collect();
    let mut graph = ArrangementGraph::default();
    let mut collection_producer_map = CollectionProducerMap::new();
    for relation in program.edbs() {
        let fingerprint = relation.fingerprint();
        let input = graph.add_arrangement(Arrangement {
            origin: ArrangementOrigin::InputDedup {
                relation_fingerprint: fingerprint,
            },
            layout: AttributeLayout::new(relation.arity(), 0),
            inputs: Vec::new(),
        });
        collection_producer_map.insert(fingerprint, Source::Arrangement(input));
    }
    // The unit is one empty row created by codegen, with no input dedup or
    // user declaration. Keep it separate from selectable EDB boundaries.
    if plan.iter().any(StratumPlanner::reads_unit) {
        collection_producer_map.insert(Atom::unit_fingerprint(), Source::Unit);
    }
    let evaluation_order = plan.iter().enumerate();
    for (stratum_index, stratum) in evaluation_order {
        build_stratum(
            stratum_index,
            stratum,
            &relation_declaration_map,
            &mut collection_producer_map,
            &mut graph,
        )?;
    }
    // Sharing can leave projection chains across strata, so fuse only after
    // every feed is present. The original plans retain the execution structure.
    fuse_feed(&mut graph);
    Ok(graph)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::Mutability;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::super::arrangement_graph::Arrangement;
    use super::super::arrangement_graph::ArrangementGraph;
    use super::super::arrangement_graph::ArrangementId;
    use super::super::arrangement_graph::ArrangementOrigin;
    use super::super::arrangement_graph::AttributeLayout;
    use super::super::arrangement_graph::Feed;
    use super::super::arrangement_graph::Scope;
    use super::super::arrangement_graph::Source;
    use super::super::arrangement_graph::TransformationLocation;
    use super::build_graph;
    use super::fuse_feed::fuse_feed;
    use crate::catalog::ArithmeticPos;
    use crate::catalog::AtomArgumentSignature;
    use crate::catalog::AtomSignature;
    use crate::catalog::KvPredicates;
    use crate::planner::Constraints;
    use crate::planner::KeyValueLayout;
    use crate::planner::PlanError;
    use crate::planner::ProgramPlanner;
    use crate::planner::Transformation;
    use crate::planner::TransformationArgument;
    use crate::planner::TransformationFlow;
    use crate::planner::TransformationInfo;

    /// Finds a boundary by its plan identity, independently of registration order.
    ///
    /// # Panics
    ///
    /// Panics when the expected plan boundary is absent.
    #[must_use]
    fn arrangement_id(graph: &ArrangementGraph<'_>, origin: ArrangementOrigin) -> ArrangementId {
        ArrangementId(
            graph
                .arrangements
                .iter()
                .position(|arrangement| arrangement.origin == origin)
                .unwrap_or_else(|| panic!("missing boundary {origin:?}")),
        )
    }

    #[test]
    fn a_binding_unions_its_seed_and_every_rule_result_in_order() {
        let program = program(
            r#"
            .decl A(x:int32)
            .input A
            .decl B(x:int32)
            .input B
            .decl R(x:int32)
            .input R
            .output R
            R(x) :- A(x).
            R(x) :- B(x).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        let graph = build_graph(plan.strata(), &program).unwrap();
        let binding = arrangement_id(
            &graph,
            ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("r"),
            },
        );
        let [seed, Source::Feed(first), Source::Feed(second)] =
            graph.arrangements[binding.0].inputs.as_slice()
        else {
            panic!("R must union its input seed with both rule results");
        };
        let input = |name| {
            Source::Arrangement(arrangement_id(
                &graph,
                ArrangementOrigin::InputDedup {
                    relation_fingerprint: compute_fp(name),
                },
            ))
        };
        assert_eq!(*seed, input("r"));
        assert_eq!(graph.feeds[first.0].inputs, [input("a")]);
        assert_eq!(graph.feeds[second.0].inputs, [input("b")]);
    }

    #[test]
    fn recursive_feedback_uses_the_inner_binding_while_seed_keeps_the_earlier_binding() {
        let program = program(
            r#"
            .decl Arc(x:int32,y:int32)
            .input Arc
            .decl Reach(x:int32,y:int32)
            .output Reach
            Reach(x,y) :- Arc(x,y).
            Reach(x,z) :- Reach(x,y), Arc(y,z).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        let graph = build_graph(plan.strata(), &program).unwrap();
        let earlier = arrangement_id(
            &graph,
            ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("reach"),
            },
        );
        let recursive = arrangement_id(
            &graph,
            ArrangementOrigin::Dedup {
                stratum_index: 1,
                scope: Scope::Recursive,
                relation_fingerprint: compute_fp("reach"),
            },
        );
        let [seed, Source::Feed(rule)] = graph.arrangements[recursive.0].inputs.as_slice() else {
            panic!("Reach must union its earlier seed with the recursive rule");
        };
        assert_eq!(*seed, Source::Arrangement(earlier));
        assert!(matches!(
            graph.feeds[rule.0].step.as_ref(),
            Transformation::JnToRow { .. }
        ));
        let feedback_read = graph
            .feeds
            .iter()
            .find(|feed| {
                feed.location.scope == Scope::Recursive
                    && feed.step.input_fingerprints() == [compute_fp("reach")]
            })
            .unwrap();
        assert_eq!(feedback_read.inputs, [Source::Arrangement(recursive)]);
    }

    /// Sharing leaves a map chain across strata, but another consumer still needs
    /// the intermediate projection. Fusion must not overwrite that shared feed.
    #[test]
    fn cross_stratum_fusion_preserves_shared_feeds_and_the_original_plan() {
        let program = program(
            r#"
            .decl R(k:int32,a:int32)
            .input R
            .decl S(k:int32,b:int32,c:int32)
            .input S
            .decl Wide(a:int32,b:int32,c:int32)
            .decl Mid(b:int32,c:int32)
            .decl Out(c:int32,z:int32)
            .output Wide
            .output Out
            Wide(a,b,c) :- R(k,a), S(k,b,c).
            Mid(b,c) :- R(k,_), S(k,b,c).
            Out(c,z) :- R(k,_), S(k,_,c), Mid(c,z).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        let graph = build_graph(plan.strata(), &program).unwrap();
        let location = TransformationLocation {
            stratum_index: 1,
            scope: Scope::NonRecursive,
            transformation_index: 0,
        };
        let fused = graph
            .feeds
            .iter()
            .find(|feed| feed.location == location)
            .unwrap();
        let original = &plan.strata()[1].non_recursive_transformations()[0];
        assert!(matches!(fused.step, Cow::Owned(_)));
        assert_eq!(fused.step.unary_input().arity(), (0, 3));
        assert_eq!(fused.step.flow().to_string(), "K:(V2)");
        assert!(Arc::ptr_eq(fused.step.output(), original.output()));
        assert_eq!(original.unary_input().arity(), (0, 2));
        assert_eq!(original.flow().to_string(), "K:(V1)");

        let mid = arrangement_id(
            &graph,
            ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: compute_fp("mid"),
            },
        );
        let [Source::Feed(projection)] = graph.arrangements[mid.0].inputs.as_slice() else {
            panic!("Mid must retain its shared (b,c) projection");
        };
        let shared = &graph.feeds[projection.0];
        assert!(matches!(shared.step, Cow::Borrowed(_)));
        assert_eq!(shared.step.flow().to_string(), "V:(V1, V2)");
        assert_eq!(shared.inputs, fused.inputs);
        let [Source::Feed(join)] = fused.inputs.as_slice() else {
            panic!("the fused projection must read the original join");
        };
        assert!(matches!(
            graph.feeds[join.0].step.as_ref(),
            Transformation::JnToRow { .. }
        ));
        let mid_read = graph
            .feeds
            .iter()
            .find(|feed| feed.step.input_fingerprints() == [compute_fp("mid")])
            .unwrap();
        assert_eq!(mid_read.inputs, [Source::Arrangement(mid)]);
    }

    // Cases: input mutability, inner value fields, separate exit fold.
    #[rstest]
    #[case::static_inputs("", vec![], true)]
    #[case::append_inputs(" append", vec![1], false)]
    #[case::mutable_inputs(" mutable", vec![1], false)]
    fn aggregation_consumers_read_the_correct_recursive_exit(
        #[case] mutability: &str,
        #[case] value: Vec<usize>,
        #[case] separate_exit: bool,
    ) {
        let program = program(&format!(
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
            "#,
        ));
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        let graph = build_graph(plan.strata(), &program).unwrap();
        let inner = arrangement_id(
            &graph,
            ArrangementOrigin::Aggregation {
                stratum_index: 1,
                scope: Scope::Recursive,
                relation_fingerprint: compute_fp("r"),
            },
        );
        assert_eq!(
            graph.arrangements[inner.0].layout,
            AttributeLayout {
                key: vec![0, 2],
                value,
            }
        );
        let exit = graph.arrangements.iter().position(|arrangement| {
            arrangement.origin
                == ArrangementOrigin::AggregationExit {
                    stratum_index: 1,
                    relation_fingerprint: compute_fp("r"),
                }
        });
        assert_eq!(exit.is_some(), separate_exit);
        let completed = if let Some(index) = exit {
            assert_eq!(
                graph.arrangements[index].layout,
                AttributeLayout {
                    key: vec![0, 2],
                    value: vec![],
                }
            );
            assert_eq!(
                graph.arrangements[index].inputs,
                [Source::Arrangement(inner)]
            );
            ArrangementId(index)
        } else {
            inner
        };
        let out_read = graph
            .feeds
            .iter()
            .find(|feed| feed.location.stratum_index == 2)
            .unwrap();
        assert_eq!(out_read.inputs, [Source::Arrangement(completed)]);
    }

    #[test]
    fn a_missing_internal_producer_is_rejected_instead_of_becoming_external() {
        let program = program(
            r#"
            .decl E(x:int32)
            .input E
            .decl P(x:int32)
            .decl Out(x:int32)
            .output Out
            P(x) :- E(x).
            Out(x) :- P(x).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        // Omit P's producing stratum to exercise incomplete-plan rejection.
        let error = build_graph(&plan.strata()[1..], &program).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert_eq!(
            error.to_string(),
            "internal compiler error at stage `planner`: collection 0x1dd05ccf89abd763 has no producer in the local or outer scope",
        );
    }

    // =============================================================================
    // Feed fusion
    // =============================================================================

    // Cases: row output, fused field mapping, output arity.
    #[rstest]
    #[case::row_output(true, "V:(K0, V1)", (0, 2))]
    #[case::keyed_output(false, "K:(K0), V:(V1)", (1, 1))]
    fn fusion_reads_both_key_and_value_columns_from_the_original_arrangement(
        #[case] row_output: bool,
        #[case] expected_flow: &str,
        #[case] expected_arity: (usize, usize),
    ) {
        let column = |argument| {
            ArithmeticPos::from_var_signature(AtomArgumentSignature::new(
                AtomSignature::new(true, 0),
                argument,
            ))
        };
        let keyed_layout = KeyValueLayout::new(vec![column(0)], vec![column(1), column(2)]);
        let projected_layout = KeyValueLayout::new(vec![], vec![column(2), column(0)]);
        let mut produced = HashMap::new();
        let mutability_of = |_| Some(Mutability::Static);
        let arrange_info = TransformationInfo::kv_to_kv(
            compute_fp("r"),
            "r".into(),
            "by_a".into(),
            /*is_row_input*/ true,
            KeyValueLayout::new(vec![], vec![column(0), column(1), column(2)]),
            keyed_layout.clone(),
            KvPredicates::default(),
        );
        let arranged =
            Transformation::from_info(&arrange_info, &mut produced, &mutability_of).unwrap();
        let mut project_info = TransformationInfo::kv_to_kv(
            arranged.output().fingerprint(),
            "by_a".into(),
            "c_a".into(),
            /*is_row_input*/ false,
            keyed_layout,
            projected_layout.clone(),
            KvPredicates::default(),
        );
        project_info.update_row_output(/*is_row_output*/ true);
        let projected =
            Transformation::from_info(&project_info, &mut produced, &mutability_of).unwrap();
        let output_layout = if row_output {
            KeyValueLayout::new(vec![], vec![column(0), column(2)])
        } else {
            KeyValueLayout::new(vec![column(0)], vec![column(2)])
        };
        let mut output_info = TransformationInfo::kv_to_kv(
            projected.output().fingerprint(),
            "c_a".into(),
            "a_c".into(),
            /*is_row_input*/ true,
            projected_layout,
            output_layout,
            KvPredicates::default(),
        );
        output_info.update_row_output(row_output);
        let output =
            Transformation::from_info(&output_info, &mut produced, &mutability_of).unwrap();
        let location = |transformation_index| TransformationLocation {
            stratum_index: 0,
            scope: Scope::NonRecursive,
            transformation_index,
        };
        let mut graph = ArrangementGraph::default();
        let input = graph.add_arrangement(Arrangement {
            origin: ArrangementOrigin::InputDedup {
                relation_fingerprint: compute_fp("r"),
            },
            layout: AttributeLayout::new(3, 0),
            inputs: Vec::new(),
        });
        let arrange_feed = graph.add_feed(Feed {
            location: location(0),
            step: Cow::Borrowed(&arranged),
            inputs: vec![Source::Arrangement(input)],
            join_orientation: None,
        });
        let arrangement = graph.add_arrangement(Arrangement {
            origin: ArrangementOrigin::Transformation(location(0)),
            layout: AttributeLayout::new(1, 2),
            inputs: vec![Source::Feed(arrange_feed)],
        });
        let project_feed = graph.add_feed(Feed {
            location: location(1),
            step: Cow::Borrowed(&projected),
            inputs: vec![Source::Arrangement(arrangement)],
            join_orientation: None,
        });
        let output_feed = graph.add_feed(Feed {
            location: location(2),
            step: Cow::Borrowed(&output),
            inputs: vec![Source::Feed(project_feed)],
            join_orientation: None,
        });

        fuse_feed(&mut graph);

        let fused = &graph.feeds[output_feed.0];
        if row_output {
            assert!(matches!(
                fused.step.as_ref(),
                Transformation::KvToRow { .. }
            ));
        } else {
            assert!(matches!(fused.step.as_ref(), Transformation::KvToKv { .. }));
        }
        assert_eq!(fused.inputs, [Source::Arrangement(arrangement)]);
        assert_eq!(fused.step.flow().to_string(), expected_flow);
        assert_eq!(fused.step.output().arity(), expected_arity);
        assert_eq!(fused.step.unary_input().arity(), (1, 2));
        assert_eq!(
            fused.step.unary_input().fingerprint(),
            arranged.output().fingerprint()
        );
    }

    // Cases: filter on upstream rather than downstream.
    #[rstest]
    #[case::upstream_filter(true)]
    #[case::downstream_filter(false)]
    fn fusion_keeps_a_filtered_projection_explicit(#[case] filter_on_upstream: bool) {
        let column = |argument| {
            ArithmeticPos::from_var_signature(AtomArgumentSignature::new(
                AtomSignature::new(/*is_positive*/ true, 0),
                argument,
            ))
        };
        let projected_layout = KeyValueLayout::new(vec![], vec![column(1), column(2)]);
        let mut produced = HashMap::new();
        let mutability_of = |_| Some(Mutability::Static);
        let mut upstream_info = TransformationInfo::kv_to_kv(
            compute_fp("r"),
            "r".into(),
            "b_c".into(),
            /*is_row_input*/ true,
            KeyValueLayout::new(vec![], vec![column(0), column(1), column(2)]),
            projected_layout.clone(),
            KvPredicates::default(),
        );
        upstream_info.update_row_output(/*is_row_output*/ true);
        let mut upstream =
            Transformation::from_info(&upstream_info, &mut produced, &mutability_of).unwrap();
        let mut downstream_info = TransformationInfo::kv_to_kv(
            upstream.output().fingerprint(),
            "b_c".into(),
            "c".into(),
            /*is_row_input*/ true,
            projected_layout,
            KeyValueLayout::new(vec![], vec![column(2)]),
            KvPredicates::default(),
        );
        downstream_info.update_row_output(/*is_row_output*/ true);
        let mut downstream =
            Transformation::from_info(&downstream_info, &mut produced, &mutability_of).unwrap();
        let filtered = if filter_on_upstream {
            &mut upstream
        } else {
            &mut downstream
        };
        let Transformation::RowToRow {
            flow: TransformationFlow::KVToKV { constraints, .. },
            ..
        } = filtered
        else {
            panic!("row projections must have a column-mapping flow");
        };
        *constraints = Constraints::new(
            vec![],
            vec![(
                TransformationArgument::KV((false, 0)),
                TransformationArgument::KV((false, 1)),
            )],
        );
        let mut graph = ArrangementGraph::default();
        let input = graph.add_arrangement(Arrangement {
            origin: ArrangementOrigin::InputDedup {
                relation_fingerprint: compute_fp("r"),
            },
            layout: AttributeLayout::new(3, 0),
            inputs: Vec::new(),
        });
        let upstream_id = graph.add_feed(Feed {
            location: TransformationLocation {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                transformation_index: 0,
            },
            step: Cow::Borrowed(&upstream),
            inputs: vec![Source::Arrangement(input)],
            join_orientation: None,
        });
        let downstream_id = graph.add_feed(Feed {
            location: TransformationLocation {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                transformation_index: 1,
            },
            step: Cow::Borrowed(&downstream),
            inputs: vec![Source::Feed(upstream_id)],
            join_orientation: None,
        });

        fuse_feed(&mut graph);

        let result = &graph.feeds[downstream_id.0];
        assert_eq!(result.inputs, [Source::Feed(upstream_id)]);
        assert!(std::ptr::eq(result.step.as_ref(), &raw const downstream));
        assert!(std::ptr::eq(
            graph.feeds[upstream_id.0].step.as_ref(),
            &raw const upstream
        ));
    }
}
