//! Projection fusion with one cached result per transformation identity.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::Producer;
use super::arrangement_graph::TransformationId;
use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

/// Composes pure projection chains in every arrangement feed. Shared
/// transformations are visited once; arrangements and external inputs stop
/// traversal. The original plans stay unchanged, and consumers retain
/// independent projections.
pub(super) fn fuse_projection_feeds(graph: &mut ArrangementGraph<'_>) {
    let mut fusion = TransformationFusion::default();
    for edge in &mut graph.edges {
        edge.input = fusion.fuse(&edge.input);
    }
}

// =============================================================================
// TransformationFusion
// =============================================================================

/// Retains each transformation's fused feed across consumers. Identities
/// include stratum, scope and binding position, keeping seeds separate from
/// recursive reads even when their collections use the same fingerprint.
#[derive(Debug, Default)]
struct TransformationFusion<'plan> {
    fused_feed_map: BTreeMap<TransformationId, Producer<'plan>>,
}

impl<'plan> TransformationFusion<'plan> {
    /// Returns a feed with pure projections composed and cached upstream
    /// sources shared. Materialized boundaries remain references, so
    /// recursive feedback never unfolds into this traversal.
    #[must_use]
    fn fuse(&mut self, producer: &Producer<'plan>) -> Producer<'plan> {
        match producer {
            Producer::External(_) | Producer::Arrangement(_) => producer.clone(),
            Producer::Transformation { id, step, sources } => {
                if let Some(cached) = self.fused_feed_map.get(id) {
                    return cached.clone();
                }
                let sources: Vec<Producer<'plan>> =
                    sources.iter().map(|source| self.fuse(source)).collect();
                let fused = fuse_projection(*id, step.clone(), sources.into());
                self.fused_feed_map.insert(*id, fused.clone());
                fused
            }
            Producer::Union(parts) => {
                Producer::Union(parts.iter().map(|part| self.fuse(part)).collect())
            }
        }
    }
}

// =============================================================================
// Projection composition
// =============================================================================

/// Collapses pure projection chains within a feed. Physical arrangements,
/// filters and computed expressions stop fusion. Shared feeds stay intact.
#[must_use]
fn fuse_projection<'plan>(
    id: TransformationId,
    mut step: Cow<'plan, Transformation>,
    mut sources: Rc<[Producer<'plan>]>,
) -> Producer<'plan> {
    // Codegen emits an identity row map as an alias, with no runtime operator.
    // Keep its existing producer so binding projections do not hide a join
    // or create a false conversion after an arrangement.
    if let Transformation::RowToRow { input, flow, .. } = step.as_ref() {
        let identity = flow.value().iter().enumerate().all(|(column, argument)| {
            argument.rest.is_empty()
                && argument.init == FactorArgument::Var(TransformationArgument::KV((false, column)))
        });
        if sources.len() == 1
            && flow.key().is_empty()
            && flow.value().len() == input.arity().1
            && flow.constraints().is_empty()
            && flow.compares().is_empty()
            && identity
        {
            return sources[0].clone();
        }
    }
    while step.is_map() {
        let [
            Producer::Transformation {
                step: upstream_step,
                sources: upstream_sources,
                ..
            },
        ] = sources.as_ref()
        else {
            break;
        };
        let (input, input_is_row) = match upstream_step.as_ref() {
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
        let Some(flow) = compose_projection(step.flow(), upstream_step.flow()) else {
            break;
        };
        let input = Arc::clone(input);
        let output = Arc::clone(step.output());
        let output_is_arranged = step.need_arrange();
        step = Cow::Owned(match (input_is_row, output_is_arranged) {
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
        sources = Rc::clone(upstream_sources);
    }
    Producer::Transformation { id, step, sources }
}

/// Composes two column-only maps over the earlier input. Returns `None` if
/// either map has a predicate or computed expression, if a flow is not a map,
/// or if a projected column has no source position.
#[must_use]
fn compose_projection(
    downstream_flow: &TransformationFlow,
    upstream_flow: &TransformationFlow,
) -> Option<TransformationFlow> {
    let TransformationFlow::KVToKV {
        key,
        value,
        constraints,
        compares,
    } = downstream_flow
    else {
        return None;
    };
    let TransformationFlow::KVToKV {
        key: upstream_key,
        value: upstream_value,
        constraints: upstream_constraints,
        compares: upstream_compares,
    } = upstream_flow
    else {
        return None;
    };
    if !constraints.is_empty()
        || !compares.is_empty()
        || !upstream_constraints.is_empty()
        || !upstream_compares.is_empty()
    {
        return None;
    }
    let is_column_reference = |argument: &ArithmeticArgument| {
        argument.rest.is_empty()
            && matches!(
                argument.init,
                FactorArgument::Var(TransformationArgument::KV(_))
            )
    };
    if !upstream_key
        .iter()
        .chain(upstream_value.iter())
        .all(is_column_reference)
    {
        return None;
    }
    let substitute_column = |argument: &ArithmeticArgument| {
        if !argument.rest.is_empty() {
            return None;
        }
        let FactorArgument::Var(TransformationArgument::KV((is_key, index))) = &argument.init
        else {
            return None;
        };
        let fields = if *is_key {
            upstream_key
        } else {
            upstream_value
        };
        fields.get(*index).cloned()
    };
    let key: Option<Vec<ArithmeticArgument>> = key.iter().map(substitute_column).collect();
    let value: Option<Vec<ArithmeticArgument>> = value.iter().map(substitute_column).collect();
    Some(TransformationFlow::KVToKV {
        key: Arc::new(key?),
        value: Arc::new(value?),
        constraints: constraints.clone(),
        compares: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_parser::Mutability;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::planner::Collection;
    use crate::planner::Constraints;
    use crate::planner::arity_order::arrangement_graph::Arities;
    use crate::planner::arity_order::arrangement_graph::Edge;
    use crate::planner::arity_order::arrangement_graph::PhysicalArrangement;
    use crate::planner::arity_order::arrangement_graph::Scope;

    #[test]
    fn consumers_share_fused_upstream_sources_but_keep_their_own_projections() {
        let program = program(".decl E(a:int32,b:int32)\n.input E\n.output E\n");
        let input = Arc::new(Collection::from_relation(
            program.edbs()[0],
            Mutability::Static,
        ));
        let mut computed = Transformation::row_projection(Arc::clone(&input), 1, &[0, 1]).unwrap();
        let Transformation::RowToRow {
            flow: TransformationFlow::KVToKV { value, .. },
            ..
        } = &mut computed
        else {
            panic!("row_projection produces a RowToRow map");
        };
        Arc::make_mut(value)[0].rest.push((
            ArithmeticOperator::Plus,
            FactorArgument::Const(Constant::new(DataType::Int32, "1")),
        ));
        let shared = Producer::Transformation {
            id: TransformationId::Rule {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                transformation_index: 0,
            },
            step: Cow::Borrowed(&computed),
            sources: vec![Producer::External(input.fingerprint())].into(),
        };
        let reordered =
            Transformation::row_projection(Arc::clone(computed.output()), 2, &[1, 0]).unwrap();
        let projected =
            Transformation::row_projection(Arc::clone(computed.output()), 3, &[0]).unwrap();
        let mut graph = ArrangementGraph::default();
        let consumers = [&reordered, &projected].into_iter().enumerate();
        for (index, projection) in consumers {
            let node = PhysicalArrangement::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: projection.output().fingerprint(),
            };
            graph
                .nodes
                .insert(node.clone(), Arities::new(projection.output().arity().1, 0));
            graph.edges.push(Edge {
                input: Producer::Transformation {
                    id: TransformationId::Rule {
                        stratum_index: 0,
                        scope: Scope::NonRecursive,
                        transformation_index: index + 1,
                    },
                    step: Cow::Borrowed(projection),
                    sources: vec![shared.clone()].into(),
                },
                output: node,
            });
        }

        fuse_projection_feeds(&mut graph);

        let Producer::Transformation {
            step: reordered,
            sources: reordered_sources,
            ..
        } = &graph.edges[0].input
        else {
            panic!("the first consumer retains its projection");
        };
        let Producer::Transformation {
            step: projected,
            sources: projected_sources,
            ..
        } = &graph.edges[1].input
        else {
            panic!("the second consumer retains its projection");
        };
        assert_eq!(reordered.flow().to_string(), "V:(V1, V0)");
        assert_eq!(projected.flow().to_string(), "V:(V0)");
        let [Producer::Transformation { sources: left, .. }] = reordered_sources.as_ref() else {
            panic!("the first consumer reads the computed producer");
        };
        let [Producer::Transformation { sources: right, .. }] = projected_sources.as_ref() else {
            panic!("the second consumer reads the computed producer");
        };
        assert!(Rc::ptr_eq(left, right));
    }

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
}
