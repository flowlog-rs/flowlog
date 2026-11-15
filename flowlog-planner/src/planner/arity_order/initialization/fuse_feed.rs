//! Fusion of projection feeds in dependency order.

use std::borrow::Cow;
use std::sync::Arc;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::Source;
use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

/// Fuses column-only maps without changing the original plans. The downstream
/// feed keeps its ID, location and output; its owned transformation and input
/// sources describe the composed computation together. Arrangement references
/// stop fusion, and logical identity maps remain explicit. Repeated calls
/// preserve the fused computations.
pub(super) fn fuse_feed(graph: &mut ArrangementGraph<'_>) {
    // Feed inputs precede their consumers, even across strata. Each upstream
    // step is already fused, so one pass replaces recursive traversal and
    // memoization. Feedback stops at an arrangement reference.
    for index in 0..graph.feeds.len() {
        let (earlier, remaining) = graph.feeds.split_at_mut(index);
        let feed = &mut remaining[0];
        let [Source::Feed(id)] = feed.inputs.as_slice() else {
            continue;
        };
        let upstream = &earlier[id.0];
        let [source] = upstream.inputs.as_slice() else {
            continue;
        };
        let Some(step) = fuse_projection(feed.step.as_ref(), upstream.step.as_ref()) else {
            continue;
        };
        feed.step = Cow::Owned(step);
        feed.inputs[0] = *source;
    }
}

/// Builds a projection over the earlier input, preserving the downstream
/// output. Only maps over row-producing projections can fuse; materialized
/// inputs, predicates and computed expressions keep their original steps.
#[must_use]
fn fuse_projection(
    downstream: &Transformation,
    upstream: &Transformation,
) -> Option<Transformation> {
    let arranged_output = match downstream {
        Transformation::RowToRow { .. } | Transformation::KvToRow { .. } => false,
        Transformation::RowToKv { .. } | Transformation::KvToKv { .. } => true,
        Transformation::JnToRow { .. }
        | Transformation::JnToKv { .. }
        | Transformation::NJnToRow { .. }
        | Transformation::NJnToKv { .. }
        | Transformation::AggToRow { .. }
        | Transformation::AggToKv { .. } => return None,
    };
    let (input, row_input) = match upstream {
        Transformation::RowToRow { input, .. } => (input, true),
        Transformation::KvToRow { input, .. } => (input, false),
        Transformation::RowToKv { .. }
        | Transformation::KvToKv { .. }
        | Transformation::JnToRow { .. }
        | Transformation::JnToKv { .. }
        | Transformation::NJnToRow { .. }
        | Transformation::NJnToKv { .. }
        | Transformation::AggToRow { .. }
        | Transformation::AggToKv { .. } => return None,
    };
    let flow = compose_projection(downstream.flow(), upstream.flow())?;
    let input = Arc::clone(input);
    let output = Arc::clone(downstream.output());
    Some(match (row_input, arranged_output) {
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
    })
}

/// Composes column-only flows over the earlier input. Predicates, computed
/// expressions, constants and missing source columns prevent composition.
#[must_use]
fn compose_projection(
    downstream: &TransformationFlow,
    upstream: &TransformationFlow,
) -> Option<TransformationFlow> {
    let TransformationFlow::KVToKV {
        key,
        value,
        constraints,
        compares,
    } = downstream
    else {
        return None;
    };
    let TransformationFlow::KVToKV {
        key: upstream_key,
        value: upstream_value,
        constraints: upstream_constraints,
        compares: upstream_compares,
    } = upstream
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
    let is_column = |argument: &ArithmeticArgument| {
        argument.rest.is_empty()
            && matches!(
                argument.init,
                FactorArgument::Var(TransformationArgument::KV(_))
            )
    };
    if !upstream_key
        .iter()
        .chain(upstream_value.iter())
        .all(is_column)
    {
        return None;
    }
    let substitute = |argument: &ArithmeticArgument| {
        if !argument.rest.is_empty() {
            return None;
        }
        let FactorArgument::Var(TransformationArgument::KV((is_key, index))) = &argument.init
        else {
            return None;
        };
        let columns = if *is_key {
            upstream_key
        } else {
            upstream_value
        };
        columns.get(*index).cloned()
    };
    let key: Option<Vec<ArithmeticArgument>> = key.iter().map(substitute).collect();
    let value: Option<Vec<ArithmeticArgument>> = value.iter().map(substitute).collect();
    Some(TransformationFlow::KVToKV {
        key: Arc::new(key?),
        value: Arc::new(value?),
        constraints: constraints.clone(),
        compares: Vec::new(),
    })
}
