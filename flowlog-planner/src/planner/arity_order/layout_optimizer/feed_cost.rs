//! Conversion cost from one computation's upstream layouts to a sorting
//! boundary. Projection chains retain their original scan dimensions; joins
//! combine both inputs before the target layout is scored.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementId;
use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::JoinOrientation;
use super::super::arrangement_graph::Source;
use super::LayoutChoice;
use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;

/// One contribution to a sorting boundary, with field slots resolved before
/// evaluating candidate layouts.
#[derive(Debug)]
pub(super) struct FeedCost {
    output: ArrangementId,
    width: usize,
    column_slots: Vec<usize>,
    pub(super) source: Source,
    pub(super) recursive: bool,
}

impl FeedCost {
    #[must_use]
    pub(super) fn new(
        graph: &ArrangementGraph<'_>,
        output: ArrangementId,
        input_index: usize,
        recursive: bool,
    ) -> Self {
        let arrangement = &graph.arrangements()[output.index()];
        let source = arrangement.inputs()[input_index];
        let original = arrangement.layout();
        let width = match arrangement.origin() {
            ArrangementOrigin::Aggregation { .. } => original.key().len() + 1,
            ArrangementOrigin::Transformation(_) | ArrangementOrigin::Dedup { .. } => {
                original.key().len() + original.value().len()
            }
            // The exit folds keys and weights without decoding an answer row.
            ArrangementOrigin::AggregationExit { .. } => 0,
        };
        let columns = original.key().iter().chain(original.value());
        let slot_count = columns.max().map_or(0, |&column| column + 1);
        let column_slots = if matches!(arrangement.origin(), ArrangementOrigin::Dedup { .. })
            && matches!(source, Source::Feed(_))
        {
            let mut slots = vec![usize::MAX; slot_count];
            let columns = original.key().iter().enumerate();
            for (slot, &column) in columns {
                slots[column] = slot;
            }
            slots
        } else {
            (0..slot_count).collect()
        };
        Self {
            output,
            width,
            column_slots,
            source,
            recursive,
        }
    }
}

/// Estimates work as the square of traversed field slots, including projected
/// dimensions and repeated input occurrences. This unitless proxy is not a
/// runtime guarantee. Stops above `limit`, returning its saturating successor.
#[must_use]
pub(super) fn feed_work(graph: &ArrangementGraph<'_>, term: &FeedCost, limit: usize) -> usize {
    let mut fields = term.column_slots.len().saturating_add(1);
    let mut pending = vec![(term.source, term.width)];
    loop {
        let work = fields.saturating_mul(fields);
        if work > limit {
            return limit.saturating_add(1);
        }
        let Some((source, width)) = pending.pop() else {
            return work;
        };
        fields = fields.saturating_add(1);
        match source {
            Source::External(_) => fields = fields.saturating_add(width),
            Source::Arrangement(id) => {
                let layout = graph.arrangements()[id.index()].layout();
                let columns = layout.key().iter().chain(layout.value());
                let slots = columns.max().map_or(0, |&field| field.saturating_add(1));
                fields = fields.saturating_add(width.max(slots));
            }
            Source::Feed(id) => {
                let feed = &graph.feeds()[id.index()];
                let step = feed.step().as_ref();
                let flow = step.flow();
                fields = fields
                    .saturating_add(flow.key().len())
                    .saturating_add(flow.value().len());
                if step.is_unary() {
                    let (key, value) = step.unary_input().arity();
                    pending.push((feed.inputs()[0], key.saturating_add(value)));
                } else {
                    let (left, right) = step.binary_input();
                    let inputs = feed.inputs().iter().zip([left, right]);
                    for (&source, input) in inputs {
                        let (key, value) = input.arity();
                        pending.push((source, key.saturating_add(value)));
                    }
                }
            }
        }
    }
}

/// Scores one input contribution to an arrangement. Join inputs participate
/// jointly; separate union inputs and direct seeds have separate contributions.
/// Returns `None` for an unrepresentable conversion or a source exceeding
/// the binary model's 64-coordinate domain.
#[must_use]
pub(super) fn feed_cost(
    graph: &ArrangementGraph<'_>,
    term: &FeedCost,
    choice: &LayoutChoice,
    cache: &mut HashMap<Vec<usize>, u128>,
) -> Option<u128> {
    let row = source_row(graph, choice, term.source, term.width, &mut 0)?;
    let selected = choice.layout(graph, term.output);
    let mut target = Vec::new();
    let fields = selected.key().iter().chain(selected.value());
    for &field in fields {
        let position = *term.column_slots.get(field)?;
        target.push(*row.fields.get(position)?);
    }
    inversions(&row, &target, cache)
}

/// Counts binary-record inversions, retaining dropped coordinates in source
/// order. Completed relative permutations share cached costs across edges.
#[must_use]
fn inversions(
    row: &Row,
    target: &[Option<usize>],
    cache: &mut HashMap<Vec<usize>, u128>,
) -> Option<u128> {
    let width = row.order.len();
    if width > 64 {
        return None;
    }
    let mut seen = vec![false; width];
    let mut permutation = Vec::with_capacity(width);
    for &coordinate in target.iter().flatten() {
        let position = row.order.iter().position(|&field| field == coordinate)?;
        if !seen[position] {
            seen[position] = true;
            permutation.push(position);
        }
    }
    // Keeping the dropped tail in source order completes the permutation
    // without introducing extra inversions between absent coordinates.
    let absent = seen.iter().enumerate();
    for (position, &seen) in absent {
        if !seen {
            permutation.push(position);
        }
    }
    if let Some(&cost) = cache.get(&permutation) {
        return Some(cost);
    }
    let mut remaining = (1_u128 << width) - 1;
    let mut cost = 0_u128;
    let targets = permutation.iter().enumerate();
    for (depth, &position) in targets {
        let preceding = (1_u128 << position) - 1;
        let remaining_before = (remaining & preceding).count_ones() as usize;
        if remaining_before > 0 {
            // Count pairs whose first differing target bit is this one,
            // but whose first differing source bit precedes it.
            let shift = 2 * width - 3 - depth - remaining_before;
            let prefixes = (1_u128 << remaining_before) - 1;
            cost = cost.checked_add(prefixes.checked_shl(shift.try_into().ok()?)?)?;
        }
        remaining &= !(1_u128 << position);
    }
    cache.insert(permutation, cost);
    Some(cost)
}

/// Logical slots name coordinates, while order retains the original scan.
/// A constant has no independent coordinate; copied slots can share one.
#[derive(Debug)]
struct Row {
    fields: Vec<Option<usize>>,
    order: Vec<usize>,
}

impl Row {
    #[must_use]
    fn keys(&self, arity: usize) -> Option<Vec<usize>> {
        let keys = self.fields.get(..arity)?;
        let mut permutation = Vec::with_capacity(arity);
        for &coordinate in &self.order {
            if let Some(position) = keys.iter().position(|&key| key == Some(coordinate)) {
                permutation.push(position);
            }
        }
        (permutation.len() == arity).then_some(permutation)
    }

    #[must_use]
    fn slot(&self, key_arity: usize, is_key: bool, index: usize) -> Option<Option<usize>> {
        self.fields
            .get(if is_key { index } else { key_arity + index })
            .copied()
    }
}

#[must_use]
fn source_row(
    graph: &ArrangementGraph<'_>,
    choice: &LayoutChoice,
    source: Source,
    width: usize,
    next: &mut usize,
) -> Option<Row> {
    match source {
        Source::External(_) => {
            let order: Vec<usize> = (*next..*next + width).collect();
            *next += width;
            Some(Row {
                fields: order.iter().copied().map(Some).collect(),
                order,
            })
        }
        Source::Arrangement(id) => {
            let layout = choice.layout(graph, id);
            let selected = layout.key().iter().chain(layout.value());
            let slot_count = width.max(selected.clone().max().map_or(0, |&field| field + 1));
            let mut row = Row {
                fields: vec![None; slot_count],
                order: Vec::with_capacity(slot_count),
            };
            for &field in selected {
                if row.fields[field].is_some() {
                    return None;
                }
                row.fields[field] = Some(*next);
                row.order.push(*next);
                *next += 1;
            }
            // A static min/max answer lives in the weight. Reading it as a
            // column creates an independent value after the physical key.
            let requested_fields = row.fields.iter_mut().take(width);
            for field in requested_fields {
                if field.is_none() {
                    *field = Some(*next);
                    row.order.push(*next);
                    *next += 1;
                }
            }
            Some(row)
        }
        Source::Feed(id) => {
            let feed = &graph.feeds()[id.index()];
            let step = feed.step().as_ref();
            let inputs = if step.is_unary() {
                vec![step.unary_input().as_ref()]
            } else {
                let (left, right) = step.binary_input();
                vec![left.as_ref(), right.as_ref()]
            };
            let mut rows = Vec::with_capacity(inputs.len());
            let sources = feed.inputs().iter().zip(&inputs);
            for (&source, input) in sources {
                let (key, value) = input.arity();
                rows.push(source_row(graph, choice, source, key + value, next)?);
            }
            let mut order = match step {
                Transformation::RowToRow { .. }
                | Transformation::RowToKv { .. }
                | Transformation::KvToRow { .. }
                | Transformation::KvToKv { .. } => std::mem::take(&mut rows.first_mut()?.order),
                Transformation::JnToRow { .. }
                | Transformation::JnToKv { .. }
                | Transformation::NJnToRow { .. }
                | Transformation::NJnToKv { .. } => {
                    let key_arity = inputs[0].arity().0;
                    if key_arity != inputs[1].arity().0
                        || rows[0].keys(key_arity)? != rows[1].keys(key_arity)?
                    {
                        return None;
                    }
                    let left_keys = rows[0].fields[..key_arity].to_vec();
                    let right_keys = rows[1].fields[..key_arity].to_vec();
                    let replacements: BTreeMap<usize, usize> = right_keys
                        .iter()
                        .zip(&left_keys)
                        .map(|(&right, &left)| Some((right?, left?)))
                        .collect::<Option<_>>()?;
                    for field in rows[1].fields.iter_mut().flatten() {
                        if let Some(&left) = replacements.get(field) {
                            *field = left;
                        }
                    }
                    for coordinate in &mut rows[1].order {
                        if let Some(&left) = replacements.get(coordinate) {
                            *coordinate = left;
                        }
                    }
                    let is_negative = matches!(
                        step,
                        Transformation::NJnToRow { .. } | Transformation::NJnToKv { .. }
                    );
                    if is_negative {
                        // Antijoin output KV slots belong to the right input.
                        rows[0].fields.truncate(key_arity);
                        std::mem::take(&mut rows[1].order)
                    } else {
                        let mut order = Vec::new();
                        let key_coordinates: BTreeSet<usize> =
                            left_keys.into_iter().flatten().collect();
                        order.extend(
                            rows[0]
                                .order
                                .iter()
                                .filter(|key| key_coordinates.contains(key))
                                .copied(),
                        );
                        let sides =
                            if choice.orientation(graph, id.index()) == JoinOrientation::Swapped {
                                [1, 0]
                            } else {
                                [0, 1]
                            };
                        for side in sides {
                            order.extend(
                                rows[side]
                                    .order
                                    .iter()
                                    .filter(|coordinate| !key_coordinates.contains(coordinate))
                                    .copied(),
                            );
                        }
                        order
                    }
                }
                Transformation::AggToRow { .. } | Transformation::AggToKv { .. } => {
                    let key_arity = inputs[0].arity().0;
                    let keys = rows[0].keys(key_arity)?;
                    if rows.len() == 2 && keys != rows[1].keys(key_arity)? {
                        return None;
                    }
                    let fields = &mut rows[0].fields;
                    fields.truncate(key_arity);
                    let mut order: Vec<usize> =
                        keys.into_iter().filter_map(|slot| fields[slot]).collect();
                    order.push(*next);
                    fields.push(Some(*next));
                    *next += 1;
                    order
                }
            };
            let is_negative = matches!(
                step,
                Transformation::NJnToRow { .. } | Transformation::NJnToKv { .. }
            );
            let resolve = |argument| match argument {
                TransformationArgument::KV((is_key, index)) => {
                    let side = usize::from(is_negative);
                    rows.get(side)?.slot(inputs[side].arity().0, is_key, index)
                }
                TransformationArgument::Jn((left, is_key, index)) => {
                    let side = usize::from(!left);
                    rows.get(side)?.slot(inputs[side].arity().0, is_key, index)
                }
            };
            let flow = step.flow();
            let mut fields = Vec::with_capacity(flow.key().len() + flow.value().len());
            let expressions = flow.key().iter().chain(flow.value().iter());
            for expression in expressions {
                fields.push(expression_coordinate(
                    expression, &resolve, &mut order, next,
                )?);
            }
            Some(Row { fields, order })
        }
    }
}

/// Treats computed outputs as independent trailing dimensions. Correlations
/// from arithmetic and predicates are outside this binary permutation model.
#[must_use]
fn expression_coordinate(
    expression: &ArithmeticArgument,
    resolve: &impl Fn(TransformationArgument) -> Option<Option<usize>>,
    order: &mut Vec<usize>,
    next: &mut usize,
) -> Option<Option<usize>> {
    if expression.rest.is_empty()
        && let FactorArgument::Var(argument) = expression.init
    {
        return resolve(argument);
    }
    if expression.rest.is_empty() && matches!(expression.init, FactorArgument::Const(_)) {
        return Some(None);
    }
    let coordinate = *next;
    *next += 1;
    order.push(coordinate);
    Some(Some(coordinate))
}
