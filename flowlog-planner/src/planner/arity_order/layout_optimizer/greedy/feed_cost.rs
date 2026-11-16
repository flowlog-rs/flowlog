//! Conversion cost from one computation's upstream layouts to a sorting
//! boundary. Projection chains retain their original scan dimensions; joins
//! combine both inputs before the target layout is scored.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::ArrangementId;
use super::super::super::arrangement_graph::ArrangementOrigin;
use super::super::super::arrangement_graph::FeedId;
use super::super::super::arrangement_graph::JoinOrientation;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
use super::variables::Column;
use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;

/// One contribution to a sorting boundary, with field slots resolved before
/// evaluating candidate layouts.
#[derive(Debug, Clone)]
pub(super) struct FeedCost {
    output: Option<ArrangementId>,
    join_output: Option<FeedId>,
    row_bindings: BTreeMap<usize, Option<ArrangementId>>,
    width: usize,
    column_slots: Vec<usize>,
    pub(super) source: Source,
    pub(super) recursive: bool,
}

impl FeedCost {
    #[must_use]
    pub(super) fn output(&self) -> Option<ArrangementId> {
        self.output
    }

    #[must_use]
    pub(super) fn stratum_index(&self, graph: &ArrangementGraph<'_>) -> Option<usize> {
        self.join_output.map_or_else(
            || {
                graph.arrangements()[self.output?.index()]
                    .origin()
                    .stratum_index()
            },
            |feed| Some(graph.feeds()[feed.index()].location().stratum_index()),
        )
    }

    /// Orders output columns by their coordinates in the upstream scan.
    /// Constants retain their relative order after all varying coordinates.
    #[must_use]
    pub(super) fn scan_columns(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &LayoutChoice,
    ) -> Option<Vec<Column>> {
        let output = self.output?;
        let row = self.source_row(graph, choice)?;
        let layout = choice.layout(graph, output);
        let mut columns = Vec::new();
        let fields = layout.key().iter().chain(layout.value());
        for &field in fields {
            let coordinate = *row.fields.get(*self.column_slots.get(field)?)?;
            let rank = coordinate
                .and_then(|coordinate| row.order.iter().position(|&field| field == coordinate))
                .unwrap_or(usize::MAX);
            columns.push((field, rank));
        }
        columns.sort_by_key(|&(_, rank)| rank);
        Some(
            columns
                .into_iter()
                .map(|(field, _)| Column {
                    arrangement: output,
                    field,
                })
                .collect(),
        )
    }

    /// Maps the selected target order to the physical regions supplying it.
    #[must_use]
    pub(super) fn upstream_columns(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &LayoutChoice,
    ) -> Option<Vec<Column>> {
        let row = self.source_row(graph, choice)?;
        Some(
            self.target_slots(graph, choice)?
                .into_iter()
                .filter_map(|slot| row.origins.get(slot).copied().flatten())
                .collect(),
        )
    }

    /// Returns layout dependencies, stopping at locally sorted join outputs.
    #[must_use]
    pub(super) fn sources(&self, graph: &ArrangementGraph<'_>) -> Vec<Source> {
        let mut sources = Vec::new();
        let mut pending = vec![self.source];
        let mut visited = BTreeSet::new();
        while let Some(source) = pending.pop() {
            if let Source::Feed(id) = source {
                if !visited.insert(id.index()) {
                    continue;
                }
                if self.join_output != Some(id)
                    && let Some(binding) = self.row_bindings.get(&id.index())
                {
                    if let Some(binding) = binding {
                        sources.push(Source::Arrangement(*binding));
                    }
                    continue;
                }
                pending.extend_from_slice(graph.feeds()[id.index()].inputs());
            }
            sources.push(source);
        }
        sources
    }

    /// Returns physical dependencies without adding derived layout backedges.
    #[must_use]
    pub(super) fn physical_sources(&self, graph: &ArrangementGraph<'_>) -> Vec<Source> {
        let mut sources = Vec::new();
        let mut pending = vec![self.source];
        let mut visited = BTreeSet::new();
        while let Some(source) = pending.pop() {
            if let Source::Feed(id) = source {
                if !visited.insert(id.index()) {
                    continue;
                }
                pending.extend_from_slice(graph.feeds()[id.index()].inputs());
            }
            sources.push(source);
        }
        sources
    }

    #[must_use]
    fn source_row(&self, graph: &ArrangementGraph<'_>, choice: &LayoutChoice) -> Option<Row> {
        source_row(
            graph,
            choice,
            self.source,
            self.width,
            &mut 0,
            self.join_output,
            &self.row_bindings,
        )
    }

    #[must_use]
    fn target_slots(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &LayoutChoice,
    ) -> Option<Vec<usize>> {
        let Some(output) = self.output else {
            return Some((0..self.width).collect());
        };
        let layout = choice.layout(graph, output);
        layout
            .key()
            .iter()
            .chain(layout.value())
            .map(|&field| self.column_slots.get(field).copied())
            .collect()
    }

    /// Registers a join buffer's existing order, without a new layout variable.
    #[must_use]
    pub(super) fn join(graph: &ArrangementGraph<'_>, feed: FeedId, recursive: bool) -> Self {
        let row_bindings = row_bindings(graph, Source::Feed(feed), Some(feed));
        let output = row_bindings[&feed.index()];
        let width = graph.feeds()[feed.index()].step().output().arity().1;
        let column_slots = output.map_or_else(
            || (0..width).collect(),
            |output| {
                let layout = graph.arrangements()[output.index()].layout();
                let mut slots = vec![usize::MAX; width];
                let columns = layout.key().iter().enumerate();
                for (slot, &column) in columns {
                    slots[column] = slot;
                }
                slots
            },
        );
        Self {
            output,
            join_output: Some(feed),
            row_bindings,
            width,
            column_slots,
            source: Source::Feed(feed),
            recursive,
        }
    }

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
            ArrangementOrigin::InputDedup { .. }
            | ArrangementOrigin::Transformation(_)
            | ArrangementOrigin::Dedup { .. } => original.key().len() + original.value().len(),
            // The exit folds keys and weights without decoding an answer row.
            ArrangementOrigin::AggregationExit { .. } => 0,
        };
        let columns = original.key().iter().chain(original.value());
        let slot_count = columns.max().map_or(0, |&column| column + 1);
        let column_slots = if matches!(
            arrangement.origin(),
            ArrangementOrigin::InputDedup { .. } | ArrangementOrigin::Dedup { .. }
        ) && matches!(source, Source::Feed(_))
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
            output: Some(output),
            join_output: None,
            row_bindings: row_bindings(graph, source, None),
            width,
            column_slots,
            source,
            recursive,
        }
    }
}

/// Direct rule results inherit their binding's declaration-column order.
/// Other row joins keep the original output order. Plain projections do not
/// introduce a sorting boundary.
#[must_use]
fn row_bindings(
    graph: &ArrangementGraph<'_>,
    source: Source,
    raw_join: Option<FeedId>,
) -> BTreeMap<usize, Option<ArrangementId>> {
    let mut bindings = BTreeMap::new();
    let mut pending = vec![source];
    let mut visited = BTreeSet::new();
    while let Some(source) = pending.pop() {
        let Source::Feed(feed) = source else {
            continue;
        };
        if !visited.insert(feed.index()) {
            continue;
        }
        let step = &graph.feeds()[feed.index()];
        if matches!(step.step().as_ref(), Transformation::JnToRow { .. }) {
            let binding =
                graph
                    .arrangements()
                    .iter()
                    .enumerate()
                    .find_map(|(index, arrangement)| {
                        (matches!(arrangement.origin(), ArrangementOrigin::Dedup { .. })
                            && arrangement.inputs().contains(&Source::Feed(feed)))
                        .then_some(ArrangementId::new(index))
                    });
            bindings.insert(feed.index(), binding);
            if raw_join != Some(feed) {
                continue;
            }
        }
        pending.extend_from_slice(step.inputs());
    }
    bindings
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
            Source::Unit => {}
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
                if term.join_output != Some(id) && term.row_bindings.contains_key(&id.index()) {
                    continue;
                }
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
    let row = term.source_row(graph, choice)?;
    let target: Option<Vec<_>> = term
        .target_slots(graph, choice)?
        .into_iter()
        .map(|slot| row.fields.get(slot).copied())
        .collect();
    inversions(&row, &target?, cache)
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
    origins: Vec<Option<Column>>,
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
    raw_join: Option<FeedId>,
    row_bindings: &BTreeMap<usize, Option<ArrangementId>>,
) -> Option<Row> {
    match source {
        Source::Unit => (width == 0).then_some(Row {
            fields: Vec::new(),
            order: Vec::new(),
            origins: Vec::new(),
        }),
        Source::Arrangement(id) => {
            let layout = choice.layout(graph, id);
            let selected = layout.key().iter().chain(layout.value());
            let slot_count = width.max(selected.clone().max().map_or(0, |&field| field + 1));
            let mut row = Row {
                fields: vec![None; slot_count],
                order: Vec::with_capacity(slot_count),
                origins: vec![None; slot_count],
            };
            for &field in selected {
                if row.fields[field].is_some() {
                    return None;
                }
                row.fields[field] = Some(*next);
                row.origins[field] = Some(Column {
                    arrangement: id,
                    field,
                });
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
            if raw_join != Some(id)
                && let Some(binding) = row_bindings.get(&id.index())
            {
                let width = step.output().arity().1;
                let mut row = Row {
                    fields: vec![None; width],
                    order: Vec::with_capacity(width),
                    origins: vec![None; width],
                };
                let order: Vec<_> = if let Some(binding) = binding {
                    let original = graph.arrangements()[binding.index()].layout();
                    let selected = choice.layout(graph, *binding);
                    let mut slots = Vec::with_capacity(width);
                    for &field in selected.key() {
                        let slot = original.key().iter().position(|&column| column == field)?;
                        row.origins[slot] = Some(Column {
                            arrangement: *binding,
                            field,
                        });
                        slots.push(slot);
                    }
                    slots
                } else {
                    (0..width).collect()
                };
                for slot in order {
                    row.fields[slot] = Some(*next);
                    row.order.push(*next);
                    *next += 1;
                }
                return Some(row);
            }
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
                rows.push(source_row(
                    graph,
                    choice,
                    source,
                    key + value,
                    next,
                    None,
                    row_bindings,
                )?);
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
                    rows[0].origins.truncate(key_arity);
                    rows[0].origins.push(None);
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
            let mut origins = Vec::with_capacity(fields.capacity());
            let expressions = flow.key().iter().chain(flow.value().iter());
            for expression in expressions {
                fields.push(expression_coordinate(
                    expression, &resolve, &mut order, next,
                )?);
                let origin = if expression.rest.is_empty()
                    && let FactorArgument::Var(argument) = expression.init
                {
                    let (side, is_key, slot) = match argument {
                        TransformationArgument::KV((is_key, slot)) => {
                            (usize::from(is_negative), is_key, slot)
                        }
                        TransformationArgument::Jn((left, is_key, slot)) => {
                            (usize::from(!left), is_key, slot)
                        }
                    };
                    let slot = if is_key {
                        slot
                    } else {
                        inputs[side].arity().0 + slot
                    };
                    rows[side].origins.get(slot).copied().flatten()
                } else {
                    None
                };
                origins.push(origin);
            }
            Some(Row {
                fields,
                order,
                origins,
            })
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use flowlog_parser::Mutability;

    use super::super::calc_cost::prepare_costs;
    use super::*;
    use crate::planner::CanonicalForm;
    use crate::planner::Collection;
    use crate::planner::Constraints;
    use crate::planner::KeyValueLayout;
    use crate::planner::TransformationFlow;
    use crate::planner::arity_order::arrangement_graph::Arrangement;
    use crate::planner::arity_order::arrangement_graph::AttributeLayout;
    use crate::planner::arity_order::arrangement_graph::Feed;
    use crate::planner::arity_order::arrangement_graph::Scope;
    use crate::planner::arity_order::arrangement_graph::TransformationLocation;

    fn collection(fingerprint: u64, key: usize, value: usize) -> Arc<Collection> {
        let canonical = CanonicalForm::relation("r", key + value);
        let columns = canonical.value();
        Arc::new(Collection::new(
            fingerprint,
            "r".into(),
            KeyValueLayout::new(columns[..key].to_vec(), columns[key..].to_vec()),
            canonical,
            Mutability::Static,
        ))
    }

    fn argument(init: TransformationArgument) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(init),
            rest: Vec::new(),
        }
    }

    #[test]
    fn join_buffer_is_scored_once_before_three_unsorted_projections() {
        let mut graph = ArrangementGraph::default();
        let location = |index| TransformationLocation::new(0, Scope::NonRecursive, index);
        let inputs: Vec<_> = (0..2)
            .map(|index| {
                graph.add_arrangement(Arrangement::new(
                    ArrangementOrigin::Transformation(location(index)),
                    AttributeLayout::new(1, 1),
                    Vec::new(),
                ))
            })
            .collect();
        let join = Transformation::JnToRow {
            input: (collection(0, 1, 1), collection(1, 1, 1)),
            output: collection(2, 0, 3),
            flow: TransformationFlow::JnToKV {
                key: Arc::new(Vec::new()),
                value: Arc::new(vec![
                    argument(TransformationArgument::Jn((true, true, 0))),
                    argument(TransformationArgument::Jn((true, false, 0))),
                    argument(TransformationArgument::Jn((false, false, 0))),
                ]),
                compares: Vec::new(),
            },
        };
        let join = graph.add_feed(Feed::new(
            location(2),
            Cow::Owned(join),
            inputs.into_iter().map(Source::Arrangement).collect(),
            Some(JoinOrientation::Original),
        ));
        let projections = [vec![0, 1], vec![1, 2], vec![0]].into_iter().enumerate();
        for (index, columns) in projections {
            let projection = Transformation::RowToKv {
                input: collection(2, 0, 3),
                output: collection(3 + index as u64, columns.len(), 0),
                flow: TransformationFlow::KVToKV {
                    key: Arc::new(
                        columns
                            .iter()
                            .map(|&column| argument(TransformationArgument::KV((false, column))))
                            .collect(),
                    ),
                    value: Arc::new(Vec::new()),
                    constraints: Constraints::new(Vec::new(), Vec::new()),
                    compares: Vec::new(),
                },
            };
            let feed = graph.add_feed(Feed::new(
                location(3 + index),
                Cow::Owned(projection),
                vec![Source::Feed(join)],
                None,
            ));
            let _ = graph.add_arrangement(Arrangement::new(
                ArrangementOrigin::Transformation(location(3 + index)),
                AttributeLayout::new(columns.len(), 0),
                vec![Source::Feed(feed)],
            ));
        }
        let costs = prepare_costs(&graph);
        let mut cache = HashMap::new();
        let actual: Vec<_> = costs
            .iter()
            .map(|cost| feed_cost(&graph, cost, &LayoutChoice::default(), &mut cache).unwrap())
            .collect();
        assert_eq!(actual, [0, 6, 0, 0]);
        assert_eq!(graph.arrangements().len(), 5);

        let binding = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: 3,
            },
            AttributeLayout::from_columns(vec![2, 0, 1], Vec::new()),
            vec![Source::Feed(join)],
        ));
        let mut choice = LayoutChoice::default();
        choice.layouts.insert(
            binding,
            AttributeLayout::from_columns(vec![1, 2, 0], Vec::new()),
        );
        let cost = FeedCost::join(&graph, join, /*recursive*/ false);
        assert_eq!(cost.target_slots(&graph, &choice), Some(vec![2, 0, 1]));
        let actual: Vec<_> = prepare_costs(&graph)
            .iter()
            .map(|cost| feed_cost(&graph, cost, &choice, &mut cache).unwrap())
            .collect();
        assert_eq!(actual, [6, 6, 4, 0, 6]);
    }
}
