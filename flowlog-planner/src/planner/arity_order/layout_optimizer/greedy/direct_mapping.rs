//! Field correspondence for direct alignment with one selected neighbor.
//!
//! Backward alignment maps the consumer's fields to an upstream region.
//! Forward alignment projects the cost model's scan order onto the output.

use std::collections::BTreeSet;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::JoinOrientation;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
use super::feed_cost::FeedCost;
use super::variables::Column;
use super::variables::Variable;
use super::variables::Variables;

/// Which neighbor supplies the requested field order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Neighbor {
    Downstream,
    Upstream,
}

#[derive(Debug, Default)]
pub(super) struct Mapping {
    pub(super) input_variables: BTreeSet<Variable>,
    pub(super) joins: Vec<usize>,
}

impl Mapping {
    /// Resolves a scoring boundary's variables independently of layout choices.
    #[must_use]
    pub(super) fn new(
        graph: &ArrangementGraph<'_>,
        variables: &Variables,
        cost: &FeedCost,
    ) -> Self {
        let mut mapping = Self::default();
        for source in cost.sources(graph) {
            match source {
                Source::Arrangement(id) => {
                    mapping.input_variables.extend(variables.arrangement(id))
                }
                Source::Feed(id) => {
                    if graph.feeds()[id.index()].join_orientation().is_some() {
                        mapping.joins.push(id.index());
                        mapping.input_variables.insert(Variable::Join(id.index()));
                    }
                }
                Source::Unit => {}
            }
        }
        mapping.joins.sort_unstable();
        mapping
    }

    /// Maps the selected neighbor's order into original field identities.
    /// Unavailable lineage contributes no request to an upstream layout.
    #[must_use]
    pub(super) fn requested_columns(
        &self,
        graph: &ArrangementGraph<'_>,
        cost: &FeedCost,
        choice: &LayoutChoice,
        neighbor: Neighbor,
    ) -> Option<Vec<Column>> {
        match neighbor {
            Neighbor::Downstream => cost.upstream_columns(graph, choice),
            Neighbor::Upstream => cost.scan_columns(graph, choice),
        }
    }
}

/// Toggles only the physical side choice; original graph metadata stays fixed.
pub(super) fn flip_join(graph: &ArrangementGraph<'_>, choice: &mut LayoutChoice, feed: usize) {
    let orientation = match choice.orientation(graph, feed) {
        JoinOrientation::Original => JoinOrientation::Swapped,
        JoinOrientation::Swapped => JoinOrientation::Original,
    };
    if Some(orientation) == graph.feeds()[feed].join_orientation() {
        choice.join_orientations.remove(&feed);
    } else {
        choice.join_orientations.insert(feed, orientation);
    }
}
