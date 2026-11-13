//! Per-stratum topology for layout solving.
//!
//! The undirected graph contains arrangement variables; edges borrow their
//! original computation feeds. Contraction refines the arity groups that
//! define permutation domains before direct-mapping search. Selected
//! choices accumulate separately until all strata have been searched.

mod block_contraction;
mod greedy;
mod refinement;
mod stratum_graph;

use std::collections::BTreeMap;

pub(super) use self::greedy::optimize;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::ArrangementId;
use super::arrangement_graph::AttributeLayout;
use super::arrangement_graph::JoinOrientation;

/// Candidate choices retain original field IDs and feed indices. They do not
/// own or rewrite computations; absent entries keep the graph's choices.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LayoutChoice {
    layouts: BTreeMap<ArrangementId, AttributeLayout>,
    join_orientations: BTreeMap<usize, JoinOrientation>,
}

impl LayoutChoice {
    #[must_use]
    fn layout<'layout>(
        &'layout self,
        graph: &'layout ArrangementGraph<'_>,
        id: ArrangementId,
    ) -> &'layout AttributeLayout {
        self.layouts
            .get(&id)
            .unwrap_or_else(|| graph.arrangements()[id.index()].layout())
    }

    #[must_use]
    fn orientation(&self, graph: &ArrangementGraph<'_>, feed_index: usize) -> JoinOrientation {
        self.join_orientations
            .get(&feed_index)
            .copied()
            .or_else(|| graph.feeds()[feed_index].join_orientation())
            .unwrap_or(JoinOrientation::Original)
    }

    /// Installs accumulated choices after all scoring and refinement finish.
    /// Original field correspondence must remain available until then.
    pub(super) fn apply(self, graph: &mut ArrangementGraph<'_>) {
        for (id, layout) in self.layouts {
            *graph.arrangements_mut()[id.index()].layout_mut() = layout;
        }
        for (index, orientation) in self.join_orientations {
            *graph.feeds_mut()[index].join_orientation_mut() = Some(orientation);
        }
    }
}
