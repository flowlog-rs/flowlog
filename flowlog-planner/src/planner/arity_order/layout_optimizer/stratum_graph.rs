//! Arrangement layout variables and their undirected dependencies.
//!
//! Edges borrow the original feeds instead of introducing computation nodes.
//! Input paths retain join ports and projection chains for joint transitions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementId;
use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::Feed;
use super::super::arrangement_graph::Source;

/// A nonempty ordered group of original field IDs. Permutation moves whole
/// groups and preserves their internal order; refinement may split groups.
#[derive(Debug)]
pub(super) struct ArityGroup {
    pub(super) arities: Vec<usize>,
}

/// One arrangement's independently permutable key and value groups. Groups
/// cannot cross the key/value boundary, and fixed layouts cannot change.
#[derive(Debug)]
pub(super) struct ArrangementNode {
    pub(super) key: Vec<ArityGroup>,
    pub(super) value: Vec<ArityGroup>,
    pub(super) edges: Vec<usize>,
    pub(super) fixed: bool,
}

impl ArrangementNode {
    /// Initializes each nonempty key or value as one complete group in its
    /// original field order, independently of whether the layout is fixed.
    #[must_use]
    fn new(layout: &AttributeLayout, fixed: bool) -> Self {
        let initialize_groups = |arities: &[usize]| {
            if arities.is_empty() {
                Vec::new()
            } else {
                vec![ArityGroup {
                    arities: arities.to_vec(),
                }]
            }
        };
        Self {
            key: initialize_groups(layout.key()),
            value: initialize_groups(layout.value()),
            edges: Vec::new(),
            fixed,
        }
    }
}

/// One input occurrence at an arrangement boundary. `input` is an arrangement
/// reference or the fixed unit, never a feed node. `feed` borrows the root
/// computation; `input_path` follows its input ports to this occurrence. Direct
/// dependencies have no feed and an empty path. Input ports of one feed
/// require a joint transition.
#[derive(Debug, Clone)]
pub(super) struct Edge<'graph, 'plan> {
    pub(super) input: Source,
    pub(super) output: ArrangementId,
    pub(super) feed: Option<&'graph Feed<'plan>>,
    pub(super) input_path: Vec<usize>,
}

/// A per-stratum undirected graph for contracting original layout domains.
/// Foreign arrangements and declared aggregation exits have fixed layouts.
/// Input dedup domains are registered with stratum zero.
#[derive(Debug, Default)]
pub(super) struct StratumGraph<'graph, 'plan> {
    pub(super) nodes: BTreeMap<ArrangementId, ArrangementNode>,
    pub(super) edges: Vec<Edge<'graph, 'plan>>,
}

impl<'graph, 'plan> StratumGraph<'graph, 'plan> {
    /// Selects this stratum's undirected neighborhood, stopping at fixed
    /// arrangements. Feed chains retain their original expressions; following
    /// each input occurrence preserves self joins and shared computations.
    /// Input dedups belong to stratum zero's domain construction. Their raw
    /// input has no assumed scan order or incoming cost.
    #[must_use]
    pub(super) fn from_graph(graph: &'graph ArrangementGraph<'plan>, stratum_index: usize) -> Self {
        let belongs_to_stratum = |origin| match origin {
            ArrangementOrigin::InputDedup { .. } => stratum_index == 0,
            ArrangementOrigin::Transformation(location) => {
                location.stratum_index() == stratum_index
            }
            ArrangementOrigin::Dedup {
                stratum_index: owner,
                ..
            }
            | ArrangementOrigin::Aggregation {
                stratum_index: owner,
                ..
            }
            | ArrangementOrigin::AggregationExit {
                stratum_index: owner,
                ..
            } => owner == stratum_index,
        };
        let mut edges = Vec::new();
        let mut contribution_ranges = Vec::new();
        let mut pending = Vec::new();
        let arrangements = graph.arrangements().iter().enumerate();
        for (index, arrangement) in arrangements {
            let output = ArrangementId::new(index);
            if belongs_to_stratum(arrangement.origin()) {
                pending.push(output);
            }
            for &input in arrangement.inputs() {
                let feed = match input {
                    Source::Feed(id) => Some(&graph.feeds()[id.index()]),
                    Source::Arrangement(_) | Source::Unit => None,
                };
                let mut edge = Edge {
                    input,
                    output,
                    feed,
                    input_path: Vec::new(),
                };
                let start = edges.len();
                expand_input(graph, input, &mut edge, &mut edges);
                let end = edges.len();
                contribution_ranges.resize(end, start..end);
            }
        }

        let mut adjacency: BTreeMap<ArrangementId, Vec<usize>> = BTreeMap::new();
        let indexed_edges = edges.iter().enumerate();
        for (edge_index, edge) in indexed_edges {
            adjacency.entry(edge.output).or_default().push(edge_index);
            if let Source::Arrangement(input) = edge.input
                && input != edge.output
            {
                adjacency.entry(input).or_default().push(edge_index);
            }
        }

        let mut subgraph = Self::default();
        let mut selected_edges = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if subgraph.nodes.contains_key(&id) {
                continue;
            }
            let arrangement = &graph.arrangements()[id.index()];
            let origin = arrangement.origin();
            let fixed = !belongs_to_stratum(origin)
                || matches!(origin, ArrangementOrigin::AggregationExit { .. });
            let layout = arrangement.layout();
            subgraph
                .nodes
                .insert(id, ArrangementNode::new(layout, fixed));
            if fixed {
                continue;
            }
            let neighbors = adjacency.get(&id).into_iter().flatten();
            for &edge_index in neighbors {
                if selected_edges.contains(&edge_index) {
                    continue;
                }
                // A join's inputs remain coupled even at a fixed output.
                let contribution = contribution_ranges[edge_index].clone();
                for edge_index in contribution {
                    selected_edges.insert(edge_index);
                    let edge = &edges[edge_index];
                    pending.push(edge.output);
                    if let Source::Arrangement(input) = edge.input {
                        pending.push(input);
                    }
                }
            }
        }
        let mut selected_edge_indices = vec![None; edges.len()];
        let indexed_edges = edges.into_iter().enumerate();
        for (edge_index, edge) in indexed_edges {
            if !selected_edges.contains(&edge_index) {
                continue;
            }
            selected_edge_indices[edge_index] = Some(subgraph.edges.len());
            subgraph.edges.push(edge);
        }
        for (id, node) in &mut subgraph.nodes {
            node.edges = adjacency
                .get(id)
                .into_iter()
                .flatten()
                .filter_map(|&index| selected_edge_indices[index])
                .collect();
        }
        subgraph
    }
}

/// Expands only computations between sorting boundaries. The port path keeps
/// repeated uses of an input distinct without copying feed expressions.
fn expand_input<'graph, 'plan>(
    graph: &'graph ArrangementGraph<'plan>,
    source: Source,
    edge: &mut Edge<'graph, 'plan>,
    edges: &mut Vec<Edge<'graph, 'plan>>,
) {
    match source {
        Source::Feed(id) => {
            let inputs = graph.feeds()[id.index()].inputs().iter().enumerate();
            for (input_index, &input) in inputs {
                edge.input_path.push(input_index);
                expand_input(graph, input, edge, edges);
                edge.input_path.pop();
            }
        }
        Source::Arrangement(_) | Source::Unit => {
            edge.input = source;
            edges.push(edge.clone());
        }
    }
}
