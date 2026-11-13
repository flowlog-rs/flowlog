//! Shared join-key domains and legal permutations of contracted field groups.
//!
//! A layout variable moves whole groups within one key or value region. Join
//! directions are separate choices and never masquerade as field orders.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::ArrangementId;
use super::super::super::arrangement_graph::AttributeLayout;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
use super::super::stratum_graph::ArrangementNode;
use crate::planner::PlanError;

/// An original field ID at one physical sorting boundary.
#[derive(Debug, Clone, Copy)]
pub(super) struct Column {
    pub(super) arrangement: ArrangementId,
    pub(super) field: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Variable {
    Layout(usize),
    Join(usize),
}

/// One independent region; shared keys translate the same original-slot
/// permutation to every member's own field IDs.
#[derive(Debug)]
struct Domain {
    members: Vec<ArrangementId>,
    groups: Vec<Vec<usize>>,
    key: bool,
    fixed: bool,
}

#[derive(Debug)]
pub(super) struct Variables {
    domains: Vec<Domain>,
    arrangements: Vec<[usize; 2]>,
}

impl Variables {
    /// Couples corresponding join keys and intersects their group boundaries.
    /// Returns an internal error when the joined keys have different widths.
    pub(super) fn new(
        graph: &ArrangementGraph<'_>,
        nodes: &BTreeMap<ArrangementId, ArrangementNode>,
    ) -> Result<Self, PlanError> {
        let mut parents: Vec<_> = (0..graph.arrangements().len()).collect();
        for feed in graph.feeds() {
            if let [Source::Arrangement(left), Source::Arrangement(right)] = feed.inputs() {
                let left_width = graph.arrangements()[left.index()].layout().key().len();
                let right_width = graph.arrangements()[right.index()].layout().key().len();
                if left_width != right_width {
                    return Err(PlanError::internal(format!(
                        "join inputs {} and {} have different key widths {left_width} and {right_width}",
                        left.index(),
                        right.index(),
                    )));
                }
                let left = root(&parents, left.index());
                let right = root(&parents, right.index());
                parents[left.max(right)] = left.min(right);
            }
        }
        let mut domains = Vec::<Domain>::new();
        let mut keys = BTreeMap::new();
        let mut arrangements = vec![[0; 2]; parents.len()];
        for (&arrangement, node) in nodes {
            let root = root(&parents, arrangement.index());
            let key = *keys.entry(root).or_insert_with(|| {
                domains.push(Domain {
                    members: Vec::new(),
                    groups: Vec::new(),
                    key: true,
                    fixed: false,
                });
                domains.len() - 1
            });
            domains[key].members.push(arrangement);
            domains[key].fixed |= node.fixed;
            let layout = graph.arrangements()[arrangement.index()].layout();
            let groups = node
                .value
                .iter()
                .map(|group| {
                    group
                        .arities
                        .iter()
                        .map(|field| {
                            layout
                                .value()
                                .iter()
                                .position(|column| column == field)
                                .expect("contracted fields belong to their original region")
                        })
                        .collect()
                })
                .collect();
            let value = domains.len();
            domains.push(Domain {
                members: vec![arrangement],
                groups,
                key: false,
                fixed: node.fixed,
            });
            arrangements[arrangement.index()] = [key, value];
        }
        for domain in &mut domains {
            if !domain.key {
                continue;
            }
            let width = graph.arrangements()[domain.members[0].index()]
                .layout()
                .key()
                .len();
            let mut cuts = BTreeSet::from([0, width]);
            for member in &domain.members {
                let mut end = 0;
                for group in &nodes[member].key {
                    end += group.arities.len();
                    cuts.insert(end);
                }
            }
            let cuts: Vec<_> = cuts.into_iter().collect();
            domain.groups = cuts
                .windows(2)
                .map(|pair| (pair[0]..pair[1]).collect())
                .collect();
        }
        Ok(Self {
            domains,
            arrangements,
        })
    }

    #[must_use]
    pub(super) fn arrangement(&self, id: ArrangementId) -> [Variable; 2] {
        self.arrangements[id.index()].map(Variable::Layout)
    }

    #[must_use]
    pub(super) fn fixed(&self) -> BTreeSet<Variable> {
        self.domains
            .iter()
            .enumerate()
            .filter_map(|(index, domain)| {
                (domain.fixed || domain.groups.len() <= 1).then_some(Variable::Layout(index))
            })
            .collect()
    }

    /// Returns an independent whole-row region, excluding keys shared by joins.
    #[must_use]
    pub(super) fn row(&self, graph: &ArrangementGraph<'_>, id: ArrangementId) -> Option<usize> {
        let layout = graph.arrangements()[id.index()].layout();
        let [key, value] = self.arrangements[id.index()];
        if layout.value().is_empty() && self.domains[key].members.len() == 1 {
            Some(key)
        } else if layout.key().is_empty() {
            Some(value)
        } else {
            None
        }
    }

    /// Returns a layout's order in original region slots.
    #[must_use]
    pub(super) fn order(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &LayoutChoice,
        id: usize,
    ) -> Vec<usize> {
        let domain = &self.domains[id];
        let member = domain.members[0];
        let original = graph.arrangements()[member.index()].layout();
        let selected = choice.layout(graph, member);
        let (original, selected) = if domain.key {
            (original.key(), selected.key())
        } else {
            (original.value(), selected.value())
        };
        selected
            .iter()
            .map(|field| {
                original
                    .iter()
                    .position(|column| column == field)
                    .expect("selected layouts permute their original region")
            })
            .collect()
    }

    /// Applies a whole-group order atomically to all shared members. Returns
    /// `None` if the requested order splits, repeats, or omits a group.
    #[must_use]
    pub(super) fn set_order(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        order: &[usize],
    ) -> Option<()> {
        let domain = &self.domains[id];
        let mut remaining = order;
        let mut seen = BTreeSet::new();
        while let Some(&first) = remaining.first() {
            let group = domain.groups.iter().position(|group| group[0] == first)?;
            if !seen.insert(group) {
                return None;
            }
            remaining = remaining.strip_prefix(domain.groups[group].as_slice())?;
        }
        if seen.len() != domain.groups.len() {
            return None;
        }
        for &member in &domain.members {
            let original = graph.arrangements()[member.index()].layout();
            let current = choice.layout(graph, member);
            let columns = if domain.key {
                original.key()
            } else {
                original.value()
            };
            let permuted = order.iter().map(|&slot| columns[slot]).collect();
            let layout = if domain.key {
                AttributeLayout::from_columns(permuted, current.value().to_vec())
            } else {
                AttributeLayout::from_columns(current.key().to_vec(), permuted)
            };
            if layout == *original {
                choice.layouts.remove(&member);
            } else {
                choice.layouts.insert(member, layout);
            }
        }
        Some(())
    }

    /// Moves groups by the first requested field they contain. Unmentioned
    /// groups follow in their current order; group interiors remain unchanged.
    #[must_use]
    pub(super) fn align(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        requested: &[Column],
    ) -> Option<()> {
        let domain = &self.domains[id];
        let current = self.order(graph, choice, id);
        let mut groups: Vec<_> = domain.groups.iter().collect();
        groups.sort_by_key(|group| current.iter().position(|slot| *slot == group[0]));
        let mut ranks = BTreeMap::new();
        let fields = requested.iter().enumerate();
        for (rank, &Column { arrangement, field }) in fields {
            if !domain.members.contains(&arrangement) {
                continue;
            }
            let layout = graph.arrangements()[arrangement.index()].layout();
            let columns = if domain.key {
                layout.key()
            } else {
                layout.value()
            };
            if let Some(slot) = columns.iter().position(|&column| column == field) {
                ranks.entry(slot).or_insert(rank);
            }
        }
        groups.sort_by_key(|group| {
            group
                .iter()
                .filter_map(|slot| ranks.get(slot))
                .min()
                .copied()
                .unwrap_or(usize::MAX)
        });
        let order: Vec<_> = groups.into_iter().flatten().copied().collect();
        self.set_order(graph, choice, id, &order)
    }

    /// Randomizes complete groups for one restart's relation-dedup seed.
    #[must_use]
    pub(super) fn randomize(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        random: &mut SmallRng,
    ) -> Option<()> {
        let mut groups = self.domains[id].groups.clone();
        groups.shuffle(random);
        self.set_order(
            graph,
            choice,
            id,
            &groups.into_iter().flatten().collect::<Vec<_>>(),
        )
    }
}

#[must_use]
fn root(parents: &[usize], mut id: usize) -> usize {
    while parents[id] != id {
        id = parents[id];
    }
    id
}
