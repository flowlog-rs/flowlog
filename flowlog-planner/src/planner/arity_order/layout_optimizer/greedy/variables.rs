//! Shared layout domains and field permutations within key/value boundaries.
//!
//! Each field can move independently within its region. Join directions are
//! separate choices and never masquerade as field orders.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::ArrangementId;
use super::super::super::arrangement_graph::ArrangementOrigin;
use super::super::super::arrangement_graph::AttributeLayout;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
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
    width: usize,
    key: bool,
    fixed: bool,
}

#[derive(Debug)]
pub(super) struct Variables {
    domains: Vec<Domain>,
    arrangements: Vec<[usize; 2]>,
}

impl Variables {
    /// Couples join keys and bindings sharing a producer in original slots.
    /// Aggregation inputs keep their original row order.
    /// Returns an internal error when the joined keys have different widths.
    pub(super) fn new(graph: &ArrangementGraph<'_>) -> Result<Self, PlanError> {
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
        let mut producers: BTreeMap<usize, Vec<ArrangementId>> = BTreeMap::new();
        let mut aggregation_producers = BTreeSet::new();
        let indexed_arrangements = graph.arrangements().iter().enumerate();
        for (index, arrangement) in indexed_arrangements {
            let dedup = match arrangement.origin() {
                ArrangementOrigin::Dedup { .. } => true,
                ArrangementOrigin::Aggregation { .. } => false,
                ArrangementOrigin::InputDedup { .. }
                | ArrangementOrigin::Transformation(_)
                | ArrangementOrigin::AggregationExit { .. } => continue,
            };
            for source in arrangement.inputs() {
                let Source::Feed(feed) = source else {
                    continue;
                };
                if dedup {
                    producers
                        .entry(feed.index())
                        .or_default()
                        .push(ArrangementId::new(index));
                } else {
                    aggregation_producers.insert(feed.index());
                }
            }
        }
        let mut fixed_keys = BTreeSet::new();
        for (feed, members) in producers {
            let Some((&first, remaining)) = members.split_first() else {
                continue;
            };
            if aggregation_producers.contains(&feed) {
                fixed_keys.insert(first);
            }
            // Declaration column IDs differ between bindings, but a shared
            // producer must receive the same permutation of its output slots.
            for member in remaining {
                let left = root(&parents, first.index());
                let right = root(&parents, member.index());
                parents[left.max(right)] = left.min(right);
            }
        }
        let mut domains = Vec::<Domain>::new();
        let mut keys = BTreeMap::new();
        let mut arrangements = vec![[0; 2]; parents.len()];
        let indexed_arrangements = graph.arrangements().iter().enumerate();
        for (index, boundary) in indexed_arrangements {
            let arrangement = ArrangementId::new(index);
            let layout = boundary.layout();
            let fixed = matches!(boundary.origin(), ArrangementOrigin::AggregationExit { .. });
            let root = root(&parents, index);
            let key = *keys.entry(root).or_insert_with(|| {
                domains.push(Domain {
                    members: Vec::new(),
                    width: layout.key().len(),
                    key: true,
                    fixed: false,
                });
                domains.len() - 1
            });
            if domains[key].width != layout.key().len() {
                return Err(PlanError::internal(format!(
                    "shared key domain for arrangement {index} has width {}, expected {}",
                    layout.key().len(),
                    domains[key].width,
                )));
            }
            domains[key].members.push(arrangement);
            domains[key].fixed |= fixed || fixed_keys.contains(&arrangement);
            let value = domains.len();
            domains.push(Domain {
                members: vec![arrangement],
                width: layout.value().len(),
                key: false,
                fixed,
            });
            arrangements[index] = [key, value];
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
                (domain.fixed || domain.width <= 1).then_some(Variable::Layout(index))
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

    /// Applies an original-slot permutation atomically to all shared members.
    /// Returns `None` if a slot is repeated, omitted, or outside the region.
    #[must_use]
    pub(super) fn set_order(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        order: &[usize],
    ) -> Option<()> {
        let domain = &self.domains[id];
        if order.len() != domain.width {
            return None;
        }
        let mut seen = vec![false; domain.width];
        for &slot in order {
            if std::mem::replace(seen.get_mut(slot)?, true) {
                return None;
            }
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

    /// Follows the requested field order within a region. Unmentioned fields
    /// retain their current relative order after the requested fields.
    #[must_use]
    pub(super) fn align(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        requested: &[Column],
    ) -> Option<()> {
        let domain = &self.domains[id];
        let mut order = self.order(graph, choice, id);
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
        order.sort_by_key(|slot| ranks.get(slot).copied().unwrap_or(usize::MAX));
        self.set_order(graph, choice, id, &order)
    }

    /// Randomizes the fields of one restart's relation-dedup seed.
    #[must_use]
    pub(super) fn randomize(
        &self,
        graph: &ArrangementGraph<'_>,
        choice: &mut LayoutChoice,
        id: usize,
        random: &mut SmallRng,
    ) -> Option<()> {
        let mut order: Vec<_> = (0..self.domains[id].width).collect();
        order.shuffle(random);
        self.set_order(graph, choice, id, &order)
    }
}

#[must_use]
fn root(parents: &[usize], mut id: usize) -> usize {
    while parents[id] != id {
        id = parents[id];
    }
    id
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use flowlog_parser::Mutability;

    use super::*;
    use crate::planner::ArithmeticArgument;
    use crate::planner::CanonicalForm;
    use crate::planner::Collection;
    use crate::planner::Constraints;
    use crate::planner::FactorArgument;
    use crate::planner::KeyValueLayout;
    use crate::planner::Transformation;
    use crate::planner::TransformationArgument;
    use crate::planner::TransformationFlow;
    use crate::planner::arity_order::arrangement_graph::Arrangement;
    use crate::planner::arity_order::arrangement_graph::Feed;
    use crate::planner::arity_order::arrangement_graph::FeedId;
    use crate::planner::arity_order::arrangement_graph::JoinOrientation;
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

    fn shared_projection() -> (ArrangementGraph<'static>, FeedId) {
        let mut graph = ArrangementGraph::default();
        let input = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::InputDedup {
                relation_fingerprint: 0,
            },
            AttributeLayout::new(2, 0),
            Vec::new(),
        ));
        let arguments = [1, 1, 0].map(|column| ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, column))),
            rest: Vec::new(),
        });
        let projection = Transformation::RowToRow {
            input: collection(0, 0, 2),
            output: collection(1, 0, 3),
            flow: TransformationFlow::KVToKV {
                key: Arc::new(Vec::new()),
                value: Arc::new(arguments.to_vec()),
                constraints: Constraints::new(Vec::new(), Vec::new()),
                compares: Vec::new(),
            },
        };
        let feed = graph.add_feed(Feed::new(
            TransformationLocation::new(0, Scope::NonRecursive, 0),
            Cow::Owned(projection),
            vec![Source::Arrangement(input)],
            None,
        ));
        (graph, feed)
    }

    #[test]
    fn shared_producer_couples_binding_orders_by_original_slots() {
        let (mut graph, feed) = shared_projection();
        let bindings = [(2, vec![2, 0, 1]), (3, vec![1, 2, 0])].map(|(relation, order)| {
            graph.add_arrangement(Arrangement::new(
                ArrangementOrigin::Dedup {
                    stratum_index: 0,
                    scope: Scope::NonRecursive,
                    relation_fingerprint: relation,
                },
                AttributeLayout::from_columns(order, Vec::new()),
                vec![Source::Feed(feed)],
            ))
        });
        let variables = Variables::new(&graph).unwrap();
        let [key, _] = variables.arrangements[bindings[0].index()];
        assert_eq!(variables.arrangements[bindings[1].index()][0], key);

        let mut choice = LayoutChoice::default();
        assert_eq!(
            variables.set_order(&graph, &mut choice, key, &[1, 2, 0]),
            Some(())
        );
        assert_eq!(choice.layout(&graph, bindings[0]).key(), [0, 1, 2]);
        assert_eq!(choice.layout(&graph, bindings[1]).key(), [2, 0, 1]);
        assert_eq!(
            variables.set_order(&graph, &mut choice, key, &[0, 1, 2]),
            Some(())
        );
        assert_eq!(choice, LayoutChoice::default());
    }

    #[test]
    fn shared_aggregation_input_fixes_dedup_order_without_fixing_grouping_keys() {
        let (mut graph, feed) = shared_projection();
        let dedup = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::Dedup {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: 2,
            },
            AttributeLayout::from_columns(vec![2, 0, 1], Vec::new()),
            vec![Source::Feed(feed)],
        ));
        let aggregation = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::Aggregation {
                stratum_index: 0,
                scope: Scope::NonRecursive,
                relation_fingerprint: 3,
            },
            AttributeLayout::new(2, 0),
            vec![Source::Feed(feed)],
        ));
        let variables = Variables::new(&graph).unwrap();
        let [dedup_key, _] = variables.arrangement(dedup);
        let [aggregation_key, _] = variables.arrangement(aggregation);
        assert!(variables.fixed().contains(&dedup_key));
        assert!(!variables.fixed().contains(&aggregation_key));

        let Variable::Layout(key) = aggregation_key else {
            unreachable!();
        };
        let mut choice = LayoutChoice::default();
        assert_eq!(
            variables.set_order(&graph, &mut choice, key, &[1, 0]),
            Some(())
        );
        assert_eq!(choice.layout(&graph, dedup).key(), [2, 0, 1]);
        assert_eq!(choice.layout(&graph, aggregation).key(), [1, 0]);
    }

    #[test]
    fn field_orders_preserve_key_value_boundaries() {
        let mut graph = ArrangementGraph::default();
        let arrangement = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::Transformation(TransformationLocation::new(
                0,
                Scope::NonRecursive,
                0,
            )),
            AttributeLayout::new(3, 3),
            Vec::new(),
        ));
        let variables = Variables::new(&graph).unwrap();
        let [key, value] = variables.arrangements[arrangement.index()];
        let mut choice = LayoutChoice::default();
        assert_eq!(
            variables.set_order(&graph, &mut choice, key, &[2, 1, 0]),
            Some(())
        );
        assert_eq!(
            variables.set_order(&graph, &mut choice, value, &[2, 0, 1]),
            Some(())
        );
        assert_eq!(choice.layout(&graph, arrangement).key(), [2, 1, 0]);
        assert_eq!(choice.layout(&graph, arrangement).value(), [5, 3, 4]);
        let requested = [Column {
            arrangement,
            field: 1,
        }];
        assert_eq!(
            variables.align(&graph, &mut choice, key, &requested),
            Some(())
        );
        assert_eq!(choice.layout(&graph, arrangement).key(), [1, 2, 0]);
        assert_eq!(choice.layout(&graph, arrangement).value(), [5, 3, 4]);
    }

    #[rstest::rstest]
    #[case::duplicate(&[0, 0, 2])]
    #[case::missing(&[0, 1])]
    #[case::outside(&[0, 1, 3])]
    fn invalid_field_orders_leave_the_choice_unchanged(#[case] order: &[usize]) {
        let mut graph = ArrangementGraph::default();
        let arrangement = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::InputDedup {
                relation_fingerprint: 0,
            },
            AttributeLayout::new(3, 0),
            Vec::new(),
        ));
        let variables = Variables::new(&graph).unwrap();
        let [key, _] = variables.arrangements[arrangement.index()];
        let mut choice = LayoutChoice::default();
        assert_eq!(
            variables.set_order(&graph, &mut choice, key, &[2, 1, 0]),
            Some(())
        );
        let before = choice.clone();
        assert_eq!(variables.set_order(&graph, &mut choice, key, order), None);
        assert_eq!(choice, before);
    }

    #[test]
    fn aggregation_exit_layout_is_fixed() {
        let mut graph = ArrangementGraph::default();
        let input = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::Aggregation {
                stratum_index: 0,
                scope: Scope::Recursive,
                relation_fingerprint: 0,
            },
            AttributeLayout::new(3, 0),
            Vec::new(),
        ));
        let exit = graph.add_aggregation_exit(0, 0, input);
        let variables = Variables::new(&graph).unwrap();
        let [input_key, _] = variables.arrangement(input);
        let [exit_key, _] = variables.arrangement(exit);
        assert!(!variables.fixed().contains(&input_key));
        assert!(variables.fixed().contains(&exit_key));
    }

    #[test]
    fn join_keys_share_slot_orders_while_values_remain_independent() {
        let mut graph = ArrangementGraph::default();
        let location = |index| TransformationLocation::new(0, Scope::NonRecursive, index);
        let inputs = [0, 1].map(|index| {
            graph.add_arrangement(Arrangement::new(
                ArrangementOrigin::Transformation(location(index)),
                AttributeLayout::new(2, 2),
                Vec::new(),
            ))
        });
        let argument = |init| ArithmeticArgument {
            init: FactorArgument::Var(init),
            rest: Vec::new(),
        };
        let join = Transformation::JnToRow {
            input: (collection(0, 2, 2), collection(1, 2, 2)),
            output: collection(2, 0, 1),
            flow: TransformationFlow::JnToKV {
                key: Arc::new(Vec::new()),
                value: Arc::new(vec![argument(TransformationArgument::Jn((true, true, 0)))]),
                compares: Vec::new(),
            },
        };
        let _ = graph.add_feed(Feed::new(
            location(2),
            Cow::Owned(join),
            inputs.map(Source::Arrangement).to_vec(),
            Some(JoinOrientation::Original),
        ));
        let variables = Variables::new(&graph).unwrap();
        let [left_key, left_value] = variables.arrangements[inputs[0].index()];
        let [right_key, _] = variables.arrangements[inputs[1].index()];
        assert_eq!(left_key, right_key);
        let mut choice = LayoutChoice::default();
        assert_eq!(
            variables.set_order(&graph, &mut choice, left_key, &[1, 0]),
            Some(())
        );
        assert_eq!(
            variables.set_order(&graph, &mut choice, left_value, &[1, 0]),
            Some(())
        );
        assert_eq!(choice.layout(&graph, inputs[0]).key(), [1, 0]);
        assert_eq!(choice.layout(&graph, inputs[1]).key(), [1, 0]);
        assert_eq!(choice.layout(&graph, inputs[0]).value(), [3, 2]);
        assert_eq!(choice.layout(&graph, inputs[1]).value(), [2, 3]);
    }
}
