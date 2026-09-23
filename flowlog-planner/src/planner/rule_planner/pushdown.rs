//! Pushdown phase: copies each folded original filter down the finished
//! plan, wherever a copy shrinks a join that nothing else would.
//!
//! A robust plan applies every filter atom to every atom that can take it,
//! so no join ever reads rows that a filter elsewhere in the rule would
//! discard. In differential dataflow that robustness is not free: each
//! semijoin or antijoin arranges its retained input, and the arrangement
//! is kept in memory and maintained on every update. On small inputs or
//! under incremental evaluation the arrangements cost more than the rows
//! they save, so prepare and core fold each filter into exactly ONE
//! superset, the minimum that keeps the rule correct.
//!
//! One application is often enough. Once the filtered atom joins another
//! atom on the filtered columns, the join enforces the filter on that
//! other atom as well, one hop at a time. It is not enough when an atom
//! that could take the filter joins something else first: that join is
//! built from the unfiltered atom, and every row it produces from the
//! discarded ones is wasted work.
//!
//! Take `Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x)`. The fold
//! picks `B` for `A`, and core joins in body order:
//!
//! ```text
//! ((C join D) join (A semijoin B))
//! ```
//!
//! `C join D` is built from the whole of `C`, although only the rows whose
//! `x` is in `A` can survive the next join. This phase adds the copy
//! `A semijoin C`, and nothing on `B`'s side of the root join, where the
//! join already enforces `A`. It keeps the copies that buy a smaller join
//! and skips the ones that only buy an arrangement.
//!
//! For each folded original filter, walk the plan top down and land a copy
//! on every deepest node whose columns still cover the filter's variables,
//! unless the first join above already carries the filter on its other
//! input. A copy sits on the edge the walk came down, between a node and
//! the one reader that led to it, and the node's column names come from
//! that reader. Names cannot live on the node: two atoms of one relation
//! can plan the same transformation, share its fingerprint, and name the
//! same column differently. The walk is documented on
//! [`RulePlanner::push_into`] and the shape of a copy on
//! [`RulePlanner::insert_copy`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::Catalog;
use crate::catalog::JoinPredicates;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

// =========================================================================
// Filter
// =========================================================================

/// A folded original atom, as its first application in the plan describes
/// it.
///
/// Every copy reuses these: the filter is arranged once, by `layout`, and
/// every semijoin or antijoin against it shares that arrangement.
struct Filter {
    /// Fingerprint of the filter's collection.
    fp: u64,
    /// Hierarchical name of the filter's collection.
    name: String,
    /// Layout the filter is arranged by: all of its columns as keys, no
    /// values.
    layout: KeyValueLayout,
    /// Variable names of `layout`'s keys, in key order.
    key_names: Vec<String>,
    /// Whether copies are semijoins (`true`) or antijoins (`false`).
    positive: bool,
}

// =========================================================================
// Side
// =========================================================================

/// One input of a transformation. A unary transformation has only a left
/// input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

// =========================================================================
// View
// =========================================================================

/// How a reader sees one collection: a position per column of the
/// collection, in column order, and the variable each column holds for
/// that reader (`None` for a column that is not a plain variable).
struct View {
    positions: Vec<ArithmeticPos>,
    names: Vec<Option<String>>,
}

impl View {
    /// The output of `tx` as `tx` itself names it.
    fn of_output(tx: &TransformationInfo) -> Self {
        let layout = tx.output_kv_layout();
        Self {
            positions: layout.key().iter().chain(layout.value()).cloned().collect(),
            names: tx
                .output_variables()
                .into_iter()
                .map(|name| name.map(str::to_string))
                .collect(),
        }
    }

    /// The input of `tx` read through `layout`, as `tx` names it.
    fn of_input(tx: &TransformationInfo, layout: &KeyValueLayout) -> Self {
        // A reader lists its input's columns in the order it arranges them,
        // keys first, and names each by the producer's column index.
        let mut positions: Vec<ArithmeticPos> =
            layout.key().iter().chain(layout.value()).cloned().collect();
        positions.sort_by_key(|position| {
            position
                .init()
                .as_var_signature()
                .map_or(usize::MAX, AtomArgumentSignature::argument_id)
        });
        let names = positions
            .iter()
            .map(|position| {
                if !position.rest().is_empty() {
                    return None;
                }
                position
                    .init()
                    .as_var_signature()
                    .and_then(|signature| tx.variables().get(signature))
                    .cloned()
            })
            .collect();
        Self { positions, names }
    }

    /// Column holding the variable `name`, if any.
    fn column(&self, name: &str) -> Option<usize> {
        self.names
            .iter()
            .position(|column| column.as_deref() == Some(name))
    }

    /// Returns `true` if every variable of `filter` is a column.
    fn covers(&self, filter: &Filter) -> bool {
        filter
            .key_names
            .iter()
            .all(|key| self.column(key).is_some())
    }
}

// =========================================================================
// Entry point
// =========================================================================
impl RulePlanner {
    /// Copies every folded original filter down to the deepest plan nodes
    /// that cover its variables, following the rules in the module
    /// documentation. The output tuple set does not change.
    ///
    /// Runs after core, when the catalog holds the single atom that names
    /// the plan's root, and before fuse, which expects consumers to point
    /// at their final producers.
    pub(crate) fn pushdown(&mut self, catalog: &Catalog) -> Result<(), PlanError> {
        if self.folded_filters.is_empty() {
            return Ok(());
        }
        let root = catalog.positive_atom_fingerprint(0)?;
        let originals = catalog.original_atom_fingerprints();
        // Described before any copy is inserted: a copy can land on another
        // filter's collection and redirect that filter's application to
        // the copy, after which the application no longer reads the
        // recorded fingerprint.
        let filters = self
            .folded_filters
            .iter()
            .filter(|&&fp| self.is_narrowed_original(fp, originals))
            .map(|&fp| self.folded_filter(fp))
            .collect::<Result<Vec<_>, _>>()?;
        let root_index = self.producer(root).ok_or_else(|| {
            PlanError::internal(format!("pushdown: root {root:#018x} has no producer"))
        })?;
        let count = self.transformation_infos.len();
        for filter in &filters {
            // An atom of constants, such as `isMethod("<...run()>")` in
            // DOOP, folds as a semijoin with no key: an existence test. It
            // covers every node, and a copy anywhere would shrink nothing.
            if filter.key_names.is_empty() {
                continue;
            }
            let view = View::of_output(&self.transformation_infos[root_index]);
            self.push_into(filter, root, None, view, false, originals)?;
        }
        if self.transformation_infos.len() != count {
            self.sort_pipeline(originals)?;
            // Post reads the plan's root as the last transformation. Every
            // copy is an ancestor of the root, so the sort keeps it last.
            if self
                .transformation_infos
                .last()
                .map(|tx| tx.output_info_fp())
                != Some(root)
            {
                return Err(PlanError::internal(
                    "pushdown: the plan's root is no longer its last transformation".to_string(),
                ));
            }
        }
        trace!(
            "Transformation infos after pushdown:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }
}

// =========================================================================
// Walk
// =========================================================================
impl RulePlanner {
    /// Describes the filter with collection fingerprint `fp` from the
    /// semijoin or antijoin that first applied it, naming its columns as
    /// that application does.
    ///
    /// Returns an internal error if no transformation applies the filter,
    /// or if one of the application's key columns has no variable name.
    fn folded_filter(&self, fp: u64) -> Result<Filter, PlanError> {
        let applied = self
            .transformation_infos
            .iter()
            .find(|tx| matches!(tx.input_info_fp(), (left, Some(_)) if left == fp))
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "pushdown: folded filter {fp:#018x} has no semijoin or antijoin"
                ))
            })?;
        let name = applied.input_name().0.to_string();
        let layout = applied.input_kv_layout().0.clone();
        let key_names = layout
            .key()
            .iter()
            .map(|position| {
                position
                    .init()
                    .as_var_signature()
                    .and_then(|signature| applied.variables().get(signature))
                    .cloned()
                    .ok_or_else(|| {
                        PlanError::internal(format!(
                            "pushdown: filter {name} key {position} has no variable name"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Filter {
            fp,
            name,
            layout,
            key_names,
            positive: !applied.is_neg_join(),
        })
    }

    /// Pushes `filter` into the subtree producing `fp`, reached through
    /// the input edge of transformation `parent` that reads it as `view`,
    /// adding a copy on each deepest node of the subtree that covers the
    /// filter's variables. A copy sits on the edge it was reached through:
    /// only `parent` reads it, and any other reader of the same collection
    /// is untouched. The negated side of an antijoin never takes a copy,
    /// where a filter would let rows through instead of removing them.
    ///
    /// `enforced` says a copy landing directly on this subtree would be
    /// redundant: the first join above it, reached through unary maps and
    /// the retained sides of semijoins and antijoins, already carries the
    /// filter on its other input. With `S = F semijoin M` and
    /// `M = G semijoin C`, a landing on `C` is one hop from `S`, so `C`
    /// inherits `S`'s enforcement through `M`. Under a full join
    /// `M = G join C`, `C` does not: a copy on `C` shrinks `M`.
    fn push_into(
        &mut self,
        filter: &Filter,
        fp: u64,
        parent: Option<usize>,
        view: View,
        enforced: bool,
        originals: &BTreeSet<u64>,
    ) -> Result<(), PlanError> {
        if fp == filter.fp || !view.covers(filter) {
            return Ok(());
        }
        let index = self.producer(fp).ok_or_else(|| {
            PlanError::internal(format!("pushdown: collection {fp:#018x} has no producer"))
        })?;
        // The inputs to descend into, each with whether it is a retained
        // side that inherits `enforced`, and whether the node has a second
        // input whose carrying the filter makes a copy on the other
        // redundant. The negated side of an antijoin is left out on
        // purpose. A semijoin is a join whose left input contributes keys
        // only; its right input is the retained one.
        let (sides, binary): (Vec<(Side, bool)>, bool) = match &self.transformation_infos[index] {
            TransformationInfo::KVToKV { .. } => (vec![(Side::Left, true)], false),
            TransformationInfo::JoinToKV {
                left_input_kv_layout,
                ..
            } => {
                let semijoin = left_input_kv_layout.value().is_empty();
                (vec![(Side::Left, false), (Side::Right, semijoin)], true)
            }
            TransformationInfo::AntiJoinToKV { .. } => (vec![(Side::Right, true)], true),
        };

        let mut descended = false;
        for (side, retained) in sides {
            let (child, child_view) = self.input(index, side)?;
            // An original atom is not a node: the walk stops at the
            // transformation that reads it.
            if child == filter.fp || self.producer(child).is_none() || !child_view.covers(filter) {
                continue;
            }
            descended = true;
            // Computed now rather than up front: when both inputs of a join
            // cover the filter and neither carries it, the copies added
            // under the first are what make a copy under the second
            // redundant.
            let child_enforced = (retained && enforced)
                || (binary && {
                    let (sibling, sibling_view) = self.input(index, side.other())?;
                    self.carries(sibling, filter) && sibling_view.covers(filter)
                });
            self.push_into(
                filter,
                child,
                Some(index),
                child_view,
                child_enforced,
                originals,
            )?;
        }
        if descended || enforced {
            return Ok(());
        }
        // The root has no edge above it; a copy there would repeat the fold
        // that already applies the filter on the way to the root.
        let Some(parent) = parent else {
            return Ok(());
        };
        // Fuse arranges each producer by one key layout. The copy takes
        // over `parent`'s read of the node, so the node's readers must not
        // outnumber its producers afterwards either. A folded filter is
        // read by all of its columns everywhere and takes no copy.
        let (producers, consumers) = &self.producer_consumer[&fp];
        if self.folded_filters.contains(&fp) || consumers.len() > producers.len() {
            return Ok(());
        }
        self.insert_copy(filter, index, parent, &view, originals)
    }

    /// The input of transformation `index` on `side`: its fingerprint and
    /// how the transformation sees it.
    ///
    /// Returns an internal error for the right side of a unary
    /// transformation.
    fn input(&self, index: usize, side: Side) -> Result<(u64, View), PlanError> {
        let tx = &self.transformation_infos[index];
        let (left_fp, right_fp) = tx.input_info_fp();
        let (left_layout, right_layout) = tx.input_kv_layout();
        let (fp, layout) = match side {
            Side::Left => (Some(left_fp), Some(left_layout)),
            Side::Right => (right_fp, right_layout),
        };
        let (Some(fp), Some(layout)) = (fp, layout) else {
            return Err(PlanError::internal(format!(
                "pushdown: {} has no {side:?} input",
                tx.output_name()
            )));
        };
        Ok((fp, View::of_input(tx, layout)))
    }

    /// Returns `true` if the collection `fp` is an original atom or is
    /// produced from one through unary transformations only, such as a
    /// premap, a constant filter, or a projection.
    fn is_narrowed_original(&self, fp: u64, originals: &BTreeSet<u64>) -> bool {
        if originals.contains(&fp) {
            return true;
        }
        let Some(index) = self.producer(fp) else {
            return false;
        };
        match &self.transformation_infos[index] {
            TransformationInfo::KVToKV { input_info_fp, .. } => {
                self.is_narrowed_original(*input_info_fp, originals)
            }
            TransformationInfo::JoinToKV { .. } | TransformationInfo::AntiJoinToKV { .. } => false,
        }
    }

    /// Returns `true` if every row of the collection `fp` already satisfies
    /// `filter`: it is the filter itself, an antijoin against it, or it is
    /// produced from a collection that does, other than through the negated
    /// side of an antijoin.
    ///
    /// A join or semijoin carries the filter when either input does, since
    /// its rows agree with both inputs. An antijoin carries it only through
    /// its retained input: `T antijoin N` keeps rows of `T`, and a filter
    /// applied to `N` says nothing about them.
    fn carries(&self, fp: u64, filter: &Filter) -> bool {
        // Content-based fingerprints let one collection feed both inputs
        // of a join; without the visited set a chain of such joins would
        // be walked once per path through it.
        let mut visited = BTreeSet::new();
        let mut pending = vec![fp];
        while let Some(fp) = pending.pop() {
            if fp == filter.fp {
                return true;
            }
            let Some(index) = self.producer(fp) else {
                continue;
            };
            if !visited.insert(index) {
                continue;
            }
            let (left, right) = self.transformation_infos[index].input_info_fp();
            match &self.transformation_infos[index] {
                TransformationInfo::KVToKV { .. } => pending.push(left),
                TransformationInfo::JoinToKV { .. } => {
                    pending.push(left);
                    pending.extend(right);
                }
                TransformationInfo::AntiJoinToKV { .. } => {
                    if left == filter.fp {
                        return true;
                    }
                    pending.extend(right);
                }
            }
        }
        false
    }
}

// =========================================================================
// Rewrite
// =========================================================================
impl RulePlanner {
    /// Inserts a copy of `filter` between the output of transformation
    /// `index` and its reader `parent`, which sees that output as `view`:
    /// the copy reads the output, and `parent` reads the copy instead.
    ///
    /// Returns an internal error if `view` does not name every variable of
    /// the filter.
    fn insert_copy(
        &mut self,
        filter: &Filter,
        index: usize,
        parent: usize,
        view: &View,
        originals: &BTreeSet<u64>,
    ) -> Result<(), PlanError> {
        let target = &self.transformation_infos[index];
        let target_fp = target.output_info_fp();
        let target_name = target.output_name().to_string();
        let key_count = target.output_kv_layout().key().len();
        // The copy reads the target through `parent`'s own positions:
        // their argument ids are the target's columns, which is what fuse
        // reorders the target by, and their names are `parent`'s. The
        // copy joins on the columns holding the filter's variables, in the
        // filter's key order, so key `i` on both sides is one variable.
        let keys = filter
            .key_names
            .iter()
            .map(|key| {
                view.column(key)
                    .map(|column| view.positions[column].clone())
                    .ok_or_else(|| {
                        PlanError::internal(format!(
                            "pushdown: reader of {target_name} lost variable {key} it covered"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values: Vec<ArithmeticPos> = view
            .positions
            .iter()
            .filter(|position| !keys.contains(position))
            .cloned()
            .collect();
        let variables: BTreeMap<AtomArgumentSignature, String> = view
            .positions
            .iter()
            .zip(&view.names)
            .filter_map(|(position, name)| {
                let signature = position.init().as_var_signature()?;
                Some((*signature, name.clone()?))
            })
            .collect();
        let retained = KeyValueLayout::new(keys, values);
        // `parent` addresses the columns by index, so the copy keeps the
        // target's column order and key/value split; only the arrangement
        // the copy joins on puts the filter's variables first.
        let output = KeyValueLayout::new(
            view.positions[..key_count].to_vec(),
            view.positions[key_count..].to_vec(),
        );
        let mut tx = if filter.positive {
            TransformationInfo::join_to_kv(
                filter.fp,
                filter.name.clone(),
                target_fp,
                target_name.clone(),
                Self::semijoin_name(&filter.name, &target_name, &filter.key_names),
                filter.layout.clone(),
                retained,
                output,
                JoinPredicates::default(),
            )
        } else {
            TransformationInfo::anti_join_to_kv(
                filter.fp,
                filter.name.clone(),
                target_fp,
                target_name.clone(),
                Self::antijoin_name(&filter.name, &target_name, &filter.key_names),
                filter.layout.clone(),
                retained,
                output,
            )
        };
        tx.set_variables(variables);
        let copy_fp = tx.output_info_fp();
        trace!("Pushdown transformation:\n{tx}");
        // Appended out of pipeline order: the filter's producer may sit
        // after the target when core folded the filter late. `pushdown`
        // restores the order once every copy is in. The walk itself reads
        // the producer-consumer map, rebuilt here, never the list order.
        self.transformation_infos.push(tx);
        // `parent` now hashes an input it no longer reads. Fuse refreshes
        // every fingerprint in pipeline order once the copies are in, so
        // nothing downstream of `parent` needs recomputing here.
        self.transformation_infos[parent].update_input_fp(copy_fp, target_fp);
        self.rebuild_producer_consumer(originals)
    }

    /// Reorders the transformations so that every input is produced before
    /// it is consumed, keeping the current relative order otherwise.
    /// Materialization and code generation rely on that pipeline order.
    ///
    /// Returns an internal error if the transformations form a cycle.
    fn sort_pipeline(&mut self, originals: &BTreeSet<u64>) -> Result<(), PlanError> {
        let count = self.transformation_infos.len();
        let mut produced = originals.clone();
        let mut placed = vec![false; count];
        let mut order = Vec::with_capacity(count);
        while order.len() < count {
            let next = (0..count)
                .find(|&index| {
                    let (left, right) = self.transformation_infos[index].input_info_fp();
                    !placed[index]
                        && produced.contains(&left)
                        && right.is_none_or(|right| produced.contains(&right))
                })
                .ok_or_else(|| {
                    PlanError::internal("pushdown: transformations form a cycle".to_string())
                })?;
            placed[next] = true;
            produced.insert(self.transformation_infos[next].output_info_fp());
            order.push(next);
        }
        let sorted: Vec<TransformationInfo> = order
            .iter()
            .map(|&index| self.transformation_infos[index].clone())
            .collect();
        self.transformation_infos = sorted;
        self.rebuild_producer_consumer(originals)
    }
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use super::super::common::test_setup;
    use super::*;

    /// Plans `src` through prepare, core in body order, and pushdown.
    fn plan(src: &str) -> RulePlanner {
        let (mut planner, mut catalog) = test_setup(src);
        planner.prepare(&mut catalog).unwrap();
        while !catalog.is_planned() {
            planner.core(&mut catalog, (0, 1)).unwrap();
        }
        planner.pushdown(&catalog).unwrap();
        planner
    }

    /// Transformations whose two inputs are named `left` and `right`.
    fn binary<'a>(
        planner: &'a RulePlanner,
        left: &str,
        right: &str,
    ) -> Vec<&'a TransformationInfo> {
        planner
            .transformation_infos()
            .iter()
            .filter(|tx| tx.input_name() == (left, Some(right)))
            .collect()
    }

    /// `A(x)` folds into `B` by arity, so `C` joins `D` without it; the copy
    /// lands on `C` and the join reads the copy.
    #[test]
    fn filter_reaches_a_leaf_joined_before_its_carrier() {
        let planner = plan(
            ".decl C(x: int32, z: int32, v: int32)\n.input C\n\
             .decl D(z: int32, w: int32)\n.input D\n\
             .decl B(x: int32, y: int32)\n.input B\n\
             .decl A(x: int32)\n.input A\n\
             .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
             Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x).\n",
        );

        let copies = binary(&planner, "a", "c");
        assert_eq!(copies.len(), 1);
        assert!(matches!(copies[0], TransformationInfo::JoinToKV { .. }));
        assert_eq!(
            copies[0].output_variables(),
            vec![Some("x"), Some("z"), Some("v")]
        );
        let join = binary(&planner, "c", "d");
        assert_eq!(join.len(), 1);
        assert_eq!(join[0].input_info_fp().0, copies[0].output_info_fp());
    }

    /// A copy is appended after its target and its filter, whichever is
    /// later; every consumer must still come after both.
    #[test]
    fn copies_keep_pipeline_order() {
        let planner = plan(
            ".decl C(x: int32, z: int32, v: int32)\n.input C\n\
             .decl D(z: int32, w: int32)\n.input D\n\
             .decl B(x: int32, y: int32)\n.input B\n\
             .decl A(x: int32)\n.input A\n\
             .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
             Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x).\n",
        );

        let mut produced = std::collections::BTreeSet::new();
        for tx in planner.transformation_infos() {
            let (left, right) = tx.input_info_fp();
            for input in std::iter::once(left).chain(right) {
                assert!(
                    produced.contains(&input) || planner.producer(input).is_none(),
                    "input {input:#018x} of {} is produced later",
                    tx.output_name()
                );
            }
            produced.insert(tx.output_info_fp());
        }
    }

    /// `C` meets the carrier `B` at its first join, so it gets no copy.
    #[test]
    fn side_enforced_by_its_first_join_gets_no_copy() {
        let planner = plan(
            ".decl B(x: int32, y: int32)\n.input B\n\
             .decl C(x: int32, z: int32)\n.input C\n\
             .decl A(x: int32)\n.input A\n\
             .decl Out(x: int32, y: int32, z: int32)\n.output Out\n\
             Out(x, y, z) :- B(x, y), C(x, z), A(x).\n",
        );

        assert_eq!(binary(&planner, "a", "b").len(), 1);
        assert!(binary(&planner, "a", "c").is_empty());
    }

    /// Same plan shape with `!N(x)` in place of `A(x)`: the copy on `C` is
    /// an antijoin.
    #[test]
    fn negated_filter_reaches_a_leaf_as_an_antijoin() {
        let planner = plan(
            ".decl C(x: int32, z: int32, v: int32)\n.input C\n\
             .decl D(z: int32, w: int32)\n.input D\n\
             .decl B(x: int32, y: int32)\n.input B\n\
             .decl N(x: int32)\n.input N\n\
             .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
             Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), !N(x).\n",
        );

        let copies = binary(&planner, "n", "c");
        assert_eq!(copies.len(), 1);
        assert!(matches!(copies[0], TransformationInfo::AntiJoinToKV { .. }));
    }

    /// `F(x, z)` spans the join of `A` and `B`, so core folds it on the join
    /// output and no leaf can take a copy.
    #[test]
    fn spanning_filter_stays_on_the_join() {
        let planner = plan(
            ".decl A(x: int32, y: int32)\n.input A\n\
             .decl B(y: int32, z: int32)\n.input B\n\
             .decl F(x: int32, z: int32)\n.input F\n\
             .decl Out(x: int32, y: int32, z: int32)\n.output Out\n\
             Out(x, y, z) :- A(x, y), B(y, z), F(x, z).\n",
        );

        let applications = planner
            .transformation_infos()
            .iter()
            .filter(|tx| matches!(tx.input_name(), ("f", Some(_))))
            .count();
        assert_eq!(applications, 1);
    }

    /// `A(x)` and `E(x)` merge into one semijoin output before folding into
    /// `B`. Only `A`, an original atom, is copied onto `C`.
    #[test]
    fn merged_filter_is_not_copied() {
        let planner = plan(
            ".decl C(x: int32, z: int32, v: int32)\n.input C\n\
             .decl D(z: int32, w: int32)\n.input D\n\
             .decl B(x: int32, y: int32)\n.input B\n\
             .decl A(x: int32)\n.input A\n\
             .decl E(x: int32)\n.input E\n\
             .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
             Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x), E(x).\n",
        );

        let onto_c: Vec<_> = planner
            .transformation_infos()
            .iter()
            .filter(|tx| tx.input_name().1 == Some("c"))
            .collect();
        assert_eq!(onto_c.len(), 1);
        assert_eq!(onto_c[0].input_name().0, "a");
    }

    /// `N(x, z)` is antijoined against `C`; the copy of `A` lands on `C`'s
    /// side of that antijoin and never on `N`.
    #[test]
    fn copy_skips_the_negated_side_of_an_antijoin() {
        let planner = plan(
            ".decl C(x: int32, z: int32, v: int32)\n.input C\n\
             .decl D(z: int32, w: int32)\n.input D\n\
             .decl B(x: int32, y: int32)\n.input B\n\
             .decl A(x: int32)\n.input A\n\
             .decl N(x: int32, z: int32)\n.input N\n\
             .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
             Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x), !N(x, z).\n",
        );

        assert_eq!(binary(&planner, "a", "c").len(), 1);
        assert!(binary(&planner, "a", "n").is_empty());
    }
}
