//! Pushdown phase: copies each folded original filter down the finished
//! plan, wherever a copy shrinks a join that nothing else would.
//!
//! A robust plan applies every filter atom to every atom that can take it,
//! so no join ever reads rows that a filter elsewhere in the rule would
//! discard. In differential dataflow that robustness is not free: each
//! semijoin or antijoin arranges its retained input, and the arrangement
//! is kept in memory and maintained on every update. On small inputs or
//! under incremental evaluation the arrangements cost more than the rows
//! they save, so prepare and the join steps fold each filter into exactly ONE
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
//! picks `B` for `A`, and the join steps go in body order:
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
//! For each folded original filter, walk the plan top down to every
//! deepest node whose columns still cover the filter's variables, unless
//! the first join above already carries the filter on its other input,
//! and land a copy at the top of the chain of maps above that node: on
//! the edge into the join or antijoin that reads the chain. Lower in the
//! chain the copy would shrink no join, since a chain has none, and would
//! arrange rows and columns the maps above it drop; below a filter it
//! would also leave the filter reading a join, which fuse cannot merge a
//! filter into. A copy sits between the chain and the one reader that led
//! to it, and the chain's column names come from that reader. Names
//! cannot live on the node: two atoms of one relation can plan the same
//! transformation, share its fingerprint, and name the same column
//! differently. A fold holds within its scope, the rule's body or the
//! braces of an aggregate, so each scope is walked on its own with the
//! folds inside it; [`RulePlanner::scopes`] tells them apart. The walk is
//! documented on [`RulePlanner::push_into`] and the shape of a copy on
//! [`RulePlanner::insert_copy`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::Catalog;
use crate::catalog::FactorPos;
use crate::catalog::JoinPredicates;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

// =========================================================================
// Filter
// =========================================================================

/// A folded original atom, as the semijoin or antijoin that folded it
/// describes it.
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

/// One scope of the plan: the rule's body or the braces of an aggregate.
struct Scope {
    /// The collection the scope's plan ends in, where its walk starts.
    root: u64,
    /// The transformations the walk may enter: the scope's own and those
    /// of the scopes nested in it. A fold of the enclosing scope reaches
    /// into the braces, as its rows agree with the groups; a fold inside
    /// the braces holds for the matches alone, so its walk stops at what
    /// the enclosing scope produces, the keys the braces start from first
    /// of all.
    transformations: BTreeSet<usize>,
    /// The folds of original filters inside the scope, in pipeline order.
    filters: Vec<Filter>,
}

/// What the walk for one scope reads.
struct Walk<'a> {
    /// The rule's original atoms.
    originals: &'a BTreeSet<u64>,
    /// The collections folded as filters anywhere in the plan.
    folded_filters: &'a BTreeSet<u64>,
    /// The transformations the walk may enter.
    transformations: &'a BTreeSet<usize>,
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
    /// Each scope of the plan is walked from its own root with its own
    /// folds: the rule's body from the plan's root, the braces of each
    /// aggregate from their matches. A fold inside braces holds for the
    /// matches alone, never for the groups answered with no match, so it
    /// is copied nowhere outside them; a fold of the enclosing scope
    /// reaches into the braces through the aggregate's matches, as its
    /// rows agree with the groups.
    ///
    /// Runs after the last step, when the catalog holds the single atom
    /// that names the plan's root, and before fuse, which expects consumers
    /// to point at their final producers.
    pub(crate) fn pushdown(&mut self, catalog: &Catalog) -> Result<(), PlanError> {
        let root = catalog.positive_atom_fingerprint(0)?;
        let originals = catalog.original_atom_fingerprints();
        // Described before any copy is inserted, so every filter is read
        // from the plan as the steps left it.
        let scopes = self.scopes(root, originals)?;
        let folded_filters = self.folded_filters();
        let count = self.transformation_infos.len();
        for Scope {
            root,
            transformations,
            filters,
        } in &scopes
        {
            if filters.is_empty() {
                continue;
            }
            let root_index = self.producer(*root).ok_or_else(|| {
                PlanError::internal(format!("pushdown: root {root:#018x} has no producer"))
            })?;
            let walk = Walk {
                originals,
                folded_filters: &folded_filters,
                transformations,
            };
            for filter in filters {
                // An atom of constants, such as `isMethod("<...run()>")` in
                // DOOP, folds as a semijoin with no key: an existence test.
                // It covers every node, and a copy anywhere would shrink
                // nothing.
                if filter.key_names.is_empty() {
                    continue;
                }
                let view = View::of_output(&self.transformation_infos[root_index]);
                // A copy wanted on the root's own edge is dropped: the root
                // has no edge above it, and a copy there would repeat the
                // fold that already applies the filter on the way to the
                // root.
                self.push_into(filter, *root, &view, false, &walk)?;
            }
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
            "Transformations after pushdown:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }

    /// The scopes of the plan under `root`: the rule's body first, then
    /// the braces of each aggregate, whose matches are their root, each
    /// with the folds of original filters inside it.
    ///
    /// A transformation belongs to the first scope whose walk reaches it,
    /// enclosing scopes walking first: the keys the braces start from,
    /// produced outside them and read as their groups too, belong to the
    /// enclosing scope.
    fn scopes(&self, root: u64, originals: &BTreeSet<u64>) -> Result<Vec<Scope>, PlanError> {
        let mut scopes: Vec<Scope> = Vec::new();
        // Each root with the index of the scope enclosing it.
        let mut roots = VecDeque::from([(root, None)]);
        let mut parents = Vec::new();
        let mut claimed = BTreeSet::new();
        while let Some((root, parent)) = roots.pop_front() {
            let mut transformations = BTreeSet::new();
            let mut folds = BTreeSet::new();
            let mut pending = vec![root];
            while let Some(fp) = pending.pop() {
                let Some(index) = self.producer(fp) else {
                    continue;
                };
                if !claimed.insert(index) {
                    continue;
                }
                transformations.insert(index);
                let info = &self.transformation_infos[index];
                if is_fold(info) {
                    folds.insert(index);
                }
                let (left, right) = info.input_info_fp();
                match info {
                    TransformationInfo::KVToKV { .. }
                    | TransformationInfo::JoinToKV { .. }
                    | TransformationInfo::AntiJoinToKV { .. } => {
                        pending.push(left);
                        pending.extend(right);
                    }
                    // The groups are read from this scope's keys; the
                    // matches are a scope of their own.
                    TransformationInfo::AggregateToKV { .. } => {
                        roots.push_back((left, Some(scopes.len())));
                        pending.extend(right);
                    }
                }
            }
            let filters = folds
                .into_iter()
                .filter(|&index| {
                    let (left, _) = self.transformation_infos[index].input_info_fp();
                    self.is_narrowed_original(left, originals)
                })
                .map(|index| self.folded_filter(index))
                .collect::<Result<_, _>>()?;
            parents.push(parent);
            scopes.push(Scope {
                root,
                transformations,
                filters,
            });
        }
        // A scope's walk may enter the scopes nested in it. Every scope
        // follows its enclosing one, so a nested scope is complete when
        // it is added to its parent.
        for child in (0..scopes.len()).rev() {
            if let Some(parent) = parents[child] {
                let nested = scopes[child].transformations.clone();
                scopes[parent].transformations.extend(nested);
            }
        }
        Ok(scopes)
    }

    /// The collections folded as filters anywhere in the plan, original
    /// or not.
    fn folded_filters(&self) -> BTreeSet<u64> {
        self.transformation_infos
            .iter()
            .filter(|info| is_fold(info))
            .map(|info| info.input_info_fp().0)
            .collect()
    }
}

/// Returns `true` if `info` folds a filter: it is a semijoin, a join whose
/// left input holds keys alone, or an antijoin. The steps build no other
/// join with a keys-only left input, since an atom whose variables another
/// covers folds before any join is chosen.
fn is_fold(info: &TransformationInfo) -> bool {
    match info {
        TransformationInfo::JoinToKV {
            left_input_kv_layout,
            ..
        } => left_input_kv_layout.value().is_empty(),
        TransformationInfo::AntiJoinToKV { .. } => true,
        TransformationInfo::KVToKV { .. } | TransformationInfo::AggregateToKV { .. } => false,
    }
}

// =========================================================================
// Walk
// =========================================================================
impl RulePlanner {
    /// Describes the filter the semijoin or antijoin at index `applied`
    /// folded, naming its columns as that application does.
    ///
    /// Returns an internal error if one of the application's key columns
    /// has no variable name.
    fn folded_filter(&self, applied: usize) -> Result<Filter, PlanError> {
        let applied = &self.transformation_infos[applied];
        let (fp, _) = applied.input_info_fp();
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

    /// Pushes `filter` into the subtree producing `fp`, which its reader
    /// sees as `view`, and lands a copy at the top of every chain of maps
    /// whose deepest node covers the filter's variables: on the edge into
    /// the join or antijoin reading the chain, so that only this reader
    /// sees the copy and any other reader of the same collection is
    /// untouched. Returns `true` if a copy belongs on the edge above `fp`
    /// and is not placed yet, because `fp` is such a deepest node or a map
    /// above it; the join reading the chain lands it, a map passes it up.
    /// The negated side of an antijoin never takes a copy, where a filter
    /// would let rows through instead of removing them.
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
        view: &View,
        enforced: bool,
        walk: &Walk<'_>,
    ) -> Result<bool, PlanError> {
        if fp == filter.fp || !view.covers(filter) {
            return Ok(false);
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
            // The matches are the retained side: every answer's key is a
            // group's, so a filter the groups carry holds for the matches.
            // The groups take no copy; the keys they are read from carry
            // every fold of the enclosing scope already.
            TransformationInfo::AggregateToKV {
                groups_input_info_fp,
                ..
            } => (vec![(Side::Left, true)], groups_input_info_fp.is_some()),
        };

        let mut descended = false;
        for (side, retained) in sides {
            let (child, child_view) = self.input(index, side)?;
            // An original atom is not a node: the walk stops at the
            // transformation that reads it.
            let Some(target) = self.producer(child) else {
                continue;
            };
            if child == filter.fp || !child_view.covers(filter) {
                continue;
            }
            descended = true;
            // A collection the scope does not produce, the keys its braces
            // start from, is a leaf the walk has looked into: nothing of
            // it takes a copy, and none belongs above this node for it.
            if !walk.transformations.contains(&target) {
                continue;
            }
            // Computed now rather than up front: when both inputs of a join
            // cover the filter and neither carries it, the copies added
            // under the first are what make a copy under the second
            // redundant.
            let child_enforced = (retained && enforced)
                || (binary && {
                    let (sibling, sibling_view) = self.input(index, side.other())?;
                    self.carries(sibling, filter) && sibling_view.covers(filter)
                });
            if !self.push_into(filter, child, &child_view, child_enforced, walk)? {
                continue;
            }
            // Inside a chain of maps the copy climbs to the join that reads
            // the chain; the module doc says why the top is the place.
            if !binary {
                return Ok(true);
            }
            self.land(filter, target, index, &child_view, walk)?;
        }
        Ok(!descended && !enforced)
    }

    /// Lands a copy of `filter` on the edge from the output of
    /// transformation `target` into transformation `reader`, which sees
    /// that output as `view`, unless the output is a folded filter or has
    /// more readers than producers.
    fn land(
        &mut self,
        filter: &Filter,
        target: usize,
        reader: usize,
        view: &View,
        walk: &Walk<'_>,
    ) -> Result<(), PlanError> {
        let fp = self.transformation_infos[target].output_info_fp();
        // Fuse arranges each producer by one key layout. The copy takes
        // over `reader`'s read of the output, so its readers must not
        // outnumber its producers afterwards either. A folded filter is
        // read by all of its columns everywhere and takes no copy, whether
        // or not it is an original one.
        let (producers, consumers) = &self.producer_consumer[&fp];
        if walk.folded_filters.contains(&fp) || consumers.len() > producers.len() {
            return Ok(());
        }
        self.insert_copy(filter, target, reader, view, walk.originals)
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
            TransformationInfo::JoinToKV { .. }
            | TransformationInfo::AntiJoinToKV { .. }
            | TransformationInfo::AggregateToKV { .. } => false,
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
                // Every answer's key is a group's, so the answers carry
                // what the groups do. The matches are left out: a group
                // with no match is answered too, and no fold inside the
                // braces holds for it.
                TransformationInfo::AggregateToKV { .. } => pending.extend(right),
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
    /// the copy reads the output, and `parent` reads the copy instead. The
    /// copy numbers the atoms of its two inputs apart, whatever the fold
    /// and the reader numbered them.
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
        // A copy is the one join built outside the catalog: the filter's
        // layout keeps the atom number its fold gave it and the target's
        // positions the one their reader gave the target, and nothing kept
        // the two apart. The copy numbers the filter as the atom after the
        // target's, as a fold would number the filter had the rule listed
        // it right after the target. Fuse and codegen address columns by
        // argument id, which the renumbering keeps.
        let next = view
            .positions
            .iter()
            .flat_map(ArithmeticPos::signatures)
            .map(|signature| signature.atom_signature().rhs_id() + 1)
            .max()
            .unwrap_or(0);
        let keys = filter
            .layout
            .key()
            .iter()
            .map(|position| {
                position.map_vars(&|signature| {
                    FactorPos::Var(AtomArgumentSignature::new(
                        AtomSignature::new(signature.atom_signature().is_positive(), next),
                        signature.argument_id(),
                    ))
                })
            })
            .collect();
        let arranged = KeyValueLayout::new(keys, Vec::new());
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
                arranged,
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
                arranged,
                retained,
                output,
            )
        };
        tx.set_variables(variables);
        let copy_fp = tx.output_info_fp();
        trace!("Pushdown transformation:\n{tx}");
        // Appended out of pipeline order: the filter's producer may sit
        // after the target when a step folded the filter late. `pushdown`
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
    use rstest::rstest;

    use super::*;
    use crate::test_harness::rule_planner;

    /// Plans `src` through prepare, joins in body order, and pushdown.
    fn plan(src: &str) -> RulePlanner {
        let (mut planner, mut catalog) = rule_planner(src);
        planner.prepare(&mut catalog).unwrap();
        while !catalog.is_planned() {
            planner.join(&mut catalog, (0, 1)).unwrap();
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

    /// `F(x, z)` spans the join of `A` and `B`, so the join step folds it
    /// on the join output and no leaf can take a copy.
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

    /// `C(x, "c", "d", z, v)` plans as two constant filters over `C`, read
    /// by the join with `D`, and `E` folds into `B`.
    const FILTERED_LEAF: &str = ".decl C(x: int32, t: symbol, k: symbol, z: int32, v: int32)\n.input C\n\
         .decl D(z: int32, w: int32)\n.input D\n\
         .decl B(x: int32, y: int32)\n.input B\n\
         .decl E(x: int32)\n.input E\n\
         .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n.output Out\n\
         Out(x, y, z, w, v) :- C(x, \"c\", \"d\", z, v), D(z, w), B(x, y), E(x).\n";

    /// The copy of `E` lands above both filters on `C`. Between them it
    /// would leave the second filter reading a semijoin, and fuse has no
    /// place for a filter's predicate on a semijoin or an antijoin.
    #[test]
    fn copy_never_separates_a_filter_from_its_producer() {
        let planner = plan(FILTERED_LEAF);

        let infos = planner.transformation_infos();
        let applications = infos
            .iter()
            .filter(|tx| matches!(tx, TransformationInfo::JoinToKV { .. }))
            .filter(|tx| tx.input_name().0 == "e")
            .count();
        assert_eq!(
            applications, 2,
            "the fold of `E` into `B` and one copy onto `C`"
        );
        for tx in infos {
            let TransformationInfo::KVToKV {
                input_info_fp,
                predicates,
                ..
            } = tx
            else {
                continue;
            };
            if predicates.is_empty() {
                continue;
            }
            let Some(producer) = planner.producer(*input_info_fp) else {
                continue;
            };
            assert!(
                matches!(infos[producer], TransformationInfo::KVToKV { .. }),
                "filter {} reads a semijoin or antijoin",
                tx.output_name()
            );
        }
    }

    /// The copy of `E` lands on the edge from `C`'s chain into the join
    /// with `D`, the top of the chain, where it arranges only the rows
    /// both filters let through.
    #[test]
    fn copy_lands_at_the_top_of_a_chain_of_maps() {
        let planner = plan(FILTERED_LEAF);

        let infos = planner.transformation_infos();
        let join = infos
            .iter()
            .find(|tx| tx.input_name().1 == Some("d"))
            .expect("the join of C and D");
        let copy = &infos[planner
            .producer(join.input_info_fp().0)
            .expect("the join's left input is produced")];
        assert_eq!(copy.input_name().0, "e", "the join reads the copy of E");
        let (_, Some(mut fp)) = copy.input_info_fp() else {
            panic!("a copy reads two inputs");
        };
        let mut filters = 0;
        while let Some(index) = planner.producer(fp) {
            let TransformationInfo::KVToKV {
                input_info_fp,
                predicates,
                ..
            } = &infos[index]
            else {
                panic!("the chain under the copy is maps only");
            };
            assert!(
                !predicates.is_empty(),
                "each map under the copy is a filter"
            );
            filters += 1;
            fp = *input_info_fp;
        }
        assert_eq!(filters, 2, "both filters sit under the copy");
    }

    /// `P(x, "a", "b", _)` folds into `A` after two constant filters and a
    /// projection. A folded filter takes no copy, so the copy of `!U` does
    /// not land inside its chain either, where it would have separated
    /// the second filter from its producer (issue #409).
    #[test]
    fn copy_skips_a_folded_filter_however_deep_its_chain() {
        let planner = plan(
            ".decl P(x: symbol, t: symbol, k: symbol, d: symbol)\n.input P\n\
             .decl A(x: symbol)\n.input A\n\
             .decl U(x: symbol, k: symbol, v: symbol)\n.input U\n\
             .decl V(x: symbol)\n.output V\n\
             V(x) :- P(x, \"a\", \"b\", _), A(x), !U(x, _, _).\n",
        );

        let antijoins = planner
            .transformation_infos()
            .iter()
            .filter(|tx| matches!(tx, TransformationInfo::AntiJoinToKV { .. }))
            .count();
        assert_eq!(antijoins, 1, "only the fold of `!U`");
    }

    /// `A(x, "p", p)` and `A(x, "k", k)` fold to `x` and fold into one
    /// another, as does the third atom that keeps only `x`. A copy of one
    /// onto another's chain would make each read the other, so none lands
    /// there; the one copy that does land is of `A(x, "p", p)` onto
    /// `A(x, "r", r)`, which folds nowhere. Naming the unused `p` and `k`
    /// keeps a projection above each constant filter, so each chain has an
    /// inside (issue #411: copies on the leaves of two folded filters'
    /// chains formed a cycle; issue #410: a copy under the projection of
    /// one left fuse merging that projection into a copy).
    // Cases: rule, (the number of folds plus the copy).
    #[rstest]
    #[case::filtered(
        "V(x) :- P(x, \"a\", \"b\", \"c\"), A(x, \"r\", r), A(x, \"p\", p), A(x, \"k\", k), \
         R(x, r, \"u\", u).",
        5
    )]
    #[case::unary(
        "V(x) :- S(x), A(x, \"r\", r), A(x, \"p\", p), A(x, \"k\", k), R(x, r, _, u).",
        5
    )]
    fn folded_filters_take_no_copies_of_each_other(#[case] rule: &str, #[case] expected: usize) {
        let planner = plan(&format!(
            ".decl P(x: symbol, t: symbol, k: symbol, d: symbol)\n.input P\n\
             .decl S(x: symbol)\n.input S\n\
             .decl A(x: symbol, k: symbol, v: symbol)\n.input A\n\
             .decl R(x: symbol, r: symbol, k: symbol, v: symbol)\n.input R\n\
             .decl V(x: symbol)\n.output V\n{rule}\n"
        ));

        let infos = planner.transformation_infos();
        let folded = planner.folded_filters();
        // A folded filter and every map in the chain under it.
        let mut chains = std::collections::BTreeSet::new();
        for &filter in &folded {
            let mut fp = filter;
            chains.insert(fp);
            while let Some(index) = planner.producer(fp) {
                let TransformationInfo::KVToKV { input_info_fp, .. } = &infos[index] else {
                    break;
                };
                fp = *input_info_fp;
                chains.insert(fp);
            }
        }
        let mut applications = 0;
        for tx in infos {
            let (filter, Some(target)) = tx.input_info_fp() else {
                continue;
            };
            if !folded.contains(&filter) {
                continue;
            }
            applications += 1;
            assert!(
                !chains.contains(&target),
                "{} applies a folded filter inside another's chain",
                tx.output_name()
            );
        }
        assert_eq!(
            applications, expected,
            "the joins that fold a filter, and the copy onto `A(x, \"r\", r)`"
        );
    }

    /// `!A(x, "n", _)` folds onto `A(x, "p", p)` under the projection
    /// that keeps `x`, so the copy of `S` lands on the positive side of
    /// that antijoin. The antijoin numbers that side atom 0, as `S`'s fold
    /// numbers `S`; the copy numbers `S` past it, or canonical form could
    /// not tell the two inputs apart (issue #410).
    #[test]
    fn copy_onto_an_antijoin_fold_tells_its_inputs_apart() {
        let planner = plan(
            ".decl S(x: symbol)\n.input S\n\
             .decl A(x: symbol, k: symbol, v: symbol)\n.input A\n\
             .decl R(x: symbol, r: symbol, k: symbol, v: symbol)\n.input R\n\
             .decl V(x: symbol)\n.output V\n\
             V(x) :- S(x), A(x, \"r\", r), A(x, \"p\", p), A(x, \"k\", k), !A(x, \"n\", _), \
             R(x, r, _, u).\n",
        );

        let atoms = |layout: &KeyValueLayout| -> BTreeSet<AtomSignature> {
            layout
                .key()
                .iter()
                .chain(layout.value())
                .flat_map(ArithmeticPos::signatures)
                .map(|signature| *signature.atom_signature())
                .collect()
        };
        let copy = binary(&planner, "s", "π[x,p](a)");
        assert_eq!(copy.len(), 1, "the copy of `S` onto `A(x, \"p\", p)`");
        let (left, Some(right)) = copy[0].input_kv_layout() else {
            panic!("a copy reads two inputs");
        };
        assert!(
            atoms(left).is_disjoint(&atoms(right)),
            "the copy numbers an atom of both inputs alike: {left} and {right}"
        );
    }
}
