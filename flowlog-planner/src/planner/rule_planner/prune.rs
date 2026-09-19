//! Prune phase: drops semijoins and antijoins that the plan reaching it
//! applies more than once.
//!
//! The semijoin pass (`apply_semijoin`, run to a fixed point by prepare and
//! again by core) pushes each semijoin and antijoin into every atom that
//! can take it. Take `Out(x, y, z) :- R(x), A(x, y), B(x, y, z).`: both
//! `A` and `B`
//! get a semijoin against `R`, and then the filtered `B` is semijoined
//! against the filtered `A` on `(x, y)`. That last step already forces `x`
//! to be in `R`, so the semijoin on `B` does nothing the plan does not do
//! anyway. Prune finds such duplicate filters, works out which copies the
//! others make redundant, and rewires the plan around them.
//!
//! Comparisons are left alone on purpose. In differential dataflow a
//! comparison is a filter closure with no arrangement, so a duplicate copy
//! costs almost nothing and only shrinks the rows that reach the next
//! arrangement. A semijoin or antijoin is a join with an arranged retained
//! input, so a redundant copy is real state worth removing.
//!
//! How the plan is read here: every transformation produces a relation
//! named by its output fingerprint. The relation's columns are numbered by
//! position, keys first. When a consumer's layout mentions a position, the
//! `argument_id` of that position is the producer column it reads. A join
//! output position names the input position it copies, and the key
//! positions of both join inputs hold the same values on every output
//! row. That equality is what lets a filter on one input stand in for the
//! same filter on the other.
//!
//! The file has five parts: the entry point, finding filters, tracing
//! where each filter's constraint keeps holding (and the cover search
//! that picks the smallest set of filters to keep), rewriting the plan,
//! and the column bookkeeping the other parts share.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;

use tracing::trace;
use tracing::warn;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::ComparisonExprPos;
use crate::catalog::FactorPos;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

/// Search nodes `minimum_cover` may visit before it settles for the best
/// cover found so far. Realistic groups are solved in a few dozen nodes;
/// the budget only matters when the clauses form large cliques, where an
/// exact cover is exponential, and there a slightly larger cover only
/// means a few more filters stay.
const COVER_BUDGET: usize = 1 << 16;

// =============================================================================
// Entry point
// =============================================================================

impl RulePlanner {
    /// Removes semijoins and antijoins that another copy of the same
    /// filter already makes redundant.
    ///
    /// The output tuple set does not change. What changes is the shape of
    /// the plan, and that is a trade. In differential dataflow every
    /// semijoin or antijoin is an operator with an arranged retained
    /// input, and an arrangement costs memory and per-update work for as
    /// long as the dataflow runs. A duplicate copy does make the
    /// intermediate it feeds smaller. But once that intermediate meets an
    /// input filtered the same way, the join enforces the constraint by
    /// itself, and the copy pays for its arrangement without buying
    /// anything. Prune takes the smaller plan and accepts the larger
    /// intermediate.
    ///
    /// The trade is only made within one hop. A copy is removed when the
    /// matching filter sits on the other input of the first semijoin,
    /// antijoin, or full join its output reaches. On the way there it may
    /// pass through unary maps and through the retained input of a
    /// semijoin or antijoin, since none of those make a relation bigger.
    /// It never crosses a full join. A full join can produce more rows than
    /// either input has, so a filter behind it may be the only thing that
    /// keeps that output small, and nothing downstream can refund the work
    /// of producing those rows.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the plan does not have exactly one
    /// final output, if a consumer of a removed filter refers to a column
    /// the filter's input does not carry, or if the rebuilt dependency
    /// index finds an input with no producer.
    pub(crate) fn prune(&mut self) -> Result<(), PlanError> {
        if self.transformation_infos.is_empty() {
            return Ok(());
        }
        let original_atoms = self.rhs_atom_fps();
        let root = self.root()?;
        loop {
            let removed = self.redundant_filters();
            if removed.is_empty() {
                trace!(
                    "Transformation infos after prune:\n{}",
                    self.transformation_infos_dump()
                );
                return Ok(());
            }
            for filter in &removed {
                trace!("[prune] removing {:?}", filter);
                self.remove_filter(filter)?;
            }
            // `remove_filter` changes inputs only, so the producer side of
            // the index, which `remove_unreachable` reads, is still current.
            // The consumer side is stale and dropping transformations
            // shifts indices, so the index is rebuilt before the next pass.
            self.remove_unreachable(root);
            self.rebuild_producer_consumer(&original_atoms)?;
        }
    }
}

// =============================================================================
// Filters
// =============================================================================

/// Which input of a binary transformation is meant. A unary transformation
/// has only `Left`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Side {
    Left,
    Right,
}

/// One logical filter: a semijoin or antijoin, seen through the relation
/// it produces. It says: every row of relation `fp` has its `tested`
/// columns in the witness (or, for an antijoin, not in it), because every
/// producer of `fp` enforces that. Producers that share a fingerprint are
/// identical copies, so a filter is kept or removed for all of them
/// together.
///
/// The witness is the key-only input of a semijoin or antijoin. The name
/// comes from its role: a row of the retained input survives a semijoin
/// only if some row of the witness vouches for it by having the same key.
/// The semijoin pass always uses every witness column, in order, as that
/// key, so the witness fingerprint alone says which set is tested, and
/// `tested[k]` lines up with witness column `k`. Two filters are copies of
/// one another when they share `witness` and `negative`.
#[derive(Debug)]
struct Filter {
    /// Output fingerprint of the producers that apply the filter.
    fp: u64,
    /// Output fingerprint of the witness relation.
    witness: u64,
    /// `true` for an antijoin.
    negative: bool,
    /// The columns of `fp` compared with the witness, `tested[k]` with
    /// witness column `k`. Semijoining `B(x, y, z)` against `R(x)` yields
    /// the columns `(x | y, z)` and tests `[0]`.
    tested: Vec<usize>,
}

impl RulePlanner {
    /// Collects every filter in the plan, in producer order, one per output
    /// fingerprint. A filter whose output no longer carries one of its
    /// tested columns is skipped: nothing downstream can see that column,
    /// so the filter can neither prove another copy nor be proven by one.
    fn filters(&self) -> Vec<Filter> {
        let mut filters = Vec::new();
        for (index, tx) in self.transformation_infos.iter().enumerate() {
            let fp = tx.output_info_fp();
            if self.producers(fp).first() != Some(&index) {
                continue;
            }
            match tx {
                TransformationInfo::JoinToKV {
                    left_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    ..
                } if left_input_kv_layout.value().is_empty() => {
                    filters.extend(membership_filter(
                        tx,
                        fp,
                        *left_input_info_fp,
                        false,
                        right_input_kv_layout,
                    ));
                }
                TransformationInfo::AntiJoinToKV {
                    left_input_info_fp,
                    right_input_kv_layout,
                    ..
                } => {
                    filters.extend(membership_filter(
                        tx,
                        fp,
                        *left_input_info_fp,
                        true,
                        right_input_kv_layout,
                    ));
                }
                // A full join combines rows; it filters nothing. A map may
                // filter, but a comparison is a closure without an
                // arrangement, so its duplicates are not worth removing.
                TransformationInfo::JoinToKV { .. } | TransformationInfo::KVToKV { .. } => {}
            }
        }
        filters
    }

    /// Finds the first group of duplicate filters with at least one
    /// redundant member and returns those members.
    ///
    /// Only one group is handled per call. Removing a filter changes the
    /// paths that every other filter's proof was traced along, so the
    /// remaining groups are recomputed afterwards.
    fn redundant_filters(&self) -> Vec<Filter> {
        for group in duplicate_groups(self.filters()) {
            let reach: Vec<BTreeSet<Trace>> = group.iter().map(|f| self.reach(f)).collect();
            let clauses: Vec<Vec<usize>> = (0..group.len())
                .flat_map(|member| self.exit_clauses(&group, &reach, member))
                .collect();
            let kept = minimum_cover(&clauses, group.len());
            let removed: Vec<Filter> = group
                .into_iter()
                .enumerate()
                .filter(|(member, _)| !kept.contains(*member))
                .map(|(_, filter)| filter)
                .collect();
            if !removed.is_empty() {
                return removed;
            }
        }
        Vec::new()
    }
}

/// Builds the filter of a semijoin or antijoin from its witness fingerprint
/// and the layout of its retained input. Returns `None` when a retained
/// key is a computed expression rather than a plain column, or when the
/// output of `tx` no longer carries one of the tested columns.
fn membership_filter(
    tx: &TransformationInfo,
    fp: u64,
    witness: u64,
    negative: bool,
    retained: &KeyValueLayout,
) -> Option<Filter> {
    let tested = plain_columns(retained.key())?;
    Some(Filter {
        fp,
        witness,
        negative,
        tested: carry(tx, Side::Right, &tested)?,
    })
}

/// Groups filters that share a witness and polarity, in order of first
/// appearance, and drops every group of one: a filter with no twin has
/// nothing to be compared with.
fn duplicate_groups(filters: Vec<Filter>) -> Vec<Vec<Filter>> {
    let mut groups: Vec<Vec<Filter>> = Vec::new();
    for filter in filters {
        match groups.iter_mut().find(|group| {
            group[0].witness == filter.witness && group[0].negative == filter.negative
        }) {
            Some(group) => group.push(filter),
            None => groups.push(vec![filter]),
        }
    }
    groups.retain(|group| group.len() > 1);
    groups
}

// =============================================================================
// Tracing
// =============================================================================

/// A place where a filter's constraint is known to hold: every row of
/// relation `fp` satisfies it on these columns. Traces are the common
/// currency of the proof. A join's output columns are the frame its two
/// inputs share: output key `k` is fed by key slot `k` of both inputs, so
/// two copies arriving at a join on the same key slot produce the same
/// trace there, while a value column comes from one input only and can
/// never match a trace from the other.
type Trace = (u64, Vec<usize>);

impl RulePlanner {
    /// Walks forward from `start` through every consumer input the
    /// constraint reaches. `visit` is called once per consumer input with
    /// the result of [`Self::step`] and returns the trace to continue
    /// with, if any. Reaching the final output calls `visit` with no trace.
    fn follow(&self, start: Trace, mut visit: impl FnMut(Option<Trace>, bool) -> Option<Trace>) {
        let mut pending = vec![start];
        let mut seen: HashSet<Trace> = HashSet::new();
        while let Some(trace) = pending.pop() {
            if !seen.insert(trace.clone()) {
                continue;
            }
            let consumers = self.consumers(trace.0);
            if consumers.is_empty() {
                visit(None, false);
                continue;
            }
            for consumer in consumers {
                for side in [Side::Left, Side::Right] {
                    if input_fp(&self.transformation_infos[consumer], side) != Some(trace.0) {
                        continue;
                    }
                    let (known, continues) = self.step(&trace, consumer, side);
                    pending.extend(visit(known, continues));
                }
            }
        }
    }

    /// What input `side` of `consumer` does with a trace: the trace on the
    /// consumer's output, if the output still carries the tested columns,
    /// and whether the walk may go on past this consumer.
    ///
    /// The constraint holds on every output column that carries a tested
    /// column: through a map, through either input of a join (keys of both
    /// inputs are equal on the output), and through the retained input of
    /// an antijoin. An antijoin's excluded input carries nothing to the
    /// output, so the trace ends there with no proof; widening the excluded
    /// set would drop rows that nothing downstream can bring back.
    ///
    /// The walk continues only past a map and past the retained input of a
    /// semijoin or antijoin, since none of those make a relation bigger. A
    /// full join or a semijoin's witness ends the walk: past that point the
    /// plan must not be widened, so the copy can only be proven right there.
    fn step(&self, (_, columns): &Trace, consumer: usize, side: Side) -> (Option<Trace>, bool) {
        let tx = &self.transformation_infos[consumer];
        let known = carry(tx, side, columns).map(|columns| (tx.output_info_fp(), columns));
        let continues = match tx {
            TransformationInfo::KVToKV { .. } => true,
            TransformationInfo::AntiJoinToKV { .. } => side == Side::Right,
            TransformationInfo::JoinToKV {
                left_input_kv_layout,
                ..
            } => left_input_kv_layout.value().is_empty() && side == Side::Right,
        };
        (known, continues)
    }

    /// Every trace of `filter` if it is kept: its own output and every
    /// place downstream where its constraint is known to hold, including
    /// the outputs of the joins that end its walk.
    fn reach(&self, filter: &Filter) -> BTreeSet<Trace> {
        let start = (filter.fp, filter.tested.clone());
        let mut traces = BTreeSet::from([start.clone()]);
        self.follow(start, |known, continues| {
            traces.extend(known.clone());
            if continues { known } else { None }
        });
        traces
    }

    /// The clauses that member `member` of `group` contributes: one per
    /// place its walk ends, each a set of members of which at least one
    /// must stay. A member is proven
    /// wherever another member's reach holds the same trace. The member
    /// itself is in every one of its clauses, so two copies that prove each
    /// other can never both be removed. A walk that is proven at a point it
    /// could continue past stops there instead.
    fn exit_clauses(
        &self,
        group: &[Filter],
        reach: &[BTreeSet<Trace>],
        member: usize,
    ) -> Vec<Vec<usize>> {
        let provers = |known: &Trace| -> Vec<usize> {
            (0..group.len())
                .filter(|&other| other != member && reach[other].contains(known))
                .collect()
        };
        let mut clauses = Vec::new();
        self.follow(
            (group[member].fp, group[member].tested.clone()),
            |known, continues| {
                let proven_by = known.as_ref().map_or_else(Vec::new, provers);
                match known {
                    Some(next) if continues && proven_by.is_empty() => Some(next),
                    _ => {
                        let mut clause = proven_by;
                        clause.push(member);
                        clause.sort_unstable();
                        clauses.push(clause);
                        None
                    }
                }
            },
        );
        clauses
    }
}

/// The selected members of one cover, packed into machine words.
#[derive(Clone)]
struct Cover(Box<[u64]>);

impl Cover {
    const WORD_BITS: usize = u64::BITS as usize;

    fn empty(members: usize) -> Self {
        Self(vec![0; members.div_ceil(Self::WORD_BITS)].into_boxed_slice())
    }

    fn all(members: usize) -> Self {
        let full_words = members / Self::WORD_BITS;
        let remaining_bits = members % Self::WORD_BITS;
        let mut words = vec![u64::MAX; full_words];
        if remaining_bits != 0 {
            words.push((1 << remaining_bits) - 1);
        }
        Self(words.into_boxed_slice())
    }

    fn contains(&self, member: usize) -> bool {
        let (word, bit) = Self::word_and_bit(member);
        self.0[word] & bit != 0
    }

    /// Returns `true` if `member` was not selected already.
    fn insert(&mut self, member: usize) -> bool {
        let (word, bit) = Self::word_and_bit(member);
        let was_missing = self.0[word] & bit == 0;
        self.0[word] |= bit;
        was_missing
    }

    fn remove(&mut self, member: usize) {
        let (word, bit) = Self::word_and_bit(member);
        self.0[word] &= !bit;
    }

    fn word_and_bit(member: usize) -> (usize, u64) {
        let word = member / Self::WORD_BITS;
        let bit = 1 << (member % Self::WORD_BITS);
        (word, bit)
    }
}

/// The smallest set of members that hits every clause, by branch and
/// bound within [`COVER_BUDGET`] nodes; past the budget the best cover
/// found so far is returned, which is still a cover, just perhaps not the
/// smallest. Among equally small sets the first one found wins, and the
/// search tries members in ascending order, so that favors the filters
/// that come first in the plan. Members named by a unit clause are fixed
/// before the search.
fn minimum_cover(clauses: &[Vec<usize>], members: usize) -> Cover {
    fn search(
        clauses: &[Vec<usize>],
        chosen: &mut Cover,
        chosen_count: usize,
        best: &mut Cover,
        best_count: &mut usize,
        budget: &mut usize,
    ) -> bool {
        let Some(clause) = clauses
            .iter()
            .find(|clause| clause.iter().all(|&member| !chosen.contains(member)))
        else {
            if chosen_count < *best_count {
                *best = chosen.clone();
                *best_count = chosen_count;
            }
            return false;
        };
        // Covering the clause costs one more member, so a branch that
        // cannot beat `best` is cut here. Cutting at equal size matters:
        // without it every same-size cover would be enumerated, and a run
        // of disjoint clauses has exponentially many.
        if *budget == 0 {
            warn!(
                "prune: cover search reached the {COVER_BUDGET}-node budget; \
                 returning the best valid cover found so far"
            );
            return true;
        }
        if chosen_count + 1 >= *best_count {
            return false;
        }
        *budget -= 1;
        for &member in clause {
            debug_assert!(!chosen.contains(member));
            chosen.insert(member);
            let exhausted = search(clauses, chosen, chosen_count + 1, best, best_count, budget);
            chosen.remove(member);
            if exhausted {
                return true;
            }
        }
        false
    }

    let mut best = Cover::all(members);
    let mut best_count = members;
    let mut forced = Cover::empty(members);
    let mut forced_count = 0;
    for clause in clauses.iter().filter(|clause| clause.len() == 1) {
        let member = clause[0];
        if forced.insert(member) {
            forced_count += 1;
        }
    }
    let mut budget = COVER_BUDGET;
    search(
        clauses,
        &mut forced,
        forced_count,
        &mut best,
        &mut best_count,
        &mut budget,
    );
    best
}

// =============================================================================
// Rewrite
// =============================================================================

impl RulePlanner {
    /// Removes `filter` from the plan: every consumer of `filter.fp` is
    /// rewired to read the filter's retained input directly, and labels
    /// downstream stop naming the filter.
    ///
    /// Consumers are found by scanning the current inputs, not the
    /// dependency index. That way, when a removed filter reads another
    /// removed filter, the order does not matter: the later rewrite builds
    /// on the earlier one.
    ///
    /// The retained input's only consumers were the removed producers, so
    /// after the rewrite it serves exactly the layouts they served, and
    /// the fuse phase's rule of one layout per producer still holds.
    fn remove_filter(&mut self, filter: &Filter) -> Result<(), PlanError> {
        let Some(&representative) = self.producers(filter.fp).first() else {
            return Err(PlanError::internal(format!(
                "prune: filter output {:#018x} has no producer",
                filter.fp
            )));
        };
        let producer = &self.transformation_infos[representative];
        let old_label = producer.output_name().to_string();
        let (target_fp, target_name, target_columns) = detour(producer)?;
        for tx in &mut self.transformation_infos {
            for side in [Side::Left, Side::Right] {
                if input_fp(tx, side) == Some(filter.fp) {
                    rewire(tx, side, target_fp, &target_name, &target_columns)?;
                }
            }
        }
        self.relabel(&old_label, &target_name);
        Ok(())
    }

    /// Replaces `old` with `new` in every hierarchical name, so that names
    /// downstream of a removed filter stop mentioning it.
    fn relabel(&mut self, old: &str, new: &str) {
        if old == new {
            return;
        }
        for tx in &mut self.transformation_infos {
            match tx {
                TransformationInfo::KVToKV {
                    input_name,
                    output_name,
                    ..
                } => {
                    *input_name = input_name.replace(old, new);
                    *output_name = output_name.replace(old, new);
                }
                TransformationInfo::JoinToKV {
                    left_input_name,
                    right_input_name,
                    output_name,
                    ..
                }
                | TransformationInfo::AntiJoinToKV {
                    left_input_name,
                    right_input_name,
                    output_name,
                    ..
                } => {
                    *left_input_name = left_input_name.replace(old, new);
                    *right_input_name = right_input_name.replace(old, new);
                    *output_name = output_name.replace(old, new);
                }
            }
        }
    }

    /// The final output: the one relation that no transformation reads.
    ///
    /// # Errors
    ///
    /// Returns an internal error if there is no such relation or more than
    /// one. Pruning towards the wrong root would silently drop the plan, so
    /// this is checked before the first rewrite.
    fn root(&self) -> Result<u64, PlanError> {
        let roots: BTreeSet<u64> = self
            .transformation_infos
            .iter()
            .map(TransformationInfo::output_info_fp)
            .filter(|&fp| self.consumers(fp).is_empty())
            .collect();
        match roots.iter().next() {
            Some(&root) if roots.len() == 1 => Ok(root),
            _ => {
                let listed: Vec<String> = roots.iter().map(|fp| format!("{fp:#018x}")).collect();
                Err(PlanError::internal(format!(
                    "prune: plan has {} final outputs, expected exactly one: [{}]",
                    roots.len(),
                    listed.join(", ")
                )))
            }
        }
    }

    /// Drops transformations that no longer feed `root`.
    fn remove_unreachable(&mut self, root: u64) {
        let mut pending = vec![root];
        let mut required: HashSet<usize> = HashSet::new();
        while let Some(fp) = pending.pop() {
            for &producer in self.producers(fp) {
                if required.insert(producer) {
                    let (left, right) = self.transformation_infos[producer].input_info_fp();
                    pending.push(left);
                    pending.extend(right);
                }
            }
        }
        let mut index = 0;
        self.transformation_infos.retain(|_| {
            let keep = required.contains(&index);
            index += 1;
            keep
        });
    }
}

/// Where the consumers of `tx` can read instead of `tx`: the fingerprint
/// and name of its retained input, and for each output column of `tx` the
/// retained column that carries it.
///
/// # Errors
///
/// Returns an internal error if `tx` is not a semijoin or antijoin, or if
/// one of its output columns does not come from its retained input.
fn detour(tx: &TransformationInfo) -> Result<(u64, String, Vec<usize>), PlanError> {
    let (right_input_info_fp, right_input_name) = match tx {
        TransformationInfo::JoinToKV {
            right_input_info_fp,
            right_input_name,
            ..
        }
        | TransformationInfo::AntiJoinToKV {
            right_input_info_fp,
            right_input_name,
            ..
        } => (*right_input_info_fp, right_input_name.clone()),
        TransformationInfo::KVToKV { .. } => {
            return Err(PlanError::internal(format!(
                "prune: map {:#018x} is not a semijoin or antijoin",
                tx.output_info_fp()
            )));
        }
    };
    let arity = tx.output_kv_layout().key().len() + tx.output_kv_layout().value().len();
    let columns = (0..arity)
        .map(|column| {
            input_column(tx, Side::Right, column).ok_or_else(|| {
                PlanError::internal(format!(
                    "prune: output column {column} of {:#018x} is not a retained input column",
                    tx.output_info_fp()
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((right_input_info_fp, right_input_name, columns))
}

/// Makes input `side` of `tx` read relation `fp`, named `name`, in place
/// of its current input, where `columns[c]` is the column of `fp` that
/// carries column `c` of the old input. Every position that referred to
/// the old input is renamed to the new column, in the input layout, the
/// output layout, and the predicates, so they all keep naming the same
/// values. Positions of the other input are left alone.
///
/// The output fingerprint is left alone as well, unlike in the fuse phase,
/// which recomputes it after every layout change. It is the lineage name
/// that every downstream input, the root, and the dependency index use,
/// and materialization replaces it with a content fingerprint anyway, so
/// recomputing it here would only mean re-threading those references.
///
/// # Errors
///
/// Returns an internal error if the input layout holds a computed position
/// or refers to a column the new input does not carry.
fn rewire(
    tx: &mut TransformationInfo,
    side: Side,
    fp: u64,
    name: &str,
    columns: &[usize],
) -> Result<(), PlanError> {
    let layout = input_layout(tx, side).ok_or_else(|| {
        PlanError::internal(format!(
            "prune: {:#018x} has no {side:?} input to rewire",
            tx.output_info_fp()
        ))
    })?;
    let mut renames: BTreeMap<AtomArgumentSignature, AtomArgumentSignature> = BTreeMap::new();
    for position in layout.key().iter().chain(layout.value()) {
        let signature = position.plain_var().ok_or_else(|| {
            PlanError::internal(format!(
                "prune: cannot rewire computed input position {position} of {:#018x}",
                tx.output_info_fp()
            ))
        })?;
        let column = columns.get(signature.argument_id()).ok_or_else(|| {
            PlanError::internal(format!(
                "prune: input position {position} of {:#018x} is past the {} columns of \
                 {fp:#018x}",
                tx.output_info_fp(),
                columns.len(),
            ))
        })?;
        renames.insert(
            signature,
            AtomArgumentSignature::new(*signature.atom_signature(), *column),
        );
    }
    let rename = |signature: &AtomArgumentSignature| *renames.get(signature).unwrap_or(signature);
    let rename_expression = |expression: &ArithmeticPos| {
        expression.map_vars(&|signature| FactorPos::Var(rename(signature)))
    };
    let rename_layout = |layout: &KeyValueLayout| {
        KeyValueLayout::new(
            layout.key().iter().map(rename_expression).collect(),
            layout.value().iter().map(rename_expression).collect(),
        )
    };
    let rename_comparisons = |comparisons: &mut Vec<ComparisonExprPos>| {
        for comparison in comparisons.iter_mut() {
            *comparison = ComparisonExprPos::from_parts(
                rename_expression(comparison.left()),
                comparison.operator().clone(),
                rename_expression(comparison.right()),
            );
        }
    };

    match tx {
        TransformationInfo::KVToKV {
            input_info_fp,
            input_name,
            input_kv_layout,
            output_kv_layout,
            predicates,
            ..
        } => {
            *input_info_fp = fp;
            *input_name = name.to_string();
            *input_kv_layout = rename_layout(input_kv_layout);
            *output_kv_layout = rename_layout(output_kv_layout);
            for (signature, _) in &mut predicates.const_eq {
                *signature = rename(signature);
            }
            for (left, right) in &mut predicates.var_eq {
                *left = rename(left);
                *right = rename(right);
            }
            rename_comparisons(&mut predicates.compare_exprs);
        }
        TransformationInfo::JoinToKV {
            left_input_info_fp,
            left_input_name,
            right_input_info_fp,
            right_input_name,
            left_input_kv_layout,
            right_input_kv_layout,
            output_kv_layout,
            predicates,
            ..
        } => {
            let (input_fp, input_name, layout) = match side {
                Side::Left => (left_input_info_fp, left_input_name, left_input_kv_layout),
                Side::Right => (right_input_info_fp, right_input_name, right_input_kv_layout),
            };
            *input_fp = fp;
            *input_name = name.to_string();
            *layout = rename_layout(layout);
            *output_kv_layout = rename_layout(output_kv_layout);
            rename_comparisons(&mut predicates.compare_exprs);
        }
        TransformationInfo::AntiJoinToKV {
            left_input_info_fp,
            left_input_name,
            right_input_info_fp,
            right_input_name,
            left_input_kv_layout,
            right_input_kv_layout,
            output_kv_layout,
            ..
        } => {
            let (input_fp, input_name, layout) = match side {
                Side::Left => (left_input_info_fp, left_input_name, left_input_kv_layout),
                Side::Right => (right_input_info_fp, right_input_name, right_input_kv_layout),
            };
            *input_fp = fp;
            *input_name = name.to_string();
            *layout = rename_layout(layout);
            *output_kv_layout = rename_layout(output_kv_layout);
        }
    }
    Ok(())
}

// =============================================================================
// Column bookkeeping
// =============================================================================

fn input_fp(tx: &TransformationInfo, side: Side) -> Option<u64> {
    let (left, right) = tx.input_info_fp();
    match side {
        Side::Left => Some(left),
        Side::Right => right,
    }
}

fn input_layout(tx: &TransformationInfo, side: Side) -> Option<&KeyValueLayout> {
    let (left, right) = tx.input_kv_layout();
    match side {
        Side::Left => Some(left),
        Side::Right => right,
    }
}

/// The columns that a list of plain positions refers to. `None` if any of
/// them is a computed expression rather than a column.
fn plain_columns(positions: &[ArithmeticPos]) -> Option<Vec<usize>> {
    positions
        .iter()
        .map(|position| {
            position
                .plain_var()
                .map(|signature| signature.argument_id())
        })
        .collect()
}

fn contains_position(layout: &KeyValueLayout, signature: AtomArgumentSignature) -> bool {
    layout
        .key()
        .iter()
        .chain(layout.value())
        .any(|position| position.plain_var() == Some(signature))
}

/// The column of input `side` whose value ends up in output column
/// `output_column` of `tx`, if any. An output key of a join carries the
/// key column of both inputs, because the join made them equal. An
/// antijoin's output carries only columns of its retained input.
fn input_column(tx: &TransformationInfo, side: Side, output_column: usize) -> Option<usize> {
    let output = tx.output_kv_layout();
    let signature = output
        .key()
        .iter()
        .chain(output.value())
        .nth(output_column)?
        .plain_var()?;
    match tx {
        TransformationInfo::KVToKV { .. } => {
            (side == Side::Left).then_some(signature.argument_id())
        }
        TransformationInfo::JoinToKV {
            left_input_kv_layout,
            right_input_kv_layout,
            ..
        } => {
            let (this, other) = match side {
                Side::Left => (left_input_kv_layout, right_input_kv_layout),
                Side::Right => (right_input_kv_layout, left_input_kv_layout),
            };
            if contains_position(this, signature) {
                return Some(signature.argument_id());
            }
            let slot = other
                .key()
                .iter()
                .position(|position| position.plain_var() == Some(signature))?;
            this.key()
                .get(slot)?
                .plain_var()
                .map(|key| key.argument_id())
        }
        TransformationInfo::AntiJoinToKV {
            right_input_kv_layout,
            ..
        } => (side == Side::Right && contains_position(right_input_kv_layout, signature))
            .then_some(signature.argument_id()),
    }
}

/// The output columns of `tx` that carry the given `columns` of input
/// `side`, in the same order. `None` if the output drops any of them.
fn carry(tx: &TransformationInfo, side: Side, columns: &[usize]) -> Option<Vec<usize>> {
    let output = tx.output_kv_layout();
    let arity = output.key().len() + output.value().len();
    columns
        .iter()
        .map(|&column| {
            (0..arity).find(|&candidate| input_column(tx, side, candidate) == Some(column))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;
    use super::*;
    use crate::catalog::AtomSignature;
    use crate::catalog::Catalog;
    use crate::catalog::KvPredicates;

    /// Prepares and core-plans `source` with the first two atoms joined
    /// first, the way the stratum planner would with a trivial optimizer.
    fn plan(source: &str) -> (RulePlanner, Catalog) {
        let (mut planner, mut catalog) = test_setup(source);
        planner.prepare(&mut catalog).expect("prepare");
        while !catalog.is_planned() {
            planner.core(&mut catalog, (0, 1)).expect("core");
        }
        (planner, catalog)
    }

    fn memberships(planner: &RulePlanner) -> usize {
        planner
            .transformation_infos()
            .iter()
            .filter(|tx| match tx {
                TransformationInfo::JoinToKV {
                    left_input_kv_layout,
                    ..
                } => left_input_kv_layout.value().is_empty(),
                TransformationInfo::AntiJoinToKV { .. } => true,
                TransformationInfo::KVToKV { .. } => false,
            })
            .count()
    }

    fn antijoins(planner: &RulePlanner) -> usize {
        planner
            .transformation_infos()
            .iter()
            .filter(|tx| tx.is_neg_join())
            .count()
    }

    fn root(planner: &RulePlanner) -> &TransformationInfo {
        planner.transformation_infos().last().expect("root")
    }

    fn ids(positions: &[ArithmeticPos]) -> Vec<usize> {
        plain_columns(positions).expect("plain positions")
    }

    fn kept_members(cover: &Cover) -> Vec<usize> {
        (0..cover.0.len() * Cover::WORD_BITS)
            .filter(|&member| cover.contains(member))
            .collect()
    }

    /// Whether every output position of `tx` names a position of one of
    /// its inputs, which is what materialization requires.
    fn output_resolves(tx: &TransformationInfo) -> bool {
        let (left, right) = tx.input_kv_layout();
        let output = tx.output_kv_layout();
        output.key().iter().chain(output.value()).all(|position| {
            let signature = position.plain_var().expect("plain output position");
            contains_position(left, signature)
                || right.is_some_and(|right| contains_position(right, signature))
        })
    }

    /// Signature of column `column` of a hand-built relation. Across a
    /// producer/consumer edge only the column is read; `atom` need only
    /// differ between the two inputs of one transformation, so that an
    /// output position resolves to exactly one of them.
    fn sig(atom: usize, column: usize) -> AtomArgumentSignature {
        AtomArgumentSignature::new(AtomSignature::new(true, atom), column)
    }

    fn layout(keys: &[AtomArgumentSignature], values: &[AtomArgumentSignature]) -> KeyValueLayout {
        let positions = |signatures: &[AtomArgumentSignature]| {
            signatures
                .iter()
                .map(|&signature| ArithmeticPos::from_var_signature(signature))
                .collect()
        };
        KeyValueLayout::new(positions(keys), positions(values))
    }

    /// Fingerprint and first argument signature of positive atom `index`.
    fn atom(catalog: &Catalog, index: usize) -> (u64, AtomArgumentSignature) {
        (
            catalog
                .positive_atom_fingerprint(index)
                .expect("fingerprint"),
            catalog
                .positive_atom_argument_signature(index)
                .expect("signature")[0],
        )
    }

    fn install(planner: &mut RulePlanner, transformations: Vec<TransformationInfo>) {
        planner.transformation_infos = transformations;
        let originals = planner.rhs_atom_fps();
        planner
            .rebuild_producer_consumer(&originals)
            .expect("dependency index");
    }

    const R_A_B: &str = "\
        .decl R(x: int32)\n\
        .decl A(x: int32, y: int32)\n\
        .decl B(x: int32, y: int32, z: int32)\n\
        .decl Out(x: int32, y: int32, z: int32)\n\
        .input R\n\
        .input A\n\
        .input B\n\
        .output Out\n\
        Out(x, y, z) :- R(x), A(x, y), B(x, y, z).\n";

    /// `B semijoin R` feeds the retained input of
    /// `(B semijoin R) semijoin (A semijoin R)`; the witness already
    /// restricts `x` to `R`, so the final semijoin reads `B` directly with
    /// its own two-column key intact.
    #[test]
    fn duplicate_semijoin_on_a_retained_input_is_removed() {
        let (mut planner, _) = plan(R_A_B);
        let b_premap = planner.transformation_infos()[2].output_info_fp();
        let root_fp = root(&planner).output_info_fp();
        assert_eq!(memberships(&planner), 3);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 2);
        let final_semijoin = root(&planner);
        assert_eq!(final_semijoin.output_info_fp(), root_fp);
        assert_eq!(final_semijoin.input_info_fp().1, Some(b_premap));
        let right = final_semijoin.input_kv_layout().1.expect("right input");
        assert_eq!(ids(right.key()), vec![0, 1]);
        assert_eq!(ids(right.value()), vec![2]);
        assert!(output_resolves(final_semijoin));
    }

    #[test]
    fn one_of_two_semijoins_meeting_on_a_full_join_key_is_removed() {
        let (mut planner, _) = plan(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl B(x: int32, z: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input R\n\
             .input A\n\
             .input B\n\
             .output Out\n\
             Out(x, y, z) :- R(x), A(x, y), B(x, z).\n",
        );
        let a_semijoin = planner.transformation_infos()[3].output_info_fp();
        let b_premap = planner.transformation_infos()[2].output_info_fp();
        assert_eq!(memberships(&planner), 2);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 1);
        let join = root(&planner);
        assert_eq!(join.input_info_fp(), (a_semijoin, Some(b_premap)));
        assert!(output_resolves(join));
    }

    #[test]
    fn semijoins_tested_on_join_value_columns_are_both_kept() {
        let (mut planner, _) = plan(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl B(y: int32, z: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input R\n\
             .input A\n\
             .input B\n\
             .output Out\n\
             Out(x, y, z) :- R(x), A(x, y), B(y, z), R(z).\n",
        );
        assert_eq!(memberships(&planner), 2);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 2);
    }

    /// `A semijoin R` sits under a full join with `C`; `B semijoin R` is
    /// the witness of the semijoin above that join. Neither reaches the
    /// other.
    #[test]
    fn filters_in_separate_full_join_subtrees_are_kept() {
        let (mut planner, mut catalog) = test_setup(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl B(x: int32, z: int32)\n\
             .decl C(y: int32, z: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input R\n\
             .input A\n\
             .input B\n\
             .input C\n\
             .output Out\n\
             Out(x, y, z) :- R(x), A(x, y), B(x, z), C(y, z).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner.core(&mut catalog, (0, 2)).expect("core");
        assert!(catalog.is_planned());
        assert_eq!(memberships(&planner), 3);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 3);
    }

    /// `A semijoin R` on `x` feeds `(A semijoin R) semijoin R` on `y`: the
    /// same constraint, but the second copy tests a column the first never
    /// did.
    #[test]
    fn duplicate_testing_a_different_column_of_the_same_input_is_kept() {
        let (mut planner, _) = plan(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl Out(x: int32, y: int32)\n\
             .input R\n\
             .input A\n\
             .output Out\n\
             Out(x, y) :- R(x), A(x, y), R(y).\n",
        );
        assert_eq!(memberships(&planner), 2);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 2);
    }

    #[test]
    fn duplicate_antijoins_meeting_on_a_full_join_key_collapse() {
        let (mut planner, _) = plan(
            ".decl A(x: int32, y: int32)\n\
             .decl B(x: int32, z: int32)\n\
             .decl N(x: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input A\n\
             .input B\n\
             .input N\n\
             .output Out\n\
             Out(x, y, z) :- A(x, y), B(x, z), !N(x).\n",
        );
        assert_eq!(antijoins(&planner), 2);

        planner.prune().expect("prune");

        assert_eq!(antijoins(&planner), 1);
        assert!(output_resolves(root(&planner)));
    }

    #[test]
    fn plan_without_duplicate_filters_is_unchanged() {
        let (mut planner, _) = plan(
            ".decl A(x: int32, y: int32)\n\
             .decl B(y: int32, z: int32)\n\
             .decl Out(x: int32, z: int32)\n\
             .input A\n\
             .input B\n\
             .output Out\n\
             Out(x, z) :- A(x, y), B(y, z).\n",
        );
        let before = planner.transformation_infos_dump();

        planner.prune().expect("prune");

        assert_eq!(planner.transformation_infos_dump(), before);
    }

    #[test]
    fn empty_plan_is_left_alone() {
        let (mut planner, _) = plan(
            ".decl A(x: int32)\n\
             .decl Out(x: int32)\n\
             .input A\n\
             .output Out\n\
             Out(x) :- A(x).\n",
        );
        assert!(planner.transformation_infos().is_empty());

        planner.prune().expect("prune");

        assert!(planner.transformation_infos().is_empty());
    }

    #[test]
    fn labels_stop_naming_a_removed_semijoin() {
        let (mut planner, _) = plan(R_A_B);
        let removed = planner.transformation_infos()[4].output_name().to_string();
        assert!(root(&planner).output_name().contains(&removed));

        planner.prune().expect("prune");

        let final_semijoin = root(&planner);
        assert!(!final_semijoin.output_name().contains(&removed));
        assert_eq!(final_semijoin.input_name().1, Some("b"));
    }

    #[test]
    fn plan_with_a_second_final_output_is_an_internal_error() {
        let (mut planner, _) = plan(R_A_B);
        let mut transformations = planner.transformation_infos().to_vec();
        let mut stray = transformations[0].clone();
        stray.update_output_name("stray".into());
        stray.update_row_output(true);
        stray.update_output_fake_sig();
        transformations.push(stray);
        install(&mut planner, transformations);

        let error = planner.prune().expect_err("two final outputs");

        assert!(error.to_string().contains("2 final outputs"));
    }

    /// Clique-shaped clauses make the search for a cover smaller than the
    /// minimum exponential. The budget must cut that search off while the
    /// minimum, 23 of each 24-clique, still comes back.
    #[test]
    fn minimum_cover_stays_within_budget_on_cliques() {
        let mut clauses = Vec::new();
        for clique in 0..2 {
            for a in 0..24 {
                for b in (a + 1)..24 {
                    clauses.push(vec![clique * 24 + a, clique * 24 + b]);
                }
            }
        }

        let cover = minimum_cover(&clauses, 48);

        assert!(
            clauses
                .iter()
                .all(|clause| clause.iter().any(|&member| cover.contains(member)))
        );
        assert_eq!(kept_members(&cover).len(), 46);
    }

    /// A map swaps the two columns of `A semijoin R` before the full join,
    /// so the tested column `x` moves from position 0 to position 1. The
    /// trace must follow the move to line up with `B semijoin R` on the
    /// join key. Built by hand: every map the planner emits keeps its
    /// input's column order, only dropping or appending columns.
    #[test]
    fn tested_column_is_followed_through_a_map_that_moves_it() {
        let (mut planner, catalog) = test_setup(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl B(x: int32, z: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input R\n\
             .input A\n\
             .input B\n\
             .output Out\n\
             Out(x, y, z) :- R(x), A(x, y), B(x, z).\n",
        );
        let (r_fp, r) = atom(&catalog, 0);
        let (a_fp, a) = atom(&catalog, 1);
        let (b_fp, b) = atom(&catalog, 2);
        let second = |signature: AtomArgumentSignature| {
            AtomArgumentSignature::new(*signature.atom_signature(), 1)
        };
        let a_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            a_fp,
            "A".into(),
            "A semi R".into(),
            layout(&[r], &[]),
            layout(&[a], &[second(a)]),
            layout(&[r], &[second(a)]),
            Default::default(),
        );
        let swapped = TransformationInfo::kv_to_kv(
            a_in_r.output_info_fp(),
            "A semi R".into(),
            "swapped".into(),
            false,
            layout(&[], &[sig(10, 0), sig(10, 1)]),
            layout(&[], &[sig(10, 1), sig(10, 0)]),
            KvPredicates::default(),
        );
        let b_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            b_fp,
            "B".into(),
            "B semi R".into(),
            layout(&[r], &[]),
            layout(&[b], &[second(b)]),
            layout(&[r], &[second(b)]),
            Default::default(),
        );
        let join = TransformationInfo::join_to_kv(
            swapped.output_info_fp(),
            "swapped".into(),
            b_in_r.output_info_fp(),
            "B semi R".into(),
            "join".into(),
            layout(&[sig(11, 1)], &[sig(11, 0)]),
            layout(&[sig(12, 0)], &[sig(12, 1)]),
            layout(&[sig(11, 1)], &[sig(11, 0), sig(12, 1)]),
            Default::default(),
        );
        install(&mut planner, vec![a_in_r, swapped, b_in_r, join]);
        assert_eq!(memberships(&planner), 2);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 1);
        let join = root(&planner);
        assert_eq!(join.input_info_fp().1, Some(b_fp));
        assert!(output_resolves(join));
    }

    /// A second producer of `A semijoin R` shares its output fingerprint. The
    /// filter is decided once for both copies, and neither is dropped when
    /// the fingerprint stays in use.
    #[test]
    fn every_producer_of_a_shared_filter_output_is_kept_together() {
        let (mut planner, _) = plan(R_A_B);
        let a_semijoin = planner.transformation_infos()[3].clone();
        let shared = a_semijoin.output_info_fp();
        let mut transformations = planner.transformation_infos().to_vec();
        transformations.insert(4, a_semijoin);
        install(&mut planner, transformations);
        assert_eq!(memberships(&planner), 4);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 3);
        assert_eq!(planner.producers(shared).len(), 2);
    }

    /// `T` antijoined against `X semijoin R`: widening the excluded input
    /// to `X` would drop rows of `T`, so the filter on it stays even though
    /// `Y semijoin R` tests the same constraint elsewhere. Built by hand:
    /// the planner never semijoins a negated atom, so no plan it emits has
    /// a filter on an excluded input. The program only supplies the atoms.
    #[test]
    fn filter_on_an_excluded_antijoin_input_is_kept() {
        let (mut planner, catalog) = test_setup(
            ".decl R(x: int32)\n\
             .decl X(x: int32)\n\
             .decl Y(x: int32)\n\
             .decl T(x: int32)\n\
             .decl Out(x: int32)\n\
             .input R\n\
             .input X\n\
             .input Y\n\
             .input T\n\
             .output Out\n\
             Out(x) :- R(x), X(x), Y(x), T(x).\n",
        );
        let (r_fp, r) = atom(&catalog, 0);
        let (x_fp, x) = atom(&catalog, 1);
        let (y_fp, y) = atom(&catalog, 2);
        let (t_fp, t) = atom(&catalog, 3);
        let x_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            x_fp,
            "X".into(),
            "X semi R".into(),
            layout(&[r], &[]),
            layout(&[x], &[]),
            layout(&[r], &[]),
            Default::default(),
        );
        let y_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            y_fp,
            "Y".into(),
            "Y semi R".into(),
            layout(&[r], &[]),
            layout(&[y], &[]),
            layout(&[r], &[]),
            Default::default(),
        );
        let t_not_in_x = TransformationInfo::anti_join_to_kv(
            x_in_r.output_info_fp(),
            "X semi R".into(),
            t_fp,
            "T".into(),
            "T anti (X semi R)".into(),
            layout(&[sig(10, 0)], &[]),
            layout(&[t], &[]),
            layout(&[t], &[]),
        );
        let product = TransformationInfo::join_to_kv(
            t_not_in_x.output_info_fp(),
            "T anti (X semi R)".into(),
            y_in_r.output_info_fp(),
            "Y semi R".into(),
            "product".into(),
            layout(&[], &[sig(11, 0)]),
            layout(&[], &[sig(12, 0)]),
            layout(&[], &[sig(11, 0), sig(12, 0)]),
            Default::default(),
        );
        install(&mut planner, vec![x_in_r, y_in_r, t_not_in_x, product]);
        assert_eq!(memberships(&planner), 3);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 3);
    }

    /// `X semijoin R` feeds the retained input of
    /// `(X semijoin R) antijoin E`, whose output meets `Y semijoin R` on
    /// the join key. The constraint survives the antijoin, so one copy of
    /// the `R` test suffices. Built by hand: the planner pushes an antijoin
    /// into every atom that can take it, which would add a second
    /// duplicate group; here only the `R` test is duplicated.
    #[test]
    fn filter_on_a_retained_antijoin_input_passes_through() {
        let (mut planner, catalog) = test_setup(
            ".decl R(x: int32)\n\
             .decl X(x: int32)\n\
             .decl Y(x: int32)\n\
             .decl E(x: int32)\n\
             .decl Out(x: int32)\n\
             .input R\n\
             .input X\n\
             .input Y\n\
             .input E\n\
             .output Out\n\
             Out(x) :- R(x), X(x), Y(x), E(x).\n",
        );
        let (r_fp, r) = atom(&catalog, 0);
        let (x_fp, x) = atom(&catalog, 1);
        let (y_fp, y) = atom(&catalog, 2);
        let (e_fp, e) = atom(&catalog, 3);
        let x_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            x_fp,
            "X".into(),
            "X semi R".into(),
            layout(&[r], &[]),
            layout(&[x], &[]),
            layout(&[r], &[]),
            Default::default(),
        );
        let y_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            y_fp,
            "Y".into(),
            "Y semi R".into(),
            layout(&[r], &[]),
            layout(&[y], &[]),
            layout(&[r], &[]),
            Default::default(),
        );
        let x_not_in_e = TransformationInfo::anti_join_to_kv(
            e_fp,
            "E".into(),
            x_in_r.output_info_fp(),
            "X semi R".into(),
            "(X semi R) anti E".into(),
            layout(&[e], &[]),
            layout(&[sig(10, 0)], &[]),
            layout(&[sig(10, 0)], &[]),
        );
        let join = TransformationInfo::join_to_kv(
            x_not_in_e.output_info_fp(),
            "(X semi R) anti E".into(),
            y_in_r.output_info_fp(),
            "Y semi R".into(),
            "join".into(),
            layout(&[sig(11, 0)], &[sig(11, 0)]),
            layout(&[sig(12, 0)], &[]),
            layout(&[sig(11, 0)], &[sig(11, 0)]),
            Default::default(),
        );
        let y_premap_free_root = join.output_info_fp();
        install(&mut planner, vec![x_in_r, y_in_r, x_not_in_e, join]);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 2);
        let join = root(&planner);
        assert_eq!(join.output_info_fp(), y_premap_free_root);
        assert_eq!(join.input_info_fp().1, Some(y_fp));
        assert!(output_resolves(join));
    }

    /// `A semijoin R` is the witness of
    /// `(B semijoin R) semijoin (A semijoin R)`. Join keys are equal on
    /// both inputs, so the retained side's copy proves the witness's copy
    /// just as well as the reverse; here the retained copy also feeds a
    /// full join and must stay, so the witness copy goes. Built by hand:
    /// the planner keys a semijoin on every witness column, and
    /// `A semijoin R` carries a `y` that `B semijoin R` does not.
    #[test]
    fn witness_copy_is_proven_by_the_retained_copy() {
        let (mut planner, catalog) = test_setup(
            ".decl R(x: int32)\n\
             .decl A(x: int32, y: int32)\n\
             .decl B(x: int32, z: int32)\n\
             .decl Out(x: int32, y: int32, z: int32)\n\
             .input R\n\
             .input A\n\
             .input B\n\
             .output Out\n\
             Out(x, y, z) :- R(x), A(x, y), B(x, z).\n",
        );
        let (r_fp, r) = atom(&catalog, 0);
        let (a_fp, a) = atom(&catalog, 1);
        let (b_fp, b) = atom(&catalog, 2);
        let second = |signature: AtomArgumentSignature| {
            AtomArgumentSignature::new(*signature.atom_signature(), 1)
        };
        let a_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            a_fp,
            "A".into(),
            "A semi R".into(),
            layout(&[r], &[]),
            layout(&[a], &[second(a)]),
            layout(&[r], &[second(a)]),
            Default::default(),
        );
        let b_in_r = TransformationInfo::join_to_kv(
            r_fp,
            "R".into(),
            b_fp,
            "B".into(),
            "B semi R".into(),
            layout(&[r], &[]),
            layout(&[b], &[second(b)]),
            layout(&[r], &[second(b)]),
            Default::default(),
        );
        let b_in_a = TransformationInfo::join_to_kv(
            a_in_r.output_info_fp(),
            "A semi R".into(),
            b_in_r.output_info_fp(),
            "B semi R".into(),
            "(B semi R) semi (A semi R)".into(),
            layout(&[sig(10, 0)], &[]),
            layout(&[sig(11, 0)], &[sig(11, 1)]),
            layout(&[sig(10, 0)], &[sig(11, 1)]),
            Default::default(),
        );
        let join = TransformationInfo::join_to_kv(
            b_in_a.output_info_fp(),
            "(B semi R) semi (A semi R)".into(),
            b_in_r.output_info_fp(),
            "B semi R".into(),
            "join".into(),
            layout(&[sig(12, 0)], &[sig(12, 1)]),
            layout(&[sig(13, 0)], &[sig(13, 1)]),
            layout(&[sig(12, 0)], &[sig(12, 1), sig(13, 1)]),
            Default::default(),
        );
        install(&mut planner, vec![a_in_r, b_in_r, b_in_a, join]);

        planner.prune().expect("prune");

        assert_eq!(memberships(&planner), 2);
        let b_in_a = &planner.transformation_infos()[1];
        assert_eq!(b_in_a.input_info_fp().0, a_fp);
        assert_eq!(ids(b_in_a.input_kv_layout().0.key()), vec![0]);
        assert!(output_resolves(b_in_a));
    }

    /// `minimum_cover` is driven directly here and below: through `prune`
    /// the clauses come from whole plans, and a plan shaped to yield a
    /// given clause set would bury the cover behavior under the planner's
    /// output.
    #[test]
    fn minimum_cover_picks_the_pair_hitting_every_clause() {
        let clauses = [
            vec![0, 1],
            vec![0, 2, 3],
            vec![0, 3],
            vec![1, 2],
            vec![1, 3],
            vec![2, 3],
        ];

        assert_eq!(kept_members(&minimum_cover(&clauses, 4)), vec![1, 3]);
    }

    #[test]
    fn minimum_cover_keeps_members_named_by_unit_clauses() {
        let clauses = [vec![0], vec![0, 1], vec![1]];

        assert_eq!(kept_members(&minimum_cover(&clauses, 2)), vec![0, 1]);
    }

    #[test]
    fn minimum_cover_breaks_ties_toward_earlier_members() {
        assert_eq!(kept_members(&minimum_cover(&[vec![0, 1]], 2)), vec![0]);
        assert_eq!(
            kept_members(&minimum_cover(&[vec![1, 2], vec![0, 1]], 3)),
            vec![1]
        );
    }

    #[test]
    fn minimum_cover_handles_members_past_64() {
        let clauses = [vec![0, 64], vec![1, 64]];

        assert_eq!(kept_members(&minimum_cover(&clauses, 65)), vec![64]);
    }
}
