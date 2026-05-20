//! Cost model for join-tree alternatives.
//!
//! The DP search (`plan`/`dp_search`) scores every candidate `JoinTree`
//! and keeps the cheapest. A subtree's evaluation carries its result size
//! (`output`) and the `peak` the DP minimizes — the largest intermediate
//! inside it.
//!
//! # Where the numbers come from
//!
//! Leaf sizes come from `Feedback`'s cache: EDB row counts seeded from the
//! fact files, and IDB sizes recorded as each stratum is planned — `plan`
//! returns a rule's `output` size and the optimizer caches it, so a later
//! stratum sees its lower-stratum IDBs as ordinary cached leaves. An
//! untraced leaf — a not-yet-planned same-stratum recursive IDB — falls
//! back to the largest known size.
//!
//! A cached `0` is treated as *unmeasured*, not as an empty relation: `0`
//! in a truncated profile means the rule stalled before producing output.
//!
//! # How a plan is scored
//!
//! `peak` is the largest join intermediate the plan builds. The DP
//! minimizes `(peak, depth)`: cheapest peak first, then the bushier tree
//! (smaller depth) as a tiebreaker — a balanced shape exposes more
//! parallel work than a chain of equal cost.
//!
//! # The join estimate
//!
//! A join's size is [`join_estimate`] from [`super::comm`] —
//! `|L|·|R| / domain(key)`. The join key's distinct-value count comes from
//! the per-column domains traced into [`Feedback`] at optimizer setup.
//!
//! # Composite leaves
//!
//! Once a prior round commits a join, `join_modify` collapses the
//! operands into one catalog atom named for the join expression.
//! `estimate_name` peels the transparent `⋉`/`▷`/`π`/`σ` wrappers (only
//! `⋈` grows a result) to size a composite leaf from its parts.
//!
//! Lookup keys flow through [`canonical`] so operand and key order don't
//! fragment the cache; join names are rebuilt with the same
//! [`naming::join_name`](crate::common::join_name) helper the planner emits.

use std::collections::{HashMap, HashSet};

use crate::catalog::Catalog;
use crate::common::join_name;
use crate::optimizer::canonical::{canonical, join_skeleton, split_outer_binop, split_prefix_op};
use crate::optimizer::cardinality::comm::{ColumnTrace, intersect, join_estimate};
use crate::optimizer::feedback::Feedback;
use crate::optimizer::tree::JoinTree;

/// Cost stamped on a join the profiler flagged as a hog (its
/// projection-transparent skeleton is in `Feedback::bad_subtrees`). Large
/// enough to dominate any real alternative so the DP never rebuilds a
/// known-bad grouping.
const BAD_SUBTREE_PENALTY: u64 = 1 << 60;

/// Refuse DP above this atom count — the O(3^n) subset search becomes
/// unwieldy. Datalog rules rarely exceed this; above it we fall back to a
/// left-deep order. Unused while [`CostEstimator::plan`] is soft-blocked.
#[allow(dead_code)]
const MAX_ATOMS_FOR_DP: usize = 12;

/// Result of evaluating a subtree.
#[derive(Debug, Clone)]
struct Evaluation {
    /// Estimated size of this subtree's *output*.
    output: u64,
    /// Largest intermediate inside the subtree — the value the DP
    /// minimizes.
    peak: u64,
    /// Canonical hierarchical name, e.g. `(arc ⋈[y] reach)`.
    name: String,
}

/// Walks a [`JoinTree`] bottom-up against the live catalog, consulting
/// [`Feedback`] at each level.
pub(crate) struct CostEstimator<'a> {
    feedback: &'a Feedback,
}

impl<'a> CostEstimator<'a> {
    pub(crate) fn new(feedback: &'a Feedback) -> Self {
        Self { feedback }
    }

    fn evaluate(&self, tree: &JoinTree, catalog: &Catalog) -> Evaluation {
        match tree {
            JoinTree::Leaf(i) => self.evaluate_leaf(*i, catalog),
            JoinTree::Join { left, right } => self.evaluate_join(left, right, catalog),
        }
    }

    fn evaluate_leaf(&self, i: usize, catalog: &Catalog) -> Evaluation {
        let name = catalog
            .positive_atom_name(i)
            .expect("JoinTree leaf must reference a live positive atom")
            .to_string();
        // A leaf is a base relation or a composite built by a prior round.
        // `estimate_name` peels composites down to their measured/estimated
        // parts; an untraced relation falls back to the largest known size.
        let output = self.estimate_name(&name);
        Evaluation {
            output,
            peak: 0,
            name,
        }
    }

    fn evaluate_join(&self, left: &JoinTree, right: &JoinTree, catalog: &Catalog) -> Evaluation {
        let l = self.evaluate(left, catalog);
        let r = self.evaluate(right, catalog);

        let l_vars = subtree_vars(left, catalog);
        let r_vars = subtree_vars(right, catalog);
        let keys: Vec<String> = {
            let mut k: Vec<String> = l_vars.intersection(&r_vars).cloned().collect();
            k.sort();
            k
        };
        let raw_name = join_name(&l.name, &r.name, &keys);

        let output = self.join_output(l.output, r.output, &keys, catalog, &raw_name);
        // A profiler-flagged hog — its projection-transparent skeleton is in
        // `bad_subtrees`. The penalty is a *cost* signal, not a real size:
        // it inflates `peak` (which the DP minimizes) so the search avoids
        // this grouping, while `output` stays the true size estimate.
        let penalty = if self
            .feedback
            .bad_subtrees()
            .contains(&join_skeleton(&raw_name))
        {
            BAD_SUBTREE_PENALTY
        } else {
            0
        };
        Evaluation {
            // Peak: the worst of the children's peaks, this join's own
            // output size, and any bad-subtree penalty.
            peak: l.peak.max(r.peak).max(output).max(penalty),
            output,
            name: raw_name,
        }
    }

    /// Estimated output size of joining two evaluated sides — the rule's
    /// true size, with no cost penalties.
    ///
    /// A measured join wins outright; a join key with disjoint value ranges
    /// is provably empty; otherwise the shared [`join_estimate`] primitive
    /// sizes it from the join key's distinct-value domain.
    fn join_output(
        &self,
        l: u64,
        r: u64,
        keys: &[String],
        catalog: &Catalog,
        raw_name: &str,
    ) -> u64 {
        // A measurement for the whole join wins. A cached `0` is a stalled
        // rule, not an empty relation — fall through to the estimate.
        if let Some(v) = self.feedback.lookup(&canonical(raw_name))
            && v > 0
        {
            return v;
        }
        let var_traces = relation_var_traces(catalog, self.feedback);
        // A join key whose two sides' value ranges do not overlap can
        // produce no rows. `relation_var_traces` intersects each variable's
        // range across the atoms it appears in, so an empty (`min > max`)
        // result flags exactly a disjoint join key.
        if keys
            .iter()
            .any(|k| var_traces.get(k).is_some_and(ColumnTrace::is_disjoint))
        {
            return 1;
        }
        // `|L|·|R| / domain(key)`: the join key's distinct-value count is
        // the largest domain among the shared variables — the loosest
        // single key keeps the estimate an upper bound.
        let key_domain = keys
            .iter()
            .filter_map(|k| var_traces.get(k).map(|t| t.domain))
            .filter(|&d| d > 0)
            .max();
        join_estimate(l, r, key_domain, None)
    }

    /// Estimated size of a collection by name — a bare relation or a
    /// composite built by a prior round.
    ///
    /// Only `⋈` can grow a result. `⋉`/`▷` filter their left input and
    /// `π`/`σ` rewrite a single input — all three are peeled transparently
    /// to the surviving operand. An untraced bare relation falls back to
    /// the largest known cache entry, so a leaf always has a number.
    fn estimate_name(&self, name: &str) -> u64 {
        let canon = canonical(name);
        // A measurement (or recorded IDB size) for this exact (sub)expression.
        // A self-join occurrence carries a `#<index>` tag the per-relation
        // seed does not, so retry with the tag stripped. A cached `0` is a
        // stalled rule — treat it as unmeasured.
        let measured = self
            .feedback
            .lookup(&canon)
            .or_else(|| strip_occurrence_tag(&canon).and_then(|bare| self.feedback.lookup(bare)));
        if let Some(v) = measured
            && v > 0
        {
            return v;
        }
        // π[..](X) / σ[..](X) — transparent: recurse into the inner expr.
        if let Some((_, _, inner)) = split_prefix_op(name) {
            return self.estimate_name(inner);
        }
        match split_outer_binop(name) {
            // ⋈ — joined from both sides via the shared `join_estimate`.
            // The key domain is not recoverable from the name alone, so
            // it folds as a cartesian upper bound here.
            Some(("⋈", left, _keys, right)) => {
                let l = self.estimate_name(left);
                let r = self.estimate_name(right);
                join_estimate(l, r, None, None)
            }
            // ⋉ / ▷ — semijoin/antijoin keep only their filtered left input.
            Some((_, left, ..)) => self.estimate_name(left),
            // An untraced base or IDB relation — fall back to the largest
            // known cache entry so the leaf still carries a number.
            None => self.largest_known(),
        }
    }

    /// Largest cardinality currently in the cache — the upper-bound
    /// fallback for a relation with no estimate of its own. `1` when the
    /// cache is empty, so a leaf is never sized `0`.
    fn largest_known(&self) -> u64 {
        self.feedback.max_cardinality().max(1)
    }

    /// Plan the live catalog: return `(output size, join tree)`.
    ///
    /// The `output` is the rule's estimated result cardinality — the
    /// optimizer records it as the head IDB's size so a later stratum sees
    /// it as a cached leaf. The `JoinTree` is the order to commit; `None`
    /// when the catalog has fewer than two atoms to join (its `output` is
    /// still the rule's size).
    ///
    /// The DP enumerates every candidate order, lets a profiler-flagged hog
    /// inflate that candidate's `peak` (see `BAD_SUBTREE_PENALTY`), and the
    /// lowest-`peak` survivor wins.
    pub(crate) fn plan(&self, catalog: &Catalog) -> (u64, Option<JoinTree>) {
        let n = catalog.core_atom_number();
        if n == 0 {
            return (1, None); // a body of only comparisons — at most one row.
        }
        if n == 1 {
            return (self.evaluate(&JoinTree::Leaf(0), catalog).output, None);
        }

        // ADHOC — DP soft-blocked from participating in plan selection.
        //
        // The cost estimator's recursive-IDB fallback (`largest_known`) is
        // currently strong enough to flip the DP into picking plans that
        // wedge the runtime (e.g. batik's `assign` rule materializing a
        // 980 M-row EDB×EDB intermediate instead of filtering through the
        // recursive `callgraphedge`). Until that fallback is sharpened in
        // a follow-up commit, every rule is planned left-deep in the order
        // the atoms appear in the source — predictable, debuggable, and
        // gives the profiler / feedback / hog infrastructure a stable
        // signal to test against.
        //
        // [`dp_search`] is intentionally left intact (with its
        // cartesian-forbid logic) so the follow-up can re-enable it
        // without re-deriving anything.
        let tree = JoinTree::left_deep(0..n);
        let output = tree
            .as_ref()
            .map_or(1, |t| self.evaluate(t, catalog).output);
        (output, tree)
    }

    /// For each non-empty subset of the rule's positive atoms, keep the
    /// single best plan (smallest `peak`). Composition is by all
    /// `(left, right)` partitions of the subset; subsets are processed in
    /// ascending popcount so each split's children are already filled.
    ///
    /// When `allow_cartesian` is `false`, bipartitions where the two
    /// sides share *no* variable (a `⋈[]` join) are skipped. Some subsets
    /// may then have no plan at all — that's expected and propagates up:
    /// the full atom set has a plan iff the rule's atom variable graph is
    /// connected. Caller decides whether to retry with cartesians allowed.
    ///
    /// Currently uncalled — [`Self::plan`] is soft-blocked to left-deep
    /// until the recursive-IDB cost estimate is sharpened. Preserved so
    /// the follow-up commit can re-enable it without re-deriving anything.
    #[allow(dead_code)]
    fn dp_search(
        &self,
        catalog: &Catalog,
        n: usize,
        allow_cartesian: bool,
    ) -> Option<(Evaluation, JoinTree)> {
        let full_mask: u32 = (1u32 << n) - 1;

        // best[mask] -> (eval, tree). Indexed by subset bitmask.
        let mut best: Vec<Option<(Evaluation, JoinTree)>> = vec![None; (full_mask + 1) as usize];

        // Base case: singletons.
        for i in 0..n {
            let mask = 1u32 << i;
            let leaf = JoinTree::Leaf(i);
            let eval = self.evaluate(&leaf, catalog);
            best[mask as usize] = Some((eval, leaf));
        }

        let mut masks: Vec<u32> = (1u32..=full_mask).collect();
        masks.sort_by_key(|m| m.count_ones());
        for mask in masks {
            if mask.count_ones() < 2 {
                continue;
            }
            let mut chosen: Option<(Evaluation, JoinTree)> = None;
            let mut left = (mask - 1) & mask;
            while left != 0 {
                let right = mask ^ left;
                // Enumerate ordered (left, right) — JoinTree is structurally
                // directed and the cached cost may differ across orderings
                // when nested subtree names differ. Both directions get
                // their fair shot.
                let l_slot = best[left as usize].as_ref();
                let r_slot = best[right as usize].as_ref();
                if let (Some((_, l_tree)), Some((_, r_tree))) = (l_slot, r_slot) {
                    if !allow_cartesian
                        && subtree_vars(l_tree, catalog)
                            .is_disjoint(&subtree_vars(r_tree, catalog))
                    {
                        // `⋈[]` split — skip in the cartesian-forbid pass.
                        left = (left.wrapping_sub(1)) & mask;
                        continue;
                    }
                    let candidate = JoinTree::Join {
                        left: Box::new(l_tree.clone()),
                        right: Box::new(r_tree.clone()),
                    };
                    let eval = self.evaluate(&candidate, catalog);
                    // Ordering: smaller peak first, then bushier tree
                    // (smaller depth) as the tiebreaker — a balanced shape
                    // exposes more parallel work than a chain of equal cost.
                    let take = match &chosen {
                        None => true,
                        Some((cur, cur_tree)) => {
                            (eval.peak, candidate.depth()) < (cur.peak, cur_tree.depth())
                        }
                    };
                    if take {
                        chosen = Some((eval, candidate));
                    }
                }
                left = (left.wrapping_sub(1)) & mask;
            }
            best[mask as usize] = chosen;
        }

        best[full_mask as usize].take()
    }
}

/// Strip a self-join occurrence tag (`relation#<digits>` → `relation`),
/// returning `None` when there is no trailing `#<digits>`. Used so a
/// tagged leaf can fall back to the per-relation EDB cardinality seed,
/// which is keyed by the bare relation name.
///
/// Unambiguous: the grammar's `identifier` rule forbids `#` in any
/// relation or variable name (and `#` even starts a comment), so the only
/// `#` in a transformation name is the one `tag_self_join_atoms` appends.
fn strip_occurrence_tag(name: &str) -> Option<&str> {
    let (bare, idx) = name.rsplit_once('#')?;
    let tagged = !idx.is_empty() && idx.bytes().all(|b| b.is_ascii_digit());
    tagged.then_some(bare)
}

/// Traced [`ColumnTrace`] of every variable bound by a catalog's positive
/// atoms, read off the per-column stats in `Feedback`.
///
/// The catalog-atom analogue of `comm::body_var_traces`: each positive
/// atom maps its argument columns to variable strings via the argument
/// signatures; column `c` of relation `rel` carries `feedback.column(rel,
/// c)`. A self-join tag (`rel#3`) is stripped to match the untagged keys
/// the pre-pass traces. A variable appearing in several atoms is
/// *intersected* — `min` domain and intersected range — so a join key
/// whose occurrences cannot coincide ends up with an empty range.
fn relation_var_traces(catalog: &Catalog, feedback: &Feedback) -> HashMap<String, ColumnTrace> {
    let mut out: HashMap<String, ColumnTrace> = HashMap::new();
    for i in 0..catalog.positive_atom_number() {
        let Ok(name) = catalog.positive_atom_name(i) else {
            continue;
        };
        let relation = strip_occurrence_tag(name).unwrap_or(name);
        for (col, sig) in catalog
            .positive_atom_argument_signature(i)
            .iter()
            .enumerate()
        {
            let Some(ct) = feedback.column(relation, col).map(ColumnTrace::from) else {
                continue;
            };
            let var = catalog.signature_to_argument_str(sig).clone();
            out.entry(var)
                .and_modify(|cur| {
                    cur.domain = cur.domain.min(ct.domain);
                    cur.range = intersect(cur.range, ct.range);
                })
                .or_insert(ct);
        }
    }
    out
}

/// Variables bound by all leaf atoms in a subtree.
fn subtree_vars(tree: &JoinTree, catalog: &Catalog) -> HashSet<String> {
    let mut out = HashSet::new();
    walk_vars(tree, catalog, &mut out);
    out
}

fn walk_vars(tree: &JoinTree, catalog: &Catalog, out: &mut HashSet<String>) {
    match tree {
        JoinTree::Leaf(i) => out.extend(catalog.positive_atom_var_set(*i).iter().cloned()),
        JoinTree::Join { left, right } => {
            walk_vars(left, catalog, out);
            walk_vars(right, catalog, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- strip_occurrence_tag ------------------------------------------

    #[test]
    fn strip_occurrence_tag_recovers_bare_relation() {
        assert_eq!(strip_occurrence_tag("arc#3"), Some("arc"));
        // No trailing `#<digits>` — not a tag.
        assert_eq!(strip_occurrence_tag("arc"), None);
        assert_eq!(strip_occurrence_tag("(a ⋈[x] b)"), None);
        assert_eq!(strip_occurrence_tag("arc#x"), None);
    }

    // ---- estimate_name: a leaf always carries a number -----------------

    #[test]
    fn estimate_name_measured_wins() {
        let mut fb = Feedback::default();
        fb.insert("(arc ⋈[x] reach)", 9_999);
        let est = CostEstimator::new(&fb);
        assert_eq!(est.estimate_name("(arc ⋈[x] reach)"), 9_999);
    }

    #[test]
    fn estimate_name_untraced_falls_back_to_largest_known() {
        let mut fb = Feedback::default();
        fb.insert("arc", 100);
        let est = CostEstimator::new(&fb);
        // An untraced bare relation gets the largest known cache entry, so
        // a leaf is never sized 0.
        assert_eq!(est.estimate_name("reach"), 100);
    }

    #[test]
    fn estimate_name_peels_transparent_operators() {
        // π/σ are transparent; ⋉/▷ keep only the (filtered) left input —
        // so an unmeasured right side does not change the estimate.
        let mut fb = Feedback::default();
        fb.insert("arc", 100);
        let est = CostEstimator::new(&fb);
        assert_eq!(est.estimate_name("π[x](arc)"), 100);
        assert_eq!(est.estimate_name("(arc ⋉[x] reach)"), 100);
    }

    #[test]
    fn estimate_name_zero_measurement_is_unmeasured() {
        // A cached 0 means the relation never ran (stalled rule) — treat
        // it as unmeasured and fall back, not as an empty relation.
        let mut fb = Feedback::default();
        fb.insert("stalled", 0);
        fb.insert("other", 50);
        let est = CostEstimator::new(&fb);
        assert_eq!(est.estimate_name("stalled"), 50);
    }

    // ---- join_estimate is the one shared join primitive ----------------

    #[test]
    fn estimate_name_join_uses_shared_primitive() {
        // A composite ⋈ leaf folds its parts through `join_estimate`; with
        // no key domain it is the cartesian upper bound.
        let mut fb = Feedback::default();
        fb.insert("a", 100);
        fb.insert("b", 200);
        let est = CostEstimator::new(&fb);
        assert_eq!(
            est.estimate_name("(a ⋈[x] b)"),
            join_estimate(100, 200, None, None)
        );
    }
}
