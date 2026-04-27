//! Yannakakis+ planning for an α-acyclic rule.
//!
//! Two stages run in sequence after `prepare`'s alpha-elimination:
//!
//! 1. **Bottom-up semijoin reduction** — post-order traversal of the join
//!    tree, semijoining each parent by every child via the SIP machinery.
//!    Together with `prepare` this completes the paper's Algorithm 1.
//!
//! 2. **Top-down reduction** (paper's Algorithm 2) — iteratively reduces
//!    the catalog to one atom: each iteration rebuilds the join tree, finds
//!    a `(Ri, Rj)` pair where `Ri` is dangling-free and `Rj` is reducible
//!    for `Ri` (paper's definition: `Ak ∩ Ai ⊆ O` for every other neighbor
//!    `Rk` of `Ri`), then either does a full join (Algorithm 2 step) or, if
//!    no reducible pair exists, a SIP semijoin to propagate dangling-free
//!    downward (Lemma 3.14). Repeats until a single atom remains.
//!
//! Dangling-free atoms are tracked by *fingerprint*, not by index, so the
//! set survives the per-iteration tree rebuild without manual index
//! translation. Tracking is *conservative*: only fingerprints we explicitly
//! produce (the merged result of a `core()` call, the rewritten Rj of a
//! Lemma 3.14 SIP) are added. Any dangling-free atoms produced as a side
//! effect by `core()`'s internal `apply_semijoin` go untracked — at worst
//! we apply a redundant Lemma 3.14 step later, which Phase 6 dedup squashes.

use std::collections::HashSet;

use super::RulePlanner;
use crate::{
    catalog::Catalog,
    optimizer::JoinTree,
    planner::{PlanError, TransformationInfo},
};

impl RulePlanner {
    /// Run Yannakakis+ on the catalog: bottom-up semijoins, then top-down
    /// reduction until a single atom remains.
    ///
    /// Builds the initial join tree internally — `yannakakis_top_down`
    /// rebuilds it after every Algorithm 2 step anyway, so we don't ask the
    /// caller to hand one in.
    pub(crate) fn yannakakis_plus(&mut self, catalog: &mut Catalog) -> Result<(), PlanError> {
        let join_tree = JoinTree::from_catalog(catalog)?;
        self.yannakakis_bottom_up(catalog, &join_tree, join_tree.root())?;

        let head_vars = catalog.head_arguments_strs();
        // Lemma 3.9: after bottom-up, the original tree's root is dangling-free.
        let mut dangling_free: HashSet<u64> =
            HashSet::from([catalog.positive_atom_fingerprint(join_tree.root())]);

        self.yannakakis_top_down(catalog, &head_vars, &mut dangling_free)
    }

    /// Post-order DFS: every child subtree is fully reduced before its parent
    /// is touched. For each (child, parent) tree edge, reuse the SIP machinery
    /// with LHS=child and RHS=parent so the parent atom in the catalog is
    /// replaced by `parent ⋉ child` — which is exactly the Yannakakis
    /// bottom-up semijoin-reduction step.
    fn yannakakis_bottom_up(
        &mut self,
        catalog: &mut Catalog,
        tree: &JoinTree,
        node: usize,
    ) -> Result<(), PlanError> {
        for &child in tree.children(node) {
            self.yannakakis_bottom_up(catalog, tree, child)?;
            self.apply_sip_premaps(catalog, (child, node))?;
            self.apply_sip_projection_semijoin(catalog, child, node)?;
        }
        Ok(())
    }

    /// Iterate Algorithm 2 + Lemma 3.14 until the catalog has one positive
    /// atom left. Rebuilds the join tree on every iteration so that index
    /// translation is implicit (each iteration's indices are fresh against
    /// the current catalog state).
    fn yannakakis_top_down(
        &mut self,
        catalog: &mut Catalog,
        head_vars: &HashSet<String>,
        dangling_free: &mut HashSet<u64>,
    ) -> Result<(), PlanError> {
        loop {
            let n = catalog.positive_atom_number();
            if n <= 1 {
                return Ok(());
            }

            let tree = JoinTree::from_catalog(catalog)?;
            let adj = tree.undirected_adjacency();

            if let Some((ri, rj)) = find_reducible_pair(catalog, &adj, dangling_free, head_vars) {
                // Algorithm 2 step: full join + projection (via core's fixed-point).
                let start = self.transformation_infos.len();
                self.core(catalog, (ri, rj))?;
                let merged_fp = walk_merged_atom_fp(&self.transformation_infos, start);
                dangling_free.insert(merged_fp);
            } else if let Some((ri, rj)) = find_lemma_3_14_pair(catalog, &adj, dangling_free) {
                // Lemma 3.14: semijoin Rj by dangling-free Ri to make Rj dangling-free.
                self.apply_sip_premaps(catalog, (ri, rj))?;
                self.apply_sip_projection_semijoin(catalog, ri, rj)?;
                dangling_free.insert(catalog.positive_atom_fingerprint(rj));
            } else {
                // Per the paper this is unreachable for α-acyclic input: a
                // dangling-free leaf always exists (or can be created via
                // Lemma 3.14), and a leaf's parent is reducible for it.
                return Err(PlanError::internal(
                    "Algorithm 2 stuck: no reducible pair and no Lemma 3.14 target",
                ));
            }
        }
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Find a `(Ri, Rj)` pair where `Ri` is dangling-free and `Rj` is reducible
/// for `Ri`: for every *other* neighbor `Rk` of `Ri`, `attrs(Rk) ∩ attrs(Ri)
/// ⊆ head_vars`. Returns the first such pair under iteration order.
///
/// The "leaf is dangling-free, parent is reducible for it" special case
/// (paper §3.2) falls out for free: a leaf's only neighbor is its parent,
/// so the "for every other neighbor" loop is empty and the test passes
/// trivially.
fn find_reducible_pair(
    catalog: &Catalog,
    adj: &[Vec<usize>],
    dangling_free: &HashSet<u64>,
    head_vars: &HashSet<String>,
) -> Option<(usize, usize)> {
    for (ri, neighbors) in adj.iter().enumerate() {
        if !dangling_free.contains(&catalog.positive_atom_fingerprint(ri)) {
            continue;
        }
        let attrs_ri = catalog.positive_atom_argument_vars_str_set(ri);
        for &rj in neighbors {
            let reducible = neighbors.iter().filter(|&&rk| rk != rj).all(|&rk| {
                let attrs_rk = catalog.positive_atom_argument_vars_str_set(rk);
                attrs_rk
                    .intersection(attrs_ri)
                    .all(|v| head_vars.contains(v))
            });
            if reducible {
                return Some((ri, rj));
            }
        }
    }
    None
}

/// Find a `(Ri, Rj)` pair where `Ri` is dangling-free and `Rj` is a neighbor
/// that isn't yet dangling-free — the target for a Lemma 3.14 semijoin to
/// propagate the dangling-free property.
fn find_lemma_3_14_pair(
    catalog: &Catalog,
    adj: &[Vec<usize>],
    dangling_free: &HashSet<u64>,
) -> Option<(usize, usize)> {
    for (ri, neighbors) in adj.iter().enumerate() {
        if !dangling_free.contains(&catalog.positive_atom_fingerprint(ri)) {
            continue;
        }
        for &rj in neighbors {
            if !dangling_free.contains(&catalog.positive_atom_fingerprint(rj)) {
                return Some((ri, rj));
            }
        }
    }
    None
}

/// Walk the linear chain of transformations that originated from a
/// `core()` call's apply_join, ending at the merged atom's *final*
/// fingerprint after all internal cleanup (semijoin, projection, etc.).
///
/// The first transformation pushed by `core()` is the explicit join — its
/// `output_info_fp` is the initial merged-atom fingerprint. Subsequent
/// transformations that consume that fingerprint as input have rewritten
/// the merged atom in place (projection, internal semijoin where it's the
/// RHS), producing a new fingerprint. We follow that linear chain to the
/// last fingerprint reachable.
///
/// This single-fingerprint walk is sufficient *because* `prepare` has
/// already run alpha-elimination: by the time we reach Algorithm 2, the
/// merged atom carries the largest var set in the catalog, so internal
/// `apply_positive_semijoin` always uses it as the RHS (which gets rewritten
/// in place) rather than the LHS (which would be consumed and forced a
/// fan-out tracker).
fn walk_merged_atom_fp(transformation_infos: &[TransformationInfo], start: usize) -> u64 {
    let mut current = transformation_infos[start].output_info_fp();
    for tx in &transformation_infos[start + 1..] {
        let (left, right) = tx.input_info_fp();
        if left == current || right == Some(current) {
            current = tx.output_info_fp();
        }
    }
    current
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;
    use crate::optimizer::cyclic_core_join_pair;

    /// Single-atom rule: `yannakakis_plus` is a structural no-op.
    /// Nothing to join, no tree edges, neither bottom-up nor top-down does
    /// any work. Catalog stays at 1 positive atom and no SIP projection is
    /// emitted by this phase. Regression check: an off-by-one that mistakenly
    /// runs the top-down loop on n==1 would either hang or push spurious
    /// transformations.
    #[test]
    fn yannakakis_plus_single_atom_is_noop() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .decl Out(x: int32)\n\
            .output Out\n\
            Out(x) :- A(x).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        while let Some(pair) = cyclic_core_join_pair(&catalog) {
            planner.core(&mut catalog, pair).expect("core");
        }
        let before = planner.transformation_infos().len();
        planner
            .yannakakis_plus(&mut catalog)
            .expect("yannakakis_plus");
        let after = planner.transformation_infos().len();

        assert_eq!(catalog.positive_atom_number(), 1);
        assert_eq!(
            before, after,
            "yannakakis_plus must add no transformations on a single-atom rule",
        );
    }

    /// Non-free-connex 3-atom chain: head vars `x1, x2, x8` span the two
    /// leaf atoms (rela carries x1+x2, relc carries x8) and not the
    /// centroid relb. After prepare, no atom is subset-reducible, so all
    /// three survive into yannakakis_plus.
    ///
    /// The width-min root is relb (depth-1 flat tree), but neither child
    /// is reducible for relb (each child shares a non-output join key
    /// with the *other* child). The driver must therefore:
    ///   1. emit `N-1 = 2` bottom-up SIP semijoins, then
    ///   2. fall back to Lemma 3.14 to push dangling-free into a leaf, then
    ///   3. fire Algorithm 2 (leaf-reducible special case) until n == 1.
    ///
    /// We assert both the reduction-to-one-atom invariant and a SIP-count
    /// floor that's only met if bottom-up *and* Lemma 3.14 both fired.
    /// Regression: a missing Lemma 3.14 fallback would hang (ICE
    /// "Algorithm 2 stuck"); a broken bottom-up would emit < 2 SIPs.
    #[test]
    fn yannakakis_plus_reduces_non_free_connex_chain() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32, c: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl C(a: int32, b: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .decl Out(x1: int32, x2: int32, x8: int32)\n\
            .output Out\n\
            Out(x1, x2, x8) :- A(x1, x2, x4), B(x4, x7), C(x7, x8).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        while let Some(pair) = cyclic_core_join_pair(&catalog) {
            planner.core(&mut catalog, pair).expect("core");
        }
        planner
            .yannakakis_plus(&mut catalog)
            .expect("yannakakis_plus");

        assert_eq!(
            catalog.positive_atom_number(),
            1,
            "a non-free-connex acyclic rule must still reduce to a single atom",
        );
        let sip_count = planner
            .transformation_infos()
            .iter()
            .filter(|t| t.is_sip_projection())
            .count();
        assert!(
            sip_count >= 3,
            "expected ≥3 SIP projections (2 from bottom-up + ≥1 from Lemma 3.14); got {sip_count}",
        );
    }
}
