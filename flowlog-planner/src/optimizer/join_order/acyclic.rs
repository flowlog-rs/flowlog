//! Join order for a body with a join tree: only neighbors in the tree.
//!
//! Joining two atoms that are not neighbors in any join tree matches them
//! on only part of what links them, the rest being checked by an atom in
//! between that joins later; on a column with few distinct values that
//! join is close to a cross product.
//!
//! The tree is never built. The join trees of an acyclic body are exactly
//! the maximum-weight spanning trees of the graph that links two atoms by
//! the number of keys they share, so a pair is a tree edge when no path of
//! strictly heavier links already connects it, and the heaviest link of
//! any one atom is always a tree edge. Joining two neighbors leaves the
//! body acyclic, so the same test holds at the next step.

use super::AtomKeys;

/// Returns the pair joining atom `first` with a neighbor in a join tree, or
/// `None` when `first` shares a key with no later atom. Atoms before
/// `first` must share a key with no atom.
///
/// The neighbor is `first + 1` when that pair is a tree edge, so a written
/// order that follows a join tree is kept; otherwise it is the later atom
/// sharing the most keys with `first`, the lowest index among equals.
pub(super) fn partner(keys: &AtomKeys, first: usize) -> Option<(usize, usize)> {
    let second = first + 1;
    if second >= keys.atom_count() {
        return None;
    }
    if keys.shared(first, second) > 0 && !has_stronger_path(keys, first, second) {
        return Some((first, second));
    }
    let strongest = (second..keys.atom_count())
        .filter(|&j| keys.shared(first, j) > 0)
        .max_by_key(|&j| (keys.shared(first, j), std::cmp::Reverse(j)))?;
    Some(if strongest == second {
        (first, second)
    } else {
        (strongest, first)
    })
}

/// Returns `true` if a path of atoms, each sharing strictly more keys with
/// the next than `left` shares with `right`, connects `left` and `right`.
///
/// On an acyclic body this is exactly the pair not being neighbors in any
/// join tree.
fn has_stronger_path(keys: &AtomKeys, left: usize, right: usize) -> bool {
    let threshold = keys.shared(left, right);
    let mut group: Vec<usize> = (0..keys.atom_count()).collect();
    for a in 0..keys.atom_count() {
        for b in a + 1..keys.atom_count() {
            if keys.shared(a, b) > threshold {
                let (ga, gb) = (representative(&mut group, a), representative(&mut group, b));
                group[ga] = gb;
            }
        }
    }
    representative(&mut group, left) == representative(&mut group, right)
}

/// Returns the representative of `node`'s group, compressing the path to it.
fn representative(group: &mut [usize], mut node: usize) -> usize {
    while group[node] != node {
        group[node] = group[group[node]];
        node = group[node];
    }
    node
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::next_join;
    use super::super::tests::atoms;
    use super::*;

    // The def-use shape from cvc5: `A` holds every join variable, and the
    // two leaves `B` and `C` share only the low-cardinality `v`.
    const LEAF_B: &str = "def v used";
    const LEAF_C: &str = "block v next index";
    const HUB_A: &str = "used block v";

    // Cases: atoms, next join.
    #[rstest]
    #[case::chain_in_order(&["x y", "y z", "z w"], Some((0, 1)))]
    #[case::hub_written_first(&[HUB_A, LEAF_B, LEAF_C], Some((0, 1)))]
    #[case::leaves_written_first(&[LEAF_B, LEAF_C, HUB_A], Some((2, 0)))]
    #[case::star_on_one_variable(&["x a", "x b", "x c"], Some((0, 1)))]
    #[case::chain_ends_first(&["x y", "z w", "y z"], Some((2, 0)))]
    #[case::heaviest_link_wins(&["x y", "y z w", "x y z w"], Some((2, 0)))]
    #[case::equal_links_take_lowest(&["x y", "a", "y b", "x c"], Some((2, 0)))]
    #[case::cross_product_waits_on_a_tree_edge(&["q", LEAF_B, LEAF_C, HUB_A], Some((3, 1)))]
    fn an_acyclic_body_joins_only_join_tree_neighbors(
        #[case] lists: &[&str],
        #[case] expected: Option<(usize, usize)>,
    ) {
        assert!(atoms(lists).is_acyclic());
        assert_eq!(next_join(&atoms(lists)), expected);
    }

    // Cases: atoms, left, right, stronger path.
    #[rstest]
    #[case::leaves_through_the_hub(&[LEAF_B, LEAF_C, HUB_A], 0, 1, true)]
    #[case::leaf_and_hub(&[LEAF_B, LEAF_C, HUB_A], 0, 2, false)]
    #[case::star_leaves(&["x a", "x b", "x c"], 1, 2, false)]
    #[case::chain_link(&["x y", "z w", "y z"], 0, 2, false)]
    fn a_pair_is_bypassed_only_by_a_strictly_stronger_path(
        #[case] lists: &[&str],
        #[case] left: usize,
        #[case] right: usize,
        #[case] expected: bool,
    ) {
        assert_eq!(has_stronger_path(&atoms(lists), left, right), expected);
        assert_eq!(has_stronger_path(&atoms(lists), right, left), expected);
    }
}
