//! Join order for a body without a join tree: the written order, skipping
//! pairs that would be a cross product.

// TODO: plan cyclic bodies with per-atom lookup paths (worst-case optimal
// where two extenders both lack a key) once the lookup operator and the
// path planner land.

use super::AtomKeys;

/// Returns the pair joining atom `first` with the first later atom it
/// shares a key with, or `None` when it shares a key with none. Atoms
/// before `first` must share a key with no atom.
pub(super) fn partner(keys: &AtomKeys, first: usize) -> Option<(usize, usize)> {
    let second = first + 1;
    let linked = (second..keys.atom_count()).find(|&j| keys.shared(first, j) > 0)?;
    Some(if linked == second {
        (first, second)
    } else {
        (linked, first)
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::super::next_join;
    use super::super::tests::atoms;

    // Cases: atoms, next join.
    #[rstest]
    #[case::triangle_in_order(&["y w", "w r z", "x r y"], Some((0, 1)))]
    #[case::cycle_corners_first(&["x y", "z w", "y z", "w x"], Some((2, 0)))]
    #[case::written_order_over_stronger_links(&["x y", "y z", "z x", "x y w", "w q"], Some((0, 1)))]
    fn a_cyclic_body_keeps_its_written_order_without_cross_products(
        #[case] lists: &[&str],
        #[case] expected: Option<(usize, usize)>,
    ) {
        assert!(!atoms(lists).is_acyclic());
        assert_eq!(next_join(&atoms(lists)), expected);
    }
}
