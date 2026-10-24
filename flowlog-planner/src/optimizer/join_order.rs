//! The next join of a rule body, by a strategy per body shape.
//!
//! - `acyclic`: a body with a join tree joins only neighbors in the tree.
//! - `cyclic`: a body without one keeps its written order, skipping pairs
//!   that would be a cross product.
//!
//! Both start from the running result, atom 0, and leave a cross product
//! that cannot be avoided until the rest of the body is joined.

mod acyclic;
mod cyclic;

use std::collections::BTreeSet;
use std::collections::HashMap;

use flowlog_parser::Variable;

// =============================================================================
// JoinKey
// =============================================================================

/// What two positive atoms can be joined on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum JoinKey {
    /// A variable of the atom.
    Variable(Variable),
    /// A spanning equality, by comparison index, that equijoin fusion keys
    /// at least one pair on. Every atom that resolves either side holds
    /// it, as every such atom would hold the column fusion adds.
    Equality(usize),
}

// =============================================================================
// AtomKeys
// =============================================================================

/// The join keys of each positive atom of a rule body, and the equalities
/// fusion keys each pair on, by catalog index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AtomKeys {
    keys: Vec<BTreeSet<JoinKey>>,
    /// Keyed by `(a, b)` with `a < b`.
    fusable: HashMap<(usize, usize), BTreeSet<usize>>,
}

impl AtomKeys {
    pub(crate) fn new(
        keys: Vec<BTreeSet<JoinKey>>,
        fusable: HashMap<(usize, usize), BTreeSet<usize>>,
    ) -> Self {
        Self { keys, fusable }
    }

    fn atom_count(&self) -> usize {
        self.keys.len()
    }

    /// Returns how many keys atoms `a` and `b` can join on; 0 means joining
    /// them is a cross product. A shared equality counts only when fusion
    /// keys this pair on it: elsewhere it stays a filter.
    fn shared(&self, a: usize, b: usize) -> usize {
        let pair = (a.min(b), a.max(b));
        self.keys[a]
            .intersection(&self.keys[b])
            .filter(|key| match key {
                JoinKey::Variable(_) => true,
                JoinKey::Equality(comparison) => self
                    .fusable
                    .get(&pair)
                    .is_some_and(|fusable| fusable.contains(comparison)),
            })
            .count()
    }

    /// Returns `true` if the body has a join tree.
    ///
    /// Reduces the key sets until neither step applies: a key held by one
    /// atom only is dropped, and an atom whose keys another atom holds is
    /// dropped. The body is acyclic when at most one atom is left.
    fn is_acyclic(&self) -> bool {
        let mut atoms: Vec<BTreeSet<&JoinKey>> =
            self.keys.iter().map(|keys| keys.iter().collect()).collect();
        loop {
            let mut holders: HashMap<&JoinKey, usize> = HashMap::new();
            for key in atoms.iter().flatten() {
                *holders.entry(key).or_default() += 1;
            }
            let mut changed = false;
            for atom in &mut atoms {
                let before = atom.len();
                atom.retain(|key| holders[key] > 1);
                changed |= atom.len() != before;
            }
            // One atom per round, so two atoms with equal sets do not each
            // drop the other.
            let contained = (0..atoms.len())
                .find(|&i| (0..atoms.len()).any(|j| j != i && atoms[i].is_subset(&atoms[j])));
            if let Some(i) = contained {
                atoms.remove(i);
                changed = true;
            }
            if !changed {
                return atoms.len() <= 1;
            }
        }
    }
}

// =============================================================================
// Next join
// =============================================================================

/// Returns the next pair of positive atoms to join, by index, or `None`
/// when fewer than two atoms are left.
///
/// Atom 0 holds the running result, and the pair keeps it there: `(0, 1)`
/// joins the first two atoms, `(j, 0)` joins atom `j` into atom 0. The
/// body's strategy picks atom 0's partner; when atom 0 shares a key with
/// no atom, its cross product is unavoidable and waits: the strategy picks
/// among the later atoms instead, leaving atom 0 in place. `(0, 1)`, a
/// cross product, comes only when no two atoms share a key.
pub(crate) fn next_join(keys: &AtomKeys) -> Option<(usize, usize)> {
    if keys.atom_count() < 2 {
        return None;
    }
    let partner: fn(&AtomKeys, usize) -> Option<(usize, usize)> = if keys.is_acyclic() {
        acyclic::partner
    } else {
        cyclic::partner
    };
    // An atom that shares no key lies on no path between the later atoms,
    // so the strategy sees the same body without it.
    let linked = (0..keys.atom_count()).find_map(|first| partner(keys, first));
    Some(linked.unwrap_or((0, 1)))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Atom keys from one space-separated variable list per atom.
    pub(super) fn atoms(lists: &[&str]) -> AtomKeys {
        AtomKeys::new(
            lists
                .iter()
                .map(|list| {
                    list.split_whitespace()
                        .map(|var| JoinKey::Variable(var.into()))
                        .collect()
                })
                .collect(),
            HashMap::new(),
        )
    }

    // Cases: atoms, acyclic.
    #[rstest]
    #[case::chain(&["x y", "y z", "z w"], true)]
    #[case::hub_and_leaves(&["def v used", "block v next index", "used block v"], true)]
    #[case::disconnected(&["a b", "c d"], true)]
    #[case::equal_atoms(&["x y", "x y"], true)]
    #[case::covered_triangle(&["x y", "y z", "z x", "x y z"], true)]
    #[case::triangle(&["x y", "y z", "z x"], false)]
    #[case::four_cycle(&["a b", "b c", "c d", "d a"], false)]
    fn a_body_is_acyclic_when_it_reduces_to_one_atom(
        #[case] lists: &[&str],
        #[case] expected: bool,
    ) {
        assert_eq!(atoms(lists).is_acyclic(), expected);
    }

    /// Atom keys as [`atoms`] builds them, plus equality 0 held by atoms
    /// `holders` and fused on `pair`.
    fn with_equality(lists: &[&str], holders: &[usize], pair: (usize, usize)) -> AtomKeys {
        let mut keys = atoms(lists).keys;
        for &holder in holders {
            keys[holder].insert(JoinKey::Equality(0));
        }
        AtomKeys::new(keys, HashMap::from([(pair, BTreeSet::from([0]))]))
    }

    #[test]
    fn an_equality_links_atoms_like_a_shared_variable() {
        let keys = with_equality(&["x", "y", "z"], &[0, 2], (0, 2));
        assert_eq!(next_join(&keys), Some((2, 0)));
    }

    /// An equality closes a cycle the variables alone leave open.
    #[test]
    fn an_equality_can_make_a_body_cyclic() {
        assert!(atoms(&["x a", "a b", "b y"]).is_acyclic());
        assert!(!with_equality(&["x a", "a b", "b y"], &[0, 2], (0, 2)).is_acyclic());
    }

    /// An equality weights only a pair fusion keys on it: not two atoms
    /// on the same side, and not a pair that shares a variable.
    #[test]
    fn an_equality_weights_only_the_pair_it_fuses() {
        // Equality 0 reads `x` against `y`; atoms 0 and 2 share `a`.
        let keys = with_equality(&["x a", "x b", "y a"], &[0, 1, 2], (1, 2));
        assert_eq!(keys.shared(0, 1), 1);
        assert_eq!(keys.shared(0, 2), 1);
        assert_eq!(keys.shared(1, 2), 1);
    }

    // Cases: atoms, next join.
    #[rstest]
    #[case::acyclic_cross_product_waits(&["a", "x y", "y z"], Some((1, 2)))]
    #[case::cyclic_cross_product_waits(&["a", "x y", "y z", "z x"], Some((1, 2)))]
    #[case::only_cross_products(&["a", "b", "c"], Some((0, 1)))]
    #[case::one_atom(&["x"], None)]
    fn a_cross_product_waits_for_the_rest_of_the_body(
        #[case] lists: &[&str],
        #[case] expected: Option<(usize, usize)>,
    ) {
        assert_eq!(next_join(&atoms(lists)), expected);
    }
}
