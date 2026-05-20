//! Binary join tree over a catalog's live positive atoms.
//!
//! Built fresh each planning round by the optimizer's DP search and then
//! consumed once: the leftmost two-leaf join is committed back to the rule
//! planner, the catalog mutates, and the next round re-plans on the
//! remaining atoms. There is no per-rule state to keep across rounds —
//! atom indices shift after `join_modify`, and re-running DP is cheap at
//! the atom counts we see in practice.

use std::fmt;

/// A binary join tree. Leaves are *current* catalog positive-atom indices
/// (i.e. valid in the live catalog at planning time, not stable across
/// rounds).
#[derive(Debug, Clone)]
pub(crate) enum JoinTree {
    Leaf(usize),
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
    },
}

impl JoinTree {
    /// Build a left-deep tree from `atoms` in the given order — `(((a0 ⋈ a1)
    /// ⋈ a2) ⋈ …)`. Returns `None` if `atoms` is empty; a single atom
    /// becomes a `Leaf`.
    pub(crate) fn left_deep(atoms: impl IntoIterator<Item = usize>) -> Option<Self> {
        let mut it = atoms.into_iter();
        let first = it.next()?;
        let mut acc = JoinTree::Leaf(first);
        for next in it {
            acc = JoinTree::Join {
                left: Box::new(acc),
                right: Box::new(JoinTree::Leaf(next)),
            };
        }
        Some(acc)
    }

    /// Number of join layers from this node down to the deepest leaf.
    /// `Leaf` returns 0; a balanced bushy tree minimizes this value, which
    /// the DP search uses as a tiebreaker when two plans have equal peak
    /// cost — bushier trees expose more independent work to the parallel
    /// scheduler. Unused while the DP search is soft-blocked.
    #[allow(dead_code)]
    pub(crate) fn depth(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 0,
            JoinTree::Join { left, right } => 1 + left.depth().max(right.depth()),
        }
    }

    /// The first join pair to commit this round: the leftmost
    /// `Join { Leaf(i), Leaf(j) }` reachable by descending `left` first.
    ///
    /// Returns `None` if the tree is a single `Leaf` (nothing to join).
    pub(crate) fn first_join_pair(&self) -> Option<(usize, usize)> {
        match self {
            JoinTree::Leaf(_) => None,
            JoinTree::Join { left, right } => {
                // Descend into left until we find a Join whose both children
                // are leaves, or until left itself is a leaf.
                if let Some(pair) = left.first_join_pair() {
                    return Some(pair);
                }
                if let Some(pair) = right.first_join_pair() {
                    return Some(pair);
                }
                // Both sides are leaves — this is the deepest leftmost pair.
                match (left.as_ref(), right.as_ref()) {
                    (JoinTree::Leaf(i), JoinTree::Leaf(j)) => Some((*i, *j)),
                    _ => unreachable!("non-leaf subtrees handled above"),
                }
            }
        }
    }
}

impl fmt::Display for JoinTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinTree::Leaf(i) => write!(f, "#{}", i),
            JoinTree::Join { left, right } => write!(f, "({} ⋈ {})", left, right),
        }
    }
}
