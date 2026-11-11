//! [`Append`]: presence for relations that only grow.

use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

use super::Max;
use super::Mutable;
use super::Static;
use super::Weight;

/// Presence for a relation that only grows: a datum is present from its
/// first announcement at every later time.
///
/// The arithmetic matches [`Static`]; the type differs so operators can
/// tell a relation that may still receive rows from one that cannot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Append;

impl IsZero for Append {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Append {
    #[inline]
    fn plus_equals(&mut self, _rhs: &Self) {}
}

/// An append row counts one. Exact when the arranged append collection
/// holds each datum once, as
/// [`flowlog_arrange`](crate::operators::flowlog_arrange) arranges it: a
/// datum held at two times would count two once a join accumulates its
/// history.
impl Weight for Append {
    #[inline]
    fn from_count(count: Mutable) -> Self {
        assert!(
            count > 0,
            "presence holds only a positive count, found {count}"
        );
        Self
    }

    #[inline]
    fn count(self) -> Mutable {
        1
    }
}

impl Max<Static> for Append {
    type Output = Self;
}

impl Max<Append> for Append {
    type Output = Self;
}

impl Max<Mutable> for Append {
    type Output = Mutable;
}
