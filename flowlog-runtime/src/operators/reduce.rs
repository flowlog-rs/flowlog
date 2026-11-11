//! Group-by reduction building blocks shared by [`super::lattice`] and
//! [`super::aggregate`]: the [`Reduction`]s, in the accumulators of
//! `semiring`, and the pieces of the strategies a weight selects.
//! `presence` accumulates contributions as weights, which only add up;
//! `signed` keeps the current answer through Differential Dataflow's
//! reduce, over an arrangement of rows or of those weights, replacing it
//! through insertions and retractions.

pub(super) mod presence;
pub(super) mod semiring;
pub(super) mod signed;

use semiring::Float;
use semiring::MaxAccumulator;
use semiring::MeanAccumulator;
use semiring::MinAccumulator;
use semiring::Scalar;
use semiring::Semiring;
use semiring::SumAccumulator;
use semiring::TextMaxAccumulator;
use semiring::TextMinAccumulator;

use crate::intern::Symbol;

// =============================================================================
// Reduction
// =============================================================================

/// What one row contributes to its group, for a given reduced column.
///
/// Every strategy uses this definition: a presence weight contributes per
/// row as it arrives, `diff::Mutable` per arranged entry. They must agree
/// on what one distinct input contributes. Floating-point results can
/// differ between strategies; see [`super::aggregate::flowlog_aggregate`].
pub trait Reduction<V, C>: 'static {
    /// The accumulator this reduction runs in.
    type Semiring: Semiring<Value = C>;

    /// The accumulation of a group without rows, which such a group
    /// answers, finished, where the reduction is defined on one; `None`,
    /// the default, leaves the group unanswered. A group's rows accumulate
    /// onto it, so it must leave every accumulation unchanged, as a sum's
    /// `0` does.
    #[must_use]
    fn empty() -> Option<Self::Semiring> {
        None
    }

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Semiring;
}

/// Declares one reduction over the column's own type, whose group without
/// rows answers `$empty` when one is given.
macro_rules! reduction {
    ($name:ident, $semiring:ident, $doc:literal $(, empty = $empty:expr)?) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub struct $name;

        impl<V: Scalar> Reduction<V, V> for $name {
            type Semiring = $semiring<V>;

            $(
                #[inline]
                fn empty() -> Option<Self::Semiring> {
                    Some($semiring::lift($empty))
                }
            )?

            #[inline]
            fn contribute(value: &V) -> Self::Semiring {
                $semiring::lift(*value)
            }
        }
    };
}

reduction!(Min, MinAccumulator, "Smallest value in the column.");
reduction!(Max, MaxAccumulator, "Largest value in the column.");
reduction!(Sum, SumAccumulator, "Total of the column.", empty = V::ZERO);

/// Declares the extreme `$name` over the string column type `$ty`, which
/// runs in a text accumulator because a string is no [`Scalar`]. Each
/// type gets its own impl, as a blanket one over [`semiring::ValueOrd`],
/// which the numbers implement too, would overlap the [`Scalar`] one
/// above.
macro_rules! text_reduction {
    ($name:ident, $semiring:ident, $ty:ty) => {
        impl Reduction<$ty, $ty> for $name {
            type Semiring = $semiring<$ty>;

            #[inline]
            fn contribute(value: &$ty) -> Self::Semiring {
                $semiring::lift(value.clone())
            }
        }
    };
}

text_reduction!(Min, TextMinAccumulator, String);
text_reduction!(Min, TextMinAccumulator, Symbol);
text_reduction!(Max, TextMaxAccumulator, String);
text_reduction!(Max, TextMaxAccumulator, Symbol);

/// Mean of the column, summed at the column's wide type and answered in
/// the float the mean's column is declared as.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Mean;

impl<V: Scalar, F: Float> Reduction<V, F> for Mean {
    type Semiring = MeanAccumulator<V, F>;

    #[inline]
    fn contribute(value: &V) -> Self::Semiring {
        MeanAccumulator::lift(*value)
    }
}

/// Number of rows in the group.
///
/// Unlike the others this one reads no column: `V` is whatever its caller
/// hands it, and each row contributes one in the answer's type `C`. Rows
/// stay distinct by their whole value, which the strategies keep, not by
/// anything `count` sees.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Count;

impl<V, C: Scalar> Reduction<V, C> for Count {
    type Semiring = SumAccumulator<C>;

    #[inline]
    fn empty() -> Option<Self::Semiring> {
        Some(SumAccumulator::lift(C::ZERO))
    }

    #[inline]
    fn contribute(_value: &V) -> Self::Semiring {
        SumAccumulator::lift(C::ONE)
    }
}
