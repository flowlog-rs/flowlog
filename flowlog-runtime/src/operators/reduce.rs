//! Group-by reduction building blocks shared by [`super::lattice`] and
//! [`super::aggregate`]: the [`Reduction`]s, the accumulators they run
//! in (`accumulator`), and the pieces of the strategies a weight selects.
//! `fold` turns an aggregate's matches into weights, one contribution
//! per distinct match while it is present; `presence` accumulates
//! contributions as weights, which only add up; `signed` keeps the
//! current answer through Differential Dataflow's reduce, over an
//! arrangement of rows or of those weights, replacing it through
//! insertions and retractions.

pub(super) mod accumulator;
pub(super) mod fold;
pub(super) mod presence;
pub(super) mod signed;

use accumulator::Accumulator;
use accumulator::ChoiceAccumulator;
use accumulator::Float;
use accumulator::MaxAccumulator;
use accumulator::MeanAccumulator;
use accumulator::MinAccumulator;
use accumulator::Scalar;
use accumulator::SumAccumulator;
use accumulator::TextMaxAccumulator;
use accumulator::TextMinAccumulator;
use accumulator::ValueOrd;

use crate::intern::Symbol;

// =============================================================================
// Reduction
// =============================================================================

/// What one row contributes to its group, for a given reduced column.
///
/// Every strategy uses this definition: a presence weight contributes per
/// row as it arrives, `diff::Mutable` per arranged entry. They must agree
/// on what one distinct input contributes. Floating-point results can
/// differ between strategies; see [`super::aggregate::aggregate`].
pub trait Reduction<V, C>: 'static {
    /// The accumulator this reduction runs in.
    type Accumulator: Accumulator<Value = C>;

    /// The accumulation of a group without rows, which such a group
    /// answers, finished, where the reduction is defined on one; `None`,
    /// the default, leaves the group unanswered. A group's rows accumulate
    /// onto it, so it must leave every accumulation unchanged, as a sum's
    /// `0` does.
    #[must_use]
    fn empty() -> Option<Self::Accumulator> {
        None
    }

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Accumulator;
}

/// Declares one reduction over the column's own type, whose group without
/// rows answers `$empty` when one is given.
macro_rules! reduction {
    ($name:ident, $accumulator:ident, $doc:literal $(, empty = $empty:expr)?) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub struct $name;

        impl<V: Scalar> Reduction<V, V> for $name {
            type Accumulator = $accumulator<V>;

            $(
                #[inline]
                fn empty() -> Option<Self::Accumulator> {
                    Some($accumulator::lift($empty))
                }
            )?

            #[inline]
            fn contribute(value: &V) -> Self::Accumulator {
                $accumulator::lift(*value)
            }
        }
    };
}

reduction!(Min, MinAccumulator, "Smallest value in the column.");
reduction!(Max, MaxAccumulator, "Largest value in the column.");
reduction!(Sum, SumAccumulator, "Total of the column.", empty = V::ZERO);

/// Declares the extreme `$name` over the string column type `$ty`, which
/// runs in a text accumulator because a string is no [`Scalar`]. Each
/// type gets its own impl, as a blanket one over [`accumulator::ValueOrd`],
/// which the numbers implement too, would overlap the [`Scalar`] one
/// above.
macro_rules! text_reduction {
    ($name:ident, $accumulator:ident, $ty:ty) => {
        impl Reduction<$ty, $ty> for $name {
            type Accumulator = $accumulator<$ty>;

            #[inline]
            fn contribute(value: &$ty) -> Self::Accumulator {
                $accumulator::lift(value.clone())
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
    type Accumulator = MeanAccumulator<V, F>;

    #[inline]
    fn contribute(value: &V) -> Self::Accumulator {
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
    type Accumulator = SumAccumulator<C>;

    #[inline]
    fn empty() -> Option<Self::Accumulator> {
        Some(SumAccumulator::lift(C::ZERO))
    }

    #[inline]
    fn contribute(_value: &V) -> Self::Accumulator {
        SumAccumulator::lift(C::ONE)
    }
}

/// A group's choice: its least value under [`ValueOrd`]. Unlike a
/// [`Min`], nothing supersedes it, so a static choice is final once made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) struct Choice;

impl<V: ValueOrd> Reduction<V, V> for Choice {
    type Accumulator = ChoiceAccumulator<V>;

    #[inline]
    fn contribute(value: &V) -> Self::Accumulator {
        ChoiceAccumulator::lift(value.clone())
    }
}
