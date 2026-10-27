//! Group-by reduction building blocks shared by [`super::lattice`] and
//! [`super::aggregate`]: the [`Reduction`]s, in the accumulators of
//! `semiring`, and the pieces of the two strategies a weight selects.
//! `presence` accumulates contributions as weights and can only add
//! answers; `signed` keeps the current answer through Differential
//! Dataflow's reduce over an arrangement, replacing it through insertions
//! and retractions.

pub(super) mod presence;
pub(super) mod semiring;
pub(super) mod signed;

use lasso::Spur;
use semiring::Float;
use semiring::MaxAccumulator;
use semiring::MeanAccumulator;
use semiring::MinAccumulator;
use semiring::Scalar;
use semiring::Semiring;
use semiring::SumAccumulator;
use semiring::TextMaxAccumulator;
use semiring::TextMinAccumulator;

// =============================================================================
// Reduction
// =============================================================================

/// What one row contributes to its group, for a given reduced column.
///
/// Both strategies use this definition: `diff::Static` contributes per row,
/// the signed ones per arranged entry. They must agree on what one distinct
/// input contributes so batch and incremental modes produce the same result.
pub trait Reduction<V, C>: 'static {
    /// The accumulator this reduction runs in.
    type Semiring: Semiring<Value = C>;

    /// Whether the reduction is defined over no input at all, answering
    /// the accumulator's `zero`, finished, for a known group with no
    /// input. The zero must then be a live weight, one `is_zero` does not
    /// report, so that Differential Dataflow keeps it; a numeric zero must
    /// not mean an absent record.
    const DEFINED_ON_EMPTY: bool = false;

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Semiring;
}

/// Declares one reduction over the column's own type.
macro_rules! reduction {
    ($name:ident, $semiring:ident, $doc:literal $(, defined_on_empty = $defined_on_empty:literal)?) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub struct $name;

        impl<V: Scalar> Reduction<V, V> for $name {
            type Semiring = $semiring<V>;

            $(const DEFINED_ON_EMPTY: bool = $defined_on_empty;)?

            #[inline]
            fn contribute(value: &V) -> Self::Semiring {
                $semiring::lift(*value)
            }
        }
    };
}

reduction!(Min, MinAccumulator, "Smallest value in the column.");
reduction!(Max, MaxAccumulator, "Largest value in the column.");
reduction!(
    Sum,
    SumAccumulator,
    "Total of the column.",
    defined_on_empty = true
);

/// Declares the extreme `$name` over the string column type `$ty`, which
/// runs in a text accumulator because a string is no [`Scalar`]. Each
/// type gets its own impl, as a blanket one over [`semiring::Text`] would
/// overlap the [`Scalar`] one above.
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
text_reduction!(Min, TextMinAccumulator, Spur);
text_reduction!(Max, TextMaxAccumulator, String);
text_reduction!(Max, TextMaxAccumulator, Spur);

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

    const DEFINED_ON_EMPTY: bool = true;

    #[inline]
    fn contribute(_value: &V) -> Self::Semiring {
        SumAccumulator::lift(C::ONE)
    }
}
