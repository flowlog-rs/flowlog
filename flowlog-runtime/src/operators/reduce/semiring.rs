//! Semirings that carry an aggregate in the difference position.
//!
//! Each type accumulates one FlowLog aggregation under `plus_equals`, so a
//! collection keyed by the group-by columns computes the aggregate through
//! Differential Dataflow's own consolidation rather than a reduce
//! arrangement. [`Semiring`] is what the reduce operators dispatch on; the
//! concrete types are private to the crate because generated code names an
//! operator, never a semiring.
//!
//! The scalar table lives in [`Scalar`], one impl per numeric column type
//! FlowLog admits.

use std::ops::Add;

use differential_dataflow::ExchangeData;
use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

// =========================================================================
// Scalar
// =========================================================================

/// Numeric column types an aggregation can run over.
///
/// [`Scalar::Wide`] is the accumulator averages sum into. It is wider than
/// the column itself for integers, so that a group larger than the column's
/// range cannot wrap the running total before the division; floats sum in
/// their own width, since widening them would not buy exactness.
pub trait Scalar: ExchangeData + Copy + Ord + Add<Output = Self> {
    /// Accumulator for the average's running sum.
    type Wide: ExchangeData + Copy + Ord + Add<Output = Self::Wide> + From<Self>;

    /// Identity of [`Largest`], and the smallest value of the column type.
    const MIN: Self;
    /// Identity of [`Smallest`], and the largest value of the column type.
    const MAX: Self;
    /// Identity of [`Total`], and of the average's running sum.
    const ZERO: Self;
    /// One row's contribution to a `count`.
    const ONE: Self;

    /// Divides an average accumulator by a non-zero count, narrowing back to
    /// the column type.
    fn mean(accum: Self::Wide, count: u64) -> Self;
}

/// One [`Scalar`] impl per integer column type, summing averages in `$wide`.
macro_rules! int_scalar {
    ($ty:ty, $wide:ty) => {
        impl Scalar for $ty {
            type Wide = $wide;

            const MIN: Self = <$ty>::MIN;
            const MAX: Self = <$ty>::MAX;
            const ZERO: Self = 0;
            const ONE: Self = 1;

            #[inline]
            fn mean(accum: Self::Wide, count: u64) -> Self {
                (accum / count as $wide) as $ty
            }
        }
    };
}

int_scalar!(i8, i64);
int_scalar!(i16, i64);
int_scalar!(i32, i64);
int_scalar!(i64, i64);
int_scalar!(u8, u64);
int_scalar!(u16, u64);
int_scalar!(u32, u64);
int_scalar!(u64, u64);

/// One [`Scalar`] impl per float column type. `MIN` / `MAX` are the
/// infinities rather than the finite bounds, so that `Smallest` and `Largest` start
/// from a true identity.
macro_rules! float_scalar {
    ($inner:ty) => {
        impl Scalar for OrderedFloat<$inner> {
            type Wide = OrderedFloat<$inner>;

            const MIN: Self = OrderedFloat(<$inner>::NEG_INFINITY);
            const MAX: Self = OrderedFloat(<$inner>::INFINITY);
            const ZERO: Self = OrderedFloat(0.0);
            const ONE: Self = OrderedFloat(1.0);

            #[inline]
            fn mean(accum: Self::Wide, count: u64) -> Self {
                accum / count as $inner
            }
        }
    };
}

float_scalar!(f32);
float_scalar!(f64);

// =========================================================================
// Semiring
// =========================================================================

/// An aggregate accumulated in the difference position.
///
/// Implementors are `Monoid`s whose `plus_equals` is the aggregation step,
/// which is what lets Differential Dataflow's consolidation do the work.
pub trait Semiring: Monoid + Copy + 'static {
    /// The aggregated column type.
    type Value: Scalar;

    /// Wraps one input row's contribution.
    fn lift(value: Self::Value) -> Self;

    /// Unwraps the accumulated aggregate.
    ///
    /// Never called on an empty accumulation, though each caller rules that
    /// out its own way: the threshold predicate drops what [`IsZero`] reports,
    /// `consolidate` drops it before the recursive path completes an
    /// aggregate, and `reduce_abelian` does not invoke its logic for an empty
    /// group. Only [`Mean`] has an empty state at all.
    fn finish(self) -> Self::Value;

    /// Returns `true` if this accumulation supersedes `current` and must be
    /// re-emitted: a tightened bound for the extremes, any change at all for
    /// the accumulating aggregates.
    fn supersedes(&self, current: &Self) -> bool;
}

/// Declares a semiring whose accumulation is one binary operation over a
/// single column value.
///
/// `is_zero` is `false` throughout: these have no absorbing element, and
/// reporting one would let Differential Dataflow drop a live aggregate.
macro_rules! value_semiring {
    (
        $name:ident,
        $doc:literal,
        $zero:expr, |
        $a:ident,
        $b:ident |
        $accumulate:expr, |
        $new:ident,
        $current:ident |
        $supersedes:expr
    ) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(bound = "")]
        pub struct $name<V: Scalar> {
            value: V,
        }

        impl<V: Scalar> IsZero for $name<V> {
            #[inline]
            fn is_zero(&self) -> bool {
                false
            }
        }

        impl<V: Scalar> Semigroup for $name<V> {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                let ($a, $b) = (self.value, rhs.value);
                self.value = $accumulate;
            }
        }

        impl<V: Scalar> Monoid for $name<V> {
            #[inline]
            fn zero() -> Self {
                Self { value: $zero }
            }
        }

        impl<V: Scalar> Semiring for $name<V> {
            type Value = V;

            #[inline]
            fn lift(value: V) -> Self {
                Self { value }
            }

            #[inline]
            fn finish(self) -> V {
                self.value
            }

            #[inline]
            fn supersedes(&self, current: &Self) -> bool {
                let ($new, $current) = (self.value, current.value);
                $supersedes
            }
        }
    };
}

value_semiring!(
    Smallest,
    "Running minimum, identity `V::MAX`.",
    V::MAX,
    |a, b| a.min(b),
    |new, current| new < current
);

value_semiring!(
    Largest,
    "Running maximum, identity `V::MIN`.",
    V::MIN,
    |a, b| a.max(b),
    |new, current| new > current
);

value_semiring!(
    Total,
    "Running total, identity `V::ZERO`. Also backs `count`, whose rows each \
     contribute one.",
    V::ZERO,
    |a, b| a + b,
    |new, current| new != current
);

// =========================================================================
// Avg
// =========================================================================

/// Running average, held as the pair it decomposes into.
///
/// Unlike the other semirings this one has a genuine zero -- an empty
/// accumulation -- because `(sum, count)` starts at `(0, 0)` and a count of
/// zero has no average to report.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Mean<V: Scalar> {
    sum: V::Wide,
    count: u64,
}

impl<V: Scalar> IsZero for Mean<V> {
    #[inline]
    fn is_zero(&self) -> bool {
        self.count == 0
    }
}

impl<V: Scalar> Semigroup for Mean<V> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.sum = self.sum + rhs.sum;
        self.count += rhs.count;
    }
}

impl<V: Scalar> Monoid for Mean<V> {
    #[inline]
    fn zero() -> Self {
        Self {
            sum: V::Wide::from(V::ZERO),
            count: 0,
        }
    }
}

impl<V: Scalar> Semiring for Mean<V> {
    type Value = V;

    #[inline]
    fn lift(value: V) -> Self {
        Self {
            sum: V::Wide::from(value),
            count: 1,
        }
    }

    #[inline]
    fn finish(self) -> V {
        // Unreachable per `Semiring::finish`, but an identity beats dividing
        // by zero if a future caller forgets to filter.
        if self.count == 0 {
            return V::ZERO;
        }
        V::mean(self.sum, self.count)
    }

    #[inline]
    fn supersedes(&self, current: &Self) -> bool {
        self != current
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Accumulate a group the way `plus_equals` does during consolidation.
    fn accumulate<S: Semiring>(values: impl IntoIterator<Item = S::Value>) -> S {
        let mut acc = S::zero();
        for value in values {
            acc.plus_equals(&S::lift(value));
        }
        acc
    }

    #[rstest]
    #[case(vec![3i64, 1, 2], 1)]
    #[case(vec![-5i64, 5], -5)]
    #[case(vec![7i64], 7)]
    fn min_accumulates_to_the_smallest(#[case] values: Vec<i64>, #[case] expected: i64) {
        assert_eq!(accumulate::<Smallest<i64>>(values).finish(), expected);
    }

    #[rstest]
    #[case(vec![3i64, 1, 2], 3)]
    #[case(vec![-5i64, 5], 5)]
    fn max_accumulates_to_the_largest(#[case] values: Vec<i64>, #[case] expected: i64) {
        assert_eq!(accumulate::<Largest<i64>>(values).finish(), expected);
    }

    #[test]
    fn sum_accumulates_at_the_column_width() {
        assert_eq!(accumulate::<Total<i32>>(vec![1, 2, 3]).finish(), 6);
    }

    /// The identity must not participate in the result: a `Min` seeded at
    /// `V::MAX` and folded with real values reports the values' minimum,
    /// and an empty fold reports the identity itself.
    #[test]
    fn identities_are_neutral() {
        assert_eq!(accumulate::<Smallest<i32>>(vec![]).finish(), i32::MAX);
        assert_eq!(accumulate::<Largest<i32>>(vec![]).finish(), i32::MIN);
        assert_eq!(accumulate::<Total<i32>>(vec![]).finish(), 0);
        assert_eq!(
            accumulate::<Smallest<OrderedFloat<f64>>>(vec![]).finish(),
            OrderedFloat(f64::INFINITY)
        );
    }

    /// An `i8` group longer than the column's range: the average must be
    /// exact, which it can only be if the running sum is wider than `i8`.
    /// Accumulating at the column width would wrap well before the 200th
    /// row and report a value outside the input's range.
    #[test]
    fn avg_of_a_narrow_column_does_not_wrap() {
        let values = vec![100i8; 200];
        assert_eq!(accumulate::<Mean<i8>>(values).finish(), 100);
    }

    #[rstest]
    #[case(vec![1i64, 2, 3], 2)]
    #[case(vec![1i64, 2], 1)] // truncating division, as integer avg always has
    fn avg_divides_sum_by_count(#[case] values: Vec<i64>, #[case] expected: i64) {
        assert_eq!(accumulate::<Mean<i64>>(values).finish(), expected);
    }

    #[test]
    fn avg_of_floats_divides_at_float_width() {
        let values = vec![OrderedFloat(1.0f64), OrderedFloat(2.0)];
        assert_eq!(accumulate::<Mean<OrderedFloat<f64>>>(values).finish(), 1.5);
    }

    /// Only `Avg` reports a zero, and only while empty. The others must
    /// never report one, or a live aggregate whose value happens to be `0`
    /// would be dropped from the collection.
    #[test]
    fn only_an_empty_average_is_zero() {
        assert!(Mean::<i64>::zero().is_zero());
        assert!(!accumulate::<Mean<i64>>(vec![0i64]).is_zero());
        assert!(!accumulate::<Total<i64>>(vec![0i64]).is_zero());
        assert!(!Total::<i64>::zero().is_zero());
        assert!(!Smallest::<i64>::zero().is_zero());
    }

    /// `supersedes` is what the threshold predicate re-emits on: bounds only
    /// tighten, while the accumulating aggregates re-emit on any change.
    #[test]
    fn supersedes_tightens_bounds_and_tracks_changes() {
        let (lo, hi) = (Smallest::lift(1i64), Smallest::lift(5i64));
        assert!(lo.supersedes(&hi));
        assert!(!hi.supersedes(&lo));
        assert!(!lo.supersedes(&lo));

        let (lo, hi) = (Largest::lift(1i64), Largest::lift(5i64));
        assert!(hi.supersedes(&lo));
        assert!(!lo.supersedes(&hi));

        assert!(Total::lift(1i64).supersedes(&Total::lift(2i64)));
        assert!(!Total::lift(2i64).supersedes(&Total::lift(2i64)));
    }
}
