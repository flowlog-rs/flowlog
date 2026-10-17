//! Semirings that carry a reduction in the difference position.
//!
//! Each type accumulates one reduction under `plus_equals`: the presence
//! strategy carries it as a weight, so a collection keyed by the group
//! columns reduces through Differential Dataflow's own consolidation, and
//! the signed strategy folds it inside its reduce. [`Semiring`] is what
//! both dispatch on; the concrete types stay internal because generated
//! code names a reduction, never a semiring.
//!
//! The scalar table lives in [`Scalar`], one impl per numeric column type
//! FlowLog admits, and [`Float`] is the part of it a mean answers in.

use std::marker::PhantomData;
use std::ops::Add;

use differential_dataflow::ExchangeData;
use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

// =============================================================================
// Scalar
// =============================================================================

/// Numeric column types a reduction can run over.
///
/// [`Scalar::Wide`] is the type a mean sums the column in: 64 bits for an
/// integer, so that the narrower columns cannot wrap the running total
/// and the sum stays exact whatever order the rows arrive in, and `f64`
/// for a float.
pub trait Scalar: ExchangeData + Copy + Ord + Add<Output = Self> {
    /// The type a mean's running sum is held in.
    type Wide: Scalar;

    /// Identity of [`MaxAccumulator`], and the smallest value of the column
    /// type.
    const MIN: Self;
    /// Identity of [`MinAccumulator`], and the largest value of the column
    /// type.
    const MAX: Self;
    /// Identity of [`SumAccumulator`], and of a mean's running sum.
    const ZERO: Self;
    /// One row's contribution to a `count`.
    const ONE: Self;

    /// The value at the mean's summing width.
    fn widen(self) -> Self::Wide;

    /// The value as a float, the width a mean divides at.
    fn to_f64(self) -> OrderedFloat<f64>;
}

/// One [`Scalar`] impl per integer column type, summing means in `$wide`.
macro_rules! int_scalar {
    ($ty:ty, $wide:ty) => {
        impl Scalar for $ty {
            type Wide = $wide;

            const MIN: Self = <$ty>::MIN;
            const MAX: Self = <$ty>::MAX;
            const ZERO: Self = 0;
            const ONE: Self = 1;

            #[inline]
            fn widen(self) -> $wide {
                <$wide>::from(self)
            }

            #[inline]
            fn to_f64(self) -> OrderedFloat<f64> {
                OrderedFloat(self as f64)
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
/// infinities rather than the finite bounds, so that `MinAccumulator` and
/// `MaxAccumulator` start from a true identity.
macro_rules! float_scalar {
    ($inner:ty) => {
        impl Scalar for OrderedFloat<$inner> {
            type Wide = OrderedFloat<f64>;

            const MIN: Self = OrderedFloat(<$inner>::NEG_INFINITY);
            const MAX: Self = OrderedFloat(<$inner>::INFINITY);
            const ZERO: Self = OrderedFloat(0.0);
            const ONE: Self = OrderedFloat(1.0);

            #[inline]
            fn widen(self) -> OrderedFloat<f64> {
                self.to_f64()
            }

            #[inline]
            fn to_f64(self) -> OrderedFloat<f64> {
                OrderedFloat(f64::from(self.0))
            }
        }
    };
}

float_scalar!(f32);
float_scalar!(f64);

/// A float column type, which a mean answers in: the mean divides at
/// `f64` and narrows to the column's width.
pub trait Float: Scalar {
    /// The value at this width.
    fn from_f64(value: OrderedFloat<f64>) -> Self;
}

impl Float for OrderedFloat<f32> {
    #[inline]
    fn from_f64(value: OrderedFloat<f64>) -> Self {
        OrderedFloat(value.0 as f32)
    }
}

impl Float for OrderedFloat<f64> {
    #[inline]
    fn from_f64(value: OrderedFloat<f64>) -> Self {
        value
    }
}

// =============================================================================
// Semiring
// =============================================================================

/// A reduction accumulated in the difference position.
///
/// Implementors are `Monoid`s whose `plus_equals` is the reduction step,
/// which is what lets Differential Dataflow's consolidation do the work.
/// One row's contribution is built by the accumulator's own `lift`, as
/// [`super::Reduction::contribute`] does; the trait only folds and
/// answers.
pub trait Semiring: Monoid + Copy + 'static {
    /// The type the reduction answers in: the column's, or the float a
    /// mean is declared to answer.
    type Value: Scalar;

    /// Unwraps the accumulated answer.
    ///
    /// Never called on an empty accumulation, though each caller rules that
    /// out its own way: the threshold predicate drops what [`IsZero`] reports,
    /// `consolidate` drops it before a static loop's lattice leaves, and
    /// `reduce_abelian` does not invoke its logic for an empty group. Only
    /// [`MeanAccumulator`] has an empty state at all.
    fn finish(self) -> Self::Value;

    /// Returns `true` if this accumulation supersedes `current` and must be
    /// re-emitted: a tightened bound for the extremes, any change at all for
    /// the accumulating reductions.
    fn supersedes(&self, current: &Self) -> bool;
}

/// Declares a semiring whose accumulation is one binary operation over a
/// single column value.
///
/// A result can equal the numeric identity, such as a sum of zero.
/// `is_zero` must stay `false` so Differential Dataflow does not mistake
/// that valid result for an absent record and discard it.
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

        impl<V: Scalar> $name<V> {
            /// One row's contribution.
            #[inline]
            pub(super) fn lift(value: V) -> Self {
                Self { value }
            }
        }

        impl<V: Scalar> Semiring for $name<V> {
            type Value = V;

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
    MinAccumulator,
    "Running minimum, identity `V::MAX`.",
    V::MAX,
    |a, b| a.min(b),
    |new, current| new < current
);

value_semiring!(
    MaxAccumulator,
    "Running maximum, identity `V::MIN`.",
    V::MIN,
    |a, b| a.max(b),
    |new, current| new > current
);

value_semiring!(
    SumAccumulator,
    "Running total, identity `V::ZERO`. Also backs `count`, whose rows each \
     contribute one.",
    V::ZERO,
    |a, b| a + b,
    |new, current| new != current
);

// =============================================================================
// MeanAccumulator
// =============================================================================

/// Running mean over a column `V`, held as the pair it decomposes into:
/// a sum at the column's [`Scalar::Wide`] and a count, divided once at
/// the end in `f64` and answered in the float `F` the mean's column is
/// declared as.
///
/// Unlike the other semirings this one has a genuine zero -- an empty
/// accumulation -- because `(sum, count)` starts at `(0, 0)` and a count of
/// zero has no mean to report.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct MeanAccumulator<V: Scalar, F: Float> {
    sum: V::Wide,
    count: u64,
    answer: PhantomData<F>,
}

impl<V: Scalar, F: Float> IsZero for MeanAccumulator<V, F> {
    #[inline]
    fn is_zero(&self) -> bool {
        self.count == 0
    }
}

impl<V: Scalar, F: Float> Semigroup for MeanAccumulator<V, F> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.sum = self.sum + rhs.sum;
        self.count += rhs.count;
    }
}

impl<V: Scalar, F: Float> Monoid for MeanAccumulator<V, F> {
    #[inline]
    fn zero() -> Self {
        Self {
            sum: V::Wide::ZERO,
            count: 0,
            answer: PhantomData,
        }
    }
}

impl<V: Scalar, F: Float> MeanAccumulator<V, F> {
    /// One row's contribution, its value widened, counted once.
    #[inline]
    pub(super) fn lift(value: V) -> Self {
        Self {
            sum: value.widen(),
            count: 1,
            answer: PhantomData,
        }
    }
}

impl<V: Scalar, F: Float> Semiring for MeanAccumulator<V, F> {
    type Value = F;

    #[inline]
    fn finish(self) -> F {
        // Unreachable per `Semiring::finish`, but an identity beats dividing
        // by zero if a future caller forgets to filter.
        if self.count == 0 {
            return F::ZERO;
        }
        F::from_f64(self.sum.to_f64() / self.count as f64)
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
    fn accumulate<S: Semiring>(parts: impl IntoIterator<Item = S>) -> S {
        let mut acc = S::zero();
        for part in parts {
            acc.plus_equals(&part);
        }
        acc
    }

    type Mean64<V> = MeanAccumulator<V, OrderedFloat<f64>>;
    type Mean32<V> = MeanAccumulator<V, OrderedFloat<f32>>;

    #[rstest]
    #[case(vec![3i64, 1, 2], 1)]
    #[case(vec![-5i64, 5], -5)]
    #[case(vec![7i64], 7)]
    fn min_accumulates_to_the_smallest(#[case] values: Vec<i64>, #[case] expected: i64) {
        let parts = values.into_iter().map(MinAccumulator::lift);
        assert_eq!(accumulate(parts).finish(), expected);
    }

    #[rstest]
    #[case(vec![3i64, 1, 2], 3)]
    #[case(vec![-5i64, 5], 5)]
    fn max_accumulates_to_the_largest(#[case] values: Vec<i64>, #[case] expected: i64) {
        let parts = values.into_iter().map(MaxAccumulator::lift);
        assert_eq!(accumulate(parts).finish(), expected);
    }

    #[test]
    fn sum_accumulates_at_the_column_width() {
        let parts = [1i32, 2, 3].map(SumAccumulator::lift);
        assert_eq!(accumulate(parts).finish(), 6);
    }

    /// An empty fold reports the identity itself, which the folds above
    /// show does not take part in a result.
    #[test]
    fn identities_are_neutral() {
        assert_eq!(accumulate::<MinAccumulator<i32>>([]).finish(), i32::MAX);
        assert_eq!(accumulate::<MaxAccumulator<i32>>([]).finish(), i32::MIN);
        assert_eq!(accumulate::<SumAccumulator<i32>>([]).finish(), 0);
        assert_eq!(
            accumulate::<MinAccumulator<OrderedFloat<f64>>>([]).finish(),
            OrderedFloat(f64::INFINITY)
        );
    }

    /// An `i8` group longer than the column's range reports its exact
    /// mean: the sum runs at the wide type, not at the column's width.
    #[test]
    fn mean_sums_beyond_the_columns_range() {
        let parts = std::iter::repeat_n(Mean64::<i8>::lift(100), 200);
        assert_eq!(accumulate(parts).finish(), 100.0);
    }

    /// A mean divides as a float, with no truncation, and answers at the
    /// declared width.
    #[rstest]
    #[case(vec![1, 2, 3], 2.0)]
    #[case(vec![1, 2], 1.5)]
    fn mean_divides_sum_by_count(#[case] values: Vec<i32>, #[case] expected: f64) {
        let wide = values.iter().copied().map(Mean64::<i32>::lift);
        assert_eq!(accumulate(wide).finish(), expected);
        let narrow = values.iter().copied().map(Mean32::<i32>::lift);
        assert_eq!(accumulate(narrow).finish(), expected as f32);
    }

    /// A float column sums at `f64`, whatever its own width.
    #[test]
    fn a_float_column_sums_wide() {
        let parts = [0.1f32, 0.2, 0.3]
            .map(OrderedFloat)
            .map(Mean32::<OrderedFloat<f32>>::lift);
        let narrow = accumulate(parts).finish();
        let exact = [0.1f32, 0.2, 0.3]
            .iter()
            .map(|value| f64::from(*value))
            .sum::<f64>()
            / 3.0;
        assert_eq!(narrow, OrderedFloat(exact as f32));
    }

    /// Only `MeanAccumulator` reports a zero, and only while empty. The
    /// others must never report one, or a live answer whose value happens
    /// to be `0` would be dropped from the collection.
    #[test]
    fn only_an_empty_mean_is_zero() {
        assert!(Mean64::<i64>::zero().is_zero());
        assert!(!Mean64::<i64>::lift(0).is_zero());
        assert!(!accumulate([SumAccumulator::lift(0i64)]).is_zero());
        assert!(!SumAccumulator::<i64>::zero().is_zero());
        assert!(!MinAccumulator::<i64>::zero().is_zero());
    }

    /// `supersedes` is what the threshold predicate re-emits on: bounds only
    /// tighten, while the accumulating reductions re-emit on any change.
    #[test]
    fn supersedes_tightens_bounds_and_tracks_changes() {
        let (lo, hi) = (MinAccumulator::lift(1i64), MinAccumulator::lift(5i64));
        assert!(lo.supersedes(&hi));
        assert!(!hi.supersedes(&lo));
        assert!(!lo.supersedes(&lo));

        let (lo, hi) = (MaxAccumulator::lift(1i64), MaxAccumulator::lift(5i64));
        assert!(hi.supersedes(&lo));
        assert!(!lo.supersedes(&hi));

        assert!(SumAccumulator::lift(1i64).supersedes(&SumAccumulator::lift(2i64)));
        assert!(!SumAccumulator::lift(2i64).supersedes(&SumAccumulator::lift(2i64)));
    }
}
