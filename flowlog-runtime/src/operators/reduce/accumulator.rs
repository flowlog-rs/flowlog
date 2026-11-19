//! Accumulators that carry a reduction in the difference position.
//!
//! Each type accumulates one reduction under `plus_equals`. The presence
//! weights, `diff::Static` and `diff::Append`, carry it as a weight, so a
//! collection keyed by the group columns reduces through Differential
//! Dataflow's own consolidation. `diff::Mutable` carries it the same way,
//! wrapped in a [`Counted`], when its aggregate folds; otherwise its
//! reduce accumulates it. [`Accumulator`] is what they all dispatch on; the
//! concrete types stay internal because generated code names a reduction,
//! never an accumulator.
//!
//! The scalar table lives in [`Scalar`], one impl per numeric column type
//! FlowLog admits. [`Integer`] is the part of it whose total takes a value
//! back out exactly, and [`Float`] the part a mean answers in.
//! [`ValueOrd`] is the order every column type compares in, which the
//! text extremes and a [`ChoiceAccumulator`] run over.
//!
//! A law that `plus_equals` obeys beyond merging in any order is a trait
//! of its own, which an operator relying on the law names as a bound:
//!
//! | trait | law | buys |
//! |---|---|---|
//! | [`Idempotent`] | `a + a = a` | a repeated row merges as one |
//! | [`Selective`] | `a + b` is `a` or `b` | an answer is one row's value |
//! | [`Abelian`] | `a + -a = 0` | a deleted match subtracts |
//!
//! [`Abelian`] is Differential Dataflow's, on its [`Monoid`]: a count, sum
//! or mean has a zero, the accumulation of no rows, and over integers each
//! weight has a negative. Only the zero weight is both idempotent and
//! abelian, since `a = a + a - a = a - a = 0`, so an operator names the
//! law its input needs: a merge that may see a row twice needs
//! idempotence, a fold that takes a deleted match back out needs
//! negation.

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::ExchangeData;
use differential_dataflow::difference::Abelian;
use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

use crate::intern::Symbol;
use crate::intern::resolve;

// =============================================================================
// Scalar
// =============================================================================

/// Numeric column types a reduction can run over.
///
/// [`Scalar::Wide`] is the type a mean sums the column in: 64 bits for an
/// integer, so that the narrower columns cannot wrap the running total
/// and the sum stays exact whatever order the rows arrive in, and `f64`
/// for a float.
pub trait Scalar: ExchangeData + Copy + Ord {
    /// The type a mean's running sum is held in.
    type Wide: Scalar;

    /// Identity of [`SumAccumulator`], and of a mean's running sum.
    const ZERO: Self;
    /// One row's contribution to a `count`.
    const ONE: Self;

    /// Adds the two values. Integer addition wraps on overflow, so a
    /// total that overflows and later comes back into range is exact.
    #[must_use]
    fn plus(self, other: Self) -> Self;

    /// The value at the mean's summing width.
    fn widen(self) -> Self::Wide;

    /// The value as a float, the width a mean divides at.
    fn to_f64(self) -> OrderedFloat<f64>;
}

/// An integer column type, whose total takes a value back out exactly:
/// its addition wraps on overflow, so adding a value's negation removes
/// the value whatever order the additions come in. A float is not one:
/// its addition rounds, and `1e20 + 1 - 1e20` is `0`, not `1`.
pub trait Integer: Scalar {
    /// Returns the value that adds with this one to zero.
    #[must_use]
    fn negate(self) -> Self;
}

/// One [`Scalar`] and [`Integer`] impl per integer column type, summing
/// means in `$wide`.
macro_rules! int_scalar {
    ($ty:ty, $wide:ty) => {
        impl Scalar for $ty {
            type Wide = $wide;

            const ZERO: Self = 0;
            const ONE: Self = 1;

            #[inline]
            fn plus(self, other: Self) -> Self {
                self.wrapping_add(other)
            }

            #[inline]
            fn widen(self) -> $wide {
                <$wide>::from(self)
            }

            #[inline]
            fn to_f64(self) -> OrderedFloat<f64> {
                OrderedFloat(self as f64)
            }
        }

        impl Integer for $ty {
            #[inline]
            fn negate(self) -> Self {
                self.wrapping_neg()
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

/// One [`Scalar`] impl per float column type.
macro_rules! float_scalar {
    ($inner:ty) => {
        impl Scalar for OrderedFloat<$inner> {
            type Wide = OrderedFloat<f64>;

            const ZERO: Self = OrderedFloat(0.0);
            const ONE: Self = OrderedFloat(1.0);

            #[inline]
            fn plus(self, other: Self) -> Self {
                self + other
            }

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
// Accumulator
// =============================================================================

/// A reduction accumulated in the difference position.
///
/// Implementors are `Semigroup`s whose `plus_equals` is the reduction
/// step, which is what lets Differential Dataflow's consolidation do the
/// work. One row's contribution is built by the accumulator's own `lift`,
/// as [`super::Reduction::contribute`] does; the trait only folds and
/// answers. An accumulation starts from its first contribution, so an
/// accumulator needs no zero: a group without rows answers
/// [`super::Reduction::empty`], where the reduction is defined on one, and
/// that accumulation then starts the group's.
pub trait Accumulator: Semigroup + 'static {
    /// The type the reduction answers in: the column's, or the float a
    /// mean is declared to answer.
    type Value: ExchangeData;

    /// Unwraps the accumulated answer.
    ///
    /// Never called on an empty accumulation, though each caller rules that
    /// out its own way: the threshold predicate drops what [`IsZero`] reports,
    /// `consolidate` drops it before a static loop's lattice leaves, and
    /// `reduce_abelian` does not invoke its logic for an empty group. Only
    /// [`MeanAccumulator`] has an empty state at all, which a deleted match
    /// subtracting the only one leaves.
    fn finish(self) -> Self::Value;

    /// Returns `true` if this accumulation supersedes `current` and must be
    /// re-emitted: a tightened bound for the extremes, any change at all for
    /// the accumulating reductions, and never for a choice, which stands
    /// once a threshold settles it.
    fn supersedes(&self, current: &Self) -> bool;
}

/// A weight whose `plus_equals` absorbs its repeats: `a + a = a`, so a
/// row that contributes twice answers as if it contributed once. The
/// extremes are idempotent; a count, sum or mean is not, as each repeat
/// adds again. The law is not checked; implementing this trait promises
/// it.
pub trait Idempotent: Semigroup {}

/// A weight whose `plus_equals` picks one of its terms: `a + b` is `a` or
/// `b`, whichever beats the other, so an answer is always one row's
/// value, never a blend of several. The extremes are selective, and a
/// selective weight is idempotent, as `a + a` can only be `a`.
///
/// `beats` must order the terms strictly: of two unequal terms exactly
/// one beats the other, and no term beats an equal one, so the sum is the
/// same in any order. [`select`] adds by it. The law is not checked;
/// implementing this trait promises it.
pub trait Selective: Idempotent {
    /// Returns `true` if this term replaces `held` in a sum.
    fn beats(&self, held: &Self) -> bool;
}

/// Adds `term` to `held` under a [`Selective`] weight: `term` replaces
/// `held` if it beats it, so a tie keeps `held`.
#[inline]
fn select<S: Selective>(held: &mut S, term: &S) {
    if term.beats(held) {
        held.clone_from(term);
    }
}

/// Declares an accumulator holding the extreme of a [`Scalar`] column: a
/// contribution takes over when it compares `$wins` to the held one.
///
/// Every accumulation is some row's value, so `is_zero` stays `false`:
/// Differential Dataflow would otherwise mistake it for an absent record.
macro_rules! scalar_extreme {
    ($name:ident, $doc:literal, $wins:expr) => {
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
                select(self, rhs);
            }
        }

        impl<V: Scalar> Idempotent for $name<V> {}

        impl<V: Scalar> Selective for $name<V> {
            #[inline]
            fn beats(&self, held: &Self) -> bool {
                self.value.cmp(&held.value) == $wins
            }
        }

        impl<V: Scalar> $name<V> {
            /// One row's contribution.
            #[inline]
            pub(super) fn lift(value: V) -> Self {
                Self { value }
            }
        }

        impl<V: Scalar> Accumulator for $name<V> {
            type Value = V;

            #[inline]
            fn finish(self) -> V {
                self.value
            }

            #[inline]
            fn supersedes(&self, current: &Self) -> bool {
                self.beats(current)
            }
        }
    };
}

scalar_extreme!(MinAccumulator, "Running minimum.", Ordering::Less);

scalar_extreme!(MaxAccumulator, "Running maximum.", Ordering::Greater);

// =============================================================================
// SumAccumulator
// =============================================================================

/// Running total, identity `V::ZERO`. Also backs `count`, whose rows each
/// contribute one.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct SumAccumulator<V: Scalar> {
    value: V,
}

impl<V: Scalar> IsZero for SumAccumulator<V> {
    #[inline]
    fn is_zero(&self) -> bool {
        // A total of zero is still an answer, which Differential Dataflow
        // would discard as an absent record. `IsZero` may miss a zero;
        // `Counted` tells an empty group by its count instead.
        false
    }
}

impl<V: Scalar> Semigroup for SumAccumulator<V> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.value = self.value.plus(rhs.value);
    }
}

impl<V: Scalar> SumAccumulator<V> {
    /// One row's contribution.
    #[inline]
    pub(super) fn lift(value: V) -> Self {
        Self { value }
    }
}

impl<V: Scalar> Monoid for SumAccumulator<V> {
    #[inline]
    fn zero() -> Self {
        Self::lift(V::ZERO)
    }
}

/// Negates the total, which wraps.
impl<V: Integer> Abelian for SumAccumulator<V> {
    #[inline]
    fn negate(&mut self) {
        self.value = self.value.negate();
    }
}

impl<V: Scalar> Accumulator for SumAccumulator<V> {
    type Value = V;

    #[inline]
    fn finish(self) -> V {
        self.value
    }

    #[inline]
    fn supersedes(&self, current: &Self) -> bool {
        self != current
    }
}

// =============================================================================
// MeanAccumulator
// =============================================================================

/// Running mean over a column `V`, held as the pair it decomposes into:
/// a sum at the column's [`Scalar::Wide`] and a count, divided once at
/// the end in `f64` and answered in the float `F` the mean's column is
/// declared as.
///
/// Unlike the other accumulators this one reports its zero, the sum and
/// count of no rows, which has no mean to report: a deleted match that
/// subtracts a group's only one leaves it.
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
        // A count back at zero beside a sum that is not, as swapping one
        // value for another leaves, still changes the sum.
        self.count == 0 && self.sum == V::Wide::ZERO
    }
}

impl<V: Scalar, F: Float> Semigroup for MeanAccumulator<V, F> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.sum = self.sum.plus(rhs.sum);
        self.count = self.count.plus(rhs.count);
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

/// Negates both the sum and the count, which both wrap.
impl<V: Scalar<Wide: Integer>, F: Float> Abelian for MeanAccumulator<V, F> {
    #[inline]
    fn negate(&mut self) {
        self.sum = self.sum.negate();
        self.count = self.count.negate();
    }
}

impl<V: Scalar, F: Float> Accumulator for MeanAccumulator<V, F> {
    type Value = F;

    #[inline]
    fn finish(self) -> F {
        // Unreachable per `Accumulator::finish`, but an identity beats dividing
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

// =============================================================================
// ValueOrd
// =============================================================================

/// The order FlowLog compares column values in: numbers by value, strings
/// by text, tuples field by field.
///
/// An interned string's key orders by when the string was first interned,
/// not by its text, so a [`Symbol`] compares the text it stands for, as a
/// comparison between interned strings does. Every other column type
/// compares by its own `Ord`.
pub trait ValueOrd: ExchangeData {
    /// Compares `self` with `other` in FlowLog's order.
    fn value_cmp(&self, other: &Self) -> Ordering;
}

/// [`ValueOrd`] for column types whose own `Ord` is FlowLog's order.
macro_rules! ord_value {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ValueOrd for $ty {
                #[inline]
                fn value_cmp(&self, other: &Self) -> Ordering {
                    self.cmp(other)
                }
            }
        )+
    };
}

ord_value!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    OrderedFloat<f32>,
    OrderedFloat<f64>,
    bool,
    String,
    (),
);

impl ValueOrd for Symbol {
    #[inline]
    fn value_cmp(&self, other: &Self) -> Ordering {
        // One key stands for one string, which spares the lookups.
        if self == other {
            return Ordering::Equal;
        }
        resolve(*self).cmp(resolve(*other))
    }
}

/// [`ValueOrd`] for a tuple of [`ValueOrd`] fields, compared in order: a
/// tuple column, or the columns a choice ranges over.
macro_rules! tuple_value {
    ($($field:ident $index:tt),+) => {
        impl<$($field: ValueOrd),+> ValueOrd for ($($field,)+) {
            #[inline]
            fn value_cmp(&self, other: &Self) -> Ordering {
                Ordering::Equal$(.then_with(|| self.$index.value_cmp(&other.$index)))+
            }
        }
    };
}

tuple_value!(A 0);
tuple_value!(A 0, B 1);
tuple_value!(A 0, B 1, C 2);
tuple_value!(A 0, B 1, C 2, D 3);
tuple_value!(A 0, B 1, C 2, D 3, E 4);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_value!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

// =============================================================================
// Text extremes
// =============================================================================

/// Declares an accumulator holding the extreme of a string column: a
/// contribution takes over when it compares `$wins` to the held one
/// under [`ValueOrd`].
///
/// Every accumulation is some row's value, so `is_zero` stays `false`, as
/// for the numeric extremes.
macro_rules! text_extreme {
    ($name:ident, $doc:literal, $wins:expr) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(bound = "")]
        pub struct $name<V: ValueOrd> {
            value: V,
        }

        impl<V: ValueOrd> IsZero for $name<V> {
            #[inline]
            fn is_zero(&self) -> bool {
                false
            }
        }

        impl<V: ValueOrd> Semigroup for $name<V> {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                select(self, rhs);
            }
        }

        impl<V: ValueOrd> Idempotent for $name<V> {}

        impl<V: ValueOrd> Selective for $name<V> {
            #[inline]
            fn beats(&self, held: &Self) -> bool {
                self.value.value_cmp(&held.value) == $wins
            }
        }

        impl<V: ValueOrd> $name<V> {
            /// One row's contribution.
            #[inline]
            pub(super) fn lift(value: V) -> Self {
                Self { value }
            }
        }

        impl<V: ValueOrd> Accumulator for $name<V> {
            type Value = V;

            #[inline]
            fn finish(self) -> V {
                self.value
            }

            #[inline]
            fn supersedes(&self, current: &Self) -> bool {
                self.beats(current)
            }
        }
    };
}

text_extreme!(
    TextMinAccumulator,
    "Running minimum of a string column.",
    Ordering::Less
);

text_extreme!(
    TextMaxAccumulator,
    "Running maximum of a string column.",
    Ordering::Greater
);

// =============================================================================
// Counted
// =============================================================================

/// A reduction together with the number of matches in it. This is the
/// weight a `diff::Mutable` group carries when it folds: a new match adds
/// one to the count and its contribution to the accumulation, and a
/// deleted match subtracts both, which takes an [`Abelian`] reduction.
///
/// The count is there because the accumulation alone cannot tell an
/// empty group from one whose matches add up to the zero, such as a sum
/// of zero. The weight is zero only when the count is zero and the
/// accumulation is its [`Monoid::zero`], and a group with a zero weight
/// has no answer. Replacing one match with another at the same time
/// changes the accumulation but not the count, so that change is not
/// zero.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Counted<S> {
    count: i64,
    accumulation: S,
}

impl<S> Counted<S> {
    /// The weight of `count` matches whose contributions add up to
    /// `accumulation`.
    #[inline]
    pub(in crate::operators) fn new(count: i64, accumulation: S) -> Self {
        Self {
            count,
            accumulation,
        }
    }
}

impl<S: Monoid + PartialEq> IsZero for Counted<S> {
    #[inline]
    fn is_zero(&self) -> bool {
        self.count == 0 && self.accumulation == S::zero()
    }
}

impl<S: Monoid + PartialEq> Semigroup for Counted<S> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.count = self.count.plus(rhs.count);
        self.accumulation.plus_equals(&rhs.accumulation);
    }
}

impl<S: Monoid + PartialEq> Monoid for Counted<S> {
    #[inline]
    fn zero() -> Self {
        Self::new(0, S::zero())
    }
}

/// Negates both the count, which wraps, and the accumulation.
impl<S: Abelian + PartialEq> Abelian for Counted<S> {
    #[inline]
    fn negate(&mut self) {
        // `i64` is abelian too, but its negation panics on overflow in a
        // debug build.
        self.count = Integer::negate(self.count);
        self.accumulation.negate();
    }
}

impl<S: Accumulator + Monoid + PartialEq> Accumulator for Counted<S> {
    type Value = S::Value;

    #[inline]
    fn finish(self) -> S::Value {
        self.accumulation.finish()
    }

    #[inline]
    fn supersedes(&self, current: &Self) -> bool {
        self.accumulation.supersedes(&current.accumulation)
    }
}

// =============================================================================
// ChoiceAccumulator
// =============================================================================

/// A group's choice: its least value under [`ValueOrd`].
///
/// It accumulates as a text minimum does, over any column type, from a
/// group's first candidate: a choice answers no group without one.
/// Nothing supersedes a choice, so a threshold settles a group once, at
/// the first time it has a value: a static loop's choice is the least of
/// the first round with a candidate.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ChoiceAccumulator<V: ValueOrd> {
    value: V,
}

impl<V: ValueOrd> IsZero for ChoiceAccumulator<V> {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl<V: ValueOrd> Semigroup for ChoiceAccumulator<V> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        select(self, rhs);
    }
}

impl<V: ValueOrd> Idempotent for ChoiceAccumulator<V> {}

impl<V: ValueOrd> Selective for ChoiceAccumulator<V> {
    #[inline]
    fn beats(&self, held: &Self) -> bool {
        self.value.value_cmp(&held.value) == Ordering::Less
    }
}

impl<V: ValueOrd> ChoiceAccumulator<V> {
    /// One row's candidate.
    #[inline]
    pub(super) fn lift(value: V) -> Self {
        Self { value }
    }
}

impl<V: ValueOrd> Accumulator for ChoiceAccumulator<V> {
    type Value = V;

    #[inline]
    fn finish(self) -> V {
        self.value
    }

    #[inline]
    fn supersedes(&self, _current: &Self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rstest::rstest;

    use super::*;
    use crate::intern::intern;

    /// Accumulates a group the way `plus_equals` does during
    /// consolidation, from its first part, as a group without a seed does.
    fn accumulate<S: Semigroup>(parts: impl IntoIterator<Item = S>) -> S {
        let mut parts = parts.into_iter();
        let mut acc = parts.next().expect("a group accumulates at least one part");
        for part in parts {
            acc.plus_equals(&part);
        }
        acc
    }

    /// The weight that cancels `weight`.
    fn negated<S: Abelian>(mut weight: S) -> S {
        weight.negate();
        weight
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

    /// The text extremes keep the least and the greatest text, whatever
    /// order the rows arrive in.
    #[test]
    fn text_extremes_follow_text_order() {
        let texts = || ["pear", "apple", "fig"].map(String::from);
        let least = accumulate(texts().map(TextMinAccumulator::lift)).finish();
        let greatest = accumulate(texts().map(TextMaxAccumulator::lift)).finish();
        assert_eq!((least.as_str(), greatest.as_str()), ("apple", "pear"));
    }

    /// An interned extreme compares the text, not the key, which orders by
    /// when each string was first interned.
    #[test]
    fn an_interned_extreme_orders_by_text_not_by_key() {
        let zebra = intern("accumulator-test-zebra");
        let apple = intern("accumulator-test-apple");
        assert!(
            zebra < apple,
            "zebra was interned first, so its key is smaller"
        );
        let least = accumulate([zebra, apple].map(TextMinAccumulator::lift)).finish();
        let greatest = accumulate([zebra, apple].map(TextMaxAccumulator::lift)).finish();
        assert_eq!(
            (resolve(least), resolve(greatest)),
            ("accumulator-test-apple", "accumulator-test-zebra")
        );
    }

    // Cases: rows, the extreme they merge to.
    #[rstest]
    #[case::min([3i64, 1].map(MinAccumulator::lift), 1)]
    #[case::max([3i64, 1].map(MaxAccumulator::lift), 3)]
    #[case::text_min(
        ["pear", "fig"].map(|text| TextMinAccumulator::lift(text.to_owned())),
        "fig".to_owned()
    )]
    #[case::text_max(
        ["pear", "fig"].map(|text| TextMaxAccumulator::lift(text.to_owned())),
        "pear".to_owned()
    )]
    #[case::interned_min(
        ["accumulator-test-pear", "accumulator-test-fig"]
            .map(|text| TextMinAccumulator::lift(intern(text))),
        intern("accumulator-test-fig")
    )]
    #[case::interned_max(
        ["accumulator-test-pear", "accumulator-test-fig"]
            .map(|text| TextMaxAccumulator::lift(intern(text))),
        intern("accumulator-test-pear")
    )]
    fn an_idempotent_accumulator_absorbs_a_repeat<S>(
        #[case] rows: [S; 2],
        #[case] expected: S::Value,
    ) where
        S: Accumulator + Idempotent,
        S::Value: PartialEq + Debug,
    {
        for row in &rows {
            let mut again = accumulate(rows.clone());
            again.plus_equals(row);
            assert_eq!(again.finish(), expected);
        }
    }

    /// A selective sum is one of its terms, the same in either order.
    #[rstest]
    #[case::min([3i64, 1].map(MinAccumulator::lift))]
    #[case::max([3i64, 1].map(MaxAccumulator::lift))]
    #[case::tie([2i64, 2].map(MaxAccumulator::lift))]
    #[case::text_min(["pear", "fig"].map(|text| TextMinAccumulator::lift(text.to_owned())))]
    #[case::text_max(["pear", "fig"].map(|text| TextMaxAccumulator::lift(text.to_owned())))]
    fn a_selective_sum_is_one_of_its_terms<S>(#[case] rows: [S; 2])
    where
        S: Accumulator + Selective + PartialEq + Debug,
    {
        let [a, b] = rows;
        // Of two unequal terms exactly one beats the other; of two equal
        // terms, neither does.
        assert_eq!(
            u8::from(a.beats(&b)) + u8::from(b.beats(&a)),
            u8::from(a != b)
        );
        let sum = accumulate([a.clone(), b.clone()]);
        assert!(sum == a || sum == b, "{sum:?} is neither {a:?} nor {b:?}");
        assert_eq!(accumulate([b, a]), sum);
    }

    /// A tie keeps the term already held: of two equal zeros, the first.
    #[test]
    fn a_tie_keeps_the_held_term() {
        let zeros = [-0.0, 0.0].map(OrderedFloat::<f64>);
        let max = accumulate(zeros.map(MaxAccumulator::lift)).finish();
        assert!(max.is_sign_negative());
    }

    #[test]
    fn sum_accumulates_at_the_column_width() {
        let parts = [1i32, 2, 3].map(SumAccumulator::lift);
        assert_eq!(accumulate(parts).finish(), 6);
    }

    /// The zero adds nothing, on either side.
    // Cases: weight.
    #[rstest]
    #[case::sum(SumAccumulator::lift(7i64))]
    #[case::mean(Mean64::lift(7i64))]
    #[case::counted(Counted::new(2, SumAccumulator::lift(7i64)))]
    fn the_zero_adds_nothing<S>(#[case] weight: S)
    where
        S: Monoid + PartialEq + Debug,
    {
        assert_eq!(accumulate([weight.clone(), S::zero()]), weight);
        assert_eq!(accumulate([S::zero(), weight.clone()]), weight);
    }

    /// A weight and its negation add up to the zero, even where the
    /// negation wraps: at the bottom of a column's range, and in an
    /// unsigned count.
    // Cases: weight.
    #[rstest]
    #[case::sum(SumAccumulator::lift(7i64))]
    #[case::sum_at_the_bottom(SumAccumulator::lift(i32::MIN))]
    #[case::mean(Mean64::lift(7i64))]
    #[case::unsigned_mean(Mean64::lift(7u8))]
    #[case::counted(Counted::new(2, SumAccumulator::lift(7i64)))]
    fn a_weight_and_its_negation_add_up_to_the_zero<S>(#[case] weight: S)
    where
        S: Abelian + PartialEq + Debug,
    {
        assert_eq!(accumulate([weight.clone(), negated(weight)]), S::zero());
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

    /// Only a `MeanAccumulator` reports a zero, and only while empty. The
    /// others must never report one, or a live answer whose value happens
    /// to be `0` would be dropped from the collection. A mean whose count
    /// is back at zero but whose sum is not, as swapping one value for
    /// another leaves, still changes a sum, so it is not zero either.
    // Cases: weight, zero.
    #[rstest]
    #[case::an_emptied_mean(accumulate([Mean64::lift(2i64), negated(Mean64::lift(2))]), true)]
    #[case::a_swapped_mean(accumulate([Mean64::lift(7i64), negated(Mean64::lift(2))]), false)]
    #[case::a_mean_of_zero(Mean64::lift(0i64), false)]
    #[case::a_sum_of_zero(SumAccumulator::lift(0i64), false)]
    #[case::a_min_of_zero(MinAccumulator::lift(0i64), false)]
    #[case::an_empty_text_min(TextMinAccumulator::lift(String::new()), false)]
    #[case::a_choice_of_zero(ChoiceAccumulator::lift(0i64), false)]
    fn only_an_empty_mean_is_zero<S: IsZero>(#[case] weight: S, #[case] zero: bool) {
        assert_eq!(weight.is_zero(), zero);
    }

    /// An integer total that overflows is exact again once the values
    /// that pushed it out of range are cancelled.
    // Cases: values in arrival order, total.
    #[rstest]
    #[case::through_the_top(vec![i32::MAX, i32::MAX, 5, -i32::MAX, -i32::MAX], 5)]
    #[case::through_the_bottom(vec![i32::MIN, -1, 7, 1], i32::MIN + 7)]
    fn an_integer_total_lands_exactly_through_an_overflow(
        #[case] values: Vec<i32>,
        #[case] expected: i32,
    ) {
        let parts = values.into_iter().map(SumAccumulator::lift);
        assert_eq!(accumulate(parts).finish(), expected);
    }

    /// Adding a value's negation to a total removes that value.
    #[test]
    fn negation_takes_a_value_back_out_of_a_total() {
        let sum = accumulate([1i64, 2, 3].map(SumAccumulator::lift));
        let sum = accumulate([sum, negated(SumAccumulator::lift(2))]);
        assert_eq!(sum.finish(), 4);
    }

    /// Adding a value's negation to a mean removes it from both the sum
    /// and the count. Removing every value leaves the empty mean.
    #[test]
    fn negation_takes_a_value_back_out_of_a_mean() {
        let mean = accumulate([1i64, 2, 3].map(Mean64::lift));
        let mean = accumulate([mean, negated(Mean64::lift(1))]);
        assert_eq!(mean.finish(), 2.5);
        let leaving = [2, 3].map(|value| negated(Mean64::lift(value)));
        let empty = accumulate([mean].into_iter().chain(leaving));
        assert_eq!(empty, Mean64::zero());
    }

    /// A counted weight is zero only when its count and its total are
    /// both zero. A group whose matches sum to zero is not empty, and
    /// replacing one match with another changes the total but not the
    /// count.
    // Cases: count, total, zero.
    #[rstest]
    #[case::empty(0, 0, true)]
    #[case::a_zero_total(2, 0, false)]
    #[case::a_swap(0, 5, false)]
    #[case::a_match(1, 5, false)]
    fn a_counted_weight_is_zero_only_when_empty(
        #[case] count: i64,
        #[case] total: i64,
        #[case] zero: bool,
    ) {
        let weight = Counted::new(count, SumAccumulator::lift(total));
        assert_eq!(weight.is_zero(), zero);
    }

    /// A counted group adds up counts and totals. Adding the negation of
    /// every match leaves the zero weight.
    #[test]
    fn a_counted_match_leaves_as_it_came() {
        let matches = [3i64, -3, 4].map(|value| Counted::new(1, SumAccumulator::lift(value)));
        let group = accumulate(matches);
        assert_eq!(group, Counted::new(3, SumAccumulator::lift(4)));
        let leaving = matches.map(negated);
        assert!(accumulate([group].into_iter().chain(leaving)).is_zero());
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

        let lo = TextMinAccumulator::lift("a".to_string());
        let hi = TextMinAccumulator::lift("b".to_string());
        assert!(lo.supersedes(&hi));
        assert!(!hi.supersedes(&lo));
        assert!(!lo.supersedes(&lo));

        let lo = TextMaxAccumulator::lift("a".to_string());
        let hi = TextMaxAccumulator::lift("b".to_string());
        assert!(hi.supersedes(&lo));
        assert!(!lo.supersedes(&hi));

        assert!(SumAccumulator::lift(1i64).supersedes(&SumAccumulator::lift(2i64)));
        assert!(!SumAccumulator::lift(2i64).supersedes(&SumAccumulator::lift(2i64)));
    }

    /// A tuple compares field by field, the first that differs deciding.
    // Cases: left, right, order.
    #[rstest]
    #[case((1, 9), (2, 0), Ordering::Less)]
    #[case((2, 0), (1, 9), Ordering::Greater)]
    #[case((1, 2), (1, 3), Ordering::Less)]
    #[case((1, 2), (1, 2), Ordering::Equal)]
    fn a_tuple_compares_field_by_field(
        #[case] left: (i32, i32),
        #[case] right: (i32, i32),
        #[case] expected: Ordering,
    ) {
        assert_eq!(left.value_cmp(&right), expected);
    }

    /// An interned field compares by its text, not by its key, which orders
    /// by when the string was first interned.
    #[test]
    fn an_interned_field_compares_by_text() {
        let zebra = intern("accumulator-test-field-zebra");
        let apple = intern("accumulator-test-field-apple");
        assert!(
            zebra < apple,
            "zebra was interned first, so its key is smaller"
        );
        assert_eq!((7, zebra).value_cmp(&(7, apple)), Ordering::Greater);
    }

    /// A choice accumulates from its group's first candidate to the least
    /// one, whichever candidate comes first.
    // Cases: candidates.
    #[rstest]
    #[case([(2, "b"), (1, "z"), (1, "a")])]
    #[case([(1, "a"), (2, "b"), (1, "z")])]
    #[case([(1, "z"), (1, "a"), (2, "b")])]
    fn a_choice_accumulates_to_the_least_value(#[case] candidates: [(i32, &str); 3]) {
        let [mut choice, rest @ ..] =
            candidates.map(|(n, s)| ChoiceAccumulator::lift((n, s.to_string())));
        for candidate in &rest {
            choice.plus_equals(candidate);
        }
        assert_eq!(choice.finish(), (1, "a".to_string()));
    }
}
