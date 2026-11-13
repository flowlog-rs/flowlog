//! Semirings that carry a reduction in the difference position.
//!
//! Each type accumulates one reduction under `plus_equals`. The presence
//! weights, `diff::Static` and `diff::Append`, carry it as a weight, so a
//! collection keyed by the group columns reduces through Differential
//! Dataflow's own consolidation. `diff::Mutable` carries it the same way,
//! wrapped in a [`Counted`], when its aggregate folds; otherwise its
//! reduce accumulates it. [`Semiring`] is what they all dispatch on; the
//! concrete types stay internal because generated code names a reduction,
//! never a semiring.
//!
//! The scalar table lives in [`Scalar`], one impl per numeric column type
//! FlowLog admits, and [`Float`] is the part of it a mean answers in.
//! [`Text`] holds the string column types, which only the extremes run
//! over.
//!
//! A law a semiring obeys beyond merging in any order is a trait of its
//! own, which an operator relying on the law names as a bound. The
//! extremes are idempotent and selective:
//!
//! | trait | law | buys |
//! |---|---|---|
//! | [`Idempotent`] | `a + a = a` | a repeated row merges as one |
//! | [`Selective`] | `a + b` is `a` or `b` | an answer is one row's value |

use std::cmp::Ordering;
use std::marker::PhantomData;

use differential_dataflow::ExchangeData;
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

    /// Whether adding a value's negation to a total removes that value
    /// exactly. `true` for integers: their addition wraps on overflow, so
    /// the same additions give the same total in any order. `false` for
    /// floats, whose addition rounds: `1e20 + 1 - 1e20` is `0`, not `1`.
    const INVERTIBLE: bool;

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

    /// Adds the two values. Integer addition wraps on overflow, so a
    /// total that overflows and later comes back into range is exact.
    #[must_use]
    fn plus(self, other: Self) -> Self;

    /// Returns the value that adds with this one to zero.
    #[must_use]
    fn negate(self) -> Self;

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

            const INVERTIBLE: bool = true;
            const MIN: Self = <$ty>::MIN;
            const MAX: Self = <$ty>::MAX;
            const ZERO: Self = 0;
            const ONE: Self = 1;

            #[inline]
            fn plus(self, other: Self) -> Self {
                self.wrapping_add(other)
            }

            #[inline]
            fn negate(self) -> Self {
                self.wrapping_neg()
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

            const INVERTIBLE: bool = false;
            const MIN: Self = OrderedFloat(<$inner>::NEG_INFINITY);
            const MAX: Self = OrderedFloat(<$inner>::INFINITY);
            const ZERO: Self = OrderedFloat(0.0);
            const ONE: Self = OrderedFloat(1.0);

            #[inline]
            fn plus(self, other: Self) -> Self {
                self + other
            }

            #[inline]
            fn negate(self) -> Self {
                -self
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
// Semiring
// =============================================================================

/// A reduction accumulated in the difference position.
///
/// Implementors are `Monoid`s whose `plus_equals` is the reduction step,
/// which is what lets Differential Dataflow's consolidation do the work.
/// One row's contribution is built by the accumulator's own `lift`, as
/// [`super::Reduction::contribute`] does; the trait only folds and
/// answers.
pub trait Semiring: Monoid + 'static {
    /// The type the reduction answers in: the column's, or the float a
    /// mean is declared to answer.
    type Value: ExchangeData;

    /// Whether adding a contribution's [`Semiring::negate`] removes that
    /// contribution exactly, so that a deleted match can be subtracted
    /// from its group's weight. A count, sum or mean is invertible when
    /// its scalar is ([`Scalar::INVERTIBLE`]). A minimum or maximum never
    /// is: it does not keep the values it passed over.
    const INVERTIBLE: bool = false;

    /// Unwraps the accumulated answer.
    ///
    /// Never called on an empty accumulation, though each caller rules that
    /// out its own way: the threshold predicate drops what [`IsZero`] reports,
    /// `consolidate` drops it before a static loop's lattice leaves, and
    /// `reduce_abelian` does not invoke its logic for an empty group. Only
    /// [`MeanAccumulator`] and the text extremes have an empty state at all.
    fn finish(self) -> Self::Value;

    /// Returns `true` if this accumulation supersedes `current` and must be
    /// re-emitted: a tightened bound for the extremes, any change at all for
    /// the accumulating reductions.
    fn supersedes(&self, current: &Self) -> bool;

    /// Returns the weight that cancels this one under `plus_equals`. The
    /// result is exact when [`Semiring::INVERTIBLE`]; a float sum or mean
    /// cancels only up to rounding.
    ///
    /// # Panics
    ///
    /// Panics for a minimum or maximum, which has no negation.
    #[must_use]
    fn negate(self) -> Self {
        unreachable!("a minimum or maximum has no negation, so no fold subtracts from one")
    }
}

/// A semiring that absorbs its repeats: `a + a = a`, so a row that
/// contributes twice answers as if it contributed once. The extremes are
/// idempotent; a count, sum or mean is not, as each repeat adds again.
///
/// A row can contribute more than once where nothing dedups it first, as
/// a lattice relation's row does when a later round derives it again, so
/// only an idempotent semiring merges such rows. The law is not checked;
/// implementing this trait promises it.
pub trait Idempotent: Semigroup {}

/// A semiring whose sum picks one of its terms: `a + b` is `a` or `b`,
/// whichever beats the other, so an answer is always one row's value,
/// never a blend of several. The extremes are selective, and a selective
/// semiring is idempotent, as `a + a` can only be `a`.
///
/// `beats` must order the terms strictly: of two unequal terms exactly
/// one beats the other, and no term beats an equal one, so the sum is the
/// same in any order. [`select`] adds by it. The law is not checked;
/// implementing this trait promises it.
pub trait Selective: Idempotent {
    /// Returns `true` if this term replaces `held` in a sum.
    fn beats(&self, held: &Self) -> bool;
}

/// Adds `term` to `held` in a [`Selective`] semiring: `term` replaces
/// `held` if it beats it, so a tie keeps `held`.
#[inline]
fn select<S: Selective>(held: &mut S, term: &S) {
    if term.beats(held) {
        held.clone_from(term);
    }
}

/// Declares a semiring holding the extreme of a [`Scalar`] column: it
/// starts at `$zero`, a bound of the column type, and a contribution
/// takes over when it compares `$keeps` to the held one.
///
/// A group's extreme can equal the bound, such as a minimum of `V::MAX`.
/// `is_zero` must stay `false` so Differential Dataflow does not mistake
/// that valid result for an absent record and discard it.
macro_rules! scalar_extreme {
    ($name:ident, $doc:literal, $zero:expr, $keeps:expr) => {
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

        impl<V: Scalar> Monoid for $name<V> {
            #[inline]
            fn zero() -> Self {
                Self { value: $zero }
            }
        }

        impl<V: Scalar> Idempotent for $name<V> {}

        impl<V: Scalar> Selective for $name<V> {
            #[inline]
            fn beats(&self, held: &Self) -> bool {
                self.value.cmp(&held.value) == $keeps
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
                self.beats(current)
            }
        }
    };
}

scalar_extreme!(
    MinAccumulator,
    "Running minimum, identity `V::MAX`.",
    V::MAX,
    Ordering::Less
);

scalar_extreme!(
    MaxAccumulator,
    "Running maximum, identity `V::MIN`.",
    V::MIN,
    Ordering::Greater
);

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
        // would discard as an absent record.
        false
    }
}

impl<V: Scalar> Semigroup for SumAccumulator<V> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.value = self.value.plus(rhs.value);
    }
}

impl<V: Scalar> Monoid for SumAccumulator<V> {
    #[inline]
    fn zero() -> Self {
        Self { value: V::ZERO }
    }
}

impl<V: Scalar> SumAccumulator<V> {
    /// One row's contribution.
    #[inline]
    pub(super) fn lift(value: V) -> Self {
        Self { value }
    }
}

impl<V: Scalar> Semiring for SumAccumulator<V> {
    type Value = V;

    const INVERTIBLE: bool = V::INVERTIBLE;

    #[inline]
    fn finish(self) -> V {
        self.value
    }

    #[inline]
    fn supersedes(&self, current: &Self) -> bool {
        self != current
    }

    #[inline]
    fn negate(self) -> Self {
        Self {
            value: self.value.negate(),
        }
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
/// Unlike the other numeric semirings this one has a genuine zero -- an
/// empty accumulation -- because `(sum, count)` starts at `(0, 0)` and a
/// count of zero has no mean to report.
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
        self.sum = self.sum.plus(rhs.sum);
        self.count = self.count.plus(rhs.count);
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

    const INVERTIBLE: bool = V::Wide::INVERTIBLE;

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

    /// Negates both the sum and the count, which both wrap.
    #[inline]
    fn negate(self) -> Self {
        Self {
            sum: self.sum.negate(),
            count: self.count.negate(),
            answer: PhantomData,
        }
    }
}

// =============================================================================
// Text
// =============================================================================

/// String column types an extreme can run over, read as their text.
///
/// An interned string's key orders by when the string was first interned,
/// not by its text, so the extremes compare the text, as a comparison
/// between interned strings does.
pub trait Text: ExchangeData {
    /// The string the value stands for.
    fn text(&self) -> &str;
}

impl Text for String {
    #[inline]
    fn text(&self) -> &str {
        self
    }
}

impl Text for Symbol {
    #[inline]
    fn text(&self) -> &str {
        resolve(*self)
    }
}

/// Declares a semiring holding the extreme of a [`Text`] column: a
/// contribution takes over when its text compares `$keeps` to the held
/// one's.
///
/// No string is greater than every other, so unlike [`MinAccumulator`] a
/// text minimum has no bound of its column type to start from. Both text
/// extremes start empty instead: the empty accumulation is their zero,
/// which [`IsZero`] reports, as no group is answered without a row. The
/// numeric extremes keep their bounds, as an `Option` would widen their
/// weights; for a string it costs nothing, `None` taking a bit pattern no
/// `String` or `Symbol` uses.
macro_rules! text_extreme {
    ($name:ident, $doc:literal, $keeps:expr) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(bound = "")]
        pub struct $name<V: Text> {
            value: Option<V>,
        }

        impl<V: Text> IsZero for $name<V> {
            #[inline]
            fn is_zero(&self) -> bool {
                self.value.is_none()
            }
        }

        impl<V: Text> Semigroup for $name<V> {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                select(self, rhs);
            }
        }

        impl<V: Text> Monoid for $name<V> {
            #[inline]
            fn zero() -> Self {
                Self { value: None }
            }
        }

        impl<V: Text> Idempotent for $name<V> {}

        impl<V: Text> Selective for $name<V> {
            #[inline]
            fn beats(&self, held: &Self) -> bool {
                match (&self.value, &held.value) {
                    (Some(term), Some(held)) => term.text().cmp(held.text()) == $keeps,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            }
        }

        impl<V: Text> $name<V> {
            /// One row's contribution.
            #[inline]
            pub(super) fn lift(value: V) -> Self {
                Self { value: Some(value) }
            }
        }

        impl<V: Text> Semiring for $name<V> {
            type Value = V;

            /// # Panics
            ///
            /// Panics on the empty accumulation, which no caller finishes.
            #[inline]
            fn finish(self) -> V {
                self.value
                    .expect("an extreme answers only a group with a row, so it is never empty")
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
    "Running minimum of a [`Text`] column, empty until a row contributes.",
    Ordering::Less
);

text_extreme!(
    TextMaxAccumulator,
    "Running maximum of a [`Text`] column, empty until a row contributes.",
    Ordering::Greater
);

// =============================================================================
// Counted
// =============================================================================

/// An invertible reduction together with the number of matches in it.
/// This is the weight a `diff::Mutable` group carries when it folds: a
/// new match adds one to the count and its contribution to the
/// accumulation, and a deleted match subtracts both.
///
/// The count is there because the accumulation alone cannot tell an
/// empty group from one whose matches add up to the identity, such as a
/// sum of zero. The weight is zero only when the count and the
/// accumulation are both zero, and a group with a zero weight has no
/// answer. Replacing one match with another at the same time changes
/// the accumulation but not the count, so that change is not zero.
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

impl<S: Semiring + Eq> IsZero for Counted<S> {
    #[inline]
    fn is_zero(&self) -> bool {
        self.count == 0 && self.accumulation == S::zero()
    }
}

impl<S: Semiring + Eq> Semigroup for Counted<S> {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.count = self.count.plus(rhs.count);
        self.accumulation.plus_equals(&rhs.accumulation);
    }
}

impl<S: Semiring + Eq> Monoid for Counted<S> {
    #[inline]
    fn zero() -> Self {
        Self::new(0, S::zero())
    }
}

impl<S: Semiring + Eq> Semiring for Counted<S> {
    type Value = S::Value;

    const INVERTIBLE: bool = S::INVERTIBLE;

    #[inline]
    fn finish(self) -> S::Value {
        self.accumulation.finish()
    }

    #[inline]
    fn supersedes(&self, current: &Self) -> bool {
        self.accumulation.supersedes(&current.accumulation)
    }

    #[inline]
    fn negate(self) -> Self {
        Self::new(self.count.negate(), self.accumulation.negate())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rstest::rstest;

    use super::*;
    use crate::intern::intern;

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
        let zebra = intern("semiring-test-zebra");
        let apple = intern("semiring-test-apple");
        assert!(
            zebra < apple,
            "zebra was interned first, so its key is smaller"
        );
        let least = accumulate([zebra, apple].map(TextMinAccumulator::lift)).finish();
        let greatest = accumulate([zebra, apple].map(TextMaxAccumulator::lift)).finish();
        assert_eq!(
            (resolve(least), resolve(greatest)),
            ("semiring-test-apple", "semiring-test-zebra")
        );
    }

    /// An idempotent semiring absorbs a repeat: a merge that takes in one
    /// of its rows again is unchanged.
    #[rstest]
    #[case::min([3i64, 1].map(MinAccumulator::lift))]
    #[case::max([3i64, 1].map(MaxAccumulator::lift))]
    #[case::text_min(["pear", "fig"].map(|text| TextMinAccumulator::lift(text.to_owned())))]
    #[case::text_max(["pear", "fig"].map(|text| TextMaxAccumulator::lift(text.to_owned())))]
    fn an_idempotent_semiring_absorbs_a_repeat<S>(#[case] rows: [S; 2])
    where
        S: Semiring + Idempotent + PartialEq + Debug,
    {
        let merged = accumulate(rows.clone());
        for row in rows {
            let mut again = merged.clone();
            again.plus_equals(&row);
            assert_eq!(again, merged);
        }
    }

    /// A selective sum is one of its terms, the same in either order.
    #[rstest]
    #[case::min([3i64, 1].map(MinAccumulator::lift))]
    #[case::max([3i64, 1].map(MaxAccumulator::lift))]
    #[case::tie([2i64, 2].map(MaxAccumulator::lift))]
    #[case::text_min(["pear", "fig"].map(|text| TextMinAccumulator::lift(text.to_owned())))]
    #[case::text_max(["pear", "fig"].map(|text| TextMaxAccumulator::lift(text.to_owned())))]
    #[case::text_empty([TextMinAccumulator::zero(), TextMinAccumulator::lift("fig".to_owned())])]
    fn a_selective_sum_is_one_of_its_terms<S>(#[case] rows: [S; 2])
    where
        S: Semiring + Selective + PartialEq + Debug,
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

    /// Only `MeanAccumulator` and the text extremes report a zero, and
    /// only while empty. The others must never report one, or a live answer
    /// whose value happens to be `0` would be dropped from the collection.
    #[test]
    fn only_an_empty_mean_or_text_extreme_is_zero() {
        assert!(Mean64::<i64>::zero().is_zero());
        assert!(!Mean64::<i64>::lift(0).is_zero());
        assert!(TextMinAccumulator::<String>::zero().is_zero());
        assert!(!TextMinAccumulator::lift(String::new()).is_zero());
        assert!(!accumulate([SumAccumulator::lift(0i64)]).is_zero());
        assert!(!SumAccumulator::<i64>::zero().is_zero());
        assert!(!MinAccumulator::<i64>::zero().is_zero());
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
        let sum = accumulate([sum, SumAccumulator::lift(2).negate()]);
        assert_eq!(sum.finish(), 4);
    }

    /// Adding a value's negation to a mean removes it from both the sum
    /// and the count. Removing every value leaves the empty mean.
    #[test]
    fn negation_takes_a_value_back_out_of_a_mean() {
        let mean = accumulate([1i64, 2, 3].map(Mean64::lift));
        let mean = accumulate([mean, Mean64::lift(1).negate()]);
        assert_eq!(mean.finish(), 2.5);
        let empty = accumulate([mean, Mean64::lift(2).negate(), Mean64::lift(3).negate()]);
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
        let leaving = matches.map(Semiring::negate);
        assert!(accumulate([group].into_iter().chain(leaving)).is_zero());
    }

    /// `supersedes` is what the threshold predicate re-emits on: bounds only
    /// tighten, any text tightening an empty text extreme, while the
    /// accumulating reductions re-emit on any change.
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
        assert!(lo.supersedes(&TextMinAccumulator::zero()));
        assert!(!TextMinAccumulator::zero().supersedes(&lo));

        let lo = TextMaxAccumulator::lift("a".to_string());
        let hi = TextMaxAccumulator::lift("b".to_string());
        assert!(hi.supersedes(&lo));
        assert!(!lo.supersedes(&hi));

        assert!(SumAccumulator::lift(1i64).supersedes(&SumAccumulator::lift(2i64)));
        assert!(!SumAccumulator::lift(2i64).supersedes(&SumAccumulator::lift(2i64)));
    }
}
