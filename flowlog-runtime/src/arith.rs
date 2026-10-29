//! Arithmetic operators whose semantics FlowLog defines itself, as
//! functions generated code calls: codegen emits one call whatever the
//! operand type, and the trait impls here pick the meaning. Only `band`,
//! `bor`, and `bxor`, which Rust's infix form already gets right, are
//! emitted inline.
//!
//! Integer `+`, `-`, `*`, `/`, and `%` wrap on overflow whatever the
//! build: `/` and `%` truncate toward zero, `MIN / -1` wraps to `MIN` and
//! `MIN % -1` is `0`, and a zero divisor panics. Float arithmetic is IEEE.
//!
//! The shifts and `^` follow Souffle. A shift count is masked to the
//! operand width (`1 bshl 64` is `1 bshl 0` on 64 bits) instead of
//! overflowing. `bshr` extends the sign of a signed operand and `bshru`
//! fills with zeros; on unsigned operands they agree. Integer `^` wraps on
//! overflow, and under a negative exponent it truncates toward zero as
//! Souffle's `pow` in `double` does: the result is `0` unless the base is
//! `1` or `-1`. Float `^` is `powf`.

use ordered_float::OrderedFloat;

/// Integer operand of the shift operators. The count has the operand's
/// own type, as the typechecker requires both sides to agree.
pub trait Shift: Copy {
    /// `self << count`, the count masked to the operand width.
    fn bshl(self, count: Self) -> Self;
    /// `self >> count` keeping the sign, the count masked to the width.
    fn bshr(self, count: Self) -> Self;
    /// `self >> count` filling with zeros, the count masked to the width.
    fn bshru(self, count: Self) -> Self;
}

/// Numeric operand of `+`, `-`, `*`, `/`, and `%`.
///
/// Rust's infix `+` wraps only where overflow checks are off, which the
/// crate a program is built into decides, and its `/` panics on
/// `MIN / -1`, so integers wrap explicitly here instead. A call also
/// keeps rustc from evaluating a literal operation such as
/// `2147483647 + 1` at compile time and rejecting the crate.
pub trait Arith: Copy {
    /// `self + rhs`.
    fn add(self, rhs: Self) -> Self;
    /// `self - rhs`.
    fn sub(self, rhs: Self) -> Self;
    /// `self * rhs`.
    fn mul(self, rhs: Self) -> Self;
    /// `self / rhs`, truncating an integer quotient toward zero.
    ///
    /// # Panics
    ///
    /// Panics on an integer `rhs` of zero.
    fn div(self, rhs: Self) -> Self;
    /// `self % rhs`, with the sign of `self`.
    ///
    /// # Panics
    ///
    /// Panics on an integer `rhs` of zero.
    fn rem(self, rhs: Self) -> Self;
}

/// Numeric operand of `^`.
pub trait Pow: Copy {
    /// `self` raised to `exponent`.
    fn pow(self, exponent: Self) -> Self;
}

macro_rules! unsigned_ops {
    ($($t:ty),*) => {$(
        impl Shift for $t {
            #[inline]
            fn bshl(self, count: Self) -> Self {
                self.wrapping_shl(count as u32)
            }
            #[inline]
            fn bshr(self, count: Self) -> Self {
                self.wrapping_shr(count as u32)
            }
            #[inline]
            fn bshru(self, count: Self) -> Self {
                self.wrapping_shr(count as u32)
            }
        }
        impl Pow for $t {
            #[inline]
            fn pow(self, exponent: Self) -> Self {
                let exponent = exponent as u64;
                match u32::try_from(exponent) {
                    Ok(exponent) => self.wrapping_pow(exponent),
                    // `wrapping_pow` takes a `u32`, so a wider exponent
                    // splits into 32-bit halves: `self^exponent` is
                    // `self^low * (self^(2^32))^high`, where `self^(2^32)`
                    // is `(self^(2^16))^(2^16)`. Wrapping multiplication
                    // is exact modulo the width, so the product is too.
                    Err(_) => {
                        let low = exponent as u32;
                        let high = (exponent >> 32) as u32;
                        let self_to_2_32 = self.wrapping_pow(1 << 16).wrapping_pow(1 << 16);
                        self.wrapping_pow(low)
                            .wrapping_mul(self_to_2_32.wrapping_pow(high))
                    }
                }
            }
        }
    )*};
}

macro_rules! signed_ops {
    ($($t:ty => $u:ty),*) => {$(
        impl Shift for $t {
            #[inline]
            fn bshl(self, count: Self) -> Self {
                // `count as u32` keeps the low bits a negative count would
                // supply in Souffle's `count & (width - 1)`.
                self.wrapping_shl(count as u32)
            }
            #[inline]
            fn bshr(self, count: Self) -> Self {
                self.wrapping_shr(count as u32)
            }
            #[inline]
            fn bshru(self, count: Self) -> Self {
                (self as $u).wrapping_shr(count as u32) as $t
            }
        }
        impl Pow for $t {
            #[inline]
            fn pow(self, exponent: Self) -> Self {
                match self {
                    // A unit base is its own inverse, so it stays a unit
                    // under a negative exponent: only the exponent's
                    // parity counts.
                    1 => 1,
                    -1 if exponent % 2 == 0 => 1,
                    -1 => -1,
                    // `1 / self^-exponent` truncates to zero for any other
                    // nonzero base; `0` has no inverse and stays `0`.
                    _ if exponent < 0 => 0,
                    // Wrapping multiplication produces the same bits whether
                    // the base reads as signed or unsigned, and the
                    // exponent is non-negative here, so it converts as is.
                    _ => Pow::pow(self as $u, exponent as $u) as $t,
                }
            }
        }
    )*};
}

macro_rules! float_ops {
    ($($t:ty),*) => {$(
        impl Pow for OrderedFloat<$t> {
            #[inline]
            fn pow(self, exponent: Self) -> Self {
                OrderedFloat(self.0.powf(exponent.0))
            }
        }
        impl Arith for OrderedFloat<$t> {
            #[inline]
            fn add(self, rhs: Self) -> Self {
                self + rhs
            }
            #[inline]
            fn sub(self, rhs: Self) -> Self {
                self - rhs
            }
            #[inline]
            fn mul(self, rhs: Self) -> Self {
                self * rhs
            }
            #[inline]
            fn div(self, rhs: Self) -> Self {
                self / rhs
            }
            #[inline]
            fn rem(self, rhs: Self) -> Self {
                self % rhs
            }
        }
    )*};
}

macro_rules! integer_arith {
    ($($t:ty),*) => {$(
        impl Arith for $t {
            #[inline]
            fn add(self, rhs: Self) -> Self {
                self.wrapping_add(rhs)
            }
            #[inline]
            fn sub(self, rhs: Self) -> Self {
                self.wrapping_sub(rhs)
            }
            #[inline]
            fn mul(self, rhs: Self) -> Self {
                self.wrapping_mul(rhs)
            }
            #[inline]
            fn div(self, rhs: Self) -> Self {
                self.wrapping_div(rhs)
            }
            #[inline]
            fn rem(self, rhs: Self) -> Self {
                self.wrapping_rem(rhs)
            }
        }
    )*};
}

unsigned_ops!(u8, u16, u32, u64);
signed_ops!(i8 => u8, i16 => u16, i32 => u32, i64 => u64);
float_ops!(f32, f64);
integer_arith!(u8, u16, u32, u64, i8, i16, i32, i64);

/// `x + y`.
#[inline]
pub fn add<T: Arith>(x: T, y: T) -> T {
    x.add(y)
}

/// `x - y`.
#[inline]
pub fn sub<T: Arith>(x: T, y: T) -> T {
    x.sub(y)
}

/// `x * y`.
#[inline]
pub fn mul<T: Arith>(x: T, y: T) -> T {
    x.mul(y)
}

/// `x / y`.
///
/// # Panics
///
/// Panics on an integer `y` of zero.
#[inline]
pub fn div<T: Arith>(x: T, y: T) -> T {
    x.div(y)
}

/// `x % y`.
///
/// # Panics
///
/// Panics on an integer `y` of zero.
#[inline]
pub fn rem<T: Arith>(x: T, y: T) -> T {
    x.rem(y)
}

/// `x bshl count`.
#[inline]
pub fn bshl<T: Shift>(x: T, count: T) -> T {
    x.bshl(count)
}

/// `x bshr count`.
#[inline]
pub fn bshr<T: Shift>(x: T, count: T) -> T {
    x.bshr(count)
}

/// `x bshru count`.
#[inline]
pub fn bshru<T: Shift>(x: T, count: T) -> T {
    x.bshru(count)
}

/// `x ^ exponent`.
#[inline]
pub fn pow<T: Pow>(x: T, exponent: T) -> T {
    x.pow(exponent)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// The count is masked to the width, so a full-width or negative count
    /// shifts by its low bits instead of overflowing.
    #[rstest]
    #[case::plain(1i64, 4, 16)]
    #[case::full_width(1i64, 64, 1)]
    #[case::negative_count(3i64, -1, i64::MIN)]
    #[case::top_bit_out(i64::MIN, 1, 0)]
    fn bshl_masks_the_count(#[case] x: i64, #[case] count: i64, #[case] expected: i64) {
        assert_eq!(bshl(x, count), expected);
    }

    #[rstest]
    #[case::plain(255u8, 4, 15)]
    #[case::full_width(255u8, 8, 255)]
    fn bshr_masks_the_count_per_width(#[case] x: u8, #[case] count: u8, #[case] expected: u8) {
        assert_eq!(bshr(x, count), expected);
    }

    /// `bshr` keeps the sign, `bshru` fills with zeros; they only differ
    /// on a negative signed operand.
    #[rstest]
    #[case::negative(-8i64, 1, -4, 0x7FFF_FFFF_FFFF_FFFC)]
    #[case::positive(8i64, 1, 4, 4)]
    #[case::shift_out(-1i64, 63, -1, 1)]
    fn bshr_extends_sign_and_bshru_fills_zero(
        #[case] x: i64,
        #[case] count: i64,
        #[case] arithmetic: i64,
        #[case] logical: i64,
    ) {
        assert_eq!(bshr(x, count), arithmetic);
        assert_eq!(bshru(x, count), logical);
    }

    #[test]
    fn unsigned_shifts_agree() {
        assert_eq!(bshr(u64::MAX, 1), bshru(u64::MAX, 1));
        assert_eq!(bshr(u64::MAX, 1), u64::MAX >> 1);
    }

    /// Integer power is exact until it wraps, and a negative exponent
    /// truncates to zero as Souffle's double `pow` does.
    #[rstest]
    #[case::small(2i64, 10, 1024)]
    #[case::zero_exponent(7i64, 0, 1)]
    #[case::negative_exponent(2i64, -1, 0)]
    #[case::wraps(2i64, 64, 0)]
    #[case::negative_base(-2i64, 3, -8)]
    fn integer_pow(#[case] x: i64, #[case] exponent: i64, #[case] expected: i64) {
        assert_eq!(pow(x, exponent), expected);
    }

    /// A base of `1` or `-1` stays a unit under a negative exponent: it
    /// does not truncate to zero, and the sign of `-1` follows the
    /// exponent's parity.
    #[rstest]
    #[case::one_negative_exponent(1i64, -1, 1)]
    #[case::minus_one_odd_negative_exponent(-1i64, -1, -1)]
    #[case::minus_one_even_negative_exponent(-1i64, -2, 1)]
    fn unit_base_pow_is_exact(#[case] x: i64, #[case] exponent: i64, #[case] expected: i64) {
        assert_eq!(pow(x, exponent), expected);
    }

    #[test]
    fn unsigned_pow_wraps() {
        assert_eq!(pow(2u8, 8), 0);
        assert_eq!(pow(3u32, 4), 81);
    }

    /// An exponent past the `u32` that `wrapping_pow` takes still wraps
    /// exactly: the result is the true power modulo `2^64`, as Python's
    /// `pow(x, exponent, 2**64)` computes it. Both 32-bit halves of
    /// `u64::MAX` are nonzero.
    #[test]
    fn unsigned_exponent_past_u32_wraps_exactly() {
        assert_eq!(pow(3u64, u64::MAX), 12_297_829_382_473_034_411);
    }

    /// A signed power past `u32` is the unsigned one on the same bits, so a
    /// negative base keeps the sign of an odd exponent.
    #[test]
    fn signed_exponent_past_u32_reads_the_unsigned_power_as_signed() {
        assert_eq!(pow(-3i64, (1 << 32) + 1), -7_473_929_035_676_909_571);
    }

    #[test]
    fn float_pow_is_powf() {
        assert_eq!(
            pow(OrderedFloat(2.0f64), OrderedFloat(0.5)),
            OrderedFloat(2.0f64.sqrt())
        );
        assert_eq!(
            pow(OrderedFloat(2.0f32), OrderedFloat(-1.0)),
            OrderedFloat(0.5)
        );
    }

    /// Integer arithmetic wraps where it overflows, `/` and `%` at
    /// `MIN / -1` included, rather than panicking with overflow checks on.
    // Cases: operation, x, y, expected.
    #[rstest]
    #[case::add(add::<i32> as fn(i32, i32) -> i32, i32::MAX, 1, i32::MIN)]
    #[case::sub(sub::<i32> as fn(i32, i32) -> i32, i32::MIN, 1, i32::MAX)]
    #[case::mul(mul::<i32> as fn(i32, i32) -> i32, i32::MAX, 2, -2)]
    #[case::div(div::<i32> as fn(i32, i32) -> i32, i32::MIN, -1, i32::MIN)]
    #[case::rem(rem::<i32> as fn(i32, i32) -> i32, i32::MIN, -1, 0)]
    fn integer_arithmetic_wraps(
        #[case] operation: fn(i32, i32) -> i32,
        #[case] x: i32,
        #[case] y: i32,
        #[case] expected: i32,
    ) {
        assert_eq!(operation(x, y), expected);
    }

    #[test]
    #[should_panic(expected = "attempt to divide by zero")]
    fn integer_division_by_zero_panics() {
        let _ = div(1i32, 0);
    }

    /// Float arithmetic is IEEE: dividing by zero is infinite, not a panic.
    #[test]
    fn float_division_by_zero_is_infinite() {
        assert_eq!(
            div(OrderedFloat(1.0f64), OrderedFloat(0.0)),
            OrderedFloat(f64::INFINITY)
        );
    }
}
