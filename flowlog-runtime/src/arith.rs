//! Arithmetic operators whose semantics FlowLog defines itself, as
//! functions generated code calls. An operator lands here when Rust's
//! own spelling does not carry FlowLog's meaning, or has no infix form:
//! a method call on an unsuffixed literal such as `1 bshl n` does not
//! compile, so a function lets the operand type be inferred instead.
//! Operators that Rust's infix form already gets right (`+`, `band`,
//! ...) are emitted inline and do not appear here.
//!
//! Today that is the shifts and `^`. The rules follow Souffle. A shift
//! count is masked to the operand width (`1 bshl 64` is `1 bshl 0` on 64
//! bits) instead of overflowing. `bshr` extends the sign of a signed
//! operand and `bshru` fills with zeros; on unsigned operands they agree.
//! Integer `^` wraps on overflow and yields `0` for a negative exponent,
//! where Souffle's `pow` in `double` would round to zero. Float `^` is
//! `powf`.

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

/// Numeric operand of `^`.
pub trait Pow: Copy {
    /// `self` raised to `exponent`.
    fn pow(self, exponent: Self) -> Self;
}

/// A non-negative integer exponent clamped into `u32`, the range
/// `wrapping_pow` takes. Clamping keeps bases `0` and `1` exact for any
/// exponent, which truncation would not.
fn clamp_exponent<T: TryInto<u32>>(exponent: T) -> u32 {
    exponent.try_into().unwrap_or(u32::MAX)
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
                self.wrapping_pow(clamp_exponent(exponent))
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
                if exponent < 0 {
                    0
                } else {
                    self.wrapping_pow(clamp_exponent(exponent))
                }
            }
        }
    )*};
}

macro_rules! float_ops {
    ($($t:ty),*) => {$(
        impl Pow for $t {
            #[inline]
            fn pow(self, exponent: Self) -> Self {
                self.powf(exponent)
            }
        }
    )*};
}

unsigned_ops!(u8, u16, u32, u64);
signed_ops!(i8 => u8, i16 => u16, i32 => u32, i64 => u64);
float_ops!(f32, f64);

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

    /// Integer power is exact until it wraps, and a negative exponent is
    /// zero as Souffle's double `pow` rounds it.
    #[rstest]
    #[case::small(2i64, 10, 1024)]
    #[case::zero_exponent(7i64, 0, 1)]
    #[case::negative_exponent(2i64, -1, 0)]
    #[case::wraps(2i64, 64, 0)]
    #[case::negative_base(-2i64, 3, -8)]
    fn integer_pow(#[case] x: i64, #[case] exponent: i64, #[case] expected: i64) {
        assert_eq!(pow(x, exponent), expected);
    }

    #[test]
    fn unsigned_pow_wraps() {
        assert_eq!(pow(2u8, 8), 0);
        assert_eq!(pow(3u32, 4), 81);
    }

    /// An exponent past `u32` is clamped rather than truncated, so `1`
    /// and `0` bases stay exact and large bases still wrap.
    #[test]
    fn huge_exponent_is_clamped() {
        assert_eq!(pow(1i64, i64::MAX), 1);
        assert_eq!(pow(0i64, i64::MAX), 0);
        assert_eq!(pow(2i64, i64::MAX), 0);
    }

    #[test]
    fn float_pow_is_powf() {
        assert_eq!(pow(2.0f64, 0.5), 2.0f64.sqrt());
        assert_eq!(pow(2.0f32, -1.0), 0.5);
    }
}
