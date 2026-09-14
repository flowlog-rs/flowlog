//! Converting owned dataflow tuples into ordinary Rust values.
//!
//! [`TypedEncoder`] converts both rows and nested fields. Owned strings move
//! unchanged; interned strings become owned and floats lose their wrappers.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::intern::resolve_out;
use crate::io::output::encode::Encode;

// =============================================================================
// TypedEncoder
// =============================================================================

/// Converts an owned value without cloning its owned fields.
///
/// Only interned strings require a new allocation. Tuple conversions
/// preserve nesting and field order. Interned keys must belong to the runtime
/// interner.
#[derive(Debug)]
pub(in crate::io::output) struct TypedEncoder;

macro_rules! identity {
    ($($ty:ty),+ $(,)?) => {$(
        impl Encode<$ty> for TypedEncoder {
            type Output = $ty;

            #[inline]
            fn encode(&mut self, src: $ty) -> Self::Output {
                src
            }
        }
    )+};
}

identity!((), i8, i16, i32, i64, u8, u16, u32, u64, bool, String);

impl Encode<Spur> for TypedEncoder {
    type Output = String;

    #[inline]
    fn encode(&mut self, src: Spur) -> Self::Output {
        resolve_out(src).to_owned()
    }
}

impl Encode<OrderedFloat<f32>> for TypedEncoder {
    type Output = f32;

    #[inline]
    fn encode(&mut self, src: OrderedFloat<f32>) -> Self::Output {
        src.into_inner()
    }
}

impl Encode<OrderedFloat<f64>> for TypedEncoder {
    type Output = f64;

    #[inline]
    fn encode(&mut self, src: OrderedFloat<f64>) -> Self::Output {
        src.into_inner()
    }
}

macro_rules! encode_tuple {
    ($(($($field:ident . $index:tt),+))+) => {$(
        impl<$($field,)+> Encode<($($field,)+)> for TypedEncoder
        where
            $(TypedEncoder: Encode<$field>,)+
        {
            type Output = ($(<TypedEncoder as Encode<$field>>::Output,)+);

            #[inline]
            fn encode(&mut self, src: ($($field,)+)) -> Self::Output {
                ($(self.encode(src.$index),)+)
            }
        }
    )+};
}

encode_tuple! {
    (F0.0)
    (F0.0, F1.1)
    (F0.0, F1.1, F2.2)
    (F0.0, F1.1, F2.2, F3.3)
    (F0.0, F1.1, F2.2, F3.3, F4.4)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9, F10.10)
    (F0.0, F1.1, F2.2, F3.3, F4.4, F5.5, F6.6, F7.7, F8.8, F9.9, F10.10, F11.11)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rstest::rstest;

    use super::*;
    use crate::intern::intern;
    use crate::io::input::decode::Decode;

    #[rstest]
    #[case::i8(i8::MIN)]
    #[case::i16(i16::MIN)]
    #[case::i32(i32::MIN)]
    #[case::i64(i64::MIN)]
    #[case::u8(u8::MAX)]
    #[case::u16(u16::MAX)]
    #[case::u32(u32::MAX)]
    #[case::u64(u64::MAX)]
    #[case::true_value(true)]
    #[case::false_value(false)]
    fn primitive_values_are_unchanged<T>(#[case] value: T)
    where
        T: Copy + Debug + PartialEq,
        TypedEncoder: Encode<T, Output = T>,
    {
        assert_eq!(TypedEncoder.encode(value), value);
    }

    #[test]
    fn owned_strings_keep_their_allocations_in_nested_tuples() {
        let name = String::from("outer");
        let label = String::from("inner");
        let pointers = (name.as_ptr(), label.as_ptr());

        let row = TypedEncoder.encode((name, (label, OrderedFloat(2.5f64))));

        assert_eq!(row, (String::from("outer"), (String::from("inner"), 2.5)));
        assert_eq!((row.0.as_ptr(), row.1.0.as_ptr()), pointers);
    }

    #[test]
    fn interned_strings_become_independent_owned_values() {
        let key = intern("hello");
        let mut value = TypedEncoder.encode(key);
        value.push('!');
        assert_eq!(value, "hello!");
        assert_eq!(resolve_out(key), "hello");
    }

    #[rstest]
    #[case::negative_zero(-0.0)]
    #[case::finite(2.5)]
    #[case::infinity(f64::INFINITY)]
    #[case::negative_infinity(f64::NEG_INFINITY)]
    #[case::nan(f64::from_bits(0x7ff8_0000_0000_0042))]
    fn float64_conversion_preserves_bits(#[case] value: f64) {
        assert_eq!(
            TypedEncoder.encode(OrderedFloat(value)).to_bits(),
            value.to_bits()
        );
    }

    #[rstest]
    #[case::negative_zero(-0.0)]
    #[case::finite(2.5)]
    #[case::infinity(f32::INFINITY)]
    #[case::negative_infinity(f32::NEG_INFINITY)]
    #[case::nan(f32::from_bits(0x7fc0_0042))]
    fn float32_conversion_preserves_bits(#[case] value: f32) {
        assert_eq!(
            TypedEncoder.encode(OrderedFloat(value)).to_bits(),
            value.to_bits()
        );
    }

    #[test]
    fn nested_rows_roundtrip_through_typed_input() {
        let input = ((7i32, String::from("hello")), (2.5f64,), false);
        let tuple: ((i32, Spur), (OrderedFloat<f64>,), bool) =
            Decode::decode(&input).expect("typed row");
        assert_eq!(
            TypedEncoder.encode(tuple),
            ((7, String::from("hello")), (2.5,), false)
        );
    }

    #[rstest]
    #[case::empty((), ())]
    #[case::single((7,), (7,))]
    #[case::twelve(
        (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
    )]
    fn row_arity_boundaries_preserve_shape<T>(#[case] row: T, #[case] expected: T)
    where
        T: Debug + PartialEq,
        TypedEncoder: Encode<T, Output = T>,
    {
        assert_eq!(TypedEncoder.encode(row), expected);
    }
}
