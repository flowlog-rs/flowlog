//! Converting typed rows into dataflow tuples.
//!
//! [`DecodeField`] converts individual fields; the [`Decode`]
//! implementations assemble tuples from those conversions.

use ordered_float::OrderedFloat;

use crate::error::RuntimeError;
use crate::intern::Symbol;
use crate::intern::intern;
use crate::io::input::decode::Decode;
use crate::row::for_each_row;
use crate::row::row_type;
use crate::row::row_value;

// =============================================================================
// DecodeField
// =============================================================================

/// Converts a borrowed input field into its dataflow representation.
///
/// Source and destination types select the conversion. Unsupported pairs
/// are rejected at compile time, not while loading a row.
pub trait DecodeField<Src> {
    fn decode_field(field: &Src) -> Self;
}

macro_rules! copy_fields {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeField<$ty> for $ty {
            #[inline]
            fn decode_field(field: &$ty) -> Self {
                *field
            }
        }
    )+};
}

copy_fields!(i8, i16, i32, i64, u8, u16, u32, u64, bool);

impl DecodeField<String> for String {
    #[inline]
    fn decode_field(field: &String) -> Self {
        field.clone()
    }
}

impl DecodeField<String> for Symbol {
    #[inline]
    fn decode_field(field: &String) -> Self {
        intern(field)
    }
}

impl DecodeField<f32> for OrderedFloat<f32> {
    #[inline]
    fn decode_field(field: &f32) -> Self {
        OrderedFloat(*field)
    }
}

impl DecodeField<f64> for OrderedFloat<f64> {
    #[inline]
    fn decode_field(field: &f64) -> Self {
        OrderedFloat(*field)
    }
}

// =============================================================================
// Tuples
// =============================================================================

impl Decode<()> for () {
    #[inline]
    fn decode(_row: &()) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// Tuple conversions also implement `DecodeField`, allowing tuple-valued
/// columns to nest within rows.
macro_rules! decode_tuple {
    ($([$($row:ident)?] ($($field:ident $source:ident $index:tt),+))+) => {$(
        impl<$($source,)+ $($field: DecodeField<$source>,)+>
            DecodeField<row_type!([$($row)?] $($source),+)> for row_type!([$($row)?] $($field),+)
        {
            #[inline]
            fn decode_field(row: &row_type!([$($row)?] $($source),+)) -> Self {
                row_value!([$($row)?] $($field::decode_field(&row.$index)),+)
            }
        }

        impl<$($source,)+ $($field: DecodeField<$source>,)+>
            Decode<row_type!([$($row)?] $($source),+)> for row_type!([$($row)?] $($field),+)
        {
            #[inline]
            fn decode(row: &row_type!([$($row)?] $($source),+)) -> Result<Self, RuntimeError> {
                Ok(Self::decode_field(row))
            }
        }
    )+};
}

for_each_row!(decode_tuple);

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use rstest::rstest;

    use super::*;
    use crate::row::Row13;

    #[rstest]
    #[case::i8(-7i8)]
    #[case::i16(-7i16)]
    #[case::i32(-7i32)]
    #[case::i64(-7i64)]
    #[case::u8(7u8)]
    #[case::u16(7u16)]
    #[case::u32(7u32)]
    #[case::u64(7u64)]
    #[case::true_value(true)]
    #[case::false_value(false)]
    fn primitive_fields_preserve_their_values<T>(#[case] field: T)
    where
        T: Copy + Debug + PartialEq + DecodeField<T>,
    {
        assert_eq!(T::decode_field(&field), field);
    }

    #[test]
    fn string_fields_produce_an_independent_owned_value() {
        let field = String::from("hello");
        let mut decoded = String::decode_field(&field);
        decoded.push('!');
        assert_eq!(decoded, "hello!");
        assert_eq!(field, "hello");
    }

    #[test]
    fn string_fields_convert_to_interned_keys() {
        let field = String::from("hello");
        assert_eq!(Symbol::decode_field(&field), intern("hello"));
    }

    #[rstest]
    #[case::f32(0.5f32, OrderedFloat(0.5f32))]
    #[case::f64(2.5f64, OrderedFloat(2.5f64))]
    fn float_fields_are_wrapped<T>(#[case] field: T, #[case] expected: OrderedFloat<T>)
    where
        OrderedFloat<T>: Debug + PartialEq + DecodeField<T>,
    {
        assert_eq!(OrderedFloat::<T>::decode_field(&field), expected);
    }

    #[test]
    fn tuple_fields_convert_in_position_order() {
        let row = (7i32, String::from("hello"), true, 2.5f64);
        let tuple: (i32, Symbol, bool, OrderedFloat<f64>) = Decode::decode(&row).expect("row");
        assert_eq!(tuple, (7, intern("hello"), true, OrderedFloat(2.5)));
    }

    #[test]
    fn nested_tuple_fields_convert_recursively() {
        let row = ((1i32, String::from("k")), 4u64);
        let tuple: ((i32, Symbol), u64) = Decode::decode(&row).expect("row");
        assert_eq!(tuple, ((1, intern("k")), 4));
    }

    #[rstest]
    #[case::empty((), ())]
    #[case::single_column((7,), (7,))]
    #[case::twelve_columns(
        (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
    )]
    #[case::thirteen_columns(
        Row13(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        Row13(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    )]
    fn row_arity_boundaries_decode<T>(#[case] row: T, #[case] expected: T)
    where
        T: Debug + PartialEq + Decode<T>,
    {
        assert_eq!(T::decode(&row).expect("row"), expected);
    }
}
