//! Borrowed text encoding shared by file and stdout writers.
//!
//! [`TextEncoder`] writes a row's columns with the supplied separator.
//! [`EncodeField`] preserves nested tuples. The constant `DEBUG` selects
//! quoted strings and debug floats; plain strings remain verbatim.

use std::fmt;
use std::fmt::Debug;
use std::io;
use std::io::Write;

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::intern::resolve_out;
use crate::io::output::encode::Encode;

// =============================================================================
// TextEncoder
// =============================================================================

/// Writes borrowed columns without a newline or surrounding parentheses.
/// Nullary rows write `True`. Sink errors are returned immediately.
pub(in crate::io::output) struct TextEncoder<'a, W: Write, const DEBUG: bool> {
    out: &'a mut W,
    separator: &'a [u8],
    integers: &'a mut itoa::Buffer,
}

impl<'a, W: Write, const DEBUG: bool> TextEncoder<'a, W, DEBUG> {
    /// Borrows the destination and reusable scratch without allocating.
    #[inline]
    pub(in crate::io::output) fn new(
        out: &'a mut W,
        separator: &'a [u8],
        integers: &'a mut itoa::Buffer,
    ) -> Self {
        Self {
            out,
            separator,
            integers,
        }
    }
}

impl<W: Write + Debug, const DEBUG: bool> Debug for TextEncoder<'_, W, DEBUG> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("TextEncoder")
            .field("out", &self.out)
            .field("separator", &self.separator)
            .finish_non_exhaustive()
    }
}

impl<W: Write, const DEBUG: bool> Encode<&()> for TextEncoder<'_, W, DEBUG> {
    type Output = io::Result<()>;

    #[inline]
    fn encode(&mut self, _src: &()) -> Self::Output {
        self.out.write_all(b"True")
    }
}

// =============================================================================
// EncodeField
// =============================================================================

/// Formats one borrowed field, resolving interned strings only when consumed.
///
/// Interned keys must belong to the runtime interner. Nested tuples preserve
/// their parentheses, comma-space separators, and singleton trailing commas.
trait EncodeField {
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        integers: &mut itoa::Buffer,
    ) -> io::Result<()>;
}

macro_rules! encode_integer {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField for $ty {
            #[inline]
            fn encode_field<const DEBUG: bool, W: Write>(
                &self,
                out: &mut W,
                integers: &mut itoa::Buffer,
            ) -> io::Result<()> {
                out.write_all(integers.format(*self).as_bytes())
            }
        }
    )+};
}

encode_integer!(i8, i16, i32, i64, u8, u16, u32, u64);

impl EncodeField for bool {
    #[inline]
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        _integers: &mut itoa::Buffer,
    ) -> io::Result<()> {
        out.write_all(if *self { b"true" } else { b"false" })
    }
}

impl EncodeField for str {
    #[inline]
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        _integers: &mut itoa::Buffer,
    ) -> io::Result<()> {
        if DEBUG {
            write!(out, "{self:?}")
        } else {
            out.write_all(self.as_bytes())
        }
    }
}

impl EncodeField for String {
    #[inline]
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        integers: &mut itoa::Buffer,
    ) -> io::Result<()> {
        self.as_str().encode_field::<DEBUG, _>(out, integers)
    }
}

impl EncodeField for Spur {
    #[inline]
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        integers: &mut itoa::Buffer,
    ) -> io::Result<()> {
        resolve_out(*self).encode_field::<DEBUG, _>(out, integers)
    }
}

macro_rules! encode_float {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField for OrderedFloat<$ty> {
            #[inline]
            fn encode_field<const DEBUG: bool, W: Write>(
                &self,
                out: &mut W,
                _integers: &mut itoa::Buffer,
            ) -> io::Result<()> {
                if DEBUG {
                    write!(out, "{self:?}")
                } else {
                    write!(out, "{self}")
                }
            }
        }
    )+};
}

encode_float!(f32, f64);

impl EncodeField for () {
    #[inline]
    fn encode_field<const DEBUG: bool, W: Write>(
        &self,
        out: &mut W,
        _integers: &mut itoa::Buffer,
    ) -> io::Result<()> {
        out.write_all(b"()")
    }
}

// =============================================================================
// Tuples
// =============================================================================

macro_rules! encode_tuple {
    (@end $out:ident) => { $out.write_all(b",)") };
    (@end $out:ident $($field:ident),+) => { $out.write_all(b")") };
    ($(($first:ident . $first_index:tt $(, $field:ident . $index:tt)*))+) => {$(
        impl<$first: EncodeField, $($field: EncodeField,)*> EncodeField
            for ($first, $($field,)*)
        {
            #[inline]
            fn encode_field<const DEBUG: bool, W: Write>(
                &self,
                out: &mut W,
                integers: &mut itoa::Buffer,
            ) -> io::Result<()> {
                out.write_all(b"(")?;
                self.$first_index.encode_field::<DEBUG, _>(out, integers)?;
                $(
                    out.write_all(b", ")?;
                    self.$index.encode_field::<DEBUG, _>(out, integers)?;
                )*
                encode_tuple!(@end out $($field),*)
            }
        }

        impl<W: Write, const DEBUG: bool, $first: EncodeField, $($field: EncodeField,)*>
            Encode<&($first, $($field,)*)> for TextEncoder<'_, W, DEBUG>
        {
            type Output = io::Result<()>;

            #[inline]
            fn encode(&mut self, src: &($first, $($field,)*)) -> Self::Output {
                src.$first_index.encode_field::<DEBUG, _>(self.out, self.integers)?;
                $(
                    self.out.write_all(self.separator)?;
                    src.$index.encode_field::<DEBUG, _>(self.out, self.integers)?;
                )*
                Ok(())
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
    use rstest::rstest;

    use super::*;
    use crate::intern::intern;

    #[rstest]
    #[case::nullary((), "True", "True")]
    #[case::single((7,), "7", "7")]
    #[case::nested_empty(((),), "()", "()")]
    #[case::nested_single(((7,),), "(7,)", "(7,)")]
    #[case::twelve(
        (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        "0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11",
        "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11",
    )]
    fn row_arity_preserves_column_boundaries<R>(
        #[case] row: R,
        #[case] plain: &str,
        #[case] debug: &str,
    ) where
        for<'a, 'r> TextEncoder<'a, Vec<u8>, false>: Encode<&'r R, Output = io::Result<()>>,
        for<'a, 'r> TextEncoder<'a, Vec<u8>, true>: Encode<&'r R, Output = io::Result<()>>,
    {
        let mut bytes = Vec::new();
        let mut integers = itoa::Buffer::new();
        TextEncoder::<_, false>::new(&mut bytes, b"\t", &mut integers)
            .encode(&row)
            .expect("plain row");
        assert_eq!(bytes, plain.as_bytes());
        bytes.clear();
        TextEncoder::<_, true>::new(&mut bytes, b", ", &mut integers)
            .encode(&row)
            .expect("debug row");
        assert_eq!(bytes, debug.as_bytes());
    }

    #[test]
    fn text_style_applies_recursively_without_changing_column_order() {
        let row = (
            intern("a\n\""),
            (String::from("\u{03bb}"), OrderedFloat(1.0f64)),
        );
        let mut plain = Vec::new();
        let mut debug = Vec::new();
        let mut integers = itoa::Buffer::new();
        TextEncoder::<_, false>::new(&mut plain, b"|", &mut integers)
            .encode(&row)
            .expect("plain row");
        TextEncoder::<_, true>::new(&mut debug, b", ", &mut integers)
            .encode(&row)
            .expect("debug row");
        assert_eq!(plain, "a\n\"|(\u{03bb}, 1)".as_bytes());
        assert_eq!(debug, "\"a\\n\\\"\", (\"\u{03bb}\", 1.0)".as_bytes());
    }

    #[rstest]
    #[case::i8((i8::MIN, i8::MAX), "(-128, 127)", "(-128, 127)")]
    #[case::i16((i16::MIN, i16::MAX), "(-32768, 32767)", "(-32768, 32767)")]
    #[case::i32((i32::MIN, i32::MAX), "(-2147483648, 2147483647)", "(-2147483648, 2147483647)")]
    #[case::i64(
        (i64::MIN, i64::MAX),
        "(-9223372036854775808, 9223372036854775807)",
        "(-9223372036854775808, 9223372036854775807)",
    )]
    #[case::u8((0u8, u8::MAX), "(0, 255)", "(0, 255)")]
    #[case::u16((0u16, u16::MAX), "(0, 65535)", "(0, 65535)")]
    #[case::u32((0u32, u32::MAX), "(0, 4294967295)", "(0, 4294967295)")]
    #[case::u64((0u64, u64::MAX), "(0, 18446744073709551615)", "(0, 18446744073709551615)")]
    #[case::bool((true, false), "(true, false)", "(true, false)")]
    #[case::f32(
        (OrderedFloat(-0.0f32), OrderedFloat(1.0f32)),
        "(-0, 1)",
        "(-0.0, 1.0)",
    )]
    #[case::f64(
        (OrderedFloat(-0.0f64), OrderedFloat(1.0f64)),
        "(-0, 1)",
        "(-0.0, 1.0)",
    )]
    #[case::non_finite_f32(
        (OrderedFloat(f32::NEG_INFINITY), OrderedFloat(f32::INFINITY), OrderedFloat(f32::NAN)),
        "(-inf, inf, NaN)",
        "(-inf, inf, NaN)",
    )]
    #[case::non_finite_f64(
        (OrderedFloat(f64::NEG_INFINITY), OrderedFloat(f64::INFINITY), OrderedFloat(f64::NAN)),
        "(-inf, inf, NaN)",
        "(-inf, inf, NaN)",
    )]
    #[case::owned_string(
        String::from("a\"\\\n\r\t\0\u{03bb}"),
        "a\"\\\n\r\t\0\u{03bb}",
        "\"a\\\"\\\\\\n\\r\\t\\0\u{03bb}\""
    )]
    #[case::interned_string(
        intern("a\"\\\n\r\t\0\u{03bb}"),
        "a\"\\\n\r\t\0\u{03bb}",
        "\"a\\\"\\\\\\n\\r\\t\\0\u{03bb}\""
    )]
    fn fields_preserve_their_plain_and_debug_spelling<F: EncodeField>(
        #[case] field: F,
        #[case] plain: &str,
        #[case] debug: &str,
    ) {
        let row = (field,);
        let mut bytes = Vec::new();
        let mut integers = itoa::Buffer::new();
        TextEncoder::<_, false>::new(&mut bytes, b"\t", &mut integers)
            .encode(&row)
            .expect("plain field");
        assert_eq!(bytes, plain.as_bytes());
        bytes.clear();
        TextEncoder::<_, true>::new(&mut bytes, b", ", &mut integers)
            .encode(&row)
            .expect("debug field");
        assert_eq!(bytes, debug.as_bytes());
    }
}
