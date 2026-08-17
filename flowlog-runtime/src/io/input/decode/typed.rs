//! Converting one already-typed record into a slot tuple.
//!
//! A record is a tuple a host program built, so its types were fixed when
//! that program compiled: `insert_edge(vec![(7, "a".to_string())])`. Only
//! a field's shape changes on the way in, never whether it is valid.
//!
//! [`DecodeField`] holds the per-position rules; the [`Decode`] impls apply
//! them across every arity.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

/// Build one slot field from the record field at the same position.
///
/// One impl per legal pair, so the compiler checks the pairing: a program
/// whose `Rows` says `String` where its `Tuple` says `i32` does not build.
///
/// | record field     | slot field        | rule   |
/// |------------------|-------------------|--------|
/// | integers, `bool` | the same type     | copy   |
/// | `String`         | `Spur`            | intern |
/// | `String`         | `String`          | clone  |
/// | `f32` / `f64`    | `OrderedFloat<_>` | wrap   |
pub trait DecodeField<F> {
    fn decode_field(field: &F) -> Self;
}

/// The pairs whose slot type is the record type, which pass through
/// unchanged: `7i32` is already the slot value.
///
/// `String` is here because a string column spells itself that way with
/// interning off.
macro_rules! decode_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeField<$ty> for $ty {
            #[inline]
            fn decode_field(field: &$ty) -> Self {
                field.clone()
            }
        }
    )+};
}

decode_field!(i8, i16, i32, i64, u8, u16, u32, u64, bool, String);

/// A string column under interning: `"a"` becomes the same [`Spur`] an
/// equal string computed during the run would get, which is what lets a
/// host-supplied fact join against derived ones.
impl DecodeField<String> for Spur {
    #[inline]
    fn decode_field(field: &String) -> Self {
        intern(field)
    }
}

/// A float column: the slot wraps, because a differential tuple must be
/// `Ord` and a bare float is not (`NaN`).
macro_rules! decode_float_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeField<$ty> for OrderedFloat<$ty> {
            #[inline]
            fn decode_field(field: &$ty) -> Self {
                OrderedFloat(*field)
            }
        }
    )+};
}

decode_float_field!(f32, f64);

/// A nullary relation has no columns, so its host row is the empty tuple.
impl Decode<()> for () {
    #[inline]
    fn decode(_record: &()) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// One [`Decode`] impl per arity, and the matching [`DecodeField`] impl so
/// that a tuple is itself a field.
///
/// For example, Arity 2 expands to:
///
/// ```ignore
/// impl<F0, F1, S0: DecodeField<F0>, S1: DecodeField<F1>>
///     Decode<(F0, F1)> for (S0, S1)
/// {
///     fn decode(record: &(F0, F1)) -> Result<Self, RuntimeError> {
///         Ok((S0::decode_field(&record.0), S1::decode_field(&record.1)))
///     }
/// }
/// ```
///
/// so `<(i32, Spur)>::decode(&(7, "a".to_string()))` is
/// `Ok((7, intern("a")))`.
macro_rules! decode_tuple {
    ($(($($s:ident . $f:ident . $i:tt),+))+) => {$(
        impl<$($f,)+ $($s: DecodeField<$f>,)+> DecodeField<($($f,)+)> for ($($s,)+) {
            #[inline]
            fn decode_field(record: &($($f,)+)) -> Self {
                ($($s::decode_field(&record.$i),)+)
            }
        }

        impl<$($f,)+ $($s: DecodeField<$f>,)+> Decode<($($f,)+)> for ($($s,)+) {
            #[inline]
            fn decode(record: &($($f,)+)) -> Result<Self, RuntimeError> {
                Ok(Self::decode_field(record))
            }
        }
    )+};
}

decode_tuple! {
    (S0.F0.0)
    (S0.F0.0, S1.F1.1)
    (S0.F0.0, S1.F1.1, S2.F2.2)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6, S7.F7.7)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6, S7.F7.7, S8.F8.8)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6, S7.F7.7, S8.F8.8, S9.F9.9)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6, S7.F7.7, S8.F8.8, S9.F9.9, S10.F10.10)
    (S0.F0.0, S1.F1.1, S2.F2.2, S3.F3.3, S4.F4.4, S5.F5.5, S6.F6.6, S7.F7.7, S8.F8.8, S9.F9.9, S10.F10.10, S11.F11.11)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intern::intern;

    /// A record converts per position: integers copy, the string interns
    /// to the key an equal computed string would get, the float wraps.
    #[test]
    fn a_record_converts_per_position() {
        let record = (7i32, "hello".to_string(), true, 2.5f64);
        let slot: (i32, Spur, bool, OrderedFloat<f64>) = Decode::decode(&record).expect("typed");
        assert_eq!(slot, (7, intern("hello"), true, OrderedFloat(2.5)));
    }

    /// With interning off the string slot is `String` itself, and the
    /// same record selects the identity rule instead.
    #[test]
    fn uninterned_string_field_stays_a_string() {
        let record = (7i32, "hello".to_string());
        let slot: (i32, String) = Decode::decode(&record).expect("typed");
        assert_eq!(slot, (7, "hello".to_string()));
    }

    /// A nested tuple column converts through the same impls, recursively.
    #[test]
    fn nested_tuple_fields_convert_recursively() {
        let record = ((1i32, "k".to_string()), 4u64);
        let slot: ((i32, Spur), u64) = Decode::decode(&record).expect("typed");
        assert_eq!(slot, ((1, intern("k")), 4));
    }
}
