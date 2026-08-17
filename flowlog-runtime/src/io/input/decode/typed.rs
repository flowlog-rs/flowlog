//! Converting one already-typed record into a slot tuple.
//!
//! The typed source's whole conversion: a record is one tuple a host
//! program built (`insert_edge(vec![(7, "a".to_string())])`), so its types
//! were checked when that program compiled and only two fields change
//! shape on the way in: a `String` interns where the slot is a `Spur`,
//! and a float wraps. [`DecodeField`] is the per-position rule; the
//! [`Decode`] impls apply it across every arity.
//!
//! Nothing here can refuse a record today, because a well-typed value has
//! no invalid state. The fallible [`Decode`] face is kept anyway, so a
//! conversion that ever can fail reports like any other source instead of
//! being trusted.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

/// Build one slot field from the record field at the same position.
///
/// Implemented per `(record field, slot field)` pair, so the pairing is
/// checked when the generated crate compiles: a program whose `Rows` says
/// `String` where its `Tuple` says `i32` does not build, and the values
/// arriving at run time have nothing left to prove.
///
/// The pairs, exhaustively:
///
/// | record field       | slot field         | rule            |
/// |--------------------|--------------------|-----------------|
/// | integers, `bool`   | the same type      | copy            |
/// | `String`           | `Spur`             | intern          |
/// | `String`           | `String`           | clone           |
/// | `f32` / `f64`      | `OrderedFloat<_>`  | wrap            |
pub trait DecodeField<F> {
    fn decode_field(field: &F) -> Self;
}

/// The identity, for a field whose slot type is its own: `7i32` is
/// already the slot value. `String` to `String` is the interning-off
/// spelling of a string column, and the one clone here: the record is
/// borrowed, and the session needs an owned value.
macro_rules! identity_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeField<$ty> for $ty {
            #[inline]
            fn decode_field(field: &$ty) -> Self {
                field.clone()
            }
        }
    )+};
}

identity_field!(i8, i16, i32, i64, u8, u16, u32, u64, bool, String);

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
macro_rules! float_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeField<$ty> for OrderedFloat<$ty> {
            #[inline]
            fn decode_field(field: &$ty) -> Self {
                OrderedFloat(*field)
            }
        }
    )+};
}

float_field!(f32, f64);

/// A nullary relation's host row carries no fields either.
impl Decode<()> for () {
    #[inline]
    fn decode(_record: &()) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// A record converts per position, which also covers a nested tuple
/// column: its slot is itself a tuple, so these impls recurse. Arity 2
/// expands to:
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
/// so `(i32, Spur)::decode(&(7, "a".to_string()))` is
/// `Ok((7, intern("a")))`.
macro_rules! decode_record {
    ($(($($s:ident . $f:ident . $i:tt),+))+) => {$(
        // The per-position body lives on `DecodeField`, so a tuple is
        // itself a field: that is what makes a nested tuple column
        // recurse with no extra rule.
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

decode_record! {
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
