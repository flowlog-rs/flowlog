//! Converting a slot tuple back into the host tuple a caller reads out.
//!
//! The reverse of [`typed`](crate::io::input::decode::typed), field for field: a
//! `Spur` resolves to the `String` it interned, a float unwraps, and
//! everything else is already its own host type. [`EncodeField`] is the
//! per-position rule; the [`Encode`] impls apply it across every arity.
//!
//! A host tuple type is a plain tuple alias, so these generic impls cover
//! every relation and no program generates a conversion of its own.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::intern::resolve_out;
use crate::io::output::encode::Encode;

/// Build the host field at one position from the slot field there.
///
/// Implemented per `(slot field, host field)` pair, so the pairing is
/// checked when the generated crate compiles.
///
/// The pairs, exhaustively:
///
/// | slot field         | host field    | rule    |
/// |--------------------|---------------|---------|
/// | integers, `bool`   | the same type | move    |
/// | `String`           | `String`      | move    |
/// | `Spur`             | `String`      | resolve |
/// | `OrderedFloat<_>`  | `f32` / `f64` | unwrap  |
pub trait EncodeField<U> {
    /// Convert this slot field to its host form.
    fn encode_field(self) -> U;
}

/// The identity, for a field whose host type is its own. Takes `self` by
/// value, so a `String` slot moves out rather than being copied.
macro_rules! identity_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField<$ty> for $ty {
            #[inline]
            fn encode_field(self) -> $ty {
                self
            }
        }
    )+};
}

identity_field!(i8, i16, i32, i64, u8, u16, u32, u64, bool, String);

/// A string column under interning: the key resolves through the flat
/// snapshot rather than the concurrent map, because a drain runs after the
/// dataflow has stopped interning.
impl EncodeField<String> for Spur {
    #[inline]
    fn encode_field(self) -> String {
        resolve_out(self).to_string()
    }
}

/// A float column: the slot wraps so a differential tuple can be `Ord`,
/// and the host sees the bare float again.
macro_rules! float_field {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeField<$ty> for OrderedFloat<$ty> {
            #[inline]
            fn encode_field(self) -> $ty {
                self.into_inner()
            }
        }
    )+};
}

float_field!(f32, f64);

/// A slot tuple converts per position, and pushes as one host tuple.
///
/// The per-position body lives on [`EncodeField`], so a tuple is itself a
/// field: that is what makes a nested tuple column recurse with no extra
/// rule. Arity 2 expands to:
///
/// ```ignore
/// impl<S0: EncodeField<U0>, S1: EncodeField<U1>, U0, U1>
///     Encode<Vec<(U0, U1)>> for (S0, S1)
/// {
///     fn encode(self, dst: &mut Vec<(U0, U1)>) {
///         dst.push((self.0.encode_field(), self.1.encode_field()));
///     }
/// }
/// ```
macro_rules! encode_record {
    ($(($($s:ident . $u:ident . $i:tt),+))+) => {$(
        impl<$($u,)+ $($s: EncodeField<$u>,)+> EncodeField<($($u,)+)> for ($($s,)+) {
            #[inline]
            fn encode_field(self) -> ($($u,)+) {
                ($(self.$i.encode_field(),)+)
            }
        }

        impl<$($u,)+ $($s: EncodeField<$u>,)+> Encode<Vec<($($u,)+)>> for ($($s,)+) {
            #[inline]
            fn encode(self, dst: &mut Vec<($($u,)+)>) {
                dst.push(self.encode_field());
            }
        }
    )+};
}

encode_record! {
    (S0.U0.0)
    (S0.U0.0, S1.U1.1)
    (S0.U0.0, S1.U1.1, S2.U2.2)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6, S7.U7.7)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6, S7.U7.7, S8.U8.8)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6, S7.U7.7, S8.U8.8, S9.U9.9)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6, S7.U7.7, S8.U8.8, S9.U9.9, S10.U10.10)
    (S0.U0.0, S1.U1.1, S2.U2.2, S3.U3.3, S4.U4.4, S5.U5.5, S6.U6.6, S7.U7.7, S8.U8.8, S9.U9.9, S10.U10.10, S11.U11.11)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intern::intern;

    /// A slot tuple converts per position: integers move, the interned key
    /// resolves, the float unwraps.
    #[test]
    fn a_slot_tuple_converts_per_position() {
        let slot = (7i32, intern("hello"), true, OrderedFloat(2.5f64));
        let mut out: Vec<(i32, String, bool, f64)> = Vec::new();
        slot.encode(&mut out);
        assert_eq!(out, vec![(7, "hello".to_string(), true, 2.5)]);
    }

    /// With interning off the string slot is `String` itself, and the same
    /// tuple selects the identity rule instead.
    #[test]
    fn an_uninterned_string_slot_stays_a_string() {
        let mut out: Vec<(i32, String)> = Vec::new();
        (7i32, "hello".to_string()).encode(&mut out);
        assert_eq!(out, vec![(7, "hello".to_string())]);
    }

    /// A nested tuple column converts through the same impls, recursively.
    #[test]
    fn nested_tuple_fields_convert_recursively() {
        let mut out: Vec<((i32, String), u64)> = Vec::new();
        ((1i32, intern("k")), 4u64).encode(&mut out);
        assert_eq!(out, vec![((1, "k".to_string()), 4)]);
    }

    /// Rows accumulate in push order, which is the order the drain feeds
    /// them and therefore the order the host sees.
    #[test]
    fn rows_accumulate_in_push_order() {
        let mut out: Vec<(i32,)> = Vec::new();
        (1i32,).encode(&mut out);
        (2i32,).encode(&mut out);
        assert_eq!(out, vec![(1,), (2,)]);
    }
}
