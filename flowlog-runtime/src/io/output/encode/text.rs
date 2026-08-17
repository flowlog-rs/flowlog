//! Encoding a slot tuple as one line of delimited text.
//!
//! [`TextRows`] is the buffer a text writer hands over, [`EncodeCell`]
//! turns one slot value into a column's text, and the [`Encode`] impls walk
//! a whole tuple in column order.
//!
//! The bytes here are a contract, not a preference: they are diffed against
//! `tests/fixtures/*/expected/*.csv`. The unit tests at the bottom pin one
//! case per shape those fixtures cover.

use std::fmt;
use std::fmt::Display;
use std::io::Write as _;

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::intern::resolve_out;
use crate::io::output::encode::Encode;
use crate::txn::Diff;

// =============================================================================
// TextRows
// =============================================================================

/// A run of output rows under construction: the bytes, the integer scratch,
/// and the relation's column delimiter.
///
/// Owns its buffers rather than borrowing them. Holding `&mut Vec<u8>` and
/// `&mut itoa::Buffer` instead costs 9-15% on the hot path, because the
/// extra pointer hop stops the pointer, length, and capacity staying in
/// registers across a row's columns. Owning them is also what keeps `itoa`
/// out of every generated crate.
///
/// One of these per serial writer, or one per lane when a text writer
/// formats in parallel.
pub struct TextRows {
    bytes: Vec<u8>,
    itoa: itoa::Buffer,
    delim: &'static [u8],
}

/// Shows the run, not the integer scratch, whose contents are whatever the
/// last integer column left behind.
impl fmt::Debug for TextRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextRows")
            .field("bytes", &String::from_utf8_lossy(&self.bytes))
            .field("delim", &String::from_utf8_lossy(self.delim))
            .finish_non_exhaustive()
    }
}

impl TextRows {
    /// An empty run whose columns are separated by `delim`.
    ///
    /// The delimiter is a byte string, not a byte: a relation's
    /// `delimiter=` is any decoded string literal, so `"||"` is legal.
    pub fn new(delim: &'static [u8]) -> Self {
        Self {
            bytes: Vec::new(),
            itoa: itoa::Buffer::new(),
            delim,
        }
    }

    /// Everything appended since the last [`clear`](Self::clear).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The column separator this run was opened with.
    pub fn delim(&self) -> &'static [u8] {
        self.delim
    }

    /// Drop the accumulated bytes, keeping the allocation for reuse.
    pub fn clear(&mut self) {
        self.bytes.clear();
    }

    /// Append one whole row: its columns, then the diff column when `diff`
    /// is `Some`, then one newline.
    ///
    /// The caller decides whether a diff column exists, because the row
    /// type cannot say: a nullary relation writes no diff even in
    /// incremental mode, where every other relation writes one.
    #[inline]
    pub fn push<T: Encode<TextRows>>(&mut self, row: T, diff: Option<Diff>) {
        row.encode(self);
        if let Some(diff) = diff {
            self.push_delim();
            // `{:+}`-shaped: itoa renders the '-' itself, so only a
            // non-negative diff needs its sign written.
            if diff >= 0 {
                self.bytes.push(b'+');
            }
            self.push_int(diff);
        }
        self.bytes.push(b'\n');
    }

    /// Append the column delimiter.
    #[inline]
    pub fn push_delim(&mut self) {
        self.bytes.extend_from_slice(self.delim);
    }

    /// Append bytes exactly as given, with no quoting and no escaping.
    ///
    /// A string column containing the delimiter therefore produces a row
    /// that does not read back as the same columns. That is the format
    /// FlowLog writes today and fixtures pin it, so quoting here would be a
    /// silent change to every existing output file.
    #[inline]
    pub fn push_raw(&mut self, raw: &[u8]) {
        self.bytes.extend_from_slice(raw);
    }

    /// Append an integer in its `Display` spelling.
    #[inline]
    pub fn push_int<I: itoa::Integer>(&mut self, value: I) {
        // Split the borrow: `format` holds `itoa` for as long as its result
        // lives, which overlaps the append into `bytes`.
        let Self { bytes, itoa, .. } = self;
        bytes.extend_from_slice(itoa.format(value).as_bytes());
    }

    /// Append a value in its `Display` spelling.
    #[inline]
    pub fn push_display<D: Display>(&mut self, value: D) {
        // Writing to a Vec<u8> through fmt cannot fail.
        let _ = write!(self.bytes, "{value}");
    }
}

// =============================================================================
// EncodeCell
// =============================================================================

/// One slot value as one output column.
pub trait EncodeCell {
    /// Append this value's column text to `dst`.
    fn encode_cell(self, dst: &mut TextRows);
}

/// Every integer column renders as `Display` does, through `itoa`.
macro_rules! encode_int_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeCell for $ty {
            #[inline]
            fn encode_cell(self, dst: &mut TextRows) {
                dst.push_int(self);
            }
        }
    )+};
}

encode_int_cell!(i8, i16, i32, i64, u8, u16, u32, u64);

impl EncodeCell for bool {
    #[inline]
    fn encode_cell(self, dst: &mut TextRows) {
        dst.push_raw(if self { b"true" } else { b"false" });
    }
}

/// A string column with interning off: the slot is already the spelling.
impl EncodeCell for String {
    #[inline]
    fn encode_cell(self, dst: &mut TextRows) {
        dst.push_raw(self.as_bytes());
    }
}

/// A string column with interning on: the key resolves to the spelling it
/// interned, through the flat snapshot rather than the concurrent map,
/// because output runs after the dataflow has stopped interning.
impl EncodeCell for Spur {
    #[inline]
    fn encode_cell(self, dst: &mut TextRows) {
        dst.push_raw(resolve_out(self).as_bytes());
    }
}

/// A float column renders as `Display`, which the wrapper forwards to the
/// inner float. Not `ryu`: it would print `1.0` and `1e21` where the
/// fixtures pin `1` and `1000000000000000000000`.
macro_rules! encode_float_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl EncodeCell for OrderedFloat<$ty> {
            #[inline]
            fn encode_cell(self, dst: &mut TextRows) {
                dst.push_display(self);
            }
        }
    )+};
}

encode_float_cell!(f32, f64);

/// A tuple column renders in FlowLog's own tuple form, `(a, b)`, whose
/// `, ` separator is independent of the relation's column delimiter. A
/// one-field tuple keeps a trailing comma, as the source syntax does.
macro_rules! encode_tuple_cell {
    ($(($($f:ident . $i:tt),+))+) => {$(
        impl<$($f: EncodeCell,)+> EncodeCell for ($($f,)+) {
            #[inline]
            fn encode_cell(self, dst: &mut TextRows) {
                dst.push_raw(b"(");
                encode_tuple_cell!(@fields dst, self, $($i),+);
                dst.push_raw(b")");
            }
        }
    )+};

    (@fields $dst:ident, $self:ident, $first:tt) => {
        $self.$first.encode_cell($dst);
        $dst.push_raw(b",");
    };

    (@fields $dst:ident, $self:ident, $first:tt, $($rest:tt),+) => {
        $self.$first.encode_cell($dst);
        $($dst.push_raw(b", "); $self.$rest.encode_cell($dst);)+
    };
}

encode_tuple_cell! {
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
// Encode<TextRows>
// =============================================================================

/// The nullary presence marker: a derived nullary fact is one `True` line,
/// and never carries a diff column.
impl Encode<TextRows> for () {
    #[inline]
    fn encode(self, dst: &mut TextRows) {
        dst.push_raw(b"True");
    }
}

/// A slot tuple encodes as a whole row, one cell per position in column
/// order, separated by the relation's delimiter.
///
/// A separate trait from [`EncodeCell`] on purpose, because the same Rust
/// type means different bytes in the two positions: `(Spur, Spur)` is
/// `p<delim>q` as a row and `(p, q)` as a column. Never add a blanket
/// `impl<T: EncodeCell> Encode<TextRows> for T`: it would pick the cell
/// form for a one-column row and write `(p,)` where `p` is correct, with
/// nothing to catch it at compile time.
macro_rules! encode_row {
    ($(($($f:ident . $i:tt),+))+) => {$(
        impl<$($f: EncodeCell,)+> Encode<TextRows> for ($($f,)+) {
            #[inline]
            fn encode(self, dst: &mut TextRows) {
                encode_row!(@cells dst, self, $($i),+);
            }
        }
    )+};

    (@cells $dst:ident, $self:ident, $first:tt) => {
        $self.$first.encode_cell($dst);
    };

    (@cells $dst:ident, $self:ident, $first:tt, $($rest:tt),+) => {
        $self.$first.encode_cell($dst);
        $($dst.push_delim(); $self.$rest.encode_cell($dst);)+
    };
}

encode_row! {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intern::intern;

    /// Encode one row into a tab-delimited run and read the bytes back.
    fn row<T: Encode<TextRows>>(value: T, diff: Option<Diff>) -> String {
        let mut rows = TextRows::new(b"\t");
        rows.push(value, diff);
        String::from_utf8(rows.as_bytes().to_vec()).expect("utf-8")
    }

    /// Columns are separated by the relation's delimiter and the row ends
    /// with exactly one newline.
    #[test]
    fn a_row_is_delimited_and_newline_terminated() {
        assert_eq!(row((7i32, true), None), "7\ttrue\n");
    }

    /// A delimiter is any byte string, not a single byte: `delimiter="||"`
    /// is legal and every column boundary carries all of it.
    #[test]
    fn a_multi_byte_delimiter_is_written_whole() {
        let mut rows = TextRows::new(b"||");
        rows.push((1i32, 2i32, 3i32), None);
        assert_eq!(rows.as_bytes(), b"1||2||3\n");
    }

    /// Integers render as `Display` at the extremes of their width.
    #[test]
    fn integers_render_at_their_extremes() {
        assert_eq!(
            row((i64::MIN, u64::MAX), None),
            "-9223372036854775808\t18446744073709551615\n"
        );
        assert_eq!(row((i8::MIN, u8::MAX), None), "-128\t255\n");
    }

    /// Floats render as `Display`, so a whole value keeps no `.0` and a
    /// large one does not become exponential.
    #[test]
    fn floats_render_as_display() {
        let value = (
            OrderedFloat(1.0f64),
            OrderedFloat(1e21f64),
            OrderedFloat(0.5f32),
        );
        assert_eq!(row(value, None), "1\t1000000000000000000000\t0.5\n");
    }

    /// An interned column resolves to the string it interned, so interning
    /// is invisible in the output.
    #[test]
    fn interning_does_not_change_the_bytes() {
        let interned = row((intern("alpha"), 1i32), None);
        let plain = row(("alpha".to_string(), 1i32), None);
        assert_eq!(interned, plain);
        assert_eq!(interned, "alpha\t1\n");
    }

    /// A tuple column takes FlowLog's tuple form, whose `, ` separator is
    /// its own rather than the relation's delimiter.
    #[test]
    fn a_tuple_column_uses_flowlog_tuple_form() {
        assert_eq!(row(((1i32, 2i32), 3i32), None), "(1, 2)\t3\n");
    }

    /// A one-field tuple column keeps its trailing comma, which is what
    /// tells it apart from a bare value.
    #[test]
    fn a_one_field_tuple_column_keeps_its_comma() {
        let mut rows = TextRows::new(b"\t");
        rows.push((intern("p"), (intern("q"),)), None);
        assert_eq!(rows.as_bytes(), b"p\t(q,)\n");
    }

    /// The same Rust type means different bytes as a row and as a column:
    /// a one-column row is bare where a one-field tuple column is wrapped.
    #[test]
    fn a_one_column_row_is_not_a_tuple_column() {
        assert_eq!(row((intern("p"),), None), "p\n");
    }

    /// Tuple columns nest, each level taking the tuple form again.
    #[test]
    fn tuple_columns_nest() {
        assert_eq!(row((intern("p"), (intern("q"),)), None), "p\t(q,)\n");
        assert_eq!(row(((1i32, (2i32, 3i32)),), None), "(1, (2, 3))\n");
    }

    /// The diff column trails the data columns, carrying an explicit sign
    /// when it is not negative.
    #[test]
    fn the_diff_column_is_signed_and_trails() {
        assert_eq!(row((intern("alpha"), 1i32), Some(1)), "alpha\t1\t+1\n");
        assert_eq!(row((intern("alpha"), 1i32), Some(-1)), "alpha\t1\t-1\n");
        assert_eq!(row((0i32,), Some(0)), "0\t+0\n");
    }

    /// A nullary relation is a presence marker, and carries no diff column
    /// even when the caller is draining an incremental epoch.
    #[test]
    fn a_nullary_row_is_true_without_a_diff() {
        assert_eq!(row((), None), "True\n");
    }

    /// Strings are written raw. A column holding the delimiter produces a
    /// row that does not read back as the same columns, which is the
    /// format as it stands rather than an oversight.
    #[test]
    fn strings_are_never_quoted_or_escaped() {
        assert_eq!(
            row(("a\tb".to_string(), ")".to_string()), None),
            "a\tb\t)\n"
        );
    }

    // --- Lines copied from tests/fixtures/datalog-batch/output_all_types ---

    /// Every float shape that fixture's `Floats.csv` holds, including the
    /// two that a shortest-representation formatter would get wrong: a
    /// whole value keeps no `.0`, and a large one does not go exponential.
    /// Negative zero keeps its sign.
    #[test]
    fn floats_match_the_all_types_fixture() {
        let f = |v: f64| row((OrderedFloat(v),), None);
        assert_eq!(f(-2.5), "-2.5\n");
        assert_eq!(f(4.140000000000001), "4.140000000000001\n");
        assert_eq!(f(-0.0), "-0\n");
        assert_eq!(f(0.5), "0.5\n");
        assert_eq!(f(0.1), "0.1\n");
        assert_eq!(f(1e21), "1000000000000000000000\n");
        assert_eq!(f(1.0), "1\n");
    }

    /// The widest negative value of each signed width, from that fixture's
    /// `Ints.csv`.
    #[test]
    fn signed_minima_match_the_all_types_fixture() {
        let value = (i8::MIN, i16::MIN, i32::MIN, i64::MIN);
        assert_eq!(
            row(value, None),
            "-128\t-32768\t-2147483648\t-9223372036854775808\n"
        );
    }

    /// The widest value of each unsigned width, from `UInts.csv`.
    #[test]
    fn unsigned_maxima_match_the_all_types_fixture() {
        let value = (u8::MAX, u16::MAX, u32::MAX, u64::MAX);
        assert_eq!(
            row(value, None),
            "255\t65535\t4294967295\t18446744073709551615\n"
        );
    }

    /// A mixed row from `Mixed.csv`, including a string column holding
    /// non-ASCII text: the bytes go out as they are, neither escaped nor
    /// transcoded.
    #[test]
    fn a_mixed_row_matches_the_all_types_fixture() {
        let plain = (1i32, "alpha".to_string(), true, OrderedFloat(1.5f64));
        assert_eq!(row(plain, None), "1\talpha\ttrue\t1.5\n");

        let wide = "h\u{e9}llo \u{4e16}\u{754c}".to_string();
        let value = (2i32, wide.clone(), false, OrderedFloat(2.0f64));
        assert_eq!(row(value, None), format!("2\t{wide}\tfalse\t2\n"));
    }

    /// Rows accumulate, and `clear` drops them without dropping the
    /// allocation.
    #[test]
    fn rows_accumulate_until_cleared() {
        let mut rows = TextRows::new(b",");
        rows.push((1i32,), None);
        rows.push((2i32,), None);
        assert_eq!(rows.as_bytes(), b"1\n2\n");
        rows.clear();
        assert!(rows.as_bytes().is_empty());
    }
}
