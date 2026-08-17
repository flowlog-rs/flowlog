//! Decoding one incoming row of delimited text as a slot tuple.
//!
//! A record is [`TextRow`], one line of a file or one `put` command, and
//! a cell of it is text until a column's slot type says what it should be.
//!
//! [`DecodeCell`] holds the per-column rules; the [`Decode`] impls walk a
//! whole row in column order.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

// =============================================================================
// TextRow
// =============================================================================

/// One incoming row, as one line of delimited text: the text readers hand
/// it over with its terminator already stripped and its bytes already UTF-8
/// validated.
#[derive(Debug, Clone, Copy)]
pub struct TextRow<'a> {
    pub(crate) text: &'a str,
    pub(crate) delim: u8,
    /// Line number within the file, counting from 1, or 0 for a record with
    /// no place in one (a `put` tuple).
    pub(crate) position: u64,
}

impl<'a> TextRow<'a> {
    /// Take the next cell out of `rest`, without the whitespace around it,
    /// or refuse a row that ran out before `column`.
    ///
    /// `rest` is the unconsumed remainder, and `None` once the last cell
    /// has been taken, which an empty `&str` cannot mean: a trailing empty
    /// cell is a value.
    ///
    /// Requires a delimiter of one ASCII byte, which the parser establishes,
    /// so no index this hands to a slice can fall inside a character.
    // Hand-written scan rather than `str::split`, and always inlined, for
    // one reason: one call per column makes this the hottest path here, and
    // the `char` pattern costs enough per cell to lose to a plain loop.
    #[inline(always)]
    fn take(&self, rest: &mut Option<&'a str>, column: usize) -> Result<&'a str, RuntimeError> {
        let Some(cell) = *rest else {
            // Running out at `column` means the row held exactly that
            // many cells.
            return Err(RuntimeError::MissingColumn {
                position: self.position,
                column,
                arity: column,
            });
        };
        match cell.as_bytes().iter().position(|&b| b == self.delim) {
            Some(i) => {
                *rest = Some(&cell[i + 1..]);
                Ok(cell[..i].trim())
            }
            None => {
                *rest = None;
                Ok(cell.trim())
            }
        }
    }
}

// =============================================================================
// DecodeCell
// =============================================================================

/// One text cell as one slot value.
pub trait DecodeCell: Sized {
    /// Parse `cell` (already trimmed), or report why it is not this type.
    fn decode_cell(cell: &str, position: u64, column: usize) -> Result<Self, RuntimeError>;
}

/// Every type that spells itself, parsed through `FromStr`, which also
/// range-checks an integer against its width.
macro_rules! decode_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeCell for $ty {
            #[inline]
            fn decode_cell(cell: &str, position: u64, column: usize) -> Result<Self, RuntimeError> {
                cell.parse::<$ty>().map_err(|_| RuntimeError::Malformed {
                    position,
                    column,
                    value: cell.to_owned(),
                    expected: stringify!($ty),
                })
            }
        }
    )+};
}

decode_cell!(i8, i16, i32, i64, u8, u16, u32, u64, bool, f32, f64);

/// A string column with interning off: the cell is already the slot value,
/// so only the owned copy the session needs is left to make.
impl DecodeCell for String {
    #[inline]
    fn decode_cell(cell: &str, _position: u64, _column: usize) -> Result<Self, RuntimeError> {
        Ok(cell.to_owned())
    }
}

/// A string column with interning on: the cell becomes the same [`Spur`]
/// an equal string computed during the run would get, which is what lets a
/// loaded fact join against derived ones.
impl DecodeCell for Spur {
    #[inline]
    fn decode_cell(cell: &str, _position: u64, _column: usize) -> Result<Self, RuntimeError> {
        Ok(intern(cell))
    }
}

/// A float column: the slot wraps, because a differential tuple must be
/// `Ord` and a bare float is not (`NaN`).
macro_rules! decode_float_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeCell for OrderedFloat<$ty> {
            #[inline]
            fn decode_cell(
                cell: &str,
                position: u64,
                column: usize,
            ) -> Result<Self, RuntimeError> {
                <$ty>::decode_cell(cell, position, column).map(OrderedFloat)
            }
        }
    )+};
}

decode_float_cell!(f32, f64);

// =============================================================================
// Decode<TextRow>
// =============================================================================

/// A nullary relation has no columns, so any row decodes as its tuple and
/// the row's own bytes are ignored here.
///
/// They are not meaningless: a nullary row says whether the fact holds,
/// which is the update's multiplicity rather than any column. `Decode`
/// hands back a tuple and cannot express a count, so the two entry points
/// settle it themselves. `load_file` keeps this impl and counts one
/// assertion per line; a nullary relation's generated `load_line` overrides
/// the default instead, reading `True` or `False` as the diff's sign without
/// decoding at all.
impl Decode<TextRow<'_>> for () {
    #[inline]
    fn decode(_row: &TextRow<'_>) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// One [`Decode`] impl per arity, each taking one cell per column in order.
///
/// The tuple's arity is how many cells are asked for. A row holding more is
/// read down to that arity and the rest ignored, which is what lets a wider
/// file load as its leading columns and is what Souffle does too; a row
/// holding fewer is refused at the column it ran out at.
macro_rules! decode_tuple {
    ($(($($f:ident . $i:tt),+))+) => {$(
        impl<$($f: DecodeCell,)+> Decode<TextRow<'_>> for ($($f,)+) {
            #[inline]
            fn decode(row: &TextRow<'_>) -> Result<Self, RuntimeError> {
                let mut rest = Some(row.text);
                Ok(($(
                    $f::decode_cell(row.take(&mut rest, $i)?, row.position, $i)?,
                )+))
            }
        }
    )+};
}

decode_tuple! {
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

    /// A comma-delimited row, read as row 7 of its file.
    fn row(text: &str) -> TextRow<'_> {
        TextRow {
            text,
            delim: b',',
            position: 7,
        }
    }

    /// A row decodes as whatever tuple its relation declares, one cell
    /// per column, trimmed.
    #[test]
    fn a_row_decodes_as_the_declared_tuple() {
        let t: (i32, String, bool) = Decode::decode(&row("42, hello ,true")).expect("row");
        assert_eq!(t, (42, "hello".to_string(), true));
    }

    /// An interned column yields the key an equal string computed during
    /// the run would get, which is what makes the two join.
    #[test]
    fn an_interned_cell_matches_a_computed_string() {
        let t: (Spur, i32) = Decode::decode(&row("alpha,1")).expect("row");
        assert_eq!(t, (intern("alpha"), 1));
    }

    /// A float column decodes into the wrapper a differential tuple needs.
    #[test]
    fn a_float_cell_decodes_wrapped() {
        let t: (OrderedFloat<f32>, OrderedFloat<f64>) =
            Decode::decode(&row("0.5,2.25")).expect("row");
        assert_eq!(t, (OrderedFloat(0.5), OrderedFloat(2.25)));
    }

    /// A cell that does not parse names the position, column, and value.
    #[test]
    fn a_bad_cell_is_refused_with_its_coordinates() {
        let err = <(i32, i32)>::decode(&row("1,x")).expect_err("x is not i32");
        assert!(
            matches!(&err, RuntimeError::Malformed { position: 7, column: 1, value, .. }
                if value == "x"),
            "got: {err}"
        );
    }

    /// A number too wide for its column is refused like any other cell
    /// that is not what the column declares.
    #[test]
    fn a_number_past_its_width_is_refused() {
        let err = <(i8,)>::decode(&row("300")).expect_err("300 does not fit i8");
        assert!(
            matches!(
                &err,
                RuntimeError::Malformed {
                    column: 0,
                    expected: "i8",
                    ..
                }
            ),
            "got: {err}"
        );
    }

    /// A float column reports the float it failed to be, not the wrapper.
    #[test]
    fn a_bad_float_cell_names_the_float_type() {
        let err = <(OrderedFloat<f64>,)>::decode(&row("x")).expect_err("x is not f64");
        assert!(
            matches!(
                &err,
                RuntimeError::Malformed {
                    expected: "f64",
                    ..
                }
            ),
            "got: {err}"
        );
    }

    /// A row with fewer cells than columns is refused at the column it
    /// ran out at, reporting how many it held.
    #[test]
    fn a_short_row_is_refused_where_it_ran_out() {
        let err = <(i32, i32, i32)>::decode(&row("1,2")).expect_err("two cells");
        assert!(
            matches!(
                err,
                RuntimeError::MissingColumn {
                    position: 7,
                    column: 2,
                    arity: 2,
                }
            ),
            "got: {err}"
        );
    }

    /// Cells past the declared arity are ignored, so a wider file loads as
    /// its leading columns. Souffle reads one the same way, which is why
    /// this is tolerance rather than an error.
    #[test]
    fn extra_cells_are_ignored() {
        let t: (i32,) = Decode::decode(&row("1,junk,junk")).expect("row");
        assert_eq!(t, (1,));
    }

    /// An empty trailing cell is a value, not a missing column: it is the
    /// case an empty remainder could not tell apart on its own.
    #[test]
    fn empty_trailing_cell_is_kept() {
        let t: (i32, String) = Decode::decode(&row("1,")).expect("row");
        assert_eq!(t, (1, String::new()));
    }
}
