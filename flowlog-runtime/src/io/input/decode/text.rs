//! Parsing text rows into tuples and standalone booleans.
//!
//! [`TextRow`] carries input text. [`DecodeCell`] parses individual cells;
//! the [`Decode`] implementations handle complete rows.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::error::Position;
use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

// =============================================================================
// TextRow
// =============================================================================

/// One row of delimited text, with its terminator stripped and its bytes
/// already validated as UTF-8.
#[derive(Debug, Clone, Copy)]
pub struct TextRow<'a> {
    pub(crate) text: &'a str,
    pub(crate) delim: u8,
    pub(crate) at: Position,
}

impl<'a> TextRow<'a> {
    /// Take the next cell out of `rest`, without the whitespace around it,
    /// or refuse a row that ran out before `column`.
    ///
    /// `rest` is the unconsumed remainder, and `None` once the last cell
    /// has been taken, which an empty `&str` cannot mean: a trailing empty
    /// cell is a value.
    ///
    /// Requires an ASCII delimiter so cell boundaries cannot split a UTF-8
    /// character.
    // A hand scan rather than `str::split`, always inlined: one call per
    // column makes this the hottest path here, and the `char` pattern
    // costs enough per cell to lose to a plain byte loop.
    #[inline(always)]
    fn take(&self, rest: &mut Option<&'a str>, column: usize) -> Result<&'a str, RuntimeError> {
        let Some(cell) = *rest else {
            return Err(RuntimeError::MissingColumn {
                at: self.at,
                column,
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
    /// Parse `cell`, already trimmed, or report why it is not this type.
    fn decode_cell(cell: &str, at: Position, column: usize) -> Result<Self, RuntimeError>;
}

/// Every type that spells itself, parsed through `FromStr`, which also
/// range-checks an integer against its width.
macro_rules! decode_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeCell for $ty {
            #[inline]
            fn decode_cell(cell: &str, at: Position, column: usize) -> Result<Self, RuntimeError> {
                cell.parse::<$ty>().map_err(|_| RuntimeError::Malformed {
                    at,
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
    fn decode_cell(cell: &str, _at: Position, _column: usize) -> Result<Self, RuntimeError> {
        Ok(cell.to_owned())
    }
}

/// A string column with interning on: the cell becomes the same [`Spur`]
/// an equal string computed during the run would get, which is what lets a
/// loaded fact join against derived ones.
impl DecodeCell for Spur {
    #[inline]
    fn decode_cell(cell: &str, _at: Position, _column: usize) -> Result<Self, RuntimeError> {
        Ok(intern(cell))
    }
}

/// A float column: the slot wraps, because a differential tuple must be
/// `Ord` and a bare float is not (`NaN`).
macro_rules! decode_float_cell {
    ($($ty:ty),+ $(,)?) => {$(
        impl DecodeCell for OrderedFloat<$ty> {
            #[inline]
            fn decode_cell(cell: &str, at: Position, column: usize) -> Result<Self, RuntimeError> {
                <$ty>::decode_cell(cell, at, column).map(OrderedFloat)
            }
        }
    )+};
}

decode_float_cell!(f32, f64);

// =============================================================================
// Decode<TextRow>
// =============================================================================

/// A standalone boolean accepts `True` or `False` in any ASCII case,
/// with surrounding whitespace ignored. The whole row must match.
impl Decode<TextRow<'_>> for bool {
    fn decode(row: &TextRow<'_>) -> Result<Self, RuntimeError> {
        let text = row.text.trim();
        if text.eq_ignore_ascii_case("true") {
            return Ok(true);
        }
        if text.eq_ignore_ascii_case("false") {
            return Ok(false);
        }
        Err(RuntimeError::Malformed {
            at: row.at,
            column: 0,
            value: text.to_owned(),
            expected: "`True` or `False`",
        })
    }
}

/// A nullary tuple asks for no cells, so any row decodes as it: a file
/// row counts as one assertion whatever its bytes. A nullary `put` is
/// decoded as a boolean instead, since its text determines the update's sign.
impl Decode<TextRow<'_>> for () {
    #[inline]
    fn decode(_row: &TextRow<'_>) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// One [`Decode`] impl per arity, each taking one cell per column in order.
///
/// The tuple's arity is how many cells are asked for. A row holding more is
/// read down to that arity and the rest ignored, which lets a wider file
/// load as its leading columns and is what Souffle does too; a row holding
/// fewer is refused at the column it ran out at.
macro_rules! decode_tuple {
    ($(($($f:ident . $i:tt),+))+) => {$(
        impl<$($f: DecodeCell,)+> Decode<TextRow<'_>> for ($($f,)+) {
            #[inline]
            fn decode(row: &TextRow<'_>) -> Result<Self, RuntimeError> {
                let mut rest = Some(row.text);
                Ok(($(
                    $f::decode_cell(row.take(&mut rest, $i)?, row.at, $i)?,
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
    use rstest::rstest;

    use super::*;

    /// A comma-delimited row, read as line 7 of its file.
    fn row(text: &str) -> TextRow<'_> {
        TextRow {
            text,
            delim: b',',
            at: Position::Line(7),
        }
    }

    /// A row decodes as whatever tuple its relation declares, one cell per
    /// column, trimmed.
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

    /// A cell that does not parse names the row, column, and value.
    #[test]
    fn a_bad_cell_is_refused_with_its_coordinates() {
        let err = <(i32, i32)>::decode(&row("1,x")).expect_err("x is not i32");
        assert!(
            matches!(
                &err,
                RuntimeError::Malformed { at: Position::Line(7), column: 1, value, expected: "i32" }
                    if value == "x"
            ),
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

    /// A row with fewer cells than columns is refused at the column it ran
    /// out at.
    #[test]
    fn a_short_row_is_refused_where_it_ran_out() {
        let err = <(i32, i32, i32)>::decode(&row("1,2")).expect_err("two cells");
        assert!(
            matches!(
                err,
                RuntimeError::MissingColumn {
                    at: Position::Line(7),
                    column: 2,
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

    /// A nullary tuple decodes from any row, bytes included, because the
    /// row's meaning is a weight the decoder has no channel for.
    #[test]
    fn a_nullary_tuple_decodes_from_any_row() {
        <()>::decode(&row("")).expect("empty row");
        <()>::decode(&row("anything at all")).expect("non-empty row");
    }

    /// The error's text places the cell for a reader of stderr.
    #[test]
    fn a_refusal_renders_its_place() {
        let err = <(i32,)>::decode(&row("x")).expect_err("x is not i32");
        assert_eq!(err.to_string(), "line 7 column 0: \"x\" is not i32");
    }

    #[rstest]
    #[case("True", true)]
    #[case("FALSE", false)]
    #[case(" tRuE ", true)]
    #[case(" false ", false)]
    fn standalone_booleans_accept_case_and_whitespace(#[case] text: &str, #[case] expected: bool) {
        assert_eq!(bool::decode(&row(text)).expect("boolean"), expected);
    }

    #[rstest]
    #[case("")]
    #[case("maybe")]
    #[case("1")]
    #[case("True,junk")]
    #[case("False,")]
    fn standalone_booleans_reject_anything_other_than_a_complete_value(#[case] text: &str) {
        let err = bool::decode(&row(text)).expect_err("not a boolean");
        assert!(matches!(
            err,
            RuntimeError::Malformed {
                at: Position::Line(7),
                column: 0,
                value,
                expected: "`True` or `False`",
            } if value == text
        ));
    }

    #[rstest]
    #[case("True")]
    #[case("FALSE")]
    fn boolean_columns_still_reject_non_lowercase_spellings(#[case] text: &str) {
        let err = <(bool,)>::decode(&row(text)).expect_err("not a lowercase boolean");
        assert!(matches!(
            err,
            RuntimeError::Malformed {
                at: Position::Line(7),
                column: 0,
                value,
                expected: "bool",
            } if value == text
        ));
    }
}
