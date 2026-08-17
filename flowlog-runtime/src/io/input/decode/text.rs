//! Decoding one line of delimited text as a slot tuple.
//!
//! [`Line`] is the record a text reader hands over, [`DecodeCell`] turns
//! one of its cells into a column's slot type, and the [`Decode`] impls
//! walk a whole line in column order.

use lasso::Spur;
use ordered_float::OrderedFloat;

use crate::error::RuntimeError;
use crate::intern::intern;
use crate::io::input::decode::Decode;

// =============================================================================
// Line
// =============================================================================

/// One line of delimited text, as the text readers hand it over: already
/// UTF-8 validated, with the delimiter and the line's position for error
/// reports.
#[derive(Debug, Clone, Copy)]
pub struct Line<'a> {
    pub(crate) text: &'a str,
    pub(crate) delim: u8,
    pub(crate) position: u64,
}

impl<'a> Line<'a> {
    /// Take the next cell out of `rest`, trimmed as the loader has always
    /// done, or refuse a line that ran out before `column`.
    ///
    /// `rest` is the unconsumed remainder, and `None` once the last cell
    /// has been taken, which an empty `&str` cannot mean: a trailing empty
    /// cell is a value. The delimiter is ASCII, so the byte scan cannot
    /// split inside a character.
    ///
    /// Scans bytes rather than `str::split`: the `char` pattern costs
    /// enough per cell to lose against a hand-written loader, and the
    /// remainder is the only state either way. Always inlined for the
    /// same reason, one call per column being the hottest path here.
    #[inline(always)]
    fn take(&self, rest: &mut Option<&'a str>, column: usize) -> Result<&'a str, RuntimeError> {
        let Some(cell) = *rest else {
            // Running out at `column` means the line held exactly that
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

/// Every column type that arrives as its own text spelling parses through
/// `FromStr`, which also range-checks an integer against its width.
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

/// A string column with interning off: the cell is already the slot's
/// spelling, and only the copy the session needs is left.
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
/// `Ord` and a bare float is not (`NaN`). Refusals come from the inner
/// parse, so a bad cell is reported as the float it failed to be rather
/// than as the wrapper.
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
// Decode<Line>
// =============================================================================

/// A nullary relation has no columns, so any line decodes as its tuple.
///
/// What the line says is still meaningful, but it sets the fact's
/// multiplicity rather than its contents: `True` asserts, `False`
/// retracts. A decoder returns a tuple and cannot express a sign, so that
/// reading belongs to the relation's `put`, not here.
impl Decode<Line<'_>> for () {
    #[inline]
    fn decode(_line: &Line<'_>) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

/// A line decodes as a whole slot tuple, one cell per position in column
/// order.
///
/// The tuple's arity is how many cells are asked for: a longer line keeps
/// the extra cells, as the loader has always done, and a shorter one is
/// refused at the column it ran out at.
macro_rules! decode_line {
    ($(($($f:ident . $i:tt),+))+) => {$(
        impl<$($f: DecodeCell,)+> Decode<Line<'_>> for ($($f,)+) {
            #[inline]
            fn decode(line: &Line<'_>) -> Result<Self, RuntimeError> {
                let mut rest = Some(line.text);
                Ok(($(
                    $f::decode_cell(line.take(&mut rest, $i)?, line.position, $i)?,
                )+))
            }
        }
    )+};
}

decode_line! {
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

    /// A comma-delimited line, read for row 7.
    fn line(text: &str) -> Line<'_> {
        Line {
            text,
            delim: b',',
            position: 7,
        }
    }

    /// A line decodes as whatever tuple its relation declares, one cell
    /// per column, trimmed.
    #[test]
    fn a_line_decodes_as_the_declared_tuple() {
        let t: (i32, String, bool) = Decode::decode(&line("42, hello ,true")).expect("row");
        assert_eq!(t, (42, "hello".to_string(), true));
    }

    /// An interned column yields the key an equal string computed during
    /// the run would get, which is what makes the two join.
    #[test]
    fn an_interned_cell_matches_a_computed_string() {
        let t: (Spur, i32) = Decode::decode(&line("alpha,1")).expect("row");
        assert_eq!(t, (intern("alpha"), 1));
    }

    /// A float column decodes into the wrapper a differential tuple needs.
    #[test]
    fn a_float_cell_decodes_wrapped() {
        let t: (OrderedFloat<f32>, OrderedFloat<f64>) =
            Decode::decode(&line("0.5,2.25")).expect("row");
        assert_eq!(t, (OrderedFloat(0.5), OrderedFloat(2.25)));
    }

    /// A cell that does not parse names the position, column, and value.
    #[test]
    fn a_bad_cell_is_refused_with_its_coordinates() {
        let err = <(i32, i32)>::decode(&line("1,x")).expect_err("x is not i32");
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
        let err = <(i8,)>::decode(&line("300")).expect_err("300 does not fit i8");
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
        let err = <(OrderedFloat<f64>,)>::decode(&line("x")).expect_err("x is not f64");
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

    /// A line with fewer cells than columns is refused at the column it
    /// ran out at, reporting how many it held.
    #[test]
    fn a_short_line_is_refused_where_it_ran_out() {
        let err = <(i32, i32, i32)>::decode(&line("1,2")).expect_err("two cells");
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

    /// Cells past the declared arity are ignored, as the loader has
    /// always done.
    #[test]
    fn extra_cells_are_ignored() {
        let t: (i32,) = Decode::decode(&line("1,junk,junk")).expect("row");
        assert_eq!(t, (1,));
    }

    /// An empty trailing cell is a value, not a missing column: it is the
    /// case an empty remainder could not tell apart on its own.
    #[test]
    fn empty_trailing_cell_is_kept() {
        let t: (i32, String) = Decode::decode(&line("1,")).expect("row");
        assert_eq!(t, (1, String::new()));
    }
}
