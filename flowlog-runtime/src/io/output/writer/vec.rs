//! Collecting a relation's rows as host tuples.
//!
//! Two writers, because batch and incremental hand back different types:
//! [`VecWriter`] yields the rows, [`DeltaWriter`] yields each row with its
//! multiplicity.

use crate::error::RuntimeError;
use crate::io::output::encode::Encode;
use crate::io::output::writer::Writer;
use crate::io::spec::OutputSpec;

/// Collects rows as host tuples, dropping multiplicity.
///
/// Nothing here can fail. `open` and `finish` still return `Result`, so a
/// caller drains a file and a `Vec` the same way, which is the bargain
/// [`Decode`](crate::io::input::decode::Decode) already makes for a typed source.
#[derive(Debug)]
pub struct VecWriter<U> {
    rows: Vec<U>,
}

impl<U> VecWriter<U> {
    /// An empty collection, for a caller that has no spec to hand over.
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// The rows collected so far.
    ///
    /// Inherent, like [`TextWriter::commit`](crate::io::TextWriter::commit),
    /// so closing a writer never makes a caller name a row type.
    pub fn into_rows(self) -> Vec<U> {
        self.rows
    }
}

impl<U> Default for VecWriter<U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<U, T: Encode<Vec<U>>> Writer<T> for VecWriter<U> {
    type Out = Vec<U>;

    /// Ignores the spec: a host `Vec` has no path and no delimiter.
    fn open(_spec: &OutputSpec<'_>) -> Result<Self, RuntimeError> {
        Ok(Self::new())
    }

    #[inline]
    fn push(&mut self, row: T, _diff: Option<i32>) {
        row.encode(&mut self.rows);
    }

    fn finish(self) -> Result<Vec<U>, RuntimeError> {
        Ok(self.into_rows())
    }
}

/// Collects rows as host tuples, each paired with its multiplicity.
///
/// Diffs accumulate alongside the rows rather than inside them, so one
/// [`Encode`] impl serves this writer and [`VecWriter`] both. They are
/// zipped once at [`finish`](Writer::finish).
#[derive(Debug)]
pub struct DeltaWriter<U> {
    rows: Vec<U>,
    diffs: Vec<i32>,
}

impl<U> DeltaWriter<U> {
    /// An empty collection, for a caller that has no spec to hand over.
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            diffs: Vec::new(),
        }
    }

    /// The rows collected so far, each with its multiplicity.
    pub fn into_rows(self) -> Vec<(U, i32)> {
        self.rows.into_iter().zip(self.diffs).collect()
    }
}

impl<U> Default for DeltaWriter<U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<U, T: Encode<Vec<U>>> Writer<T> for DeltaWriter<U> {
    type Out = Vec<(U, i32)>;

    /// Ignores the spec: a host `Vec` has no path and no delimiter.
    fn open(_spec: &OutputSpec<'_>) -> Result<Self, RuntimeError> {
        Ok(Self::new())
    }

    /// A row pushed without a diff counts once, which is what a batch
    /// drain means by an unqualified row.
    #[inline]
    fn push(&mut self, row: T, diff: Option<i32>) {
        row.encode(&mut self.rows);
        self.diffs.push(diff.unwrap_or(1));
    }

    fn finish(self) -> Result<Vec<(U, i32)>, RuntimeError> {
        Ok(self.into_rows())
    }
}

#[cfg(test)]
mod tests {
    use lasso::Spur;

    use super::*;
    use crate::intern::intern;

    fn spec() -> OutputSpec<'static> {
        OutputSpec {
            relation: "Out",
            path: "",
            delim: b'\t',
        }
    }

    /// Rows reach the host in push order, converted per position.
    #[test]
    fn rows_collect_in_order() {
        let mut writer: VecWriter<(i32, String)> =
            Writer::<(i32, Spur)>::open(&spec()).expect("open");
        writer.push((1, intern("a")), None);
        writer.push((2, intern("b")), None);
        let rows = Writer::<(i32, Spur)>::finish(writer).expect("finish");
        assert_eq!(rows, vec![(1, "a".to_string()), (2, "b".to_string())]);
    }

    /// The delta writer pairs each row with the diff it was pushed with,
    /// in the same order.
    #[test]
    fn deltas_pair_with_their_rows() {
        let mut writer: DeltaWriter<(i32,)> = Writer::<(i32,)>::open(&spec()).expect("open");
        writer.push((1,), Some(1));
        writer.push((2,), Some(-1));
        let rows = Writer::<(i32,)>::finish(writer).expect("finish");
        assert_eq!(rows, vec![((1,), 1), ((2,), -1)]);
    }

    /// A row with no diff counts once.
    #[test]
    fn a_row_without_a_diff_counts_once() {
        let mut writer: DeltaWriter<(i32,)> = Writer::<(i32,)>::open(&spec()).expect("open");
        writer.push((7,), None);
        assert_eq!(
            Writer::<(i32,)>::finish(writer).expect("finish"),
            vec![((7,), 1)]
        );
    }
}
