//! Reading delimited text files in worker-local byte ranges.
//!
//! [`byte_range`] aligns each share to whole lines. [`FileReader`] handles
//! headers and line endings before decoding rows.

use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use crate::error::Position;
use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::TextRow;
use crate::io::input::reader::Reader;

// =============================================================================
// ByteRange
// =============================================================================

/// One worker's byte range of a file, positioned at its first whole line.
#[derive(Debug)]
pub(crate) struct ByteRange {
    pub(crate) reader: BufReader<File>,
    /// Bytes left to read, extending to the next line boundary if needed.
    pub(crate) budget: u64,
    /// True when the range owns byte zero and any header there.
    pub(crate) starts_at_zero: bool,
}

/// Aligns a worker's share of a `len`-byte file to complete lines.
///
/// Ranges are equal `len / peers` chunks with the remainder on the last
/// worker. Each line belongs to the range containing its first byte.
/// When `peers` exceeds `len`, the last worker owns the whole file.
///
/// Invalid worker coordinates return [`io::ErrorKind::InvalidInput`].
pub(crate) fn byte_range(
    mut file: File,
    len: u64,
    index: usize,
    peers: usize,
) -> io::Result<ByteRange> {
    if peers == 0 || index >= peers {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            RuntimeError::InvalidWorker { peers, index },
        ));
    }
    let chunk = len / peers as u64;
    let start = chunk * index as u64;
    let end = if index == peers - 1 {
        len
    } else {
        chunk * (index + 1) as u64
    };
    if start >= end {
        return Ok(ByteRange {
            reader: BufReader::new(file),
            budget: 0,
            starts_at_zero: false,
        });
    }
    if start == 0 {
        return Ok(ByteRange {
            reader: BufReader::new(file),
            budget: end,
            starts_at_zero: true,
        });
    }
    // A line starting before this range belongs to an earlier worker.
    file.seek(SeekFrom::Start(start - 1))?;
    let mut reader = BufReader::new(file);
    let mut before = [0u8; 1];
    reader.read_exact(&mut before)?;
    let skipped = if before[0] == b'\n' {
        0
    } else {
        reader.skip_until(b'\n')? as u64
    };
    Ok(ByteRange {
        reader,
        budget: (end - start).saturating_sub(skipped),
        starts_at_zero: false,
    })
}

// =============================================================================
// FileReader
// =============================================================================

/// Decodes complete lines within a worker's byte range.
///
/// Line numbers count from the first complete line in this range,
/// including a header.
#[derive(Debug)]
pub(crate) struct FileReader {
    reader: BufReader<File>,
    delimiter: u8,
    remaining: u64,
    line: Vec<u8>,
    line_number: u64,
    keep_empty_lines: bool,
}

impl FileReader {
    /// Prepares a worker's share, skipping the header only where the file
    /// begins.
    ///
    /// The file must be positioned at byte zero and `delimiter` must be ASCII.
    /// Empty lines are skipped unless `keep_empty_lines` is set.
    pub(crate) fn open(
        file: File,
        keep_empty_lines: bool,
        delimiter: u8,
        has_header: bool,
        peers: usize,
        index: usize,
    ) -> Result<Self, RuntimeError> {
        let len = file.metadata()?.len();
        let range = byte_range(file, len, index, peers)?;
        let mut reader = Self {
            reader: range.reader,
            delimiter,
            remaining: range.budget,
            line: Vec::new(),
            line_number: 0,
            keep_empty_lines,
        };
        if has_header && range.starts_at_zero {
            reader.read_line()?;
        }
        Ok(reader)
    }

    /// Reads a line without its terminator, stopping when the range is
    /// exhausted.
    fn read_line(&mut self) -> Result<bool, RuntimeError> {
        if self.remaining == 0 {
            return Ok(false);
        }
        self.line.clear();
        let read = self.reader.read_until(b'\n', &mut self.line)?;
        if read == 0 {
            self.remaining = 0;
            return Ok(false);
        }
        self.remaining = self.remaining.saturating_sub(read as u64);
        self.line_number += 1;
        if self.line.last() == Some(&b'\n') {
            self.line.pop();
        }
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        Ok(true)
    }
}

impl<T: for<'l> Decode<TextRow<'l>>> Reader<T> for FileReader {
    #[inline]
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        while self.read_line()? {
            if self.line.is_empty() && !self.keep_empty_lines {
                continue;
            }
            let at = Position::Line(self.line_number);
            let text = std::str::from_utf8(&self.line).map_err(|_| RuntimeError::NotUtf8 { at })?;
            return Ok(Some(T::decode(&TextRow {
                text,
                delim: self.delimiter,
                at,
            })));
        }
        Ok(None)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct Line {
        text: String,
        delimiter: u8,
        at: Position,
    }

    impl Decode<TextRow<'_>> for Line {
        fn decode(row: &TextRow<'_>) -> Result<Self, RuntimeError> {
            Ok(Self {
                text: row.text.to_owned(),
                delimiter: row.delim,
                at: row.at,
            })
        }
    }

    fn text_file(content: &[u8]) -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, content).expect("write");
        (dir, path)
    }

    fn read_lines(
        path: &Path,
        keep_empty_lines: bool,
        has_header: bool,
        peers: usize,
        index: usize,
    ) -> Vec<Line> {
        let file = File::open(path).expect("open file");
        let mut reader = FileReader::open(file, keep_empty_lines, b',', has_header, peers, index)
            .expect("open reader");
        let mut rows = Vec::new();
        while let Some(row) = reader.next().expect("source") {
            rows.push(row.expect("row"));
        }
        rows
    }

    #[rstest]
    #[case::empty(b"", &[])]
    #[case::lines(b"a,1\nb,2\n", &["a,1", "b,2"])]
    #[case::blank_lines(b"\na,1\n\nb,2\n\n", &["a,1", "b,2"])]
    #[case::no_final_newline(b"a,1\nb,2", &["a,1", "b,2"])]
    #[case::crlf(b"a,1\r\nb,2\r\n", &["a,1", "b,2"])]
    #[case::trailing_cr(b"a,1\r", &["a,1"])]
    #[case::blank_crlf(b"\r\na,1\r\n\r\n", &["a,1"])]
    #[case::whitespace(b" a,1 \n \n", &[" a,1 ", " "])]
    #[case::embedded_cr(b"a\rb,1\n", &["a\rb,1"])]
    #[case::empty_cell(b"a,\n", &["a,"])]
    #[case::utf8(b"\xc3\xa9,1\n", &["\u{e9},1"])]
    fn lines_reach_the_decoder_without_terminators(
        #[case] content: &[u8],
        #[case] expected: &[&str],
    ) {
        let (_dir, path) = text_file(content);
        let lines = read_lines(&path, false, false, 1, 0);
        let text: Vec<_> = lines.iter().map(|line| line.text.as_str()).collect();
        assert_eq!(text, expected);
    }

    #[rstest]
    #[case(b"", &[])]
    #[case(b"\n", &[""])]
    #[case(b"\n\n\n", &["", "", ""])]
    #[case(b"\r\n", &[""])]
    fn empty_lines_are_preserved_when_requested(#[case] content: &[u8], #[case] expected: &[&str]) {
        let (_dir, path) = text_file(content);
        let lines = read_lines(&path, true, false, 1, 0);
        let text: Vec<_> = lines.iter().map(|line| line.text.as_str()).collect();
        assert_eq!(text, expected);
    }

    #[rstest]
    #[case(b"", &[])]
    #[case(b"header", &[])]
    #[case(b"header\n", &[])]
    #[case(b"header\na,1\n", &["a,1"])]
    fn a_declared_header_is_not_decoded(#[case] content: &[u8], #[case] expected: &[&str]) {
        let (_dir, path) = text_file(content);
        let lines = read_lines(&path, false, true, 1, 0);
        let text: Vec<_> = lines.iter().map(|line| line.text.as_str()).collect();
        assert_eq!(text, expected);
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(64)]
    fn only_the_range_owning_the_header_skips_it(#[case] peers: usize) {
        let (_dir, path) = text_file(b"header\na,1\nb,2\n");
        let text: Vec<_> = (0..peers)
            .flat_map(|index| read_lines(&path, false, true, peers, index))
            .map(|line| line.text)
            .collect();
        assert_eq!(text, vec!["a,1", "b,2"]);
    }

    #[test]
    fn decoder_receives_the_delimiter_and_line_number_including_header_and_blanks() {
        let (_dir, path) = text_file(b"header\n\na|1\n");
        let file = File::open(&path).expect("open file");
        let mut reader = FileReader::open(file, false, b'|', true, 1, 0).expect("open reader");
        let line = Reader::<Line>::next(&mut reader)
            .expect("source")
            .expect("line")
            .expect("row");
        assert_eq!(line.text, "a|1");
        assert_eq!(line.delimiter, b'|');
        assert_eq!(line.at, Position::Line(3));
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(64)]
    fn workers_together_read_every_line_once_in_order(#[case] peers: usize) {
        let (_dir, path) = text_file(b"a\nlonger\nb\nlast");
        let text: Vec<_> = (0..peers)
            .flat_map(|index| read_lines(&path, false, false, peers, index))
            .map(|line| line.text)
            .collect();
        assert_eq!(text, vec!["a", "longer", "b", "last"]);
    }

    #[rstest]
    #[case(0, &["a", "b"])]
    #[case(1, &["c", "d"])]
    fn a_range_starting_on_a_line_boundary_keeps_that_line(
        #[case] index: usize,
        #[case] expected: &[&str],
    ) {
        let (_dir, path) = text_file(b"a\nb\nc\nd\n");
        let text: Vec<_> = read_lines(&path, false, false, 2, index)
            .into_iter()
            .map(|line| line.text)
            .collect();
        assert_eq!(text, expected);
    }

    #[rstest]
    #[case(0, &["long_long_line"])]
    #[case(1, &[])]
    #[case(2, &["x"])]
    fn a_line_crossing_ranges_belongs_to_the_range_where_it_starts(
        #[case] index: usize,
        #[case] expected: &[&str],
    ) {
        let (_dir, path) = text_file(b"long_long_line\nx\n");
        let text: Vec<_> = read_lines(&path, false, false, 3, index)
            .into_iter()
            .map(|line| line.text)
            .collect();
        assert_eq!(text, expected);
    }

    #[test]
    fn a_line_that_is_not_utf8_fails_the_source() {
        let (_dir, path) = text_file(b"a,1\n\xFF\xFE,2\n");
        let file = File::open(&path).expect("open file");
        let mut reader = FileReader::open(file, false, b',', false, 1, 0).expect("open reader");
        Reader::<Line>::next(&mut reader)
            .expect("source")
            .expect("row")
            .expect("first row");
        let err = Reader::<Line>::next(&mut reader).expect_err("corrupt line");
        assert!(
            matches!(
                err,
                RuntimeError::NotUtf8 {
                    at: Position::Line(2)
                }
            ),
            "got: {err}"
        );
    }

    #[test]
    fn a_rejected_row_does_not_prevent_reading_the_next_row() {
        let (_dir, path) = text_file(b"bad\n2\n");
        let file = File::open(&path).expect("open file");
        let mut reader = FileReader::open(file, false, b',', false, 1, 0).expect("open reader");
        let error = Reader::<(i32,)>::next(&mut reader)
            .expect("source")
            .expect("line")
            .expect_err("rejected row");
        assert!(matches!(
            error,
            RuntimeError::Malformed {
                at: Position::Line(1),
                column: 0,
                value,
                expected: "i32",
            } if value == "bad"
        ));
        let row = Reader::<(i32,)>::next(&mut reader)
            .expect("source")
            .expect("line")
            .expect("row");
        assert_eq!(row, (2,));
        for _ in 0..2 {
            assert!(
                Reader::<(i32,)>::next(&mut reader)
                    .expect("source")
                    .is_none()
            );
        }
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 1)]
    #[case(2, usize::MAX)]
    fn byte_ranges_reject_invalid_worker_coordinates(#[case] peers: usize, #[case] index: usize) {
        let (_dir, path) = text_file(b"a\n");
        let file = File::open(path).expect("open");
        let error = byte_range(file, 2, index, peers).expect_err("invalid worker");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
