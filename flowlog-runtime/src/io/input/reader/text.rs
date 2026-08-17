//! Delimited text files, split across workers by byte range.

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::marker::PhantomData;
use std::path::Path;

use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::Line;
use crate::io::input::reader::Reader;
use crate::io::spec::Format;
use crate::io::spec::InputSpec;

/// Open a byte-range slice of `path` for worker `index` out of `peers`.
///
/// Returns `Some((reader, bytes_to_read))` on success. The reader is
/// pre-seeked to the start of the worker's range (aligned to the next
/// line boundary for non-zero workers). The caller should read up to
/// `bytes_to_read` bytes, stopping at the first complete line beyond
/// that budget.
///
/// Returns `None` on I/O error (logged to stderr).
pub(crate) fn byte_range_reader(
    path: &Path,
    index: usize,
    peers: usize,
) -> Option<(BufReader<File>, u64)> {
    let mut file = File::open(path)
        .inspect_err(|e| {
            eprintln!(
                "[flowlog-runtime::io] failed to open {}: {e}",
                path.display()
            );
        })
        .ok()?;

    let file_size = file
        .metadata()
        .inspect_err(|e| {
            eprintln!(
                "[flowlog-runtime::io] failed to stat {}: {e}",
                path.display()
            );
        })
        .ok()?
        .len();

    let chunk = file_size / peers as u64;
    let start = chunk * index as u64;
    let end = if index == peers - 1 {
        file_size
    } else {
        chunk * (index + 1) as u64
    };

    // Nothing to read for this worker.
    if start >= end {
        return Some((BufReader::new(file), 0));
    }

    // Any worker whose range begins at byte 0 reads from the start with no
    // alignment skip; there's no previous byte to peek at. Worker 0 always
    // hits this; others hit it when `chunk == 0` (peers > file_size), which
    // puts the whole file on the last worker.
    if start == 0 {
        return Some((BufReader::new(file), end));
    }

    // Non-zero start: seek to `start - 1` and peek the byte just before our
    // range. If it's a newline we're on a line boundary; otherwise skip the
    // rest of the partial line.
    if file.seek(SeekFrom::Start(start - 1)).is_err() {
        return Some((BufReader::new(file), 0));
    }

    let mut reader = BufReader::new(file);
    let mut peek = [0u8; 1];
    if reader.read_exact(&mut peek).is_err() {
        return Some((reader, 0));
    }

    if peek[0] == b'\n' {
        // Exactly on a line boundary.
        return Some((reader, end - start));
    }

    // Mid-line: skip the rest of this partial line.
    let mut discard = Vec::new();
    let skipped = reader.read_until(b'\n', &mut discard).unwrap_or(0);
    Some((reader, (end - start).saturating_sub(skipped as u64)))
}

// =============================================================================
// TextReader
// =============================================================================

/// A worker's cursor over one byte range of a delimited text file.
#[derive(Debug)]
pub(crate) struct TextReader<T> {
    reader: BufReader<File>,
    delim: u8,
    byte_budget: u64,
    bytes_consumed: u64,
    line: Vec<u8>,
    slot: PhantomData<T>,
    line_number: u64,
}

impl<'src, T: for<'l> Decode<Line<'l>>> Reader<'src, T> for TextReader<T> {
    type Source = Path;

    /// A text scan interns strings as it reads, so under `uses_ord` it
    /// collapses to worker 0 per [`Reader`]'s rule, decided before any
    /// file open, so a missing input is attempted and reported once.
    ///
    /// A file that cannot be opened keeps the text path's legacy contract:
    /// reported to stderr and loaded as empty (`Ok(None)`), never an error.
    fn open(spec: &InputSpec<'src>) -> Result<Option<Self>, RuntimeError> {
        let Format::Text { delim, has_header } = spec.rel.format;
        let (peers, index) = if spec.rel.uses_ord {
            if spec.index != 0 {
                return Ok(None);
            }
            (1, 0)
        } else {
            (spec.peers, spec.index)
        };

        let Some((reader, byte_budget)) = byte_range_reader(spec.source, index, peers) else {
            return Ok(None);
        };
        let mut rows = Self {
            reader,
            slot: PhantomData,
            delim,
            byte_budget,
            bytes_consumed: 0,
            line: Vec::with_capacity(256),
            line_number: 0,
        };
        // Only worker 0's range starts at the top of the file, so only it
        // sees the header. An error here keeps the legacy empty-load.
        if has_header && index == 0 && rows.read_line().is_err() {
            return Ok(None);
        }
        Ok(Some(rows))
    }

    /// The next row in this worker's range, or `None` at its end.
    ///
    /// Blank lines are skipped rather than yielded as empty rows. An error
    /// is fatal: the cursor makes no forward-progress promise after one.
    // One call site per monomorphized drive loop: inlining puts the row's
    // construction and the accessors' shape match in one function, so the
    // per-row dispatch constant-folds away.
    #[inline]
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        while self.bytes_consumed < self.byte_budget {
            if !self.read_line()? {
                return Ok(None);
            }
            if self.line.is_empty() {
                continue;
            }

            // Validated once per line, so no cell re-checks it. A line
            // that is not UTF-8 is a corrupt file rather than a bad cell,
            // and stops the load like any other cursor error.
            let Ok(line) = std::str::from_utf8(&self.line) else {
                return Err(RuntimeError::Malformed {
                    position: self.line_number,
                    column: 0,
                    value: String::from_utf8_lossy(&self.line).into_owned(),
                    expected: "UTF-8",
                });
            };

            return Ok(Some(T::decode(&Line {
                text: line,
                delim: self.delim,
                position: self.line_number,
            })));
        }
        Ok(None)
    }
}

impl<T> TextReader<T> {
    /// Read one line into `self.line`, stripped of its terminator.
    ///
    /// Returns `false` at end of input.
    fn read_line(&mut self) -> Result<bool, RuntimeError> {
        self.line.clear();
        let read = self.reader.read_until(b'\n', &mut self.line)?;
        if read == 0 {
            return Ok(false);
        }
        self.bytes_consumed += read as u64;
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::io::spec::RelationSpec;
    use crate::io::spec::ShardKey;

    static R: RelationSpec = RelationSpec {
        name: "R",
        arity: 2,
        delim: b'\t',
        format: Format::Text {
            delim: b'\t',
            has_header: false,
        },
        shard: ShardKey::Str,
        uses_ord: false,
    };

    static R_HEADER: RelationSpec = RelationSpec {
        name: "R",
        arity: 2,
        delim: b'\t',
        format: Format::Text {
            delim: b'\t',
            has_header: true,
        },
        shard: ShardKey::Str,
        uses_ord: false,
    };

    /// Spec over `path` for worker `index` of `peers`, tab-delimited.
    fn spec(path: &Path, has_header: bool, peers: usize, index: usize) -> InputSpec<'_> {
        InputSpec {
            rel: if has_header { &R_HEADER } else { &R },
            source: path,
            peers,
            index,
        }
    }

    fn rows_over(content: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("rows.tsv");
        std::fs::File::create(&path)
            .expect("create")
            .write_all(content.as_bytes())
            .expect("write");
        (dir, path)
    }

    /// Every (first-cell, second-cell) pair one worker's share yields.
    fn read(path: &Path, has_header: bool, peers: usize, index: usize) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let Some(mut r) =
            <TextReader<(String, String)>>::open(&spec(path, has_header, peers, index))
                .expect("open")
        else {
            return out;
        };
        while let Some(row) = r.next().expect("cursor") {
            out.push(row.expect("row"));
        }
        out
    }

    /// Every line becomes a row, split on the delimiter.
    #[test]
    fn each_line_becomes_a_row() {
        let (_dir, path) = rows_over("a\t1\nb\t2\n");
        assert_eq!(
            read(&path, false, 1, 0),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
    }

    /// Blank lines are skipped rather than yielded as empty rows.
    #[test]
    fn blank_lines_are_skipped() {
        let (_dir, path) = rows_over("a\t1\n\nb\t2\n");
        assert_eq!(read(&path, false, 1, 0).len(), 2);
    }

    /// The final line is read even without a trailing newline.
    #[test]
    fn final_line_without_newline_is_read() {
        let (_dir, path) = rows_over("a\t1\nb\t2");
        assert_eq!(read(&path, false, 1, 0).len(), 2);
    }

    /// A carriage return is stripped with the newline.
    #[test]
    fn carriage_return_is_stripped() {
        let (_dir, path) = rows_over("a\t1\r\n");
        assert_eq!(read(&path, false, 1, 0), vec![("a".into(), "1".into())]);
    }

    /// Splitting on the delimiter keeps empty cells, which a relation may
    /// legitimately store.
    #[test]
    fn empty_cell_between_delimiters_is_kept() {
        let (_dir, path) = rows_over("a\t\n");
        assert_eq!(read(&path, false, 1, 0), vec![("a".into(), String::new())]);
    }

    /// A declared header line is skipped, and only by worker 0, whose
    /// range is the only one that starts at the top of the file.
    #[test]
    fn header_line_is_skipped_when_declared() {
        let (_dir, path) = rows_over("x\ty\na\t1\n");
        assert_eq!(read(&path, true, 1, 0), vec![("a".into(), "1".into())]);
    }

    /// The workers' byte ranges together read every line exactly once.
    #[test]
    fn workers_together_read_every_line_once() {
        let content: String = (0..200).map(|i| format!("r{i}\t{i}\n")).collect();
        let (_dir, path) = rows_over(&content);

        let mut seen = Vec::new();
        for index in 0..4 {
            seen.extend(read(&path, false, 4, index).into_iter().map(|(a, _)| a));
        }
        seen.sort();
        let mut expected: Vec<String> = (0..200).map(|i| format!("r{i}")).collect();
        expected.sort();
        assert_eq!(seen, expected);
    }
}
