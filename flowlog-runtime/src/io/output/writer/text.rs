//! Writing a relation's rows to a delimited text file.

use std::io;
use std::io::BufWriter;
use std::io::Write as _;

use rayon::iter::IndexedParallelIterator as _;
use rayon::iter::IntoParallelRefMutIterator as _;
use rayon::iter::ParallelIterator as _;

use crate::error::RuntimeError;
use crate::io::atomic::AtomicFile;
use crate::io::output::encode::Encode;
use crate::io::output::encode::text::TextRows;
use crate::io::output::writer::Writer;
use crate::io::spec::OutputSpec;
use crate::txn::Diff;

/// One `write` syscall per megabyte rather than per row. A few drains run
/// concurrently in file mode, but 1 MiB each is negligible next to the data.
const OUTPUT_BUFFER_BYTES: usize = 1 << 20;

/// Rows per segment in [`TextWriter::write_parallel`]. A memory bound, not
/// a parallelism knob: peak in-flight memory is roughly
/// `lanes * SEG_ROWS * row_width`, and 8K amortizes per-segment overhead
/// while keeping the transient buffers small.
const SEG_ROWS: usize = 8192;

/// A relation's output file, with the scratch its rows are formatted in.
#[derive(Debug)]
pub struct TextWriter {
    out: BufWriter<AtomicFile>,
    rows: TextRows,
    relation: String,
    path: String,
}

impl TextWriter {
    /// Write a whole relation to the file `spec` names.
    ///
    /// Opens, formats every row, and commits, so the path holds the
    /// complete relation or nothing at all. The hot path for a text sink
    /// with arity > 0 and no `ORDER BY`.
    ///
    /// One call rather than open-push-finish, so a caller does not name
    /// the row type twice for something the rows already determine.
    ///
    /// # Panics
    ///
    /// If a write to the underlying file fails, matching
    /// [`push`](Writer::push).
    pub fn write_file<T, Ts>(
        spec: &OutputSpec<'_>,
        per_worker: Vec<Vec<(T, Ts, Diff)>>,
        with_diff: bool,
    ) -> Result<(), RuntimeError>
    where
        T: Encode<TextRows> + Send,
        Ts: Send,
    {
        <Self as Writer<T>>::open(spec)?.write_parallel(per_worker, with_diff)
    }

    /// Begin the relation's output file.
    ///
    /// Nothing appears at the path until [`commit`](Self::commit), so a run
    /// that dies partway through leaves the previous output intact.
    ///
    /// The parent directory is not created: a missing output directory is
    /// the caller's to prepare, and reporting it is more useful than
    /// silently inventing one.
    ///
    /// Paired with [`commit`](Self::commit) rather than named for the
    /// [`Writer`] methods it backs, so a caller opening a file never has to
    /// name a row type or bring the trait into scope.
    pub fn create(spec: &OutputSpec<'_>) -> Result<Self, RuntimeError> {
        let out = AtomicFile::create(spec.path).map_err(|source| RuntimeError::Output {
            relation: spec.relation.to_owned(),
            path: spec.path.to_owned(),
            source,
        })?;
        Ok(Self {
            out: BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, out),
            rows: TextRows::new(spec.delim),
            relation: spec.relation.to_owned(),
            path: spec.path.to_owned(),
        })
    }

    /// Flush the buffer and publish the file.
    ///
    /// Both steps are checked here rather than left to `Drop`, which would
    /// swallow a failed tail write and, worse, abandon the file entirely.
    pub fn commit(mut self) -> Result<(), RuntimeError> {
        let relation = self.relation;
        let path = self.path;
        let failed = |source| RuntimeError::Output {
            relation: relation.clone(),
            path: path.clone(),
            source,
        };
        self.out.flush().map_err(&failed)?;
        self.out
            .into_inner()
            .map_err(|e| failed(e.into_error()))?
            .commit()
            .map_err(&failed)
    }

    /// Report an I/O failure as this relation's.
    fn failed(&self, source: io::Error) -> RuntimeError {
        RuntimeError::Output {
            relation: self.relation.clone(),
            path: self.path.clone(),
            source,
        }
    }

    /// Write every row, formatting segments across `rayon` lanes.
    ///
    /// Byte-identical to pushing the same rows one at a time: lanes format
    /// into private buffers and each wave is written in worker-then-row
    /// order. Peak formatted bytes is one wave, so a large relation never
    /// materializes a second copy of itself.
    ///
    /// Not a [`Writer`] method because it is an optimisation of this one
    /// sink: a host `Vec` has no wave to stream and nothing to gain from
    /// formatting in parallel.
    fn write_parallel<T, Ts>(
        mut self,
        per_worker: Vec<Vec<(T, Ts, Diff)>>,
        with_diff: bool,
    ) -> Result<(), RuntimeError>
    where
        T: Encode<TextRows> + Send,
        Ts: Send,
    {
        let lanes = rayon::current_num_threads().max(1);
        let mut pool: Vec<TextRows> = (0..lanes)
            .map(|_| TextRows::new(self.rows.delim()))
            .collect();
        let mut segments: Vec<Vec<(T, Ts, Diff)>> = (0..lanes).map(|_| Vec::new()).collect();

        // Flattening preserves worker-then-row order, and drops each
        // worker's allocation as it is consumed, so the rows still resident
        // shrink as the waves advance.
        let mut source = per_worker.into_iter().flatten();

        loop {
            let mut filled = 0;
            for segment in &mut segments {
                segment.clear();
                segment.extend(source.by_ref().take(SEG_ROWS));
                if segment.is_empty() {
                    break;
                }
                filled += 1;
            }
            if filled == 0 {
                break;
            }

            pool[..filled]
                .par_iter_mut()
                .zip(segments[..filled].par_iter_mut())
                .for_each(|(buffer, segment)| {
                    buffer.clear();
                    for (tuple, _time, diff) in segment.drain(..) {
                        buffer.push(tuple, with_diff.then_some(diff));
                    }
                });

            for buffer in &pool[..filled] {
                if let Err(source) = self.out.write_all(buffer.as_bytes()) {
                    panic!("{}", self.failed(source));
                }
            }
        }

        self.commit()
    }
}

impl<T: Encode<TextRows>> Writer<T> for TextWriter {
    type Out = ();

    fn open(spec: &OutputSpec<'_>) -> Result<Self, RuntimeError> {
        Self::create(spec)
    }

    /// # Panics
    ///
    /// If the write fails. A row-level failure means the file is already
    /// truncated and partly written, so there is nothing to salvage by
    /// continuing.
    #[inline]
    fn push(&mut self, row: T, diff: Option<Diff>) {
        self.rows.clear();
        self.rows.push(row, diff);
        if let Err(source) = self.out.write_all(self.rows.as_bytes()) {
            panic!("{}", self.failed(source));
        }
    }

    fn finish(self) -> Result<(), RuntimeError> {
        Self::commit(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::output::Row;

    /// A two-column relation's buffered rows, as the drain hands them
    /// over: one inner Vec per worker.
    type Buffers = Vec<Vec<Row<(i32, i32), u32>>>;

    /// A writer over a temporary path, and the bytes it left there.
    fn write_with(rows: Buffers, with_diff: bool, parallel: bool) -> Vec<u8> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("Out.csv");
        let path = path.to_str().expect("utf-8 path");
        let spec = OutputSpec {
            relation: "Out",
            path,
            delim: b'\t',
        };
        if parallel {
            let writer = <TextWriter as Writer<(i32, i32)>>::open(&spec).expect("open");
            writer.write_parallel(rows, with_diff).expect("write");
        } else {
            let mut writer = <TextWriter as Writer<(i32, i32)>>::open(&spec).expect("open");
            for worker in rows {
                for (tuple, _time, diff) in worker {
                    writer.push(tuple, with_diff.then_some(diff));
                }
            }
            Writer::<(i32, i32)>::finish(writer).expect("finish");
        }
        std::fs::read(path).expect("read back")
    }

    /// Rows spanning several workers, enough to cross the segment boundary
    /// so the parallel path runs more than one wave.
    fn many_rows() -> Buffers {
        (0..3)
            .map(|w| (0..SEG_ROWS as i32 + 17).map(|r| ((w, r), 0, 1)).collect())
            .collect()
    }

    /// The parallel path is byte-identical to pushing the same rows one at
    /// a time, which is the property that lets the router pick either.
    #[test]
    fn parallel_output_matches_serial() {
        let serial = write_with(many_rows(), false, false);
        let parallel = write_with(many_rows(), false, true);
        assert_eq!(serial, parallel);
    }

    /// Worker order and row-within-worker order both survive the waves.
    #[test]
    fn parallel_output_keeps_row_order() {
        let bytes = write_with(many_rows(), false, true);
        let text = String::from_utf8(bytes).expect("utf-8");
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("0\t0"));
        assert_eq!(lines.nth(SEG_ROWS + 16), Some("1\t0"));
    }

    /// The diff column survives the parallel path too, since a lane
    /// formats through the same buffer a serial push does.
    #[test]
    fn parallel_output_carries_the_diff_column() {
        let rows = vec![vec![((1, 2), 0u32, 1), ((3, 4), 0, -1)]];
        assert_eq!(write_with(rows, true, true), b"1\t2\t+1\n3\t4\t-1\n");
    }

    /// A relation that derived nothing still leaves its file behind, empty:
    /// downstream tooling distinguishes "no rows" from "never ran".
    #[test]
    fn an_empty_relation_still_creates_its_file() {
        assert!(write_with(vec![], false, true).is_empty());
        assert!(write_with(vec![vec![]], false, false).is_empty());
    }
}
