//! Buffered file output with reusable formatting storage.
//!
//! [`FileWriter`] writes individual rows or worker partitions. Partitioned
//! output formats one bounded wave in parallel, then writes it in input order.

use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use rayon::prelude::*;

use crate::io::Relation;
use crate::io::output::encode::Encode;
use crate::io::output::encode::text::TextEncoder;
use crate::io::output::writer::Writer;

// A 1 MiB buffer amortizes writes across small rows; 8K-row segments bound
// formatting storage while amortizing parallel work.
const OUTPUT_BUFFER_BYTES: usize = 1 << 20;
const SEGMENT_ROWS: usize = 8192;

// =============================================================================
// FileWriter
// =============================================================================

/// Writes delimited rows without collecting another copy of the relation.
///
/// Call [`finish`](Self::finish) to report final buffered write errors.
pub struct FileWriter<W: Write = File> {
    out: BufWriter<W>,
    delimiter: u8,
    bytes: Vec<u8>,
    integers: itoa::Buffer,
    pool: Vec<Vec<u8>>,
}

impl FileWriter<File> {
    /// Creates or truncates the resolved output path immediately.
    ///
    /// Errors may leave a partial file. Parent directories must already exist.
    pub(in crate::io::output) fn create(path: &Path, delimiter: u8) -> io::Result<Self> {
        // Immediate truncation and partial writes are observable. Atomic
        // replacement would defer visibility until the entire output succeeds.
        Ok(Self::new(File::create(path)?, delimiter))
    }
}

impl<W: Write> FileWriter<W> {
    /// Buffers a byte sink until the buffer fills or [`finish`](Self::finish).
    fn new(out: W, delimiter: u8) -> Self {
        Self {
            out: BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, out),
            delimiter,
            bytes: Vec::new(),
            integers: itoa::Buffer::new(),
            pool: Vec::new(),
        }
    }
}

impl<R: Relation, T: Sync, W: Write, const INCREMENTAL: bool> Writer<R, T, INCREMENTAL>
    for FileWriter<W>
where
    for<'a, 'r> TextEncoder<'a, Vec<u8>, false>: Encode<&'r R::Tuple, Output = io::Result<()>>,
    R::Tuple: Sync,
{
    type Output = ();
    type Error = io::Error;

    fn write_row(&mut self, (row, _, diff): (R::Tuple, T, i32)) -> io::Result<()> {
        self.bytes.clear();
        encode_row::<INCREMENTAL, R>(
            &row,
            self.delimiter,
            diff,
            &mut self.bytes,
            &mut self.integers,
        );
        self.out.write_all(&self.bytes)
    }

    /// Preserves partition order and the row order within each partition.
    ///
    /// Timestamps are ignored; incremental output includes signed weights.
    /// At most one 8K-row segment per Rayon lane is formatted at a time.
    /// Scratch retains its largest wave until this writer is dropped.
    fn write_batch(&mut self, per_worker: &mut [Vec<(R::Tuple, T, i32)>]) -> io::Result<()> {
        if R::ARITY == 0 {
            for rows in per_worker {
                rows.drain(..)
                    .try_for_each(|row| Writer::<R, T, INCREMENTAL>::write_row(self, row))?;
            }
            return Ok(());
        }
        if per_worker.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let lanes = rayon::current_num_threads();
        let mut segments = per_worker.iter().flat_map(|rows| rows.chunks(SEGMENT_ROWS));
        let mut wave = Vec::with_capacity(lanes);
        loop {
            wave.clear();
            wave.extend(segments.by_ref().take(lanes));
            if wave.is_empty() {
                break;
            }
            while self.pool.len() < wave.len() {
                self.pool.push(Vec::new());
            }
            let delimiter = self.delimiter;
            self.pool[..wave.len()]
                .par_iter_mut()
                .zip(wave.par_iter())
                .for_each(|(bytes, rows)| {
                    bytes.clear();
                    let mut integers = itoa::Buffer::new();
                    for (row, _, diff) in rows.iter() {
                        encode_row::<INCREMENTAL, R>(row, delimiter, *diff, bytes, &mut integers);
                    }
                });
            for bytes in &self.pool[..wave.len()] {
                self.out.write_all(bytes)?;
            }
        }
        Ok(())
    }

    /// Flushes buffered bytes and the underlying sink, returning any I/O error.
    fn finish(mut self) -> io::Result<()> {
        self.out.flush()
    }
}

impl<W: Write + Debug> Debug for FileWriter<W> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("FileWriter")
            .field("out", &self.out)
            .field("delimiter", &self.delimiter)
            .finish_non_exhaustive()
    }
}

/// Appends one file row, including its optional weight and final newline.
#[inline]
fn encode_row<const INCREMENTAL: bool, R: Relation>(
    row: &R::Tuple,
    delimiter: u8,
    diff: i32,
    bytes: &mut Vec<u8>,
    integers: &mut itoa::Buffer,
) where
    for<'a, 'r> TextEncoder<'a, Vec<u8>, false>: Encode<&'r R::Tuple, Output = io::Result<()>>,
{
    // Vec<u8>'s Write implementation cannot return an I/O error.
    TextEncoder::<_, false>::new(bytes, &[delimiter], integers)
        .encode(row)
        .expect("writing to a Vec cannot fail");
    if INCREMENTAL && R::ARITY > 0 {
        bytes.push(delimiter);
        if diff >= 0 {
            bytes.push(b'+');
        }
        bytes.extend_from_slice(integers.format(diff).as_bytes());
    }
    bytes.push(b'\n');
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::fs;

    use lasso::Spur;
    use ordered_float::OrderedFloat;
    use rayon::ThreadPoolBuilder;
    use rstest::rstest;

    use super::*;
    use crate::error::Position;
    use crate::intern::intern;
    use crate::io::input::decode::Decode;
    use crate::io::input::decode::text::TextRow;

    #[derive(Debug)]
    struct TestRelation<T, const ARITY: usize>(T);

    impl<T: differential_dataflow::Data, const ARITY: usize> Relation for TestRelation<T, ARITY> {
        const NAME: &'static str = "Out";
        const ARITY: usize = ARITY;
        type Tuple = T;
    }

    #[rstest]
    #[case::empty(vec![], b"")]
    #[case::nonempty(vec![(7,), (3,)], b"7\n3\n")]
    fn file_output_replaces_existing_contents(#[case] rows: Vec<(i32,)>, #[case] expected: &[u8]) {
        let dir = tempfile::tempdir().expect("output directory");
        let path = dir.path().join("Out.csv");
        fs::write(&path, "previous output").expect("existing file");
        let mut writer = FileWriter::create(&path, b'\t').expect("create output");
        assert_eq!(fs::metadata(&path).expect("output metadata").len(), 0);
        for row in rows {
            Writer::<TestRelation<_, 1>, ()>::write_row(&mut writer, (row, (), 1))
                .expect("write row");
        }
        Writer::<TestRelation<(i32,), 1>, ()>::finish(writer).expect("flush output");
        assert_eq!(fs::read(&path).expect("output bytes"), expected);
    }

    #[test]
    fn creating_a_file_does_not_create_missing_parent_directories() {
        let dir = tempfile::tempdir().expect("output directory");
        let path = dir.path().join("missing").join("Out.csv");
        let error = FileWriter::create(&path, b'\t').expect_err("missing parent");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(!path.parent().expect("parent path").exists());
    }

    #[test]
    fn sequential_writes_preserve_delivery_order_and_delta_weights() {
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'|');
        Writer::<TestRelation<_, 1>, (), true>::write_row(&mut writer, ((7,), (), -2))
            .expect("retract row");
        Writer::<TestRelation<_, 1>, (), true>::write_row(&mut writer, ((3,), (), 5))
            .expect("insert row");
        Writer::<TestRelation<_, 1>, (), true>::write_row(&mut writer, ((7,), (), 0))
            .expect("zero weight");
        Writer::<TestRelation<(i32,), 1>, (), true>::finish(writer).expect("flush output");
        assert_eq!(bytes, b"7|-2\n3|+5\n7|+0\n");
    }

    #[rstest]
    #[case(1)]
    #[case(3)]
    fn parallel_waves_keep_partition_and_row_order(#[case] lanes: usize) {
        let pool = ThreadPoolBuilder::new()
            .num_threads(lanes)
            .build()
            .expect("formatting pool");
        pool.install(|| {
            let mut first = vec![((1,), 5, 1); 8192];
            first.push(((7,), 9, 0));
            let mut second = vec![((2,), 3, -1); 8192];
            second.push(((3,), 4, 7));
            let mut partitions = vec![Vec::new(), first, Vec::new(), second, Vec::new()];
            let mut bytes = Vec::new();
            let mut writer = FileWriter::new(&mut bytes, b'\t');
            Writer::<TestRelation<_, 1>, _, true>::write_batch(&mut writer, &mut partitions)
                .expect("parallel output");
            Writer::<TestRelation<(i32,), 1>, i32, true>::finish(writer).expect("flush output");
            let expected = "1\t+1\n".repeat(8192) + "7\t+0\n" + &"2\t-1\n".repeat(8192) + "3\t+7\n";
            assert_eq!(bytes, expected.as_bytes());
        });
    }

    /// Scratch ownership is internal, so the memory bound is checked here
    /// after writing enough rows to require several waves.
    #[test]
    fn parallel_formatting_reuses_only_one_wave_of_scratch() {
        let pool = ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .expect("formatting pool");
        pool.install(|| {
            let mut partitions = vec![vec![((7,), (), 1); 5 * 8192]];
            let mut writer = FileWriter::new(io::sink(), b'\t');
            Writer::<TestRelation<_, 1>, _, false>::write_batch(&mut writer, &mut partitions)
                .expect("parallel output");
            assert_eq!(writer.pool.len(), 2);
            assert!(writer.pool.iter().all(|bytes| bytes.capacity() <= 32_768));
            let storage: Vec<_> = writer.pool.iter().map(|bytes| bytes.as_ptr()).collect();
            Writer::<TestRelation<_, 1>, _, false>::write_batch(&mut writer, &mut partitions)
                .expect("reuse formatting scratch");
            assert_eq!(
                writer
                    .pool
                    .iter()
                    .map(|bytes| bytes.as_ptr())
                    .collect::<Vec<_>>(),
                storage,
            );
            Writer::<TestRelation<(i32,), 1>, ()>::finish(writer).expect("flush output");
        });
    }

    #[test]
    fn empty_partitions_write_no_bytes() {
        let mut partitions: Vec<Vec<((i32,), (), i32)>> = vec![Vec::new(), Vec::new()];
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'\t');
        Writer::<TestRelation<_, 1>, _, false>::write_batch(&mut writer, &mut partitions)
            .expect("empty partitions");
        Writer::<TestRelation<(i32,), 1>, ()>::finish(writer).expect("flush empty output");
        assert_eq!(bytes, b"");
    }

    #[rstest]
    #[case::buffered_write(true, io::ErrorKind::BrokenPipe, "closed sink")]
    #[case::flush(false, io::ErrorKind::Other, "flush failed")]
    fn finishing_reports_buffered_write_and_flush_errors(
        #[case] fail_write: bool,
        #[case] kind: io::ErrorKind,
        #[case] message: &str,
    ) {
        #[derive(Debug)]
        struct Failing {
            fail_write: bool,
        }

        impl Write for Failing {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.fail_write {
                    Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed sink"))
                } else {
                    Ok(bytes.len())
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("flush failed"))
            }
        }

        let mut writer = FileWriter::new(Failing { fail_write }, b'\t');
        Writer::<TestRelation<_, 1>, ()>::write_row(&mut writer, ((7,), (), 1))
            .expect("buffer row");
        let error =
            Writer::<TestRelation<(i32,), 1>, ()>::finish(writer).expect_err("sink failure");
        assert_eq!(error.kind(), kind);
        assert_eq!(error.to_string(), message);
    }

    #[test]
    fn file_batches_match_existing_fixture_bytes() {
        type Rows = TestRelation<(OrderedFloat<f32>, OrderedFloat<f64>), 2>;
        let rows = [
            (OrderedFloat(-2.5f32), OrderedFloat(4.140000000000001f64)),
            (OrderedFloat(-0.0), OrderedFloat(0.5)),
            (OrderedFloat(0.1), OrderedFloat(1e20)),
            (OrderedFloat(1.0), OrderedFloat(1.0)),
        ];
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'\t');
        let mut partitions = [
            Vec::new(),
            rows.into_iter().map(|row| (row, (), 1)).collect(),
        ];
        Writer::<Rows, ()>::write_batch(&mut writer, &mut partitions).expect("file batch");
        Writer::<Rows, ()>::finish(writer).expect("flush output");
        assert_eq!(
            bytes,
            include_bytes!(
                "../../../../../tests/fixtures/batch/output_all_types/expected/Floats.csv"
            ),
        );
    }

    #[rstest]
    #[case(i32::MIN, b"7|-2147483648\n")]
    #[case(-1, b"7|-1\n")]
    #[case(0, b"7|+0\n")]
    #[case(1, b"7|+1\n")]
    #[case(i32::MAX, b"7|+2147483647\n")]
    fn incremental_rows_append_signed_weights(#[case] diff: i32, #[case] expected: &[u8]) {
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'|');
        Writer::<TestRelation<_, 1>, (), true>::write_row(&mut writer, ((7,), (), diff))
            .expect("delta row");
        Writer::<TestRelation<(i32,), 1>, (), true>::finish(writer).expect("flush output");
        assert_eq!(bytes, expected);
    }

    #[rstest]
    #[case(-1)]
    #[case(0)]
    #[case(1)]
    fn nullary_file_rows_always_use_the_presence_marker(#[case] diff: i32) {
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b',');
        Writer::<TestRelation<(), 0>, ()>::write_row(&mut writer, ((), (), diff))
            .expect("nullary row");
        Writer::<TestRelation<(), 0>, (), true>::write_row(&mut writer, ((), (), diff))
            .expect("nullary delta");
        Writer::<TestRelation<(), 0>, ()>::finish(writer).expect("flush output");
        assert_eq!(bytes, b"True\nTrue\n");
    }

    /// Output bytes cannot reveal whether the private scratch was reused.
    #[test]
    fn sequential_rows_reuse_formatting_storage() {
        type Rows = TestRelation<(i32, Spur, (bool,)), 3>;
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'|');
        let row = (7, intern("hello"), (false,));
        Writer::<Rows, (), true>::write_row(&mut writer, (row, (), -1)).expect("delta row");
        let storage = (writer.bytes.as_ptr(), writer.bytes.capacity());
        Writer::<Rows, ()>::write_row(&mut writer, (row, (), 1)).expect("file row");
        assert_eq!((writer.bytes.as_ptr(), writer.bytes.capacity()), storage);
        Writer::<Rows, ()>::finish(writer).expect("flush output");
        assert_eq!(bytes, b"7|hello|(false,)|-1\n7|hello|(false,)\n");
    }

    #[test]
    fn scalar_file_rows_roundtrip_through_text_input() {
        type Rows = TestRelation<(i32, Spur, bool, OrderedFloat<f64>), 4>;
        let row = (7i32, intern("alpha"), false, OrderedFloat(2.5f64));
        let mut bytes = Vec::new();
        let mut writer = FileWriter::new(&mut bytes, b'\t');
        Writer::<Rows, ()>::write_row(&mut writer, (row, (), 1)).expect("file row");
        Writer::<Rows, ()>::finish(writer).expect("flush output");
        assert_eq!(bytes, b"7\talpha\tfalse\t2.5\n");
        let text = TextRow {
            text: str::from_utf8(&bytes).expect("UTF-8 row"),
            delim: b'\t',
            at: Position::Put,
        };
        let decoded: (i32, Spur, bool, OrderedFloat<f64>) = Decode::decode(&text).expect("row");
        assert_eq!(decoded, (7, intern("alpha"), false, OrderedFloat(2.5)));
    }
}
