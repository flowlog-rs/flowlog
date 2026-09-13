//! Stdout rows and size reports for one relation.
//!
//! [`StdoutWriter`] owns the prepared row prefix and integer scratch. Each
//! call holds stdout's lock for one complete record.

use std::fmt;
use std::fmt::Debug;
use std::io;
use std::io::Stdout;
use std::io::StdoutLock;
use std::io::Write;

use crate::io::Relation;
use crate::io::output::encode::Encode;
use crate::io::output::encode::text::TextEncoder;
use crate::io::output::writer::Writer;

// =============================================================================
// StdoutWriter
// =============================================================================

/// Writes timestamped rows and counts without staging their text.
pub struct StdoutWriter {
    out: Stdout,
    name: &'static str,
    prefix: String,
    integers: itoa::Buffer,
}

impl StdoutWriter {
    /// Binds the relation name without acquiring stdout's lock.
    pub(in crate::io::output) fn new(name: &'static str) -> Self {
        Self {
            out: io::stdout(),
            name,
            prefix: format!("[tuple][{name}]  t="),
            integers: itoa::Buffer::new(),
        }
    }

    /// Writes the supplied count independently of emitted rows or limits.
    ///
    /// Incremental counts may be negative. No rows need to be written first.
    pub(in crate::io::output) fn write_size<T: Debug>(
        name: &str,
        time: &T,
        size: i32,
    ) -> io::Result<()> {
        writeln!(io::stdout(), "[size][{name}]  t={time:?}  size={size}")
    }
}

impl<R: Relation, T: Debug> Writer<R, T> for StdoutWriter
where
    for<'a, 'r> TextEncoder<'a, StdoutLock<'static>, true>:
        Encode<&'r R::Tuple, Output = io::Result<()>>,
{
    type Output = ();
    type Error = io::Error;

    /// Writes one row under a single stdout lock, preserving newline flushing.
    fn write_row(&mut self, (row, time, diff): (R::Tuple, T, i32)) -> io::Result<()> {
        write_record::<R, _, _>(
            &row,
            &mut self.out.lock(),
            &self.prefix,
            &time,
            diff,
            &mut self.integers,
        )
    }

    fn write_batch(&mut self, batches: &mut [Vec<(R::Tuple, T, i32)>]) -> io::Result<()> {
        for rows in batches {
            rows.drain(..)
                .try_for_each(|row| Writer::<R, T>::write_row(self, row))?;
        }
        Ok(())
    }

    fn finish(self) -> io::Result<()> {
        // Every record ends in a newline and keeps stdout's existing flushing.
        Ok(())
    }
}

impl Debug for StdoutWriter {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("StdoutWriter")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Writes one debug record; the sink must stay locked for the complete call.
#[inline]
fn write_record<R: Relation, W: Write, T: Debug>(
    row: &R::Tuple,
    out: &mut W,
    prefix: &str,
    time: &T,
    diff: i32,
    integers: &mut itoa::Buffer,
) -> io::Result<()>
where
    for<'a, 'r> TextEncoder<'a, W, true>: Encode<&'r R::Tuple, Output = io::Result<()>>,
{
    out.write_all(prefix.as_bytes())?;
    write!(out, "{time:?}")?;
    out.write_all(if R::ARITY == 0 { b"  " } else { b"  data=(" })?;
    TextEncoder::<_, true>::new(out, b", ", integers).encode(row)?;
    if R::ARITY > 0 {
        out.write_all(b")")?;
    }
    out.write_all(if diff >= 0 { b"  diff=+" } else { b"  diff=" })?;
    out.write_all(integers.format(diff).as_bytes())?;
    out.write_all(b"\n")
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::env;
    use std::process::Command;

    use ordered_float::OrderedFloat;
    use rstest::rstest;

    use super::*;
    use crate::intern::intern;

    #[derive(Debug)]
    struct TestRelation<T, const ARITY: usize>(T);

    impl<T: differential_dataflow::Data, const ARITY: usize> Relation for TestRelation<T, ARITY> {
        const NAME: &'static str = "Out";
        const ARITY: usize = ARITY;
        type Tuple = T;
    }

    #[test]
    fn stdout_rows_and_independent_sizes_share_the_existing_layout() {
        const CHILD: &str = "FLOWLOG_STDOUT_WRITER_CHILD";
        if env::var_os(CHILD).is_some() {
            let mut writer = StdoutWriter::new("Out");
            Writer::<TestRelation<_, 1>, _>::write_batch(
                &mut writer,
                &mut [Vec::new(), vec![((7,), 5, 1)]],
            )
            .expect("stdout batch");
            StdoutWriter::write_size("Out", &5, 42).expect("independent count");
            Writer::<TestRelation<_, 0>, _>::write_row(&mut writer, ((), (2, 3), -1))
                .expect("nullary row");
            StdoutWriter::write_size("CountOnly", &(), -2).expect("count without rows");
            return;
        }

        // A child process exposes the actual stdout lock and line buffer,
        // which the test harness's thread-local print capture does not own.
        let output = Command::new(env::current_exe().expect("test executable"))
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::stdout_rows_and_independent_sizes_share_the_existing_layout"
                )
                .strip_prefix("flowlog_runtime::")
                .expect("runtime test path"),
                "--nocapture",
                "--quiet",
            ])
            .env(CHILD, "1")
            .output()
            .expect("stdout child");
        assert!(output.status.success(), "stdout child: {output:?}");
        let text = String::from_utf8(output.stdout).expect("stdout UTF-8");
        let records: Vec<_> = text.lines().filter(|line| line.starts_with('[')).collect();
        assert_eq!(
            records,
            [
                "[tuple][Out]  t=5  data=(7)  diff=+1",
                "[size][Out]  t=5  size=42",
                "[tuple][Out]  t=(2, 3)  True  diff=-1",
                "[size][CountOnly]  t=()  size=-2",
            ],
        );
    }
    /// The process stdout sink cannot be replaced with a byte capture.
    #[rstest]
    #[case::nullary(TestRelation::<_, 0>(()), "[tuple][Out]  t=5  True  diff=-2\n")]
    #[case::single(TestRelation::<_, 1>((7,)), "[tuple][Out]  t=5  data=(7)  diff=-2\n")]
    #[case::nested_empty(TestRelation::<_, 1>(((),)), "[tuple][Out]  t=5  data=(())  diff=-2\n")]
    #[case::nested_single(TestRelation::<_, 1>(((7,),)), "[tuple][Out]  t=5  data=((7,))  diff=-2\n")]
    #[case::nested(
        TestRelation::<_, 1>(((intern("p"), (intern("q"),)),)),
        "[tuple][Out]  t=5  data=((\"p\", (\"q\",)))  diff=-2\n",
    )]
    #[case::mixed(
        TestRelation::<_, 4>((7, String::from("a\"\nb"), true, OrderedFloat(1.0f64))),
        "[tuple][Out]  t=5  data=(7, \"a\\\"\\nb\", true, 1.0)  diff=-2\n",
    )]
    #[case::twelve(
        TestRelation::<_, 12>((0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)),
        "[tuple][Out]  t=5  data=(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)  diff=-2\n",
    )]
    fn stdout_rows_preserve_the_existing_layout<R, const ARITY: usize>(
        #[case] row: TestRelation<R, ARITY>,
        #[case] expected: &str,
    ) where
        R: differential_dataflow::Data,
        for<'a, 'r> TextEncoder<'a, Vec<u8>, true>: Encode<&'r R, Output = io::Result<()>>,
    {
        let mut bytes = Vec::new();
        write_record::<TestRelation<R, ARITY>, _, _>(
            &row.0,
            &mut bytes,
            "[tuple][Out]  t=",
            &5,
            -2,
            &mut itoa::Buffer::new(),
        )
        .expect("stdout row");
        assert_eq!(bytes, expected.as_bytes());
    }

    /// The process stdout sink cannot be replaced with a byte capture.
    #[test]
    fn stdout_preserves_structured_timestamps_and_positive_weights() {
        let mut bytes = Vec::new();
        write_record::<TestRelation<_, 1>, _, _>(
            &(7,),
            &mut bytes,
            "[tuple][Out]  t=",
            &(2, 3),
            1,
            &mut itoa::Buffer::new(),
        )
        .expect("stdout row");
        assert_eq!(bytes, b"[tuple][Out]  t=(2, 3)  data=(7)  diff=+1\n");
    }

    /// The process stdout sink cannot be replaced with a byte capture.
    #[rstest]
    #[case(i32::MIN, "[tuple][Out]  t=()  data=(7)  diff=-2147483648\n")]
    #[case(0, "[tuple][Out]  t=()  data=(7)  diff=+0\n")]
    #[case(i32::MAX, "[tuple][Out]  t=()  data=(7)  diff=+2147483647\n")]
    fn stdout_weights_preserve_their_sign(#[case] diff: i32, #[case] expected: &str) {
        let mut bytes = Vec::new();
        write_record::<TestRelation<_, 1>, _, _>(
            &(7,),
            &mut bytes,
            "[tuple][Out]  t=",
            &(),
            diff,
            &mut itoa::Buffer::new(),
        )
        .expect("stdout row");
        assert_eq!(bytes, expected.as_bytes());
    }

    /// A supplied sink makes scratch reuse observable without process stdout.
    #[test]
    fn stdout_appends_records_and_reuses_storage() {
        let row = (7, String::from("hello"));
        let mut bytes = Vec::with_capacity(256);
        let storage = bytes.as_ptr();
        let prefix = "[tuple][Out]  t=";
        let mut integers = itoa::Buffer::new();
        write_record::<TestRelation<_, 2>, _, _>(&row, &mut bytes, prefix, &(), 1, &mut integers)
            .expect("insert row");
        write_record::<TestRelation<_, 2>, _, _>(&row, &mut bytes, prefix, &(), -1, &mut integers)
            .expect("retract row");
        assert_eq!(
            bytes,
            concat!(
                "[tuple][Out]  t=()  data=(7, \"hello\")  diff=+1\n",
                "[tuple][Out]  t=()  data=(7, \"hello\")  diff=-1\n",
            )
            .as_bytes(),
        );
        assert_eq!(bytes.as_ptr(), storage);
        assert_eq!(bytes.capacity(), 256);
    }

    /// Process stdout cannot inject short writes or interruptions on demand.
    #[test]
    fn stdout_retries_interrupted_and_short_writes() {
        #[derive(Debug)]
        struct ShortWriter {
            bytes: Vec<u8>,
            interrupt: bool,
        }

        impl Write for ShortWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.interrupt {
                    self.interrupt = false;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                self.interrupt = true;
                let len = bytes.len().min(3);
                self.bytes.extend_from_slice(&bytes[..len]);
                Ok(len)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut out = ShortWriter {
            bytes: Vec::new(),
            interrupt: true,
        };
        write_record::<TestRelation<_, 3>, _, _>(
            &(7, String::from("hello"), OrderedFloat(1.0f64)),
            &mut out,
            "[tuple][Out]  t=",
            &(),
            1,
            &mut itoa::Buffer::new(),
        )
        .expect("stdout row");
        assert_eq!(
            out.bytes,
            b"[tuple][Out]  t=()  data=(7, \"hello\", 1.0)  diff=+1\n",
        );
    }

    /// Process stdout cannot inject errors at chosen record boundaries.
    #[rstest]
    #[case::prefix("")]
    #[case::timestamp("[tuple][Out]  t=")]
    #[case::payload("[tuple][Out]  t=()  data=(")]
    #[case::string("[tuple][Out]  t=()  data=(\"he")]
    #[case::weight("[tuple][Out]  t=()  data=(\"hello\")  diff=+")]
    #[case::newline("[tuple][Out]  t=()  data=(\"hello\")  diff=+1")]
    fn stdout_propagates_errors_after_partial_records(#[case] expected: &str) {
        #[derive(Debug)]
        struct Broken {
            bytes: Vec<u8>,
            remaining: usize,
        }

        impl Write for Broken {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed sink"));
                }
                let len = bytes.len().min(self.remaining);
                self.bytes.extend_from_slice(&bytes[..len]);
                self.remaining -= len;
                Ok(len)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut out = Broken {
            bytes: Vec::new(),
            remaining: expected.len(),
        };
        let error = write_record::<TestRelation<_, 1>, _, _>(
            &(String::from("hello"),),
            &mut out,
            "[tuple][Out]  t=",
            &(),
            1,
            &mut itoa::Buffer::new(),
        )
        .expect_err("closed sink");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(error.to_string(), "closed sink");
        assert_eq!(out.bytes, expected.as_bytes());
    }
}
