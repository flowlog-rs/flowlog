//! SQLite output transactions and table writers.
//!
//! [`SqliteWriter`] groups destinations by database. [`TableWriter`] encodes
//! one relation within that database's transaction. The SQLite encoder owns
//! column layouts and value binding.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::PathBuf;

use rusqlite::Connection;
use rusqlite::Statement;
use rusqlite::Transaction;
use rusqlite::TransactionBehavior;

use crate::error::RuntimeError;
use crate::io::Relation;
use crate::io::output::encode::sqlite::EncodeField;
use crate::io::output::encode::sqlite::EncodeRow;
use crate::io::output::encode::sqlite::SqliteEncoder;
use crate::io::output::writer::Writer;

/// Tracks databases initialized during one execution. The first successful
/// write replaces targeted tables; later writes append to them. Unrelated
/// tables are preserved. A failed transaction rolls back database changes,
/// but does not restore consumed emitter buffers; callers must stop the run.
#[derive(Debug, Default)]
pub struct SqliteWriter {
    initialized: HashSet<PathBuf>,
}

impl SqliteWriter {
    /// Commits each database's destinations together, passing their original
    /// indices and whether this is the database's first write to `emit`.
    /// The destination list must stay the same throughout one execution.
    /// Symlink and relative-path aliases share a transaction. Separate
    /// databases commit independently; hard-link aliases are not supported.
    pub fn write(
        &mut self,
        paths: &[PathBuf],
        mut emit: impl FnMut(usize, &Transaction<'_>, bool) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        let mut groups: BTreeMap<PathBuf, Vec<usize>> = BTreeMap::new();
        for (index, path) in paths.iter().enumerate() {
            // Existing destinations need no connection until their shared
            // transaction starts. New files must exist to resolve aliases.
            let resolved = (|| match fs::canonicalize(path) {
                Ok(resolved) => Ok(resolved),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Connection::open(path)?;
                    Ok::<_, RuntimeError>(fs::canonicalize(path)?)
                }
                Err(error) => Err(error.into()),
            })()
            .map_err(|source| RuntimeError::SqlitePath {
                path: path.clone(),
                source: Box::new(source),
            })?;
            groups.entry(resolved).or_default().push(index);
        }
        for (path, indices) in groups {
            (|| {
                let mut connection = Connection::open(&path)?;
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let reset = !self.initialized.contains(&path);
                for index in indices {
                    emit(index, &transaction, reset)?;
                }
                transaction.commit()?;
                Ok::<_, RuntimeError>(())
            })()
            .map_err(|source| RuntimeError::SqlitePath {
                path: path.clone(),
                source: Box::new(source),
            })?;
            self.initialized.insert(path);
        }
        Ok(())
    }
}

/// Borrows a transaction so multiple relations can commit as one epoch.
#[derive(Debug)]
pub struct TableWriter<'a> {
    statement: Statement<'a>,
}

impl<'a> TableWriter<'a> {
    pub(in crate::io::output) fn new<R: Relation, const INCREMENTAL: bool>(
        transaction: &'a Transaction<'_>,
        names: &[&str],
        reset: bool,
    ) -> Result<Self, RuntimeError>
    where
        R::Tuple: EncodeRow,
    {
        let mut columns = columns::<R::Tuple>(names)?;
        if INCREMENTAL {
            columns.extend([
                ("__flowlog_timestamp".to_owned(), "INTEGER"),
                ("__flowlog_insert".to_owned(), "INTEGER"),
            ]);
        }
        let table = identifier(R::NAME);
        if reset {
            transaction.execute(&format!("DROP TABLE IF EXISTS {table}"), [])?;
            let definitions = columns
                .iter()
                .map(|(name, ty)| {
                    let check = match name.as_str() {
                        "__flowlog_insert" => " CHECK (__flowlog_insert IN (0, 1))",
                        "__flowlog_present" => " CHECK (__flowlog_present = 1)",
                        _ => "",
                    };
                    format!("{} {ty} NOT NULL{check}", identifier(name))
                })
                .collect::<Vec<_>>()
                .join(", ");
            transaction.execute(&format!("CREATE TABLE {table} ({definitions})"), [])?;
        }
        let names = columns
            .iter()
            .map(|(name, _)| identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let parameters = vec!["?"; columns.len()].join(", ");
        let statement = transaction.prepare(&format!(
            "INSERT INTO {table} ({names}) VALUES ({parameters})"
        ))?;
        Ok(Self { statement })
    }
}

impl<R: Relation, T: EncodeField, const INCREMENTAL: bool> Writer<R, T, INCREMENTAL>
    for TableWriter<'_>
where
    R::Tuple: EncodeRow,
{
    type Output = ();
    type Error = RuntimeError;

    fn write_row(&mut self, (row, time, diff): (R::Tuple, T, i32)) -> Result<(), RuntimeError> {
        if INCREMENTAL && !matches!(diff, -1 | 1) {
            return Err(RuntimeError::SqliteWeight {
                relation: R::NAME,
                diff,
            });
        }
        let mut encoder = SqliteEncoder::new(&mut self.statement);
        row.write(&mut encoder)?;
        if encoder.is_empty() {
            encoder.bind(1)?;
        }
        if INCREMENTAL {
            time.write(&mut encoder)?;
            encoder.bind(i64::from(diff == 1))?;
        }
        self.statement.raw_execute()?;
        Ok(())
    }

    fn write_batch(&mut self, batches: &mut [Vec<(R::Tuple, T, i32)>]) -> Result<(), RuntimeError> {
        for batch in batches {
            for update in batch.drain(..) {
                <Self as Writer<R, T, INCREMENTAL>>::write_row(self, update)?;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// Quotes an identifier without interpreting embedded quotes as SQL syntax.
fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Validates the flattened columns and supplies a storage column for an empty
/// tuple. Names beginning with `__flowlog_` belong to the storage format.
fn columns<R: EncodeRow>(names: &[&str]) -> Result<Vec<(String, &'static str)>, RuntimeError> {
    let columns = R::schema(names)?;
    let mut seen = HashSet::new();
    for (name, _) in &columns {
        let folded = name.to_ascii_lowercase();
        if name.is_empty() || name.contains('\0') || folded.starts_with("__flowlog_") {
            return Err(RuntimeError::SqliteSchema(format!(
                "column `{name}` is empty, contains NUL, or uses the reserved __flowlog_ prefix"
            )));
        }
        if !seen.insert(folded) {
            return Err(RuntimeError::SqliteSchema(format!(
                "duplicate column `{name}`"
            )));
        }
    }
    if columns.is_empty() {
        Ok(vec![("__flowlog_present".to_owned(), "INTEGER")])
    } else {
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use lasso::Spur;
    use ordered_float::OrderedFloat;
    use rstest::rstest;
    use rusqlite::Connection;

    use super::SqliteWriter;
    use crate::differential_dataflow::input::InputSession;
    use crate::error::RuntimeError as Error;
    use crate::io::Relation;
    use crate::io::input::Loader;
    use crate::io::input::decode::Decode;
    use crate::io::input::decode::sqlite::SqliteRow;
    use crate::io::output::Emitter;
    use crate::io::output::encode::sqlite::EncodeField;
    use crate::io::output::encode::sqlite::SqliteEncoder;
    use crate::timely;

    struct Rows;
    impl Relation for Rows {
        const NAME: &'static str = "Rows";
        const ARITY: usize = 1;
        const ORDERED: bool = true;
        const LIMIT: Option<usize> = Some(1);
        type Tuple = (i32,);
    }

    #[test]
    fn incremental_history_preserves_all_changes_and_resets_on_a_new_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows.sqlite");
        let emitter = Emitter::<Rows, u32>::new();
        let worker = emitter.worker();
        let mut outputs = SqliteWriter::default();
        for (time, diff) in [(0, 1), (1, -1)] {
            worker.record(&(7,), &time, diff);
            worker.record(&(8,), &time, diff);
            worker.publish();
            outputs
                .write(std::slice::from_ref(&path), |_, tx, reset| {
                    emitter.emit_sqlite::<true>(tx, &["value"], reset)
                })
                .unwrap();
        }
        let connection = Connection::open(&path).unwrap();
        let mut query = connection
            .prepare("SELECT value, __flowlog_timestamp, __flowlog_insert FROM Rows ORDER BY 2, 1")
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            [(7, 0, true), (8, 0, true), (7, 1, false), (8, 1, false)]
        );
        drop(query);
        SqliteWriter::default()
            .write(&[path], |_, tx, reset| {
                emitter.emit_sqlite::<true>(tx, &["value"], reset)
            })
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM Rows", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn batch_output_limits_rows_and_loader_reads_only_once_across_workers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows.sqlite");
        let emitter = Emitter::<Rows, ()>::new();
        let worker = emitter.worker();
        worker.record(&(8,), &(), 1);
        worker.record(&(7,), &(), 1);
        worker.publish();
        SqliteWriter::default()
            .write(std::slice::from_ref(&path), |_, tx, reset| {
                emitter.emit_sqlite::<false>(tx, &["value"], reset)
            })
            .unwrap();
        for index in 0..3 {
            let path = path.clone();
            let updates = timely::execute_directly(move |worker| {
                let seen = Rc::new(RefCell::new(Vec::new()));
                let mut session = InputSession::<u32, (i32,), i32>::new();
                let probe = worker.dataflow::<u32, _, _>(|scope| {
                    let seen = Rc::clone(&seen);
                    let (probe, _) = session
                        .to_collection(scope)
                        .inspect(move |update| seen.borrow_mut().push(*update))
                        .probe();
                    probe
                });
                let mut loader = Loader::<Rows, u32, i32>::new(session, 3, index, false).unwrap();
                loader.load_sqlite(&path, &["value"], -1).unwrap();
                loader.close();
                worker.step_while(|| !probe.done());
                seen.take()
            });
            assert_eq!(
                updates,
                if index == 0 {
                    vec![((7,), 0, -1)]
                } else {
                    vec![]
                }
            );
        }
    }

    #[test]
    fn failing_a_later_relation_rolls_back_the_whole_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TABLE Rows(value INTEGER); INSERT INTO Rows VALUES (99); CREATE TABLE Untouched(value INTEGER); INSERT INTO Untouched VALUES (42);").unwrap();
        let emitter = Emitter::<Rows, u32>::new();
        let worker = emitter.worker();
        worker.record(&(7,), &0, 1);
        worker.publish();
        let mut outputs = SqliteWriter::default();
        let error = outputs
            .write(
                &[path.clone(), dir.path().join("./rows.sqlite")],
                |index, tx, reset| {
                    if index == 0 {
                        emitter.emit_sqlite::<true>(tx, &["value"], reset)
                    } else {
                        Err(Error::SqliteWeight {
                            relation: "Other",
                            diff: 2,
                        })
                    }
                },
            )
            .unwrap_err();
        let Error::SqlitePath { source, .. } = error else {
            panic!("database path context");
        };
        assert!(matches!(
            *source,
            Error::SqliteWeight {
                relation: "Other",
                diff: 2
            }
        ));
        assert_eq!(
            connection
                .query_row("SELECT value FROM Rows", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            99
        );
        assert_eq!(
            connection
                .query_row("SELECT value FROM Untouched", [], |row| row
                    .get::<_, i32>(0))
                .unwrap(),
            42
        );
        outputs
            .write(&[path], |_, tx, reset| {
                assert!(reset);
                emitter.emit_sqlite::<true>(tx, &["value"], reset)
            })
            .unwrap();
    }

    #[test]
    fn encoding_failure_restores_the_previous_table() {
        struct Unsigned;
        impl Relation for Unsigned {
            const NAME: &'static str = "Unsigned";
            const ARITY: usize = 1;
            type Tuple = (u64,);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE Unsigned(value INTEGER); INSERT INTO Unsigned VALUES (99)")
            .unwrap();
        let emitter = Emitter::<Unsigned, ()>::new();
        let worker = emitter.worker();
        worker.record(&(1,), &(), 1);
        worker.record(&(u64::MAX,), &(), 1);
        worker.publish();

        let error = SqliteWriter::default()
            .write(&[path], |_, tx, reset| {
                emitter.emit_sqlite::<false>(tx, &["value"], reset)
            })
            .unwrap_err();

        let Error::SqlitePath { source, .. } = error else {
            panic!("database path context");
        };
        assert!(matches!(
            *source,
            Error::SqliteValue {
                column: 0,
                expected: "a signed 64-bit SQLite integer",
                ..
            }
        ));
        let mut statement = connection.prepare("SELECT value FROM Unsigned").unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![99]);
    }

    #[rstest]
    #[case(0)]
    #[case(2)]
    #[case(-2)]
    fn non_set_weights_are_rejected(#[case] diff: i32) {
        let mut connection = Connection::open_in_memory().unwrap();
        let tx = connection.transaction().unwrap();
        let emitter = Emitter::<Rows, u32>::new();
        let worker = emitter.worker();
        worker.record(&(7,), &0, diff);
        worker.publish();
        let error = emitter
            .emit_sqlite::<true>(&tx, &["value"], true)
            .unwrap_err();
        assert!(
            matches!(error, Error::SqliteWeight { relation: "Rows", diff: actual } if actual == diff)
        );
    }

    #[test]
    fn nested_values_and_interned_strings_round_trip_without_text_formatting() {
        let connection = Connection::open_in_memory().unwrap();
        let input = (
            (i64::MIN, u32::MAX),
            (true, OrderedFloat(2.5_f64)),
            " \u{e9}\0\t\n ".to_owned(),
        );
        let mut query = connection.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        input.write(&mut SqliteEncoder::new(&mut query)).unwrap();
        let mut rows = query.raw_query();
        let row = rows.next().unwrap().unwrap();
        let decoded =
            <((i64, u32), (bool, OrderedFloat<f64>), String)>::decode(&SqliteRow(row)).unwrap();
        assert_eq!(
            decoded,
            (
                (i64::MIN, u32::MAX),
                (true, OrderedFloat(2.5_f64)),
                " \u{e9}\0\t\n ".to_owned()
            )
        );
        let interned =
            <((i64, u32), (bool, OrderedFloat<f64>), Spur)>::decode(&SqliteRow(row)).unwrap();
        let mut output = connection.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
        interned
            .write(&mut SqliteEncoder::new(&mut output))
            .unwrap();
        let mut rows = output.raw_query();
        let text: String = rows.next().unwrap().unwrap().get(4).unwrap();
        assert_eq!(text, " \u{e9}\0\t\n ".to_owned());
    }

    #[test]
    fn nullary_output_records_presence_and_retraction() {
        struct Flag;
        impl Relation for Flag {
            const NAME: &'static str = "Flag";
            const ARITY: usize = 0;
            type Tuple = ();
        }
        let emitter = Emitter::<Flag, u32>::new();
        let worker = emitter.worker();
        worker.record(&(), &0, 1);
        worker.record(&(), &1, -1);
        worker.publish();
        let mut connection = Connection::open_in_memory().unwrap();
        let tx = connection.transaction().unwrap();
        emitter.emit_sqlite::<true>(&tx, &[], true).unwrap();
        tx.commit().unwrap();
        let mut query = connection
            .prepare("SELECT * FROM Flag ORDER BY __flowlog_timestamp")
            .unwrap();
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, i32>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, [(1, 0, 1), (1, 1, 0)]);
    }
}
