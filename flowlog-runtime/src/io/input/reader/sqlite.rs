//! SQLite snapshot queries feeding the borrowed row decoder, through one
//! connection per worker and input database.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::path::PathBuf;

use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::params_from_iter;

use crate::error::RuntimeError;
use crate::intern::dictionary;
use crate::io::Relation;
use crate::io::SqliteLayout;
use crate::io::configure_sqlite;
use crate::io::input::decode::Decode;
use crate::io::input::decode::sqlite::ReadSqlite;
use crate::io::input::decode::sqlite::SqliteRow;
use crate::io::sqlite_identifier;

/// Reads a worker's rowid range, using sequential rowid traversal for `ord`.
/// Sources without an accessible rowid use worker zero. Input must remain
/// unchanged during loading. Missing files are never created.
///
/// A database whose symbol table this run loaded, and which stores the
/// relation in the dictionary layout (an `_R` table of ids), is read from
/// that table by position; any other is read by attribute name, through
/// the layout's view `R` or a plain table.
pub(crate) fn read<R: Relation>(
    path: &Path,
    names: &[&str],
    peers: usize,
    index: usize,
    uses_ord: bool,
    apply: impl FnMut(R::Tuple),
) -> Result<(), RuntimeError>
where
    R::Tuple: ReadSqlite,
{
    if names.len() != R::Tuple::COLUMNS {
        return Err(RuntimeError::SqliteSchema(format!(
            "{} attribute names for {} columns",
            names.len(),
            R::Tuple::COLUMNS,
        )));
    }
    let mut seen = HashSet::new();
    for name in names {
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
    DATABASES.with(|databases| {
        let mut databases = databases.borrow_mut();
        let database = match databases.entry(path.to_path_buf()) {
            Entry::Occupied(open) => open.into_mut(),
            Entry::Vacant(closed) => closed.insert(Database::open(path)?),
        };
        database.refresh()?;
        let loaded = dictionary::is_loaded(path);
        read_from::<R>(database, names, peers, index, uses_ord, loaded, apply)
    })
}

thread_local! {
    /// The worker's connection to each input database it has loaded from,
    /// with the database's schema, kept for the relations it loads next.
    // A connection parses the schema on its first statement, and a schema
    // pragma walks it: on a database of 232 tables, views and indexes that
    // is 5 ms per connection against 0.2 ms per relation on an open one,
    // and gdb samples of 32 workers opening a connection per relation found
    // nearly every worker in SQLite's allocator waiting on its mutex (see
    // `io::configure_sqlite`).
    static DATABASES: RefCell<HashMap<PathBuf, Database>> = RefCell::new(HashMap::new());
}

/// An open input database, read only, and the tables its schema holds.
struct Database {
    connection: Connection,
    /// The schema's version when `tables` was read, so a database replaced
    /// between loads is read anew.
    schema_version: i64,
    /// Each table, by lower-cased name, and whether it is an ordinary table:
    /// not a view, and with a rowid.
    tables: HashMap<String, bool>,
}

impl Database {
    fn open(path: &Path) -> Result<Self, RuntimeError> {
        // The process-wide setting must precede its first connection.
        configure_sqlite();
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self {
            connection,
            // No schema has a negative version, so the first refresh reads.
            schema_version: -1,
            tables: HashMap::new(),
        })
    }

    /// Reads the table list again if the schema changed since.
    fn refresh(&mut self) -> Result<(), RuntimeError> {
        let version: i64 = self
            .connection
            .query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        if version == self.schema_version {
            return Ok(());
        }
        let mut statement = self.connection.prepare(
            "SELECT name, type = 'table' AND wr = 0 FROM pragma_table_list WHERE schema = 'main' AND type IN ('table', 'view')",
        )?;
        let tables = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?.to_ascii_lowercase(), row.get(1)?))
        })?;
        self.tables = tables.collect::<Result<_, _>>()?;
        self.schema_version = version;
        Ok(())
    }

    /// Returns `true` if the database has a table or view named `name`.
    #[must_use]
    fn has(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_ascii_lowercase())
    }
}

/// Reads the relation's rows from an open `database`; see [`read`].
/// `loaded` says whether the database's symbol table is the run's
/// dictionary.
fn read_from<R: Relation>(
    database: &mut Database,
    names: &[&str],
    peers: usize,
    index: usize,
    uses_ord: bool,
    loaded: bool,
    mut apply: impl FnMut(R::Tuple),
) -> Result<(), RuntimeError>
where
    R::Tuple: ReadSqlite,
{
    let shadow = format!("_{}", R::NAME);
    let layout = if loaded && database.has(&shadow) {
        SqliteLayout::Dictionary
    } else {
        SqliteLayout::Plain
    };
    let tables = &database.tables;
    let transaction = database.connection.transaction()?;
    let source = match layout {
        SqliteLayout::Dictionary => shadow.as_str(),
        SqliteLayout::Plain => R::NAME,
    };
    let table = sqlite_identifier(source);
    // Qualifying names prevents SQLite's legacy double-quoted string fallback
    // from turning a missing column into a constant string.
    let column = |name: &str| format!("{table}.{}", sqlite_identifier(name));
    let selected = match layout {
        // The dictionary layout names the columns of `_R` by position.
        SqliteLayout::Dictionary => (0..names.len())
            .map(|position| column(&position.to_string()))
            .collect::<Vec<_>>()
            .join(", "),
        SqliteLayout::Plain => names
            .iter()
            .map(|name| column(name))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let mut sql = if selected.is_empty() {
        format!("SELECT 1 FROM {table}")
    } else {
        format!("SELECT {selected} FROM {table}")
    };
    let mut bounds = None;
    if uses_ord {
        if let Some(key) = rowid(&transaction, tables, source)? {
            // Request the rowid index order so a covering column index cannot
            // change interning order. SQLite can traverse it without sorting.
            sql.push_str(&format!(" ORDER BY {table}.{}", sqlite_identifier(key)));
        }
    } else if peers > 1 {
        if let Some(key) = rowid(&transaction, tables, source)? {
            let key = format!("{table}.{}", sqlite_identifier(key));
            // Separate extrema queries let SQLite seek each end of the rowid
            // index. Combining MIN and MAX in one aggregate scans the table.
            let (first, last): (Option<i64>, Option<i64>) = transaction.query_row(
                &format!(
                    "SELECT (SELECT MIN({key}) FROM {table}), (SELECT MAX({key}) FROM {table})"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if let (Some(first), Some(last)) = (first, last) {
                // Split the integer span, not the row count: this avoids a
                // counting scan but sparse keys can leave uneven worker loads.
                // Widen before arithmetic to include both signed extremes.
                let span = i128::from(last) - i128::from(first) + 1;
                let peers = peers as i128;
                let index = index as i128;
                let width = span / peers;
                let extra = span % peers;
                let start = i128::from(first) + width * index + index.min(extra);
                let end = start + width + i128::from(index < extra);
                if start < end {
                    bounds = Some([start as i64, (end - 1) as i64]);
                }
            }
            if bounds.is_some() {
                sql.push_str(&format!(" WHERE {key} BETWEEN ? AND ?"));
            } else {
                sql.push_str(" WHERE 0");
            }
        } else if index != 0 {
            return Ok(());
        }
    }
    let mut statement = transaction.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(bounds.into_iter().flatten()))?;
    while let Some(row) = rows.next()? {
        let tuple = match layout {
            SqliteLayout::Dictionary => R::Tuple::read::<true>(row, &mut 0)?,
            SqliteLayout::Plain => R::Tuple::decode(&SqliteRow(row))?,
        };
        apply(tuple);
    }
    Ok(())
}

/// Finds an unshadowed alias of the rowid index on an ordinary table: one
/// that `tables` maps to `true` under its lower-cased name.
fn rowid(
    connection: &Connection,
    tables: &HashMap<String, bool>,
    table: &str,
) -> Result<Option<&'static str>, RuntimeError> {
    if !tables
        .get(&table.to_ascii_lowercase())
        .copied()
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let mut statement = connection.prepare("SELECT name FROM pragma_table_xinfo(?)")?;
    let names = statement
        .query_map([table], |row| {
            Ok(row.get::<_, String>(0)?.to_ascii_lowercase())
        })?
        .collect::<Result<HashSet<_>, _>>()?;
    Ok(["rowid", "_rowid_", "oid"]
        .into_iter()
        .find(|name| !names.contains(*name)))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    struct Rows;

    impl Relation for Rows {
        const NAME: &'static str = "Rows";
        const ARITY: usize = 1;
        type Tuple = (i64,);
    }

    #[rstest]
    #[case::dense(
        "INSERT INTO Rows(rowid, value) VALUES (1, 10), (2, 10), (3, 30), (4, 40)",
        vec![vec![(10,), (10,)], vec![(30,), (40,)]]
    )]
    #[case::compact_with_remainder(
        "INSERT INTO Rows(rowid, value) VALUES (7, 10), (8, 20), (9, 30), (10, 40), (11, 50)",
        vec![vec![(10,), (20,), (30,)], vec![(40,), (50,)]]
    )]
    #[case::negative_and_sparse(
        "INSERT INTO Rows(rowid, value) VALUES (-8, 10), (-7, 20), (4, 30), (5, 40)",
        vec![vec![(10,), (20,)], vec![(30,), (40,)]]
    )]
    #[case::signed_extremes(
        "INSERT INTO Rows(rowid, value) VALUES (-9223372036854775808, 10), (-1, 20), (0, 30), (9223372036854775807, 40)",
        vec![vec![(10,), (20,)], vec![(30,), (40,)]]
    )]
    #[case::empty("", vec![vec![], vec![]])]
    #[case::one_row(
        "INSERT INTO Rows(rowid, value) VALUES (7, 10)",
        vec![vec![(10,)], vec![]]
    )]
    fn concurrent_ranges_cover_each_row_once(
        #[case] insert: &str,
        #[case] expected: Vec<Vec<(i64,)>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE Rows(value INTEGER)")
            .unwrap();
        connection.execute_batch(insert).unwrap();
        drop(connection);

        let actual = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..2)
                .map(|index| {
                    let path = &path;
                    scope.spawn(move || {
                        let mut rows = Vec::new();
                        read::<Rows>(path, &["value"], 2, index, false, |row| rows.push(row))
                            .unwrap();
                        rows.sort();
                        rows
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::shadowed_rowid(
        "CREATE TABLE Rows(rowid INTEGER, value INTEGER); INSERT INTO Rows VALUES (0, 10), (0, 20)",
        vec![vec![(10,)], vec![(20,)]]
    )]
    #[case::all_aliases_shadowed(
        "CREATE TABLE Rows(rowid INTEGER, _rowid_ INTEGER, oid INTEGER, value INTEGER); INSERT INTO Rows VALUES (0, 0, 0, 10), (0, 0, 0, 20)",
        vec![vec![(10,), (20,)], vec![]]
    )]
    #[case::without_rowid(
        "CREATE TABLE Rows(value INTEGER PRIMARY KEY) WITHOUT ROWID; INSERT INTO Rows VALUES (10), (20)",
        vec![vec![(10,), (20,)], vec![]]
    )]
    #[case::view(
        "CREATE TABLE Source(value INTEGER); INSERT INTO Source VALUES (10), (20); CREATE VIEW Rows AS SELECT value FROM Source",
        vec![vec![(10,), (20,)], vec![]]
    )]
    fn rowid_aliases_and_fallbacks_preserve_exactly_once_loading(
        #[case] schema: &str,
        #[case] expected: Vec<Vec<(i64,)>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.sqlite");
        Connection::open(&path)
            .unwrap()
            .execute_batch(schema)
            .unwrap();
        let actual: Vec<_> = (0..2)
            .map(|index| {
                let mut rows = Vec::new();
                read::<Rows>(&path, &["value"], 2, index, false, |row| rows.push(row)).unwrap();
                rows.sort();
                rows
            })
            .collect();

        assert_eq!(actual, expected);
    }

    #[test]
    fn ord_reads_in_rowid_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.sqlite");
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE Rows(value INTEGER); INSERT INTO Rows VALUES (30), (10), (20); CREATE INDEX by_value ON Rows(value)",
            )
            .unwrap();
        let mut rows = Vec::new();

        read::<Rows>(&path, &["value"], 1, 0, true, |row| rows.push(row)).unwrap();

        assert_eq!(rows, vec![(30,), (10,), (20,)]);
    }
    #[test]
    fn missing_database_is_not_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.sqlite");
        let error = read::<Rows>(&path, &["value"], 1, 0, false, |_| {}).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::Sqlite(rusqlite::Error::SqliteFailure(_, _))
        ));
        assert!(!path.exists());
    }

    /// The worker keeps its connection between loads, so a schema change in
    /// between, here the table appearing, is seen by the next load.
    #[test]
    fn a_table_created_after_the_first_load_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("late.sqlite");
        Connection::open(&path).unwrap();
        let error = read::<Rows>(&path, &["value"], 1, 0, false, |_| {}).unwrap_err();
        assert!(error.to_string().contains("no such table"), "{error}");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE Rows(value INTEGER); INSERT INTO Rows VALUES (5)")
            .unwrap();
        let mut rows = Vec::new();
        read::<Rows>(&path, &["value"], 1, 0, false, |row| rows.push(row)).unwrap();
        assert_eq!(rows, [(5,)]);
    }

    #[rstest]
    #[case(vec!["missing"], "no such column: Rows.missing")]
    #[case(vec!["__FLOWLOG_timestamp"], "invalid SQLite schema: column `__FLOWLOG_timestamp` is empty, contains NUL, or uses the reserved __flowlog_ prefix")]
    #[case(vec![], "invalid SQLite schema: 0 attribute names for 1 columns")]
    fn invalid_input_schemas_are_reported(#[case] names: Vec<&str>, #[case] message: &str) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE Rows(value INTEGER); INSERT INTO Rows VALUES (7)")
            .unwrap();
        let error = read::<Rows>(&path, &names, 1, 0, false, |_| {}).unwrap_err();
        assert!(error.to_string().starts_with(message), "{error}");
    }
}
