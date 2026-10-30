//! The interner on disk, as Souffle's `__SymbolTable(id, symbol)`.
//!
//! A run loads the table of at most one of its databases before any other
//! string is interned, so that every loaded string's [`Symbol`] ordinal is
//! its id and a relation table can store ids in place of text. Output
//! appends the strings interned since, so the next run can load the table
//! the same way.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;

use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::Transaction;
use rusqlite::types::ValueRef;

use crate::error::RuntimeError;
use crate::intern::INTERNER;
use crate::intern::Symbol;
use crate::intern::intern;
use crate::io::configure_sqlite;
use crate::io::sqlite_identifier;

/// Name of the symbol table in every database, Souffle's spelling.
pub(crate) const TABLE: &str = "__SymbolTable";

/// The database whose symbol table this run loaded, and its greatest id:
/// every symbol up to it is one of the table's strings, under the table's
/// own number.
#[derive(Debug)]
struct Loaded {
    path: PathBuf,
    numbered: usize,
}

static LOADED: OnceLock<Loaded> = OnceLock::new();

/// Loads the symbol table of the one database among `paths`, the run's
/// SQLite inputs and outputs, that has one, before any worker starts. A
/// path that names no file, or a database without the table, is skipped;
/// the input load reports a missing input later, and output creates a
/// missing database.
///
/// # Errors
///
/// If two databases hold a symbol table, if a run already loaded one, or
/// if the table's ids do not run from 1 without gaps in a process that has
/// interned nothing yet: the ids would not be the symbols.
pub fn load(paths: &[PathBuf]) -> Result<(), RuntimeError> {
    // The process-wide setting must precede its first connection.
    configure_sqlite();
    let mut loaded: Option<Loaded> = None;
    for path in paths {
        let Ok(resolved) = fs::canonicalize(path) else {
            continue;
        };
        if loaded
            .as_ref()
            .is_some_and(|loaded| loaded.path == resolved)
        {
            continue;
        }
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|source| sqlite_path(path, source.into()))?;
        if !has_table(&connection, TABLE).map_err(|source| sqlite_path(path, source))? {
            continue;
        }
        if let Some(loaded) = &loaded {
            return Err(RuntimeError::SqliteDictionary {
                message: format!(
                    "databases '{}' and '{}' both hold one; at most one database of a run may",
                    loaded.path.display(),
                    path.display(),
                ),
            });
        }
        let numbered = load_table(&connection).map_err(|source| sqlite_path(path, source))?;
        loaded = Some(Loaded {
            path: resolved,
            numbered,
        });
    }
    if let Some(loaded) = loaded
        && let Err(loaded) = LOADED.set(loaded)
    {
        return Err(RuntimeError::SqliteDictionary {
            message: format!(
                "'{}' cannot load after a dictionary was already loaded",
                loaded.path.display()
            ),
        });
    }
    Ok(())
}

/// Interns every row of `connection`'s symbol table in id order and returns
/// the greatest id, zero for an empty table, checking that each id became
/// that row's symbol.
fn load_table(connection: &Connection) -> Result<usize, RuntimeError> {
    let table = sqlite_identifier(TABLE);
    let mut statement =
        connection.prepare(&format!("SELECT id, symbol FROM {table} ORDER BY id"))?;
    let mut rows = statement.query([])?;
    let mut numbered = 0;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let text = match row.get_ref(1)? {
            ValueRef::Text(bytes) => {
                std::str::from_utf8(bytes).map_err(|_| RuntimeError::SqliteDictionary {
                    message: format!("id {id} holds a symbol that is not UTF-8 text"),
                })?
            }
            ValueRef::Null | ValueRef::Integer(_) | ValueRef::Real(_) | ValueRef::Blob(_) => {
                return Err(RuntimeError::SqliteDictionary {
                    message: format!("id {id} holds a symbol that is not text"),
                });
            }
        };
        let symbol = intern(text);
        if i64::from(symbol.ordinal()) != id {
            return Err(RuntimeError::SqliteDictionary {
                message: format!(
                    "id {id} holds {text:?}, which is symbol {}: ids must run from 1 without gaps, \
                 and the table must load before any other string is interned",
                    symbol.ordinal()
                ),
            });
        }
        numbered = symbol.index() + 1;
    }
    Ok(numbered)
}

/// Returns how many symbols the loaded symbol table numbers, its greatest
/// id, or `None` if this run loaded none.
#[must_use]
fn loaded_count() -> Option<usize> {
    LOADED.get().map(|loaded| loaded.numbered)
}

/// Returns `true` if `path` is the database whose symbol table this run
/// loaded.
#[must_use]
pub(crate) fn is_loaded(path: &Path) -> bool {
    let Some(loaded) = LOADED.get() else {
        return false;
    };
    fs::canonicalize(path).is_ok_and(|resolved| resolved == loaded.path)
}

/// Returns how many symbols the symbol table of the output database at
/// `path`, open in `transaction`, already numbers: the loaded count for the
/// loaded database, zero for a database without the table. Creates the
/// table so that the views written next can name it.
///
/// # Errors
///
/// If the database holds a symbol table this run did not load, because
/// another database's was: its ids would not be the symbols the relation
/// tables store.
pub(crate) fn prepare(transaction: &Transaction<'_>, path: &Path) -> Result<usize, RuntimeError> {
    if is_loaded(path) {
        return Ok(loaded_count().expect("a loaded path has a count"));
    }
    if has_table(transaction, TABLE)? {
        return Err(RuntimeError::SqliteDictionary {
            message: "the database holds one that this run did not load, so its ids are not this \
                      run's symbols; a run loads one database's table, and another database \
                      held it"
                .to_owned(),
        });
    }
    create(transaction)?;
    Ok(0)
}

/// Appends every symbol from position `written` on to `transaction`'s symbol
/// table and returns how many symbols the table numbers now. Runs while no
/// thread interns, after the workers have published, so the walk sees every
/// symbol and the count is exact.
pub(crate) fn append(transaction: &Transaction<'_>, written: usize) -> Result<usize, RuntimeError> {
    create(transaction)?;
    let table = sqlite_identifier(TABLE);
    let mut insert =
        transaction.prepare(&format!("INSERT INTO {table} (id, symbol) VALUES (?, ?)"))?;
    for (symbol, text) in INTERNER.texts_from(written) {
        insert.execute((symbol.ordinal(), text))?;
    }
    Ok(INTERNER.len())
}

fn create(transaction: &Transaction<'_>) -> Result<(), RuntimeError> {
    let table = sqlite_identifier(TABLE);
    transaction.execute(
        &format!("CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE)"),
        [],
    )?;
    Ok(())
}

/// Returns `true` if `connection`'s main schema has a table named `name`.
fn has_table(connection: &Connection, name: &str) -> Result<bool, RuntimeError> {
    let exists = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_list WHERE schema = 'main' AND name = ? COLLATE NOCASE AND type = 'table')",
        [name],
        |row| row.get(0),
    )?;
    Ok(exists)
}

/// Returns `true` if `symbol` numbers a string of the loaded symbol table.
#[must_use]
pub(crate) fn numbers_loaded(symbol: Symbol) -> bool {
    loaded_count().is_some_and(|numbered| symbol.index() < numbered)
}

fn sqlite_path(path: &Path, source: RuntimeError) -> RuntimeError {
    RuntimeError::SqlitePath {
        path: path.to_path_buf(),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Relation;
    use crate::io::SqliteLayout;
    use crate::io::input::reader::sqlite::read;
    use crate::io::output::Emitter;
    use crate::io::output::SqliteWriter;

    /// `load` interns into the process-wide interner, so a test's table
    /// numbers its rows by the symbols its texts already have; the identity
    /// check then holds exactly when the table lists them in order.
    fn table_of_interned(connection: &Connection, texts: &[&str]) {
        connection
            .execute_batch(
                "CREATE TABLE __SymbolTable (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE)",
            )
            .unwrap();
        for text in texts {
            connection
                .execute(
                    "INSERT INTO __SymbolTable (id, symbol) VALUES (?, ?)",
                    (intern(text).ordinal(), text),
                )
                .unwrap();
        }
    }

    #[test]
    fn load_accepts_a_table_whose_ids_are_its_rows_symbols() {
        let connection = Connection::open_in_memory().unwrap();
        table_of_interned(&connection, &["dict-a", "dict-b"]);
        assert_eq!(
            load_table(&connection).unwrap(),
            intern("dict-b").index() + 1
        );
    }

    #[test]
    fn load_rejects_an_id_that_is_not_its_rows_symbol() {
        let connection = Connection::open_in_memory().unwrap();
        table_of_interned(&connection, &["dict-c"]);
        // No symbol has ordinal 0, so this row cannot be its id.
        connection
            .execute_batch("INSERT INTO __SymbolTable (id, symbol) VALUES (0, 'dict-d')")
            .unwrap();
        let error = load_table(&connection).unwrap_err();
        assert!(
            matches!(&error, RuntimeError::SqliteDictionary { message } if message.contains("without gaps")),
            "{error}"
        );
    }

    #[test]
    fn load_rejects_a_symbol_that_is_not_text() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE __SymbolTable (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE); \
                 INSERT INTO __SymbolTable (id, symbol) VALUES (1, NULL)",
            )
            .unwrap();
        let error = load_table(&connection).unwrap_err();
        assert!(
            matches!(&error, RuntimeError::SqliteDictionary { message } if message.contains("not text")),
            "{error}"
        );
    }

    #[test]
    fn append_writes_the_symbols_from_a_position_on() {
        let first = intern("append-a");
        let later = [intern("append-b"), intern("append-c")];
        let mut connection = Connection::open_in_memory().unwrap();
        let transaction = connection.transaction().unwrap();
        let numbered = append(&transaction, first.index() + 1).unwrap();
        assert!(numbered > later[1].index());
        let stored: Vec<(u32, String)> = transaction
            .prepare("SELECT id, symbol FROM __SymbolTable ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for (symbol, text) in later.iter().zip(["append-b", "append-c"]) {
            assert!(
                stored.contains(&(symbol.ordinal(), text.to_owned())),
                "{stored:?}"
            );
        }
        assert!(!stored.iter().any(|(_, text)| text == "append-a"));
    }

    #[test]
    fn prepare_refuses_a_symbol_table_this_run_did_not_load() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE __SymbolTable (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE)",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        let error = prepare(&transaction, Path::new("/nonexistent/output.sqlite")).unwrap_err();
        assert!(
            matches!(error, RuntimeError::SqliteDictionary { .. }),
            "{error}"
        );
    }

    #[test]
    fn prepare_creates_the_table_for_a_fresh_database() {
        let mut connection = Connection::open_in_memory().unwrap();
        let transaction = connection.transaction().unwrap();
        assert_eq!(
            prepare(&transaction, Path::new("/nonexistent/output.sqlite")).unwrap(),
            0
        );
        assert!(has_table(&transaction, TABLE).unwrap());
    }

    #[test]
    fn load_refuses_two_databases_with_a_symbol_table() {
        let dir = tempfile::tempdir().unwrap();
        let paths = [
            dir.path().join("first.sqlite"),
            dir.path().join("second.sqlite"),
        ];
        for (path, text) in paths.iter().zip(["two-a", "two-b"]) {
            table_of_interned(&Connection::open(path).unwrap(), &[text]);
        }
        let error = load(&paths).unwrap_err();
        assert!(
            matches!(&error, RuntimeError::SqliteDictionary { message } if message.contains("both hold one")),
            "{error}"
        );
    }

    struct Named;
    impl Relation for Named {
        const NAME: &'static str = "Named";
        const ARITY: usize = 2;
        type Tuple = (Symbol, i32);
    }

    /// The process loads one symbol table, so one test covers what follows
    /// from loading: ids read as symbols from `_R`, a foreign id refused,
    /// and the loaded database extended rather than rewritten.
    #[test]
    fn the_loaded_database_reads_ids_as_symbols_and_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("loaded.sqlite");
        let (alpha, beta) = (intern("loaded-alpha"), intern("loaded-beta"));
        {
            let connection = Connection::open(&path).unwrap();
            table_of_interned(&connection, &["loaded-alpha", "loaded-beta"]);
            connection
                .execute_batch(
                    "CREATE TABLE _Named(\"0\" INTEGER NOT NULL, \"1\" INTEGER NOT NULL)",
                )
                .unwrap();
            for (symbol, number) in [(alpha, 1), (beta, 2)] {
                connection
                    .execute(
                        "INSERT INTO _Named VALUES (?, ?)",
                        (symbol.ordinal(), number),
                    )
                    .unwrap();
            }
        }
        load(&[path.clone(), path.clone()]).unwrap();
        assert!(is_loaded(&path));
        assert_eq!(loaded_count(), Some(beta.index() + 1));

        let mut rows = Vec::new();
        read::<Named>(&path, &["name", "number"], 1, 0, false, |row| {
            rows.push(row)
        })
        .unwrap();
        rows.sort();
        assert_eq!(rows, [(alpha, 1), (beta, 2)]);

        let unloaded = intern("loaded-gamma");
        assert!(!numbers_loaded(unloaded));
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute("INSERT INTO _Named VALUES (?, 3)", [unloaded.ordinal()])
                .unwrap();
        }
        let error = read::<Named>(&path, &["name", "number"], 1, 0, false, |_| {}).unwrap_err();
        assert!(
            matches!(
                error,
                RuntimeError::SqliteValue {
                    column: 0,
                    expected: "an id of the loaded symbol table",
                    ..
                }
            ),
            "{error}"
        );

        let mut connection = Connection::open(&path).unwrap();
        let transaction = connection.transaction().unwrap();
        assert_eq!(prepare(&transaction, &path).unwrap(), beta.index() + 1);
        drop(transaction);

        // Writing back extends the table from the loaded count on and
        // replaces the relation in place.
        let emitter = Emitter::<Named, ()>::new();
        let worker = emitter.worker();
        worker.record(&(unloaded, 4), &(), 1);
        worker.publish();
        SqliteWriter::default()
            .write(
                std::slice::from_ref(&path),
                SqliteLayout::Dictionary,
                |_, tx, reset| {
                    emitter.emit_sqlite::<false>(
                        tx,
                        &["name", "number"],
                        reset,
                        SqliteLayout::Dictionary,
                    )
                },
            )
            .unwrap();
        let symbols: Vec<(u32, String)> = connection
            .prepare("SELECT id, symbol FROM __SymbolTable WHERE id > ? ORDER BY id")
            .unwrap()
            .query_map([beta.ordinal()], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            symbols.contains(&(unloaded.ordinal(), "loaded-gamma".to_owned())),
            "{symbols:?}"
        );
        let named: Vec<(String, i32)> = connection
            .prepare("SELECT name, number FROM Named")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(named, [("loaded-gamma".to_owned(), 4)]);
    }
}
