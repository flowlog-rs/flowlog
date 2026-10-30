//! Relation declarations, input loading, output sinks, and atomic writing.
//!
//! [`Relation`] declares relations independently of their I/O. [`input`]
//! owns loading and [`output`] owns output conversion and writing.
//! [`write_atomic`] replaces completed files without exposing a partial write.
//! [`SqliteLayout`] is the shape both sides give a SQLite database,
//! `configure_sqlite` the process-wide setting both apply first, and
//! `sqlite_identifier` the quoting both use.

#[cfg(feature = "sqlite")]
use std::sync::Once;

pub mod input;
pub mod output;
mod relation;

pub use output::atomic::write_atomic;
pub use relation::Relation;

/// How a SQLite database stores its relations.
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteLayout {
    /// One table per relation, named after it, holding text and numbers
    /// under the attribute names.
    Plain,
    /// One table `_R` per relation, its columns named by position, holding
    /// symbol ids and numbers; a view `R` resolving the ids through the
    /// dictionary `__SymbolTable` under the attribute names. This is the
    /// layout Souffle reads and writes.
    Dictionary,
}

/// Quotes a SQLite identifier without interpreting embedded quotes as SQL
/// syntax.
#[cfg(feature = "sqlite")]
#[must_use]
pub(crate) fn sqlite_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(feature = "sqlite")]
static SQLITE_CONFIGURED: Once = Once::new();

/// Configures SQLite for the runtime, once per process and before its first
/// connection: memory statistics are turned off. A process that opened a
/// connection before the runtime keeps its own configuration, since SQLite
/// refuses the change then, and runs with the statistics on.
// With the statistics on, every allocation and free in SQLite takes one
// process-wide mutex. The workers open a connection each and parse the
// database's schema, which allocates per object. gdb samples of 32 workers
// loading 104 relations from a database of 232 tables, views and indexes,
// with a connection per relation and worker, found nearly every worker in
// `sqlite3Malloc` under `pthread_mutex_lock`. The reader now keeps one
// connection per worker and database, and this setting removes the mutex
// from the allocations that remain; the two were not measured apart.
#[cfg(feature = "sqlite")]
pub(crate) fn configure_sqlite() {
    SQLITE_CONFIGURED.call_once(|| {
        // SAFETY: `sqlite3_config` takes an option code and, for
        // `SQLITE_CONFIG_MEMSTATUS`, one `int`, reading nothing else. It
        // runs before this process opens its first connection, or returns
        // `SQLITE_MISUSE` without effect if one is open. No safe binding
        // exposes it.
        unsafe {
            rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0_i32);
        }
    });
}
