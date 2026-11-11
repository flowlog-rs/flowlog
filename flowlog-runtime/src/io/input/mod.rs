//! Reading a relation's rows into the engine.
//!
//! [`Loader`] owns the session, applies update weights, and manages progress.
//! Internally, `reader` selects each worker's share of a source and yields
//! decoded rows. `decode` parses text cells or converts typed host fields
//! into the relation's tuple representation. `with_kept_sqlite_connections`
//! brackets the startup load, within which a worker reads every relation
//! of a SQLite database through one connection.

pub(crate) mod decode;
pub(crate) mod loader;
pub(crate) mod reader;

pub use loader::Loader;
#[cfg(feature = "sqlite")]
pub use reader::sqlite::with_kept_sqlite_connections;
