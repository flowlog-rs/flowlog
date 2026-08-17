//! Reading a relation's rows into the engine.
//!
//! - [`ingest`]: the API, one handler per relation.
//! - `reader`: this worker's share of a source, one per kind (private).
//! - [`decode`]: how a source's record becomes a slot tuple.
//! - [`session`]: owning a closable differential input session.

pub mod decode;
pub mod ingest;
pub(crate) mod reader;
pub mod session;

pub use ingest::Ingest;
