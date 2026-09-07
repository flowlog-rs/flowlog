//! Reading a relation's rows into the engine.
//!
//! - [`relation`]: the tuple type, declaration metadata, and inline facts.
//! - [`loader`]: the API, one `Loader` per relation.
//! - [`reader`]: this worker's share of a source, one per kind.
//! - [`decode`]: how a source's record becomes a slot tuple.

pub(crate) mod decode;
pub(crate) mod loader;
pub(crate) mod reader;
pub(crate) mod relation;
