//! Reading a relation's rows into the engine.
//!
//! [`Relation`] declares the tuple type, metadata, and inline facts.
//! [`Loader`] owns the session, applies update weights, and manages progress.
//! Internally, `reader` selects each worker's share of a source and yields
//! decoded rows. `decode` parses text cells or converts typed host fields
//! into the relation's tuple representation.

pub(crate) mod decode;
pub(crate) mod loader;
pub(crate) mod reader;
pub(crate) mod relation;

pub use loader::Loader;
pub use relation::Relation;
