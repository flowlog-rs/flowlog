//! The share-cursor contract, and one reader per kind of source.
//!
//! Named for where the facts come from, not how they are spelled: two of
//! these read the same delimited text, and which format a record arrives in
//! is `decode`'s axis instead.
//!
//! - `file`: a delimited text file, split across workers by byte range.
//! - `put`: one broadcast `put` tuple, owned by the worker it hashes to.
//! - `host`: rows a host program supplied, split by index range.

pub(crate) mod file;
pub(crate) mod host;
pub(crate) mod put;

use crate::error::RuntimeError;
use crate::io::spec::InputSpec;

/// A worker's cursor over its share of one relation's input, yielding the
/// relation's slot tuples directly.
///
/// `T` is the slot tuple; how a source becomes one is each reader's own
/// business (text parses, host rows convert per position). The outer
/// `Result` is the cursor: an error there stops the load, because a source
/// that failed to produce a row makes no forward-progress promise. The
/// inner `Result` is one row: text can refuse a row and the load skips it.
///
/// `ord` exposes interner keys as numbers, so interning order must not
/// vary with worker count. Every `open` owes the same collapse: under
/// `uses_ord`, worker 0 takes the whole share and the rest take `None`,
/// decided before anything is opened so a missing input is still reported
/// exactly once.
pub(crate) trait Reader<'src, T>: Sized {
    /// What the [`InputSpec`]'s source slot holds for this reader: a file
    /// path, the rows themselves, or the broadcast line.
    type Source: ?Sized;

    /// Open this worker's share of the source `spec` names, or `None`
    /// when it has none.
    fn open(spec: &InputSpec<'src, Self::Source>) -> Result<Option<Self>, RuntimeError>;

    /// The next row of this worker's share, or `None` at its end.
    #[allow(clippy::type_complexity)]
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError>;
}
