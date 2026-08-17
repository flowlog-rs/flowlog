//! What generated code says about a relation's I/O, for the runtime to
//! open it: [`RelationSpec`], everything a `.decl` fixes, [`InputSpec`],
//! what one worker's read of it adds, and [`OutputSpec`], where a drain
//! puts it.

use std::path::Path;

/// Which storage backs a relation's input, with the knobs only that
/// storage has.
///
/// Names only sources a directive can spell; host-supplied rows enter
/// through the vec reader directly and need no name here.
///
/// Non-exhaustive so a future format is not a breaking release downstream;
/// in-crate matches stay exhaustive.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Format {
    /// A delimited text file.
    Text {
        /// Cell separator, a single byte.
        delim: u8,
        has_header: bool,
    },
}

/// What a relation's `.decl` fixes, independent of any one worker or file.
///
/// Generated code emits one of these per relation and the runtime builds
/// each [`InputSpec`] from it, so a relation's constants are written once
/// as data rather than inlined into every entry point.
#[derive(Debug)]
pub struct RelationSpec {
    /// Relation name, for diagnostics and table lookup.
    pub name: &'static str,
    pub arity: usize,
    /// Cell separator, a single byte.
    pub delim: u8,
    pub format: Format,
    /// How column 0 decides ownership of a `put` tuple.
    pub shard: ShardKey,
    /// Whether the program's results depend on string interning order.
    pub uses_ord: bool,
}

/// Where one worker reads one relation's input from.
///
/// Carries only what a single read adds to the relation's own description:
/// the source, and which share of it this worker takes. `Src` is whatever
/// the reader opens, defaulted to a file path; host-supplied rows use the
/// same spec with their slice in the source slot.
#[derive(Debug)]
pub struct InputSpec<'a, Src: ?Sized = Path> {
    pub rel: &'a RelationSpec,
    pub source: &'a Src,
    pub peers: usize,
    /// This worker's index in `0..peers`.
    pub index: usize,
}

/// Where one relation's drained rows go.
///
/// The write-side counterpart of [`InputSpec`], and shorter for a reason:
/// a read derives this worker's share of the source, where a drain has
/// already been gathered onto one worker and has no share left to answer.
///
/// `ORDER BY` and `LIMIT` are not here. They pick which pump the caller
/// runs rather than describing the sink, and their comparator stays with
/// the generated code that knows the column types.
#[derive(Debug)]
pub struct OutputSpec<'a> {
    /// Relation name as the `.decl` spells it, for diagnostics.
    pub relation: &'a str,
    pub path: &'a str,
    /// Full separator bytes, not one byte: `delimiter=` is any string
    /// literal, so a multi-byte separator is legal.
    pub delim: &'static [u8],
}

/// First-column ownership rule for a `put` tuple.
///
/// Mirrors how the column would decode, because ownership hashes the
/// decoded value: a `u64` column accepts values past `i64::MAX`, and an
/// `f32` hashes its own 32-bit pattern, not the wider one.
#[derive(Debug, Clone, Copy)]
pub enum ShardKey {
    /// Signed integer columns of any width.
    Int,
    /// Unsigned integer columns of any width.
    UInt,
    /// Boolean columns, hashed as 0 or 1.
    Bool,
    F32Bits,
    F64Bits,
    /// String columns read as text.
    Str,
    /// String columns read as interned keys.
    Spur,
}
