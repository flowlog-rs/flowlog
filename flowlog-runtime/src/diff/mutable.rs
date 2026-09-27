//! [`Mutable`]: signed counts for relations with insertions and deletions.

/// Signed multiplicity for a relation that accepts insertions and deletions.
///
/// Any signed integer ring would do: after a dedup, operators test only
/// whether a count is positive. `i32` is the width every mutable collection
/// uses today.
pub type Mutable = i32;
