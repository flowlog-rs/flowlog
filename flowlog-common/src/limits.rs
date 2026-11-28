//! Limits on the programs FlowLog accepts. Each is picked rather than
//! derived, trading what programs may do against what they cost, and
//! records why it holds its value and what must change with it.

/// The most columns one row may hold: a relation's declared columns, a
/// tuple's fields, declared as a type or written as an expression, or
/// either side, key or value, of a collection the planner builds. A wider
/// row is refused with a diagnostic.
// Picked rather than measured. Each width past 12 is a
// `RowN` struct, with its trait impls, that flowlog-runtime compiles, so
// raising the limit lengthens the runtime's build and lowering it refuses
// more programs. The widest shape `for_each_row!` lists in flowlog-runtime's
// `row` module must match it; a test there checks that.
pub const MAX_ROW_WIDTH: usize = 64;

/// The most columns a table of an e-graph may hold. The rebuild packs every
/// table's row into one fixed array, so each column past a table's own
/// costs every row of every table. Four holds a binary operator's two
/// children, its class, and one more. flowlog-runtime's `eqsat::ARITY` must
/// match it; a test there checks that.
pub const MAX_EGRAPH_ARITY: usize = 4;

/// The most rounds `.run` may ask of an e-graph's loop. The rebuild stamps
/// a fresh id with one past the round that made it, in the 14 bits between
/// its 48-bit hash and the two bits that mark an id fresh, and a run of `n`
/// rounds reaches round `n`. flowlog-runtime's `eqsat` stamp must hold
/// every round up to it; a test there checks that.
pub const MAX_EGRAPH_ROUNDS: u32 = (1 << 14) - 2;
