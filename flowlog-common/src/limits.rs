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
