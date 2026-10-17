//! FlowLog-owned operators used by generated dataflows.
//!
//! These wrappers define the operator surface that code generation targets.
//! They delegate dataflow mechanics to Differential Dataflow while retaining
//! FlowLog's naming and semantic choices.

mod aggregate;
mod arrange;
mod dedup;
mod join;
mod lattice;
mod map;
mod reduce;

pub use aggregate::Groups;
pub use aggregate::flowlog_aggregate;
pub use arrange::flowlog_arrange;
pub use arrange::flowlog_arrange_self;
pub use dedup::flowlog_dedup;
pub use dedup::flowlog_input_dedup;
pub use join::flowlog_antijoin;
pub use join::flowlog_join;
pub use lattice::flowlog_lattice;
pub use lattice::flowlog_lattice_leave;
pub use map::flowlog_filter;
pub use map::flowlog_lift;
pub use map::flowlog_map;
pub use map::flowlog_map_in_place;
pub use reduce::Count;
pub use reduce::Max;
pub use reduce::Mean;
pub use reduce::Min;
pub use reduce::Sum;
