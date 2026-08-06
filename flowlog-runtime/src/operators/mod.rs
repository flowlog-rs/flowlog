//! FlowLog-owned operators used by generated dataflows.
//!
//! These wrappers define the operator surface that code generation targets.
//! They delegate dataflow mechanics to Differential Dataflow while retaining
//! FlowLog's naming and semantic choices.

mod dedup;
mod join;
mod map;

pub use dedup::flowlog_dedup;
pub use dedup::flowlog_dedup_retained;
pub use join::flowlog_antijoin;
pub use join::flowlog_join;
pub use map::flowlog_filter;
pub use map::flowlog_map;
pub use map::flowlog_map_in_place;
