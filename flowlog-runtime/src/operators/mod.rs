//! FlowLog-owned operators used by generated dataflows.
//!
//! These wrappers define the operator surface that code generation targets.
//! They delegate dataflow mechanics to Differential Dataflow while retaining
//! FlowLog's naming and semantic choices.

mod arrange;
mod dedup;
mod join;
mod map;
mod reduce;

pub use arrange::flowlog_arrange;
pub use arrange::flowlog_arrange_self;
pub use dedup::flowlog_dedup;
pub use dedup::flowlog_input_dedup;
pub use join::flowlog_antijoin;
pub use join::flowlog_join;
pub use map::flowlog_filter;
pub use map::flowlog_lift;
pub use map::flowlog_map;
pub use map::flowlog_map_in_place;
pub use reduce::Avg;
pub use reduce::Count;
pub use reduce::Max;
pub use reduce::Min;
pub use reduce::Sum;
pub use reduce::flowlog_reduce;
pub use reduce::flowlog_reduce_append;
pub use reduce::flowlog_reduce_leave;
