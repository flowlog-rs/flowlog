//! FlowLog-owned operators used by generated dataflows.
//!
//! These wrappers define the operator surface that code generation targets.
//! They delegate dataflow mechanics to Differential Dataflow while retaining
//! FlowLog's naming and semantic choices.

mod aggregate;
mod arrange;
mod choice;
mod dedup;
mod join;
mod lattice;
mod lookup;
mod map;
mod reduce;
mod unit;

pub use aggregate::Groups;
pub use aggregate::KeyGroups;
pub use aggregate::aggregate;
pub use aggregate::aggregate_abelian;
pub use arrange::arrange;
pub use arrange::arrange_self;
pub use choice::choice;
pub use dedup::dedup;
pub use dedup::input_dedup;
pub use join::antijoin;
pub use join::join;
pub use lattice::lattice;
pub use lattice::lattice_leave;
pub use lookup::New;
pub use lookup::Old;
pub use lookup::Version;
pub use lookup::lookup;
pub use map::filter;
pub use map::lift;
pub use map::map;
pub use map::map_in_place;
pub use reduce::Count;
pub use reduce::Max;
pub use reduce::Mean;
pub use reduce::Min;
pub use reduce::Sum;
pub use unit::unit;
