//! The profiler: read a run's logs and join them onto the plan.
//!
//! [`read()`] is the one way in -- it returns one [`Snapshot`] of
//! measured facts per committed transaction, keyed to the
//! [`crate::PlanGraph`] it was read against.
//!
//! One module per aggregation layer, each holding the layer's data and
//! the functions that produce it, top down:
//!
//! - `snapshot`: the driver -- [`read()`] walks the transactions and
//!   packs one [`Snapshot`] each
//! - `transaction`: the metrics directory discovered into
//!   per-transaction log text
//! - `edge`: one worker's channels resolved -> per-operator flow and
//!   the edges that carry it
//! - `operator`: workers folded -> `OperatorMetrics` per address
//! - `node`: the transformation layer -- operators bound to plan
//!   nodes -> `NodeMetrics`
//!
//! `channel` is the wire channel table, the raw substrate flow is
//! measured from; `cardinality` is the flow measure the layers share.

mod cardinality;
mod channel;
mod edge;
mod node;
mod operator;
mod snapshot;
mod transaction;

pub use operator::Stats;
pub use snapshot::Snapshot;
pub use snapshot::read;
