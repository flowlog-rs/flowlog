//! Cardinality estimation for the optimizer.
//!
//! Every relation — base or derived, measured or not — must carry a
//! numeric leaf size before the DP can score a join tree. Two modules:
//!
//! * [`comm`] — the shared estimation core: the textbook
//!   [`join_estimate`](comm::join_estimate) formula and
//!   [`trace_columns`](comm::trace_columns), the `(relation, column) →
//!   domain` trace the optimizer folds into `Feedback` at setup.
//! * [`dp`] — the DP cost model ([`CostEstimator`]): scores candidate join
//!   trees, and yields each rule's estimated output size so the optimizer
//!   can record IDB cardinalities as it plans.

mod comm;
mod dp;

pub(crate) use comm::trace_columns;
pub(crate) use dp::CostEstimator;
