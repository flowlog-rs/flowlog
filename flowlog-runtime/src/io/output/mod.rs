//! Writing a relation's rows back out.
//!
//! - [`drain`]: the pumps, feeding gathered rows to a writer in the order
//!   the relation asked for.
//! - [`writer`]: the destination, one per kind.
//! - [`encode`]: how a slot tuple becomes a sink's record.

pub mod drain;
pub mod encode;
pub mod writer;

pub use drain::Row;
pub use drain::drain_flat;
pub use drain::drain_sorted;
pub use drain::drain_topk;
pub use drain::for_each_flat;
pub use drain::for_each_sorted;
pub use drain::for_each_topk;
