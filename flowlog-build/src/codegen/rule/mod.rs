//! Rules, `head :- body`: [`body`] lowers each planned step of a rule body
//! to its operator, and [`head`] a relation's union of its rule heads, the
//! dedup, and the aggregation. Both serve a stratum of either kind.

pub(super) mod body;
pub(super) mod head;
