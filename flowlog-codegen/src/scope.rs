//! The dataflow's two scopes, each holding its inputs, its part of every
//! stratum, and its outputs' inspectors. [`once`] is the dataflow's own
//! scope, at `time::Once`, where the static part is computed once whatever
//! the engine. [`epoch`] is the scope an incremental engine nests in it,
//! at `time::Epoch`, for the non-static part, which enters the static
//! bindings it reads.

mod epoch;
mod once;
