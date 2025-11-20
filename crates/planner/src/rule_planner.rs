//! Rule planner for per-rule transformation planning.
//!
//! This component turns a single rule into a sequence of executable
//! transformations. The planning pipeline is split into phases:
//!
//! - `prepare`: applies local filters (var==var, var==const, placeholders), may
//!   perform (anti-)semijoins and comparison pushdown, and removes unused
//!   arguments to simplify the rule before joining.
//! - `core`: performs the initial join between two selected positive atoms and
//!   then iterates semijoin/pushdown and projection removal to a fixed point.
//! - `fuse`: merges compatible KV-to-KV map steps into their producers and
//!   propagates key/value layout requirements upstream.
//! - `post`: aligns the final pipeline output with the rule head (variables and
//!   arithmetic expressions). Aggregation in the head is handled earlier at the
//!   stratum planning phase.
//!
//! The planner maintains a vector of transformation descriptors along with two
//! caches to accelerate shape and dependency analyses. These caches are always
//! rebuilt after structural changes (see `fuse` phase).

use crate::TransformationInfo;
use std::collections::HashMap;

mod common; // small utilities shared by planner phases
mod core; // initial join + fixed-point of semijoin/pushdown and projection removal
mod fuse; // fuse KV-to-KV maps and propagate key/value layout constraints upstream
mod post; // align final output to the rule head (vars and arithmetic)
mod prepare; // local filters and simplifications before the join

/// Planner state for a single rule.
#[derive(Debug, Default)]
pub struct RulePlanner {
    /// Linear list of planned transformation infos for the current rule.
    transformation_infos: Vec<TransformationInfo>,

    /// Mapping from an fingerprint to its producer indices and optional
    /// list of consumer indices.
    ///
    /// Final transformation outputs have no consumers.
    producer_consumer: HashMap<u64, (Vec<usize>, Vec<usize>)>,
}

impl RulePlanner {
    /// Creates a new empty RulePlanner.
    pub fn new() -> Self {
        Self {
            transformation_infos: Vec::new(),
            producer_consumer: HashMap::new(),
        }
    }

    /// Returns the planned transformations for this rule.
    #[inline]
    pub fn transformation_infos(&self) -> &Vec<TransformationInfo> {
        &self.transformation_infos
    }
}
