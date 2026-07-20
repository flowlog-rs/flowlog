//! [`Node`], the plan graph's unit of attribution.

use serde::Deserialize;
use serde::Serialize;

use crate::Addr;
use crate::plan::block::Block;

/// One logical operation of the compiled plan, at the granularity a
/// human reads: "load this input", "join these relations", "dedup that
/// result", "print this size". At runtime a node becomes the small run
/// of consecutive timely operators in `operators`, which is how measured
/// metrics find their way back to it.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Node {
    /// Counts up in the order nodes were recorded; `parents` refers to
    /// these ids.
    pub id: usize,
    /// Display name, e.g. `reach: dedup` or a join's transformation label.
    pub name: String,

    /// Which part of the dataflow the node lives in.
    pub block: Block,
    /// Ids of the nodes whose outputs feed this one.
    pub parents: Vec<usize>,
    /// Present exactly on rule transformations, matching the
    /// [`crate::plan::rule::TreeNode`] with the same value; `None` on
    /// plumbing nodes (inputs, dedups, inspects).
    pub fingerprint: Option<String>,

    /// The consecutive timely operator addresses this node is predicted
    /// to occupy; metric rows at these addresses belong to it.
    pub operators: Vec<Addr>,
}
