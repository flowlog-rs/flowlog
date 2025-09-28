//! Rule planner implementing the per-rule preparation planning.

use crate::TransformationInfo;
use std::collections::HashMap;

mod core;
mod prepare;

/// Rule planner for the per-rule planning.
#[derive(Debug, Default)]
pub struct RulePlanner {
    /// The list of transformation info generated during the alpha elimination prepare phase.
    transformation_infos: Vec<TransformationInfo>,

    /// The map of key-value layout indexed by transformation fingerprint.
    /// We cache the index of the start of value columns.
    kv_layouts: HashMap<u64, usize>,
}

impl RulePlanner {
    /// Creates a new empty `RulePlanner`.
    pub fn new() -> Self {
        Self {
            transformation_infos: Vec::new(),
            kv_layouts: HashMap::new(),
        }
    }

    /// NOTE: keeps original API (clone) to avoid breaking callers.
    pub fn transformation_infos(&self) -> Vec<TransformationInfo> {
        self.transformation_infos.clone()
    }
}
