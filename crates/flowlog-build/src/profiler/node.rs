//! Node profiling structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::profiler::addr::Addr;

/// A profiled node in the dataflow plan graph.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct NodeProfile {
    /// Node id.
    id: usize,
    /// Human-friendly node name.
    name: String,
    /// Hierarchical Collection name (e.g. `(reach ⋈[y] arc)`).
    transformation_name: Option<String>,
    /// Block label (e.g., input/stratum/inspect) for grouping in UI.
    block: String,
    /// Optional fingerprint used to identify the rule transformation.
    fingerprint: Option<String>,
    /// Tags for presentation and filtering.
    tags: Vec<String>,
    /// Operator addresses associated with this node.
    operators: Vec<Addr>,
    /// Parent node ids (upstream dependencies).
    parents: Vec<usize>,
}

impl NodeProfile {
    /// Hierarchical Collection name of the transformation this node
    /// implements, if any (`None` for runtime plumbing).
    pub(crate) fn transformation_name(&self) -> Option<&str> {
        self.transformation_name.as_deref()
    }

    /// Fingerprint identifying the rule transformation, if any.
    pub(crate) fn fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }
}

/// Internal nodes manager that tracks ids, scope addresses, and dependencies.
#[derive(Debug, Clone, Default)]
pub(super) struct NodeManager {
    /// Next node id to allocate.
    id_cnt: usize,
    /// Address counter used to create operator addresses.
    addr_cnt: Addr,
    /// Current block label for node grouping.
    block: String,

    /// Tracks the most recent node id for each variable name.
    node_map: HashMap<String, usize>,
}

impl NodeManager {
    /// Switch to the input block label.
    pub(super) fn update_input_block(&mut self) {
        self.block = "input".to_string();
    }

    /// Switch to a stratum block label.
    pub(super) fn update_stratum_block(&mut self, stratum_id: usize) {
        self.block = format!("stratum {}", stratum_id);
    }

    /// Switch to the inspect block label.
    pub(super) fn update_inspect_block(&mut self) {
        self.block = "inspect".to_string();
    }

    /// Enter a nested scope for address generation.
    pub(super) fn enter_scope(&mut self) {
        self.addr_cnt.enter_scope();
    }

    /// Leave the current scope for address generation.
    pub(super) fn leave_scope(&mut self) {
        self.addr_cnt.leave_scope();
        self.addr_cnt.advance(1);
    }

    /// Build a node with common metadata and parents. `transformation`
    /// carries the `(fingerprint, canonical name)` of the plan
    /// transformation this node implements — `None` for runtime plumbing.
    pub(super) fn build_node(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: Option<String>,
        tag: &str,
        operator_steps: u32,
        transformation: Option<(u64, String)>,
    ) -> NodeProfile {
        let parents = input_variable_names
            .iter()
            .filter_map(|variable_name| self.node_map.get(variable_name).copied())
            .collect();
        let (fingerprint, transformation_name) = transformation
            .map(|(fp, name)| (format!("0x{fp:016x}"), name))
            .unzip();

        let node = NodeProfile {
            id: self.id_cnt,
            name,
            transformation_name,
            block: self.block.clone(),
            fingerprint,
            tags: vec![tag.to_string()],
            operators: self.addr_cnt.advance(operator_steps),
            parents,
        };

        if let Some(output_variable_name) = output_variable_name {
            self.node_map.insert(output_variable_name, self.id_cnt);
        }
        self.id_cnt += 1;

        node
    }
}
