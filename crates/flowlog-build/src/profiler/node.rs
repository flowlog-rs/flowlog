//! Node profiling structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::profiler::addr::Addr;

/// A profiled node in the dataflow plan graph.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(super) struct NodeProfile {
    /// Node id.
    id: usize,
    /// Human-friendly node name.
    name: String,
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

    /// Build a node with common metadata, optional fingerprint, and parents.
    pub(super) fn build_node(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: Option<String>,
        tag: &str,
        operator_steps: u32,
        fingerprint: Option<u64>,
    ) -> NodeProfile {
        let id = self.id_cnt;
        let parents = input_variable_names
            .iter()
            .filter_map(|variable_name| self.node_map.get(variable_name).copied())
            .collect();

        let node = NodeProfile {
            id,
            name,
            block: self.block.clone(),
            fingerprint: fingerprint.map(|fp| format!("0x{fp:016x}")),
            tags: vec![tag.to_string()],
            operators: self.addr_cnt.advance(operator_steps),
            parents,
        };

        if let Some(output_variable_name) = output_variable_name {
            self.node_map.insert(output_variable_name, id);
        }
        self.id_cnt += 1;

        node
    }
}
