//! Node profiling structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::addr::Addr;

/// A profiled node in the dataflow plan graph.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct NodeProfile {
    /// Node id.
    pub id: usize,
    /// Human-friendly node name.
    pub name: String,
    /// Block label (e.g., input/stratum/inspect) for grouping in UI.
    pub block: String,
    /// Optional fingerprint used to identify the rule transformation.
    pub fingerprint: Option<String>,
    /// Tags for presentation and filtering.
    pub tags: Vec<String>,
    /// Operator addresses associated with this node.
    pub operators: Vec<Addr>,
    /// Parent node ids (upstream dependencies).
    pub parents: Vec<usize>,
}

impl NodeProfile {
    /// Update the node id.
    pub(crate) fn update_id(&mut self, new_id: usize) {
        self.id = new_id;
    }

    /// Update the node name.
    pub(crate) fn update_name(&mut self, new_name: String) {
        self.name = new_name;
    }

    /// Update the block label.
    pub(crate) fn update_block(&mut self, new_block: String) {
        self.block = new_block;
    }

    /// Set the fingerprint from a raw u64 value.
    pub(crate) fn update_fingerprint(&mut self, fingerprint: u64) {
        self.fingerprint = Some(format!("0x{:016x}", fingerprint));
    }

    /// Add a tag to the node.
    pub(crate) fn add_tag(&mut self, tag: String) {
        self.tags.push(tag);
    }

    /// Append operator addresses.
    pub(crate) fn add_operators(&mut self, operator_addrs: Vec<Addr>) {
        self.operators.extend(operator_addrs);
    }

    /// Append parent node ids.
    pub(crate) fn add_parents(&mut self, parent_ids: Vec<usize>) {
        self.parents.extend(parent_ids);
    }
}

/// Internal nodes manager that tracks ids, scope addresses, and dependencies.
#[derive(Debug, Clone, Default)]
pub(super) struct NodeManager {
    /// Next node id to allocate.
    pub(super) id_cnt: usize,
    /// Address counter used to create operator addresses.
    pub(super) addr_cnt: Addr,
    /// Current block label for node grouping.
    pub(super) block: String,

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
        let mut node = NodeProfile::default();

        let parent_ids = input_variable_names
            .iter()
            .filter_map(|variable_name| self.node_map.get(variable_name).copied())
            .collect();

        node.update_id(self.id_cnt);
        node.update_name(name);
        node.update_block(self.block.clone());
        if let Some(fingerprint) = fingerprint {
            node.update_fingerprint(fingerprint);
        }
        node.add_tag(tag.to_string());
        node.add_operators(self.addr_cnt.advance(operator_steps));
        node.add_parents(parent_ids);

        if let Some(output_variable_name) = output_variable_name {
            self.node_map.insert(output_variable_name, self.id_cnt);
        }
        self.id_cnt += 1;

        node
    }
}
