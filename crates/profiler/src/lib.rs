mod addr;
mod node;
mod rule;

use crate::node::{NodeManager, NodeProfile};
use crate::rule::RuleProfile;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Profiler {
    rules: Vec<RuleProfile>,
    nodes: Vec<NodeProfile>,

    #[serde(skip)]
    node_manager: NodeManager,
}

impl Profiler {
    pub fn write_json<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self).expect("Failed to serialize profiler data");
        std::fs::write(path, json)
    }

    /// Insert a rule using raw plan tree info; the plan tree is rendered internally.
    pub fn insert_rule(
        &mut self,
        rule_text: String,
        plan_tree_info: Vec<((u64, Option<u64>), u64)>,
    ) {
        self.rules.push(RuleProfile::new(rule_text, plan_tree_info));
    }

    pub fn update_input_block(&mut self) {
        self.node_manager.update_input_block();
    }

    pub fn update_stratum_block(&mut self, stratum_id: usize) {
        self.node_manager.update_stratum_block(stratum_id);
    }

    pub fn update_inspect_block(&mut self) {
        self.node_manager.update_inspect_block();
    }

    pub fn enter_scope(&mut self) {
        self.node_manager.enter_scope();
    }

    pub fn leave_scope(&mut self) {
        self.node_manager.leave_scope();
    }

    pub fn input_edb_operator(&mut self, edb_name: String, output_variable_name: String) {
        let node = self.node_manager.build_node(
            format!("{}: input", edb_name),
            vec![],
            Some(output_variable_name),
            "Input",
            1,
            None,
        );
        self.nodes.push(node);
    }

    pub fn input_dedup_operator(
        &mut self,
        edb_name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        let node = self.node_manager.build_node(
            format!("{}: dedup", edb_name),
            vec![input_variable_name],
            Some(output_variable_name),
            "Input",
            3,
            None,
        );
        self.nodes.push(node);
    }

    pub fn map_join_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
    ) {
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            "Stage",
            1,
            Some(fingerprint),
        );
        self.nodes.push(node);
    }

    pub fn map_join_arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
    ) {
        let operator_steps = if is_key_only { 2 } else { 3 };
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            "Stage",
            operator_steps,
            Some(fingerprint),
        );
        self.nodes.push(node);
    }

    pub fn anti_join_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
    ) {
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            "Stage",
            15,
            Some(fingerprint),
        );
        self.nodes.push(node);
    }

    pub fn anti_join_arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
    ) {
        let operator_steps = if is_key_only { 16 } else { 17 };
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            "Stage",
            operator_steps,
            Some(fingerprint),
        );
        self.nodes.push(node);
    }

    pub fn concat_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        concat_number: u32,
    ) {
        let node = self.node_manager.build_node(
            format!("{}: concat & dedup", name),
            input_variable_names,
            Some(output_variable_name),
            "Runtime",
            concat_number + 3,
            None,
        );
        self.nodes.push(node);
    }

    pub fn recursive_enter_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
    ) {
        let node = self.node_manager.build_node(
            format!("{}: enter", name),
            input_variable_names,
            Some(output_variable_name),
            "Runtime",
            1,
            None,
        );
        self.nodes.push(node);
    }

    pub fn recursive_feedback_operator(&mut self, name: String, output_variable_name: String) {
        let node = self.node_manager.build_node(
            format!("{}: feedback", name),
            vec![],
            Some(output_variable_name),
            "Runtime",
            1,
            None,
        );
        self.nodes.push(node);
    }

    pub fn recursive_resultsin_operator(&mut self, name: String) {
        let node = self.node_manager.build_node(
            format!("{}: resultsin", name),
            vec![],
            None,
            "Runtime",
            1,
            None,
        );
        self.nodes.push(node);
    }

    pub fn recursive_leave_operator(&mut self, name: String) {
        let node = self.node_manager.build_node(
            format!("{}: leave", name),
            vec![],
            None,
            "Runtime",
            1,
            None,
        );
        self.nodes.push(node);
    }

    pub fn inspect_size_operator(&mut self, name: String) {
        let node = self.node_manager.build_node(
            format!("{}: inspect", name),
            vec![],
            None,
            "Inspect",
            9,
            None,
        );
        self.nodes.push(node);
    }

    pub fn inspect_content_terminal_operator(&mut self, _name: String) {
        todo!("inspect content terminal operator not implemented yet")
    }

    pub fn inspect_content_file_operator(&mut self, _name: String) {
        todo!("inspect content file operator not implemented yet")
    }
}
