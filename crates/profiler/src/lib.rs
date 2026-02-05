//! Profiling utilities for FlowLog compilation and execution.

mod addr;
mod node;
mod rule;

use crate::node::{NodeManager, NodeProfile};
use crate::rule::RuleProfile;
use serde::{Deserialize, Serialize};
use std::io::{self, ErrorKind};

const TAG_INPUT: &str = "Input";
const TAG_STAGE: &str = "Stage";
const TAG_RUNTIME: &str = "Runtime";
const TAG_INSPECT: &str = "Inspect";

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Profiler {
    rules: Vec<RuleProfile>,
    nodes: Vec<NodeProfile>,

    #[serde(skip)]
    node_manager: NodeManager,
}

impl Profiler {
    /// Serialize profiler data to a pretty JSON file.
    pub fn write_json<P: AsRef<std::path::Path>>(&self, path: P) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
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

    /// Update the node manager to the input block.
    pub fn update_input_block(&mut self) {
        self.node_manager.update_input_block();
    }

    /// Update the node manager to a stratum block.
    pub fn update_stratum_block(&mut self, stratum_id: usize) {
        self.node_manager.update_stratum_block(stratum_id);
    }

    /// Update the node manager to the inspect block.
    pub fn update_inspect_block(&mut self) {
        self.node_manager.update_inspect_block();
    }

    /// Enter a nested scope for operator addresses.
    pub fn enter_scope(&mut self) {
        self.node_manager.enter_scope();
    }

    /// Leave the current scope for operator addresses.
    pub fn leave_scope(&mut self) {
        self.node_manager.leave_scope();
    }

    fn push_node(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: Option<String>,
        tag: &str,
        operator_steps: u32,
        fingerprint: Option<u64>,
    ) {
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            output_variable_name,
            tag,
            operator_steps,
            fingerprint,
        );
        self.nodes.push(node);
    }

    // Input block operators.
    pub fn input_edb_operator(&mut self, edb_name: String, output_variable_name: String) {
        self.push_node(
            format!("{}: input", edb_name),
            vec![],
            Some(output_variable_name),
            TAG_INPUT,
            1,
            None,
        );
    }

    pub fn input_dedup_operator(
        &mut self,
        edb_name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: dedup", edb_name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_INPUT,
            3,
            None,
        );
    }

    // Stage block operators.
    pub fn map_join_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
    ) {
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            1,
            Some(fingerprint),
        );
    }

    pub fn map_join_arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
    ) {
        let operator_steps = if is_key_only { 3 } else { 2 };
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            operator_steps,
            Some(fingerprint),
        );
    }

    pub fn anti_join_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
    ) {
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            15,
            Some(fingerprint),
        );
    }

    pub fn anti_join_arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
    ) {
        let operator_steps = if is_key_only { 17 } else { 16 };
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            operator_steps,
            Some(fingerprint),
        );
    }

    pub fn aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_STAGE,
            4,
            None,
        );
    }

    // Runtime block operators.
    pub fn concat_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        concat_number: u32,
    ) {
        self.push_node(
            format!("{}: concat & dedup", name),
            input_variable_names,
            Some(output_variable_name),
            TAG_RUNTIME,
            concat_number + 3,
            None,
        );
    }

    pub fn recursive_enter_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: enter", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_RUNTIME,
            1,
            None,
        );
    }

    pub fn recursive_feedback_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: feedback", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_RUNTIME,
            1,
            None,
        );
    }

    pub fn recursive_resultsin_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: resultsin", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_RUNTIME,
            1,
            None,
        );
    }

    pub fn recursive_leave_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: leave", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_RUNTIME,
            1,
            None,
        );
    }

    // Inspect block operators.
    pub fn inspect_size_operator(&mut self, input_variable_name: String, name: String) {
        self.push_node(
            format!("{}: inspect size", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            9,
            None,
        );
    }

    pub fn inspect_content_terminal_operator(&mut self, input_variable_name: String, name: String) {
        self.push_node(
            format!("{}: inspect terminal", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            1,
            None,
        );
    }

    pub fn inspect_content_file_operator(&mut self, input_variable_name: String, name: String) {
        self.push_node(
            format!("{}: inspect file", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            1,
            None,
        );
    }
}
