//! The recording methods flowlog-build calls during codegen: each appends
//! one transformation's node to the plan graph, delegating id and address
//! allocation to [`crate::plan::manager::NodeManager`].

use crate::PlanGraph;
use crate::ProfilerError;
use crate::plan::rule::Rule;
use crate::plan::steps;

// =============================================================================
// PlanGraph: rules, scopes, and the shared node builder
// =============================================================================

impl PlanGraph {
    /// Insert a rule using raw plan tree info; the plan tree is rendered
    /// internally.
    pub fn insert_rule(
        &mut self,
        rule_text: String,
        plan_tree_info: Vec<((u64, Option<u64>), u64)>,
    ) {
        self.rules.push(Rule::new(rule_text, plan_tree_info));
    }

    /// Switch to the input block.
    pub fn update_input_block(&mut self) {
        self.node_manager.update_input_block();
    }

    /// Switch to a stratum block.
    pub fn update_stratum_block(&mut self, stratum_id: usize) {
        self.node_manager.update_stratum_block(stratum_id);
    }

    /// Switch to the inspect block.
    pub fn update_inspect_block(&mut self) {
        self.node_manager.update_inspect_block();
    }

    /// Enter a nested scope for address generation.
    pub fn enter_scope(&mut self) {
        self.node_manager.enter_scope();
    }

    /// Leave the current scope for address generation. Errors on an
    /// unbalanced leave (a codegen bug), which would corrupt later
    /// addresses.
    pub fn leave_scope(&mut self) -> Result<(), ProfilerError> {
        self.node_manager.leave_scope()
    }

    /// Record a node with the given operator-step count, appending it to
    /// the plan graph.
    fn push_node(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: Option<String>,
        operator_steps: u32,
        fingerprint: Option<u64>,
    ) {
        let node = self.node_manager.build_node(
            name,
            input_variable_names,
            output_variable_name,
            operator_steps,
            fingerprint,
        );
        self.nodes.push(node);
    }
}

// =============================================================================
// Input block
// =============================================================================

impl PlanGraph {
    pub fn input_edb_operator(&mut self, edb_name: String, output_variable_name: String) {
        self.push_node(
            format!("{}: input", edb_name),
            vec![],
            Some(output_variable_name),
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
            steps::DEDUP_NONRECURSIVE,
            None,
        );
    }
}

// =============================================================================
// Stage block
// =============================================================================

impl PlanGraph {
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
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            1 + steps::arrange(is_key_only),
            Some(fingerprint),
        );
    }

    /// Like [`Self::map_join_arrange_operator`] with a dedup between the
    /// projection and the arrangement: the SIP key-only projection dedups
    /// when no predicate already filters it, adding the mode's dedup
    /// operators.
    pub fn map_dedup_arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
        recursive: bool,
    ) {
        let dedup = if recursive {
            steps::dedup_recursive(self.mode)
        } else {
            steps::DEDUP_NONRECURSIVE
        };
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            1 + dedup + steps::arrange(is_key_only),
            Some(fingerprint),
        );
    }

    /// Arrangement of an identity-projected input: the `flat_map` was aliased
    /// away, so only the arrangement itself remains (one fewer op than
    /// [`Self::map_join_arrange_operator`]).
    pub fn arrange_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        is_key_only: bool,
    ) {
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            steps::arrange(is_key_only),
            Some(fingerprint),
        );
    }

    /// A copy rule `B :- A` whose identity `flat_map` was elided entirely: no
    /// timely operator is emitted, but the output relation still needs a node
    /// so downstream references to its fingerprint resolve. Zero operator
    /// steps (nothing to attribute at runtime), so it does not advance the
    /// operator-address counter.
    pub fn identity_alias_operator(
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
            0,
            Some(fingerprint),
        );
    }

    pub fn anti_join_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        fingerprint: u64,
        recursive: bool,
    ) {
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            steps::anti_join(self.mode, recursive),
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
        recursive: bool,
    ) {
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            steps::anti_join(self.mode, recursive) + steps::arrange(is_key_only),
            Some(fingerprint),
        );
    }

    pub fn i32_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            steps::I32_AGGREGATE,
            None,
        );
    }

    pub fn present_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: opt aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            steps::PRESENT_AGGREGATE,
            None,
        );
    }
}

// =============================================================================
// Runtime block
// =============================================================================

impl PlanGraph {
    /// Registers a recursive-loop runtime operator that consumes one input,
    /// produces one output, and maps to a single timely step.
    fn push_recursive_runtime_step(&mut self, name: String, input: String, output: String) {
        self.push_node(name, vec![input], Some(output), 1, None);
    }

    pub fn concat_dedup_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        concat_count: u32,
        recursive: bool,
    ) {
        let dedup = if recursive {
            steps::dedup_recursive(self.mode)
        } else {
            steps::DEDUP_NONRECURSIVE
        };
        self.push_node(
            format!("{}: concat & dedup", name),
            input_variable_names,
            Some(output_variable_name),
            concat_count + dedup,
            None,
        );
    }

    pub fn recursive_enter_operator(
        &mut self,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_recursive_runtime_step(
            "enter".to_string(),
            input_variable_name,
            output_variable_name,
        );
    }

    pub fn recursive_feedback_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_recursive_runtime_step(
            format!("{}: feedback", name),
            input_variable_name,
            output_variable_name,
        );
    }

    pub fn recursive_resultsin_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_recursive_runtime_step(
            format!("{}: resultsin", name),
            input_variable_name,
            output_variable_name,
        );
    }

    pub fn recursive_pre_leave_present_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_recursive_runtime_step(
            format!("{}: pre-leave opt aggregate", name),
            input_variable_name,
            output_variable_name,
        );
    }

    pub fn recursive_leave_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_recursive_runtime_step(
            format!("{}: leave", name),
            input_variable_name,
            output_variable_name,
        );
    }

    pub fn recursive_post_leave_present_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: post-leave opt aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            steps::POST_LEAVE_PRESENT_AGGREGATE,
            None,
        );
    }
}

// =============================================================================
// Inspect block
// =============================================================================

impl PlanGraph {
    /// Registers an `inspect_content` sink (`terminal` or `file`); both share
    /// the same step count and only differ in the label woven into the node
    /// name.
    fn push_inspect_content(&mut self, kind: &str, input: String, name: String) {
        self.push_node(
            format!("{}: inspect {}", name, kind),
            vec![input],
            None,
            steps::inspect_content(self.mode),
            None,
        );
    }

    pub fn inspect_size_operator(&mut self, input_variable_name: String, name: String) {
        self.push_node(
            format!("{}: inspect size", name),
            vec![input_variable_name],
            None,
            steps::INSPECT_SIZE,
            None,
        );
    }

    pub fn inspect_content_terminal_operator(&mut self, input_variable_name: String, name: String) {
        self.push_inspect_content("terminal", input_variable_name, name);
    }

    pub fn inspect_content_file_operator(&mut self, input_variable_name: String, name: String) {
        self.push_inspect_content("file", input_variable_name, name);
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::ExecutionMode;

    use crate::Addr;
    use crate::PlanGraph;

    /// The recording methods drive scope tracking through the aggregate:
    /// a node recorded after leaving a subscope must not reuse the subscope
    /// operator's own address slot.
    #[test]
    fn recording_across_a_scope_boundary_keeps_addresses_distinct() {
        let mut graph = PlanGraph::new(ExecutionMode::Batch);
        graph.map_join_operator("outer".into(), vec![], "a".into(), 1);
        graph.enter_scope();
        graph.map_join_operator("inner".into(), vec![], "b".into(), 1);
        graph.leave_scope().unwrap();
        graph.map_join_operator("after".into(), vec![], "c".into(), 1);
        let nodes = graph.nodes();
        assert_eq!(nodes[1].operators, vec![Addr(vec![1, 1])]);
        assert_eq!(nodes[2].operators, vec![Addr(vec![2])]);
    }
}
