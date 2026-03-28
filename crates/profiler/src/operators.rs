//! Operator registration methods for the profiler.
//!
//! Each method records a logical operator node in the profiler's plan graph,
//! advancing the address counter by the correct number of timely operators.

use crate::Profiler;

const TAG_INPUT: &str = "Input";
const TAG_STAGE: &str = "Stage";
const TAG_RUNTIME: &str = "Runtime";
const TAG_INSPECT: &str = "Inspect";

// =========================================================================
// Input block
// =========================================================================

impl Profiler {
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
        let steps = self.dedup_collection_steps();
        self.push_node(
            format!("{}: dedup", edb_name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_INPUT,
            steps,
            None,
        );
    }
}

// =========================================================================
// Stage block
// =========================================================================

impl Profiler {
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
        let steps = self.anti_join_steps();
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            steps,
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
        let steps = self.anti_join_steps() + if is_key_only { 2 } else { 1 };
        self.push_node(
            name,
            input_variable_names,
            Some(output_variable_name),
            TAG_STAGE,
            steps,
            Some(fingerprint),
        );
    }

    pub fn general_aggregate_operator(
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

    pub fn opt_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: opt aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_STAGE,
            5,
            None,
        );
    }
}

// =========================================================================
// Runtime block
// =========================================================================

impl Profiler {
    pub fn concat_dedup_operator(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: String,
        concat_count: u32,
        recursive: bool,
    ) {
        let dedup = if recursive {
            self.dedup_recursive_steps()
        } else {
            self.dedup_collection_steps()
        };
        self.push_node(
            format!("{}: concat & dedup", name),
            input_variable_names,
            Some(output_variable_name),
            TAG_RUNTIME,
            concat_count + dedup,
            None,
        );
    }

    pub fn recursive_enter_operator(
        &mut self,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            "enter".to_string(),
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

    pub fn recursive_pre_leave_opt_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: pre-leave opt aggregate", name),
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

    pub fn recursive_post_leave_opt_aggregate_operator(
        &mut self,
        name: String,
        input_variable_name: String,
        output_variable_name: String,
    ) {
        self.push_node(
            format!("{}: post-leave opt aggregate", name),
            vec![input_variable_name],
            Some(output_variable_name),
            TAG_RUNTIME,
            4,
            None,
        );
    }
}

// =========================================================================
// Inspect block
// =========================================================================

impl Profiler {
    pub fn inspect_size_operator(&mut self, input_variable_name: String, name: String) {
        let steps = self.inspect_size_steps();
        self.push_node(
            format!("{}: inspect size", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            steps,
            None,
        );
    }

    pub fn inspect_content_terminal_operator(
        &mut self,
        input_variable_name: String,
        name: String,
    ) {
        let steps = self.inspect_content_steps();
        self.push_node(
            format!("{}: inspect terminal", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            steps,
            None,
        );
    }

    pub fn inspect_content_file_operator(&mut self, input_variable_name: String, name: String) {
        let steps = self.inspect_content_steps();
        self.push_node(
            format!("{}: inspect file", name),
            vec![input_variable_name],
            None,
            TAG_INSPECT,
            steps,
            None,
        );
    }
}
