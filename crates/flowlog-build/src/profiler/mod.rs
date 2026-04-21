//! Profiling utilities for FlowLog compilation and execution.
//!
//! The profiler records the logical operator plan graph during compilation,
//! mapping each operator to its predicted timely dataflow address range.
//! At runtime, this is cross-referenced with timely's actual operator logs.
//!
//! # Module structure
//!
//! - [`operators`] — Operator registration methods (input, stage, runtime, inspect)
//! - [`steps`] — Mode-dependent operator step counts (how many timely ops each DD call creates)
//! - [`node`] — Node profile data structures
//! - [`addr`] — Address tracking for timely operator indices
//! - [`rule`] — Rule profile data structures

mod addr;
mod node;
mod operators;
mod rule;
mod steps;

use crate::profiler::node::{NodeManager, NodeProfile};
use crate::profiler::rule::RuleProfile;
use crate::common::ExecutionMode;
use serde::{Deserialize, Serialize};
use std::io;

/// Profiler that records the operator plan graph during compilation.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Profiler {
    rules: Vec<RuleProfile>,
    nodes: Vec<NodeProfile>,

    #[serde(skip)]
    node_manager: NodeManager,

    #[serde(skip)]
    mode: ExecutionMode,
}

/// Run a closure if a profiler instance is present.
pub fn with_profiler<F>(profiler: &mut Option<Profiler>, f: F)
where
    F: FnOnce(&mut Profiler),
{
    if let Some(profiler) = profiler.as_mut() {
        f(profiler);
    }
}

/// Run a fallible closure if a profiler instance is present (shared reference).
pub fn with_profiler_ref<F, E>(profiler: &Option<Profiler>, f: F) -> Result<(), E>
where
    F: FnOnce(&Profiler) -> Result<(), E>,
{
    if let Some(profiler) = profiler.as_ref() {
        f(profiler)
    } else {
        Ok(())
    }
}

impl Profiler {
    /// Create a new profiler with the given execution mode.
    pub fn new(mode: ExecutionMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Serialize profiler data to a pretty JSON file.
    pub fn write_json<P: AsRef<std::path::Path>>(&self, path: P) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
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

    // =================================================================
    // Node manager delegation (scope & block tracking)
    // =================================================================

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

    // =================================================================
    // Internal node builder
    // =================================================================

    pub(crate) fn push_node(
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
}
