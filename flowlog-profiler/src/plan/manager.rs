//! [`NodeManager`], the allocator behind plan-graph recording: it hands out
//! node ids and contiguous operator address ranges, and resolves each
//! variable name to its latest producing node.

use std::collections::HashMap;

use crate::Addr;
use crate::ProfilerError;
use crate::plan::block::Block;
use crate::plan::graph::format_fingerprint;
use crate::plan::node::Node;

/// Allocates node ids and operator address ranges, and resolves variable
/// names to their latest producing node.
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeManager {
    /// Id the next node will receive.
    next_id: usize,
    /// Address the next operator will receive; advances across nodes so
    /// ranges stay contiguous.
    next_addr: Addr,
    /// Block stamped on nodes built from now on.
    block: Block,

    /// The node that most recently produced each generated-code variable.
    /// Last write wins, mirroring rebinding in the generated code: a
    /// consumer registered after a variable is rebound (e.g. a relation's
    /// collection after its dedup) gets the newer node as parent.
    latest_producer: HashMap<String, usize>,
}

impl NodeManager {
    /// Switch to the input block.
    pub(crate) fn update_input_block(&mut self) {
        self.block = Block::Input;
    }

    /// Switch to a stratum block.
    pub(crate) fn update_stratum_block(&mut self, stratum_id: usize) {
        self.block = Block::Stratum(stratum_id);
    }

    /// Switch to the inspect block.
    pub(crate) fn update_inspect_block(&mut self) {
        self.block = Block::Inspect;
    }

    /// Enter a nested scope for address generation.
    pub(crate) fn enter_scope(&mut self) {
        self.next_addr.enter_scope();
    }

    /// Leave the current scope for address generation. Errors on an
    /// unbalanced leave (more `leave_scope`s than `enter_scope`s), naming
    /// the block being recorded so the codegen bug can be located.
    pub(crate) fn leave_scope(&mut self) -> Result<(), ProfilerError> {
        if !self.next_addr.leave_scope() {
            return Err(ProfilerError::internal(format!(
                "unbalanced leave_scope while recording {}: no enclosing scope to leave",
                self.block
            )));
        }
        // Step past the subscope operator's own slot in the parent scope.
        self.next_addr.advance(1);
        Ok(())
    }

    /// Builds the node, consuming `operator_steps` addresses for its range
    /// and recording `output_variable_name` as produced by it, so later
    /// nodes naming that variable resolve their parent to this one.
    pub(crate) fn build_node(
        &mut self,
        name: String,
        input_variable_names: Vec<String>,
        output_variable_name: Option<String>,
        operator_steps: u32,
        fingerprint: Option<u64>,
    ) -> Node {
        let parents = input_variable_names
            .iter()
            .filter_map(|variable_name| self.latest_producer.get(variable_name).copied())
            .collect();

        let node = Node {
            id: self.next_id,
            name,
            block: self.block,
            parents,
            fingerprint: fingerprint.map(format_fingerprint),
            operators: self.next_addr.advance(operator_steps),
        };

        if let Some(output_variable_name) = output_variable_name {
            self.latest_producer
                .insert(output_variable_name, self.next_id);
        }
        self.next_id += 1;

        node
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(mgr: &mut NodeManager, name: &str, inputs: &[&str], output: &str) -> Node {
        mgr.build_node(
            name.to_string(),
            inputs.iter().map(|s| s.to_string()).collect(),
            Some(output.to_string()),
            1,
            None,
        )
    }

    #[test]
    fn build_node_assigns_contiguous_address_ranges() {
        let mut mgr = NodeManager::default();
        let first = mgr.build_node("a".into(), vec![], None, 2, None);
        let second = mgr.build_node("b".into(), vec![], None, 1, None);
        assert_eq!(first.operators, vec![Addr(vec![0]), Addr(vec![1])]);
        assert_eq!(second.operators, vec![Addr(vec![2])]);
    }

    #[test]
    fn parents_resolve_to_the_latest_producer_of_each_input() {
        let mut mgr = NodeManager::default();
        node(&mut mgr, "a", &[], "x");
        let overwriter = node(&mut mgr, "b", &[], "x");
        let consumer = node(&mut mgr, "c", &["x"], "y");
        assert_eq!(consumer.parents, vec![overwriter.id]);
    }

    #[test]
    fn inputs_without_a_recorded_producer_yield_no_parents() {
        let mut mgr = NodeManager::default();
        let consumer = node(&mut mgr, "c", &["never_produced"], "y");
        assert!(consumer.parents.is_empty());
    }

    /// The subscope operator occupies one slot in the parent scope, so the
    /// first node after leaving must not reuse its address.
    #[test]
    fn leaving_a_scope_steps_past_the_subscope_slot() {
        let mut mgr = NodeManager::default();
        mgr.build_node("outer".into(), vec![], None, 1, None);
        mgr.enter_scope();
        let inner = mgr.build_node("inner".into(), vec![], None, 1, None);
        mgr.leave_scope().unwrap();
        let after = mgr.build_node("after".into(), vec![], None, 1, None);
        assert_eq!(inner.operators, vec![Addr(vec![1, 1])]);
        assert_eq!(after.operators, vec![Addr(vec![2])]);
    }

    /// An unbalanced leave is an internal error whose message names the
    /// block being recorded, so a developer can locate the codegen bug.
    #[test]
    fn unbalanced_leave_names_the_block_in_the_error() {
        let mut mgr = NodeManager::default();
        mgr.update_stratum_block(2);
        let Err(ProfilerError::Internal(e)) = mgr.leave_scope() else {
            panic!("expected an internal error");
        };
        assert!(e.to_string().contains("stratum 2"), "message: {e}");
    }

    /// A fingerprint is stamped in the shared `0x…` wire form.
    #[test]
    fn build_node_stamps_the_fingerprint_in_wire_form() {
        let mut mgr = NodeManager::default();
        let node = mgr.build_node("n".into(), vec![], None, 1, Some(0xAB));
        assert_eq!(node.fingerprint.as_deref(), Some("0x00000000000000ab"));
    }

    /// A node carries whichever block was current when it was built.
    #[test]
    fn build_node_stamps_the_current_block() {
        let mut mgr = NodeManager::default();
        mgr.update_stratum_block(3);
        let stratum = mgr.build_node("s".into(), vec![], None, 1, None);
        mgr.update_inspect_block();
        let inspect = mgr.build_node("i".into(), vec![], None, 1, None);
        assert_eq!(stratum.block, Block::Stratum(3));
        assert_eq!(inspect.block, Block::Inspect);
    }
}
