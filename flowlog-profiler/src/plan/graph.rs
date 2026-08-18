//! [`PlanGraph`], the aggregate every registration records into, plus the
//! optional-profiling entry points and the fingerprint wire format.

use flowlog_common::ExecutionMode;
use serde::Deserialize;
use serde::Serialize;

use crate::ProfilerError;
use crate::plan::manager::NodeManager;
use crate::plan::node::Node;
use crate::plan::rule::Rule;

/// The wire form of a fingerprint in `ops.json`: `0x` plus 16 hex digits.
/// Node and rule entries render through this one helper so their
/// fingerprints join exactly.
pub(crate) fn format_fingerprint(fp: u64) -> String {
    format!("0x{fp:016x}")
}

/// The recorded plan graph: every logical operator node and rule plan tree
/// a compilation registers.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct PlanGraph {
    // Only the recording methods (see `crate::plan::builder`) may shape
    // these, and they all live inside `plan`; outside the module the graph
    // is read-only, through `nodes()`/`rules()`. Hence `pub(super)` (i.e.
    // visible within `plan`), not `pub(crate)`.
    pub(super) rules: Vec<Rule>,
    pub(super) nodes: Vec<Node>,

    #[serde(skip)]
    pub(super) node_manager: NodeManager,

    #[serde(skip)]
    pub(super) mode: ExecutionMode,
}

/// Run a closure if a plan graph is present. For recording steps that
/// cannot fail; use [`try_with_plan_graph`] for one that returns a `Result`.
pub fn with_plan_graph<F>(graph: &mut Option<PlanGraph>, f: F)
where
    F: FnOnce(&mut PlanGraph),
{
    if let Some(graph) = graph.as_mut() {
        f(graph);
    }
}

/// Run a fallible recording closure if a plan graph is present,
/// propagating its error; `Ok(())` when profiling is off. The fallible
/// counterpart to [`with_plan_graph`], for recording steps that can hit an
/// invariant violation (e.g. an unbalanced [`PlanGraph::leave_scope`]).
pub fn try_with_plan_graph<F>(graph: &mut Option<PlanGraph>, f: F) -> Result<(), ProfilerError>
where
    F: FnOnce(&mut PlanGraph) -> Result<(), ProfilerError>,
{
    match graph.as_mut() {
        Some(graph) => f(graph),
        None => Ok(()),
    }
}

impl PlanGraph {
    /// Create a new plan graph for the given execution mode.
    pub fn new(mode: ExecutionMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Logical nodes recorded so far.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Rule plan trees recorded so far.
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// Serialize the plan graph to a pretty JSON string, fit for baking
    /// into generated source as a `const &str`. Errors only if
    /// serialization fails, which a `Serialize`-derived `PlanGraph` never
    /// does in practice.
    pub fn to_json_string(&self) -> Result<String, ProfilerError> {
        serde_json::to_string_pretty(self).map_err(ProfilerError::Serialize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The visualizer joins node fingerprints against rule-tree ones, so
    /// the wire form is a contract, not a formatting choice.
    #[test]
    fn fingerprint_wire_form_is_0x_plus_16_hex_digits() {
        assert_eq!(format_fingerprint(0xAB), "0x00000000000000ab");
    }

    /// An unbalanced `leave_scope` is a codegen bug: it surfaces as an
    /// internal error rather than corrupting the addresses that follow.
    #[test]
    fn leaving_the_root_scope_is_rejected() {
        let mut graph = PlanGraph::new(ExecutionMode::Batch);
        assert!(matches!(
            graph.leave_scope(),
            Err(ProfilerError::Internal(_))
        ));
    }

    /// Recorded rules read back in insertion order (close the record/read
    /// loop the visualizer depends on).
    #[test]
    fn recorded_rules_read_back_in_order() {
        let mut graph = PlanGraph::new(ExecutionMode::Batch);
        graph.insert_rule("r0".into(), vec![((1, None), 2)]);
        graph.insert_rule("r1".into(), vec![((3, None), 4)]);
        let rules = graph.rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].text, "r0");
        assert_eq!(rules[1].text, "r1");
    }

    /// A recorded graph serializes and deserializes back to the same nodes.
    #[test]
    fn to_json_string_round_trips_through_serde() {
        let mut graph = PlanGraph::new(ExecutionMode::Batch);
        graph.map_join_operator("n".into(), vec![], "a".into(), 1);
        let json = graph.to_json_string().expect("serializes");
        let reparsed: PlanGraph = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(reparsed.nodes().len(), 1);
        assert_eq!(reparsed.nodes()[0].name, "n");
    }

    /// The fallible seam runs the closure and propagates its error when a
    /// graph is present, and is a no-op `Ok` when profiling is off.
    #[test]
    fn try_with_plan_graph_propagates_and_no_ops() {
        let mut on = Some(PlanGraph::new(ExecutionMode::Batch));
        assert!(matches!(
            try_with_plan_graph(&mut on, |g| g.leave_scope()),
            Err(ProfilerError::Internal(_))
        ));

        let mut off: Option<PlanGraph> = None;
        assert!(try_with_plan_graph(&mut off, |g| g.leave_scope()).is_ok());
    }

    /// The infallible seam skips the closure entirely when profiling is off.
    #[test]
    fn with_plan_graph_skips_the_closure_when_off() {
        let mut off: Option<PlanGraph> = None;
        let mut ran = false;
        with_plan_graph(&mut off, |_| ran = true);
        assert!(!ran);
    }
}
