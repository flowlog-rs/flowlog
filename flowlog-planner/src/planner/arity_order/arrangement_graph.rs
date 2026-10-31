//! Physical arrangements and the expressions that build them.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use crate::planner::BindingOperation;
use crate::planner::Transformation;

// =============================================================================
// Scope
// =============================================================================

/// Whether a boundary is outside or inside its stratum's feedback loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Scope {
    NonRecursive,
    Recursive,
}

// =============================================================================
// Arities
// =============================================================================

/// Column positions in the feed's flattened row, grouped into key and value
/// blocks. The key precedes the value. Variable boundaries may reorder fields
/// within a block; fixed boundaries retain the listed order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Arities {
    pub(super) key: Vec<usize>,
    pub(super) value: Vec<usize>,
}

impl Arities {
    /// Assigns contiguous slots to a key followed by its value.
    #[must_use]
    pub(super) fn new(key: usize, value: usize) -> Self {
        Self {
            key: (0..key).collect(),
            value: (key..key + value).collect(),
        }
    }
}

// =============================================================================
// PhysicalArrangement
// =============================================================================

/// One materialized layout, including dedup and aggregation results. Static
/// dedup is a sorting boundary even when consolidation retains no trace.
/// Stratum zero is current; later indices follow evaluation order.
/// Its identity stays unchanged when a selected field order changes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PhysicalArrangement {
    Arrangement {
        stratum: usize,
        scope: Scope,
        transformation: usize,
    },
    /// All union feeds share this node's single dedup layout.
    Dedup {
        stratum: usize,
        scope: Scope,
        relation: u64,
    },
    Aggregation {
        stratum: usize,
        scope: Scope,
        relation: u64,
    },
}

impl PhysicalArrangement {
    /// Identifies a completed binding independently of its selected order.
    #[must_use]
    pub(super) fn relation_binding(
        relation: u64,
        operation: &BindingOperation,
        stratum: usize,
        scope: Scope,
    ) -> Self {
        match operation {
            BindingOperation::Aggregation { .. } => Self::Aggregation {
                stratum,
                scope,
                relation,
            },
            BindingOperation::Dedup { .. } => Self::Dedup {
                stratum,
                scope,
                relation,
            },
        }
    }
}

// =============================================================================
// TransformationId and JoinOrientation
// =============================================================================

/// A step's location, including joins that produce only a row collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TransformationId {
    pub(super) stratum: usize,
    pub(super) scope: Scope,
    pub(super) transformation: usize,
}

/// Which planned input supplies the left side of an ordinary join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrientation {
    Original,
    Swapped,
}

// =============================================================================
// Producer
// =============================================================================

/// A feed expression stops at an arrangement or an external collection.
/// Row-only steps retain their field mappings without becoming nodes.
/// Borrowed steps reference the plan; owned steps compose projections for
/// one feed without rewriting the shared collection's other consumers.
/// Cloning copies the feed tree, including synthesized projections, so each
/// consumer can fuse its own path while the underlying collections stay shared.
#[derive(Clone)]
pub(super) enum Producer<'plan> {
    /// A collection outside this graph, whose layout is a boundary condition.
    External(u64),
    Arrangement(PhysicalArrangement),
    Transformation {
        id: TransformationId,
        step: Cow<'plan, Transformation>,
        sources: Vec<Producer<'plan>>,
    },
    Union(Vec<Producer<'plan>>),
}

impl fmt::Debug for Producer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::External(fingerprint) => {
                write!(f, "External(0x{fingerprint:016x})")
            }
            Self::Arrangement(node) => node.fmt(f),
            // The full step also prints collections and canonical forms;
            // only the operation, mappings and feeds belong in this graph.
            Self::Transformation { id, step, sources } => f
                .debug_struct(step.operation_name())
                .field("id", id)
                .field("sources", sources)
                .field("flow", step.flow())
                .finish(),
            Self::Union(parts) => f.debug_tuple("Union").field(parts).finish(),
        }
    }
}

// =============================================================================
// Edge
// =============================================================================

/// One input expression builds one output arrangement. A join keeps both
/// ordered sources in its expression; a relation binding keeps every
/// union feed.
#[derive(Debug)]
pub(super) struct Edge<'plan> {
    pub(super) input: Producer<'plan>,
    pub(super) output: PhysicalArrangement,
}

// =============================================================================
// ArrangementGraph
// =============================================================================

/// Sorting boundaries and the expressions that feed them. Each node stores
/// one selected field order, initialized from the plan, rather than a domain
/// of permutations. Edges reference stable identities; changing an order does
/// not invalidate them. Feed references can form cycles through feedback.
#[derive(Debug, Default)]
pub(super) struct ArrangementGraph<'plan> {
    pub(super) nodes: BTreeMap<PhysicalArrangement, Arities>,
    pub(super) joins: BTreeMap<TransformationId, JoinOrientation>,
    pub(super) edges: Vec<Edge<'plan>>,
}
