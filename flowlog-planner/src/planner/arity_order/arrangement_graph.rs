//! Sorting boundaries and the computations between them.
//!
//! An arrangement establishes a physical field order. A feed describes an
//! existing transformation that supplies rows to a boundary or another feed;
//! it does not establish a sorting boundary of its own. Both are stored once
//! in flat arrays and connected by IDs, including shared and recursive reads.

use std::borrow::Cow;
use std::fmt;

use flowlog_parser::Mutability;
use flowlog_parser::Relation;

use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::Transformation;

// =============================================================================
// Scope and TransformationLocation
// =============================================================================

/// Whether a boundary is outside or inside its stratum's feedback loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Scope {
    NonRecursive,
    Recursive,
}

/// A transformation's position in the original plan, independent of its feed
/// ID or any composed field mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TransformationLocation {
    stratum_index: usize,
    scope: Scope,
    transformation_index: usize,
}

impl TransformationLocation {
    #[must_use]
    pub(super) fn new(stratum_index: usize, scope: Scope, transformation_index: usize) -> Self {
        Self {
            stratum_index,
            scope,
            transformation_index,
        }
    }

    #[must_use]
    pub(super) fn stratum_index(self) -> usize {
        self.stratum_index
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn scope(self) -> Scope {
        self.scope
    }
}

// =============================================================================
// AttributeLayout
// =============================================================================

/// Field IDs of a boundary's logical row in physical key/value order. Binding
/// IDs follow declaration positions; ordinary arrangement IDs follow the
/// original flattened key/value row. The key precedes the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AttributeLayout {
    key: Vec<usize>,
    value: Vec<usize>,
}

impl AttributeLayout {
    #[must_use]
    pub(super) fn from_columns(key: Vec<usize>, value: Vec<usize>) -> Self {
        Self { key, value }
    }

    #[must_use]
    pub(super) fn key(&self) -> &[usize] {
        &self.key
    }

    #[must_use]
    pub(super) fn value(&self) -> &[usize] {
        &self.value
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn key_mut(&mut self) -> &mut [usize] {
        &mut self.key
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn value_mut(&mut self) -> &mut [usize] {
        &mut self.value
    }

    /// Assigns contiguous slots to a key followed by its value.
    #[must_use]
    pub(super) fn new(key_arity: usize, value_arity: usize) -> Self {
        Self {
            key: (0..key_arity).collect(),
            value: (key_arity..key_arity + value_arity).collect(),
        }
    }

    /// Uses declaration column IDs for a binding's key and value. Static
    /// min/max contributions live in weights; append and mutable folds retain
    /// the aggregate column as a value.
    ///
    /// # Errors
    ///
    /// Returns an internal error if an aggregation has no declared min/max
    /// column.
    pub(super) fn from_binding(
        operation: &BindingOperation,
        declaration: &Relation,
        mutability: Mutability,
    ) -> Result<Self, PlanError> {
        match operation {
            BindingOperation::Dedup { order } => Ok(Self {
                key: order.clone(),
                value: Vec::new(),
            }),
            BindingOperation::Aggregation { order } => {
                let Some((aggregate_column, _)) = declaration.lattice() else {
                    return Err(PlanError::internal(format!(
                        "aggregation of {} has no declared min/max column",
                        declaration.name(),
                    )));
                };
                Ok(Self {
                    key: order.clone(),
                    value: match mutability {
                        Mutability::Static => Vec::new(),
                        Mutability::Append | Mutability::Mutable => vec![aggregate_column],
                    },
                })
            }
        }
    }
}

// =============================================================================
// ArrangementId and ArrangementOrigin
// =============================================================================

/// An index in one graph's arrangement array, stable when layouts change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ArrangementId(usize);

impl ArrangementId {
    #[must_use]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    #[must_use]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

/// The original plan boundary represented by an arrangement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ArrangementOrigin {
    /// Input rows have no known scan order before their first dedup. Its
    /// chosen order constrains consumers without an incoming sorting cost.
    InputDedup {
        relation_fingerprint: u64,
    },
    Transformation(TransformationLocation),
    Dedup {
        stratum_index: usize,
        scope: Scope,
        relation_fingerprint: u64,
    },
    Aggregation {
        stratum_index: usize,
        scope: Scope,
        relation_fingerprint: u64,
    },
    /// The static recursive min/max fold after leaving the loop. Its key
    /// order stays fixed by the declaration, independently of the inner fold.
    AggregationExit {
        stratum_index: usize,
        relation_fingerprint: u64,
    },
}

impl ArrangementOrigin {
    /// Identifies a binding's sorting boundary independently of its layout.
    #[must_use]
    pub(super) fn relation_binding(
        relation_fingerprint: u64,
        operation: &BindingOperation,
        stratum_index: usize,
        scope: Scope,
    ) -> Self {
        match operation {
            BindingOperation::Dedup { .. } => Self::Dedup {
                stratum_index,
                scope,
                relation_fingerprint,
            },
            BindingOperation::Aggregation { .. } => Self::Aggregation {
                stratum_index,
                scope,
                relation_fingerprint,
            },
        }
    }
}

// =============================================================================
// Arrangement
// =============================================================================

/// One materialized sorting boundary: an index, a dedup, or an aggregation.
/// Static dedup is included even when consolidation retains no trace. All
/// inputs contribute to this single boundary and share its selected layout;
/// multiple inputs express a union without an additional computation record.
#[derive(Debug)]
pub(super) struct Arrangement {
    origin: ArrangementOrigin,
    layout: AttributeLayout,
    inputs: Vec<Source>,
}

impl Arrangement {
    #[must_use]
    pub(super) fn new(
        origin: ArrangementOrigin,
        layout: AttributeLayout,
        inputs: Vec<Source>,
    ) -> Self {
        Self {
            origin,
            layout,
            inputs,
        }
    }

    #[must_use]
    pub(super) fn origin(&self) -> ArrangementOrigin {
        self.origin
    }

    #[must_use]
    pub(super) fn layout(&self) -> &AttributeLayout {
        &self.layout
    }

    #[must_use]
    pub(super) fn inputs(&self) -> &[Source] {
        &self.inputs
    }

    #[must_use]
    pub(super) fn inputs_mut(&mut self) -> &mut Vec<Source> {
        &mut self.inputs
    }
}

// =============================================================================
// FeedId
// =============================================================================

/// An index in one graph's feed array. A feed can reference only earlier
/// feeds; recursive dependencies pass through arrangement IDs instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FeedId(usize);

impl FeedId {
    #[must_use]
    pub(super) fn new(index: usize) -> Self {
        Self(index)
    }

    #[must_use]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

// =============================================================================
// Feed and JoinOrientation
// =============================================================================

/// Which planned input supplies the left side of an ordinary join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOrientation {
    Original,
    Swapped,
}

/// One computation supplying an arrangement or another feed. Unchanged steps
/// borrow the original plan; fused steps own a transformation whose inputs
/// and flow describe the current computation together. Fusion preserves the
/// downstream feed ID, location and output without changing the original
/// plan. Output order follows the computation and its inputs; a feed does not
/// represent an independent sorting boundary.
pub(super) struct Feed<'plan> {
    location: TransformationLocation,
    step: Cow<'plan, Transformation>,
    inputs: Vec<Source>,
    join_orientation: Option<JoinOrientation>,
}

impl<'plan> Feed<'plan> {
    #[must_use]
    pub(super) fn new(
        location: TransformationLocation,
        step: Cow<'plan, Transformation>,
        inputs: Vec<Source>,
        join_orientation: Option<JoinOrientation>,
    ) -> Self {
        Self {
            location,
            step,
            inputs,
            join_orientation,
        }
    }

    #[must_use]
    pub(super) fn location(&self) -> TransformationLocation {
        self.location
    }

    #[must_use]
    pub(super) fn step(&self) -> &Cow<'plan, Transformation> {
        &self.step
    }

    #[must_use]
    pub(super) fn step_mut(&mut self) -> &mut Cow<'plan, Transformation> {
        &mut self.step
    }

    #[must_use]
    pub(super) fn inputs(&self) -> &[Source] {
        &self.inputs
    }

    #[must_use]
    pub(super) fn join_orientation(&self) -> Option<JoinOrientation> {
        self.join_orientation
    }

    #[must_use]
    pub(super) fn inputs_mut(&mut self) -> &mut [Source] {
        &mut self.inputs
    }
}

impl fmt::Debug for Feed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The whole step includes collections and canonical forms. Keep the
        // operation and field mappings visible without expanding that state.
        f.debug_struct(self.step.operation_name())
            .field("location", &self.location)
            .field("inputs", &self.inputs)
            .field("flow", self.step.flow())
            .field("join_orientation", &self.join_orientation)
            .finish()
    }
}

// =============================================================================
// Source
// =============================================================================

/// A reference to an input representation, without owning its computation.
/// Program inputs reference their first dedup arrangement. Entering a scope
/// or reading feedback reuses a source ID without another sorting boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    /// The fixed collection of one empty row, without a sorting boundary.
    Unit,
    Arrangement(ArrangementId),
    Feed(FeedId),
}

// =============================================================================
// ArrangementGraph
// =============================================================================

/// The graph owns each sorting boundary and computation once. Inputs store
/// IDs, so shared consumers need no reference counting or copied source trees.
/// Arrangement references may close feedback cycles; feed references retain
/// dependency order, allowing projection composition in one forward pass.
#[derive(Debug, Default)]
pub(super) struct ArrangementGraph<'plan> {
    arrangements: Vec<Arrangement>,
    feeds: Vec<Feed<'plan>>,
}

impl<'plan> ArrangementGraph<'plan> {
    #[must_use]
    pub(super) fn arrangements(&self) -> &[Arrangement] {
        &self.arrangements
    }

    #[must_use]
    pub(super) fn arrangements_mut(&mut self) -> &mut [Arrangement] {
        &mut self.arrangements
    }

    #[must_use]
    pub(super) fn feeds(&self) -> &[Feed<'plan>] {
        &self.feeds
    }

    #[must_use]
    pub(super) fn feeds_mut(&mut self) -> &mut [Feed<'plan>] {
        &mut self.feeds
    }

    /// Registers a boundary under a stable ID. Its inputs may be filled after
    /// reservation so a recursive body can already refer to its result.
    #[must_use]
    pub(super) fn add_arrangement(&mut self, arrangement: Arrangement) -> ArrangementId {
        let id = ArrangementId::new(self.arrangements.len());
        self.arrangements.push(arrangement);
        id
    }

    /// Registers the static min/max fold after a recursive binding leaves its
    /// loop. `input` must identify that binding's arrangement; the exit owns a
    /// separate grouping layout in declaration order.
    #[must_use]
    pub(super) fn add_aggregation_exit(
        &mut self,
        stratum_index: usize,
        relation_fingerprint: u64,
        input: ArrangementId,
    ) -> ArrangementId {
        let mut key = self.arrangements[input.0].layout.key.clone();
        key.sort_unstable();
        self.add_arrangement(Arrangement {
            origin: ArrangementOrigin::AggregationExit {
                stratum_index,
                relation_fingerprint,
            },
            layout: AttributeLayout {
                key,
                value: Vec::new(),
            },
            inputs: vec![Source::Arrangement(input)],
        })
    }

    /// Registers a computation after its input feeds have been registered.
    #[must_use]
    pub(super) fn add_feed(&mut self, feed: Feed<'plan>) -> FeedId {
        let id = FeedId::new(self.feeds.len());
        self.feeds.push(feed);
        id
    }
}
