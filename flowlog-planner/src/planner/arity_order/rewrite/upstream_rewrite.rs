//! Rewritten upstream outputs and their consumer input references.
//!
//! Transformations and bindings share the same input rewrite. A consumer can
//! read keyed data or flattened rows from a producer with the same identity.

use std::collections::HashMap;
use std::sync::Arc;

use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::JoinOrientation;
use crate::planner::Collection;
use crate::planner::PlanError;
use crate::planner::Transformation;

/// A producer's rewritten collection and its column correspondence.
/// The same record describes an output or a consumer's view of that output.
#[derive(Debug, Clone)]
pub(super) struct RewrittenCollection {
    /// Metadata for the new physical order, retaining the producer fingerprint.
    collection: Arc<Collection>,
    /// Original flattened column IDs, listed in their new physical order.
    /// For a row `(a,b,c)` rewritten as `(c,a,b)`, `value` is `[2,0,1]`.
    /// Reading old column 0 therefore uses new value slot 1.
    column_order: AttributeLayout,
}

impl RewrittenCollection {
    /// Records the new producer view and its original-column correspondence.
    #[must_use]
    pub(super) fn new(collection: Arc<Collection>, column_order: AttributeLayout) -> Self {
        Self {
            collection,
            column_order,
        }
    }

    #[must_use]
    pub(super) fn collection(&self) -> &Arc<Collection> {
        &self.collection
    }

    #[must_use]
    pub(super) fn column_order(&self) -> &AttributeLayout {
        &self.column_order
    }

    /// Resolves a producer while preserving a consumer's keyed or row view.
    /// Returns an internal error if a producer changes the input's arity.
    pub(super) fn resolve_upstream_output(
        input: &Arc<Collection>,
        rewritten_outputs: &HashMap<u64, Self>,
    ) -> Result<Self, PlanError> {
        let Some(producer_rewrite) = rewritten_outputs.get(&input.fingerprint()) else {
            // Graph validation has already resolved every source. A source
            // absent from this output map is a relation reader: its slots stay
            // logical, and codegen decodes the binding's physical row order.
            let (key_arity, value_arity) = input.arity();
            return Ok(Self {
                collection: Arc::clone(input),
                column_order: AttributeLayout::new(key_arity, value_arity),
            });
        };
        if input.arity() == producer_rewrite.collection.arity() {
            return Ok(producer_rewrite.clone());
        }
        let (key_arity, value_arity) = producer_rewrite.collection.arity();
        if input.arity() == (0, key_arity + value_arity) {
            // Flattening an arrangement keeps its physical key-then-value
            // sequence. The correspondence must flatten with it.
            return Ok(Self {
                collection: Arc::new(producer_rewrite.collection.unkeyed()),
                column_order: AttributeLayout::from_columns(
                    Vec::new(),
                    producer_rewrite
                        .column_order
                        .key()
                        .iter()
                        .chain(producer_rewrite.column_order.value())
                        .copied()
                        .collect(),
                ),
            });
        }
        Err(PlanError::internal(format!(
            "collection 0x{:016x} changes input arity from {:?} to {:?}",
            input.fingerprint(),
            input.arity(),
            producer_rewrite.collection.arity(),
        )))
    }

    /// Finds the physical slot holding an original input column.
    /// Returns an internal error for a missing or cross-block column.
    pub(super) fn remap_slot(
        &self,
        is_key: bool,
        original_index: usize,
    ) -> Result<usize, PlanError> {
        let original_columns = if is_key {
            self.column_order.key()
        } else {
            self.column_order.value()
        };
        let original_column = if is_key {
            original_index
        } else {
            self.column_order.key().len() + original_index
        };
        original_columns
            .iter()
            .position(|&column| column == original_column)
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "collection 0x{:016x} has no {} slot {original_index} under {:?}",
                    self.collection.fingerprint(),
                    if is_key { "key" } else { "value" },
                    self.column_order,
                ))
            })
    }
}

/// Updates one input reference and returns its original-column mapping.
/// The consumer retains its keyed or flattened view of the producer.
/// Returns an internal error if the producer no longer supplies this view.
pub(super) fn rewrite_collection(
    input: &mut Arc<Collection>,
    rewritten_outputs: &HashMap<u64, RewrittenCollection>,
) -> Result<AttributeLayout, PlanError> {
    let input_rewrite = RewrittenCollection::resolve_upstream_output(input, rewritten_outputs)?;
    *input = input_rewrite.collection;
    Ok(input_rewrite.column_order)
}

/// Reconnects an operator's existing inputs and applies its chosen direction.
/// Expression references must already reflect these representations and sides.
/// Returns an internal error if a producer changes an input's arity.
pub(super) fn rewrite_transformation(
    transformation: &mut Transformation,
    rewritten_outputs: &HashMap<u64, RewrittenCollection>,
    join_orientation: JoinOrientation,
) -> Result<(), PlanError> {
    match transformation {
        Transformation::RowToRow { input, .. }
        | Transformation::RowToKv { input, .. }
        | Transformation::KvToRow { input, .. }
        | Transformation::KvToKv { input, .. } => {
            rewrite_collection(input, rewritten_outputs)?;
        }
        Transformation::JnToRow {
            input: (left, right),
            ..
        }
        | Transformation::JnToKv {
            input: (left, right),
            ..
        }
        | Transformation::NJnToRow {
            input: (left, right),
            ..
        }
        | Transformation::NJnToKv {
            input: (left, right),
            ..
        } => {
            rewrite_collection(left, rewritten_outputs)?;
            rewrite_collection(right, rewritten_outputs)?;
            if join_orientation == JoinOrientation::Swapped {
                std::mem::swap(left, right);
            }
        }
        Transformation::AggToRow {
            input: (matches, groups),
            ..
        }
        | Transformation::AggToKv {
            input: (matches, groups),
            ..
        } => {
            rewrite_collection(matches, rewritten_outputs)?;
            if let Some(groups) = groups {
                rewrite_collection(groups, rewritten_outputs)?;
            }
        }
    }
    Ok(())
}
