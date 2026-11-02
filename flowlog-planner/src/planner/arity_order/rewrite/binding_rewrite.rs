//! Binding input alignment, seed conversion and operation column order.
//!
//! Upstream collection references use `upstream_rewrite`. A seed conversion
//! is returned separately so it can remain outside the recursive scope.

use std::collections::HashMap;
use std::sync::Arc;

use flowlog_common::compute_fp;

use super::super::arrangement_graph::AttributeLayout;
use super::upstream_rewrite;
use super::upstream_rewrite::RewrittenCollection;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::RelationBindingPlan;
use crate::planner::Transformation;

/// Applies a binding's input order without resolving its producers again.
/// Dedup consumes rows in the selected declaration-column order. Aggregation
/// consumes logical rows and selects its grouping columns internally.
/// Returns a seed projection when its row order still differs from that order,
/// or an internal error for an invalid seed view. The projection runs before
/// the binding and outside any feedback loop.
pub(super) fn rewrite(
    binding: &mut RelationBindingPlan,
    binding_layout: &AttributeLayout,
    rewritten_outputs: &HashMap<u64, RewrittenCollection>,
    stratum_index: usize,
    relation_fingerprint: u64,
) -> Result<Option<Transformation>, PlanError> {
    for input in binding.inputs_mut() {
        upstream_rewrite::rewrite_collection(input, rewritten_outputs)?;
    }
    let mut seed_projection = None;
    let required_seed_columns: Vec<usize> = match binding.operation() {
        BindingOperation::Dedup {
            order: original_binding_order,
        } => binding_layout
            .key()
            .iter()
            .map(|declaration_column| {
                original_binding_order
                    .iter()
                    .position(|column| column == declaration_column)
                    .expect("validated binding column")
            })
            .collect(),
        BindingOperation::Aggregation { .. } => (0..binding.inputs()[0].arity().1).collect(),
    };
    if let Some(seed) = binding.seed_mut() {
        let seed_column_order = upstream_rewrite::rewrite_collection(seed, rewritten_outputs)?;
        // The binding order names declaration columns; this projection must
        // instead name positions in the seed's current physical row.
        let seed_projection_columns: Vec<usize> = required_seed_columns
            .iter()
            .map(|original_column| {
                seed_column_order
                    .value()
                    .iter()
                    .position(|column| column == original_column)
                    .expect("seed retains every column")
            })
            .collect();
        if !seed_projection_columns
            .iter()
            .copied()
            .eq(0..seed_projection_columns.len())
        {
            let projection_fingerprint = compute_fp((
                "layout_seed",
                stratum_index,
                relation_fingerprint,
                seed.fingerprint(),
                &seed_projection_columns,
            ));
            let projection = Transformation::row_projection(
                Arc::clone(seed),
                projection_fingerprint,
                &seed_projection_columns,
            )?;
            *seed = Arc::clone(projection.output());
            seed_projection = Some(projection);
        }
    }
    let (BindingOperation::Dedup { order } | BindingOperation::Aggregation { order }) =
        binding.operation_mut();
    *order = binding_layout.key().to_vec();
    Ok(seed_projection)
}
