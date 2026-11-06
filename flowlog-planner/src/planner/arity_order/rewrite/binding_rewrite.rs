//! Binding operation layouts and alignment of direct relation seeds.
//!
//! Rule-result references use `upstream_rewrite`. Direct relation seeds are
//! adapted here; new conversions are returned for placement outside recursion.

use std::collections::HashMap;
use std::sync::Arc;

use flowlog_common::compute_fp;

use super::super::arrangement_graph::AttributeLayout;
use super::transformation_rewrite;
use super::upstream_rewrite;
use super::upstream_rewrite::RewrittenCollection;
use crate::planner::ArithmeticArgument;
use crate::planner::BindingOperation;
use crate::planner::Collection;
use crate::planner::Constraints;
use crate::planner::FactorArgument;
use crate::planner::PlanError;
use crate::planner::RelationBindingPlan;
use crate::planner::Transformation;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

/// Applies a binding's input order without resolving its producers again.
/// Dedup consumes rows in the selected declaration-column order. Aggregation
/// consumes logical rows and selects its grouping columns internally.
/// Rule-result producers must already emit the operation's input format.
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
    // A seed enters the union directly from an input relation or an earlier
    // binding, without a rule-result flow to rewrite. For a seed in (a,b,c)
    // and this binding's input in (c,a,b), adapt only this incoming edge;
    // changing the earlier binding would also change its other consumers.
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
            let projection = permute_seed_row(
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

/// Converts a seed to a permutation of its row columns under a new identity.
/// Canonical query content and mutability stay tied to the earlier binding.
/// `columns` must permute all input row positions. Returns an internal error
/// for keyed input, a wrong column count or an out-of-range column.
fn permute_seed_row(
    input: Arc<Collection>,
    fingerprint: u64,
    columns: &[usize],
) -> Result<Transformation, PlanError> {
    if input.arity() != (0, columns.len()) {
        return Err(PlanError::internal(format!(
            "seed conversion expects a row with {} columns, got {input}",
            columns.len(),
        )));
    }
    let reordered = transformation_rewrite::prepare_output(
        &input,
        AttributeLayout::from_columns(Vec::new(), columns.to_vec()),
    )?;
    let output = Arc::new(Collection::new(
        fingerprint,
        "layout_seed".to_owned(),
        reordered.collection().kv_layout().clone(),
        reordered.collection().canonical().clone(),
        input.mutability(),
    ));
    let values: Vec<ArithmeticArgument> = columns
        .iter()
        .map(|&column| ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, column))),
            rest: Vec::new(),
        })
        .collect();
    Ok(Transformation::RowToRow {
        input,
        output,
        flow: TransformationFlow::KVToKV {
            key: Arc::new(Vec::new()),
            value: Arc::new(values),
            constraints: Constraints::new(Vec::new(), Vec::new()),
            compares: Vec::new(),
        },
    })
}
