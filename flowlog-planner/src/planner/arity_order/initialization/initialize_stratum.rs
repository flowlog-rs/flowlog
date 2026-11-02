//! Stratum graph construction: feedback scopes, binding arrangements,
//! transformation feeds, and source resolution.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;

use flowlog_parser::Mutability;
use flowlog_parser::Relation;

use super::super::arrangement_graph::Arrangement;
use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementId;
use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::Feed;
use super::super::arrangement_graph::JoinOrientation;
use super::super::arrangement_graph::Scope;
use super::super::arrangement_graph::Source;
use super::super::arrangement_graph::TransformationLocation;
use super::CollectionProducerMap;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::RelationBindingPlan;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

/// Adds a stratum's boundaries and computations, publishing its completed
/// outer results. Feedback and the earlier seed retain distinct source IDs.
///
/// # Errors
///
/// Returns an internal error for unresolved producers or incomplete binding
/// declaration, mutability, or aggregation metadata.
pub(super) fn build_stratum<'plan>(
    stratum_index: usize,
    stratum: &'plan StratumPlanner,
    relation_declaration_map: &HashMap<u64, &Relation>,
    collection_producer_map: &mut CollectionProducerMap,
    graph: &mut ArrangementGraph<'plan>,
) -> Result<(), PlanError> {
    // Seeds and entered inputs can read prelude results, so resolve their
    // producers before replacing any earlier relation binding.
    register_transformations(
        stratum_index,
        Scope::NonRecursive,
        stratum.non_recursive_transformations(),
        collection_producer_map,
        None,
        graph,
    )?;

    // An unobservable loop is omitted by codegen, but its prelude still runs.
    if stratum.is_recursive() && stratum.recursion_leave_collections().is_empty() {
        return Ok(());
    }
    let binding_arrangement_map =
        reserve_binding_arrangements(stratum_index, stratum, relation_declaration_map, graph)?;

    if !stratum.is_recursive() {
        connect_binding_arrangements(
            stratum.relation_bindings(),
            &binding_arrangement_map,
            collection_producer_map,
            collection_producer_map,
            graph,
        )?;
        // Publish together after resolving every union against earlier
        // bindings, so a relation cannot accidentally replace its own seed.
        let completed_bindings = binding_arrangement_map
            .into_iter()
            .map(|(relation, id)| (relation, Source::Arrangement(id)));
        collection_producer_map.extend(completed_bindings);
        return Ok(());
    }

    let mut recursive_collection_producer_map: CollectionProducerMap = stratum
        .recursion_enter_collections()
        .iter()
        .map(|&fingerprint| {
            Ok((
                fingerprint,
                resolve_source(fingerprint, collection_producer_map, None)?,
            ))
        })
        .collect::<Result<_, PlanError>>()?;
    // Body reads use feedback while seeds keep reading the unchanged outer map.
    for &relation in stratum.recursion_feedback_collections() {
        if let Some(&id) = binding_arrangement_map.get(&relation) {
            recursive_collection_producer_map.insert(relation, Source::Arrangement(id));
        }
    }
    register_transformations(
        stratum_index,
        Scope::Recursive,
        stratum.recursive_transformations(),
        &mut recursive_collection_producer_map,
        Some(collection_producer_map),
        graph,
    )?;
    connect_binding_arrangements(
        stratum.relation_bindings(),
        &binding_arrangement_map,
        &recursive_collection_producer_map,
        collection_producer_map,
        graph,
    )?;

    // Static min/max folds again after leaving the loop, in declaration key
    // order. Keep that boundary separate from the inner feedback arrangement.
    for &relation in stratum.recursion_leave_collections() {
        let Some(&id) = binding_arrangement_map.get(&relation) else {
            continue;
        };
        let mut source = Source::Arrangement(id);
        let is_static_aggregation = stratum.mutability(relation) == Some(Mutability::Static)
            && matches!(
                stratum.relation_bindings()[&relation].operation(),
                BindingOperation::Aggregation { .. }
            );
        if is_static_aggregation {
            let exit = graph.add_aggregation_exit(stratum_index, relation, id);
            source = Source::Arrangement(exit);
        }
        collection_producer_map.insert(relation, source);
    }
    Ok(())
}

/// Registers each original transformation once, after its input feeds. A
/// keyed output additionally establishes an arrangement; row outputs retain
/// their feed ID. Binary input order follows the original transformation.
///
/// # Errors
///
/// Returns an internal error when an input producer cannot be resolved in the
/// current or permitted outer scope.
fn register_transformations<'plan>(
    stratum_index: usize,
    scope: Scope,
    transformations: &'plan [Transformation],
    collection_producer_map: &mut CollectionProducerMap,
    outer_producer_map: Option<&CollectionProducerMap>,
    graph: &mut ArrangementGraph<'plan>,
) -> Result<(), PlanError> {
    let steps = transformations.iter().enumerate();
    for (transformation_index, step) in steps {
        let input_fingerprints = step.input_fingerprints();
        let inputs: Vec<Source> = input_fingerprints
            .into_iter()
            .map(|fingerprint| {
                resolve_source(fingerprint, collection_producer_map, outer_producer_map)
            })
            .collect::<Result<_, _>>()?;
        let location = TransformationLocation::new(stratum_index, scope, transformation_index);
        let join_orientation = matches!(
            step,
            Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
        )
        .then_some(JoinOrientation::Original);
        let feed = graph.add_feed(Feed::new(
            location,
            Cow::Borrowed(step),
            inputs,
            join_orientation,
        ));
        let source = if step.need_arrange() {
            let (key, value) = step.output().arity();
            Source::Arrangement(graph.add_arrangement(Arrangement::new(
                ArrangementOrigin::Transformation(location),
                AttributeLayout::new(key, value),
                vec![Source::Feed(feed)],
            )))
        } else {
            Source::Feed(feed)
        };
        collection_producer_map.insert(step.output().fingerprint(), source);
    }
    Ok(())
}

/// Registers every binding with empty inputs so feedback can reference its
/// arrangement before the recursive body exists. The returned IDs stay valid
/// when those inputs are filled.
///
/// # Errors
///
/// Returns an internal error for missing declaration or mutability metadata,
/// or an aggregation without a declared min/max column.
fn reserve_binding_arrangements(
    stratum_index: usize,
    stratum: &StratumPlanner,
    relation_declaration_map: &HashMap<u64, &Relation>,
    graph: &mut ArrangementGraph<'_>,
) -> Result<BTreeMap<u64, ArrangementId>, PlanError> {
    let scope = if stratum.is_recursive() {
        Scope::Recursive
    } else {
        Scope::NonRecursive
    };
    let mut binding_arrangement_map = BTreeMap::new();
    for (&relation, binding) in stratum.relation_bindings() {
        let declaration = relation_declaration_map.get(&relation).ok_or_else(|| {
            PlanError::internal(format!("binding 0x{relation:016x} has no declaration"))
        })?;
        let mutability = stratum.mutability(relation).ok_or_else(|| {
            PlanError::internal(format!("binding 0x{relation:016x} has no mutability"))
        })?;
        let id = graph.add_arrangement(Arrangement::new(
            ArrangementOrigin::relation_binding(
                relation,
                binding.operation(),
                stratum_index,
                scope,
            ),
            AttributeLayout::from_binding(binding.operation(), declaration, mutability)?,
            Vec::new(),
        ));
        binding_arrangement_map.insert(relation, id);
    }
    Ok(binding_arrangement_map)
}

/// Connects reserved binding arrangements to their inputs in union order,
/// with the seed first. Seeds read only the outer scope; rules read locally
/// before checking the outer scope, so feedback cannot replace an earlier
/// seed. Every binding's inputs must be connected before publishing results
/// to the outer producer map.
///
/// # Errors
///
/// Returns an internal error when a seed or rule-result producer cannot be
/// resolved in its permitted scopes.
fn connect_binding_arrangements(
    relation_bindings: &BTreeMap<u64, RelationBindingPlan>,
    binding_arrangement_map: &BTreeMap<u64, ArrangementId>,
    rule_producer_map: &CollectionProducerMap,
    outer_producer_map: &CollectionProducerMap,
    graph: &mut ArrangementGraph<'_>,
) -> Result<(), PlanError> {
    for (&relation, binding) in relation_bindings {
        let mut inputs = Vec::new();
        if let Some(seed) = binding.seed() {
            inputs.push(resolve_source(
                seed.fingerprint(),
                outer_producer_map,
                None,
            )?);
        }
        for input in binding.inputs() {
            inputs.push(resolve_source(
                input.fingerprint(),
                rule_producer_map,
                Some(outer_producer_map),
            )?);
        }
        let id = binding_arrangement_map[&relation];
        *graph.arrangements_mut()[id.index()].inputs_mut() = inputs;
    }
    Ok(())
}

/// Resolves a collection locally before checking the permitted outer scope.
/// Missing producers indicate an incomplete plan or dependency order.
fn resolve_source(
    fingerprint: u64,
    collection_producer_map: &CollectionProducerMap,
    outer_producer_map: Option<&CollectionProducerMap>,
) -> Result<Source, PlanError> {
    collection_producer_map
        .get(&fingerprint)
        .or_else(|| outer_producer_map.and_then(|outer| outer.get(&fingerprint)))
        .copied()
        .ok_or_else(|| {
            PlanError::internal(format!(
                "collection 0x{fingerprint:016x} has no producer in the local or outer scope"
            ))
        })
}
