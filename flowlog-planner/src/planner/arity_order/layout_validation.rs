//! Validation of layout selections against the original plan boundaries.
//! Arrangement identities, key/value membership and fixed aggregation exits
//! are checked before plan rewriting begins.

use std::collections::HashMap;

use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;

use super::arrangement_graph::Arities;
use super::arrangement_graph::PhysicalArrangement;
use super::arrangement_graph::Scope;
use super::arrangement_graph::TransformationId;
use super::initialize::binding_arities;
use super::stratum_rewritter::Selection;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

/// Rejects choices outside their original K/V blocks or fixed boundaries.
/// Every identity uses the complete program's evaluation order.
/// Returns an internal error for inconsistent metadata, missing boundaries,
/// invalid permutations, changed fixed layouts or choices on non-join steps.
pub(super) fn validate(
    selection: &Selection,
    plan: &[StratumPlanner],
    program: &Program,
) -> Result<(), PlanError> {
    let relation_declaration_map: HashMap<u64, &Relation> = program
        .relations()
        .iter()
        .map(|declaration| (declaration.fingerprint(), declaration))
        .collect();
    for (node, selected) in &selection.layouts {
        let (stratum_index, scope) = match node {
            PhysicalArrangement::Arrangement {
                stratum_index,
                scope,
                ..
            }
            | PhysicalArrangement::Dedup {
                stratum_index,
                scope,
                ..
            }
            | PhysicalArrangement::Aggregation {
                stratum_index,
                scope,
                ..
            } => (*stratum_index, *scope),
            PhysicalArrangement::AggregationExit { stratum_index, .. } => {
                (*stratum_index, Scope::NonRecursive)
            }
        };
        let strata = plan.get(stratum_index).ok_or_else(|| {
            PlanError::internal(format!("layout selects missing stratum {stratum_index}"))
        })?;
        if matches!(
            node,
            PhysicalArrangement::Dedup { .. } | PhysicalArrangement::Aggregation { .. }
        ) && scope
            != if strata.is_recursive() {
                Scope::Recursive
            } else {
                Scope::NonRecursive
            }
        {
            return Err(PlanError::internal(format!(
                "binding layout selects the wrong scope: {node:?}"
            )));
        }
        let expected = match node {
            PhysicalArrangement::Arrangement {
                transformation_index,
                ..
            } => {
                let step = steps(strata, scope)
                    .get(*transformation_index)
                    .ok_or_else(|| {
                        PlanError::internal(format!("layout selects missing step {node:?}"))
                    })?;
                if !step.need_arrange() {
                    return Err(PlanError::internal(format!(
                        "layout selects a row-only step {node:?}"
                    )));
                }
                let (key, value) = step.output().arity();
                Arities::new(key, value)
            }
            PhysicalArrangement::Dedup {
                relation_fingerprint,
                ..
            }
            | PhysicalArrangement::Aggregation {
                relation_fingerprint,
                ..
            }
            | PhysicalArrangement::AggregationExit {
                relation_fingerprint,
                ..
            } => {
                let binding = strata
                    .relation_bindings()
                    .get(relation_fingerprint)
                    .ok_or_else(|| {
                        PlanError::internal(format!("layout selects missing binding {node:?}"))
                    })?;
                let declaration = relation_declaration_map
                    .get(relation_fingerprint)
                    .ok_or_else(|| {
                        PlanError::internal(format!("layout selects undeclared binding {node:?}"))
                    })?;
                let mutability = strata.mutability(*relation_fingerprint).ok_or_else(|| {
                    PlanError::internal(format!(
                        "layout selects binding without mutability {node:?}"
                    ))
                })?;
                if matches!(node, PhysicalArrangement::AggregationExit { .. }) {
                    if !strata.is_recursive()
                        || !strata
                            .recursion_leave_collections()
                            .contains(relation_fingerprint)
                        || mutability != Mutability::Static
                        || !matches!(binding.operation(), BindingOperation::Aggregation { .. })
                    {
                        return Err(PlanError::internal(format!(
                            "layout selects a nonexistent aggregation exit {node:?}"
                        )));
                    }
                    let Some((aggregate_column, _)) = declaration.lattice() else {
                        return Err(PlanError::internal(format!(
                            "aggregation exit {node:?} has no declared min/max column"
                        )));
                    };
                    Arities {
                        key: (0..declaration.arity())
                            .filter(|&column| column != aggregate_column)
                            .collect(),
                        value: Vec::new(),
                    }
                } else {
                    let expected_node = PhysicalArrangement::relation_binding(
                        *relation_fingerprint,
                        binding.operation(),
                        stratum_index,
                        scope,
                    );
                    if *node != expected_node {
                        return Err(PlanError::internal(format!(
                            "layout selects {node:?}, expected {expected_node:?}"
                        )));
                    }
                    binding_arities(binding.operation(), declaration, mutability)?
                }
            }
        };
        let mut key = selected.key.clone();
        let mut value = selected.value.clone();
        let mut expected_key = expected.key.clone();
        let mut expected_value = expected.value.clone();
        key.sort_unstable();
        value.sort_unstable();
        expected_key.sort_unstable();
        expected_value.sort_unstable();
        if key != expected_key || value != expected_value {
            return Err(PlanError::internal(format!(
                "layout {selected:?} does not permute the blocks {expected:?} of {node:?}"
            )));
        }
        if matches!(node, PhysicalArrangement::AggregationExit { .. }) && *selected != expected {
            return Err(PlanError::internal(format!(
                "layout changes fixed aggregation exit {node:?}"
            )));
        }
    }
    for id in selection.joins.keys() {
        let (stratum_index, scope, transformation_index) = match id {
            TransformationId::Rule {
                stratum_index,
                scope,
                transformation_index,
            } => (*stratum_index, *scope, *transformation_index),
            TransformationId::BindingInput { .. } | TransformationId::BindingSeed { .. } => {
                return Err(PlanError::internal(format!(
                    "orientation selects a binding projection {id:?}"
                )));
            }
        };
        let strata = plan.get(stratum_index).ok_or_else(|| {
            PlanError::internal(format!(
                "orientation selects missing stratum {stratum_index}"
            ))
        })?;
        let step = steps(strata, scope)
            .get(transformation_index)
            .ok_or_else(|| {
                PlanError::internal(format!("orientation selects missing step {id:?}"))
            })?;
        if !matches!(
            step,
            Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
        ) {
            return Err(PlanError::internal(format!(
                "orientation selects a step other than an ordinary join: {id:?}"
            )));
        }
    }
    Ok(())
}

/// Resolves the scope-local numbering used by graph transformation IDs.
#[must_use]
fn steps(strata: &StratumPlanner, scope: Scope) -> &[Transformation] {
    match scope {
        Scope::NonRecursive => strata.non_recursive_transformations(),
        Scope::Recursive => strata.recursive_transformations(),
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;

    use super::*;
    use crate::planner::ProgramPlanner;
    use crate::planner::arity_order::initialize::initialize;

    #[test]
    fn fixed_aggregation_exits_cannot_be_reordered() {
        let program = program(
            r#"
            .decl Seed(a: int32, b: int32, c: int32)
            .input Seed
            .decl Edge(src: int32, dst: int32, w: int32)
            .input Edge
            .decl R(a: int32, b: min<int32>, c: int32)
            .output R
            R(a,b,c) :- Seed(a,b,c).
            R(to,b+w,c) :- R(from,b,c), Edge(from,to,w).
            "#,
        );
        let plan = ProgramPlanner::from_program(&program, &mut None).unwrap();
        let graph = initialize(plan.strata(), &program).unwrap();
        let mut chosen = Selection {
            layouts: graph.nodes,
            joins: graph.joins,
        };
        let node = PhysicalArrangement::AggregationExit {
            stratum_index: 1,
            relation_fingerprint: compute_fp("r"),
        };
        chosen.layouts.get_mut(&node).unwrap().key.reverse();

        let error = validate(&chosen, plan.strata(), &program).unwrap_err();

        assert!(matches!(&error, PlanError::Internal(_)));
        assert!(error.to_string().contains("fixed aggregation exit"));
    }
}
