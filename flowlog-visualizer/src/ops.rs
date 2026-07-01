//! Validation over the profiler's `ops.json` plan graph.
//!
//! The wire types (`Profiler`, `NodeProfile`, `RuleProfile`, `Addr`, …) are
//! shared with the compiler via the `flowlog-profiler` crate, so this module
//! only owns the *validated views* (`NodeSpec`/`RuleSpec`/`ValidatedOps`)
//! derived from a deserialized [`Profiler`]: it validates ids, turns operator
//! address arrays into the graph, and computes roots (nodes with no incoming
//! edges).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::bail;
use flowlog_profiler::Addr;
use flowlog_profiler::Profiler;

use crate::Result;

/// Flattened, validated node ready for aggregation.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub id: usize,
    pub label: String,
    pub block: String,
    pub fingerprint: Option<String>,
    pub tags: Vec<String>,
    pub parents: Vec<usize>,
    pub operators: BTreeSet<Addr>,
}

#[derive(Debug, Clone)]
pub struct RulePlanNodeSpec {
    pub children: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuleSpec {
    pub text: String,
    pub root: String,
    pub nodes: BTreeMap<String, RulePlanNodeSpec>,
}

#[derive(Debug, Clone)]
pub struct ValidatedOps {
    pub nodes: BTreeMap<usize, NodeSpec>,
    pub roots: Vec<usize>,
    pub rules: Vec<RuleSpec>,
    pub fingerprint_to_node: BTreeMap<String, usize>,
}

/// Flatten all nodes, ensure unique ids, and compute roots.
///
/// 1) Normalize node rows (dedup parents, normalize fingerprints).
/// 2) Validate structural integrity (unique ids, parents exist, fingerprint rules).
/// 3) Build rule plan trees (derive children, compute roots).
pub fn validate_and_build(profiler: &Profiler) -> Result<ValidatedOps> {
    // Phase 1: build map keyed by id and normalize node fields.
    let mut nodes: BTreeMap<usize, NodeSpec> = BTreeMap::new();
    for raw in profiler.nodes() {
        if nodes.contains_key(&raw.id) {
            bail!(format!("duplicate node id in ops.json: {}", raw.id));
        }

        let block = if raw.block.trim().is_empty() {
            "other".to_string()
        } else {
            raw.block.clone()
        };

        let fingerprint = raw
            .fingerprint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let ops: BTreeSet<Addr> = raw.operators.iter().cloned().collect();
        let parents = normalize_parents(raw.parents.clone());

        nodes.insert(
            raw.id,
            NodeSpec {
                id: raw.id,
                label: raw.name.clone(),
                block,
                fingerprint,
                tags: raw.tags.clone(),
                parents,
                operators: ops,
            },
        );
    }

    if nodes.is_empty() {
        bail!("ops.json contained no nodes");
    }

    // Phase 2a: enforce unique, non-empty fingerprints within the same block.
    // We still record a global fingerprint->node mapping for rule validation.
    let mut fingerprint_to_node: BTreeMap<String, usize> = BTreeMap::new();
    let mut fingerprint_block_to_node: BTreeMap<(String, String), usize> = BTreeMap::new();
    for (id, node) in &nodes {
        if let Some(fp) = &node.fingerprint {
            let key = (node.block.clone(), fp.clone());
            if let Some(prev) = fingerprint_block_to_node.insert(key.clone(), *id) {
                bail!(format!(
                    "fingerprint '{}' is used by multiple nodes in block '{}' ({} and {})",
                    fp, key.0, prev, id
                ));
            }
            fingerprint_to_node.entry(fp.clone()).or_insert(*id);
        }
    }

    // Phase 2b: compute roots (nodes with no parents).
    let mut roots: Vec<usize> = Vec::new();
    for (id, node) in &nodes {
        if node.parents.is_empty() {
            roots.push(*id);
        }
    }
    roots.sort();
    roots.dedup();

    // Phase 2c: basic sanity—every parent id must exist.
    for node in nodes.values() {
        for pid in &node.parents {
            if !nodes.contains_key(pid) {
                bail!(format!(
                    "node {} references missing parent id {}",
                    node.id, pid
                ));
            }
        }
    }

    // Phase 3: validate rules + plan trees (if provided).
    let mut rules_out: Vec<RuleSpec> = Vec::new();
    for raw_rule in profiler.rules() {
        let mut raw_parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut nodes_map: BTreeMap<String, RulePlanNodeSpec> = BTreeMap::new();

        for pn in &raw_rule.plan_tree {
            let fp = pn.fingerprint.trim();
            if fp.is_empty() {
                bail!(format!(
                    "rule '{}' has an empty fingerprint entry",
                    raw_rule.text
                ));
            }
            if nodes_map.contains_key(fp) {
                bail!(format!(
                    "rule '{}' has duplicate fingerprint '{}' in plan tree",
                    raw_rule.text, fp
                ));
            }
            if !fingerprint_to_node.contains_key(fp) {
                bail!(format!(
                    "rule '{}' references fingerprint '{}' not found in any node",
                    raw_rule.text, fp
                ));
            }

            let parents = normalize_parents(pn.parents.clone());
            raw_parents.insert(fp.to_string(), parents);
        }

        // Validate that all parents exist within the plan tree.
        for (fp, parents) in &raw_parents {
            for parent in parents {
                if !raw_parents.contains_key(parent) {
                    bail!(format!(
                        "rule '{}' references parent fingerprint '{}' not present in its plan tree",
                        raw_rule.text, parent
                    ));
                }
            }
            nodes_map.entry(fp.clone()).or_insert(RulePlanNodeSpec {
                children: Vec::new(),
            });
        }

        // Derive children from parent edges (child <- parent).
        for (child, parents) in &raw_parents {
            for parent in parents {
                nodes_map
                    .entry(parent.clone())
                    .or_insert(RulePlanNodeSpec {
                        children: Vec::new(),
                    })
                    .children
                    .push(child.clone());
            }
        }
        for node in nodes_map.values_mut() {
            node.children.sort();
            node.children.dedup();
        }

        // Compute sink (node with no children) and ensure exactly one.
        let sinks: Vec<String> = nodes_map
            .iter()
            .filter(|(_, node)| node.children.is_empty())
            .map(|(fp, _)| fp.clone())
            .collect();

        if sinks.len() != 1 {
            bail!(format!(
                "rule '{}' plan tree must have exactly one sink fingerprint (found {})",
                raw_rule.text,
                sinks.len()
            ));
        }

        let root_fp = sinks[0].clone();

        rules_out.push(RuleSpec {
            text: raw_rule.text.clone(),
            root: root_fp,
            nodes: nodes_map,
        });
    }

    // Phase 4: enforce that every fingerprinted node appears in rules.
    let mut rule_fps: BTreeSet<String> = BTreeSet::new();
    for rule in &rules_out {
        rule_fps.extend(rule.nodes.keys().cloned());
    }
    for (id, node) in &nodes {
        if let Some(fp) = &node.fingerprint
            && !rule_fps.contains(fp)
        {
            bail!(format!(
                "node {} has fingerprint '{}' but it is not recorded in rules",
                id, fp
            ));
        }
    }

    Ok(ValidatedOps {
        nodes,
        roots,
        rules: rules_out,
        fingerprint_to_node,
    })
}

pub(crate) fn normalize_parents<T: Ord>(mut parents: Vec<T>) -> Vec<T> {
    // Sort + deduplicate to ensure stable ordering for output and comparisons.
    parents.sort();
    parents.dedup();
    parents
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Contract test against the committed `ops.json` fixture (emitted by a real
    /// `reach.dl -P` build). The wire types are shared with `flowlog-profiler`,
    /// so a drift in the profiler's emit is already a compile error here; this
    /// additionally guards that an emitted graph still deserializes and passes
    /// the visualizer's validation.
    #[test]
    fn example_ops_json_deserializes_and_validates() {
        let json = include_str!("../examples/ops.json");
        let profiler: Profiler = serde_json::from_str(json)
            .expect("examples/ops.json should deserialize into the shared Profiler type");

        let validated = validate_and_build(&profiler).expect("ops.json should validate");

        assert!(!validated.nodes.is_empty(), "expected nodes");
        assert!(!validated.roots.is_empty(), "expected at least one root");
        // reach.dl has exactly two rules.
        assert_eq!(validated.rules.len(), 2, "expected reach.dl's two rules");
    }
}
