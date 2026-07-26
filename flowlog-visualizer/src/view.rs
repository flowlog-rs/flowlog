//! The presentation view. The profiler ships facts: a [`PlanGraph`]
//! (structure) and a [`Snapshot`] (measured metrics keyed to plan ids).
//! This module joins them into the JSON shape the template renders,
//! deriving all layout here: the spanning tree, extra/cross parents, roots,
//! the rule trees, and the shared-fingerprint badge. Flow is an integer
//! total; time is a per-worker [`Stats`] distribution.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use flowlog_profiler::PlanGraph;
use flowlog_profiler::Stats;
use flowlog_profiler::metrics::Snapshot;
use serde::Serialize;

/// One rendered snapshot (the template's per-snapshot object).
#[derive(Serialize)]
pub struct Report {
    pub label: String,
    pub num_workers: usize,
    pub roots: Vec<String>,
    pub nodes: BTreeMap<String, Node>,
    pub rules: Vec<Rule>,
    pub totals: Totals,
}

#[derive(Serialize)]
pub struct Node {
    pub name: String,
    pub label: String,
    pub block: String,
    pub fingerprint: Option<String>,
    pub children: Vec<String>,
    pub dag_parents: Vec<String>,
    pub extra_parents: Vec<String>,
    pub self_activations: Stats,
    pub self_total_active_ms: Stats,
    pub self_tup_in: Option<i64>,
    pub self_tup_out: Option<i64>,
    pub num_workers: usize,
    pub operators: Vec<Operator>,
}

#[derive(Serialize)]
pub struct Operator {
    pub addr: Vec<u32>,
    pub op_name: String,
    pub activations: Stats,
    pub total_active_ms: Stats,
    pub tup_in: Option<i64>,
    pub tup_out: Option<i64>,
}

#[derive(Serialize)]
pub struct Rule {
    pub text: String,
    pub root: String,
    pub nodes: BTreeMap<String, TreeNode>,
}

#[derive(Serialize)]
pub struct TreeNode {
    pub fingerprint: String,
    pub node: Option<String>,
    pub label: Option<String>,
    pub children: Vec<String>,
    pub parents: Vec<String>,
    pub shared: bool,
}

#[derive(Serialize)]
pub struct Totals {
    pub names: usize,
    pub operators_in_time: usize,
    pub operators_mapped: usize,
    pub total_mapped_ms: Stats,
    pub total_mapped_activations: Stats,
}

/// Join the plan with one snapshot's metrics into the render view.
pub fn build(plan: &PlanGraph, run: &Snapshot) -> Report {
    // Per-node deduped string parents (the DAG), keyed the way the view is.
    // Dedup as ids, not strings: "10" sorts before "2".
    let parents: BTreeMap<String, Vec<String>> = plan
        .nodes()
        .iter()
        .map(|n| {
            (
                n.id.to_string(),
                dedup(n.parents.clone())
                    .iter()
                    .map(usize::to_string)
                    .collect(),
            )
        })
        .collect();

    // Spanning tree: each node's first parent is its primary (tree) edge.
    let mut tree_children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, ps) in &parents {
        if let Some(primary) = ps.first() {
            tree_children
                .entry(primary.clone())
                .or_default()
                .push(id.clone());
        }
    }
    for kids in tree_children.values_mut() {
        kids.sort();
    }

    let mut roots: Vec<usize> = plan
        .nodes()
        .iter()
        .filter(|n| n.parents.is_empty())
        .map(|n| n.id)
        .collect();
    roots.sort_unstable();
    let roots: Vec<String> = roots.iter().map(usize::to_string).collect();

    let mut total_mapped_ms = Stats::default();
    let mut total_mapped_activations = Stats::default();
    let mut operators_mapped = 0usize;

    let mut nodes = BTreeMap::new();
    for n in plan.nodes() {
        let id = n.id.to_string();
        let m = &run.nodes[&n.id];
        operators_mapped += m.operators.len();
        total_mapped_ms = &total_mapped_ms + &m.active_ms;
        total_mapped_activations = &total_mapped_activations + &m.activations;

        let operators = m
            .operators
            .iter()
            .map(|op| Operator {
                addr: op.addr.0.clone(),
                op_name: op.op_name.clone(),
                activations: op.activations.clone(),
                total_active_ms: op.active_ms.clone(),
                tup_in: op.flow.tup_in,
                tup_out: op.flow.tup_out,
            })
            .collect();

        let dag_parents = parents[&id].clone();
        let extra_parents = dag_parents.get(1..).unwrap_or_default().to_vec();

        nodes.insert(
            id.clone(),
            Node {
                name: id.clone(),
                label: n.name.clone(),
                block: n.block.to_string(),
                fingerprint: n.fingerprint.clone(),
                children: tree_children.get(&id).cloned().unwrap_or_default(),
                dag_parents,
                extra_parents,
                self_activations: m.activations.clone(),
                self_total_active_ms: m.active_ms.clone(),
                self_tup_in: m.flow.tup_in,
                self_tup_out: m.flow.tup_out,
                num_workers: run.num_workers,
                operators,
            },
        );
    }

    Report {
        label: run.label.clone(),
        num_workers: run.num_workers,
        roots,
        totals: Totals {
            names: plan.nodes().len(),
            operators_in_time: run.operators_in_log,
            operators_mapped,
            total_mapped_ms,
            total_mapped_activations,
        },
        rules: build_rules(plan),
        nodes,
    }
}

/// One rendered rule-plan-tree view per plan rule, with the fingerprint
/// adjacency inverted to children and the shared-fingerprint badge computed
/// across rules.
fn build_rules(plan: &PlanGraph) -> Vec<Rule> {
    // fingerprint -> node id, and -> the node's display label.
    let mut fp_to_id: BTreeMap<&str, String> = BTreeMap::new();
    let mut fp_to_label: BTreeMap<&str, String> = BTreeMap::new();
    for n in plan.nodes() {
        if let Some(fp) = n.fingerprint.as_deref() {
            fp_to_id.entry(fp).or_insert_with(|| n.id.to_string());
            fp_to_label.entry(fp).or_insert_with(|| n.name.clone());
        }
    }

    // A fingerprint is shared when it appears in more than one rule.
    let mut fp_rule_count: BTreeMap<&str, usize> = BTreeMap::new();
    for rule in plan.rules() {
        let mut seen = BTreeSet::new();
        for tn in &rule.plan_tree {
            if seen.insert(tn.fingerprint.as_str()) {
                *fp_rule_count.entry(tn.fingerprint.as_str()).or_default() += 1;
            }
        }
    }

    plan.rules()
        .iter()
        .map(|rule| {
            // fingerprint -> its parents (last entry wins on a repeated fp).
            let mut parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for tn in &rule.plan_tree {
                parents.insert(tn.fingerprint.clone(), dedup(tn.parents.clone()));
            }
            // Invert to children.
            let mut children: BTreeMap<String, Vec<String>> =
                parents.keys().map(|fp| (fp.clone(), Vec::new())).collect();
            for (child, ps) in &parents {
                for p in ps {
                    children.entry(p.clone()).or_default().push(child.clone());
                }
            }
            for kids in children.values_mut() {
                kids.sort();
                kids.dedup();
            }
            // Root: the sink fingerprint (feeds nothing).
            let root = children
                .iter()
                .find(|(_, kids)| kids.is_empty())
                .map(|(fp, _)| fp.clone())
                .unwrap_or_default();

            let nodes = parents
                .keys()
                .map(|fp| {
                    (
                        fp.clone(),
                        TreeNode {
                            fingerprint: fp.clone(),
                            node: fp_to_id.get(fp.as_str()).cloned(),
                            label: fp_to_label.get(fp.as_str()).cloned(),
                            children: children.get(fp).cloned().unwrap_or_default(),
                            parents: parents[fp].clone(),
                            shared: fp_rule_count.get(fp.as_str()).is_some_and(|&c| c > 1),
                        },
                    )
                })
                .collect();

            Rule {
                text: rule.text.clone(),
                root,
                nodes,
            }
        })
        .collect()
}

fn dedup<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v.dedup();
    v
}
