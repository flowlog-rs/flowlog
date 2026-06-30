//! Aggregation model: combine UI tree (from ops.json) with time and memory logs.

use crate::Result;
use crate::log::{MemoryIndex, TimeIndex};
use crate::ops::{NodeSpec, RuleSpec, normalize_parents};
use crate::stats::Stats;
use flowlog_profiler::Addr;

use anyhow::bail;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize)]
pub struct OperatorView {
    pub addr: Vec<u32>,
    pub op_name: String,
    pub activations: Stats,
    pub total_active_ms: Stats,
    /// Tuple flow in/out; None when the operator reports `n/a`.
    pub tup_in: Option<Stats>,
    pub tup_out: Option<Stats>,
    /// None if this operator has no memory row in the memory log.
    pub batched_in: Option<Stats>,
    pub merges: Option<Stats>,
    pub merge_in: Option<Stats>,
    pub merge_out: Option<Stats>,
    pub dropped: Option<Stats>,
    /// Batcher peak size / capacity; present only with memory data.
    pub bat_bytes: Option<Stats>,
    pub bat_cap: Option<Stats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NameNodeView {
    pub name: String,
    pub label: String,
    pub block: String,
    pub fingerprint: Option<String>,
    pub tags: Vec<String>,

    /// Primary tree children (spanning tree derived from DAG).
    pub children: Vec<String>,

    /// All DAG parents (may include multi-parent edges).
    pub dag_parents: Vec<String>,

    /// Additional parents beyond the chosen primary parent (for DAG info).
    pub extra_parents: Vec<String>,

    /// Aggregated over operators owned by this name (sum of means).
    pub self_activations: Stats,
    pub self_total_active_ms: Stats,

    /// Aggregated tuple flow (summed over operators that report it).
    pub self_tup_in: Stats,
    pub self_tup_out: Stats,
    /// True if at least one operator reports tuple flow.
    pub has_flow_data: bool,

    /// Aggregated memory fields (summed over operators that have memory data).
    pub self_batched_in: Stats,
    pub self_merges: Stats,
    pub self_merge_in: Stats,
    pub self_merge_out: Stats,
    pub self_dropped: Stats,
    /// True if at least one operator has memory row.
    pub has_memory_data: bool,

    /// Number of workers used for aggregation.
    pub num_workers: usize,

    /// Operators owned by this name (sorted by addr asc).
    pub operators: Vec<OperatorView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RulePlanNodeView {
    pub fingerprint: String,
    pub node: Option<String>,
    pub label: Option<String>,
    pub children: Vec<String>,
    pub parents: Vec<String>,
    pub shared: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleView {
    pub text: String,
    pub root: String,
    pub nodes: BTreeMap<String, RulePlanNodeView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportData {
    pub roots: Vec<String>,
    pub nodes: BTreeMap<String, NameNodeView>,
    pub rules: Vec<RuleView>,
    pub totals: TotalsView,
    pub num_workers: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TotalsView {
    pub names: usize,
    pub operators_in_time: usize,
    pub operators_mapped: usize,
    pub total_mapped_ms: Stats,
    pub total_mapped_activations: Stats,
    pub total_batched_in: Stats,
}

/// Build report data. Performs:
/// - detect operator addr assigned to multiple names (error)
/// - warn (stderr) about mapped addrs missing from time log
/// - validate that wherever an addr appears in both logs, the op_name agrees
pub fn build_report_data(
    nodes_spec: &BTreeMap<String, NodeSpec>,
    roots: &[String],
    rules_spec: &[RuleSpec],
    fingerprint_to_node: &BTreeMap<String, String>,
    time: &TimeIndex,
    memory: &MemoryIndex,
) -> Result<ReportData> {
    // Phase 0: cross-validate op_name alignment between time log and memory log.
    for (addr, mr) in memory {
        if let Some(tr) = time.get(addr)
            && tr.op_name != mr.op_name
        {
            bail!(format!(
                "op_name mismatch at addr {:?}: time log has {:?} but memory log has {:?}",
                addr.0, tr.op_name, mr.op_name
            ));
        }
    }

    // Phase 1: enforce each operator addr belongs to at most one name (strict).
    let mut owner: BTreeMap<&Addr, &str> = BTreeMap::new();
    for (name, spec) in nodes_spec {
        for addr in &spec.operators {
            if let Some(prev) = owner.insert(addr, name.as_str()) {
                bail!(format!(
                    "operator addr {:?} is assigned to multiple names: {} and {}",
                    addr.0, prev, name
                ));
            }
        }
    }

    // Phase 2: normalize parent lists once and derive tree/DAG metadata.
    let mut normalized_parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, spec) in nodes_spec {
        let parents = normalize_parents(spec.parents.iter().map(|p| p.to_string()).collect());
        normalized_parents.insert(name.clone(), parents);
    }

    let mut primary_parent: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut extra_parents: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (name, parents) in &normalized_parents {
        if parents.is_empty() {
            primary_parent.insert(name.clone(), None);
            extra_parents.insert(name.clone(), vec![]);
            continue;
        }

        let primary = parents[0].clone();
        let extras = parents[1..].to_vec();
        primary_parent.insert(name.clone(), Some(primary));
        extra_parents.insert(name.clone(), extras);
    }

    let mut tree_children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (child, pp) in &primary_parent {
        if let Some(p) = pp {
            tree_children
                .entry(p.clone())
                .or_default()
                .push(child.clone());
        }
    }
    for kids in tree_children.values_mut() {
        kids.sort();
    }

    let mut roots_set: BTreeSet<String> = roots.iter().cloned().collect();
    for (name, pp) in &primary_parent {
        if pp.is_none() {
            roots_set.insert(name.clone());
        }
    }
    let roots: Vec<String> = roots_set.into_iter().collect();

    // Phase 3: build per-name operator lists + aggregates.
    let mut nodes_view: BTreeMap<String, NameNodeView> = BTreeMap::new();

    let mut total_mapped_ms = Stats::default();
    let mut total_mapped_activations = Stats::default();
    let mut operators_mapped = 0usize;
    let mut total_batched_in = Stats::default();
    let mut num_workers = 0usize;

    for (name, spec) in nodes_spec {
        let mut operators: Vec<OperatorView> = Vec::new();
        let mut self_ms = Stats::default();
        let mut self_act = Stats::default();
        let mut self_tup_in = Stats::default();
        let mut self_tup_out = Stats::default();
        let mut has_flow_data = false;
        let mut self_batched_in = Stats::default();
        let mut self_merges = Stats::default();
        let mut self_merge_in = Stats::default();
        let mut self_merge_out = Stats::default();
        let mut self_dropped = Stats::default();
        let mut has_memory_data = false;

        for addr in &spec.operators {
            let (act_stats, ms_stats, tup_in, tup_out, op_name) = match time.get(addr) {
                Some(tr) => {
                    self_ms = &self_ms + &tr.total_active_ms;
                    self_act = &self_act + &tr.activations;
                    total_mapped_ms = &total_mapped_ms + &tr.total_active_ms;
                    total_mapped_activations = &total_mapped_activations + &tr.activations;
                    operators_mapped += 1;
                    if tr.num_workers > num_workers {
                        num_workers = tr.num_workers;
                    }
                    (
                        tr.activations.clone(),
                        tr.total_active_ms.clone(),
                        tr.tup_in.clone(),
                        tr.tup_out.clone(),
                        tr.op_name.clone(),
                    )
                }
                None => {
                    tracing::warn!(
                        "ops.json maps name '{}' to addr {:?}, but addr not found in time log",
                        name,
                        addr.0
                    );
                    (
                        Stats::default(),
                        Stats::default(),
                        None,
                        None,
                        String::new(),
                    )
                }
            };

            let mem_row = memory.get(addr);

            let (batched_in, merges, merge_in_s, merge_out_s, dropped, bat_bytes, bat_cap) =
                match mem_row {
                    Some(mr) => {
                        has_memory_data = true;
                        self_batched_in = &self_batched_in + &mr.batched_in;
                        self_merges = &self_merges + &mr.merges;
                        self_merge_in = &self_merge_in + &mr.merge_in;
                        self_merge_out = &self_merge_out + &mr.merge_out;
                        self_dropped = &self_dropped + &mr.dropped;
                        total_batched_in = &total_batched_in + &mr.batched_in;
                        if mr.num_workers > num_workers {
                            num_workers = mr.num_workers;
                        }
                        (
                            Some(mr.batched_in.clone()),
                            Some(mr.merges.clone()),
                            Some(mr.merge_in.clone()),
                            Some(mr.merge_out.clone()),
                            Some(mr.dropped.clone()),
                            Some(mr.bat_bytes.clone()),
                            Some(mr.bat_cap.clone()),
                        )
                    }
                    None => (None, None, None, None, None, None, None),
                };

            if let Some(t) = &tup_in {
                self_tup_in = &self_tup_in + t;
                has_flow_data = true;
            }
            if let Some(t) = &tup_out {
                self_tup_out = &self_tup_out + t;
                has_flow_data = true;
            }

            operators.push(OperatorView {
                addr: addr.0.clone(),
                op_name,
                activations: act_stats,
                total_active_ms: ms_stats,
                tup_in,
                tup_out,
                batched_in,
                merges,
                merge_in: merge_in_s,
                merge_out: merge_out_s,
                dropped,
                bat_bytes,
                bat_cap,
            });
        }

        operators.sort_by(|a, b| a.addr.cmp(&b.addr));

        nodes_view.insert(
            name.clone(),
            NameNodeView {
                name: name.clone(),
                label: spec.label.clone(),
                block: spec.block.clone(),
                fingerprint: spec.fingerprint.clone(),
                tags: spec.tags.clone(),
                children: tree_children.get(name).cloned().unwrap_or_default(),
                dag_parents: normalized_parents.get(name).cloned().unwrap_or_default(),
                extra_parents: extra_parents.get(name).cloned().unwrap_or_default(),
                self_activations: self_act,
                self_total_active_ms: self_ms,
                self_tup_in,
                self_tup_out,
                has_flow_data,
                self_batched_in,
                self_merges,
                self_merge_in,
                self_merge_out,
                self_dropped,
                has_memory_data,
                num_workers,
                operators,
            },
        );
    }

    Ok(ReportData {
        roots,
        num_workers,
        totals: TotalsView {
            names: nodes_spec.len(),
            operators_in_time: time.len(),
            operators_mapped,
            total_mapped_ms,
            total_mapped_activations,
            total_batched_in,
        },
        nodes: nodes_view,
        rules: build_rule_views(rules_spec, nodes_spec, fingerprint_to_node),
    })
}

fn build_rule_views(
    rules_spec: &[RuleSpec],
    nodes_spec: &BTreeMap<String, NodeSpec>,
    fingerprint_to_node: &BTreeMap<String, String>,
) -> Vec<RuleView> {
    let mut fp_to_rules: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (rule_idx, rule) in rules_spec.iter().enumerate() {
        for fp in rule.nodes.keys() {
            fp_to_rules.entry(fp.clone()).or_default().push(rule_idx);
        }
    }

    let mut views = Vec::new();

    for (rule_idx, rule) in rules_spec.iter().enumerate() {
        let mut parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (fp, node) in &rule.nodes {
            for child in &node.children {
                parents.entry(child.clone()).or_default().push(fp.clone());
            }
        }

        let mut nodes_view: BTreeMap<String, RulePlanNodeView> = BTreeMap::new();
        for (fp, node) in &rule.nodes {
            let node_name = fingerprint_to_node.get(fp).cloned();
            let label = node_name
                .as_ref()
                .and_then(|n| nodes_spec.get(n))
                .map(|s| s.label.clone());

            let parent_list = parents.get(fp).cloned().unwrap_or_default();
            let shared = fp_to_rules
                .get(fp)
                .map(|rules| rules.iter().any(|&idx| idx != rule_idx))
                .unwrap_or(false);

            nodes_view.insert(
                fp.clone(),
                RulePlanNodeView {
                    fingerprint: fp.clone(),
                    node: node_name,
                    label,
                    children: node.children.clone(),
                    parents: parent_list,
                    shared,
                },
            );
        }

        views.push(RuleView {
            text: rule.text.clone(),
            root: rule.root.clone(),
            nodes: nodes_view,
        });
    }

    views
}
