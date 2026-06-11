//! Rule planner for per-rule transformation planning.
//!
//! This component turns a single rule into a sequence of executable
//! transformations. The planning pipeline is split into phases:
//!
//! - `prepare`: applies local filters (var==var, var==const, placeholders), may
//!   perform (anti-)semijoins and comparison pushdown, and removes unused
//!   arguments to simplify the rule before joining.
//! - `core`: performs the core join between two selected positive atoms and
//!   then iterates semijoin/pushdown and projection removal to a fixed point.
//! - `fuse`: merges compatible KV-to-KV map steps into their producers and
//!   propagates key/value layout requirements upstream.
//! - `post`: aligns the final pipeline output with the rule head (variables and
//!   arithmetic expressions). Aggregation in the head is handled earlier at the
//!   stratum planning phase.
//!
//! The planner maintains a vector of transformation descriptors along with
//! dependency analyses.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;

use crate::parser::{Atom, FlowLogRule, Predicate};
use crate::planner::{Transformation, TransformationInfo};

mod common; // small utilities shared by planner phases
mod core; // core join, plus fixed-point of semijoin/pushdown and projection removal
mod fuse; // fuse KV-to-KV maps and propagate key/value layout constraints upstream
mod post; // align final output to the rule head (vars and arithmetic)
mod prepare; // local filters, semi-join and comparison before the core join
mod sip; // Side Information Passing (SIP) optimization for pushing down filters

/// Planner state for a single rule.
#[derive(Debug)]
pub(crate) struct RulePlanner {
    /// The original rule.
    rule: FlowLogRule,

    /// Linear list of planned transformation infos for the current rule.
    transformation_infos: Vec<TransformationInfo>,

    /// Mapping from a fingerprint to its producer indices and optional
    /// list of consumer indices.
    ///
    /// Note:
    /// 1. final transformation outputs have no consumers.
    /// 2. original EDBs have no producers.
    /// 3. One collection could have multiple producers.
    ///    e.g. when an IDB is derived multiple times in a single rule,
    ///    galen: OutP(x,z) :- C(y,w,z),OutP(x,w), OutP(x,y).
    /// 4. One collection could have multiple consumers.
    ///    e.g. when an atom can be semijoined to multiple other atoms.
    producer_consumer: HashMap<u64, (Vec<usize>, Vec<usize>)>,
}

impl RulePlanner {
    /// Creates a new empty RulePlanner.
    pub(crate) fn new(rule: FlowLogRule) -> Self {
        Self {
            rule,
            transformation_infos: Vec::new(),
            producer_consumer: HashMap::new(),
        }
    }

    /// Returns the planned transformations for this rule.
    #[inline]
    pub(crate) fn transformation_infos(&self) -> &Vec<TransformationInfo> {
        &self.transformation_infos
    }

    /// Returns the original rule.
    #[inline]
    pub(crate) fn rule(&self) -> &FlowLogRule {
        &self.rule
    }

    /// Rehash every transformation's lineage fingerprint into its
    /// content-based [canonical fingerprint](TransformationInfo::canonical_fp),
    /// so content-identical transformations across rules and strata share.
    ///
    /// `transformation_infos` is producer-before-consumer, so a single forward
    /// pass computes each canonical fingerprint with its inputs already
    /// canonical; a second pass substitutes uniformly. Splitting the passes
    /// avoids reading a half-rewritten state. Must run after `post` (the final
    /// lineage mutation) and before any fingerprint reader.
    pub(crate) fn canonicalize_fingerprints(&mut self) {
        let mut remap: HashMap<u64, u64> = HashMap::with_capacity(self.transformation_infos.len());
        for info in &self.transformation_infos {
            let canonical = info.canonical_fp(&remap);
            remap.insert(info.output_info_fp(), canonical);
        }
        for info in &mut self.transformation_infos {
            info.remap_fps(&remap);
        }
    }
}

/// =========================================================================
/// Rule Plan Tree Debugging Information
/// ========================================================================
impl RulePlanner {
    pub(crate) fn generate_rule_plan_tree_debug_map(&self) -> BTreeMap<u64, (String, Vec<u64>)> {
        let mut debug_info_map: BTreeMap<u64, (String, Vec<u64>)> = BTreeMap::new();

        if self.transformation_infos.is_empty() {
            return debug_info_map;
        }

        let atom_labels = self.rhs_atom_labels();
        let mut referenced_children = BTreeSet::new();

        for info in &self.transformation_infos {
            let tx = match info {
                TransformationInfo::KVToKV { .. } => Transformation::kv_to_kv(info),
                TransformationInfo::JoinToKV { .. } => Transformation::join(info),
                TransformationInfo::AntiJoinToKV { .. } => Transformation::antijoin(info),
            };
            let (label, children) = Self::build_transformation_debug_entry(&tx);
            referenced_children.extend(children.iter().copied());
            debug_info_map.insert(tx.output().fingerprint(), (label, children));
        }

        for child_fp in referenced_children {
            debug_info_map
                .entry(child_fp)
                .or_insert_with(|| (atom_labels[&child_fp].clone(), Vec::new()));
        }

        debug_info_map
    }

    /// Map of atom fingerprint → formatted `"name(arg1, ..., argN)"` label
    /// for every positive or negative atom on the rule's rhs, derived from
    /// `Atom`'s `Display` impl. Consumed by the plan-tree developer-debug
    /// walker so the leaf nodes show full binding context.
    pub(crate) fn rhs_atom_labels(&self) -> HashMap<u64, String> {
        self.rhs_atom_map(Atom::to_string)
    }

    /// Fingerprints of every positive/negative atom on the rule's rhs.
    /// Consumed by codegen to decide which transformation inputs are named
    /// atoms (label text comes from `display_name`).
    pub(crate) fn rhs_atom_fps(&self) -> HashSet<u64> {
        self.rhs_atom_map(|_| ()).into_keys().collect()
    }

    /// Build a `fingerprint → value` map over every positive/negative atom
    /// on the rule's rhs, with `value_of` deciding the value per atom.
    /// First occurrence wins on duplicate fingerprints.
    fn rhs_atom_map<T, F>(&self, mut value_of: F) -> HashMap<u64, T>
    where
        F: FnMut(&Atom) -> T,
    {
        let mut out = HashMap::new();
        for predicate in self.rule.rhs() {
            if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = predicate {
                out.entry(atom.fingerprint())
                    .or_insert_with(|| value_of(atom));
            }
        }
        out
    }

    fn build_transformation_debug_entry(tx: &Transformation) -> (String, Vec<u64>) {
        let children = tx.input_fingerprints();

        let mut label = tx.operation_name().to_string();
        if !label.is_empty() {
            label.push(' ');
        }
        label.push_str(&tx.flow().to_string());

        (label, children)
    }

    /// Render `self.transformation_infos` as an indexed list for trace output.
    /// `TransformationInfo::Display` terminates each block with `\n`; we add
    /// only the `[i]` prefix here.
    pub(crate) fn transformation_infos_dump(&self) -> String {
        if self.transformation_infos.is_empty() {
            return "  (none)".to_string();
        }
        let mut out = String::new();
        for (i, tx) in self.transformation_infos.iter().enumerate() {
            let _ = write!(out, "  [{:>3}] {}", i, tx);
        }
        out
    }
}

impl std::fmt::Display for RulePlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(last) = self.transformation_infos.last() else {
            return writeln!(f, "Plan Tree: (empty)");
        };
        let root = last.output_info_fp();
        let debug_map = self.generate_rule_plan_tree_debug_map();

        writeln!(f)?;
        writeln!(f, "Rule:\n{}", self.rule)?;
        writeln!(f)?;
        writeln!(f, "Plan Tree:")?;

        let mut walker = Walker {
            debug_info_map: &debug_map,
            ids: HashMap::new(),
            next_id: 1,
            expanded: HashSet::new(),
            stack: HashSet::new(),
        };
        let root_uid = walker.get_id(root);
        writeln!(f, "#{}  {}", root_uid, walker.node_title(root))?;
        walker.expanded.insert(root);
        walker.stack.insert(root);

        let rc: Vec<_> = walker.children(root).to_vec();
        for (i, cid) in rc.iter().enumerate() {
            walker.fmt_node(f, *cid, "", i + 1 == rc.len())?;
        }
        walker.stack.remove(&root);
        Ok(())
    }
}

struct Walker<'a> {
    debug_info_map: &'a BTreeMap<u64, (String, Vec<u64>)>,
    ids: HashMap<u64, usize>,
    next_id: usize,
    expanded: HashSet<u64>,
    stack: HashSet<u64>,
}

impl<'a> Walker<'a> {
    fn node_title(&self, id: u64) -> &str {
        self.debug_info_map
            .get(&id)
            .map_or("<unknown>", |(lbl, _)| lbl.as_str())
    }

    fn children(&self, id: u64) -> &[u64] {
        self.debug_info_map
            .get(&id)
            .map_or(&[], |(_, kids)| kids.as_slice())
    }

    fn get_id(&mut self, node: u64) -> usize {
        *self.ids.entry(node).or_insert_with(|| {
            let id = self.next_id;
            self.next_id += 1;
            id
        })
    }

    fn fmt_node(
        &mut self,
        f: &mut std::fmt::Formatter<'_>,
        node: u64,
        prefix: &str,
        is_last: bool,
    ) -> std::fmt::Result {
        let (branch, spacer) = if is_last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };
        let uid = self.get_id(node);
        let title = self.node_title(node);

        if self.stack.contains(&node) {
            writeln!(f, "{}{}⟲ #{} (cycle)", prefix, branch, uid)?;
            return Ok(());
        }

        if self.expanded.contains(&node) {
            writeln!(f, "{}{}↪ #{}", prefix, branch, uid)?;
            return Ok(());
        }

        writeln!(f, "{}{}#{}  {}", prefix, branch, uid, title)?;
        self.expanded.insert(node);
        self.stack.insert(node);

        let kids: Vec<_> = self.children(node).to_vec();
        let next_prefix = format!("{}{}", prefix, spacer);
        for (i, cid) in kids.iter().enumerate() {
            self.fmt_node(f, *cid, &next_prefix, i + 1 == kids.len())?;
        }
        self.stack.remove(&node);
        Ok(())
    }
}
