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

use crate::{Transformation, TransformationInfo};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use parser::{FlowLogRule, Predicate};

mod common; // small utilities shared by planner phases
mod core; // core join, plus fixed-point of semijoin/pushdown and projection removal
mod fuse; // fuse KV-to-KV maps and propagate key/value layout constraints upstream
mod post; // align final output to the rule head (vars and arithmetic)
mod prepare; // local filters, semi-join and comparison before the core join

/// Planner state for a single rule.
#[derive(Debug)]
pub struct RulePlanner {
    /// The original rule.
    rule: FlowLogRule,

    /// Linear list of planned transformation infos for the current rule.
    transformation_infos: Vec<TransformationInfo>,

    /// Mapping from an fingerprint to its producer indices and optional
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
    pub fn new(rule: FlowLogRule) -> Self {
        Self {
            rule,
            transformation_infos: Vec::new(),
            producer_consumer: HashMap::new(),
        }
    }

    /// Returns the planned transformations for this rule.
    #[inline]
    pub fn transformation_infos(&self) -> &Vec<TransformationInfo> {
        &self.transformation_infos
    }

    /// Returns the original rule.
    #[inline]
    pub(super) fn rule(&self) -> &FlowLogRule {
        &self.rule
    }
}

/// =========================================================================
/// Rule Plan Tree Debugging Information
/// ========================================================================
impl RulePlanner {
    pub fn generate_rule_plan_tree_debug_map(&self) -> BTreeMap<u64, (String, Vec<u64>)> {
        let mut debug_info_map: BTreeMap<u64, (String, Vec<u64>)> = BTreeMap::new();

        if self.transformation_infos.is_empty() {
            return debug_info_map;
        }

        let atom_info = self.collect_atom_debug_info();

        let transformations: Vec<Transformation> = self
            .transformation_infos
            .iter()
            .map(|info| match info {
                TransformationInfo::KVToKV { .. } => Transformation::kv_to_kv(info),
                TransformationInfo::JoinToKV { .. } => Transformation::join(info),
                TransformationInfo::AntiJoinToKV { .. } => Transformation::antijoin(info),
            })
            .collect();

        let mut referenced_children = BTreeSet::new();

        for tx in &transformations {
            let (label, children) = Self::build_transformation_debug_entry(tx);
            referenced_children.extend(children.iter().copied());
            debug_info_map.insert(tx.output().fingerprint(), (label, children));
        }

        for child_fp in referenced_children {
            debug_info_map.entry(child_fp).or_insert_with(|| {
                let (name, args) = atom_info.get(&child_fp).unwrap();
                let values = args.join(", ");
                (format!("[{}] K:(), V:({})", name, values), Vec::new())
            });
        }

        debug_info_map
    }

    fn collect_atom_debug_info(&self) -> HashMap<u64, (String, Vec<String>)> {
        let mut atom_info = HashMap::new();
        for predicate in self.rule.rhs() {
            if let Some((fp, info)) = match predicate {
                Predicate::PositiveAtomPredicate(atom) | Predicate::NegativeAtomPredicate(atom) => {
                    Some((
                        atom.fingerprint(),
                        (
                            atom.name().to_string(),
                            atom.arguments().iter().map(|arg| arg.to_string()).collect(),
                        ),
                    ))
                }
                _ => None,
            } {
                atom_info.insert(fp, info);
            }
        }
        atom_info
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
}

impl fmt::Display for RulePlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let root = self.transformation_infos.last().unwrap().output_info_fp();
        struct Walker<'a> {
            debug_info_map: &'a BTreeMap<u64, (String, Vec<u64>)>,
            ids: BTreeMap<u64, usize>,
            next_id: usize,
            expanded: BTreeSet<u64>,
            stack: BTreeSet<u64>,
        }

        impl<'a> Walker<'a> {
            fn node_title(&self, id: u64) -> &str {
                self.debug_info_map
                    .get(&id)
                    .map(|(lbl, _)| lbl.as_str())
                    .unwrap_or("<unknown>")
            }

            fn children(&self, id: u64) -> &[u64] {
                self.debug_info_map
                    .get(&id)
                    .map(|(_, kids)| kids.as_slice())
                    .unwrap_or(&[])
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
                f: &mut fmt::Formatter<'_>,
                node: u64,
                prefix: &str,
                is_last: bool,
            ) -> fmt::Result {
                let (branch, spacer) = if is_last {
                    ("└───────────── ", "               ")
                } else {
                    ("├───────────── ", "│              ")
                };
                let uid = self.get_id(node);
                let title = self.node_title(node);

                if self.stack.contains(&node) {
                    writeln!(
                        f,
                        "{}{}{}  [@{}]  ⚠ reentry suppressed",
                        prefix, branch, title, uid
                    )?;
                    return Ok(());
                }

                if self.expanded.contains(&node) {
                    writeln!(f, "{}{}[@{}]  ↩︎ ref", prefix, branch, uid)?;
                    return Ok(());
                }

                writeln!(f, "{}{}{}  [@{}]", prefix, branch, title, uid)?;
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

        let mut walker = Walker {
            debug_info_map: &self.generate_rule_plan_tree_debug_map(),
            ids: BTreeMap::new(),
            next_id: 1,
            expanded: BTreeSet::new(),
            stack: BTreeSet::new(),
        };

        writeln!(f)?;
        writeln!(f, "Rule:\n{}", self.rule)?;
        writeln!(f)?;

        writeln!(f, "Plan Tree:")?;
        let root_uid = walker.get_id(root);
        writeln!(f, "{}  [@{}]", walker.node_title(root), root_uid)?;
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
