use std::collections::HashMap;
use std::collections::HashSet;

use tracing::warn;

use super::Program;
use crate::declaration::Relation;
use crate::logic::FlowLogRule;
use crate::logic::Predicate;
use crate::segment::Segment;

// =============================================================================
// Dead-code elimination
// =============================================================================

/// Sentinel used in the dependency map to represent a predicate that has no
/// rule deriving it anywhere in the program — e.g. a pure `.input` relation
/// or any otherwise externally-satisfied predicate.  No rule index exists for
/// such predicates in the flattened rule list, so this is stored instead to
/// signal that DFS traversal should stop here.
const NO_TOP_LEVEL_RULE_ID: usize = usize::MAX;

impl Program {
    /// Compute the transitive closure of dependencies needed by outputs + facts.
    ///
    /// Operates on all rules across all segments (plain and loop-internal),
    /// flattened in source order.
    ///
    /// Returns `((needed_rule_indices, needed_predicate_names), underived_idb_names)`.
    #[must_use]
    fn identify_needed_components(&self) -> ((HashSet<usize>, HashSet<String>), HashSet<String>) {
        // Flatten all rules (plain and loop-internal) in source order.
        let all_rules: Vec<&FlowLogRule> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            })
            .collect();

        let mut needed_preds: HashSet<String> = self
            .idbs()
            .into_iter()
            .map(|d| d.name().to_string())
            .collect();

        // Fact relations are always needed.
        needed_preds.extend(self.facts.keys().cloned());

        // Relations referenced by loop until conditions must be retained even if
        // they are not outputs. They are semantically live because the loop
        // controller reads them to decide termination.
        needed_preds.extend(
            self.segments
                .iter()
                .filter_map(Segment::as_loop)
                .flat_map(|block| {
                    block
                        .condition()
                        .and_then(|cond| cond.until_part())
                        .into_iter()
                        .flat_map(|stop| stop.relations().map(|rel| rel.name().to_string()))
                }),
        );

        // If no outputs and no facts, keep everything.
        if needed_preds.is_empty() {
            let all_indices = (0..all_rules.len()).collect();
            let all_preds = self
                .relations
                .iter()
                .map(|d| d.name().to_string())
                .collect();
            return ((all_indices, all_preds), HashSet::new());
        }

        // Map: head name -> rule indices that derive it.
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in all_rules.iter().enumerate() {
            head_to_rules
                .entry(r.head().name().to_string())
                .or_default()
                .push(i);
        }

        // Remove IDB relations that are declared (with .output/.printsize) but
        // never derived by any rule and have no facts.  They will always be
        // empty, so keeping them only causes downstream codegen errors.
        let input_relations: HashSet<String> = self
            .relations
            .iter()
            .filter(|r| r.has_input())
            .map(|r| r.name().to_string())
            .collect();
        let underived: Vec<String> = needed_preds
            .iter()
            .filter(|p| {
                !head_to_rules.contains_key(p.as_str())
                    && !self.facts.contains_key(p.as_str())
                    && !input_relations.contains(p.as_str())
            })
            .cloned()
            .collect();
        for name in &underived {
            needed_preds.remove(name);
        }

        // Seed: rules that define already-needed predicates.
        let mut needed_rules: HashSet<usize> = needed_preds
            .iter()
            .flat_map(|p| head_to_rules.get(p).into_iter().flatten().copied())
            .collect();

        // Build a dependency map: rule index → [(dep_rule_index, predicate_name)].
        // For predicates with no defining rule, [`NO_TOP_LEVEL_RULE_ID`] is stored as the index.
        let dep_map: HashMap<usize, Vec<(usize, String)>> = all_rules
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let deps = r
                    .rhs()
                    .iter()
                    .filter_map(|pred| match pred {
                        Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => Some(a.name()),
                        _ => None,
                    })
                    .flat_map(|atom_name| {
                        if let Some(ids) = head_to_rules.get(atom_name) {
                            ids.iter()
                                .map(|&id| (id, atom_name.to_string()))
                                .collect::<Vec<_>>()
                        } else {
                            // No top-level rule derives this predicate.
                            vec![(NO_TOP_LEVEL_RULE_ID, atom_name.to_string())]
                        }
                    })
                    .collect();
                (i, deps)
            })
            .collect();

        // DFS traversal.
        let mut processed: HashSet<usize> = HashSet::new();
        let mut stack: Vec<usize> = needed_rules.iter().copied().collect();

        while let Some(rule_id) = stack.pop() {
            if !processed.insert(rule_id) {
                continue;
            }
            for (dep_rule_id, pred_name) in dep_map.get(&rule_id).into_iter().flatten() {
                needed_preds.insert(pred_name.clone());
                if *dep_rule_id != NO_TOP_LEVEL_RULE_ID && !processed.contains(dep_rule_id) {
                    needed_rules.insert(*dep_rule_id);
                    stack.push(*dep_rule_id);
                }
            }
        }

        let underived: HashSet<String> = underived.into_iter().collect();
        ((needed_rules, needed_preds), underived)
    }

    /// Remove dead rules and relations in place, logging what was dropped.
    pub(super) fn prune_dead_components(&mut self) {
        let ((needed_rules, needed_preds), underived) = self.identify_needed_components();

        // Collect dead relations (unreachable) and dead rules for a single
        // structured warning so the output is easy to scan.
        let dead_relations: Vec<_> = self
            .relations
            .iter()
            .filter(|d| !needed_preds.contains(d.name()) && !underived.contains(d.name()))
            .map(|d| d.raw_name().to_string())
            .collect();

        let dead_rules: Vec<_> = self
            .segments
            .iter()
            .flat_map(|item| match item {
                Segment::Plain(rules) => rules.as_slice(),
                Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
            })
            .enumerate()
            .filter(|(i, _)| !needed_rules.contains(i))
            .map(|(i, r)| format!("#{}: {}", i, r))
            .collect();

        if !underived.is_empty() || !dead_relations.is_empty() || !dead_rules.is_empty() {
            let mut parts = Vec::new();
            if !underived.is_empty() {
                // Display-only: show the user's spelling (`underived`
                // keeps canonical names for the membership tests above).
                let mut sorted: Vec<&str> = self
                    .relations
                    .iter()
                    .filter(|r| underived.contains(r.name()))
                    .map(Relation::raw_name)
                    .collect();
                sorted.sort_unstable();
                parts.push(format!(
                    "  underived IDBs (declared but no rules): {}",
                    sorted.join(", ")
                ));
            }
            if !dead_relations.is_empty() {
                parts.push(format!(
                    "  unreachable relations: {}",
                    dead_relations.join(", ")
                ));
            }
            if !dead_rules.is_empty() {
                parts.push(format!("  unreachable rules: {}", dead_rules.join(", ")));
            }
            warn!("Pruned dead components:\n{}", parts.join("\n"));
        }

        self.relations.retain(|d| needed_preds.contains(d.name()));

        // Filter dead rules from all segments; drop any segment that becomes empty.
        let mut global_idx = 0usize;
        let new_items: Vec<Segment> = self
            .segments
            .drain(..)
            .filter_map(|item| match item {
                Segment::Plain(rules) => {
                    let filtered: Vec<FlowLogRule> = rules
                        .into_iter()
                        .filter(|_| {
                            let keep = needed_rules.contains(&global_idx);
                            global_idx += 1;
                            keep
                        })
                        .collect();
                    if filtered.is_empty() {
                        None
                    } else {
                        Some(Segment::Plain(filtered))
                    }
                }
                Segment::Loop(mut block) => {
                    block.rules_mut().retain(|_| {
                        let keep = needed_rules.contains(&global_idx);
                        global_idx += 1;
                        keep
                    });
                    if block.rules().is_empty() {
                        None
                    } else {
                        Some(Segment::Loop(block))
                    }
                }
                Segment::Fixpoint(mut block) => {
                    block.rules_mut().retain(|_| {
                        let keep = needed_rules.contains(&global_idx);
                        global_idx += 1;
                        keep
                    });
                    if block.rules().is_empty() {
                        None
                    } else {
                        Some(Segment::Fixpoint(block))
                    }
                }
            })
            .collect();
        self.segments = new_items;

        self.facts
            .retain(|rel, _| needed_preds.contains(rel.as_str()));
    }
}
