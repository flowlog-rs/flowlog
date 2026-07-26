//! Two structural passes over the [`Program`]: prune dead components (drop the
//! rules and relations nothing needs) and materialize orphans (give a
//! referenced-but-underived relation an empty entry). [`prune`] runs both.

use std::collections::HashMap;
use std::collections::HashSet;

use tracing::warn;

use crate::ast::FlowLogRule;
use crate::ast::Predicate;
use crate::declaration::Relation;
use crate::program::Program;
use crate::segment::Segment;

/// Prune dead components, then materialize orphans. Idempotent.
// Order matters: materialize must run after pruning so it cannot re-add a
// dropped relation.
pub fn prune(program: &mut Program) {
    prune_dead_components(program);
    materialize_orphan_relations(program);
}

/// Dependency-map index for a predicate no rule derives (e.g. a pure
/// `.input`); the DFS stops at such an edge.
const NO_TOP_LEVEL_RULE_ID: usize = usize::MAX;

/// The rule indices and predicate names transitively needed by outputs and
/// facts, plus the underived IDBs. Rule indices are into all segments' rules,
/// flattened in source order.
#[must_use]
fn identify_needed_components(
    program: &Program,
) -> ((HashSet<usize>, HashSet<String>), HashSet<String>) {
    // Flatten all rules (plain and loop-internal) in source order.
    let all_rules: Vec<&FlowLogRule> = program
        .segments
        .iter()
        .flat_map(|item| match item {
            Segment::Plain(rules) => rules.as_slice(),
            Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
        })
        .collect();

    let mut needed_preds: HashSet<String> = program
        .idbs()
        .into_iter()
        .map(|d| d.name().to_string())
        .collect();

    // Loop-until relations stay live even if not outputs: the loop reads them
    // to decide termination.
    needed_preds.extend(
        program
            .segments
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

    // If no outputs and no loop conditions, keep everything.
    if needed_preds.is_empty() {
        let all_indices = (0..all_rules.len()).collect();
        let all_preds = program
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

    // Drop declared-but-underived IDBs (no rule, no facts): always empty, and
    // keeping them only trips downstream codegen.
    let input_relations: HashSet<String> = program
        .relations
        .iter()
        .filter(|r| r.has_input())
        .map(|r| r.name().to_string())
        .collect();
    let underived: Vec<String> = needed_preds
        .iter()
        .filter(|p| {
            !head_to_rules.contains_key(p.as_str())
                && !program.facts.contains_key(p.as_str())
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

    // Dependency map: rule index -> [(dep rule index, predicate name)]; a
    // predicate with no deriving rule uses NO_TOP_LEVEL_RULE_ID.
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
fn prune_dead_components(program: &mut Program) {
    let ((needed_rules, needed_preds), underived) = identify_needed_components(program);

    // Collect dead relations and rules for one structured warning.
    let dead_relations: Vec<_> = program
        .relations
        .iter()
        .filter(|d| !needed_preds.contains(d.name()) && !underived.contains(d.name()))
        .map(|d| d.raw_name().to_string())
        .collect();

    let dead_rules: Vec<_> = program
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
            // Display-only: show the user's spelling; `underived` holds
            // canonical names for the tests above.
            let mut sorted: Vec<&str> = program
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

    program
        .relations
        .retain(|d| needed_preds.contains(d.name()));

    // A fact is a derivation, not a demand: facts of pruned relations go
    // with them.
    program.facts.retain(|name, _| needed_preds.contains(name));

    // Filter dead rules from all segments; drop any segment that becomes empty.
    let mut global_idx = 0usize;
    let new_items: Vec<Segment> = program
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
    program.segments = new_items;

    program
        .facts
        .retain(|rel, _| needed_preds.contains(rel.as_str()));
}

/// Give every orphan an empty fact entry. An orphan is declared and referenced
/// in a rule body but never produced (no rule, no `.input`, no facts); under
/// Souffle semantics it is the empty relation, so codegen gets an empty
/// collection instead of an undefined binding.
fn materialize_orphan_relations(program: &mut Program) {
    let mut produced: HashSet<String> = HashSet::new();
    let mut referenced: HashSet<String> = HashSet::new();
    for segment in &program.segments {
        let rules: &[FlowLogRule] = match segment {
            Segment::Plain(rules) => rules,
            Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
        };
        for rule in rules {
            produced.insert(rule.head().name().to_string());
            for pred in rule.rhs() {
                if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
                    referenced.insert(atom.name().to_string());
                }
            }
        }
    }

    let orphans: Vec<String> = program
        .relations
        .iter()
        .filter(|rel| {
            let name = rel.name();
            referenced.contains(name)
                && !produced.contains(name)
                && !program.facts.contains_key(name)
                && !rel.has_input()
        })
        .map(|rel| rel.name().to_string())
        .collect();
    for name in orphans {
        program.facts.entry(name).or_default();
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::pruned;

    /// A fact is a derivation, not a demand: an inline fact on a relation
    /// nothing reads or outputs does not keep the relation, its facts, or
    /// its recursion alive.
    #[test]
    fn unread_fact_seeded_recursion_is_pruned() {
        let program = pruned(
            "
            .decl Src(x: number)
            .decl P(x: number)
            .decl Out(x: number)
            Src(1).
            P(1).
            P(x) :- P(x).
            Out(x) :- Src(x).
            .output Out
            ",
        )
        .expect("valid program");
        assert!(!program.relations().iter().any(|rel| rel.name() == "p"));
        assert!(!program.facts().contains_key("p"));
        // The consumed fact relation stays, facts intact.
        assert!(program.facts().contains_key("src"));
    }

    /// A loop-`until` relation is kept live even though nothing else reads it,
    /// while an unreferenced derived relation (`dead`) is pruned.
    #[test]
    fn dead_code_elimination_keeps_loop_until_relations() {
        let src = "
            .decl edge(x: number, y: number)
            .decl keep()
            .decl dead()
            .output edge

            edge(1, 2).

            loop until { keep } {
                keep() :- edge(1, 2).
            }

            dead() :- edge(2, 3).
        ";
        let program = pruned(src).expect("valid program");

        assert!(program.relations().iter().any(|rel| rel.name() == "keep"));
        assert!(!program.relations().iter().any(|rel| rel.name() == "dead"));
    }

    /// `.output R` with no rules, no facts, and no body references is pruned from
    /// the dataflow (so codegen doesn't emit a buffer for a non-existent node).
    #[test]
    fn empty_output_pruned_from_dataflow() {
        let program = pruned(
            "
            .decl Nothing(x: symbol)
            .decl Src(x: symbol)
            .decl Out(x: symbol)
            Src(\"v\").
            Out(x) :- Src(x).
            .output Nothing
            .output Out
            ",
        )
        .expect("valid program");
        // `Nothing` is pruned from output_idbs (no rules, no facts, unreferenced).
        assert!(
            program.output_idbs().iter().all(|r| r.name() != "nothing"),
            "empty `.output` should be pruned from output_idbs, got: {:?}",
            program
                .output_idbs()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>()
        );
        // `Out` flows through the normal drain path.
        assert!(program.output_idbs().iter().any(|r| r.name() == "out"));
    }

    /// A relation that is declared and referenced by a live rule but never
    /// produced (no rule, no `.input`, no facts) is materialized as an empty
    /// inline-fact relation so codegen emits an empty collection for it.
    /// An `.input`-backed relation is NOT an orphan: its collection comes
    /// from the fact file.
    #[test]
    fn orphan_relation_referenced_by_live_rule_is_materialized_empty() {
        let program = pruned(
            "
            .decl O(x:symbol)
            .decl I(x:symbol)
            .input I(IO=\"file\",filename=\"I.csv\")
            .decl R(x:symbol)
            R(x) :- O(x), I(x).
            .output R
            ",
        )
        .expect("valid program");
        assert!(
            program.facts().contains_key("o"),
            "orphan relation should be materialized"
        );
        assert!(
            program.facts()["o"].is_empty(),
            "materialized orphan must be empty"
        );
        assert!(
            !program.facts().contains_key("i"),
            ".input relation must not be materialized as an orphan"
        );
    }

    /// Known gap, pinned as-is: `prune` marks loop-until relations live by
    /// their condition spelling, which keeps the user's dot while the
    /// declaration was normalized to `\u{b7}`: the spellings no longer match, so
    /// the guard relation is dropped. Rewriting condition names in
    /// `normalize_inliner_dots` would close the gap; that is a behavior change.
    #[test]
    fn prune_drops_dotted_until_relation_after_normalization() {
        let src = "
            .comp C { .decl Holds() }
            .init c = C
            .decl edge(x: number, y: number)
            .output edge
            edge(1, 2).
            loop until { c.Holds } {
                edge(X, Y) :- edge(Y, X).
            }
        ";
        let program = pruned(src).expect("valid program");
        assert!(
            !program
                .relations()
                .iter()
                .any(|r| r.name() == "c\u{b7}holds")
        );
    }
}
