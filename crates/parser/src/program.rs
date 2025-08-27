//! Macaron Datalog program representation and manipulation.
//!
//! A program contains:
//! - EDB declarations (inputs)
//! - IDB declarations (computed relations; some may be final outputs)
//! - Rules
//! - Boolean facts extracted from rules whose bodies are *pure* booleans

use super::{
    declaration::Relation,
    logic::{MacaronRule, Predicate},
    ConstType, Lexeme, MacaronParser, Rule,
};
use pest::{iterators::Pair, Parser};
use std::collections::{HashMap, HashSet};
use std::{fmt, fs};
use tracing::{info, warn};

/// A complete Macaron program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    edbs: Vec<Relation>,
    idbs: Vec<Relation>,
    rules: Vec<MacaronRule>,
    /// Map: relation name -> [(constant tuple, boolean value)]
    bool_facts: HashMap<String, Vec<(Vec<ConstType>, bool)>>,
}

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let edbs = self
            .edbs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let idbs = self
            .idbs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let rules = self
            .rules
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");

        let mut bool_facts_str = String::new();
        for (rel_name, facts) in &self.bool_facts {
            for (vals, boolean) in facts {
                let values = vals
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let b = if *boolean { "True" } else { "False" };
                bool_facts_str.push_str(&format!("{rel_name}({values}) :- {b}.\n"));
            }
        }

        write!(
            f,
            ".in \n{edbs}\n.out \n{idbs}\n.rule \n{rules}\n{bool_facts_str}"
        )
    }
}

impl Program {
    /// Parse a program from a file, extract boolean facts, and prune dead components.
    ///
    /// Panics on I/O or parse errors.
    #[must_use]
    pub fn parse(path: &str) -> Self {
        let unparsed = fs::read_to_string(path).expect("Parser error: failed to read file");
        let parsed = MacaronParser::parse(Rule::main_grammar, &unparsed)
            .expect("Parser error: failed to parse Macaron program")
            .next()
            .expect("Parser error: no parsed rule found");

        // Build structure + extract boolean facts inside `from_parsed_rule`.
        let program = Self::from_parsed_rule(parsed);

        // Prune unused declarations and rules.
        program.prune_dead_components()
    }

    /// EDB (input) relations.
    #[must_use]
    #[inline]
    pub fn edbs(&self) -> &[Relation] {
        &self.edbs
    }

    /// IDB (computed) relations (both intermediate and final).
    #[must_use]
    #[inline]
    pub fn idbs(&self) -> &[Relation] {
        &self.idbs
    }

    /// Transformation rules (boolean-only rules are extracted into `bool_facts`).
    #[must_use]
    #[inline]
    pub fn rules(&self) -> &[MacaronRule] {
        &self.rules
    }

    /// Extracted boolean facts.
    #[must_use]
    #[inline]
    pub fn bool_facts(&self) -> &HashMap<String, Vec<(Vec<ConstType>, bool)>> {
        &self.bool_facts
    }

    /// IDB relations that are marked for final output.
    #[must_use]
    pub fn output_relations(&self) -> Vec<&Relation> {
        self.idbs
            .iter()
            .filter(|r| r.output_path().is_some())
            .collect()
    }

    /// Extract boolean facts from rules whose *entire* body is boolean.
    ///
    /// - A rule is considered a boolean fact rule iff `rhs().iter().all(BoolPredicate)`.
    /// - The overall boolean value is the conjunction of those literals (all must be `true`).
    /// - Only `true` facts are materialized; `false` are ignored (can be added later).
    fn extract_boolean_facts(&mut self) {
        let mut keep = Vec::with_capacity(self.rules.len());

        for rule in self.rules.drain(..) {
            let all_bool = rule
                .rhs()
                .iter()
                .all(|p| matches!(p, Predicate::BoolPredicate(_)));
            if !all_bool {
                keep.push(rule);
                continue;
            }

            // Conjunction of boolean literals.
            let overall_true = rule
                .rhs()
                .iter()
                .all(|p| matches!(p, Predicate::BoolPredicate(true)));

            if overall_true {
                let rel_name = rule.head().name().to_string();
                let tuple = rule.extract_constants_from_head(); // panics if head isn't constants
                self.bool_facts
                    .entry(rel_name)
                    .or_default()
                    .push((tuple, true));
            }
            // If overall is false: ignore (no fact contributed).
        }

        self.rules = keep;
    }

    /// Compute the transitive closure of dependencies needed by outputs + boolean facts.
    ///
    /// Returns `(needed_rule_indices, needed_predicate_names)`.
    #[must_use]
    fn identify_needed_components(&self) -> (HashSet<usize>, HashSet<String>) {
        let mut needed_preds: HashSet<String> = self
            .output_relations()
            .into_iter()
            .map(|d| d.name().to_string())
            .collect();

        // Boolean fact relations are always needed.
        needed_preds.extend(self.bool_facts.keys().cloned());

        // If no outputs and no bool facts, keep everything.
        if needed_preds.is_empty() {
            let all_rules = (0..self.rules.len()).collect();
            let all_preds = self
                .idbs
                .iter()
                .map(|d| d.name().to_string())
                .chain(self.edbs.iter().map(|d| d.name().to_string()))
                .collect();
            return (all_rules, all_preds);
        }

        // Map head name -> rule IDs that define it.
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in self.rules.iter().enumerate() {
            head_to_rules
                .entry(r.head().name().to_string())
                .or_default()
                .push(i);
        }

        // Seed: rules that define already needed predicates.
        let mut needed_rules: HashSet<usize> = HashSet::new();
        for p in &needed_preds {
            if let Some(ids) = head_to_rules.get(p) {
                needed_rules.extend(ids);
            }
        }

        // Build dependency map: rule -> deps (rule_id or EDB sentinel, plus predicate name).
        let mut dep_map: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
        for (i, r) in self.rules.iter().enumerate() {
            let mut deps = Vec::new();
            for pred in r.rhs() {
                let atom_name = match pred {
                    Predicate::PositiveAtomPredicate(a) | Predicate::NegatedAtomPredicate(a) => {
                        a.name()
                    }
                    _ => continue,
                };
                if let Some(ids) = head_to_rules.get(atom_name) {
                    for &dep_id in ids {
                        deps.push((dep_id, atom_name.to_string()));
                    }
                } else {
                    // Treat as EDB usage.
                    deps.push((usize::MAX, atom_name.to_string()));
                }
            }
            dep_map.insert(i, deps);
        }

        // Traverse dependencies.
        let mut processed = HashSet::new();
        let mut stack: Vec<usize> = needed_rules.iter().copied().collect();

        while let Some(rule_id) = stack.pop() {
            if !processed.insert(rule_id) {
                continue;
            }
            if let Some(deps) = dep_map.get(&rule_id) {
                for &(dep_rule_id, ref pred_name) in deps {
                    needed_preds.insert(pred_name.clone());
                    if dep_rule_id != usize::MAX && !processed.contains(&dep_rule_id) {
                        needed_rules.insert(dep_rule_id);
                        stack.push(dep_rule_id);
                    }
                }
            }
        }

        (needed_rules, needed_preds)
    }

    /// Return a copy of the program with dead rules/relations removed, logging what was dropped.
    #[must_use]
    fn prune_dead_components(&self) -> Self {
        let (needed_rules, needed_preds) = self.identify_needed_components();

        // Dead EDBs
        let dead_edbs: Vec<_> = self
            .edbs
            .iter()
            .filter(|d| !needed_preds.contains(d.name()))
            .collect();
        if !dead_edbs.is_empty() {
            warn!("Dead Input Relations (EDBs):");
            for d in &dead_edbs {
                info!("  - {}", d.name());
            }
        }

        // Dead IDBs
        let dead_idbs: Vec<_> = self
            .idbs
            .iter()
            .filter(|d| !needed_preds.contains(d.name()))
            .collect();
        if !dead_idbs.is_empty() {
            warn!("Dead Output Relations (IDBs):");
            for d in &dead_idbs {
                warn!("  - {}", d.name());
            }
        }

        // Dead rules
        let dead_rules: Vec<_> = self
            .rules
            .iter()
            .enumerate()
            .filter(|(i, _)| !needed_rules.contains(i))
            .collect();
        if !dead_rules.is_empty() {
            warn!("Dead Rules:");
            for (i, r) in &dead_rules {
                warn!("  - Rule #{}: {}", i, r);
            }
        }

        // Keep only needed components.
        let pruned_rules: Vec<_> = self
            .rules
            .iter()
            .enumerate()
            .filter(|(i, _)| needed_rules.contains(i))
            .map(|(_, r)| r.clone())
            .collect();

        let pruned_idbs = self
            .idbs
            .iter()
            .filter(|d| needed_preds.contains(d.name()))
            .cloned()
            .collect();
        let pruned_edbs = self
            .edbs
            .iter()
            .filter(|d| needed_preds.contains(d.name()))
            .cloned()
            .collect();

        let mut pruned_bool = HashMap::new();
        for (rel, facts) in &self.bool_facts {
            if needed_preds.contains(rel.as_str()) {
                pruned_bool.insert(rel.clone(), facts.clone());
            }
        }

        Self {
            edbs: pruned_edbs,
            idbs: pruned_idbs,
            rules: pruned_rules,
            bool_facts: pruned_bool,
        }
    }
}

impl Lexeme for Program {
    /// Build a program from the top-level grammar node.
    ///
    /// Performs section dispatch and boolean fact extraction.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut edbs = Vec::new();
        let mut idbs = Vec::new();
        let mut rules = Vec::new();

        for section in parsed_rule.into_inner() {
            match section.as_rule() {
                Rule::edb_decl => {
                    for node in section.into_inner() {
                        if matches!(
                            node.as_rule(),
                            Rule::edb_relation_decl | Rule::idb_relation_decl
                        ) {
                            edbs.push(Relation::from_parsed_rule(node));
                        }
                    }
                }
                Rule::idb_decl => {
                    for node in section.into_inner() {
                        if matches!(
                            node.as_rule(),
                            Rule::edb_relation_decl | Rule::idb_relation_decl
                        ) {
                            idbs.push(Relation::from_parsed_rule(node));
                        }
                    }
                }
                Rule::rule_decl => {
                    for node in section.into_inner() {
                        if node.as_rule() == Rule::rule {
                            rules.push(MacaronRule::from_parsed_rule(node));
                        }
                    }
                }
                _ => {}
            }
        }

        let mut program = Self {
            edbs,
            idbs,
            rules,
            bool_facts: HashMap::new(),
        };

        // Extract boolean-only rules into `bool_facts`.
        program.extract_boolean_facts();
        program
    }
}
