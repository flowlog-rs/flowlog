//! Macaron Datalog program representation and manipulation.
//!
//! A program contains:
//! - Relation declarations (schema only)
//! - Input directives (specify how to read EDB data)
//! - Output directives (specify which relations to output)
//! - Print size directives (specify which relations to print size for)
//! - Rules
//! - Boolean facts extracted from rules whose bodies are *pure* booleans

use super::{
    declaration::{InputDirective, OutputDirective, PrintSizeDirective, Relation},
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
    relations: Vec<Relation>,
    rules: Vec<MacaronRule>,
    /// Map: relation name -> [(constant tuple, boolean value)]
    bool_facts: HashMap<String, Vec<(Vec<ConstType>, bool)>>,
}

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=============================================")?;
        writeln!(f, "MACARON DATALOG PROGRAM")?;
        writeln!(f, "=============================================")?;
        writeln!(f)?;

        // EDB Section (Extensional Database - Input Relations)
        let edb_relations = self.edbs();
        if !edb_relations.is_empty() {
            writeln!(f, "EDB Relations")?;
            writeln!(f, "---------------------------------------------")?;
            for relation in edb_relations {
                writeln!(f, "{}", relation)?;
            }
            writeln!(f)?;
        }

        // IDB Section (Intensional Database - Computed Relations)
        let idb_relations = self.idbs();
        if !idb_relations.is_empty() {
            writeln!(f, "IDB Relations")?;
            writeln!(f, "---------------------------------------------")?;
            for relation in idb_relations {
                writeln!(f, "{}", relation)?;
            }
            writeln!(f)?;
        }

        // Rules Section
        if !self.rules.is_empty() {
            writeln!(f, "Rules")?;
            writeln!(f, "---------------------------------------------")?;
            for rule in &self.rules {
                writeln!(f, "{}", rule)?;
            }
            writeln!(f)?;
        }

        // Boolean Facts Section
        if !self.bool_facts.is_empty() {
            writeln!(f, "Boolean Facts")?;
            writeln!(f, "---------------------------------------------")?;
            for (rel_name, facts) in &self.bool_facts {
                for (vals, boolean) in facts {
                    let values = vals
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let b = if *boolean { "True" } else { "False" };
                    writeln!(f, "{}({}) :- {}.", rel_name, values, b)?;
                }
            }
        }

        Ok(())
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

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// EDB relations (those with input parameters).
    #[must_use]
    pub fn edbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.is_edb()).collect()
    }

    /// Ordered EDB relation names.
    #[must_use]
    pub fn edb_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .edbs()
            .iter()
            .map(|rel| rel.name().to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// IDB relations (those without input parameters).
    #[must_use]
    pub fn idbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| !rel.is_edb()).collect()
    }

    /// Output relations (those marked for output).
    #[must_use]
    pub fn output_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_output())
            .collect()
    }

    /// Relations marked for printsize.
    #[must_use]
    pub fn printsize_relations(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.printsize())
            .collect()
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
                .relations
                .iter()
                .map(|d| d.name().to_string())
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

        // Dead relations
        let dead_relations: Vec<_> = self
            .relations
            .iter()
            .filter(|d| !needed_preds.contains(d.name()))
            .collect();
        if !dead_relations.is_empty() {
            warn!("Dead Relations:");
            for d in &dead_relations {
                info!("  - {}", d.name());
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

        let pruned_relations = self
            .relations
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
            relations: pruned_relations,
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
        let mut relations = Vec::new();
        let mut input_directives = Vec::new();
        let mut output_directives = Vec::new();
        let mut printsize_directives = Vec::new();
        let mut rules = Vec::new();

        // First pass: collect all declarations and directives
        for node in parsed_rule.into_inner() {
            match node.as_rule() {
                Rule::declaration => {
                    let relation = Relation::from_parsed_rule(node);
                    relations.push(relation);
                }
                Rule::input_directive => {
                    let input_dir = InputDirective::from_parsed_rule(node);
                    input_directives.push(input_dir);
                }
                Rule::output_directive => {
                    let output_dir = OutputDirective::from_parsed_rule(node);
                    output_directives.push(output_dir);
                }
                Rule::printsize_directive => {
                    let printsize_dir = PrintSizeDirective::from_parsed_rule(node);
                    printsize_directives.push(printsize_dir);
                }
                Rule::rule => {
                    rules.push(MacaronRule::from_parsed_rule(node));
                }
                _ => {}
            }
        }

        // Second pass: validate and annotate relations with directives
        // Check for duplicate input directives
        let mut input_relation_names = HashSet::new();
        for input_dir in &input_directives {
            if !input_relation_names.insert(input_dir.relation_name()) {
                panic!(
                    "Parser error: duplicate input directive for relation '{}'",
                    input_dir.relation_name()
                );
            }
        }

        // Check for duplicate output directives
        let mut output_relation_names = HashSet::new();
        for output_dir in &output_directives {
            if !output_relation_names.insert(output_dir.relation_name()) {
                panic!(
                    "Parser error: duplicate output directive for relation '{}'",
                    output_dir.relation_name()
                );
            }
        }

        // Check for duplicate printsize directives
        let mut printsize_relation_names = HashSet::new();
        for printsize_dir in &printsize_directives {
            if !printsize_relation_names.insert(printsize_dir.relation_name()) {
                panic!(
                    "Parser error: duplicate printsize directive for relation '{}'",
                    printsize_dir.relation_name()
                );
            }
        }

        // Check for invalid combinations: EDB relations cannot have output/printsize, IDB relations cannot have input
        for input_dir in &input_directives {
            let relation_name = input_dir.relation_name();

            // Check if this relation also has output directive
            if output_relation_names.contains(relation_name) {
                panic!(
                    "Parser error: relation '{}' cannot have both input and output directives. Relations must be either EDB (input only) or IDB (output/printsize only).",
                    relation_name
                );
            }

            // Check if this relation also has printsize directive
            if printsize_relation_names.contains(relation_name) {
                panic!(
                    "Parser error: relation '{}' cannot have both input and printsize directives. Relations must be either EDB (input only) or IDB (output/printsize only).",
                    relation_name
                );
            }
        }

        // Apply input directives (creates EDB relations)
        for input_dir in input_directives {
            if let Some(relation) = relations
                .iter_mut()
                .find(|r| r.name() == input_dir.relation_name())
            {
                relation.set_input_params(input_dir.parameters().clone());
            } else {
                panic!(
                    "Parser error: input directive for UNKNOWN relation '{}'. Relation must be declared with .decl before using .input directive.",
                    input_dir.relation_name()
                );
            }
        }

        // Apply output directives
        for output_dir in output_directives {
            if let Some(relation) = relations
                .iter_mut()
                .find(|r| r.name() == output_dir.relation_name())
            {
                // Simplified output handling: just mark relation for output; path ignored.
                relation.set_output(true);
            } else {
                panic!(
                    "Parser error: output directive for UNKNOWN relation '{}'. Relation must be declared with .decl before using .output directive.",
                    output_dir.relation_name()
                );
            }
        }

        // Apply printsize directives
        for printsize_dir in printsize_directives {
            if let Some(relation) = relations
                .iter_mut()
                .find(|r| r.name() == printsize_dir.relation_name())
            {
                relation.set_printsize(true);
            } else {
                panic!(
                    "Parser error: printsize directive for UNKNOWN relation '{}'. Relation must be declared with .decl before using .printsize directive.",
                    printsize_dir.relation_name()
                );
            }
        }

        let mut program = Self {
            relations,
            rules,
            bool_facts: HashMap::new(),
        };

        // Extract boolean-only rules into `bool_facts`.
        program.extract_boolean_facts();
        program
    }
}
