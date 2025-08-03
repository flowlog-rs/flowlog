//! FlowLog program representation and manipulation.
//!
//! This module defines the complete structure of a FlowLog program, including:
//! - EDB (Extensional Database) relation declarations for input data
//! - IDB (Intensional Database) relation declarations for derived data  
//! - Rules that define how data flows and is transformed
//! - Boolean facts extracted from rules with constant predicates
//!
//! The program supports dead code elimination to remove unused components
//! and automatic extraction of boolean facts from rules.

use pest::{iterators::Pair, Parser};
use std::collections::{HashMap, HashSet};
use std::{fmt, fs};
use tracing::{info, warn};

use super::{
    declaration::RelationDecl,
    rule::{FLRule, Predicate},
    Const, FlowLogParser, Lexeme, Rule,
};

/// Represents a complete FlowLog program.
///
/// A FlowLog program consists of relation declarations and rules that define
/// how data flows through the system. The program automatically handles:
/// - Dead code elimination to remove unused components
/// - Boolean fact extraction from rules with constant True/False predicates
/// - Dependency analysis to identify required components
///
/// # Program Structure
///
/// - **EDBs**: Input relations loaded from external sources
/// - **IDBs**: Output relations computed by rules
/// - **Rules**: Logic rules that transform input data into output data
/// - **Boolean Facts**: Constants extracted from boolean predicates
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    edbs: Vec<RelationDecl>,
    idbs: Vec<RelationDecl>,
    rules: Vec<FLRule>,
    bool_facts: HashMap<String, Vec<(Vec<Const>, bool)>>,
}

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let edbs = self
            .edbs
            .iter()
            .map(|rel_decl| rel_decl.to_string())
            .collect::<Vec<String>>()
            .join("\n");

        let idbs = self
            .idbs
            .iter()
            .map(|rel_decl| rel_decl.to_string())
            .collect::<Vec<String>>()
            .join("\n");

        let rules = self
            .rules
            .iter()
            .map(|rule| rule.to_string())
            .collect::<Vec<String>>()
            .join("\n");

        // Also display boolean facts
        let mut bool_facts_str = String::new();
        for (rel_name, facts) in &self.bool_facts {
            for (fact, boolean) in facts {
                let values: Vec<String> = fact.iter().map(|c| c.to_string()).collect();
                let bool_value = if *boolean { "True" } else { "False" };
                bool_facts_str.push_str(&format!(
                    "{rel_name}({}) :- {bool_value}.\n",
                    values.join(", ")
                ));
            }
        }

        write!(
            f,
            ".in \n{edbs}\n.out \n{idbs}\n.rule \n{rules}\n{bool_facts_str}"
        )
    }
}

impl Program {
    /// Parse a FlowLog program from a file path.
    ///
    /// # Panics
    ///
    /// Panics if the file cannot be read or if parsing fails.
    pub fn parse(path: &str) -> Self {
        let unparsed_str = fs::read_to_string(path).expect("Failed to read file");

        let parsed_rule = FlowLogParser::parse(Rule::main_grammar, &unparsed_str)
            .expect("Failed to parse FlowLog program")
            .next()
            .expect("No parsed rule found");

        // Parse the program
        let mut program = Self::from_parsed_rule(parsed_rule);

        // Extract boolean facts
        program.extract_boolean_facts();

        // Detect dead rules, declaration and remove it
        program.prune_dead_components()
    }

    /// Returns all input (EDB) relations.
    #[must_use]
    pub fn edbs(&self) -> &[RelationDecl] {
        &self.edbs
    }

    /// Returns all output (IDB) relations.
    #[must_use]
    pub fn idbs(&self) -> &[RelationDecl] {
        &self.idbs
    }

    /// Returns all rules in the program.
    #[must_use]
    pub fn rules(&self) -> &[FLRule] {
        &self.rules
    }

    /// Returns the boolean facts extracted from rules with True predicates.
    #[must_use]
    pub fn bool_facts(&self) -> &HashMap<String, Vec<(Vec<Const>, bool)>> {
        &self.bool_facts
    }

    /// Returns all output relations that are marked for final output.
    ///
    /// These are IDB relations that have the `.output` directive.
    #[must_use]
    pub fn output_relations(&self) -> Vec<&RelationDecl> {
        self.idbs
            .iter()
            .filter(|rel| rel.output_path().is_some())
            .collect()
    }

    /// Extracts boolean facts from rules with a single True/False predicate.
    ///
    /// This method processes rules that have only boolean predicates in their body,
    /// extracts the constant values from their heads, and stores them as facts.
    /// The original rules are removed from the program.
    ///
    /// # Examples
    ///
    /// A rule like `fact(1, 2) :- True.` becomes a boolean fact `fact -> [(1, 2), true]`.
    pub fn extract_boolean_facts(&mut self) {
        let mut bool_rules = Vec::new();
        let mut normal_rules = Vec::new();

        // Separate boolean rules from normal rules
        for rule in self.rules.iter() {
            if rule.is_boolean() {
                bool_rules.push(rule);
            } else {
                normal_rules.push(rule.clone());
            }
        }

        // Process boolean rules and extract constants
        for rule in bool_rules {
            // Only process rules with True predicates
            if let Predicate::BoolPredicate(boolean) = rule.rhs()[0] {
                let rel_name = rule.head().name().to_string();
                let head_args = rule.extract_constants_from_head();

                // Add to bool_facts map
                self.bool_facts
                    .entry(rel_name)
                    .or_default()
                    .push((head_args, boolean));
            }
            // We ignore False predicates as they don't contribute facts
            // TODO: this can be supported
        }

        // Update rules list to contain only normal rules
        self.rules = normal_rules;
    }

    /// Identifies needed components based on output predicates and dependencies.
    ///
    /// This method performs dependency analysis to determine which rules and
    /// declarations are actually needed based on the output relations.
    /// Returns a tuple of (needed_rule_indices, needed_predicate_names).
    ///
    /// # Returns
    ///
    /// - `HashSet<usize>`: Indices of rules that are needed
    /// - `HashSet<String>`: Names of predicates (relations) that are needed
    #[must_use]
    pub fn identify_needed_components(&self) -> (HashSet<usize>, HashSet<String>) {
        let output_predicates: HashSet<String> = self
            .output_relations()
            .iter()
            .map(|decl| decl.name().to_string())
            .collect();

        // Add boolean fact relations to needed predicates
        let mut all_needed_predicates = output_predicates.clone();
        for rel_name in self.bool_facts.keys() {
            all_needed_predicates.insert(rel_name.clone());
        }

        // If there are no output predicates, everything is needed
        if all_needed_predicates.is_empty() {
            let all_rule_ids: HashSet<usize> = (0..self.rules.len()).collect();
            let all_predicate_names: HashSet<String> = self
                .idbs
                .iter()
                .map(|decl| decl.name().to_string())
                .chain(self.edbs.iter().map(|decl| decl.name().to_string()))
                .collect();

            return (all_rule_ids, all_predicate_names);
        }

        // Create a map from head name to rule IDs that define it
        let mut head_to_rule_ids_map: HashMap<String, Vec<usize>> = HashMap::new();
        for (rule_id, rule) in self.rules.iter().enumerate() {
            let head_name = rule.head().name();
            let rule_ids = head_to_rule_ids_map
                .entry(String::from(head_name))
                .or_default();
            rule_ids.push(rule_id);
        }

        // Initial set: rules that define output predicates
        let mut needed_rules = HashSet::new();
        let mut needed_predicates = all_needed_predicates.clone();

        for predicate_name in &all_needed_predicates {
            if let Some(rule_ids) = head_to_rule_ids_map.get(predicate_name) {
                needed_rules.extend(rule_ids);
            }
        }

        // Create a map of rule dependencies
        let mut rule_dependency_map: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
        for (rule_id, rule) in self.rules.iter().enumerate() {
            let mut dependencies = Vec::new();

            for predicate in rule.rhs() {
                let atom_name = match predicate {
                    Predicate::PositiveAtomPredicate(atom) => atom.name(),
                    Predicate::NegatedAtomPredicate(atom) => atom.name(),
                    _ => continue,
                };

                // Track both the rule dependency and the predicate name
                if let Some(atom_as_head_rule_ids) = head_to_rule_ids_map.get(atom_name) {
                    for &dep_rule_id in atom_as_head_rule_ids {
                        dependencies.push((dep_rule_id, atom_name.to_string()));
                    }
                } else {
                    // This could be an EDB predicate
                    dependencies.push((usize::MAX, atom_name.to_string()));
                }
            }

            rule_dependency_map.insert(rule_id, dependencies);
        }

        // Process needed rules and collect needed predicates
        let mut processed = HashSet::new();
        let mut worklist: Vec<usize> = needed_rules.iter().cloned().collect();

        while let Some(rule_id) = worklist.pop() {
            if processed.contains(&rule_id) {
                continue;
            }

            processed.insert(rule_id);

            // Add all dependencies to needed rules and worklist
            if let Some(dependencies) = rule_dependency_map.get(&rule_id) {
                for &(dep_rule_id, ref predicate_name) in dependencies {
                    // Add the predicate name to needed predicates
                    needed_predicates.insert(predicate_name.clone());

                    // If it's a rule dependency, process it
                    if dep_rule_id != usize::MAX && !processed.contains(&dep_rule_id) {
                        needed_rules.insert(dep_rule_id);
                        worklist.push(dep_rule_id);
                    }
                }
            }
        }

        (needed_rules, needed_predicates)
    }

    /// Creates a new Program with dead rules and declarations removed.
    ///
    /// This method performs dead code elimination by:
    /// 1. Identifying which components are actually needed based on output relations
    /// 2. Removing unused EDBs, IDBs, rules, and boolean facts
    /// 3. Logging information about removed components
    ///
    /// # Returns
    ///
    /// A new `Program` instance with only the necessary components.
    #[must_use]
    pub fn prune_dead_components(&self) -> Self {
        let (needed_rules, needed_predicates) = self.identify_needed_components();

        // Check for dead EDBs
        let dead_edbs: Vec<&RelationDecl> = self
            .edbs
            .iter()
            .filter(|decl| !needed_predicates.contains(decl.name()))
            .collect();

        if !dead_edbs.is_empty() {
            warn!("Dead Input Relations (EDBs):");
            for edb in &dead_edbs {
                info!("  - {}", edb.name());
            }
        }

        // Check for dead IDBs
        let dead_idbs: Vec<&RelationDecl> = self
            .idbs
            .iter()
            .filter(|decl| !needed_predicates.contains(decl.name()))
            .collect();

        if !dead_idbs.is_empty() {
            warn!("Dead Output Relations (IDBs):");
            for idb in &dead_idbs {
                warn!("  - {}", idb.name());
            }
        }

        // Check for dead rules
        let dead_rules: Vec<(usize, &FLRule)> = self
            .rules
            .iter()
            .enumerate()
            .filter(|&(rule_id, _)| !needed_rules.contains(&rule_id))
            .collect();

        if !dead_rules.is_empty() {
            warn!("Dead Rules:");
            for (idx, rule) in &dead_rules {
                warn!("  - Rule #{}: {}", idx, rule);
            }
        }

        // Filter the rules to keep only needed ones
        let pruned_rules: Vec<FLRule> = self
            .rules
            .iter()
            .enumerate()
            .filter(|(rule_id, _)| needed_rules.contains(rule_id))
            .map(|(_, rule)| rule.clone())
            .collect();

        // Filter IDBs and EDBs to keep only needed ones
        let pruned_idbs: Vec<RelationDecl> = self
            .idbs
            .iter()
            .filter(|decl| needed_predicates.contains(decl.name()))
            .cloned()
            .collect();

        let pruned_edbs: Vec<RelationDecl> = self
            .edbs
            .iter()
            .filter(|decl| needed_predicates.contains(decl.name()))
            .cloned()
            .collect();

        // Filter boolean facts to keep only needed ones
        let mut pruned_bool_facts = HashMap::new();
        for (rel_name, facts) in &self.bool_facts {
            if needed_predicates.contains(rel_name) {
                pruned_bool_facts.insert(rel_name.clone(), facts.clone());
            }
        }

        // Create a new Program with the pruned components
        Self {
            edbs: pruned_edbs,
            idbs: pruned_idbs,
            rules: pruned_rules,
            bool_facts: pruned_bool_facts,
        }
    }
}

impl Lexeme for Program {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner_rules = parsed_rule.into_inner();
        let mut edbs: Vec<RelationDecl> = Vec::new();
        let mut idbs: Vec<RelationDecl> = Vec::new();
        let mut rules: Vec<FLRule> = Vec::new();

        // Parse relation declarations and collect them in the appropriate vector
        fn parse_rel_decls(vec: &mut Vec<RelationDecl>, rule: Pair<Rule>) {
            let rel_decls = rule.into_inner();
            for rel_decl in rel_decls {
                // Only process relation declarations
                if rel_decl.as_rule() == Rule::edb_relation_decl
                    || rel_decl.as_rule() == Rule::idb_relation_decl
                {
                    vec.push(RelationDecl::from_parsed_rule(rel_decl));
                }
            }
        }

        // Parse rules and collect them in a vector
        fn parse_rules(vec: &mut Vec<FLRule>, rule: Pair<Rule>) {
            let rules_iterator = rule.into_inner();
            for rule in rules_iterator {
                if rule.as_rule() == Rule::rule {
                    vec.push(FLRule::from_parsed_rule(rule));
                }
            }
        }

        // Process each section of the program
        for inner_rule in inner_rules {
            match inner_rule.as_rule() {
                Rule::edb_decl => parse_rel_decls(&mut edbs, inner_rule),
                Rule::idb_decl => parse_rel_decls(&mut idbs, inner_rule),
                Rule::rule_decl => parse_rules(&mut rules, inner_rule),
                _ => {} // Ignore other rules
            }
        }

        let mut program = Self {
            edbs,
            idbs,
            rules,
            bool_facts: HashMap::new(),
        };

        // Extract boolean facts during parsing
        program.extract_boolean_facts();

        program
    }
}
