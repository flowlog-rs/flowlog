//! FlowLog program representation and manipulation.
//!
//! This module provides the complete infrastructure for representing, parsing, and
//! optimizing FlowLog programs. FlowLog is a datalog programming language engine.
//!
//! # Program Structure
//!
//! A FlowLog program consists of several key components:
//!
//! ## Relation Declarations
//! - **EDB (Extensional Database)**: Input relations that define the base facts
//! - **IDB (Intensional Database)**: Output relations derived through rules
//!
//! ## Rules and Facts
//! - **Logic Rules**: Define how data flows and transformations occur
//! - **Boolean Facts**: Constant facts extracted from rules with boolean predicates
//!
//! ## Program Optimization
//! - **Dead Code Elimination**: Removes unused relations, rules, and facts
//! - **Boolean Fact Extraction**: Automatically extracts constants from boolean rules
//!
//! # FlowLog Language Overview
//!
//! FlowLog programs follow a declarative paradigm where:
//! ```text
//! .in                     // Input relation declarations
//! person(name: text, age: integer).
//!
//! .out                    // Output relation declarations  
//! adult(name: text).output("adults.csv").
//!
//! .rule                   // Logic rules
//! adult(Name) :- person(Name, Age), Age >= 18.
//! config("debug") :- True.  // Boolean fact
//! ```
//!
//! # Program Processing Pipeline
//!
//! 1. **Parsing**: Convert text to structured [`Program`] representation
//! 2. **Boolean Extraction**: Extract constant facts from boolean rules
//! 3. **Dead Code Analysis**: Identify unused components based on outputs
//! 4. **Optimization**: Remove dead code to create minimal program
//!
//! # Examples
//!
//! ```rust
//! use parser::program::Program;
//!
//! // Parse a complete FlowLog program from file
//! let program = Program::parse("../../example/tc.dl");
//!
//! // Access program components
//! println!("Input relations: {}", program.edbs().len());
//! println!("Output relations: {}", program.idbs().len());
//! println!("Logic rules: {}", program.rules().len());
//! println!("Boolean facts: {}", program.bool_facts().len());
//!
//! // Get final output relations
//! let outputs = program.output_relations();
//! for relation in outputs {
//!     println!("Output: {} -> {:?}", relation.name(), relation.output_path());
//! }
//! ```

use pest::{iterators::Pair, Parser};
use std::collections::{HashMap, HashSet};
use std::{fmt, fs};
use tracing::{info, warn};

use super::{
    declaration::Relation,
    logic::{FLRule, Predicate},
    ConstType, FlowLogParser, Lexeme, Rule,
};

/// Represents a complete FlowLog program.
///
/// A FlowLog program encapsulates all components needed for declarative logic
/// programming: input/output relation schemas, transformation rules, and derived
/// facts.
///
/// # Core Components
///
/// ## Relation Declarations
/// - **EDBs (Extensional Database)**: Input relations loaded from external sources
/// - **IDBs (Intensional Database)**: Output relations computed by program rules
///
/// ## Computational Logic  
/// - **Rules**: Logic rules that define data transformations and derivations
/// - **Boolean Facts**: Constant facts extracted from rules with boolean predicates
///
/// # Examples
///
/// ```rust
/// use parser::program::Program;
/// use parser::declaration::Relation;
/// use parser::logic::{FLRule, Head, HeadArg, Predicate};
///
/// // Parse from file (recommended approach)
/// let program = Program::parse("../../example/tc.dl");
///
/// // Access program structure
/// println!("Program has {} input relations", program.edbs().len());
/// println!("Program has {} output relations", program.idbs().len());
/// println!("Program has {} transformation rules", program.rules().len());
///
/// // Boolean facts are automatically extracted
/// for (relation, facts) in program.bool_facts() {
///     println!("Relation {} has {} constant facts", relation, facts.len());
/// }
/// ```
/// # Performance Considerations
///
/// - **File Parsing**: Use [`Program::parse()`] for best performance with file-based programs
/// - **Memory Usage**: Call [`prune_dead_components()`] to minimize memory footprint
/// - **Boolean Facts**: Automatic extraction reduces rule evaluation overhead
///
/// [`Program::parse()`]: Program::parse
/// [`prune_dead_components()`]: Program::prune_dead_components
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    edbs: Vec<Relation>,
    idbs: Vec<Relation>,
    rules: Vec<FLRule>,
    bool_facts: HashMap<String, Vec<(Vec<ConstType>, bool)>>,
}

/// Provides formatted string representation of FlowLog programs.
///
/// Formats the complete program in standard FlowLog syntax with proper section
/// organization and readable structure. The output follows the canonical FlowLog
/// format that can be parsed back into a program.
///
/// # Format Structure
///
/// The display format organizes components into distinct sections:
/// 1. **`.in`** section: All EDB (input) relation declarations
/// 2. **`.out`** section: All IDB (output) relation declarations  
/// 3. **`.rule`** section: All logic rules for data transformation
/// 4. **Boolean facts**: Extracted from `.rule` constant facts (if any)
///
/// # Output Format
///
/// ```text
/// .in
/// input_relation(attr1: type1, attr2: type2).
///
/// .out
/// output_relation(result: type).output("file.csv").
///
/// .rule
/// output_relation(Result) :- input_relation(X, Y), condition(X, Y, Result).
/// ```
///
/// # Examples
///
/// ```rust
/// use parser::program::Program;
/// use std::fs;
///
/// let program = Program::parse("../../example/tc.dl");
///
/// // Display complete program
/// println!("{}", program);
///
/// // Save optimized program to file (this would normally work in a real environment)
/// let optimized = program.prune_dead_components();
/// // fs::write("optimized.dl", format!("{}", optimized))
/// //     .expect("Failed to write optimized program");
///
/// // Compare original vs optimized size
/// let original_size = format!("{}", program).len();
/// let optimized_size = format!("{}", optimized).len();
/// println!("Size reduction: {:.1}%",
///          (1.0 - optimized_size as f64 / original_size as f64) * 100.0);
/// ```
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
    /// Parses a complete FlowLog program from a file path.
    ///
    /// This is the primary entry point for loading FlowLog programs from disk.
    /// The method handles the complete parsing pipeline including grammar parsing,
    /// AST construction, boolean fact extraction, and dead code elimination.
    ///
    /// # Arguments
    ///
    /// * `path` - File system path to the FlowLog program file (`.dl` extension recommended)
    ///
    /// # Returns
    ///
    /// A fully parsed and optimized [`Program`] instance ready for execution
    ///
    /// # Parsing Pipeline
    ///
    /// 1. **File Reading**: Loads the entire file content into memory
    /// 2. **Lexical Analysis**: Tokenizes the FlowLog source code
    /// 3. **Syntax Parsing**: Constructs AST according to FlowLog grammar
    /// 4. **Semantic Analysis**: Builds typed program representation
    /// 5. **Boolean Extraction**: Extracts constant facts from boolean rules
    /// 6. **Dead Code Elimination**: Removes unused components for optimization
    ///
    /// # Error Handling
    ///
    /// The method uses aggressive error handling appropriate for program initialization:
    /// - **File I/O Errors**: Panics with descriptive message if file cannot be read
    /// - **Syntax Errors**: Panics with parser error details and line information
    /// - **Semantic Errors**: Panics with context about invalid program structure
    ///
    /// For production use cases requiring graceful error handling, consider wrapping
    /// this method in appropriate error handling logic.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    /// ```rust
    /// use parser::program::Program;
    ///
    /// // Parse a network policy program
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// println!("Loaded program with {} rules", program.rules().len());
    /// println!("Input relations: {}", program.edbs().len());
    /// println!("Output relations: {}", program.idbs().len());
    /// ```
    ///
    /// # Panics
    ///
    /// This method panics in the following situations:
    /// - **File Not Found**: The specified path does not exist or is not readable
    /// - **Permission Denied**: Insufficient permissions to read the file
    /// - **Invalid UTF-8**: File contains invalid UTF-8 sequences
    /// - **Syntax Error**: FlowLog source contains invalid syntax
    /// - **Grammar Error**: Source doesn't conform to FlowLog grammar rules
    /// - **Empty Parse**: No valid program structure found in source
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

    /// Returns all extensional database (EDB) relations.
    ///
    /// EDB relations represent the input data sources for the FlowLog program.
    /// These are typically loaded from external files, databases, or network
    /// sources and serve as the foundation for all derived computations.
    ///
    /// # Returns
    ///
    /// A slice of [`Relation`] references representing all input relations,
    /// preserving the order they were declared in the source program
    ///
    /// # Relation Properties
    ///
    /// Each EDB relation includes:
    /// - **Name**: Unique identifier for the relation
    /// - **Schema**: Typed attribute definitions
    /// - **Input Path**: Optional file path for data loading
    /// - **Constraints**: Type and domain constraints on attributes
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::program::Program;
    ///
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// // Examine all input relations
    /// for edb in program.edbs() {
    ///     println!("Input relation: {}", edb.name());
    ///     println!("  Arity: {}", edb.arity());
    ///     
    ///     if let Some(input_path) = edb.input_path() {
    ///         println!("  Data source: {}", input_path);
    ///     }
    ///     
    ///     for attr in edb.attributes() {
    ///         println!("  Attribute: {} ({})", attr.name(), attr.data_type());
    ///     }
    /// }
    ///
    /// // Check for required data files
    /// let input_files: Vec<_> = program.edbs()
    ///     .iter()
    ///     .filter_map(|edb| edb.input_path())
    ///     .collect();
    ///
    /// println!("Program requires {} input files", input_files.len());
    /// ```
    #[must_use]
    pub fn edbs(&self) -> &[Relation] {
        &self.edbs
    }

    /// Returns all intensional database (IDB) relations.
    ///
    /// IDB relations represent the output data computed by the FlowLog program.
    /// These relations are derived through the application of logic rules to
    /// input data and other derived relations.
    ///
    /// # Returns
    ///
    /// A slice of [`Relation`] references representing all output relations,
    /// preserving the order they were declared in the source program
    ///
    /// # Relation Properties
    ///
    /// Each IDB relation includes:
    /// - **Name**: Unique identifier for the relation
    /// - **Schema**: Typed attribute definitions for computed results
    /// - **Output Path**: Optional file path for result writing
    /// - **Derivation Rules**: Logic rules that compute this relation's tuples
    ///
    /// # Output Relations vs. IDB Relations
    ///
    /// Not all IDB relations are output relations:
    /// - **IDB Relations**: All computed relations (intermediate + final)
    /// - **Output Relations**: Only IDBs marked with `.output()` directive
    /// - Use [`output_relations()`] to get only final output relations
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::program::Program;
    ///
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// // Examine all computed relations
    /// for idb in program.idbs() {
    ///     println!("Computed relation: {}", idb.name());
    ///     println!("  Schema: {} attributes", idb.arity());
    ///     
    ///     // Check if this relation is marked for output
    ///     if idb.output_path().is_some() {
    ///         println!("  Status: Final output relation");
    ///     } else {
    ///         println!("  Status: Intermediate computation");
    ///     }
    /// }
    ///
    /// // Separate intermediate from final relations
    /// let intermediate_count = program.idbs()
    ///     .iter()
    ///     .filter(|idb| idb.output_path().is_none())
    ///     .count();
    ///     
    /// let output_count = program.output_relations().len();
    ///
    /// println!("Intermediate relations: {}", intermediate_count);
    /// println!("Final output relations: {}", output_count);
    /// ```
    ///
    /// [`output_relations()`]: Program::output_relations
    #[must_use]
    pub fn idbs(&self) -> &[Relation] {
        &self.idbs
    }

    /// Returns all logic rules in the program.
    ///
    /// Logic rules define the computational logic of the FlowLog program,
    /// specifying how input data is transformed and combined to produce
    /// derived facts and final outputs.
    ///
    /// # Returns
    ///
    /// A slice of [`FLRule`] references representing all transformation rules,
    /// preserving the order they were declared in the source program
    ///
    /// Note: Boolean fact rules (rules with only `True`/`False` predicates)
    /// are automatically extracted during parsing and stored separately.
    /// Use [`bool_facts()`] to access extracted boolean facts.
    ///
    /// # Rule Properties
    ///
    /// Each rule contains:
    /// - **Head**: The relation being derived
    /// - **Body**: Conditions that must be satisfied
    /// - **Variables**: Variable bindings across head and body
    /// - **Planning Flag**: Whether optimization is enabled
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::program::Program;
    /// use parser::logic::Predicate;
    ///
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// // Analyze rule complexity
    /// for (i, rule) in program.rules().iter().enumerate() {
    ///     println!("Rule {}: {}", i, rule.head().name());
    ///     println!("  Body predicates: {}", rule.rhs().len());
    ///     println!("  Planning enabled: {}", rule.is_planning());
    ///     
    ///     // Count different predicate types
    ///     let atom_count = rule.rhs().iter()
    ///         .filter(|p| matches!(p, Predicate::PositiveAtomPredicate(_)))
    ///         .count();
    ///     let negation_count = rule.rhs().iter()
    ///         .filter(|p| matches!(p, Predicate::NegatedAtomPredicate(_)))
    ///         .count();
    ///     let comparison_count = rule.rhs().iter()
    ///         .filter(|p| matches!(p, Predicate::ComparePredicate(_)))
    ///         .count();
    ///         
    ///     println!("  Atoms: {}, Negations: {}, Comparisons: {}",
    ///              atom_count, negation_count, comparison_count);
    /// }
    ///
    /// // Find rules by complexity
    /// let complex_rules: Vec<_> = program.rules()
    ///     .iter()
    ///     .enumerate()
    ///     .filter(|(_, rule)| rule.rhs().len() > 5)
    ///     .collect();
    ///     
    /// println!("Found {} complex rules (>5 predicates)", complex_rules.len());
    /// ```
    ///
    /// [`bool_facts()`]: Program::bool_facts
    #[must_use]
    pub fn rules(&self) -> &[FLRule] {
        &self.rules
    }

    /// Returns the boolean facts extracted from rules with constant predicates.
    ///
    /// Boolean facts are constant facts automatically extracted from rules that
    /// contain only boolean predicates (`True` or `False`). These facts represent
    /// static configuration, initialization data, or testing constants.
    ///
    /// # Returns
    ///
    /// A reference to a [`HashMap`] where:
    /// - **Key**: Relation name as [`String`]
    /// - **Value**: Vector of tuples containing:
    ///   - [`Vec<ConstType>`]: Constant values extracted from rule head
    ///   - [`bool`]: The boolean value (`true` for `True` predicates, `false` for `False`)
    ///
    /// # Extraction Process
    ///
    /// Boolean facts are extracted during program parsing:
    /// 1. **Rule Analysis**: Identify rules with only boolean predicates
    /// 2. **Head Extraction**: Extract constant values from rule heads
    /// 3. **Fact Storage**: Store as (values, boolean) tuples
    /// 4. **Rule Removal**: Remove original boolean rules from rule set
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::program::Program;
    /// use parser::primitive::ConstType;
    ///
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// // Examine all boolean facts
    /// for (relation_name, facts) in program.bool_facts() {
    ///     println!("Boolean facts for relation '{}':", relation_name);
    ///     
    ///     for (values, is_true) in facts {
    ///         let value_strings: Vec<String> = values.iter()
    ///             .map(|v| match v {
    ///                 ConstType::Integer(i) => i.to_string(),
    ///                 ConstType::Text(s) => format!("\"{}\"", s),
    ///             })
    ///             .collect();
    ///             
    ///         let status = if *is_true { "TRUE" } else { "FALSE" };
    ///         println!("  {}({}) = {}", relation_name, value_strings.join(", "), status);
    ///     }
    /// }
    ///
    /// // Extract configuration values
    /// if let Some(config_facts) = program.bool_facts().get("config") {
    ///     for (values, is_true) in config_facts {
    ///         if *is_true && values.len() >= 1 {
    ///             if let ConstType::Text(key) = &values[0] {
    ///                 match key.as_str() {
    ///                     "debug" => println!("Debug mode enabled"),
    ///                     "verbose" => println!("Verbose output enabled"),
    ///                     _ => println!("Unknown config: {}", key),
    ///                 }
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn bool_facts(&self) -> &HashMap<String, Vec<(Vec<ConstType>, bool)>> {
        &self.bool_facts
    }

    /// Returns all output relations that are marked for final output.
    ///
    /// Output relations are IDB relations that have been explicitly marked
    /// with the `.output("path")` directive, indicating they should be
    /// materialized and written to external storage.
    ///
    /// # Returns
    ///
    /// A [`Vec`] of [`Relation`] references representing only the relations
    /// marked for final output, filtered from all IDB relations
    ///
    /// # Output Directives
    ///
    /// Relations are marked for output using the `.output()` directive:
    /// ```text
    /// .out
    /// results(data: text).output("results.csv").
    /// intermediate(temp: integer).  // Not marked for output
    /// ```
    ///
    /// Only `results` would be returned by this method, even though both
    /// are IDB relations.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::program::Program;
    ///
    /// let program = Program::parse("../../example/tc.dl");
    ///
    /// // Process all output relations
    /// for output_rel in program.output_relations() {
    ///     println!("Output relation: {}", output_rel.name());
    ///     
    ///     if let Some(output_path) = output_rel.output_path() {
    ///         println!("  Will be written to: {}", output_path);
    ///         println!("  Schema: {} attributes", output_rel.arity());
    ///         
    ///         // List attribute details
    ///         for attr in output_rel.attributes() {
    ///             println!("    {}: {}", attr.name(), attr.data_type());
    ///         }
    ///     }
    /// }
    ///
    /// // Validate output configuration
    /// let output_count = program.output_relations().len();
    /// let total_idb_count = program.idbs().len();
    ///
    /// println!("Final outputs: {} of {} computed relations",
    ///          output_count, total_idb_count);
    ///          
    /// if output_count == 0 {
    ///     println!("WARNING: No relations marked for output!");
    /// }
    ///
    /// // Check for output file conflicts
    /// let mut output_paths = std::collections::HashSet::new();
    /// for rel in program.output_relations() {
    ///     if let Some(path) = rel.output_path() {
    ///         if !output_paths.insert(path) {
    ///             println!("WARNING: Multiple relations output to {}", path);
    ///         }
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn output_relations(&self) -> Vec<&Relation> {
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
        let dead_edbs: Vec<&Relation> = self
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
        let dead_idbs: Vec<&Relation> = self
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
        let pruned_idbs: Vec<Relation> = self
            .idbs
            .iter()
            .filter(|decl| needed_predicates.contains(decl.name()))
            .cloned()
            .collect();

        let pruned_edbs: Vec<Relation> = self
            .edbs
            .iter()
            .filter(|decl| needed_predicates.contains(decl.name()))
            .cloned()
            .collect();

        // Filter boolean facts to keep only needed ones
        let mut pruned_bool_facts = HashMap::new();
        for (rel_name, facts) in &self.bool_facts {
            if needed_predicates.contains(rel_name.as_str()) {
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

/// Enables parsing FlowLog programs from grammar tokens.
///
/// Implements the [`Lexeme`] trait to support parsing complete FlowLog programs
/// from Pest grammar parse trees. This implementation handles the entire program
/// construction pipeline from tokens to a fully structured program object.
///
/// # Parsing Process
///
/// The method processes the parse tree in several phases:
/// 1. **Section Identification**: Recognizes `.in`, `.out`, and `.rule` sections
/// 2. **Relation Parsing**: Constructs EDB and IDB relation declarations
/// 3. **Rule Parsing**: Builds logic rules from rule declarations
/// 4. **Program Assembly**: Combines all components into a coherent program
/// 5. **Boolean Extraction**: Automatically extracts boolean facts from rules
///
/// # Section Processing
///
/// ## EDB Section (`.in`)
/// Processes input relation declarations that define:
/// - Relation names and schemas
/// - Attribute types and constraints
/// - Optional input file paths
///
/// ## IDB Section (`.out`)
/// Processes output relation declarations that define:
/// - Computed relation schemas
/// - Optional output file paths
/// - Result materialization directives
///
/// ## Rule Section (`.rule`)
/// Processes logic rules that define:
/// - Head predicates (what is derived)
/// - Body predicates (conditions and computations)
/// - Variable bindings and constraints
/// - Planning optimization directives
///
/// # Automatic Processing
///
/// During parsing, the method automatically:
/// - **Boolean Fact Extraction**: Identifies and extracts constant facts
/// - **Rule Classification**: Separates boolean rules from logic rules
/// - **Schema Validation**: Ensures relation declarations are well-formed
/// - **Dependency Preparation**: Sets up structures for dependency analysis
///
/// # Examples
///
/// The parser handles complete FlowLog programs:
/// ```text
/// .in
/// user(name: text, age: integer).
/// permission(user: text, resource: text).
///
/// .out
/// access(user: text, resource: text).output("access.csv").
/// audit_log(event: text, timestamp: integer).
///
/// .rule
/// access(User, Resource) :-
///     user(User, Age),
///     permission(User, Resource),
///     Age >= 18.
///
/// config("debug") :- True.
/// audit_log("startup", 0) :- True.
/// ```
///
/// This results in a program with:
/// - 2 EDB relations (`user`, `permission`)
/// - 2 IDB relations (`access`, `audit_log`)
/// - 1 logic rule (age-based access control)
/// - 2 boolean facts (config and audit entries)
impl Lexeme for Program {
    /// Constructs a FlowLog program from a parsed grammar rule.
    ///
    /// This method serves as the primary entry point for converting a Pest
    /// parse tree into a structured [`Program`] object. It processes all
    /// program sections and automatically optimizes the result.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A Pest [`Pair<Rule>`] representing the complete program parse tree
    ///
    /// # Returns
    ///
    /// A fully constructed [`Program`] with all components parsed and boolean facts extracted
    ///
    /// # Processing Steps
    ///
    /// 1. **Parse Tree Traversal**: Iterates through top-level program sections
    /// 2. **Section Dispatch**: Routes each section to appropriate parsing function
    /// 3. **Component Assembly**: Combines parsed components into program structure
    /// 4. **Boolean Processing**: Extracts boolean facts from constant rules
    /// 5. **Program Finalization**: Returns optimized program ready for execution
    ///
    /// # Section Parsing Functions
    ///
    /// The method uses nested helper functions for section-specific parsing:
    /// - `parse_rel_decls()`: Processes EDB and IDB relation declarations
    /// - `parse_rules()`: Processes logic rules and boolean fact rules
    ///
    /// These functions handle the specific grammar requirements for each section
    /// while maintaining type safety and error resilience.
    ///
    /// # Grammar Rule Mapping
    ///
    /// | Grammar Rule | Program Component | Processing Function               |
    /// |--------------|-------------------|-----------------------------------|
    /// | `edb_decl`   | EDB relations     | `parse_rel_decls(&mut edbs, ...)` |
    /// | `idb_decl`   | IDB relations     | `parse_rel_decls(&mut idbs, ...)` |
    /// | `rule_decl`  | Logic rules       | `parse_rules(&mut rules, ...)`    |
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner_rules = parsed_rule.into_inner();
        let mut edbs: Vec<Relation> = Vec::new();
        let mut idbs: Vec<Relation> = Vec::new();
        let mut rules: Vec<FLRule> = Vec::new();

        // Parse relation declarations and collect them in the appropriate vector
        /// Parses relation declarations from a grammar section.
        ///
        /// This helper function processes either EDB or IDB declarations from
        /// the `.in` or `.out` sections of a FlowLog program. It iterates through
        /// all relation declarations in the section and constructs [`Relation`] objects.
        ///
        /// # Arguments
        ///
        /// * `vec` - Mutable reference to vector for collecting parsed relations
        /// * `rule` - Pest rule containing relation declarations (edb_decl or idb_decl)
        ///
        /// # Processing Logic
        ///
        /// 1. **Rule Iteration**: Processes each child rule in the section
        /// 2. **Type Filtering**: Only processes actual relation declarations
        /// 3. **Relation Construction**: Uses [`Relation::from_parsed_rule()`] for parsing
        /// 4. **Collection**: Adds parsed relations to the provided vector
        ///
        /// # Supported Relation Types
        ///
        /// - **EDB Relations**: Input data schemas with optional file paths
        /// - **IDB Relations**: Output data schemas with optional output directives
        ///
        /// Both types support full attribute specifications with types and constraints.
        fn parse_rel_decls(vec: &mut Vec<Relation>, rule: Pair<Rule>) {
            let rel_decls = rule.into_inner();
            for rel_decl in rel_decls {
                // Only process relation declarations
                if rel_decl.as_rule() == Rule::edb_relation_decl
                    || rel_decl.as_rule() == Rule::idb_relation_decl
                {
                    vec.push(Relation::from_parsed_rule(rel_decl));
                }
            }
        }

        // Parse rules and collect them in a vector
        /// Parses logic rules from the `.rule` section.
        ///
        /// This helper function processes all logic rules defined in the `.rule`
        /// section of a FlowLog program. It constructs [`FLRule`] objects that
        /// define the computational logic of the program.
        ///
        /// # Arguments
        ///
        /// * `vec` - Mutable reference to vector for collecting parsed rules
        /// * `rule` - Pest rule containing rule declarations (rule_decl)
        ///
        /// # Processing Logic
        ///
        /// 1. **Rule Iteration**: Processes each rule definition in the section
        /// 2. **Type Validation**: Only processes actual rule grammar elements
        /// 3. **Rule Construction**: Uses [`FLRule::from_parsed_rule()`] for parsing
        /// 4. **Collection**: Adds parsed rules to the provided vector
        ///
        /// Note: Boolean rules are later extracted separately during program finalization.
        fn parse_rules(vec: &mut Vec<FLRule>, rule: Pair<Rule>) {
            let rules_iterator = rule.into_inner();
            for rule in rules_iterator {
                if rule.as_rule() == Rule::rule {
                    vec.push(FLRule::from_parsed_rule(rule));
                }
            }
        }

        // Process each section of the program
        //
        // Main parsing loop that processes top-level program sections.
        //
        // This loop iterates through all major sections of the FlowLog program
        // and dispatches each section to the appropriate parsing function based
        // on the grammar rule type.
        //
        // Section Processing Order:
        // The sections can appear in any order in the source, but are processed as:
        // 1. EDB Declarations (.in section) - Input relation schemas
        // 2. IDB Declarations (.out section) - Output relation schemas
        // 3. Rule Declarations (.rule section) - Logic transformation rules
        //
        // Grammar Rule Dispatch:
        // - edb_decl -> Parse input relations
        // - idb_decl -> Parse output relations
        // - rule_decl -> Parse logic rules
        // - Other rules -> Ignored for forward compatibility
        for inner_rule in inner_rules {
            match inner_rule.as_rule() {
                Rule::edb_decl => parse_rel_decls(&mut edbs, inner_rule),
                Rule::idb_decl => parse_rel_decls(&mut idbs, inner_rule),
                Rule::rule_decl => parse_rules(&mut rules, inner_rule),
                _ => {} // Ignore other rules
            }
        }

        // Create initial program structure with parsed components
        let mut program = Self {
            edbs,
            idbs,
            rules,
            bool_facts: HashMap::new(),
        };

        // Extract boolean facts during parsing
        //
        // Automatically processes rules with boolean predicates to extract
        // constant facts. This optimization:
        // 1. Identifies rules with only True/False predicates
        // 2. Extracts constant values from rule heads
        // 3. Stores facts in bool_facts map for efficient access
        // 4. Removes boolean rules from the main rules collection
        //
        // This separation improves performance by avoiding repeated
        // evaluation of constant facts during program execution.
        program.extract_boolean_facts();

        program
    }
}
