//! FlowLog Datalog program representation and manipulation.
//!
//! A program contains:
//! - Relation declarations (schema only)
//! - Input directives (specify how to read EDB data)
//! - Output directives (specify which relations to output)
//! - Print size directives (specify which relations to print size for)
//! - Rules
//! - Inline facts (ground tuples like `relation(1, 2).`)

use super::{
    declaration::{ExternFn, InputDirective, OutputDirective, PrintSizeDirective, Relation},
    logic::{Arithmetic, AtomArg, Factor, FlowLogRule, FnCall, Head, Predicate},
    ConstType, FlowLogParser, Lexeme, Rule,
};
use pest::{iterators::Pair, Parser};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::{fmt, fs};
use tracing::{debug, info, warn};

/// A complete FlowLog program.
///
/// ```ignore
/// use FlowLog_parser::program::Program;
///
/// let program = Program::parse("PATH_TO_DATALOG_FILE");
/// println!("{}", program);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Program {
    relations: Vec<Relation>,
    rules: Vec<FlowLogRule>,
    /// External scalar UDF declarations.
    udfs: Vec<ExternFn>,
    /// Map: relation name -> [constant tuple]
    facts: HashMap<String, Vec<Vec<ConstType>>>,
}

// =============================================================================
// Display
// =============================================================================

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=============================================")?;
        writeln!(f, "FlowLog DATALOG PROGRAM")?;
        writeln!(f, "=============================================")?;
        writeln!(f)?;

        if !self.relations.is_empty() {
            writeln!(f, "Relations")?;
            writeln!(f, "---------------------------------------------")?;
            for rel in &self.relations {
                writeln!(f, "{}", rel)?;
            }
            writeln!(f)?;
        }

        if !self.udfs.is_empty() {
            writeln!(f, "Extern Functions")?;
            writeln!(f, "---------------------------------------------")?;
            for udf in &self.udfs {
                writeln!(f, "{}", udf)?;
            }
            writeln!(f)?;
        }

        if !self.rules.is_empty() {
            writeln!(f, "Rules")?;
            writeln!(f, "---------------------------------------------")?;
            for rule in &self.rules {
                writeln!(f, "{}", rule)?;
            }
            writeln!(f)?;
        }

        if !self.facts.is_empty() {
            writeln!(f, "Facts")?;
            writeln!(f, "---------------------------------------------")?;
            for (rel_name, facts) in &self.facts {
                for vals in facts {
                    let values = vals
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    writeln!(f, "{}({}).", rel_name, values)?;
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Public API
// =============================================================================

impl Program {
    /// Parse a program from a file, resolving `.import` directives recursively.
    ///
    /// Imported files are resolved relative to the importing file's directory.
    /// Panics on I/O errors, parse errors, or circular imports.
    #[must_use]
    pub fn parse(path: &str) -> Self {
        // Two sets to distinguish true cycles from diamond imports:
        //   `in_progress` — ancestors on the current DFS path; revisiting one is a cycle.
        //   `completed`   — files fully merged in a prior branch; safe to skip.
        let mut in_progress = HashSet::new();
        let mut completed = HashSet::new();
        let mut program =
            Self::parse_recursive(Path::new(path), &mut in_progress, &mut completed);
        program.prune_dead_components();

        debug!("\n{}", program);
        info!("Successfully parsed program from '{}'.", path);

        program
    }

    /// All relation declarations.
    #[must_use]
    #[inline]
    pub fn relations(&self) -> &[Relation] {
        &self.relations
    }

    /// EDB relations (those with input parameters).
    #[must_use]
    #[inline]
    pub fn edbs(&self) -> Vec<&Relation> {
        self.relations.iter().filter(|rel| rel.is_edb()).collect()
    }

    /// Ordered EDB relation names (sorted lexicographically).
    #[must_use]
    pub fn edb_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .relations
            .iter()
            .filter(|rel| rel.is_edb())
            .map(|rel| rel.name().to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Deduplicated EDB relation fingerprints.
    #[must_use]
    pub fn edb_fingerprints(&self) -> HashSet<u64> {
        self.relations
            .iter()
            .filter(|rel| rel.is_edb())
            .map(|rel| rel.fingerprint())
            .collect()
    }

    /// IDB relations (those annotated with `.output` or `.printsize`).
    ///
    /// Returned in declaration order.
    #[must_use]
    #[inline]
    pub fn idbs(&self) -> Vec<&Relation> {
        self.relations
            .iter()
            .filter(|rel| rel.is_output_printsize())
            .collect()
    }

    /// Transformation rules.
    #[must_use]
    #[inline]
    pub fn rules(&self) -> &[FlowLogRule] {
        &self.rules
    }

    /// Inline facts (ground tuples).
    #[must_use]
    #[inline]
    pub fn facts(&self) -> &HashMap<String, Vec<Vec<ConstType>>> {
        &self.facts
    }

    /// External UDF declarations.
    #[must_use]
    #[inline]
    pub fn udfs(&self) -> &[ExternFn] {
        &self.udfs
    }
}

// =============================================================================
// File loading
// =============================================================================

impl Program {
    /// Recursively parse a file and all its `.import`-ed files, merging
    /// everything into a single flat [`Program`].
    ///
    /// `in_progress` tracks files on the current DFS path — revisiting one means
    /// a cycle and panics. `completed` tracks files fully merged in a prior branch
    /// (diamond imports) — revisiting one is a no-op.
    fn parse_recursive(
        path: &Path,
        in_progress: &mut HashSet<PathBuf>,
        completed: &mut HashSet<PathBuf>,
    ) -> Self {
        let canonical = fs::canonicalize(path)
            .unwrap_or_else(|_| panic!("Parser error: cannot resolve path '{}'", path.display()));

        if in_progress.contains(&canonical) {
            panic!(
                "Parser error: circular import detected — '{}' is already being loaded",
                path.display()
            );
        }
        if completed.contains(&canonical) {
            return Self::default(); // Diamond import — already merged, skip.
        }

        let source = fs::read_to_string(path)
            .unwrap_or_else(|_| panic!("Parser error: failed to read '{}'", path.display()));

        let mut pairs = FlowLogParser::parse(Rule::main_grammar, &source)
            .unwrap_or_else(|e| panic!("Parser error in '{}': {}", path.display(), e));
        let root = pairs.next().expect("Parser error: no parsed rule found");

        let base_dir = path.parent().unwrap_or(Path::new("."));
        let (mut program, import_paths) = Self::from_parsed_rule_with_imports(root);

        in_progress.insert(canonical.clone());
        for import_path in import_paths {
            let full_path = base_dir.join(&import_path);
            info!("Importing '{}'.", full_path.display());
            let imported = Self::parse_recursive(&full_path, in_progress, completed);
            program.merge(imported);
        }
        in_progress.remove(&canonical);
        completed.insert(canonical);

        program
    }

    /// Merge `other` into `self`.
    ///
    /// - Relations: identical declarations are silently deduplicated; conflicting
    ///   schemas (same name, different attributes) panic.
    /// - UDFs: deduplicated by name.
    /// - Rules and facts: merged additively.
    fn merge(&mut self, other: Self) {
        for rel in other.relations {
            match self.relations.iter().find(|r| r.name() == rel.name()) {
                Some(existing) if existing == &rel => {} // identical — skip
                Some(_) => panic!(
                    "Parser error: conflicting declarations for relation '{}' across imported files",
                    rel.name()
                ),
                None => self.relations.push(rel),
            }
        }

        let existing_udfs: HashSet<String> = self.udfs.iter().map(|u| u.name().to_string()).collect();
        for udf in other.udfs {
            if !existing_udfs.contains(udf.name()) {
                self.udfs.push(udf);
            }
        }

        self.rules.extend(other.rules);

        for (rel, tuples) in other.facts {
            self.facts.entry(rel).or_default().extend(tuples);
        }
    }
}

// =============================================================================
// Parsing internals
// =============================================================================

impl Program {
    /// Build a program from the top-level grammar node, also returning the
    /// `.import` paths found in declaration order.
    ///
    /// Import paths are raw strings as written in the source; callers resolve
    /// them relative to the importing file's directory.
    fn from_parsed_rule_with_imports(parsed_rule: Pair<Rule>) -> (Self, Vec<String>) {
        let mut relations = Vec::new();
        let mut input_directives: Vec<InputDirective> = Vec::new();
        let mut output_directives: Vec<OutputDirective> = Vec::new();
        let mut printsize_directives: Vec<PrintSizeDirective> = Vec::new();
        let mut rules = Vec::new();
        let mut udfs = Vec::new();
        let mut raw_facts = Vec::new();
        let mut import_paths: Vec<String> = Vec::new();

        for node in parsed_rule.into_inner() {
            match node.as_rule() {
                Rule::declaration => relations.push(Relation::from_parsed_rule(node)),
                Rule::extern_fn => udfs.push(ExternFn::from_parsed_rule(node)),
                Rule::include_directive => {
                    // Child is the `string` token, e.g. `"lib/base.dl"` — strip quotes.
                    let raw = node
                        .into_inner()
                        .next()
                        .expect("Parser error: import/include directive missing path")
                        .as_str();
                    import_paths.push(raw.trim_matches('"').to_string());
                }
                Rule::input_directive => {
                    input_directives.push(InputDirective::from_parsed_rule(node))
                }
                Rule::output_directive => {
                    output_directives.push(OutputDirective::from_parsed_rule(node))
                }
                Rule::printsize_directive => {
                    printsize_directives.push(PrintSizeDirective::from_parsed_rule(node))
                }
                Rule::rule => rules.push(FlowLogRule::from_parsed_rule(node)),
                Rule::fact => {
                    let head_node = node
                        .into_inner()
                        .next()
                        .expect("Parser error: fact missing head");
                    raw_facts.push(FlowLogRule::new(Head::from_parsed_rule(head_node), vec![], false));
                }
                _ => {}
            }
        }

        Self::apply_directives(
            &mut relations,
            input_directives,
            output_directives,
            printsize_directives,
        );
        Self::reclassify_udf_predicates(&mut rules, &udfs);

        let mut program = Self { relations, rules, udfs, ..Self::default() };
        for fact in raw_facts {
            program.extract_fact(fact);
        }

        (program, import_paths)
    }

    /// Validate and apply input / output / printsize directives to `relations`.
    ///
    /// Panics on duplicate directives or directives that reference undeclared relations.
    fn apply_directives(
        relations: &mut [Relation],
        input_directives: Vec<InputDirective>,
        output_directives: Vec<OutputDirective>,
        printsize_directives: Vec<PrintSizeDirective>,
    ) {
        // --- validate: no duplicates within each directive kind ---

        let mut seen: HashSet<&str> = HashSet::new();
        for d in &input_directives {
            if !seen.insert(d.relation_name()) {
                panic!("Parser error: duplicate .input directive for relation '{}'", d.relation_name());
            }
        }

        seen.clear();
        for d in &output_directives {
            if !seen.insert(d.relation_name()) {
                panic!("Parser error: duplicate .output directive for relation '{}'", d.relation_name());
            }
        }

        seen.clear();
        for d in &printsize_directives {
            if !seen.insert(d.relation_name()) {
                panic!("Parser error: duplicate .printsize directive for relation '{}'", d.relation_name());
            }
        }

        // --- apply ---

        for d in input_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_input_params(d.parameters().clone()),
                None => panic!(
                    "Parser error: .input directive for undeclared relation '{}'. \
                     Declare it with .decl first.",
                    d.relation_name()
                ),
            }
        }

        for d in output_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_output(true),
                None => panic!(
                    "Parser error: .output directive for undeclared relation '{}'. \
                     Declare it with .decl first.",
                    d.relation_name()
                ),
            }
        }

        for d in printsize_directives {
            match relations.iter_mut().find(|r| r.name() == d.relation_name()) {
                Some(rel) => rel.set_printsize(true),
                None => panic!(
                    "Parser error: .printsize directive for undeclared relation '{}'. \
                     Declare it with .decl first.",
                    d.relation_name()
                ),
            }
        }
    }

    /// Reclassify body atoms whose name matches an `.extern fn` as [`FnCallPredicate`].
    ///
    /// PEG grammars resolve `name(args...)` as `atom` before `fn_call_expr` since
    /// they share identical syntax. The distinction is semantic (declared as a
    /// relation vs. an extern function), so we correct it here after all
    /// declarations have been collected.
    fn reclassify_udf_predicates(rules: &mut [FlowLogRule], udfs: &[ExternFn]) {
        let udf_names: HashSet<&str> = udfs.iter().map(ExternFn::name).collect();
        if udf_names.is_empty() {
            return;
        }

        for rule in rules.iter_mut() {
            let needs_rewrite = rule.rhs().iter().any(|p| {
                matches!(
                    p,
                    Predicate::PositiveAtomPredicate(a) | Predicate::NegativeAtomPredicate(a)
                    if udf_names.contains(a.name())
                )
            });
            if !needs_rewrite {
                continue;
            }

            let new_rhs = rule
                .rhs()
                .iter()
                .map(|pred| match pred {
                    Predicate::PositiveAtomPredicate(atom)
                    | Predicate::NegativeAtomPredicate(atom)
                        if udf_names.contains(atom.name()) =>
                    {
                        let args = atom
                            .arguments()
                            .iter()
                            .map(|a| match a {
                                AtomArg::Var(v) => Arithmetic::new(Factor::Var(v.clone()), vec![]),
                                AtomArg::Const(c) => Arithmetic::new(Factor::Const(c.clone()), vec![]),
                                AtomArg::Placeholder => panic!(
                                    "Parser error: placeholder '_' not allowed in UDF call '{}'",
                                    atom.name()
                                ),
                            })
                            .collect();
                        Predicate::FnCallPredicate(FnCall::new(
                            atom.name().to_string(),
                            args,
                            matches!(pred, Predicate::NegativeAtomPredicate(_)),
                        ))
                    }
                    other => other.clone(),
                })
                .collect();

            *rule = FlowLogRule::new(rule.head().clone(), new_rhs, rule.is_planning());
        }
    }

    /// Insert a ground-tuple fact into `self.facts`.
    fn extract_fact(&mut self, fact_rule: FlowLogRule) {
        let rel_name = fact_rule.head().name().to_string();
        let tuple = fact_rule.extract_constants_from_head();
        self.facts.entry(rel_name).or_default().push(tuple);
    }
}

// =============================================================================
// Dead-code elimination
// =============================================================================

impl Program {
    /// Compute the transitive closure of dependencies needed by outputs + facts.
    ///
    /// Returns `(needed_rule_indices, needed_predicate_names)`.
    #[must_use]
    fn identify_needed_components(&self) -> (HashSet<usize>, HashSet<String>) {
        let mut needed_preds: HashSet<String> = self
            .idbs()
            .into_iter()
            .map(|d| d.name().to_string())
            .collect();

        // Fact relations are always needed.
        needed_preds.extend(self.facts.keys().cloned());

        // If no outputs and no facts, keep everything.
        if needed_preds.is_empty() {
            let all_rules = (0..self.rules.len()).collect();
            let all_preds = self.relations.iter().map(|d| d.name().to_string()).collect();
            return (all_rules, all_preds);
        }

        // Map: head name -> rule indices that derive it.
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in self.rules.iter().enumerate() {
            head_to_rules
                .entry(r.head().name().to_string())
                .or_default()
                .push(i);
        }

        // Seed: rules that define already-needed predicates.
        let mut needed_rules: HashSet<usize> = needed_preds
            .iter()
            .flat_map(|p| head_to_rules.get(p).into_iter().flatten().copied())
            .collect();

        // Build dependency map: rule index -> [(dep_rule_index, predicate_name)].
        // `usize::MAX` is used as a sentinel for EDB (no defining rule).
        let dep_map: HashMap<usize, Vec<(usize, String)>> = self
            .rules
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let deps = r
                    .rhs()
                    .iter()
                    .filter_map(|pred| match pred {
                        Predicate::PositiveAtomPredicate(a)
                        | Predicate::NegativeAtomPredicate(a) => Some(a.name()),
                        _ => None,
                    })
                    .flat_map(|atom_name| {
                        if let Some(ids) = head_to_rules.get(atom_name) {
                            ids.iter()
                                .map(|&id| (id, atom_name.to_string()))
                                .collect::<Vec<_>>()
                        } else {
                            vec![(usize::MAX, atom_name.to_string())]
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
            for &(dep_rule_id, ref pred_name) in dep_map.get(&rule_id).into_iter().flatten() {
                needed_preds.insert(pred_name.clone());
                if dep_rule_id != usize::MAX && !processed.contains(&dep_rule_id) {
                    needed_rules.insert(dep_rule_id);
                    stack.push(dep_rule_id);
                }
            }
        }

        (needed_rules, needed_preds)
    }

    /// Remove dead rules and relations in place, logging what was dropped.
    fn prune_dead_components(&mut self) {
        let (needed_rules, needed_preds) = self.identify_needed_components();

        let dead_relations: Vec<_> = self
            .relations
            .iter()
            .filter(|d| !needed_preds.contains(d.name()))
            .map(|d| d.name().to_string())
            .collect();
        if !dead_relations.is_empty() {
            warn!("Dead relations: {}", dead_relations.join(", "));
        }

        let dead_rules: Vec<_> = self
            .rules
            .iter()
            .enumerate()
            .filter(|(i, _)| !needed_rules.contains(i))
            .map(|(i, r)| format!("#{}: {}", i, r))
            .collect();
        if !dead_rules.is_empty() {
            warn!("Dead rules: {}", dead_rules.join(", "));
        }

        self.relations.retain(|d| needed_preds.contains(d.name()));
        self.rules = self
            .rules
            .drain(..)
            .enumerate()
            .filter(|(i, _)| needed_rules.contains(i))
            .map(|(_, r)| r)
            .collect();
        self.facts.retain(|rel, _| needed_preds.contains(rel.as_str()));
    }
}

// =============================================================================
// Lexeme trait
// =============================================================================

impl Lexeme for Program {
    /// Build a program from the top-level grammar node (ignoring imports).
    ///
    /// For import-aware parsing use [`Program::parse`], which calls
    /// [`Program::from_parsed_rule_with_imports`] internally.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        Self::from_parsed_rule_with_imports(parsed_rule).0
    }
}
