//! Catalog of per-rule metadata and signatures for Macaron Datalog programs.
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::atom::{AtomArgumentSignature, AtomSignature};
use crate::filter::Filters;
use parser::{AtomArg, ComparisonExpr, ConstType, HeadArg, MacaronRule, Predicate};

/// Per-rule catalog with precomputed metadata.
#[derive(Debug)]
pub struct Catalog {
    /// The original rule.
    rule: MacaronRule,

    /// Reverse map from argument signatures (positive/negated/subatom) to their variable strings.
    signature_to_argument_str_map: HashMap<AtomArgumentSignature, String>,
    /// For each variable string, the first presence (if any) in every positive atom.
    /// Example: for `tc(x, z) :- arc(x, y), tc(y, z)` → `{ x: [Some(0.0), None], y: [Some(0.1), Some(1.0)], z: [None, Some(1.1)] }`.
    argument_presence_in_positive_atom_map: HashMap<String, Vec<Option<AtomArgumentSignature>>>,

    /// RHS positive atom names.
    positive_atom_names: Vec<String>,
    /// For each RHS positive atom, its argument signatures.
    positive_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,

    /// RHS negated atom names.
    negated_atom_names: Vec<String>,
    /// For each RHS negated atom, its argument signatures.
    negated_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,

    /// Local filters: variable equality constraints, constant equality constraints, placeholders.
    filters: Filters,

    /// Comparison predicates in the body.
    comparison_predicates: Vec<ComparisonExpr>,
    /// Variable sets per comparison predicate (deduplicated).
    comparison_predicates_vars_set: Vec<HashSet<String>>,

    /// For each head argument's string form, a copy of the argument.
    head_arguments_map: HashMap<String, HeadArg>,
}

// Getters and basic accessors
impl Catalog {
    /// The underlying rule.
    #[inline]
    pub fn rule(&self) -> &MacaronRule {
        &self.rule
    }

    /// All dependent relation names (positive and negated) in the body.
    pub fn dependent_atom_names(&self) -> HashSet<String> {
        self.positive_atom_names
            .iter()
            .chain(self.negated_atom_names.iter())
            .cloned()
            .collect::<HashSet<String>>()
    }

    /// Reverse map from signatures to their variable strings.
    #[inline]
    pub fn signature_to_argument_str_map(&self) -> &HashMap<AtomArgumentSignature, String> {
        &self.signature_to_argument_str_map
    }

    /// Convert signatures into their variable strings (skips those absent from the map).
    pub fn signature_to_argument_strs(
        &self,
        argument_signatures: &[AtomArgumentSignature],
    ) -> Vec<String> {
        argument_signatures
            .iter()
            .filter_map(|signature| self.signature_to_argument_str_map.get(signature).cloned())
            .collect::<Vec<String>>()
    }

    /// Active negated atoms w.r.t. the given signature arguments (subset check).
    pub fn sub_negated_atoms(
        &self,
        signature_arguments: &[AtomArgumentSignature],
    ) -> Vec<AtomSignature> {
        let signature_arguments_str_set = self
            .signature_to_argument_strs(signature_arguments)
            .into_iter()
            .collect::<HashSet<String>>();
        self.negated_atom_argument_signatures
            .iter()
            .enumerate()
            .filter_map(|(i, negated_atom_argument_signatures)| {
                let negated_atom_argument_strs_set = self
                    .signature_to_argument_strs(negated_atom_argument_signatures)
                    .into_iter()
                    .collect::<HashSet<String>>();
                if negated_atom_argument_strs_set.is_subset(&signature_arguments_str_set) {
                    Some(AtomSignature::new(false, i))
                } else {
                    None
                }
            })
            .collect::<Vec<AtomSignature>>()
    }

    /// For a variable string, returns its first-presence per positive atom (None if absent).
    #[inline]
    pub fn argument_presence_in_positive_atom_map(
        &self,
        argument_str: &str,
    ) -> &Vec<Option<AtomArgumentSignature>> {
        &self.argument_presence_in_positive_atom_map[argument_str]
    }

    /// Positive atom names in the rule body (RHS), in source order.
    #[inline]
    pub fn positive_atom_names(&self) -> &Vec<String> {
        &self.positive_atom_names
    }

    /// Argument signatures for each positive atom (order matches `positive_atom_names`).
    #[inline]
    pub fn positive_atom_argument_signatures(&self) -> &Vec<Vec<AtomArgumentSignature>> {
        &self.positive_atom_argument_signatures
    }

    /// Negated atom names in the rule body (RHS), in source order.
    #[inline]
    pub fn negated_atom_names(&self) -> &Vec<String> {
        &self.negated_atom_names
    }

    /// Argument signatures for each negated atom (order matches `negated_atom_names`).
    #[inline]
    pub fn negated_atom_argument_signatures(&self) -> &Vec<Vec<AtomArgumentSignature>> {
        &self.negated_atom_argument_signatures
    }

    /// Base constraint filters (var==var, var==const, placeholders).
    #[inline]
    pub fn filters(&self) -> &Filters {
        &self.filters
    }

    /// Name of the head relation.
    #[inline]
    pub fn head_name(&self) -> &str {
        self.rule.head().name()
    }

    /// Structured head arguments in order.
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        self.rule.head().head_arguments()
    }

    /// All variables referenced by the head arguments, deduplicated (order not guaranteed).
    pub fn head_arguments_strs(&self) -> Vec<String> {
        self.head_arguments()
            .iter()
            .flat_map(|head_arg| head_arg.vars().into_iter().cloned())
            .collect()
    }

    /// Whether a signature is constrained by a const, var equality, or placeholder filter.
    #[inline]
    pub fn is_const_or_var_eq_or_placeholder(&self, signature: &AtomArgumentSignature) -> bool {
        self.filters.is_const_or_var_eq_or_placeholder(signature)
    }

    /// Constant-equality constraints for the provided signatures.
    pub fn const_signatures(
        &self,
        signature_arguments: &[AtomArgumentSignature],
    ) -> Vec<(AtomArgumentSignature, ConstType)> {
        signature_arguments
            .iter()
            .filter_map(|signature| {
                self.filters
                    .const_map()
                    .get(signature)
                    .map(|constant| (*signature, constant.clone()))
            })
            .collect()
    }

    /// Variable-equality constraints `(target, alias)` for the provided signatures.
    pub fn var_eq_signatures(
        &self,
        signature_arguments: &[AtomArgumentSignature],
    ) -> Vec<(AtomArgumentSignature, AtomArgumentSignature)> {
        signature_arguments
            .iter()
            .filter_map(|alias| {
                self.filters
                    .var_eq_map()
                    .get(alias)
                    .map(|signature| (*signature, *alias))
            })
            .collect()
    }

    /// All variable strings from the given positive atom indices (excludes const/eq/placeholders).
    pub fn positive_vars_set(&self, rhs_ids: &[usize]) -> HashSet<&String> {
        rhs_ids
            .iter()
            .flat_map(|&rhs_id| self.positive_atom_argument_signatures[rhs_id].iter())
            .filter_map(|signature| {
                if self.is_const_or_var_eq_or_placeholder(signature) {
                    None
                } else {
                    Some(&self.signature_to_argument_str_map()[signature])
                }
            })
            .collect()
    }

    /// All variable strings from the given negated atom indices (excludes const/eq/placeholders).
    pub fn negated_vars_set(&self, neg_rhs_ids: &[usize]) -> HashSet<&String> {
        self.negated_vars(neg_rhs_ids).into_iter().collect()
    }

    /// Variable strings (ordered, may contain duplicates) from the given negated atom indices.
    pub fn negated_vars(&self, neg_rhs_ids: &[usize]) -> Vec<&String> {
        neg_rhs_ids
            .iter()
            .flat_map(|&neg_rhs_id| self.negated_atom_argument_signatures[neg_rhs_id].iter())
            .filter_map(|signature| {
                if self.is_const_or_var_eq_or_placeholder(signature) {
                    None
                } else {
                    Some(&self.signature_to_argument_str_map()[signature])
                }
            })
            .collect()
    }

    /// All comparison predicates in the body, in source order.
    #[inline]
    pub fn comparison_predicates(&self) -> &Vec<ComparisonExpr> {
        &self.comparison_predicates
    }

    /// Flattened variable strings referenced by the selected comparison predicates.
    pub fn comparison_predicates_vars_set(&self, comp_ids: &[usize]) -> Vec<&String> {
        comp_ids
            .iter()
            .flat_map(|&comp_id| self.comparison_predicates_vars_set[comp_id].iter())
            .collect()
    }

    /// Mapping from head argument text to its structured form.
    #[inline]
    pub fn head_arguments_map(&self) -> &HashMap<String, HeadArg> {
        &self.head_arguments_map
    }
}

// Construction and updates
impl Catalog {
    /// Build a Catalog from a single rule (derives signatures, filters and helper maps).
    pub fn from_rule(rule: &MacaronRule) -> Self {
        let mut catalog = Self {
            rule: rule.clone(),
            signature_to_argument_str_map: HashMap::new(),
            argument_presence_in_positive_atom_map: HashMap::new(),
            positive_atom_names: Vec::new(),
            positive_atom_argument_signatures: Vec::new(),
            negated_atom_names: Vec::new(),
            negated_atom_argument_signatures: Vec::new(),
            filters: Filters::new(HashMap::new(), HashMap::new(), HashSet::new()),
            comparison_predicates: Vec::new(),
            comparison_predicates_vars_set: Vec::new(),
            head_arguments_map: HashMap::new(),
        };
        catalog.populate_all_metadata();
        catalog
    }

    /// Update the catalog with a new rule, recomputing all metadata.
    pub fn update_rule(&mut self, rule: &MacaronRule) {
        // Update the rule first
        self.rule = rule.clone();

        // Clear all existing metadata
        self.clear();

        // Recompute all metadata with correct ordering
        self.populate_all_metadata();
    }

    /// Populate all metadata with the correct ordering.
    /// Order is important: signatures first, then presence map (depends on signatures),
    /// then comparison predicates vars (depends on comparison_predicates from signatures),
    /// finally head arguments map.
    fn populate_all_metadata(&mut self) {
        // 1. Populate argument signatures and related data (includes filters, comparison_predicates)
        self.populate_argument_signatures();

        // 2. Populate argument presence map (depends on signatures and filters from step 1)
        self.populate_argument_presence_in_positive_atom_map();

        // 3. Populate comparison predicates vars set (depends on comparison_predicates from step 1)
        self.populate_comparison_predicates_vars_set();

        // 4. Map each head arg's string to the argument itself
        self.head_arguments_map = self
            .rule
            .head()
            .head_arguments()
            .iter()
            .map(|head_arg| (head_arg.to_string(), head_arg.clone()))
            .collect();
    }

    /// Compute the deduplicated variable set for each comparison predicate.
    fn populate_comparison_predicates_vars_set(&mut self) {
        self.comparison_predicates_vars_set = self
            .comparison_predicates
            .iter()
            .map(|comparison_expr| {
                comparison_expr
                    .vars_set()
                    .into_iter()
                    .cloned()
                    .collect::<HashSet<String>>()
            })
            .collect();
    }

    fn populate_argument_signatures(&mut self) {
        // Track variables seen in positive atoms; only these are safe in negations.
        let mut is_safe_set = HashSet::new();

        // Local filters for the rule: x==y, x==1, x==_.
        let mut local_var_eq_map = HashMap::new();
        // Map each var to its first occurrence to derive var==var constraints.
        let mut local_var_first_occurrence_map: HashMap<String, AtomArgumentSignature> =
            HashMap::new();
        let mut local_const_map = HashMap::new();
        let mut local_placeholder_set = HashSet::new();

        let (positive_atoms, negated_atoms, comparison_predicates): (Vec<_>, Vec<_>, Vec<_>) =
            self.rule.rhs().iter().fold(
                (Vec::new(), Vec::new(), Vec::new()),
                |(mut pos, mut neg, mut comp), p| {
                    match p {
                        Predicate::PositiveAtomPredicate(atom) => pos.push(atom),
                        Predicate::NegatedAtomPredicate(atom) => neg.push(atom),
                        Predicate::ComparePredicate(expr) => comp.push(expr.clone()),
                        Predicate::BoolPredicate(_) => {}
                    }
                    (pos, neg, comp)
                },
            );

        // (i) populate the signatures of positive atoms
        for (rhs_id, atom) in positive_atoms.iter().enumerate() {
            self.positive_atom_names.push(atom.name().to_owned());
            let mut atom_signatures = Vec::new();
            for (argument_id, argument) in atom.arguments().iter().enumerate() {
                let rule_argument_signature =
                    AtomArgumentSignature::new(AtomSignature::new(true, rhs_id), argument_id);
                atom_signatures.push(rule_argument_signature);

                match argument {
                    AtomArg::Var(var) => {
                        is_safe_set.insert(var);
                        self.signature_to_argument_str_map
                            .insert(rule_argument_signature, var.to_string());

                        if let Some(first_occurence) = local_var_first_occurrence_map.get(var) {
                            // if the var is in the map, it is a local variable equality constraint
                            local_var_eq_map.insert(rule_argument_signature, *first_occurence);
                        } else {
                            // if the var is not in the map, it is the first occurrence
                            local_var_first_occurrence_map
                                .insert(var.to_string(), rule_argument_signature);
                        }
                    }

                    AtomArg::Const(constant) => {
                        local_const_map.insert(rule_argument_signature, constant.to_owned());
                    }

                    AtomArg::Placeholder => {
                        local_placeholder_set.insert(rule_argument_signature);
                    }
                }
            }
            self.positive_atom_argument_signatures.push(atom_signatures);
            local_var_first_occurrence_map.clear();
        }

        // (ii) populate the signatures of negated atoms
        for (neg_rhs_id, atom) in negated_atoms.iter().enumerate() {
            self.negated_atom_names.push(atom.name().to_owned());
            let mut negated_atom_signatures = Vec::new();
            for (argument_id, argument) in atom.arguments().iter().enumerate() {
                let rule_argument_signature =
                    AtomArgumentSignature::new(AtomSignature::new(false, neg_rhs_id), argument_id);
                negated_atom_signatures.push(rule_argument_signature);

                match argument {
                    AtomArg::Var(var) => {
                        if is_safe_set.contains(var) {
                            self.signature_to_argument_str_map
                                .insert(rule_argument_signature, var.to_string());

                            if let Some(first_occurence) = local_var_first_occurrence_map.get(var) {
                                // if the var is in the map, it is a local variable equality constraint
                                local_var_eq_map.insert(rule_argument_signature, *first_occurence);
                            } else {
                                // if the var is not in the map, it is the first occurrence
                                local_var_first_occurrence_map
                                    .insert(var.to_string(), rule_argument_signature);
                            }
                        } else {
                            panic!(
                                "unsafe var detected at negation !{} of rule {}",
                                atom, self.rule
                            );
                        }
                    }

                    AtomArg::Const(constant) => {
                        local_const_map.insert(rule_argument_signature, constant.to_owned());
                    }

                    AtomArg::Placeholder => {
                        local_placeholder_set.insert(rule_argument_signature);
                    }
                }
            }
            self.negated_atom_argument_signatures
                .push(negated_atom_signatures);
            local_var_first_occurrence_map.clear();
        }

        // Update filters and comparison predicates
        self.filters = Filters::new(local_var_eq_map, local_const_map, local_placeholder_set);
        self.comparison_predicates = comparison_predicates;
    }

    /// Map each variable string to its first presence per positive atom (None if absent).
    fn populate_argument_presence_in_positive_atom_map(&mut self) {
        self.argument_presence_in_positive_atom_map.clear();

        for (rhs_id, argument_signatures) in
            self.positive_atom_argument_signatures.iter().enumerate()
        {
            for argument_signature in argument_signatures {
                // skip if it is a filter
                if self
                    .filters
                    .is_const_or_var_eq_or_placeholder(argument_signature)
                {
                    continue;
                }

                if let Some(variable) = self.signature_to_argument_str_map.get(argument_signature) {
                    // Initialize the entry with a vector of None.
                    let entry = self
                        .argument_presence_in_positive_atom_map
                        .entry(variable.clone())
                        .or_insert(vec![None; self.positive_atom_argument_signatures.len()]);

                    // Record the first presence for this atom.
                    if entry[rhs_id].is_none() {
                        entry[rhs_id] = Some(*argument_signature);
                    }
                } else {
                    panic!("populate_argument_presence_map: argument signature {:?} absent from the signature map", argument_signature);
                }
            }
        }
    }

    /// Clear all metadata (but keep the rule).
    fn clear(&mut self) {
        self.signature_to_argument_str_map.clear();
        self.argument_presence_in_positive_atom_map.clear();
        self.positive_atom_names.clear();
        self.positive_atom_argument_signatures.clear();
        self.negated_atom_names.clear();
        self.negated_atom_argument_signatures.clear();
        self.filters = Filters::new(HashMap::new(), HashMap::new(), HashSet::new());
        self.comparison_predicates.clear();
        self.comparison_predicates_vars_set.clear();
        self.head_arguments_map.clear();
    }
}

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Catalog for rule: {}", self.rule())?;

        // Helper to render signatures with var names (skip those not in the reverse map)
        let fmt_sig_list = |sigs: &Vec<AtomArgumentSignature>,
                            map: &HashMap<AtomArgumentSignature, String>|
         -> String {
            let items: Vec<String> = sigs
                .iter()
                .filter_map(|s| map.get(s).map(|v| format!("{}:{}", s, v)))
                .collect();
            format!("[{}]", items.join(", "))
        };

        // Positive atoms with indices, core flag, and args
        writeln!(f, "Positive atoms:")?;
        for (i, name) in self.positive_atom_names.iter().enumerate() {
            let args = fmt_sig_list(
                &self.positive_atom_argument_signatures[i],
                &self.signature_to_argument_str_map,
            );
            writeln!(f, "  [{:>2}] {:<} args: {}", i, name, args)?;
        }

        // Negated atoms (if any)
        writeln!(f, "\nNegated atoms:")?;
        if self.negated_atom_names.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, name) in self.negated_atom_names.iter().enumerate() {
                let args = fmt_sig_list(
                    &self.negated_atom_argument_signatures[i],
                    &self.signature_to_argument_str_map,
                );
                writeln!(f, "  [{:>2}] {:<} args: {}", i, name, args)?;
            }
        }

        // Signature map (compact)
        let mut sig_entries: Vec<(String, String)> = self
            .signature_to_argument_str_map
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        sig_entries.sort_by(|a, b| a.0.cmp(&b.0));
        let sig_line = sig_entries
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(f, "\nSignature ↔ Var:")?;
        if sig_line.is_empty() {
            writeln!(f, "  (empty)")?;
        } else {
            writeln!(f, "  {}", sig_line)?;
        }

        // Argument presence map (sorted by var name), using '-' for None
        writeln!(f, "\nArgument presence per positive atom:")?;
        let mut vars: Vec<&String> = self.argument_presence_in_positive_atom_map.keys().collect();
        vars.sort();
        for var in vars {
            let row = self.argument_presence_in_positive_atom_map[var]
                .iter()
                .map(|opt| opt.map(|s| s.to_string()).unwrap_or_else(|| "-".into()))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  {}: [{}]", var, row)?;
        }

        // Base filters
        writeln!(f, "\nBase filters:")?;
        if self.filters.is_empty() {
            writeln!(f, "  (none)")?
        } else {
            // Leverage Filters' own Display
            for line in self.filters.to_string().lines() {
                writeln!(f, "  {}", line)?;
            }
        }

        // Comparison predicates (if any)
        writeln!(f, "\nComparison predicates:")?;
        if self.comparison_predicates.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, comp_pred) in self.comparison_predicates.iter().enumerate() {
                let vars_set = if i < self.comparison_predicates_vars_set.len() {
                    let mut vars: Vec<&String> =
                        self.comparison_predicates_vars_set[i].iter().collect();
                    vars.sort();
                    format!(
                        "vars: [{}]",
                        vars.iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "vars: []".to_string()
                };
                writeln!(f, "  [{:>2}] {} ({})", i, comp_pred, vars_set)?;
            }
        }

        Ok(())
    }
}
