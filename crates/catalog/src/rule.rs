//! Catalog of per-rule metadata and signatures for Macaron Datalog programs.
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::atom::{AtomArgumentSignature, AtomSignature};
use crate::filter::Filters;
use parser::{Atom, AtomArg, ComparisonExpr, ConstType, HeadArg, MacaronRule, Predicate};

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

    /// RHS positive atom fingerprints.
    positive_atom_fingerprints: Vec<u64>,
    /// For each RHS positive atom, its argument signatures.
    positive_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// For each RHS positive atom, its superset positive atoms.
    positive_supersets: Vec<Vec<usize>>,

    /// RHS negated atom fingerprints.
    negated_atom_fingerprints: Vec<u64>,
    /// For each RHS negated atom, its argument signatures.
    negated_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// For each RHS negated atom, its superset positive atoms.
    negated_supersets: Vec<Vec<usize>>,

    /// Local filters: variable equality constraints, constant equality constraints, placeholders.
    filters: Filters,

    /// Comparison predicates in the body.
    comparison_predicates: Vec<ComparisonExpr>,
    /// Variable sets per comparison predicate (deduplicated).
    comparison_predicates_vars_set: Vec<HashSet<String>>,
    /// For each comparison predicate, its superset positive atoms.
    comparison_supersets: Vec<Vec<usize>>,

    /// For each head argument's string form, a copy of the argument.
    head_arguments_map: HashMap<String, HeadArg>,

    /// Atom argument signatures whose variable string occurs exactly once across
    /// all predicates in the rule body (positive atoms, negated atoms, comparisons).
    /// Grouped unused arguments per atom signature for convenience in planners.
    unused_arguments_per_atom: HashMap<AtomSignature, Vec<AtomArgumentSignature>>,
}

// Getters and basic accessors
impl Catalog {
    /// The underlying rule.
    #[inline]
    pub fn rule(&self) -> &MacaronRule {
        &self.rule
    }

    /// All dependent relation fingerprints (positive and negated) in the body.
    pub fn dependent_atom_fingerprints(&self) -> HashSet<u64> {
        self.positive_atom_fingerprints
            .iter()
            .chain(self.negated_atom_fingerprints.iter())
            .cloned()
            .collect()
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

    /// Positive atom fingerprints in the rule body (RHS), in source order.
    #[inline]
    pub fn positive_atom_fingerprints(&self) -> &Vec<u64> {
        &self.positive_atom_fingerprints
    }

    /// Argument signatures for each positive atom (order matches `positive_atom_names`).
    #[inline]
    pub fn positive_atom_argument_signatures(&self) -> &Vec<Vec<AtomArgumentSignature>> {
        &self.positive_atom_argument_signatures
    }

    /// Negated atom fingerprints in the rule body (RHS), in source order.
    #[inline]
    pub fn negated_atom_fingerprints(&self) -> &Vec<u64> {
        &self.negated_atom_fingerprints
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

    /// For each positive atom, indices of positive atoms with superset var sets.
    #[inline]
    pub fn positive_supersets(&self) -> &Vec<Vec<usize>> {
        &self.positive_supersets
    }

    /// For each negative atom, indices of positive atoms with superset var sets.
    #[inline]
    pub fn negated_supersets(&self) -> &Vec<Vec<usize>> {
        &self.negated_supersets
    }

    /// For each comparison predicate, indices of positive atoms with superset var sets.
    #[inline]
    pub fn comparison_supersets(&self) -> &Vec<Vec<usize>> {
        &self.comparison_supersets
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

    /// Grouped unused arguments by atom signature.
    #[inline]
    pub fn unused_arguments_per_atom(&self) -> &HashMap<AtomSignature, Vec<AtomArgumentSignature>> {
        &self.unused_arguments_per_atom
    }

    /// Resolve an atom's context by its signature.
    #[inline]
    pub fn resolve_atom(
        &self,
        atom_signature: &AtomSignature,
    ) -> (&[AtomArgumentSignature], u64, usize) {
        let atom_id = atom_signature.rhs_id();
        if atom_signature.is_positive() {
            (
                &self.positive_atom_argument_signatures[atom_id],
                self.positive_atom_fingerprints[atom_id],
                atom_id,
            )
        } else {
            (
                &self.negated_atom_argument_signatures[atom_id],
                self.negated_atom_fingerprints[atom_id],
                atom_id,
            )
        }
    }
}

// Construction
impl Catalog {
    /// Build a Catalog from a single rule (derives signatures, filters and helper maps).
    pub fn from_rule(rule: &MacaronRule) -> Self {
        let mut catalog = Self {
            rule: rule.clone(),
            signature_to_argument_str_map: HashMap::new(),
            argument_presence_in_positive_atom_map: HashMap::new(),
            positive_atom_fingerprints: Vec::new(),
            positive_atom_argument_signatures: Vec::new(),
            positive_supersets: Vec::new(),
            negated_atom_fingerprints: Vec::new(),
            negated_atom_argument_signatures: Vec::new(),
            negated_supersets: Vec::new(),
            filters: Filters::new(HashMap::new(), HashMap::new(), HashSet::new()),
            comparison_predicates: Vec::new(),
            comparison_predicates_vars_set: Vec::new(),
            comparison_supersets: Vec::new(),
            head_arguments_map: HashMap::new(),
            unused_arguments_per_atom: HashMap::new(),
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

        // 3.5. Identify unused arguments (variables that occur exactly once across atoms/negations/comparisons)
        self.populate_unused_arguments();

        // 4. Map each head arg's string to the argument itself
        self.head_arguments_map = self
            .rule
            .head()
            .head_arguments()
            .iter()
            .map(|head_arg| (head_arg.to_string(), head_arg.clone()))
            .collect();

        // 5. Compute superset relationships for predicates vs positive atoms
        self.populate_supersets();
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
            self.positive_atom_fingerprints.push(atom.fingerprint());
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
            self.negated_atom_fingerprints.push(atom.fingerprint());
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

    /// Compute the list of unused atom arguments as those whose variable occurs in exactly one
    /// predicate across the rule body (counting per-predicate participation, not per-position).
    /// A variable used in the rule head is excluded (i.e., considered contributing to output).
    /// Only variables that correspond to atom positions are returned as signatures.
    fn populate_unused_arguments(&mut self) {
        use std::collections::HashSet;

        // Count occurrences per variable string across atoms/negated atoms/comparisons,
        // ensuring each predicate contributes at most once per variable.
        let mut var_occurrence_counts: HashMap<String, usize> = HashMap::new();

        // Positive atoms: unique vars per atom
        for sigs in &self.positive_atom_argument_signatures {
            let mut local: HashSet<String> = HashSet::new();
            for sig in sigs {
                if let Some(var) = self.signature_to_argument_str_map.get(sig) {
                    local.insert(var.clone());
                }
            }
            for var in local {
                *var_occurrence_counts.entry(var).or_insert(0) += 1;
            }
        }

        // Negated atoms: unique vars per atom
        for sigs in &self.negated_atom_argument_signatures {
            let mut local: HashSet<String> = HashSet::new();
            for sig in sigs {
                if let Some(var) = self.signature_to_argument_str_map.get(sig) {
                    local.insert(var.clone());
                }
            }
            for var in local {
                *var_occurrence_counts.entry(var).or_insert(0) += 1;
            }
        }

        // Comparisons: unique vars per comparison expr
        for comp in &self.comparison_predicates {
            let mut local: HashSet<String> = HashSet::new();
            for var in comp.vars_set() {
                local.insert(var.clone());
            }
            for var in local {
                *var_occurrence_counts.entry(var).or_insert(0) += 1;
            }
        }

        // Head variables should not be considered unused
        let mut head_vars: HashSet<String> = HashSet::new();
        for ha in self.rule.head().head_arguments() {
            for v in ha.vars() {
                head_vars.insert(v.clone());
            }
        }

        // Build grouped view by atom: collect signatures whose variable occurs in exactly
        // one predicate overall and is not a head variable.
        self.unused_arguments_per_atom.clear();
        for (sig, var) in &self.signature_to_argument_str_map {
            if matches!(var_occurrence_counts.get(var), Some(1)) && !head_vars.contains(var) {
                self.unused_arguments_per_atom
                    .entry(*sig.atom_signature())
                    .or_default()
                    .push(*sig);
            }
        }
    }

    /// Compute, for each predicate, the set of positive atom indices that are supersets of it.
    fn populate_supersets(&mut self) {
        // Build variable sets for positive atoms (excluding const/eq/placeholders)
        let pos_len = self.positive_atom_argument_signatures.len();
        let mut pos_var_sets: Vec<HashSet<String>> = Vec::with_capacity(pos_len);
        for sigs in &self.positive_atom_argument_signatures {
            let mut set: HashSet<String> = HashSet::new();
            for sig in sigs {
                if !self.is_const_or_var_eq_or_placeholder(sig) {
                    if let Some(var) = self.signature_to_argument_str_map.get(sig) {
                        set.insert(var.clone());
                    }
                }
            }
            pos_var_sets.push(set);
        }

        // For each positive atom i, collect positive atoms j where vars(i) ⊆ vars(j)
        self.positive_supersets = Vec::with_capacity(pos_len);
        for i in 0..pos_len {
            let mut supers: Vec<usize> = Vec::new();
            for j in 0..pos_len {
                if i == j {
                    continue; // exclude itself
                }
                if pos_var_sets[i].is_subset(&pos_var_sets[j])
                    && pos_var_sets[i].len() < pos_var_sets[j].len()
                {
                    supers.push(j);
                }
            }
            self.positive_supersets.push(supers);
        }

        // For each negated atom, compute its var set and find positive supersets
        let neg_len = self.negated_atom_argument_signatures.len();
        self.negated_supersets = Vec::with_capacity(neg_len);
        for sigs in &self.negated_atom_argument_signatures {
            let mut set: HashSet<String> = HashSet::new();
            for sig in sigs {
                if !self.is_const_or_var_eq_or_placeholder(sig) {
                    if let Some(var) = self.signature_to_argument_str_map.get(sig) {
                        set.insert(var.clone());
                    }
                }
            }
            let mut supers: Vec<usize> = Vec::new();
            for (j, pos_set) in pos_var_sets.iter().enumerate() {
                if set.is_subset(pos_set) && set.len() < pos_set.len() {
                    supers.push(j);
                }
            }
            self.negated_supersets.push(supers);
        }

        // For each comparison predicate, use its variable set and find positive supersets
        let comp_len = self.comparison_predicates.len();
        self.comparison_supersets = Vec::with_capacity(comp_len);
        for i in 0..comp_len {
            let set = if i < self.comparison_predicates_vars_set.len() {
                self.comparison_predicates_vars_set[i].clone()
            } else {
                HashSet::new()
            };
            let mut supers: Vec<usize> = Vec::new();
            for (j, pos_set) in pos_var_sets.iter().enumerate() {
                if set.is_subset(pos_set) && set.len() < pos_set.len() {
                    supers.push(j);
                }
            }
            self.comparison_supersets.push(supers);
        }
    }

    /// Clear all metadata (but keep the rule).
    fn clear(&mut self) {
        self.signature_to_argument_str_map.clear();
        self.argument_presence_in_positive_atom_map.clear();
        self.positive_atom_fingerprints.clear();
        self.positive_atom_argument_signatures.clear();
        self.positive_supersets.clear();
        self.negated_atom_fingerprints.clear();
        self.negated_atom_argument_signatures.clear();
        self.negated_supersets.clear();
        self.filters = Filters::new(HashMap::new(), HashMap::new(), HashSet::new());
        self.comparison_predicates.clear();
        self.comparison_predicates_vars_set.clear();
        self.comparison_supersets.clear();
        self.head_arguments_map.clear();
        self.unused_arguments_per_atom.clear();
    }
}

/// Modification.
impl Catalog {
    /// Projection modify: delete specified arguments from an atom and update its name and fingerprint.
    pub fn projection_modify(
        &mut self,
        atom_signature: AtomSignature,
        arguments_to_delete: Vec<AtomArgumentSignature>,
        new_name: String,
        new_fingerprint: u64,
    ) {
        // We allow arguments_to_delete to be empty for cases like key-value projection
        // where we're just renaming/re-signaturing without deleting arguments

        // If arguments provided, validate they all belong to the specified atom
        for arg_sig in &arguments_to_delete {
            if *arg_sig.atom_signature() != atom_signature {
                panic!("Catalog error: All argument signatures must belong to the specified atom");
            }
        }

        let rhs_index = self
            .rhs_index_from_signature(atom_signature)
            .unwrap_or_else(|| {
                panic!(
                    "Catalog error: Atom not found for signature {}",
                    atom_signature
                )
            });

        // Collect argument IDs to delete and sort them in descending order
        // (so we can remove from highest index to lowest to avoid index shifting)
        let mut arg_ids_to_delete: Vec<usize> = arguments_to_delete
            .iter()
            .map(|sig| sig.argument_id())
            .collect();
        arg_ids_to_delete.sort_unstable();
        arg_ids_to_delete.reverse();

        // Prepare a new predicate with the requested arguments removed and updated name/signature
        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtomPredicate(atom) | Predicate::NegatedAtomPredicate(atom) => {
                // Validate all argument IDs are in bounds
                for &arg_id in &arg_ids_to_delete {
                    if arg_id >= atom.arity() {
                        panic!(
                            "Catalog error: Argument id {} out of bounds for atom '{}' with arity {}",
                            arg_id,
                            atom.name(),
                            atom.arity()
                        );
                    }
                }

                let mut new_args: Vec<AtomArg> = atom.arguments().to_vec();
                // Remove arguments in descending order to avoid index issues
                for &arg_id in &arg_ids_to_delete {
                    new_args.remove(arg_id);
                }

                let new_atom = Atom::new(&new_name, new_args, new_fingerprint);

                // Reconstruct the appropriate predicate type
                match &self.rule.rhs()[rhs_index] {
                    Predicate::PositiveAtomPredicate(_) => {
                        Predicate::PositiveAtomPredicate(new_atom)
                    }
                    Predicate::NegatedAtomPredicate(_) => Predicate::NegatedAtomPredicate(new_atom),
                    _ => unreachable!(),
                }
            }
            other => {
                panic!(
                    "Catalog error: Target predicate at rhs index {} is not an atom: {}",
                    rhs_index, other
                )
            }
        };

        // Rebuild RHS with the updated atom
        let mut new_rhs = self.rule.rhs().to_vec();
        new_rhs[rhs_index] = new_atom;

        let new_rule = MacaronRule::new(self.rule.head().clone(), new_rhs, self.rule.is_planning());
        self.update_rule(&new_rule);
    }

    /// Join modify (multi-right): remove the left atom and all right atoms, then create
    /// one or more new joined atoms at the end of RHS. This supports semijoins where a
    /// single left can semijoin with multiple rights.
    pub fn join_modify(
        &mut self,
        left_atom_signature: AtomSignature,
        right_atom_signatures: Vec<AtomSignature>,
        new_arguments_list: Vec<Vec<AtomArgumentSignature>>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) {
        if right_atom_signatures.len() != new_arguments_list.len()
            || right_atom_signatures.len() != new_names.len()
            || right_atom_signatures.len() != new_fingerprints.len()
        {
            panic!(
                "Catalog error: right_atom_signatures, new_arguments_list, new_names, and new_fingerprints must have the same length"
            );
        }

        // Find RHS indices
        let left_rhs_index = self
            .rhs_index_from_signature(left_atom_signature)
            .unwrap_or_else(|| {
                panic!(
                    "Catalog error: Left atom not found for signature {}",
                    left_atom_signature
                )
            });

        // Validate left is an atom
        match &self.rule.rhs()[left_rhs_index] {
            Predicate::PositiveAtomPredicate(_) | Predicate::NegatedAtomPredicate(_) => {}
            _ => panic!("Catalog error: Left predicate must be an atom"),
        }

        let mut right_indices: Vec<usize> = Vec::with_capacity(right_atom_signatures.len());
        for sig in &right_atom_signatures {
            let idx = self.rhs_index_from_signature(*sig).unwrap_or_else(|| {
                panic!("Catalog error: Right atom not found for signature {}", sig)
            });
            // Validate atom type
            match &self.rule.rhs()[idx] {
                Predicate::PositiveAtomPredicate(_) | Predicate::NegatedAtomPredicate(_) => {}
                _ => panic!("Catalog error: Right predicate must be an atom"),
            }
            right_indices.push(idx);
        }

        // Build new joined atoms, one per right
        let mut new_joined_atoms: Vec<Predicate> = Vec::with_capacity(right_indices.len());
        for i in 0..right_indices.len() {
            let new_args_signatures = &new_arguments_list[i];
            let mut new_atom_args: Vec<AtomArg> = Vec::with_capacity(new_args_signatures.len());
            for arg_signature in new_args_signatures {
                if let Some(arg_str) = self.signature_to_argument_str_map.get(arg_signature) {
                    new_atom_args.push(AtomArg::Var(arg_str.clone()));
                } else {
                    panic!(
                        "Catalog error: Argument signature {:?} not found in signature map",
                        arg_signature
                    );
                }
            }
            let name = &new_names[i];
            let fingerprint = new_fingerprints[i];
            let new_atom =
                Predicate::PositiveAtomPredicate(Atom::new(name, new_atom_args, fingerprint));
            new_joined_atoms.push(new_atom);
        }

        // Build new RHS: remove left and all rights, then append the new joined atoms
        let mut new_rhs = self.rule.rhs().to_vec();
        let mut indices_to_remove = right_indices;
        indices_to_remove.push(left_rhs_index);
        indices_to_remove.sort_unstable();
        indices_to_remove.drain(..).rev().for_each(|idx| {
            new_rhs.remove(idx);
        });
        new_rhs.extend(new_joined_atoms);

        let new_rule = MacaronRule::new(self.rule.head().clone(), new_rhs, self.rule.is_planning());
        self.update_rule(&new_rule);
    }

    /// Map an atom signature (polarity + position among atoms of that polarity)
    /// back to the global RHS index in the rule's predicate vector.
    fn rhs_index_from_signature(&self, sig: AtomSignature) -> Option<usize> {
        self.rule
            .rhs()
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                matches!(
                    (p, sig.is_positive()),
                    (Predicate::PositiveAtomPredicate(_), true)
                        | (Predicate::NegatedAtomPredicate(_), false)
                )
            })
            .nth(sig.rhs_id())
            .map(|(i, _)| i)
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
        for (i, fingerprint) in self.positive_atom_fingerprints.iter().enumerate() {
            let args = fmt_sig_list(
                &self.positive_atom_argument_signatures[i],
                &self.signature_to_argument_str_map,
            );
            writeln!(f, "  [{:>2}] 0x{:x} args: {}", i, fingerprint, args)?;
        }

        // Negated atoms (if any)
        writeln!(f, "\nNegated atoms:")?;
        if self.negated_atom_fingerprints.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, fingerprint) in self.negated_atom_fingerprints.iter().enumerate() {
                let args = fmt_sig_list(
                    &self.negated_atom_argument_signatures[i],
                    &self.signature_to_argument_str_map,
                );
                writeln!(f, "  [{:>2}] 0x{:x} args: {}", i, fingerprint, args)?;
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

        // Superset relationships
        writeln!(f, "\nSupersets (per predicate → positive atom ids):")?;
        if self.positive_supersets.is_empty() {
            writeln!(f, "  positives: (none)")?;
        } else {
            writeln!(f, "  positives:")?;
            for (i, supers) in self.positive_supersets.iter().enumerate() {
                writeln!(
                    f,
                    "    [{}] -> [{}]",
                    i,
                    supers
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
        }
        if self.negated_supersets.is_empty() {
            writeln!(f, "  negated: (none)")?;
        } else {
            writeln!(f, "  negated:")?;
            for (i, supers) in self.negated_supersets.iter().enumerate() {
                writeln!(
                    f,
                    "    [{}] -> [{}]",
                    i,
                    supers
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
        }
        if self.comparison_supersets.is_empty() {
            writeln!(f, "  comparisons: (none)")?;
        } else {
            writeln!(f, "  comparisons:")?;
            for (i, supers) in self.comparison_supersets.iter().enumerate() {
                writeln!(
                    f,
                    "    [{}] -> [{}]",
                    i,
                    supers
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
            }
        }

        Ok(())
    }
}
