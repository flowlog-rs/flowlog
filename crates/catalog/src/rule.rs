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
    argument_presence_map: HashMap<String, Vec<Option<AtomArgumentSignature>>>,

    /// RHS positive atom names.
    positive_atom_names: Vec<String>,
    /// For each RHS positive atom, its argument signatures.
    positive_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Bitmap indicating if an atom is a core atom (not a subset of another's variables).
    is_core_atom_bitmap: Vec<bool>,

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

    /// Subatoms w.r.t. the given signature arguments (non-core atoms whose vars are a subset).
    pub fn sub_atoms(&self, signature_arguments: &[AtomArgumentSignature]) -> Vec<AtomSignature> {
        let signature_arguments_str_set = self
            .signature_to_argument_strs(signature_arguments)
            .into_iter()
            .collect::<HashSet<String>>();
        self.is_core_atom_bitmap()
            .iter()
            .enumerate()
            .filter_map(|(i, &is_core)| {
                if !is_core {
                    let atom_argument_strs_set = self
                        .signature_to_argument_strs(&self.positive_atom_argument_signatures()[i])
                        .into_iter()
                        .collect::<HashSet<String>>();
                    if atom_argument_strs_set.is_subset(&signature_arguments_str_set) {
                        Some(AtomSignature::new(true, i))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<AtomSignature>>()
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
    pub fn argument_presence_map(&self, argument_str: &str) -> &Vec<Option<AtomArgumentSignature>> {
        &self.argument_presence_map[argument_str]
    }

    /// Positive atom names in the rule body (RHS), in source order.
    pub fn positive_atom_names(&self) -> &Vec<String> {
        &self.positive_atom_names
    }

    /// Argument signatures for each positive atom (order matches `positive_atom_names`).
    pub fn positive_atom_argument_signatures(&self) -> &Vec<Vec<AtomArgumentSignature>> {
        &self.positive_atom_argument_signatures
    }

    /// Bitmap indicating which positive atoms are core (not strictly contained in another's vars).
    pub fn is_core_atom_bitmap(&self) -> &Vec<bool> {
        &self.is_core_atom_bitmap
    }

    /// Negated atom names in the rule body (RHS), in source order.
    pub fn negated_atom_names(&self) -> &Vec<String> {
        &self.negated_atom_names
    }

    /// Argument signatures for each negated atom (order matches `negated_atom_names`).
    pub fn negated_atom_argument_signatures(&self) -> &Vec<Vec<AtomArgumentSignature>> {
        &self.negated_atom_argument_signatures
    }

    /// Base constraint filters (var==var, var==const, placeholders).
    pub fn filters(&self) -> &Filters {
        &self.filters
    }

    /// Name of the head relation.
    pub fn head_name(&self) -> &str {
        self.rule.head().name()
    }

    /// Structured head arguments in order.
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
    pub fn head_arguments_map(&self) -> &HashMap<String, HeadArg> {
        &self.head_arguments_map
    }

    /// Partition comparison predicates into join/left/right (left and right may overlap).
    pub fn partition_comparison_predicates(
        &self,
        left_vars_set: &HashSet<&String>,
        right_vars_set: &HashSet<&String>,
        active_comparison_predicates: &[usize],
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        // Every comparison predicate must be a subset of the union of left and right.
        // If subset of left ⇒ left; subset of right ⇒ right; otherwise ⇒ join.
        let mut join_predicates = Vec::new();
        let mut left_predicates = Vec::new();
        let mut right_predicates = Vec::new();

        for &i in active_comparison_predicates {
            let comparison_predicate = &self.comparison_predicates[i];
            let vars_set = comparison_predicate.vars_set();
            let union_vars_set: HashSet<&String> =
                left_vars_set.union(right_vars_set).cloned().collect();

            assert!(
                vars_set.is_subset(&union_vars_set),
                "comp vars {:?} not a subset of the subtree vars {:?}",
                vars_set,
                union_vars_set
            );
            if vars_set.is_subset(left_vars_set) {
                left_predicates.push(i);
            }

            if vars_set.is_subset(right_vars_set) {
                right_predicates.push(i);
            }

            if !vars_set.is_subset(left_vars_set) && !vars_set.is_subset(right_vars_set) {
                join_predicates.push(i);
            }
        }

        (join_predicates, left_predicates, right_predicates)
    }

    /// Isolate negated atoms that span both sides but are contained in their union; mark them inactive.
    pub fn attach_negated_atoms_on_joins(
        &self,
        left_vars_set: &HashSet<&String>,
        right_vars_set: &HashSet<&String>,
        is_active_negation_bitmap: &mut [bool],
    ) -> Vec<AtomSignature> {
        let mut isolated_negated_atoms = Vec::new();

        for (i, negated_atom_argument_signatures) in
            self.negated_atom_argument_signatures.iter().enumerate()
        {
            // if it is not active, skip
            if !is_active_negation_bitmap[i] {
                continue;
            }

            let negated_atom_argument_strs =
                self.signature_to_argument_strs(negated_atom_argument_signatures);
            let negated_atom_argument_strs_set = negated_atom_argument_strs
                .iter()
                .collect::<HashSet<&String>>();

            if !negated_atom_argument_strs_set.is_subset(left_vars_set)
                && !negated_atom_argument_strs_set.is_subset(right_vars_set)
                && negated_atom_argument_strs_set
                    .is_subset(&left_vars_set.union(right_vars_set).cloned().collect())
            {
                // if the negated atom (1) is not a subset of both left and right, and (2) is a subset of the union of left and right, it should be attached on the join
                isolated_negated_atoms.push(AtomSignature::new(false, i));
                is_active_negation_bitmap[i] = false;
            }
        }

        isolated_negated_atoms
    }
}

impl Catalog {
    /// Build a Catalog from a single rule (derives signatures, filters and helper maps).
    pub fn from_strata(rule: &MacaronRule) -> Self {
        let (
            signature_to_argument_str_map,
            positive_atom_names,
            positive_atom_argument_signatures, // all non-repeating variables
            negated_atom_names,
            negated_atom_argument_signatures,
            filters,
            comparison_predicates,
        ) = Self::populate_argument_signatures(rule);

        let argument_presence_map = Self::populate_argument_presence_map(
            &signature_to_argument_str_map,
            &positive_atom_argument_signatures,
            &filters,
        );
        let is_core_atom_bitmap = Self::populate_is_core_atom_bitmap(
            &signature_to_argument_str_map,
            &positive_atom_argument_signatures,
        );

        let comparison_predicates_vars_set =
            Self::populate_comparison_predicates_vars_set(&comparison_predicates);

        // Map each head arg's string to the argument itself, e.g., "x" -> Var(x), "x + y" -> Arith(x + y)
        let head_arguments_map: HashMap<String, HeadArg> = rule
            .head()
            .head_arguments()
            .iter()
            .map(|head_arg| (head_arg.to_string(), head_arg.clone()))
            .collect();

        Self {
            rule: rule.clone(),
            signature_to_argument_str_map,
            argument_presence_map,
            positive_atom_names,
            positive_atom_argument_signatures,
            is_core_atom_bitmap,
            negated_atom_names,
            negated_atom_argument_signatures,
            filters,
            comparison_predicates,
            comparison_predicates_vars_set,
            head_arguments_map,
        }
    }

    /// Compute the deduplicated variable set for each comparison predicate.
    fn populate_comparison_predicates_vars_set(
        comparison_predicates: &[ComparisonExpr],
    ) -> Vec<HashSet<String>> {
        comparison_predicates
            .iter()
            .map(|comparison_expr| {
                comparison_expr
                    .vars_set()
                    .into_iter()
                    .cloned()
                    .collect::<HashSet<String>>()
            })
            .collect()
    }

    #[allow(clippy::type_complexity)]
    fn populate_argument_signatures(
        r: &MacaronRule,
    ) -> (
        HashMap<AtomArgumentSignature, String>,
        Vec<String>,
        Vec<Vec<AtomArgumentSignature>>,
        Vec<String>,
        Vec<Vec<AtomArgumentSignature>>,
        Filters,
        Vec<ComparisonExpr>,
    ) {
        // Track variables seen in positive atoms; only these are safe in negations.
        let mut is_safe_set = HashSet::new();
        // Map each rule atom signature to the variable string.
        let mut signature_to_argument_str_map = HashMap::new();

        let mut atom_names = Vec::new();
        // For each positive atom, the vector of its argument signatures.
        let mut atom_argument_signatures = Vec::new();

        let mut negated_atom_names = Vec::new();
        let mut negated_atom_argument_signatures = Vec::new();

        // Local filters for the rule: x==y, x==1, x==_.
        let mut local_var_eq_map = HashMap::new();
        // Map each var to its first occurrence to derive var==var constraints.
        let mut local_var_first_occurrence_map: HashMap<String, AtomArgumentSignature> =
            HashMap::new();
        let mut local_const_map = HashMap::new();
        let mut local_placeholder_set = HashSet::new();

        let (positive_atoms, negated_atoms, comparison_predicates): (Vec<_>, Vec<_>, Vec<_>) =
            r.rhs().iter().fold(
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
            atom_names.push(atom.name().to_owned());
            let mut atom_signatures = Vec::new();
            for (argument_id, argument) in atom.arguments().iter().enumerate() {
                let rule_argument_signature =
                    AtomArgumentSignature::new(AtomSignature::new(true, rhs_id), argument_id);
                atom_signatures.push(rule_argument_signature);

                match argument {
                    AtomArg::Var(var) => {
                        is_safe_set.insert(var);
                        signature_to_argument_str_map
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
            atom_argument_signatures.push(atom_signatures);
            local_var_first_occurrence_map.clear();
        }

        // (ii) populate the signatures of negated atoms
        for (neg_rhs_id, atom) in negated_atoms.iter().enumerate() {
            negated_atom_names.push(atom.name().to_owned());
            let mut negated_atom_signatures = Vec::new();
            for (argument_id, argument) in atom.arguments().iter().enumerate() {
                let rule_argument_signature =
                    AtomArgumentSignature::new(AtomSignature::new(false, neg_rhs_id), argument_id);
                negated_atom_signatures.push(rule_argument_signature);

                match argument {
                    AtomArg::Var(var) => {
                        if is_safe_set.contains(var) {
                            signature_to_argument_str_map
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
                            panic!("unsafe var detected at negation !{} of rule {}", atom, r);
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
            negated_atom_argument_signatures.push(negated_atom_signatures);
            local_var_first_occurrence_map.clear();
        }

        (
            signature_to_argument_str_map,
            atom_names,
            atom_argument_signatures,
            negated_atom_names,
            negated_atom_argument_signatures,
            Filters::new(local_var_eq_map, local_const_map, local_placeholder_set),
            comparison_predicates,
        )
    }

    /// Identify core atoms: positive atoms not strictly contained in another's variable set.
    fn populate_is_core_atom_bitmap(
        signature_to_argument_str_map: &HashMap<AtomArgumentSignature, String>,
        atom_argument_signatures: &[Vec<AtomArgumentSignature>], // only positive atoms
    ) -> Vec<bool> {
        let mut is_core_atom_bitmap = vec![true; atom_argument_signatures.len()];

        let core_atom_argument_strs_set = atom_argument_signatures
            .iter()
            .map(|atom_argument_signatures| {
                atom_argument_signatures
                    .iter()
                    .filter_map(|signature| signature_to_argument_str_map.get(signature).cloned())
                    .collect::<HashSet<String>>()
            })
            .collect::<Vec<HashSet<String>>>();

        for (i, core_atom_argument_strs) in core_atom_argument_strs_set.iter().enumerate() {
            for (j, other_core_atom_argument_strs) in core_atom_argument_strs_set.iter().enumerate()
            {
                if i != j && core_atom_argument_strs.is_subset(other_core_atom_argument_strs) {
                    // if they are strictly equal, the larger index is not a core atom
                    if core_atom_argument_strs.len() < other_core_atom_argument_strs.len() {
                        // if other_core_atom_argument_strs is a strict superset
                        is_core_atom_bitmap[i] = false;
                    } else {
                        // if they are identical, the larger one is not a core atom
                        let larger = if i > j { i } else { j };
                        is_core_atom_bitmap[larger] = false;
                    }
                }
            }
        }
        is_core_atom_bitmap
    }

    /// Map each variable string to its first presence per positive atom (None if absent).
    fn populate_argument_presence_map(
        signature_to_argument_str_map: &HashMap<AtomArgumentSignature, String>,
        atom_argument_signatures: &[Vec<AtomArgumentSignature>], /* only positive atoms */
        filters: &Filters,
    ) -> HashMap<String, Vec<Option<AtomArgumentSignature>>> {
        let mut argument_presence_map: HashMap<String, Vec<Option<AtomArgumentSignature>>> =
            HashMap::new();

        for (rhs_id, argument_signatures) in atom_argument_signatures.iter().enumerate() {
            for argument_signature in argument_signatures {
                // skip if it is a filter
                if filters.is_const_or_var_eq_or_placeholder(argument_signature) {
                    continue;
                }

                if let Some(variable) = signature_to_argument_str_map.get(argument_signature) {
                    // Initialize the entry with a vector of None.
                    let entry = argument_presence_map
                        .entry(variable.clone())
                        .or_insert(vec![None; atom_argument_signatures.len()]);

                    // Record the first presence for this atom.
                    if entry[rhs_id].is_none() {
                        entry[rhs_id] = Some(*argument_signature);
                    }
                } else {
                    panic!("populate_argument_presence_map: argument signature {:?} absent from the signature map", argument_signature);
                }
            }
        }

        argument_presence_map
    }
}

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Catalog for Rule: {}", self.rule())?;

        // Positive Atom Names
        writeln!(f, "\nPositive Atom Names (Core/Non-Core):")?;
        for (i, atom_name) in self.positive_atom_names.iter().enumerate() {
            let status = if self.is_core_atom_bitmap[i] {
                "Core"
            } else {
                "Non-Core"
            };
            writeln!(f, "  - {} ({})", atom_name, status)?;
        }

        // Positive Atom Argument Signatures
        writeln!(f, "\nPositive Atom Argument Signatures:")?;
        for atom_signatures in &self.positive_atom_argument_signatures {
            writeln!(f, "  Atom:")?;
            for signature in atom_signatures {
                writeln!(f, "    Argument Signature: {}", signature)?;
            }
        }

        // Negated Atom Names
        writeln!(f, "\nNegated Atom Names:")?;
        for negated_atom_name in &self.negated_atom_names {
            writeln!(f, "  - {}", negated_atom_name)?;
        }

        // Negated Atom Argument Signatures
        writeln!(f, "\nNegated Atom Argument Signatures:")?;
        for negated_signatures in &self.negated_atom_argument_signatures {
            writeln!(f, "  Negated Atom:")?;
            for signature in negated_signatures {
                writeln!(f, "    Argument Signature: {:?}", signature)?;
            }
        }

        // Signature to Argument String Map
        writeln!(f, "\nSignature to Argument String Map:")?;
        for (signature, arg_str) in &self.signature_to_argument_str_map {
            writeln!(f, "  {} -> {}", signature, arg_str)?;
        }

        // Argument Presence Map
        writeln!(f, "\nArgument Presence Map:")?;
        for (arg_str, presence_vec) in &self.argument_presence_map {
            write!(f, "  Argument {}: [", arg_str)?;
            let mut first = true;
            for presence in presence_vec {
                if !first {
                    write!(f, ", ")?;
                }
                match presence {
                    Some(signature) => write!(f, "{}", signature)?,
                    None => write!(f, "None")?,
                }
                first = false;
            }
            writeln!(f, "]")?;
        }

        // Base Filters
        writeln!(f, "\nBase Filters:")?;
        writeln!(f, "  {}", self.filters)?;

        Ok(())
    }
}
