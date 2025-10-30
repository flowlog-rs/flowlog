//! Catalog of per-rule metadata and signatures for Macaron Datalog programs.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::atom::{AtomArgumentSignature, AtomSignature};
use crate::filter::Filters;
use crate::ComparisonExprPos;
use parser::{ComparisonExpr, HeadArg, MacaronRule};

// Implementation modules
mod modify;
mod populate;

/// Per-rule catalog with precomputed metadata.
///
/// This structure maintains signatures, variable mappings, filters, and superset relationships
/// for efficient rule analysis and transformation planning.
#[derive(Debug)]
pub struct Catalog {
    /// The original rule.
    rule: MacaronRule,

    /// Reverse map from argument signatures to their variable strings.
    signature_to_argument_str_map: HashMap<AtomArgumentSignature, String>,
    /// For each variable string, the first presence (if any) in every positive atom.
    /// Example: for `tc(x, z) :- arc(x, y), tc(y, z)` → `{ x: [Some(0.0), None], y: [Some(0.1), Some(1.0)], z: [None, Some(1.1)] }`.
    argument_presence_in_positive_atom_map: HashMap<String, Vec<Option<AtomArgumentSignature>>>,
    /// Original rule atom fingerprints
    original_atom_fingerprints: HashSet<u64>,

    /// RHS positive atom fingerprints (in source order).
    positive_atom_fingerprints: Vec<u64>,
    /// For each RHS positive atom, its argument signatures.
    positive_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Variable string sets per positive atom (deduplicated).
    positive_atom_argument_vars_str_sets: Vec<HashSet<String>>,
    /// For each RHS positive atom, indices of positive atoms that are supersets.
    positive_supersets: Vec<Vec<usize>>,
    /// For each RHS positive atom, its RHS id (index).
    positive_atom_rhs_ids: Vec<usize>,

    /// RHS negated atom fingerprints (in source order).
    negated_atom_fingerprints: Vec<u64>,
    /// For each RHS negated atom, its argument signatures.
    negated_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Variable string sets per negated atom (deduplicated).
    negated_atom_argument_vars_str_sets: Vec<HashSet<String>>,
    /// For each RHS negated atom, indices of positive atoms that are supersets.
    negated_supersets: Vec<Vec<usize>>,
    /// For each RHS negated atom, its RHS id (index).
    negated_atom_rhs_ids: Vec<usize>,

    /// Local filters: variable equality constraints, constant equality constraints, placeholders.
    filters: Filters,

    /// Comparison predicates in the rule body.
    comparison_predicates: Vec<ComparisonExpr>,
    /// Variable string sets per comparison predicate (deduplicated).
    comparison_predicates_vars_str_set: Vec<HashSet<String>>,
    /// For each comparison predicate, indices of positive atoms that are supersets.
    comparison_supersets: Vec<Vec<usize>>,

    /// Mapping from head argument string representation to structured form.
    head_arguments_map: HashMap<String, HeadArg>,
    /// Head IDB fingerprint.
    head_idb_fingerprint: u64,

    /// Atom argument signatures whose variable string occurs exactly once across
    /// all predicates in the rule body (positive atoms, negated atoms, comparisons).
    /// Grouped by atom signature for convenience in planners.
    unused_arguments_per_atom: HashMap<AtomSignature, Vec<AtomArgumentSignature>>,
}

// Construction and lifecycle management
impl Catalog {
    /// Build a Catalog from a single rule (derives signatures, filters and helper maps).
    pub fn from_rule(rule: &MacaronRule) -> Self {
        let mut catalog = Self {
            rule: rule.clone(),
            signature_to_argument_str_map: HashMap::new(),
            argument_presence_in_positive_atom_map: HashMap::new(),
            original_atom_fingerprints: HashSet::new(),
            positive_atom_fingerprints: Vec::new(),
            positive_atom_argument_signatures: Vec::new(),
            positive_atom_argument_vars_str_sets: Vec::new(),
            positive_supersets: Vec::new(),
            positive_atom_rhs_ids: Vec::new(),
            negated_atom_fingerprints: Vec::new(),
            negated_atom_argument_signatures: Vec::new(),
            negated_atom_argument_vars_str_sets: Vec::new(),
            negated_supersets: Vec::new(),
            negated_atom_rhs_ids: Vec::new(),
            filters: Filters::new(HashMap::new(), HashMap::new(), HashSet::new()),
            comparison_predicates: Vec::new(),
            comparison_predicates_vars_str_set: Vec::new(),
            comparison_supersets: Vec::new(),
            head_arguments_map: HashMap::new(),
            head_idb_fingerprint: {
                // Generate head IDB fingerprint following the same pattern as atom fingerprints
                let mut hasher = DefaultHasher::new();
                "atom".hash(&mut hasher);
                rule.head().name().hash(&mut hasher);
                hasher.finish()
            },
            unused_arguments_per_atom: HashMap::new(),
        };
        catalog.populate_all_metadata();

        // Store original atom fingerprints for reference
        for predicate in rule.rhs() {
            match predicate {
                parser::Predicate::PositiveAtomPredicate(atom)
                | parser::Predicate::NegatedAtomPredicate(atom) => {
                    catalog
                        .original_atom_fingerprints
                        .insert(atom.fingerprint());
                }
                _ => {}
            }
        }

        catalog
    }

    /// Clear all metadata while keeping the rule unchanged.
    /// This method should be called each time before recomputing metadata.
    pub(crate) fn clear(&mut self) {
        self.signature_to_argument_str_map.clear();
        self.argument_presence_in_positive_atom_map.clear();
        self.positive_atom_fingerprints.clear();
        self.positive_atom_argument_signatures.clear();
        self.positive_atom_argument_vars_str_sets.clear();
        self.positive_supersets.clear();
        self.positive_atom_rhs_ids.clear();
        self.negated_atom_fingerprints.clear();
        self.negated_atom_argument_signatures.clear();
        self.negated_atom_argument_vars_str_sets.clear();
        self.negated_supersets.clear();
        self.negated_atom_rhs_ids.clear();
        self.filters = Filters::new(HashMap::new(), HashMap::new(), HashSet::new());
        self.comparison_predicates.clear();
        self.comparison_predicates_vars_str_set.clear();
        self.comparison_supersets.clear();
        self.head_arguments_map.clear();
        self.unused_arguments_per_atom.clear();
    }

    /// Update the catalog with a new rule, recomputing corresponding metadata.
    pub fn update_rule(&mut self, rule: &MacaronRule) {
        self.rule = rule.clone();
        self.clear();
        self.populate_all_metadata();
    }
}

// Public API: Getters and query methods
impl Catalog {
    /// Get the underlying rule.
    #[inline]
    pub fn rule(&self) -> &MacaronRule {
        &self.rule
    }

    /// Get the argument string for a specific signature.
    #[inline]
    pub fn signature_to_argument_str(&self, sig: &AtomArgumentSignature) -> &String {
        self.signature_to_argument_str_map.get(sig).unwrap()
    }

    /// Get the core atom count for rules with no supersets or filters.
    ///
    /// # Panics
    /// Panics if the rule has any supersets or filters, as this method
    /// is only valid for "core" rules without optimization opportunities.
    #[inline]
    pub fn core_atom_number(&self) -> usize {
        assert!(
            self.positive_supersets.iter().all(|s| s.is_empty()),
            "Rule has positive supersets - not a core rule"
        );
        assert!(
            self.negated_supersets.iter().all(|s| s.is_empty()),
            "Rule has negated supersets - not a core rule"
        );
        assert!(
            self.comparison_supersets.iter().all(|s| s.is_empty()),
            "Rule has comparison supersets - not a core rule"
        );
        assert!(
            self.filters.is_empty(),
            "Rule has filters - not a core rule"
        );
        self.positive_atom_fingerprints.len()
    }

    // Get original atom fingerprints
    #[inline]
    pub fn original_atom_fingerprints(&self) -> &HashSet<u64> {
        &self.original_atom_fingerprints
    }

    // === Positive Atoms ===

    /// Get the fingerprint of a positive atom by its index.
    #[inline]
    pub fn positive_atom_fingerprint(&self, index: usize) -> u64 {
        self.positive_atom_fingerprints[index]
    }

    /// Get the argument signatures for a positive atom by its index.
    #[inline]
    pub fn positive_atom_argument_signature(&self, index: usize) -> &Vec<AtomArgumentSignature> {
        &self.positive_atom_argument_signatures[index]
    }

    /// For each positive atom, get indices of positive atoms with superset variable sets.
    #[inline]
    pub fn positive_supersets(&self) -> &Vec<Vec<usize>> {
        &self.positive_supersets
    }

    /// Get the RHS id (index) of a positive atom by its index.
    #[inline]
    pub fn positive_atom_rhs_id(&self, index: usize) -> usize {
        self.positive_atom_rhs_ids[index]
    }

    // === Negated Atoms ===

    /// Get the fingerprint of a negated atom by its index.
    #[inline]
    pub fn negated_atom_fingerprint(&self, index: usize) -> u64 {
        self.negated_atom_fingerprints[index]
    }

    /// Get the argument signatures for a negated atom by its index.
    #[inline]
    pub fn negated_atom_argument_signature(&self, index: usize) -> &Vec<AtomArgumentSignature> {
        &self.negated_atom_argument_signatures[index]
    }

    /// For each negated atom, get indices of positive atoms with superset variable sets.
    #[inline]
    pub fn negated_supersets(&self) -> &Vec<Vec<usize>> {
        &self.negated_supersets
    }

    /// Get the RHS id (index) of a negated atom by its index.
    #[inline]
    pub fn negated_atom_rhs_id(&self, index: usize) -> usize {
        self.negated_atom_rhs_ids[index]
    }

    // === Atom Resolution ===

    /// Resolve an atom signature to its argument signatures, fingerprint, and RHS ID.
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

    /// Finds the global RHS index of an atom given its signature.
    pub fn rhs_index_from_signature(&self, sig: AtomSignature) -> usize {
        if sig.is_positive() {
            self.positive_atom_rhs_ids[sig.rhs_id()]
        } else {
            self.negated_atom_rhs_ids[sig.rhs_id()]
        }
    }

    // === Comparison Predicates ===

    /// Get a comparison predicate by its index.
    #[inline]
    pub fn comparison_predicate(&self, index: usize) -> ComparisonExpr {
        self.comparison_predicates[index].clone()
    }

    /// For each comparison predicate, get indices of positive atoms with superset variable sets.
    #[inline]
    pub fn comparison_supersets(&self) -> &Vec<Vec<usize>> {
        &self.comparison_supersets
    }

    /// Resolve a comparison expression with argument positions for a given positive atom and comparison predicate.
    pub fn resolve_comparison_predicates(
        &self,
        pos_atom_id: usize,
        comp_id: usize,
    ) -> ComparisonExprPos {
        let comp_exprs = &self.comparison_predicates[comp_id];
        let left_var_signatures = comp_exprs
            .left()
            .vars()
            .iter()
            .map(|&v| {
                self.argument_presence_in_positive_atom_map[v][pos_atom_id]
                    .expect("variable in comparison lhs not found in positive atom")
            })
            .collect::<Vec<_>>();
        let right_var_signatures = comp_exprs
            .right()
            .vars()
            .iter()
            .map(|&v| {
                self.argument_presence_in_positive_atom_map[v][pos_atom_id]
                    .expect("variable in comparison rhs not found in positive atom")
            })
            .collect::<Vec<_>>();
        ComparisonExprPos::from_comparison_expr(
            comp_exprs,
            &left_var_signatures,
            &right_var_signatures,
        )
    }

    // === Filters ===

    /// Get the local atom filters (var==var, var==const, placeholders).
    #[inline]
    pub fn filters(&self) -> &Filters {
        &self.filters
    }

    // === Head Information ===

    /// Get the head predicate name.
    #[inline]
    pub fn head_name(&self) -> &str {
        self.rule.head().name()
    }

    /// Get the head IDB fingerprint.
    #[inline]
    pub fn head_idb_fingerprint(&self) -> u64 {
        self.head_idb_fingerprint
    }

    /// Get the head arguments.
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        self.rule.head().head_arguments()
    }

    /// Get all variable strings from the head arguments.
    pub fn head_arguments_strs(&self) -> HashSet<String> {
        self.head_arguments()
            .iter()
            .flat_map(|h| h.vars().into_iter().cloned())
            .collect()
    }

    /// Get the mapping from head argument text to its structured form.
    #[inline]
    pub fn head_arguments_map(&self) -> &HashMap<String, HeadArg> {
        &self.head_arguments_map
    }

    // === Unused Arguments ===

    /// Get unused arguments grouped by atom signature.
    #[inline]
    pub fn unused_arguments_per_atom(&self) -> &HashMap<AtomSignature, Vec<AtomArgumentSignature>> {
        &self.unused_arguments_per_atom
    }

    // === Plan Logic ===
    /// Check if current rule planning is done.
    pub fn is_planned(&self) -> bool {
        self.positive_atom_fingerprints.len() == 1
            && self.negated_atom_fingerprints.is_empty()
            && self.filters.is_empty()
            && self.comparison_predicates.is_empty()
    }
}

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Rule:\n  {}", self.rule())?;

        let fmt_sig_list = |sigs: &[AtomArgumentSignature],
                            map: &HashMap<AtomArgumentSignature, String>|
         -> String {
            let mut items: Vec<String> = sigs
                .iter()
                .filter_map(|s| map.get(s).map(|v| format!("{}:{}", s, v)))
                .collect();
            items.sort();
            format!("[{}]", items.join(", "))
        };

        // NEW: simple index vector pretty-printer (keeps original order)
        let fmt_idx_list = |idxs: &[usize]| -> String {
            format!(
                "[{}]",
                idxs.iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        writeln!(f, "\nPositive atoms:")?;
        for (i, fp) in self.positive_atom_fingerprints.iter().enumerate() {
            let args = fmt_sig_list(
                &self.positive_atom_argument_signatures[i],
                &self.signature_to_argument_str_map,
            );
            writeln!(f, "  [{:>2}] 0x{:016x} args: {}", i, fp, args)?;
        }

        writeln!(f, "\nNegated atoms:")?;
        if self.negated_atom_fingerprints.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, fp) in self.negated_atom_fingerprints.iter().enumerate() {
                let args = fmt_sig_list(
                    &self.negated_atom_argument_signatures[i],
                    &self.signature_to_argument_str_map,
                );
                writeln!(f, "  [{:>2}] 0x{:016x} args: {}", i, fp, args)?;
            }
        }

        // NEW: print RHS index vectors with per-entry mapping
        writeln!(f, "\nRHS indices (by atom kind):")?;
        writeln!(
            f,
            "  positive_atom_rhs_ids ({}): {}",
            self.positive_atom_rhs_ids.len(),
            fmt_idx_list(&self.positive_atom_rhs_ids)
        )?;
        for (i, rhs_id) in self.positive_atom_rhs_ids.iter().copied().enumerate() {
            writeln!(f, "    pos[{:>2}] -> rhs[{:>2}]", i, rhs_id)?;
        }
        writeln!(
            f,
            "  negated_atom_rhs_ids  ({}): {}",
            self.negated_atom_rhs_ids.len(),
            fmt_idx_list(&self.negated_atom_rhs_ids)
        )?;
        for (i, rhs_id) in self.negated_atom_rhs_ids.iter().copied().enumerate() {
            writeln!(f, "    neg[{:>2}] -> rhs[{:>2}]", i, rhs_id)?;
        }

        let mut sig_entries: Vec<_> = self.signature_to_argument_str_map.iter().collect();
        sig_entries.sort_by(|(a, _), (b, _)| a.to_string().cmp(&b.to_string()));
        let sig_line = sig_entries
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(f, "\nSignature ↔ Var:")?;
        if sig_line.is_empty() {
            writeln!(f, "  (empty)")?;
        } else {
            writeln!(f, "  {}", sig_line)?;
        }

        writeln!(f, "\nArgument presence per positive atom:")?;
        let mut vars: Vec<_> = self.argument_presence_in_positive_atom_map.keys().collect();
        vars.sort();
        for var in vars {
            let row = self.argument_presence_in_positive_atom_map[var]
                .iter()
                .map(|opt| opt.map(|s| s.to_string()).unwrap_or_else(|| "-".into()))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  {}: [{}]", var, row)?;
        }

        writeln!(f, "\nBase filters:")?;
        if self.filters.is_empty() {
            writeln!(f, "  (none)")?
        } else {
            for line in self.filters.to_string().lines() {
                writeln!(f, "  {}", line)?;
            }
        }

        writeln!(f, "\nComparison predicates:")?;
        if self.comparison_predicates.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, comp_pred) in self.comparison_predicates.iter().enumerate() {
                let vars_set = if i < self.comparison_predicates_vars_str_set.len() {
                    let mut vars: Vec<_> =
                        self.comparison_predicates_vars_str_set[i].iter().collect();
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

        writeln!(f, "\nSupersets (per predicate → positive atom ids):")?;
        let mut print_supersets = |label: &str, supersets: &Vec<Vec<usize>>| -> fmt::Result {
            if supersets.is_empty() || supersets.iter().all(|supers| supers.is_empty()) {
                writeln!(f, "  {}: (none)", label)
            } else {
                writeln!(f, "  {}:", label)?;
                for (i, supers) in supersets.iter().enumerate() {
                    if !supers.is_empty() {
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
        };
        print_supersets("positives", &self.positive_supersets)?;
        print_supersets("negated", &self.negated_supersets)?;
        print_supersets("comparisons", &self.comparison_supersets)?;

        writeln!(f, "\nUnused arguments per atom:")?;
        if self.unused_arguments_per_atom.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (atom_sig, args) in &self.unused_arguments_per_atom {
                writeln!(f, "  {:?} -> {:?}", atom_sig, args)?;
            }
        }

        Ok(())
    }
}
