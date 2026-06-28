//! Catalog of per-rule metadata and signatures for FlowLog Datalog programs.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, AtomSignature, CatalogError, ComparisonExprPos, Filters,
    FnCallPredicatePos,
};
use crate::parser::{ComparisonExpr, FlowLogRule, FnCall, HeadArg, Predicate};
use flowlog_common::{SECTION_BAR, SUBSECTION_BAR};
use tracing::debug;

// Implementation modules
mod modify;
mod populate;

/// Per-rule catalog with precomputed metadata.
///
/// This structure maintains signatures, variable mappings, filters, and superset relationships
/// for efficient rule analysis and transformation planning.
///
/// Note:
/// 1. We introduce positive and negative atom RHS indices, which refer to the positions of atoms
///    in the rule's right-hand side (RHS) when considering only positive or negative atoms, respectively.
///    These two indices are not same as the global RHS indices.
#[derive(Debug)]
pub struct Catalog {
    /// The rule.
    /// Rule could be modified via modification methods.
    /// Metadata is also updated accordingly.
    rule: FlowLogRule,

    /// Reverse map from argument signatures to their variable strings.
    signature_to_argument_str_map: HashMap<AtomArgumentSignature, String>,
    /// For each variable string, the first presence (if any) in every positive atom.
    /// Example: for `tc(x, z) :- arc(x, y), tc(y, z)` → `{ x: [Some(0.0), None], y: [Some(0.1), Some(1.0)], z: [None, Some(1.1)] }`.
    argument_presence_in_positive_atom_map: HashMap<String, Vec<Option<AtomArgumentSignature>>>,
    /// Original rule atom fingerprints
    original_atom_fingerprints: HashSet<u64>,

    // All these positive metadata are positive indexed.
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

    // All these negative metadata are negative indexed.
    /// RHS negative atom fingerprints (in source order).
    negative_atom_fingerprints: Vec<u64>,
    /// For each RHS negative atom, its argument signatures.
    negative_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Variable string sets per negative atom (deduplicated).
    negative_atom_argument_vars_str_sets: Vec<HashSet<String>>,
    /// For each RHS negative atom, indices of positive atoms that are supersets.
    negative_supersets: Vec<Vec<usize>>,
    /// For each RHS negative atom, its RHS id (index).
    negative_atom_rhs_ids: Vec<usize>,

    /// Local filters: variable equality constraints, constant equality constraints, placeholders.
    filters: Filters,

    /// Comparison predicates in the rule body.
    comparison_predicates: Vec<ComparisonExpr>,
    /// Variable string sets per comparison predicate (deduplicated).
    comparison_predicates_vars_str_set: Vec<HashSet<String>>,
    /// For each comparison predicate, indices of positive atoms that are supersets.
    comparison_supersets: Vec<Vec<usize>>,

    /// UDF fn_call predicates in the rule body.
    fn_call_predicates: Vec<FnCall>,
    /// Variable string sets per fn_call predicate (deduplicated).
    fn_call_predicates_vars_str_set: Vec<HashSet<String>>,
    /// For each fn_call predicate, indices of positive atoms that are supersets.
    fn_call_supersets: Vec<Vec<usize>>,

    /// Mapping from head argument string representation to structured form.
    head_arguments_map: HashMap<String, HeadArg>,
    /// Head IDB fingerprint.
    head_idb_fingerprint: u64,

    /// Atom argument signatures whose variable string occurs exactly once across
    /// all predicates in the rule body (positive atoms, negative atoms, comparisons).
    /// Grouped by atom signature for convenience elimination in planners.
    unused_arguments_per_atom: HashMap<AtomSignature, Vec<AtomArgumentSignature>>,
}

// Construction and lifecycle management
impl Catalog {
    /// Build a Catalog from a single rule (derives signatures, filters and helper maps).
    pub fn from_rule(rule: &FlowLogRule) -> Result<Self, CatalogError> {
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
            negative_atom_fingerprints: Vec::new(),
            negative_atom_argument_signatures: Vec::new(),
            negative_atom_argument_vars_str_sets: Vec::new(),
            negative_supersets: Vec::new(),
            negative_atom_rhs_ids: Vec::new(),
            filters: Filters::new(HashMap::new(), HashMap::new(), HashSet::new()),
            comparison_predicates: Vec::new(),
            comparison_predicates_vars_str_set: Vec::new(),
            comparison_supersets: Vec::new(),
            fn_call_predicates: Vec::new(),
            fn_call_predicates_vars_str_set: Vec::new(),
            fn_call_supersets: Vec::new(),
            head_arguments_map: HashMap::new(),
            head_idb_fingerprint: rule.head().head_fingerprint(),
            unused_arguments_per_atom: HashMap::new(),
        };
        catalog.populate_all_metadata()?;

        // Store original atom fingerprints for reference
        for predicate in rule.rhs() {
            match predicate {
                Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                    catalog
                        .original_atom_fingerprints
                        .insert(atom.fingerprint());
                }
                _ => {}
            }
        }

        // Debug info print
        debug!("\n{}", catalog);

        Ok(catalog)
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
        self.negative_atom_fingerprints.clear();
        self.negative_atom_argument_signatures.clear();
        self.negative_atom_argument_vars_str_sets.clear();
        self.negative_supersets.clear();
        self.negative_atom_rhs_ids.clear();
        self.filters = Filters::new(HashMap::new(), HashMap::new(), HashSet::new());
        self.comparison_predicates.clear();
        self.comparison_predicates_vars_str_set.clear();
        self.comparison_supersets.clear();
        self.fn_call_predicates.clear();
        self.fn_call_predicates_vars_str_set.clear();
        self.fn_call_supersets.clear();
        self.head_arguments_map.clear();
        self.unused_arguments_per_atom.clear();
    }

    /// Update the catalog with a new rule, recomputing corresponding metadata.
    pub(crate) fn update_rule(&mut self, rule: &FlowLogRule) -> Result<(), CatalogError> {
        self.rule = rule.clone();
        self.clear();
        self.populate_all_metadata()
    }
}

// Public API: Getters and query methods
impl Catalog {
    /// Get the underlying rule.
    #[inline]
    pub(crate) fn rule(&self) -> &FlowLogRule {
        &self.rule
    }

    /// Get the argument string for a specific signature.
    #[inline]
    pub(crate) fn signature_to_argument_str(&self, sig: &AtomArgumentSignature) -> &String {
        self.signature_to_argument_str_map.get(sig).unwrap()
    }

    /// Get the core atom count for rules with no supersets or filters.
    ///
    /// # Panics
    /// Panics if the rule has any supersets or filters, as this method
    /// is only valid for "core" rules without optimization opportunities.
    #[inline]
    pub(crate) fn core_atom_number(&self) -> usize {
        assert!(
            self.positive_supersets.iter().all(|s| s.is_empty()),
            "Rule has positive supersets - not a core rule"
        );
        assert!(
            self.negative_supersets.iter().all(|s| s.is_empty()),
            "Rule has negative supersets - not a core rule"
        );
        assert!(
            self.comparison_supersets.iter().all(|s| s.is_empty()),
            "Rule has comparison supersets - not a core rule"
        );
        assert!(
            self.fn_call_supersets.iter().all(|s| s.is_empty()),
            "Rule has fn_call supersets - not a core rule"
        );
        assert!(
            self.filters.is_empty(),
            "Rule has filters - not a core rule"
        );
        self.positive_atom_fingerprints.len()
    }

    // Get original atom fingerprints
    #[inline]
    pub(crate) fn original_atom_fingerprints(&self) -> &HashSet<u64> {
        &self.original_atom_fingerprints
    }

    // === Positive Atoms ===

    /// Get the number of positive atoms in the RHS.
    #[inline]
    pub(crate) fn positive_atom_number(&self) -> usize {
        self.positive_atom_fingerprints.len()
    }

    /// Get the fingerprint of a positive atom by its index.
    #[inline]
    pub(crate) fn positive_atom_fingerprint(&self, index: usize) -> u64 {
        self.positive_atom_fingerprints[index]
    }

    /// Get the current (possibly rewritten) name of a positive atom by its index.
    /// Atom names are mutated in place by `projection_modify` / `join_modify` /
    /// `comparison_modify` / `fn_call_modify`, so this returns the hierarchical
    /// name that currently describes the atom.
    #[inline]
    pub(crate) fn positive_atom_name(&self, index: usize) -> Result<&str, CatalogError> {
        let rhs_idx = self.positive_atom_rhs_ids[index];
        match &self.rule.rhs()[rhs_idx] {
            Predicate::PositiveAtom(a) => Ok(a.name()),
            other => Err(CatalogError::internal(format!(
                "positive_atom_rhs_ids[{index}] points at non-positive-atom predicate {other:?}"
            ))),
        }
    }

    /// Get the argument signatures for a positive atom by its index.
    #[inline]
    pub(crate) fn positive_atom_argument_signature(
        &self,
        index: usize,
    ) -> &Vec<AtomArgumentSignature> {
        &self.positive_atom_argument_signatures[index]
    }

    /// For each positive atom, get indices of positive atoms with superset variable sets.
    #[inline]
    pub(crate) fn positive_supersets(&self) -> &Vec<Vec<usize>> {
        &self.positive_supersets
    }

    /// Get the RHS id (index) of a positive atom by its index.
    #[inline]
    pub(crate) fn positive_atom_rhs_id(&self, index: usize) -> usize {
        self.positive_atom_rhs_ids[index]
    }

    /// Returns `true` if two positive atoms share at least one variable,
    /// indicating that a SIP (side-information passing) semijoin between
    /// them may be beneficial.
    #[inline]
    pub(crate) fn check_sip_pair(&self, left_atom_id: usize, right_atom_id: usize) -> bool {
        let left_vars = &self.positive_atom_argument_vars_str_sets[left_atom_id];
        let right_vars = &self.positive_atom_argument_vars_str_sets[right_atom_id];
        !left_vars.is_disjoint(right_vars)
    }

    // === Negative Atoms ===
    /// Get the fingerprint of a negative atom by its index.
    #[inline]
    pub(crate) fn negative_atom_fingerprint(&self, index: usize) -> u64 {
        self.negative_atom_fingerprints[index]
    }

    /// Get the current (possibly rewritten) name of a negative atom by its index.
    #[inline]
    pub(crate) fn negative_atom_name(&self, index: usize) -> Result<&str, CatalogError> {
        let rhs_idx = self.negative_atom_rhs_ids[index];
        match &self.rule.rhs()[rhs_idx] {
            Predicate::NegativeAtom(a) => Ok(a.name()),
            other => Err(CatalogError::internal(format!(
                "negative_atom_rhs_ids[{index}] points at non-negative-atom predicate {other:?}"
            ))),
        }
    }

    /// Get the argument signatures for a negative atom by its index.
    #[inline]
    pub(crate) fn negative_atom_argument_signature(
        &self,
        index: usize,
    ) -> &Vec<AtomArgumentSignature> {
        &self.negative_atom_argument_signatures[index]
    }

    /// For each negative atom, get indices of positive atoms with superset variable sets.
    #[inline]
    pub(crate) fn negative_supersets(&self) -> &Vec<Vec<usize>> {
        &self.negative_supersets
    }

    /// Get the RHS id (index) of a negative atom by its index.
    #[inline]
    pub(crate) fn negative_atom_rhs_id(&self, index: usize) -> usize {
        self.negative_atom_rhs_ids[index]
    }

    // === Atom Resolution ===

    /// Resolve an atom signature to its argument signatures, fingerprint,
    /// RHS id, and current hierarchical name.
    #[inline]
    pub(crate) fn resolve_atom(
        &self,
        atom_signature: &AtomSignature,
    ) -> Result<(&[AtomArgumentSignature], u64, usize, &str), CatalogError> {
        let atom_id = atom_signature.rhs_id();
        if atom_signature.is_positive() {
            Ok((
                &self.positive_atom_argument_signatures[atom_id],
                self.positive_atom_fingerprints[atom_id],
                atom_id,
                self.positive_atom_name(atom_id)?,
            ))
        } else {
            Ok((
                &self.negative_atom_argument_signatures[atom_id],
                self.negative_atom_fingerprints[atom_id],
                atom_id,
                self.negative_atom_name(atom_id)?,
            ))
        }
    }

    /// Finds the global RHS index of an atom given its signature.
    pub(crate) fn rhs_index_from_signature(&self, sig: AtomSignature) -> usize {
        if sig.is_positive() {
            self.positive_atom_rhs_ids[sig.rhs_id()]
        } else {
            self.negative_atom_rhs_ids[sig.rhs_id()]
        }
    }

    // === Comparison Predicates ===

    /// Get a comparison predicate by its index.
    #[inline]
    pub(crate) fn comparison_predicate(&self, index: usize) -> ComparisonExpr {
        self.comparison_predicates[index].clone()
    }

    /// For each comparison predicate, get indices of positive atoms with superset variable sets.
    #[inline]
    pub(crate) fn comparison_supersets(&self) -> &Vec<Vec<usize>> {
        &self.comparison_supersets
    }

    /// Resolve a comparison expression with argument positions for a given positive atom and comparison predicate.
    pub(crate) fn resolve_comparison_predicates(
        &self,
        pos_atom_id: usize,
        comp_id: usize,
    ) -> Result<ComparisonExprPos, CatalogError> {
        let comp_exprs = &self.comparison_predicates[comp_id];
        let resolve = |side: &'static str,
                       v: &String|
         -> Result<AtomArgumentSignature, CatalogError> {
            self.argument_presence_in_positive_atom_map
                .get(v)
                .and_then(|row| row.get(pos_atom_id).copied().flatten())
                .ok_or_else(|| {
                    CatalogError::internal(format!(
                        "variable `{v}` in comparison {side} not found in positive atom #{pos_atom_id}"
                    ))
                })
        };
        let left_var_signatures = comp_exprs
            .left()
            .vars()
            .iter()
            .map(|&v| resolve("lhs", v))
            .collect::<Result<Vec<_>, _>>()?;
        let right_var_signatures = comp_exprs
            .right()
            .vars()
            .iter()
            .map(|&v| resolve("rhs", v))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ComparisonExprPos::from_comparison_expr(
            comp_exprs,
            &left_var_signatures,
            &right_var_signatures,
        ))
    }

    // === FnCall Predicates ===

    /// Get a fn_call predicate by its index.
    #[inline]
    pub(crate) fn fn_call_predicate(&self, index: usize) -> &FnCall {
        &self.fn_call_predicates[index]
    }

    /// For each fn_call predicate, get indices of positive atoms with superset variable sets.
    #[inline]
    pub(crate) fn fn_call_supersets(&self) -> &Vec<Vec<usize>> {
        &self.fn_call_supersets
    }

    /// Resolve a fn_call predicate with argument positions for a given positive atom.
    pub(crate) fn resolve_fn_call_predicates(
        &self,
        pos_atom_id: usize,
        fn_call_id: usize,
    ) -> Result<FnCallPredicatePos, CatalogError> {
        let fn_call = &self.fn_call_predicates[fn_call_id];
        let args_pos: Vec<ArithmeticPos> = fn_call
            .args()
            .iter()
            .map(|arg| {
                let var_sigs: Vec<AtomArgumentSignature> = arg
                    .vars()
                    .iter()
                    .map(|&v| {
                        self.argument_presence_in_positive_atom_map
                            .get(v)
                            .and_then(|row| row.get(pos_atom_id).copied().flatten())
                            .ok_or_else(|| {
                                CatalogError::internal(format!(
                                    "variable `{v}` in fn_call arg not found in positive \
                                     atom #{pos_atom_id}"
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, CatalogError>(ArithmeticPos::from_arithmetic(arg, &var_sigs))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FnCallPredicatePos::new(
            fn_call.name().to_string(),
            args_pos,
            fn_call.is_negated(),
        ))
    }

    // === Filters ===

    /// Get the local atom filters (var==var, var==const, placeholders).
    #[inline]
    pub(crate) fn filters(&self) -> &Filters {
        &self.filters
    }

    // === Head Information ===

    /// Get the head IDB fingerprint.
    #[inline]
    pub(crate) fn head_idb_fingerprint(&self) -> u64 {
        self.head_idb_fingerprint
    }

    /// Get the head arguments.
    #[inline]
    pub(crate) fn head_arguments(&self) -> &[HeadArg] {
        self.rule.head().head_arguments()
    }

    /// Get all variable strings from the head arguments.
    pub(crate) fn head_arguments_strs(&self) -> HashSet<String> {
        self.head_arguments()
            .iter()
            .flat_map(|h| h.vars().into_iter().cloned())
            .collect()
    }

    // === Unused Arguments ===

    /// Get unused arguments grouped by atom signature.
    #[inline]
    pub(crate) fn unused_arguments_per_atom(
        &self,
    ) -> &HashMap<AtomSignature, Vec<AtomArgumentSignature>> {
        &self.unused_arguments_per_atom
    }

    // === Plan Logic ===
    /// Check if current rule planning is done.
    pub(crate) fn is_planned(&self) -> bool {
        self.positive_atom_fingerprints.len() == 1
            && self.negative_atom_fingerprints.is_empty()
            && self.filters.is_empty()
            && self.comparison_predicates.is_empty()
            && self.fn_call_predicates.is_empty()
    }
}

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", SECTION_BAR)?;

        writeln!(f, "Catalog of rule:\n  {:?}", self.rule())?;

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

        // Simple index vector pretty-printer (keeps original order)
        let fmt_idx_list = |idxs: &[usize]| -> String {
            format!(
                "[{}]",
                idxs.iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Positive atoms:")?;
        for (i, fp) in self.positive_atom_fingerprints.iter().enumerate() {
            let args = fmt_sig_list(
                &self.positive_atom_argument_signatures[i],
                &self.signature_to_argument_str_map,
            );
            writeln!(f, "  [{:>2}] 0x{:016x} args: {}", i, fp, args)?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Negative atoms:")?;
        if self.negative_atom_fingerprints.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, fp) in self.negative_atom_fingerprints.iter().enumerate() {
                let args = fmt_sig_list(
                    &self.negative_atom_argument_signatures[i],
                    &self.signature_to_argument_str_map,
                );
                writeln!(f, "  [{:>2}] 0x{:016x} args: {}", i, fp, args)?;
            }
        }

        // NEW: print RHS index vectors with per-entry mapping
        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "RHS indices (by atom kind):")?;
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
            "  negative_atom_rhs_ids  ({}): {}",
            self.negative_atom_rhs_ids.len(),
            fmt_idx_list(&self.negative_atom_rhs_ids)
        )?;
        for (i, rhs_id) in self.negative_atom_rhs_ids.iter().copied().enumerate() {
            writeln!(f, "    neg[{:>2}] -> rhs[{:>2}]", i, rhs_id)?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Signature ↔ Var:")?;
        let mut sig_entries: Vec<_> = self.signature_to_argument_str_map.iter().collect();
        sig_entries.sort_by_key(|(a, _)| a.to_string());
        let sig_line = sig_entries
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        if sig_line.is_empty() {
            writeln!(f, "  (empty)")?;
        } else {
            writeln!(f, "  {}", sig_line)?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Argument presence per positive atom:")?;
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

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Base filters:")?;
        if self.filters.is_empty() {
            writeln!(f, "  (none)")?
        } else {
            for line in self.filters.to_string().lines() {
                writeln!(f, "  {}", line)?;
            }
        }

        // Render the sorted var-set for the i-th predicate, or `[]` if out of range.
        let fmt_pred_vars = |sets: &[HashSet<String>], i: usize| -> String {
            let Some(set) = sets.get(i) else {
                return "vars: []".to_string();
            };
            let mut vars: Vec<&str> = set.iter().map(String::as_str).collect();
            vars.sort();
            format!("vars: [{}]", vars.join(", "))
        };

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Comparison predicates:")?;
        if self.comparison_predicates.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, comp_pred) in self.comparison_predicates.iter().enumerate() {
                let vars_set = fmt_pred_vars(&self.comparison_predicates_vars_str_set, i);
                writeln!(f, "  [{:>2}] {} ({})", i, comp_pred, vars_set)?;
            }
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "FnCall predicates:")?;
        if self.fn_call_predicates.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, fn_call_pred) in self.fn_call_predicates.iter().enumerate() {
                let vars_set = fmt_pred_vars(&self.fn_call_predicates_vars_str_set, i);
                writeln!(f, "  [{:>2}] {} ({})", i, fn_call_pred, vars_set)?;
            }
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Supersets (per predicate → positive atom ids):")?;
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
        print_supersets("negative", &self.negative_supersets)?;
        print_supersets("comparisons", &self.comparison_supersets)?;
        print_supersets("fn_calls", &self.fn_call_supersets)?;

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Unused arguments per atom:")?;
        if self.unused_arguments_per_atom.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (atom_sig, args) in &self.unused_arguments_per_atom {
                writeln!(f, "  {:?} -> {:?}", atom_sig, args)?;
            }
        }

        writeln!(f, "{}", SECTION_BAR)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests target invariants that integration fixtures (positive
    //! paths in e2e + 3 UnsafeVariable error fixtures) can't pin down:
    //! within-atom equality filters, placeholder/const recording, SIP
    //! eligibility truth table, and the "head var never unused" rule.
    //!
    //! Tests skip the typechecker pass so literals stay polymorphic
    //! (`ConstType::Int(_)`), matching how `tests/catalog_errors.rs`
    //! drives the catalog.
    use super::*;
    use crate::parser::{ConstType, Program};
    use flowlog_common::SourceMap;
    use std::io::Write;

    fn catalog_for(src: &str) -> Catalog {
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let program =
            Program::parse(&tmp.path().to_string_lossy(), false, &mut sm).expect("parse failed");
        Catalog::from_rule(program.rules()[0]).expect("catalog build failed")
    }

    /// `A(x, x)` — the second `x` must not create a fresh binding; it must
    /// emit an equality filter mapping arg1's sig to arg0's sig. A broken
    /// `local_var_first_occurrence_map` would re-bind and silently drop
    /// the join predicate.
    #[test]
    fn repeated_var_in_atom_creates_var_eq_entry() {
        let src = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, x).\n";
        let catalog = catalog_for(src);
        let sigs = catalog.positive_atom_argument_signature(0);
        let var_eq = catalog.filters().var_eq_map();
        assert_eq!(
            var_eq.get(&sigs[1]),
            Some(&sigs[0]),
            "second `x` must map back to first `x`"
        );
    }

    /// `A(_, 5)` — arg0 is a placeholder, arg1 is a const. Both must
    /// populate their respective filter sets. Downstream codegen runs
    /// pruning and const folding off these sets; empty sets on either
    /// side would emit wrong filters.
    #[test]
    fn placeholder_and_const_populate_filter_sets() {
        let src = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out()\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out() :- A(_, 5).\n";
        let catalog = catalog_for(src);
        let sigs = catalog.positive_atom_argument_signature(0);
        let filters = catalog.filters();
        assert!(
            filters.placeholder_set().contains(&sigs[0]),
            "placeholder `_` missing from placeholder_set"
        );
        assert_eq!(
            filters.const_map().get(&sigs[1]),
            Some(&ConstType::Int(5)),
            "constant 5 missing from const_map"
        );
    }

    /// `check_sip_pair` guards SIP eligibility — two positive atoms must
    /// share at least one body variable. Inverted `is_disjoint` logic or
    /// indexing into the wrong backing array would silently enable or
    /// disable every semijoin the planner considers.
    #[test]
    fn check_sip_pair_var_share_semantics() {
        // Shared `y` → eligible.
        let shared = catalog_for(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl Out(x: int32, y: int32, z: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y, z) :- A(x, y), B(y, z).\n",
        );
        assert!(
            shared.check_sip_pair(0, 1),
            "atoms sharing `y` should be SIP-eligible"
        );

        // No shared var → ineligible.
        let disjoint = catalog_for(
            "\
            .decl A(a: int32)\n\
            .decl B(a: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y).\n",
        );
        assert!(
            !disjoint.check_sip_pair(0, 1),
            "atoms with no shared var must not be SIP-eligible"
        );
    }

    /// Head variables must NEVER be marked unused, even when they appear
    /// only once in the body. A regression would let codegen drop a
    /// head-bound var's backing column, producing wrong rule output.
    #[test]
    fn head_variable_never_marked_unused() {
        let src = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, y).\n";
        let catalog = catalog_for(src);
        let sigs = catalog.positive_atom_argument_signature(0);
        let x_sig = sigs[0];
        let y_sig = sigs[1];
        let all_unused: HashSet<&AtomArgumentSignature> = catalog
            .unused_arguments_per_atom()
            .values()
            .flat_map(|v| v.iter())
            .collect();
        assert!(
            !all_unused.contains(&x_sig),
            "`x` is a head var and must never be marked unused"
        );
        assert!(
            all_unused.contains(&y_sig),
            "`y` appears once and is not in the head → should be marked unused"
        );
    }
}
