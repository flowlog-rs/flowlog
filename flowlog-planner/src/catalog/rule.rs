//! One rule's [`Catalog`]: construction, metadata queries, and diagnostic
//! display. Rewrites and metadata population live in child modules.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::Arithmetic;
use flowlog_parser::ComparisonExpr;
use flowlog_parser::ComparisonOperator;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use tracing::debug;

use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::CatalogError;
use crate::catalog::ComparisonExprPos;
use crate::catalog::Filters;

mod modify;
mod populate;

/// Returns indexed metadata or an internal error describing the violated
/// bounds.
fn metadata_at<'a, T>(
    metadata: &'a [T],
    index: usize,
    field_name: &str,
) -> Result<&'a T, CatalogError> {
    metadata.get(index).ok_or_else(|| {
        CatalogError::internal(format!(
            "{field_name} index {index} out of bounds for length {}",
            metadata.len()
        ))
    })
}

/// One rule's precomputed metadata: signatures, variable mappings,
/// filters, and superset relationships, kept in step with the rule as the
/// planner rewrites it.
///
/// Metadata maps and sets have deterministic iteration order.
///
/// Positive and negative atoms have separate local indices. Body-index
/// vectors map each local index back to the complete rule body.
#[derive(Debug)]
pub(crate) struct Catalog {
    /// The rule this metadata describes, including catalog rewrites.
    rule: FlowLogRule,

    /// Variable name at each variable-bearing argument position.
    argument_variables: BTreeMap<AtomArgumentSignature, String>,

    /// For each variable, its first occurrence in every positive atom.
    ///
    /// For `tc(x, z) :- arc(x, y), tc(y, z)`, this maps `x` to
    /// `[Some(0.0), None]`, `y` to `[Some(0.1), Some(1.0)]`, and `z` to
    /// `[None, Some(1.1)]`.
    positive_argument_presence: BTreeMap<String, Vec<Option<AtomArgumentSignature>>>,
    /// Atom fingerprints as the rule was first catalogued, before any
    /// rewrite.
    original_atom_fingerprints: BTreeSet<u64>,

    // --- Positive atom metadata ---
    /// Positive atom fingerprints, in source order.
    positive_atom_fingerprints: Vec<u64>,
    /// Argument signatures per positive atom.
    positive_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Variable names per positive atom (deduplicated).
    positive_atom_variables: Vec<BTreeSet<String>>,
    /// Positive atoms whose variable set is a superset of this one's.
    positive_supersets: Vec<Vec<usize>>,
    /// Index in the full rule body for each positive atom.
    positive_atom_body_indices: Vec<usize>,

    // --- Negative atom metadata ---
    /// Negative atom fingerprints, in source order.
    negative_atom_fingerprints: Vec<u64>,
    /// Argument signatures per negative atom.
    negative_atom_argument_signatures: Vec<Vec<AtomArgumentSignature>>,
    /// Variable names per negative atom (deduplicated).
    negative_atom_variables: Vec<BTreeSet<String>>,
    /// Positive atoms whose variable set covers this negative atom's.
    negative_supersets: Vec<Vec<usize>>,
    /// Index in the full rule body for each negative atom.
    negative_atom_body_indices: Vec<usize>,

    /// Variable-equality, constant, and placeholder constraints.
    filters: Filters,

    /// Comparison predicates in the rule body.
    comparison_predicates: Vec<ComparisonExpr>,
    /// Variable names per comparison predicate (deduplicated).
    comparison_variables: Vec<BTreeSet<String>>,
    /// Positive atoms whose variable set covers each comparison.
    comparison_supersets: Vec<Vec<usize>>,

    /// Head fingerprint as first catalogued.
    original_head_fingerprint: u64,

    /// Projectable argument positions whose variable is used by exactly
    /// one body predicate and not in the head. Grouped by atom.
    unused_arguments_per_atom: BTreeMap<AtomSignature, Vec<AtomArgumentSignature>>,
}

// =============================================================================
// Construction
// =============================================================================

impl Catalog {
    fn with_empty_metadata(rule: &FlowLogRule) -> Self {
        Self {
            rule: rule.clone(),
            argument_variables: BTreeMap::new(),
            positive_argument_presence: BTreeMap::new(),
            original_atom_fingerprints: BTreeSet::new(),
            positive_atom_fingerprints: Vec::new(),
            positive_atom_argument_signatures: Vec::new(),
            positive_atom_variables: Vec::new(),
            positive_supersets: Vec::new(),
            positive_atom_body_indices: Vec::new(),
            negative_atom_fingerprints: Vec::new(),
            negative_atom_argument_signatures: Vec::new(),
            negative_atom_variables: Vec::new(),
            negative_supersets: Vec::new(),
            negative_atom_body_indices: Vec::new(),
            filters: Filters::new(BTreeMap::new(), BTreeMap::new(), BTreeSet::new()),
            comparison_predicates: Vec::new(),
            comparison_variables: Vec::new(),
            comparison_supersets: Vec::new(),
            original_head_fingerprint: rule.head().head_fingerprint(),
            unused_arguments_per_atom: BTreeMap::new(),
        }
    }

    /// Builds a catalog for `rule`, deriving every signature, filter, and
    /// occurrence map.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::UnsafeVariable`] when a negated atom or
    /// comparison uses a variable no positive atom binds. Returns an
    /// internal error if derived metadata is inconsistent.
    pub(crate) fn from_rule(rule: &FlowLogRule) -> Result<Self, CatalogError> {
        let mut catalog = Self::with_empty_metadata(rule);
        catalog.populate_all_metadata()?;

        for predicate in rule.rhs() {
            match predicate {
                Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                    catalog
                        .original_atom_fingerprints
                        .insert(atom.fingerprint());
                }
                Predicate::Compare(_) => {}
            }
        }

        debug!("\n{}", catalog);

        Ok(catalog)
    }

    /// Replaces the current rule and its metadata while preserving the
    /// original atom and head identities.
    ///
    /// Leaves the catalog unchanged when the replacement rule is invalid.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::UnsafeVariable`] when a negated atom or
    /// comparison uses a variable no positive atom binds. Returns an
    /// internal error if derived metadata is inconsistent.
    pub(crate) fn update_rule(&mut self, rule: &FlowLogRule) -> Result<(), CatalogError> {
        let mut replacement = Self::with_empty_metadata(rule);
        replacement.populate_all_metadata()?;
        replacement.original_atom_fingerprints = self.original_atom_fingerprints.clone();
        replacement.original_head_fingerprint = self.original_head_fingerprint;
        *self = replacement;
        Ok(())
    }
}

// =============================================================================
// Queries
// =============================================================================

impl Catalog {
    /// Returns the rule, including every rewrite applied so far.
    #[inline]
    pub(crate) fn rule(&self) -> &FlowLogRule {
        &self.rule
    }

    /// Returns the variable name at an argument position.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `signature` has no variable mapping.
    #[inline]
    pub(crate) fn signature_to_argument_str(
        &self,
        signature: &AtomArgumentSignature,
    ) -> Result<&str, CatalogError> {
        self.argument_variables
            .get(signature)
            .map(String::as_str)
            .ok_or_else(|| {
                CatalogError::internal(format!(
                    "argument signature {signature} absent from signature-to-variable map"
                ))
            })
    }

    /// Returns the positive-atom count for a core rule.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the rule still has an optimization
    /// opportunity.
    #[inline]
    pub(crate) fn core_atom_number(&self) -> Result<usize, CatalogError> {
        let residual = if self
            .positive_supersets
            .iter()
            .any(|supersets| !supersets.is_empty())
        {
            Some("positive supersets")
        } else if self
            .negative_supersets
            .iter()
            .any(|supersets| !supersets.is_empty())
        {
            Some("negative supersets")
        } else if self
            .comparison_supersets
            .iter()
            .any(|supersets| !supersets.is_empty())
        {
            Some("comparison supersets")
        } else if !self.filters.is_empty() {
            Some("filters")
        } else {
            None
        };
        if let Some(residual) = residual {
            return Err(CatalogError::internal(format!(
                "core rule still has {residual}: {}",
                self.rule
            )));
        }
        Ok(self.positive_atom_fingerprints.len())
    }

    /// Returns the atom fingerprints as first catalogued.
    #[inline]
    pub(crate) fn original_atom_fingerprints(&self) -> &BTreeSet<u64> {
        &self.original_atom_fingerprints
    }

    // --- Positive atoms ---

    /// Returns the number of positive atoms in the body.
    #[inline]
    pub(crate) fn positive_atom_number(&self) -> usize {
        self.positive_atom_fingerprints.len()
    }

    /// Returns a positive atom's fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the positive atoms.
    #[inline]
    pub(crate) fn positive_atom_fingerprint(&self, index: usize) -> Result<u64, CatalogError> {
        metadata_at(
            &self.positive_atom_fingerprints,
            index,
            "positive atom fingerprint",
        )
        .copied()
    }

    /// Returns a positive atom's current hierarchical name, reflecting
    /// every rewrite applied so far rather than the source spelling.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the positive atoms
    /// or its body position does not contain a positive atom.
    #[inline]
    pub(crate) fn positive_atom_name(&self, index: usize) -> Result<&str, CatalogError> {
        let body_index = self.positive_atom_rhs_id(index)?;
        match metadata_at(self.rule.rhs(), body_index, "rule body")? {
            Predicate::PositiveAtom(atom) => Ok(atom.name()),
            other @ (Predicate::NegativeAtom(_) | Predicate::Compare(_)) => {
                Err(CatalogError::internal(format!(
                    "positive atom {index} maps to non-positive body predicate {other}"
                )))
            }
        }
    }

    /// Returns a positive atom's argument signatures.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the positive atoms.
    #[inline]
    pub(crate) fn positive_atom_argument_signature(
        &self,
        index: usize,
    ) -> Result<&[AtomArgumentSignature], CatalogError> {
        metadata_at(
            &self.positive_atom_argument_signatures,
            index,
            "positive atom argument signatures",
        )
        .map(Vec::as_slice)
    }

    /// Returns, per positive atom, the positive atoms whose variable set
    /// is a superset of its own.
    #[inline]
    pub(crate) fn positive_supersets(&self) -> &[Vec<usize>] {
        &self.positive_supersets
    }

    /// Returns a positive atom's index in the full rule body.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the positive atoms.
    #[inline]
    pub(crate) fn positive_atom_rhs_id(&self, index: usize) -> Result<usize, CatalogError> {
        metadata_at(
            &self.positive_atom_body_indices,
            index,
            "positive atom body index",
        )
        .copied()
    }

    /// Returns `true` if two positive atoms share at least one variable,
    /// indicating that a SIP (side-information passing) semijoin between
    /// them may be beneficial.
    ///
    /// # Errors
    ///
    /// Returns an internal error if either index is outside the positive
    /// atoms.
    #[inline]
    pub(crate) fn check_sip_pair(
        &self,
        left_atom_index: usize,
        right_atom_index: usize,
    ) -> Result<bool, CatalogError> {
        let left_vars = metadata_at(
            &self.positive_atom_variables,
            left_atom_index,
            "left positive atom variables",
        )?;
        let right_vars = metadata_at(
            &self.positive_atom_variables,
            right_atom_index,
            "right positive atom variables",
        )?;
        Ok(!left_vars.is_disjoint(right_vars))
    }

    // --- Negative atoms ---

    /// Returns a negative atom's fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the negative atoms.
    #[inline]
    pub(crate) fn negative_atom_fingerprint(&self, index: usize) -> Result<u64, CatalogError> {
        metadata_at(
            &self.negative_atom_fingerprints,
            index,
            "negative atom fingerprint",
        )
        .copied()
    }

    /// Returns a negative atom's current hierarchical name, reflecting
    /// every rewrite applied so far.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the negative atoms
    /// or its body position does not contain a negative atom.
    #[inline]
    pub(crate) fn negative_atom_name(&self, index: usize) -> Result<&str, CatalogError> {
        let body_index = self.negative_atom_rhs_id(index)?;
        match metadata_at(self.rule.rhs(), body_index, "rule body")? {
            Predicate::NegativeAtom(atom) => Ok(atom.name()),
            other @ (Predicate::PositiveAtom(_) | Predicate::Compare(_)) => {
                Err(CatalogError::internal(format!(
                    "negative atom {index} maps to non-negative body predicate {other}"
                )))
            }
        }
    }

    /// Returns a negative atom's argument signatures.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the negative atoms.
    #[inline]
    pub(crate) fn negative_atom_argument_signature(
        &self,
        index: usize,
    ) -> Result<&[AtomArgumentSignature], CatalogError> {
        metadata_at(
            &self.negative_atom_argument_signatures,
            index,
            "negative atom argument signatures",
        )
        .map(Vec::as_slice)
    }

    /// Returns, per negative atom, the positive atoms whose variable set
    /// covers it.
    #[inline]
    pub(crate) fn negative_supersets(&self) -> &[Vec<usize>] {
        &self.negative_supersets
    }

    /// Returns a negative atom's index in the full rule body.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the negative atoms.
    #[inline]
    pub(crate) fn negative_atom_rhs_id(&self, index: usize) -> Result<usize, CatalogError> {
        metadata_at(
            &self.negative_atom_body_indices,
            index,
            "negative atom body index",
        )
        .copied()
    }

    // --- Atom resolution ---

    /// Resolves an atom to its argument signatures, fingerprint,
    /// polarity-local index, and current hierarchical name.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the signature identifies no atom or
    /// the atom metadata is inconsistent.
    #[inline]
    pub(crate) fn resolve_atom(
        &self,
        atom_signature: &AtomSignature,
    ) -> Result<(&[AtomArgumentSignature], u64, usize, &str), CatalogError> {
        let atom_index = atom_signature.rhs_id();
        if atom_signature.is_positive() {
            Ok((
                self.positive_atom_argument_signature(atom_index)?,
                self.positive_atom_fingerprint(atom_index)?,
                atom_index,
                self.positive_atom_name(atom_index)?,
            ))
        } else {
            Ok((
                self.negative_atom_argument_signature(atom_index)?,
                self.negative_atom_fingerprint(atom_index)?,
                atom_index,
                self.negative_atom_name(atom_index)?,
            ))
        }
    }

    /// Returns an atom's index in the full rule body.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `signature` identifies no atom or its
    /// body index is invalid.
    pub(crate) fn rhs_index_from_signature(
        &self,
        signature: AtomSignature,
    ) -> Result<usize, CatalogError> {
        let body_index = if signature.is_positive() {
            self.positive_atom_rhs_id(signature.rhs_id())?
        } else {
            self.negative_atom_rhs_id(signature.rhs_id())?
        };
        metadata_at(self.rule.rhs(), body_index, "rule body")?;
        Ok(body_index)
    }

    // --- Comparison predicates ---

    /// Returns a comparison predicate.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `index` is outside the comparisons.
    #[inline]
    pub(crate) fn comparison_predicate(
        &self,
        index: usize,
    ) -> Result<ComparisonExpr, CatalogError> {
        metadata_at(&self.comparison_predicates, index, "comparison predicate").cloned()
    }

    /// Returns, per comparison, the positive atoms whose variable set
    /// covers it.
    #[inline]
    pub(crate) fn comparison_supersets(&self) -> &[Vec<usize>] {
        &self.comparison_supersets
    }

    #[inline]
    fn variable_signature_in_positive_atom(
        &self,
        variable: &str,
        atom_index: usize,
    ) -> Option<AtomArgumentSignature> {
        self.positive_argument_presence
            .get(variable)
            .and_then(|presence| presence.get(atom_index).copied().flatten())
    }

    /// Resolves a comparison's variables to argument positions within one
    /// positive atom.
    ///
    /// # Errors
    ///
    /// Returns an internal error if either index is invalid, a variable is
    /// absent from the atom, or positional arithmetic construction fails.
    pub(crate) fn resolve_comparison_predicates(
        &self,
        positive_atom_index: usize,
        comparison_index: usize,
    ) -> Result<ComparisonExprPos, CatalogError> {
        let comparison = metadata_at(
            &self.comparison_predicates,
            comparison_index,
            "comparison predicate",
        )?;
        let resolve =
            |side: &str, variable: &String| -> Result<AtomArgumentSignature, CatalogError> {
                self.variable_signature_in_positive_atom(variable, positive_atom_index)
                    .ok_or_else(|| {
                        CatalogError::internal(format!(
                            "variable `{variable}` in comparison {side} not found in positive atom \
                             #{positive_atom_index}"
                        ))
                    })
            };
        let left_variable_signatures = comparison
            .left()
            .vars()
            .iter()
            .map(|&variable| resolve("left side", variable))
            .collect::<Result<Vec<_>, _>>()?;
        let right_variable_signatures = comparison
            .right()
            .vars()
            .iter()
            .map(|&variable| resolve("right side", variable))
            .collect::<Result<Vec<_>, _>>()?;
        ComparisonExprPos::from_comparison_expr(
            comparison,
            &left_variable_signatures,
            &right_variable_signatures,
        )
    }

    /// Returns equalities usable as join keys between two positive atoms.
    ///
    /// Each result is `(comparison_index, left_key, right_key)`.
    ///
    /// # Errors
    ///
    /// Returns an internal error if comparison metadata is inconsistent or
    /// an equality cannot be converted to positional arithmetic.
    pub(crate) fn equijoin_keys_for_pair(
        &self,
        left_atom_index: usize,
        right_atom_index: usize,
    ) -> Result<Vec<(usize, ArithmeticPos, ArithmeticPos)>, CatalogError> {
        // An equality qualifies when each side resolves wholly in one of the
        // two atoms; fresh-variable equalities are desugared to bindings
        // before planning, so every comparison here is between two grounded
        // expressions and is safe to key a join on.

        let resolve_in_atom = |arithmetic: &Arithmetic,
                               atom_index: usize|
         -> Result<Option<ArithmeticPos>, CatalogError> {
            let Some(signatures) = arithmetic
                .vars()
                .iter()
                .map(|&variable| self.variable_signature_in_positive_atom(variable, atom_index))
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            ArithmeticPos::from_arithmetic(arithmetic, &signatures).map(Some)
        };

        let mut keys = Vec::new();
        for (comparison_index, comparison) in self.comparison_predicates.iter().enumerate() {
            if *comparison.operator() != ComparisonOperator::Equal {
                continue;
            }
            let supersets = metadata_at(
                &self.comparison_supersets,
                comparison_index,
                "comparison supersets",
            )?;
            if !supersets.is_empty() {
                continue;
            }
            let resolved = match (
                resolve_in_atom(comparison.left(), left_atom_index)?,
                resolve_in_atom(comparison.right(), right_atom_index)?,
            ) {
                (Some(left), Some(right)) => Some((left, right)),
                (None, _) | (_, None) => resolve_in_atom(comparison.right(), left_atom_index)?
                    .zip(resolve_in_atom(comparison.left(), right_atom_index)?),
            };
            if let Some((left, right)) = resolved {
                keys.push((comparison_index, left, right));
            }
        }
        Ok(keys)
    }

    /// Returns a fresh shadow-column name for a fused equality.
    pub(crate) fn fresh_equijoin_key_name(&self, comparison_index: usize) -> String {
        // User identifiers have at most one leading underscore, so the
        // reserved `__` prefix cannot collide with source names.
        let mut name = format!("__eqk{comparison_index}");
        while self.positive_argument_presence.contains_key(&name) {
            name.push('_');
        }
        name
    }

    // --- Filters ---

    /// Returns the rule's local filters.
    #[inline]
    pub(crate) fn filters(&self) -> &Filters {
        &self.filters
    }

    // --- Head information ---

    /// Returns the head relation's original fingerprint.
    #[inline]
    pub(crate) fn head_idb_fingerprint(&self) -> u64 {
        self.original_head_fingerprint
    }

    /// Returns the head arguments.
    #[inline]
    pub(crate) fn head_arguments(&self) -> &[HeadArg] {
        self.rule.head().head_arguments()
    }

    /// Returns every variable named in the head.
    pub(crate) fn head_variables(&self) -> BTreeSet<String> {
        self.head_arguments()
            .iter()
            .flat_map(|argument| argument.vars().into_iter().cloned())
            .collect()
    }

    // --- Unused arguments ---

    /// Returns the projectable argument positions, grouped by atom.
    #[inline]
    pub(crate) fn unused_arguments_per_atom(
        &self,
    ) -> &BTreeMap<AtomSignature, Vec<AtomArgumentSignature>> {
        &self.unused_arguments_per_atom
    }

    // --- Plan logic ---

    /// Returns `true` once every atom has been folded into the plan.
    pub(crate) fn is_planned(&self) -> bool {
        self.positive_atom_fingerprints.len() == 1
            && self.negative_atom_fingerprints.is_empty()
            && self.filters.is_empty()
            && self.comparison_predicates.is_empty()
    }
}

impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", SECTION_BAR)?;

        writeln!(f, "Catalog of rule:\n  {}", self.rule())?;

        let format_signature_list = |signatures: &[AtomArgumentSignature],
                                     variables: &BTreeMap<AtomArgumentSignature, String>|
         -> String {
            let items = signatures
                .iter()
                .map(|signature| {
                    variables.get(signature).map_or_else(
                        || format!("{signature}:<missing variable>"),
                        |variable| format!("{signature}:{variable}"),
                    )
                })
                .collect::<Vec<_>>();
            format!("[{}]", items.join(", "))
        };

        let format_index_list = |indices: &[usize]| -> String {
            format!(
                "[{}]",
                indices
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Positive atoms:")?;
        for (atom_index, fingerprint) in self.positive_atom_fingerprints.iter().enumerate() {
            let arguments = self
                .positive_atom_argument_signatures
                .get(atom_index)
                .map(|signatures| format_signature_list(signatures, &self.argument_variables))
                .unwrap_or_else(|| "[missing argument metadata]".to_string());
            writeln!(
                f,
                "  [{atom_index:>2}] 0x{fingerprint:016x} args: {arguments}"
            )?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Negative atoms:")?;
        if self.negative_atom_fingerprints.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (atom_index, fingerprint) in self.negative_atom_fingerprints.iter().enumerate() {
                let arguments = self
                    .negative_atom_argument_signatures
                    .get(atom_index)
                    .map(|signatures| format_signature_list(signatures, &self.argument_variables))
                    .unwrap_or_else(|| "[missing argument metadata]".to_string());
                writeln!(
                    f,
                    "  [{atom_index:>2}] 0x{fingerprint:016x} args: {arguments}"
                )?;
            }
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Body indices (by atom kind):")?;
        writeln!(
            f,
            "  positive ({}): {}",
            self.positive_atom_body_indices.len(),
            format_index_list(&self.positive_atom_body_indices)
        )?;
        for (atom_index, body_index) in self.positive_atom_body_indices.iter().copied().enumerate()
        {
            writeln!(f, "    positive[{atom_index:>2}] -> body[{body_index:>2}]")?;
        }
        writeln!(
            f,
            "  negative ({}): {}",
            self.negative_atom_body_indices.len(),
            format_index_list(&self.negative_atom_body_indices)
        )?;
        for (atom_index, body_index) in self.negative_atom_body_indices.iter().copied().enumerate()
        {
            writeln!(f, "    negative[{atom_index:>2}] -> body[{body_index:>2}]")?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Argument variables:")?;
        let signature_line = self
            .argument_variables
            .iter()
            .map(|(signature, variable)| format!("{signature}={variable}"))
            .collect::<Vec<_>>()
            .join(", ");
        if signature_line.is_empty() {
            writeln!(f, "  (empty)")?;
        } else {
            writeln!(f, "  {signature_line}")?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Argument presence per positive atom:")?;
        for (variable, presence) in &self.positive_argument_presence {
            let row = presence
                .iter()
                .map(|signature| {
                    signature
                        .map(|signature| signature.to_string())
                        .unwrap_or_else(|| "-".to_string())
                })
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(f, "  {variable}: [{row}]")?;
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Base filters:")?;
        if self.filters.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for line in self.filters.to_string().lines() {
                writeln!(f, "  {line}")?;
            }
        }

        let format_predicate_variables =
            |variable_sets: &[BTreeSet<String>], index: usize| -> String {
                let Some(variables) = variable_sets.get(index) else {
                    return "vars: <missing metadata>".to_string();
                };
                format!(
                    "vars: [{}]",
                    variables
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Comparison predicates:")?;
        if self.comparison_predicates.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (index, comparison) in self.comparison_predicates.iter().enumerate() {
                let variables = format_predicate_variables(&self.comparison_variables, index);
                writeln!(f, "  [{index:>2}] {comparison} ({variables})")?;
            }
        }

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Covering positive atoms by predicate:")?;
        let mut print_supersets = |label: &str, supersets: &[Vec<usize>]| -> fmt::Result {
            if supersets.is_empty() || supersets.iter().all(|supers| supers.is_empty()) {
                writeln!(f, "  {label}: (none)")
            } else {
                writeln!(f, "  {label}:")?;
                for (index, superset_indices) in supersets.iter().enumerate() {
                    if !superset_indices.is_empty() {
                        writeln!(
                            f,
                            "    [{index}] -> [{}]",
                            superset_indices
                                .iter()
                                .map(ToString::to_string)
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

        writeln!(f, "\n{}", SUBSECTION_BAR)?;
        writeln!(f, "Unused arguments per atom:")?;
        if self.unused_arguments_per_atom.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (atom_signature, arguments) in &self.unused_arguments_per_atom {
                let arguments = arguments
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                writeln!(f, "  {atom_signature} -> [{}]", arguments.join(", "))?;
            }
        }

        writeln!(f, "{}", SECTION_BAR)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_error::SourceMap;
    use flowlog_parser::FlowLogRule;
    use tempfile::NamedTempFile;

    use super::*;

    fn parsed_rule(source: &str) -> FlowLogRule {
        let mut tmp = NamedTempFile::new().expect("tempfile");
        tmp.write_all(source.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let program = flowlog_parser::parse(
            &tmp.path().to_string_lossy(),
            &[],
            &mut sm,
            &mut Config::default(),
        )
        .expect("parse failed");
        let rules = program.rules();
        (*rules.first().expect("test source produced no rule")).clone()
    }

    fn catalog_for(source: &str) -> Catalog {
        Catalog::from_rule(&parsed_rule(source)).expect("catalog build failed")
    }

    #[test]
    fn sip_pair_requires_a_shared_variable() {
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
        assert!(shared.check_sip_pair(0, 1).expect("valid atom indices"));

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
        assert!(!disjoint.check_sip_pair(0, 1).expect("valid atom indices"));
    }

    #[test]
    fn caller_controlled_positive_atom_index_returns_internal_error() {
        let catalog = catalog_for(
            "\
            .decl A(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x).\n",
        );
        let error = catalog
            .positive_atom_fingerprint(1)
            .expect_err("index 1 is outside a one-atom catalog");
        assert_eq!(
            error.to_string(),
            "internal compiler error at stage `catalog`: positive atom fingerprint index 1 out \
             of bounds for length 1"
        );
    }

    #[test]
    fn core_atom_number_rejects_unprepared_rule() {
        let catalog = catalog_for(
            "\
            .decl A(a: int32)\n\
            .decl B(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x), B(x).\n",
        );
        let error = catalog
            .core_atom_number()
            .expect_err("shared-variable atoms have residual supersets");
        assert_eq!(
            error.to_string(),
            "internal compiler error at stage `catalog`: core rule still has positive supersets: \
             out(x) :- A(x), B(x)."
        );
    }

    #[test]
    fn invalid_rule_update_leaves_catalog_unchanged() {
        let mut catalog = catalog_for(
            "\
            .decl A(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x).\n",
        );
        let original = catalog.to_string();
        let invalid_rule = parsed_rule(
            "\
            .decl A(a: int32)\n\
            .decl Blocked(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input Blocked(IO=\"file\", filename=\"Blocked.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x), !Blocked(other).\n",
        );

        let error = catalog
            .update_rule(&invalid_rule)
            .expect_err("unsafe replacement rule must be rejected");
        assert_eq!(
            error.to_string(),
            "unsafe variable `other` in negated atom `!Blocked(other)`"
        );
        assert_eq!(catalog.to_string(), original);
    }
}
