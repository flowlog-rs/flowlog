//! Transformation information used during query planning.
//!
//! A transformation info describes how to transform input collections
//! (with their key/value layouts) into an output collection (with its
//! key/value layout), along with any constraints (constant/variable
//! equalities, comparisons) that must hold for the transformation to
//! produce an output tuple.
//!
//! These are high-level descriptions that do not yet refer to concrete
//! collections (with their actual schemas). Layouts start as drafts that
//! later passes refine in place; the output fingerprint is recomputed from
//! current content on every mutation, so equal fingerprints always mean
//! "materializes to the same transformation".

use std::fmt;

use flowlog_common::compute_fp;
use flowlog_parser::Constant;

use super::TransformationFlow;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::ComparisonExprPos;
use crate::catalog::JoinPredicates;
use crate::catalog::KvPredicates;
use crate::planner::Collection;
use crate::planner::PlanError;

/// Key/Value layout of a collection: which positions form the key-value.
#[derive(PartialEq, Clone, Eq, Hash, Debug)]
pub(crate) struct KeyValueLayout {
    pub(crate) key: Vec<ArithmeticPos>,
    pub(crate) value: Vec<ArithmeticPos>,
}

impl KeyValueLayout {
    /// Construct a new Key-Value layout.
    #[inline]
    pub(crate) fn new(key: Vec<ArithmeticPos>, value: Vec<ArithmeticPos>) -> Self {
        Self { key, value }
    }

    /// Reference to key positions.
    #[inline]
    pub(crate) fn key(&self) -> &[ArithmeticPos] {
        &self.key
    }

    /// Reference to value positions.
    #[inline]
    pub(crate) fn value(&self) -> &[ArithmeticPos] {
        &self.value
    }

    /// Extract argument IDs from key/value positions in the layout.
    ///
    /// This method flattens all signatures within the key and value positions
    /// and extracts their argument IDs. It returns a tuple of (key_arg_ids, value_arg_ids).
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `Vec<usize>`: Argument IDs from all key positions
    /// - `Vec<usize>`: Argument IDs from all value positions
    #[inline]
    pub(crate) fn extract_argument_ids_from_layout(&self) -> (Vec<usize>, Vec<usize>) {
        let extract = |positions: &[ArithmeticPos]| -> Vec<usize> {
            positions
                .iter()
                .flat_map(|pos| pos.signatures())
                .map(|sig| sig.argument_id())
                .collect()
        };
        (extract(self.key()), extract(self.value()))
    }

    #[inline]
    pub(crate) fn extract_atom_id(&self) -> Result<usize, PlanError> {
        self.key()
            .iter()
            .chain(self.value().iter())
            .flat_map(|pos| pos.signatures())
            .map(|sig| sig.atom_signature().rhs_id())
            .next()
            .ok_or_else(|| {
                PlanError::internal(
                    "extract_atom_id: empty key/value layout has no atom signatures",
                )
            })
    }
}

/// Transformation information, describing how to transform input collection(s)
/// into an output collection, along with any constraints that must hold.
///
/// Equal `output_info_fp`s mean "materializes to the same transformation",
/// regardless of which rule or atom position built the info, compare
/// fingerprints to find shareable work at any planning stage. After
/// mutating an info, call [`Self::refresh_output_fp`] to keep this true.
#[derive(Clone, Debug)]
pub(crate) enum TransformationInfo {
    /// Unary Key-Value to Key-Value transformation (filter, map, projection, etc.).
    KVToKV {
        /// Upstream (input) collection fingerprint (re-pointed by later passes).
        input_info_fp: u64,
        /// Upstream collection's hierarchical name (e.g. `π[x](reach)`).
        input_name: String,
        /// Output collection fingerprint (refreshed by later passes).
        output_info_fp: u64,
        /// Output collection's hierarchical name.
        output_name: String,
        /// Whether row input
        is_row_input: bool,
        /// Whether row output
        is_row_output: bool,
        /// Input layout (key/value positions).
        input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (refined by later passes).
        output_kv_layout: KeyValueLayout,
        /// Filter predicates (equality constraints, comparisons, UDF predicates).
        predicates: KvPredicates,
        /// SIP projection
        is_sip_projection: bool,
    },

    /// Binary Join to Key-Value transformation.
    JoinToKV {
        /// Left input collection fingerprint (re-pointed by later passes).
        left_input_info_fp: u64,
        /// Left input's hierarchical name.
        left_input_name: String,
        /// Right input collection fingerprint (re-pointed by later passes).
        right_input_info_fp: u64,
        /// Right input's hierarchical name.
        right_input_name: String,
        /// Output collection fingerprint (refreshed by later passes).
        output_info_fp: u64,
        /// Output collection's hierarchical name (e.g. `(reach ⋈[y] arc)`).
        output_name: String,
        /// Whether row output
        is_row_output: bool,
        /// Left input layout (its key is the join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value contributes to output value).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (refined by later passes).
        output_kv_layout: KeyValueLayout,
        /// Filter predicates (comparisons and UDF predicates).
        predicates: JoinPredicates,
    },

    /// Binary Anti-Join to Key-Value transformation.
    AntiJoinToKV {
        /// Left input collection fingerprint (re-pointed by later passes).
        left_input_info_fp: u64,
        /// Left input's hierarchical name.
        left_input_name: String,
        /// Right input collection fingerprint (re-pointed by later passes).
        right_input_info_fp: u64,
        /// Right input's hierarchical name.
        right_input_name: String,
        /// Output collection fingerprint (refreshed by later passes).
        output_info_fp: u64,
        /// Output collection's hierarchical name (e.g. `(reach ▷[y] arc)`).
        output_name: String,
        /// Whether row output
        is_row_output: bool,
        /// Left input layout (its key is the anti-join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value is ignored in the output, but key participates).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (refined by later passes).
        output_kv_layout: KeyValueLayout,
    },
}

// ========================
// Constructors
// ========================
impl TransformationInfo {
    /// Build a Key-Value to Key-Value transformation.
    pub(crate) fn kv_to_kv(
        input_fp: u64,
        input_name: String,
        output_name: String,
        is_row_input: bool,
        input_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
        predicates: KvPredicates,
    ) -> Self {
        let mut info = Self::KVToKV {
            input_info_fp: input_fp,
            input_name,
            output_info_fp: 0,
            output_name,
            is_row_input,
            is_row_output: false,
            input_kv_layout,
            output_kv_layout,
            predicates,
            is_sip_projection: false,
        };
        info.refresh_output_fp();
        info
    }

    /// Mark this Key-Value transformation as a SIP projection.
    pub(crate) fn into_sip_projection(mut self) -> Result<Self, PlanError> {
        let Self::KVToKV {
            is_sip_projection, ..
        } = &mut self
        else {
            return Err(PlanError::internal(
                "into_sip_projection: only applicable to KVToKV transformations",
            ));
        };
        *is_sip_projection = true;
        Ok(self)
    }

    /// Build a Join to Key-Value transformation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn join_to_kv(
        left_input_fp: u64,
        left_input_name: String,
        right_input_fp: u64,
        right_input_name: String,
        output_name: String,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
        predicates: JoinPredicates,
    ) -> Self {
        let mut info = Self::JoinToKV {
            left_input_info_fp: left_input_fp,
            left_input_name,
            right_input_info_fp: right_input_fp,
            right_input_name,
            output_info_fp: 0,
            output_name,
            is_row_output: false,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout,
            predicates,
        };
        info.refresh_output_fp();
        info
    }

    /// Build an AntiJoin to Key-Value transformation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn anti_join_to_kv(
        left_input_fp: u64,
        left_input_name: String,
        right_input_fp: u64,
        right_input_name: String,
        output_name: String,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
    ) -> Self {
        let mut info = Self::AntiJoinToKV {
            left_input_info_fp: left_input_fp,
            left_input_name,
            right_input_info_fp: right_input_fp,
            right_input_name,
            output_info_fp: 0,
            output_name,
            is_row_output: false,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout,
        };
        info.refresh_output_fp();
        info
    }
}

// ========================
// Getters
// ========================
impl TransformationInfo {
    // Type checking methods

    /// Whether this is a neg join transformation info.
    #[inline]
    pub(crate) fn is_neg_join(&self) -> bool {
        matches!(self, Self::AntiJoinToKV { .. })
    }

    /// `true` only for a KVToKV whose `is_sip_projection` flag is set.
    /// A plain KVToKV filter or any join/anti-join returns `false`.
    #[cfg(test)]
    #[inline]
    pub(crate) fn is_sip_projection(&self) -> bool {
        matches!(
            self,
            Self::KVToKV {
                is_sip_projection: true,
                ..
            }
        )
    }

    // Fingerprint getters

    /// Input fingerprint(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub(crate) fn input_info_fp(&self) -> (u64, Option<u64>) {
        match self {
            Self::KVToKV { input_info_fp, .. } => (*input_info_fp, None),
            Self::JoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                ..
            } => (*left_input_info_fp, Some(*right_input_info_fp)),
        }
    }

    /// Output fingerprint.
    #[inline]
    pub(crate) fn output_info_fp(&self) -> u64 {
        match self {
            Self::KVToKV { output_info_fp, .. }
            | Self::JoinToKV { output_info_fp, .. }
            | Self::AntiJoinToKV { output_info_fp, .. } => *output_info_fp,
        }
    }

    /// Input hierarchical name(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub(crate) fn input_name(&self) -> (&str, Option<&str>) {
        match self {
            Self::KVToKV { input_name, .. } => (input_name.as_str(), None),
            Self::JoinToKV {
                left_input_name,
                right_input_name,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_name,
                right_input_name,
                ..
            } => (left_input_name.as_str(), Some(right_input_name.as_str())),
        }
    }

    /// Output hierarchical name.
    #[inline]
    pub(crate) fn output_name(&self) -> &str {
        match self {
            Self::KVToKV { output_name, .. }
            | Self::JoinToKV { output_name, .. }
            | Self::AntiJoinToKV { output_name, .. } => output_name.as_str(),
        }
    }

    /// Whether the input is row-based.
    /// Only KVtoKV needs this info.
    #[inline]
    pub(crate) fn is_row_input(&self) -> bool {
        match self {
            Self::KVToKV { is_row_input, .. } => *is_row_input,
            _ => panic!("Planner error: is_row_input is only available for KVToKV"),
        }
    }

    /// Whether the output is row-based.
    #[inline]
    pub(crate) fn is_row_output(&self) -> bool {
        match self {
            Self::KVToKV { is_row_output, .. }
            | Self::JoinToKV { is_row_output, .. }
            | Self::AntiJoinToKV { is_row_output, .. } => *is_row_output,
        }
    }

    // Layout getters

    /// Input layout(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub(crate) fn input_kv_layout(&self) -> (&KeyValueLayout, Option<&KeyValueLayout>) {
        match self {
            Self::KVToKV {
                input_kv_layout, ..
            } => (input_kv_layout, None),
            Self::JoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                ..
            } => (left_input_kv_layout, Some(right_input_kv_layout)),
        }
    }

    /// Output layout (key/value positions).
    #[inline]
    pub(crate) fn output_kv_layout(&self) -> &KeyValueLayout {
        match self {
            Self::KVToKV {
                output_kv_layout, ..
            }
            | Self::JoinToKV {
                output_kv_layout, ..
            }
            | Self::AntiJoinToKV {
                output_kv_layout, ..
            } => output_kv_layout,
        }
    }

    // Layout modifier

    /// Input layout modifier for SIP premap transformations; only applicable to KVToKV transformations.
    #[inline]
    pub(crate) fn update_input_layout(&mut self, new_input_kv_layout: KeyValueLayout) {
        match self {
            Self::KVToKV {
                input_kv_layout, ..
            } => {
                *input_kv_layout = new_input_kv_layout;
            }
            _ => panic!(
                "Planner error: update_input_layout is only applicable to KVToKV transformations"
            ),
        }
    }

    /// Predicate filters for KVToKV transformations.
    #[cfg(test)]
    #[inline]
    pub(crate) fn kv_predicates(&self) -> &KvPredicates {
        match self {
            Self::KVToKV { predicates, .. } => predicates,
            _ => panic!("Planner error: kv_predicates is only available for KVToKV"),
        }
    }

    /// Variant tag mixed into the output fingerprint; names the
    /// [`Transformation`](crate::planner::Transformation) variant this info
    /// materializes into, so equal fingerprints imply the same variant.
    fn content_tag(&self) -> &'static str {
        match self {
            Self::KVToKV { .. } => match (self.is_row_input(), self.is_row_output()) {
                (true, true) => "row_to_row",
                (true, false) => "row_to_kv",
                (false, true) => "kv_to_row",
                (false, false) => "kv_to_kv",
            },
            Self::JoinToKV { .. } => {
                if self.is_row_output() {
                    "jn_to_row"
                } else {
                    "jn_to_kv"
                }
            }
            Self::AntiJoinToKV { .. } => {
                if self.is_row_output() {
                    "njn_to_row"
                } else {
                    "njn_to_kv"
                }
            }
        }
    }

    /// Lower the current layouts and predicates to the positional
    /// [`TransformationFlow`] this info materializes into. The flow
    /// references collection slots, not atom-argument signatures, which is
    /// what keeps the output fingerprint free of rule-local positions.
    pub(crate) fn flow(&self) -> TransformationFlow {
        match self {
            Self::KVToKV {
                input_kv_layout,
                output_kv_layout,
                predicates,
                ..
            } => TransformationFlow::kv_to_kv(input_kv_layout, output_kv_layout, predicates),
            Self::JoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                predicates,
                ..
            } => TransformationFlow::join_to_kv(
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                predicates,
            ),
            Self::AntiJoinToKV {
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                ..
            } => TransformationFlow::join_to_kv(
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                // Antijoins carry no comparison predicates.
                &JoinPredicates::default(),
            ),
        }
    }
}

// ========================
// Mutating Methods
// ========================
impl TransformationInfo {
    /// Rewire one input port to a new collection fingerprint.
    ///
    /// Port-addressed counterpart of [`Self::update_input_fp`]: use it when
    /// ports need *different* targets. One collection can feed both sides
    /// of a join, so a fingerprint cannot single out a port; `is_left` can.
    /// It selects the left input of a join/anti-join, and the single input
    /// of a unary transformation counts as its left.
    ///
    /// # Panics
    ///
    /// Panics if `is_left` is `false` on a unary transformation.
    pub(crate) fn set_input_fp(&mut self, is_left: bool, new_fp: u64) {
        assert!(
            is_left || !matches!(self, Self::KVToKV { .. }),
            "Planner error: set_input_fp: no right input on a unary transformation"
        );
        match self {
            Self::KVToKV { input_info_fp, .. } => {
                *input_info_fp = new_fp;
            }
            Self::JoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                ..
            } => {
                if is_left {
                    *left_input_info_fp = new_fp;
                } else {
                    *right_input_info_fp = new_fp;
                }
            }
        }
    }

    /// Rename a consumed collection: re-point every input port holding
    /// `old_fp` to `new_fp`.
    ///
    /// Value-addressed counterpart of [`Self::set_input_fp`]: safe exactly
    /// because every matching port gets the *same* target, a self-join
    /// reading `old_fp` on both sides moves both in one call. When ports
    /// need different targets, address them via [`Self::set_input_fp`]
    /// instead.
    ///
    /// # Panics
    ///
    /// For binary operations, panics if `old_fp` matches neither input.
    pub(crate) fn update_input_fp(&mut self, new_fp: u64, old_fp: &u64) {
        let (left_fp, right_fp) = self.input_info_fp();
        let Some(right_fp) = right_fp else {
            self.set_input_fp(true, new_fp);
            return;
        };
        // Both sides may hold the same collection (a self-join over one
        // shared arrangement); patch every side that matches.
        let mut matched = false;
        if left_fp == *old_fp {
            self.set_input_fp(true, new_fp);
            matched = true;
        }
        if right_fp == *old_fp {
            self.set_input_fp(false, new_fp);
            matched = true;
        }
        assert!(
            matched,
            "Planner error: update_input_fp: {old_fp:#018x} matches neither input"
        );
    }

    /// Update whether the output is row-based.
    pub(crate) fn update_row_output(&mut self, is_row_output: bool) {
        match self {
            Self::KVToKV {
                is_row_output: row_out,
                ..
            }
            | Self::JoinToKV {
                is_row_output: row_out,
                ..
            }
            | Self::AntiJoinToKV {
                is_row_output: row_out,
                ..
            } => {
                *row_out = is_row_output;
            }
        }
    }

    /// Update the hierarchical output name.
    ///
    /// Used by the fuse phase when a map transformation is absorbed into its
    /// producer: the producer now semantically emits what the fused map used
    /// to emit, so its `output_name` must reflect that.
    pub(crate) fn update_output_name(&mut self, new_output_name: String) {
        match self {
            Self::KVToKV { output_name, .. }
            | Self::JoinToKV { output_name, .. }
            | Self::AntiJoinToKV { output_name, .. } => {
                *output_name = new_output_name;
            }
        }
    }

    /// Replace the draft output layout with its resolved positions.
    ///
    /// Necessary once the actual output schema is known, since downstream operators
    /// (e.g., joins) require concrete key/value layouts.
    pub(crate) fn update_output_key_value_layout(&mut self, real_output_kv_layout: KeyValueLayout) {
        match self {
            Self::KVToKV {
                output_kv_layout, ..
            }
            | Self::JoinToKV {
                output_kv_layout, ..
            }
            | Self::AntiJoinToKV {
                output_kv_layout, ..
            } => {
                *output_kv_layout = real_output_kv_layout;
            }
        }
    }

    /// Refactor the output key/value layout by splitting at a given key offset.
    ///
    /// Necessary when the actual key/value split is known, e.g., after downstream
    /// join operators determine the key-value layout.
    pub(crate) fn refactor_output_key_value_layout(
        &mut self,
        real_key_indices: &[usize],
        real_value_indices: &[usize],
    ) {
        match self {
            Self::KVToKV {
                output_kv_layout,
                output_info_fp,
                ..
            }
            | Self::JoinToKV {
                output_kv_layout,
                output_info_fp,
                ..
            }
            | Self::AntiJoinToKV {
                output_kv_layout,
                output_info_fp,
                ..
            } => {
                let all_positions: Vec<ArithmeticPos> = output_kv_layout
                    .key()
                    .iter()
                    .chain(output_kv_layout.value().iter())
                    .cloned()
                    .collect();

                let remap = |indices: &[usize]| -> Vec<ArithmeticPos> {
                    indices
                        .iter()
                        .map(|idx| {
                            all_positions.get(*idx).cloned().unwrap_or_else(|| {
                                panic!(
                                    "Planner error: 0x{:016x} output layout index {} out of bounds (len {})",
                                    output_info_fp,
                                    idx,
                                    all_positions.len()
                                )
                            })
                        })
                        .collect()
                };

                *output_kv_layout =
                    KeyValueLayout::new(remap(real_key_indices), remap(real_value_indices));
            }
        }
    }

    /// Update comparison expressions for transformations that support them.
    ///
    /// Comparison expressions should be added incrementally.
    pub(crate) fn update_comparisons(
        &mut self,
        new_compare_exprs: Vec<ComparisonExprPos>,
    ) -> Result<(), PlanError> {
        match self {
            Self::KVToKV { predicates, .. } => {
                predicates.compare_exprs.extend(new_compare_exprs);
                Ok(())
            }
            Self::JoinToKV { predicates, .. } => {
                predicates.compare_exprs.extend(new_compare_exprs);
                Ok(())
            }
            Self::AntiJoinToKV { .. } => Err(PlanError::internal(
                "update_comparisons: AntiJoinToKV has no comparisons to update",
            )),
        }
    }

    /// Update constant equality constraints, avoiding duplicates.
    pub(crate) fn update_const_eq_and_var_eq_constraints(
        &mut self,
        const_eq: Vec<(AtomArgumentSignature, Constant)>,
        var_eq: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
    ) -> Result<(), PlanError> {
        match self {
            Self::KVToKV { predicates, .. } => {
                predicates.const_eq.extend(const_eq);
                predicates.var_eq.extend(var_eq);
                Ok(())
            }
            Self::JoinToKV { .. } | Self::AntiJoinToKV { .. } => Err(PlanError::internal(
                "update_const_eq_and_var_eq_constraints: only applicable to unary (KVToKV) transformations",
            )),
        }
    }

    /// Recompute the output fingerprint from the current fields: the variant
    /// tag, the input fingerprint(s), and the positional flow. Nothing else
    /// enters the hash, so the fingerprint is free of rule-local atom
    /// positions at every stage. Call after any mutation of inputs, layouts,
    /// flags, or constraints.
    pub(crate) fn refresh_output_fp(&mut self) {
        let (left_fp, right_fp) = self.input_info_fp();
        let fp = compute_fp((self.content_tag(), left_fp, right_fp, &self.flow()));
        match self {
            Self::KVToKV { output_info_fp, .. }
            | Self::JoinToKV { output_info_fp, .. }
            | Self::AntiJoinToKV { output_info_fp, .. } => *output_info_fp = fp,
        }
    }
}

impl TransformationInfo {
    /// Display label mirroring [`crate::planner::Transformation::operation_name`].
    pub(crate) fn operation_name(&self) -> &'static str {
        match self {
            Self::KVToKV { .. } => match (self.is_row_input(), self.is_row_output()) {
                (true, true) => "[Row -> Row]",
                (true, false) => "[Row -> KV]",
                (false, true) => "[KV -> Row]",
                (false, false) => "[KV -> KV]",
            },
            Self::JoinToKV { .. } => {
                if self.is_row_output() {
                    "[Join -> Row]"
                } else {
                    "[Join -> KV]"
                }
            }
            Self::AntiJoinToKV { .. } => {
                if self.is_row_output() {
                    "[AntiJoin -> Row]"
                } else {
                    "[AntiJoin -> KV]"
                }
            }
        }
    }
}

impl fmt::Display for TransformationInfo {
    /// Multi-line block form:
    /// ```text
    /// [Join -> KV]
    ///     Left : (reach ⋈[y] arc) [0x....], key:(..), value:(..)
    ///     Right: arc [0x....], key:(..), value:(..)
    ///     Out  : ((reach ⋈[y] arc) ⋈[y] arc) [0x....], key:(..), value:(..)
    ///     F    : (if x = 5 and y > 0)
    /// ```
    ///
    /// Each collection is rendered as `<hierarchical-name> [0x<fingerprint>], key:(..), value:(..)`.
    /// The name encodes the full construction path from EDBs (composed by
    /// each phase's constructor); the fingerprint is the disambiguating
    /// identity. Unlike [`crate::planner::Transformation`], there is no `Flow` line —
    /// the `TransformationFlow` is only materialized when a `Transformation`
    /// is built from this info. The `F` line is omitted when no predicates
    /// apply.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let coll = |fp: u64, name: &str, kv: &KeyValueLayout| {
            Collection::new(fp, name.to_string(), kv.key(), kv.value())
        };

        writeln!(f, "{}", self.operation_name())?;
        match self {
            Self::KVToKV {
                input_info_fp,
                input_name,
                output_info_fp,
                output_name,
                input_kv_layout,
                output_kv_layout,
                predicates,
                ..
            } => {
                writeln!(
                    f,
                    "    In   : {}",
                    coll(*input_info_fp, input_name, input_kv_layout)
                )?;
                writeln!(
                    f,
                    "    Out  : {}",
                    coll(*output_info_fp, output_name, output_kv_layout)
                )?;
                if !predicates.is_empty() {
                    writeln!(f, "    F    : (if {})", predicates)?;
                }
            }
            Self::JoinToKV {
                left_input_info_fp,
                left_input_name,
                right_input_info_fp,
                right_input_name,
                output_info_fp,
                output_name,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                predicates,
                ..
            } => {
                writeln!(
                    f,
                    "    Left : {}",
                    coll(*left_input_info_fp, left_input_name, left_input_kv_layout)
                )?;
                writeln!(
                    f,
                    "    Right: {}",
                    coll(
                        *right_input_info_fp,
                        right_input_name,
                        right_input_kv_layout
                    )
                )?;
                writeln!(
                    f,
                    "    Out  : {}",
                    coll(*output_info_fp, output_name, output_kv_layout)
                )?;
                if !predicates.is_empty() {
                    writeln!(f, "    F    : (if {})", predicates)?;
                }
            }
            Self::AntiJoinToKV {
                left_input_info_fp,
                left_input_name,
                right_input_info_fp,
                right_input_name,
                output_info_fp,
                output_name,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                ..
            } => {
                writeln!(
                    f,
                    "    Left : {}",
                    coll(*left_input_info_fp, left_input_name, left_input_kv_layout)
                )?;
                writeln!(
                    f,
                    "    Right: {}",
                    coll(
                        *right_input_info_fp,
                        right_input_name,
                        right_input_kv_layout
                    )
                )?;
                writeln!(
                    f,
                    "    Out  : {}",
                    coll(*output_info_fp, output_name, output_kv_layout)
                )?;
            }
        }
        Ok(())
    }
}
