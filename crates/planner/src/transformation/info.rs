//! Transformation information used during query planning.
//!
//! A transformation info describes how to transform input collections
//! (with their key/value layouts) into an output collection (with its
//! key/value layout), along with any constraints (constant/variable
//! equalities, comparisons) that must hold for the transformation to
//! produce an output tuple.
//!
//! These are high-level descriptions that do not yet refer to concrete
//! collections (with their actual schemas). Instead, they use fake
//! fingerprints and key/value layouts as placeholders, which are later
//! replaced with real ones once they are known. This allows building
//! a transformation plan before all details are finalized.

use std::hash::Hash;

use catalog::{
    ArithmeticPos, AtomArgumentSignature, ComparisonExprPos, FnCallPredicatePos, JoinPredicates,
    KvPredicates,
};
use common::compute_fp;
use parser::ConstType;

use crate::collection::Collection;

/// Key/Value layout of a collection: which positions form the key-value.
#[derive(PartialEq, Clone, Eq, Hash, Debug)]
pub struct KeyValueLayout {
    pub key: Vec<ArithmeticPos>,
    pub value: Vec<ArithmeticPos>,
}

impl KeyValueLayout {
    /// Construct a new Key-Value layout.
    #[inline]
    pub fn new(key: Vec<ArithmeticPos>, value: Vec<ArithmeticPos>) -> Self {
        Self { key, value }
    }

    /// Reference to key positions.
    #[inline]
    pub fn key(&self) -> &[ArithmeticPos] {
        &self.key
    }

    /// Reference to value positions.
    #[inline]
    pub fn value(&self) -> &[ArithmeticPos] {
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
    pub fn extract_argument_ids_from_layout(&self) -> (Vec<usize>, Vec<usize>) {
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
    pub fn extract_atom_id(&self) -> usize {
        self.key()
            .iter()
            .chain(self.value().iter())
            .flat_map(|pos| pos.signatures())
            .map(|sig| sig.atom_signature().rhs_id())
            .next()
            .unwrap_or_else(|| {
                panic!("Planner error: attempted to extract atom ID from empty key/value layout")
            })
    }
}

/// Transformation information, describing how to transform input collection(s)
/// into an output collection, along with any constraints that must hold.
#[derive(PartialEq, Clone, Eq, Hash, Debug)]
pub enum TransformationInfo {
    /// Unary Key-Value to Key-Value transformation (filter, map, projection, etc.).
    KVToKV {
        /// Upstream (input) collection fingerprint (fake until resolved).
        input_info_fp: u64,
        /// Upstream collection's hierarchical name (e.g. `π[x](reach)`).
        input_name: String,
        /// Output collection fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Output collection's hierarchical name.
        output_name: String,
        /// Whether row input
        is_row_input: bool,
        /// Whether row output
        is_row_output: bool,
        /// Input layout (key/value positions).
        input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
        /// Filter predicates (equality constraints, comparisons, UDF predicates).
        predicates: KvPredicates,
        /// SIP projection
        is_sip_projection: bool,
    },

    /// Binary Join to Key-Value transformation.
    JoinToKV {
        /// Left input collection fingerprint.
        left_input_info_fp: u64,
        /// Left input's hierarchical name.
        left_input_name: String,
        /// Right input collection fingerprint.
        right_input_info_fp: u64,
        /// Right input's hierarchical name.
        right_input_name: String,
        /// Output collection fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Output collection's hierarchical name (e.g. `(reach ⋈[y] arc)`).
        output_name: String,
        /// Whether row output
        is_row_output: bool,
        /// Left input layout (its key is the join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value contributes to output value).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
        /// Filter predicates (comparisons and UDF predicates).
        predicates: JoinPredicates,
    },

    /// Binary Anti-Join to Key-Value transformation.
    AntiJoinToKV {
        /// Left input collection fingerprint.
        left_input_info_fp: u64,
        /// Left input's hierarchical name.
        left_input_name: String,
        /// Right input collection fingerprint.
        right_input_info_fp: u64,
        /// Right input's hierarchical name.
        right_input_name: String,
        /// Output collection fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Output collection's hierarchical name (e.g. `(reach ▷[y] arc)`).
        output_name: String,
        /// Whether row output
        is_row_output: bool,
        /// Left input layout (its key is the anti-join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value is ignored in the output, but key participates).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
    },
}

// ========================
// Constructors
// ========================
impl TransformationInfo {
    /// Build a Key-Value to Key-Value transformation with a derived (fake) output fingerprint.
    pub fn kv_to_kv(
        input_fake_sig: u64,
        input_name: String,
        output_name: String,
        is_row_input: bool,
        input_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
        predicates: KvPredicates,
    ) -> Self {
        let fake_output_sig = compute_fp((
            "kv_to_kv",
            &input_fake_sig,
            &input_kv_layout,
            &output_fake_kv_layout,
            &predicates,
        ));
        Self::KVToKV {
            input_info_fp: input_fake_sig,
            input_name,
            output_info_fp: fake_output_sig,
            output_name,
            is_row_input,
            is_row_output: false,
            input_kv_layout,
            output_kv_layout: output_fake_kv_layout,
            predicates,
            is_sip_projection: false,
        }
    }

    /// Mark this Key-Value transformation as a SIP projection.
    pub fn into_sip_projection(mut self) -> Self {
        match &mut self {
            Self::KVToKV {
                is_sip_projection, ..
            } => *is_sip_projection = true,
            _ => panic!("Planner error: into_sip_projection is only applicable to KVToKV"),
        }
        self
    }

    /// Build a Join to Key-Value transformation with a derived (fake) output fingerprint.
    #[allow(clippy::too_many_arguments)]
    pub fn join_to_kv(
        left_fake_sig: u64,
        left_input_name: String,
        right_fake_sig: u64,
        right_input_name: String,
        output_name: String,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
        predicates: JoinPredicates,
    ) -> Self {
        let fake_output_sig = compute_fp((
            "join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_kv_layout,
            &right_kv_layout,
            &output_fake_kv_layout,
            &predicates,
        ));
        Self::JoinToKV {
            left_input_info_fp: left_fake_sig,
            left_input_name,
            right_input_info_fp: right_fake_sig,
            right_input_name,
            output_info_fp: fake_output_sig,
            output_name,
            is_row_output: false,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout: output_fake_kv_layout,
            predicates,
        }
    }

    /// Build an AntiJoin to Key-Value transformation with a derived (fake) output fingerprint.
    #[allow(clippy::too_many_arguments)]
    pub fn anti_join_to_kv(
        left_fake_sig: u64,
        left_input_name: String,
        right_fake_sig: u64,
        right_input_name: String,
        output_name: String,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
    ) -> Self {
        let fake_output_sig = compute_fp((
            "anti_join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_kv_layout,
            &right_kv_layout,
            &output_fake_kv_layout,
        ));

        Self::AntiJoinToKV {
            left_input_info_fp: left_fake_sig,
            left_input_name,
            right_input_info_fp: right_fake_sig,
            right_input_name,
            output_info_fp: fake_output_sig,
            output_name,
            is_row_output: false,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout: output_fake_kv_layout,
        }
    }
}

// ========================
// Getters
// ========================
impl TransformationInfo {
    // Type checking methods

    /// Whether this is a neg join transformation info.
    #[inline]
    pub fn is_neg_join(&self) -> bool {
        matches!(self, Self::AntiJoinToKV { .. })
    }

    // Fingerprint getters

    /// Input fingerprint(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub fn input_info_fp(&self) -> (u64, Option<u64>) {
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
    pub fn output_info_fp(&self) -> u64 {
        match self {
            Self::KVToKV { output_info_fp, .. }
            | Self::JoinToKV { output_info_fp, .. }
            | Self::AntiJoinToKV { output_info_fp, .. } => *output_info_fp,
        }
    }

    /// Input hierarchical name(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub fn input_name(&self) -> (&str, Option<&str>) {
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
    pub fn output_name(&self) -> &str {
        match self {
            Self::KVToKV { output_name, .. }
            | Self::JoinToKV { output_name, .. }
            | Self::AntiJoinToKV { output_name, .. } => output_name.as_str(),
        }
    }

    /// Whether the input is row-based.
    /// Only KVtoKV needs this info.
    #[inline]
    pub fn is_row_input(&self) -> bool {
        match self {
            Self::KVToKV { is_row_input, .. } => *is_row_input,
            _ => panic!("Planner error: is_row_input is only available for KVToKV"),
        }
    }

    /// Whether the output is row-based.
    #[inline]
    pub fn is_row_output(&self) -> bool {
        match self {
            Self::KVToKV { is_row_output, .. } => *is_row_output,
            Self::JoinToKV { is_row_output, .. } => *is_row_output,
            Self::AntiJoinToKV { is_row_output, .. } => *is_row_output,
        }
    }

    // Layout getters

    /// Input layout(s); for joins/anti-joins returns `(left, Some(right))`.
    #[inline]
    pub fn input_kv_layout(&self) -> (&KeyValueLayout, Option<&KeyValueLayout>) {
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
    pub fn output_kv_layout(&self) -> &KeyValueLayout {
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
    pub fn update_input_layout(&mut self, new_input_kv_layout: KeyValueLayout) {
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
    #[inline]
    pub fn kv_predicates(&self) -> &KvPredicates {
        match self {
            Self::KVToKV { predicates, .. } => predicates,
            _ => panic!("Planner error: kv_predicates is only available for KVToKV"),
        }
    }

    /// Predicate filters for JoinToKV transformations.
    #[inline]
    pub fn join_predicates(&self) -> &JoinPredicates {
        match self {
            Self::JoinToKV { predicates, .. } => predicates,
            _ => panic!("Planner error: join_predicates is only available for JoinToKV"),
        }
    }
}

// ========================
// Mutating Methods
// ========================
impl TransformationInfo {
    /// Replace a placeholder (fake) input fingerprint with the resolved (real) one.
    ///
    /// This method updates the input fingerprint after an upstream transformation
    /// finalizes its output fingerprint. For binary operations (joins/anti-joins),
    /// it automatically determines which input (left or right) to update based on
    /// the provided fake signature.
    ///
    /// # Arguments
    ///
    /// * `input_real_sig` - The resolved (real) fingerprint to use
    /// * `input_fake_sig` - The placeholder fingerprint to replace
    ///
    /// # Panics
    ///
    /// For binary operations, panics if `input_fake_sig` doesn't match either
    /// the left or right input fingerprint.
    pub fn update_input_fake_info_fp(&mut self, input_real_sig: u64, input_fake_sig: &u64) {
        match self {
            Self::KVToKV { input_info_fp, .. } => {
                *input_info_fp = input_real_sig;
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
                if left_input_info_fp == input_fake_sig {
                    *left_input_info_fp = input_real_sig;
                } else {
                    *right_input_info_fp = input_real_sig;
                }
            }
        }
    }

    /// Update whether the output is row-based.
    pub fn update_row_output(&mut self, is_row_output: bool) {
        match self {
            Self::KVToKV {
                is_row_output: row_out,
                ..
            } => {
                *row_out = is_row_output;
            }
            Self::JoinToKV {
                is_row_output: row_out,
                ..
            } => {
                *row_out = is_row_output;
            }
            Self::AntiJoinToKV {
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
    pub fn update_output_name(&mut self, new_output_name: String) {
        match self {
            Self::KVToKV { output_name, .. }
            | Self::JoinToKV { output_name, .. }
            | Self::AntiJoinToKV { output_name, .. } => {
                *output_name = new_output_name;
            }
        }
    }

    /// Replace a placeholder (fake) output layout with its resolved (real) positions.
    ///
    /// Necessary once the actual output schema is known, since downstream operators
    /// (e.g., joins) require concrete key/value layouts.
    pub fn update_output_key_value_layout(&mut self, real_output_kv_layout: KeyValueLayout) {
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
    pub fn refactor_output_key_value_layout(
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
    pub fn update_comparisons(&mut self, new_compare_exprs: Vec<ComparisonExprPos>) {
        match self {
            Self::KVToKV { predicates, .. } => predicates.compare_exprs.extend(new_compare_exprs),
            Self::JoinToKV { predicates, .. } => predicates.compare_exprs.extend(new_compare_exprs),
            Self::AntiJoinToKV { .. } => {
                panic!("Planner error: AntiJoinToKV has no comparisons to update");
            }
        }
    }

    /// Update boolean UDF predicate filters for transformations that support them.
    ///
    /// UDF predicates should be added incrementally.
    pub fn update_fn_call_preds(&mut self, new_fn_call_preds: Vec<FnCallPredicatePos>) {
        match self {
            Self::KVToKV { predicates, .. } => predicates.fn_call_preds.extend(new_fn_call_preds),
            Self::JoinToKV { predicates, .. } => predicates.fn_call_preds.extend(new_fn_call_preds),
            Self::AntiJoinToKV { .. } => {
                panic!("Planner error: AntiJoinToKV has no fn_call_preds to update");
            }
        }
    }

    /// Update constant equality constraints, avoiding duplicates.
    pub fn update_const_eq_and_var_eq_constraints(
        &mut self,
        const_eq: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
    ) {
        match self {
            Self::KVToKV { predicates, .. } => {
                predicates.const_eq.extend(const_eq);
                predicates.var_eq.extend(var_eq);
            }
            Self::JoinToKV { .. } | Self::AntiJoinToKV { .. } => {
                panic!("Planner error: attempting to append const constraints to non-unary transformation")
            }
        }
    }

    /// Recompute the (fake) output fingerprint using the current resolved fields.
    ///
    /// Call this after all relevant inputs/layouts/constraints are up-to-date.
    pub fn update_output_fake_sig(&mut self) {
        match self {
            Self::KVToKV {
                input_info_fp,
                is_row_input,
                is_row_output,
                input_kv_layout,
                output_kv_layout,
                predicates,
                output_info_fp,
                ..
            } => {
                *output_info_fp = compute_fp((
                    "kv_to_kv",
                    input_info_fp,
                    is_row_input,
                    is_row_output,
                    input_kv_layout,
                    output_kv_layout,
                    predicates,
                ));
            }
            Self::JoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                is_row_output,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                predicates,
                output_info_fp,
                ..
            } => {
                *output_info_fp = compute_fp((
                    "join_to_kv",
                    left_input_info_fp,
                    right_input_info_fp,
                    is_row_output,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    output_kv_layout,
                    predicates,
                ));
            }
            Self::AntiJoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                is_row_output,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                output_info_fp,
                ..
            } => {
                *output_info_fp = compute_fp((
                    "anti_join_to_kv",
                    left_input_info_fp,
                    right_input_info_fp,
                    is_row_output,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    output_kv_layout,
                ));
            }
        }
    }
}

impl TransformationInfo {
    /// Display label mirroring [`crate::Transformation::operation_name`].
    pub fn operation_name(&self) -> &'static str {
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

impl std::fmt::Display for TransformationInfo {
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
    /// identity. Unlike [`crate::Transformation`], there is no `Flow` line —
    /// the `TransformationFlow` is only materialized when a `Transformation`
    /// is built from this info. The `F` line is omitted when no predicates
    /// apply.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
