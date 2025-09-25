//! Transformation information for query planning in Macaron Datalog programs.
//!
//! This module stores all metadata needed to generate transformations during
//! the query planning phase. `TransformationInfo` captures the essential info
//! about planned transformations before they are converted to executable ones.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos};
use parser::ConstType;

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
}

/// Transformation information, prior to translate to real executable transformation.
#[derive(PartialEq, Clone, Eq, Hash)]
pub enum TransformationInfo {
    /// Unary Key-Value to Key-Value transformation (filter, map, projection, etc.).
    KVToKV {
        /// Upstream (input) collection fingerprint (fake until resolved).
        input_info_fp: u64,
        /// This transformation's output fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Input layout (key/value positions).
        input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
        /// Constant equality constraints (lhs = const).
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        /// Variable equality constraints (lhs = rhs).
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        /// Comparison expressions (e.g., x < y).
        compare_exprs_pos: Vec<ComparisonExprPos>,
    },

    /// Binary Join to Key-Value transformation.
    JoinToKV {
        /// Left input fingerprint.
        left_input_info_fp: u64,
        /// Right input fingerprint.
        right_input_info_fp: u64,
        /// Output fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Left input layout (its key is the join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value contributes to output value).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
        /// Join comparisons (if any).
        compare_exprs_pos: Vec<ComparisonExprPos>,
    },

    /// Binary Anti-Join to Key-Value transformation.
    AntiJoinToKV {
        /// Left input fingerprint.
        left_input_info_fp: u64,
        /// Right input fingerprint.
        right_input_info_fp: u64,
        /// Output fingerprint (fake until resolved).
        output_info_fp: u64,
        /// Left input layout (its key is the anti-join key).
        left_input_kv_layout: KeyValueLayout,
        /// Right input layout (its value is ignored in the output, but key participates).
        right_input_kv_layout: KeyValueLayout,
        /// Output layout (key/value positions) (fake until resolved).
        output_kv_layout: KeyValueLayout,
    },
}

impl TransformationInfo {
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

    /// Output fingerprint (fake until resolved).
    #[inline]
    pub fn output_info_fp(&self) -> u64 {
        match self {
            Self::KVToKV { output_info_fp, .. }
            | Self::JoinToKV { output_info_fp, .. }
            | Self::AntiJoinToKV { output_info_fp, .. } => *output_info_fp,
        }
    }

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

    /// Constant equality constraints (Key-Value to Key-Value only).
    #[inline]
    pub fn const_eq_constraints(&self) -> &[(AtomArgumentSignature, ConstType)] {
        match self {
            Self::KVToKV {
                const_eq_constraints,
                ..
            } => const_eq_constraints,
            Self::JoinToKV { .. } => panic!("JoinToKV has no const_eq_constraints"),
            Self::AntiJoinToKV { .. } => panic!("AntiJoinToKV has no const_eq_constraints"),
        }
    }

    /// Variable equality constraints (Key-Value to Key-Value only).
    #[inline]
    pub fn var_eq_constraints(&self) -> &[(AtomArgumentSignature, AtomArgumentSignature)] {
        match self {
            Self::KVToKV {
                var_eq_constraints, ..
            } => var_eq_constraints,
            Self::JoinToKV { .. } => panic!("JoinToKV has no var_eq_constraints"),
            Self::AntiJoinToKV { .. } => panic!("AntiJoinToKV has no var_eq_constraints"),
        }
    }

    /// Comparison expressions (Key-Value to Key-Value and Join to Key-Value).
    #[inline]
    pub fn compare_exprs(&self) -> &[ComparisonExprPos] {
        match self {
            Self::KVToKV {
                compare_exprs_pos, ..
            }
            | Self::JoinToKV {
                compare_exprs_pos, ..
            } => compare_exprs_pos,
            Self::AntiJoinToKV { .. } => panic!("AntiJoinToKV has no compare_exprs"),
        }
    }

    // ------------------------------------------------------------------------
    // Constructors (produce fake output fingerprints)
    // ------------------------------------------------------------------------

    /// Build a Key-Value to Key-Value transformation with a derived (fake) output fingerprint.
    pub fn kv_to_kv(
        input_fake_sig: u64,
        input_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs_pos: Vec<ComparisonExprPos>,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "kv_to_kv",
            &input_fake_sig,
            &input_kv_layout,
            &output_fake_kv_layout,
            &const_eq_constraints,
            &var_eq_constraints,
            &compare_exprs_pos,
        ));

        Self::KVToKV {
            input_info_fp: input_fake_sig,
            output_info_fp: fake_output_sig,
            input_kv_layout,
            output_kv_layout: output_fake_kv_layout,
            const_eq_constraints,
            var_eq_constraints,
            compare_exprs_pos,
        }
    }

    /// Build a Join to Key-Value transformation with a derived (fake) output fingerprint.
    pub fn join_to_kv(
        left_fake_sig: u64,
        right_fake_sig: u64,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
        compare_exprs_pos: Vec<ComparisonExprPos>,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_kv_layout,
            &right_kv_layout,
            &output_fake_kv_layout,
            &compare_exprs_pos,
        ));

        Self::JoinToKV {
            left_input_info_fp: left_fake_sig,
            right_input_info_fp: right_fake_sig,
            output_info_fp: fake_output_sig,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout: output_fake_kv_layout,
            compare_exprs_pos,
        }
    }

    /// Build an AntiJoin to Key-Value transformation with a derived (fake) output fingerprint.
    pub fn anti_join_to_kv(
        left_fake_sig: u64,
        right_fake_sig: u64,
        left_kv_layout: KeyValueLayout,
        right_kv_layout: KeyValueLayout,
        output_fake_kv_layout: KeyValueLayout,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "anti_join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_kv_layout,
            &right_kv_layout,
            &output_fake_kv_layout,
        ));

        Self::AntiJoinToKV {
            left_input_info_fp: left_fake_sig,
            right_input_info_fp: right_fake_sig,
            output_info_fp: fake_output_sig,
            left_input_kv_layout: left_kv_layout,
            right_input_kv_layout: right_kv_layout,
            output_kv_layout: output_fake_kv_layout,
        }
    }

    // ------------------------------------------------------------------------
    // Patching placeholders with resolved fingerprints/layouts
    // ------------------------------------------------------------------------

    /// Replace a placeholder (fake) input fingerprint with the resolved (real) one.
    ///
    /// Call this after an upstream transformation finalizes its output fingerprint.
    pub fn update_input_fake_info_fp(&mut self, input_real_sig: u64, left: bool) {
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
                if left {
                    *left_input_info_fp = input_real_sig;
                } else {
                    *right_input_info_fp = input_real_sig;
                }
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

    /// Recompute the (fake) output fingerprint using the current resolved fields.
    ///
    /// Call this after all relevant inputs/layouts/constraints are up-to-date.
    pub fn update_output_fake_sig(&mut self) {
        match self {
            Self::KVToKV {
                input_info_fp,
                input_kv_layout,
                output_kv_layout,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs_pos,
                output_info_fp,
            } => {
                *output_info_fp = compute_sig((
                    "kv_to_kv",
                    input_info_fp,
                    input_kv_layout,
                    output_kv_layout,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs_pos,
                ));
            }
            Self::JoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                compare_exprs_pos,
                output_info_fp,
            } => {
                *output_info_fp = compute_sig((
                    "join_to_kv",
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    output_kv_layout,
                    compare_exprs_pos,
                ));
            }
            Self::AntiJoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                output_info_fp,
            } => {
                *output_info_fp = compute_sig((
                    "anti_join_to_kv",
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    output_kv_layout,
                ));
            }
        }
    }
}

impl fmt::Display for TransformationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KVToKV {
                input_info_fp,
                input_kv_layout,
                output_info_fp,
                output_kv_layout,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs_pos,
            } => {
                let in_coll = fmt_collection(input_info_fp, input_kv_layout);
                let out_coll = fmt_collection(output_info_fp, output_kv_layout);
                let filters = fmt_flow_kv(
                    output_kv_layout,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs_pos,
                );

                if filters.is_empty() {
                    write!(
                        f,
                        "   ┌─ In   : {}\n         └─> Out : {}\n",
                        in_coll, out_coll
                    )
                } else {
                    write!(
                        f,
                        "   ┌─ In   : {}\n         └─> Out : {}\n       WHERE {}\n",
                        in_coll, out_coll, filters
                    )
                }
            }

            Self::JoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                output_info_fp,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
                compare_exprs_pos,
            } => {
                let l = fmt_collection(left_input_info_fp, left_input_kv_layout);

                // Right display uses the *join key* from the left and value positions from the right.
                let r = fmt_collection(
                    right_input_info_fp,
                    &KeyValueLayout::new(
                        left_input_kv_layout.key().to_vec(),
                        right_input_kv_layout.value().to_vec(),
                    ),
                );

                let out = fmt_collection(output_info_fp, output_kv_layout);
                let filters = fmt_flow_kv(output_kv_layout, &[], &[], compare_exprs_pos);

                if filters.is_empty() {
                    write!(
                        f,
                        "   ┌─ Left : {}\n         ├─ Right: {}\n         └─> Out : {}\n",
                        l, r, out
                    )
                } else {
                    write!(
                        f,
                        "   ┌─ Left : {}\n         ├─ Right: {}\n         └─> Out : {}\n       WHERE {}\n",
                        l, r, out, filters
                    )
                }
            }

            Self::AntiJoinToKV {
                left_input_info_fp,
                right_input_info_fp,
                output_info_fp,
                left_input_kv_layout,
                right_input_kv_layout,
                output_kv_layout,
            } => {
                let l = fmt_collection(left_input_info_fp, left_input_kv_layout);

                // Right display uses the *join key* from the left and value positions from the right.
                let r = fmt_collection(
                    right_input_info_fp,
                    &KeyValueLayout::new(
                        left_input_kv_layout.key().to_vec(),
                        right_input_kv_layout.value().to_vec(),
                    ),
                );

                let out = fmt_collection(output_info_fp, output_kv_layout);

                write!(
                    f,
                    "   ┌─ Left : {}\n         ├─ Right: {}\n         └─> Out : {}\n",
                    l, r, out
                )
            }
        }
    }
}

impl fmt::Debug for TransformationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/* ------------------------------- Helpers ---------------------------------- */

/// Computes a derived fingerprint by hashing all identifying inputs together.
///
/// NOTE: Uses `DefaultHasher` which is deterministic within a build but not
/// guaranteed stable across Rust versions. If you need long-term stability,
/// use a fixed hash (e.g., blake3) over a stable serialization.
fn compute_sig<T: Hash>(t: T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// Formats a collection as:
/// - with keys:    `ffffffffffffffff [key: (k1, k2), value: (v1, v2)]`
/// - without keys: `ffffffffffffffff [key: (), value: (v1, v2)]`
fn fmt_collection(sig: &u64, kv_layout: &KeyValueLayout) -> String {
    let k = kv_layout
        .key()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let v = kv_layout
        .value()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    if k.is_empty() {
        format!("{:016x} [key: (), value: ({})]", sig, v)
    } else {
        format!("{:016x} [key: ({}), value: ({})]", sig, k, v)
    }
}

/// Formats filters (const-eq, var-eq, comparisons) joined by `AND`.
fn fmt_flow_kv(
    _out_kv_layout: &KeyValueLayout,
    consts: &[(AtomArgumentSignature, ConstType)],
    vars: &[(AtomArgumentSignature, AtomArgumentSignature)],
    comps: &[ComparisonExprPos],
) -> String {
    let consts_str = consts
        .iter()
        .map(|(sig, c)| format!("{} = {:?}", sig, c))
        .collect::<Vec<_>>()
        .join(" AND ");

    let vars_str = vars
        .iter()
        .map(|(l, r)| format!("{} = {}", l, r))
        .collect::<Vec<_>>()
        .join(" AND ");

    let comps_str = comps
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut parts = Vec::new();
    if !consts.is_empty() {
        parts.push(consts_str);
    }
    if !vars.is_empty() {
        parts.push(vars_str);
    }
    if !comps.is_empty() {
        parts.push(comps_str);
    }
    parts.join(" AND ")
}
