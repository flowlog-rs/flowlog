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

#[derive(PartialEq, Clone, Eq, Hash)]
pub struct KeyValueLayout {
    pub key: Vec<ArithmeticPos>,
    pub value: Vec<ArithmeticPos>,
}

impl KeyValueLayout {
    /// Creates a new KeyValueLayout.
    pub fn new(key: Vec<ArithmeticPos>, value: Vec<ArithmeticPos>) -> Self {
        Self { key, value }
    }

    /// Get a reference to the key arithmetic positions.
    pub fn key(&self) -> &[ArithmeticPos] {
        &self.key
    }

    /// Get a reference to the value arithmetic positions.
    pub fn value(&self) -> &[ArithmeticPos] {
        &self.value
    }
}

#[derive(PartialEq, Clone, Eq, Hash)]
pub enum TransformationInfo {
    KVToKV {
        input_info_fp: u64,
        output_info_fp: u64,
        input_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs_pos: Vec<ComparisonExprPos>,
    },

    JoinToKV {
        left_input_info_fp: u64,
        right_input_info_fp: u64,
        output_info_fp: u64,
        left_input_kv_layout: KeyValueLayout,
        right_input_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
        compare_exprs_pos: Vec<ComparisonExprPos>,
    },

    AntiJoinToKV {
        left_input_info_fp: u64,
        right_input_info_fp: u64,
        output_info_fp: u64,
        left_input_kv_layout: KeyValueLayout,
        right_input_kv_layout: KeyValueLayout,
        output_kv_layout: KeyValueLayout,
    },
}

impl TransformationInfo {
    /// Get input information fingerprint(s); for joins, returns (left, Some(right))
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

    /// Get output information fingerprint.
    #[inline]
    pub fn output_info_fp(&self) -> u64 {
        match self {
            Self::KVToKV { output_info_fp, .. }
            | Self::JoinToKV { output_info_fp, .. }
            | Self::AntiJoinToKV { output_info_fp, .. } => *output_info_fp,
        }
    }

    /// Get a reference to the input key-value layouts; for joins, returns (left, Some(right)).
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

    /// Get a reference to the output key-value layout.
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

    /// Get a reference to the constant equality constraints (key-value to key-value only).
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

    /// Get a reference to the variable equality constraints (key-value to key-value only).
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

    /// Get a reference to the comparison expressions (key-value to key-value & join to key-value).
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

    /// Build a key-value to key-value transformation with a derived (fake) output signature.
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

    /// Build a join to key-value transformation with a derived (fake) output signature.
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

    /// Build a negated join to key-value transformation with a derived (fake) output signature.
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

    /// Updates the placeholder (fake) input info fingerprint with the resolved (real) info fingerprint.
    ///
    /// Call this after a prior transformation has produced a real output info fingerprint
    /// for the upstream collection (e.g., after key/value layout changes or map-fusion).
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

    /// Updates the placeholder (fake) output key/value layouts with resolved real layouts.
    ///
    /// This step is necessary once the actual output layout is known, since later
    /// operators—particularly joins—require concrete key/value layouts to proceed.
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
                        "   ┌─ In   : {}\n         └─> Out : {}\n      WHERE {}\n",
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
                // Right side uses the *join key* for display, and its own value positions.
                let r = fmt_collection(right_input_info_fp, right_input_kv_layout);
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
                        "   ┌─ Left : {}\n         ├─ Right: {}\n         └─> Out : {}\n      WHERE {}\n",
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
                // Right side uses the *join key* for display, and its own value positions.
                let r = fmt_collection(right_input_info_fp, right_input_kv_layout);
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

/* --- helpers -------------------------------------------------------------- */

/// Computes a derived signature by hashing all identifying inputs together.
fn compute_sig<T: Hash>(t: T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// Formats a collection-like string
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

/// Formats a flow-like string: returns only the combined filter expression (no wrappers)
fn fmt_flow_kv(
    out_kv_layout: &KeyValueLayout,
    consts: &[(AtomArgumentSignature, ConstType)],
    vars: &[(AtomArgumentSignature, AtomArgumentSignature)],
    comps: &[ComparisonExprPos],
) -> String {
    let _k = out_kv_layout
        .key()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let _v = out_kv_layout
        .value()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

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
