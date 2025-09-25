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
pub enum TransformationInfo {
    KVToKV {
        input_sig: u64,
        output_sig: u64,
        input_key: Vec<ArithmeticPos>,
        input_value: Vec<ArithmeticPos>,
        output_key: Vec<ArithmeticPos>,
        output_value: Vec<ArithmeticPos>,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs: Vec<ComparisonExprPos>,
    },

    JoinToKV {
        left_input_sig: u64,
        right_input_sig: u64,
        output_sig: u64,
        left_input_key: Vec<ArithmeticPos>,
        left_input_value: Vec<ArithmeticPos>,
        right_input_value: Vec<ArithmeticPos>,
        output_key: Vec<ArithmeticPos>,
        output_value: Vec<ArithmeticPos>,
        compare_exprs: Vec<ComparisonExprPos>,
    },

    AntiJoinToKV {
        left_input_sig: u64,
        right_input_sig: u64,
        output_sig: u64,
        left_input_key: Vec<ArithmeticPos>,
        left_input_value: Vec<ArithmeticPos>,
        right_input_value: Vec<ArithmeticPos>,
        output_key: Vec<ArithmeticPos>,
        output_value: Vec<ArithmeticPos>,
    },
}

impl TransformationInfo {
    /// Get input signature(s); for joins, returns (left, Some(right))
    #[inline]
    pub fn input_sig(&self) -> (u64, Option<u64>) {
        match self {
            Self::KVToKV { input_sig, .. } => (*input_sig, None),
            Self::JoinToKV {
                left_input_sig,
                right_input_sig,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_sig,
                right_input_sig,
                ..
            } => (*left_input_sig, Some(*right_input_sig)),
        }
    }

    /// Get output signature.
    #[inline]
    pub fn output_sig(&self) -> u64 {
        match self {
            Self::KVToKV { output_sig, .. }
            | Self::JoinToKV { output_sig, .. }
            | Self::AntiJoinToKV { output_sig, .. } => *output_sig,
        }
    }

    /// Get a reference to the input keys' arithmetic positions; for joins, returns (left, Some(right)).
    #[inline]
    pub fn input_key(&self) -> &[ArithmeticPos] {
        match self {
            Self::KVToKV { input_key, .. }
            | Self::JoinToKV {
                left_input_key: input_key,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_key: input_key,
                ..
            } => input_key,
        }
    }

    /// Get a reference to the input values' arithmetic positions; for joins, returns (left, Some(right)).
    #[inline]
    pub fn input_value(&self) -> (&[ArithmeticPos], Option<&[ArithmeticPos]>) {
        match self {
            Self::KVToKV { input_value, .. } => (input_value, None),
            Self::JoinToKV {
                left_input_value,
                right_input_value,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_value,
                right_input_value,
                ..
            } => (left_input_value, Some(right_input_value)),
        }
    }

    /// Get a reference to the output key arithmetic positions.
    #[inline]
    pub fn output_key(&self) -> &[ArithmeticPos] {
        match self {
            Self::KVToKV { output_key, .. }
            | Self::JoinToKV { output_key, .. }
            | Self::AntiJoinToKV { output_key, .. } => output_key,
        }
    }

    /// Get a reference to the output value arithmetic positions.
    #[inline]
    pub fn output_value(&self) -> &[ArithmeticPos] {
        match self {
            Self::KVToKV { output_value, .. }
            | Self::JoinToKV { output_value, .. }
            | Self::AntiJoinToKV { output_value, .. } => output_value,
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
            Self::KVToKV { compare_exprs, .. } | Self::JoinToKV { compare_exprs, .. } => {
                compare_exprs
            }
            Self::AntiJoinToKV { .. } => panic!("AntiJoinToKV has no compare_exprs"),
        }
    }

    /// Build a key-value to key-value transformation with a derived (fake) output signature.
    pub fn kv_to_kv(
        input_fake_sig: u64,
        input_key: Vec<ArithmeticPos>,
        input_value: Vec<ArithmeticPos>,
        output_fake_key: Vec<ArithmeticPos>,
        output_fake_value: Vec<ArithmeticPos>,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs: Vec<ComparisonExprPos>,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "kv_to_kv",
            &input_fake_sig,
            &input_key,
            &input_value,
            &output_fake_key,
            &output_fake_value,
            &const_eq_constraints,
            &var_eq_constraints,
            &compare_exprs,
        ));

        Self::KVToKV {
            input_sig: input_fake_sig,
            output_sig: fake_output_sig,
            input_key,
            input_value,
            output_key: output_fake_key,
            output_value: output_fake_value,
            const_eq_constraints,
            var_eq_constraints,
            compare_exprs,
        }
    }

    /// Build a join to key-value transformation with a derived (fake) output signature.
    pub fn join_to_kv(
        left_fake_sig: u64,
        right_fake_sig: u64,
        left_key: Vec<ArithmeticPos>,
        left_value: Vec<ArithmeticPos>,
        right_value: Vec<ArithmeticPos>,
        output_fake_key: Vec<ArithmeticPos>,
        output_fake_value: Vec<ArithmeticPos>,
        compare_exprs: Vec<ComparisonExprPos>,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_key,
            &left_value,
            &right_value,
            &output_fake_key,
            &output_fake_value,
            &compare_exprs,
        ));

        Self::JoinToKV {
            left_input_sig: left_fake_sig,
            right_input_sig: right_fake_sig,
            output_sig: fake_output_sig,
            left_input_key: left_key,
            left_input_value: left_value,
            right_input_value: right_value,
            output_key: output_fake_key,
            output_value: output_fake_value,
            compare_exprs,
        }
    }

    /// Build a negated join to key-value transformation with a derived (fake) output signature.
    pub fn anti_join_to_kv(
        left_fake_sig: u64,
        right_fake_sig: u64,
        left_key: Vec<ArithmeticPos>,
        left_value: Vec<ArithmeticPos>,
        right_value: Vec<ArithmeticPos>,
        output_fake_key: Vec<ArithmeticPos>,
        output_fake_value: Vec<ArithmeticPos>,
    ) -> Self {
        let fake_output_sig = compute_sig((
            "anti_join_to_kv",
            &left_fake_sig,
            &right_fake_sig,
            &left_key,
            &left_value,
            &right_value,
            &output_fake_key,
            &output_fake_value,
        ));

        Self::AntiJoinToKV {
            left_input_sig: left_fake_sig,
            right_input_sig: right_fake_sig,
            output_sig: fake_output_sig,
            left_input_key: left_key,
            left_input_value: left_value,
            right_input_value: right_value,
            output_key: output_fake_key,
            output_value: output_fake_value,
        }
    }

    /// Updates the placeholder (fake) input signature with the resolved (real) signature.
    ///
    /// Call this after a prior transformation has produced a real output signature
    /// for the upstream collection (e.g., after key/value layout changes or map-fusion).
    pub fn update_input_fake_sig(&mut self, input_real_sig: u64, left: bool) {
        match self {
            Self::KVToKV { input_sig, .. } => {
                *input_sig = input_real_sig;
            }
            Self::JoinToKV {
                left_input_sig,
                right_input_sig,
                ..
            }
            | Self::AntiJoinToKV {
                left_input_sig,
                right_input_sig,
                ..
            } => {
                if left {
                    *left_input_sig = input_real_sig;
                } else {
                    *right_input_sig = input_real_sig;
                }
            }
        }
    }

    /// Updates the placeholder (fake) output key/value layouts with resolved real layouts.
    ///
    /// This step is necessary once the actual output layout is known, since later
    /// operators—particularly joins—require concrete key/value layouts to proceed.
    pub fn update_output_key_value(
        &mut self,
        real_output_key: Vec<ArithmeticPos>,
        real_output_value: Vec<ArithmeticPos>,
    ) {
        match self {
            Self::KVToKV {
                output_key,
                output_value,
                ..
            }
            | Self::JoinToKV {
                output_key,
                output_value,
                ..
            }
            | Self::AntiJoinToKV {
                output_key,
                output_value,
                ..
            } => {
                *output_key = real_output_key;
                *output_value = real_output_value;
            }
        }
    }

    pub fn update_output_fake_sig(&mut self) {
        match self {
            Self::KVToKV {
                input_sig,
                input_key,
                input_value,
                output_key,
                output_value,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs,
                output_sig,
            } => {
                *output_sig = compute_sig((
                    "kv_to_kv",
                    input_sig,
                    input_key,
                    input_value,
                    output_key,
                    output_value,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs,
                ));
            }
            Self::JoinToKV {
                left_input_sig,
                right_input_sig,
                left_input_key,
                left_input_value,
                right_input_value,
                output_key,
                output_value,
                compare_exprs,

                output_sig,
            } => {
                *output_sig = compute_sig((
                    "join_to_kv",
                    left_input_sig,
                    right_input_sig,
                    left_input_key,
                    left_input_value,
                    right_input_value,
                    output_key,
                    output_value,
                    compare_exprs,
                ));
            }
            Self::AntiJoinToKV {
                left_input_sig,
                right_input_sig,
                left_input_key,
                left_input_value,
                right_input_value,
                output_key,
                output_value,
                output_sig,
            } => {
                *output_sig = compute_sig((
                    "anti_join_to_kv",
                    left_input_sig,
                    right_input_sig,
                    left_input_key,
                    left_input_value,
                    right_input_value,
                    output_key,
                    output_value,
                ));
            }
        }
    }
}

impl fmt::Display for TransformationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KVToKV {
                input_sig,
                input_key,
                input_value,
                output_sig,
                output_key,
                output_value,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs,
            } => {
                let in_coll = fmt_collection(input_sig, input_key, input_value);
                let out_coll = fmt_collection(output_sig, output_key, output_value);
                let filters = fmt_flow_kv(
                    output_key,
                    output_value,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs,
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
                left_input_sig,
                right_input_sig,
                output_sig,
                left_input_key,
                left_input_value,
                right_input_value,
                output_key,
                output_value,
                compare_exprs,
            } => {
                let l = fmt_collection(left_input_sig, left_input_key, left_input_value);
                // Right side uses the *join key* for display, and its own value positions.
                let r = fmt_collection(right_input_sig, left_input_key, right_input_value);
                let out = fmt_collection(output_sig, output_key, output_value);
                let filters = fmt_flow_kv(output_key, output_value, &[], &[], compare_exprs);

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
                left_input_sig,
                right_input_sig,
                output_sig,
                left_input_key,
                left_input_value,
                right_input_value,
                output_key,
                output_value,
            } => {
                let l = fmt_collection(left_input_sig, left_input_key, left_input_value);
                // Right side uses the *join key* for display, and its own value positions.
                let r = fmt_collection(right_input_sig, left_input_key, right_input_value);
                let out = fmt_collection(output_sig, output_key, output_value);

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
fn fmt_collection(sig: &u64, key: &[ArithmeticPos], value: &[ArithmeticPos]) -> String {
    let k = key
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let v = value
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    if key.is_empty() {
        format!("{:016x} [key: (), value: ({})]", sig, v)
    } else {
        format!("{:016x} [key: ({}), value: ({})]", sig, k, v)
    }
}

/// Formats a flow-like string: returns only the combined filter expression (no wrappers)
fn fmt_flow_kv(
    out_key: &[ArithmeticPos],
    out_value: &[ArithmeticPos],
    consts: &[(AtomArgumentSignature, ConstType)],
    vars: &[(AtomArgumentSignature, AtomArgumentSignature)],
    comps: &[ComparisonExprPos],
) -> String {
    let _k = out_key
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let _v = out_value
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
