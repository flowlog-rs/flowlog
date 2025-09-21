//! Transformation operations for query planning in Macaron Datalog programs.

use crate::{Collection, CollectionSignature, Transformation};
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos};
use parser::ConstType;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(PartialEq, Clone, Eq, Hash)]
pub enum FakeTransformation {
    FakeKVToKV {
        fake_input_sig: u64,
        fake_output_sig: u64,
        input_key: Vec<ArithmeticPos>,
        input_value: Vec<ArithmeticPos>,
        fake_output_key: Vec<ArithmeticPos>,
        fake_output_value: Vec<ArithmeticPos>,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs: Vec<ComparisonExprPos>,
    },

    FakeJoinToKV {
        fake_left_sig: u64,
        fake_right_sig: u64,
        fake_output_sig: u64,
        left_key: Vec<ArithmeticPos>,
        left_value: Vec<ArithmeticPos>,
        right_value: Vec<ArithmeticPos>,
        fake_output_key: Vec<ArithmeticPos>,
        fake_output_value: Vec<ArithmeticPos>,
        compare_exprs: Vec<ComparisonExprPos>,
    },
}

impl FakeTransformation {
    pub fn fake_output_sig(&self) -> u64 {
        match self {
            FakeTransformation::FakeKVToKV {
                fake_output_sig, ..
            } => *fake_output_sig,
            FakeTransformation::FakeJoinToKV {
                fake_output_sig, ..
            } => *fake_output_sig,
        }
    }

    pub fn fake_kv_to_kv(
        input_fake_sig: u64,
        input_key: Vec<ArithmeticPos>,
        input_value: Vec<ArithmeticPos>,
        output_key: Vec<ArithmeticPos>,
        output_value: Vec<ArithmeticPos>,
        const_eq_constraints: Vec<(AtomArgumentSignature, ConstType)>,
        var_eq_constraints: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
        compare_exprs: Vec<ComparisonExprPos>,
    ) -> Self {
        // Compute fake_output_sig as a stable hash over all other inputs
        let mut hasher = DefaultHasher::new();
        "fake_kv_to_kv".hash(&mut hasher);
        input_fake_sig.hash(&mut hasher);
        input_key.hash(&mut hasher);
        input_value.hash(&mut hasher);
        output_key.hash(&mut hasher);
        output_value.hash(&mut hasher);
        const_eq_constraints.hash(&mut hasher);
        var_eq_constraints.hash(&mut hasher);
        compare_exprs.hash(&mut hasher);
        let fake_output_sig = hasher.finish();

        FakeTransformation::FakeKVToKV {
            fake_input_sig: input_fake_sig,
            fake_output_sig,
            input_key,
            input_value,
            fake_output_key: output_key,
            fake_output_value: output_value,
            const_eq_constraints,
            var_eq_constraints,
            compare_exprs,
        }
    }

    pub fn fake_join_to_kv(
        left_fake_sig: u64,
        right_fake_sig: u64,
        left_key: Vec<ArithmeticPos>,
        left_value: Vec<ArithmeticPos>,
        right_value: Vec<ArithmeticPos>,
        output_key: Vec<ArithmeticPos>,
        output_value: Vec<ArithmeticPos>,
        compare_exprs: Vec<ComparisonExprPos>,
    ) -> Self {
        // Compute fake_output_sig as a stable hash over all other inputs
        let mut hasher = DefaultHasher::new();
        "fake_join_to_kv".hash(&mut hasher);
        left_fake_sig.hash(&mut hasher);
        right_fake_sig.hash(&mut hasher);
        left_key.hash(&mut hasher);
        left_value.hash(&mut hasher);
        right_value.hash(&mut hasher);
        output_key.hash(&mut hasher);
        output_value.hash(&mut hasher);
        compare_exprs.hash(&mut hasher);
        let fake_output_sig = hasher.finish();

        FakeTransformation::FakeJoinToKV {
            fake_left_sig: left_fake_sig,
            fake_right_sig: right_fake_sig,
            fake_output_sig,
            left_key,
            left_value,
            right_value,
            fake_output_key: output_key,
            fake_output_value: output_value,
            compare_exprs,
        }
    }

    /// Update the input fake signature after verifying it matches the expected fake.
    pub fn update_input_fake_sig(&mut self, input_real_sig: u64, left: bool) {
        match self {
            FakeTransformation::FakeKVToKV { fake_input_sig, .. } => {
                *fake_input_sig = input_real_sig;
            }
            FakeTransformation::FakeJoinToKV {
                fake_left_sig,
                fake_right_sig,
                ..
            } => {
                if left {
                    *fake_left_sig = input_real_sig;
                } else {
                    *fake_right_sig = input_real_sig;
                }
            }
        }
    }

    /// Update the input fake signature after verifying it matches the expected fake.
    pub fn update_output_key_value(
        &mut self,
        real_output_key: Vec<ArithmeticPos>,
        real_fake_output_value: Vec<ArithmeticPos>,
    ) {
        match self {
            FakeTransformation::FakeKVToKV {
                fake_output_key,
                fake_output_value,
                ..
            } => {
                *fake_output_key = real_output_key;
                *fake_output_value = real_fake_output_value;
            }
            FakeTransformation::FakeJoinToKV {
                fake_output_key,
                fake_output_value,
                ..
            } => {
                *fake_output_key = real_output_key;
                *fake_output_value = real_fake_output_value;
            }
        }
    }

    /// Generate real transformation after all fake signatures and output key/values have been updated.
    pub fn generate_transformation(&self) -> Transformation {
        match self {
            FakeTransformation::FakeKVToKV {
                fake_input_sig,
                input_key,
                input_value,
                fake_output_key,
                fake_output_value,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs,
                ..
            } => {
                let input = Arc::new(Collection::new(
                    CollectionSignature(*fake_input_sig),
                    input_key,
                    input_value,
                ));
                Transformation::kv_to_kv(
                    input,
                    fake_output_key,
                    fake_output_value,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs,
                )
            }
            FakeTransformation::FakeJoinToKV {
                fake_left_sig,
                fake_right_sig,
                left_key,
                left_value,
                right_value,
                fake_output_key,
                fake_output_value,
                compare_exprs,
                ..
            } => {
                let left = Arc::new(Collection::new(
                    CollectionSignature(*fake_left_sig),
                    left_key,
                    left_value,
                ));
                let right = Arc::new(Collection::new(
                    CollectionSignature(*fake_right_sig),
                    left_key,
                    right_value,
                ));
                Transformation::join(
                    (left, right),
                    fake_output_key,
                    fake_output_value,
                    compare_exprs,
                )
            }
        }
    }
}

impl fmt::Display for FakeTransformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // helper to format a collection-like string "sig(keys: values)" or "sig(values)"
        fn fmt_collection(sig: &u64, key: &[ArithmeticPos], value: &[ArithmeticPos]) -> String {
            let k = key
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let v = value
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            if key.is_empty() {
                format!("{:016x}({})", sig, v)
            } else {
                format!("{:016x}({}: {})", sig, k, v)
            }
        }

        // helper to format a flow-like string "|(...key: value...) if filters|"
        fn fmt_flow_kv(
            out_key: &[ArithmeticPos],
            out_value: &[ArithmeticPos],
            consts: &[(AtomArgumentSignature, ConstType)],
            vars: &[(AtomArgumentSignature, AtomArgumentSignature)],
            comps: &[ComparisonExprPos],
        ) -> String {
            let k = out_key
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let v = out_value
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            let consts_str = consts
                .iter()
                .map(|(sig, c)| format!("{} = {:?}", sig, c))
                .collect::<Vec<_>>()
                .join(", ");
            let vars_str = vars
                .iter()
                .map(|(l, r)| format!("{} = {}", l, r))
                .collect::<Vec<_>>()
                .join(", ");
            let comps_str = comps
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ");

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
            let filters = if parts.is_empty() {
                String::new()
            } else {
                format!(" if {}", parts.join(" and "))
            };

            if out_key.is_empty() {
                format!("|({}){}|", v, filters)
            } else {
                format!("|({}: {}){}|", k, v, filters)
            }
        }

        match self {
            FakeTransformation::FakeKVToKV {
                fake_input_sig,
                input_key,
                input_value,
                fake_output_sig,
                fake_output_key,
                fake_output_value,
                const_eq_constraints,
                var_eq_constraints,
                compare_exprs,
            } => {
                let in_coll = fmt_collection(fake_input_sig, input_key, input_value);
                let out_coll = fmt_collection(fake_output_sig, fake_output_key, fake_output_value);
                let flow_str = fmt_flow_kv(
                    fake_output_key,
                    fake_output_value,
                    const_eq_constraints,
                    var_eq_constraints,
                    compare_exprs,
                );
                write!(f, "{} ----{}----> {}", in_coll, flow_str, out_coll)
            }
            FakeTransformation::FakeJoinToKV {
                fake_left_sig,
                fake_right_sig,
                fake_output_sig,
                left_key,
                left_value,
                right_value,
                fake_output_key,
                fake_output_value,
                compare_exprs,
            } => {
                let l = fmt_collection(fake_left_sig, left_key, left_value);
                let r = fmt_collection(fake_right_sig, left_key, right_value);
                let out = fmt_collection(fake_output_sig, fake_output_key, fake_output_value);
                let flow_str =
                    fmt_flow_kv(fake_output_key, fake_output_value, &[], &[], compare_exprs);
                write!(f, "({} ⋈ {}) ----{}----> {}", l, r, flow_str, out)
            }
        }
    }
}

impl fmt::Debug for FakeTransformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
