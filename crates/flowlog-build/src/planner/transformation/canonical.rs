//! Content-canonical fingerprints for plan-time arrangement sharing.
//!
//! A transformation's `output_info_fp` (see [`super::info`]) is the identity of
//! the collection it produces. It is hashed from the transformation's input
//! fingerprint(s), key/value layouts, and predicates — and those layouts and
//! predicates reference columns via [`AtomArgumentSignature`], which carries two
//! pieces of *rule-local* metadata that are irrelevant to a collection's
//! content:
//!
//!   - `rhs_id`      — the source atom's position in the rule body, and
//!   - `is_positive` — whether the occurrence was positive (`R(x)`) or negative
//!     (`!R(x)`).
//!
//! Hashing those splits the fingerprint of the *same* logical arrangement
//! (relation `R` keyed on the same columns) across rules — e.g. when `R` sits
//! at a different body index, or is joined positively in one rule and used as an
//! antijoin in another. The planner's two dedup passes
//! ([`StratumPlanner::deduplicate_transformation_infos`] within a stratum and
//! [`prune_cross_stratum_duplicates`] across strata) both key on this
//! fingerprint, so the splits leak straight through to codegen as duplicate
//! `arrange_by_key`s — extra sorted indices rebuilt every recursive round.
//!
//! Canonicalizing the fingerprint fixes the sharing entirely in the plan: the
//! existing dedup passes then collapse content-identical transformations, so the
//! plan becomes a DAG with shared nodes and codegen lowers it 1:1 (no
//! codegen-side sharing logic). This module produces the canonical operands the
//! fingerprint is hashed from.
//!
//! # Canonicalization
//!
//! Within one transformation, every distinct atom (`AtomSignature`) is renamed
//! to a canonical index by first-appearance order over a fixed traversal.
//! `argument_id` (which column) is preserved; `rhs_id` and `is_positive` are
//! normalized away. Conjunctive predicate lists are additionally order-
//! normalized (sorted by element fingerprint) so two rules that state the same
//! filters in a different textual order still share.
//!
//! # Soundness
//!
//! Interning keys on the *full* `AtomSignature` (polarity + position), so
//! distinct atoms always receive distinct canonical indices — the renaming is
//! injective and never merges genuinely different transformations. Input
//! fingerprints are already canonical (a leaf's is the relation-name hash; a
//! derived input's is the canonical fp of its producer), so canonicalizing each
//! transformation's own operands composes bottom-up. Two transformations
//! collide only when they have identical input fps, identical column structure
//! (modulo position), and the identical predicate set — i.e. they provably
//! produce the same collection.
//!
//! Polarity is content-irrelevant because an arrangement holds the projected
//! relation data; the positive/negative distinction lives in the *consumer*
//! (a semijoin vs an antijoin), both of which want the identical keyed trace.
//!
//! The canonical operands are used *only* for the fingerprint hash; the stored
//! layouts/predicates keep their original form, so codegen is unaffected.

use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, AtomSignature, ComparisonExprPos, FactorPos,
    FnCallPredicatePos, JoinPredicates, KvPredicates,
};
use crate::common::compute_fp;
use std::collections::HashMap;
use std::hash::Hash;

use super::KeyValueLayout;

/// A rule-position-independent renaming of the atom signatures appearing in one
/// transformation. Distinct atoms are interned to canonical indices
/// `0, 1, 2, …` in first-appearance order over a fixed operand traversal.
#[derive(Default)]
struct SigCanon {
    map: HashMap<AtomSignature, usize>,
}

impl SigCanon {
    fn intern(&mut self, atom: AtomSignature) {
        let next = self.map.len();
        self.map.entry(atom).or_insert(next);
    }

    fn observe_layout(&mut self, layout: &KeyValueLayout) {
        for pos in layout.key().iter().chain(layout.value().iter()) {
            self.observe_arith(pos);
        }
    }

    fn observe_arith(&mut self, arith: &ArithmeticPos) {
        for sig in arith.signatures() {
            self.intern(*sig.atom_signature());
        }
    }

    fn observe_kv_preds(&mut self, preds: &KvPredicates) {
        for (sig, _) in &preds.const_eq {
            self.intern(*sig.atom_signature());
        }
        for (left, right) in &preds.var_eq {
            self.intern(*left.atom_signature());
            self.intern(*right.atom_signature());
        }
        for cmp in &preds.compare_exprs {
            self.observe_arith(cmp.left());
            self.observe_arith(cmp.right());
        }
        for fc in &preds.fn_call_preds {
            for arg in fc.args() {
                self.observe_arith(arg);
            }
        }
    }

    fn observe_join_preds(&mut self, preds: &JoinPredicates) {
        for cmp in &preds.compare_exprs {
            self.observe_arith(cmp.left());
            self.observe_arith(cmp.right());
        }
        for fc in &preds.fn_call_preds {
            for arg in fc.args() {
                self.observe_arith(arg);
            }
        }
    }

    /// Map a signature to its canonical form. Polarity is normalized to a
    /// constant; the canonical index (interned on the full `AtomSignature`)
    /// preserves all distinctions, so this stays injective.
    fn remap_sig(&self, sig: &AtomArgumentSignature) -> AtomArgumentSignature {
        // Every signature is interned during the observe pass, so the lookup
        // cannot miss; fall back to the original on the impossible miss rather
        // than panicking inside fingerprinting.
        match self.map.get(sig.atom_signature()) {
            Some(&idx) => {
                AtomArgumentSignature::new(AtomSignature::new(true, idx), sig.argument_id())
            }
            None => *sig,
        }
    }

    fn remap_arith(&self, arith: &ArithmeticPos) -> ArithmeticPos {
        arith.map_vars(&|sig| FactorPos::Var(self.remap_sig(sig)))
    }

    fn remap_layout(&self, layout: &KeyValueLayout) -> KeyValueLayout {
        // Key/value order is significant (it maps to tuple positions), so the
        // lists are remapped in place, never reordered.
        KeyValueLayout::new(
            layout.key().iter().map(|p| self.remap_arith(p)).collect(),
            layout.value().iter().map(|p| self.remap_arith(p)).collect(),
        )
    }

    fn remap_cmp(&self, cmp: &ComparisonExprPos) -> ComparisonExprPos {
        ComparisonExprPos::from_parts(
            self.remap_arith(cmp.left()),
            cmp.operator().clone(),
            self.remap_arith(cmp.right()),
        )
    }

    fn remap_fn_call(&self, fc: &FnCallPredicatePos) -> FnCallPredicatePos {
        FnCallPredicatePos::new(
            fc.name().to_string(),
            fc.args().iter().map(|a| self.remap_arith(a)).collect(),
            fc.is_negated(),
        )
    }

    fn remap_kv_preds(&self, preds: &KvPredicates) -> KvPredicates {
        // Predicate lists are conjunctive, so their order does not affect the
        // result. Every predicate constrains an input column, which is interned
        // while observing the input layout (always before predicates), so the
        // canonical indices are predicate-order-independent; sorting each list
        // by element fingerprint therefore makes the hash order-independent
        // without losing information. (The stored predicates keep their order
        // for codegen — only these fingerprint operands are reordered.)
        KvPredicates {
            const_eq: sorted_by_fp(
                preds
                    .const_eq
                    .iter()
                    .map(|(s, c)| (self.remap_sig(s), c.clone()))
                    .collect(),
            ),
            var_eq: sorted_by_fp(
                preds
                    .var_eq
                    .iter()
                    .map(|(l, r)| (self.remap_sig(l), self.remap_sig(r)))
                    .collect(),
            ),
            compare_exprs: sorted_by_fp(preds.compare_exprs.iter().map(|c| self.remap_cmp(c)).collect()),
            fn_call_preds: sorted_by_fp(preds.fn_call_preds.iter().map(|f| self.remap_fn_call(f)).collect()),
        }
    }

    fn remap_join_preds(&self, preds: &JoinPredicates) -> JoinPredicates {
        JoinPredicates {
            compare_exprs: sorted_by_fp(preds.compare_exprs.iter().map(|c| self.remap_cmp(c)).collect()),
            fn_call_preds: sorted_by_fp(preds.fn_call_preds.iter().map(|f| self.remap_fn_call(f)).collect()),
        }
    }
}

/// Sort a conjunctive predicate list into a canonical order (by element
/// fingerprint) so that the same set of predicates hashes identically
/// regardless of the order the rule stated them in.
fn sorted_by_fp<T: Hash>(mut items: Vec<T>) -> Vec<T> {
    items.sort_by_key(|item| compute_fp(item));
    items
}

/// Canonical (rule-position-independent) operands of a unary transformation's
/// input/output layouts and predicates, for fingerprinting only.
pub(super) fn canon_unary(
    input: &KeyValueLayout,
    output: &KeyValueLayout,
    preds: &KvPredicates,
) -> (KeyValueLayout, KeyValueLayout, KvPredicates) {
    let mut canon = SigCanon::default();
    canon.observe_layout(input);
    canon.observe_layout(output);
    canon.observe_kv_preds(preds);
    (
        canon.remap_layout(input),
        canon.remap_layout(output),
        canon.remap_kv_preds(preds),
    )
}

/// Canonical operands of a join transformation's layouts and predicates.
pub(super) fn canon_join(
    left: &KeyValueLayout,
    right: &KeyValueLayout,
    output: &KeyValueLayout,
    preds: &JoinPredicates,
) -> (KeyValueLayout, KeyValueLayout, KeyValueLayout, JoinPredicates) {
    let mut canon = SigCanon::default();
    canon.observe_layout(left);
    canon.observe_layout(right);
    canon.observe_layout(output);
    canon.observe_join_preds(preds);
    (
        canon.remap_layout(left),
        canon.remap_layout(right),
        canon.remap_layout(output),
        canon.remap_join_preds(preds),
    )
}

/// Canonical operands of an anti-join transformation's layouts (no predicates).
pub(super) fn canon_anti_join(
    left: &KeyValueLayout,
    right: &KeyValueLayout,
    output: &KeyValueLayout,
) -> (KeyValueLayout, KeyValueLayout, KeyValueLayout) {
    let mut canon = SigCanon::default();
    canon.observe_layout(left);
    canon.observe_layout(right);
    canon.observe_layout(output);
    (
        canon.remap_layout(left),
        canon.remap_layout(right),
        canon.remap_layout(output),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ConstType;

    fn sig(positive: bool, rhs: usize, arg: usize) -> AtomArgumentSignature {
        AtomArgumentSignature::new(AtomSignature::new(positive, rhs), arg)
    }

    fn layout(key: Vec<AtomArgumentSignature>, val: Vec<AtomArgumentSignature>) -> KeyValueLayout {
        KeyValueLayout::new(
            key.into_iter().map(ArithmeticPos::from_var_signature).collect(),
            val.into_iter().map(ArithmeticPos::from_var_signature).collect(),
        )
    }

    /// The crux: projecting `R` to `key=col0, val=col1` must canonicalize
    /// identically whether `R` is body atom #5 or #2 — otherwise the two
    /// projections keep distinct fingerprints and codegen arranges twice.
    #[test]
    fn ignores_body_position() {
        let (a_in, a_out, _) = canon_unary(
            &layout(vec![], vec![sig(true, 5, 0), sig(true, 5, 1)]),
            &layout(vec![sig(true, 5, 0)], vec![sig(true, 5, 1)]),
            &KvPredicates::default(),
        );
        let (b_in, b_out, _) = canon_unary(
            &layout(vec![], vec![sig(true, 2, 0), sig(true, 2, 1)]),
            &layout(vec![sig(true, 2, 0)], vec![sig(true, 2, 1)]),
            &KvPredicates::default(),
        );
        assert_eq!((a_in, a_out), (b_in, b_out));
    }

    /// Soundness guard: keying on a *different* column must NOT canonicalize
    /// equal — two genuinely distinct arrangements must never merge.
    #[test]
    fn distinguishes_key_column() {
        let input = layout(vec![], vec![sig(true, 0, 0), sig(true, 0, 1)]);
        let (_, key_on_0, _) = canon_unary(
            &input,
            &layout(vec![sig(true, 0, 0)], vec![sig(true, 0, 1)]),
            &KvPredicates::default(),
        );
        let (_, key_on_1, _) = canon_unary(
            &input,
            &layout(vec![sig(true, 0, 1)], vec![sig(true, 0, 0)]),
            &KvPredicates::default(),
        );
        assert_ne!(key_on_0, key_on_1);
    }

    /// Polarity is normalized: a lone positive-atom projection and a lone
    /// negative-atom projection of the same relation/columns canonicalize
    /// EQUAL, so `S(x)` and `!S(x)` share one `S`-keyed arrangement.
    #[test]
    fn polarity_shared_across_transformations() {
        let pos = layout(vec![sig(true, 0, 0)], vec![]);
        let neg = layout(vec![sig(false, 0, 0)], vec![]);
        let (_, c_pos, _) = canon_unary(&pos, &pos, &KvPredicates::default());
        let (_, c_neg, _) = canon_unary(&neg, &neg, &KvPredicates::default());
        assert_eq!(c_pos, c_neg);
    }

    /// Within one transformation, distinct atoms stay distinct even when they
    /// share a body position across the positive/negative lists.
    #[test]
    fn distinct_atoms_stay_distinct() {
        let (left, right, _, _) = canon_join(
            &layout(vec![sig(true, 0, 0)], vec![]),
            &layout(vec![sig(false, 0, 0)], vec![]),
            &layout(vec![sig(true, 0, 0)], vec![]),
            &JoinPredicates::default(),
        );
        assert_ne!(left, right);
    }

    /// A join canonicalizes independently of either atom's body position
    /// (left → 0, right → 1), so the same logical join from different rules
    /// shares one arrangement.
    #[test]
    fn join_ignores_body_position() {
        let mk = |l: usize, r: usize| {
            canon_join(
                &layout(vec![sig(true, l, 0)], vec![sig(true, l, 1)]),
                &layout(vec![sig(true, r, 0)], vec![sig(true, r, 1)]),
                &layout(vec![sig(true, l, 0)], vec![sig(true, l, 1), sig(true, r, 1)]),
                &JoinPredicates::default(),
            )
        };
        assert_eq!(mk(0, 1), mk(3, 5));
    }

    /// Swapping which side a value comes from must NOT canonicalize equal.
    #[test]
    fn join_distinguishes_value_origin() {
        let l = layout(vec![sig(true, 0, 0)], vec![sig(true, 0, 1)]);
        let r = layout(vec![sig(true, 1, 0)], vec![sig(true, 1, 1)]);
        let (_, _, out_lr, _) = canon_join(
            &l,
            &r,
            &layout(vec![sig(true, 0, 0)], vec![sig(true, 0, 1), sig(true, 1, 1)]),
            &JoinPredicates::default(),
        );
        let (_, _, out_rl, _) = canon_join(
            &l,
            &r,
            &layout(vec![sig(true, 0, 0)], vec![sig(true, 1, 1), sig(true, 0, 1)]),
            &JoinPredicates::default(),
        );
        assert_ne!(out_lr, out_rl);
    }

    /// Conjunctive predicates stated in a different order must canonicalize
    /// equal — completeness parity with a rendered-code key, which collapses
    /// codegen-irrelevant ordering. A different predicate *set* must not.
    #[test]
    fn predicate_order_is_normalized() {
        let input = layout(vec![], vec![sig(true, 0, 0), sig(true, 0, 1)]);
        let out = layout(vec![sig(true, 0, 0)], vec![sig(true, 0, 1)]);
        let ab = KvPredicates {
            const_eq: vec![
                (sig(true, 0, 0), ConstType::Int(1)),
                (sig(true, 0, 1), ConstType::Int(2)),
            ],
            ..Default::default()
        };
        let ba = KvPredicates {
            const_eq: vec![
                (sig(true, 0, 1), ConstType::Int(2)),
                (sig(true, 0, 0), ConstType::Int(1)),
            ],
            ..Default::default()
        };
        let (_, _, c_ab) = canon_unary(&input, &out, &ab);
        let (_, _, c_ba) = canon_unary(&input, &out, &ba);
        assert_eq!(c_ab, c_ba, "predicate ordering must not split the fingerprint");

        let different = KvPredicates {
            const_eq: vec![(sig(true, 0, 0), ConstType::Int(9))],
            ..Default::default()
        };
        let (_, _, c_diff) = canon_unary(&input, &out, &different);
        assert_ne!(c_ab, c_diff, "a different predicate set must stay distinct");
    }
}
