//! Canonical labels for transformation outputs.
//!
//! Each helper renders the logical operation applied to its inputs — e.g.
//! `(reach ⋈[y] arc)` for a join keyed on `y`. The string becomes the
//! transformation's `Collection` name and serves as its structural cache
//! key for profile feedback, so the planner (which emits these names) and
//! the optimizer's cost estimator (which reconstructs them) must format
//! names identically. This module is the single source of that formatting.

/// `π[attrs](input)` — a projection of `input` onto `attrs`.
pub(crate) fn proj_name(input_name: &str, attrs: &[String]) -> String {
    format!("π[{}]({})", attrs.join(","), input_name)
}

/// `σ[cond](input)` — a selection of `input` by `cond`.
pub(crate) fn filter_name(input_name: &str, cond: &str) -> String {
    format!("σ[{cond}]({input_name})")
}

/// Render a join's key list. Keys are a *set*, so they are sorted: a join
/// keyed `[y,x]` and one keyed `[x,y]` must produce the same label, or the
/// optimizer's reconstructed name would miss the planner's cache key.
fn sorted_keys(keys: &[String]) -> String {
    let mut k: Vec<&str> = keys.iter().map(String::as_str).collect();
    k.sort();
    k.join(",")
}

/// `(left ⋈[keys] right)` — an equi-join. `⋈` is symmetric, so the two
/// operands are sorted lexicographically and `(A ⋈ B)` / `(B ⋈ A)`
/// collapse to one label. The planner builds names bottom-up with this
/// helper and the optimizer reuses it, so a join is canonical at every
/// nesting level — equal joins share a feedback-cache key with no
/// post-hoc canonicalization.
///
/// The label is cosmetic: a transformation's identity is its fingerprint,
/// not its name, so sorting operands here does not affect execution.
pub(crate) fn join_name(left: &str, right: &str, keys: &[String]) -> String {
    let (a, b) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    format!("({a} ⋈[{}] {b})", sorted_keys(keys))
}

/// `(left ⋉[keys] right)` — a semijoin. Unlike `⋈` it is *not* symmetric
/// — only `left`'s tuples survive — so operand order is preserved.
pub(crate) fn semijoin_name(left: &str, right: &str, keys: &[String]) -> String {
    format!("({left} ⋉[{}] {right})", sorted_keys(keys))
}

/// `(left ▷[keys] right)` — an antijoin. Like `⋉`, operand order matters.
pub(crate) fn antijoin_name(left: &str, right: &str, keys: &[String]) -> String {
    format!("({left} ▷[{}] {right})", sorted_keys(keys))
}
