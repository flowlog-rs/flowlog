//! Canonicalization of transformation (Collection) names.
//!
//! A transformation's name already encodes its construction tree —
//! `((a ⋈[x] b) ⋈[y] c)`, `π[v](…)`, … . [`canonical`] strips the
//! *spelling freedom* from that string so equivalent joins share a key;
//! [`join_skeleton`] also drops `π`/`σ`, matching joins that group the
//! same relations however projections were woven in.

use std::collections::BTreeSet;

/// Canonicalize a Collection name so equivalent joins share a cache key,
/// removing each operator's spelling freedom:
///
/// - **`⋈`** — associative *and* commutative: flatten the chain, sort
///   operands, union keys, re-emit left-deep.
/// - **`⋉`, `▷`** — neither: keep operands and order, sort keys only.
/// - **`π`, `σ`** — canonicalize the inner expression, keep the wrapper.
pub(crate) fn canonical(name: &str) -> String {
    let name = name.trim();

    // π[..](X) / σ[..](X): canonicalize the inner expression, keep the rest.
    if let Some((glyph, bracket, inner)) = split_prefix_op(name) {
        return format!("{glyph}[{bracket}]({})", canonical(inner));
    }

    match split_outer_binop(name) {
        // ⋈ cluster: flatten the whole associative+commutative chain.
        Some(("⋈", ..)) => {
            let mut operands: Vec<&str> = Vec::new();
            let mut keys: BTreeSet<&str> = BTreeSet::new();
            flatten_join(name, &mut operands, &mut keys);
            let mut canon: Vec<String> = operands.iter().map(|o| canonical(o)).collect();
            canon.sort();
            let keystr = keys.into_iter().collect::<Vec<_>>().join(",");
            canon
                .into_iter()
                .reduce(|acc, operand| format!("({acc} ⋈[{keystr}] {operand})"))
                .expect("a ⋈ chain has at least two operands")
        }
        // ⋉ / ▷: order matters — recurse into both sides, sort keys only.
        Some((glyph, left, keys, right)) => {
            let mut k: Vec<&str> = keys
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            k.sort();
            format!(
                "({} {glyph}[{}] {})",
                canonical(left),
                k.join(","),
                canonical(right)
            )
        }
        None => name.to_string(),
    }
}

/// [`canonical`] form with every `π`/`σ` stripped — the bare relation
/// grouping. Codegen weaves projections between joins, the optimizer's
/// `JoinTree` does not; erasing them lets a profiled bad subtree match an
/// optimizer candidate that groups the same relations.
pub(crate) fn join_skeleton(name: &str) -> String {
    canonical(&strip_projections(name))
}

/// Drop every `π`/`σ` wrapper, leaving the bare `⋈`/`⋉`/`▷` tree —
/// neither operator changes *which* relations are grouped, the identity
/// [`join_skeleton`] matches on.
fn strip_projections(name: &str) -> String {
    let name = name.trim();
    if let Some((_glyph, _bracket, inner)) = split_prefix_op(name) {
        return strip_projections(inner);
    }
    match split_outer_binop(name) {
        Some((glyph, left, keys, right)) => format!(
            "({} {glyph}[{keys}] {})",
            strip_projections(left),
            strip_projections(right),
        ),
        None => name.to_string(),
    }
}

/// Collect a maximal `⋈` chain rooted at `name` into a flat operand list
/// and a unioned key set. A non-`⋈` operand (a leaf or `⋉`/`▷`/`π`/`σ`
/// expression) is pushed whole.
fn flatten_join<'a>(name: &'a str, operands: &mut Vec<&'a str>, keys: &mut BTreeSet<&'a str>) {
    match split_outer_binop(name) {
        Some(("⋈", left, k, right)) => {
            keys.extend(k.split(',').map(str::trim).filter(|s| !s.is_empty()));
            flatten_join(left, operands, keys);
            flatten_join(right, operands, keys);
        }
        _ => operands.push(name),
    }
}

/// Parse a prefix-operator name `π[BRACKET](INNER)` or `σ[BRACKET](INNER)`.
/// Returns `(operator glyph, bracket contents, inner expression)`.
pub(crate) fn split_prefix_op(name: &str) -> Option<(&'static str, &str, &str)> {
    for glyph in ["π", "σ"] {
        let Some(rest) = name.strip_prefix(glyph) else {
            continue;
        };
        let rest = rest.strip_prefix('[')?;
        let close = matching_bracket(rest.as_bytes())?;
        let bracket = &rest[..close];
        let inner = rest[close + 1..]
            .trim()
            .strip_prefix('(')?
            .strip_suffix(')')?;
        return Some((glyph, bracket, inner));
    }
    None
}

/// Parse `(LEFT <op>[KEYS] RIGHT)` for `op ∈ {⋈, ⋉, ▷}` at the outer
/// level → `(glyph, left, keys, right)`.
///
/// Scanning `()[]` by byte is safe — no operator glyph's UTF-8 bytes
/// collide with those ASCII bytes, so the depth counter can't be fooled.
pub(crate) fn split_outer_binop(name: &str) -> Option<(&'static str, &str, &str, &str)> {
    let inner = name.strip_prefix('(')?.strip_suffix(')')?;
    let bytes = inner.as_bytes();
    let ops: [(&str, &[u8]); 3] = [
        ("⋈", "⋈[".as_bytes()),
        ("⋉", "⋉[".as_bytes()),
        ("▷", "▷[".as_bytes()),
    ];
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            for (glyph, marker) in ops {
                if bytes[i..].starts_with(marker) {
                    let left = inner[..i].trim();
                    if left.is_empty() {
                        return None;
                    }
                    let after = i + marker.len();
                    let close = after + matching_bracket(&bytes[after..])?;
                    let keys = &inner[after..close];
                    let right = inner[close + 1..].trim();
                    return Some((glyph, left, keys, right));
                }
            }
        }
        i += 1;
    }
    None
}

/// Index of the `]` closing an already-open `[`; `bytes` starts just past
/// that `[`. `None` if the bracket is unbalanced.
fn matching_bracket(bytes: &[u8]) -> Option<usize> {
    let mut depth: i32 = 1;
    for (j, &b) in bytes.iter().enumerate() {
        match b {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(j);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `⋈` is associative *and* commutative — every bracketing and
    /// ordering of one cluster collapses to a single canonical key.
    #[test]
    fn canonical_collapses_rebracketings() {
        let forms = [
            "((a ⋈[i] b) ⋈[k] c)",
            "(a ⋈[k] (b ⋈[i] c))",
            "(c ⋈[k] (b ⋈[i] a))",
            "((c ⋈[i] b) ⋈[k] a)",
        ];
        let want = canonical(forms[0]);
        assert_eq!(want, "((a ⋈[i,k] b) ⋈[i,k] c)");
        for f in forms {
            assert_eq!(canonical(f), want, "{f} should canonicalize to {want}");
        }
    }

    /// `⋉` is neither commutative nor associative — inside a `⋈` chain it
    /// stays an opaque, order-preserved operand while the `⋈` flattens.
    #[test]
    fn canonical_keeps_semijoin_as_opaque_operand() {
        assert_eq!(canonical("((a ⋉[x] b) ⋈[y] c)"), "((a ⋉[x] b) ⋈[y] c)");
        assert_eq!(canonical("(c ⋈[y] (a ⋉[x] b))"), "((a ⋉[x] b) ⋈[y] c)");
    }

    /// `join_skeleton` sees through the `π` projections codegen weaves
    /// between joins, so a profiled plan and an optimizer candidate land
    /// on one key.
    #[test]
    fn join_skeleton_is_projection_transparent() {
        let profiled = "((a ⋈[k] b) ⋈[j] π[x,y]((c ⋈[m] d)))";
        let optimizer = "((a ⋈[k] b) ⋈[j] (c ⋈[m] d))";
        assert_eq!(join_skeleton(profiled), join_skeleton(optimizer));
    }
}
