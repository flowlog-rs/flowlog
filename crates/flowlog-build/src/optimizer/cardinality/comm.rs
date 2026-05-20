//! Shared cardinality-estimation core.
//!
//! Everything here is used by *both* the pre-pass ([`super::pre`]) and the
//! DP cost model ([`super::dp`]): the textbook [`join_estimate`] formula,
//! the [`ColumnTrace`] map, and the variable → EDB-column tracing.
//!
//! # Variable → EDB column tracing
//!
//! Both estimators need, for a rule variable, the distinct-value count —
//! and numeric range — of the EDB column it ultimately draws from. A
//! variable sitting directly in an EDB body atom takes that column's
//! scanned stats; a variable seen only in IDB atoms is traced transitively
//! through those IDBs' own rules. The trace is a fixpoint over
//! (relation, column) → [`ColumnTrace`], run once in [`trace_columns`].

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::optimizer::feedback::{ColumnStats, Feedback};
use crate::parser::{AtomArg, FlowLogRule, HeadArg, Predicate, Program};

/// Traced statistics of a `(relation, column)` position — the
/// distinct-value domain and, for a numeric column, the value range.
///
/// Built by the [`trace_columns`] fixpoint; `Optimizer::new` folds it into
/// each relation's per-column [`ColumnStats`], so the cost model reads a
/// join key's domain and range straight off [`Feedback`].
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ColumnTrace {
    /// Distinct-value count — `0` until known.
    pub(crate) domain: u64,
    /// Numeric value range `[min, max]`; `None` for a non-numeric column
    /// or one not soundly boundable. `min > max` marks a provably empty
    /// (disjoint) column — see [`ColumnTrace::is_disjoint`].
    pub(crate) range: Option<(i64, i64)>,
}

impl ColumnTrace {
    /// Has this column been narrowed to an empty value range — e.g. a join
    /// key whose two sides' ranges do not overlap? Such a column binds no
    /// value, so any join on it is empty.
    pub(super) fn is_disjoint(&self) -> bool {
        matches!(self.range, Some((lo, hi)) if lo > hi)
    }
}

impl From<&ColumnStats> for ColumnTrace {
    fn from(stats: &ColumnStats) -> Self {
        Self {
            domain: stats.domain.unwrap_or(0),
            range: stats.range(),
        }
    }
}

/// `(relation name, column index)` → traced [`ColumnTrace`]. An EDB
/// position is seeded from its scanned stats; an IDB position is resolved
/// by the [`trace_columns`] fixpoint.
pub(crate) type Traces = HashMap<(String, usize), ColumnTrace>;

/// Intersection of two optional value ranges — the values a join key can
/// take on *both* sides. An unknown range (`None`) acts as the universal
/// set, never tightening; a result with `min > max` signals the two
/// ranges are disjoint.
pub(super) fn intersect(a: Option<(i64, i64)>, b: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (a, b) {
        (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.max(blo), ahi.min(bhi))),
        (known, None) | (None, known) => known,
    }
}

/// Union of two optional value ranges — the bounding interval covering
/// both, since a relation defined by several rules holds every rule's
/// rows. An unknown range (`None`) absorbs: the column cannot be bounded.
fn union(a: Option<(i64, i64)>, b: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (a, b) {
        (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.min(blo), ahi.max(bhi))),
        _ => None,
    }
}

/// Compute, for every relation column in the program, the distinct-value
/// domain and numeric range it draws from — seeded from EDB column stats
/// and propagated to IDB head columns by a fixpoint over the rules.
///
/// Both quantities widen monotonically: a head column's domain is the
/// `max` over the rules that define it and its range the `union`, so a
/// column reachable from several rules gets the loosest (safest) bound.
/// Within one rule body a variable's stats are the *intersection* across
/// the atoms it occurs in — the join binds it to satisfy every occurrence.
pub(crate) fn trace_columns(feedback: &Feedback, program: &Program) -> Traces {
    let mut traces: Traces = HashMap::new();

    // Seed: each scanned EDB column.
    for rel in program.edbs() {
        if let Some(cols) = feedback.columns(rel.name()) {
            for (i, stats) in cols.iter().enumerate() {
                traces.insert((rel.name().to_string(), i), ColumnTrace::from(stats));
            }
        }
    }

    // Propagate to IDB head columns until nothing changes.
    let rules = program.rules();
    let mut changed = true;
    while changed {
        changed = false;
        for rule in &rules {
            let var_traces = body_var_traces(rule, &traces);
            for (col, arg) in rule.head().head_arguments().iter().enumerate() {
                // `domain` follows any single head variable (a loose upper
                // bound); `range` only a *plain* pass-through `Var` — an
                // arithmetic/aggregation head shifts or destroys it, so it
                // is left unbounded (`None`).
                let Some(domain) = arg
                    .vars()
                    .first()
                    .and_then(|v| var_traces.get(*v))
                    .map(|t| t.domain)
                else {
                    continue;
                };
                let range = match arg {
                    HeadArg::Var(v) => var_traces.get(v).and_then(|t| t.range),
                    _ => None,
                };
                let key = (rule.head().name().to_string(), col);
                match traces.entry(key) {
                    Entry::Vacant(e) => {
                        e.insert(ColumnTrace { domain, range });
                        changed = true;
                    }
                    Entry::Occupied(mut e) => {
                        let slot = e.get_mut();
                        let merged = ColumnTrace {
                            domain: slot.domain.max(domain),
                            range: union(slot.range, range),
                        };
                        if (merged.domain, merged.range) != (slot.domain, slot.range) {
                            *slot = merged;
                            changed = true;
                        }
                    }
                }
            }
        }
    }
    traces
}

/// Traced [`ColumnTrace`] of every variable bound by a rule's positive
/// body atoms, given the currently-known column traces.
///
/// A variable appearing in several atoms is *intersected*: the join binds
/// it to no more than its most selective occurrence — `min` distinct-value
/// domain, intersected value range.
fn body_var_traces(rule: &FlowLogRule, traces: &Traces) -> HashMap<String, ColumnTrace> {
    let mut out: HashMap<String, ColumnTrace> = HashMap::new();
    for pred in rule.rhs() {
        let Predicate::PositiveAtom(atom) = pred else {
            continue;
        };
        for (col, arg) in atom.arguments().iter().enumerate() {
            let AtomArg::Var(v) = arg else { continue };
            let Some(&ct) = traces.get(&(atom.name().to_string(), col)) else {
                continue;
            };
            out.entry(v.clone())
                .and_modify(|cur| {
                    cur.domain = cur.domain.min(ct.domain);
                    cur.range = intersect(cur.range, ct.range);
                })
                .or_insert(ct);
        }
    }
    out
}

/// Textbook two-way join size: `|L|·|R| / max(d_L, d_R)`.
///
/// `key_domain` is the distinct-value count of the join key (`None` for a
/// cartesian product — no shared variable — which keeps the full product).
/// `degree_cap`, when present, is `max_degree` of the right side's EDB key
/// column: `|L|·degree_cap` is then an independent upper bound and the
/// estimate takes the tighter of the two. Saturates rather than wrapping.
pub(crate) fn join_estimate(
    l_size: u64,
    r_size: u64,
    key_domain: Option<u64>,
    degree_cap: Option<u64>,
) -> u64 {
    let product = l_size.saturating_mul(r_size);
    let textbook = match key_domain {
        Some(d) if d > 0 => (product / d).max(1),
        _ => product,
    };
    match degree_cap {
        Some(deg) => textbook.min(l_size.saturating_mul(deg).max(1)),
        None => textbook,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- join_estimate: the textbook two-way formula -------------------

    #[test]
    fn join_estimate_divides_by_key_domain() {
        // Foreign-key join: |L|·|R| / d. With d == |R| this recovers |L|.
        assert_eq!(join_estimate(1_000, 500, Some(500), None), 1_000);
        // A larger key domain shrinks the result further.
        assert!(
            join_estimate(1_000, 500, Some(1_000), None)
                < join_estimate(1_000, 500, Some(100), None)
        );
    }

    #[test]
    fn join_estimate_cartesian_keeps_full_product() {
        // No shared variable → full cartesian product.
        assert_eq!(join_estimate(1_000, 500, None, None), 500_000);
        // A zero key domain is treated as no key, not a divide-by-zero.
        assert_eq!(join_estimate(1_000, 500, Some(0), None), 500_000);
    }

    #[test]
    fn join_estimate_degree_cap_tightens_estimate() {
        // Degree cap `|L|·max_degree` wins when it is below the textbook
        // estimate — a low-skew EDB key fans each L row out to few rows.
        assert_eq!(join_estimate(1_000, 500, Some(100), Some(2)), 2_000);
        // ... but a loose cap leaves the textbook estimate untouched.
        assert_eq!(join_estimate(1_000, 500, Some(100), Some(9_999)), 5_000);
    }

    #[test]
    fn join_estimate_saturates_on_overflow() {
        assert_eq!(join_estimate(u64::MAX, u64::MAX, None, None), u64::MAX);
        assert!(join_estimate(u64::MAX, u64::MAX, Some(2), None) > 0);
    }

    // ---- range algebra: intersect / union -----------------------------

    #[test]
    fn intersect_is_the_tighter_overlap() {
        // Larger min, smaller max.
        assert_eq!(intersect(Some((0, 100)), Some((50, 200))), Some((50, 100)));
        // An unknown range never tightens — it acts as the universal set.
        assert_eq!(intersect(Some((0, 100)), None), Some((0, 100)));
        // Disjoint ranges produce an empty `min > max` interval.
        let empty = intersect(Some((0, 10)), Some((20, 30))).expect("some");
        assert!(empty.0 > empty.1);
    }

    #[test]
    fn union_is_the_bounding_interval() {
        // Smaller min, larger max — covers every rule's rows.
        assert_eq!(union(Some((10, 20)), Some((0, 5))), Some((0, 20)));
        // An unknown range absorbs: the column cannot be bounded.
        assert_eq!(union(Some((0, 100)), None), None);
    }
}
