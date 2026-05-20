//! Hog detector — which join subtrees blew up in a profiled run.
//!
//! A profiled run drops an `ops.json`: the static plan graph, one
//! transformation DAG per rule. [`detect_bad_subtrees`] sizes each join
//! from the measured cardinality cache and flags the bad ones in two steps:
//!
//! 1. **Per rule** — *wasteful?* The plan's `peak` intermediate vs the
//!    rule's output: a plan that balloons far past what it keeps is wasteful.
//! 2. **Per node, inside a wasteful rule** — flag every join that *jumped*,
//!    i.e. grew far past its largest input. Those are the growth points; a
//!    join that only inherited a big intermediate is a victim, left alone.
//!
//! Two steps because the *jump* (where a blowup grew) and the *waste* (a
//! node huge vs the output) often land on different nodes of a deep plan —
//! one per-node "wasteful AND jumped" test misses both. The DP then
//! penalizes any candidate that rebuilds a flagged `join_skeleton`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use tracing::debug;

use crate::optimizer::canonical::{canonical, join_skeleton};
use crate::optimizer::error::OptimizerError;
use crate::profiler::Profiler;

/// Waste gate: a rule's plan is wasteful when its peak intermediate
/// exceeds this multiple of the rule's output (head relation) size.
const EXPLODE_RATIO: u64 = 1000;

/// Jump gate: a join is a blowup growth point only when it grew past this
/// multiple of its largest input join — not if it merely inherited an
/// already-huge intermediate from a child.
const JUMP_RATIO: u64 = 10;

/// Detect the exploded join subtrees recorded in a profiled run's
/// `ops.json` at `path`, sized against the measured cardinality `cache`.
///
/// A missing `ops.json` yields an empty set — an expected case, not every
/// run is profiled. An unreadable or malformed one is a corrupt compiler
/// artifact, surfaced as an [`OptimizerError`].
pub(super) fn detect_bad_subtrees(
    path: &Path,
    cache: &HashMap<String, u64>,
) -> Result<HashSet<String>, OptimizerError> {
    // Open-then-match avoids a TOCTOU exists() + open() race; the
    // not-profiled case is just `NotFound` on the open.
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(OptimizerError::internal(e)),
    };
    // `ops.json` is a serialized [`Profiler`] — the static plan graph baked
    // into the generated source and dropped next to the runtime logs. The
    // detector reads it back through that same type, so the schema can't
    // drift between profiler (writer) and optimizer (reader).
    let ops: Profiler =
        serde_json::from_reader(BufReader::new(file)).map_err(OptimizerError::internal)?;
    Ok(collect_hogs(&ops, cache))
}

// =========================================================================
// Detection — wasteful rules, jumped joins
// =========================================================================

/// Head relation name of a rule — the identifier before the first `(`.
fn rule_head(rule_text: &str) -> &str {
    rule_text.split('(').next().unwrap_or("").trim()
}

/// Is a rule's plan wasteful — did its `peak` intermediate balloon past
/// [`EXPLODE_RATIO`]× the output the rule ultimately keeps?
///
/// `rule_output == 0` — a truncated profile, the rule never finished —
/// makes any measured peak wasteful: a rule bad enough to stall its own
/// completion is exactly what we want to flag.
fn is_wasteful(peak: u64, rule_output: u64) -> bool {
    peak > rule_output.saturating_mul(EXPLODE_RATIO)
}

/// Did this join's size jump past [`JUMP_RATIO`]× its largest input — the
/// place a blowup grew, as opposed to a node that merely carried one
/// through. `max_child == 0` (no measured input) counts as a jump.
fn jumped(size: u64, max_child: u64) -> bool {
    max_child == 0 || size > max_child.saturating_mul(JUMP_RATIO)
}

/// One measured `⋈` join of a rule's plan: its canonical name, output
/// size, and the size of its largest input join.
struct Join<'a> {
    name: &'a str,
    size: u64,
    max_child: u64,
}

/// Flag the [`join_skeleton`] of every blowup growth point: walk each
/// rule, and if the plan is wasteful (its peak dwarfs the rule output),
/// flag every `⋈` that jumped. Sizes come from `cache`, keyed by canonical
/// transformation name.
fn collect_hogs(ops: &Profiler, cache: &HashMap<String, u64>) -> HashSet<String> {
    // fingerprint → canonical transformation name (the cache key).
    let fp_name: HashMap<&str, String> = ops
        .nodes()
        .iter()
        .filter_map(|n| Some((n.fingerprint()?, canonical(n.transformation_name()?))))
        .collect();
    let size_of = |fp: &str| -> u64 {
        fp_name
            .get(fp)
            .and_then(|name| cache.get(name))
            .copied()
            .unwrap_or(0)
    };

    let mut bad = HashSet::new();
    for rule in ops.rules() {
        let head = rule_head(rule.text());
        let rule_output = cache.get(&canonical(head)).copied().unwrap_or(0);

        // The rule's measured `⋈` joins — only those can be a hog.
        let joins: Vec<Join> = rule
            .plan_tree()
            .iter()
            .filter_map(|ptn| {
                let name = fp_name.get(ptn.fingerprint())?;
                if !name.contains('⋈') {
                    return None;
                }
                let size = cache.get(name).copied().unwrap_or(0);
                if size == 0 {
                    return None; // unmeasured — nothing to judge
                }
                let max_child = ptn
                    .parents()
                    .iter()
                    .map(|fp| size_of(fp))
                    .max()
                    .unwrap_or(0);
                Some(Join {
                    name,
                    size,
                    max_child,
                })
            })
            .collect();

        // Step 1: is the plan wasteful? Its peak intermediate vs the output.
        let peak = joins.iter().map(|j| j.size).max().unwrap_or(0);
        if !is_wasteful(peak, rule_output) {
            continue;
        }
        // Step 2: inside a wasteful rule, every join that jumped is a
        // growth point — flag it.
        for join in &joins {
            if jumped(join.size, join.max_child) {
                let skeleton = join_skeleton(join.name);
                debug!(
                    "hog[{head}] size={} max_child={} peak={peak} \
                     rule_output={rule_output} skeleton={skeleton}",
                    join.size, join.max_child,
                );
                bad.insert(skeleton);
            }
        }
    }
    bad
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A plan is wasteful when its peak intermediate dwarfs the rule's
    /// output; a truncated profile (`rule_output` 0) is always wasteful.
    #[test]
    fn is_wasteful_compares_peak_to_output() {
        // Peak 3000× the output — wasteful.
        assert!(is_wasteful(3_000_000, 1_000));
        // Peak only 100× the output — within budget.
        assert!(!is_wasteful(100_000, 1_000));
        // Truncated profile: any measured peak is wasteful.
        assert!(is_wasteful(5, 0));
    }

    /// A join jumped when it grew past `JUMP_RATIO`× its largest input;
    /// growing only modestly means it inherited the size, not caused it.
    #[test]
    fn jumped_flags_growth_not_inheritance() {
        assert!(jumped(2_000_000, 100_000)); // grew 20× — a growth point
        assert!(!jumped(500_000, 100_000)); // grew 5× — inherited
        assert!(jumped(2_000_000, 0)); // no measured input — counts as a jump
    }
}
