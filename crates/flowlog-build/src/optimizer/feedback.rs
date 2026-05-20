//! Profile-feedback loader.
//!
//! Reads a `<stem>_log/` directory produced by a profiled run and builds
//! the cardinality cache + bad-subtree set the optimizer's cost model
//! consults.
//!
//! ## How a measurement reaches a collection
//!
//! Every arrangement is named — at codegen, in `register_arrangement` —
//! with the **canonical** form of the collection it indexes, via
//! differential dataflow's `arrange_by_{key,self}_named`. Timely's
//! `Operates` event carries that name, and the memory profiler writes it
//! into the `name` column of `memory/memory_worker_t0_*.log`.
//!
//! So the loader simply reads that column: the runtime tells us, by name,
//! which collection each arrangement holds — no operator-address
//! prediction, no DAG reconstruction.
//!
//! Only arrangements are observable. A transformation that lowers to a
//! `map`/`flat_map`/to-`Row` operator builds no arrangement and so has no
//! measurement — in practice the rule *head* join. Its output is the head
//! relation, which is itself measured under its bare name because every
//! recursive IDB is arranged when consumed.
//!
//! This module holds the [`Feedback`] store and the memory-log scanner
//! that fills its cardinality cache. The other two subsystems `load`
//! orchestrates live alongside it: the hog detector in [`hog`](super::hog)
//! and name canonicalization in [`canonical`](super::canonical).

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::optimizer::canonical::canonical;
use crate::optimizer::error::OptimizerError;
use crate::optimizer::hog;

// =========================================================================
// Feedback — the optimizer's cost-data store
// =========================================================================

/// Statistics of one relation column, stored in [`RelationStats::columns`].
///
/// Every field is optional so a single type serves both kinds of column:
/// an **EDB column**, scanned from the fact file, and an **IDB column**,
/// whose stats are propagated by the column-domain trace. A `None` field
/// means "unknown — estimate it".
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ColumnStats {
    /// Distinct-value domain — the textbook join formula's join-key domain
    /// size. For an EDB column it is the fact-file distinct count tightened
    /// by the numeric range (`seed_edb_cardinalities`); for an IDB column
    /// the domain trace propagates it from the feeding EDB columns.
    pub(crate) domain: Option<u64>,
    /// Numeric value range `[min, max]`. For an EDB column the fact-file
    /// scan fills it; for an IDB column the trace propagates it —
    /// intersected through joins, unioned across rules. `None` for a
    /// non-numeric column or one whose range cannot be soundly bounded
    /// (e.g. an arithmetic head argument).
    pub(crate) min: Option<i64>,
    pub(crate) max: Option<i64>,
}

impl ColumnStats {
    /// The numeric value range, when both bounds are known.
    pub(crate) fn range(&self) -> Option<(i64, i64)> {
        Some((self.min?, self.max?))
    }
}

/// Per-relation statistics, keyed by canonical name in [`Feedback::cache`].
///
/// The two fields have different lifetimes, which is why `RelationStats`
/// holds both: `rows` is **profile feedback** — persisted across wire-back
/// rounds through the `feedback_accum.json` accumulator. `columns` is a
/// **static input** — re-derived every compile, so it is `#[serde(skip)]`.
/// `#[serde(transparent)]` therefore serializes a `RelationStats` as the
/// bare `rows` number (`{"rel": 42}`).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RelationStats {
    /// Measured cardinality — a lower bound: cumulative arrangement
    /// throughput, which for a recursive stratum is total fixpoint work
    /// rather than a peak size. Aggregated by `max`.
    rows: u64,
    /// Per-column [`ColumnStats`]; empty until a column has been scanned
    /// (EDB) or traced by the pre-pass (IDB).
    #[serde(skip)]
    columns: Vec<ColumnStats>,
}

/// Everything the optimizer's cost model consults, in two kinds:
///
/// * **Profile feedback** — `bad_subtrees`, and the `rows` of each `cache`
///   entry. Measurements from prior `--profile-hint` runs. *Persisted*:
///   `merge`/`insert` aggregate it monotonically, and it is the on-disk
///   `feedback_accum.json` accumulator format, so a measured bad plan is
///   remembered across wire-back rounds.
/// * **Static cardinality inputs** — the per-column [`ColumnStats`] of each
///   `cache` entry. Derived from the EDB fact files and the cardinality
///   pre-pass, *not persisted*: re-derived every compile.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct Feedback {
    /// Canonical relation name → [`RelationStats`]. `#[serde(default)]`
    /// keeps an older accumulator file loadable.
    #[serde(default)]
    cache: HashMap<String, RelationStats>,
    /// Projection-transparent skeletons (see [`join_skeleton`]) of join
    /// intermediates the detector found to explode — the DP steers away
    /// from rebuilding them. Aggregated by union.
    #[serde(default)]
    bad_subtrees: HashSet<String>,
}

impl Feedback {
    // ---- Loading, persistence, and merging --------------------------------

    /// Load a hint directory: memory logs build the `cache`, and
    /// `ops.json` (if present) drives the hog detector to fill
    /// `bad_subtrees`. A missing directory yields empty feedback — an
    /// expected case (it may belong to a different program), not an error.
    pub(crate) fn load(dir: &Path) -> Result<Self, OptimizerError> {
        let cache = scan_memory_logs(&dir.join("memory")).map_err(OptimizerError::internal)?;
        let bad_subtrees = hog::detect_bad_subtrees(&dir.join("ops.json"), &cache)?;
        Ok(Self {
            cache: cache
                .into_iter()
                .map(|(name, rows)| {
                    (
                        name,
                        RelationStats {
                            rows,
                            columns: Vec::new(),
                        },
                    )
                })
                .collect(),
            bad_subtrees,
        })
    }

    /// Load a previously-saved accumulator file. A missing file yields
    /// empty feedback (expected on the first round).
    pub(crate) fn load_accum(path: &Path) -> Result<Self, OptimizerError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let file = File::open(path).map_err(OptimizerError::internal)?;
        serde_json::from_reader(BufReader::new(file)).map_err(OptimizerError::internal)
    }

    /// Persist this feedback to `path` as JSON, carrying measurements
    /// across wire-back rounds.
    ///
    /// Each profiled run overwrites the raw memory logs, so a compile only
    /// sees the last run's plan. The accumulator keeps every measurement
    /// ever seen (`max`-merged), so a rule the optimizer has already fixed
    /// keeps its prior bad-plan cost on record and is not undone.
    pub(crate) fn save_accum(&self, path: &Path) -> Result<(), OptimizerError> {
        let json = serde_json::to_string(self).map_err(OptimizerError::internal)?;
        fs::write(path, json).map_err(OptimizerError::internal)?;
        Ok(())
    }

    /// Merge another feedback in: per-relation `rows` `max`, `columns`
    /// kept whenever this side lacks them, `bad_subtrees` unioned (a join
    /// known to explode stays flagged across rounds).
    pub(crate) fn merge(&mut self, other: Feedback) {
        for (k, v) in other.cache {
            self.cache
                .entry(k)
                .and_modify(|c| {
                    c.rows = c.rows.max(v.rows);
                    if c.columns.is_empty() {
                        c.columns = v.columns.clone();
                    }
                })
                .or_insert(v);
        }
        self.bad_subtrees.extend(other.bad_subtrees);
    }

    // ---- Relation cardinality cache ---------------------------------------

    /// Insert a single (name, cardinality) measurement, canonicalizing the
    /// name and `max`-aggregating. Used to seed EDB row counts from the
    /// fact directory alongside any profile-hint data.
    pub(crate) fn insert(&mut self, name: &str, card: u64) {
        self.cache
            .entry(canonical(name))
            .and_modify(|c| c.rows = c.rows.max(card))
            .or_insert(RelationStats {
                rows: card,
                columns: Vec::new(),
            });
    }

    /// Look up a measured cardinality by canonical collection name.
    pub(crate) fn lookup(&self, canonical_name: &str) -> Option<u64> {
        self.cache.get(canonical_name).map(|s| s.rows)
    }

    /// Largest cardinality in the cache, `0` when it is empty — the cost
    /// model's upper-bound fallback for an untraced leaf relation.
    pub(crate) fn max_cardinality(&self) -> u64 {
        self.cache.values().map(|s| s.rows).max().unwrap_or(0)
    }

    // ---- Per-column statistics --------------------------------------------

    /// Record the per-column statistics of an EDB relation, derived from
    /// its fact file. The name is canonicalized to match `cache` keys.
    pub(crate) fn set_columns(&mut self, name: &str, columns: Vec<ColumnStats>) {
        self.cache.entry(canonical(name)).or_default().columns = columns;
    }

    /// Record a column's traced distinct-value domain and numeric range,
    /// growing the relation's column vector to fit. The pre-pass uses this
    /// to fold its IDB-column trace fixpoint into the per-relation stats.
    pub(crate) fn set_column_trace(
        &mut self,
        name: &str,
        col: usize,
        domain: u64,
        range: Option<(i64, i64)>,
    ) {
        let columns = &mut self.cache.entry(canonical(name)).or_default().columns;
        if columns.len() <= col {
            columns.resize(col + 1, ColumnStats::default());
        }
        columns[col].domain = Some(domain);
        if let Some((min, max)) = range {
            columns[col].min = Some(min);
            columns[col].max = Some(max);
        }
    }

    /// Per-column statistics of a relation, if any column has been scanned
    /// (EDB) or traced (IDB).
    pub(crate) fn columns(&self, name: &str) -> Option<&[ColumnStats]> {
        self.cache
            .get(&canonical(name))
            .map(|s| s.columns.as_slice())
            .filter(|c| !c.is_empty())
    }

    /// Statistics of one relation column — `None` when the column was
    /// never scanned or traced, so the cost model must estimate it.
    pub(crate) fn column(&self, name: &str, col: usize) -> Option<&ColumnStats> {
        self.cache.get(&canonical(name))?.columns.get(col)
    }

    // ---- Bad subtrees and diagnostics -------------------------------------

    /// Projection-transparent skeletons of join intermediates that
    /// exploded. The optimizer's DP penalty consults this set.
    pub(crate) fn bad_subtrees(&self) -> &HashSet<String> {
        &self.bad_subtrees
    }

    /// Number of cached `name → cardinality` entries (for diagnostics).
    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }
}

// =========================================================================
// Memory-log scanning: memory/*.log → canonical name → cardinality
// =========================================================================

/// Per-arrangement-operator counters, summed across worker files.
#[derive(Default, Debug, Clone, Copy)]
struct AddrStats {
    batched_in: u64,
    merge_out: u64,
}

impl AddrStats {
    /// Cardinality estimate for this arrangement — the larger of total
    /// tuples batched in and total tuples emitted by compaction merges.
    fn estimate(&self) -> u64 {
        self.batched_in.max(self.merge_out)
    }
}

/// Scan a `memory/` directory into `canonical name → cardinality`.
///
/// Aggregation is two-axis: counters for one operator address are
/// **summed** across worker files (worker partitions of one arrangement),
/// then collapsed to the canonical name with **max** across distinct
/// arrangements that share a name (each ingests the whole collection).
fn scan_memory_logs(mem_dir: &Path) -> Result<HashMap<String, u64>, std::io::Error> {
    // operator address → (counters summed across workers, operator name).
    let mut by_addr: HashMap<Vec<u32>, (AddrStats, String)> = HashMap::new();
    if mem_dir.exists() {
        for entry in fs::read_dir(mem_dir)? {
            let path = entry?.path();
            if path.is_file() {
                parse_memory_file(&path, &mut by_addr)?;
            }
        }
    }

    let mut cache: HashMap<String, u64> = HashMap::new();
    for (_addr, (stats, name)) in by_addr {
        let est = stats.estimate();
        if est == 0 {
            continue;
        }
        cache
            .entry(canonical(&name))
            .and_modify(|c| *c = (*c).max(est))
            .or_insert(est);
    }
    Ok(cache)
}

/// Parse one `memory_worker_t0_*.log` file, folding its rows into
/// `by_addr` (counters per address summed across worker files).
fn parse_memory_file(
    path: &Path,
    by_addr: &mut HashMap<Vec<u32>, (AddrStats, String)>,
) -> Result<(), std::io::Error> {
    let reader = BufReader::new(File::open(path)?);
    for line in reader.lines() {
        let Some((addr, stats, name)) = parse_memory_line(&line?) else {
            continue;
        };
        let e = by_addr
            .entry(addr)
            .or_insert_with(|| (AddrStats::default(), name));
        e.0.batched_in = e.0.batched_in.saturating_add(stats.batched_in);
        e.0.merge_out = e.0.merge_out.saturating_add(stats.merge_out);
    }
    Ok(())
}

/// Parse one memory-log row into `(addr, counters, operator name)`.
///
/// Row format (whitespace-separated, column-padded):
///   `<addr> <batched_in> <merges> <merge_in> <merge_out> <dropped>`
///   `<batch_count> <batcher_bytes> <name>`
/// where `<addr>` is a `[d, d, d]` bracketed comma-list and `<name>` is
/// the rest of the line — a canonical collection name may contain spaces
/// (`(arc ⋈[x] reach)`), so it is rejoined with single spaces, matching
/// what [`canonical`] emits.
///
/// `None` for the header, blank lines, and any row without a parseable
/// `[addr]` and the eight stat columns.
fn parse_memory_line(line: &str) -> Option<(Vec<u32>, AddrStats, String)> {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with("addr") {
        return None;
    }
    let close = trimmed.find(']')?;
    let addr = parse_addr(&trimmed[..=close])?;
    // batched_in merges merge_in merge_out dropped batch_count batcher_bytes name…
    let cols: Vec<&str> = trimmed[close + 1..].split_whitespace().collect();
    if cols.len() < 8 {
        return None;
    }
    let stats = AddrStats {
        batched_in: cols[0].parse().unwrap_or(0),
        merge_out: cols[3].parse().unwrap_or(0),
    };
    Some((addr, stats, cols[7..].join(" ")))
}

/// `[0, 14, 10]` → `vec![0, 14, 10]`. Returns `None` on malformed input.
fn parse_addr(s: &str) -> Option<Vec<u32>> {
    let inner = s.strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    inner
        .split(',')
        .map(|p| p.trim().parse::<u32>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A memory-log row's `name` is the rest of the line — a canonical
    /// name has internal spaces, so the column split must rejoin it intact.
    #[test]
    fn parse_memory_line_extracts_addr_stats_and_name() {
        // addr batched_in merges merge_in merge_out dropped batch_count batcher_bytes name…
        let (addr, stats, name) = parse_memory_line(
            "[0, 27, 10]   2   3   3   5   2   9   128   π[y,z]((load ⋈[x] pointsto))",
        )
        .expect("valid row");
        assert_eq!(addr, vec![0, 27, 10]);
        assert_eq!((stats.batched_in, stats.merge_out), (2, 5));
        assert_eq!(name, "π[y,z]((load ⋈[x] pointsto))");
    }
}
