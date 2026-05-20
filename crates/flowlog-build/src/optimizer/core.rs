//! Optimizer for FlowLog Datalog programs.
//!
//! `Optimizer::new` loads any `--profile-hint` feedback, scans the EDB fact
//! files for row counts and per-column statistics, and traces each relation
//! column's value domain. IDB sizes are *not* computed up front: as the
//! planner walks strata, [`Optimizer::plan_stratum`] records every rule's
//! estimated output size, so a later stratum sees the earlier ones as
//! cached leaves. The planner just constructs an optimizer and calls
//! `plan_stratum` — it never touches fact files or profile directories.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tracing::debug;

use crate::catalog::Catalog;
use crate::common::Config;
use crate::optimizer::cardinality::{CostEstimator, trace_columns};
use crate::optimizer::error::OptimizerError;
use crate::optimizer::feedback::{ColumnStats, Feedback};
use crate::parser::Program;

#[derive(Debug, Default)]
pub struct Optimizer {
    /// Cost data the DP consults: `--profile-hint` measurements, EDB row
    /// counts + column stats and domain traces, and IDB sizes recorded as
    /// strata are planned.
    feedback: Feedback,
    /// Head relations whose IDB size is already recorded, so `plan_stratum`
    /// captures each exactly once — re-recording a recursive head every
    /// round would compound its own estimate.
    sized: HashSet<String>,
}

impl Optimizer {
    /// Build an optimizer for `program`: load any `--profile-hint`
    /// feedback, scan the EDB fact files for row counts and per-column
    /// statistics, and trace each relation column's value domain.
    ///
    /// IDB sizes are not computed here — [`Optimizer::plan_stratum`]
    /// records them as it plans each stratum.
    ///
    /// Fails only when a `--profile-hint` directory exists but one of its
    /// files is unreadable or malformed — a corrupt compiler artifact. A
    /// *missing* hint directory is not an error: it yields empty feedback.
    pub fn new(config: &Config, program: &Program) -> Result<Self, OptimizerError> {
        let mut feedback = if config.profile_hint().is_empty() {
            Feedback::default()
        } else {
            load_profile_feedback(config.profile_hint())?
        };
        // EDB row counts + per-column stats give the estimator real leaf
        // sizes for `.input`-backed relations even without a profile hint.
        // The fact dir is optional.
        if let Some(dir) = config.fact_dir() {
            seed_edb_cardinalities(&mut feedback, program, dir);
        }
        // Trace every relation column's distinct-value domain and numeric
        // range into the per-column stats. Structural (rule shape only, no
        // sizes), so it runs once here; IDB sizes are filled in per stratum.
        for ((rel, col), trace) in &trace_columns(&feedback, program) {
            feedback.set_column_trace(rel, *col, trace.domain, trace.range);
        }
        debug!(
            "optimizer feedback assembled: {} cached collections",
            feedback.len(),
        );
        Ok(Self {
            feedback,
            sized: HashSet::new(),
        })
    }

    /// Plan one round of stratum-wide joins, and — the first time each
    /// head is seen — record its IDB size.
    ///
    /// Returns one entry per catalog in `catalogs`:
    /// - `None` when the catalog is already fully planned (no join left).
    /// - `Some((i, j))` — the next pair of live positive-atom indices to
    ///   join, picked by the DP search over the current atoms.
    ///
    /// The DP re-runs against the live catalog each round (atom indices
    /// shift after `join_modify`). The DP also yields each rule's estimated
    /// output size; summed per head relation it is the IDB's cardinality,
    /// recorded into the cache so a later stratum sizes it as a cached
    /// leaf. That capture happens once per head (tracked by `sized`):
    /// re-recording a recursive head each round would compound its own
    /// estimate.
    pub fn plan_stratum(&mut self, catalogs: &[Catalog]) -> Vec<Option<(usize, usize)>> {
        let mut decisions = Vec::with_capacity(catalogs.len());
        // Head relation → summed output size of its rules, for heads not
        // yet recorded.
        let mut idb_sizes: HashMap<String, u64> = HashMap::new();
        {
            let estimator = CostEstimator::new(&self.feedback);
            for catalog in catalogs {
                let (output, tree) = estimator.plan(catalog);
                if let Some(tree) = &tree {
                    debug!("\nRule:\n  {:?}\nJoin tree:\n  {}", catalog.rule(), tree);
                }
                decisions.push(tree.and_then(|t| t.first_join_pair()));

                let head = catalog.rule().head().name();
                if !self.sized.contains(head) {
                    *idb_sizes.entry(head.to_string()).or_default() += output;
                }
            }
        }
        for (head, size) in idb_sizes {
            self.feedback.insert(&head, size);
            self.sized.insert(head);
        }
        decisions
    }
}

// =========================================================================
// Feedback assembly — file-backed inputs the optimizer owns
// =========================================================================

/// Scan each EDB's CSV fact file and feed its row count into `feedback`
/// under the relation's declared name, so the cost estimator sees real
/// `Measured` leaf sizes during planning even with no `--profile-hint`.
///
/// The same single pass also collects per-column [`ColumnStats`] — the
/// distinct-value domain and numeric range — and stores them on
/// `feedback`. The domain trace propagates these to the IDBs.
///
/// A missing or unreadable fact file just means that EDB stays `Unknown`
/// — the planner still proceeds on the default leaf size.
fn seed_edb_cardinalities(feedback: &mut Feedback, program: &Program, fact_dir: &str) {
    for rel in program.edbs() {
        if !rel.is_file_backed() {
            continue;
        }
        let path = Path::new(fact_dir).join(rel.input_file_name());
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                debug!(target: "flowlog_build::optimizer",
                    "skipping EDB stats for {}: {e}", path.display());
                continue;
            }
        };

        let arity = rel.arity().max(1);
        let delim = rel.input_delimiter();
        // Per-column distinct-value sets, for the `distinct` domain count.
        // Numeric bounds are tracked alongside in the same pass.
        // `contains` before `insert` avoids allocating a String on each hit
        // — for wide, low-cardinality columns this is most of the cells.
        let mut columns: Vec<HashSet<String>> = vec![HashSet::new(); arity];
        let mut bounds: Vec<(i64, i64)> = vec![(i64::MAX, i64::MIN); arity];
        let mut rows: u64 = 0;
        for line in std::io::BufRead::lines(std::io::BufReader::new(file)) {
            let Ok(line) = line else { continue };
            if line.trim().is_empty() {
                continue;
            }
            rows += 1;
            for (i, field) in line.split(delim).take(arity).enumerate() {
                let field = field.trim();
                if !columns[i].contains(field) {
                    columns[i].insert(field.to_string());
                }
                if let Ok(v) = field.parse::<i64>() {
                    bounds[i].0 = bounds[i].0.min(v);
                    bounds[i].1 = bounds[i].1.max(v);
                }
            }
        }

        feedback.insert(rel.name(), rows);
        let col_stats: Vec<ColumnStats> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let distinct = (col.len() as u64).max(1);
                // A numeric column carries a value range; `hi - lo + 1` is
                // then a second domain bound and the tighter one wins.
                let (lo, hi) = bounds[i];
                let range = (lo <= hi).then_some((lo, hi));
                let domain = match range {
                    Some((lo, hi)) => {
                        let span = (hi - lo).unsigned_abs().saturating_add(1);
                        distinct.min(span)
                    }
                    None => distinct,
                };
                debug!(target: "flowlog_build::optimizer",
                    "edb {} col{i}: rows={rows} domain={domain} range={range:?}",
                    rel.name());
                ColumnStats {
                    domain: Some(domain),
                    min: range.map(|(lo, _)| lo),
                    max: range.map(|(_, hi)| hi),
                }
            })
            .collect();
        feedback.set_columns(rel.name(), col_stats);
    }
}

/// Merge profile feedback across one or more `--profile-hint` directories.
/// A *missing* directory yields empty feedback; a directory that exists but
/// holds an unreadable or malformed file fails — that is a corrupt compiler
/// artifact, surfaced as an [`OptimizerError`] for the planner to report.
///
/// Each directory carries a `feedback_accum.json` accumulator: the loader
/// merges that run's raw `ops.json`/memory profile into the prior
/// accumulator and writes it back, so a rule's bad-plan cost — once
/// measured — is remembered across runs even after the optimizer stops
/// choosing that plan. Without it, a profile only reflects the last run
/// and a fix would be undone the moment a different plan ran.
fn load_profile_feedback(dirs: &[String]) -> Result<Feedback, OptimizerError> {
    let mut acc = Feedback::default();
    for dir in dirs {
        let dir_path = Path::new(dir);
        let accum_path = dir_path.join("feedback_accum.json");

        // Prior accumulator from earlier runs, plus this round's raw
        // profile max-merged on top.
        let mut dir_total = Feedback::load_accum(&accum_path)?;
        let fb = Feedback::load(dir_path)?;
        debug!(target: "flowlog_build::optimizer",
            "loaded {} profile hint entries from {}", fb.len(), dir);
        dir_total.merge(fb);

        // Persist the grown accumulator for the next round.
        dir_total.save_accum(&accum_path)?;

        acc.merge(dir_total);
    }
    Ok(acc)
}
