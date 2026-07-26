//! The operator layer: cross-worker metrics per operator address.
//!
//! In timely dataflow, an operator is the unit of computation: a vertex
//! of the dataflow graph, addressed by its path through nested scopes
//! ([`Addr`]), scheduled in short activations whenever input is ready.
//! The profiled runtime accumulates each operator's activation count and
//! active time into one table row, under the name timely gave it.
//!
//! ```text
//! operators_worker_t{t}_{i}.log
//!     addr  acts  active_ms  name
//! ```
//!
//! - [`operators`]: parse that table into one worker's rows
//! - [`aggregate`]: fold the workers into [`OperatorMetrics`] -- time as
//!   a [`Stats`] distribution, flow as a summed cardinality

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Add;
use std::str::FromStr;

use serde::Serialize;

use crate::Addr;
use crate::metrics::cardinality::Cardinality;

// =============================================================================
// Parse
// =============================================================================

/// One parsed operator-table row: time and name.
#[derive(Debug)]
pub(crate) struct Operator {
    pub(crate) activations: u64,
    pub(crate) total_active_ms: f64,
    pub(crate) op_name: String,
}

/// Parse one worker's operator table. Non-data lines (header, blanks, the
/// `(no operators recorded)` sentinel) and malformed rows are skipped; on a
/// duplicate address the first row wins.
pub(crate) fn operators(text: &str) -> BTreeMap<Addr, Operator> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        if let Some((addr, row)) = operator_row(line) {
            out.entry(addr).or_insert(row);
        }
    }
    out
}

/// Parse one operator row; `None` for anything that is not a complete row.
fn operator_row(line: &str) -> Option<(Addr, Operator)> {
    // The address may contain internal spaces (`[0, 15, 10]`) and so may
    // the trailing name (`Arrange: ThresholdTotal`), so cells split only
    // between `]` and the name.
    let line = line.trim();
    if !line.starts_with('[') {
        return None;
    }
    let close = line.find(']')?;
    let addr: Addr = line[..=close].parse().ok()?;

    let mut cells = line[close + 1..].split_whitespace();
    // Timing is present for every operator; treat a stray `n/a` as 0.
    let activations = cell::<u64>(cells.next()?)?.unwrap_or(0);
    let total_active_ms = cell::<f64>(cells.next()?)?.unwrap_or(0.0);
    let op_name = cells.collect::<Vec<_>>().join(" ");
    if op_name.is_empty() {
        return None;
    }

    Some((
        addr,
        Operator {
            activations,
            total_active_ms,
            op_name,
        },
    ))
}

/// One metric cell: `n/a` is `Some(None)`, a `T` value `Some(Some(_))`,
/// anything else `None` (the row is malformed).
fn cell<T: FromStr>(s: &str) -> Option<Option<T>> {
    if s == "n/a" {
        return Some(None);
    }
    s.parse().ok().map(Some)
}

// =============================================================================
// Cross-worker fold
// =============================================================================

/// One measured quantity's spread across workers.
#[derive(Debug, Clone, Serialize, Default)]
pub struct Stats {
    pub mean: f64,
    pub var: f64,
    pub min: f64,
    pub max: f64,
}

impl Stats {
    /// Compute stats from per-worker samples.
    pub(crate) fn from_values(values: &[f64]) -> Self {
        let n = values.len() as f64;
        if n == 0.0 {
            return Self::default();
        }
        let mean = values.iter().sum::<f64>() / n;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let var = if n > 1.0 {
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n
        } else {
            0.0
        };
        Self {
            mean,
            var,
            min,
            max,
        }
    }
}

/// Adding two Stats sums means and variances (valid for independent variables).
/// Min/max are also summed, providing conservative bounds.
impl Add for &Stats {
    type Output = Stats;
    fn add(self, rhs: &Stats) -> Stats {
        Stats {
            mean: self.mean + rhs.mean,
            var: self.var + rhs.var,
            min: self.min + rhs.min,
            max: self.max + rhs.max,
        }
    }
}

/// One timely operator's measured metrics, folded across workers.
#[derive(Debug, Clone)]
pub struct OperatorMetrics {
    pub addr: Addr,
    pub op_name: String,
    /// Per-worker time distribution.
    pub activations: Stats,
    pub active_ms: Stats,
    pub flow: Cardinality,
}

/// Fold per-worker rows into aggregated [`OperatorMetrics`] per address;
/// `ops[i]` and `flows[i]` are worker `i`'s parsed rows and resolved flow.
/// An operator missing from a worker samples as `0` time and no flow
/// there; on a name disagreement (only from truncation) the first worker
/// wins.
pub(crate) fn aggregate(
    ops: &[BTreeMap<Addr, Operator>],
    flows: &[BTreeMap<Addr, Cardinality>],
) -> BTreeMap<Addr, OperatorMetrics> {
    let all_addrs: BTreeSet<&Addr> = ops.iter().flat_map(|w| w.keys()).collect();

    let mut out = BTreeMap::new();
    for addr in all_addrs {
        let activations: Vec<f64> = ops
            .iter()
            .map(|w| w.get(addr).map_or(0.0, |r| r.activations as f64))
            .collect();
        let active_ms: Vec<f64> = ops
            .iter()
            .map(|w| w.get(addr).map_or(0.0, |r| r.total_active_ms))
            .collect();
        out.insert(
            addr.clone(),
            OperatorMetrics {
                addr: addr.clone(),
                op_name: ops
                    .iter()
                    .find_map(|w| w.get(addr))
                    .map(|r| r.op_name.clone())
                    .unwrap_or_default(),
                activations: Stats::from_values(&activations),
                active_ms: Stats::from_values(&active_ms),
                flow: Cardinality::sum(
                    flows
                        .iter()
                        .map(|w| w.get(addr).copied().unwrap_or_default()),
                ),
            },
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = "\
addr                 acts   active_ms   name
[0, 1]               3      1.500       InputSession
[0, 2]               4      2.250       ArrangeByKey
[0, 3]               5      10.000      Join
(no operators recorded)
";

    #[test]
    fn numeric_row_parses_all_cells() {
        let rows = operators(TABLE);
        let join = &rows[&Addr(vec![0, 3])];
        assert_eq!(join.activations, 5);
        assert_eq!(join.total_active_ms, 10.0);
        assert_eq!(join.op_name, "Join");
    }

    #[test]
    fn header_and_sentinel_lines_are_skipped() {
        assert_eq!(operators(TABLE).len(), 3);
    }

    /// A partial flush can cut a row anywhere; the fragment must not produce
    /// an entry or fail the surrounding file.
    #[test]
    fn truncated_row_is_skipped() {
        assert!(operators("[0, 2]               4").is_empty());
    }

    #[test]
    fn na_timing_cells_parse_as_zero() {
        let rows = operators("[0, 5]   n/a   n/a   Probe");
        assert_eq!(rows[&Addr(vec![0, 5])].activations, 0);
        assert_eq!(rows[&Addr(vec![0, 5])].total_active_ms, 0.0);
    }

    #[test]
    fn names_with_spaces_survive_cell_splitting() {
        let rows = operators("[0, 9]     2    0.100    Arrange: ThresholdTotal");
        assert_eq!(rows[&Addr(vec![0, 9])].op_name, "Arrange: ThresholdTotal");
    }

    #[test]
    fn empty_slice_yields_all_zero() {
        let s = Stats::from_values(&[]);
        assert_eq!((s.mean, s.var, s.min, s.max), (0.0, 0.0, 0.0, 0.0));
    }

    /// A single sample has no spread, so variance is 0 rather than NaN.
    #[test]
    fn single_value_has_zero_variance() {
        let s = Stats::from_values(&[4.0]);
        assert_eq!((s.mean, s.var, s.min, s.max), (4.0, 0.0, 4.0, 4.0));
    }

    #[test]
    fn multiple_values_reduce_to_mean_variance_min_max() {
        let s = Stats::from_values(&[1.0, 3.0]);
        assert_eq!((s.mean, s.var, s.min, s.max), (2.0, 1.0, 1.0, 3.0));
    }

    #[test]
    fn add_sums_each_field() {
        let a = Stats {
            mean: 1.0,
            var: 2.0,
            min: 3.0,
            max: 4.0,
        };
        let b = Stats {
            mean: 10.0,
            var: 20.0,
            min: 30.0,
            max: 40.0,
        };
        let s = &a + &b;
        assert_eq!((s.mean, s.var, s.min, s.max), (11.0, 22.0, 33.0, 44.0));
    }

    type Worker = (BTreeMap<Addr, Operator>, BTreeMap<Addr, Cardinality>);

    fn worker(
        name: &str,
        addr: Addr,
        acts: u64,
        ms: f64,
        tup_in: Option<i64>,
        tup_out: Option<i64>,
    ) -> Worker {
        (
            [(
                addr.clone(),
                Operator {
                    activations: acts,
                    total_active_ms: ms,
                    op_name: name.to_string(),
                },
            )]
            .into(),
            [(addr, Cardinality { tup_in, tup_out })].into(),
        )
    }

    /// Time is averaged into a distribution; flow is summed into a total.
    #[test]
    fn aggregate_distributes_time_and_sums_flow() {
        let (o0, f0) = worker("Join", Addr(vec![0, 3]), 5, 10.0, Some(100), Some(400));
        let (o1, f1) = worker("Join", Addr(vec![0, 3]), 5, 30.0, Some(300), Some(200));
        let join = &aggregate(&[o0, o1], &[f0, f1])[&Addr(vec![0, 3])];

        assert_eq!(join.active_ms.mean, 20.0);
        assert_eq!(join.flow.tup_in, Some(400));
        assert_eq!(join.flow.tup_out, Some(600));
    }

    #[test]
    fn aggregate_flow_absent_in_one_worker_counts_as_zero_but_keeps_the_field() {
        let (o0, f0) = worker("Join", Addr(vec![0, 3]), 5, 10.0, Some(100), Some(400));
        let (o1, f1) = worker("Join", Addr(vec![0, 3]), 5, 30.0, Some(300), None);
        assert_eq!(
            aggregate(&[o0, o1], &[f0, f1])[&Addr(vec![0, 3])]
                .flow
                .tup_out,
            Some(400)
        );
    }

    #[test]
    fn aggregate_flow_absent_in_every_worker_stays_absent() {
        let (o0, f0) = worker("Join", Addr(vec![0, 3]), 5, 10.0, None, None);
        let (o1, f1) = worker("Join", Addr(vec![0, 3]), 5, 30.0, None, None);
        let join = &aggregate(&[o0, o1], &[f0, f1])[&Addr(vec![0, 3])];

        assert!(join.flow.tup_in.is_none());
        assert!(join.flow.tup_out.is_none());
    }

    #[test]
    fn aggregate_operator_missing_from_a_worker_samples_as_zero() {
        let (o0, f0) = worker("Join", Addr(vec![0, 3]), 5, 10.0, None, None);
        let (mut o1, mut f1) = worker("Join", Addr(vec![0, 3]), 5, 30.0, None, None);
        let (extra_o, extra_f) = worker("Map", Addr(vec![0, 4]), 2, 4.0, None, None);
        o1.extend(extra_o);
        f1.extend(extra_f);
        assert_eq!(
            aggregate(&[o0, o1], &[f0, f1])[&Addr(vec![0, 4])]
                .activations
                .mean,
            1.0
        );
    }

    #[test]
    fn aggregate_first_workers_name_wins_on_disagreement() {
        let (o0, f0) = worker("Join", Addr(vec![0, 3]), 5, 10.0, None, None);
        let (o1, f1) = worker("Jo", Addr(vec![0, 3]), 5, 30.0, None, None);
        assert_eq!(
            aggregate(&[o0, o1], &[f0, f1])[&Addr(vec![0, 3])].op_name,
            "Join"
        );
    }
}
