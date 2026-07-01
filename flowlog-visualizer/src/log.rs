//! Parsing for the unified per-operator metrics log emitted by the profiler.
//!
//! The profiler writes one file per worker into `<stem>_log/metrics/`:
//! `metrics_worker_t{t}_{index}.log` (`t` = committed-transaction index, `0` for
//! batch mode; `index` = worker id). Each file is a fixed-width table:
//!
//! ```text
//! addr  acts  active_ms  tup_in  tup_out  arr_in  merges  merge_in  merge_out  dropped  bat_bytes  bat_cap  name
//! ```
//!
//! Cells that don't apply to an operator are `n/a` (timing is present for every
//! operator; arrangement columns only for arranged operators). We parse all
//! worker files for a transaction, then aggregate into mean + variance across
//! workers, exposing a [`TimeIndex`] (every operator) and a [`MemoryIndex`]
//! (arranged operators only) so the downstream views are unchanged from the
//! legacy split time/memory logs.
//!
//! The `tup_in`/`tup_out` (flow) and `bat_bytes`/`bat_cap` (batcher peak)
//! columns are new in the unified log; they are parsed past but not yet surfaced
//! in the report.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use flowlog_profiler::Addr;
use regex::Captures;
use regex::Regex;

use crate::Result;
use crate::stats::Stats;

// ---------------------------------------------------------------------------
// Aggregated row types (mean + variance across workers)
// ---------------------------------------------------------------------------

/// Aggregated time row across workers.
#[derive(Debug, Clone)]
pub struct TimeRow {
    pub activations: Stats,
    pub total_active_ms: Stats,
    /// Tuples flowed in/out (channel volume); `None` when the operator reports
    /// `n/a` (e.g. scope boundaries whose I/O lives on the parent-scope edges).
    pub tup_in: Option<Stats>,
    pub tup_out: Option<Stats>,
    pub op_name: String,
    pub num_workers: usize,
}

pub type TimeIndex = BTreeMap<Addr, TimeRow>;

/// Aggregated memory row across workers.
#[derive(Debug, Clone)]
pub struct MemoryRow {
    pub batched_in: Stats,
    pub merges: Stats,
    pub merge_in: Stats,
    pub merge_out: Stats,
    pub dropped: Stats,
    /// Batcher peak size / capacity, reported alongside the arrangement columns.
    pub bat_bytes: Stats,
    pub bat_cap: Stats,
    pub op_name: String,
    pub num_workers: usize,
}

pub type MemoryIndex = BTreeMap<Addr, MemoryRow>;

// ---------------------------------------------------------------------------
// Raw per-worker row types (internal): one parsed line of the unified table
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RawRow {
    /// Timing — present for every operator.
    activations: u64,
    total_active_ms: f64,
    /// Tuple flow in/out; `None` for operators that report `n/a`.
    tup_in: Option<i64>,
    tup_out: Option<i64>,
    /// Arrangement/memory columns — `Some` only for arranged operators.
    arrange: Option<RawArrange>,
    op_name: String,
}

#[derive(Debug, Clone)]
struct RawArrange {
    batched_in: u64,
    merges: u64,
    merge_in: u64,
    merge_out: u64,
    dropped: u64,
    bat_bytes: u64,
    bat_cap: u64,
}

type RawIndex = BTreeMap<Addr, RawRow>;

// ---------------------------------------------------------------------------
// Public API: folder-based parsing (returns snapshots grouped by transaction)
// ---------------------------------------------------------------------------

/// One labeled snapshot: aggregated time + memory for a single transaction.
pub struct MetricsSnapshot {
    pub label: String,
    pub time: TimeIndex,
    pub memory: MemoryIndex,
}

/// Parse every `metrics_worker_*.log` in `dir`, grouped by the `_t{N}_`
/// transaction index in each filename (one snapshot per transaction; a single
/// `t0` snapshot for batch mode).
pub fn parse_metrics_folder(dir: &str) -> Result<Vec<MetricsSnapshot>> {
    let files = collect_log_files(dir)?;
    if files.is_empty() {
        bail!(format!("no .log files found in metrics folder {}", dir));
    }

    let mut snapshots = Vec::new();
    for (label, group_files) in group_by_timestamp(&files) {
        let mut workers = Vec::with_capacity(group_files.len());
        for f in &group_files {
            workers.push(parse_raw_metrics_file(f)?);
        }
        let (time, memory) = aggregate(&workers, &group_files)?;
        snapshots.push(MetricsSnapshot {
            label,
            time,
            memory,
        });
    }
    Ok(snapshots)
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Validate that op_name is consistent across workers for a given addr.
fn validate_op_name(
    op_name: &mut Option<String>,
    candidate: &str,
    addr: &Addr,
    first_file: &str,
    current_file: &str,
) -> Result<()> {
    if let Some(existing) = op_name.as_ref() {
        if existing != candidate {
            bail!(format!(
                "op_name mismatch for addr {:?} between {} ({:?}) and {} ({:?})",
                addr.0, first_file, existing, current_file, candidate
            ));
        }
    } else {
        *op_name = Some(candidate.to_string());
    }
    Ok(())
}

/// Fold per-worker raw rows into aggregated time + memory indexes. An operator
/// missing from a worker contributes `0` to that worker's sample (matching the
/// legacy split-log behavior); the memory index only includes operators that are
/// arranged in at least one worker.
fn aggregate(workers: &[RawIndex], files: &[String]) -> Result<(TimeIndex, MemoryIndex)> {
    let n = workers.len();
    let all_addrs: BTreeSet<&Addr> = workers.iter().flat_map(|w| w.keys()).collect();

    let mut time = TimeIndex::new();
    let mut memory = MemoryIndex::new();

    for addr in all_addrs {
        let mut activations = Vec::with_capacity(n);
        let mut ms = Vec::with_capacity(n);
        let mut op_name: Option<String> = None;

        // Flow accumulators; `tup_in`/`tup_out` are reported independently and
        // either may be `n/a`, so each tracks whether any worker had a value.
        let mut has_tup_in = false;
        let mut has_tup_out = false;
        let mut tup_in = Vec::with_capacity(n);
        let mut tup_out = Vec::with_capacity(n);

        // Arrangement accumulators; an operator is "arranged" if any worker
        // reported arrangement columns (i.e. not `n/a`) for it.
        let mut arranged = false;
        let mut batched_in = Vec::with_capacity(n);
        let mut merges = Vec::with_capacity(n);
        let mut merge_in = Vec::with_capacity(n);
        let mut merge_out = Vec::with_capacity(n);
        let mut dropped = Vec::with_capacity(n);
        let mut bat_bytes = Vec::with_capacity(n);
        let mut bat_cap = Vec::with_capacity(n);

        for (wi, w) in workers.iter().enumerate() {
            match w.get(addr) {
                Some(row) => {
                    validate_op_name(&mut op_name, &row.op_name, addr, &files[0], &files[wi])?;
                    activations.push(row.activations as f64);
                    ms.push(row.total_active_ms);
                    match row.tup_in {
                        Some(v) => {
                            has_tup_in = true;
                            tup_in.push(v as f64);
                        }
                        None => tup_in.push(0.0),
                    }
                    match row.tup_out {
                        Some(v) => {
                            has_tup_out = true;
                            tup_out.push(v as f64);
                        }
                        None => tup_out.push(0.0),
                    }
                    match &row.arrange {
                        Some(a) => {
                            arranged = true;
                            batched_in.push(a.batched_in as f64);
                            merges.push(a.merges as f64);
                            merge_in.push(a.merge_in as f64);
                            merge_out.push(a.merge_out as f64);
                            dropped.push(a.dropped as f64);
                            bat_bytes.push(a.bat_bytes as f64);
                            bat_cap.push(a.bat_cap as f64);
                        }
                        None => {
                            batched_in.push(0.0);
                            merges.push(0.0);
                            merge_in.push(0.0);
                            merge_out.push(0.0);
                            dropped.push(0.0);
                            bat_bytes.push(0.0);
                            bat_cap.push(0.0);
                        }
                    }
                }
                None => {
                    activations.push(0.0);
                    ms.push(0.0);
                    tup_in.push(0.0);
                    tup_out.push(0.0);
                    batched_in.push(0.0);
                    merges.push(0.0);
                    merge_in.push(0.0);
                    merge_out.push(0.0);
                    dropped.push(0.0);
                    bat_bytes.push(0.0);
                    bat_cap.push(0.0);
                }
            }
        }

        let op_name = op_name.unwrap_or_default();

        time.insert(
            addr.clone(),
            TimeRow {
                activations: Stats::from_values(&activations),
                total_active_ms: Stats::from_values(&ms),
                tup_in: has_tup_in.then(|| Stats::from_values(&tup_in)),
                tup_out: has_tup_out.then(|| Stats::from_values(&tup_out)),
                op_name: op_name.clone(),
                num_workers: n,
            },
        );

        if arranged {
            memory.insert(
                addr.clone(),
                MemoryRow {
                    batched_in: Stats::from_values(&batched_in),
                    merges: Stats::from_values(&merges),
                    merge_in: Stats::from_values(&merge_in),
                    merge_out: Stats::from_values(&merge_out),
                    dropped: Stats::from_values(&dropped),
                    bat_bytes: Stats::from_values(&bat_bytes),
                    bat_cap: Stats::from_values(&bat_cap),
                    op_name,
                    num_workers: n,
                },
            );
        }
    }

    Ok((time, memory))
}

// ---------------------------------------------------------------------------
// File collection and transaction grouping
// ---------------------------------------------------------------------------

/// Regex to detect the transaction index `_t{N}_` in a filename.
static TIMESTAMP_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"_t(\d+)_").unwrap());

fn collect_log_files(dir: &str) -> Result<Vec<String>> {
    let path = Path::new(dir);
    if !path.is_dir() {
        bail!(format!("{} is not a directory", dir));
    }

    let mut files: Vec<String> = Vec::new();
    for entry in fs::read_dir(path).with_context(|| format!("read directory {}", dir))? {
        let entry = entry?;
        let p = entry.path();
        if p.is_file()
            && let Some(ext) = p.extension()
            && ext == "log"
        {
            files.push(p.to_string_lossy().to_string());
        }
    }
    files.sort();
    Ok(files)
}

/// Group files by transaction index `_t{N}_` in the filename. Returns sorted
/// `(label, files)` pairs.
fn group_by_timestamp(files: &[String]) -> Vec<(String, Vec<String>)> {
    let re = &*TIMESTAMP_RE;
    let mut groups: BTreeMap<u64, Vec<String>> = BTreeMap::new();

    for f in files {
        let filename = Path::new(f)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let ts = re
            .captures(&filename)
            .and_then(|c| c[1].parse::<u64>().ok())
            .unwrap_or(0);
        groups.entry(ts).or_default().push(f.clone());
    }

    groups
        .into_iter()
        .map(|(ts, files)| (format!("t{}", ts), files))
        .collect()
}

// ---------------------------------------------------------------------------
// Unified-table parsing (one file per worker)
// ---------------------------------------------------------------------------

/// Matches one data row of the unified metrics table. The address may contain
/// internal spaces (`[0, 15, 10]`) and the trailing name may too
/// (`Arrange: ThresholdTotal`), so both are captured explicitly; every metric
/// column is captured (cells may be `n/a`).
static METRICS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        ^\s*(\[[^\]]*\])                              # 1: addr
        \s+(\S+)\s+(\S+)                              # 2: acts      3: active_ms
        \s+(\S+)\s+(\S+)                              # 4: tup_in    5: tup_out
        \s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)      # 6: arr_in 7: merges 8: merge_in 9: merge_out 10: dropped
        \s+(\S+)\s+(\S+)                              # 11: bat_bytes 12: bat_cap
        \s+(.*?)\s*$                                  # 13: name
        ",
    )
    .unwrap()
});

fn parse_raw_metrics_file(path: &str) -> Result<RawIndex> {
    let text =
        fs::read_to_string(path).with_context(|| format!("read metrics log file {}", path))?;

    let re = &*METRICS_RE;
    let mut out = RawIndex::new();

    for (lineno, line) in text.lines().enumerate() {
        let lno = lineno + 1;
        let line = line.trim_end();

        // Skip blanks, the header row, and the `(no operators recorded)`
        // sentinel — only bracketed-addr data rows are parsed.
        if !line.trim_start().starts_with('[') {
            continue;
        }

        let caps = re.captures(line).ok_or_else(|| {
            anyhow!(format!(
                "metrics log parse error at {}:{}: cannot parse line: {:?}",
                path, lno, line
            ))
        })?;

        let addr_str = caps.get(1).unwrap().as_str();
        let addr: Addr = addr_str
            .parse()
            .map_err(|e| anyhow!(format!("bad addr at {}:{}: {}", path, lno, e)))?;

        // Timing is present for every operator; treat a stray `n/a` as 0.
        let activations =
            parse_cell::<u64>(caps.get(2).unwrap().as_str(), path, lno, "acts")?.unwrap_or(0);
        let total_active_ms =
            parse_cell::<f64>(caps.get(3).unwrap().as_str(), path, lno, "active_ms")?
                .unwrap_or(0.0);

        let tup_in = parse_cell::<i64>(caps.get(4).unwrap().as_str(), path, lno, "tup_in")?;
        let tup_out = parse_cell::<i64>(caps.get(5).unwrap().as_str(), path, lno, "tup_out")?;

        let arrange = parse_arrange(&caps, path, lno)?;
        let op_name = caps.get(13).unwrap().as_str().to_string();

        let row = RawRow {
            activations,
            total_active_ms,
            tup_in,
            tup_out,
            arrange,
            op_name,
        };

        if out.insert(addr.clone(), row).is_some() {
            bail!(format!(
                "duplicate addr entry in metrics log at {}:{}: {}",
                path, lno, addr_str
            ));
        }
    }

    Ok(out)
}

/// Parse the seven arrangement columns. They are reported together: either all
/// numeric (an arranged operator) or all `n/a` (not arranged) — so the first
/// column (`arr_in`) decides whether the operator carries memory data.
fn parse_arrange(caps: &Captures, path: &str, lno: usize) -> Result<Option<RawArrange>> {
    let Some(batched_in) = parse_cell::<u64>(caps.get(6).unwrap().as_str(), path, lno, "arr_in")?
    else {
        return Ok(None);
    };
    Ok(Some(RawArrange {
        batched_in,
        merges: parse_cell::<u64>(caps.get(7).unwrap().as_str(), path, lno, "merges")?.unwrap_or(0),
        merge_in: parse_cell::<u64>(caps.get(8).unwrap().as_str(), path, lno, "merge_in")?
            .unwrap_or(0),
        merge_out: parse_cell::<u64>(caps.get(9).unwrap().as_str(), path, lno, "merge_out")?
            .unwrap_or(0),
        dropped: parse_cell::<u64>(caps.get(10).unwrap().as_str(), path, lno, "dropped")?
            .unwrap_or(0),
        bat_bytes: parse_cell::<u64>(caps.get(11).unwrap().as_str(), path, lno, "bat_bytes")?
            .unwrap_or(0),
        bat_cap: parse_cell::<u64>(caps.get(12).unwrap().as_str(), path, lno, "bat_cap")?
            .unwrap_or(0),
    }))
}

/// Parse a metric cell that is either `n/a` (→ `None`) or a value of type `T`
/// (e.g. `u64` counts, `i64` flow that can go negative, `f64` times).
fn parse_cell<T>(s: &str, path: &str, lno: usize, field: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    if s == "n/a" {
        return Ok(None);
    }
    Ok(Some(s.parse::<T>().with_context(|| {
        format!(
            "metrics parse error at {}:{}: bad {} {:?}",
            path, lno, field, s
        )
    })?))
}
