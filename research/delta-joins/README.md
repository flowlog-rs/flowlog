# Delta joins in FlowLog: a study

This directory studies delta joins and half joins, as Frank McSherry built them for differential
dataflow (DD) and as Materialize uses them, and asks where they would make FlowLog faster or
leaner. It gathers evidence for a future compiler feature; it is not that feature. Each baseline
is the Rust that FlowLog generates for a program, and every other plan is that code rewritten by a
small patch. All of them run on FlowLog's benchmark data.

- [REPORT.md](REPORT.md) is the write-up. Part 1, about fifteen minutes, explains how delta joins
  work, what was measured, and a rule for when to use one. Part 2 is the lab notebook behind
  every number.
- This README covers how the work is split into pull requests, and how to rebuild and rerun any
  of it.

## Findings in brief

- **What a delta join saves.** A binary plan for a rule with three or more atoms joins two atoms at
  a time, and stores each intermediate result, the middle step, as an index for the next join. A
  delta join stores no middle step. For each atom that can change, it runs one path that looks the
  other atoms up in their indexes.
- **Where it wins:** where a large middle step really is stored and the lookups stay cheap.
  - Non-recursive rules in batch mode: triangles on G40K run 50% faster in 77% less memory.
  - Recursive rules whose middle step is joined next with another recursive atom: CSPA rule 6
    uses 15% less memory, and a Doop points-to rule runs 9% faster in 30% less memory.
  - Rules with three or more atoms in incremental mode: CSPA uses 12% less memory and commits
    12% faster.
- **Where it loses:**
  - where nothing was stored: FlowLog's batch mode already frees some middle steps (SG, Dyck);
  - where it needs large new indexes: Andersen's store rule uses 22% more memory;
  - where a lookup fans out: GALEN rule 4 with the wrong lookup order runs 7.2 times slower.
- **Choosing well is most of the gain.** Over 20 cases, using one only when it stores fewer
  records and its paths are cheap, with a heavy/light router when every lookup order is costly,
  saves 10% time and 15% memory (geometric means): within about a point of the best choices in
  hindsight. Materialize's rule (three or more atoms, no new index) saves 7% and 11%, and makes 4
  cases worse.
- **The rule held up on new programs.** On 9 more cases, 7 of them from 6 more programs, with each
  policy's picks saved before the runs, it saved 5% time and 3% memory, against 6% and 4% for the
  best choices.
  - Its biggest new win, Doop's AssignCast rule, came from a lookup order that reads a shared
    index, so a 37M-record index goes.
  - Its one miss, Doop's InstanceFieldPointsTo rule, saved 0.5% of the program's records and ran
    10% slower.
  - A margin set after these runs, a saving of at least 5% of the records the program stores,
    skips that miss and Dyck's incremental one. It still needs a test of its own.
- **The lookup must be fast.** Seeking each key once instead of once per row took CSPA's delta join
  from 11% more CPU than the binary plan to 5% less. With that, TC's lookup runs 10-13% faster than
  DD's join at 1 and 8 workers.
- **A review of the lookup operator** (REPORT section 12) found its answers correct and, with
  integer keys, faster than the half joins Materialize uses. It also caught a slowdown this study
  had added: merging duplicate rows rebuilt each string key in every comparison of a sort.
  Building each key once made Doop's delta plans 4% faster. One interface change is left for the
  compiler: borrow each row's key instead of building a new one ([#534][pr-b]).
- **FlowLog need not keep an operator of its own** (REPORT section 12.7). dogs3's `half_join2`
  computes the same answers once a `Monoid` bound it never uses is relaxed, a three-line change,
  and a thin adapter over it passes the lookup's tests ([#535][pr-c]). It is faster with string
  keys and slower with integer keys, even with a second patch that groups rows by key. Differential
  0.26 has since shipped the bound, so the adapter needs no patch, but it has no grouping: with
  integer keys the adapter is now 53% slower than FlowLog's operator (below). FlowLog's own
  operator is still the better choice.

REPORT.md's [What I would do next in FlowLog](REPORT.md#what-i-would-do-next-in-flowlog) and
[section 13](REPORT.md#13-ideas-for-bringing-this-into-flowlog-easiest-first) turn these into an
order of work, and [section 12](REPORT.md#12-reviewing-the-lookup-operator) reviews the lookup
operator they would build on.

## How the work is split

Four draft pull requests, stacked:

| Pull request | Branch | Builds on | Adds |
|---|---|---|---|
| [#533][pr-a] | `research/lookup-operator` | `main` | The lookup operator, as this study measured it. |
| [#532][pr-study] | `research/delta-joins` | `research/lookup-operator` | This directory: the study. |
| [#534][pr-b] | `research/lookup-borrowed-keys` | `research/delta-joins` | Lookups that borrow each row's key (REPORT section 12.3). |
| [#535][pr-c] | `research/lookup-on-dogs3` | `research/delta-joins` | Lookups run on differential-dogs3 0.26's `half_join`, with no patch (section 12.7). |

[pr-a]: https://github.com/flowlog-rs/flowlog/pull/533
[pr-study]: https://github.com/flowlog-rs/flowlog/pull/532
[pr-b]: https://github.com/flowlog-rs/flowlog/pull/534
[pr-c]: https://github.com/flowlog-rs/flowlog/pull/535

[#533][pr-a] is four commits on FlowLog `main` (6cb118c):

1. **`feat(runtime): lookup operators for delta join paths`**: the team's `feat/lookup-operator`
   commit (c24f89e, [#450](https://github.com/flowlog-rs/flowlog/pull/450)), as is. It adds
   `flowlog_lookup`, `flowlog_lookup_exists`, `flowlog_path`, `flowlog_land` and
   `Version::{New, Old}` to `flowlog-runtime`.
2. **`perf(runtime): pace lookups, read each key once, merge duplicate rows`**: the operator as
   this study measured it. It yields when it runs out of fuel, seeks each key once instead of once
   per row, merges duplicate rows before they probe, and builds each string key once per row
   instead of in every comparison of a sort (REPORT sections 4 and 12).
3. **`feat(runtime): count the rows lookups hold`**: the gauge behind REPORT section 10.5.
4. **`refactor(runtime): port lookups to timely 0.32 and differential 0.26`**: the versions `main`
   moved to after the study, which ran on timely 0.31 and differential 0.25.1.

The runtime's tests cover the operator (`cargo test -p flowlog-runtime`). Its code was reorganized
after the study's measurements, with the same behavior: `results/lookup_bench_refactor.log` reruns
section 12.2's bench on both versions, and they agree within noise, with medians within 1.5% in
every scenario and the same peak memory.

The fourth commit, and the bench's port to the same versions, came after the study's measurements
too. `results/lookup_bench_port.log` runs the bench on both versions side by side, 5 reps, and
every run reaches the study's checksums. On the new versions FlowLog's lookup answers as fast at 1
worker and 9% to 17% faster at 8, with the same peak memory or less, and the binary join is within
4%. dogs3's half join (`half_join2` in 0.25, `half_join` in 0.26) answers 4% to 24% slower on `int`
and `str`, and peaks 45 to 93 MiB higher: 0.26 copies the updates it releases twice, in
`BlobList::extract` and `CursorTactic::prep`, where 0.25 sorted them in place.

[#534][pr-b] and [#535][pr-c] moved to the same versions. `results/lookup_bench_port_branches.log`
runs their benches beside this checkout's, 5 reps, and every run reaches the study's checksums.
Medians:

| Lookup | `int`, 1 / 8 workers | `str`, 1 / 8 workers | `inc`, 1 / 8 workers | Peak at 1 worker, `int` / `str` |
|---|---|---|---|---|
| FlowLog's operator, [#533][pr-a] | 0.74 / 0.16 s | 1.15 / 0.17 s | 2.74 / 0.31 s | 356 / 307 MiB |
| Borrowed keys, [#534][pr-b] | 0.74 / 0.15 s | 0.95 / 0.14 s | 2.73 / 0.31 s | 356 / 307 MiB |
| On dogs3, [#535][pr-c] | 1.14 / 0.24 s | 1.05 / 0.17 s | 2.66 / 0.34 s | 450 / 346 MiB |
| dogs3's `half_join`, called directly | 1.08 / 0.20 s | 0.98 / 0.16 s | 2.60 / 0.33 s | 448 / 337 MiB |

- Borrowed keys answer 17% faster with string keys, at 1 and 8 workers, and within 5% otherwise.
- The adapter needs no patch on 0.26. With integer keys it is 53% slower at 1 worker and 50% at
  8, and peaks 77 to 94 MiB higher: 0.26 has no grouping, and copies the rows it releases. With
  string keys it is 9% faster than [#533][pr-a] at 1 worker, but 10% slower than [#534][pr-b].

[#532][pr-study] adds this directory, and an entry in `_typos.toml` that skips its result logs.
[#534][pr-b] and [#535][pr-c] are experiments on top of it: each changes the lookup in one way
that section 12 measured, for the team to adopt or drop.

The compiler is unchanged in all four, so FlowLog plans and runs every program as on `main`. Only
the patched builds here use the lookup operators.

## Layout

| Path | Contents |
|---|---|
| `REPORT.md` | The write-up. |
| `builds.txt` | Every binary the study measured: the program it compiles, or the build it starts from and the patches and settings that change it. |
| `programs/` | Datalog programs that are not in `example/`: triangles (`tri.dl`), and incremental versions, with inputs declared `append`, of triangles and of the examples. |
| `patches/` | One rewrite of a generated `main.rs` per experiment: a Python script, or a unified diff (`tc_lookup.diff`). `_edit.py` holds the edits the scripts share; `probe.py` and `count.py` add the memory probe and record counters. |
| `scripts/` | Tools to fetch data, build, run and analyze; each runnable script answers `-h`. `paths.py` and `galen_data.py` are modules the others import. |
| `bench/` | A crate of its own that times one lookup step: FlowLog's lookup against differential-dogs3's half join and a binary join (REPORT section 12). |
| `results/` | The study's own measurements, one or more files per experiment. New runs write to `$RESULTS` instead. |

## Requirements

- Linux. The study ran on a shared 64-core x86-64 machine (AMD EPYC 7763) with 503 GB of RAM.
- Rust 1.85 or later (FlowLog uses edition 2024), Python 3.10 or later, bash 4 or later, GNU time
  at `/usr/bin/time`, and `curl`, `unzip`, `tar` and `patch`.
- Disk: the datasets take about 1.4 GB to download and 6.4 GB unpacked. Builds, the compiler
  included, take about 0.6 GB for the quick start's TC runs, 0.85 GB with its incremental run, and
  about 13 GB for all 105, 4 GB of it for the nine Doop builds.
- Memory: most runs need a few GB. CSPA on httpd peaks at about 9 GB in batch mode and 12 GB in
  incremental mode, Polonius on `borrow` at about 13 GB, and Doop on `batik` at about 37 GB.

## Quick start

TC on G5K in batch mode: DD's binary join against a lookup into Arc's index (REPORT section 4).
With an empty build directory it took 74 seconds on the study's machine, most of it compiling.

```sh
cd research/delta-joins
source scripts/env.sh                 # sets ROOT, REPO, WORK, BIN, DATA and RESULTS
scripts/fetch_data.sh G5K-0.001       # into $DATA/G5K-0.001
scripts/build.py tc_base tc_lookup tc_lookup_p
scripts/bench.py --label tc_base --bin "$BIN/tc_base" --facts "$DATA/G5K-0.001" --expect Tc=24730729
scripts/bench.py --label tc_lookup --bin "$BIN/tc_lookup" --facts "$DATA/G5K-0.001" --expect Tc=24730729
FLOWLOG_PROBE=1 "$BIN/tc_lookup_p" -w 8 -F "$DATA/G5K-0.001" -D - | grep '^\[probe\] peak'
```

`bench.py` runs a binary 3 times at 8 workers, checks the size of its output, prints each run and
the medians, and appends the runs to `$RESULTS/runs.csv`. On the study's machine the medians were
about 4.3 s and 1,130 MB for `tc_base`, and 3.8 s and 1,110 MB for `tc_lookup`; on timely 0.32 and
differential 0.26, about 3.6 s and 3.3 s, with the same memory. The probe build prints the peak
records held in arrangements, in batchers and inside lookups; on this input it reports up to
`peak_lookup_rows_held=9824500`. That gauge adds up the rows all workers hold at the same moment,
so it depends on their timing: five runs on the study's versions all reported 9824500, and six runs
on the new ones reported 9.2M to 9.8M.

An incremental run loads a split (all but every 1000th row), commits the rest as one transaction,
and prints one line with the commit time, peak memory and output sizes:

```sh
scripts/fetch_data.sh cspa-split
scripts/build.py cspa_inc cspa_inc_r45
scripts/inc_run.sh "$BIN/cspa_inc_r45" 8 cspa_inc_r45 "$DATA/cspa-split" \
  assign=Assign.csv dereference=Dereference.csv
```

With rules 4 and 6 as delta joins (build names call rule 6 `r5`; see
[Reading the results](#reading-the-results)), the commit takes about 1.5 s on the study's machine,
and the run peaks at about 11,000 MB. REPORT section 8 has the medians of three runs: 1.54 s and
10,854 MB, against 1.76 s and 12,280 MB for `cspa_inc`. On timely 0.32 and differential 0.26 the
commits take about 1.3 s and 1.5 s, and the peaks stay within 3% of these.

## How a build is made

`scripts/build.py NAME...` builds each named row of `builds.txt`, and first any build it starts
from that is not built yet:

- A **program row**, `name | file.dl`, compiles the program with this checkout's compiler, against
  this checkout's runtime.
- A **variant row**, `name | base | settings | patches`, copies the base build's generated crate,
  applies the patches in order to its `src/main.rs`, and builds it. A `.py` patch runs with the
  settings as environment variables; a `.diff` goes through `patch`. Suffixes name the common
  variants: `_p` adds the memory probe, and `_pc` also counts records.

Crates go to `$WORK/crates/NAME` and binaries to `$BIN/NAME`. `scripts/build.py --list` lists every
build.

Patches find the code they rewrite by the names FlowLog generates: `t_<hash>`, which hashes the
plan, and `rel_<n>_<relation>`. They match the compiler on this branch. After the planner changes,
a patch whose code moved stops with a message such as
`expected 'let t_... = ' once in the generated code, found it 0 times` instead of rewriting the
wrong place; update the names in the patch from the newly generated crate.

## Adding an experiment

1. Choose the program. Put a new one in `programs/` and add a program row for it.
2. Build it, and read the generated `$WORK/crates/NAME/src/main.rs`: every map, arrangement and
   join is one `let` statement, so a patch replaces statements.
3. Write `patches/NEW.py`. It reads the file named by its argument, rewrites it with the helpers in
   `_edit.py` (`find`, `replace_span`, `insert_before`, `insert_after`, `statement_end`,
   `remove_statement`) and writes it back. Settings arrive as environment variables.
   `tri_lookup.py` is the smallest example, a lookup chain in batch mode; `tri_inc_delta.py` is a
   full delta join, one path per atom with `Version::New` and `Version::Old`.
4. Add a variant row, and a `_p` row that adds `probe.py` to measure memory.
5. Check that the output matches the base build's: `bench.py --expect` in batch mode, or the
   sizes per epoch that `inc_run.sh` prints.

The operators are documented in `flowlog-runtime/src/operators/lookup.rs`.

## Settings

### Paths

`scripts/env.sh` sets these for the shell scripts and `scripts/paths.py` for the Python ones. An
environment variable of the same name overrides each.

| Variable | Default | Holds |
|---|---|---|
| `WORK` | `<repo>/target/delta-joins` | generated crates and cargo output |
| `BIN` | `$WORK/bin` | built binaries |
| `DATA` | `$WORK/data` | datasets and splits |
| `RESULTS` | `$WORK/results` | logs of new runs; `results/` keeps the study's own |

### Runtime switches

The binaries read these when they start.

| Variable | Default | Effect |
|---|---|---|
| `FLOWLOG_LOOKUP_FUEL` | `1000000` | Rows probed plus records emitted before a lookup yields; `0` for no limit. |
| `FLOWLOG_LOOKUP_GROUP` | on | `0` seeks once per row instead of once per key. |
| `FLOWLOG_LOOKUP_CONSOLIDATE` | on | `0` lets duplicate rows probe separately instead of merging them first. |
| `FLOWLOG_PROBE` | off | `1` makes probe builds (`_p`, `_pc`) print, at exit, the records stored in arrangements and the rows held inside lookups. |
| `PROBE_TOP` | 16 | How many of the largest arrangements a probe build lists. |
| `FLOWLOG_SPLIT_T` | `64` | The heavy/light threshold of the `galen_d4s` builds: rows whose key has a larger degree take the heavy path. |

On [#535][pr-c]'s branch only `FLOWLOG_LOOKUP_FUEL` applies, and it counts records emitted only.

### Patch settings

The third column of `builds.txt`.

| Patch | Setting | Effect |
|---|---|---|
| `galen_delta.py` | `RULES` | Rules to convert: any of `3` and `6`, and at most one of `4`, `4s` (with a router) and `4p` (the other lookup order). |
| `cspa_r5_delta.py` | `SYM=1` | Read MemoryAlias's existing index through its symmetry instead of building a new one. |
| `tri_inc_delta.py` | `ORDER=oldfirst` | Path 2 looks up its `Old` atom first. |
| `tri_inc_delta.py` | `FILTER=snapshot` | Paths 1 and 2, which each read an `Old` atom, drop their rows from the load. |
| `sg_delta.py`, `dyck_delta.py` | `MODE=inc` | The incremental delta join instead of the batch lookup chain. |
| `dyck_delta.py` | `FILTER=snapshot`, `FILTER=loop` | `snapshot` drops path 1's rows from the load; `loop` also drops path 3's (REPORT section 10.4). |
| `andersen_delta.py` | `RULES` | Rules to convert: `load`, `store`. |
| `polonius_lookup.py` | `RULES` | Rules to convert: `me` (the `move_error` rule), `pm` (the recursive `path_maybe_uninitialized_on_exit` rule), `sa` and `sb` (two recursive `subset` rules). |
| `loop_lookup.py` | `PROGRAM` | The program whose recursive 2-atom join becomes a lookup: `reach`, `cc`, `sssp`, `csda` or `bipartite`. |
| `doop_delta.py` | `RULES` | Rules to convert: `ifpt` (the `InstanceFieldPointsTo` rule), and at most one of `ac` (the `AssignCast` rule, hops in the rule's order) and `acv` (the same rule, cheaper hop order). |
| `count.py` | `COUNT=t_X=label;...` | Print how many records statement `t_X` produces, under `label`. |

## Rerunning a section

| REPORT section | Builds | How | Results |
|---|---|---|---|
| 2: half joins FlowLog already gets in batch mode | `tc_base_p`, `tc_inc_p`, `cspa_inc_p` | run with `FLOWLOG_PROBE=1` | `probe_tc_batch.txt`, `probe_tc_inc.txt`, `probe_cspa_inc.txt` |
| 4: a fast lookup | `tc_base`, `tc_lookup`, `cspa_base`, `cspa_delta_sym` | `bench.py`, with `--workers 1,8,32` for TC and `--env FLOWLOG_LOOKUP_GROUP=0` for per-row lookups | `group_recheck.log`, `group_recheck.csv`, `tc_group.log`, `cspa_group.log` |
| 5.1: CSPA rule 6 | `cspa_base`, `cspa_delta`, `cspa_delta_sym` and their `_p` builds | `bench.py` on `cspa-httpd`; `_p` builds with `FLOWLOG_PROBE=1` | `cspa_matrix.log`, `cspa_rss_recheck.log`, `probe_cspa_*_p.txt`, `count_cspa_*.txt` |
| 5.2 and 6: GALEN rules 3, 4 and 6 | `galen_base`, `galen_d3`, `galen_d4`, `galen_d6` and their `_p` builds | `bench.py`, or `guarded.py` for plans that may blow up; `galen_fanout.py` and `galen_wco.py` count the paths' work | `galen_matrix.log`, `galen_blowup.log`, `galen_wco.txt`, `probe_galen_*.txt` |
| 7: a heavy/light router | `galen_d4s`, `galen_d4p`, `galen_d4s6` | `bench.py`; `guarded.py` with `FLOWLOG_SPLIT_T` from 0 to 4096; `galen_split.py` | `galen_split.txt`, `galen_split_matrix.log`, `galen_split_sweep.log` |
| 8: incremental mode | `cspa_inc`, `cspa_inc_r4`, `cspa_inc_r5`, `cspa_inc_r45` and their `_p` builds | `inc_run.sh` on `cspa-split`, as in the quick start; `rss_timeline.py` | `cspa_inc.log`, `cspa_inc_matrix.log`, `probe_cspa_inc_*_split.txt`, `rss_timeline_*.tsv` |
| 10.1-10.3: seven policies on more workloads | listed in the script | `heur_bench.sh`, then `heuristics.py --results "$RESULTS"` (below) | `heur_runs.csv`, `heur_inc.log`, `heur_bench.log`, `heuristics_out.md`, the other `probe_*.txt` |
| 10.4: at a load, only one path needs to run | listed in each script | `oldfirst_bench.sh`, `snapfilter_bench.sh` | `oldfirst_inc.log`, `snapfilter_inc.log`, `snapfilter_probe.log`, `snapfilter_probe_rerun.log` |
| 10.5: rows held inside lookups | listed in the script | `lookup_rows_probe.sh` | `lookup_rows_probe.log` |
| 11: the held-out test | listed in the script; the planner inputs come from `doop_*_pc` and `polonius_*_pc` | `heldout_bench.sh`, then `heuristics.py --results "$RESULTS"`; run the `_pc` builds with `FLOWLOG_PROBE=1 PROBE_TOP=60` | `heldout_runs.csv`, `heldout_bench.log`, `heldout_picks.md`, `probe_doop_*_batik.txt`, `probe_polonius_*_borrow.txt` |
| 12: reviewing the lookup | `bench/`, which the script builds; `tc_lookup`; the Doop builds; for borrowed keys, the bench on [#534][pr-b]'s branch (below) | `lookup_bench.sh`; for grouping, `$WORK/bench/release/lookup-bench ENGINE SCENARIO 1`, with `FLOWLOG_LOOKUP_GROUP=0` for per-row lookups; `bench.py` on `tc_lookup` with `--workers 1,8,32`, with and without `--env FLOWLOG_LOOKUP_CONSOLIDATE=0`; `heldout_bench.sh doop` | `lookup_bench.log`, `lookup_bench_refactor.log`, `lookup_bench_port.log`, `lookup_bench_port_branches.log`, `lookup_bench_borrowed.log`, `lookup_group0.log`, `cons_recheck.csv`, `cons_recheck.log`, `heldout_doop_recheck.log`, `heldout_doop_before_fix.csv` |
| 12.7: the lookup on dogs3 | the bench on [#535][pr-c]'s branch (below) | `lookup_bench.sh`, with `LABEL=BINARY` for the adapter and `LABEL=BINARY:hj2` for dogs3's half join itself | `lookup_bench.log`, from line 207; `lookup_bench_dogs3.log`; `lookup_bench_port_branches.log` |

Sections 12.3 and 12.7 also time the bench on the branches of [#534][pr-b] and [#535][pr-c].
From `research/delta-joins`, with `scripts/env.sh` sourced, build each in a worktree of its own
and pass it to `lookup_bench.sh`, which runs it beside this checkout's build:

```sh
git fetch origin research/lookup-borrowed-keys research/lookup-on-dogs3
for branch in lookup-borrowed-keys lookup-on-dogs3; do
  git worktree add --detach "$WORK/$branch" "origin/research/$branch"
  cargo build --release --manifest-path "$WORK/$branch/research/delta-joins/bench/Cargo.toml" \
    --target-dir "$WORK/$branch-bench"
done
scripts/lookup_bench.sh borrowed="$WORK/lookup-borrowed-keys-bench/release/lookup-bench" \
  dogs3="$WORK/lookup-on-dogs3-bench/release/lookup-bench"
```

The labels are the names in `lookup_bench_port_branches.log`. The study's runs of these branches
came from their heads before the port, on differential 0.25.1: 5c7f5af for `borrowed`, 1a40471
for `split-group2` and `hj2-group2`, and its parent for `split` and plain `half_join2`. In either
worktree, `cargo test -p flowlog-runtime` runs the lookup's tests as that branch changes them.

The drivers check that their builds and data exist and print the command that makes any that are
missing. Probe builds print their counts at exit when run with `FLOWLOG_PROBE=1`: run batch ones
directly, as in the quick start, and incremental ones through `inc_run.sh` with `PROBE_LINES=N`
to keep the first N `[probe]` lines.

`heuristics.py` with no arguments rescores the study's own runs and prints
`results/heuristics_out.md`: every policy of REPORT sections 10 and 11, H0 to H7, on the tuning
set, the held-out set and both. With `--results "$RESULTS"` it scores the runs that the drivers of
sections 10.1 to 11 wrote there. Plans measured only in earlier sections keep the medians
REPORT.md publishes, and a plan with no runs reads `n/m`.

Probe logs hold the rest of the planner inputs: `probe_cspa_base_postgresql.txt` and
`probe_cspa_base_linux.txt` (from `cspa_base_p`), `probe_sg_inc.txt` (from `sg_inc_p`), and the
records each binary program stores, which H7's margin reads. Those totals come from
`lookup_rows_probe.log` and from the base programs' probe logs that `STORED_LOGS` in the script
names. Copy the study's logs from `results/` or make new ones; the script names any log it cannot
find. The held-out set's other inputs are constants in the script, copied from
`probe_doop_*_batik.txt` and `probe_polonius_*_borrow.txt`. `--picks` prints only what each policy
picks, from the planner inputs alone; `heldout_picks.md` is that output for the held-out set,
saved before its runs, for H0 to H6. H7 came after them.

## Reading the results

- The seven CSV files have one row per run, with its switches in the `env` column. `runs.csv` holds
  the batch runs behind sections 4 to 7, `group_recheck.csv` section 4's rerun at 1, 8 and 32
  workers, `heur_runs.csv` the batch runs of section 10, `scout.csv` the first runs of section
  10's workloads, and `heldout_runs.csv` the runs of section 11. `cons_recheck.csv` holds section
  12's rerun of TC with ready rows merged and not, and `heldout_doop_before_fix.csv` section 11's
  first Doop runs, made before section 12's fix. New runs of `bench.py` go to
  `$RESULTS/runs.csv` unless `--csv` names a file.
- Memory is peak RSS. The CSV files (`maxrss_kb`) and `inc_run.sh` (`MAXRSS`) give it in KiB, as
  GNU time reports it; `bench.py`, `guarded.py`, `heuristics.py` and the REPORT divide it by 1024
  and call the result MB.
- Labels are build names, with `@dataset` or `@split` where a build ran on several inputs. Other
  suffixes name runtime switches: `_g0` and `_g1` for `FLOWLOG_LOOKUP_GROUP=0` and `1`, `nofuel`
  and `nocons` for `FLOWLOG_LOOKUP_FUEL=0` and `FLOWLOG_LOOKUP_CONSOLIDATE=0`, `d4s_T<N>` for
  `galen_d4s` with `FLOWLOG_SPLIT_T=N`, `_w<N>` for N workers, and `_recheck` for a later rerun.
- In `tc_group.log`, `cspa_group.log`, `galen_matrix.log` and `galen_split_matrix.log`, each line
  marked `MEDIAN` is one run (REPORT section 15).
- In `lookup_bench.log`, `build=` names the bench binary and `engine=` what it ran. `checkout` is
  the bench as committed, `v0` the lookup before section 12's fix and `borrowed` the lookup that
  borrows keys, [#534][pr-b]. From line 207 come section 12.7's builds of [#535][pr-c]'s
  adapter: `reuse` with keyed rows, from a bench that still called `flowlog_lookup`; `split` with
  split rows, at 1a40471's parent; and `split-group` and `split-group2` with grouping in dogs3,
  without and with its in-place walk of single-row keys. `split-group2` is 1a40471, the branch's
  head before the port, and `split-group` was never committed. `hj2-group` and `hj2-group2` run
  `half_join2` itself on those two dogs3 builds.
- `lookup_bench_refactor.log` is the same bench, 5 reps, after the operator's code was
  reorganized: `checkout` is the operator as committed, and `old` the one the study measured.
- `lookup_bench_port.log` is the same bench, 5 reps, on both versions. `checkout` is the bench as
  committed, on differential 0.26 and timely 0.32. `old`, `old-noland`, `old-hj2` and `old-join`
  are the bench before its port (786902c), on differential 0.25.1 and timely 0.31, running
  `flowlog`, `flowlog-noland`, `hj2` and `join`.
- `lookup_bench_borrowed.log` and `lookup_bench_dogs3.log` rerun the bench, 3 reps, on the
  branches of [#534][pr-b] and [#535][pr-c] as pushed before the port, at 5c7f5af and 1a40471. In
  each, `checkout` is the branch's head and `refactor` the operator of [#533][pr-a]. In the
  second, the head's `flowlog` and `hj2` engines are `split-group2` and `hj2-group2`, and `split`
  and `hj2` come from 1a40471's parent.
- `lookup_bench_port_branches.log` is the same bench, 5 reps, after the port. `checkout` is the
  bench as committed, and `borrowed` and `dogs3` run the `flowlog` engine of [#534][pr-b]'s and
  [#535][pr-c]'s heads.
- Logs written before the lookup-row gauge got its final name print `peak_lookup_pending_rows`; it
  is the same count as `peak_lookup_rows_held`.
- Some runs predate the operator as committed, and the REPORT says which: for example, section 4's
  fuel ablation ran before lookups were grouped.
- `r5` in file names, builds and count labels means CSPA rule 6. Rules count from 1 in source
  order, and the scripts kept an older label (REPORT section 3).
