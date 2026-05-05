#!/usr/bin/env bash
# tools/sweep/run_full_sweep.sh
#
# One ultimate entry point that runs the *entire* FlowLog test/benchmark
# stack against the current working tree, in dependency order, and emits
# a single human-readable diagnosis at the end.
#
# Layers (each runs only if the previous passed):
#
#   L0  Cargo workspace          cargo build + cargo test --workspace
#   L1  Unit fixtures            tests/unit/unit_compiler.sh
#                                tests/unit/unit_lib.sh
#   L2  Souffle oracle           tests/complex/datalog_batch_compiler.sh
#                                tests/complex/datalog_batch_lib.sh
#   L3  Performance compare      tools/benchmark/compare.sh
#                                  → result/benchmark/comparison_results.csv
#                                  (records both wall time AND peak RSS)
#   L4  LDBC SNB     (optional)  tests/ldbc/ldbc.sh
#
# Output:
#   <run-dir>/00..40_*.log   per-step transcripts
#   <run-dir>/diagnosis.txt  the rolled-up final report (also printed)
#   <run-dir>/comparison_results.csv  copy of the perf CSV (if L3 ran)
#   <run-dir>/meta.txt       git head, branch, env, timing
#
# Usage:
#   bash tools/sweep/run_full_sweep.sh                   # full sweep
#   bash tools/sweep/run_full_sweep.sh --smoke           # quick subset (~5 min)
#   bash tools/sweep/run_full_sweep.sh --skip-l3         # skip the long perf
#   bash tools/sweep/run_full_sweep.sh --include-ldbc    # opt-in L4
#   bash tools/sweep/run_full_sweep.sh --keep-going      # don't stop on first failure
#   bash tools/sweep/run_full_sweep.sh --workers N       # thread cap (default min(64, nproc))
#   bash tools/sweep/run_full_sweep.sh --baseline=souffle      # L3 baseline (default interpreter)
#   bash tools/sweep/run_full_sweep.sh --num-runs N      # L3 timed runs per pair (default 3)
#
# Environment (all optional; flags above take precedence):
#   WORKERS                  thread count for every engine   (default: min(64, nproc))
#   L3_BASELINE              L3 baseline list                (default: interpreter)
#                              comma-separated, any of {interpreter, souffle}
#                              Bare `BASELINE` accepted as back-compat alias.
#   L3_NUM_RUNS              L3 timed runs per (engine, pair) (default: 3)
#                              Bare `NUM_RUNS` accepted as back-compat alias.
#   FLOWLOG_KEEP_DATASETS    1 = retain datasets on cleanup  (sourced from
#                              /datasets/env.sh if present, else 0)
#   RUN_DIR                  override the run-output dir
#
# Exit code: 0 iff every layer that ran returned 0; the diagnosis lists
# the failed layer (if any) on the first line so a wrapper can grep it.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

# ----------------------------------------------------------------------
# Args
# ----------------------------------------------------------------------
SMOKE=0
SKIP_L3=0
INCLUDE_LDBC=0
KEEP_GOING=0
# L3-baseline forwarder: passed verbatim to compare.sh's --baseline= flag.
# `L3_BASELINE` is the canonical sweep-level name; bare `BASELINE` is a
# back-compat alias. Default "interpreter" preserves prior behaviour.
# Common values:
#   interpreter            time vs vldb26 interpreter (default)
#   souffle                time vs canonical Souffle programs only
#   interpreter,souffle    both side-by-side
L3_BASELINE="${L3_BASELINE:-${BASELINE:-interpreter}}"
# NUM_RUNS forwarder: how many timed runs per (engine, pair) for L3.
# `L3_NUM_RUNS` is the canonical sweep-level name; bare `NUM_RUNS`
# (compare.sh's native name) is a back-compat alias. Default unset →
# compare.sh keeps its own default (3). Other layers ignore this.
L3_NUM_RUNS="${L3_NUM_RUNS:-${NUM_RUNS:-}}"

# WORKERS controls thread count for EVERY engine (interpreter / compiler /
# library / souffle), so all baselines compete with the same parallelism.
# Default = min(64, nproc) — caps at the VLDB paper rig (64 cores) on
# bigger boxes so headline numbers stay paper-comparable, but
# auto-shrinks on smaller hosts. Override `make sweep WORKERS=...` or
# `--workers N` when co-running with another heavy job (e.g. an agent
# that needs cores). Keep the value consistent across runs you compare.
_NPROC=$(nproc 2>/dev/null || echo 64)
[[ "$_NPROC" =~ ^[0-9]+$ ]] && (( _NPROC > 0 )) || _NPROC=64
_DEFAULT_WORKERS=$(( _NPROC < 64 ? _NPROC : 64 ))
WORKERS="${WORKERS:-$_DEFAULT_WORKERS}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)         SMOKE=1; shift ;;
        --skip-l3)       SKIP_L3=1; shift ;;
        --include-ldbc)  INCLUDE_LDBC=1; shift ;;
        --keep-going)    KEEP_GOING=1; shift ;;
        --workers)
            WORKERS="$2"; shift 2
            [[ "$WORKERS" =~ ^[0-9]+$ ]] && (( WORKERS > 0 )) \
                || { echo "error: --workers must be a positive integer, got: $WORKERS" >&2; exit 2; }
            ;;
        --baseline=*)    L3_BASELINE="${1#--baseline=}"; shift ;;
        --baseline)      L3_BASELINE="$2"; shift 2 ;;
        --num-runs)
            L3_NUM_RUNS="$2"; shift 2
            [[ "$L3_NUM_RUNS" =~ ^[0-9]+$ ]] && (( L3_NUM_RUNS > 0 )) \
                || { echo "error: --num-runs must be a positive integer, got: $L3_NUM_RUNS" >&2; exit 2; }
            ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1 (try --help)" >&2; exit 2 ;;
    esac
done

# Validate any WORKERS provided via env (the --workers flag already
# validated above). One canonical check site means a typo in
# `WORKERS=foo make sweep` fails fast at sweep startup, not 30 min
# later inside L1 fixtures with a cryptic engine-side error.
[[ "$WORKERS" =~ ^[0-9]+$ ]] && (( WORKERS > 0 )) \
    || { echo "error: WORKERS must be a positive integer, got: $WORKERS" >&2; exit 2; }

# ----------------------------------------------------------------------
# Run dir + meta
# ----------------------------------------------------------------------
TS="$(date -u +%Y%m%d-%H%M%S)"
RUN_DIR="${RUN_DIR:-${ROOT_DIR}/result/sweep/${TS}}"
mkdir -p "$RUN_DIR"

# Source the dataset-cache env if present; saves redownloading multi-GB
# tarballs between layers.
[[ -f /datasets/env.sh ]] && source /datasets/env.sh

GIT_HEAD="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git branch --show-current 2>/dev/null || echo detached)"

cat > "$RUN_DIR/meta.txt" <<EOF
FlowLog full-sweep
==================
root        : $ROOT_DIR
HEAD        : $GIT_HEAD
branch      : $GIT_BRANCH
started     : $(date -u +"%Y-%m-%dT%H:%M:%SZ")
WORKERS     : $WORKERS
NUM_RUNS    : ${L3_NUM_RUNS:-(default: 3)}
L3_BASELINE : $L3_BASELINE
smoke       : $SMOKE
skip-l3     : $SKIP_L3
include-ldbc: $INCLUDE_LDBC
keep-going  : $KEEP_GOING
keep-data   : ${FLOWLOG_KEEP_DATASETS:-0}
host        : $(hostname)
EOF
echo "[sweep] run-dir: $RUN_DIR"
cat "$RUN_DIR/meta.txt"

# ----------------------------------------------------------------------
# Per-step runner. Records elapsed time + status; never breaks the loop
# unless KEEP_GOING=0 AND the step failed.
# ----------------------------------------------------------------------
declare -a STEP_NAMES=()
declare -a STEP_STATUS=()      # OK / FAIL / SKIP
declare -a STEP_ELAPSED=()
declare -a STEP_DETAIL=()      # one-line summary
declare -i ANY_FAIL=0

run_step() {
    # run_step <pretty-name> <log-file-stem> <skip-flag> -- <cmd...>
    local name="$1" stem="$2" skip="$3"; shift 3
    [[ "$1" == "--" ]] && shift
    local logfile="$RUN_DIR/${stem}.log"

    if [[ "$skip" == "1" ]]; then
        STEP_NAMES+=("$name"); STEP_STATUS+=("SKIP"); STEP_ELAPSED+=("0")
        STEP_DETAIL+=("skipped by flag")
        echo "[sweep] [$name] SKIPPED"
        return 0
    fi

    local t0; t0=$(date +%s)
    echo
    echo "============================================================"
    echo "[sweep] [$name] starting at $(date -u +%H:%M:%SZ)"
    echo "[sweep]   cmd: $*"
    echo "[sweep]   log: $logfile"
    echo "[sweep]   (follow live: tail -f \"$logfile\")"
    echo "============================================================"

    if "$@" >"$logfile" 2>&1; then
        local dt=$(( $(date +%s) - t0 ))
        STEP_NAMES+=("$name"); STEP_STATUS+=("OK"); STEP_ELAPSED+=("$dt")
        local detail
        detail="$(tail -n 1 "$logfile" 2>/dev/null | tr -d '\r' | cut -c1-120)"
        STEP_DETAIL+=("${detail:-no output}")
        echo "[sweep] [$name] OK (${dt}s)"
        return 0
    else
        local rc=$?
        local dt=$(( $(date +%s) - t0 ))
        STEP_NAMES+=("$name"); STEP_STATUS+=("FAIL"); STEP_ELAPSED+=("$dt")
        local detail
        # `|| true` is load-bearing: under `set -euo pipefail`, a no-match
        # grep would otherwise abort the sweep here, swallowing the failure
        # we are trying to record.
        detail="$(grep -iE 'fail|error|panicked' "$logfile" 2>/dev/null | tail -n 1 | tr -d '\r' | cut -c1-120 || true)"
        STEP_DETAIL+=("${detail:-rc=$rc}")
        ANY_FAIL=1
        echo "[sweep] [$name] FAIL rc=$rc (${dt}s) — last 30 lines:"
        tail -n 30 "$logfile" | sed 's/^/    /'
        if [[ "$KEEP_GOING" != "1" ]]; then
            echo "[sweep] aborting (use --keep-going to continue past failures)"
            write_diagnosis
            exit "$rc"
        fi
        return 0
    fi
}

# ----------------------------------------------------------------------
# Look up a recorded step's status by glob pattern. Returns the LATEST
# matching step's status ("OK" / "FAIL" / "SKIP"), or "" if no match.
# Used by write_diagnosis to gate perf-CSV ingestion on L3's outcome —
# without this gate, a stale CSV from a prior sweep would be ingested
# whenever the current sweep skips L3 (--skip-l3) or aborts before L3.
# ----------------------------------------------------------------------
step_status() {
    local pattern="$1"
    local i
    for (( i=${#STEP_NAMES[@]}-1; i>=0; i-- )); do
        # shellcheck disable=SC2053
        if [[ "${STEP_NAMES[$i]}" == $pattern ]]; then
            echo "${STEP_STATUS[$i]}"
            return
        fi
    done
    echo ""
}

# ----------------------------------------------------------------------
# Diagnosis writer. Always called at the end (and on early abort).
# ----------------------------------------------------------------------
write_diagnosis() {
    local diag="$RUN_DIR/diagnosis.txt"
    local csv="${ROOT_DIR}/result/benchmark/comparison_results.csv"

    # Did L3 actually run successfully THIS sweep? The perf CSV lives at
    # a fixed path; without a gate the diagnosis would happily ingest a
    # leftover CSV from a previous sweep when the current one was invoked
    # with --skip-l3 (or aborted before L3 even started).
    #
    # Three states matter:
    #   L3 OK                        → ingest as a complete sweep.
    #   L3 FAIL with non-empty CSV   → ingest as PARTIAL — under
    #                                  fail-closed pair semantics, every
    #                                  CSV row reflects a pair where every
    #                                  required engine succeeded, so the
    #                                  rows that ARE present remain valid;
    #                                  flagging it just tells the operator
    #                                  the sweep stopped early.
    #   L3 SKIP / never-ran / empty  → don't ingest; emit a clear "not
    #                                  consumed" message.
    local l3_status
    l3_status="$(step_status 'L3 *')"
    local ingest_csv=0
    local l3_partial=0
    local n_csv_rows=0
    if [[ -f "$csv" ]]; then
        # Count non-blank, non-header rows. `|| true` keeps `set -e` happy
        # when the CSV is empty.
        n_csv_rows=$(awk 'NR>1 && NF>0' "$csv" 2>/dev/null | wc -l || true)
    fi
    if [[ "$l3_status" == "OK" && -f "$csv" ]]; then
        ingest_csv=1
    elif [[ "$l3_status" == "FAIL" && -f "$csv" && $n_csv_rows -gt 0 ]]; then
        ingest_csv=1
        l3_partial=1
    fi

    # ----- Pre-compute the "problems flagged" list from the perf CSV
    # so the verdict line at the top can mention them. Empty when the
    # CSV is absent or every row is clean.
    local flags_file="$RUN_DIR/.flags"
    : > "$flags_file"
    if (( ingest_csv )); then
        python3 - "$csv" "$flags_file" <<'PY' || true
import csv, sys
path, out = sys.argv[1], sys.argv[2]
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
flags = []
with open(path) as f:
    for r in csv.DictReader(f):
        pair = f"{r['Program']}/{r['Dataset']}"
        # 1. Cross-check mismatches on output row counts (or partial overlap
        #    where one engine produced relations the other didn't).
        xc = (r.get("Crosscheck_Souffle") or "").strip()
        if xc.startswith("MISMATCH") or xc.startswith("PARTIAL"):
            flags.append(f"CROSSCHECK   {pair}: {xc}")
        # 2. Lib runner more than 20% off compiler exec time (sanity).
        lvc = num(r.get("Lib_vs_Compiler_Exec"))
        # 2. Lib runner more than 20% off compiler exec time (sanity).
        #    Empirical envelope: lib should be within ±30% of compiler
        #    exec time on the paper rig — both run the same generated
        #    code; differences come from the lib-runner glue and
        #    run-to-run noise. Anything outside [0.7×, 1.4×] flags as
        #    LIB-DRIFT (often noise on tiny workloads — see PARTIAL).
        lvc = num(r.get("Lib_vs_Compiler_Exec"))
        if lvc is not None and (lvc < 0.7 or lvc > 1.4):
            flags.append(f"LIB-DRIFT    {pair}: lib/compiler exec ratio = {lvc:.2f}x")
        # 3. Compiler 1.5x+ slower than Souffle (a real perf regression hint).
        svc = num(r.get("Souffle_vs_Compiler_Total"))
        if svc is not None and svc < 0.66:
            flags.append(f"PERF         {pair}: souffle is {1/svc:.2f}x faster than compiler "
                         f"(souffle_total/compiler_total = {svc:.2f})")
        # 4. Compiler used >2x interpreter peak RSS.
        c_mem = num(r.get("Compiler_PeakRss_MB"))
        i_mem = num(r.get("Interp_PeakRss_MB"))
        if c_mem is not None and i_mem is not None and i_mem > 0 and c_mem / i_mem > 2.0:
            flags.append(f"MEM          {pair}: compiler peak RSS {c_mem:.0f} MB "
                         f"vs interpreter {i_mem:.0f} MB ({c_mem/i_mem:.2f}x)")
        # 5. Partial samples — fewer than NUM_RUNS runs of an engine
        #    succeeded for this pair. The median is still recorded but
        #    over fewer samples, so it's less reliable.
        for engine in ("Compiler", "Lib", "Souffle", "Interp"):
            n = (r.get(f"{engine}_RunsSucceeded") or "").strip()
            if n.isdigit() and int(n) < 3 and int(n) > 0:
                flags.append(f"PARTIAL      {pair}: {engine} only {n}/3 runs succeeded")
with open(out, "w") as f:
    for line in flags:
        f.write(line + "\n")
PY
    fi
    local n_flags=0
    [[ -s "$flags_file" ]] && n_flags=$(wc -l < "$flags_file")

    {
        echo "FlowLog full-sweep diagnosis"
        echo "============================"
        echo "HEAD              : $GIT_HEAD"
        echo "branch            : $GIT_BRANCH"
        echo "run dir           : $RUN_DIR"
        echo "started           : $(grep '^started' "$RUN_DIR/meta.txt" | cut -d: -f2- | xargs)"
        echo "ended             : $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        echo "WORKERS           : $WORKERS  (applied identically to every L3 engine — fairness invariant)"
        echo "smoke             : $SMOKE   skip-l3: $SKIP_L3   include-ldbc: $INCLUDE_LDBC"
        echo

        # Top-of-file one-line verdict so an agent or human gets the
        # answer without scrolling.
        if (( ANY_FAIL )); then
            echo "VERDICT: FAIL — at least one suite failed (see steps below)."
        elif (( n_flags )); then
            echo "VERDICT: PASS WITH $n_flags FLAG(S) — all suites green; perf/correctness anomalies listed at end."
        else
            echo "VERDICT: PASS — every suite green, no anomalies flagged."
        fi
        echo

        printf "%-32s  %-5s  %8s  %s\n" "Layer / Step" "Stat" "Sec" "Last-line / first-error"
        printf "%-32s  %-5s  %8s  %s\n" "--------------------------------" "-----" "--------" "------------------------"
        local n=${#STEP_NAMES[@]}
        local total=0
        for ((i=0; i<n; i++)); do
            printf "%-32s  %-5s  %8s  %s\n" \
                "${STEP_NAMES[$i]}" "${STEP_STATUS[$i]}" "${STEP_ELAPSED[$i]}" "${STEP_DETAIL[$i]}"
            total=$((total + STEP_ELAPSED[i]))
        done
        echo
        printf "%-32s  %-5s  %8s\n" "TOTAL" "" "$total"
        echo

        # ----- Perf CSV roll-up -----
        if (( ingest_csv )); then
            cp "$csv" "$RUN_DIR/comparison_results.csv"
            if (( l3_partial )); then
                echo "Performance CSV ($csv) — PARTIAL ($n_csv_rows pair(s) in CSV; L3 step ended FAIL — see step log):"
            else
                echo "Performance CSV ($csv):"
            fi
            echo "----------------------------------------------------------------"
            python3 - "$csv" <<'PY'
import csv, sys
path = sys.argv[1]
with open(path) as f:
    rows = list(csv.DictReader(f))
if not rows:
    print("(no rows)")
    sys.exit(0)

def num(x):
    try:    return float(x)
    except (TypeError, ValueError): return None

cols_time = ["Compiler_Exec", "Lib_Exec", "Exec_Speedup", "Lib_vs_Compiler_Exec"]
cols_mem  = ["Compiler_PeakRss_MB", "Lib_PeakRss_MB", "Lib_vs_Compiler_Mem"]
cols_sf   = ["Souffle_Total", "Souffle_PeakRss_MB", "Souffle_vs_Compiler_Total", "Crosscheck_Souffle"]
have_mem  = any((r.get("Compiler_PeakRss_MB") or "").strip() for r in rows)
have_sf   = any((r.get("Souffle_Total") or "N/A").strip() not in ("", "N/A") for r in rows)

hdr = ["Program", "Dataset"] + cols_time + (cols_mem if have_mem else []) + (cols_sf if have_sf else [])
widths = [max(len(h), max(len((r.get(h) or "").strip()) for r in rows)) for h in hdr]
def emit(vals): print("  " + "  ".join(f"{v:<{w}}" for v, w in zip(vals, widths)))
emit(hdr)
emit(["-" * w for w in widths])
for r in rows:
    emit([(r.get(h) or "").strip() for h in hdr])

# Aggregates.
times = [num(r.get("Compiler_Exec")) for r in rows]
times = [t for t in times if t is not None]
if times:
    print()
    print(f"  pairs           : {len(rows)}")
    print(f"  Compiler_Exec   : min={min(times):.6f}s  max={max(times):.6f}s  sum={sum(times):.6f}s")
if have_mem:
    rss = [num(r.get("Compiler_PeakRss_MB")) for r in rows]
    rss = [v for v in rss if v is not None]
    if rss:
        print(f"  Compiler_PeakRss: min={min(rss):.2f}MB  max={max(rss):.2f}MB")
if have_sf:
    n_match    = sum(1 for r in rows if (r.get("Crosscheck_Souffle") or "").startswith("match"))
    n_mismatch = sum(1 for r in rows if (r.get("Crosscheck_Souffle") or "").startswith("MISMATCH"))
    n_na       = sum(1 for r in rows if (r.get("Crosscheck_Souffle") or "").strip() in ("", "n/a"))
    print(f"  Souffle xcheck  : {n_match} match, {n_mismatch} mismatch, {n_na} n/a")
PY
            echo "----------------------------------------------------------------"
        else
            case "$l3_status" in
                "")     echo "(perf CSV not produced — L3 never ran in this sweep)" ;;
                SKIP)   echo "(perf CSV not produced — L3 was skipped via --skip-l3)" ;;
                FAIL)   echo "(perf CSV not consumed — L3 failed and the CSV is empty; see step log)" ;;
                *)      echo "(perf CSV not consumed — L3 status: $l3_status)" ;;
            esac
        fi

        echo
        # ----- Problems flagged (from the pre-computed flags file) -----
        if (( n_flags )); then
            echo "Problems flagged ($n_flags)"
            echo "----------------------------------------------------------------"
            cat "$flags_file"
            echo "----------------------------------------------------------------"
            echo
        fi

        if [[ $ANY_FAIL -eq 0 && $n_flags -eq 0 ]]; then
            echo "OVERALL: ALL LAYERS PASSED — no anomalies flagged."
        elif [[ $ANY_FAIL -eq 0 ]]; then
            echo "OVERALL: ALL LAYERS PASSED — but $n_flags anomalie(s) flagged above."
        else
            echo "OVERALL: AT LEAST ONE LAYER FAILED -- see per-step logs above"
        fi
    } | tee "$diag"
}

# ----------------------------------------------------------------------
# Pre-flight - safety regression test for cleanup_dataset across L2/L3/L4.
# Runs in <1s and trips before L1/L2/L3 if the symlink-protection guard
# regresses, so a buggy patch can't accidentally rm -rf the persistent
# /datasets cache through a facts→/datasets/facts symlink.
# ----------------------------------------------------------------------
run_step "Pre-flight cleanup safety" "00a-safety-cleanup" 0 -- \
    bash tests/safety/cleanup_dataset_test.sh

# ----------------------------------------------------------------------
# L0 - workspace build + Rust unit tests
#
# Note: we deliberately do NOT pass `--quiet` to cargo here. Cold cargo
# builds take 5-15 minutes; the `tail -f $logfile` hint above is far more
# useful when the log shows per-crate "Compiling foo v0.1.2" progress
# instead of a deafening silence followed by a single "Finished" line.
# ----------------------------------------------------------------------
run_step "L0 cargo build"   "00-cargo-build" 0 -- \
    bash -c "RAYON_NUM_THREADS=$WORKERS CARGO_BUILD_JOBS=$WORKERS cargo build --release --workspace"

run_step "L0 cargo test"    "01-cargo-test"  0 -- \
    bash -c "RAYON_NUM_THREADS=$WORKERS CARGO_BUILD_JOBS=$WORKERS cargo test --release --workspace --quiet"

# ----------------------------------------------------------------------
# L1 - unit fixtures (binary + library mode)
# ----------------------------------------------------------------------
if [[ "$SMOKE" == "1" ]]; then
    L1_FIXTURES=(agg_sum recursive_tc compare_eq)
    run_step "L1 unit_compiler (smoke)" "10-unit-compiler" 0 -- \
        bash tests/unit/unit_compiler.sh "${L1_FIXTURES[@]}"
    run_step "L1 unit_lib (smoke)"      "11-unit-lib"      0 -- \
        bash tests/unit/unit_lib.sh      "${L1_FIXTURES[@]}"
else
    run_step "L1 unit_compiler"         "10-unit-compiler" 0 -- \
        bash tests/unit/unit_compiler.sh
    run_step "L1 unit_lib"              "11-unit-lib"      0 -- \
        bash tests/unit/unit_lib.sh
fi

# ----------------------------------------------------------------------
# L2 - Souffle-oracle correctness on real benchmark programs
# ----------------------------------------------------------------------
if [[ "$SMOKE" == "1" ]]; then
    L2_CFG="$RUN_DIR/complex-smoke.txt"
    cat > "$L2_CFG" <<EOF
graph_analysis/tc.dl=G5K-0.001
graph_analysis/sg.dl=G5K-0.001
knowledge_reasoning/crdt.dl=crdt
EOF
else
    L2_CFG="${ROOT_DIR}/tests/complex/config_integer.txt"
fi

run_step "L2 datalog_batch_compiler"   "20-complex-compiler" 0 -- \
    bash -c "WORKERS=$WORKERS RAYON_NUM_THREADS=$WORKERS bash tests/complex/datalog_batch_compiler.sh $L2_CFG"
run_step "L2 datalog_batch_lib"        "21-complex-lib"      0 -- \
    bash -c "WORKERS=$WORKERS RAYON_NUM_THREADS=$WORKERS bash tests/complex/datalog_batch_lib.sh $L2_CFG"

# ----------------------------------------------------------------------
# L3 - perf compare (records both timing AND peak RSS in CSV)
# ----------------------------------------------------------------------
# Pre-build the env-prefix and the --baseline flag so the same string is
# echoed in the diagnosis logs for reproducibility.
_L3_ENV="WORKERS=$WORKERS"
[[ -n "$L3_NUM_RUNS" ]] && _L3_ENV="$_L3_ENV NUM_RUNS=$L3_NUM_RUNS"
_L3_BASELINE_FLAG="--baseline=$L3_BASELINE"

if [[ "$SKIP_L3" == "1" ]]; then
    run_step "L3 benchmark/compare"    "30-bench-compare"    1 -- :
elif [[ "$SMOKE" == "1" ]]; then
    L3_CFG="${ROOT_DIR}/tools/benchmark/config-smoke-tmp.txt"
    cat > "$L3_CFG" <<EOF
graph_analysis/tc.dl=G5K-0.001
knowledge_reasoning/crdt.dl=crdt
EOF
    run_step "L3 benchmark/compare (smoke)" "30-bench-compare" 0 -- \
        bash -c "$_L3_ENV bash tools/benchmark/compare.sh --fresh $_L3_BASELINE_FLAG $L3_CFG"
    rm -f "$L3_CFG"
else
    run_step "L3 benchmark/compare"    "30-bench-compare"    0 -- \
        bash -c "$_L3_ENV bash tools/benchmark/compare.sh --fresh $_L3_BASELINE_FLAG"
fi

# ----------------------------------------------------------------------
# L4 - LDBC (opt-in)
# ----------------------------------------------------------------------
if [[ "$INCLUDE_LDBC" == "1" ]]; then
    run_step "L4 ldbc"                 "40-ldbc"             0 -- \
        bash -c "WORKERS=$WORKERS bash tests/ldbc/ldbc.sh"
else
    run_step "L4 ldbc"                 "40-ldbc"             1 -- :
fi

# ----------------------------------------------------------------------
# Final diagnosis
# ----------------------------------------------------------------------
write_diagnosis
exit "$ANY_FAIL"
