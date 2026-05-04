#!/usr/bin/env bash
# tools/agentic/run_full_sweep.sh
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
#   bash tools/agentic/run_full_sweep.sh                # full sweep
#   bash tools/agentic/run_full_sweep.sh --smoke        # quick subset
#   bash tools/agentic/run_full_sweep.sh --skip-l3      # skip the long perf
#   bash tools/agentic/run_full_sweep.sh --include-ldbc # opt-in L4
#   bash tools/agentic/run_full_sweep.sh --keep-going   # don't stop on first failure
#
# Environment:
#   WORKERS                  override worker count        (default: nproc)
#   FLOWLOG_KEEP_DATASETS    1 = retain datasets          (sourced from
#                             /datasets/env.sh if present, else 0)
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
WORKERS="${WORKERS:-$(nproc)}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)         SMOKE=1; shift ;;
        --skip-l3)       SKIP_L3=1; shift ;;
        --include-ldbc)  INCLUDE_LDBC=1; shift ;;
        --keep-going)    KEEP_GOING=1; shift ;;
        --workers)       WORKERS="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

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
        detail="$(grep -iE 'fail|error|panicked' "$logfile" | tail -n 1 | tr -d '\r' | cut -c1-120)"
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
# Diagnosis writer. Always called at the end (and on early abort).
# ----------------------------------------------------------------------
write_diagnosis() {
    local diag="$RUN_DIR/diagnosis.txt"
    {
        echo "FlowLog full-sweep diagnosis"
        echo "============================"
        echo "HEAD              : $GIT_HEAD"
        echo "branch            : $GIT_BRANCH"
        echo "run dir           : $RUN_DIR"
        echo "started           : $(grep '^started' "$RUN_DIR/meta.txt" | cut -d: -f2- | xargs)"
        echo "ended             : $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        echo "WORKERS           : $WORKERS"
        echo "smoke             : $SMOKE   skip-l3: $SKIP_L3   include-ldbc: $INCLUDE_LDBC"
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

        # ----- Perf CSV roll-up (if present) -----
        local csv="${ROOT_DIR}/result/benchmark/comparison_results.csv"
        if [[ -f "$csv" ]]; then
            cp "$csv" "$RUN_DIR/comparison_results.csv"
            echo "Performance CSV ($csv):"
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

# Wide columns we always print when present.
cols_time = ["Compiler_Exec", "Lib_Exec", "Exec_Speedup", "Lib_vs_Compiler_Exec"]
cols_mem  = ["Compiler_PeakRss_MB", "Lib_PeakRss_MB", "Lib_vs_Compiler_Mem"]
have_mem  = any((r.get("Compiler_PeakRss_MB") or "").strip() for r in rows)

hdr = ["Program", "Dataset"] + cols_time + (cols_mem if have_mem else [])
widths = [max(len(h), max(len((r.get(h) or "").strip()) for r in rows)) for h in hdr]
def emit(vals): print("  " + "  ".join(f"{v:<{w}}" for v, w in zip(vals, widths)))
emit(hdr)
emit(["-" * w for w in widths])
for r in rows:
    emit([(r.get(h) or "").strip() for h in hdr])

# Quick aggregates over numeric Compiler_Exec / Compiler_PeakRss_MB.
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
PY
            echo "----------------------------------------------------------------"
        else
            echo "(perf CSV not produced — L3 skipped or failed)"
        fi

        echo
        if [[ $ANY_FAIL -eq 0 ]]; then
            echo "OVERALL: ALL LAYERS PASSED"
        else
            echo "OVERALL: AT LEAST ONE LAYER FAILED -- see per-step logs above"
        fi
    } | tee "$diag"
}

# ----------------------------------------------------------------------
# L0 - workspace build + Rust unit tests
# ----------------------------------------------------------------------
run_step "L0 cargo build"   "00-cargo-build" 0 -- \
    bash -c "RAYON_NUM_THREADS=$WORKERS CARGO_BUILD_JOBS=$WORKERS cargo build --release --workspace --quiet"

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
if [[ "$SKIP_L3" == "1" ]]; then
    run_step "L3 benchmark/compare"    "30-bench-compare"    1 -- :
elif [[ "$SMOKE" == "1" ]]; then
    L3_CFG="${ROOT_DIR}/tools/benchmark/config-smoke-tmp.txt"
    cat > "$L3_CFG" <<EOF
graph_analysis/tc.dl=G5K-0.001
knowledge_reasoning/crdt.dl=crdt
EOF
    run_step "L3 benchmark/compare (smoke)" "30-bench-compare" 0 -- \
        bash -c "WORKERS=$WORKERS bash tools/benchmark/compare.sh $L3_CFG"
    rm -f "$L3_CFG"
else
    run_step "L3 benchmark/compare"    "30-bench-compare"    0 -- \
        bash -c "WORKERS=$WORKERS bash tools/benchmark/compare.sh"
fi

# ----------------------------------------------------------------------
# L4 - LDBC (opt-in)
# ----------------------------------------------------------------------
if [[ "$INCLUDE_LDBC" == "1" ]]; then
    run_step "L4 ldbc"                 "40-ldbc"             0 -- \
        bash tests/ldbc/ldbc.sh
else
    run_step "L4 ldbc"                 "40-ldbc"             1 -- :
fi

# ----------------------------------------------------------------------
# Final diagnosis
# ----------------------------------------------------------------------
write_diagnosis
exit "$ANY_FAIL"
