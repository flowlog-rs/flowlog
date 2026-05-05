#!/bin/bash
#
# Single-pair benchmark wrapper used by the agentic perf gate.
#
# Mirrors compare.sh's `run_lib`: builds the lib runner once for the given
# (program.dl, dataset) pair, runs it NUM_RUNS times, then prints two
# lines on stdout for downstream extractors:
#
#     elapsed_seconds <median> <min> <max> <runs> <workers>
#     peak_rss_kb     <median> <min> <max> <runs> <workers>
#
# Field index 1 (the median) is what extractors read. The `peak_rss_kb`
# line is added in addition to the original `elapsed_seconds` line so
# perf gates that only know about `elapsed_seconds` keep working
# unchanged. To gate on memory, point the extractor at `peak_rss_kb`.
#
# All times are in seconds with 9 decimal digits; memory is in kibibytes
# as reported by `/usr/bin/time -v`'s "Maximum resident set size".
#
# Usage:
#     bash tools/benchmark/bench_one.sh <prog_rel> <dataset_name>
# Example:
#     bash tools/benchmark/bench_one.sh example/knowledge_reasoning/crdt.dl facts/crdt
#
# Both args may be given relative to the repo root OR with the
# example/ / facts/ prefixes — the prefixes are stripped if present.
#
# Environment overrides:
#     WORKERS    -- worker thread count   (default: 1; matches the agentic
#                   "smallest perf signal" goal — single-thread is the most
#                   stable measurement, parallel runs add noise)
#     NUM_RUNS   -- timed runs per call   (default: 3)
#     QUIET      -- if 1, suppress per-run progress lines on stderr
#
# All log/build output is sent to stderr so stdout stays clean for
# extractors. Failures exit non-zero (so the perf gate fails closed).

set -euo pipefail

# ----------------------------------------------------------------------
# Helpers expected by tools/benchmark/lib_runner.sh (it does not import
# them itself; compare.sh defines them in its own scope).
# ----------------------------------------------------------------------
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
log() {
    [[ "${QUIET:-0}" = "1" ]] && return 0
    # Signature compatible with compare.sh's `log <colour> <tag> <msg...>`,
    # but colour-free and unconditional — these go to stderr.
    local _c="${1:-}" tag="${2:-LOG}"
    shift 2 || true
    printf '[%s] %s\n' "$tag" "$*" >&2
}

# ----------------------------------------------------------------------
# Path / config setup (kept compatible with compare.sh's globals so the
# shared lib_runner.sh helpers see exactly the same environment).
# ----------------------------------------------------------------------
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROG_DIR="${ROOT_DIR}/example"
FACT_DIR="${ROOT_DIR}/facts"
LIB_BENCH_RUNNER_DIR="${ROOT_DIR}/target/bench-lib/runner"
LIB_BENCH_BIN="${LIB_BENCH_RUNNER_DIR}/target/release/flowlog_bench_lib"

WORKERS="${WORKERS:-1}"
NUM_RUNS="${NUM_RUNS:-3}"

# Match compare.sh's run_lib: integer-typed programs, no SIP, no string
# interning. crdt + galen are integer-typed in config.txt.
LIB_BENCH_SIP=0
LIB_BENCH_STR_INTERN=0

# Source the shared synthesis helpers (also used by compare.sh).
# shellcheck source=lib_runner.sh
source "$(dirname "$0")/lib_runner.sh"

# ----------------------------------------------------------------------
# Argument parsing.
# ----------------------------------------------------------------------
[[ $# -eq 2 ]] || die "usage: $0 <prog.dl> <dataset>"
PROG_ARG="$1"
DATASET_ARG="$2"

# Accept either `example/foo.dl` or `foo.dl` (repo-relative). Strip a
# leading `example/` so we can reuse PROG_DIR.
PROG_REL="${PROG_ARG#example/}"
PROG_PATH="${PROG_DIR}/${PROG_REL}"
[[ -f "$PROG_PATH" ]] || die "program not found: $PROG_PATH"

# Same idea for the dataset name (`facts/crdt` -> `crdt`).
DATASET_NAME="${DATASET_ARG#facts/}"
DATASET_PATH="${FACT_DIR}/${DATASET_NAME}"
[[ -d "$DATASET_PATH" ]] || die "dataset dir not found: $DATASET_PATH"

PROG_FILE="$(basename "$PROG_PATH")"
log "" "BENCH" "${PROG_FILE} + ${DATASET_NAME} (workers=${WORKERS}, runs=${NUM_RUNS})"

# ----------------------------------------------------------------------
# Build the lib runner crate for this (program, dataset) pair.
#
# We always re-synthesize main.rs and trigger a `cargo build --release`,
# because the candidate file under test may have changed the codegen
# output. cargo's incremental cache makes this cheap when nothing in the
# workspace actually changed.
# ----------------------------------------------------------------------
mkdir -p "$LIB_BENCH_RUNNER_DIR"
bench_lib_ensure_crate

PAIRS_RAW="$(bench_lib_discover_csvs "$PROG_PATH" "$(realpath "$DATASET_PATH")")"
[[ -n "$PAIRS_RAW" ]] || die "no CSVs discovered for $PROG_FILE under $DATASET_PATH"

# Stage program.dl unchanged (matches compare.sh: no .printsize→.output
# rewrite — would force materializing output Vecs and skew the timing).
PREPARED_DL="${LIB_BENCH_RUNNER_DIR}/program.dl"
cp "$PROG_PATH" "$PREPARED_DL"

bench_lib_write_build_rs

PAIRS_SPACE="$(printf '%s' "$PAIRS_RAW" | tr '\n' ' ')"
bench_lib_write_main_rs "$PREPARED_DL" "$PAIRS_SPACE" \
    || die "main.rs synthesis failed for $PROG_FILE"

log "" "BUILD" "cargo build --release --quiet"
(cd "$LIB_BENCH_RUNNER_DIR" && cargo build --release --quiet >&2) \
    || die "cargo build failed for $PROG_FILE"
[[ -x "$LIB_BENCH_BIN" ]] || die "lib bench binary not found: $LIB_BENCH_BIN"

# ----------------------------------------------------------------------
# Build env-var array: FLOWLOG_CSV_<REL>=<abspath>.
# ----------------------------------------------------------------------
declare -a CSV_ENVS=()
while IFS= read -r LINE; do
    [[ -n "$LINE" ]] || continue
    REL="${LINE%%=*}"
    CSV_ABS="${LINE#*=}"
    CSV_ENVS+=("FLOWLOG_CSV_${REL^^}=${CSV_ABS}")
done <<< "$PAIRS_RAW"

# ----------------------------------------------------------------------
# Run the binary NUM_RUNS times. Extract the "Dataflow executed in <Dur>"
# line from each run; convert to seconds. Also wrap the binary with
# `/usr/bin/time -v` so we can read peak RSS for the same run.
# ----------------------------------------------------------------------
TIMES=()
RSS_KB=()
RUN_LOG_DIR="${LIB_BENCH_RUNNER_DIR}/.bench-logs"
mkdir -p "$RUN_LOG_DIR"

# /usr/bin/time -v is GNU time (writes "Maximum resident set size (kbytes): N").
# Bash's builtin `time` does NOT support -v, so we require the binary.
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
[[ -x "$TIME_BIN" ]] || die "GNU /usr/bin/time not found (apt install time); set TIME_BIN=<path>"

for run in $(seq 1 "$NUM_RUNS"); do
    RUN_LOG="${RUN_LOG_DIR}/run${run}.log"
    RSS_LOG="${RUN_LOG_DIR}/run${run}.rss"

    if ! env "${CSV_ENVS[@]}" WORKERS="$WORKERS" \
            "$TIME_BIN" -v -o "$RSS_LOG" "$LIB_BENCH_BIN" \
            > "$RUN_LOG" 2>&1; then
        log "" "WARN" "run $run failed (see $RUN_LOG); skipping"
        continue
    fi

    # Pull the "Dataflow executed in <Duration>" line and convert to seconds.
    # Duration formats: "12.345s", "621.15ms", "17.804µs". Mirrors
    # compare.sh::_extract_time_for_pattern.
    LINE="$(grep 'Dataflow executed' "$RUN_LOG" | tail -1 || true)"
    [[ -n "$LINE" ]] || { log "" "WARN" "run $run: no timing line"; continue; }

    SEC="$(python3 -c '
import re, sys
line = sys.argv[1]
m = re.search(r"([0-9]+\.?[0-9]*)(µs|ms|s)", line)
if not m:
    print("")
    sys.exit(0)
v = float(m.group(1))
unit = m.group(2)
if unit == "ms":  v /= 1_000.0
elif unit == "µs": v /= 1_000_000.0
print(f"{v:.9f}")
' "$LINE")"

    # Pull the peak RSS in kibibytes from /usr/bin/time -v's log.
    # Format: "        Maximum resident set size (kbytes): 12345".
    RSS="$(awk '/Maximum resident set size/ {print $NF; exit}' "$RSS_LOG" 2>/dev/null || true)"

    if [[ -n "$SEC" ]]; then
        TIMES+=("$SEC")
        if [[ "$RSS" =~ ^[0-9]+$ ]]; then
            RSS_KB+=("$RSS")
            log "" "RUN" "  run $run: ${SEC}s, peak ${RSS} KiB"
        else
            log "" "RUN" "  run $run: ${SEC}s (rss N/A)"
        fi
    else
        log "" "WARN" "run $run: could not parse time from: $LINE"
    fi
done

[[ ${#TIMES[@]} -gt 0 ]] || die "all $NUM_RUNS runs failed"

# ----------------------------------------------------------------------
# Compute median / min / max for both elapsed time and peak RSS in Python
# (handles even/odd run counts; medians are independent because the two
# distributions are not perfectly correlated). Print two contract lines
# on stdout — extractors that only know about `elapsed_seconds` keep
# working untouched; gates that want memory regression detection point
# their token at `peak_rss_kb` instead.
# ----------------------------------------------------------------------
python3 - "${TIMES[@]}" "--rss" "${RSS_KB[@]}" "--workers" "$WORKERS" <<'PY'
import sys

argv = sys.argv[1:]
sep_rss = argv.index("--rss")
sep_w   = argv.index("--workers")
times   = sorted(float(x) for x in argv[:sep_rss])
rss     = sorted(int(x)   for x in argv[sep_rss + 1 : sep_w])
workers = argv[sep_w + 1]

def med(xs):
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0

n_t = len(times)
m_t = med(times)
print(f"elapsed_seconds {m_t:.9f} {times[0]:.9f} {times[-1]:.9f} {n_t} {workers}")

if rss:
    n_r = len(rss)
    m_r = med(rss)
    # Round median back to int kibibytes; min/max stay native.
    print(f"peak_rss_kb {int(round(m_r))} {rss[0]} {rss[-1]} {n_r} {workers}")
else:
    # Emit a placeholder line so extractors keying on the token still
    # see it (they will read N/A and either skip or fail closed).
    print(f"peak_rss_kb N/A N/A N/A 0 {workers}")
PY
