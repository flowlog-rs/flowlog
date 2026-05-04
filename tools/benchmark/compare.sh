#!/bin/bash
set -euo pipefail

# ==========================================================================
# FlowLog Version Comparison Benchmark
# ==========================================================================
#
# Compares the current FlowLog compiler (this repo) against the previous
# interpreter version (vldb26-artifact).  Only the non-optimization (no -O)
# configuration is run for the interpreter.
#
# Each benchmark pair is executed NUM_RUNS times; the median is kept.
#
# Usage:
#   bash tools/benchmark/compare.sh [config_file]
#
# Environment variables:
#   WORKERS  -- number of worker threads passed to EVERY competitor in
#              this comparison (interpreter via --workers, compiler via
#              -w, library mode via WORKERS env, souffle via -j). One
#              value, four engines — that's the fairness contract: all
#              baselines get identical parallelism on identical hardware.
#              Default: 64 (matches the VLDB paper rig). Override e.g.
#              `WORKERS=16 bash compare.sh` to time on a smaller host;
#              just keep it the same across runs you compare.
# ==========================================================================

############################################################
# COLOURS
############################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Print a coloured log message:  log <colour> <tag> <message...>
log() {
    local c="$1" t="$2"
    shift 2
    echo -e "${c}[${t}]${NC} $*"
}

# Print an error and exit immediately.
die() { log "$RED" "ERROR" "$*"; exit 1; }

############################################################
# PATH CONFIGURATION
############################################################

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

# --fresh forces a clean run (wipes logs + CSV). Without it, the script
# resumes: existing CSV rows are preserved, and any (program, dataset)
# pair already present in the CSV is skipped.
#
# --baseline=<list> picks which extra engine(s) to time alongside the
# compiler + library mode. Any combination of "interpreter" and "souffle"
# is accepted (comma-separated). Default: "interpreter" (preserves the
# original behaviour).
#
#   --baseline=interpreter        # default — vldb26 interpreter
#   --baseline=souffle            # canonical Souffle programs from
#                                 # tools/benchmark/souffle-programs/
#   --baseline=interpreter,souffle  # both, side-by-side in the CSV
FRESH=0
BASELINES="interpreter"
POSITIONAL_ARGS=()
while (( $# )); do
    case "$1" in
        --fresh)            FRESH=1; shift ;;
        --baseline=*)       BASELINES="${1#--baseline=}"; shift ;;
        --baseline)         BASELINES="$2"; shift 2 ;;
        --)                 shift; POSITIONAL_ARGS+=("$@"); break ;;
        *)                  POSITIONAL_ARGS+=("$1"); shift ;;
    esac
done

# Normalise the baseline list once.
RUN_INTERPRETER=0
RUN_SOUFFLE=0
case ",$BASELINES," in *,interpreter,*) RUN_INTERPRETER=1 ;; esac
case ",$BASELINES," in *,souffle,*)     RUN_SOUFFLE=1 ;; esac
[[ $RUN_INTERPRETER -eq 0 && $RUN_SOUFFLE -eq 0 ]] && \
    die "--baseline must include 'interpreter' and/or 'souffle' (got: $BASELINES)"

CONFIG_FILE="${POSITIONAL_ARGS[0]:-${ROOT_DIR}/tools/benchmark/config.txt}"
PROG_DIR="${ROOT_DIR}/example"
FACT_DIR="${ROOT_DIR}/facts"
LOG_DIR="${ROOT_DIR}/result/benchmark"
# WORKERS is the thread count passed to EVERY engine in this run
# (interpreter --workers, compiler -w, library WORKERS env, souffle -j).
# Default = min(64, nproc):
#   - Caps at 64 to match the VLDB paper rig (cloudlab c6525, 64 cores)
#     so cross-machine numbers stay comparable on hosts that have at
#     least 64 cores.
#   - Auto-shrinks on smaller hardware so a 16-core laptop doesn't
#     context-switch through a 64-thread storm.
# Override (e.g. when co-running with an agent that needs cores):
#   WORKERS=48 bash compare.sh
# Just keep it the same value across runs you compare.
_DEFAULT_WORKERS=$(( $(nproc) < 64 ? $(nproc) : 64 ))
WORKERS="${WORKERS:-$_DEFAULT_WORKERS}"

# Interpreter repo (vldb26-artifact) is expected next to this repo.
INTERPRETER_DIR="${ROOT_DIR}/../vldb26-artifact"
INTERPRETER_BIN="${INTERPRETER_DIR}/target/release/executing"
INTERPRETER_PROG_DIR="${INTERPRETER_DIR}/test/correctness_test/program/flowlog"
INTERPRETER_PROG_URL="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/program/flowlog_interpreter"

# Compiler binary built from this repo.
COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"

# Library-mode runner crate + built binary. Built once per (prog, dataset)
# pair, then run NUM_RUNS times identically to the compiler path.
LIB_BENCH_RUNNER_DIR="${ROOT_DIR}/target/bench-lib/runner"
LIB_BENCH_BIN="${LIB_BENCH_RUNNER_DIR}/target/release/flowlog_bench_lib"

# Souffle baseline (--baseline=souffle). The canonical .dl programs come
# from the FlowLog-Reproduction repo; we keep a vendored copy under
# tools/benchmark/souffle-programs/ so this script is hermetic.
# Souffle is invoked at run-time (compile + execute) — unlike L2's
# correctness oracle which only diffs against pre-baked tarballs.
SOUFFLE_BIN="${SOUFFLE_BIN:-/usr/bin/souffle}"
SOUFFLE_PROG_DIR="${ROOT_DIR}/tools/benchmark/souffle-programs"

DATASET_URL="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/dataset/csv"
NUM_RUNS="${NUM_RUNS:-5}"

CSV_FILE="${LOG_DIR}/comparison_results.csv"

# Synthesis helpers for the lib runner crate. Self-contained under tools/ —
# intentionally does not share code with tests/lib_runner_synth.sh.
source "$(dirname "$0")/lib_runner.sh"

############################################################
# STRING HELPERS
############################################################

# Strip leading and trailing whitespace from a string.
trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

# Parse a config line "prog = dataset [tag tag ...]" and set
# PROG_NAME / DATASET_NAME / PAIR_TAGS. Returns 1 when the line should
# be skipped (blank, comment, test.dl).
#
# Per-pair tags follow the dataset in square brackets; multiple tags
# separated by whitespace. Recognised tags:
#   [interp:skip]     — skip the interpreter run for this pair (used for
#                       pairs the vldb26 interpreter can't or won't run:
#                       OOM on huge graphs, missing arithmetic-head
#                       support, …). The CSV records "N/A" for the
#                       interpreter columns; compiler/lib still run.
#   [souffle:skip]    — skip the Souffle run for this pair (only
#                       relevant when --baseline=souffle).
#
# Tags are stored in PAIR_TAGS as a single space-separated string;
# helpers `pair_has_tag <tag>` test for membership.
parse_config_line() {
    local raw="$1"

    # Strip inline comments and trim whitespace.
    local line="${raw%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && return 1

    # Split off any "[tag] [tag] ..." suffix from the dataset side.
    PAIR_TAGS=""
    if [[ "$line" =~ ^(.*[^[:space:]])[[:space:]]+(\[.*\])[[:space:]]*$ ]]; then
        line="${BASH_REMATCH[1]}"
        PAIR_TAGS="${BASH_REMATCH[2]}"
    fi

    IFS='=' read -r PROG_NAME DATASET_NAME <<< "$line"
    PROG_NAME="$(trim "${PROG_NAME:-}")"
    DATASET_NAME="$(trim "${DATASET_NAME:-}")"

    [[ -z "$PROG_NAME" || -z "$DATASET_NAME" ]] && return 1
    [[ "$PROG_NAME" == "test.dl" ]]              && return 1

    return 0
}

# Test whether the current PAIR_TAGS includes <tag>.
pair_has_tag() {
    [[ "${PAIR_TAGS:-}" == *"[$1]"* ]]
}

############################################################
# DATASET MANAGEMENT
############################################################

# Download and extract a dataset into FACT_DIR if not already present.
setup_dataset() {
    local name="$1"
    local zip="/dev/shm/${name}.zip"
    local dir="${FACT_DIR}/${name}"

    if [[ -d "$dir" ]]; then
        log "$GREEN" "FOUND" "Dataset $name"
        return
    fi

    mkdir -p "$FACT_DIR"

    log "$CYAN" "DOWNLOAD" "${name}.zip -> /dev/shm (tmpfs)"
    wget -q -O "$zip" "${DATASET_URL}/${name}.zip" \
        || die "Download failed: $name"

    log "$YELLOW" "EXTRACT" "$name"
    unzip -q "$zip" -d "$FACT_DIR" || die "Extract failed: $name"

    rm -f "$zip"
    log "$GREEN" "CLEANED" "Removed $zip from tmpfs"
}

# Remove dataset files to reclaim disk space after a benchmark pair.
cleanup_dataset() {
    local name="$1"
    log "$YELLOW" "CLEANUP" "$name"
    rm -rf "${FACT_DIR}/${name}"
}

############################################################
# BUILD SETUP
############################################################

# Clone (if needed) and build the interpreter in release mode.
setup_interpreter() {
    log "$BLUE" "SETUP" "Setting up interpreter (vldb26-artifact)"

    if [[ ! -d "$INTERPRETER_DIR" ]]; then
        log "$CYAN" "CLONE" "Cloning vldb26-artifact"
        git clone --depth 1 \
            https://github.com/flowlog-rs/vldb26-artifact.git "$INTERPRETER_DIR" \
            || die "Failed to clone vldb26-artifact"
    else
        log "$GREEN" "FOUND" "vldb26-artifact already cloned"
    fi

    pushd "$INTERPRETER_DIR" >/dev/null
    log "$YELLOW" "BUILD" "Building interpreter (release)"
    cargo build --release 2>&1 | tail -5
    popd >/dev/null

    [[ -x "$INTERPRETER_BIN" ]] || die "Interpreter binary not found: $INTERPRETER_BIN"
    log "$GREEN" "OK" "Interpreter ready"
}

# Build the compiler workspace in release mode.
setup_compiler() {
    log "$BLUE" "SETUP" "Setting up compiler (current repo)"

    pushd "$ROOT_DIR" >/dev/null
    log "$YELLOW" "BUILD" "Building compiler workspace (release)"
    cargo build --release 2>&1 | tail -5
    popd >/dev/null

    [[ -x "$COMPILER_BIN" ]] || die "Compiler binary not found: $COMPILER_BIN"
    log "$GREEN" "OK" "Compiler ready"
}

# Download an interpreter .dl program file if it is not already cached.
download_interpreter_program() {
    local file="$1"
    local path="${INTERPRETER_PROG_DIR}/${file}"

    mkdir -p "$INTERPRETER_PROG_DIR"
    [[ -f "$path" ]] && return

    log "$CYAN" "DOWNLOAD" "Interpreter program: $file"
    wget -q -O "$path" "${INTERPRETER_PROG_URL}/${file}" \
        || die "Download failed: $file"
}

############################################################
# TIME / MEMORY EXTRACTION
############################################################

# /usr/bin/time -v is GNU time. Bash's builtin `time` does NOT support -v;
# we require the binary so the compiler/lib/interp runs can be wrapped
# uniformly to capture peak RSS in addition to wall time.
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
[[ -x "$TIME_BIN" ]] || die "GNU /usr/bin/time not found (apt install time); set TIME_BIN=<path>"

# Extract peak RSS (kibibytes, integer) from a /usr/bin/time -v sidecar
# log. Returns "N/A" when the file is missing or doesn't contain the line.
_extract_peak_rss_kb() {
    local rss_file="$1"
    [[ -f "$rss_file" ]] || { echo "N/A"; return; }
    local val
    val=$(awk '/Maximum resident set size/ {print $NF; exit}' "$rss_file" 2>/dev/null) || true
    [[ "$val" =~ ^[0-9]+$ ]] && echo "$val" || echo "N/A"
}

# Extract the last timestamp matching PATTERN from a log file (in seconds).
# Handles both "12.345s" and "621.15ms" formats.  Returns "N/A" on failure.
_extract_time_for_pattern() {
    local log_file="$1" pattern="$2"

    [[ -f "$log_file" ]] || { echo "N/A"; return; }

    local time_line
    time_line=$(grep "$pattern" "$log_file" 2>/dev/null | tail -1) || true
    [[ -z "$time_line" ]] && { echo "N/A"; return; }

    # Try seconds first (e.g. "12.777558167s").
    local extracted=""
    extracted=$(echo "$time_line" \
        | grep -oE '[0-9]+\.[0-9]+s' | head -1 | sed 's/s$//' 2>/dev/null) || true

    # Fall back to milliseconds (e.g. "621.153479ms") and convert.
    if [[ -z "$extracted" ]]; then
        local ms_val
        ms_val=$(echo "$time_line" \
            | grep -oE '[0-9]+\.[0-9]+ms' | head -1 | sed 's/ms$//' 2>/dev/null) || true
        if [[ -n "$ms_val" ]]; then
            extracted=$(python3 -c "print(f'{${ms_val}/1000:.9f}')" 2>/dev/null) || true
        fi
    fi

    # Fall back to microseconds (e.g. "17.804µs") and convert.
    if [[ -z "$extracted" ]]; then
        local us_val
        us_val=$(echo "$time_line" \
            | grep -oE '[0-9]+\.[0-9]+µs' | head -1 | sed 's/µs$//' 2>/dev/null) || true
        if [[ -n "$us_val" ]]; then
            extracted=$(python3 -c "print(f'{${us_val}/1000000:.9f}')" 2>/dev/null) || true
        fi
    fi

    echo "${extracted:-N/A}"
}

# Total time: the "Dataflow executed" line.
extract_total_time() { _extract_time_for_pattern "$1" "Dataflow executed"; }

# Load time: the last "Data loaded for" line (all relations loaded by then).
extract_load_time() { _extract_time_for_pattern "$1" "Data loaded for"; }

# Execute time = total - load.
compute_exec_time() {
    local total="$1" load="$2"
    if [[ "$total" =~ ^[0-9] ]] && [[ "$load" =~ ^[0-9] ]]; then
        python3 -c "print(f'{max(${total}-${load},0):.9f}')" 2>/dev/null || echo "N/A"
    else
        echo "N/A"
    fi
}

############################################################
# FORMATTING HELPERS
############################################################

# Right-align a time value for table display (15 chars).
fmt_time() {
    local t="$1"
    if [[ "$t" =~ ^[0-9] ]]; then
        printf "%13.6f" "$t"
    else
        printf "%13s" "$t"
    fi
}

# Compute and format a speedup ratio (e.g. "2.34x").
fmt_speedup() {
    local t1="$1" t2="$2"
    if [[ "$t1" =~ ^[0-9] ]] && [[ "$t2" =~ ^[0-9] ]]; then
        python3 -c "print(f'{${t1}/${t2}:.2f}x') if ${t2}>0 else print('N/A')" \
            2>/dev/null || echo "N/A"
    else
        echo "N/A"
    fi
}

fmt_speedup_cell() {
    local s="$1"
    printf "%11s" "$s"
}

# Compute a raw speedup number for CSV (no trailing "x").
raw_speedup() {
    local t1="$1" t2="$2"
    if [[ "$t1" =~ ^[0-9] ]] && [[ "$t2" =~ ^[0-9] ]]; then
        python3 -c "print(f'{${t1}/${t2}:.6f}') if ${t2}>0 else print('')" \
            2>/dev/null || echo ""
    else
        echo ""
    fi
}

# Given space-separated "time:logpath" pairs, return the median entry.
pick_median() {
    local entries="$1"
    python3 -c "
pairs = '${entries}'.split()
pairs.sort(key=lambda x: float(x.split(':')[0]))
print(pairs[len(pairs) // 2])
" 2>/dev/null
}

# Given space-separated kibibyte values, return the median (integer) or "N/A".
# RSS distribution is independent from time; we take the median over peaks
# rather than tracking the peak that came from the median-time run.
pick_median_rss() {
    local values="$1"
    [[ -z "$values" ]] && { echo "N/A"; return; }
    python3 -c "
xs = sorted(int(x) for x in '${values}'.split() if x.isdigit())
if not xs:
    print('N/A')
else:
    n = len(xs)
    print(xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) // 2)
"
}

# Convert kibibytes -> mebibytes (rounded to 2 decimals) for CSV/table.
fmt_rss_mb() {
    local kb="$1"
    if [[ "$kb" =~ ^[0-9]+$ ]]; then
        python3 -c "print(f'{${kb}/1024:.2f}')"
    else
        echo "N/A"
    fi
}

# Collect all timing metrics for a single log file.
# Prints a space-separated triple: "total load exec".
collect_times() {
    local log_file="$1"
    local total load exec_t
    total=$(extract_total_time "$log_file")
    load=$(extract_load_time "$log_file")
    exec_t=$(compute_exec_time "$total" "$load")
    echo "$total $load $exec_t"
}

# Print a per-pair summary block to the console.
print_pair_summary() {
    local label="$1" interp_log="$2" comp_log="$3" lib_log="$4" sf_log="${5:-}"

    read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
    read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"
    local l_exec
    l_exec=$(extract_total_time "$lib_log")

    local i_rss_mb c_rss_mb l_rss_mb
    i_rss_mb=$(fmt_rss_mb "$(cat "${interp_log}.median_rss_kb" 2>/dev/null || echo)")
    c_rss_mb=$(fmt_rss_mb "$(cat "${comp_log}.median_rss_kb"   2>/dev/null || echo)")
    l_rss_mb=$(fmt_rss_mb "$(cat "${lib_log}.median_rss_kb"    2>/dev/null || echo)")

    local sf_total="" sf_rss_mb=""
    if [[ -n "$sf_log" && -s "${sf_log}.median_total_s" ]]; then
        sf_total=$(cat "${sf_log}.median_total_s")
        sf_rss_mb=$(fmt_rss_mb "$(cat "${sf_log}.median_rss_kb" 2>/dev/null || echo)")
    fi

    echo "----------------------------------------"
    log "$GREEN" "RESULT" "$label"
    log "$GREEN" "  LOAD" \
        "Interpreter=${i_load}s  Compiler=${c_load}s  Speedup=$(fmt_speedup "$i_load" "$c_load")"
    log "$GREEN" "  EXEC" \
        "Interpreter=${i_exec}s  Compiler=${c_exec}s  Lib=${l_exec}s  Lib/Compiler=$(fmt_speedup "$c_exec" "$l_exec")"
    log "$GREEN" " TOTAL" \
        "Interpreter=${i_total}s  Compiler=${c_total}s  Speedup=$(fmt_speedup "$i_total" "$c_total")"
    log "$GREEN" "   MEM" \
        "Interpreter=${i_rss_mb}MB  Compiler=${c_rss_mb}MB  Lib=${l_rss_mb}MB  Lib/Compiler=$(fmt_speedup "$c_rss_mb" "$l_rss_mb")"
    if [[ -n "$sf_total" ]]; then
        log "$GREEN" "SOUFFLE" \
            "Total=${sf_total}s  PeakRss=${sf_rss_mb}MB  Souffle/Compiler=$(fmt_speedup "$sf_total" "$c_total")"
    fi
    echo "----------------------------------------"
}

############################################################
# BENCHMARK RUNNERS
############################################################

# Run the interpreter NUM_RUNS times and keep the median log.
run_interpreter() {
    local prog_name="$1" dataset_name="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local stem="${prog_file%.*}"

    download_interpreter_program "$prog_file"

    local prog_path="${INTERPRETER_PROG_DIR}/${prog_file}"
    local fact_path="${FACT_DIR}/${dataset_name}"
    local best_log="${LOG_DIR}/${stem}_${dataset_name}_interpreter.log"

    log "$BLUE" "RUN" \
        "Interpreter: $prog_file + $dataset_name (no optimisation, w=$WORKERS, runs=$NUM_RUNS)"
    mkdir -p "$LOG_DIR"

    local entries=""
    local rss_values=""
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_interpreter_run${run}.log"
        local rss_log="${run_log}.rss"

        log "$YELLOW" "RUN" "  Interpreter attempt $run/$NUM_RUNS"
        RUST_LOG=info "$TIME_BIN" -v -o "$rss_log" \
            "$INTERPRETER_BIN" \
            --program "$prog_path" \
            --facts "$fact_path" \
            --workers "$WORKERS" \
            > "$run_log" 2>&1 || {
                log "$RED" "WARN" \
                    "Interpreter run $run failed for $prog_file + $dataset_name (see $run_log)"
                continue
            }

        local t r
        t=$(extract_total_time "$run_log")
        r=$(_extract_peak_rss_kb "$rss_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s, peak ${r} KiB"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
        [[ "$r" =~ ^[0-9] ]] && rss_values="${rss_values:+$rss_values }${r}"
    done

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log median_rss
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        median_rss=$(pick_median_rss "$rss_values")
        cp "$median_log" "$best_log"
        cp "${median_log}.rss" "${best_log}.rss" 2>/dev/null || true
        echo "$median_rss" > "${best_log}.median_rss_kb"
        log "$GREEN" "DONE" \
            "Interpreter: $prog_file + $dataset_name (median: ${median_time}s, peak ${median_rss} KiB)"
    else
        log "$RED" "WARN" \
            "Interpreter: all $NUM_RUNS runs failed for $prog_file + $dataset_name"
        return 1
    fi
}

# Run the compiler NUM_RUNS times (batch mode, no SIP/opt) and keep the median log.
run_compiler() {
    local prog_name="$1" dataset_name="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local stem="${prog_file%.*}"

    local prog_path="${PROG_DIR}/${prog_name}"
    [[ -f "$prog_path" ]] || die "Compiler program not found: $prog_path"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local binary="${ROOT_DIR}/bench_${stem}_${dataset_name}"
    local best_log="${LOG_DIR}/${stem}_${dataset_name}_compiler.log"

    log "$BLUE" "RUN" \
        "Compiler:  $prog_file + $dataset_name (batch, w=$WORKERS, runs=$NUM_RUNS)"
    mkdir -p "$LOG_DIR"

    # Compile .dl -> standalone executable (once).
    rm -f "$binary"
    "$COMPILER_BIN" "$prog_path" \
        -F "$dataset_path" \
        -o "$binary" \
        --mode datalog-batch \
        || die "Compilation failed for $prog_file"
    [[ -x "$binary" ]] || die "Binary not found: $binary"

    # Run N times.
    local entries=""
    local rss_values=""
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_compiler_run${run}.log"
        local rss_log="${run_log}.rss"

        log "$YELLOW" "RUN" "  Compiler attempt $run/$NUM_RUNS"
        "$TIME_BIN" -v -o "$rss_log" "$binary" -w "$WORKERS" > "$run_log" 2>&1 || {
            log "$RED" "WARN" \
                "Compiler run $run failed for $prog_file + $dataset_name (see $run_log)"
            continue
        }

        local t r
        t=$(extract_total_time "$run_log")
        r=$(_extract_peak_rss_kb "$rss_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s, peak ${r} KiB"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
        [[ "$r" =~ ^[0-9] ]] && rss_values="${rss_values:+$rss_values }${r}"
    done

    # Clean up the binary.
    rm -f "$binary"

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log median_rss
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        median_rss=$(pick_median_rss "$rss_values")
        cp "$median_log" "$best_log"
        cp "${median_log}.rss" "${best_log}.rss" 2>/dev/null || true
        echo "$median_rss" > "${best_log}.median_rss_kb"
        # Cheap cross-validation hook: extract per-relation sizes from
        # the median log (FlowLog compiler emits "[size][<rel>] t=() size=N")
        # so they can be diff'd against Souffle's own .printsize output.
        grep -oE '\[size\]\[[^]]+\] t=\(\) size=[0-9]+' "$median_log" 2>/dev/null \
            | sed -E 's/^\[size\]\[([^]]+)\] t=\(\) size=([0-9]+)$/\1\t\2/' \
            > "${best_log}.sizes" 2>/dev/null
        log "$GREEN" "DONE" \
            "Compiler:  $prog_file + $dataset_name (median: ${median_time}s, peak ${median_rss} KiB)"
    else
        log "$RED" "WARN" \
            "Compiler: all $NUM_RUNS runs failed for $prog_file + $dataset_name"
        return 1
    fi
}

# Build a minimal lib runner crate once to warm the cargo cache. Real
# per-pair builds reuse this crate and only pay for the `program.dl`-driven
# codegen + one link.
setup_lib_runner() {
    log "$BLUE" "SETUP" "Setting up lib runner crate at $LIB_BENCH_RUNNER_DIR"

    bench_lib_ensure_crate

    # Warm-up program: trivial reach so the crate builds end-to-end.
    cat > "${LIB_BENCH_RUNNER_DIR}/program.dl" <<'EOF'
.decl Edge(x: int32, y: int32)
.input Edge()
.decl Reach(x: int32, y: int32)
Reach(x, y) :- Edge(x, y).
Reach(x, y) :- Reach(x, z), Edge(z, y).
.output Reach
EOF
    LIB_BENCH_SIP=0 LIB_BENCH_STR_INTERN=0 bench_lib_write_build_rs
    cat > "${LIB_BENCH_RUNNER_DIR}/src/main.rs" <<'EOF'
pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}
fn main() {}
EOF
    log "$YELLOW" "BUILD" "Warming lib runner crate (release)"
    (cd "$LIB_BENCH_RUNNER_DIR" && cargo build --release --quiet 2>&1 | tail -5) \
        || die "Lib runner warm-up failed"
    log "$GREEN" "OK" "Lib runner ready"
}

# Run the library path NUM_RUNS times (batch mode) and keep the median log.
#
# Build happens once per pair: we stage program.dl + synthesize build.rs and
# main.rs, then `cargo build --release` rebuilds flowlog_bench_lib with the
# per-program codegen. Subsequent runs just re-exec the same binary.
run_lib() {
    local prog_name="$1" dataset_name="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local stem="${prog_file%.*}"

    local prog_path="${PROG_DIR}/${prog_name}"
    [[ -f "$prog_path" ]] || die "Lib program not found: $prog_path"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local best_log="${LOG_DIR}/${stem}_${dataset_name}_lib.log"

    log "$BLUE" "RUN" \
        "Lib:       $prog_file + $dataset_name (batch, w=$WORKERS, runs=$NUM_RUNS)"
    mkdir -p "$LOG_DIR"

    # Discover input-relation → CSV mapping (case-insensitive).
    local pairs
    pairs=$(bench_lib_discover_csvs "$prog_path" "$dataset_path")
    [[ -n "$pairs" ]] || die "No CSVs discovered for $prog_file under $dataset_path"

    # Build env var exports: FLOWLOG_CSV_<REL>=<abspath> (upper-cased).
    local -a csv_envs=()
    local line rel csv_abs env_name
    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        rel="${line%%=*}"
        csv_abs="${line#*=}"
        env_name="FLOWLOG_CSV_${rel^^}"
        csv_envs+=("${env_name}=${csv_abs}")
    done <<< "$pairs"

    # Stage program.dl as-is. We deliberately do NOT rewrite .printsize →
    # .output: compare.sh's compiler path runs the program unchanged, so
    # rewriting here would force lib to materialize full output Vecs
    # (Tc, Reach, …) while the compiler only updates a size counter —
    # that's a huge dataflow workload difference, not a runtime gap.
    local prepared_dl="${LIB_BENCH_RUNNER_DIR}/program.dl"
    cp "$prog_path" "$prepared_dl"

    # No string_intern / sip: matches how compare.sh runs the compiler
    # (all current benchmark programs are integer-typed — see config.txt).
    LIB_BENCH_SIP=0 LIB_BENCH_STR_INTERN=0 bench_lib_write_build_rs

    # Synthesize main.rs with one loader per input relation.
    local pairs_space
    pairs_space="$(echo "$pairs" | tr '\n' ' ')"
    bench_lib_write_main_rs "$prepared_dl" "$pairs_space" \
        || die "main.rs synthesis failed for $prog_file"

    # Single build — recompiles build.rs output + main.rs.
    log "$YELLOW" "BUILD" "  Lib: cargo build --release"
    (cd "$LIB_BENCH_RUNNER_DIR" && cargo build --release --quiet) \
        || die "Lib build failed for $prog_file"
    [[ -x "$LIB_BENCH_BIN" ]] || die "Lib bench binary not found: $LIB_BENCH_BIN"

    # Run N times with CSV paths + WORKERS in the environment.
    local entries=""
    local rss_values=""
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_lib_run${run}.log"
        local rss_log="${run_log}.rss"

        log "$YELLOW" "RUN" "  Lib attempt $run/$NUM_RUNS"
        env "${csv_envs[@]}" WORKERS="$WORKERS" \
            "$TIME_BIN" -v -o "$rss_log" "$LIB_BENCH_BIN" \
            > "$run_log" 2>&1 || {
                log "$RED" "WARN" \
                    "Lib run $run failed for $prog_file + $dataset_name (see $run_log)"
                continue
            }

        local t r
        t=$(extract_total_time "$run_log")
        r=$(_extract_peak_rss_kb "$rss_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s, peak ${r} KiB"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
        [[ "$r" =~ ^[0-9] ]] && rss_values="${rss_values:+$rss_values }${r}"
    done

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log median_rss
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        median_rss=$(pick_median_rss "$rss_values")
        cp "$median_log" "$best_log"
        cp "${median_log}.rss" "${best_log}.rss" 2>/dev/null || true
        echo "$median_rss" > "${best_log}.median_rss_kb"
        log "$GREEN" "DONE" \
            "Lib:       $prog_file + $dataset_name (median: ${median_time}s, peak ${median_rss} KiB)"
    else
        log "$RED" "WARN" \
            "Lib: all $NUM_RUNS runs failed for $prog_file + $dataset_name"
        return 1
    fi
}

############################################################
# SOUFFLE BASELINE
############################################################

# Sanity-check Souffle binary + that we have a canonical .dl program for
# every pair we'd run with --baseline=souffle. Runs once at startup.
setup_souffle() {
    [[ -x "$SOUFFLE_BIN" ]] || die "Souffle binary not found at $SOUFFLE_BIN (apt install souffle, or set SOUFFLE_BIN)"
    [[ -d "$SOUFFLE_PROG_DIR" ]] || die "Souffle program dir not found: $SOUFFLE_PROG_DIR"
    log "$BLUE" "SETUP" "Souffle: $($SOUFFLE_BIN --version 2>&1 | head -1)"
    mkdir -p "${LOG_DIR}/sf-bin"
}

# Run Souffle on (prog, dataset) NUM_RUNS times.
#
# Souffle has TWO execution modes:
#   1. INTERPRETED — `souffle prog.dl -F facts -D out` walks the AST.
#      The `-j N` flag is accepted but does NOT enable runtime
#      parallelism; the interpreter is effectively single-threaded.
#   2. COMPILED — `souffle -c -j N -F facts -o bin prog.dl` generates
#      and compiles a parallel C++ executable; -j N at codegen is
#      what wires the parallelism in. The resulting binary is then
#      run with `bin -F facts -D out -j N` for true parallelism.
#
# Mode 2 is what the FlowLog VLDB paper / FlowLog-Reproduction
# benchmark uses, and it's the only fair comparison against FlowLog's
# Differential Dataflow runtime. We use it here:
#
#   - One compile per program (cached at $sf_bin so we don't rebuild
#     for repeated runs or for the same program across multiple
#     datasets). Compile time is NOT timed (it's a one-off).
#   - Each timed run wraps `bin -F <facts> -D <out> -j $WORKERS`
#     in /usr/bin/time -v for wall-time + peak RSS.
run_souffle() {
    local prog_name="$1" dataset_name="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local stem="${prog_file%.*}"

    local sf_src="${SOUFFLE_PROG_DIR}/${stem}.dl"
    local fact_path="${FACT_DIR}/${dataset_name}"
    local best_log="${LOG_DIR}/${stem}_${dataset_name}_souffle.log"
    local sf_bin="${LOG_DIR}/sf-bin/${stem}"

    if [[ ! -f "$sf_src" ]]; then
        log "$YELLOW" "WARN" \
            "Souffle: no canonical .dl for $stem at $sf_src — recording N/A"
        rm -f "${best_log}.median_rss_kb" "${best_log}.median_total_s"
        : > "$best_log"
        return 1
    fi

    # Compile once per program (cached). The -j flag at compile time
    # is what wires Souffle's parallelism into the generated C++;
    # passing it again at run-time is the standard idiom and matches
    # the paper. Souffle's compile step also validates `.input`
    # directives by trying to load the dataset, so -F has to be
    # present at compile time too — we use the dataset for this pair
    # as the schema sample.
    if [[ ! -x "$sf_bin" ]]; then
        log "$BLUE" "BUILD" \
            "Souffle: compiling $stem with -j $WORKERS (one-off)"
        mkdir -p "$(dirname "$sf_bin")"
        if ! "$SOUFFLE_BIN" -c -j "$WORKERS" -F "$fact_path" -o "$sf_bin" "$sf_src" \
                > "${sf_bin}.compile.log" 2>&1; then
            log "$RED" "WARN" \
                "Souffle: -c compile failed for $stem (see ${sf_bin}.compile.log) — recording N/A"
            rm -f "${best_log}.median_rss_kb" "${best_log}.median_total_s"
            : > "$best_log"
            return 1
        fi
    fi

    log "$BLUE" "RUN" \
        "Souffle:   $prog_file + $dataset_name (compiled, w=$WORKERS, runs=$NUM_RUNS)"
    mkdir -p "$LOG_DIR"

    local entries=""
    local rss_values=""
    local sizes_sidecar="${best_log}.sizes"
    : > "$sizes_sidecar"
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_souffle_run${run}.log"
        local rss_log="${run_log}.rss"
        local out_dir="${LOG_DIR}/sf_${stem}_${dataset_name}_run${run}"
        mkdir -p "$out_dir"

        log "$YELLOW" "RUN" "  Souffle attempt $run/$NUM_RUNS"
        local t_start t_end
        t_start=$(date +%s.%N)
        "$TIME_BIN" -v -o "$rss_log" \
            "$sf_bin" -F "$fact_path" -D "$out_dir" -j "$WORKERS" \
            > "$run_log" 2>&1 || {
                log "$RED" "WARN" \
                    "Souffle run $run failed for $prog_file + $dataset_name (see $run_log)"
                rm -rf "$out_dir"
                continue
            }
        t_end=$(date +%s.%N)

        # Cheap cross-validation hook: record one row per output relation
        # ("<lowercased_name>\t<count>") to the sizes sidecar while we
        # still have the produced .csv files in $out_dir. Populated on
        # the first successful run only — re-runs are deterministic.
        # `.printsize` relations don't write a .csv in souffle; pick
        # them up from "Relation\tN" lines in the run log.
        if [[ ! -s "$sizes_sidecar" ]]; then
            for csv in "$out_dir"/*.csv; do
                [[ -f "$csv" ]] || continue
                local rel=$(basename "$csv" .csv)
                local rows=$(wc -l < "$csv")
                printf '%s\t%s\n' "${rel,,}" "$rows" >> "$sizes_sidecar"
            done
            grep -E '^[A-Za-z][A-Za-z0-9_]*\s+[0-9]+$' "$run_log" 2>/dev/null \
                | awk -v IGNORECASE=1 '{ printf "%s\t%s\n", tolower($1), $2 }' \
                >> "$sizes_sidecar"
            sort -u -k1,1 -o "$sizes_sidecar" "$sizes_sidecar" 2>/dev/null || true
        fi

        rm -rf "$out_dir"

        local t r
        t=$(python3 -c "print(f'{${t_end}-${t_start}:.9f}')")
        r=$(_extract_peak_rss_kb "$rss_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s, peak ${r} KiB"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
        [[ "$r" =~ ^[0-9] ]] && rss_values="${rss_values:+$rss_values }${r}"
    done

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log median_rss
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        median_rss=$(pick_median_rss "$rss_values")
        cp "$median_log" "$best_log"
        cp "${median_log}.rss" "${best_log}.rss" 2>/dev/null || true
        echo "$median_rss" > "${best_log}.median_rss_kb"
        echo "$median_time" > "${best_log}.median_total_s"
        log "$GREEN" "DONE" \
            "Souffle:   $prog_file + $dataset_name (median: ${median_time}s, peak ${median_rss} KiB)"
    else
        log "$RED" "WARN" \
            "Souffle: all $NUM_RUNS runs failed for $prog_file + $dataset_name"
        rm -f "${best_log}.median_rss_kb" "${best_log}.median_total_s"
        : > "$best_log"
        return 1
    fi
}

############################################################
# RESULT SUMMARY
############################################################

# Initialise the CSV file with a header row.
#
# Columns are grouped by stage:
#   timing  : *_Load / *_Exec / *_Total / Load_Speedup / Exec_Speedup / Total_Speedup
#   library : Lib_Exec / Lib_vs_Interp_Exec / Lib_vs_Compiler_Exec
#   memory  : *_PeakRss_MB (peak RSS in MiB, median over NUM_RUNS;
#             N/A if /usr/bin/time -v emitted no value) and
#             Lib_vs_Compiler_Mem (compiler/lib ratio).
#
# Memory columns sit at the end so existing CSV consumers (groomer carve /
# dashboards) keep working unchanged when they ignore unknown trailing columns.
CSV_HEADER="Program,Dataset,Interp_Load,Compiler_Load,Load_Speedup,Interp_Exec,Compiler_Exec,Exec_Speedup,Interp_Total,Compiler_Total,Total_Speedup,Lib_Exec,Lib_vs_Interp_Exec,Lib_vs_Compiler_Exec,Interp_PeakRss_MB,Compiler_PeakRss_MB,Lib_PeakRss_MB,Lib_vs_Compiler_Mem,Souffle_Total,Souffle_PeakRss_MB,Souffle_vs_Compiler_Total,Crosscheck_Souffle"

# Cross-validate compiler-vs-Souffle row counts.
#
# Both engines write a `<best_log>.sizes` sidecar with one
# `<lowercased_relation>\t<count>` line per output relation; FlowLog's
# is parsed from "[size][rel] t=() size=N" log lines (run_compiler),
# Souffle's from the per-relation .csv files plus .printsize lines in
# its run log (run_souffle).
#
# A pair passes when, for every relation present in BOTH sidecars, the
# counts match. Returns one of:
#   "match"    — every shared relation has equal counts
#   "MISMATCH:<rel>=<flowlog>vs<souffle>(+more)" — first divergence(s)
#   "n/a"      — one or both sidecars empty (e.g. --baseline did not
#                include souffle, or the program lacks a canonical
#                Souffle .dl). The cell is the literal string "n/a".
crosscheck_compiler_vs_souffle() {
    local comp_sizes="$1" sf_sizes="$2"
    [[ -s "$comp_sizes" && -s "$sf_sizes" ]] || { echo "n/a"; return; }
    python3 - "$comp_sizes" "$sf_sizes" <<'PY'
import sys
def load(path):
    out = {}
    for line in open(path):
        parts = line.strip().split()
        if len(parts) >= 2 and parts[1].isdigit():
            out[parts[0].lower()] = int(parts[1])
    return out
fl, sf = load(sys.argv[1]), load(sys.argv[2])
shared = set(fl) & set(sf)
if not shared:
    print("n/a")
    sys.exit(0)
mismatches = [(r, fl[r], sf[r]) for r in sorted(shared) if fl[r] != sf[r]]
if not mismatches:
    print(f"match({len(shared)})")
else:
    head = ";".join(f"{r}={fl[r]}vs{sf[r]}" for r, _, _ in mismatches[:3])
    suffix = f"+{len(mismatches)-3}more" if len(mismatches) > 3 else ""
    print(f"MISMATCH:{head}{suffix}")
PY
}

# Write the CSV header if the file is missing or empty. Preserves existing
# rows so a killed run can resume without losing completed pairs. Lib only
# has a single "exec" number (no load phase measured); we surface it
# alongside the compiler exec for direct comparison.
init_csv() {
    mkdir -p "$(dirname "$CSV_FILE")"
    if [[ ! -s "$CSV_FILE" ]]; then
        echo "$CSV_HEADER" > "$CSV_FILE"
    fi
}

# Return 0 (true) if (program_stem, dataset) is already in the CSV.
# Uses a literal-match grep against the leading `stem,dataset,` prefix so
# program names containing regex metacharacters are handled correctly.
pair_already_done() {
    local stem="$1" dataset="$2"
    [[ -f "$CSV_FILE" ]] || return 1
    grep -Fq -- "${stem},${dataset}," "$CSV_FILE"
}

# Append one benchmark pair's results to the CSV (called after each pair).
append_csv_row() {
    local stem="$1" dataset="$2" interp_log="$3" comp_log="$4" lib_log="$5" sf_log="$6"

    read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
    read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"

    # Lib runner prints only a single "Dataflow executed" line — its value
    # is already exec-only (no load included).
    local l_exec
    l_exec=$(extract_total_time "$lib_log")

    local rs_load rs_exec rs_total
    rs_load=$(raw_speedup "$i_load" "$c_load")
    rs_exec=$(raw_speedup "$i_exec" "$c_exec")
    rs_total=$(raw_speedup "$i_total" "$c_total")

    local lib_vs_interp lib_vs_comp
    lib_vs_interp=$(raw_speedup "$i_exec" "$l_exec")
    lib_vs_comp=$(raw_speedup   "$c_exec" "$l_exec")

    # Pull the median peak-RSS sidecars (written by run_*) and convert
    # KiB → MiB. raw_speedup is reused as a generic ratio helper for the
    # lib-vs-compiler memory column.
    local i_rss_kb c_rss_kb l_rss_kb
    i_rss_kb=$(cat "${interp_log}.median_rss_kb" 2>/dev/null || echo "N/A")
    c_rss_kb=$(cat "${comp_log}.median_rss_kb"   2>/dev/null || echo "N/A")
    l_rss_kb=$(cat "${lib_log}.median_rss_kb"    2>/dev/null || echo "N/A")
    local i_rss_mb c_rss_mb l_rss_mb lib_vs_comp_mem
    i_rss_mb=$(fmt_rss_mb "$i_rss_kb")
    c_rss_mb=$(fmt_rss_mb "$c_rss_kb")
    l_rss_mb=$(fmt_rss_mb "$l_rss_kb")
    lib_vs_comp_mem=$(raw_speedup "$c_rss_mb" "$l_rss_mb")

    # Souffle baseline (optional; columns are "N/A" when --baseline did
    # not include souffle, or when the program lacks a canonical Souffle
    # equivalent — e.g. cc / sssp).
    local sf_total="N/A" sf_rss_mb="N/A" sf_vs_comp_total="N/A"
    if [[ -n "${sf_log:-}" && -s "${sf_log}.median_total_s" ]]; then
        sf_total=$(cat "${sf_log}.median_total_s")
        local sf_rss_kb=$(cat "${sf_log}.median_rss_kb" 2>/dev/null || echo "N/A")
        sf_rss_mb=$(fmt_rss_mb "$sf_rss_kb")
        sf_vs_comp_total=$(raw_speedup "$sf_total" "$c_total")
    fi

    # Cross-check compiler vs Souffle on per-relation row counts. Free
    # because both engines already wrote a `.sizes` sidecar this pair.
    # Reports "match(N)" when every shared relation agrees, "MISMATCH:..."
    # with the first divergent relations on disagreement, "n/a" when one
    # side is missing (no souffle baseline run, or no canonical .dl).
    local crosscheck="n/a"
    if [[ "$sf_total" != "N/A" ]]; then
        crosscheck=$(crosscheck_compiler_vs_souffle "${comp_log}.sizes" "${sf_log}.sizes")
        if [[ "$crosscheck" == match* ]]; then
            log "$GREEN" "XCHECK" "compiler vs souffle: $crosscheck"
        elif [[ "$crosscheck" == MISMATCH* ]]; then
            log "$RED" "XCHECK" "compiler vs souffle: $crosscheck"
        fi
    fi

    echo "${stem},${dataset},${i_load},${c_load},${rs_load},${i_exec},${c_exec},${rs_exec},${i_total},${c_total},${rs_total},${l_exec},${lib_vs_interp},${lib_vs_comp},${i_rss_mb},${c_rss_mb},${l_rss_mb},${lib_vs_comp_mem},${sf_total},${sf_rss_mb},${sf_vs_comp_total},${crosscheck}" \
        >> "$CSV_FILE"

    log "$GREEN" "CSV" "Appended ${stem}_${dataset} to $CSV_FILE"
}

# Print the final comparison table to the terminal.
generate_results() {
    echo ""
    echo "==================================================================================================================================================="
    log "$BLUE" "SUMMARY" "Version Comparison Results (median of $NUM_RUNS runs)"
    echo "==================================================================================================================================================="
    echo ""

    # Table header. Exec column now carries a third sub-column for lib's
    # dataflow time plus Lib-vs-Compiler speedup.
    printf "| %-40s | %-39s | %-53s | %-39s |\n" \
        "Program-Dataset" "Load time (s)" "Execute time (s) — lib exec included" "Total time (s)"
    printf "| %-40s | %13s %13s %11s | %13s %13s %13s %11s | %13s %13s %11s |\n" \
        "" "Interp" "Compiler" "Speedup" \
        "Interp" "Compiler" "Lib" "Lib/Comp" \
        "Interp" "Compiler" "Speedup"

    printf '%s' "|------------------------------------------|"
    printf '%s' "-----------------------------------------|"
    printf '%s' "-------------------------------------------------------|"
    printf '%s\n' "-----------------------------------------|"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        parse_config_line "$raw_line" || continue

        local prog_base
        prog_base="$(basename "$PROG_NAME")"
        local file_stem="${prog_base%.*}"
        local display_stem="${PROG_NAME%.*}"
        local label="${display_stem}_${DATASET_NAME}"
        local interp_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_interpreter.log"
        local comp_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_compiler.log"
        local lib_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_lib.log"

        read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
        read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"
        local l_exec
        l_exec=$(extract_total_time "$lib_log")

        local spd_load spd_exec_ic spd_exec_lc spd_total
        spd_load=$(fmt_speedup "$i_load" "$c_load")
        spd_exec_ic=$(fmt_speedup "$i_exec" "$c_exec")
        spd_exec_lc=$(fmt_speedup "$c_exec" "$l_exec")
        spd_total=$(fmt_speedup "$i_total" "$c_total")

        printf "| %-40s | %s %s %s | %s %s %s %s | %s %s %s |\n" \
            "$label" \
            "$(fmt_time "$i_load")"  "$(fmt_time "$c_load")"  "$(fmt_speedup_cell "$spd_load")" \
            "$(fmt_time "$i_exec")"  "$(fmt_time "$c_exec")"  "$(fmt_time "$l_exec")" "$(fmt_speedup_cell "$spd_exec_lc")" \
            "$(fmt_time "$i_total")" "$(fmt_time "$c_total")" "$(fmt_speedup_cell "$spd_total")"
    done < "$CONFIG_FILE"

    echo ""
    log "$GREEN" "CSV" "Results saved to: $CSV_FILE"
}

############################################################
# MAIN
############################################################

main() {
    log "$BLUE" "START" "FlowLog Version Comparison Benchmark"
    echo "  Compiler repo : $ROOT_DIR"
    echo "  Interpreter   : $INTERPRETER_DIR  (timed: $RUN_INTERPRETER)"
    echo "  Souffle       : $SOUFFLE_BIN  (timed: $RUN_SOUFFLE)"
    echo "  Config        : $CONFIG_FILE"
    echo "  Workers       : $WORKERS  (applied identically to every engine: interp --workers, compiler -w, lib WORKERS, souffle -j)"
    echo ""

    [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

    # Build all three versions.
    [[ $RUN_INTERPRETER -eq 1 ]] && setup_interpreter
    setup_compiler
    setup_lib_runner
    [[ $RUN_SOUFFLE -eq 1 ]] && setup_souffle

    # With --fresh, wipe logs + CSV. Without, keep both so we can resume.
    if (( FRESH )); then
        rm -rf "$LOG_DIR"
        log "$YELLOW" "FRESH" "Wiped $LOG_DIR (--fresh)"
    fi
    mkdir -p "$LOG_DIR"

    # Initialise CSV (no-op if it already has rows from a prior run).
    init_csv

    # Iterate over every program/dataset pair in the config file.
    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        parse_config_line "$raw_line" || continue

        local _prog_base="$(basename "$PROG_NAME")"
        local _display_stem="${PROG_NAME%.*}"
        if pair_already_done "$_display_stem" "$DATASET_NAME"; then
            log "$GREEN" "SKIP" "$_display_stem + $DATASET_NAME — already in CSV"
            continue
        fi

        echo ""
        echo "========================================"
        log "$CYAN" "BENCH" "$PROG_NAME + $DATASET_NAME${PAIR_TAGS:+  $PAIR_TAGS}"
        echo "========================================"

        setup_dataset "$DATASET_NAME"

        local prog_base
        prog_base="$(basename "$PROG_NAME")"
        local file_stem="${prog_base%.*}"
        local display_stem="${PROG_NAME%.*}"
        local lbl="${display_stem}_${DATASET_NAME}"
        local interp_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_interpreter.log"
        local comp_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_compiler.log"
        local lib_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_lib.log"
        local sf_log="${LOG_DIR}/${file_stem}_${DATASET_NAME}_souffle.log"

        # Make sure stale RSS/total sidecars from a previous run don't
        # leak into this iteration's CSV row.
        rm -f "${interp_log}" "${interp_log}.median_rss_kb" \
              "${sf_log}" "${sf_log}.median_rss_kb" "${sf_log}.median_total_s"

        if [[ $RUN_INTERPRETER -eq 1 ]] && ! pair_has_tag "interp:skip"; then
            run_interpreter "$PROG_NAME" "$DATASET_NAME" || true
        elif pair_has_tag "interp:skip"; then
            log "$YELLOW" "SKIP" "Interpreter: $PROG_NAME + $DATASET_NAME (per [interp:skip] tag)"
        fi

        run_compiler    "$PROG_NAME" "$DATASET_NAME" || true
        run_lib         "$PROG_NAME" "$DATASET_NAME" || true

        if [[ $RUN_SOUFFLE -eq 1 ]] && ! pair_has_tag "souffle:skip"; then
            run_souffle "$PROG_NAME" "$DATASET_NAME" || true
        elif pair_has_tag "souffle:skip"; then
            log "$YELLOW" "SKIP" "Souffle: $PROG_NAME + $DATASET_NAME (per [souffle:skip] tag)"
        fi

        print_pair_summary "$lbl" "$interp_log" "$comp_log" "$lib_log" "$sf_log"

        # Append this pair's results to CSV incrementally.
        append_csv_row "$display_stem" "$DATASET_NAME" "$interp_log" "$comp_log" "$lib_log" "$sf_log"

        # Cleanup dataset to save disk space
        cleanup_dataset "$DATASET_NAME"
    done < "$CONFIG_FILE"

    generate_results
}

main "$@"
