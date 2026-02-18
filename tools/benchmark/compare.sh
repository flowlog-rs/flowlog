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
#   WORKERS  -- number of worker threads (default: 64)
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

CONFIG_FILE="${1:-${ROOT_DIR}/tools/check/config.txt}"
PROG_DIR="${ROOT_DIR}/example"
FACT_DIR="${ROOT_DIR}/facts"
LOG_DIR="${ROOT_DIR}/result/benchmark"
WORKERS="${WORKERS:-64}"

# Interpreter repo (vldb26-artifact) is expected next to this repo.
INTERPRETER_DIR="${ROOT_DIR}/../vldb26-artifact"
INTERPRETER_BIN="${INTERPRETER_DIR}/target/release/executing"
INTERPRETER_PROG_DIR="${INTERPRETER_DIR}/test/correctness_test/program/flowlog"
INTERPRETER_PROG_URL="https://pages.cs.wisc.edu/~m0riarty/program/flowlog"

# Compiler binary built from this repo.
COMPILER_BIN="${ROOT_DIR}/target/release/flowlog"

DATASET_URL="https://pages.cs.wisc.edu/~m0riarty/dataset/csv"
NUM_RUNS=5

CSV_FILE="${LOG_DIR}/comparison_results.csv"

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

# Turn an arbitrary string into a valid Cargo package name
# (lowercase, no special chars, does not start with a digit).
sanitize_package_name() {
    local s
    s="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
    s="$(printf '%s' "$s" | tr -c 'a-z0-9_-' '_')"
    s="$(printf '%s' "$s" | sed 's/_\{2,\}/_/g; s/-\{2,\}/-/g; s/^[-_]//; s/[-_]$//')"
    [[ -z "$s" ]]         && s="flowlog_bench"
    [[ "$s" =~ ^[0-9] ]] && s="flowlog_${s}"
    printf '%s' "$s"
}

# Parse a config line "prog = dataset" and set PROG_NAME / DATASET_NAME.
# Returns 1 when the line should be skipped (blank, comment, test.dl).
parse_config_line() {
    local raw="$1"

    # Strip inline comments and trim whitespace.
    local line="${raw%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && return 1

    IFS='=' read -r PROG_NAME DATASET_NAME <<< "$line"
    PROG_NAME="$(trim "${PROG_NAME:-}")"
    DATASET_NAME="$(trim "${DATASET_NAME:-}")"

    [[ -z "$PROG_NAME" || -z "$DATASET_NAME" ]] && return 1
    [[ "$PROG_NAME" == "test.dl" ]]              && return 1

    return 0
}

############################################################
# DATASET MANAGEMENT
############################################################

# Download and extract a dataset into FACT_DIR if not already present.
setup_dataset() {
    local name="$1"
    local zip="${FACT_DIR}/${name}.zip"
    local dir="${FACT_DIR}/${name}"

    if [[ -d "$dir" ]]; then
        log "$GREEN" "FOUND" "Dataset $name"
        return
    fi

    mkdir -p "$FACT_DIR"

    if [[ ! -f "$zip" ]]; then
        log "$CYAN" "DOWNLOAD" "${name}.zip"
        wget -q -O "$zip" "${DATASET_URL}/${name}.zip" \
            || die "Download failed: $name"
    fi

    log "$YELLOW" "EXTRACT" "$name"
    unzip -q "$zip" -d "$FACT_DIR" || die "Extract failed: $name"
}

# Remove dataset files to reclaim disk space after a benchmark pair.
cleanup_dataset() {
    local name="$1"
    log "$YELLOW" "CLEANUP" "$name"
    rm -rf "${FACT_DIR}/${name}" "${FACT_DIR}/${name}.zip"
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
# TIME EXTRACTION
############################################################

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
    local label="$1" interp_log="$2" comp_log="$3"

    read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
    read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"

    echo "----------------------------------------"
    log "$GREEN" "RESULT" "$label"
    log "$GREEN" "  LOAD" \
        "Interpreter=${i_load}s  Compiler=${c_load}s  Speedup=$(fmt_speedup "$i_load" "$c_load")"
    log "$GREEN" "  EXEC" \
        "Interpreter=${i_exec}s  Compiler=${c_exec}s  Speedup=$(fmt_speedup "$i_exec" "$c_exec")"
    log "$GREEN" " TOTAL" \
        "Interpreter=${i_total}s  Compiler=${c_total}s  Speedup=$(fmt_speedup "$i_total" "$c_total")"
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
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_interpreter_run${run}.log"

        log "$YELLOW" "RUN" "  Interpreter attempt $run/$NUM_RUNS"
        RUST_LOG=info "$INTERPRETER_BIN" \
            --program "$prog_path" \
            --facts "$fact_path" \
            --workers "$WORKERS" \
            > "$run_log" 2>&1 || {
                log "$RED" "WARN" \
                    "Interpreter run $run failed for $prog_file + $dataset_name (see $run_log)"
                continue
            }

        local t
        t=$(extract_total_time "$run_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
    done

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        cp "$median_log" "$best_log"
        log "$GREEN" "DONE" \
            "Interpreter: $prog_file + $dataset_name (median: ${median_time}s)"
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

    local prog_path="${PROG_DIR}/${prog_file}"
    [[ -f "$prog_path" ]] || die "Compiler program not found: $prog_path"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local package_name
    package_name="$(sanitize_package_name "bench_${stem}_${dataset_name}")"
    local project_dir="${ROOT_DIR}/${package_name}"
    local best_log="${LOG_DIR}/${stem}_${dataset_name}_compiler.log"

    log "$BLUE" "RUN" \
        "Compiler:  $prog_file + $dataset_name (batch, w=$WORKERS, runs=$NUM_RUNS)"
    mkdir -p "$LOG_DIR"

    # Generate the Cargo project (once).
    rm -rf "$project_dir"
    "$COMPILER_BIN" "$prog_path" \
        -F "$dataset_path" \
        -o "$project_dir" \
        --mode batch \
        || die "Code generation failed for $prog_file"
    [[ -d "$project_dir" ]] || die "Generated project not found: $project_dir"

    # Build once, then run N times.
    pushd "$project_dir" >/dev/null
    log "$YELLOW" "BUILD" "  Building generated project"
    cargo build --release 2>&1 | tail -3

    local entries=""
    for run in $(seq 1 "$NUM_RUNS"); do
        local run_log="${LOG_DIR}/${stem}_${dataset_name}_compiler_run${run}.log"

        log "$YELLOW" "RUN" "  Compiler attempt $run/$NUM_RUNS"
        cargo run --release -- -w "$WORKERS" > "$run_log" 2>&1 || {
            log "$RED" "WARN" \
                "Compiler run $run failed for $prog_file + $dataset_name (see $run_log)"
            continue
        }

        local t
        t=$(extract_total_time "$run_log")
        log "$YELLOW" "TIME" "  Run $run: ${t}s"

        [[ "$t" =~ ^[0-9] ]] && entries="${entries:+$entries }${t}:${run_log}"
    done
    popd >/dev/null

    # Clean up the generated Cargo project.
    rm -rf "$project_dir"

    if [[ -n "$entries" ]]; then
        local median_entry median_time median_log
        median_entry=$(pick_median "$entries")
        median_time="${median_entry%%:*}"
        median_log="${median_entry#*:}"
        cp "$median_log" "$best_log"
        log "$GREEN" "DONE" \
            "Compiler:  $prog_file + $dataset_name (median: ${median_time}s)"
    else
        log "$RED" "WARN" \
            "Compiler: all $NUM_RUNS runs failed for $prog_file + $dataset_name"
        return 1
    fi
}

############################################################
# RESULT SUMMARY
############################################################

# Initialise the CSV file with a header row.
init_csv() {
    mkdir -p "$(dirname "$CSV_FILE")"
    echo "Program,Dataset,Interp_Load,Compiler_Load,Load_Speedup,Interp_Exec,Compiler_Exec,Exec_Speedup,Interp_Total,Compiler_Total,Total_Speedup" \
        > "$CSV_FILE"
}

# Append one benchmark pair's results to the CSV (called after each pair).
append_csv_row() {
    local stem="$1" dataset="$2" interp_log="$3" comp_log="$4"

    read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
    read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"

    local rs_load rs_exec rs_total
    rs_load=$(raw_speedup "$i_load" "$c_load")
    rs_exec=$(raw_speedup "$i_exec" "$c_exec")
    rs_total=$(raw_speedup "$i_total" "$c_total")

    echo "${stem},${dataset},${i_load},${c_load},${rs_load},${i_exec},${c_exec},${rs_exec},${i_total},${c_total},${rs_total}" \
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

    # Table header.
    printf "| %-25s | %-39s | %-39s | %-39s |\n" \
        "Program-Dataset" "Load time (s)" "Execute time (s)" "Total time (s)"
    printf "| %-25s | %13s %13s %11s | %13s %13s %11s | %13s %13s %11s |\n" \
        "" "Interp" "Compiler" "Speedup" \
        "Interp" "Compiler" "Speedup" \
        "Interp" "Compiler" "Speedup"

    printf '%s' "|---------------------------|"
    printf '%s' "-----------------------------------------|"
    printf '%s' "-----------------------------------------|"
    printf '%s\n' "-----------------------------------------|"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        parse_config_line "$raw_line" || continue

        local stem="${PROG_NAME%.*}"
        local label="${stem}_${DATASET_NAME}"
        local interp_log="${LOG_DIR}/${stem}_${DATASET_NAME}_interpreter.log"
        local comp_log="${LOG_DIR}/${stem}_${DATASET_NAME}_compiler.log"

        read -r i_total i_load i_exec <<< "$(collect_times "$interp_log")"
        read -r c_total c_load c_exec <<< "$(collect_times "$comp_log")"

        local spd_load spd_exec spd_total
        spd_load=$(fmt_speedup "$i_load" "$c_load")
        spd_exec=$(fmt_speedup "$i_exec" "$c_exec")
        spd_total=$(fmt_speedup "$i_total" "$c_total")

        printf "| %-25s | %s %s %s | %s %s %s | %s %s %s |\n" \
            "$label" \
            "$(fmt_time "$i_load")"  "$(fmt_time "$c_load")"  "$(fmt_speedup_cell "$spd_load")" \
            "$(fmt_time "$i_exec")"  "$(fmt_time "$c_exec")"  "$(fmt_speedup_cell "$spd_exec")" \
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
    echo "  Interpreter   : $INTERPRETER_DIR"
    echo "  Config        : $CONFIG_FILE"
    echo "  Workers       : $WORKERS"
    echo ""

    [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

    # Build both versions.
    setup_interpreter
    setup_compiler

    # Start with a clean log directory.
    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    # Initialise CSV so rows are appended incrementally.
    init_csv

    # Iterate over every program/dataset pair in the config file.
    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        parse_config_line "$raw_line" || continue

        echo ""
        echo "========================================"
        log "$CYAN" "BENCH" "$PROG_NAME + $DATASET_NAME"
        echo "========================================"

        setup_dataset "$DATASET_NAME"

        run_interpreter "$PROG_NAME" "$DATASET_NAME" || true
        run_compiler    "$PROG_NAME" "$DATASET_NAME" || true

                local program_stem="${PROG_NAME%.*}"
        local lbl="${program_stem}_${DATASET_NAME}"
        local interp_log="${LOG_DIR}/${program_stem}_${DATASET_NAME}_interpreter.log"
        local comp_log="${LOG_DIR}/${program_stem}_${DATASET_NAME}_compiler.log"

        print_pair_summary "$lbl" "$interp_log" "$comp_log"

        # Append this pair's results to CSV incrementally.
        append_csv_row "$program_stem" "$DATASET_NAME" "$interp_log" "$comp_log"

        # Cleanup dataset to save disk space
        cleanup_dataset "$DATASET_NAME"
    done < "$CONFIG_FILE"

    generate_results
}

main "$@"
