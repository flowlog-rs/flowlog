#!/bin/bash
set -euo pipefail

# FlowLog correctness runner.
#
# Default mode:
#   - Builds the release workspace
#   - Ensures the FlowLog compiler binary exists
#   - Downloads datasets (if needed)
#   - Generates and runs per-benchmark projects
#   - Parses "[size]" output lines and compares against reference files
#
# Compile-only mode:
#   - Builds the release workspace and the FlowLog compiler binary
#   - Iterates through the config file
#   - Does not download or read datasets (uses an empty facts directory)
#   - Generates each project and runs `cargo check` (no execution/verification)

# ANSI color palette used for status messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log() {
    # log COLOR TAG MESSAGE...
    local color="$1"; shift
    local tag="$1"; shift
    echo -e "${color}[${tag}]${NC} $*"
}

die() {
    log "$RED" "ERROR" "$*"
    exit 1
}

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [config_file] [--compile-only]

Modes:
    Default: run all correctness tests listed in the config file.
    --compile-only: generate each project and run cargo check only (no download/run/verify).

Environment:
  WORKERS=<n>    Number of workers passed to generated executables (default: 64)

Examples:
  $(basename "$0")
  $(basename "$0") tools/check/config.txt
  $(basename "$0") --compile-only
EOF
}

# Resolve project paths and shared resources.
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

CONFIG_FILE_DEFAULT="${ROOT_DIR}/tools/check/config.txt"
CONFIG_FILE="$CONFIG_FILE_DEFAULT"
COMPILE_ONLY=0

parse_args() {
    # Backwards compatible with the old behavior:
    #   - first non-flag arg is treated as the config file path
    while [ $# -gt 0 ]; do
        case "$1" in
            --compile-only)
                COMPILE_ONLY=1
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            --)
                shift
                break
                ;;
            -*)
                die "Unknown option: $1 (try --help)"
                ;;
            *)
                if [ "$CONFIG_FILE" = "$CONFIG_FILE_DEFAULT" ]; then
                    CONFIG_FILE="$1"
                else
                    die "Unexpected extra argument: $1"
                fi
                shift
                ;;
        esac
    done

    if [ $# -gt 0 ]; then
        die "Unexpected extra arguments: $*"
    fi
}

init_paths() {
    PROG_DIR="${ROOT_DIR}/example"
    FACT_DIR="${ROOT_DIR}/facts"
    SIZE_DIR="${ROOT_DIR}/tools/check/correctness_size"
    RESULT_DIR="${ROOT_DIR}/result"
    LOG_DIR="${RESULT_DIR}/logs"
    PARSED_DIR="${RESULT_DIR}/parsed"
    COMPILER_BIN="${ROOT_DIR}/target/release/flowlog"
    WORKERS=${WORKERS:-64}

    mkdir -p "$RESULT_DIR" "$LOG_DIR" "$PARSED_DIR"
    LOG_DIR="$(realpath "$LOG_DIR")"
    PARSED_DIR="$(realpath "$PARSED_DIR")"
}

log "$BLUE" "START" "FlowLog Testing"

trim() {
    # Strip leading/trailing whitespace from configuration entries.
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

sanitize_package_name() {
    # Produce a cargo-friendly package name based on program and dataset identifiers.
    local s
    s="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
    s="$(printf '%s' "$s" | tr -c 'a-z0-9_-' '_')"
    s="$(printf '%s' "$s" | sed 's/_\{2,\}/_/g; s/-\{2,\}/-/g; s/^[-_]//; s/[-_]$//')"
    if [ -z "$s" ]; then
        s="flowlog_output"
    fi
    if [[ $s =~ ^[0-9] ]]; then
        s="flowlog_${s}"
    fi
    printf '%s' "$s"
}

setup_dataset() {
    # Ensure the requested dataset is present locally, downloading and extracting if needed.
    local dataset_name="$1"
    local dataset_zip="${FACT_DIR}/${dataset_name}.zip"
    local extract_path="${FACT_DIR}/${dataset_name}"
    local dataset_url="https://pages.cs.wisc.edu/~m0riarty/dataset/csv/${dataset_name}.zip"

    if [ -d "$extract_path" ]; then
        log "$GREEN" "FOUND" "Dataset $dataset_name"
        return
    fi

    mkdir -p "$FACT_DIR"

    if [ ! -f "$dataset_zip" ]; then
        log "$CYAN" "DOWNLOAD" "$dataset_name.zip"
        command -v wget >/dev/null 2>&1 || die "wget not found; cannot download datasets"
        wget -q -O "$dataset_zip" "$dataset_url" || die "Download failed: $dataset_name"
    fi

    log "$YELLOW" "EXTRACT" "$dataset_name"
    command -v unzip >/dev/null 2>&1 || die "unzip not found; cannot extract datasets"
    unzip -q "$dataset_zip" -d "$FACT_DIR" || die "Failed to extract dataset: $dataset_name"
}

cleanup_dataset() {
    # Remove dataset artifacts to keep the workspace tidy between test runs.
    local dataset_name="$1"
    log "$YELLOW" "CLEANUP" "$dataset_name"
    rm -rf "${FACT_DIR}/${dataset_name}" "${FACT_DIR}/${dataset_name}.zip"
}

setup_config_file() {
    # Verify that the test configuration file exists.
    if [ -f "$CONFIG_FILE" ]; then
        return
    fi
    die "config file not found: $CONFIG_FILE"
}

setup_size_reference() {
    # Validate the reference directory used for result comparison.
    if [ -d "$SIZE_DIR" ]; then
        return
    fi
    die "Reference size directory not found: $SIZE_DIR"
}

ensure_compiler_built() {
    # Build the compiler binary if it is missing.
    if [ ! -x "$COMPILER_BIN" ]; then
        log "$YELLOW" "BUILD" "Building compiler (flowlog)"
        cargo build --release --bin flowlog >/dev/null
    fi
}

compile_release_workspace() {
    # Compile the entire workspace so generated projects can link against fresh artifacts.
    log "$YELLOW" "BUILD" "Compiling release workspace"
    pushd "$ROOT_DIR" >/dev/null
    cargo build --release >/dev/null
    popd >/dev/null
}

###############################################################################
# Compile-only mode (generate + cargo check)
#
# Notes:
# - We still iterate through the config file to cover all benchmark programs.
# - The right-hand side "dataset" from the config is treated as an identifier
#   for naming/logging ONLY in this mode.
# - We do not download or read datasets; generation always uses an empty facts
#   directory because `cargo check` does not require actual data.
###############################################################################

EMPTY_FACTS_DIR=""

setup_empty_facts_dir() {
    # Create once per run and reuse for all compile-only entries.
    if [ -n "$EMPTY_FACTS_DIR" ]; then
        return 0
    fi
    EMPTY_FACTS_DIR="$(mktemp -d -t flowlog_facts_empty_XXXXXX)"
}

cleanup_empty_facts_dir() {
    if [ -n "$EMPTY_FACTS_DIR" ]; then
        rm -rf "$EMPTY_FACTS_DIR"
        EMPTY_FACTS_DIR=""
    fi
}

run_compile_check() {
    # Generate the project and run `cargo check` (no execution).
    #
    # Arguments:
    #   $1: program file name from config (e.g., tc.dl)
    #   $2: dataset id from config (used only for naming/logging)
    local prog_name="$1" dataset_id="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local prog_path="${PROG_DIR}/${prog_file}"

    if [ ! -f "$prog_path" ]; then
        die "Program not found: $prog_path"
    fi

    if [ -z "$EMPTY_FACTS_DIR" ]; then
        die "Internal error: EMPTY_FACTS_DIR not initialized"
    fi

    local facts_dir="$EMPTY_FACTS_DIR"

    local program_stem="${prog_file%.*}"
    local mode="batch"
    local package_name
    package_name="$(sanitize_package_name "${program_stem}_${dataset_id}_${mode}")"
    local project_dir="${ROOT_DIR}/${package_name}"

    log "$BLUE" "CHECK" "$prog_file (id=$dataset_id, mode=$mode)"

    rm -rf "$project_dir"
    log "$YELLOW" "GENERATE" "$COMPILER_BIN $prog_path -F $facts_dir -o $project_dir --mode $mode"
    "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -o "$project_dir" --mode "$mode"

    if [ ! -d "$project_dir" ]; then
        die "Generated project not found (mode=$mode): $project_dir"
    fi

    local log_file="${LOG_DIR}/${program_stem}_${dataset_id}_${mode}_compile.log"
    pushd "$project_dir" >/dev/null
    log "$YELLOW" "RUN" "cargo check --release (mode=$mode)"
    cargo check --release 2>&1 | tee "$log_file"
    popd >/dev/null

    log "$YELLOW" "CLEANUP" "Removing generated project $project_dir"
    rm -rf "$project_dir"
}

run_all_compile_checks() {
    log "$BLUE" "TESTS" "Running compile-only checks from config"

    while IFS= read -r raw_line || [ -n "$raw_line" ]; do
        local line="${raw_line%%#*}"
        line="$(trim "$line")"

        if [ -z "$line" ]; then
            continue
        fi

        IFS='=' read -r prog_name dataset_id <<< "$line"
        prog_name="$(trim "${prog_name:-}")"
        dataset_id="$(trim "${dataset_id:-}")"

        if [ -z "$prog_name" ] || [ -z "$dataset_id" ]; then
            continue
        fi

        if [ "$prog_name" = "test.dl" ]; then
            log "$YELLOW" "SKIP" "Skipping test.dl"
            continue
        fi

        run_compile_check "$prog_name" "$dataset_id"
    done < "$CONFIG_FILE"

    log "$GREEN" "COMPLETE" "All compile-only checks passed"
}

compile_only() {
    log "$BLUE" "MODE" "compile-only (no download; generate + cargo check only)"
    compile_release_workspace
    ensure_compiler_built
    setup_config_file

    setup_empty_facts_dir
    trap cleanup_empty_facts_dir EXIT

    run_all_compile_checks
    log "$GREEN" "FINISH" "compile-only completed successfully"
}

parse_output_to_size_file() {
    local log_file="$1"
    local out_file="$2"

    # Guard to keep the parser from producing empty output files.
    if ! grep -Eq '^[[:space:]]*\[size\]\[' "$log_file"; then
        log "$RED" "ERROR" "No size lines found in $log_file"
        return 1
    fi

    command -v python >/dev/null 2>&1 || die "python not found; cannot parse size output"
    python - "$log_file" "$out_file" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])

# Matches: [size][REL] ... size=123 (time field may be anything)
pat = re.compile(r'^\s*\[size\]\[([^\]]+)\].*\bsize=([+-]?\d+)\b')

sizes = {}  # keep the last seen size per relation
for raw in log_path.read_text(errors="replace").splitlines():
    m = pat.match(raw)
    if not m:
        continue
    rel = m.group(1).strip()
    sz = m.group(2).strip()
    sizes[rel] = sz

if not sizes:
    print(f"No parsable size lines found in {log_path}", file=sys.stderr)
    sys.exit(1)

lines = [f"{k}: {v}" for k, v in sorted(sizes.items())]
out_path.write_text("\n".join(lines) + "\n")
PY
}

verify_results_against_truth() {
    # Compare parsed output sizes against pre-recorded reference data.
    local prog_name="$1" dataset_name="$2" result_size_file="$3"
    local reference_size_file="${SIZE_DIR}/${prog_name}_${dataset_name}_size.txt"

    if [ ! -f "$result_size_file" ] || [ ! -f "$reference_size_file" ]; then
        log "$RED" "ERROR" "Missing files for reference check"
        return 1
    fi

    if python - "$reference_size_file" "$result_size_file" <<'PY'
import sys
from pathlib import Path

ref_path, actual_path = sys.argv[1], sys.argv[2]

def load(path):
    data = {}
    for idx, raw_line in enumerate(Path(path).read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        if ':' not in line:
            print(f"Invalid line format in {path} line {idx}: {raw_line!r}")
            sys.exit(1)
        key, value = line.split(':', 1)
        data[key.strip()] = value.strip()
    return data

ref = load(ref_path)
actual = load(actual_path)

missing = [k for k in ref if k not in actual]
if missing:
    print("Missing relations:", ', '.join(sorted(missing)))
    sys.exit(1)

mismatches = []
for key, expected in ref.items():
    observed = actual.get(key)
    if observed != expected:
        mismatches.append((key, expected, observed))

if mismatches:
    for key, expected, observed in mismatches:
        obs_text = '<missing>' if observed is None else observed
        print(f"Relation {key}: expected {expected}, actual {obs_text}")
    sys.exit(1)

extras = [k for k in actual if k not in ref]
if extras:
    print("Note: extra relations ignored:", ', '.join(sorted(extras)))

sys.exit(0)
PY
    then
        log "$GREEN" "PASS" "Reference check: $prog_name"
        return 0
    else
        log "$RED" "FAIL" "Reference mismatch: $prog_name"
        log "$YELLOW" "INFO" "Reference:"
        cat "$reference_size_file"
        log "$YELLOW" "INFO" "Actual:"
        cat "$result_size_file"
        return 1
    fi
}

run_test() {
    # Drive a single program/dataset pair through generation, execution, and verification.
    local prog_name="$1" dataset_name="$2"
    local prog_file
    prog_file="$(basename "$prog_name")"
    local prog_path="${PROG_DIR}/${prog_file}"

    if [ ! -f "$prog_path" ]; then
        die "Program not found: $prog_path"
    fi

    setup_dataset "$dataset_name"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local program_stem="${prog_file%.*}"
    local package_name_raw="${program_stem}_${dataset_name}"

    local mode="batch"
    local package_name
    package_name="$(sanitize_package_name "${package_name_raw}_${mode}")"
    local project_dir="${ROOT_DIR}/${package_name}"

    log "$BLUE" "TEST" "$prog_file with $dataset_name (mode=$mode)"

    rm -rf "$project_dir"

    log "$YELLOW" "GENERATE" "$COMPILER_BIN $prog_path -F $dataset_path -o $project_dir --mode $mode"
    "$COMPILER_BIN" "$prog_path" -F "$dataset_path" -o "$project_dir" --mode "$mode"

    if [ ! -d "$project_dir" ]; then
        die "Generated project not found (mode=$mode): $project_dir"
    fi

    local log_file="${LOG_DIR}/${program_stem}_${dataset_name}_${mode}.log"
    pushd "$project_dir" >/dev/null
    log "$YELLOW" "RUN" "cargo run --release -- -w $WORKERS (mode=$mode)"
    cargo run --release -- -w "$WORKERS" 2>&1 | tee "$log_file"
    popd >/dev/null

    if [ ! -f "$log_file" ]; then
        die "Run log missing (mode=$mode): $log_file"
    fi

    local parsed_file="${PARSED_DIR}/${program_stem}_${dataset_name}_${mode}_size.txt"
    parse_output_to_size_file "$log_file" "$parsed_file" || {
        die "Failed to parse output for $prog_file (mode=$mode)"
    }

    if ! verify_results_against_truth "$program_stem" "$dataset_name" "$parsed_file"; then
        die "Reference verification failed for $prog_file (mode=$mode)"
    fi

    log "$YELLOW" "CLEANUP" "Removing generated project $project_dir"
    rm -rf "$project_dir"

    cleanup_dataset "$dataset_name"
}

run_all_tests() {
    # Iterate over the config file and execute every listed benchmark.
    log "$BLUE" "TESTS" "Running compiler-mode correctness tests (batch mode)"

    while IFS= read -r raw_line || [ -n "$raw_line" ]; do
        local line="${raw_line%%#*}"
        line="$(trim "$line")"

        if [ -z "$line" ]; then
            continue
        fi

        IFS='=' read -r prog_name dataset_name <<< "$line"
        prog_name="$(trim "${prog_name:-}")"
        dataset_name="$(trim "${dataset_name:-}")"

        if [ -z "$prog_name" ] || [ -z "$dataset_name" ]; then
            continue
        fi

        if [ "$prog_name" = "test.dl" ]; then
            log "$YELLOW" "SKIP" "Skipping test.dl"
            continue
        fi

        run_test "$prog_name" "$dataset_name"
    done < "$CONFIG_FILE"

    log "$GREEN" "COMPLETE" "All tests passed"
}

main() {
    parse_args "$@"
    init_paths

    if [ "$COMPILE_ONLY" -eq 1 ]; then
        compile_only
        return 0
    fi

    compile_release_workspace
    ensure_compiler_built
    setup_config_file
    setup_size_reference
    run_all_tests

    log "$GREEN" "FINISH" "testing completed successfully"
}

main "$@"
