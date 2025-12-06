#!/bin/bash
set -euo pipefail

# ANSI color palette used for status messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}[START]${NC} FlowLog Testing"

# Resolve project paths and shared resources.
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

# Passing a custom config.txt path as the first argument
CONFIG_FILE="${ROOT_DIR}/tools/check/config.txt"
if [ $# -ge 1 ] && [ -n "$1" ]; then
    CONFIG_FILE="$1"
fi

PROG_DIR="${ROOT_DIR}/example"
FACT_DIR="${ROOT_DIR}/facts"
SIZE_DIR="${ROOT_DIR}/tools/check/correctness_size"
RESULT_DIR="${ROOT_DIR}/result"
LOG_DIR="${RESULT_DIR}/logs"
PARSED_DIR="${RESULT_DIR}/parsed"
COMPILER_BIN="${ROOT_DIR}/target/release/compiler"
WORKERS=${WORKERS:-64}

mkdir -p "$RESULT_DIR" "$LOG_DIR" "$PARSED_DIR"
LOG_DIR="$(realpath "$LOG_DIR")"
PARSED_DIR="$(realpath "$PARSED_DIR")"

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
        echo -e "${GREEN}[FOUND]${NC} Dataset $dataset_name"
        return
    fi

    mkdir -p "$FACT_DIR"

    if [ ! -f "$dataset_zip" ]; then
        echo -e "${CYAN}[DOWNLOAD]${NC} $dataset_name.zip"
        if ! wget -q -O "$dataset_zip" "$dataset_url"; then
            echo -e "${RED}[ERROR]${NC} Download failed: $dataset_name"
            exit 1
        fi
    fi

    echo -e "${YELLOW}[EXTRACT]${NC} $dataset_name"
    if ! unzip -q "$dataset_zip" -d "$FACT_DIR"; then
        echo -e "${RED}[ERROR]${NC} Failed to extract dataset: $dataset_name"
        exit 1
    fi
}

cleanup_dataset() {
    # Remove dataset artifacts to keep the workspace tidy between test runs.
    local dataset_name="$1"
    echo -e "${YELLOW}[CLEANUP]${NC} $dataset_name"
    rm -rf "${FACT_DIR}/${dataset_name}" "${FACT_DIR}/${dataset_name}.zip"
}

setup_config_file() {
    # Verify that the test configuration file exists.
    if [ -f "$CONFIG_FILE" ]; then
        return
    fi
    echo -e "${RED}[ERROR]${NC} config file not found: $CONFIG_FILE"
    exit 1
}

setup_size_reference() {
    # Validate the reference directory used for result comparison.
    if [ -d "$SIZE_DIR" ]; then
        return
    fi
    echo -e "${RED}[ERROR]${NC} Reference size directory not found: $SIZE_DIR"
    exit 1
}

ensure_compiler_built() {
    # Build the compiler binary if it is missing.
    if [ ! -x "$COMPILER_BIN" ]; then
        echo -e "${YELLOW}[BUILD]${NC} Building compiler"
        cargo build --release --bin compiler >/dev/null
    fi
}

compile_release_workspace() {
    # Compile the entire workspace so generated projects can link against fresh artifacts.
    echo -e "${YELLOW}[BUILD]${NC} Compiling release workspace"
    pushd "$ROOT_DIR" >/dev/null
    cargo build --release >/dev/null
    popd >/dev/null
}

parse_output_to_size_file() {
    local log_file="$1"
    local out_file="$2"

    # Guard to keep the parser from producing empty output files.
    if ! grep -Eq '^[[:space:]]*\[size\]' "$log_file"; then
        echo -e "${RED}[ERROR]${NC} No size lines found in $log_file"
        return 1
    fi

    grep -E '^[[:space:]]*\[size\]' "$log_file" \
        | awk '{
            name=$2;
            gsub(/^[\[]|[\]]$/, "", name);
            count=$3;
            print name ": " count;
        }' \
        | sort > "$out_file"
}

verify_results_against_truth() {
    # Compare parsed output sizes against pre-recorded reference data.
    local prog_name="$1" dataset_name="$2" result_size_file="$3"
    local reference_size_file="${SIZE_DIR}/${prog_name}_${dataset_name}_size.txt"

    if [ ! -f "$result_size_file" ] || [ ! -f "$reference_size_file" ]; then
        echo -e "${RED}[ERROR]${NC} Missing files for reference check"
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
        echo -e "${GREEN}[PASS]${NC} Reference check: $prog_name"
        return 0
    else
        echo -e "${RED}[FAIL]${NC} Reference mismatch: $prog_name"
        echo -e "${YELLOW}Reference:${NC}"
        cat "$reference_size_file"
        echo -e "${YELLOW}Actual:${NC}"
        cat "$result_size_file"
        return 1
    fi
}

run_test() {
    # Drive a single program/dataset pair through generation, execution, and verification.
    local prog_name="$1" dataset_name="$2"
    local prog_file="$(basename "$prog_name")"
    local prog_path="${PROG_DIR}/${prog_file}"

    if [ ! -f "$prog_path" ]; then
        echo -e "${RED}[ERROR]${NC} Program not found: $prog_path"
        exit 1
    fi

    setup_dataset "$dataset_name"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local program_stem="${prog_file%.*}"
    local package_name_raw="${program_stem}_${dataset_name}"
    local package_name="$(sanitize_package_name "$package_name_raw")"
    local project_dir="${ROOT_DIR}/${package_name}"

    echo -e "${BLUE}[TEST]${NC} $prog_file with $dataset_name"

    rm -rf "$project_dir"

    echo -e "${YELLOW}[GENERATE]${NC} $COMPILER_BIN $prog_path -F $dataset_path -o $project_dir"
    "$COMPILER_BIN" "$prog_path" -F "$dataset_path" -o "$project_dir"

    if [ ! -d "$project_dir" ]; then
        echo -e "${RED}[ERROR]${NC} Generated project not found: $project_dir"
        exit 1
    fi

    local log_file="${LOG_DIR}/${program_stem}_${dataset_name}.log"
    pushd "$project_dir" >/dev/null
    echo -e "${YELLOW}[RUN]${NC} cargo run --release -- -w $WORKERS"
    cargo run --release -- -w "$WORKERS" 2>&1 | tee "$log_file"
    popd >/dev/null

    if [ ! -f "$log_file" ]; then
        echo -e "${RED}[ERROR]${NC} Run log missing: $log_file"
        exit 1
    fi

    local parsed_file="${PARSED_DIR}/${program_stem}_${dataset_name}_size.txt"
    parse_output_to_size_file "$log_file" "$parsed_file" || {
        echo -e "${RED}[ERROR]${NC} Failed to parse output for $prog_file"
        exit 1
    }

    if ! verify_results_against_truth "$program_stem" "$dataset_name" "$parsed_file"; then
        echo -e "${RED}[ERROR]${NC} Reference verification failed for $prog_file"
        exit 1
    fi

    echo -e "${YELLOW}[CLEANUP]${NC} Removing generated project $project_dir"
    rm -rf "$project_dir"

    cleanup_dataset "$dataset_name"
}

run_all_tests() {
    # Iterate over the config file and execute every listed benchmark.
    echo -e "${BLUE}[TESTS]${NC} Running compiler-mode correctness tests"

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
            echo -e "${YELLOW}[SKIP]${NC} Skipping test.dl"
            continue
        fi
        run_test "$prog_name" "$dataset_name"
    done < "$CONFIG_FILE"

    echo -e "${GREEN}[COMPLETE]${NC} All tests passed"
}

compile_release_workspace
ensure_compiler_built
setup_config_file
setup_size_reference
run_all_tests

echo -e "${GREEN}[FINISH]${NC} testing completed successfully"
