#!/bin/bash
set -euo pipefail

# FlowLog end-to-end test runner.
#
# Structure:
#   tests/e2e/<test_name>/
#     program.dl        — Datalog source (must use .output directives)
#     data/             — CSV input files
#     expected/         — expected output files (one per .output relation)
#
# Usage:
#   tests/e2e/run.sh              # run all tests
#   tests/e2e/run.sh mixed_integer # run specific test(s)

###############################################################################
# Constants & globals
###############################################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
E2E_DIR="${ROOT_DIR}/tests/e2e"
COMPILER_BIN="${ROOT_DIR}/target/release/flowlog"
BUILD_DIR="${ROOT_DIR}/target/e2e"

passed=0
failed=0
errors=""

export RUST_LOG=error

###############################################################################
# Logging / utilities
###############################################################################

log() { local color="$1" tag="$2"; shift 2; echo -e "${color}[${tag}]${NC} $*"; }
die() { log "$RED" "ERROR" "$*"; exit 1; }

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [test_name ...]

Run FlowLog end-to-end tests. Each test is a directory under tests/e2e/ with:
  program.dl    Datalog source using .output directives
  data/         CSV input facts
  expected/     Expected output files (one per relation, comma-separated)

Examples:
  $(basename "$0")                # run all tests
  $(basename "$0") mixed_integer  # run one test
EOF
}

###############################################################################
# Prerequisite helpers
###############################################################################

ensure_compiler_built() {
    log "$YELLOW" "BUILD" "Building compiler (release)"
    (cd "$ROOT_DIR" && cargo build --release -p compiler 2>&1 | tail -1)
    [[ -x "$COMPILER_BIN" ]] || die "Compiler binary not found: $COMPILER_BIN"
}

###############################################################################
# Test runner
###############################################################################

run_test() {
    local test_dir="$1"
    local test_name
    test_name="$(basename "$test_dir")"
    local project_dir="${BUILD_DIR}/${test_name}"
    local output_dir="${project_dir}/output"

    log "$BLUE" "TEST" "$test_name"

    # ------------------------------------------------------------------
    # 1. Compile .dl → Rust project
    # ------------------------------------------------------------------
    rm -rf "$project_dir"
    local compile_log="${BUILD_DIR}/${test_name}_compile.log"
    if ! "$COMPILER_BIN" -D output "$test_dir/program.dl" >"$compile_log" 2>&1; then
        log "$RED" "FAIL" "$test_name — FlowLog compilation failed"
        log "$YELLOW" "HINT" "Compiler output:"
        sed 's/^/       /' "$compile_log"
        errors+="  $test_name: FlowLog compilation failed\n"
        ((failed++)) || true
        return
    fi

    # The compiler generates a project named after the .dl stem in CWD.
    local generated="${BUILD_DIR}/program"
    if [[ ! -d "$generated" ]]; then
        log "$RED" "FAIL" "$test_name — generated project not found"
        log "$YELLOW" "HINT" "Expected directory: $generated"
        errors+="  $test_name: generated project dir not found\n"
        ((failed++)) || true
        return
    fi
    mv "$generated" "$project_dir"

    # ------------------------------------------------------------------
    # 2. Copy input data into project directory
    # ------------------------------------------------------------------
    if [[ -d "$test_dir/data" ]]; then
        if compgen -G "$test_dir/data/*" > /dev/null; then
            cp "$test_dir"/data/* "$project_dir/"
        fi
    fi
    mkdir -p "$output_dir"

    # ------------------------------------------------------------------
    # 3. Build the generated project
    # ------------------------------------------------------------------
    local build_log="${BUILD_DIR}/${test_name}_build.log"
    if ! (cd "$project_dir" && cargo build --release >"$build_log" 2>&1); then
        log "$RED" "FAIL" "$test_name — cargo build failed"
        log "$YELLOW" "HINT" "Build log (last 20 lines):"
        tail -20 "$build_log" | sed 's/^/       /'
        errors+="  $test_name: cargo build failed\n"
        ((failed++)) || true
        return
    fi

    # ------------------------------------------------------------------
    # 4. Execute the generated binary
    # ------------------------------------------------------------------
    local run_log="${BUILD_DIR}/${test_name}_run.log"
    if ! (cd "$project_dir" && ./target/release/program >"$run_log" 2>&1); then
        log "$RED" "FAIL" "$test_name — execution failed"
        log "$YELLOW" "HINT" "Runtime log (last 20 lines):"
        tail -20 "$run_log" | sed 's/^/       /'
        errors+="  $test_name: execution failed\n"
        ((failed++)) || true
        return
    fi

    # ------------------------------------------------------------------
    # 5. Compare each expected output file
    # ------------------------------------------------------------------
    local all_match=1
    local diff_detail=""
    for expected_file in "$test_dir"/expected/*; do
        local rel_name
        rel_name="$(basename "$expected_file")"
        local actual_file="${output_dir}/${rel_name}"

        if [[ ! -f "$actual_file" ]]; then
            all_match=0
            diff_detail+="    Relation '${rel_name}': output file missing\n"
            diff_detail+="      Expected file: ${expected_file}\n"
            diff_detail+="      Actual file:   ${actual_file} (not found)\n"
            continue
        fi

        # Sort both before comparing (DD output order is nondeterministic).
        local diff_out
        if ! diff_out=$(diff \
            --label "expected/${rel_name}" <(sort "$expected_file") \
            --label "actual/${rel_name}"   <(sort "$actual_file") 2>&1); then
            all_match=0
            local exp_count act_count
            exp_count=$(wc -l < "$expected_file")
            act_count=$(wc -l < "$actual_file")
            diff_detail+="    Relation '${rel_name}': MISMATCH (expected ${exp_count} rows, got ${act_count} rows)\n"
            diff_detail+="      Expected (sorted):\n"
            diff_detail+="$(sort "$expected_file" | sed 's/^/        /')\n"
            diff_detail+="      Actual (sorted):\n"
            diff_detail+="$(sort "$actual_file" | sed 's/^/        /')\n"
            diff_detail+="      Diff:\n"
            diff_detail+="$(echo "$diff_out" | head -30 | sed 's/^/        /')\n"
        fi
    done

    if (( all_match )); then
        log "$GREEN" "PASS" "$test_name"
        ((passed++)) || true
    else
        log "$RED" "FAIL" "$test_name — output mismatch"
        echo -e "$diff_detail"
        errors+="  $test_name: output mismatch\n"
        ((failed++)) || true
    fi

    # Cleanup generated project to save disk.
    rm -rf "$project_dir"
    rm -f "$compile_log" "$build_log" "$run_log"
}

###############################################################################
# Entry point
###############################################################################

main() {
    if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
        usage
        exit 0
    fi

    log "$BLUE" "START" "FlowLog end-to-end tests"

    ensure_compiler_built
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    if [[ $# -gt 0 ]]; then
        for name in "$@"; do
            local test_dir="${E2E_DIR}/${name}"
            [[ -d "$test_dir" ]] || die "Test not found: $name (looked in $test_dir)"
            [[ -f "$test_dir/program.dl" ]] || die "No program.dl in test: $name"
            run_test "$test_dir"
        done
    else
        local found=0
        for test_dir in "$E2E_DIR"/*/; do
            [[ -f "$test_dir/program.dl" ]] || continue
            run_test "$test_dir"
            ((found++)) || true
        done
        (( found > 0 )) || die "No test cases found in $E2E_DIR"
    fi

    echo ""
    log "$BLUE" "RESULT" "${passed} passed, ${failed} failed"
    if (( failed > 0 )); then
        echo ""
        log "$RED" "FAILURES" ""
        echo -e "$errors"
        exit 1
    fi
    log "$GREEN" "FINISH" "All end-to-end tests passed"
}

main "$@"
