#!/usr/bin/env bash
set -euo pipefail

# FlowLog binary-mode end-to-end test runner.
#
# Layout:
#   tests/unit/<category>/              Category determines --mode flag:
#     datalog-batch  → (default)         datalog-inc  → --mode datalog-inc
#     extend-batch   → --mode extend-batch
#     extend-inc     → --mode extend-inc
#
#   tests/unit/<category>/<test_name>/
#     program.dl     Datalog source (must use .output directives)
#     data/          Optional CSV input facts copied into generated project
#     expected/      Expected output files (one per output relation)
#     commands.txt   Optional incremental transcript (enables incremental mode)
#     runtime_flags  Optional runtime flags (e.g. -w 4 for multi-worker)
#
# Usage:
#   tests/unit/unit_compiler.sh                          # run all tests
#   tests/unit/unit_compiler.sh <test_name> [test_name ...] # run specific tests

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

readonly COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"
readonly BUILD_DIR="${ROOT_DIR}/target/e2e"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [test_name ...]

Run FlowLog binary-mode end-to-end tests. Tests are organized by category:
  datalog-batch/  Standard batch Datalog evaluation (default mode)
  datalog-inc/    Incremental Datalog evaluation
  extend-batch/   Extended batch evaluation (loops)
  extend-inc/     Extended incremental evaluation

Each test directory contains:
  program.dl      Datalog source using .output directives
  data/           Optional CSV input facts
  expected/       Expected output files (one per relation)
  commands.txt    Optional incremental command transcript
  runtime_flags   Optional runtime flags (e.g. -w 4)

Examples:
  $(basename "$0")                     # run all tests
  $(basename "$0") recursive_max       # run one test
EOF
}

###############################################################################
# Category → compiler mode mapping
###############################################################################

mode_flag_for_category() {
    local category="$1"
    case "$category" in
        datalog-batch) echo "" ;;
        datalog-inc)   echo "--mode datalog-inc" ;;
        extend-batch)  echo "--mode extend-batch" ;;
        extend-inc)    echo "--mode extend-inc" ;;
        *) die "Unknown category: $category" ;;
    esac
}

###############################################################################
# Build helpers
###############################################################################

ensure_compiler_built() {
    echo -e "  ${YELLOW}Building compiler (release)...${NC}"
    (cd "$ROOT_DIR" && cargo build --release -p flowlog-compiler 2>&1 | tail -1)
    [[ -x "$COMPILER_BIN" ]] || die "Compiler binary not found: $COMPILER_BIN"
}

copy_test_data() {
    local test_dir="$1"
    local work_dir="$2"

    [[ -d "$test_dir/data" ]] || return 0
    compgen -G "$test_dir/data/*" > /dev/null || return 0
    cp "$test_dir"/data/* "$work_dir/"
}

###############################################################################
# Execution helpers
###############################################################################

run_incremental_session() {
    local work_dir="$1"
    local commands_file="$2"
    local run_log="$3"
    local fifo_path="$4"

    local feeder_pid
    local status=0

    rm -f "$fifo_path"
    mkfifo "$fifo_path"

    (
        while IFS= read -r line || [[ -n "$line" ]]; do
            printf '%s\n' "$line"
            sleep 0.02
        done < "$commands_file"
    ) > "$fifo_path" &
    feeder_pid=$!

    (cd "$work_dir" && script -qefc "./program" /dev/null < "$fifo_path" >"$run_log" 2>&1) || status=$?

    rm -f "$fifo_path"
    wait "$feeder_pid" || true
    return "$status"
}

run_generated_binary() {
    local work_dir="$1"
    local test_dir="$2"
    local test_name="$3"
    local run_log="$4"
    local incremental="$5"

    if (( incremental )); then
        command -v script >/dev/null 2>&1 || die "Required dependency 'script' not found on PATH; needed for incremental shell tests."
        run_incremental_session "$work_dir" "$test_dir/commands.txt" "$run_log" "${BUILD_DIR}/${test_name}.cmd.fifo"
        return $?
    fi

    local runtime_flags=()
    if [[ -f "$test_dir/runtime_flags" ]]; then
        mapfile -t runtime_flags < "$test_dir/runtime_flags"
    fi
    (cd "$work_dir" && ./program "${runtime_flags[@]}" >"$run_log" 2>&1)
}

###############################################################################
# Test runner
###############################################################################

run_test() {
    local test_dir="$1"
    local category="$2"
    local test_name
    test_name="$(basename "$test_dir")"
    local full_name="${category}/${test_name}"

    ((current++)) || true
    show_progress "$full_name"

    local work_dir="${BUILD_DIR}/${test_name}"
    local output_dir="${work_dir}/output"
    local compile_log="${BUILD_DIR}/${test_name}_compile.log"
    local run_log="${BUILD_DIR}/${test_name}_run.log"

    local incremental=0
    [[ -f "$test_dir/commands.txt" ]] && incremental=1

    # 1) Compile
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    local mode_flag
    mode_flag="$(mode_flag_for_category "$category")"

    local compile_flags=()
    if [[ -n "$mode_flag" ]]; then
        read -ra compile_flags <<< "$mode_flag"
    fi

    # UDF support: pass --udf-file if present
    if [[ -f "$test_dir/udf.rs" ]]; then
        compile_flags+=(--udf-file "$test_dir/udf.rs")
    fi

    # `include_dirs` file: one directory path per line (relative to the
    # fixture directory). Each becomes a `-I <abs>` flag on the compile
    # invocation. Used by fixtures that exercise `-I`-based .include
    # resolution rather than parent-file-relative paths.
    if [[ -f "$test_dir/include_dirs" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -z "$line" ]] && continue
            compile_flags+=(-I "$test_dir/$line")
        done < "$test_dir/include_dirs"
    fi

    if ! "$COMPILER_BIN" -D output "${compile_flags[@]}" "$test_dir/program.dl" -o "$work_dir/program" >"$compile_log" 2>&1; then
        local detail
        detail="$(cat "$compile_log" 2>/dev/null | tail -20 | sed 's/^/         /')"
        record_failure "$full_name" "compilation failed" "$detail"
        rm -rf "$work_dir" "$compile_log" "$run_log"
        return
    fi

    # 2) Stage inputs
    copy_test_data "$test_dir" "$work_dir"
    mkdir -p "$output_dir"

    # 3) Execute
    if ! run_generated_binary "$work_dir" "$test_dir" "$test_name" "$run_log" "$incremental"; then
        local detail
        detail="$(tail -20 "$run_log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "execution failed" "$detail"
        rm -rf "$work_dir" "$compile_log" "$run_log"
        return
    fi

    # 4) Compare
    local use_sort=0
    [[ -f "$test_dir/runtime_flags" ]] && grep -q -- '-w' "$test_dir/runtime_flags" && use_sort=1

    local mismatch_detail
    if mismatch_detail=$(compare_expected_outputs "$test_dir" "$output_dir" "$use_sort"); then
        ((passed++)) || true
    else
        record_failure "$full_name" "output mismatch" "$mismatch_detail"
    fi

    rm -rf "$work_dir"
    rm -f "$compile_log" "$run_log"
}

###############################################################################
# Entry point
###############################################################################

main() {
    if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
        usage
        exit 0
    fi

    echo ""
    echo -e "  ${BOLD}FlowLog Unit Tests (binary mode)${NC}"
    echo ""

    ensure_compiler_built
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    # Count total tests first
    total=$(count_tests "$@")
    echo -e "  ${DIM}Running ${total} tests...${NC}"
    echo ""

    if [[ $# -gt 0 ]]; then
        local name
        for name in "$@"; do
            local cat
            cat="$(find_test "$name")" || die "Test not found: $name (searched all categories)"
            run_test "${TESTS_DIR}/${cat}/${name}" "$cat"
        done
    else
        for cat in "${CATEGORIES[@]}"; do
            local cat_dir="${TESTS_DIR}/${cat}"
            [[ -d "$cat_dir" ]] || continue
            for test_dir in "$cat_dir"/*/; do
                [[ -f "$test_dir/program.dl" ]] || continue
                run_test "$test_dir" "$cat"
            done
        done
    fi

    clear_progress
    echo ""

    print_summary

    echo ""
    (( failed == 0 ))
}

main "$@"
