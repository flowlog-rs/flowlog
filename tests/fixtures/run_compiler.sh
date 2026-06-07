#!/usr/bin/env bash
set -euo pipefail

# FlowLog binary-mode end-to-end test runner.
#
# Layout:
#   tests/fixtures/<category>/          Category determines --mode flag:
#     datalog-batch  → (default)         datalog-inc  → --mode datalog-inc
#     extend-batch   → --mode extend-batch
#     extend-inc     → --mode extend-inc
#
#   tests/fixtures/<category>/<test_name>/
#     program.dl     Datalog source (must use .output directives)
#     data/          Optional CSV input facts copied into generated project
#     expected/      Expected output files (one per output relation)
#     commands.txt   Optional incremental transcript (enables incremental mode)
#     runtime_flags  Optional runtime flags (e.g. -w 4 for multi-worker)
#
# Usage:
#   tests/fixtures/run_compiler.sh                          # run all tests
#   tests/fixtures/run_compiler.sh <test_name> [test_name ...] # run specific tests

# Categories exercised by binary mode. Add `extend-inc` here when the
# first such fixture lands (binary mode already supports the mode).
CATEGORIES=(datalog-batch datalog-inc extend-batch)

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

readonly COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"
readonly BUILD_DIR="${ROOT_DIR}/target/e2e"

# Build generated crates against the workspace runtime instead of crates.io,
# so unpublished flowlog-runtime additions are testable (scaffold emits a
# [patch.crates-io] entry when this is set).
export FLOWLOG_RUNTIME_PATH="${ROOT_DIR}/crates/flowlog-runtime"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [-j N] [test_name ...]

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

Options:
  -j N            Run up to N fixtures in parallel (default 1).
                  Each fixture already gets its own work_dir, so parallelism
                  is safe in binary mode.

Examples:
  $(basename "$0")                     # run all tests sequentially
  $(basename "$0") -j 8                # 8 fixtures at a time
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

run_generated_binary() {
    local work_dir="$1"
    local test_dir="$2"
    local run_log="$3"
    local incremental="$4"

    # Incremental: feed commands via stdin. Rustyline detects non-TTY
    # stdin and falls back to a synchronous line reader, so no PTY or
    # pacing choreography is needed.
    if (( incremental )); then
        (cd "$work_dir" && ./program < "$test_dir/commands.txt" >"$run_log" 2>&1)
        return
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

    # Per-fixture `compile_flags`: append each whitespace-split token to
    # the compile invocation (e.g. `--str-intern`).
    if [[ -f "$test_dir/compile_flags" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
            # shellcheck disable=SC2206
            compile_flags+=($line)
        done < "$test_dir/compile_flags"
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
    if ! run_generated_binary "$work_dir" "$test_dir" "$run_log" "$incremental"; then
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
# Parallel scheduler
###############################################################################

# Bounded-concurrency scheduler: one subshell per task, throttled with `wait -n`
# as a counting semaphore. Per-task results land in result files; the parent
# aggregates them via `aggregate_parallel_results` after the final wait.
run_tasks_parallel() {
    local jobs="$1"; shift
    local -a tasks=("$@")  # entries are "<category>|<test_dir>"

    init_parallel_dirs "$BUILD_DIR"

    local total_count=${#tasks[@]}
    local idx=0
    local entry category test_dir result_file
    for entry in "${tasks[@]}"; do
        category="${entry%%|*}"
        test_dir="${entry#*|}"
        result_file="${PARALLEL_RESULTS_DIR}/$(printf '%04d' "$idx").result"

        while (( $(jobs -rp | wc -l) >= jobs )); do
            wait -n
        done
        (
            failure_names=(); failure_reasons=(); failure_details=()
            passed=0; failed=0; current=0
            show_progress() { :; }
            clear_progress() { :; }

            run_test "$test_dir" "$category"
            write_test_result_and_tally \
                "$result_file" "${category}/$(basename "$test_dir")" "$total_count"
        ) &
        ((idx++)) || true
    done
    wait

    aggregate_parallel_results
}

###############################################################################
# Entry point
###############################################################################

main() {
    parse_jobs_flag usage "$@"
    local jobs="$PARSED_JOBS"
    set -- "${PARSED_POSITIONAL[@]}"

    echo ""
    echo -e "  ${BOLD}FlowLog Fixture Tests (binary mode)${NC}"
    echo ""

    ensure_compiler_built
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    # Count total tests first
    total=$(count_tests "$@")
    if (( jobs > 1 )); then
        echo -e "  ${DIM}Running ${total} tests (parallel, -j ${jobs})...${NC}"
    else
        echo -e "  ${DIM}Running ${total} tests...${NC}"
    fi
    echo ""

    # Build the flat (category, test_dir) task list.
    local -a tasks=()
    if [[ $# -gt 0 ]]; then
        local name
        for name in "$@"; do
            local cat
            cat="$(find_test "$name")" || die "Test not found: $name (searched all categories)"
            tasks+=("${cat}|${TESTS_DIR}/${cat}/${name}")
        done
    else
        for cat in "${CATEGORIES[@]}"; do
            local cat_dir="${TESTS_DIR}/${cat}"
            [[ -d "$cat_dir" ]] || continue
            for test_dir in "$cat_dir"/*/; do
                [[ -f "$test_dir/program.dl" ]] || continue
                tasks+=("${cat}|${test_dir%/}")
            done
        done
    fi

    if (( jobs > 1 )); then
        run_tasks_parallel "$jobs" "${tasks[@]}"
    else
        run_tasks_sequential "${tasks[@]}"
    fi

    clear_progress
    echo ""

    print_summary

    echo ""
    (( failed == 0 ))
}

main "$@"
