#!/usr/bin/env bash
set -euo pipefail

# FlowLog binary-mode end-to-end test runner.
#
# Layout:
#   tests/fixtures/<test_name>/
#     program.dl     Datalog source (must use .output directives)
#     data/          Optional CSV input facts copied into generated project
#     expected/      Expected output files (one per output relation)
#     commands.txt   Transaction transcript; present iff the program declares
#                    a `mutable` input, which makes the fixture incremental
#     runtime_flags  Optional runtime flags (e.g. -w 4 for multi-worker)
#
# Naming: an incremental fixture is `txn_*` (transaction shell mechanics),
# `mixed_*` (static and mutable inputs in one program), or `*_delta` (a batch
# feature re-checked per epoch). Static fixtures use none of these.
#
# Usage:
#   tests/fixtures/run_compiler.sh                          # run all tests
#   tests/fixtures/run_compiler.sh <test_name> [test_name ...] # run specific tests

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$TESTS_DIR/sqlite_helper.sh"

readonly COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"
readonly BUILD_DIR="${ROOT_DIR}/target/e2e"

# A direct path dependency keeps runtime version bumps testable before release.
export FLOWLOG_RUNTIME_PATH="${ROOT_DIR}/flowlog-runtime"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [-j N] [--shard I/N] [test_name ...]

Run FlowLog binary-mode end-to-end tests. Each test directory under
tests/fixtures/<name>/ contains:
  program.dl      Datalog source using .output directives
  data/           Optional CSV input facts
  expected/       Expected output files (one per relation)
  commands.txt    Transaction transcript; present iff an input is \`mutable\`
  runtime_flags   Optional runtime flags (e.g. -w 4)

Incremental fixtures are named txn_*, mixed_*, or *_delta.

Options:
  -j N            Run up to N fixtures in parallel (default 1).
                  A supplied CARGO_TARGET_DIR is split by fixture to keep
                  generated binaries separate.
  --shard I/N     Run only shard I of N (the fixtures split into N groups).

Examples:
  $(basename "$0")                     # run all tests sequentially
  $(basename "$0") -j 8                # 8 fixtures at a time
  $(basename "$0") recursive_max       # run one test
  $(basename "$0") --shard 1/8         # first of 8 shards
EOF
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

    local runtime_flags=()
    if [[ -f "$test_dir/runtime_flags" ]]; then
        mapfile -t runtime_flags < "$test_dir/runtime_flags"
    fi
    if (( incremental )); then
        (cd "$work_dir" && ./program "${runtime_flags[@]}" < "$test_dir/commands.txt" >"$run_log" 2>&1)
        return
    fi
    (cd "$work_dir" && ./program "${runtime_flags[@]}" >"$run_log" 2>&1)
}

###############################################################################
# Test runner
###############################################################################

run_test() {
    local test_dir="$1"
    local test_name
    test_name="$(basename "$test_dir")"

    ((current++)) || true
    show_progress "$test_name"

    local work_dir="${BUILD_DIR}/${test_name}"
    local output_dir="${work_dir}/output"
    local compile_log="${BUILD_DIR}/${test_name}_compile.log"
    local run_log="${BUILD_DIR}/${test_name}_run.log"

    local incremental=0
    [[ -f "$test_dir/commands.txt" ]] && incremental=1

    # 1) Compile
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    local compile_flags=()

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
        record_failure "$test_name" "compilation failed" "$detail"
        rm -rf "$work_dir" "$compile_log" "$run_log"
        return
    fi

    # 2) Stage inputs
    copy_test_data "$test_dir" "$work_dir"
    mkdir -p "$output_dir"
    if [[ -f "$test_dir/sqlite_setup.sql" ]]; then
        if ! setup_sqlite_fixture "$test_dir" "$work_dir" >"$run_log" 2>&1; then
            record_failure "$test_name" "SQLite setup failed" "$(cat "$run_log")"
            return
        fi
    fi

    # 3) Execute
    if ! run_generated_binary "$work_dir" "$test_dir" "$run_log" "$incremental"; then
        local detail
        detail="$(tail -20 "$run_log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$test_name" "execution failed" "$detail"
        rm -rf "$work_dir" "$compile_log" "$run_log"
        return
    fi

    # `.printsize` reports on stdout rather than writing a file, so distill
    # those lines into one; the usual `expected/<name>` comparison pins them
    # from there.
    if grep -q '^\[size\]' "$run_log" 2>/dev/null; then
        grep '^\[size\]' "$run_log" > "${output_dir}/printsize"
    fi

    if [[ -f "$test_dir/sqlite_setup.sql" ]]; then
        if ! export_sqlite_outputs "$test_dir" "$work_dir" >>"$run_log" 2>&1; then
            record_failure "$test_name" "SQLite query failed" "$(cat "$run_log")"
            return
        fi
    fi

    # 4) Compare
    local use_sort=0
    [[ -f "$test_dir/sqlite_setup.sql" ]] && use_sort=1
    [[ -f "$test_dir/runtime_flags" ]] && grep -q -- '-w' "$test_dir/runtime_flags" && use_sort=1

    local mismatch_detail
    if mismatch_detail=$(compare_expected_outputs "$test_dir" "$output_dir" "$use_sort"); then
        ((passed++)) || true
    else
        record_failure "$test_name" "output mismatch" "$mismatch_detail"
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
    local -a tasks=("$@")  # fixture directories

    init_parallel_dirs "$BUILD_DIR"

    local total_count=${#tasks[@]}
    local idx=0
    local test_dir result_file
    for test_dir in "${tasks[@]}"; do
        result_file="${PARALLEL_RESULTS_DIR}/$(printf '%04d' "$idx").result"

        while (( $(jobs -rp | wc -l) >= jobs )); do
            wait -n
        done
        (
            failure_names=(); failure_reasons=(); failure_details=()
            passed=0; failed=0; current=0
            show_progress() { :; }
            clear_progress() { :; }

            # Generated crates share a binary name. Separate targets prevent
            # one build from replacing another's executable before it is copied.
            if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
                export CARGO_TARGET_DIR="${CARGO_TARGET_DIR%/}/fixtures/$(basename "$test_dir")"
            fi

            run_test "$test_dir"
            write_test_result_and_tally \
                "$result_file" "$(basename "$test_dir")" "$total_count"
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
    apply_shard
    local jobs="$PARSED_JOBS"
    set -- "${PARSED_POSITIONAL[@]}"

    echo ""
    echo -e "  ${BOLD}FlowLog Fixture Tests (binary mode)${NC}"
    echo ""

    ensure_compiler_built
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    local -a tasks=()
    mapfile -t tasks < <(test_dirs "$@")
    total=${#tasks[@]}
    if (( jobs > 1 )); then
        echo -e "  ${DIM}Running ${total} tests (parallel, -j ${jobs})...${NC}"
    else
        echo -e "  ${DIM}Running ${total} tests...${NC}"
    fi
    echo ""

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
