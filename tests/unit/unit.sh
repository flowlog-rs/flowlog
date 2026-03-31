#!/usr/bin/env bash
set -euo pipefail

# FlowLog end-to-end test runner.
#
# Layout:
#   tests/<category>/              Category determines --mode flag:
#     datalog-batch  → (default)     datalog-inc  → --mode datalog-inc
#     extend-batch   → --mode extend-batch
#     extend-inc     → --mode extend-inc
#
#   tests/<category>/<test_name>/
#     program.dl     Datalog source (must use .output directives)
#     data/          Optional CSV input facts copied into generated project
#     expected/      Expected output files (one per output relation)
#     commands.txt   Optional incremental transcript (enables incremental mode)
#     runtime_flags  Optional runtime flags (e.g. -w 4 for multi-worker)
#
# Usage:
#   tests/run.sh                           # run all tests in all categories
#   tests/run.sh <test_name> [test_name ...] # run specific tests (searched across categories)

###############################################################################
# Constants & globals
###############################################################################

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly BOLD='\033[1m'
readonly DIM='\033[2m'
readonly NC='\033[0m'
readonly CLEAR_LINE='\033[2K'

readonly ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
readonly TESTS_DIR="${ROOT_DIR}/tests/unit"
readonly COMPILER_BIN="${ROOT_DIR}/target/release/flowlog"
readonly BUILD_DIR="${ROOT_DIR}/target/e2e"

readonly -a CATEGORIES=(datalog-batch datalog-inc extend-batch extend-inc)

passed=0
failed=0
total=0
current=0

# Failure details: each entry is "test_name\treason\tdetail"
declare -a failure_names=()
declare -a failure_reasons=()
declare -a failure_details=()

export RUST_LOG=error

###############################################################################
# Logging / presentation helpers
###############################################################################

# Whether stdout is a terminal (enables progress bar)
if [[ -t 1 ]]; then
    IS_TTY=1
else
    IS_TTY=0
fi

log() {
    local color="$1"
    local tag="$2"
    shift 2
    echo -e "${color}[${tag}]${NC} $*"
}

die() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
    exit 1
}

# Print progress bar on a single line (overwritten each call)
show_progress() {
    local test_label="$1"
    if (( IS_TTY )); then
        local pct=0
        (( total > 0 )) && pct=$(( current * 100 / total ))
        # Build bar: [████████░░░░░░░░░░░░] 45% (23/51) running: test_name
        local bar_width=20
        local filled=$(( pct * bar_width / 100 ))
        local empty=$(( bar_width - filled ))
        local bar=""
        for ((i=0; i<filled; i++)); do bar+="█"; done
        for ((i=0; i<empty; i++)); do bar+="░"; done
        printf "${CLEAR_LINE}\r  ${DIM}[${bar}]${NC} ${BOLD}%3d%%${NC} ${DIM}(%d/%d)${NC} %s" \
            "$pct" "$current" "$total" "$test_label"
    fi
}

# Clear the progress line
clear_progress() {
    if (( IS_TTY )); then
        printf "${CLEAR_LINE}\r"
    fi
}

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [test_name ...]

Run FlowLog end-to-end tests. Tests are organized by category:
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

indent_file() {
    local file="$1"
    sed 's/^/         /' "$file"
}

tail_and_indent() {
    local file="$1"
    local lines="${2:-20}"
    tail -"${lines}" "$file" | sed 's/^/         /'
}

record_failure() {
    local test_name="$1"
    local reason="$2"
    local detail="${3:-}"
    failure_names+=("$test_name")
    failure_reasons+=("$reason")
    failure_details+=("$detail")
    ((failed++)) || true
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
    (cd "$ROOT_DIR" && cargo build --release -p compiler 2>&1 | tail -1)
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
# Output comparison
###############################################################################

compare_expected_outputs() {
    local test_dir="$1"
    local output_dir="$2"
    local use_sort="${3:-0}"

    local all_match=1
    local diff_detail=""
    local expected_file

    for expected_file in "$test_dir"/expected/*; do
        local rel_name
        local actual_file
        rel_name="$(basename "$expected_file")"
        actual_file="${output_dir}/${rel_name}"

        if [[ ! -f "$actual_file" ]]; then
            all_match=0
            diff_detail+="      Relation '${rel_name}': output file missing\n"
            continue
        fi

        local diff_out
        if (( use_sort )); then
            diff_out=$(diff \
                --label "expected/${rel_name}" <(sort "$expected_file") \
                --label "actual/${rel_name}"   <(sort "$actual_file") 2>&1) || true
        else
            diff_out=$(diff \
                --label "expected/${rel_name}" "$expected_file" \
                --label "actual/${rel_name}"   "$actual_file" 2>&1) || true
        fi

        if [[ -n "$diff_out" ]]; then
            all_match=0
            local exp_count act_count
            exp_count=$(wc -l < "$expected_file")
            act_count=$(wc -l < "$actual_file")
            diff_detail+="      Relation '${rel_name}': expected ${exp_count} rows, got ${act_count}\n"
            diff_detail+="$(echo "$diff_out" | head -20 | sed 's/^/         /')\n"
        fi
    done

    if (( all_match )); then
        return 0
    fi

    echo -e "$diff_detail"
    return 1
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

    # Per-test compiler flags (e.g. --str-intern)
    if [[ -f "$test_dir/compile_flags" ]]; then
        mapfile -t extra_compile_flags < "$test_dir/compile_flags"
        compile_flags+=("${extra_compile_flags[@]}")
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
# Find a test by name across all categories
###############################################################################

find_test() {
    local name="$1"
    for cat in "${CATEGORIES[@]}"; do
        local dir="${TESTS_DIR}/${cat}/${name}"
        if [[ -d "$dir" && -f "$dir/program.dl" ]]; then
            echo "$cat"
            return 0
        fi
    done
    return 1
}

###############################################################################
# Count tests
###############################################################################

count_tests() {
    local count=0
    if [[ $# -gt 0 ]]; then
        count=$#
    else
        for cat in "${CATEGORIES[@]}"; do
            local cat_dir="${TESTS_DIR}/${cat}"
            [[ -d "$cat_dir" ]] || continue
            for test_dir in "$cat_dir"/*/; do
                [[ -f "$test_dir/program.dl" ]] || continue
                ((count++)) || true
            done
        done
    fi
    echo "$count"
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
    echo -e "  ${BOLD}FlowLog Unit Tests${NC}"
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

    # Summary
    if (( failed == 0 )); then
        echo -e "  ${GREEN}${BOLD}✓ All ${passed} tests passed${NC}"
    else
        echo -e "  ${GREEN}${passed} passed${NC}  ${RED}${BOLD}${failed} failed${NC}  ${DIM}(${total} total)${NC}"
        echo ""
        echo -e "  ${RED}${BOLD}Failures:${NC}"
        echo ""
        for ((i=0; i<${#failure_names[@]}; i++)); do
            echo -e "  ${RED}✗${NC} ${BOLD}${failure_names[$i]}${NC} — ${failure_reasons[$i]}"
            if [[ -n "${failure_details[$i]}" ]]; then
                echo -e "${failure_details[$i]}"
            fi
            echo ""
        done
    fi

    echo ""
    (( failed == 0 ))
}

main "$@"
