#!/usr/bin/env bash
#
# Shared helpers for FlowLog fixture-level (L1) test runners.
#
# Sourced by:
#   tests/fixtures/run_compiler.sh — binary mode runner
#   tests/fixtures/run_lib.sh      — library mode runner
#
# Pulls generic helpers (colors, log, die, trim) from tests/lib/shared.sh,
# and layers fixture-specific bits on top: progress bar, test
# discovery across `tests/fixtures/<category>/<name>/`, output comparison
# against `expected/`, and a failure-list summary printer.
#
# Not executable on its own — defines functions and globals; the runner
# script is responsible for invoking `main`.

source "$(dirname "${BASH_SOURCE[0]}")/../lib/shared.sh"

###############################################################################
# Fixture-test globals
###############################################################################

readonly TESTS_DIR="${ROOT_DIR}/tests/fixtures"

# Each runner declares its own CATEGORIES before sourcing this file —
# common.sh defines no default. extend-inc is intentionally absent from
# both runners today (no fixtures exist; lib mode doesn't yet support
# the mode); add it to both runners when those gaps close.
[[ -n "${CATEGORIES+x}" ]] || die "CATEGORIES must be set by the runner before sourcing common.sh"
readonly -a CATEGORIES

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
# Presentation helpers
###############################################################################

# Whether stdout is a terminal (enables progress bar)
if [[ -t 1 ]]; then
    IS_TTY=1
else
    IS_TTY=0
fi

# Print progress bar on a single line (overwritten each call)
show_progress() {
    local test_label="$1"
    if (( IS_TTY )); then
        local pct=0
        (( total > 0 )) && pct=$(( current * 100 / total ))
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

clear_progress() {
    if (( IS_TTY )); then
        printf "${CLEAR_LINE}\r"
    fi
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
# Test discovery
###############################################################################

# Find a test by name across all categories. Echoes the category that
# contains it. Returns 1 if not found.
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

# Count tests across the configured categories. If named tests are passed as
# arguments, count those instead.
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
# Summary printer
###############################################################################

print_summary() {
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
}
