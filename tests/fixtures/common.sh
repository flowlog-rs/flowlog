#!/usr/bin/env bash
#
# Helpers for the fixture runner, `run.sh`: fixture discovery, the layout
# check, the worker scheduler, output comparison, and the summary. The
# runner defines `run_task <mode> <fixture_dir>`, which ends with `pass_task`
# or `fail_task`, and the scheduler calls it from worker subshells.
#
# Not executable on its own.

source "$(dirname "${BASH_SOURCE[0]}")/../lib/shared.sh"

readonly TESTS_DIR="${ROOT_DIR}/tests/fixtures"

# Cargo's target directory, honoring a user-set CARGO_TARGET_DIR. Everything
# a run leaves behind lives under `$TARGET_DIR/e2e`.
readonly TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT_DIR}/target}"
readonly E2E_DIR="${TARGET_DIR}/e2e"

export RUST_LOG=error

###############################################################################
# Fixture discovery
###############################################################################

# Echo the directory of every fixture, sorted by name.
all_test_dirs() {
    local test_dir
    for test_dir in "$TESTS_DIR"/*/; do
        [[ -f "$test_dir/program.dl" ]] || continue
        echo "${test_dir%/}"
    done
}

# Three things say a fixture is incremental, and they must agree: its
# program declares an `append` or `mutable` input, it ships a `commands.txt`
# transcript, and its name is `txn_*`, `mixed_*`, `append_*`, or `*_delta`.
# Echoes what disagrees and returns 1.
check_fixture_layout() {
    local test_dir="$1"
    local name
    name="$(basename "$test_dir")"
    local has_dynamic=0 has_commands=0 has_name=0
    # `//` comments may mention the keyword without declaring anything.
    if find "$test_dir" -name '*.dl' -exec sed 's|//.*||' {} + | grep -qwE 'append|mutable'; then
        has_dynamic=1
    fi
    [[ -f "$test_dir/commands.txt" ]] && has_commands=1
    [[ "$name" =~ ^(txn|mixed|append)_|_delta$ ]] && has_name=1
    (( has_dynamic == has_commands && has_commands == has_name )) && return 0

    local -a facts=()
    (( has_dynamic )) && facts+=("declares an append or mutable input") \
        || facts+=("declares no append or mutable input")
    (( has_commands )) && facts+=("has commands.txt") || facts+=("has no commands.txt")
    (( has_name )) && facts+=("is named like an incremental fixture") \
        || facts+=("is not named txn_*, mixed_*, append_*, or *_delta")
    printf '      %s\n' "${facts[@]}"
    return 1
}

###############################################################################
# Command line
###############################################################################

# Parse `[-m MODE] [-j N] [--shard I/N] [-h] [--] [name…]` into
# `PARSED_MODE`, `PARSED_JOBS`, `PARSED_SHARD` ("I/N" or empty), and
# `PARSED_POSITIONAL`. `-h` prints `usage` and exits 0.
PARSED_MODE=both
PARSED_JOBS=1
PARSED_SHARD=""
PARSED_POSITIONAL=()
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help) usage; exit 0 ;;
            -m|--mode) PARSED_MODE="${2:?}"; shift 2 ;;
            -m*) PARSED_MODE="${1#-m}"; shift ;;
            --mode=*) PARSED_MODE="${1#--mode=}"; shift ;;
            -j) PARSED_JOBS="${2:?}"; shift 2 ;;
            -j*) PARSED_JOBS="${1#-j}"; shift ;;
            --shard) PARSED_SHARD="${2:?}"; shift 2 ;;
            --shard=*) PARSED_SHARD="${1#--shard=}"; shift ;;
            --) shift; PARSED_POSITIONAL+=("$@"); break ;;
            -*) die "Unknown option: $1 (see -h)" ;;
            *) PARSED_POSITIONAL+=("$1"); shift ;;
        esac
    done
    [[ "$PARSED_MODE" =~ ^(compiler|lib|both)$ ]] \
        || die "Invalid -m value: $PARSED_MODE (expected compiler, lib, or both)"
    [[ "$PARSED_JOBS" =~ ^[1-9][0-9]*$ ]] \
        || die "Invalid -j value: $PARSED_JOBS (expected positive integer)"
    [[ -z "$PARSED_SHARD" || "$PARSED_SHARD" =~ ^[1-9][0-9]*/[1-9][0-9]*$ ]] \
        || die "Invalid --shard value: $PARSED_SHARD (expected I/N)"
}

# When `--shard I/N` was given, narrow `PARSED_POSITIONAL` to every Nth
# fixture of the sorted list. Lets CI fan the suite across runners without
# naming fixtures; a shard is just a subset of the usual named-test path.
apply_shard() {
    [[ -n "$PARSED_SHARD" ]] || return 0
    (( ${#PARSED_POSITIONAL[@]} == 0 )) \
        || die "--shard cannot be combined with explicit test names"
    local index="${PARSED_SHARD%/*}" total="${PARSED_SHARD#*/}"
    (( index >= 1 && index <= total )) || die "--shard out of range: $PARSED_SHARD"
    local i=0 test_dir
    while IFS= read -r test_dir; do
        (( i % total == index - 1 )) && PARSED_POSITIONAL+=("$(basename "$test_dir")")
        (( i++ )) || true
    done < <(all_test_dirs)
}

###############################################################################
# Scheduler
###############################################################################

# A task is `<mode>|<fixture_dir>` (fixture names are plain slugs, so `|`
# never occurs in one). `jobs` workers pull tasks from one queue. Each worker
# owns a slot, `$E2E_DIR/slot-<i>`, and runs its tasks one after another;
# `run_task` keeps a Cargo target directory per mode inside the slot, so the
# runtime and its dependencies build once per slot and mode, and every later
# task there compiles only its own crate. Slots survive between runs; a run
# removes the slots beyond its `-j`, since they would only go stale.
#
# A worker claims task `i` by creating `$RESULTS_DIR/claim/<i>` (`mkdir` is
# atomic, so no two workers get the same one) and leaves its verdict in
# `$RESULTS_DIR/<i>`; the parent reads the verdicts back once the workers
# are done.
RESULTS_DIR=""
RESULT_FILE=""

task_label() {
    local task="$1"
    echo "${task%%|*}/$(basename "${task#*|}")"
}

run_tasks() {
    local jobs="$1"; shift
    local -a tasks=("$@")

    RESULTS_DIR="${E2E_DIR}/.results"
    rm -rf "$RESULTS_DIR"
    mkdir -p "$RESULTS_DIR/claim"
    local slot_dir n
    for slot_dir in "$E2E_DIR"/slot-*/; do
        [[ -d "$slot_dir" ]] || continue
        n="${slot_dir%/}"; n="${n##*-}"
        (( n >= jobs )) && rm -rf "$slot_dir"
    done

    local slot
    for ((slot = 0; slot < jobs; slot++)); do
        run_worker "$slot" "${tasks[@]}" &
    done
    wait
}

run_worker() {
    local slot="$1"; shift
    local -a tasks=("$@")
    (
        SLOT_DIR="${E2E_DIR}/slot-${slot}"
        mkdir -p "$SLOT_DIR"

        local i
        for ((i = 0; i < ${#tasks[@]}; i++)); do
            mkdir "${RESULTS_DIR}/claim/${i}" 2>/dev/null || continue
            RESULT_FILE="${RESULTS_DIR}/$(printf '%04d' "$i")"
            run_task "${tasks[$i]%%|*}" "${tasks[$i]#*|}"
            print_result_line "$(task_label "${tasks[$i]}")" "${#tasks[@]}"
        done
    )
}

# Verdicts. `run_task` ends with exactly one of these.
pass_task() {
    printf 'PASS\n' > "$RESULT_FILE"
}

fail_task() {
    local reason="$1" detail="${2:-}"
    printf 'FAIL\n%s\n%s' "$reason" "$detail" > "$RESULT_FILE"
}

# One `[n/total] ✓/✗ mode/name` line as each task finishes. `n` counts the
# verdict files written so far, across every worker.
print_result_line() {
    local label="$1" total="$2"
    local n mark color
    n=$(find "$RESULTS_DIR" -maxdepth 1 -type f | wc -l)
    if [[ "$(head -n1 "$RESULT_FILE")" == PASS ]]; then
        mark="✓"; color="${GREEN}"
    else
        mark="✗"; color="${RED}"
    fi
    printf "  ${DIM}[%d/%d]${NC} ${color}%s${NC} %s\n" "$n" "$total" "$mark" "$label"
}

# Read every verdict back, print the summary, and return the failure count.
print_summary() {
    local -a tasks=("$@")
    local passed=0 failed=0
    local i result_file status
    for ((i = 0; i < ${#tasks[@]}; i++)); do
        result_file="${RESULTS_DIR}/$(printf '%04d' "$i")"
        status="$(head -n1 "$result_file" 2>/dev/null || true)"
        if [[ "$status" == PASS ]]; then
            ((passed++)) || true
            continue
        fi
        ((failed++)) || true
        if (( failed == 1 )); then
            echo -e "  ${RED}${BOLD}Failures:${NC}"
            echo ""
        fi
        if [[ "$status" == FAIL ]]; then
            echo -e "  ${RED}✗${NC} ${BOLD}$(task_label "${tasks[$i]}")${NC} — $(sed -n 2p "$result_file")"
            sed -n '3,$p' "$result_file"
        else
            echo -e "  ${RED}✗${NC} ${BOLD}$(task_label "${tasks[$i]}")${NC} — no verdict (worker died?)"
        fi
        echo ""
    done

    if (( failed == 0 )); then
        echo -e "  ${GREEN}${BOLD}✓ All ${passed} tests passed${NC}"
    else
        echo -e "  ${GREEN}${passed} passed${NC}  ${RED}${BOLD}${failed} failed${NC}  ${DIM}(${#tasks[@]} total)${NC}"
    fi
    return "$failed"
}

###############################################################################
# Output comparison
###############################################################################

# Diff every `expected/<name>` against `<output_dir>/<name>`. `use_sort`
# compares as sorted lines, for outputs whose row order is not pinned.
# `skip_names` (space-separated) leaves expectations one mode cannot
# produce uncompared: `.printsize` prints a line on stdout in compiler mode,
# while library mode exposes the count as a typed `<rel>_size` field.
# Echoes the mismatch report and returns 1 when anything differs.
compare_expected_outputs() {
    local test_dir="$1" output_dir="$2" use_sort="${3:-0}" skip_names="${4:-}"

    local all_match=1 diff_detail=""
    local expected_file rel_name actual_file diff_out
    for expected_file in "$test_dir"/expected/*; do
        rel_name="$(basename "$expected_file")"
        [[ " $skip_names " == *" $rel_name "* ]] && continue
        actual_file="${output_dir}/${rel_name}"

        if [[ ! -f "$actual_file" ]]; then
            all_match=0
            diff_detail+="      Relation '${rel_name}': output file missing\n"
            continue
        fi

        if (( use_sort )); then
            diff_out=$(diff \
                --label "expected/${rel_name}" <(sort "$expected_file") \
                --label "actual/${rel_name}"   <(sort "$actual_file") 2>&1) || true
        else
            diff_out=$(diff \
                --label "expected/${rel_name}" "$expected_file" \
                --label "actual/${rel_name}"   "$actual_file" 2>&1) || true
        fi
        [[ -n "$diff_out" ]] || continue

        all_match=0
        diff_detail+="      Relation '${rel_name}': expected $(wc -l < "$expected_file") rows, got $(wc -l < "$actual_file")\n"
        diff_detail+="$(echo "$diff_out" | head -20 | sed 's/^/         /')\n"
    done

    (( all_match )) && return 0
    echo -e "$diff_detail"
    return 1
}

# The last lines of a log, indented for a failure report.
log_tail() {
    tail -n "${2:-20}" "$1" 2>/dev/null | sed 's/^/         /'
}
