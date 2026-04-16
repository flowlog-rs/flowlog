#!/usr/bin/env bash
set -euo pipefail

# FlowLog library-mode end-to-end test runner.
#
# Mirrors `unit_compiler.sh`: same fixtures, same comparison helper, same
# directory layout. Instead of compiling a standalone binary that reads
# CSVs and writes files, this runner:
#
#   1. Copies the fixture's `program.dl` and `data/*.csv` into a persistent
#      runner crate at `target/e2e-lib/runner/`.
#   2. Synthesizes a `src/main.rs` that reads each input CSV, calls
#      `engine.insert_<rel>(..)`, runs the engine, and writes
#      `output/<rel>` files matching the binary-mode convention.
#   3. Runs `cargo run --release` on the runner crate.
#   4. Reuses `compare_expected_outputs` from `common.sh` to diff against
#      `expected/`.
#
# Batch-mode categories only — incremental fixtures (with `commands.txt`)
# are skipped. Extended syntax (loop / fixpoint blocks) is auto-detected
# by the pipeline, so `datalog-batch` and `extend-batch` share one entry.

CATEGORIES=(datalog-batch extend-batch)

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# Runner-crate location must be set before sourcing the synthesis helpers.
LIB_RUNNER_DIR="${ROOT_DIR}/target/e2e-lib/runner"
source "$(dirname "${BASH_SOURCE[0]}")/../lib_runner_synth.sh"

skipped=0

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [test_name ...]

Run FlowLog library-mode end-to-end tests against datalog-batch fixtures.

Each test directory under tests/unit/datalog-batch/<name>/ contains:
  program.dl      Datalog source using .input/.output directives
  data/           Input CSV files (filename matches relation name)
  expected/       Expected output files (one per relation)

Examples:
  $(basename "$0")                     # run all datalog-batch tests
  $(basename "$0") agg_sum             # run one test
EOF
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

    # Skip tests we don't support yet.
    if [[ -f "$test_dir/commands.txt" ]]; then
        ((skipped++)) || true
        return
    fi

    # extend-batch fixtures need `Builder::extended(true)`; plain datalog
    # fixtures must keep the fast-path `DatalogBatch` mode.
    if [[ "$category" == "extend-batch" ]]; then
        LIB_RUNNER_EXTENDED=1
    else
        LIB_RUNNER_EXTENDED=0
    fi

    # Stage fixture files into the runner crate. Wipe everything except
    # `src/` (synthesized below) and stale lib/ directories.
    rm -rf "${LIB_RUNNER_DIR}/data" "${LIB_RUNNER_DIR}/output" "${LIB_RUNNER_DIR}/program.dl" "${LIB_RUNNER_DIR}/lib"
    mkdir -p "${LIB_RUNNER_DIR}/data"
    cp "$test_dir/program.dl" "${LIB_RUNNER_DIR}/program.dl"
    rm -f "${LIB_RUNNER_DIR}/udf.rs"
    if [[ -f "$test_dir/udf.rs" ]]; then
        cp "$test_dir/udf.rs" "${LIB_RUNNER_DIR}/udf.rs"
    fi
    if [[ -d "$test_dir/data" ]]; then
        if compgen -G "$test_dir/data/*" > /dev/null; then
            cp "$test_dir/data/"* "${LIB_RUNNER_DIR}/data/"
        fi
    fi
    # Fixtures with .include directives: copy sibling directories other
    # than data/ and expected/ so relative includes resolve.
    local sibling
    for sibling in "$test_dir"/*/; do
        [[ -d "$sibling" ]] || continue
        local name
        name="$(basename "$sibling")"
        case "$name" in
            data|expected) continue ;;
        esac
        cp -r "$sibling" "${LIB_RUNNER_DIR}/${name}"
    done

    write_build_rs "$test_dir"
    if ! write_main_rs "${LIB_RUNNER_DIR}/program.dl" 2>"${LIB_RUNNER_DIR}/synth.log"; then
        local detail
        detail="$(tail -20 "${LIB_RUNNER_DIR}/synth.log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "main.rs synthesis failed" "$detail"
        return
    fi

    local build_log="${LIB_RUNNER_DIR}/build.log"
    local run_log="${LIB_RUNNER_DIR}/run.log"
    local lib_bin="${LIB_RUNNER_DIR}/target/release/flowlog_lib_runner"

    # Two-step: build (incremental) then launch the bare binary, so `cargo
    # run`'s ~300-500ms per-invocation graph-check overhead doesn't dominate
    # wall-clock for small fixtures.
    if ! (cd "${LIB_RUNNER_DIR}" && cargo build --release --quiet 2>"$build_log"); then
        local detail
        detail="$(tail -25 "$build_log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "lib build failed" "$detail"
        return
    fi
    # The synthesized main reads `data/<csv>` and writes `output/<rel>`
    # using paths relative to the runner crate dir.
    if ! (cd "${LIB_RUNNER_DIR}" && "$lib_bin" >"$run_log" 2>>"$build_log"); then
        local detail
        detail="$(tail -25 "$build_log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "lib run failed" "$detail"
        return
    fi

    # `use_sort=1`: lib mode output order is non-deterministic.
    local mismatch_detail
    if mismatch_detail=$(compare_expected_outputs "$test_dir" "${LIB_RUNNER_DIR}/output" 1); then
        ((passed++)) || true
    else
        record_failure "$full_name" "output mismatch" "$mismatch_detail"
    fi
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
    echo -e "  ${BOLD}FlowLog Unit Tests (library mode)${NC}"
    echo ""

    ensure_runner_crate

    # Warm-up build to populate the cargo cache before per-test timing kicks in.
    echo -e "  ${YELLOW}Warming runner crate (release)...${NC}"
    write_build_rs ""
    cat > "${LIB_RUNNER_DIR}/program.dl" <<'EOF'
.decl Source(id: int32)
.input Source()
.decl Edge(x: int32, y: int32)
.input Edge()
.decl Reach(id: int32)
Reach(y) :- Source(y).
Reach(y) :- Reach(x), Edge(x, y).
.output Reach
EOF
    cat > "${LIB_RUNNER_DIR}/src/main.rs" <<'EOF'
pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}
fn main() {}
EOF
    (cd "${LIB_RUNNER_DIR}" || exit; cargo build --release --quiet 2>&1 | tail -5) \
        || die "warm-up build failed"

    total=$(count_tests "$@")
    echo -e "  ${DIM}Running ${total} tests...${NC}"
    echo ""

    if [[ $# -gt 0 ]]; then
        local name
        for name in "$@"; do
            local cat
            cat="$(find_test "$name")" || die "Test not found: $name"
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
    if (( skipped > 0 )); then
        echo -e "  ${DIM}(${skipped} tests skipped — incremental / UDF / not yet supported)${NC}"
    fi

    echo ""
    (( failed == 0 ))
}

main "$@"
