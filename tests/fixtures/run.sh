#!/usr/bin/env bash
set -euo pipefail

# FlowLog fixture runner: every fixture through both lowering paths, from
# one task queue.
#
#   compiler  compiler.sh: `flowlog-compiler` compiles `program.dl` to a
#             standalone binary, which reads CSVs and writes `output/<rel>`.
#   lib       lib.sh: a runner crate links `flowlog-build` +
#             `flowlog-runtime`; a synthesized `main.rs` loads the CSVs,
#             drives the engine, and writes the same files host-side.
#
# Both modes diff `output/` against `expected/`, and both must pass.
# common.sh holds discovery, the scheduler, and the comparison; the runners
# in tests/oracle/ stay separate.
#
# Layout:
#   tests/fixtures/<test_name>/
#     program.dl     Datalog source (must use .output directives)
#     data/          Optional CSV input facts
#     expected/      Expected output files (one per output relation)
#     commands.txt   Transaction transcript; present iff the program declares
#                    an `append` or `mutable` input, which makes the fixture
#                    incremental
#     runtime_flags  Optional runtime flags (e.g. -w 4 for multi-worker)
#     compile_flags  Optional compiler flags (e.g. --str-intern)
#     udf.rs         Optional user-defined functions
#     include_dirs   Optional `-I` directories, one per line
#
# Naming: an incremental fixture is `txn_*` (transaction shell mechanics),
# `mixed_*` (static and mutable inputs in one program), `append_*` (an
# append input), or `*_delta` (a batch feature re-checked per epoch). Static
# fixtures use none of these. The runner refuses a fixture whose name,
# `commands.txt`, and `.decl`s disagree.

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/compiler.sh"
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [-m MODE] [-j N] [--shard I/N] [test_name ...]

Run the FlowLog fixtures under tests/fixtures/<name>/ through the compiler
and library lowering paths, and diff their outputs against expected/.
Incremental fixtures (those with an \`append\` or \`mutable\` input and a
commands.txt transcript) are named txn_*, mixed_*, append_*, or *_delta.

Options:
  -m MODE         compiler, lib, or both (default both).
  -j N            Run up to N tasks in parallel (default 1). Each worker
                  keeps its own Cargo caches under target/e2e/slot-N, so the
                  runtime builds once per worker and mode, not once per fixture.
  --shard I/N     Run only shard I of N (the fixtures split into N groups).

Examples:
  $(basename "$0")                     # everything, one task at a time
  $(basename "$0") -j 8                # 8 workers
  $(basename "$0") -m lib agg_sum      # one fixture, library mode only
  $(basename "$0") --shard 1/8         # first of 8 shards, both modes
EOF
}

###############################################################################
# Entry point
###############################################################################

# One task, inside a worker. Each mode builds into its own Cargo target
# directory within the slot: the generated crates' rustflags differ from the
# runner crate's, so the two cannot share artifacts.
run_task() {
    local mode="$1" test_dir="$2"
    export CARGO_TARGET_DIR="${SLOT_DIR}/${mode}/cargo"

    local detail
    if ! detail=$(check_fixture_layout "$test_dir"); then
        fail_task "fixture layout" "$detail"
        return
    fi
    case "$mode" in
        compiler) run_compiler_task "$test_dir" ;;
        lib) run_lib_task "$test_dir" ;;
    esac
}

main() {
    parse_args "$@"
    apply_shard

    local -a modes=()
    [[ "$PARSED_MODE" == lib ]] || modes+=(compiler)
    [[ "$PARSED_MODE" == compiler ]] || modes+=(lib)

    echo ""
    echo -e "  ${BOLD}FlowLog Fixture Tests${NC}"
    echo ""

    # The named fixtures, or all of them.
    local -a dirs=()
    local name
    if (( ${#PARSED_POSITIONAL[@]} > 0 )); then
        for name in "${PARSED_POSITIONAL[@]}"; do
            [[ -f "${TESTS_DIR}/${name}/program.dl" ]] || die "Test not found: $name"
            dirs+=("${TESTS_DIR}/${name}")
        done
    else
        mapfile -t dirs < <(all_test_dirs)
    fi

    # SQLite fixtures need the compiler's I/O; library engines use host I/O.
    local -a tasks=()
    local test_dir mode
    for test_dir in "${dirs[@]}"; do
        for mode in "${modes[@]}"; do
            [[ "$mode" == lib && -f "$test_dir/sqlite_setup.sql" ]] && continue
            tasks+=("${mode}|${test_dir}")
        done
    done
    (( ${#tasks[@]} > 0 )) || { echo -e "  ${DIM}No tests to run.${NC}"; return 0; }

    local jobs="$PARSED_JOBS"
    (( jobs > ${#tasks[@]} )) && jobs=${#tasks[@]}

    [[ "$PARSED_MODE" == lib ]] || build_compiler
    echo -e "  ${DIM}Running ${#tasks[@]} tests (${modes[*]}, -j ${jobs})...${NC}"
    echo ""
    run_tasks "$jobs" "${tasks[@]}"

    echo ""
    print_summary "${tasks[@]}"
}

main "$@"
