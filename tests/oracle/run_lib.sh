#!/bin/bash
set -euo pipefail

# FlowLog correctness test — datalog-batch mode, library path.
#
# Mirrors `run_compiler.sh` but drives lib mode: instead of
# compiling a standalone executable with `flowlog-compiler`, synthesize a
# Rust runner crate that uses `flowlog-build`, parse input CSVs into the
# generated `rel::*` structs, run the engine, and write outputs.
#
# For each (program, dataset) pair in the configured config file:
#   1. Builds the FlowLog release workspace (warms the runner cache)
#   2. Downloads the dataset (if needed)
#   3. Prepares the .dl file: .printsize -> .output
#      (lib mode treats .printsize as a count; the Souffle reference
#       expects tuples, so rewrite so BatchResults carries the rows)
#   4. Stages prog + CSVs into the persistent runner crate
#   5. Synthesizes build.rs / main.rs (via tests/lib/runner_synth.sh)
#   6. cargo run --release with WORKERS env plumbed into .workers(n)
#   7. Downloads Souffle reference and diffs via verify_output

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

LIB_RUNNER_DIR="${ROOT_DIR}/target/e2e-complex-lib/runner"
source "$(dirname "${BASH_SOURCE[0]}")/../lib/runner_synth.sh"

###############################################################################
# Configuration
###############################################################################

MODE="datalog-batch"
ENABLE_SIP=0
SINGLE_CONFIG=""
KEEP_DATASETS=0
WORKERS=64
SOUFFLE_REF_CACHE=""

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [config_file] [options]

Runs FlowLog in datalog-batch mode (library path) and compares output
against pre-computed Souffle reference results.

By default, runs tests/oracle/config.txt; pass a single config file
path to run a different one. Per-entry tags in the config select
extra runner behavior (e.g. `str-intern` enables
Builder::string_intern(true)).

Options:
  --sip                       Also test with Builder::sip(true).
  --keep-datasets             Don't delete datasets after each pair.
                              Required when <repo>/facts/ is a symlink.
  --workers <n>               Workers forwarded to
                              DatalogBatchEngine::workers (default: 64).
  --souffle-ref-cache <path>  Local cache of Souffle reference tarballs;
                              if present, cp instead of wget.

Examples:
  $(basename "$0")
  $(basename "$0") path/to/config.txt --keep-datasets --workers 32
EOF
}

parse_args() {
    while (( $# )); do
        case "$1" in
            --sip)               ENABLE_SIP=1 ;;
            --keep-datasets)     KEEP_DATASETS=1 ;;
            --workers)           shift; WORKERS="${1:?--workers requires a value}" ;;
            --souffle-ref-cache) shift; SOUFFLE_REF_CACHE="${1:?--souffle-ref-cache requires a path}" ;;
            -h|--help)           usage; exit 0 ;;
            --)                  shift; break ;;
            -*)                  die "Unknown option: $1 (try --help)" ;;
            *)
                [[ -z "$SINGLE_CONFIG" ]] || die "Unexpected extra argument: $1"
                SINGLE_CONFIG="$1"
                ;;
        esac
        shift
    done
}

init_paths() {
    PROG_DIR="$PROG_DIR_DEFAULT"
    FACT_DIR="$FACT_DIR_DEFAULT"
    export WORKERS
}

init_opt_flags() {
    # Each entry is a space-separated list of Builder knob env-vars to set
    # before sourcing `write_build_rs`. An empty string means "defaults".
    OPT_KNOBS=("")
    OPT_LABELS=("")
    if (( ENABLE_SIP )); then
        OPT_KNOBS+=("LIB_RUNNER_SIP=1")
        OPT_LABELS+=("sip")
    fi
}

###############################################################################
# Build + synth helpers
###############################################################################

compile_release_workspace() {
    log "$YELLOW" "BUILD" "Compiling release workspace"
    pushd "$ROOT_DIR" >/dev/null
    cargo build --release >/dev/null
    popd >/dev/null
}

# Drop a minimal program + main.rs into the runner crate and build once.
# Populates the cargo cache so per-test timing reflects incremental builds.
warm_runner_crate() {
    log "$YELLOW" "WARMUP" "Warming runner crate at $LIB_RUNNER_DIR"
    ensure_runner_crate

    # Clear fixture state from any prior run.
    rm -rf "${LIB_RUNNER_DIR}/data" "${LIB_RUNNER_DIR}/output" "${LIB_RUNNER_DIR}/program.dl"
    mkdir -p "${LIB_RUNNER_DIR}/data"

    LIB_RUNNER_SIP=0 LIB_RUNNER_STR_INTERN=0 write_build_rs ""
    cat > "${LIB_RUNNER_DIR}/program.dl" <<'EOF'
.decl Edge(x: int32, y: int32)
.input Edge()
.decl Reach(x: int32, y: int32)
Reach(x, y) :- Edge(x, y).
Reach(x, y) :- Reach(x, z), Edge(z, y).
.output Reach
EOF
    cat > "${LIB_RUNNER_DIR}/src/main.rs" <<'EOF'
pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}
fn main() {}
EOF
    (cd "${LIB_RUNNER_DIR}" && cargo build --release --quiet 2>&1 | tail -5) \
        || die "warm-up build failed"
}

###############################################################################
# Per-entry test runner
###############################################################################

# Stage the fixture into the runner crate: fresh data/, rewritten program.dl,
# and a copy of each input CSV. Uses per-call `cp -r` to avoid cross-test
# contamination.
stage_fixture() {
    local prepared_dl="$1" dataset_path="$2"

    rm -rf "${LIB_RUNNER_DIR}/data" "${LIB_RUNNER_DIR}/output"
    mkdir -p "${LIB_RUNNER_DIR}/data"
    cp "$prepared_dl" "${LIB_RUNNER_DIR}/program.dl"
    # Datasets ship inputs as .csv/.tsv (graph/program analysis) or .facts
    # (DOOP/batik). Stage every recognized extension — copying only .csv left
    # .facts inputs unstaged, running the engine on empty inputs.
    for ext in csv tsv facts; do
        if compgen -G "${dataset_path}/*.${ext}" > /dev/null; then
            cp "${dataset_path}"/*."${ext}" "${LIB_RUNNER_DIR}/data/"
        fi
    done
}

# Apply a space-separated `KEY=VAL` knob list before calling write_build_rs,
# so sip / string_intern can be combined per configuration.
write_build_rs_with_knobs() {
    local knob_list="$1"
    local str_intern="$2"  # 1 or 0 from the config suite

    local sip=0
    for kv in $knob_list; do
        case "$kv" in
            LIB_RUNNER_SIP=1)        sip=1 ;;
            LIB_RUNNER_STR_INTERN=1) str_intern=1 ;;
        esac
    done

    LIB_RUNNER_SIP="$sip" LIB_RUNNER_STR_INTERN="$str_intern" write_build_rs ""
}

run_test() {
    local prog_name="$1" dataset_name="$2" tags="${3:-}"
    local str_intern_config=0
    for t in $tags; do
        case "$t" in
            str-intern) str_intern_config=1 ;;
            *) die "Unknown tag in config: '$t' (entry: $prog_name=$dataset_name)" ;;
        esac
    done
    local prog_file prog_path program_stem
    prog_file="$(basename "$prog_name")"
    prog_path="${PROG_DIR}/${prog_name}"
    program_stem="${prog_file%.*}"

    [[ -f "$prog_path" ]] || die "Program not found: $prog_path"

    setup_dataset "$dataset_name" "$FACT_DIR"
    local ref_dir
    ref_dir="$(download_souffle_ref "$program_stem" "$dataset_name" "$SOUFFLE_REF_CACHE")"

    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    local prepared_dl="${STAGE_DIR}/flowlog_prepared_$$_${prog_file}"
    prepare_dl_file "$prog_path" "$prepared_dl"

    for oi in "${!OPT_KNOBS[@]}"; do
        local knobs="${OPT_KNOBS[$oi]}"
        local label="${OPT_LABELS[$oi]}"
        local suffix="${label:+_${label}}"

        log "$BLUE" "TEST" "$prog_file with $dataset_name (mode=lib${suffix})"

        stage_fixture "$prepared_dl" "$dataset_path"
        write_build_rs_with_knobs "$knobs" "$str_intern_config"
        write_main_rs "${LIB_RUNNER_DIR}/program.dl"

        local run_log="${LIB_RUNNER_DIR}/run.log"
        local lib_bin="${LIB_RUNNER_DIR}/target/release/flowlog_lib_runner"

        log "$YELLOW" "BUILD" "cargo build --release  (lib runner)"
        (cd "${LIB_RUNNER_DIR}" && cargo build --release --quiet) \
            || die "lib build failed: $prog_file with $dataset_name${suffix}"

        log "$YELLOW" "RUN" "$lib_bin  (WORKERS=$WORKERS)"
        # Synthesized main reads `data/*.csv` / writes `output/*` relative
        # to the runner crate dir.
        (cd "${LIB_RUNNER_DIR}" && "$lib_bin" 2>&1 | tee "$run_log") \
            || die "lib run failed: $prog_file with $dataset_name${suffix}"

        verify_output "${LIB_RUNNER_DIR}/output" "$ref_dir" \
            || die "Verification failed: $prog_file with $dataset_name${suffix}"

        rm -rf "${LIB_RUNNER_DIR}/output"
    done

    rm -f "$prepared_dl"
    rm -rf "$ref_dir"
    cleanup_dataset "$dataset_name" "$FACT_DIR" "$KEEP_DATASETS"
}

###############################################################################
# Entry point
###############################################################################

run_config() {
    local config_file="$1" label="$2"
    [[ -f "$config_file" ]] || { log "$YELLOW" "SKIP" "$label: config not found ($config_file)"; return 0; }
    log "$BLUE" "SUITE" "$label ($config_file)"
    for_each_config_entry run_test "$config_file"
}

main() {
    parse_args "$@"
    init_paths
    init_opt_flags
    log "$BLUE" "START" "FlowLog datalog-batch correctness test (library mode)"

    compile_release_workspace
    warm_runner_crate

    local config="${SINGLE_CONFIG:-$CONFIG_DEFAULT}"
    run_config "$config" "oracle"

    log "$GREEN" "FINISH" "All tests passed"
}

main "$@"
