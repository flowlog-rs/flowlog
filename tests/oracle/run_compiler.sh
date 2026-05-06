#!/bin/bash
set -euo pipefail

# FlowLog correctness test — datalog-batch mode, binary/compiler path.
#
# For each (program, dataset) pair in the configured config file:
#   1. Builds the FlowLog release workspace (if needed)
#   2. Downloads the dataset (if needed)
#   3. Prepares the .dl file: .printsize -> .output
#   4. Compiles & runs the FlowLog binary with output to files
#   5. Downloads pre-computed Souffle reference from HuggingFace
#   6. Sorts FlowLog output and diffs against Souffle reference
#   7. Reports pass/fail
#
# See `run_lib.sh` for the library-mode analog.

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

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

Runs FlowLog in datalog-batch mode (binary path) and compares output
against pre-computed Souffle reference results.

By default, runs both config_integer.txt and config_string.txt
(the latter with --str-intern). Pass a single config file to
run only that config.

Options:
  --sip                       Also test with --sip optimization.
  --keep-datasets             Don't delete datasets after each pair.
                              Required when <repo>/facts/ is a symlink.
  --workers <n>               Worker threads (default: 64).
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
    RESULT_DIR="$RESULT_DIR_DEFAULT"
    LOG_DIR="${RESULT_DIR}/logs"
    FLOWLOG_OUT_DIR="${RESULT_DIR}/flowlog_out"
    COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"

    mkdir -p "$LOG_DIR" "$FLOWLOG_OUT_DIR"
}

init_opt_flags() {
    OPT_FLAGS=("")
    OPT_LABELS=("")
    if (( ENABLE_SIP )); then
        OPT_FLAGS+=("--sip")
        OPT_LABELS+=("sip")
    fi
}

###############################################################################
# Build + compile helpers
###############################################################################

compile_release_workspace() {
    log "$YELLOW" "BUILD" "Compiling release workspace"
    pushd "$ROOT_DIR" >/dev/null
    cargo build --release >/dev/null
    popd >/dev/null
}

compute_flag_combo() {
    local oi="$1" str_intern_flag="${2:-}"
    COMBO_EXTRA_FLAGS="$(trim "${OPT_FLAGS[$oi]} ${str_intern_flag}")"

    local parts=""
    [[ -n "${OPT_LABELS[$oi]}" ]] && parts="${OPT_LABELS[$oi]}"
    COMBO_LABEL_SUFFIX="${parts:+_${parts}}"
}

invoke_compiler() {
    local prog_path="$1" facts_dir="$2" executable="$3" output_dir="$4" extra_flags="$5"

    log "$YELLOW" "COMPILE" "$COMPILER_BIN $prog_path -F $facts_dir -D $output_dir -o $executable --mode $MODE ${extra_flags}"
    local extra_arr=()
    [[ -n "$extra_flags" ]] && read -ra extra_arr <<< "$extra_flags"
    "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -D "$output_dir" -o "$executable" --mode "$MODE" "${extra_arr[@]}"
}

###############################################################################
# Per-entry test runner
###############################################################################

run_test() {
    local prog_name="$1" dataset_name="$2" str_intern_flag="${3:-}"
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

    for oi in "${!OPT_FLAGS[@]}"; do
        compute_flag_combo "$oi" "$str_intern_flag"
        local suffix="$COMBO_LABEL_SUFFIX" flags="$COMBO_EXTRA_FLAGS"

        local package_name executable log_file output_dir prepared_dl
        package_name="$(sanitize_package_name "${program_stem}_${dataset_name}_${MODE}${suffix}")"
        executable="${ROOT_DIR}/${package_name}"
        log_file="${LOG_DIR}/${program_stem}_${dataset_name}_${MODE}${suffix}.log"
        output_dir="${FLOWLOG_OUT_DIR}/${program_stem}_${dataset_name}${suffix}"
        prepared_dl="${STAGE_DIR}/flowlog_prepared_$$_${prog_file}"

        mkdir -p "$output_dir"

        log "$BLUE" "TEST" "$prog_file with $dataset_name (mode=$MODE${suffix})"

        prepare_dl_file "$prog_path" "$prepared_dl"

        rm -f "$executable"
        invoke_compiler "$prepared_dl" "$dataset_path" "$executable" "$output_dir" "$flags"
        [[ -x "$executable" ]] || die "Executable not found: $executable"

        log "$YELLOW" "RUN" "$executable -w $WORKERS"
        "$executable" -w "$WORKERS" 2>&1 | tee "$log_file"

        verify_output "$output_dir" "$ref_dir" \
            || die "Verification failed: $prog_file with $dataset_name${suffix}"

        rm -f "$executable" "${executable}_ops.json" "$prepared_dl"
        rm -rf "${ROOT_DIR}/.${package_name}.build"
        rm -rf "$output_dir"
    done

    rm -rf "$ref_dir"
    cleanup_dataset "$dataset_name" "$FACT_DIR" "$KEEP_DATASETS"
}

###############################################################################
# Entry point
###############################################################################

run_config() {
    local config_file="$1" label="$2" extra_flag="${3:-}"
    [[ -f "$config_file" ]] || { log "$YELLOW" "SKIP" "$label: config not found ($config_file)"; return 0; }
    log "$BLUE" "SUITE" "$label ($config_file)"
    for_each_config_entry run_test "$config_file" "$extra_flag"
}

main() {
    parse_args "$@"
    init_paths
    init_opt_flags
    log "$BLUE" "START" "FlowLog datalog-batch correctness test (compiler mode)"

    compile_release_workspace

    if [[ -n "$SINGLE_CONFIG" ]]; then
        local extra=""
        [[ "$(basename "$SINGLE_CONFIG")" == config_string* ]] && extra="--str-intern"
        run_config "$SINGLE_CONFIG" "custom" "$extra"
    else
        run_config "$CONFIG_INTEGER" "integer"
        run_config "$CONFIG_STRING"  "string" "--str-intern"
    fi

    log "$GREEN" "FINISH" "All tests passed"
}

main "$@"
