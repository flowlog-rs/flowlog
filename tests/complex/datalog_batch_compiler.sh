#!/bin/bash
set -euo pipefail

# FlowLog correctness test — datalog-batch mode.
#
# For each (program, dataset) pair in the config:
#   1. Builds the FlowLog compiler (if needed)
#   2. Downloads the dataset (if needed)
#   3. Prepares the .dl file: .printsize -> .output
#   4. Compiles & runs FlowLog with output to files
#   5. Downloads pre-computed Souffle reference from HuggingFace
#   6. Sorts FlowLog output and diffs against Souffle reference
#   7. Reports pass/fail

###############################################################################
# Constants & globals
###############################################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="datalog-batch"
ENABLE_SIP=0

# Config files: integer (no extra flags), string (--str-intern)
CONFIG_INTEGER="${SCRIPT_DIR}/config_integer.txt"
CONFIG_STRING="${SCRIPT_DIR}/config_string.txt"
# When a single config is passed via CLI, only that config is run.
SINGLE_CONFIG=""

# Staging directory: prefer /dev/shm (tmpfs) for speed, fall back to $TMPDIR or /tmp
if [[ -d /dev/shm && -w /dev/shm ]]; then
    STAGE_DIR="/dev/shm"
else
    STAGE_DIR="${TMPDIR:-/tmp}"
fi

SOUFFLE_BASE_URL="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/test/datalog-batch"

###############################################################################
# Logging / utilities
###############################################################################

log() { local color="$1" tag="$2"; shift 2; echo -e "${color}[${tag}]${NC} $*"; }
die() { log "$RED" "ERROR" "$*"; exit 1; }

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [config_file] [--sip]

Runs FlowLog in datalog-batch mode and compares output against
pre-computed Souffle reference results.

By default, runs both config_integer.txt and config_string.txt
(the latter with --str-intern). Pass a single config file to
run only that config.

Options:
  --sip            Also test with --sip optimization.

Environment:
  WORKERS=<n>    Number of workers (default: 64)

Examples:
  $(basename "$0")
  $(basename "$0") --sip
  $(basename "$0") path/to/config.txt
EOF
}

trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

sanitize_package_name() {
    local s
    s="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '_')"
    s="$(printf '%s' "$s" | sed 's/_\{2,\}/_/g; s/-\{2,\}/-/g; s/^[-_]//; s/[-_]$//')"
    [[ -z "$s" ]] && s="flowlog_output"
    [[ "$s" =~ ^[0-9] ]] && s="flowlog_${s}"
    printf '%s' "$s"
}

###############################################################################
# Argument parsing & path setup
###############################################################################

parse_args() {
    while (( $# )); do
        case "$1" in
            --sip)         ENABLE_SIP=1 ;;
            -h|--help)     usage; exit 0 ;;
            --)            shift; break ;;
            -*)            die "Unknown option: $1 (try --help)" ;;
            *)
                [[ -z "$SINGLE_CONFIG" ]] || die "Unexpected extra argument: $1"
                SINGLE_CONFIG="$1"
                ;;
        esac
        shift
    done
}

init_paths() {
    PROG_DIR="${ROOT_DIR}/example"
    FACT_DIR="${ROOT_DIR}/facts"
    RESULT_DIR="${ROOT_DIR}/result"
    LOG_DIR="${RESULT_DIR}/logs"
    FLOWLOG_OUT_DIR="${RESULT_DIR}/flowlog_out"
    COMPILER_BIN="${ROOT_DIR}/target/release/flowlog-compiler"
    WORKERS="${WORKERS:-64}"

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
# Prerequisite helpers
###############################################################################

compile_release_workspace() {
    log "$YELLOW" "BUILD" "Compiling release workspace"
    pushd "$ROOT_DIR" >/dev/null
    cargo build --release >/dev/null
    popd >/dev/null
}

###############################################################################
# Dataset management
###############################################################################

setup_dataset() {
    local dataset_name="$1"
    local dataset_zip="${STAGE_DIR}/${dataset_name}.zip"
    local extract_path="${FACT_DIR}/${dataset_name}"
    local dataset_url="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/dataset/csv/${dataset_name}.zip"

    if [[ -d "$extract_path" ]]; then
        log "$GREEN" "FOUND" "Dataset $dataset_name"
        return
    fi

    mkdir -p "$FACT_DIR"
    log "$CYAN" "DOWNLOAD" "$dataset_name.zip -> $STAGE_DIR"
    command -v wget >/dev/null 2>&1 || die "wget not found"
    wget -q -O "$dataset_zip" "$dataset_url" || die "Download failed: $dataset_name"

    log "$YELLOW" "EXTRACT" "$dataset_name"
    command -v unzip >/dev/null 2>&1 || die "unzip not found"
    unzip -q "$dataset_zip" -d "$FACT_DIR" || die "Extraction failed: $dataset_name"
    rm -f "$dataset_zip"
}

cleanup_dataset() {
    local dataset_name="$1"
    log "$YELLOW" "CLEANUP" "Dataset $dataset_name"
    rm -rf "${FACT_DIR}/${dataset_name}"
}

###############################################################################
# Souffle reference management
###############################################################################

download_souffle_ref() {
    local program_stem="$1" dataset_name="$2"
    local ref_name="${program_stem}_${dataset_name}"
    local ref_dir="${STAGE_DIR}/souffle_ref_$$_${ref_name}"
    local ref_tar="${STAGE_DIR}/souffle_ref_$$_${ref_name}.tar.gz"
    local ref_url="${SOUFFLE_BASE_URL}/${ref_name}.tar.gz"

    mkdir -p "$ref_dir"
    log "$CYAN" "DOWNLOAD" "Souffle reference: $ref_name" >&2
    wget -q -O "$ref_tar" "$ref_url" || die "Failed to download Souffle reference: $ref_url"
    tar xzf "$ref_tar" -C "$ref_dir" || die "Failed to extract Souffle reference: $ref_name"
    rm -f "$ref_tar"

    echo "$ref_dir"
}

###############################################################################
# Program preparation: .printsize -> .output
###############################################################################

prepare_dl_file() {
    local src_path="$1" dest_path="$2"
    sed -E 's/^\.printsize\s+(\w+)/.output \1/' "$src_path" > "$dest_path"
}

###############################################################################
# Compiler invocation
###############################################################################

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
    if [[ -n "$extra_flags" ]]; then
        # shellcheck disable=SC2086
        "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -D "$output_dir" -o "$executable" --mode "$MODE" $extra_flags
    else
        "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -D "$output_dir" -o "$executable" --mode "$MODE"
    fi
}

###############################################################################
# Verification: sort FlowLog output and diff against Souffle reference
###############################################################################

verify_output() {
    local flowlog_out_dir="$1" ref_dir="$2"
    local failed=0

    # Compare each reference CSV against FlowLog output.
    # Souffle reference files have .csv extension; FlowLog outputs without extension.
    # Match by stem name (case-insensitive).
    for ref_file in "$ref_dir"/*.csv; do
        [[ -f "$ref_file" ]] || continue
        local ref_basename ref_stem
        ref_basename="$(basename "$ref_file")"
        ref_stem="${ref_basename%.csv}"

        # FlowLog outputs without .csv extension; try stem directly, then case-insensitive
        local fl_file=""
        if [[ -f "$flowlog_out_dir/$ref_stem" ]]; then
            fl_file="$flowlog_out_dir/$ref_stem"
        elif [[ -f "$flowlog_out_dir/$ref_basename" ]]; then
            fl_file="$flowlog_out_dir/$ref_basename"
        else
            local match
            match="$(find "$flowlog_out_dir" -maxdepth 1 \( -iname "$ref_stem" -o -iname "$ref_basename" \) -print -quit 2>/dev/null)"
            if [[ -n "$match" ]]; then
                fl_file="$match"
            fi
        fi

        if [[ -z "$fl_file" ]]; then
            log "$RED" "MISSING" "FlowLog did not produce: $ref_stem"
            failed=1
            continue
        fi

        # Sort FlowLog output to match the pre-sorted reference (LC_ALL=C).
        # Detect whether columns are numeric from the first line.
        local sorted_fl="/tmp/flowlog_sorted_$$_${ref_stem}"
        local first_line
        first_line="$(head -1 "$fl_file")"
        if [[ "$first_line" =~ ^[-0-9,]+$ ]]; then
            local ncols
            ncols="$(echo "$first_line" | awk -F',' '{print NF}')"
            local sort_keys=""
            for (( c=1; c<=ncols; c++ )); do
                sort_keys="$sort_keys -k${c},${c}n"
            done
            # shellcheck disable=SC2086
            LC_ALL=C sort -t',' $sort_keys "$fl_file" > "$sorted_fl"
        else
            LC_ALL=C sort -t',' "$fl_file" > "$sorted_fl"
        fi

        # Fast byte-compare, then comm on failure
        if cmp -s "$ref_file" "$sorted_fl"; then
            local lines
            lines="$(wc -l < "$ref_file")"
            log "$GREEN" "PASS" "$ref_stem ($lines tuples)"
        else
            local ref_lines fl_lines
            ref_lines="$(wc -l < "$ref_file")"
            fl_lines="$(wc -l < "$sorted_fl")"
            log "$RED" "FAIL" "Mismatch in $ref_stem (reference: $ref_lines, flowlog: $fl_lines)"
            local only_ref only_fl
            only_ref="$(comm -23 "$ref_file" "$sorted_fl" | head -10)"
            only_fl="$(comm -13 "$ref_file" "$sorted_fl" | head -10)"
            [[ -n "$only_ref" ]] && log "$YELLOW" "INFO" "In reference but not FlowLog (sample):" && echo "$only_ref"
            [[ -n "$only_fl" ]]  && log "$YELLOW" "INFO" "In FlowLog but not reference (sample):" && echo "$only_fl"
            failed=1
        fi

        rm -f "$sorted_fl"
    done

    return $failed
}

###############################################################################
# Config file iteration
###############################################################################

for_each_config_entry() {
    local callback="$1" config_file="$2" extra_flag="${3:-}"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        local line="${raw_line%%#*}"
        line="$(trim "$line")"
        [[ -z "$line" ]] && continue

        local prog_name dataset_id
        IFS='=' read -r prog_name dataset_id <<< "$line"
        prog_name="$(trim "${prog_name:-}")"
        dataset_id="$(trim "${dataset_id:-}")"
        [[ -z "$prog_name" || -z "$dataset_id" ]] && continue

        "$callback" "$prog_name" "$dataset_id" "$extra_flag"
    done < "$config_file"
}

###############################################################################
# Main test runner
###############################################################################

run_test() {
    local prog_name="$1" dataset_name="$2" str_intern_flag="${3:-}"
    local prog_file prog_path program_stem
    prog_file="$(basename "$prog_name")"
    prog_path="${PROG_DIR}/${prog_name}"
    program_stem="${prog_file%.*}"

    [[ -f "$prog_path" ]] || die "Program not found: $prog_path"

    # Download dataset and Souffle reference
    setup_dataset "$dataset_name"
    local ref_dir
    ref_dir="$(download_souffle_ref "$program_stem" "$dataset_name")"

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
        prepared_dl="/tmp/flowlog_prepared_$$_${prog_file}"

        mkdir -p "$output_dir"

        log "$BLUE" "TEST" "$prog_file with $dataset_name (mode=$MODE${suffix})"

        # Prepare .dl: .printsize -> .output
        prepare_dl_file "$prog_path" "$prepared_dl"

        # Compile
        rm -f "$executable"
        invoke_compiler "$prepared_dl" "$dataset_path" "$executable" "$output_dir" "$flags"
        [[ -x "$executable" ]] || die "Executable not found: $executable"

        # Run
        log "$YELLOW" "RUN" "$executable -w $WORKERS"
        "$executable" -w "$WORKERS" 2>&1 | tee "$log_file"

        # Verify
        verify_output "$output_dir" "$ref_dir" \
            || die "Verification failed: $prog_file with $dataset_name${suffix}"

        # Cleanup generated artifacts
        rm -f "$executable" "${executable}_ops.json" "$prepared_dl"
        rm -rf "${ROOT_DIR}/.${package_name}.build"
        rm -rf "$output_dir"
    done

    rm -rf "$ref_dir"
    cleanup_dataset "$dataset_name"
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
    log "$BLUE" "START" "FlowLog datalog-batch correctness test"

    compile_release_workspace

    if [[ -n "$SINGLE_CONFIG" ]]; then
        local extra=""
        [[ "$(basename "$SINGLE_CONFIG")" == config_string* ]] && extra="--str-intern"
        run_config "$SINGLE_CONFIG" "custom" "$extra"
    else
        # Default: run both integer and string configs
        run_config "$CONFIG_INTEGER" "integer"
        run_config "$CONFIG_STRING"  "string" "--str-intern"
    fi

    log "$GREEN" "FINISH" "All tests passed"
}

main "$@"
