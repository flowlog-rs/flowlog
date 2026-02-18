#!/bin/bash
set -euo pipefail

# FlowLog correctness runner.
#
# Default mode:
#   - Builds the release workspace
#   - Ensures the FlowLog compiler binary exists
#   - Downloads datasets (if needed)
#   - Generates and runs per-benchmark projects
#   - Parses "[size]" output lines and compares against reference files
#
# Compile-only mode:
#   - Builds the release workspace and the FlowLog compiler binary
#   - Iterates through the config file
#   - Does not download or read datasets (uses an empty facts directory)
#   - Generates each project and runs `cargo check` (no execution/verification)

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
CONFIG_FILE_DEFAULT="${ROOT_DIR}/tools/check/config.txt"
CONFIG_FILE="$CONFIG_FILE_DEFAULT"
COMPILE_ONLY=0
EMPTY_FACTS_DIR=""
ENABLE_SIP=0

# Shared flag sets used by both run_compile_check and run_test.
#   Optimization flags: default (none); --sip adds (--sip)
#   Profiler flags:     (none, -P)
PROF_FLAGS=("" "-P")
PROF_LABELS=("" "prof")

###############################################################################
# Logging / utilities
###############################################################################

log() { local color="$1" tag="$2"; shift 2; echo -e "${color}[${tag}]${NC} $*"; }
die() { log "$RED" "ERROR" "$*"; exit 1; }

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [config_file] [--compile-only] [--sip]

Modes:
  Default:         run all correctness tests listed in the config file.
  --compile-only:  generate each project and run cargo check only (no download/run/verify).

Options:
  --sip          Also test with --sip optimization (disabled by default).

Environment:
  WORKERS=<n>    Number of workers passed to generated executables (default: 64)

Examples:
  $(basename "$0")
  $(basename "$0") tools/check/config.txt
  $(basename "$0") --compile-only --sip
  $(basename "$0") --sip
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
            --compile-only) COMPILE_ONLY=1 ;;
            --sip)          ENABLE_SIP=1 ;;
            -h|--help)      usage; exit 0 ;;
            --)             shift; break ;;
            -*)             die "Unknown option: $1 (try --help)" ;;
            *)
                [[ "$CONFIG_FILE" == "$CONFIG_FILE_DEFAULT" ]] \
                    || die "Unexpected extra argument: $1"
                CONFIG_FILE="$1"
                ;;
        esac
        shift
    done
    (( $# == 0 )) || die "Unexpected extra arguments: $*"
}

init_opt_flags() {
    OPT_FLAGS=("")
    OPT_LABELS=("")
    if (( ENABLE_SIP )); then
        OPT_FLAGS+=("--sip")
        OPT_LABELS+=("sip")
    fi
    log "$BLUE" "SIP" "SIP enabled: $ENABLE_SIP -> OPT_FLAGS=(${OPT_FLAGS[*]})"
}

init_paths() {
    PROG_DIR="${ROOT_DIR}/example"
    FACT_DIR="${ROOT_DIR}/facts"
    SIZE_DIR="${ROOT_DIR}/tools/check/correctness_size"
    RESULT_DIR="${ROOT_DIR}/result"
    LOG_DIR="${RESULT_DIR}/logs"
    PARSED_DIR="${RESULT_DIR}/parsed"
    COMPILER_BIN="${ROOT_DIR}/target/release/flowlog"
    WORKERS="${WORKERS:-64}"

    mkdir -p "$RESULT_DIR" "$LOG_DIR" "$PARSED_DIR"
    LOG_DIR="$(realpath "$LOG_DIR")"
    PARSED_DIR="$(realpath "$PARSED_DIR")"
}

###############################################################################
# Prerequisite helpers
###############################################################################

setup_config_file() {
    [[ -f "$CONFIG_FILE" ]] || die "config file not found: $CONFIG_FILE"
}

setup_size_reference() {
    [[ -d "$SIZE_DIR" ]] || die "Reference size directory not found: $SIZE_DIR"
}

ensure_compiler_built() {
    if [[ ! -x "$COMPILER_BIN" ]]; then
        log "$YELLOW" "BUILD" "Building compiler (flowlog)"
        cargo build --release --bin flowlog >/dev/null
    fi
}

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
    local dataset_zip="${FACT_DIR}/${dataset_name}.zip"
    local extract_path="${FACT_DIR}/${dataset_name}"
    local dataset_url="https://pages.cs.wisc.edu/~m0riarty/dataset/csv/${dataset_name}.zip"

    if [[ -d "$extract_path" ]]; then
        log "$GREEN" "FOUND" "Dataset $dataset_name"
        return
    fi

    mkdir -p "$FACT_DIR"
    if [[ ! -f "$dataset_zip" ]]; then
        log "$CYAN" "DOWNLOAD" "$dataset_name.zip"
        command -v wget >/dev/null 2>&1 || die "wget not found; cannot download datasets"
        wget -q -O "$dataset_zip" "$dataset_url" || die "Download failed: $dataset_name"
    fi

    log "$YELLOW" "EXTRACT" "$dataset_name"
    command -v unzip >/dev/null 2>&1 || die "unzip not found; cannot extract datasets"
    unzip -q "$dataset_zip" -d "$FACT_DIR" || die "Failed to extract dataset: $dataset_name"
}

cleanup_dataset() {
    local dataset_name="$1"
    log "$YELLOW" "CLEANUP" "$dataset_name"
    rm -rf "${FACT_DIR}/${dataset_name}" "${FACT_DIR}/${dataset_name}.zip"
}

###############################################################################
# Shared flag-combo helpers (used by both modes)
###############################################################################

# Compute label_suffix and extra_flags for a given (opt, prof) index pair.
# Sets the caller-visible variables: COMBO_EXTRA_FLAGS, COMBO_LABEL_SUFFIX
compute_flag_combo() {
    local oi="$1" pi="$2"
    COMBO_EXTRA_FLAGS="$(trim "${OPT_FLAGS[$oi]} ${PROF_FLAGS[$pi]}")"

    local parts=""
    [[ -n "${OPT_LABELS[$oi]}" ]]  && parts="${OPT_LABELS[$oi]}"
    [[ -n "${PROF_LABELS[$pi]}" ]] && parts="${parts:+${parts}_}${PROF_LABELS[$pi]}"
    COMBO_LABEL_SUFFIX="${parts:+_${parts}}"
}

# Invoke the FlowLog compiler. Handles the extra_flags word-splitting safely.
invoke_compiler() {
    local prog_path="$1" facts_dir="$2" project_dir="$3" mode="$4" extra_flags="$5"

    log "$YELLOW" "GENERATE" "$COMPILER_BIN $prog_path -F $facts_dir -o $project_dir --mode $mode ${extra_flags}"
    if [[ -n "$extra_flags" ]]; then
        # shellcheck disable=SC2086
        "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -o "$project_dir" --mode "$mode" $extra_flags
    else
        "$COMPILER_BIN" "$prog_path" -F "$facts_dir" -o "$project_dir" --mode "$mode"
    fi
}

###############################################################################
# Config file iteration
#
# Reads lines of the form "prog_name = dataset_id", skips blanks/comments.
# Calls the supplied callback with (prog_name, dataset_id) for each entry.
###############################################################################

for_each_config_entry() {
    local callback="$1"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        local line="${raw_line%%#*}"
        line="$(trim "$line")"
        [[ -z "$line" ]] && continue

        local prog_name dataset_id
        IFS='=' read -r prog_name dataset_id <<< "$line"
        prog_name="$(trim "${prog_name:-}")"
        dataset_id="$(trim "${dataset_id:-}")"
        [[ -z "$prog_name" || -z "$dataset_id" ]] && continue

        if [[ "$prog_name" == "test.dl" ]]; then
            log "$YELLOW" "SKIP" "Skipping test.dl"
            continue
        fi

        "$callback" "$prog_name" "$dataset_id"
    done < "$CONFIG_FILE"
}

###############################################################################
# Compile-only mode
###############################################################################

setup_empty_facts_dir() {
    [[ -n "$EMPTY_FACTS_DIR" ]] && return 0
    EMPTY_FACTS_DIR="$(mktemp -d -t flowlog_facts_empty_XXXXXX)"
}

cleanup_empty_facts_dir() {
    [[ -n "$EMPTY_FACTS_DIR" ]] && rm -rf "$EMPTY_FACTS_DIR"
    EMPTY_FACTS_DIR=""
}

run_compile_check() {
    local prog_name="$1" dataset_id="$2"
    local prog_file prog_path program_stem mode="batch"
    prog_file="$(basename "$prog_name")"
    prog_path="${PROG_DIR}/${prog_file}"
    program_stem="${prog_file%.*}"

    [[ -f "$prog_path" ]]       || die "Program not found: $prog_path"
    [[ -n "$EMPTY_FACTS_DIR" ]] || die "Internal error: EMPTY_FACTS_DIR not initialized"

    for oi in "${!OPT_FLAGS[@]}"; do
        for pi in "${!PROF_FLAGS[@]}"; do
            compute_flag_combo "$oi" "$pi"
            local suffix="$COMBO_LABEL_SUFFIX" flags="$COMBO_EXTRA_FLAGS"

            local package_name project_dir log_file
            package_name="$(sanitize_package_name "${program_stem}_${dataset_id}_${mode}${suffix}")"
            project_dir="${ROOT_DIR}/${package_name}"
            log_file="${LOG_DIR}/${program_stem}_${dataset_id}_${mode}${suffix}_compile.log"

            log "$BLUE" "CHECK" "$prog_file (id=$dataset_id, mode=$mode${suffix})"

            rm -rf "$project_dir"
            invoke_compiler "$prog_path" "$EMPTY_FACTS_DIR" "$project_dir" "$mode" "$flags"
            [[ -d "$project_dir" ]] || die "Generated project not found (mode=$mode${suffix}): $project_dir"

            pushd "$project_dir" >/dev/null
            log "$YELLOW" "RUN" "cargo check --release (mode=$mode${suffix})"
            cargo check --release 2>&1 | tee "$log_file"
            popd >/dev/null

            log "$YELLOW" "CLEANUP" "Removing generated project $project_dir"
            rm -rf "$project_dir"
        done
    done
}

compile_only() {
    log "$BLUE" "MODE" "compile-only (generate + cargo check only)"
    compile_release_workspace
    ensure_compiler_built
    setup_config_file
    init_opt_flags
    setup_empty_facts_dir
    trap cleanup_empty_facts_dir EXIT

    log "$BLUE" "TESTS" "Running compile-only checks from config"
    for_each_config_entry run_compile_check
    log "$GREEN" "FINISH" "compile-only completed successfully"
}

###############################################################################
# Full test mode (generate + run + verify)
###############################################################################

parse_output_to_size_file() {
    local log_file="$1" out_file="$2"

    if ! grep -Eq '^[[:space:]]*\[size\]\[' "$log_file"; then
        log "$RED" "ERROR" "No size lines found in $log_file"
        return 1
    fi

    command -v python >/dev/null 2>&1 || die "python not found; cannot parse size output"
    python - "$log_file" "$out_file" <<'PY'
import re, sys
from pathlib import Path

log_path, out_path = Path(sys.argv[1]), Path(sys.argv[2])
pat = re.compile(r'^\s*\[size\]\[([^\]]+)\].*\bsize=([+-]?\d+)\b')

sizes = {}
for raw in log_path.read_text(errors="replace").splitlines():
    m = pat.match(raw)
    if m:
        sizes[m.group(1).strip()] = m.group(2).strip()

if not sizes:
    print(f"No parsable size lines found in {log_path}", file=sys.stderr)
    sys.exit(1)

out_path.write_text("\n".join(f"{k}: {v}" for k, v in sorted(sizes.items())) + "\n")
PY
}

verify_results_against_truth() {
    local prog_name="$1" dataset_name="$2" result_size_file="$3"
    local reference_size_file="${SIZE_DIR}/${prog_name}_${dataset_name}_size.txt"

    if [[ ! -f "$result_size_file" || ! -f "$reference_size_file" ]]; then
        log "$RED" "ERROR" "Missing files for reference check"
        return 1
    fi

    if python - "$reference_size_file" "$result_size_file" <<'PY'
import sys
from pathlib import Path

def load(path):
    data = {}
    for idx, raw_line in enumerate(Path(path).read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        if ':' not in line:
            print(f"Invalid line format in {path} line {idx}: {raw_line!r}")
            sys.exit(1)
        key, value = line.split(':', 1)
        data[key.strip()] = value.strip()
    return data

ref, actual = load(sys.argv[1]), load(sys.argv[2])

missing = sorted(k for k in ref if k not in actual)
if missing:
    print("Missing relations:", ', '.join(missing))
    sys.exit(1)

mismatches = [(k, ref[k], actual.get(k)) for k in ref if actual.get(k) != ref[k]]
if mismatches:
    for key, expected, observed in mismatches:
        print(f"Relation {key}: expected {expected}, actual {observed or '<missing>'}")
    sys.exit(1)

extras = sorted(k for k in actual if k not in ref)
if extras:
    print("Note: extra relations ignored:", ', '.join(extras))
PY
    then
        log "$GREEN" "PASS" "Reference check: $prog_name"
    else
        log "$RED" "FAIL" "Reference mismatch: $prog_name"
        log "$YELLOW" "INFO" "Reference:" && cat "$reference_size_file"
        log "$YELLOW" "INFO" "Actual:"    && cat "$result_size_file"
        return 1
    fi
}

run_test() {
    local prog_name="$1" dataset_name="$2"
    local prog_file prog_path program_stem mode="batch"
    prog_file="$(basename "$prog_name")"
    prog_path="${PROG_DIR}/${prog_file}"
    program_stem="${prog_file%.*}"

    [[ -f "$prog_path" ]] || die "Program not found: $prog_path"

    setup_dataset "$dataset_name"
    local dataset_path
    dataset_path="$(realpath "${FACT_DIR}/${dataset_name}")"

    for oi in "${!OPT_FLAGS[@]}"; do
        for pi in "${!PROF_FLAGS[@]}"; do
            compute_flag_combo "$oi" "$pi"
            local suffix="$COMBO_LABEL_SUFFIX" flags="$COMBO_EXTRA_FLAGS"

            local package_name project_dir log_file parsed_file
            package_name="$(sanitize_package_name "${program_stem}_${dataset_name}_${mode}${suffix}")"
            project_dir="${ROOT_DIR}/${package_name}"
            log_file="${LOG_DIR}/${program_stem}_${dataset_name}_${mode}${suffix}.log"
            parsed_file="${PARSED_DIR}/${program_stem}_${dataset_name}_${mode}${suffix}_size.txt"

            log "$BLUE" "TEST" "$prog_file with $dataset_name (mode=$mode${suffix})"

            rm -rf "$project_dir"
            invoke_compiler "$prog_path" "$dataset_path" "$project_dir" "$mode" "$flags"
            [[ -d "$project_dir" ]] || die "Generated project not found (mode=$mode${suffix}): $project_dir"

            pushd "$project_dir" >/dev/null
            log "$YELLOW" "RUN" "cargo run --release -- -w $WORKERS (mode=$mode${suffix})"
            cargo run --release -- -w "$WORKERS" 2>&1 | tee "$log_file"
            popd >/dev/null

            [[ -f "$log_file" ]] || die "Run log missing (mode=$mode${suffix}): $log_file"

            parse_output_to_size_file "$log_file" "$parsed_file" \
                || die "Failed to parse output for $prog_file (mode=$mode${suffix})"
            verify_results_against_truth "$program_stem" "$dataset_name" "$parsed_file" \
                || die "Reference verification failed for $prog_file (mode=$mode${suffix})"

            log "$YELLOW" "CLEANUP" "Removing generated project $project_dir"
            rm -rf "$project_dir"
        done
    done

    cleanup_dataset "$dataset_name"
}

###############################################################################
# Entry point
###############################################################################

main() {
    parse_args "$@"
    init_paths
    log "$BLUE" "START" "FlowLog Testing"

    if (( COMPILE_ONLY )); then
        compile_only
        return 0
    fi

    compile_release_workspace
    ensure_compiler_built
    setup_config_file
    setup_size_reference
    init_opt_flags

    log "$BLUE" "TESTS" "Running compiler-mode correctness tests (batch mode)"
    for_each_config_entry run_test
    log "$GREEN" "FINISH" "testing completed successfully"
}

main "$@"
