#!/usr/bin/env bash
#
# Shared helpers for FlowLog Souffle-oracle (L2) test runners.
#
# Sourced by:
#   tests/oracle/run_compiler.sh — binary mode runner
#   tests/oracle/run_lib.sh      — library mode runner
#
# Pulls generic helpers (colors, log, die, trim) from tests/lib/shared.sh,
# and layers oracle-specific bits on top: dataset/reference download,
# .dl preparation, config-file iteration, and CSV-vs-Souffle verification.
#
# Not executable on its own — defines functions and globals; the runner
# script is responsible for invoking `main`.

source "$(dirname "${BASH_SOURCE[0]}")/../lib/shared.sh"

###############################################################################
# Globals
###############################################################################

readonly ORACLE_DIR="${ROOT_DIR}/tests/oracle"
readonly CONFIG_DEFAULT="${ORACLE_DIR}/config.txt"

# Staging directory: prefer /dev/shm (tmpfs) for speed, fall back to $TMPDIR or /tmp
if [[ -d /dev/shm && -w /dev/shm ]]; then
    readonly STAGE_DIR="/dev/shm"
else
    readonly STAGE_DIR="${TMPDIR:-/tmp}"
fi

readonly SOUFFLE_BASE_URL="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/test/datalog-batch"

readonly FACT_DIR_DEFAULT="${ROOT_DIR}/facts"
readonly PROG_DIR_DEFAULT="${ROOT_DIR}/example"
readonly RESULT_DIR_DEFAULT="${ROOT_DIR}/result"

###############################################################################
# Misc utilities
###############################################################################

# Sanitize a free-form string into a valid Rust-ish package name.
# Only used when a runner needs per-test unique artifact names.
sanitize_package_name() {
    local s
    s="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '_')"
    s="$(printf '%s' "$s" | sed 's/_\{2,\}/_/g; s/-\{2,\}/-/g; s/^[-_]//; s/[-_]$//')"
    [[ -z "$s" ]] && s="flowlog_output"
    [[ "$s" =~ ^[0-9] ]] && s="flowlog_${s}"
    printf '%s' "$s"
}

###############################################################################
# Dataset management
###############################################################################

setup_dataset() {
    local dataset_name="$1"
    local fact_dir="${2:-$FACT_DIR_DEFAULT}"
    local dataset_zip="${STAGE_DIR}/${dataset_name}.zip"
    local extract_path="${fact_dir}/${dataset_name}"
    local dataset_url="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/dataset/csv/${dataset_name}.zip"

    if [[ -d "$extract_path" ]]; then
        log "$GREEN" "FOUND" "Dataset $dataset_name"
        return
    fi

    mkdir -p "$fact_dir"
    log "$CYAN" "DOWNLOAD" "$dataset_name.zip -> $STAGE_DIR"
    command -v wget >/dev/null 2>&1 || die "wget not found"
    # --timeout/--tries make a transient HuggingFace 503 retry instead of
    # hanging the sweep indefinitely; --no-verbose surfaces fatal errors.
    wget --no-verbose --timeout=60 --tries=3 -O "$dataset_zip" "$dataset_url" \
        || die "Download failed: $dataset_name (check network)"

    log "$YELLOW" "EXTRACT" "$dataset_name"
    command -v unzip >/dev/null 2>&1 || die "unzip not found"
    unzip -q "$dataset_zip" -d "$fact_dir" || die "Extraction failed: $dataset_name"
    rm -f "$dataset_zip"
}

cleanup_dataset() {
    local dataset_name="$1" fact_dir="${2:-$FACT_DIR_DEFAULT}" keep="${3:-0}"

    [[ -n "$dataset_name" && -n "$fact_dir" ]] \
        || die "cleanup_dataset: empty args (refusing to rm -rf)"

    if (( keep )); then
        log "$YELLOW" "CLEANUP" "Dataset $dataset_name (kept)"
        return
    fi
    # Refuse to delete through a symlink: protects a shared cache like
    # /datasets/facts from being nuked when <repo>/facts → /datasets/facts.
    # Caller's options: pass --keep-datasets, or `rm <repo>/facts` to
    # decouple from the shared cache.
    if [[ -L "$fact_dir" ]]; then
        die "cleanup_dataset: $fact_dir is a symlink; refusing to delete through it"
    fi
    log "$YELLOW" "CLEANUP" "Dataset $dataset_name"
    # shellcheck disable=SC2115  # empty-arg + symlink guards above make the path safe
    rm -rf -- "${fact_dir}/${dataset_name}"
}

###############################################################################
# Souffle reference management
###############################################################################

download_souffle_ref() {
    local program_stem="$1" dataset_name="$2" cache_dir="${3:-}"
    local ref_name="${program_stem}_${dataset_name}"
    local ref_dir="${STAGE_DIR}/souffle_ref_$$_${ref_name}"
    local ref_tar="${STAGE_DIR}/souffle_ref_$$_${ref_name}.tar.gz"
    local ref_url="${SOUFFLE_BASE_URL}/${ref_name}.tar.gz"

    mkdir -p "$ref_dir"
    local cached="${cache_dir}/${ref_name}.tar.gz"
    if [[ -n "$cache_dir" && -f "$cached" ]]; then
        log "$CYAN" "DOWNLOAD" "Souffle reference: $ref_name (cache: $cached)" >&2
        cp "$cached" "$ref_tar"
    else
        log "$CYAN" "DOWNLOAD" "Souffle reference: $ref_name" >&2
        wget --no-verbose --timeout=60 --tries=3 -O "$ref_tar" "$ref_url" \
            || die "Failed to download Souffle reference: $ref_url"
    fi
    tar xzf "$ref_tar" -C "$ref_dir" || die "Failed to extract Souffle reference: $ref_name"
    rm -f "$ref_tar"

    echo "$ref_dir"
}

###############################################################################
# Program preparation: .printsize -> .output
#
# Souffle reference files exist for .output relations; rewriting ensures
# FlowLog emits them to disk regardless of the source directive.
###############################################################################

prepare_dl_file() {
    local src_path="$1" dest_path="$2"
    sed -E 's/^\.printsize\s+(\w+)/.output \1/' "$src_path" > "$dest_path"
}

###############################################################################
# Config iteration
#
# Config lines look like `program_name = dataset_id [tag ...]`. Lines are
# trimmed; blank lines and `#` comments are skipped. Trailing whitespace-
# separated tokens after the dataset_id are symbolic capability tags
# (e.g. `str-intern`) interpreted by the runner. The callback receives
# (program_name, dataset_id, tags).
###############################################################################

for_each_config_entry() {
    local callback="$1" config_file="$2"

    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        local line="${raw_line%%#*}"
        line="$(trim "$line")"
        [[ -z "$line" ]] && continue

        local prog_name dataset_field
        IFS='=' read -r prog_name dataset_field <<< "$line"
        prog_name="$(trim "${prog_name:-}")"
        dataset_field="$(trim "${dataset_field:-}")"
        [[ -z "$prog_name" || -z "$dataset_field" ]] && continue

        # First whitespace-separated token is the dataset; the rest are tags.
        local dataset_id tags
        read -r dataset_id tags <<< "$dataset_field"

        "$callback" "$prog_name" "$dataset_id" "$tags"
    done < "$config_file"
}

###############################################################################
# Verification: sort FlowLog output and diff against Souffle reference
#
# FlowLog produces one file per `.output` relation, named by the lowercased
# relation stem and no extension; Souffle references are `<stem>.csv`. The
# sort is driven off the first line: numeric columns get `-k<n>n`, anything
# else falls back to lexicographic.
###############################################################################

verify_output() {
    local flowlog_out_dir="$1" ref_dir="$2"
    local failed=0

    for ref_file in "$ref_dir"/*.csv; do
        [[ -f "$ref_file" ]] || continue
        local ref_basename ref_stem
        ref_basename="$(basename "$ref_file")"
        ref_stem="${ref_basename%.csv}"

        # FlowLog outputs without .csv extension; try stem directly, then
        # with extension, then case-insensitive match.
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

        local sorted_fl="${STAGE_DIR}/flowlog_sorted_$$_${ref_stem}"
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
