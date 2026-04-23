#!/usr/bin/env bash
#
# Helpers shared between the two library-mode runner synthesizers:
#   - tests/lib_runner_synth.sh         (unit + complex test runners)
#   - tools/benchmark/lib_runner.sh     (benchmark runner)
#
# Both need to: resolve `.input` filenames, find datasets case-insensitively,
# map FlowLog data types to Rust types, and pascal-case relation names.
# Pure stateless functions — no globals, no side effects.

[[ -n "${FLOWLOG_LIB_SYNTH_COMMON_LOADED:-}" ]] && return 0
FLOWLOG_LIB_SYNTH_COMMON_LOADED=1

###############################################################################
# .dl parsing — single-file reads (caller walks `.include` chains if needed)
###############################################################################

# Echo the filename when the relation is declared with `IO="file"`; exit 1
# (empty stdout) otherwise. Unlike [`input_filename_for`], this never
# synthesizes a default — callers use it to decide whether to emit a
# preload epoch for that EDB.
file_backed_filename() {
    local dl_file="$1" rel="$2"
    local line fname
    line=$(grep -iE "^[[:space:]]*\.input[[:space:]]+${rel}([[:space:]]|\\()" "$dl_file" 2>/dev/null | head -1 || true)
    [[ -n "$line" ]] || return 1
    echo "$line" | grep -qE 'IO[[:space:]]*=[[:space:]]*"file"' || return 1
    fname=$(echo "$line" | grep -oE 'filename[[:space:]]*=[[:space:]]*"[^"]+"' | sed -E 's/.*"([^"]+)"/\1/')
    [[ -n "$fname" ]] || fname="${rel}.csv"
    echo "$fname"
}

# Echo the data filename declared by `.input <Rel>(filename="X.csv", …)` —
# falls back to `<rel>.csv` when no `filename=` parameter is set.
input_filename_for() {
    local dl_file="$1" rel="$2"
    local line fname
    line=$(grep -iE "^[[:space:]]*\.input[[:space:]]+${rel}([[:space:]]|\\()" "$dl_file" 2>/dev/null | head -1 || true)
    if [[ -n "$line" ]]; then
        fname=$(echo "$line" | grep -oE 'filename[[:space:]]*=[[:space:]]*"[^"]+"' | sed -E 's/.*"([^"]+)"/\1/')
        if [[ -n "$fname" ]]; then
            echo "$fname"
            return 0
        fi
    fi
    echo "${rel}.csv"
}

###############################################################################
# Filesystem
###############################################################################

# Echo the CSV path in `dir` whose basename matches `wanted`
# case-insensitively (handles on-disk casing drift). Empty if no match.
find_csv_case_insensitive() {
    local dir="$1" wanted="$2"
    local f base
    for f in "${dir}"/*.csv; do
        [[ -f "$f" ]] || continue
        base="$(basename "$f")"
        if [[ "${base,,}" == "${wanted,,}" ]]; then
            echo "$f"
            return 0
        fi
    done
}

###############################################################################
# Type / name codegen
###############################################################################

# Map a FlowLog `.decl` data type to its Rust equivalent. Aliases mirror
# `crates/flowlog-build/src/parser/grammar.pest`:
#   number  → int32   unsigned → uint32
#   float   → f32     symbol   → string
dl_to_rust_type() {
    case "$1" in
        int8)                       echo "i8" ;;
        int16)                      echo "i16" ;;
        int32 | signed | number)    echo "i32" ;;
        int64)                      echo "i64" ;;
        uint8)                      echo "u8" ;;
        uint16)                     echo "u16" ;;
        uint32 | unsigned)          echo "u32" ;;
        uint64)                     echo "u64" ;;
        float32 | float)            echo "f32" ;;
        float64 | f64)              echo "f64" ;;
        string | symbol)            echo "String" ;;
        bool)                       echo "bool" ;;
        *)                          echo "$1" ;;
    esac
}

# Convert a snake_case / lowercase name to PascalCase, mirroring
# `crates/flowlog-build/src/build/relation/mod.rs::pascal_case`. Capitalize
# the first character and any character after `_` / `-`, dropping separators.
pascal_case() {
    local input="$1"
    local out=""
    local cap=1 i c
    for (( i=0; i<${#input}; i++ )); do
        c="${input:$i:1}"
        if [[ "$c" == "_" || "$c" == "-" ]]; then
            cap=1
        elif (( cap )); then
            out+="${c^^}"
            cap=0
        else
            out+="$c"
        fi
    done
    printf '%s' "$out"
}
