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
#   2. Synthesizes a `src/main.rs` that:
#        - includes the lib-mode generated `.rs` from `OUT_DIR`
#        - calls `engine.load_file_<rel>("data/<rel>.csv")` for each input
#        - calls `engine.run()`
#        - writes `output/<Rel>` files in the same CSV format as binary mode
#   3. Runs `cargo run --release` on the runner crate.
#   4. Reuses `compare_expected_outputs` from `common.sh` to diff against
#      `expected/`.
#
# Phase 1 supports `datalog-batch` only. Tests in other categories, tests
# with `commands.txt` (incremental), and tests with `udf.rs` are skipped.

# Override CATEGORIES BEFORE sourcing common.sh — lib mode is batch-only.
CATEGORIES=(datalog-batch)

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

readonly RUNNER_DIR="${ROOT_DIR}/target/e2e-lib/runner"
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

The runner generates a small Rust crate that uses flowlog-build to compile
the .dl program into a typed engine, then drives it through the typed
insert/load API. Output is sorted before comparison since lib mode does
not guarantee output order.

Examples:
  $(basename "$0")                     # run all datalog-batch tests
  $(basename "$0") agg_sum             # run one test
EOF
}

###############################################################################
# .dl parsing helpers
###############################################################################

# Glob for all .dl files reachable from a fixture (program.dl + any
# .dl files in sibling directories that .include resolves into). The
# parser walks .include chains itself, but our grep needs to see them all.
all_dl_files() {
    local dl_file="$1"
    echo "$dl_file"
    local dir
    dir="$(dirname "$dl_file")"
    find "$dir" -name '*.dl' ! -path "$dl_file" 2>/dev/null
}

# Extract input relation names (one per line, lowercase) from a .dl file
# and any sibling included .dl files.
#
# Looks for `.input <Name>(...)` directives. The actual filename is taken
# from the data/ directory by convention (matches existing fixture layout).
#
# `grep || true` swallows exit code 1 (no match) so the pipeline survives
# under `set -o pipefail`.
parse_input_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '\.input[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$f" 2>/dev/null || true) \
            | awk '{ print tolower($2) }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Extract output relation names (lowercase, one per line) from a .dl file
# and any sibling included .dl files.
parse_output_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '\.(output|printsize)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$f" 2>/dev/null || true) \
            | awk '{ print tolower($2) }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Lookup a `.decl <Name>(field1: type1, field2: type2, ...)` declaration
# across the .dl file and any sibling included files. The relation name is
# matched case-insensitively. Echoes a space-separated list of field names.
# Returns 1 if not found.
parse_decl_fields() {
    local dl_file="$1"
    local rel="$2"
    _parse_decl "$dl_file" "$rel" name
}

# Same as parse_decl_fields but echoes `name:dltype` pairs (for synthesizing
# per-column CSV parsing in the lib-mode runner's main.rs).
parse_decl_typed_fields() {
    local dl_file="$1"
    local rel="$2"
    _parse_decl "$dl_file" "$rel" both
}

# Shared implementation. $3 is "name" or "both".
_parse_decl() {
    local dl_file="$1"
    local rel="$2"
    local mode="$3"
    local line=""
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        line=$(grep -iE "^[[:space:]]*\.decl[[:space:]]+${rel}[[:space:]]*\(" "$f" 2>/dev/null | head -1 || true)
        [[ -n "$line" ]] && break
    done < <(all_dl_files "$dl_file")
    [[ -n "$line" ]] || return 1
    local inside
    inside=$(echo "$line" | sed -E 's/^[^(]*\(([^)]*)\).*$/\1/')
    [[ -n "$inside" ]] || { echo ""; return 0; }  # nullary
    if [[ "$mode" == "both" ]]; then
        echo "$inside" \
            | tr ',' '\n' \
            | awk -F: '{
                name = $1; ty = $2
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", ty)
                print name ":" ty
              }' \
            | tr '\n' ' '
    else
        echo "$inside" \
            | tr ',' '\n' \
            | awk -F: '{
                name = $1
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
                print name
              }' \
            | tr '\n' ' '
    fi
}

# Map a Datalog type to its Rust equivalent.
dl_type_to_rust() {
    case "$1" in
        int8)              echo "i8" ;;
        int16)             echo "i16" ;;
        int32 | signed)    echo "i32" ;;
        int64)             echo "i64" ;;
        uint8)             echo "u8" ;;
        uint16)            echo "u16" ;;
        uint32 | unsigned) echo "u32" ;;
        uint64)            echo "u64" ;;
        float32)           echo "f32" ;;
        float64)           echo "f64" ;;
        string)            echo "String" ;;
        bool)              echo "bool" ;;
        *)                 echo "$1" ;;
    esac
}

# Note: binary mode normalizes relation names to lowercase at parse time
# (see parser/src/declaration/relation.rs::Relation::new), so the
# expected/ filenames are also lowercase. We don't need a separate
# PascalCase parse pass — `parse_output_relations` already gives us the
# right thing.

###############################################################################
# Runner crate management
###############################################################################

ensure_runner_crate() {
    mkdir -p "$RUNNER_DIR/src" "$RUNNER_DIR/data"

    cat > "$RUNNER_DIR/Cargo.toml" <<EOF
[package]
name = "flowlog_lib_runner"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
flowlog-runtime = { path = "${ROOT_DIR}/crates/flowlog-runtime" }
serde = { version = "1", features = ["derive"] }

[build-dependencies]
flowlog-build = { path = "${ROOT_DIR}/crates/flowlog-build" }

[workspace]

[profile.release]
opt-level = 3
EOF
}

###############################################################################
# main.rs synthesis
###############################################################################

# Emit a block that reads `data/<csv>`, parses each line into the relation's
# user struct, and pushes the resulting Vec through `insert_batch_<rel>`.
gen_csv_loader() {
    local dl_file="$1"
    local lower_name="$2"
    local csv="$3"

    local typed_fields
    typed_fields=$(parse_decl_typed_fields "$dl_file" "$lower_name") || {
        echo "// no decl for $lower_name" >&2
        return 1
    }

    # Uppercase first letter for the PascalCase struct name.
    local pascal
    pascal=$(echo "$lower_name" | awk '{print toupper(substr($0,1,1)) substr($0,2)}')

    # Build per-column parse expressions: `cols.next().unwrap().trim().parse::<T>().unwrap()`
    # (or `.to_string()` for String columns).
    local fields_parse=""
    local first=1
    for pair in $typed_fields; do
        local name="${pair%%:*}"
        local dltype="${pair#*:}"
        local rust_ty
        rust_ty=$(dl_type_to_rust "$dltype")
        local expr
        if [[ "$rust_ty" == "String" ]]; then
            expr="cols.next().unwrap().trim().to_string()"
        else
            expr="cols.next().unwrap().trim().parse::<${rust_ty}>().unwrap()"
        fi
        if (( first )); then
            fields_parse="${name}: ${expr}"
            first=0
        else
            fields_parse+=", ${name}: ${expr}"
        fi
    done

    cat <<EOF
    {
        let __src = std::fs::read_to_string("data/${csv}")
            .expect("read data/${csv}");
        let __items: Vec<${pascal}> = __src
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let mut cols = l.split(',');
                ${pascal} { ${fields_parse} }
            })
            .collect();
        engine.insert_batch_${lower_name}(__items);
    }
EOF
}

# Build the writer block for one output relation:
#   - Fetches the field list via parse_decl_fields
#   - Generates a `format!()` with the right number of "{}" placeholders
#   - Sorts and writes one row per line into output/<Pascal>
#
# For nullary IDBs (no fields), writes `True` if the bool is set.
gen_writer_block() {
    local dl_file="$1"
    local lower_name="$2"
    local pascal_name="$3"

    local fields
    fields=$(parse_decl_fields "$dl_file" "$lower_name") || {
        echo "// no decl for $lower_name" >&2
        return 1
    }

    if [[ -z "$fields" ]]; then
        # Nullary IDB → bool field on BatchResults.
        cat <<EOF
    {
        let mut f = std::fs::File::create("output/${pascal_name}")
            .expect("create output/${pascal_name}");
        if results.${lower_name} {
            writeln!(f, "True").expect("write");
        }
    }
EOF
        return 0
    fi

    # Build "{},{},{}" with one {} per field.
    local field_count
    field_count=$(echo "$fields" | wc -w)
    local fmt=""
    local i
    for ((i=0; i<field_count; i++)); do
        if (( i > 0 )); then
            fmt+=","
        fi
        fmt+="{}"
    done

    # Build the comma-separated `r.field1, r.field2, ...` accessor list.
    local accessors=""
    local first=1
    for f in $fields; do
        if (( first )); then
            accessors+="r.${f}"
            first=0
        else
            accessors+=", r.${f}"
        fi
    done

    # No in-runner sort — `compare_expected_outputs` sorts both sides with
    # `use_sort=1` during diff.
    cat <<EOF
    {
        let mut f = std::fs::File::create("output/${pascal_name}")
            .expect("create output/${pascal_name}");
        for r in &results.${lower_name} {
            writeln!(f, "${fmt}", ${accessors}).expect("write");
        }
    }
EOF
}

# Synthesize a fresh main.rs that drives the engine for the current test.
write_main_rs() {
    local dl_file="$1"
    local main_rs="${RUNNER_DIR}/src/main.rs"

    # Inputs: for each .input relation with a matching CSV, parse lines
    # into the user struct and insert via `engine.insert_batch_<rel>(...)`.
    # Library mode deliberately exposes no file-loading API, so the runner
    # does its own CSV parsing — same as a real library consumer would.
    # Fixture convention: `data/<rel>.csv` (case-insensitive filename scan).
    local load_calls=""
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        # Fixture CSV filenames vary in case (e.g. `Edge.csv` for relation
        # `edge`). Case-insensitive scan of `data/*.csv`.
        local csv=""
        local f base stem
        for f in "${RUNNER_DIR}/data/"*.csv; do
            [[ -f "$f" ]] || continue
            base="$(basename "$f")"
            stem="${base%.csv}"
            if [[ "${stem,,}" == "${rel,,}" ]]; then
                csv="$base"
                break
            fi
        done
        [[ -n "$csv" ]] || continue
        load_calls+=$(gen_csv_loader "$dl_file" "$rel" "$csv")$'\n'
    done < <(parse_input_relations "$dl_file")

    # Output writers — one block per .output relation. Binary mode writes
    # output files using the lowercased relation name (parser normalizes at
    # parse time), so the lib runner does the same to match expected/.
    local writer_blocks=""
    local lower
    while IFS= read -r lower; do
        [[ -n "$lower" ]] || continue
        local block
        block=$(gen_writer_block "$dl_file" "$lower" "$lower")
        writer_blocks+="${block}"$'\n'
    done < <(parse_output_relations "$dl_file")

    cat > "$main_rs" <<EOF
// Auto-generated by tests/unit/unit_lib.sh — do not edit.
#![allow(unused_imports, dead_code)]

pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}

use prog::DatalogBatchEngine;
use prog::rel::*;
use std::io::Write;

fn main() {
    std::fs::create_dir_all("output").expect("create output dir");

    let mut engine = DatalogBatchEngine::new();
${load_calls}
    let results = engine.run();

${writer_blocks}
}
EOF
}

write_build_rs() {
    local test_dir="$1"
    local include_lines=""
    if [[ -n "$test_dir" && -f "$test_dir/include_dirs" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -z "$line" ]] && continue
            include_lines+="        std::path::PathBuf::from(\"$line\"),"$'\n'
        done < "$test_dir/include_dirs"
    fi

    local has_udf=0
    if [[ -n "$test_dir" && -f "$test_dir/udf.rs" ]]; then
        has_udf=1
    fi

    local builder_chain=""
    if [[ -n "$include_lines" ]]; then
        builder_chain+=$'        .compile(&["program.dl"], &include_dirs)'
    else
        builder_chain+=$'        .compile(&["program.dl"] as &[&str], &[] as &[&std::path::Path])'
    fi

    local udf_setter=""
    (( has_udf )) && udf_setter=$'        .udf_file("udf.rs")\n'

    local include_decl=""
    if [[ -n "$include_lines" ]]; then
        include_decl=$'    let include_dirs: Vec<std::path::PathBuf> = vec![\n'"${include_lines}"$'    ];\n'
    fi

    cat > "${RUNNER_DIR}/build.rs" <<EOF
fn main() -> std::io::Result<()> {
${include_decl}    flowlog_build::configure()
${udf_setter}${builder_chain}
}
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

    # Stage fixture files into the runner crate.
    #
    # Wipe everything except `src/` (synthesized below) and stale lib/
    # directories from previous tests.
    rm -rf "${RUNNER_DIR}/data" "${RUNNER_DIR}/output" "${RUNNER_DIR}/program.dl" "${RUNNER_DIR}/lib"
    mkdir -p "${RUNNER_DIR}/data"
    cp "$test_dir/program.dl" "${RUNNER_DIR}/program.dl"
    rm -f "${RUNNER_DIR}/udf.rs"
    if [[ -f "$test_dir/udf.rs" ]]; then
        cp "$test_dir/udf.rs" "${RUNNER_DIR}/udf.rs"
    fi
    if [[ -d "$test_dir/data" ]]; then
        if compgen -G "$test_dir/data/*" > /dev/null; then
            cp "$test_dir/data/"* "${RUNNER_DIR}/data/"
        fi
    fi
    # Some fixtures use .include directives that resolve relative paths
    # against the program file's directory. Copy any sibling directories
    # other than data/ and expected/ so includes resolve correctly.
    local sibling
    for sibling in "$test_dir"/*/; do
        [[ -d "$sibling" ]] || continue   # guard against empty glob
        local name
        name="$(basename "$sibling")"
        case "$name" in
            data|expected) continue ;;
        esac
        cp -r "$sibling" "${RUNNER_DIR}/${name}"
    done

    write_build_rs "$test_dir"
    if ! write_main_rs "${RUNNER_DIR}/program.dl" 2>"${RUNNER_DIR}/synth.log"; then
        local detail
        detail="$(tail -20 "${RUNNER_DIR}/synth.log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "main.rs synthesis failed" "$detail"
        return
    fi

    local build_log="${RUNNER_DIR}/build.log"
    local run_log="${RUNNER_DIR}/run.log"

    # `cargo run` re-checks the build graph, so a separate `cargo build` is
    # unnecessary. If the build fails the stderr lands in `build_log`.
    if ! (cd "${RUNNER_DIR}" || exit; cargo run --release --quiet 2>"$build_log" >"$run_log"); then
        local detail
        detail="$(tail -25 "$build_log" 2>/dev/null | sed 's/^/         /')"
        record_failure "$full_name" "lib run failed" "$detail"
        return
    fi

    # `use_sort=1`: lib mode output order is non-deterministic.
    local mismatch_detail
    if mismatch_detail=$(compare_expected_outputs "$test_dir" "${RUNNER_DIR}/output" 1); then
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
    # Uses a real reachability fixture so we don't depend on degenerate
    # edge cases (like an EDB that's also an output with no rules).
    echo -e "  ${YELLOW}Warming runner crate (release)...${NC}"
    write_build_rs ""     # no fixture yet; warm-up program has no includes
    cat > "${RUNNER_DIR}/program.dl" <<'EOF'
.decl Source(id: int32)
.input Source()
.decl Edge(x: int32, y: int32)
.input Edge()
.decl Reach(id: int32)
Reach(y) :- Source(y).
Reach(y) :- Reach(x), Edge(x, y).
.output Reach
EOF
    cat > "${RUNNER_DIR}/src/main.rs" <<'EOF'
pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}
fn main() {}
EOF
    (cd "${RUNNER_DIR}" || exit; cargo build --release --quiet 2>&1 | tail -5) \
        || die "warm-up build failed"

    total=$(count_tests "$@")
    echo -e "  ${DIM}Running ${total} tests...${NC}"
    echo ""

    if [[ $# -gt 0 ]]; then
        local name
        for name in "$@"; do
            local cat
            cat="$(find_test "$name")" || die "Test not found: $name (searched datalog-batch only)"
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
