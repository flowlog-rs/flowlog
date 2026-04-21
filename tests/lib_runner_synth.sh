#!/usr/bin/env bash
#
# Lib-mode runner crate synthesis — shared between unit and complex test
# runners. The library API has no file I/O, so each test driver synthesizes
# a small Rust crate that uses `flowlog-build` to compile the .dl into a
# typed engine, parses CSVs, drives the engine, and writes outputs.
#
# Caller contract:
#
#   # Required: path to the (persistent) runner crate directory.
#   LIB_RUNNER_DIR="${ROOT_DIR}/target/e2e-lib/runner"
#
#   # Optional Builder knobs — unset = default (off). Set to 1 to enable.
#   LIB_RUNNER_SIP=1
#   LIB_RUNNER_STR_INTERN=1
#
#   source "${TESTS_DIR}/lib_runner_synth.sh"
#
#   ensure_runner_crate
#   write_build_rs "$test_dir"
#   write_main_rs  "$test_dir/program.dl"
#   (cd "$LIB_RUNNER_DIR" && cargo run --release -- )
#
# Runtime: the synthesized `main.rs` reads `WORKERS` from the environment
# and passes it to `DatalogBatchEngine::new(n)`. Unset → workers=1.

[[ -n "${FLOWLOG_LIB_RUNNER_SYNTH_SH_LOADED:-}" ]] && return 0
FLOWLOG_LIB_RUNNER_SYNTH_SH_LOADED=1

source "$(dirname "${BASH_SOURCE[0]}")/shared.sh"
source "$(dirname "${BASH_SOURCE[0]}")/lib_synth_common.sh"

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
# and any sibling included .dl files. Anchored to line-start (modulo
# whitespace) so `// .input Foo` comments are skipped.
parse_input_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '^[[:space:]]*\.input[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$f" 2>/dev/null || true) \
            | awk '{ print tolower($2) }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Resolve the data filename for `.input <rel>` across the .dl + every
# sibling include file. Wraps [`input_filename_for`] from `tests/shared.sh`
# with the include-walk; falls back to `<rel>.csv` if no .input is found.
parse_input_filename() {
    local dl_file="$1" rel="$2"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        if grep -qiE "^[[:space:]]*\.input[[:space:]]+${rel}([[:space:]]|\\()" "$f" 2>/dev/null; then
            input_filename_for "$f" "$rel"
            return 0
        fi
    done < <(all_dl_files "$dl_file")
    echo "${rel}.csv"
}

# Extract output relation names (lowercase, one per line) from a .dl file
# and any sibling included .dl files. Treats `.printsize` as `.output` so
# callers that pre-rewrite the .dl aren't required. Anchored to line-start
# (modulo whitespace) so commented directives are skipped.
parse_output_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '^[[:space:]]*\.(output|printsize)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$f" 2>/dev/null || true) \
            | awk '{ print tolower($2) }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Lookup a `.decl <Name>(field1: type1, field2: type2, ...)` declaration
# across the .dl file and any sibling included files. Echoes a
# space-separated list of field names. Returns 1 if not found.
parse_decl_fields() {
    local dl_file="$1"
    local rel="$2"
    _parse_decl "$dl_file" "$rel" name
}

# Like parse_decl_fields but echoes `name:dltype` pairs.
parse_decl_typed_fields() {
    local dl_file="$1"
    local rel="$2"
    _parse_decl "$dl_file" "$rel" both
}

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
    # Attribute names are lowercased to mirror parser normalization
    # (crates/flowlog-build/src/parser/declaration/attribute.rs). The
    # generated struct exposes lowercase field names; loaders / writers
    # must match.
    if [[ "$mode" == "both" ]]; then
        echo "$inside" \
            | tr ',' '\n' \
            | awk -F: '{
                name = $1; ty = $2
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", ty)
                print tolower(name) ":" ty
              }' \
            | tr '\n' ' '
    else
        echo "$inside" \
            | tr ',' '\n' \
            | awk -F: '{
                name = $1
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
                print tolower(name)
              }' \
            | tr '\n' ' '
    fi
}

###############################################################################
# Runner crate management
###############################################################################

ensure_runner_crate() {
    [[ -n "${LIB_RUNNER_DIR:-}" ]] || die "ensure_runner_crate: LIB_RUNNER_DIR not set"
    mkdir -p "$LIB_RUNNER_DIR/src" "$LIB_RUNNER_DIR/data"

    cat > "$LIB_RUNNER_DIR/Cargo.toml" <<EOF
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
# build.rs synthesis
###############################################################################
#
# Honors optional globals:
#   LIB_RUNNER_SIP=1         → Builder::sip(true)
#   LIB_RUNNER_STR_INTERN=1  → Builder::string_intern(true)
#   LIB_RUNNER_EXTENDED=1    → Builder::mode(ExecutionMode::ExtendBatch)
#
# `test_dir` may be empty when called for a warm-up build.
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

    local knob_setters=""
    (( ${LIB_RUNNER_SIP:-0} ))        && knob_setters+=$'        .sip(true)\n'
    (( ${LIB_RUNNER_STR_INTERN:-0} )) && knob_setters+=$'        .string_intern(true)\n'
    (( ${LIB_RUNNER_EXTENDED:-0} ))   && knob_setters+=$'        .mode(flowlog_build::ExecutionMode::ExtendBatch)\n'

    local udf_setter=""
    (( has_udf )) && udf_setter=$'        .udf_file("udf.rs")\n'

    local builder_tail=""
    if [[ -n "$include_lines" ]]; then
        builder_tail+=$'        .compile(&["program.dl"], &include_dirs)'
    else
        builder_tail+=$'        .compile(&["program.dl"] as &[&str], &[] as &[&std::path::Path])'
    fi

    local include_decl=""
    if [[ -n "$include_lines" ]]; then
        include_decl=$'    let include_dirs: Vec<std::path::PathBuf> = vec![\n'"${include_lines}"$'    ];\n'
    fi

    cat > "${LIB_RUNNER_DIR}/build.rs" <<EOF
fn main() {
${include_decl}    let result = flowlog_build::Builder::default()
${knob_setters}${udf_setter}${builder_tail};
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
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

    local pascal
    pascal=$(pascal_case "$lower_name")

    # Build a positional tuple literal: `(parse_col_0, parse_col_1, ...)`.
    # The user-facing type `rel::<Pascal>` is a tuple alias now — no named
    # fields, no keyword-collision concerns.
    local tuple_exprs=""
    local first=1
    local arity=0
    for pair in $typed_fields; do
        local dltype="${pair#*:}"
        local rust_ty
        rust_ty=$(dl_to_rust_type "$dltype")
        local expr
        if [[ "$rust_ty" == "String" ]]; then
            expr="cols.next().unwrap().trim().to_string()"
        else
            expr="cols.next().unwrap().trim().parse::<${rust_ty}>().unwrap()"
        fi
        if (( first )); then
            tuple_exprs="${expr}"
            first=0
        else
            tuple_exprs+=", ${expr}"
        fi
        arity=$((arity + 1))
    done
    if (( arity == 1 )); then
        tuple_exprs+=","
    fi

    cat <<EOF
    {
        let __src = std::fs::read_to_string("data/${csv}")
            .expect("read data/${csv}");
        let __items: Vec<${pascal}> = __src
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let mut cols = l.split(',');
                (${tuple_exprs})
            })
            .collect();
        engine.insert_${lower_name}(__items);
    }
EOF
}

# Build the writer block for one output relation.
#
# The caller controls the output filename via `output_basename` (3rd arg).
# Unit mode passes the lowercase stem (`reach`); complex mode may pass the
# same or a suffixed form — the complex verifier accepts either.
#
# For nullary IDBs (no fields), writes `True` if the bool is set.
gen_writer_block() {
    local dl_file="$1"
    local lower_name="$2"
    local output_basename="$3"

    local fields
    fields=$(parse_decl_fields "$dl_file" "$lower_name") || {
        echo "// no decl for $lower_name" >&2
        return 1
    }

    if [[ -z "$fields" ]]; then
        cat <<EOF
    {
        let f = std::fs::File::create("output/${output_basename}")
            .expect("create output/${output_basename}");
        let mut w = std::io::BufWriter::new(f);
        if results.${lower_name} {
            writeln!(w, "True").expect("write");
        }
        w.flush().expect("flush");
    }
EOF
        return 0
    fi

    local field_count
    field_count=$(echo "$fields" | wc -w)
    local fmt=""
    local i
    for ((i=0; i<field_count; i++)); do
        (( i > 0 )) && fmt+=","
        fmt+="{}"
    done

    # Tuple indexing: `r.0, r.1, r.2 …`. Attribute names don't exist as
    # Rust fields anymore — the user-facing shape is a tuple alias.
    local accessors=""
    local i=0
    local first=1
    for _ in $fields; do
        if (( first )); then
            accessors+="r.${i}"
            first=0
        else
            accessors+=", r.${i}"
        fi
        i=$((i + 1))
    done

    cat <<EOF
    {
        let f = std::fs::File::create("output/${output_basename}")
            .expect("create output/${output_basename}");
        let mut w = std::io::BufWriter::new(f);
        for r in &results.${lower_name} {
            writeln!(w, "${fmt}", ${accessors}).expect("write");
        }
        w.flush().expect("flush");
    }
EOF
}

# Synthesize a fresh main.rs that drives the engine for the current test.
#
# Reads `WORKERS` from the environment and passes it to
# `DatalogBatchEngine::new(n)`. Unset or unparseable → 1 worker.
write_main_rs() {
    local dl_file="$1"
    local main_rs="${LIB_RUNNER_DIR}/src/main.rs"

    # Inputs: for each .input relation, find its data file (honouring any
    # `filename="..."` override on the .input directive) and emit a loader.
    local load_calls=""
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        local declared_csv csv_path
        declared_csv=$(parse_input_filename "$dl_file" "$rel")
        csv_path=$(find_csv_case_insensitive "${LIB_RUNNER_DIR}/data" "$declared_csv")
        [[ -n "$csv_path" ]] || continue
        load_calls+=$(gen_csv_loader "$dl_file" "$rel" "$(basename "$csv_path")")$'\n'
    done < <(parse_input_relations "$dl_file")

    # Output writers — one block per .output/.printsize relation. File name
    # is the lowercase relation name (matches binary-mode's convention that
    # compare_expected_outputs / verify_output both use).
    local writer_blocks=""
    local lower
    while IFS= read -r lower; do
        [[ -n "$lower" ]] || continue
        local block
        block=$(gen_writer_block "$dl_file" "$lower" "$lower")
        writer_blocks+="${block}"$'\n'
    done < <(parse_output_relations "$dl_file")

    cat > "$main_rs" <<EOF
// Auto-generated by tests/lib_runner_synth.sh — do not edit.
#![allow(unused_imports, dead_code)]

pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}

use prog::DatalogBatchEngine;
use prog::rel::*;
use std::io::Write;

fn main() {
    std::fs::create_dir_all("output").expect("create output dir");

    let workers: usize = std::env::var("WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut engine = DatalogBatchEngine::new(workers);
${load_calls}
    let results = engine.run();

${writer_blocks}
}
EOF
}
