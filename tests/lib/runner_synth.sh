#!/usr/bin/env bash
#
# Lib-mode runner crate synthesis — shared between fixture (L1) and oracle
# (L2) test runners. The library API has no file I/O, so each test driver
# synthesizes a small Rust crate that uses `flowlog-build` to compile the
# .dl into a typed engine, parses CSVs, drives the engine, and writes
# outputs.
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
#   source "${ROOT_DIR}/tests/lib/runner_synth.sh"
#
#   ensure_runner_crate
#   write_build_rs "$test_dir"
#   write_main_rs  "$test_dir/program.dl"
#   (cd "$LIB_RUNNER_DIR" && cargo run --release)
#
# Runtime: the synthesized `main.rs` reads `WORKERS` from the environment
# and passes it to `DatalogBatchEngine::new(n)`. Unset → workers=1.

[[ -n "${FLOWLOG_LIB_RUNNER_SYNTH_SH_LOADED:-}" ]] && return 0
FLOWLOG_LIB_RUNNER_SYNTH_SH_LOADED=1

source "$(dirname "${BASH_SOURCE[0]}")/shared.sh"
source "$(dirname "${BASH_SOURCE[0]}")/synth_common.sh"

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
# whitespace) so `// .input Foo` comments are skipped. The `.` in the
# name pattern admits component-instance relations like `a.P`; on output
# the dot is rewritten to `·` (U+00B7) to mirror the inliner's rename
# in `flowlog-build/src/parser/program.rs::normalize_inliner_dots`.
parse_input_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '^[[:space:]]*\.input[[:space:]]+[A-Za-z_][A-Za-z0-9_.]*' "$f" 2>/dev/null || true) \
            | awk '{ name = tolower($2); gsub(/\./, "·", name); print name }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Resolve the data filename for `.input <rel>` across the .dl + every
# sibling include file. Wraps [`input_filename_for`] from `tests/lib/shared.sh`
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
# (modulo whitespace) so commented directives are skipped. The `.` in the
# name pattern admits component-instance relations like `a.P`; on output
# the dot is rewritten to `·` (U+00B7) to mirror the inliner's rename
# in `flowlog-build/src/parser/program.rs::normalize_inliner_dots`.
parse_output_relations() {
    local dl_file="$1"
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        (grep -oE '^[[:space:]]*\.(output|printsize)[[:space:]]+[A-Za-z_][A-Za-z0-9_.]*' "$f" 2>/dev/null || true) \
            | awk '{ name = tolower($2); gsub(/\./, "·", name); print name }'
    done < <(all_dl_files "$dl_file") \
        | sort -u
}

# Map a lowered output-relation name (as produced by `parse_output_relations`,
# e.g. `reach`, `a·p`) back to its on-disk output filename in the new
# Soufflé-compat shape: the case-preserved `.output`/`.printsize` token with
# literal dots, plus a `.csv` extension (e.g. `Reach.csv`, `a.P.csv`). Mirrors
# the compiler's `<RawName>.csv` convention so lib-mode output matches the
# `expected/` files. Falls back to `<lower>.csv` if no directive is found.
output_filename_for() {
    local dl_file="$1" want_lower="$2"
    local f raw rawlower
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        while IFS= read -r raw; do
            [[ -n "$raw" ]] || continue
            # Lowercase + dot→· to match parse_output_relations' awk form.
            rawlower="${raw,,}"; rawlower="${rawlower//./·}"
            if [[ "$rawlower" == "$want_lower" ]]; then
                printf '%s.csv' "$raw"
                return 0
            fi
        done < <(grep -oE '^[[:space:]]*\.(output|printsize)[[:space:]]+[A-Za-z_][A-Za-z0-9_.]*' "$f" 2>/dev/null | awk '{print $2}')
    done < <(all_dl_files "$dl_file")
    printf '%s.csv' "$want_lower"
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
    # Names that went through the dot→· rename refer to component-instance
    # relations (`a.P` from `.init a = Pair<…>`). The user `.decl` lives
    # inside the `.comp` body under its bare suffix (`P`), so fall back to
    # matching the post-`·` tail when the full name has no direct decl.
    local search_names=("$rel")
    if [[ "$rel" == *·* ]]; then
        search_names+=("${rel##*·}")
    fi
    local line=""
    local name
    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        for name in "${search_names[@]}"; do
            line=$(grep -iE "^[[:space:]]*\.decl[[:space:]]+${name}[[:space:]]*\(" "$f" 2>/dev/null | head -1 || true)
            [[ -n "$line" ]] && break 2
        done
    done < <(all_dl_files "$dl_file")
    [[ -n "$line" ]] || return 1
    local inside
    inside=$(echo "$line" | sed -E 's/^[^(]*\(([^)]*)\).*$/\1/')
    [[ -n "$inside" ]] || { echo ""; return 0; }  # nullary
    # Attribute names are lowercased to mirror parser normalization
    # (flowlog-build/src/parser/declaration/attribute.rs). The
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
# Both flowlog-runtime and flowlog-build are path-pinned to the in-repo
# crates so a local change to either is exercised by the lib-mode test
# suites. (Pre-leanness this pinned flowlog-runtime to crates.io 0.2,
# which silently masked any local-only runtime change.)
flowlog-runtime = { path = "${ROOT_DIR}/flowlog-runtime" }
serde = { version = "1", features = ["derive"] }

[build-dependencies]
flowlog-build = { path = "${ROOT_DIR}/flowlog-build" }

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

    # Pick exactly one mode line. Defaults to `DatalogBatch`; `LIB_RUNNER_INC`
    # toggles to incremental and combines with `LIB_RUNNER_EXTENDED`.
    local mode_setter=""
    if (( ${LIB_RUNNER_INC:-0} )); then
        if (( ${LIB_RUNNER_EXTENDED:-0} )); then
            mode_setter=$'        .mode(flowlog_build::ExecutionMode::ExtendInc)\n'
        else
            mode_setter=$'        .mode(flowlog_build::ExecutionMode::DatalogInc)\n'
        fi
    elif (( ${LIB_RUNNER_EXTENDED:-0} )); then
        mode_setter=$'        .mode(flowlog_build::ExecutionMode::ExtendBatch)\n'
    fi
    knob_setters+="$mode_setter"

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

    # `cargo:rerun-if-changed` for every build-time input. Without these,
    # Cargo's default "rerun if any package file changed" heuristic is
    # unreliable across the rapid program.dl rewrites this runner does
    # (each fixture reuses the same crate dir), so the build script can skip
    # regenerating program.rs and the test silently runs against a stale
    # program (e.g. the warm-up crate's TC). Listing the inputs explicitly
    # pins the rebuild to the .dl / udf / include files.
    local rerun_lines
    rerun_lines=$'    println!("cargo:rerun-if-changed=program.dl");\n'
    rerun_lines+=$'    println!("cargo:rerun-if-changed=build.rs");\n'
    (( has_udf )) && rerun_lines+=$'    println!("cargo:rerun-if-changed=udf.rs");\n'
    if [[ -n "$test_dir" && -f "$test_dir/include_dirs" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -z "$line" ]] && continue
            rerun_lines+="    println!(\"cargo:rerun-if-changed=${line}\");"$'\n'
        done < "$test_dir/include_dirs"
    fi

    cat > "${LIB_RUNNER_DIR}/build.rs" <<EOF
fn main() {
${rerun_lines}${include_decl}    let result = flowlog_build::Builder::default()
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

    # Splice the .input `delimiter=` value directly into a Rust char literal.
    # Both flowlog `.dl` source and Rust use the same `\t`/`\n`/`\r`/`\\`/`\0`
    # escape spelling, so the raw param is already valid Rust char syntax.
    local delim
    delim=$(input_delimiter_for "$dl_file" "$lower_name")

    cat <<EOF
    {
        let __src = std::fs::read_to_string("data/${csv}")
            .expect("read data/${csv}");
        let __items: Vec<${pascal}> = __src
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let mut cols = l.split('${delim}');
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

    # `.output Rel(delimiter="…")` controls the column separator; the raw
    # value uses the same escape spelling Rust's `format!` accepts (`\t`,
    # `\n`, `\r`, `\\`), so it's spliced directly into the format string.
    local delim
    delim=$(output_delimiter_for "$dl_file" "$lower_name")

    local field_count
    field_count=$(echo "$fields" | wc -w)
    local fmt=""
    local i
    for ((i=0; i<field_count; i++)); do
        (( i > 0 )) && fmt+="${delim}"
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
    # is the case-preserved `<RawName>.csv` (via output_filename_for), matching
    # the compiler's Soufflé-compat convention that compare_expected_outputs
    # diffs against.
    local writer_blocks=""
    local lower
    while IFS= read -r lower; do
        [[ -n "$lower" ]] || continue
        local block fname
        fname=$(output_filename_for "$dl_file" "$lower")
        block=$(gen_writer_block "$dl_file" "$lower" "$fname")
        writer_blocks+="${block}"$'\n'
    done < <(parse_output_relations "$dl_file")

    cat > "$main_rs" <<EOF
// Auto-generated by tests/lib/runner_synth.sh — do not edit.
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

###############################################################################
# Incremental-mode main.rs synthesis
###############################################################################
#
# Reads binary-inc fixtures (`commands.txt` transcript + `<rel>_t<N>`
# per-epoch delta expected files) and drives `DatalogIncrementalEngine`
# through typed insert/remove/set/unset calls. Since the library returns
# full snapshots rather than per-epoch deltas, the synthesized main.rs
# keeps a host-side mirror of every output relation and diffs it against
# each new snapshot to reproduce the binary's `<cols>\t<+1|-1>` line format.

# Rust parse expression for one CSV column at index `idx` of type `rust_ty`.
_inc_col_parse() {
    local idx="$1" rust_ty="$2"
    if [[ "$rust_ty" == "String" ]]; then
        printf 'cols[%s].trim().to_string()' "$idx"
    else
        printf 'cols[%s].trim().parse::<%s>().expect("bad col")' "$idx" "$rust_ty"
    fi
}

# Tuple-literal type for a relation, e.g. `(i32, i32)` or `(String,)`.
_inc_tuple_ty() {
    local dl_file="$1" rel="$2"
    local typed_fields
    typed_fields=$(parse_decl_typed_fields "$dl_file" "$rel") || return 1
    [[ -n "$typed_fields" ]] || { echo "()"; return 0; }
    local tys=""
    local arity=0 first=1
    for pair in $typed_fields; do
        local dltype="${pair#*:}"
        local ty
        ty=$(dl_to_rust_type "$dltype")
        if (( first )); then tys="$ty"; first=0; else tys+=", $ty"; fi
        arity=$((arity + 1))
    done
    if (( arity == 1 )); then
        printf '(%s,)' "$tys"
    else
        printf '(%s)' "$tys"
    fi
}

# Format string + accessor list for a tuple with `arity` fields. Used by
# the delta-emission block to produce `1\t2\t+1`-style output lines.
# Echoes one line: `<fmt>|<accessors>`.
_inc_fmt_and_accessors() {
    local arity="$1"
    local fmt="" accessors=""
    local i
    for ((i=0; i<arity; i++)); do
        (( i > 0 )) && { fmt+="\t"; accessors+=", "; }
        fmt+="{}"
        accessors+="t.${i}"
    done
    printf '%s|%s' "$fmt" "$accessors"
}

# Match arm for `put <rel> ...` on a non-nullary relation.
_inc_put_arm_nonnullary() {
    local dl_file="$1" rel="$2"
    local typed_fields arity=0 parse_lines=""
    typed_fields=$(parse_decl_typed_fields "$dl_file" "$rel") || return 1
    local i=0
    for pair in $typed_fields; do
        local dltype="${pair#*:}"
        local rust_ty
        rust_ty=$(dl_to_rust_type "$dltype")
        local parse_expr
        parse_expr=$(_inc_col_parse "$i" "$rust_ty")
        (( arity > 0 )) && parse_lines+=$'\n'
        parse_lines+="                        ${parse_expr},"
        arity=$((arity + 1))
        i=$((i + 1))
    done
    local tuple_ty
    tuple_ty=$(_inc_tuple_ty "$dl_file" "$rel")

    cat <<EOF
                    "${rel}" => {
                        let cols: Vec<&str> = tuple_str.split(',').collect();
                        if cols.len() != ${arity} {
                            eprintln!("bad ${rel} tuple: {}", tuple_str);
                            continue;
                        }
                        let v: ${tuple_ty} = (
${parse_lines}
                        );
                        if diff > 0 {
                            engine.insert_${rel}(vec![v]);
                        } else if diff < 0 {
                            engine.remove_${rel}(vec![v]);
                        }
                    }
EOF
}

# Match arm for `put <rel> True|False` on a nullary relation. `diff` is
# ignored — presence is carried entirely by the True/False token.
_inc_put_arm_nullary() {
    local rel="$1"
    cat <<EOF
                    "${rel}" => {
                        let s = tuple_str.trim().to_ascii_lowercase();
                        if s == "true" {
                            engine.set_${rel}();
                        } else if s == "false" {
                            engine.unset_${rel}();
                        } else {
                            eprintln!("nullary ${rel} expects True/False, got: {}", tuple_str);
                        }
                    }
EOF
}

# Match arm for `file <rel> <path>`. Nullary relations don't support file
# ingestion in the binary, so we mirror that by emitting an eprintln.
_inc_file_arm_nonnullary() {
    local dl_file="$1" rel="$2"
    local typed_fields arity=0 parse_lines=""
    typed_fields=$(parse_decl_typed_fields "$dl_file" "$rel") || return 1
    local i=0
    for pair in $typed_fields; do
        local dltype="${pair#*:}"
        local rust_ty
        rust_ty=$(dl_to_rust_type "$dltype")
        local parse_expr
        parse_expr=$(_inc_col_parse "$i" "$rust_ty")
        (( arity > 0 )) && parse_lines+=$'\n'
        parse_lines+="                                ${parse_expr},"
        arity=$((arity + 1))
        i=$((i + 1))
    done
    local tuple_ty
    tuple_ty=$(_inc_tuple_ty "$dl_file" "$rel")

    cat <<EOF
                    "${rel}" => {
                        let items: Vec<${tuple_ty}> = content
                            .lines()
                            .filter(|l| !l.trim().is_empty())
                            .map(|l| {
                                let cols: Vec<&str> = l.split(',').collect();
                                (
${parse_lines}
                                )
                            })
                            .collect();
                        if diff > 0 {
                            engine.insert_${rel}(items);
                        } else if diff < 0 {
                            engine.remove_${rel}(items);
                        }
                    }
EOF
}

_inc_file_arm_nullary() {
    local rel="$1"
    cat <<EOF
                    "${rel}" => {
                        eprintln!("nullary ${rel} does not support file ingestion");
                    }
EOF
}

# Per-output running-count state declaration. The engine returns raw
# DD multiplicity deltas, so the runner tracks counts itself to recover
# set-membership transitions for the expected fixture format.
_inc_prev_decl() {
    local dl_file="$1" rel="$2"
    local fields
    fields=$(parse_decl_fields "$dl_file" "$rel") || return 1
    if [[ -z "$fields" ]]; then
        echo "    let mut prev_count_${rel}: i64 = 0;"
        return 0
    fi
    local tuple_ty
    tuple_ty=$(_inc_tuple_ty "$dl_file" "$rel")
    echo "    let mut prev_counts_${rel}: HashMap<${tuple_ty}, i64> = HashMap::new();"
}

# Per-output delta emission block, run inside the "commit" match arm.
#
# Non-nullary: fold this commit's `Vec<(tuple, i32)>` into per-tuple
# diffs, then for each tuple compare prev count vs new count to detect
# set-membership transitions. Emit `<cols>\t+1` for 0→positive and
# `<cols>\t-1` for positive→0; suppress count-only changes that don't
# cross zero. Sorted for deterministic output.
#
# Nullary: same logic over a single running count — write "True"
# whenever the unit fact crosses 0, matching binary-mode behavior.
_inc_delta_block() {
    local dl_file="$1" rel="$2"
    local fields arity=0
    # Per-epoch delta filename base: `<RawName>` (case-preserved), so the
    # written path is `<RawName>_t<N>.csv` — `_t<N>` injected before the
    # extension, matching the compiler's incremental output convention.
    local out_base
    out_base=$(output_filename_for "$dl_file" "$rel")
    out_base="${out_base%.csv}"
    fields=$(parse_decl_fields "$dl_file" "$rel") || return 1
    if [[ -z "$fields" ]]; then
        cat <<EOF
                {
                    let diff = results.${rel} as i64;
                    let new_count = prev_count_${rel} + diff;
                    let was_true = prev_count_${rel} > 0;
                    let is_true = new_count > 0;
                    let path = format!("output/${out_base}_t{}.csv", commit_num);
                    let mut f = std::fs::File::create(&path).expect("create");
                    if was_true != is_true {
                        writeln!(f, "True").unwrap();
                    }
                    prev_count_${rel} = new_count;
                }
EOF
        return 0
    fi
    for _ in $fields; do arity=$((arity + 1)); done
    local tuple_ty fmt_acc fmt accessors
    tuple_ty=$(_inc_tuple_ty "$dl_file" "$rel")
    fmt_acc=$(_inc_fmt_and_accessors "$arity")
    fmt="${fmt_acc%|*}"
    accessors="${fmt_acc#*|}"

    cat <<EOF
                {
                    let mut commit_diffs: HashMap<${tuple_ty}, i64> = HashMap::new();
                    for (t, d) in results.${rel}.into_iter() {
                        *commit_diffs.entry(t).or_insert(0) += d as i64;
                    }
                    let mut lines: Vec<String> = Vec::new();
                    for (t, dc) in commit_diffs {
                        let prev_count = prev_counts_${rel}.get(&t).copied().unwrap_or(0i64);
                        let new_count = prev_count + dc;
                        let was_in = prev_count > 0;
                        let is_in = new_count > 0;
                        if !was_in && is_in {
                            lines.push(format!("${fmt}\t+1", ${accessors}));
                        } else if was_in && !is_in {
                            lines.push(format!("${fmt}\t-1", ${accessors}));
                        }
                        if new_count == 0 {
                            prev_counts_${rel}.remove(&t);
                        } else {
                            prev_counts_${rel}.insert(t, new_count);
                        }
                    }
                    lines.sort();
                    let path = format!("output/${out_base}_t{}.csv", commit_num);
                    let mut f = std::fs::File::create(&path).expect("create");
                    for line in &lines {
                        writeln!(f, "{}", line).unwrap();
                    }
                }
EOF
}

# Synthesize an inc-mode `main.rs`. Mirrors `write_main_rs` (batch) in
# entry-point shape — reads WORKERS from env, stages inputs, and writes
# to `output/` — but drives `Transaction`-scoped commits from
# `commands.txt` and emits per-commit delta files keyed to the
# commit index.
write_main_rs_inc() {
    local dl_file="$1"
    local main_rs="${LIB_RUNNER_DIR}/src/main.rs"

    # Collect per-EDB arms + per-output prev state + delta blocks.
    local put_arms="" file_arms=""
    local rel fields
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        fields=$(parse_decl_fields "$dl_file" "$rel") || true
        if [[ -z "$fields" ]]; then
            put_arms+=$(_inc_put_arm_nullary "$rel")$'\n'
            file_arms+=$(_inc_file_arm_nullary "$rel")$'\n'
        else
            put_arms+=$(_inc_put_arm_nonnullary "$dl_file" "$rel")$'\n'
            file_arms+=$(_inc_file_arm_nonnullary "$dl_file" "$rel")$'\n'
        fi
    done < <(parse_input_relations "$dl_file")

    local prev_decls="" delta_blocks=""
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        prev_decls+=$(_inc_prev_decl "$dl_file" "$rel")$'\n'
        delta_blocks+=$(_inc_delta_block "$dl_file" "$rel")$'\n'
    done < <(parse_output_relations "$dl_file")

    # Preload epoch — generated iff any EDB is declared `IO="file"`.
    # Binary-mode runs an implicit preload commit before the first user
    # command (loading the declared file into the engine) and surfaces
    # the result as `<rel>_t1`. We mirror that here by injecting an
    # explicit first-commit block so fixture commit indices stay aligned.
    local preload_inserts="" preload_block=""
    local any_file_backed=0
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        local fname
        fname=$(file_backed_filename "$dl_file" "$rel") || continue
        any_file_backed=1
        fields=$(parse_decl_fields "$dl_file" "$rel") || true
        [[ -z "$fields" ]] && continue  # nullary: file ingest not supported
        local typed_fields
        typed_fields=$(parse_decl_typed_fields "$dl_file" "$rel")
        local parse_lines="" i=0
        for pair in $typed_fields; do
            local dltype="${pair#*:}"
            local rust_ty
            rust_ty=$(dl_to_rust_type "$dltype")
            local parse_expr
            parse_expr=$(_inc_col_parse "$i" "$rust_ty")
            (( i > 0 )) && parse_lines+=$'\n'
            parse_lines+="                    ${parse_expr},"
            i=$((i + 1))
        done
        local tuple_ty
        tuple_ty=$(_inc_tuple_ty "$dl_file" "$rel")
        preload_inserts+=$(cat <<EOF
        if let Ok(content) = std::fs::read_to_string("${fname}") {
            let items: Vec<${tuple_ty}> = content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| {
                    let cols: Vec<&str> = l.split(',').collect();
                    (
${parse_lines}
                    )
                })
                .collect();
            if !items.is_empty() {
                engine.insert_${rel}(items);
            }
        }
EOF
)$'\n'
    done < <(parse_input_relations "$dl_file")

    if (( any_file_backed )); then
        preload_block=$(cat <<EOF
    {
        engine.begin();
${preload_inserts}
        commit_num += 1;
        let results = engine.commit();
${delta_blocks}
    }
EOF
)
    fi

    cat > "$main_rs" <<EOF
// Auto-generated by tests/lib/runner_synth.sh — do not edit.
#![allow(unused_imports, dead_code, unused_mut, unused_variables)]

pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}

use prog::DatalogIncrementalEngine;
use std::collections::{HashMap, HashSet};
use std::io::Write;

fn main() {
    std::fs::create_dir_all("output").expect("mkdir output");

    let workers: usize = std::env::var("WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let mut engine = DatalogIncrementalEngine::new(workers);

${prev_decls}

    let mut commit_num: u32 = 0;

${preload_block}

    let cmds = std::fs::read_to_string("commands.txt").expect("read commands.txt");

    for raw_line in cmds.lines() {
        let line = raw_line.trim();
        if line.is_empty() { continue; }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() { continue; }
        let head = parts[0].to_ascii_lowercase();
        match head.as_str() {
            "begin" | "txn" => engine.begin(),
            "commit" | "done" => {
                commit_num += 1;
                let results = engine.commit();
${delta_blocks}
            }
            "abort" | "rollback" => engine.abort(),
            "put" => {
                if parts.len() < 3 || parts.len() > 4 {
                    eprintln!("put: bad args: {}", line);
                    continue;
                }
                let rel = parts[1];
                let tuple_str = parts[2];
                let diff: i32 = parts
                    .get(3)
                    .map(|s| s.parse().expect("bad diff"))
                    .unwrap_or(1);
                match rel {
${put_arms}                    _ => eprintln!("unknown rel: {}", rel),
                }
            }
            "file" => {
                if parts.len() < 3 || parts.len() > 4 {
                    eprintln!("file: bad args: {}", line);
                    continue;
                }
                let rel = parts[1];
                let path_str = parts[2];
                let diff: i32 = parts
                    .get(3)
                    .map(|s| s.parse().expect("bad diff"))
                    .unwrap_or(1);
                let content = std::fs::read_to_string(path_str).expect("read file");
                match rel {
${file_arms}                    _ => eprintln!("unknown rel: {}", rel),
                }
            }
            "quit" | "exit" | "q" => break,
            _ => {}
        }
    }
}
EOF
}
