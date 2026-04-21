#!/bin/bash
#
# Library-mode runner crate synthesis for the benchmark comparison.
#
# Shares the small leaf helpers in `tests/lib_synth_common.sh` with the
# unit / complex test synthesizer, but keeps its own crate-management +
# main.rs synthesis because the benchmark has different needs:
#
#   - No output-file writes (nothing to diff against).
#   - Fast, bulk CSV loading — parsed per-column into the typed Tuple
#     directly, no intermediate `rel::Foo` construction where avoidable.
#   - Emits a `Dataflow executed in <Duration>` line on stdout so compare.sh
#     can reuse `extract_total_time` without a new extractor.
#   - Load time is intentionally not reported (lib mode has no load API the
#     benchmark is measuring — the user loads however they like).
#
# Caller contract:
#
#   LIB_BENCH_RUNNER_DIR="${ROOT_DIR}/target/bench-lib/runner"
#   LIB_BENCH_SIP=0          # 1 → Builder::sip(true)
#   LIB_BENCH_STR_INTERN=0   # 1 → Builder::string_intern(true)
#
#   source "tools/benchmark/lib_runner.sh"
#
#   bench_lib_ensure_crate
#   bench_lib_write_build_rs
#   bench_lib_write_main_rs "${program_dl}"
#   (cd "$LIB_BENCH_RUNNER_DIR" && WORKERS=$W cargo run --release --quiet)
#
# Runtime: the synthesized main.rs reads WORKERS from the environment
# and passes it to `DatalogBatchEngine::new(n)`. Default 1.

[[ -n "${FLOWLOG_BENCH_LIB_RUNNER_LOADED:-}" ]] && return 0
FLOWLOG_BENCH_LIB_RUNNER_LOADED=1

# Shared lib-mode synth helpers (`.input` filename resolver, CSV finder,
# DL → Rust type, PascalCase) — also used by `tests/lib_runner_synth.sh`.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/tests/lib_synth_common.sh"

###############################################################################
# .dl parsing — minimal, local to this file. Handles a single self-contained
# program.dl (no .include chain; benchmark programs don't use includes).
###############################################################################

# Input relation names, lowercase, one per line. Anchored to line-start
# (modulo whitespace) so `// .input Foo` comments are skipped.
_bench_lib_parse_inputs() {
    grep -oE '^[[:space:]]*\.input[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$1" 2>/dev/null \
        | awk '{ print tolower($2) }' | sort -u
}

# Output relations (.output OR .printsize) — the benchmark doesn't verify,
# but printsize IDBs also need a writer block or the generated BatchResults
# field ends up unused (harmless) — we just skip touching outputs entirely.

# Echo `name1:type1 name2:type2 ...` for a `.decl Name(...)` declaration.
_bench_lib_parse_decl_typed() {
    local dl_file="$1" rel="$2"
    local line
    line=$(grep -iE "^[[:space:]]*\.decl[[:space:]]+${rel}[[:space:]]*\(" "$dl_file" \
        2>/dev/null | head -1 || true)
    [[ -n "$line" ]] || return 1
    local inside
    inside=$(echo "$line" | sed -E 's/^[^(]*\(([^)]*)\).*$/\1/')
    [[ -n "$inside" ]] || { echo ""; return 0; }  # nullary
    # Attribute names are lowercased to mirror parser normalization
    # (crates/flowlog-build/src/parser/declaration/attribute.rs) — the
    # generated struct exposes lowercase field names, so all consumers
    # must see the same.
    echo "$inside" \
        | tr ',' '\n' \
        | awk -F: '{
            name = $1; ty = $2
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", ty)
            print tolower(name) ":" ty
          }' \
        | tr '\n' ' '
}

###############################################################################
# Runner crate management
###############################################################################

bench_lib_ensure_crate() {
    [[ -n "${LIB_BENCH_RUNNER_DIR:-}" ]] || die "LIB_BENCH_RUNNER_DIR not set"
    mkdir -p "$LIB_BENCH_RUNNER_DIR/src"

    cat > "$LIB_BENCH_RUNNER_DIR/Cargo.toml" <<EOF
[package]
name = "flowlog_bench_lib"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
flowlog-runtime = { path = "${ROOT_DIR}/crates/flowlog-runtime" }
# Match the compiler-generated binary's allocator (see
# crates/flowlog-compiler/src/imports.rs). DD allocates per-batch /
# per-tuple in tight loops; mimalloc is materially faster than glibc here.
mimalloc = "0.1"
# Generated semiring code (e.g. cc.dl pulls in min-int) emits
# `#[derive(Serialize, Deserialize)]`, so serde must be in scope.
serde = { version = "1", features = ["derive"] }

[build-dependencies]
flowlog-build = { path = "${ROOT_DIR}/crates/flowlog-build" }

[workspace]

[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
EOF
}

bench_lib_write_build_rs() {
    local knob_setters=""
    (( ${LIB_BENCH_SIP:-0} ))        && knob_setters+=$'        .sip(true)\n'
    (( ${LIB_BENCH_STR_INTERN:-0} )) && knob_setters+=$'        .string_intern(true)\n'

    cat > "${LIB_BENCH_RUNNER_DIR}/build.rs" <<EOF
fn main() {
    let result = flowlog_build::Builder::default()
${knob_setters}        .compile(&["program.dl"] as &[&str], &[] as &[&std::path::Path]);
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
EOF
}

###############################################################################
# main.rs synthesis
#
# Per input relation, synthesize an efficient loader that:
#   1. Reads the file once (std::fs::read_to_string).
#   2. Iterates lines, splits by `,`, parses each column into its column type.
#   3. Builds `Vec<rel::Foo>` (a tuple alias) and calls `engine.insert_<rel>`.
#
# Load is not timed; only `engine.run()` is wrapped with Instant::now().
###############################################################################

# Generate one loader block. Expects the input CSV at path `$csv_path` from
# the runner crate's working directory (we pass absolute paths via env).
_bench_lib_gen_loader() {
    local dl_file="$1" rel="$2" csv_env="$3"

    local typed_fields
    typed_fields=$(_bench_lib_parse_decl_typed "$dl_file" "$rel") || return 1

    local pascal
    pascal=$(pascal_case "$rel")

    # Build a positional tuple literal: `(parse_col_0, parse_col_1, ...)`.
    # Attribute names no longer surface as Rust idents — the user-facing type
    # is just the tuple alias `rel::<Pascal>`.
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
    # A 1-tuple needs the trailing comma to parse as a tuple, not a grouping.
    if (( arity == 1 )); then
        tuple_exprs+=","
    fi

    cat <<EOF
    {
        let path = std::env::var("${csv_env}")
            .expect("missing env ${csv_env}");
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {}", path, e));
        let items: Vec<${pascal}> = src
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let mut cols = l.split(',');
                (${tuple_exprs})
            })
            .collect();
        engine.insert_${rel}(items);
    }
EOF
}

# Synthesize main.rs. Second arg is a space-separated list of
# `relname=csvabsolutepath` pairs; each is plumbed via a runtime env var
# the generated Rust reads (avoids embedding absolute paths in the source,
# so the crate can be reused across (program,dataset) pairs without
# recompiling the build.rs step).
bench_lib_write_main_rs() {
    local dl_file="$1" rel_csv_pairs="$2"
    local main_rs="${LIB_BENCH_RUNNER_DIR}/src/main.rs"

    # Build one env var name per input relation: FLOWLOG_CSV_<REL>.
    local loaders=""
    local rel csv_abs env_name
    for pair in $rel_csv_pairs; do
        rel="${pair%%=*}"
        env_name="FLOWLOG_CSV_${rel^^}"
        local block
        block=$(_bench_lib_gen_loader "$dl_file" "$rel" "$env_name") || return 1
        loaders+="${block}"$'\n'
    done

    cat > "$main_rs" <<EOF
// Auto-generated by tools/benchmark/lib_runner.sh — do not edit.
#![allow(unused_imports, dead_code)]

// Match the compiler-generated binary's allocator. Without this the lib
// bench runs against glibc malloc while the compiler runs against mimalloc,
// turning the benchmark into a malloc comparison rather than an engine one.
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}

use prog::DatalogBatchEngine;
use prog::rel::*;

fn main() {
    let workers: usize = std::env::var("WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut engine = DatalogBatchEngine::new(workers);
${loaders}
    // Time only the dataflow execution — matches compare.sh's
    // "exec = total - load" semantics on the compiler side.
    let start = std::time::Instant::now();
    let _results = engine.run();
    let dur = start.elapsed();

    // Format compatible with extract_total_time in compare.sh.
    println!("Dataflow executed in {:?}", dur);
}
EOF
}

###############################################################################
# Build + list discovery helpers
###############################################################################

# Emit `rel=abspath` pairs for every .input relation whose CSV exists in
# $dataset_path. Honours `.input <Rel>(filename="X.csv")` overrides via
# the shared resolver; case-insensitive match on the on-disk filename.
bench_lib_discover_csvs() {
    local dl_file="$1" dataset_path="$2"
    local rel
    while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        local declared found
        declared=$(input_filename_for "$dl_file" "$rel")
        found=$(find_csv_case_insensitive "$dataset_path" "$declared")
        [[ -n "$found" ]] && printf '%s=%s\n' "$rel" "$found"
    done < <(_bench_lib_parse_inputs "$dl_file")
}
