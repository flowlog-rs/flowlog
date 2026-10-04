#!/usr/bin/env bash
#
# Library mode for `run.sh`: a runner crate links `flowlog-build` +
# `flowlog-runtime`, and a synthesized `main.rs` (tests/lib/runner_synth.sh)
# loads the fixture's CSVs, drives the engine, and writes `output/<rel>`
# files host-side. Defines `run_lib_task <fixture_dir>`. Sourced by run.sh;
# not executable on its own.

source "$(dirname "${BASH_SOURCE[0]}")/../lib/runner_synth.sh"

# The worker's runner crate, built once against a trivial program so Cargo
# has the dependencies compiled before the first fixture. `LIB_RUNNER_DIR`
# is read by tests/lib/runner_synth.sh.
LIB_RUNNER_DIR=""
LIB_RUNNER_BIN=""
prepare_lib_crate() {
    [[ -z "$LIB_RUNNER_DIR" ]] || return 0
    LIB_RUNNER_DIR="${SLOT_DIR}/lib/crate"
    LIB_RUNNER_BIN="${CARGO_TARGET_DIR}/release/flowlog_lib_runner"
    ensure_runner_crate
    write_build_rs ""
    cat > "${LIB_RUNNER_DIR}/program.dl" <<'EOF'
.decl Source(id: int32)
.input Source()
.decl Edge(x: int32, y: int32)
.input Edge()
.decl Reach(id: int32)
Reach(y) :- Source(y).
Reach(y) :- Reach(x), Edge(x, y).
.output Reach
EOF
    cat > "${LIB_RUNNER_DIR}/src/main.rs" <<'EOF'
pub mod prog {
    include!(concat!(env!("OUT_DIR"), "/program.rs"));
}
fn main() {}
EOF
    (cd "${LIB_RUNNER_DIR}" && cargo build --release --quiet 2>&1 | tail -5) \
        || die "warm-up build failed (${LIB_RUNNER_DIR})"
}

# Replace the crate's fixture files with this fixture's, leaving `src/`,
# `Cargo.toml`, and the build cache alone.
stage_fixture() {
    local test_dir="$1" incremental="$2"
    local crate="$LIB_RUNNER_DIR"

    rm -rf "$crate/data" "$crate/output" "$crate/program.dl" "$crate/udf.rs" "$crate/commands.txt"
    # Any earlier fixture's sibling directories (`.include` sources).
    local sibling
    for sibling in "$crate"/*/; do
        case "$(basename "$sibling")" in
            src) ;;
            *) rm -rf "$sibling" ;;
        esac
    done

    mkdir -p "$crate/data"
    cp "$test_dir/program.dl" "$crate/"
    [[ -f "$test_dir/udf.rs" ]] && cp "$test_dir/udf.rs" "$crate/"
    (( incremental )) && cp "$test_dir/commands.txt" "$crate/"
    if compgen -G "$test_dir/data/*" > /dev/null; then
        cp "$test_dir"/data/* "$crate/data/"
        # Incremental `insert <rel> @<path>` commands use paths relative to the
        # crate root, matching compiler mode's layout.
        (( incremental )) && cp "$test_dir"/data/* "$crate/"
    fi
    # Sibling directories other than data/ and expected/ hold `.include`
    # sources; copy them so relative includes resolve.
    for sibling in "$test_dir"/*/; do
        case "$(basename "$sibling")" in
            data|expected) ;;
            *) cp -r "$sibling" "$crate/" ;;
        esac
    done
}

run_lib_task() {
    local test_dir="$1"
    prepare_lib_crate

    local incremental=0
    [[ -f "$test_dir/commands.txt" ]] && incremental=1

    # Per-fixture `compile_flags` become Builder knobs in the synthesized
    # build.rs.
    LIB_RUNNER_STR_INTERN=0
    if [[ -f "$test_dir/compile_flags" ]] && grep -qw -- '--str-intern' "$test_dir/compile_flags"; then
        LIB_RUNNER_STR_INTERN=1
    fi

    stage_fixture "$test_dir" "$incremental"
    write_build_rs "$test_dir"

    local synth_log="${LIB_RUNNER_DIR}/synth.log"
    local build_log="${LIB_RUNNER_DIR}/build.log"
    local run_log="${LIB_RUNNER_DIR}/run.log"
    local synth=write_main_rs
    (( incremental )) && synth=write_main_rs_inc
    if ! "$synth" "${LIB_RUNNER_DIR}/program.dl" 2>"$synth_log"; then
        fail_task "main.rs synthesis failed" "$(log_tail "$synth_log")"
        return
    fi

    # Build, then launch the binary directly: `cargo run` would add its
    # dependency-graph check to every fixture.
    if ! (cd "${LIB_RUNNER_DIR}" && cargo build --release --quiet 2>"$build_log"); then
        fail_task "lib build failed" "$(log_tail "$build_log" 25)"
        return
    fi
    # The synthesized main reads `data/<csv>` and writes `output/<rel>`
    # relative to the crate root.
    if ! (cd "${LIB_RUNNER_DIR}" && "$LIB_RUNNER_BIN" >"$run_log" 2>>"$build_log"); then
        fail_task "lib run failed" "$(log_tail "$build_log" 25)"
        return
    fi

    # Library-mode output order is not pinned, so compare as sorted lines.
    local detail
    if detail=$(compare_expected_outputs "$test_dir" "${LIB_RUNNER_DIR}/output" 1 printsize); then
        pass_task
    else
        fail_task "output mismatch" "$detail"
    fi
}
