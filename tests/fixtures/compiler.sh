#!/usr/bin/env bash
#
# Compiler mode for `run.sh`: `flowlog-compiler` compiles `program.dl` to a
# standalone binary, which reads the fixture's CSVs and writes
# `output/<rel>` files. Defines `run_compiler_task <fixture_dir>` and
# `build_compiler`. Sourced by run.sh; not executable on its own.

source "$(dirname "${BASH_SOURCE[0]}")/sqlite_helper.sh"

readonly COMPILER_BIN="${TARGET_DIR}/release/flowlog-compiler"

# A direct path dependency keeps runtime version bumps testable before release.
export FLOWLOG_RUNTIME_PATH="${ROOT_DIR}/flowlog-runtime"

build_compiler() {
    echo -e "  ${YELLOW}Building compiler (release)...${NC}"
    (cd "$ROOT_DIR" && cargo build --release -p flowlog-compiler 2>&1 | tail -1)
    [[ -x "$COMPILER_BIN" ]] || die "Compiler binary not found: $COMPILER_BIN"
}

# Flags for `flowlog-compiler`, from the fixture's optional `udf.rs`,
# `compile_flags` (whitespace-split, `#` comments), and `include_dirs`
# (paths relative to the fixture, one per line).
compile_flags_for() {
    local test_dir="$1"
    [[ -f "$test_dir/udf.rs" ]] && printf '%s\n' --udf-file "$test_dir/udf.rs"
    if [[ -f "$test_dir/compile_flags" ]]; then
        sed 's/#.*//' "$test_dir/compile_flags" | xargs -n1 2>/dev/null || true
    fi
    if [[ -f "$test_dir/include_dirs" ]]; then
        local line
        while IFS= read -r line || [[ -n "$line" ]]; do
            [[ -n "$line" ]] && printf '%s\n' -I "$test_dir/$line"
        done < "$test_dir/include_dirs"
    fi
}

run_compiler_task() {
    local test_dir="$1"

    # The slot's scratch directory holds one fixture at a time: the binary,
    # its inputs, `output/`, and the logs. It is left in place after the
    # task, so a failure can be inspected.
    local work_dir="${SLOT_DIR}/compiler/work"
    local output_dir="${work_dir}/output"
    local compile_log="${work_dir}/compile.log"
    local run_log="${work_dir}/run.log"
    rm -rf "$work_dir"
    mkdir -p "$output_dir"

    # 1) Compile
    local -a compile_flags=()
    mapfile -t compile_flags < <(compile_flags_for "$test_dir")
    if ! "$COMPILER_BIN" -D output "${compile_flags[@]}" "$test_dir/program.dl" \
            -o "$work_dir/program" >"$compile_log" 2>&1; then
        fail_task "compilation failed" "$(log_tail "$compile_log")"
        return
    fi

    # 2) Stage inputs
    if compgen -G "$test_dir/data/*" > /dev/null; then
        cp "$test_dir"/data/* "$work_dir/"
    fi
    local sqlite=0
    if [[ -f "$test_dir/sqlite_setup.sql" ]]; then
        sqlite=1
        if ! setup_sqlite_fixture "$test_dir" "$work_dir" >"$run_log" 2>&1; then
            fail_task "SQLite setup failed" "$(log_tail "$run_log")"
            return
        fi
    fi

    # 3) Execute; an incremental fixture reads its transcript on stdin.
    local -a runtime_flags=()
    [[ -f "$test_dir/runtime_flags" ]] && mapfile -t runtime_flags < "$test_dir/runtime_flags"
    local stdin=/dev/null
    [[ -f "$test_dir/commands.txt" ]] && stdin="$test_dir/commands.txt"
    if ! (cd "$work_dir" && ./program "${runtime_flags[@]}" <"$stdin" >"$run_log" 2>&1); then
        fail_task "execution failed" "$(log_tail "$run_log")"
        return
    fi

    # `.printsize` reports on stdout rather than writing a file, so distill
    # those lines into one; the usual `expected/<name>` comparison pins them
    # from there.
    if grep -q '^\[size\]' "$run_log"; then
        grep '^\[size\]' "$run_log" > "${output_dir}/printsize"
    fi
    if (( sqlite )) && ! export_sqlite_outputs "$test_dir" "$work_dir" >>"$run_log" 2>&1; then
        fail_task "SQLite query failed" "$(log_tail "$run_log")"
        return
    fi

    # 4) Compare. SQLite rows and multi-worker output have no pinned order.
    local use_sort=$sqlite detail
    [[ -f "$test_dir/runtime_flags" ]] && grep -q -- '-w' "$test_dir/runtime_flags" && use_sort=1
    if detail=$(compare_expected_outputs "$test_dir" "$output_dir" "$use_sort"); then
        pass_task
    else
        fail_task "output mismatch" "$detail"
    fi
}
