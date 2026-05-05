#!/usr/bin/env bash
#
# Safety regression test for cleanup_dataset.
#
# `tests/oracle/common.sh` is the only `cleanup_dataset` implementation
# left in this repo (the perf-side copies in `tools/benchmark/compare.sh`
# and `tests/ldbc/ldbc.sh` moved to the flowlog-bench sibling repo with
# the perf split — see ../../AGENTS.md). The contract it must honour:
#
#   1. FLOWLOG_KEEP_DATASETS truthy   → never delete (highest priority).
#                                       Truthy = 1 / yes / true / on,
#                                       any case (per tests/lib/shared.sh's
#                                       flowlog_truthy parser).
#   2. FACT_DIR is a symlink          → never delete unless
#                                       FLOWLOG_FORCE_CLEANUP=1. Protects
#                                       the persistent /datasets cache
#                                       from being rm -rf'd through the
#                                       symlink.
#   3. otherwise                      → delete the dataset's subdir.
#   4. FLOWLOG_FORCE_CLEANUP=1        → override clause #2 only (the
#                                       symlinked path).
#
# This test exercises the contract by sourcing the script with a stubbed
# environment (faux FACT_DIR / faux ROOT_DIR) so the body of
# cleanup_dataset can be invoked in isolation, no real datasets touched.
#
# It is fast (<1s) and is meant as a pre-flight gate. A regression in the
# safety guard would silently delete tens of GB of dataset cache on the
# next oracle run (or on an external tool that imports common.sh), so we
# want this guard checked first.
#
# Exit codes:
#   0  all assertions passed
#   1  at least one assertion failed (test prints diff + exits)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ----------------------------------------------------------------------
# Tiny test framework.
# ----------------------------------------------------------------------
PASS=0
FAIL=0
fail() { printf '\033[0;31m[FAIL]\033[0m %s\n' "$*" >&2; FAIL=$((FAIL + 1)); }
ok()   { printf '\033[0;32m[ OK ]\033[0m %s\n' "$*"; PASS=$((PASS + 1)); }

assert_exists() {
    local p="$1" desc="$2"
    [[ -e "$p" ]] && ok "$desc" || fail "$desc (missing: $p)"
}
assert_gone() {
    local p="$1" desc="$2"
    [[ ! -e "$p" ]] && ok "$desc" || fail "$desc (still present: $p)"
}

# ----------------------------------------------------------------------
# tests/oracle/common.sh::cleanup_dataset
# ----------------------------------------------------------------------
test_oracle_common() {
    local layer="tests/oracle/common.sh"
    local real_cache="$TMP/oracle_real_cache"
    local repo_facts="$TMP/oracle_repo_facts"
    mkdir -p "$real_cache/cspa-httpd"
    touch "$real_cache/cspa-httpd/Arc.csv"
    ln -s "$real_cache" "$repo_facts"

    local shim="$TMP/oracle_shim.sh"
    cat > "$shim" <<EOF
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$ROOT"
source "$ROOT/tests/oracle/common.sh"
cleanup_dataset "\$1" "\$2"
EOF
    chmod +x "$shim"

    # Case A: KEEP_DATASETS=1 → keep
    FLOWLOG_KEEP_DATASETS=1 "$shim" cspa-httpd "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/cspa-httpd" "$layer: KEEP_DATASETS=1 retains the dataset"

    # Case B: KEEP unset, fact_dir is a symlink → keep
    unset FLOWLOG_KEEP_DATASETS FLOWLOG_FORCE_CLEANUP || true
    "$shim" cspa-httpd "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/cspa-httpd" \
        "$layer: symlinked fact_dir retains the dataset (no FLOWLOG_FORCE_CLEANUP)"

    # Case C: FLOWLOG_FORCE_CLEANUP=1 → DELETE through the symlink
    FLOWLOG_FORCE_CLEANUP=1 "$shim" cspa-httpd "$repo_facts" >/dev/null 2>&1
    assert_gone "$real_cache/cspa-httpd" \
        "$layer: FLOWLOG_FORCE_CLEANUP=1 overrides the symlink guard"

    # Case D: KEEP unset, plain (non-symlink) fact_dir → DELETE
    rm -rf "$repo_facts" "$real_cache"
    mkdir -p "$repo_facts/csda-linux"
    touch "$repo_facts/csda-linux/marker"
    "$shim" csda-linux "$repo_facts" >/dev/null 2>&1
    assert_gone "$repo_facts/csda-linux" \
        "$layer: plain fact_dir deletes the dataset"

    # Case E: FLOWLOG_KEEP_DATASETS truthy synonyms (yes / true / on) → must keep.
    # Regression check for the "literal '1' only" trap: a user setting
    # KEEP_DATASETS=yes/true/on (common shell convention) should be
    # honoured rather than silently treated as falsy and the dataset
    # deleted.
    rm -rf "$repo_facts"
    mkdir -p "$repo_facts/csda-postgresql"
    touch "$repo_facts/csda-postgresql/marker"
    FLOWLOG_KEEP_DATASETS=yes "$shim" csda-postgresql "$repo_facts" >/dev/null 2>&1
    assert_exists "$repo_facts/csda-postgresql" \
        "$layer: KEEP_DATASETS=yes (truthy synonym) retains the dataset"
    FLOWLOG_KEEP_DATASETS=true "$shim" csda-postgresql "$repo_facts" >/dev/null 2>&1
    assert_exists "$repo_facts/csda-postgresql" \
        "$layer: KEEP_DATASETS=true (truthy synonym) retains the dataset"
    FLOWLOG_KEEP_DATASETS=on "$shim" csda-postgresql "$repo_facts" >/dev/null 2>&1
    assert_exists "$repo_facts/csda-postgresql" \
        "$layer: KEEP_DATASETS=on (truthy synonym) retains the dataset"
}

# ----------------------------------------------------------------------
# Drive the suite.
# ----------------------------------------------------------------------
echo "=== cleanup_dataset safety regression test ==="
test_oracle_common
echo
if (( FAIL > 0 )); then
    printf '\033[0;31m[FAIL]\033[0m %d/%d assertions failed\n' "$FAIL" "$((PASS + FAIL))" >&2
    exit 1
fi
printf '\033[0;32m[PASS]\033[0m all %d safety assertions passed\n' "$PASS"
