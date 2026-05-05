#!/usr/bin/env bash
#
# Safety regression test for cleanup_dataset across L2/L3/L4.
#
# All three layers (`tests/oracle/common.sh`, `tools/benchmark/compare.sh`,
# `tests/ldbc/ldbc.sh`) implement their own `cleanup_dataset` because the
# scripts run in slightly different contexts. They are required to honour
# the same env-var contract:
#
#   1. FLOWLOG_KEEP_DATASETS=1      → never delete (highest priority).
#   2. FACT_DIR is a symlink        → never delete unless FLOWLOG_FORCE_CLEANUP=1.
#                                     Protects the persistent /datasets cache
#                                     from being rm -rf'd through the symlink.
#   3. otherwise                    → delete the dataset's subdir.
#   4. FLOWLOG_FORCE_CLEANUP=1      → override clause #2 (symlinked path).
#
# This test exercises the contract by sourcing each script with a stubbed
# environment (faux FACT_DIR / faux ROOT_DIR) so the body of cleanup_dataset
# can be invoked in isolation, no real datasets touched.
#
# It is fast (<1s) and ships as a sweep pre-flight step ahead of L1 — a
# regression in the safety guard would silently delete tens of GB of dataset
# cache on the next L2/L3 run, so we want this guard checked first.
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
# Test 1: tests/oracle/common.sh::cleanup_dataset
# ----------------------------------------------------------------------
test_complex_common() {
    local layer="L2 tests/oracle/common.sh"
    local real_cache="$TMP/L2_real_cache"
    local repo_facts="$TMP/L2_repo_facts"
    mkdir -p "$real_cache/cspa-httpd"
    touch "$real_cache/cspa-httpd/Arc.csv"
    ln -s "$real_cache" "$repo_facts"

    local shim="$TMP/L2_shim.sh"
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

    # Case E: FLOWLOG_KEEP_DATASETS=yes (truthy synonym) → must keep.
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
}

# ----------------------------------------------------------------------
# Test 2: tools/benchmark/compare.sh::cleanup_dataset
# ----------------------------------------------------------------------
test_compare_sh() {
    local layer="L3 tools/benchmark/compare.sh"
    local real_cache="$TMP/L3_real_cache"
    local repo_facts="$TMP/L3_repo_facts"
    mkdir -p "$real_cache/livejournal"
    touch "$real_cache/livejournal/Arc.csv"
    ln -s "$real_cache" "$repo_facts"

    # compare.sh has top-level `set -euo pipefail` + arg parsing that would
    # exit on source. Extract just the cleanup_dataset function body via awk.
    local fn_only="$TMP/L3_cleanup_only.sh"
    awk '
        /^cleanup_dataset\(\) \{/ { in_fn = 1 }
        in_fn { print }
        in_fn && /^\}/ { in_fn = 0; exit }
    ' "$ROOT/tools/benchmark/compare.sh" > "$fn_only"

    local shim="$TMP/L3_shim.sh"
    cat > "$shim" <<EOF
#!/usr/bin/env bash
set -euo pipefail
RED=''; YELLOW=''; GREEN=''; BLUE=''; CYAN=''; NC=''
log() { :; }
flowlog_truthy() {
    case "\${1:-}" in
        1|y|Y|yes|YES|true|TRUE|True|on|ON|On) return 0 ;;
        *) return 1 ;;
    esac
}
die() { echo "[ERROR] \$*" >&2; exit 1; }
FACT_DIR="\$2"
$(cat "$fn_only")
cleanup_dataset "\$1"
EOF
    chmod +x "$shim"

    FLOWLOG_KEEP_DATASETS=1 "$shim" livejournal "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/livejournal" "$layer: KEEP_DATASETS=1 retains the dataset"

    unset FLOWLOG_KEEP_DATASETS FLOWLOG_FORCE_CLEANUP || true
    "$shim" livejournal "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/livejournal" \
        "$layer: symlinked FACT_DIR retains the dataset (no FLOWLOG_FORCE_CLEANUP)"

    FLOWLOG_FORCE_CLEANUP=1 "$shim" livejournal "$repo_facts" >/dev/null 2>&1
    assert_gone "$real_cache/livejournal" \
        "$layer: FLOWLOG_FORCE_CLEANUP=1 overrides the symlink guard"

    rm -rf "$repo_facts" "$real_cache"
    mkdir -p "$repo_facts/orkut"
    touch "$repo_facts/orkut/marker"
    "$shim" orkut "$repo_facts" >/dev/null 2>&1
    assert_gone "$repo_facts/orkut" \
        "$layer: plain FACT_DIR deletes the dataset"

    # Truthy-synonym case: KEEP_DATASETS=yes (parallel to L2's case E).
    rm -rf "$repo_facts"
    mkdir -p "$repo_facts/cspa-postgresql"
    touch "$repo_facts/cspa-postgresql/marker"
    FLOWLOG_KEEP_DATASETS=true "$shim" cspa-postgresql "$repo_facts" >/dev/null 2>&1
    assert_exists "$repo_facts/cspa-postgresql" \
        "$layer: KEEP_DATASETS=true (truthy synonym) retains the dataset"
}

# ----------------------------------------------------------------------
# Test 3: tests/ldbc/ldbc.sh::cleanup_dataset
# ----------------------------------------------------------------------
test_ldbc_sh() {
    local layer="L4 tests/ldbc/ldbc.sh"
    local real_cache="$TMP/L4_real_cache"
    local repo_facts="$TMP/L4_repo_facts"
    mkdir -p "$real_cache/sf-1"
    touch "$real_cache/sf-1/comment.csv"
    ln -s "$real_cache" "$repo_facts"

    local fn_only="$TMP/L4_cleanup_only.sh"
    awk '
        /^cleanup_dataset\(\) \{/ { in_fn = 1 }
        in_fn { print }
        in_fn && /^\}/ { in_fn = 0; exit }
    ' "$ROOT/tests/ldbc/ldbc.sh" > "$fn_only"

    local shim="$TMP/L4_shim.sh"
    cat > "$shim" <<EOF
#!/usr/bin/env bash
set -euo pipefail
log() { :; }
flowlog_truthy() {
    case "\${1:-}" in
        1|y|Y|yes|YES|true|TRUE|True|on|ON|On) return 0 ;;
        *) return 1 ;;
    esac
}
die() { echo "[ERROR] \$*" >&2; exit 1; }
FACT_DIR="\$2"
$(cat "$fn_only")
cleanup_dataset "\$1"
EOF
    chmod +x "$shim"

    FLOWLOG_KEEP_DATASETS=1 "$shim" sf-1 "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/sf-1" "$layer: KEEP_DATASETS=1 retains the dataset"

    unset FLOWLOG_KEEP_DATASETS FLOWLOG_FORCE_CLEANUP || true
    "$shim" sf-1 "$repo_facts" >/dev/null 2>&1
    assert_exists "$real_cache/sf-1" \
        "$layer: symlinked FACT_DIR retains the dataset (no FLOWLOG_FORCE_CLEANUP)"

    FLOWLOG_FORCE_CLEANUP=1 "$shim" sf-1 "$repo_facts" >/dev/null 2>&1
    assert_gone "$real_cache/sf-1" \
        "$layer: FLOWLOG_FORCE_CLEANUP=1 overrides the symlink guard"

    # Truthy-synonym case: KEEP_DATASETS=on (parallel to L2/L3 case E).
    rm -rf "$repo_facts" "$real_cache"
    mkdir -p "$repo_facts/sf-3"
    touch "$repo_facts/sf-3/marker"
    FLOWLOG_KEEP_DATASETS=on "$shim" sf-3 "$repo_facts" >/dev/null 2>&1
    assert_exists "$repo_facts/sf-3" \
        "$layer: KEEP_DATASETS=on (truthy synonym) retains the dataset"
}

# ----------------------------------------------------------------------
# Drive the suite.
# ----------------------------------------------------------------------
echo "=== cleanup_dataset safety regression test ==="
test_complex_common
test_compare_sh
test_ldbc_sh
echo
if (( FAIL > 0 )); then
    printf '\033[0;31m[FAIL]\033[0m %d/%d assertions failed\n' "$FAIL" "$((PASS + FAIL))" >&2
    exit 1
fi
printf '\033[0;32m[PASS]\033[0m all %d safety assertions passed\n' "$PASS"
