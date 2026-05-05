#!/usr/bin/env bash
#
# tools/env_check.sh — environment health check for FlowLog.
#
# Surfaces every dependency, env var, and on-disk asset the test/benchmark
# stack relies on. Cheap to run (< 1 s); meant as the canonical answer to
# *"is my dev VM ready to launch a sweep / a closed-loop perf gate?"*.
#
# Useful in three contexts:
#
#   1. Manual: `make doctor` from the repo root before starting a long sweep.
#   2. CI: a Smoke job that fails closed when the runner image misses a tool.
#   3. External loops (e.g. groomer): one stable "is FlowLog hooked up?" call
#      instead of every consumer re-implementing the same checks.
#
# Output:
#   - Per-section coloured findings: ✓ green = OK, ! yellow = warning,
#     ✗ red = blocking error.
#   - Trailing summary with counts.
#
# Exit code:
#   0   green — every required check passed (warnings are tolerated)
#   1   red   — at least one blocking error
#
# Inputs (all optional; sane defaults pick up the dev-VM layout):
#   FLOWLOG_DIR                  repo root          (default: this script's ../..)
#   FLOWLOG_DATASETS_ROOT        dataset cache root (default: /datasets)
#   FLOWLOG_SOUFFLE_REF_CACHE    Soufflé tarball cache (default: $DATASETS_ROOT/souffle_ref_tarballs)
#   FLOWLOG_KEEP_DATASETS        truthy → datasets persist between iterations
#   STRICT                       1 → treat warnings as errors

set -euo pipefail

ROOT_DIR="${FLOWLOG_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
DATASETS_ROOT="${FLOWLOG_DATASETS_ROOT:-/datasets}"
SOUFFLE_REF_CACHE_DEFAULT="${DATASETS_ROOT}/souffle_ref_tarballs"
SOUFFLE_REF_CACHE="${FLOWLOG_SOUFFLE_REF_CACHE:-$SOUFFLE_REF_CACHE_DEFAULT}"

# tests/shared.sh owns the canonical truthy parser so we don't drift.
# shellcheck disable=SC1091
source "${ROOT_DIR}/tests/shared.sh"

errors=0
warns=0

# Tiny in-house log helpers (independent from log()/die() in shared.sh
# so the output style here is uniform with the env-check fingerprint).
green()  { printf '  \033[32m✓ %s\033[0m\n' "$*"; }
yellow() { printf '  \033[33m! %s\033[0m\n' "$*"; warns=$((warns+1)); }
red()    { printf '  \033[31m✗ %s\033[0m\n' "$*"; errors=$((errors+1)); }
heading() { printf '\n\033[1m── %s ──\033[0m\n' "$*"; }

# require_cmd <bin> [<install hint>]
require_cmd() {
    if command -v "$1" >/dev/null 2>&1; then
        green "$1 is installed"
    else
        red "$1 not found${2:+ — $2}"
    fi
}

# Optional commands: print warning, not error.
suggest_cmd() {
    if command -v "$1" >/dev/null 2>&1; then
        green "$1 is installed (optional)"
    else
        yellow "$1 not found${2:+ — $2}"
    fi
}

heading "1. Repo layout"
if [[ -d "${ROOT_DIR}/.git" && -f "${ROOT_DIR}/Cargo.toml" ]]; then
    green "FLOWLOG_DIR=$ROOT_DIR is a flowlog checkout"
else
    red "FLOWLOG_DIR=$ROOT_DIR is not a flowlog repo (no .git or Cargo.toml)"
fi

heading "2. Required commands"
require_cmd cargo   "rustup → https://rustup.rs"
require_cmd python3 "needed for median/diff math; install Python 3.6+"
require_cmd wget    "needed to fetch HuggingFace datasets / Soufflé references"
require_cmd unzip   "needed to extract dataset zips"
require_cmd tar     "needed to extract Soufflé reference tarballs"
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
if [[ -x "$TIME_BIN" ]]; then
    green "GNU time -v at $TIME_BIN"
else
    red "GNU /usr/bin/time not found at $TIME_BIN — apt install time, or set TIME_BIN=<path>"
fi

heading "3. Optional commands"
suggest_cmd souffle "Soufflé baseline (--baseline=souffle); apt install souffle"
suggest_cmd duckdb  "L4 LDBC suite; install via /datasets/lib/install_duckdb.sh"

heading "4. Worker count"
nproc_val=$(nproc 2>/dev/null || echo "")
if [[ "$nproc_val" =~ ^[0-9]+$ ]] && (( nproc_val > 0 )); then
    green "nproc reports $nproc_val cores; default WORKERS = min(64, $nproc_val) = $((nproc_val<64 ? nproc_val : 64))"
else
    yellow "nproc unavailable or returned non-numeric; default WORKERS will fall back to 64"
fi

heading "5. Dataset cache (facts/)"
facts_link="${ROOT_DIR}/facts"
if [[ -L "$facts_link" ]]; then
    target=$(cd "$facts_link" 2>/dev/null && pwd -P || echo "")
    if [[ -z "$target" ]]; then
        red "facts/ is a dangling symlink (target unreachable)"
    elif [[ "$target" == "$DATASETS_ROOT/facts" ]]; then
        green "facts/ → $target (master cache)"
    else
        yellow "facts/ → $target (NOT $DATASETS_ROOT/facts; may still work)"
    fi
elif [[ -d "$facts_link" ]]; then
    yellow "facts/ is a real directory; datasets will be downloaded under it on first use"
elif [[ -e "$facts_link" ]]; then
    red "facts/ exists but is neither a symlink nor a directory"
else
    yellow "facts/ missing — first run will create it and live-download into it"
fi

heading "6. FLOWLOG_KEEP_DATASETS"
if flowlog_truthy "${FLOWLOG_KEEP_DATASETS:-}"; then
    green "FLOWLOG_KEEP_DATASETS=${FLOWLOG_KEEP_DATASETS} (truthy; per-pair cleanup is skipped)"
elif [[ -L "$facts_link" ]]; then
    yellow "FLOWLOG_KEEP_DATASETS not truthy; symlink-safety guard will still preserve facts/, but persistent runs are smoother with FLOWLOG_KEEP_DATASETS=1 (or \`source ${DATASETS_ROOT}/env.sh\`)"
else
    yellow "FLOWLOG_KEEP_DATASETS not truthy — datasets are deleted after each pair to reclaim disk"
fi

heading "7. Soufflé reference cache (optional)"
if [[ -n "${FLOWLOG_SOUFFLE_REF_CACHE:-}" ]]; then
    if [[ -d "$SOUFFLE_REF_CACHE" ]]; then
        n=$(find "$SOUFFLE_REF_CACHE" -maxdepth 1 -name '*.tar.gz' 2>/dev/null | wc -l)
        green "FLOWLOG_SOUFFLE_REF_CACHE=$SOUFFLE_REF_CACHE  ($n .tar.gz cached)"
    else
        red "FLOWLOG_SOUFFLE_REF_CACHE=$SOUFFLE_REF_CACHE — directory missing"
    fi
elif [[ -d "$SOUFFLE_REF_CACHE_DEFAULT" ]]; then
    n=$(find "$SOUFFLE_REF_CACHE_DEFAULT" -maxdepth 1 -name '*.tar.gz' 2>/dev/null | wc -l)
    yellow "Soufflé tarballs found at $SOUFFLE_REF_CACHE_DEFAULT ($n files) but FLOWLOG_SOUFFLE_REF_CACHE is unset — set it (or \`source ${DATASETS_ROOT}/env.sh\`) to skip live HuggingFace downloads"
else
    yellow "no local Soufflé reference cache; L2 will live-download from HuggingFace (~slow first run)"
fi

heading "8. Tree cleanliness"
if (cd "$ROOT_DIR" && [[ -z "$(git status --porcelain -uno 2>/dev/null)" ]]); then
    green "tree clean (any tooling that asserts is_clean will pass)"
else
    yellow "tree has uncommitted changes (closed-loop tooling like groomer's is_clean check may refuse to start):"
    (cd "$ROOT_DIR" && git status --porcelain -uno 2>/dev/null | sed 's/^/        /')
fi

heading "9. Cache-safety regression test"
if [[ -x "${ROOT_DIR}/tests/safety/cleanup_dataset_test.sh" ]]; then
    if (cd "$ROOT_DIR" && bash tests/safety/cleanup_dataset_test.sh >/dev/null 2>&1); then
        green "make test-safety passes (cleanup_dataset symlink-guard contract intact across L2/L3/L4)"
    else
        red "make test-safety FAILS — cleanup contract is broken; running L2/L3/L4 may rm -rf the dataset cache through the symlink"
    fi
else
    yellow "tests/safety/cleanup_dataset_test.sh not executable; skipping"
fi

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
heading "Summary"
if (( errors > 0 )); then
    red "$errors blocking error(s), $warns warning(s)"
    exit 1
fi
if (( warns > 0 )); then
    if [[ "${STRICT:-0}" == "1" ]]; then
        red "$warns warning(s) (STRICT=1)"
        exit 1
    fi
    yellow "0 errors, $warns warning(s) — env can run but may live-download or hit edge cases"
else
    green "all checks passed — env is ready"
fi
exit 0
