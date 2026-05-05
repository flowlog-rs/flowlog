#!/usr/bin/env bash
#
# tools/perf_compare.sh — perf + peak-RSS drift check between two SHAs.
#
# Designed for closed-loop tooling that wants to verify a series of
# in-tree changes (multiple commits accumulated by an agentic loop)
# hasn't regressed any benchmark beyond a tolerance — without invoking
# the heavy `compare.sh` cross-engine machinery (no Soufflé, no legacy
# interpreter).
#
# What it does:
#   1. Read a list of `<prog>=<dataset>` pairs from a config file.
#      Same format as tools/benchmark/config.txt; per-pair tags are
#      ignored here (no engines to skip).
#   2. For each pair, run `tools/benchmark/bench_one.sh <prog> <dataset>`
#      twice — once at <base_sha>, once at <head_sha> — capturing the
#      median elapsed_seconds + median peak_rss_kb.
#   3. Compare each metric to its baseline; flag PASS / WARN / FAIL.
#   4. Exit 0 iff every pair stayed within both tolerances; otherwise 1.
#
# A worktree is used so the active checkout / build artifacts are
# untouched. The two SHAs are checked out into TARGET_WORKTREE in
# turn; cargo's incremental cache makes the second build fast.
#
# Output:
#   - One stdout line per pair: `<pair>  time: <pct>  rss: <pct>  <verdict>`
#   - Trailing summary: `OK <n>/<N> pairs within tolerance`.
#
# Exit code:
#   0  every pair within tolerances
#   1  at least one pair regressed beyond a tolerance
#   2  argument / I/O error (config missing, sha unknown, …)
#
# Usage:
#   tools/perf_compare.sh <base_sha> <head_sha> [config_file]
#
# Environment:
#   PERF_COMPARE_CONFIG       default config file (overrides positional)
#   PERF_COMPARE_TIME_PCT     wall-time regression tolerance (default 10)
#   PERF_COMPARE_RSS_PCT      peak-RSS regression tolerance (default 20)
#   PERF_COMPARE_NUM_RUNS     bench_one.sh NUM_RUNS                (default 3)
#   PERF_COMPARE_WORKERS      bench_one.sh WORKERS                 (default 1
#                             — single-thread is the most stable signal
#                             for small-bench measurements)
#   TARGET_WORKTREE           where to put the throwaway checkout
#                             (default: <repo>/.perf-compare-work)
#
# This script is the canonical hook for groomer's
# `[finalisation].perf_compare_cmd`:
#
#   perf_compare_cmd = "tools/perf_compare.sh {base_sha} {head_sha} \
#                       /path/to/perf-bench-list.txt"

set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    awk '/^# =+$/ { sep++; next }
         sep==0 || sep==1 { sub(/^# ?/, ""); print }
         sep>=2 { exit }' "$0"
    exit 0
fi

(( $# >= 2 )) || { echo "usage: $0 <base_sha> <head_sha> [config_file]" >&2; exit 2; }

BASE_SHA="$1"
HEAD_SHA="$2"
CONFIG_FILE="${3:-${PERF_COMPARE_CONFIG:-}}"
[[ -n "$CONFIG_FILE" ]] || { echo "error: no config file given (positional or PERF_COMPARE_CONFIG)" >&2; exit 2; }
[[ -f "$CONFIG_FILE" ]] || { echo "error: config file not found: $CONFIG_FILE" >&2; exit 2; }

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIME_PCT="${PERF_COMPARE_TIME_PCT:-10}"
RSS_PCT="${PERF_COMPARE_RSS_PCT:-20}"
NUM_RUNS="${PERF_COMPARE_NUM_RUNS:-3}"
WORKERS="${PERF_COMPARE_WORKERS:-1}"
WORKTREE="${TARGET_WORKTREE:-${ROOT_DIR}/.perf-compare-work}"

# Validate SHAs exist locally before doing anything expensive.
cd "$ROOT_DIR"
git rev-parse --verify "$BASE_SHA" >/dev/null 2>&1 \
    || { echo "error: base sha not found in repo: $BASE_SHA" >&2; exit 2; }
git rev-parse --verify "$HEAD_SHA" >/dev/null 2>&1 \
    || { echo "error: head sha not found in repo: $HEAD_SHA" >&2; exit 2; }

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'
log() { printf '%s[perf-compare]%s %s\n' "${BLUE}" "${NC}" "$*"; }

# ----------------------------------------------------------------------
# Parse the config: one `<prog>=<dataset>` per line, comments / blanks
# skipped. Also drops trailing `[tag]` markers (as compare.sh does) so
# the same config files can be reused.
# ----------------------------------------------------------------------
PAIRS=()
while IFS= read -r raw; do
    line="${raw%%#*}"
    line="$(echo "$line" | xargs)"
    [[ -z "$line" ]] && continue
    while [[ "$line" =~ ^(.*[^[:space:]])[[:space:]]+(\[[^][]+\])[[:space:]]*$ ]]; do
        line="${BASH_REMATCH[1]}"
    done
    [[ "$line" == *=* ]] || continue
    PAIRS+=("$line")
done < "$CONFIG_FILE"

(( ${#PAIRS[@]} > 0 )) || { echo "error: config has no pairs: $CONFIG_FILE" >&2; exit 2; }

log "config             : $CONFIG_FILE  (${#PAIRS[@]} pairs)"
log "base sha           : $BASE_SHA"
log "head sha           : $HEAD_SHA"
log "tolerances         : time +${TIME_PCT}%, peak RSS +${RSS_PCT}%"
log "bench knobs        : NUM_RUNS=$NUM_RUNS, WORKERS=$WORKERS"
log "worktree           : $WORKTREE"

# ----------------------------------------------------------------------
# Worktree setup. The point is to NOT clobber the active checkout —
# bench_one.sh builds an in-tree library runner crate at
# <root>/target/bench-lib/, and we want the active workspace untouched.
# ----------------------------------------------------------------------
if [[ -e "$WORKTREE" ]]; then
    log "removing stale worktree at $WORKTREE"
    git worktree remove --force "$WORKTREE" >/dev/null 2>&1 || rm -rf "$WORKTREE"
fi
trap 'git worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE" || true' EXIT

# ----------------------------------------------------------------------
# Bench one (program, dataset) pair at the currently-checked-out worktree.
# Returns "<sec> <kb>" — both medians, or empty strings on failure.
# ----------------------------------------------------------------------
bench_one_pair() {
    local prog="$1" ds="$2"
    local out
    if ! out=$(WORKERS="$WORKERS" NUM_RUNS="$NUM_RUNS" QUIET=1 \
               bash "${WORKTREE}/tools/benchmark/bench_one.sh" "$prog" "$ds" 2>/dev/null); then
        echo "  "  # space-separated empty, signals failure
        return 1
    fi
    local sec kb
    sec=$(echo "$out" | awk '$1 == "elapsed_seconds" { print $2; exit }')
    kb=$( echo "$out" | awk '$1 == "peak_rss_kb"     { print $2; exit }')
    printf '%s %s' "${sec:-}" "${kb:-}"
}

# ----------------------------------------------------------------------
# Run every pair under a given sha; emit "<prog>=<ds> <sec> <kb>" lines.
# ----------------------------------------------------------------------
run_at_sha() {
    local sha="$1" label="$2"
    log "checking out $label ($sha) into $WORKTREE"
    git worktree add --force --detach "$WORKTREE" "$sha" >/dev/null 2>&1 \
        || { echo "error: git worktree add failed for $sha" >&2; exit 2; }
    log "warm cargo build (release)"
    (cd "$WORKTREE" && cargo build --release --quiet) \
        || { echo "error: cargo build failed at $sha" >&2; exit 2; }
    for pair in "${PAIRS[@]}"; do
        local prog="${pair%%=*}" ds="${pair#*=}"
        local sec_kb
        if sec_kb=$(bench_one_pair "$prog" "$ds"); then
            printf '%s %s\n' "$pair" "$sec_kb"
        else
            printf '%s   \n' "$pair"
            log "  [WARN] $pair benchmark failed at $label"
        fi
    done
    git worktree remove --force "$WORKTREE" >/dev/null 2>&1 || rm -rf "$WORKTREE"
}

BASE_OUT=$(run_at_sha "$BASE_SHA" "base")
HEAD_OUT=$(run_at_sha "$HEAD_SHA" "head")

# ----------------------------------------------------------------------
# Side-by-side compare. For each pair, fail iff EITHER median wall time
# OR median peak RSS exceeds its tolerance.
# ----------------------------------------------------------------------
log "results"
echo "----------------------------------------------------------------"
printf "%-44s  %12s  %12s  %s\n" "pair" "time" "peak RSS" "verdict"
printf "%-44s  %12s  %12s  %s\n" "----" "----" "--------" "-------"

n_pairs=${#PAIRS[@]}
n_ok=0
n_fail=0

# Index BASE_OUT for fast lookup.
declare -A BASE_SEC BASE_KB
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pair=$(echo "$line" | awk '{print $1}')
    BASE_SEC[$pair]=$(echo "$line" | awk '{print $2}')
    BASE_KB[$pair]=$( echo "$line" | awk '{print $3}')
done <<< "$BASE_OUT"

while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pair=$(echo "$line" | awk '{print $1}')
    h_sec=$(echo "$line" | awk '{print $2}')
    h_kb=$( echo "$line" | awk '{print $3}')
    b_sec="${BASE_SEC[$pair]:-}"
    b_kb="${BASE_KB[$pair]:-}"

    if [[ -z "$b_sec" || -z "$h_sec" || -z "$b_kb" || -z "$h_kb" ]]; then
        printf "%-44s  %12s  %12s  ${YELLOW}MEASURE_FAIL${NC}\n" \
            "$pair" "${h_sec:-N/A}" "${h_kb:-N/A}"
        n_fail=$((n_fail + 1))
        continue
    fi

    # Compute % deltas in python — avoids bash's lack of float math.
    read -r time_pct rss_pct verdict <<< "$(python3 - "$b_sec" "$h_sec" "$b_kb" "$h_kb" "$TIME_PCT" "$RSS_PCT" <<'PY'
import sys
b_sec, h_sec, b_kb, h_kb = (float(x) for x in sys.argv[1:5])
tol_t, tol_r = (float(x) for x in sys.argv[5:7])
time_pct = (h_sec - b_sec) / b_sec * 100.0 if b_sec > 0 else 0.0
rss_pct  = (h_kb  - b_kb ) / b_kb  * 100.0 if b_kb  > 0 else 0.0
verdict = "OK" if (time_pct <= tol_t and rss_pct <= tol_r) else "FAIL"
print(f"{time_pct:+.2f} {rss_pct:+.2f} {verdict}")
PY
)"

    color="$GREEN"; [[ "$verdict" == "FAIL" ]] && color="$RED"
    printf "%-44s  %12s  %12s  %s%s%s\n" \
        "$pair" "${time_pct}%" "${rss_pct}%" "$color" "$verdict" "$NC"
    [[ "$verdict" == "OK" ]] && n_ok=$((n_ok + 1)) || n_fail=$((n_fail + 1))
done <<< "$HEAD_OUT"

echo "----------------------------------------------------------------"
if (( n_fail == 0 )); then
    log "${GREEN}ALL OK${NC} — $n_ok/$n_pairs pairs within tolerances"
    exit 0
fi
log "${RED}REGRESSION${NC} — $n_fail/$n_pairs pair(s) exceeded a tolerance"
exit 1
