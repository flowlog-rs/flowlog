#!/usr/bin/env bash
#
# tools/perf_compare.sh — perf + peak-RSS drift check between two SHAs.
#
# Designed for closed-loop tooling (e.g. groomer's
# `[finalisation].perf_compare_cmd`) that wants to verify a series of
# in-tree changes hasn't regressed any benchmark beyond a tolerance,
# without invoking the heavy `compare.sh` cross-engine machinery
# (no Soufflé, no legacy interpreter).
#
# What it does:
#   1. Read a list of `<prog>=<dataset>` pairs from a config file
#      (same format as `tools/benchmark/config.txt`; per-pair `[tag]`
#      markers are stripped so the same files can be reused).
#   2. For each pair, run `tools/benchmark/bench_one.sh <prog> <dataset>`
#      twice — once at <base_sha>, once at <head_sha> — capturing the
#      median elapsed_seconds + median peak_rss_kb from bench_one's
#      stable stdout contract.
#   3. Compare each metric to its baseline; flag PASS / FAIL.
#   4. Exit 0 iff every pair stayed within both tolerances; otherwise 1.
#
# Worktree strategy: HEAD runs in-place (the active checkout), so
# cargo's incremental cache is reused. BASE is materialised into
# `target/perf-compare-base/<short_sha>/` (a sibling git worktree
# that survives across runs — repeat invocations at the same BASE
# reuse the cached release build, cutting wall time roughly in half
# vs. a fresh worktree per run). `target/` is gitignored.
#
# Output:
#   - One stdout line per pair on success: `<pair>  time%  rss%  OK`
#   - The summary table on stdout when every pair is OK; on stderr
#     when any pair failed, so wrapper scripts can keep extractors
#     pointed at stdout for clean signals.
#
# Exit code:
#   0  every pair within tolerances
#   1  at least one pair regressed beyond a tolerance
#   2  argument / I/O error (config missing, sha unknown, HEAD mismatch)
#   3  internal error (cargo build, bench_one, worktree add)
#
# Usage:
#   tools/perf_compare.sh <base_sha> <head_sha> <config_file>
#   tools/perf_compare.sh -h | --help
#
# Environment:
#   PERF_COMPARE_TIME_PCT     wall-time regression tolerance  (default 10)
#   PERF_COMPARE_RSS_PCT      peak-RSS regression tolerance   (default 20)
#   PERF_COMPARE_NUM_RUNS     bench_one.sh NUM_RUNS           (default 3)
#   PERF_COMPARE_WORKERS      bench_one.sh WORKERS            (default 1
#                             — single-thread is the most stable signal
#                             for small-bench measurements)
#
# This script is the canonical hook for groomer's
# `[finalisation].perf_compare_cmd` and the implementation behind
# `make perf-compare`.

set -euo pipefail

# ----------------------------------------------------------------------
# --help: extract the doc header above (everything up to the first
# bash command after `set -euo pipefail`).
# ----------------------------------------------------------------------
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    awk '/^set -euo pipefail/ { exit }
         NR > 1 { sub(/^# ?/, ""); print }' "$0"
    exit 0
fi

# ----------------------------------------------------------------------
# Argument parsing.
# ----------------------------------------------------------------------
if [[ $# -ne 3 ]]; then
    echo "usage: $0 <base_sha> <head_sha> <config_file>" >&2
    echo "       $0 --help" >&2
    exit 2
fi
BASE_SHA="$1"
HEAD_SHA="$2"
CONFIG_FILE="$3"

[[ -f "$CONFIG_FILE" ]] || { echo "ERROR: config file not found: $CONFIG_FILE" >&2; exit 2; }

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TIME_PCT="${PERF_COMPARE_TIME_PCT:-10}"
RSS_PCT="${PERF_COMPARE_RSS_PCT:-20}"
NUM_RUNS="${PERF_COMPARE_NUM_RUNS:-3}"
WORKERS="${PERF_COMPARE_WORKERS:-1}"

for var in TIME_PCT RSS_PCT NUM_RUNS WORKERS; do
    val="${!var}"
    [[ "$val" =~ ^[0-9]+$ ]] \
        || { echo "ERROR: PERF_COMPARE_$var must be a non-negative integer (got: $val)" >&2; exit 2; }
done

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'
log() { printf '%s[perf-compare]%s %s\n' "${BLUE}" "${NC}" "$*" >&2; }

# ----------------------------------------------------------------------
# Resolve shas to full 40-char form so the cached worktree path is stable.
# Assert HEAD checkout matches HEAD_SHA — perf-compare runs HEAD in-place,
# so the caller is responsible for being at the right sha.
# ----------------------------------------------------------------------
BASE_FULL="$(git rev-parse --verify "$BASE_SHA^{commit}" 2>/dev/null)" \
    || { echo "ERROR: base sha unknown: $BASE_SHA" >&2; exit 2; }
HEAD_FULL="$(git rev-parse --verify "$HEAD_SHA^{commit}" 2>/dev/null)" \
    || { echo "ERROR: head sha unknown: $HEAD_SHA" >&2; exit 2; }
HEAD_NOW="$(git rev-parse --verify HEAD)"

if [[ "$HEAD_FULL" != "$HEAD_NOW" ]]; then
    echo "ERROR: HEAD checkout (${HEAD_NOW:0:12}) does not match requested HEAD (${HEAD_FULL:0:12})" >&2
    echo "       perf_compare runs HEAD in-place; check out the head sha first." >&2
    exit 2
fi

# ----------------------------------------------------------------------
# Parse the config: one `<prog>=<dataset>` per line, blanks/comments
# skipped, trailing `[tag]` markers (a compare.sh feature) stripped so
# the same files can be reused across both tools.
# ----------------------------------------------------------------------
PAIRS=()
while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw%%#*}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "$line" ]] && continue
    while [[ "$line" =~ ^(.*[^[:space:]])[[:space:]]+(\[[^][]+\])[[:space:]]*$ ]]; do
        line="${BASH_REMATCH[1]}"
    done
    [[ "$line" == *=* ]] || { echo "ERROR: malformed pair (expected '<prog>=<dataset>'): $raw" >&2; exit 2; }
    PAIRS+=("$line")
done < "$CONFIG_FILE"

(( ${#PAIRS[@]} > 0 )) || { echo "ERROR: config has no pairs: $CONFIG_FILE" >&2; exit 2; }

# ----------------------------------------------------------------------
# Materialise BASE into target/perf-compare-base/<short_sha>/.
# Persistent across runs so iterative loops (bench against the same
# BASE many times) reuse the cached release build.
# ----------------------------------------------------------------------
BASE_SHORT="${BASE_FULL:0:12}"
BASE_TREE="${ROOT_DIR}/target/perf-compare-base/${BASE_SHORT}"
mkdir -p "$(dirname "$BASE_TREE")"

if [[ -d "$BASE_TREE/.git" || -f "$BASE_TREE/.git" ]]; then
    log "reusing cached base worktree: $BASE_TREE"
else
    log "creating base worktree at $BASE_TREE (sha=${BASE_SHORT})"
    git worktree add --detach --force "$BASE_TREE" "$BASE_FULL" >&2 \
        || { echo "ERROR: git worktree add failed" >&2; exit 3; }
fi

# Defensive re-pin in case the worktree drifted from a previous run.
( cd "$BASE_TREE" && git checkout --quiet --detach "$BASE_FULL" ) \
    || { echo "ERROR: failed to pin base worktree to $BASE_SHORT" >&2; exit 3; }

# `facts/` in the active checkout is typically a symlink to the
# datasets root (e.g. /datasets/facts). Worktrees don't inherit
# untracked files / unstaged symlinks, so reproduce it on demand.
# Use `cd … && pwd -P` to resolve the symlink portably (GNU + BSD/macOS,
# unlike `readlink -f` which is GNU-only).
if [[ ! -e "$BASE_TREE/facts" && -e "$ROOT_DIR/facts" ]]; then
    if [[ -L "$ROOT_DIR/facts" ]]; then
        ln -s "$(cd "$ROOT_DIR/facts" && pwd -P)" "$BASE_TREE/facts"
    else
        ln -s "$ROOT_DIR/facts" "$BASE_TREE/facts"
    fi
    log "linked facts/ into base worktree"
fi

log "config             : $CONFIG_FILE  (${#PAIRS[@]} pair(s))"
log "base sha           : $BASE_FULL"
log "head sha           : $HEAD_FULL  (in-place)"
log "tolerances         : time +${TIME_PCT}%, peak RSS +${RSS_PCT}%"
log "bench knobs        : NUM_RUNS=$NUM_RUNS, WORKERS=$WORKERS"

# ----------------------------------------------------------------------
# Bench one pair under a given tree. Returns "<sec> <kb>" on stdout
# (both medians, per bench_one's stable contract); empty on failure.
# ----------------------------------------------------------------------
bench_one_pair() {
    local tree="$1" prog="$2" ds="$3"
    local out
    if ! out=$(WORKERS="$WORKERS" NUM_RUNS="$NUM_RUNS" QUIET=1 \
               bash "${tree}/tools/benchmark/bench_one.sh" "$prog" "$ds" 2>/dev/null); then
        return 1
    fi
    local sec kb
    sec=$(awk '$1 == "elapsed_seconds" { print $2; exit }' <<< "$out")
    kb=$( awk '$1 == "peak_rss_kb"     { print $2; exit }' <<< "$out")
    [[ -n "$sec" ]] || return 1
    printf '%s %s\n' "$sec" "${kb:-N/A}"
}

# ----------------------------------------------------------------------
# Iterate the pair list, collecting per-pair (b_sec, h_sec, b_kb, h_kb).
# ----------------------------------------------------------------------
declare -A B_SEC B_KB H_SEC H_KB

for pair in "${PAIRS[@]}"; do
    prog="${pair%%=*}"
    ds="${pair#*=}"
    log "pair: $pair"

    log "  base@${BASE_SHORT} ..."
    if out=$(bench_one_pair "$BASE_TREE" "$prog" "$ds"); then
        B_SEC[$pair]="${out% *}"; B_KB[$pair]="${out##* }"
    else
        log "  ${YELLOW}WARN${NC}: base bench failed for $pair"
    fi

    log "  head@${HEAD_FULL:0:12} ..."
    if out=$(bench_one_pair "$ROOT_DIR" "$prog" "$ds"); then
        H_SEC[$pair]="${out% *}"; H_KB[$pair]="${out##* }"
    else
        log "  ${YELLOW}WARN${NC}: head bench failed for $pair"
    fi
done

# ----------------------------------------------------------------------
# Summary table — verdict per pair. Successes → stdout; failures →
# stderr so extractors keyed on stdout don't pick up regression rows.
# ----------------------------------------------------------------------
ROWS=()
FAILED=0

for pair in "${PAIRS[@]}"; do
    b_sec="${B_SEC[$pair]:-}"; h_sec="${H_SEC[$pair]:-}"
    b_kb="${B_KB[$pair]:-}";   h_kb="${H_KB[$pair]:-}"

    if [[ -z "$b_sec" || -z "$h_sec" ]]; then
        ROWS+=("${pair}|${b_sec:-N/A}|${h_sec:-N/A}|N/A|${b_kb:-N/A}|${h_kb:-N/A}|N/A|MEASURE_FAIL")
        FAILED=1
        continue
    fi

    # % deltas + verdict in python (avoids bashism for floats).
    eval "$(python3 - "$b_sec" "$h_sec" "$b_kb" "$h_kb" "$TIME_PCT" "$RSS_PCT" <<'PY'
import sys
b_sec, h_sec, b_kb, h_kb, tol_t, tol_r = sys.argv[1:]
b_sec_f, h_sec_f = float(b_sec), float(h_sec)
time_pct = (h_sec_f - b_sec_f) / b_sec_f * 100.0 if b_sec_f > 0 else 0.0
fail = 1 if time_pct > float(tol_t) else 0
if b_kb in ("N/A", "") or h_kb in ("N/A", ""):
    rss_pct_str = "NA"
else:
    b_kb_i, h_kb_i = int(b_kb), int(h_kb)
    rss_pct = (h_kb_i - b_kb_i) / b_kb_i * 100.0 if b_kb_i > 0 else 0.0
    rss_pct_str = f"{rss_pct:+.2f}"
    if rss_pct > float(tol_r):
        fail = 1
print(f'TIME_PCT_VAL="{time_pct:+.2f}"')
print(f'RSS_PCT_VAL="{rss_pct_str}"')
print(f'FAIL_FLAG="{fail}"')
PY
)"

    VERDICT="OK"
    if [[ "$FAIL_FLAG" = "1" ]]; then
        VERDICT="FAIL"
        FAILED=1
    fi
    ROWS+=("${pair}|${b_sec}|${h_sec}|${TIME_PCT_VAL}|${b_kb}|${h_kb}|${RSS_PCT_VAL}|${VERDICT}")
done

SINK=1
[[ "$FAILED" = "1" ]] && SINK=2

{
    printf '\n=== perf-compare: base=%s head=%s ===\n' "${BASE_FULL:0:12}" "${HEAD_FULL:0:12}"
    printf '    workers=%s  runs/sha=%s  thresholds: time +%s%%  rss +%s%%\n\n' \
        "$WORKERS" "$NUM_RUNS" "$TIME_PCT" "$RSS_PCT"
    printf '%-46s %12s %12s %9s %12s %12s %9s  %s\n' \
        pair base_time head_time time% base_rss head_rss rss% verdict
    printf '%-46s %12s %12s %9s %12s %12s %9s  %s\n' \
        ---- --------- --------- ----- -------- -------- ---- -------
    for row in "${ROWS[@]}"; do
        IFS='|' read -r pair bs hs tp bk hk rp v <<< "$row"
        color="$GREEN"; [[ "$v" != "OK" ]] && color="$RED"
        printf '%-46s %12s %12s %9s %12s %12s %9s  %s%s%s\n' \
            "$pair" "$bs" "$hs" "$tp" "$bk" "$hk" "$rp" "$color" "$v" "$NC"
    done
    printf '\n'
    if (( FAILED )); then
        printf '%sREGRESSION%s — at least one pair exceeded a tolerance\n' "$RED" "$NC"
    else
        printf '%sALL OK%s — every pair within tolerances\n' "$GREEN" "$NC"
    fi
    printf '\n'
} >&$SINK

exit $FAILED
