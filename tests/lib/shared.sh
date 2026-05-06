#!/usr/bin/env bash
#
# Shared helpers used by every FlowLog test script (fixtures + oracle,
# binary + lib). Pure presentation + tiny utilities — no test semantics.
#
# Sourced by tests/fixtures/common.sh, tests/oracle/common.sh, and
# tests/lib/runner_synth.sh. Use the include guard below instead of
# sourcing this file unconditionally in case of transitive sourcing.

[[ -n "${FLOWLOG_SHARED_SH_LOADED:-}" ]] && return 0
FLOWLOG_SHARED_SH_LOADED=1

###############################################################################
# Colors
###############################################################################

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly DIM='\033[2m'
readonly NC='\033[0m'
readonly CLEAR_LINE='\033[2K'

###############################################################################
# ROOT_DIR — repo root, resolved relative to this file.
# tests/lib/shared.sh → tests/lib → tests → <repo root>, hence "/../.."
###############################################################################

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)" \
    || { echo "error: failed to resolve ROOT_DIR" >&2; exit 1; }
readonly ROOT_DIR

###############################################################################
# Logging / small utilities
###############################################################################

log() {
    local color="$1"
    local tag="$2"
    shift 2
    echo -e "${color}[${tag}]${NC} $*"
}

die() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
    exit 1
}

trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

