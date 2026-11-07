# shellcheck shell=bash
# Paths and checks shared by the study's shell scripts; source this file.
# paths.py sets the same paths for the Python scripts. An environment
# variable of the same name overrides each one.
#
#   ROOT     research/delta-joins
#   REPO     the FlowLog checkout that holds it
#   WORK     scratch space: generated crates, cargo output, binaries, data
#   BIN      built binaries (scripts/build.py)
#   DATA     datasets (scripts/fetch_data.sh)
#   RESULTS  logs that new runs write; results/ keeps the study's own runs
#
# Every script that sources this file answers -h or --help with its header
# comment (a sourced file sees its caller's arguments).

if [[ ${1:-} == -h || ${1:-} == --help ]]; then
  awk 'NR > 1 && /^#/ { on = 1; sub(/^# ?/, ""); print; next } on { exit }' "$0"
  exit 0
fi

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO=$(cd "$ROOT/../.." && pwd)
WORK=${WORK:-$REPO/target/delta-joins}
BIN=${BIN:-$WORK/bin}
DATA=${DATA:-$WORK/data}
RESULTS=${RESULTS:-$WORK/results}
export ROOT REPO WORK BIN DATA RESULTS
mkdir -p "$RESULTS"

# usage_error: prints the calling script's usage line and exits 2.
usage_error() {
  sed -n 's/^# usage: /usage: /p' "$0" >&2
  exit 2
}

# require_builds NAME...: exits, naming the command to run, unless every
# NAME is built.
require_builds() {
  local name missing=()
  for name in "$@"; do
    [[ -x $BIN/$name ]] || missing+=("$name")
  done
  if ((${#missing[@]} > 0)); then
    echo "not built yet; run: scripts/build.py ${missing[*]}" >&2
    exit 1
  fi
}

# require_data NAME...: exits, naming the command to run, unless every
# dataset or split NAME is in $DATA.
require_data() {
  local name missing=()
  for name in "$@"; do
    [[ -d $DATA/$name ]] || missing+=("$name")
  done
  if ((${#missing[@]} > 0)); then
    echo "not fetched yet; run: scripts/fetch_data.sh ${missing[*]}" >&2
    exit 1
  fi
}
