#!/usr/bin/env bash
set -euo pipefail

# One incremental run: load SPLIT/pre, insert SPLIT/upd/FILE into each REL in
# one commit, and quit. Prints one line,
#
#   LABEL w=WORKERS | preload=TIME: commit=TIME: WALL=SECONDS MAXRSS=KB | [Rel]@t=T=SIZE ...
#
# with the binary's own preload and commit times, GNU time's wall time and
# peak RSS, and each output relation's size at each epoch (heuristics.py
# parses this line). PROBE_LINES=N then prints the first N `[probe]` lines of
# a probe build; run one with FLOWLOG_PROBE=1 to count rows held in lookups.
#
# usage: scripts/inc_run.sh BINARY WORKERS LABEL SPLIT REL=FILE...
# e.g.:  scripts/inc_run.sh "$BIN/tri_inc_delta" 8 tri_inc_delta@G40K-split "$DATA/G40K-split" arc=Arc.csv

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# >= 5)) || usage_error
binary=$1 workers=$2 label=$3 split=$4
shift 4

commands=$'begin\n'
for insert in "$@"; do
  commands+="insert ${insert%%=*} @$split/upd/${insert#*=}"$'\n'
done
commands+=$'commit\nquit\n'

status=0
out=$(printf '%s' "$commands" |
  /usr/bin/time -f 'WALL=%e MAXRSS=%M' "$binary" -w "$workers" -F "$split/pre" -D - 2>&1) || status=$?

preload=$(awk '/Dataflow assembled/ { print "preload=" $1; exit }' <<<"$out")
commit=$(awk '/Committed/ { print "commit=" $1; exit }' <<<"$out")
usage=$(grep -oE 'WALL=[0-9.]+ MAXRSS=[0-9]+' <<<"$out" || true)
# `[size][Rel]  t=1  size=195` becomes `[Rel]@t=1=195`.
sizes=$(awk '/^\[size\]/ { sub(/^\[size\]/, "", $1); sub(/^size=/, "", $3); printf "%s@%s=%s ", $1, $2, $3 }' <<<"$out")
echo "$label w=$workers | $preload $commit $usage | $sizes"
awk -v limit="${PROBE_LINES:-0}" \
  '/^\[probe\] (peak_total|peak_lookup|op=)/ && shown < limit { print; shown++ }' <<<"$out"
if ((status != 0)); then
  echo "inc_run.sh: $binary exited with status $status" >&2
fi
exit "$status"
