#!/usr/bin/env bash
set -uo pipefail

# The load filters (REPORT.md section 10.4) on the incremental delta joins.
# WHICH picks the workloads (default "tri dyck"):
#
#   tri   triangles on G40K-split and roadNet-split: the binary plan, the
#         delta join as built, with Old first, and with Materialize's snapshot
#         filter (paths 1 and 2 drop their epoch-0 rows before any lookup).
#   dyck  Dyck on kernel-split: the binary plan, the delta join, with the
#         snapshot filter (path 1 drops its rows at the loop's first time
#         (0,0) before its first hop, which is Old), and with the loop filter
#         (paths 1 and 3, driven by the static atoms, both drop them: Dyck, a
#         loop variable, is empty at (0,0)).
#
# REPS rounds (default 3) with the builds interleaved in each append to
# $RESULTS/snapfilter_inc.log, in heur_inc.log's format. Then one run of each
# delta join's probe build appends its peak rows held in lookups to
# $RESULTS/snapfilter_probe.log.
#
# usage: [REPS=N] [WHICH="tri dyck"] scripts/snapfilter_bench.sh

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# == 0)) || usage_error

WHICH=${WHICH:-"tri dyck"}
TRI=(tri_inc tri_inc_delta tri_inc_delta_of tri_inc_delta_sf)
DYCK=(dyck_inc dyck_inc_delta dyck_inc_delta_sf dyck_inc_delta_lf)
TRI_PROBES=(tri_inc_delta_of tri_inc_delta_sf)
DYCK_PROBES=(dyck_inc_delta dyck_inc_delta_sf dyck_inc_delta_lf)

for workload in $WHICH; do
  case $workload in
    tri)
      require_builds "${TRI[@]}" "${TRI_PROBES[@]/%/_p}"
      require_data G40K-split roadNet-split
      ;;
    dyck)
      require_builds "${DYCK[@]}" "${DYCK_PROBES[@]/%/_p}"
      require_data kernel-split
      ;;
    *)
      echo "snapfilter_bench.sh: unknown workload $workload; expected tri or dyck" >&2
      exit 2
      ;;
  esac
done

# runs SPLIT BUILD...: one run of each BUILD, one line each.
runs() {
  local split=$1 build
  shift
  for build in "$@"; do
    "$ROOT/scripts/inc_run.sh" "$BIN/$build" 8 "$build@$split" "$DATA/$split" arc=Arc.csv 2>&1 | tail -1
  done
}

# probes SPLIT BUILD...: one run of each BUILD's probe build, one line each.
probes() {
  local split=$1 build
  shift
  for build in "$@"; do
    FLOWLOG_PROBE=1 PROBE_LINES=2 "$ROOT/scripts/inc_run.sh" "$BIN/${build}_p" 8 "$build@$split" \
      "$DATA/$split" arc=Arc.csv 2>&1 | tr '\n' ' '
    echo
  done
}

echo "### $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg) which=$WHICH" >>"$RESULTS/snapfilter_inc.log"
for _ in $(seq "${REPS:-3}"); do
  for workload in $WHICH; do
    case $workload in
      tri) for split in G40K-split roadNet-split; do runs "$split" "${TRI[@]}"; done ;;
      dyck) runs kernel-split "${DYCK[@]}" ;;
    esac
  done
done | tee -a "$RESULTS/snapfilter_inc.log"
echo "### $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)" >>"$RESULTS/snapfilter_inc.log"

for workload in $WHICH; do
  case $workload in
    tri) for split in G40K-split roadNet-split; do probes "$split" "${TRI_PROBES[@]}"; done ;;
    dyck) probes kernel-split "${DYCK_PROBES[@]}" ;;
  esac
done | tee -a "$RESULTS/snapfilter_probe.log"
