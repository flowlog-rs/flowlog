#!/usr/bin/env bash
set -uo pipefail

# Where memory goes outside traces and batchers (REPORT.md section 10.5).
# One run of each probe build at 8 workers prints its peak trace records,
# peak batcher records and peak rows held inside lookups (stashed plus
# ready); appends one line per run to $RESULTS/lookup_rows_probe.log.
#
# usage: scripts/lookup_rows_probe.sh

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# == 0)) || usage_error

require_builds cspa_base_p cspa_delta_sym_p tri_base_p tri_lookup_p sg_base_p sg_lookup_p \
  dyck_base_p dyck_lookup_p tri_inc_p tri_inc_delta_p tri_inc_delta_of_p sg_inc_p sg_inc_delta_p \
  dyck_inc_p dyck_inc_delta_p
require_data cspa-postgresql cspa-linux cspa-httpd roadNet-CA G40K-0.001 web-BerkStan G5K-0.001 \
  kernel G40K-split roadNet-split G5K-split kernel-split

# batch LABEL BUILD DATASET: one run of $BIN/BUILD_p on $DATA/DATASET.
batch() {
  local label=$1 build=$2 dataset=$3 out
  out=$(FLOWLOG_PROBE=1 /usr/bin/time -f 'WALL=%e MAXRSS=%M' "$BIN/${build}_p" -w 8 -F "$DATA/$dataset" -D - 2>&1)
  local usage probes
  usage=$(grep -oE 'WALL=[0-9.]+ MAXRSS=[0-9]+' <<<"$out")
  probes=$(grep -E '^\[probe\] peak_(total|lookup)' <<<"$out" | sed 's/^\[probe\] //' | tr '\n' ' ')
  echo "$label | $usage | $probes"
}

# incremental SPLIT BUILD...: one run of each BUILD's probe build.
incremental() {
  local split=$1 build
  shift
  for build in "$@"; do
    FLOWLOG_PROBE=1 PROBE_LINES=2 "$ROOT/scripts/inc_run.sh" "$BIN/${build}_p" 8 "$build@$split" \
      "$DATA/$split" arc=Arc.csv 2>&1 | tr '\n' ' '
    echo
  done
}

{
  echo "### batch $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
  for dataset in cspa-postgresql cspa-linux cspa-httpd; do
    for v in base delta_sym; do batch "cspa_$v@$dataset" "cspa_$v" "$dataset"; done
  done
  for dataset in roadNet-CA G40K-0.001 web-BerkStan; do
    for v in base lookup; do batch "tri_$v@$dataset" "tri_$v" "$dataset"; done
  done
  for v in base lookup; do batch "sg_$v@G5K" "sg_$v" G5K-0.001; done
  for v in base lookup; do batch "dyck_$v@kernel" "dyck_$v" kernel; done

  echo "### inc $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
  incremental G40K-split tri_inc tri_inc_delta tri_inc_delta_of
  incremental roadNet-split tri_inc tri_inc_delta tri_inc_delta_of
  incremental G5K-split sg_inc sg_inc_delta
  incremental kernel-split dyck_inc dyck_inc_delta
  echo "### done $(date +%T)"
} | tee -a "$RESULTS/lookup_rows_probe.log"
