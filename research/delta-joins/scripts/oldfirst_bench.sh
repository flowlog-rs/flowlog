#!/usr/bin/env bash
set -uo pipefail

# Old first (REPORT.md section 10.4): the incremental triangles' binary plan,
# the delta join as built, and the delta join with each path's Old hops first.
# REPS rounds (default 3) with the builds interleaved in each; appends to
# $RESULTS/oldfirst_inc.log in heur_inc.log's format.
#
# usage: [REPS=N] scripts/oldfirst_bench.sh

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# == 0)) || usage_error

require_builds tri_inc tri_inc_delta tri_inc_delta_of
require_data G40K-split roadNet-split

for _ in $(seq "${REPS:-3}"); do
  for split in G40K-split roadNet-split; do
    for build in tri_inc tri_inc_delta tri_inc_delta_of; do
      "$ROOT/scripts/inc_run.sh" "$BIN/$build" 8 "$build@$split" "$DATA/$split" arc=Arc.csv 2>&1 | tail -1
    done
  done
done | tee -a "$RESULTS/oldfirst_inc.log"
