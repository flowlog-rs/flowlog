#!/usr/bin/env bash
set -uo pipefail

# The policy study (REPORT.md sections 10.1-10.3): every plan of the new
# workloads at 8 workers, REPS rounds (default 3) with the plans interleaved
# in each. Batch runs append to $RESULTS/heur_runs.csv (scripts/bench.py) and
# incremental runs to $RESULTS/heur_inc.log (scripts/inc_run.sh). A failed run
# is logged and the rest go on. Score them with
# `scripts/heuristics.py --results "$RESULTS"`.
#
# usage: [REPS=N] scripts/heur_bench.sh

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# == 0)) || usage_error

REPS=${REPS:-3}

require_builds tri_base tri_lookup sg_base sg_lookup dyck_base dyck_lookup \
  andersen_base andersen_dload andersen_dstore cspa_base cspa_delta_sym cspa_delta \
  polonius_base polonius_lme polonius_lpm polonius_lboth \
  tri_inc tri_inc_delta sg_inc sg_inc_delta dyck_inc dyck_inc_delta
require_data G40K-0.001 roadNet-CA web-BerkStan G5K-0.001 kernel medium \
  cspa-postgresql cspa-linux borrow G40K-split roadNet-split G5K-split kernel-split

# batch LABEL BUILD DATASET [--expect REL=SIZE]...: one run, one CSV row.
batch() {
  local label=$1 build=$2 dataset=$3
  shift 3
  "$ROOT/scripts/bench.py" --reps 1 --csv "$RESULTS/heur_runs.csv" --label "$label" \
    --bin "$BIN/$build" --facts "$DATA/$dataset" "$@" 2>&1 | grep -iE 'rep=|mismatch|fail|error'
}

# incremental SPLIT BUILD...: one run of each BUILD, one line each.
incremental() {
  local split=$1 build
  shift
  for build in "$@"; do
    "$ROOT/scripts/inc_run.sh" "$BIN/$build" 8 "$build@$split" "$DATA/$split" arc=Arc.csv 2>&1 | tail -1
  done
}

for rep in $(seq "$REPS"); do
  echo "### batch rep $rep $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
  for v in base lookup; do
    batch "tri_$v@G40K" "tri_$v" G40K-0.001 --expect Tri=63984
    batch "tri_$v@roadNet" "tri_$v" roadNet-CA --expect Tri=724056
    batch "tri_$v@BerkStan" "tri_$v" web-BerkStan --expect Tri=131368237
    batch "sg_$v@G5K" "sg_$v" G5K-0.001 --expect Sg=24730729
    batch "dyck_$v@kernel" "dyck_$v" kernel --expect Dyck=4458939
  done
  for v in base dload dstore; do
    batch "andersen_$v@medium" "andersen_$v" medium --expect PointsTo=2654871
  done
  for v in base delta_sym delta; do
    batch "cspa_$v@postgresql" "cspa_$v" cspa-postgresql \
      --expect MemoryAlias=89475479 --expect ValueAlias=223793861 --expect ValueFlow=3712452
    batch "cspa_$v@linux" "cspa_$v" cspa-linux \
      --expect MemoryAlias=13777625 --expect ValueAlias=30938106 --expect ValueFlow=5509641
  done
  for v in base lme lpm lboth; do
    batch "polonius_$v@borrow" "polonius_$v" borrow \
      --expect move_error=291546023 --expect path_maybe_uninitialized_on_exit=291552229
  done
done

for rep in $(seq "$REPS"); do
  echo "### inc rep $rep $(date +%T)"
  incremental G40K-split tri_inc tri_inc_delta
  incremental roadNet-split tri_inc tri_inc_delta
  incremental G5K-split sg_inc sg_inc_delta
  incremental kernel-split dyck_inc dyck_inc_delta
done | tee -a "$RESULTS/heur_inc.log"
echo "### done $(date +%T)"
