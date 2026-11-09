#!/usr/bin/env bash
set -uo pipefail

# The held-out batch (REPORT.md section 11): workloads the policies were not
# tuned on, measured after the policies' picks were recorded. Every plan at 8
# workers, with the plans of each program interleaved in each round:
# FAST_REPS rounds (default 5) of the short graph and CSDA runs, then REPS
# rounds (default 3) of Polonius and Doop. PROGRAMs, any of reach, cc, sssp,
# bipartite, csda, polonius and doop, limit the runs to those programs
# (section 12 re-measured doop alone). Appends to $RESULTS/heldout_runs.csv
# (scripts/bench.py). A failed run is logged and the rest go on. Score them
# with `scripts/heuristics.py --results "$RESULTS"`.
#
# usage: [REPS=N] [FAST_REPS=N] scripts/heldout_bench.sh [PROGRAM]...

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

REPS=${REPS:-3}
FAST_REPS=${FAST_REPS:-5}

PROGRAMS=("$@")
((${#PROGRAMS[@]} > 0)) || PROGRAMS=(reach cc sssp bipartite csda polonius doop)
builds=()
data=()
for program in "${PROGRAMS[@]}"; do
  case $program in
    reach | cc) builds+=("${program}_base" "${program}_lookup"); data+=(livejournal) ;;
    sssp) builds+=(sssp_base sssp_lookup); data+=(livejournal-sssp) ;;
    bipartite) builds+=(bipartite_base bipartite_lookup); data+=(mind) ;;
    csda) builds+=(csda_base csda_lookup); data+=(csda-httpd) ;;
    polonius) builds+=(polonius_base polonius_lsa polonius_lsb); data+=(borrow) ;;
    doop) builds+=(doop_base doop_dac doop_dacv doop_difpt); data+=(batik) ;;
    *) usage_error ;;
  esac
done
require_builds "${builds[@]}"
require_data "${data[@]}"

# on PROGRAM...: whether this invocation runs any of the PROGRAMs.
on() {
  local program
  for program in "$@"; do
    [[ " ${PROGRAMS[*]} " == *" $program "* ]] && return 0
  done
  return 1
}

# batch LABEL BUILD DATASET [--expect REL=SIZE]...: one run, one CSV row.
batch() {
  local label=$1 build=$2 dataset=$3
  shift 3
  "$ROOT/scripts/bench.py" --reps 1 --csv "$RESULTS/heldout_runs.csv" --label "$label" \
    --bin "$BIN/$build" --facts "$DATA/$dataset" "$@" 2>&1 | grep -iE 'rep=|mismatch|fail|error'
}

POLONIUS=(--expect subset=5068815 --expect origin_contains_loan_on_entry=1316
  --expect move_error=291546023 --expect path_maybe_uninitialized_on_exit=291552229)
DOOP=(--expect VarPointsTo=37237353 --expect InstanceFieldPointsTo=13996740
  --expect Reachable=16620 --expect CallGraphEdge=84434 --expect StaticFieldPointsTo=61218
  --expect ArrayIndexPointsTo=326574 --expect InitializedClass=3434)

if on reach cc sssp bipartite csda; then
  for rep in $(seq "$FAST_REPS"); do
    echo "### fast rep $rep $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
    for v in base lookup; do
      on reach && batch "reach_$v@livejournal" "reach_$v" livejournal --expect Reach=4400347
      on cc && batch "cc_$v@livejournal" "cc_$v" livejournal --expect CC=4847571
      on sssp && batch "sssp_$v@livejournal" "sssp_$v" livejournal-sssp --expect sssp=4400347
      on bipartite && batch "bipartite_$v@mind" "bipartite_$v" mind \
        --expect One=97506 --expect Zero=876953 --expect BipartiteViolation=0
      on csda && batch "csda_$v@httpd" "csda_$v" csda-httpd --expect NullNode=9393283
    done
  done
fi

if on polonius doop; then
  for rep in $(seq "$REPS"); do
    echo "### rep $rep $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
    if on polonius; then
      for v in base lsa lsb; do
        batch "polonius_$v@borrow" "polonius_$v" borrow "${POLONIUS[@]}"
      done
    fi
    if on doop; then
      for v in base dac dacv difpt; do
        batch "doop_$v@batik" "doop_$v" batik "${DOOP[@]}"
      done
    fi
  done
fi
echo "### done $(date +%T)"
