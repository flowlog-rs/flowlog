#!/usr/bin/env bash
set -uo pipefail

# The CTADL timings (CTADL.md): REPS rounds (default 3), each running every
# CONFIG of each WORKLOAD's index phase once, at 32 workers, in an order
# rotated by the round. A CONFIG names the VirtualAssign rules switched to a
# delta join and their order (patches/ctadl_va_delta.py): base, ORDER-all or
# ORDER-rA+rB+..., with ORDER text or cost. Each run appends a row labelled
# t-WORKLOAD-CONFIG to $RESULTS/ctadl_runs.csv (scripts/ctadl_run.py). A
# failed run is logged and the rest go on. DRY=1 prints the runs instead.
# Score them with `scripts/ctadl_heuristics.py --results "$RESULTS"`.
#
# usage: [REPS=N] [DRY=1] scripts/ctadl_bench.sh WORKLOAD=CONFIG[,CONFIG]...

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

(($# > 0)) || usage_error

REPS=${REPS:-3}

# build WORKLOAD: the delta build of WORKLOAD's front end.
build() {
  case $(<"$DATA/ctadl/$1/language") in
    jadx) echo ctadl_ji_dj ;;
    pcode) echo ctadl_pi_dj ;;
  esac
}

builds=()
for spec in "$@"; do
  [[ $spec == ?*=?* ]] || usage_error
  workload=${spec%%=*}
  if [[ ! -f $DATA/ctadl/$workload/language ]]; then
    echo "not staged yet; run: scripts/ctadl_stage.py $workload FACTS" >&2
    exit 1
  fi
  builds+=("$(build "$workload")")
done
[[ -n ${DRY:-} ]] || require_builds "${builds[@]}"

for rep in $(seq "$REPS"); do
  echo "### rep $rep $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
  for spec in "$@"; do
    workload=${spec%%=*}
    IFS=, read -ra configs <<<"${spec#*=}"
    for i in "${!configs[@]}"; do
      config=${configs[(i + rep) % ${#configs[@]}]}
      env=()
      if [[ $config != base ]]; then
        rules=${config#*-}
        env=(--env "DJ_VA=${rules//+/,}" --env "DJ_VA_ORDER=${config%%-*}")
      fi
      ${DRY:+echo} "$ROOT/scripts/ctadl_run.py" --label "t-$workload-$config" --bin "$BIN/$(build "$workload")" \
        --phase "$workload/index" "${env[@]}"
    done
  done
done
echo "### done $(date +%T)"
