#!/usr/bin/env bash
set -uo pipefail

# The lookup review (REPORT.md section 12): one lookup step timed in
# FlowLog's lookup, in differential-dogs3's half join and in a binary join
# (the bench/ crate), on integer keys, on string keys and spread over
# epochs, at 1 and 8 workers. Builds the bench from this checkout, then runs
# REPS rounds (default 5), each running every engine on every scenario once.
# Each LABEL=BINARY adds another build of the bench, such as one built on
# another branch, whose `flowlog` engine runs beside the rest;
# LABEL=BINARY:ENGINE runs that build's ENGINE instead, such as `hj2` in a
# build against a patched dogs3. Appends a line per run to
# $RESULTS/lookup_bench.log, then prints the median answer time and peak
# memory of each.
#
# usage: [REPS=N] scripts/lookup_bench.sh [LABEL=BINARY[:ENGINE]]...

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

REPS=${REPS:-5}
LOG=$RESULTS/lookup_bench.log

engines=(flowlog flowlog-noland hj2 join)
labels=()
declare -A binary=() engine_of=()
for argument in "$@"; do
  [[ $argument == ?*=?* ]] || usage_error
  label=${argument%%=*}
  binary[$label]=${argument#*=}
  engine_of[$label]=flowlog
  if [[ ${binary[$label]} == ?*:?* ]]; then
    engine_of[$label]=${binary[$label]##*:}
    binary[$label]=${binary[$label]%:*}
  fi
  [[ " ${engines[*]} " == *" ${engine_of[$label]} "* ]] || {
    echo "unknown engine: ${engine_of[$label]}" >&2
    exit 1
  }
  [[ -x ${binary[$label]} ]] || {
    echo "not executable: ${binary[$label]}" >&2
    exit 1
  }
  labels+=("$label")
done

cargo build --release --quiet --manifest-path "$ROOT/bench/Cargo.toml" \
  --target-dir "$WORK/bench" || exit 1
binary[checkout]=$WORK/bench/release/lookup-bench

# The checksum every engine must reach in each scenario.
declare -A checksum=([int]=e5b3ffec5a62c606 [str]=ca8f7954e8800049 [inc]=37d2d8313c302495)

# run LABEL ENGINE SCENARIO WORKERS: one run, one log line.
run() {
  local line
  if ! line=$("${binary[$1]}" "$2" "$3" "$4"); then
    line="engine=$2 scenario=$3 workers=$4 FAILED"
  elif [[ $line != *"checksum=${checksum[$3]}"* ]]; then
    line="$line MISMATCH"
  fi
  echo "build=$1 $line" | tee -a "$LOG"
}

touch "$LOG"
first=$(($(wc -l <"$LOG") + 1))
for rep in $(seq "$REPS"); do
  echo "### rep $rep $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)" | tee -a "$LOG"
  for workers in 1 8; do
    for scenario in int str inc; do
      # The binary join has no incremental scenario.
      for engine in "${engines[@]}"; do
        [[ $engine == join && $scenario == inc ]] && continue
        run checkout "$engine" "$scenario" "$workers"
      done
      for label in "${labels[@]}"; do
        [[ ${engine_of[$label]} == join && $scenario == inc ]] && continue
        run "$label" "${engine_of[$label]}" "$scenario" "$workers"
      done
    done
  done
done
echo "### done $(date +%T)" | tee -a "$LOG"

# Medians of this invocation's runs: answer seconds / peak MiB.
tail -n +"$first" "$LOG" | python3 -c '
import re, statistics, sys
runs, bad = {}, 0
for line in sys.stdin:
    if not line.startswith("build="):
        continue
    if line.rstrip().endswith(("FAILED", "MISMATCH")):
        bad += 1
        continue
    fields = dict(re.findall(r"(\w+)=(\S+)", line))
    key = (fields["build"], fields["engine"], fields["scenario"], fields["workers"])
    runs.setdefault(key, []).append((float(fields["answer_s"]), int(fields["peak_mib"])))
columns = [(s, w) for w in ("1", "8") for s in ("int", "str", "inc")]
print("| build | engine | " + " | ".join(f"{s} w={w}" for s, w in columns) + " |")
print("|---|---|" + "---|" * len(columns))
for build, engine in dict.fromkeys(key[:2] for key in runs):
    cells = []
    for scenario, workers in columns:
        found = runs.get((build, engine, scenario, workers))
        cells.append(
            f"{statistics.median(t for t, _ in found):.2f} s / {statistics.median(m for _, m in found):.0f} MiB"
            if found else "-"
        )
    print(f"| {build} | {engine} | " + " | ".join(cells) + " |")
if bad:
    print(f"{bad} runs failed or reached a wrong checksum; see {sys.argv[1]}.")
' "$LOG"
