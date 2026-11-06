#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    printf 'Usage: %s QUERY.dl FACT_DIR OUTPUT_DIR [WORKERS]\n' "$0" >&2
    exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
query=$(realpath "$1")
facts=$(realpath "$2")
output=$(realpath -m "$3")
workers=${4:-4}
name=${query##*/}
name=${name%.dl}
target=${FLOWLOG_LAYOUT_QUICKTEST_TARGET_DIR:-$repo_root/target/layout-quicktest}
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
export PYTHONDONTWRITEBYTECODE=1

mkdir -p "$output/plans" "$output/selected" "$output/results"

cargo build --release --locked -q -p flowlog-compiler \
    --manifest-path "$repo_root/Cargo.toml" --target-dir "$target"
cargo build --release --locked -q -p flowlog-codegen --example export_plan \
    --manifest-path "$repo_root/Cargo.toml" --target-dir "$target"

"$target/release/examples/export_plan" "$query" > "$output/plans/$name.json"
python3 "$script_dir/layout-dp-benchmark-20261021/optimizer.py" \
    --plans "$output/plans" --output "$output/selected" --programs "$name"
python3 - "$output/selected/$name.json" "$output/layout.json" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text())
Path(sys.argv[2]).write_text(json.dumps(result['selected'], indent=2) + '\n')
print('priority:', result['baseline_priority'], '->', result['selected_priority'])
PY

FLOWLOG_EXPERIMENT_LAYOUT_PLAN="$output/layout.json" \
FLOWLOG_RUNTIME_PATH="$repo_root/flowlog-runtime" \
"$target/release/flowlog-compiler" "$query" --str-intern \
    -D "$output/results" -B "$output/generated" -T "$target" \
    -o "$output/optimized"
"$output/optimized" -F "$facts" -D "$output/results" -w "$workers"
printf 'plan: %s\nselection: %s\nlayout: %s\nexecutable: %s\nresults: %s\n' \
    "$output/plans/$name.json" "$output/selected/$name.json" \
    "$output/layout.json" "$output/optimized" "$output/results"
