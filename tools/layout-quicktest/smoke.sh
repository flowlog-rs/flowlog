#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
output=$(mktemp -d)
trap 'rm -rf "$output"' EXIT

"$script_dir/run.sh" \
    "$script_dir/examples/path/path.dl" \
    "$script_dir/examples/path/facts" "$output" 4

python3 - "$output" <<'PY'
import json
import sys
from pathlib import Path

output = Path(sys.argv[1])
plan = json.loads((output / 'plans/path.json').read_text())
selection = json.loads((output / 'selected/path.json').read_text())
path_id = next(relation['id'] for relation in plan['relations']
               if relation['name'] == 'Path')
assert selection['selected']['relations'][path_id] == [1, 0]
assert list(selection['selected']['swapped'].values()) == [1]
assert selection['selected_priority'] == [6, 0, 1]
actual = {tuple(map(int, line.split())) for line in
          (output / 'results/Path.csv').read_text().splitlines()}
expected = {(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)}
assert actual == expected, (actual, expected)
print('layout quicktest smoke passed')
PY
