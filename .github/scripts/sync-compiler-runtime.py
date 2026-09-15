#!/usr/bin/env python3
"""Synchronize the compiler's runtime requirement with the release checkout."""

import argparse
import json
from pathlib import Path
import re
import subprocess


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true", help="fail if synchronization is needed")
args = parser.parse_args()

root = Path(__file__).resolve().parents[2]
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=root, text=True,
))
version = next(p["version"] for p in metadata["packages"] if p["name"] == "flowlog-runtime")
scaffold = root / "flowlog-compiler/src/scaffold.rs"
original = scaffold.read_text()
updated, count = re.subn(
    r'^const RUNTIME_VERSION: &str = "[^"]+";$',
    f'const RUNTIME_VERSION: &str = "{version}";',
    original,
    flags=re.MULTILINE,
)
if count != 1:
    raise SystemExit(f"Expected one RUNTIME_VERSION declaration in {scaffold}; found {count}")
if args.check:
    if updated != original:
        raise SystemExit(f"Compiler runtime requirement must match flowlog-runtime {version}")
elif updated != original:
    scaffold.write_text(updated)
    print(f"Synchronized compiler runtime requirement to {version}")
