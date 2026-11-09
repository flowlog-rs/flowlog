#!/usr/bin/env python3
"""Splits a dataset into a bulk load and one commit, for incremental runs.

usage: scripts/split.py SOURCE DEST EVERY FILE...

Every EVERY-th row of each FILE goes to DEST/upd and the others to DEST/pre;
the other CSV files of SOURCE are symlinked into DEST/pre unchanged. A run
then loads DEST/pre and inserts DEST/upd in one commit (scripts/inc_run.sh).
The study holds out every 1000th row (0.1%).
"""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source", type=Path)
    parser.add_argument("dest", type=Path)
    parser.add_argument("every", type=int)
    parser.add_argument("files", nargs="+", metavar="FILE")
    args = parser.parse_args()

    pre, upd = args.dest / "pre", args.dest / "upd"
    pre.mkdir(parents=True, exist_ok=True)
    upd.mkdir(parents=True, exist_ok=True)
    missing = [name for name in args.files if not (args.source / name).is_file()]
    if missing:
        raise SystemExit(f"split.py: not in {args.source}: {' '.join(missing)}")
    for source in sorted(args.source.glob("*.csv")):
        if source.name in args.files:
            rows = 0
            with open(source) as rows_in, open(pre / source.name, "w") as kept, open(upd / source.name, "w") as held:
                for rows, line in enumerate(rows_in, 1):
                    (held if rows % args.every == 0 else kept).write(line)
            print(f"split {source.name}: {rows} rows, {rows // args.every} held out")
        elif not (pre / source.name).exists():
            (pre / source.name).symlink_to(source.resolve())


if __name__ == "__main__":
    main()
