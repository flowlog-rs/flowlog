#!/usr/bin/env python3
"""Runs a batch binary several times and appends one CSV row per run.

Wall time and peak RSS come from GNU time; relation sizes come from the
binary's `[size]` lines (it runs with `-D -`, which prints them). A run is ok
when it finished and every --expect size matched. Each worker count prints a
line per run and then the medians.

example:
  scripts/bench.py --label tc_base --bin "$BIN/tc_base" --facts "$DATA/G5K-0.001" --expect Tc=24730729
"""

import argparse
import csv
import datetime
import os
import re
import statistics
import subprocess
import sys

from paths import RESULTS

COLUMNS = ["stamp", "label", "workers", "rep", "wall_s", "maxrss_kb", "sizes", "ok", "env", "load1"]


def pair(text):
    name, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"expected NAME=VALUE, got {text!r}")
    return name, value


def expectation(text):
    name, value = pair(text)
    if not value.isdigit():
        raise argparse.ArgumentTypeError(f"expected REL=SIZE, got {text!r}")
    return name, int(value)


def run_once(args, workers, env):
    """Runs the binary once; returns (wall seconds, peak RSS in KB, sizes, ok)."""
    command = ["/usr/bin/time", "-f", "WALL=%e MAXRSS=%M", args.bin, "-w", str(workers), "-F", args.facts, "-D", "-"]
    try:
        done = subprocess.run(command, capture_output=True, text=True, env=env, timeout=args.timeout, check=False)
        out = done.stdout + done.stderr
    except subprocess.TimeoutExpired:
        out = "TIMEOUT"
    usage = re.search(r"WALL=([\d.]+) MAXRSS=(\d+)", out)
    sizes = dict(re.findall(r"\[size\]\[(\w+)\][^\n]*?size=(\d+)", out))
    ok = usage is not None and all(int(sizes.get(name, -1)) == size for name, size in args.expect)
    if usage is None:
        print(out[-2000:], file=sys.stderr)
        return float("nan"), -1, sizes, False
    return float(usage[1]), int(usage[2]), sizes, ok


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True, help="name of this configuration in the CSV")
    parser.add_argument("--bin", required=True, help="the binary to run")
    parser.add_argument("--facts", required=True, help="input directory (-F)")
    parser.add_argument("--workers", default="8", help="comma-separated worker counts (default 8)")
    parser.add_argument("--reps", type=int, default=3, help="runs per worker count (default 3)")
    parser.add_argument("--env", type=pair, action="append", default=[], metavar="NAME=VALUE",
                        help="environment variable for the binary; repeatable")
    parser.add_argument("--expect", type=expectation, action="append", default=[], metavar="REL=SIZE",
                        help="expected output size; repeatable")
    parser.add_argument("--csv", default=RESULTS / "runs.csv", help="CSV to append to (default $RESULTS/runs.csv)")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds before a run is killed (default 3600)")
    args = parser.parse_args()

    env = {**os.environ, **dict(args.env)}
    env_text = " ".join(f"{name}={value}" for name, value in args.env)
    os.makedirs(os.path.dirname(os.path.abspath(args.csv)), exist_ok=True)
    new = not os.path.exists(args.csv)
    with open(args.csv, "a", newline="") as file:
        writer = csv.writer(file)
        if new:
            writer.writerow(COLUMNS)
        for workers in map(int, args.workers.split(",")):
            walls, rss = [], []
            for rep in range(args.reps):
                load1 = os.getloadavg()[0]
                wall, kb, sizes, ok = run_once(args, workers, env)
                walls.append(wall)
                rss.append(kb)
                stamp = datetime.datetime.now().isoformat(timespec="seconds")
                size_text = ";".join(f"{name}={size}" for name, size in sorted(sizes.items()))
                row = [stamp, args.label, workers, rep, wall, kb, size_text, int(ok), env_text, f"{load1:.1f}"]
                writer.writerow(row)
                file.flush()
                status = "ok" if ok else f"MISMATCH/FAIL {sizes}"
                print(f"{args.label:28s} w={workers:<3d} rep={rep} wall={wall:7.2f}s rss={kb / 1024:9.1f}MB {status}",
                      flush=True)
            print(f"{args.label:28s} w={workers:<3d} MEDIAN wall={statistics.median(walls):7.2f}s "
                  f"rss={statistics.median(rss) / 1024:9.1f}MB", flush=True)


if __name__ == "__main__":
    main()
