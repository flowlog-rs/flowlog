#!/usr/bin/env python3
r"""Samples the resident memory of one incremental run over time.

usage: scripts/rss_timeline.py OUT.tsv BINARY WORKERS SPLIT REL=FILE...

Runs the same transaction as scripts/inc_run.sh (load SPLIT/pre, insert each
SPLIT/upd/FILE into REL in one commit, quit), samples VmRSS every 100 ms into
OUT.tsv as `seconds<TAB>KB` lines, and prints the peak and the RSS at 25%,
50%, 75% and 90% of the run. For CSPA (section 8):

  scripts/rss_timeline.py "$RESULTS/rss_timeline_cspa_inc.tsv" "$BIN/cspa_inc" 8 \
      "$DATA/cspa-split" assign=Assign.csv dereference=Dereference.csv
"""

import argparse
import subprocess
import time
from pathlib import Path


def rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out", type=Path)
    parser.add_argument("binary", type=Path)
    parser.add_argument("workers")
    parser.add_argument("split", type=Path)
    parser.add_argument("inserts", nargs="+", metavar="REL=FILE")
    args = parser.parse_args()

    commands = ["begin"]
    for insert in args.inserts:
        relation, _, file = insert.partition("=")
        commands.append(f"insert {relation} @{args.split / 'upd' / file}")
    commands += ["commit", "quit"]

    command = [str(args.binary), "-w", args.workers, "-F", str(args.split / "pre"), "-D", "-"]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, text=True)
    process.stdin.write("\n".join(commands) + "\n")
    process.stdin.close()
    start, samples = time.time(), []
    while process.poll() is None:
        kb = rss_kb(process.pid)
        if kb is None:
            break
        samples.append((time.time() - start, kb))
        time.sleep(0.1)
    process.wait()
    if not samples:
        raise SystemExit("rss_timeline.py: the run ended before the first sample")

    with open(args.out, "w") as out:
        out.writelines(f"{seconds:.2f}\t{kb}\n" for seconds, kb in samples)
    gib = 1024 * 1024
    peak_at, peak = max(samples, key=lambda sample: sample[1])
    at = lambda q: samples[min(len(samples) - 1, int(q * len(samples)))][1]
    quantiles = " ".join(f"t={q:.0%}:{at(q) / gib:.2f}" for q in (0.25, 0.5, 0.75, 0.9))
    print(f"{args.binary.name}: peak {peak / gib:.2f} GiB at {peak_at:.1f}s of {samples[-1][0]:.1f}s; {quantiles}")


if __name__ == "__main__":
    main()
