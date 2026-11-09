#!/usr/bin/env python3
"""Runs a command under a memory and time limit and reports how it went.

usage: scripts/guarded.py [--max-gb GB] [--max-s SECONDS] [--label NAME] -- COMMAND...

Samples the command's resident memory every 0.2 s and kills it if that
exceeds --max-gb or the run exceeds --max-s. Prints one line: the label, wall
time, peak RSS, why the command stopped, and every output line with `size=`.
The study used it for plans that might blow up (GALEN rule 4, section 6).
"""

import argparse
import subprocess
import tempfile
import time


def status_kb(pid, field):
    """Reads a `kB` field, such as VmRSS, of /proc/PID/status; 0 if gone."""
    try:
        with open(f"/proc/{pid}/status") as status:
            for line in status:
                if line.startswith(field + ":"):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-gb", type=float, default=48, help="RSS limit in GiB (default 48)")
    parser.add_argument("--max-s", type=float, default=600, help="wall-time limit in seconds (default 600)")
    parser.add_argument("--label", default="run", help="name printed with the result")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("give a command after --")

    peak_kb, reason = 0, "exit"
    with tempfile.TemporaryFile("w+") as log:
        start = time.time()
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, text=True)
        while process.poll() is None:
            peak_kb = max(peak_kb, status_kb(process.pid, "VmHWM"))
            if status_kb(process.pid, "VmRSS") > args.max_gb * 1024 * 1024:
                reason = f"killed: RSS > {args.max_gb} GB"
            elif time.time() - start > args.max_s:
                reason = f"killed: wall > {args.max_s} s"
            if reason != "exit":
                process.kill()
                break
            time.sleep(0.2)
        process.wait()
        wall = time.time() - start
        log.seek(0)
        out = log.read()
    sizes = [line.strip() for line in out.splitlines() if "size=" in line]
    print(f"{args.label:28s} wall={wall:8.2f}s peak_rss={peak_kb / 1024:9.1f}MB {reason} | {' ; '.join(sizes)}")


if __name__ == "__main__":
    main()
