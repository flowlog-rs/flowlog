#!/usr/bin/env python3
"""GALEN rule 4's path for dP(x,y), routed per row by the static count of y.

A light row (C has at most T rows for y) proposes (w,z) from C by y and then
validates P(x,w); a heavy row proposes w from P by x and then validates
C(y,w,_). Only C's counts decide, so the split is fixed before the loop runs.
Prints the path work for a range of thresholds T (REPORT.md section 7).

usage: scripts/galen_split.py
"""

import argparse

from galen_data import count_by, relation

THRESHOLDS = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 4096, 10**9]


def main():
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    P, C = relation("P"), relation("C")
    P0, C0 = count_by(P, 0), count_by(C, 0)
    print("C by y: distinct", len(C0), "max", max(C0.values()), " | P by x: distinct", len(P0), "max", max(P0.values()))
    for threshold in THRESHOLDS:
        work = sum(C0[y] if C0[y] <= threshold else P0[x] for x, y in P if C0[y] > 0)
        heavy = sum(1 for n in C0.values() if n > threshold)
        print(f"T={threshold:>10}: path work {work / 1e6:8.1f}M   heavy keys {heavy}")
    print(f"per-row min {sum(min(C0[y], P0[x]) for x, y in P if C0[y] > 0) / 1e6:.1f}M")


if __name__ == "__main__":
    main()
