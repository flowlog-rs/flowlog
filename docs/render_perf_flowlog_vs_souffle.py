#!/usr/bin/env python3
"""Render the FlowLog-vs-Souffle workload bar chart from docs/perf-snapshot.csv.

Output: docs/perf-flowlog-vs-souffle.svg (also embedded in tests/README.md).
Re-run after a fresh L3 sweep: `python3 docs/render_perf_flowlog_vs_souffle.py`.
"""

from __future__ import annotations

import csv
import pathlib
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parent
SRC = ROOT / "perf-snapshot.csv"
OUT = ROOT / "perf-flowlog-vs-souffle.svg"


def main() -> int:
    rows: list[tuple[str, float, float]] = []
    with SRC.open() as f:
        for r in csv.DictReader(f):
            try:
                fl = float(r["Compiler_Exec_s"])
                su = float(r["Souffle_Total_s"])
            except (KeyError, ValueError):
                continue
            if fl > 0 and su > 0:
                rows.append((f"{r['Program']}/{r['Dataset']}", fl, su))
    if not rows:
        print(f"no usable rows in {SRC}", file=sys.stderr)
        return 1

    rows.sort(key=lambda x: x[2] / x[1], reverse=True)
    labels = [r[0] for r in rows]
    fl_vals = [r[1] for r in rows]
    su_vals = [r[2] for r in rows]
    n = len(rows)

    x = np.arange(n)
    w = 0.4
    fig, ax = plt.subplots(figsize=(max(14, n * 0.55), 7.5))
    ax.bar(x - w / 2, fl_vals, w, label="FlowLog (compiler)",
           color="#2E7DD7", edgecolor="black", linewidth=0.4)
    ax.bar(x + w / 2, su_vals, w, label="Souffle (compiled, -j 64)",
           color="#E8854E", edgecolor="black", linewidth=0.4)
    ax.set_yscale("log")
    ax.set_ylabel("End-to-end runtime (seconds, log scale)")
    ax.set_title(
        f"FlowLog vs Souffle compiler — end-to-end runtime "
        f"({n} workloads, WORKERS=64, median of 3 runs)\n"
        "Sorted by FlowLog speedup; lower is better",
        pad=14,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=55, ha="right", fontsize=9)
    ax.grid(True, axis="y", which="both", alpha=0.3, linestyle="--")
    ax.legend(loc="upper left")

    for i, (fl, su) in enumerate(zip(fl_vals, su_vals)):
        sp = su / fl
        ax.text(i, max(fl, su) * 1.10, f"{sp:.1f}×",
                ha="center", fontsize=8, color="#1B4F72", fontweight="bold")

    ax.set_ylim(top=ax.get_ylim()[1] * 2.0)
    fig.tight_layout()
    fig.savefig(OUT)
    print(f"wrote {OUT.relative_to(ROOT.parent)} ({n} workloads)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
