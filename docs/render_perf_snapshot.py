#!/usr/bin/env python3
"""Render docs/perf-snapshot.svg from docs/perf-snapshot.csv.

The snapshot lives in-tree so the README can show real numbers without
re-running benchmarks on every doc edit. Re-run this script after a
fresh sweep when you want to refresh the figure.

Note: the y-axis labels say "median of 5" because perf-snapshot.csv was
captured under the old `NUM_RUNS=5` default. If you regenerate the CSV
with the current default (`NUM_RUNS=3`), update both labels accordingly.

Usage:
    python3 docs/render_perf_snapshot.py
"""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).parent
CSV = HERE / "perf-snapshot.csv"
OUT = HERE / "perf-snapshot.svg"


def main() -> None:
    rows = list(csv.DictReader(CSV.open()))
    labels = [r["Program"] for r in rows]
    interp_t  = [float(r["Interp_Exec"]) for r in rows]
    comp_t    = [float(r["Compiler_Exec"]) for r in rows]
    lib_t     = [float(r["Lib_Exec"]) for r in rows]
    interp_m  = [float(r["Interp_PeakRss_MB"]) / 1024 for r in rows]   # GiB
    comp_m    = [float(r["Compiler_PeakRss_MB"]) / 1024 for r in rows]
    lib_m     = [float(r["Lib_PeakRss_MB"])     / 1024 for r in rows]

    x = np.arange(len(labels))
    width = 0.27
    interp_color = "#888888"
    comp_color   = "#1f77b4"
    lib_color    = "#2ca02c"

    fig, (ax_t, ax_m) = plt.subplots(
        1, 2, figsize=(11, 4.2), constrained_layout=True
    )

    # ---- Time panel
    ax_t.bar(x - width, interp_t, width, label="interpreter", color=interp_color)
    ax_t.bar(x,         comp_t,   width, label="compiler",    color=comp_color)
    ax_t.bar(x + width, lib_t,    width, label="library",     color=lib_color)
    ax_t.set_xticks(x)
    ax_t.set_xticklabels(labels)
    ax_t.set_ylabel("Execution time (s, median of 5)")
    ax_t.set_title("Execution time — lower is better")
    ax_t.legend(loc="upper left", frameon=False)
    ax_t.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.6)

    # ---- Memory panel
    ax_m.bar(x - width, interp_m, width, label="interpreter", color=interp_color)
    ax_m.bar(x,         comp_m,   width, label="compiler",    color=comp_color)
    ax_m.bar(x + width, lib_m,    width, label="library",     color=lib_color)
    ax_m.set_xticks(x)
    ax_m.set_xticklabels(labels)
    ax_m.set_ylabel("Peak RSS (GiB, median of 5)")
    ax_m.set_title("Peak memory — lower is better")
    ax_m.legend(loc="upper left", frameon=False)
    ax_m.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.6)

    fig.suptitle(
        "FlowLog benchmark snapshot — 6 (program, dataset) pairs, WORKERS=64",
        fontsize=11,
    )
    fig.savefig(OUT, format="svg", bbox_inches="tight")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
