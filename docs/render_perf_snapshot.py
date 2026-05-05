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
    labels  = [f"{r['Program']}/{r['Dataset']}" for r in rows]
    comp_t  = [float(r["Compiler_Exec_s"])     for r in rows]
    souf_t  = [float(r["Souffle_Total_s"])     for r in rows]
    comp_m  = [float(r["Compiler_PeakRss_MB"]) / 1024 for r in rows]  # GiB
    souf_m  = [float(r["Souffle_PeakRss_MB"])  / 1024 for r in rows]

    x = np.arange(len(labels))
    width = 0.38
    comp_color = "#1f77b4"   # FlowLog compiler — steel blue
    souf_color = "#ff7f0e"   # Souffle          — orange

    fig, (ax_t, ax_m) = plt.subplots(
        2, 1, figsize=(13, 7), constrained_layout=True
    )

    # ---- Time panel
    ax_t.bar(x - width/2, comp_t, width, label="FlowLog (compiler)", color=comp_color)
    ax_t.bar(x + width/2, souf_t, width, label="Souffle (compiled)", color=souf_color)
    ax_t.set_xticks(x)
    ax_t.set_xticklabels(labels, rotation=55, ha="right", fontsize=8)
    ax_t.set_ylabel("Execution time (s, median of 3, log scale)")
    ax_t.set_yscale("log")
    ax_t.set_title("FlowLog compiler vs Souffle — execution time (lower is better)")
    ax_t.legend(loc="upper left", frameon=False)
    ax_t.grid(axis="y", which="both", linestyle=":", linewidth=0.5, alpha=0.5)

    # ---- Memory panel
    ax_m.bar(x - width/2, comp_m, width, label="FlowLog (compiler)", color=comp_color)
    ax_m.bar(x + width/2, souf_m, width, label="Souffle (compiled)", color=souf_color)
    ax_m.set_xticks(x)
    ax_m.set_xticklabels(labels, rotation=55, ha="right", fontsize=8)
    ax_m.set_ylabel("Peak RSS (GiB, median of 3)")
    ax_m.set_title("Peak memory (lower is better)")
    ax_m.legend(loc="upper left", frameon=False)
    ax_m.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.5)

    fig.suptitle(
        "FlowLog benchmark snapshot — 30 (program, dataset) pairs, NUM_RUNS=3, WORKERS=64",
        fontsize=11,
    )
    fig.savefig(OUT, format="svg", bbox_inches="tight")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
