#!/usr/bin/env python3
"""Plot compiler vs interpreter speedup from benchmark CSV results."""

import csv
import os
import sys
from itertools import groupby

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rcParams["font.family"] = "serif"
matplotlib.rcParams["font.size"] = 11

Y_MAX = 2.0
BAR_WIDTH = 0.25
FIG_SIZE = (18, 6)


def read_csv(path):
    """Read benchmark CSV and return programs, datasets, and speedup lists."""
    programs, datasets = [], []
    load_sp, exec_sp, total_sp = [], [], []

    with open(path, "r") as f:
        for row in csv.DictReader(f):
            programs.append(row["Program"])
            datasets.append(row["Dataset"])
            load_sp.append(float(row["Load_Speedup"]))
            exec_sp.append(float(row["Exec_Speedup"]))
            total_sp.append(float(row["Total_Speedup"]))

    return programs, datasets, load_sp, exec_sp, total_sp


def build_group_info(programs):
    """Find consecutive groups of the same program for axis labeling."""
    groups = []
    idx = 0
    for prog, grp in groupby(programs):
        count = sum(1 for _ in grp)
        groups.append((prog, idx, idx + count - 1))
        idx += count
    return groups


def plot_speedup(programs, datasets, load_sp, exec_sp, total_sp):
    """Create the grouped bar chart with 1.0 baseline."""
    n = len(datasets)
    x = np.arange(n)

    clip = lambda vals: [min(v, Y_MAX) for v in vals]
    load_clipped = clip(load_sp)
    exec_clipped = clip(exec_sp)
    total_clipped = clip(total_sp)

    fig, ax = plt.subplots(figsize=FIG_SIZE)

    # Bars grow up/down from 1.0 baseline
    series = [
        (x - BAR_WIDTH, load_clipped, load_sp, "Load Speedup", "#2196F3"),
        (x, exec_clipped, exec_sp, "Execute Speedup", "#FF5722"),
        (x + BAR_WIDTH, total_clipped, total_sp, "Total Speedup", "#4CAF50"),
    ]
    for xpos, clipped, original, label, color in series:
        ax.bar(xpos, [v - 1.0 for v in clipped], BAR_WIDTH,
               bottom=1.0, label=label, color=color, zorder=3)

        # Annotate outliers with their true value
        for val, xi in zip(original, xpos):
            if val > Y_MAX:
                ax.text(xi, Y_MAX - 0.05, f"{val:.2f}",
                        ha="center", va="top", fontsize=6, fontweight="bold")

    # Baseline
    ax.axhline(y=1.0, color="black", linestyle="-", linewidth=1.5, alpha=0.9)

    # Dataset tick labels
    ax.set_xticks(x)
    ax.set_xticklabels(datasets, rotation=45, ha="right", fontsize=7)

    # Program group labels and separators
    for prog, start, end in build_group_info(programs):
        mid = (start + end) / 2.0
        ax.text(mid, -0.18, prog, ha="center", va="top", fontsize=8,
                fontweight="bold", transform=ax.get_xaxis_transform())
        if start > 0:
            ax.axvline(x=start - 0.5, color="lightgray",
                       linewidth=0.8, linestyle="-", alpha=0.5)

    ax.set_ylabel("Speedup (Compiler / Interpreter)", fontsize=12)
    ax.set_title("Compiler vs Interpreter Speedup across Benchmarks",
                 fontsize=14, fontweight="bold")
    ax.legend(loc="upper right", fontsize=10, framealpha=0.9)
    ax.grid(axis="y", alpha=0.3)
    ax.set_xlim(-0.5, n - 0.5)
    ax.set_ylim(0.4, Y_MAX + 0.05)

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.22)
    return fig


def main():
    if len(sys.argv) != 2:
        print("Usage: python plot_speedup.py <csv_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    programs, datasets, load_sp, exec_sp, total_sp = read_csv(csv_path)
    fig = plot_speedup(programs, datasets, load_sp, exec_sp, total_sp)

    output_dir = os.path.dirname(os.path.abspath(csv_path))
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(output_dir, f"speedup_figure.{ext}"),
                    dpi=300, bbox_inches="tight")

    print(f"Saved speedup_figure.{{pdf,png}} to {output_dir}")


if __name__ == "__main__":
    main()
