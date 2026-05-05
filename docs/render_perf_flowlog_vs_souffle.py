#!/usr/bin/env python3
"""Render the FlowLog-vs-Souffle workload bar chart.

Default input: `docs/perf-snapshot.csv` (the curated, committed snapshot).
Override:      pass a path on the command line, e.g. the raw 26-column
               `result/sweep/<ts>/comparison_results.csv` straight from a
               fresh sweep — both schemas are accepted.

Output: docs/perf-flowlog-vs-souffle.svg (also embedded in tests/README.md
and the top-level README.md).

Re-render after a fresh L3 sweep:
    python3 docs/render_perf_flowlog_vs_souffle.py \\
        result/sweep/<UTC-ts>/comparison_results.csv

Design goals:
  * High signal-to-ink: log-scale time, sorted by speedup, no redundant labels.
  * Restrained palette: one cool / one warm tone; subtle grid; no colored grids.
  * Per-pair speedup annotation in the FlowLog house color so the eye is drawn
    to the win, not to the absolute numbers.
"""

from __future__ import annotations

import csv
import math
import pathlib
import sys

if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
    print(__doc__)
    sys.exit(0)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import font_manager
from matplotlib.ticker import LogLocator, NullLocator

ROOT = pathlib.Path(__file__).resolve().parent
DEFAULT_SRC = ROOT / "perf-snapshot.csv"
OUT = ROOT / "perf-flowlog-vs-souffle.svg"

# Restrained, high-contrast palette.
FLOWLOG_BLUE = "#1F6FEB"   # GitHub primer accent — calm, professional
SOUFFLE_AMBER = "#D97706"  # Warm complement, lower saturation than red
TEXT_DARK = "#1F2328"
TEXT_MUTED = "#57606A"
GRID_FAINT = "#D0D7DE"

# Use a clean sans-serif if available (Inter / IBM Plex / DejaVu fallback).
for _candidate in ("Inter", "IBM Plex Sans", "Helvetica Neue", "Helvetica", "DejaVu Sans"):
    if any(_candidate in f.name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = _candidate
        break
plt.rcParams.update({
    "axes.edgecolor": GRID_FAINT,
    "axes.linewidth": 0.8,
    "axes.labelcolor": TEXT_DARK,
    "xtick.color": TEXT_MUTED,
    "ytick.color": TEXT_MUTED,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "savefig.facecolor": "white",
    "figure.facecolor": "white",
})


def _pick(row: dict[str, str], *names: str) -> str | None:
    """Return the first non-empty cell among `names`, or None.

    Lets the renderer accept either the curated snapshot schema
    (Compiler_Exec_s / Souffle_Total_s) or the raw sweep schema
    (Compiler_Exec / Souffle_Total).
    """
    for n in names:
        v = (row.get(n) or "").strip()
        if v and v.lower() not in {"n/a", "na", "-", ""}:
            return v
    return None


def main() -> int:
    src = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
    if not src.exists():
        print(f"input not found: {src}", file=sys.stderr)
        return 1

    rows: list[tuple[str, float, float]] = []
    with src.open() as f:
        for r in csv.DictReader(f):
            fl_s = _pick(r, "Compiler_Exec_s", "Compiler_Exec")
            su_s = _pick(r, "Souffle_Total_s", "Souffle_Total")
            if fl_s is None or su_s is None:
                continue
            try:
                fl, su = float(fl_s), float(su_s)
            except ValueError:
                continue
            if fl > 0 and su > 0:
                rows.append((f"{r['Program']}/{r['Dataset']}", fl, su))
    if not rows:
        print(f"no usable rows in {src}", file=sys.stderr)
        return 1

    rows.sort(key=lambda x: x[2] / x[1], reverse=True)
    labels = [r[0] for r in rows]
    fl_vals = np.array([r[1] for r in rows])
    su_vals = np.array([r[2] for r in rows])
    speedups = su_vals / fl_vals
    n = len(rows)

    geomean = math.exp(np.mean(np.log(speedups)))
    sp_min, sp_max = speedups.min(), speedups.max()

    x = np.arange(n)
    w = 0.38

    fig, ax = plt.subplots(figsize=(max(15, n * 0.55), 7.8))

    ax.bar(
        x - w / 2, fl_vals, w,
        label="FlowLog (compiler)",
        color=FLOWLOG_BLUE, edgecolor="none", zorder=3,
    )
    ax.bar(
        x + w / 2, su_vals, w,
        label="Soufflé (compiled, -j 64)",
        color=SOUFFLE_AMBER, edgecolor="none", zorder=3,
    )

    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.set_ylabel("End-to-end runtime (seconds, log scale)",
                  color=TEXT_DARK, fontsize=11)
    ax.grid(True, axis="y", which="major", color=GRID_FAINT, linewidth=0.8,
            linestyle="-", zorder=0)
    ax.set_axisbelow(True)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9.5,
                       color=TEXT_DARK)
    ax.tick_params(axis="x", length=0, pad=3)
    ax.tick_params(axis="y", length=4, pad=4)

    for i, (fl, su, sp) in enumerate(zip(fl_vals, su_vals, speedups)):
        ax.text(i, max(fl, su) * 1.18, f"{sp:.1f}×",
                ha="center", va="bottom",
                fontsize=8.5, color=FLOWLOG_BLUE, fontweight="600")

    ax.set_title(
        "FlowLog vs Soufflé — end-to-end compiler runtime",
        loc="left", color=TEXT_DARK, fontsize=15, fontweight="600", pad=24,
    )
    fig.text(
        0.012, 0.928,
        f"{n} workloads · WORKERS = 64 · median of 3 runs · "
        f"FlowLog wins {n}/{n} · geomean {geomean:.2f}× · "
        f"range {sp_min:.2f}× → {sp_max:.2f}×",
        color=TEXT_MUTED, fontsize=10.5,
    )

    leg = ax.legend(
        loc="upper left", frameon=False, fontsize=10.5,
        handlelength=1.4, handleheight=0.9, borderpad=0.6,
        labelcolor=TEXT_DARK,
    )
    for txt in leg.get_texts():
        txt.set_color(TEXT_DARK)

    ax.set_ylim(top=ax.get_ylim()[1] * 2.4)

    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(OUT, bbox_inches="tight", pad_inches=0.25)
    print(f"wrote {OUT.relative_to(ROOT.parent)} ({n} workloads, geomean {geomean:.2f}×) "
          f"from {src}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
