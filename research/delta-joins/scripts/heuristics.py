#!/usr/bin/env python3
"""Scores join-plan heuristics for FlowLog against the study's measurements.

A case is one rule, or one 2-atom join, of one workload on one dataset. For
each case the script records

  * what a planner could know or estimate before running: the mode (batch or
    incremental), the rule's atoms and how many of them change, the binary
    plan's intermediate and how much of it stays in memory, the records the
    whole binary program stores, and for each other plan the new indexes it
    builds, the indexes it lets go, its path work W and its duplication rho;
  * what every plan measured: median wall time and peak RSS at 8 workers, and
    for incremental cases the commit latency.

Each heuristic picks one plan per case from what a planner knows; the
scorecard then looks up what that plan measured. There are two sets of
cases: the tuning set, which shaped the heuristics and their thresholds
(REPORT.md section 10), and the held-out set, scored with the thresholds
frozen (section 11). The script scores each set, then both together. H7
was added after the held-out runs, to reject the plan H6 got wrong there
(and Dyck-split in the tuning set), so its held-out score is not a test.

Tuning-set measurements come from the results directory: heur_runs.csv for
batch runs, and heur_inc.log, oldfirst_inc.log and snapfilter_inc.log for
incremental ones (pooled per label). Plans measured before those runs use the
medians REPORT.md publishes; the Runs column then names the section. The
held-out set's runs are in heldout_runs.csv, kept apart so that each case
compares plans from one session.

Some tuning-set inputs come from probe builds: probe_cspa_base_postgresql.txt
and probe_cspa_base_linux.txt (build cspa_base_p) give CSPA rule 6's
intermediate and the records it holds, probe_sg_inc.txt (sg_inc_p) the
records SG's binary plan holds, and lookup_rows_probe.log and
snapfilter_probe.log the rows each plan holds inside its lookups. In both
sets, the records each binary program stores come from lookup_rows_probe.log
or, for programs it does not cover, the probe log that STORED_LOGS names. An
input whose log is missing reads n/m and counts as zero, and a CSPA rule 6 case
without its log is left out; the script names such logs on stderr. The
held-out set's other inputs are constants copied from probe_doop_*_batik.txt
and probe_polonius_*_borrow.txt.

usage: scripts/heuristics.py [--results DIR] [--picks]

DIR defaults to results/, the study's own runs, for which the output is
results/heuristics_out.md. --picks prints each heuristic's pick per case from
the planner inputs alone, without measurements (results/heldout_picks.md
recorded the held-out picks of H0 to H6 before the held-out runs).
"""

import argparse
import collections
import csv
import math
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

from paths import ROOT

M = 1e6

# CPU guards (H4 and later). A plan's path work may be at most WORK_MAX times
# the binary intermediate, and its paths may produce at most RHO_MAX output
# rows per binary output row: paths do not consolidate between hops.
WORK_MAX = 4.0
RHO_MAX = 1.5

# What H6 charges for a row held inside a lookup, in stored records: such a
# row takes 25-38 bytes of RSS, an arranged record about 15 (REPORT.md
# section 10.5).
LOOKUP_ROW_WEIGHT = 2.0

# H7's margin, chosen after the held-out runs (REPORT.md section 11.4): a plan
# of 3+ atoms must save at least this share of the records that the binary
# program stores at its peaks, traces plus batchers (rows held in lookups are
# not records). Any share from 1% to 11% picks the same plans on both sets.
MARGIN = 0.05


# ---------------------------------------------------------------- measurements


@dataclass
class Measured:
    """A plan's medians at 8 workers."""

    wall: float  # seconds
    rss: float  # MB
    commit: float | None = None  # seconds, for incremental runs
    runs: int | None = None  # runs pooled; None for a published median
    section: str | None = None  # the REPORT.md section that publishes it


def published(section, wall, rss, commit=None):
    return Measured(wall, rss, commit, section="\u00a7" + section)


# Medians that REPORT.md publishes, for plans measured before the policy runs.
PUBLISHED = {
    "tc_base": published("4", 4.33, 1121),
    "tc_lookup": published("4", 3.88, 1129),
    "cspa_base@httpd": published("5.1", 27.59, 8870),
    "cspa_delta_sym@httpd": published("5.1", 26.45, 7506),
    "cspa_delta@httpd": published("5.1", 28.46, 8224),
    "galen_base": published("5.2", 10.77, 3972),
    "galen_d3": published("6.1", 16.04, 3906),
    "galen_d4": published("6.1", 77.17, 2851),
    "galen_d4p": published("7", 13.85, 3019),
    "galen_d4s": published("7", 10.92, 2955),
    "galen_d6": published("5.2", 11.78, 3617),
    "cspa_inc": published("8", 38.31, 12280, commit=1.755),
    "cspa_inc_r4": published("8", 37.49, 11590, commit=1.577),
    "cspa_inc_r5": published("8", 37.01, 11331, commit=1.692),
    "cspa_inc_r45": published("8", 36.12, 10854, commit=1.542),
}

# A line of scripts/inc_run.sh. Durations are Rust's Debug format.
INC_LINE = re.compile(
    r"^(?P<label>\S+) w=\d+ \| preload=[\d.]+(?:s|ms|\u00b5s|us|ns): "
    r"commit=(?P<commit>[\d.]+)(?P<unit>s|ms|\u00b5s|us|ns): "
    r"WALL=(?P<wall>[\d.]+) MAXRSS=(?P<rss_kb>\d+)"
)
SECONDS = {"s": 1.0, "ms": 1e-3, "\u00b5s": 1e-6, "us": 1e-6, "ns": 1e-9}


def batch_medians(results, name="heur_runs.csv"):
    """Medians per label of the successful runs in one CSV of scripts/bench.py."""
    runs = collections.defaultdict(list)
    path = results / name
    if path.exists():
        with path.open(newline="") as file:
            for row in csv.DictReader(file):
                if row["ok"] == "1":
                    runs[row["label"]].append((float(row["wall_s"]), int(row["maxrss_kb"]) / 1024))
    return {
        label: Measured(wall=median(rows, 0), rss=median(rows, 1), runs=len(rows))
        for label, rows in runs.items()
    }


def incremental_medians(results):
    """Medians per label of the incremental runs, pooled over the logs."""
    runs = collections.defaultdict(list)
    for name in ("heur_inc.log", "oldfirst_inc.log", "snapfilter_inc.log"):
        path = results / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if found := INC_LINE.match(line):
                commit = float(found["commit"]) * SECONDS[found["unit"]]
                runs[found["label"]].append((float(found["wall"]), int(found["rss_kb"]) / 1024, commit))
    return {
        label: Measured(wall=median(rows, 0), rss=median(rows, 1), commit=median(rows, 2), runs=len(rows))
        for label, rows in runs.items()
    }


def median(rows, column):
    return statistics.median(row[column] for row in rows)


def load_measurements(results):
    """Every plan's medians by label. New batch runs take precedence over
    new incremental runs, and both over published medians."""
    return {**PUBLISHED, **incremental_medians(results), **batch_medians(results)}


def probe_peak(results, name, arrangement):
    """The peak records of one arrangement in a probe log (patches/probe.py),
    or None if the log does not have it."""
    path = results / name
    if not path.exists():
        return None
    pattern = r"peak=\s*(\d+)\s+at_total_peak=\s*\d+\s+Arrange: " + arrangement
    found = re.search(pattern, path.read_text(encoding="utf-8"))
    return int(found[1]) if found else None


# Binary programs that lookup_rows_probe.log does not cover, and the probe
# builds' logs that give their stored records instead.
STORED_LOGS = {
    "andersen_base@medium": "probe_andersen_base_medium.txt",
    "galen_base": "probe_galen_base.txt",
    "cspa_inc": "probe_cspa_inc_base_split.txt",
    "polonius_base@borrow": "probe_polonius_base_borrow.txt",
    "doop_base@batik": "probe_doop_base_batik.txt",
}

# The probe logs that hold planner inputs (see the module docstring).
PROBE_LOGS = ("probe_cspa_base_postgresql.txt", "probe_cspa_base_linux.txt", "probe_sg_inc.txt",
              "lookup_rows_probe.log", "snapfilter_probe.log", *STORED_LOGS.values())

# Dataset names in the probe logs' labels, and their names in the cases'.
PROBE_DATASETS = {"cspa-": "", "roadNet-CA": "roadNet", "G40K-0.001": "G40K", "web-BerkStan": "BerkStan"}


def probe_lines(results, name):
    """The (label, line) pairs of a log of probe runs, with the labels'
    dataset names as the cases spell them."""
    path = results / name
    if not path.exists():
        return
    for text in path.read_text(encoding="utf-8").splitlines():
        if found := re.match(r"^(\S+)(?: w=\d+)? +\|", text):
            label = found[1]
            for probe_name, case_name in PROBE_DATASETS.items():
                label = label.replace(probe_name, case_name)
            yield label, text


def lookup_rows(results):
    """Each plan's peak rows held inside its lookups: the most over the probe
    runs in lookup_rows_probe.log and snapfilter_probe.log. Logs written
    before the gauge's rename call it peak_lookup_pending_rows."""
    rows = {}
    for name in ("lookup_rows_probe.log", "snapfilter_probe.log"):
        for label, text in probe_lines(results, name):
            if found := re.search(r"peak_lookup_(?:pending_rows|rows_held)=(\d+)", text):
                rows[label] = max(rows.get(label, 0), int(found[1]))
    return rows


STORED = re.compile(r"peak_total_trace_records=(\d+) peak_batcher_records=(\d+)")


def stored_records(results):
    """The records each binary program stores at its peaks, its traces' peak
    plus its batchers' peak: the most over its probe runs."""
    stored = {}
    runs = list(probe_lines(results, "lookup_rows_probe.log"))
    for label, name in STORED_LOGS.items():
        path = results / name
        if path.exists():
            runs.append((label, path.read_text(encoding="utf-8")))
    for label, text in runs:
        if found := STORED.search(text):
            stored[label] = max(stored.get(label, 0), int(found[1]) + int(found[2]))
    return stored


# ---------------------------------------------------------------- cases


@dataclass
class Plan:
    """A plan other than the binary one, and what a planner knows about it."""

    name: str
    label: str  # its label in the measurements
    new_indexes: int = 0  # arrangements it builds that the binary plan does not
    new_records: int = 0  # the records they hold
    freed: int = 0  # records of indexes that only the binary plan reads, which this plan lets go
    work: float | None = None  # rows its paths process (W times the raw intermediate)
    rho: float | None = 1.0  # path output rows per binary output row
    shared: bool = False  # another rule needs the binary plan's arrangement anyway
    # The hop orders that build this plan: as written ("text"), Materialize's
    # ("mz"), by estimated work ("cost"), or a heavy/light router. Plans that
    # no order builds are references.
    orders: tuple = ("text", "mz", "cost")
    lookup_rows: int | None = None  # peak rows held in its lookups (profiled)


@dataclass
class Case:
    """One rule, or one 2-atom join, of one workload on one dataset."""

    name: str
    mode: str  # "batch" or "inc"
    atoms: int
    changing: int  # atoms whose collections change
    held: int | None  # binary intermediate records held in memory (None: not probed)
    intermediate: int  # binary intermediate rows, before consolidation (summed over a chain's joins)
    base: str  # the binary plan's label in the measurements
    plans: list
    binary_indexes: int = 1  # arrangements the binary plan builds
    stored: int | None = None  # records the binary program stores at its peaks (probed)


def tri_batch(dataset, wedges, arcs):
    # Not recursive: the batcher holds every wedge at once.
    return Case(f"tri {dataset}", "batch", atoms=3, changing=0, held=wedges, intermediate=wedges,
                base=f"tri_base@{dataset}",
                plans=[Plan("lookup chain", f"tri_lookup@{dataset}", work=wedges + arcs)])


def tri_inc(dataset, wedges, out_wedges, arcs):
    # out_wedges are the pairs of arcs that leave the same node: the partial
    # tuples of the one path that has rows at the load. Every planner gets the
    # snapshot filter (Materialize always applies it); the unfiltered plans
    # are references.
    split = f"{dataset}-split"
    return Case(f"tri {split}", "inc", atoms=3, changing=3, held=wedges, intermediate=wedges,
                base=f"tri_inc@{split}",
                plans=[Plan("delta + filter", f"tri_inc_delta_sf@{split}", work=out_wedges + arcs),
                       Plan("delta, no filter", f"tri_inc_delta@{split}",
                            work=wedges + out_wedges + 3 * arcs, orders=()),
                       Plan("delta, Old first, no filter", f"tri_inc_delta_of@{split}",
                            work=out_wedges + 3 * arcs, orders=())])


def cspa_r6(dataset, intermediate, held, ma):
    return Case(f"CSPA R6 {dataset}", "batch", atoms=3, changing=3, held=held, intermediate=intermediate,
                base=f"cspa_base@{dataset}",
                plans=[Plan("delta, MA symmetric", f"cspa_delta_sym@{dataset}", work=2 * intermediate, rho=1.03),
                       Plan("delta, new MA index", f"cspa_delta@{dataset}", new_indexes=1, new_records=ma,
                            work=2 * intermediate, rho=1.03)])


def cspa_r6_probed(results, dataset, ma):
    """CSPA rule 6 on a dataset whose intermediate a probe build measured."""
    name = f"probe_cspa_base_{dataset}.txt"
    path = results / name
    if not path.exists():
        return []
    raw = re.search(r"VF\*MA intermediate \(raw\)=(\d+)", path.read_text(encoding="utf-8"))
    held = probe_peak(results, name, r"\u03c0\[x,w\]")
    return [cspa_r6(dataset, int(raw[1]), held, ma)] if raw and held else []


DYCK_INTERMEDIATE = 185_876 + 420_782


def build_cases(results):
    return [
        # Batch, 2-atom joins with a static or complete partner: lookups.
        Case("TC G5K", "batch", atoms=2, changing=1, held=0, intermediate=0, base="tc_base",
             plans=[Plan("lookup", "tc_lookup")]),
        # me joins pmuoe and cfg_edge after the fixpoint; pm joins the
        # recursive pmuoe and the static cfg_edge.
        Case("polonius borrow (me, pm)", "batch", atoms=2, changing=1, held=0, intermediate=0,
             base="polonius_base@borrow",
             plans=[Plan("lookups in both", "polonius_lboth@borrow"),
                    Plan("lookup in me only", "polonius_lme@borrow", rho=None, orders=()),
                    Plan("lookup in pm only", "polonius_lpm@borrow", rho=None, orders=())]),
        # Batch, 3-atom rules.
        tri_batch("G40K", 63_901_279, 1_598_663),
        tri_batch("roadNet", 17_523_394, 5_533_214),
        tri_batch("BerkStan", 222_498_869, 7_600_595),
        # The intermediate keeps no trace: its next join is with a static atom.
        Case("SG G5K", "batch", atoms=3, changing=1, held=0, intermediate=123_862_090, base="sg_base@G5K",
             plans=[Plan("lookup chain", "sg_lookup@G5K", work=123_862_090, rho=542.5 / 231.4)]),
        # Rule 5 still needs the arrangement of Dyck by c0.
        Case("dyck kernel (R3+R4)", "batch", atoms=3, changing=1, held=0, intermediate=DYCK_INTERMEDIATE,
             base="dyck_base@kernel", binary_indexes=2,
             plans=[Plan("lookup chain", "dyck_lookup@kernel", work=DYCK_INTERMEDIATE,
                         rho=(72_155 + 262_191) / (71_162 + 262_191), shared=True)]),
        Case("andersen load", "batch", atoms=3, changing=2, held=1_346_718, intermediate=1_346_718,
             base="andersen_base@medium",
             plans=[Plan("delta", "andersen_dload@medium", new_indexes=1, new_records=2_654_871)]),
        Case("andersen store", "batch", atoms=3, changing=2, held=857_226, intermediate=857_226,
             base="andersen_base@medium",
             plans=[Plan("delta", "andersen_dstore@medium", new_indexes=1, new_records=10_500_000)]),
        cspa_r6("httpd", 131_124_337, 124_100_000, 88_900_000),
        *cspa_r6_probed(results, "postgresql", 89_475_479),
        *cspa_r6_probed(results, "linux", 13_777_625),
        Case("GALEN R3", "batch", atoms=3, changing=2, held=5_900_000, intermediate=5_900_000, base="galen_base",
             plans=[Plan("delta", "galen_d3", new_indexes=1, new_records=96_336,
                         work=375.0 * M + 5.9 * M, rho=None)]),
        Case("GALEN R4", "batch", atoms=3, changing=2, held=28_400_000, intermediate=28_400_000, base="galen_base",
             plans=[Plan("delta, C first", "galen_d4", new_indexes=1, new_records=230_161,
                         work=2929.1 * M + 28.4 * M, rho=None, orders=("text",)),
                    Plan("delta, P first", "galen_d4p", new_indexes=1, new_records=230_161,
                         work=394.1 * M + 28.4 * M, rho=None, orders=("mz", "cost")),
                    Plan("delta + router", "galen_d4s", new_indexes=3, new_records=3 * 230_161,
                         work=43.8 * M + 28.4 * M, rho=None, orders=("router",))]),
        Case("GALEN R6", "batch", atoms=3, changing=2, held=19_800_000, intermediate=19_800_000, base="galen_base",
             plans=[Plan("delta", "galen_d6", new_indexes=1, new_records=385,
                         work=25.8 * M + 19.8 * M, rho=None)]),
        # Incremental: the inputs never close, so every intermediate keeps a trace.
        tri_inc("G40K", 63_901_279, 65_461_297, 1_598_663),
        tri_inc("roadNet", 17_523_394, 17_523_394, 5_533_214),
        Case("SG G5K-split", "inc", atoms=3, changing=3,
             held=probe_peak(results, "probe_sg_inc.txt", r"\u03c0\[x,b\]"), intermediate=123_862_090,
             base="sg_inc@G5K-split",
             plans=[Plan("delta", "sg_inc_delta@G5K-split", new_indexes=1, new_records=24_730_729,
                         work=123_862_090 + 2 * 25_046, rho=542.5 / 231.4)]),
        # At the load, paths 1 and 3 are driven by One (7.5M) and Zero (2.0M).
        # The snapshot filter drops path 1's rows (its first hop is Old); the
        # loop filter also drops path 3's, which probe Dyck, a loop variable
        # that is empty at the loop's first time (0,0).
        Case("dyck kernel-split", "inc", atoms=3, changing=3, held=DYCK_INTERMEDIATE,
             intermediate=DYCK_INTERMEDIATE, base="dyck_inc@kernel-split", binary_indexes=2,
             plans=[Plan("delta + loop filter", "dyck_inc_delta_lf@kernel-split", work=DYCK_INTERMEDIATE),
                    Plan("delta + snapshot filter", "dyck_inc_delta_sf@kernel-split",
                         work=DYCK_INTERMEDIATE + 1_981_258 + 7_502_955, orders=()),
                    Plan("delta, no filter", "dyck_inc_delta@kernel-split",
                         work=DYCK_INTERMEDIATE + 2 * (1_981_258 + 7_502_955), orders=())]),
        # Measured without the filters. At the load the loop filter would drop
        # the rows of the paths driven by static atoms (SG: 25K arcs each; CSPA
        # R4: Dereference, 1.15M each, in a 38 s run); CSPA R6's paths are all
        # driven by loop variables, which have no rows at (0,0). W and rho are
        # known for R6 only.
        Case("CSPA httpd-split (R4, R6)", "inc", atoms=3, changing=3, held=141_000_000 + 120_600_000,
             intermediate=131_124_337, base="cspa_inc",
             plans=[Plan("delta in both, symmetric", "cspa_inc_r45", work=2 * 131_124_337, rho=1.03),
                    Plan("delta in R4 only", "cspa_inc_r4", rho=None, orders=()),
                    Plan("delta in R6 only", "cspa_inc_r5", rho=None, orders=())]),
    ]


def loop_lookup(program, dataset, plan="lookup"):
    """A recursive 2-atom rule with a static partner (patches/loop_lookup.py)."""
    return Case(f"{program} {dataset}", "batch", atoms=2, changing=1, held=0, intermediate=0,
                base=f"{program}_base@{dataset}", plans=[Plan(plan, f"{program}_lookup@{dataset}")])


# Polonius rule sb's two intermediates, before consolidation.
SB_RAW = 5_522_983 + 4_820_963
# Doop's AssignCast rule: its three intermediates, before consolidation.
AC_RAW = 8_102 + 147_286 + 11_678_439


def heldout_cases():
    """The held-out set (REPORT.md section 11). The inputs come from the count
    and probe builds (*_pc, *_p) and were recorded before the held-out runs."""
    return [
        loop_lookup("reach", "livejournal"),
        loop_lookup("cc", "livejournal"),
        loop_lookup("sssp", "livejournal"),
        loop_lookup("bipartite", "mind", plan="lookups"),
        loop_lookup("csda", "httpd"),
        Case("polonius borrow (sa)", "batch", atoms=2, changing=1, held=0, intermediate=0,
             base="polonius_base@borrow", plans=[Plan("lookup", "polonius_lsa@borrow")]),
        # Every join has a static partner, so DD drops the three traces once
        # the loads end: the binary plan holds nothing. The chain hops in the
        # binary plan's order, so its partial tuples are the intermediates.
        Case("polonius borrow (sb)", "batch", atoms=4, changing=1, held=0, intermediate=SB_RAW,
             base="polonius_base@borrow", binary_indexes=3,
             plans=[Plan("lookup chain", "polonius_lsb@borrow", work=SB_RAW, rho=4_603_590 / 4_600_309,
                         lookup_rows=0)]),
        # The binary plan holds I3; I1 and I2 hold under 61K records each. Both
        # plans add AssignCast by from. Path dReachable in the written order
        # hops as the binary plan does, so its partial tuples are AC_RAW; it
        # stands in for Materialize's order, which has the same path and the
        # same new index. The cost order probes VarPointsTo second, which
        # frees the indexes that only I2 and I3 read: VarPointsTo by
        # (heap,var), HeapAllocation_Type by heaptype, SupertypeOf by type.
        Case("doop batik (AC)", "batch", atoms=5, changing=2, held=11_678_439, intermediate=AC_RAW,
             base="doop_base@batik", binary_indexes=3,
             plans=[Plan("delta, written order", "doop_dac@batik", new_indexes=1, new_records=48_230,
                         work=AC_RAW + 8_144_051 + 8_052_979 + 717_022, rho=713_407 / 721_933,
                         orders=("text", "mz"), lookup_rows=22_505),
                    Plan("delta, cost order", "doop_dacv@batik", new_indexes=1, new_records=48_230,
                         freed=37_237_353 + 98_382 + 61_177, work=8_102 + 8_144_051 + 8_057_321 + 714_200,
                         rho=713_008 / 721_933, orders=("cost",), lookup_rows=48_686)]),
        # The binary plan holds I2; I1 holds at most its 12K rows. The plan adds
        # StoreInstanceField by from and by base.
        Case("doop batik (IFPT)", "batch", atoms=4, changing=3, held=950_487, intermediate=12_148 + 1_009_106,
             base="doop_base@batik", binary_indexes=2,
             plans=[Plan("delta", "doop_difpt@batik", new_indexes=2, new_records=2 * 57_236,
                         work=12_148 + 1_010_457 + 984_559 + 137_684 + 137_671,
                         rho=(10_808_935 + 6_309_411) / 16_989_747, lookup_rows=24_587)]),
    ]


# ---------------------------------------------------------------- heuristics
# Each heuristic maps a case to the plan it picks, or to None to keep the binary plan.


def plans_in_order(case, order):
    return [plan for plan in case.plans if order in plan.orders]


def saving(case, plan, lookup_row_weight=0.0):
    """Records the plan saves: the held intermediate and the indexes it lets
    go, less its new indexes and, weighted, the rows held in its lookups."""
    return ((case.held or 0) + plan.freed - plan.new_records
            - lookup_row_weight * (plan.lookup_rows or 0))


def work_ratio(case, plan):
    """W: the plan's path work over the raw intermediate (None if unknown).
    Path work counts the partial tuples after every hop but the last, plus the
    input rows that drive paths. Rows of loop variables that drive paths are
    left out, as the binary plan joins each of them too. The raw intermediate
    sums the binary plan's intermediates before consolidation."""
    return None if plan.work is None or not case.intermediate else plan.work / case.intermediate


def H0(case):
    """No plan anywhere, so the binary plan everywhere: FlowLog today."""


def H1(case):
    """Materialize's default: for 3+ atoms, a delta join if it needs no new arrangement."""
    if case.atoms < 3:
        return None
    fits = [plan for plan in plans_in_order(case, "mz") if plan.new_indexes == 0]
    return fits[0] if fits else None


def H1e(case):
    """Materialize's eager mode: no more new arrangements than the binary plan builds."""
    if case.atoms < 3:
        return None
    fits = [plan for plan in plans_in_order(case, "mz") if plan.new_indexes <= case.binary_indexes]
    return min(fits, key=lambda plan: plan.new_indexes) if fits else None


def H2(case):
    """Structural: in batch mode, lookups wherever at most one atom changes; otherwise H1."""
    if case.mode == "batch" and (case.atoms == 2 or case.changing <= 1):
        return plans_in_order(case, "text")[0]
    return H1(case)


def H3(case, order="text", guards=False, lookup_row_weight=0.0, margin=0.0):
    """Size: the plan that saves the most records, if any saves some."""
    if case.atoms == 2:
        plan = case.plans[0]
        return None if plan.shared else plan
    fits = []
    for plan in plans_in_order(case, order):
        if plan.shared or saving(case, plan, lookup_row_weight) <= 0:
            continue
        if saving(case, plan) < margin * (case.stored or 0):
            continue
        if guards:
            work = work_ratio(case, plan)
            if (plan.rho is not None and plan.rho > RHO_MAX) or (work is not None and work > WORK_MAX):
                continue
        fits.append(plan)
    return max(fits, key=lambda plan: saving(case, plan, lookup_row_weight)) if fits else None


def H4(case):
    """H3 with the CPU guards (rho, path work), each path's hops ordered by estimated work."""
    return H3(case, "cost", guards=True)


def H5(case):
    """H4, or else a heavy/light router when a skewed static atom defeats every fixed order."""
    return H4(case) or H3(case, "router", guards=True)


def H6(case, margin=0.0):
    """H5, also charging each row the plan holds in its lookups as LOOKUP_ROW_WEIGHT records."""
    return (H3(case, "cost", guards=True, lookup_row_weight=LOOKUP_ROW_WEIGHT, margin=margin)
            or H3(case, "router", guards=True, lookup_row_weight=LOOKUP_ROW_WEIGHT, margin=margin))


def H7(case):
    """H6 with a margin, chosen after the held-out runs: a plan of 3+ atoms
    must save MARGIN of the records the binary program stores, not counting
    the rows its lookups hold (records in both)."""
    return H6(case, margin=MARGIN)


HEURISTICS = [
    ("H0 binary (FlowLog today)", H0),
    ("H1 Materialize default", H1),
    ("H1e Materialize eager", H1e),
    ("H2 structural", H2),
    ("H3 size", H3),
    ("H4 size + CPU guards", H4),
    ("H5 = H4 + router", H5),
    ("H6 = H5 + lookup-row charge", H6),
    ("H7 = H6 + margin (post hoc)", H7),
]


# ---------------------------------------------------------------- scoring


def ratios(case, plan, measured):
    """The plan's wall time and peak RSS over the binary plan's (plan None is
    the binary plan), or None if either is unmeasured."""
    base = measured.get(case.base)
    this = measured.get(plan.label) if plan else base
    if not base or not this:
        return None
    return this.wall / base.wall, this.rss / base.rss


def percent(ratio):
    return f"{(ratio - 1) * 100:+.0f}%"


def stored_share(case, plan):
    """What H7 tests: the records the plan saves over the records the binary
    program stores. 2-atom lookups save CPU, not records."""
    if case.atoms == 2:
        return "-"
    if not case.stored:
        return "n/m"
    return f"{saving(case, plan) / case.stored:+.1%}"


def geomean(values):
    return math.exp(sum(map(math.log, values)) / len(values))


def print_cases(cases, measured):
    print("## Cases (each plan's wall time and peak RSS relative to the binary plan; incremental: commit too)\n")
    print("| Case | Mode | Held I | Plan | New index | Freed | Lookup rows | Saved / stored | Work / I | rho | Time "
          "| RSS | Commit | Runs |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for case in cases:
        base = measured[case.base]
        held = "-" if case.atoms == 2 else "n/m" if case.held is None else f"{case.held / M:,.1f}M"
        for plan in case.plans:
            this = measured.get(plan.label)
            work = work_ratio(case, plan)
            ratio = ratios(case, plan, measured) if this else None
            commit = runs = ""
            if this:
                if this.commit is not None and base.commit is not None:
                    commit = percent(this.commit / base.commit)
                base_runs = base.runs if base.runs is not None else base.section
                runs = this.section or f"{this.runs}/{base_runs}"
            cells = [
                case.name,
                case.mode,
                held,
                plan.name,
                f"{plan.new_records / M:,.2f}M",
                f"{plan.freed / M:,.1f}M" if plan.freed else "-",
                "n/m" if plan.lookup_rows is None else f"{plan.lookup_rows / M:,.1f}M",
                stored_share(case, plan),
                "n/m" if work is None else f"{work:.1f}",
                "n/m" if plan.rho is None else f"{plan.rho:.2f}",
                percent(ratio[0]) if ratio else "-",
                percent(ratio[1]) if ratio else "-",
                commit,
                runs,
            ]
            print("| " + " | ".join(cells) + " |")


def print_decisions(cases, measured):
    """Prints each heuristic's pick per case; returns the scores per heuristic
    and of the best measured plan per case."""
    print("\n## Decisions (plan picked per case; '-' = binary plan)\n")
    print("| Case | " + " | ".join(name.split()[0] for name, _ in HEURISTICS) + " | Best measured |")
    print("|---|" + "---|" * (len(HEURISTICS) + 1))
    scores = {name: [] for name, _ in HEURISTICS}
    best_scores = []
    for case in cases:
        picks = []
        for name, heuristic in HEURISTICS:
            plan = heuristic(case)
            ratio = ratios(case, plan, measured) if plan is None or plan.label in measured else None
            scores[name].append((case, plan, ratio))
            picks.append("-" if plan is None else plan.name if ratio else plan.name + " (n/m)")
        options = [(None, (1.0, 1.0))]
        options += [(plan, ratios(case, plan, measured)) for plan in case.plans if plan.label in measured]
        best, best_ratio = min(options, key=lambda option: option[1][0] * option[1][1])
        best_scores.append(best_ratio)
        print(f"| {case.name} | " + " | ".join(picks) + f" | {'-' if best is None else best.name} |")
    return scores, best_scores


def print_scorecard(scores, best_scores, title="Scorecard"):
    print(f"\n## {title} (geometric means over cases with a measured choice)\n")
    print("| Heuristic | Plans changed | Time | RSS | Time x RSS | Regressions > 5% | Worst time |")
    print("|---|---|---|---|---|---|---|")
    for name, _ in HEURISTICS:
        rows = [ratio for _, _, ratio in scores[name] if ratio]
        changed = sum(1 for _, plan, _ in scores[name] if plan is not None)
        regressions = [case.name for case, _, r in scores[name] if r and (r[0] > 1.05 or r[1] > 1.05)]
        listed = f"({', '.join(regressions)})" if regressions else ""
        print(f"| {name} | {changed} | {summary(rows)} | {len(regressions)} {listed} | "
              f"{max(r[0] for r in rows):.2f}x |")
    print(f"| Best measured per case | - | {summary(best_scores)} | - | {max(r[0] for r in best_scores):.2f}x |")
    missing = {case.name for picks in scores.values() for case, plan, ratio in picks if plan and not ratio}
    if missing:
        print("\nUnmeasured choices (scored as missing):", sorted(missing))


def print_picks(cases):
    """Each heuristic's pick per case, from the planner inputs alone."""
    print("| Case | " + " | ".join(name.split()[0] for name, _ in HEURISTICS) + " |")
    print("|---|" + "---|" * len(HEURISTICS))
    for case in cases:
        picks = [heuristic(case) for _, heuristic in HEURISTICS]
        print(f"| {case.name} | " + " | ".join("-" if plan is None else plan.name for plan in picks) + " |")


def summary(rows):
    """The Time, RSS and Time x RSS cells: geometric means of the ratios."""
    return " | ".join(percent(geomean(values)) for values in (
        [r[0] for r in rows], [r[1] for r in rows], [r[0] * r[1] for r in rows]))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=ROOT / "results",
                        help="directory with the runs and probe logs (default: results/)")
    parser.add_argument("--picks", action="store_true",
                        help="print each heuristic's picks from the planner inputs alone")
    args = parser.parse_args()

    if missing := [name for name in PROBE_LOGS if not (args.results / name).exists()]:
        print(f"heuristics.py: not in {args.results}: {', '.join(missing)}; "
              "the inputs they hold read n/m (see -h)", file=sys.stderr)

    tuning = build_cases(args.results)
    rows_held = lookup_rows(args.results)
    for case in tuning:
        for plan in case.plans:
            plan.lookup_rows = rows_held.get(plan.label)
    heldout = heldout_cases()
    stored = stored_records(args.results)
    for case in tuning + heldout:
        case.stored = stored.get(case.base)

    if args.picks:
        for title, cases in (("Tuning set", tuning), ("Held-out set", heldout)):
            print(f"## {title}: plan picked per case ('-' = binary plan)\n")
            print_picks(cases)
            print()
        return

    measured = load_measurements(args.results)
    heldout_measured = batch_medians(args.results, "heldout_runs.csv")
    sets = [("Tuning set (REPORT.md section 10)", tuning, measured),
            ("Held-out set (REPORT.md section 11)", heldout, heldout_measured)]
    both = {name: [] for name, _ in HEURISTICS}
    both_best = []
    scored = 0
    for title, cases, results in sets:
        cases = [case for case in cases if case.base in results]
        if not cases:
            continue
        print(f"# {title}\n")
        print_cases(cases, results)
        scores, best_scores = print_decisions(cases, results)
        print_scorecard(scores, best_scores)
        print()
        for name in both:
            both[name] += scores[name]
        both_best += best_scores
        scored += 1
    if scored > 1:
        print("# Both sets")
        print_scorecard(both, both_best, "Scorecard, both sets")


if __name__ == "__main__":
    main()
