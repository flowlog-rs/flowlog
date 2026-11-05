#!/usr/bin/env python3
"""Scores the study's planner policies on CTADL's VirtualAssign rules (CTADL.md).

usage: scripts/ctadl_heuristics.py [--results DIR] [--needed] [--sizes]

A case is one of the eight VirtualAssign rules r1-r8 (patches/ctadl_va_delta.py)
in the index phase of one workload: JADX binaryeye and wikipedia (build
ctadl_ji_dj), P-code wget and sqlite3 (ctadl_pi_dj). A rule joins A, B and C,
which all change in one loop, and its binary plan holds I = A |><| B. Its delta
plan in the text order adds one index, B by C's key; in the cost order it adds
two copies of C as well.

DIR (default results/) holds the inputs:

  ctadl_rules.csv  each rule's |A|, |B| and |C| at their peaks, and the
                   records its I holds at the program's peak (--sizes)
  ctadl_runs.csv   the runs (scripts/ctadl_run.py): the probe run p-W-base
                   gives the records the binary program stores, p-W-CONFIG the
                   rows a configuration holds in lookups, and t-W-CONFIG
                   (scripts/ctadl_bench.sh) the timings

The probe reports one peak of lookup rows for the whole program, so a set of
rules is charged the fewest rows of any probed configuration that switches all
of them. Path work W is not measured, so the CPU guards only test rho, which
is 1 for these plans.

A run measures a program's rules together, so a policy is scored by the
configuration it picks for the whole program (CONFIG = base, ORDER-all or
ORDER-rA+rB+...): its median exec (the time to "Dataflow executed") and peak
RSS over the binary plan's, ranked by exec x RSS. Wall time adds the outputs'
SQLite writes, which no plan changes. --needed lists the configurations the
policies pick that have no timed runs.

H8 is the policy CTADL.md recommends: H5's picks, kept for a family of
near-identical rules (here, a workload's eight rules) only if together they pass
H7's tests. The last section checks that on the study's 29 cases, each a family
of its own, H8 picks what H7 picks.

--sizes writes DIR/ctadl_rules.csv from the probe builds' code
($WORK/crates/ctadl_{ji,pi}_p) and their probe runs probe-{ji,pi}-WORKLOAD
(scripts/ctadl_run.py --probe), logged in DIR/ctadl.
"""

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import heuristics as H
from candidates import attach, classify, parse, probe
from paths import ROOT, WORK

# Each rule's intermediate I, as in patches/ctadl_va_delta.py.
RULES = {
    "r1": "t_5566722344065419961", "r2": "t_17647071896910041152",
    "r3": "t_12900665211573367572", "r4": "t_559396743761194383",
    "r5": "t_10859189629111168972", "r6": "t_17735140767449333032",
    "r7": "t_14374907034803111120", "r8": "t_9037523228127469135",
}
WORKLOADS = {"binaryeye": "ji", "wikipedia": "ji", "wget": "pi", "sqlite3": "pi"}  # and their front ends
ORDERS = {"delta, text order": "text", "delta, cost order": "cost"}


def H8(family, lookup_rows):
    """H5's picks for a family of near-identical rules, kept only if together they pass H7's tests:
    they save MARGIN of the records the program stores, and more than LOOKUP_ROW_WEIGHT records
    for each row their lookups hold, lookup_rows(picks), charged once. 2-atom lookups are kept,
    as in H3."""
    picks = {case.name: H.H5(case) for case in family}
    held = [(case, picks[case.name]) for case in family if picks[case.name] and case.atoms > 2]
    saved = sum(H.saving(case, plan) for case, plan in held)
    if held and (saved < H.MARGIN * (family[0].stored or 0)
                 or saved <= H.LOOKUP_ROW_WEIGHT * (lookup_rows(held) or 0)):
        picks.update((case.name, None) for case, _ in held)
    return picks


# A policy maps a family of cases, and their lookup rows, to {case: plan, or None for the binary plan}.
POLICIES = [(name, lambda family, _, policy=policy: {case.name: policy(case) for case in family})
            for name, policy in H.HEURISTICS]
POLICIES.append(("H8 = H5 + family margin (CTADL.md)", H8))


# ---------------------------------------------------------------- inputs


def read_csv(path):
    if not path.exists():
        sys.exit(f"ctadl_heuristics.py: no {path} (see -h)")
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def switched(config):
    """The order and the rules that a configuration switches to a delta join."""
    if config == "base":
        return None, set()
    order, rules = config.split("-", 1)
    return order, set(RULES) if rules == "all" else set(rules.split("+"))


def runs(results):
    """Per workload, its probe runs {config: row} and its timed runs {config: [row, ...]}."""
    probes, timed = defaultdict(dict), defaultdict(lambda: defaultdict(list))
    for row in read_csv(results / "ctadl_runs.csv"):
        kind, workload, config = (row["label"].split("-", 2) + ["", ""])[:3]
        if kind == "p" and row["lookup_rows"]:
            probes[workload][config] = row
        elif kind == "t" and row["exec_s"]:
            timed[workload][config].append(row)
    return probes, timed


def stored(row):
    return int(row["trace_records"]) + int(row["batcher_records"])


def lookup_bound(probes, order, rules):
    """The rows that `rules` hold in lookups in `order`, at most: the fewest that any probed
    configuration switching all of them holds."""
    return min((int(row["lookup_rows"]) for config, row in probes.items()
                if switched(config)[0] == order and rules <= switched(config)[1]), default=None)


def cases(workload, sizes, probes):
    """The workload's rules as cases, with their planner inputs."""
    out = []
    for rule in RULES:
        _, b, c, i = (int(sizes[workload, rule][column]) for column in "ABCI")
        plans = [H.Plan("delta, text order", f"t-{workload}-text-{rule}", new_indexes=1, new_records=b,
                      orders=("text", "mz"), lookup_rows=lookup_bound(probes, "text", {rule})),
                 H.Plan("delta, cost order", f"t-{workload}-cost-{rule}", new_indexes=3, new_records=b + 2 * c,
                      orders=("cost",), lookup_rows=lookup_bound(probes, "cost", {rule}))]
        out.append(H.Case(f"{workload} {rule}", "batch", atoms=3, changing=3, held=i, intermediate=i,
                        base=f"t-{workload}-base", plans=plans, stored=stored(probes["base"])))
    return out


def plan_in(case, order):
    return next(plan for plan in case.plans if ORDERS[plan.name] == order)


def family_rows(probes):
    """H8's lookup_rows on one workload: the bound for the picked rules, in their order."""
    def rows(held):
        (order,) = {ORDERS[plan.name] for _, plan in held}
        return lookup_bound(probes, order, {case.name.split()[1] for case, _ in held})
    return rows


def config(family, picks):
    """The configuration that runs a policy's picks."""
    chosen = [(case.name.split()[1], picks[case.name]) for case in family if picks[case.name]]
    if not chosen:
        return "base"
    orders = {ORDERS[plan.name] for _, plan in chosen}
    if len(orders) > 1:
        sys.exit(f"ctadl_heuristics.py: a policy mixes orders: {chosen}")
    rules = "all" if len(chosen) == len(RULES) else "+".join(rule for rule, _ in chosen)
    return f"{orders.pop()}-{rules}"


def medians(timed):
    """{config: medians of exec, RSS, wall and wall - exec; runs; exec's range} of a workload's timed runs."""
    out = {}
    for config_, rows in timed.items():
        values = {column: [float(row[column]) for row in rows] for column in ("exec_s", "rss_mb", "wall_s")}
        out[config_] = {column: statistics.median(v) for column, v in values.items()} | {
            "n": len(rows), "lo": min(values["exec_s"]), "hi": max(values["exec_s"]),
            "after": statistics.median(w - e for w, e in zip(values["wall_s"], values["exec_s"]))}
    return out


def rule_sizes(joins, intermediate):
    """A rule's |A|, |B| and |C| at their peaks, and the records its I holds at the program's peak."""
    first = next(join for join in joins if join["out"] == intermediate)
    second = next(join for join in joins if any(side["var"] == f"{intermediate}_arr" for side in join["sides"]))
    i = next(side for side in second["sides"] if side["var"] == f"{intermediate}_arr")
    c = next(side for side in second["sides"] if side is not i)
    return {"A": first["sides"][0]["peak"], "B": first["sides"][1]["peak"], "C": c["peak"], "I": i["at_peak"]}


def write_sizes(results):
    rows, joins = [], {}
    for workload, front in WORKLOADS.items():
        if front not in joins:
            joins[front] = classify(parse((WORK / "crates" / f"ctadl_{front}_p" / "src" / "main.rs").read_text()))
        log = results / "ctadl" / f"probe-{front}-{workload}-0.log"
        if not log.exists():
            sys.exit(f"ctadl_heuristics.py: no {log}; run: scripts/ctadl_run.py --label probe-{front}-{workload} "
                     f"--bin \"$BIN/ctadl_{front}_p\" --phase {workload}/index --probe")
        attach(joins[front], probe(log)[0])
        rows += [{"workload": workload, "rule": rule, **rule_sizes(joins[front], intermediate)}
                 for rule, intermediate in RULES.items()]
    with open(results / "ctadl_rules.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, ["workload", "rule", "A", "B", "C", "I"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {results / 'ctadl_rules.csv'}")


def study_check():
    """The study's cases, with the inputs heuristics.py gives them, and those where H8 and H7 differ."""
    results = ROOT / "results"
    tuning = H.build_cases(results)
    rows = H.lookup_rows(results)
    for case in tuning:
        for plan in case.plans:
            plan.lookup_rows = rows.get(plan.label)
    study = tuning + H.heldout_cases()
    stored_ = H.stored_records(results)
    for case in study:
        case.stored = stored_.get(case.base)
    def name(plan):
        return plan.name if plan else None

    differ = [case.name for case in study
              if name(H8([case], lambda held: held[0][1].lookup_rows)[case.name]) != name(H.H7(case))]
    return study, differ


# ---------------------------------------------------------------- report


def scorecard_row(name, picks, measured):
    rows, worse, missing = [], [], []
    for workload, config_ in picks.items():
        base, this = measured[workload].get("base"), measured[workload].get(config_)
        if not base or not this:
            missing.append(f"{workload}:{config_}")
            continue
        ratio = tuple(this[column] / base[column] for column in ("exec_s", "rss_mb", "wall_s"))
        rows.append(ratio)
        if ratio[0] > 1.05 or ratio[1] > 1.05:
            worse.append(workload)
    cells = ["-"] * 4
    if rows:
        exec_, rss, wall = zip(*rows)
        cells = [H.percent(H.geomean(values)) for values in (exec_, rss, [e * r for e, r in zip(exec_, rss)], wall)]
    changed = sum(config_ != "base" for config_ in picks.values())
    worse_cell = f"{len(worse)} ({', '.join(worse)})" if worse else "0"
    return f"| {name} | {changed} | {' | '.join(cells)} | {worse_cell} | {' '.join(missing)} |"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=ROOT / "results", metavar="DIR",
                        help="directory with ctadl_runs.csv and ctadl_rules.csv (default: results/)")
    parser.add_argument("--needed", action="store_true", help="list the picked configurations not yet timed")
    parser.add_argument("--sizes", action="store_true", help="write ctadl_rules.csv from the probe runs")
    args = parser.parse_args()
    if args.sizes:
        write_sizes(args.results)
        return

    sizes = {(row["workload"], row["rule"]): row for row in read_csv(args.results / "ctadl_rules.csv")}
    probes, timed = runs(args.results)
    families = {workload: cases(workload, sizes, probes[workload]) for workload in WORKLOADS}
    picks = {name: {workload: config(family, policy(family, family_rows(probes[workload])))
                    for workload, family in families.items()}
             for name, policy in POLICIES}
    measured = {workload: medians(timed[workload]) for workload in WORKLOADS}
    if args.needed:
        for workload, config_ in sorted({(w, c) for chosen in picks.values() for w, c in chosen.items()}):
            if config_ not in measured[workload]:
                print(workload, config_)
        return

    print("## Planner inputs per rule (records; saving = held I - new records)\n")
    print("| Case | A | B | C | Held I | Stored | Saving, text / cost | Share of stored "
          "| Lookup rows, text / cost |")
    print("|---|---|---|---|---|---|---|---|---|")
    for workload, family in families.items():
        for case in family:
            text, cost = (H.saving(case, plan) for plan in case.plans)
            a, b, c = (int(sizes[workload, case.name.split()[1]][column]) for column in "ABC")
            rows = " / ".join("n/m" if plan.lookup_rows is None else f"{plan.lookup_rows:,}" for plan in case.plans)
            print(f"| {case.name} | {a:,} | {b:,} | {c:,} | {case.held:,} | {case.stored:,} | "
                  f"{text:,.0f} / {cost:,.0f} | {cost / case.stored:.1%} | {rows} |")

    print("\n## Picks: the configuration each policy runs (rules switched to a delta join, and their order)\n")
    print("| Policy | " + " | ".join(WORKLOADS) + " |")
    print("|---|" + "---|" * len(WORKLOADS))
    for name, chosen in picks.items():
        print(f"| {name} | " + " | ".join(chosen.values()) + " |")

    print("\n## Probed configurations (one probe run each; savings are shares of the records the binary "
          "plan stores, traces + batchers)\n")
    print("| Workload | Config | Stored | Predicted saving | Probed saving: total (traces / batchers) "
          "| Lookup rows | RSS (probe run) |")
    print("|---|---|---|---|---|---|---|")
    for workload, family in families.items():
        base = probes[workload]["base"]
        total = stored(base)
        for config_, row in sorted(probes[workload].items(), key=lambda item: (item[0] != "base", item[0])):
            order, rules = switched(config_)
            predicted = sum(H.saving(case, plan_in(case, order)) for case in family if case.name.split()[1] in rules)
            traces = int(base["trace_records"]) - int(row["trace_records"])
            batchers = int(base["batcher_records"]) - int(row["batcher_records"])
            rss = "-" if config_ == "base" else H.percent(float(row["rss_mb"]) / float(base["rss_mb"]))
            print(f"| {workload} | {config_} | {stored(row):,} | {predicted / total:.1%} | "
                  f"{(traces + batchers) / total:.1%} ({traces / total:.1%} / {batchers / total:.1%}) | "
                  f"{int(row['lookup_rows']):,} | {rss} |")

    print("\n## Measured configurations (medians, and their ratios to the binary plan's; exec = time to "
          "'Dataflow executed', after = wall - exec, writing the outputs to SQLite)\n")
    print("| Workload | Config | Runs | Exec s (range) | RSS MB | Exec x RSS | Wall s | After s |")
    print("|---|---|---|---|---|---|---|---|")
    for workload, by_config in measured.items():
        base = by_config["base"]
        for config_, m in sorted(by_config.items(), key=lambda item: (item[0] != "base", item[0])):
            exec_, rss, wall = (m[column] / base[column] for column in ("exec_s", "rss_mb", "wall_s"))
            print(f"| {workload} | {config_} | {m['n']} | {m['exec_s']:.2f} ({H.percent(exec_)}; "
                  f"{m['lo']:.2f}-{m['hi']:.2f}) | {m['rss_mb']:,.0f} ({H.percent(rss)}) | {H.percent(exec_ * rss)} | "
                  f"{m['wall_s']:.1f} ({H.percent(wall)}) | {m['after']:.1f} |")

    print("\n## Scorecard (geometric means over the workloads, ratios to the binary plan; exec x RSS ranks, "
          "as wall adds the same SQLite writes)\n")
    print("| Policy | Workloads changed | Exec | RSS | Exec x RSS | Wall | Worse by > 5% (exec or RSS) | Missing |")
    print("|---|---|---|---|---|---|---|---|")
    for name, chosen in picks.items():
        print(scorecard_row(name, chosen, measured))
    best = {workload: min(by_config, key=lambda c: by_config[c]["exec_s"] * by_config[c]["rss_mb"])
            for workload, by_config in measured.items() if by_config}
    print(scorecard_row("Best in hindsight (least exec x RSS): " + ", ".join(best.values()), best, measured))

    study, differ = study_check()
    print("\n## H8 on the study's cases (REPORT.md sections 10 and 11)\n")
    print(f"Each of the {len(study)} cases is a family of its own. H8 picks what H7 picks on "
          f"{len(study) - len(differ)} of them" + (f"; not on {', '.join(differ)}." if differ else "."))


if __name__ == "__main__":
    main()
