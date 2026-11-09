#!/usr/bin/env python3
"""Lists a generated FlowLog program's joins by what their binary plan holds (CTADL.md).

usage: scripts/candidates.py MAIN_RS [--probe LOG] [--top N]

MAIN_RS is a build's src/main.rs ($WORK/crates/NAME/src/main.rs). Each `let`
statement is placed in the loop it sits in, if any. Inside a loop, the loop's
own collections (recursive_*) change, and so does every variable they feed;
collections entered from outside (in_*) are static. A join's side that
arranges two or more atoms is an intermediate, and gets the kind of what the
binary plan holds for it (REPORT.md section 9), most held first:

  delta        in a loop, a changing intermediate meets a changing side: its
               trace is held, and a delta join could drop it
  held-static  in a loop, a static intermediate meets a changing side: its
               trace is held
  chain        outside loops, in a non-recursive rule: it waits in a batcher
               in full, and a lookup chain could drop it
  free         in a loop, a changing intermediate meets a static side: its
               trace is let go once the static side is complete
  static       the rest

An intermediate that feeds several joins takes its most held kind, and so
does a join of two. A join of two single atoms is dd if both change (DD's join
already is a delta join), lookup if one does, and static otherwise.

LOG is a probe run of the same build (scripts/ctadl_run.py --probe). With it,
the script sums each kind's records, at their own peaks and at the program's
(when its traces held the most), and lists the N intermediates that held the
most at the program's peak. Shares are of the records the program stores at
its peaks, traces plus batchers, as in heuristics.py.
"""

import argparse
import bisect
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

IDENT = re.compile(r"\b(?:t_\d+(?:_arr)?|in_t_\d+_arr|in_rel_\d+_\w+?|rel_\d+_\w+?|recursive_rel_\d+_\w+?|next_\d+)\b")
OPERATOR = re.compile(r"::flowlog_runtime::operators::(\w+)\(")
PROBE_OP = re.compile(r"\[probe\] op=(\d+)\s+peak=(\d+)\s+at_total_peak=(\d+)\s+(.*)$")
PROBE_STORED = re.compile(r"peak_total_trace_records=(\d+) peak_batcher_records=(\d+)")
KINDS = ("delta", "held-static", "chain", "free", "static")  # most held first


def skip_string(code, at):
    """The position after the string literal that starts at `at`."""
    at += 1
    while code[at] != '"':
        at += 2 if code[at] == "\\" else 1
    return at + 1


def matching(code, at):
    """The position of the bracket that closes the one at `at`."""
    depth = 0
    while True:
        c = code[at]
        if c == '"':
            at = skip_string(code, at)
            continue
        if c == "'" and code[at + 2] == "'":  # a char literal such as '"'
            at += 3
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                return at
        at += 1


def statement_end(code, start):
    """The position of the `;` that ends the statement starting at `start`."""
    depth, at = 0, start
    while True:
        c = code[at]
        if c == '"':
            at = skip_string(code, at)
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == ";" and depth == 0:
            return at
        at += 1


def strings(text):
    """The string literals in `text`."""
    out, at = [], 0
    while (at := text.find('"', at)) >= 0:
        end = skip_string(text, at)
        out.append(text[at + 1 : end - 1].replace('\\"', '"'))
        at = end
    return out


def loop_spans(code):
    """Each loop's body: the positions of the braces of the closure after "Iterative"."""
    return [(m.end() - 1, matching(code, m.end() - 1)) for m in re.finditer(r'"Iterative",\s*\|inner\|\s*\{', code)]


def parse(code):
    """The `let` statements: the names each binds, its line, its loop (an index, or None), its
    operator, its label (its first string literal) and the variables it reads."""
    loops = loop_spans(code)
    newlines = [m.start() for m in re.finditer("\n", code)]
    statements = []
    for m in re.finditer(r"\blet (\(?[\w, \n]+?\)?) = ", code):
        start = m.start()
        body = code[m.end() : statement_end(code, start)]
        if ".scoped::<" in body:
            continue  # a loop: its statements are parsed one by one
        names = [name.strip() for name in m[1].strip("()").split(",") if name.strip()]
        operator = OPERATOR.search(body)
        labels = strings(body)
        refs = []
        for ref in IDENT.findall(re.sub(r'"(?:[^"\\]|\\.)*"', '""', body)):
            if ref not in refs and ref not in names:
                refs.append(ref)
        statements.append({
            "names": names,
            "line": bisect.bisect_left(newlines, start) + 1,
            "loop": next((i for i, (open_at, close_at) in enumerate(loops) if open_at < start < close_at), None),
            "op": operator[1] if operator else None,
            "label": labels[0] if labels else "",
            "refs": refs,
        })
    return statements


def classify(statements):
    """The joins: the variable each binds, its line, loop, atoms and kind, and its two sides
    (variable, label, atoms, whether it changes, and its kind if it is an intermediate)."""
    by_name, changing = {}, set()
    for s in statements:
        for name in s["names"]:
            by_name[name] = s
        if s["loop"] is not None and (any(name.startswith("recursive_") for name in s["names"])
                                      or any(ref.startswith("recursive_") or ref in changing for ref in s["refs"])):
            changing.update(s["names"])
    joins = []
    for s in statements:
        if s["op"] != "flowlog_join" or len(s["refs"]) < 2:
            continue
        sides = []
        for var in s["refs"][:2]:
            # An entered arrangement (in_t_...) has the label of the statement outside the loop.
            source = (by_name.get(var[3:]) if var.startswith("in_t_") else None) or by_name.get(var)
            label = source["label"] if source else ""
            atoms = label.count("⋈") + label.count("⋉") + 1 if label.startswith("Arrange: ") else 1
            sides.append({"var": var, "label": label, "atoms": atoms, "changing": var in changing})
        for side, other in zip(sides, sides[::-1]):
            if side["atoms"] < 2:
                side["kind"] = None
            elif s["loop"] is None:
                side["kind"] = "chain"
            elif side["changing"]:
                side["kind"] = "delta" if other["changing"] else "free"
            else:
                side["kind"] = "held-static" if other["changing"] else "static"
        if kinds := {side["kind"] for side in sides} - {None}:
            kind = next(k for k in KINDS if k in kinds)
        elif s["loop"] is not None and any(side["changing"] for side in sides):
            kind = "dd" if all(side["changing"] for side in sides) else "lookup"
        else:
            kind = "static"
        joins.append({"out": s["names"][0], "line": s["line"], "loop": s["loop"], "kind": kind,
                      "atoms": sides[0]["atoms"] + sides[1]["atoms"], "sides": sides})
    return joins


def probe(path):
    """A probe run's records per arrangement label, at their own peaks and at the program's (summed
    over the label's operators), and the program's peaks: (trace records, batcher records)."""
    records, seen, stored = defaultdict(lambda: [0, 0]), set(), None
    for line in Path(path).read_text().splitlines():
        if m := PROBE_OP.match(line):
            if m[1] not in seen:
                seen.add(m[1])
                records[m[4].strip()][0] += int(m[2])
                records[m[4].strip()][1] += int(m[3])
        elif m := PROBE_STORED.search(line):
            stored = int(m[1]), int(m[2])
    if stored is None:
        sys.exit(f"candidates.py: {path} reports no peaks; is it a probe build's run with --probe?")
    return records, stored


def attach(joins, records):
    """Sets each side's records at its own peak (peak) and at the program's (at_peak)."""
    for join in joins:
        for side in join["sides"]:
            side["peak"], side["at_peak"] = records.get(side["label"], (0, 0))


def intermediates(joins):
    """Each intermediate arrangement once, as (join, side) for the join that gives its most held kind."""
    found = {}
    for join in joins:
        for side in join["sides"]:
            known = found.get(side["label"])
            if side["kind"] and (not known or KINDS.index(side["kind"]) < KINDS.index(known[1]["kind"])):
                found[side["label"]] = join, side
    return list(found.values())


def short(label, width):
    """An arrangement label without its prefix, hash and value columns, cut to `width`."""
    text = re.sub(r"^Arrange: |, value:\(.*\)$", "", label)
    text = re.sub(r" \[0x[0-9a-f]+\],", "", text)
    return text if len(text) <= width else text[: width - 3] + "..."


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("main_rs", type=Path, metavar="MAIN_RS")
    parser.add_argument("--probe", type=Path, metavar="LOG", help="a probe run of the same build")
    parser.add_argument("--top", type=int, default=10, metavar="N", help="intermediates to list (default 10)")
    args = parser.parse_args()

    code = args.main_rs.read_text()
    statements = parse(code)
    joins = classify(statements)
    kinds = Counter(join["kind"] for join in joins)
    print(f"{len(statements):,} statements, {len(loop_spans(code))} loops, {len(joins):,} joins: "
          + ", ".join(f"{kind} {n:,}" for kind, n in kinds.most_common()))
    if not args.probe:
        return
    records, stored = probe(args.probe)
    attach(joins, records)
    total = sum(stored)
    print(f"stored at the program's peaks: {total:,} records (traces {stored[0]:,}, batchers {stored[1]:,})")

    held = intermediates(joins)
    sums = defaultdict(lambda: [0, 0, 0])
    for _, side in held:
        sums[side["kind"]][0] += 1
        sums[side["kind"]][1] += side["peak"]
        sums[side["kind"]][2] += side["at_peak"]
    print("\nintermediates by kind: arrangements, records at their own peaks, and at the program's peak")
    for kind in (kind for kind in KINDS if kind in sums):
        n, peak, at_peak = sums[kind]
        print(f"  {kind:11} {n:5,} {peak:13,} ({peak / total:5.1%}) {at_peak:13,} ({at_peak / total:5.1%})")

    print(f"\nthe {args.top} intermediates that held the most at the program's peak (C changes, S is static)")
    print(f"{'kind':11} {'loop':>4} {'atoms':>5} {'line':>6} {'at peak':>11} {'share':>6} {'own peak':>11}"
          "  intermediate | other side")
    for join, side in sorted(held, key=lambda item: -item[1]["at_peak"])[: args.top]:
        other = join["sides"][1] if side is join["sides"][0] else join["sides"][0]
        loop = "-" if join["loop"] is None else join["loop"]
        print(f"{side['kind']:11} {loop:>4} {join['atoms']:>5} {join['line']:>6} {side['at_peak']:>11,} "
              f"{side['at_peak'] / total:>6.1%} {side['peak']:>11,}  "
              f"{'SC'[side['changing']]}:{short(side['label'], 100)} "
              f"| {'SC'[other['changing']]}:{short(other['label'], 60)}")


if __name__ == "__main__":
    main()
