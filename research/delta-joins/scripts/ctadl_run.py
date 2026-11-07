#!/usr/bin/env python3
"""Runs a CTADL binary on a staged phase; times it and checks its outputs.

usage: scripts/ctadl_run.py --label LABEL --bin BIN --phase WORKLOAD/PHASE [--reps N]
         [--workers N] [--check] [--probe] [--env NAME=VALUE]... [--csv CSV]

BIN reads $DATA/ctadl/WORKLOAD/PHASE/in (scripts/ctadl_stage.py) and writes
to $WORK/ctadl_out/LABEL. Each run appends a row to CSV: the time to
"Dataflow executed" (exec_s), the whole run (wall_s) and peak RSS (rss_mb,
GNU time's KB / 1024), and logs to $RESULTS/ctadl/LABEL-REP.log. --check
compares the first run's outputs with the staged ones, every table as a
multiset of rows. --probe runs a probe build (patches/probe.py) on every
arrangement and records its peaks: the records in traces and in batchers, and
the rows held in lookups.

example:
  scripts/ctadl_run.py --label p-wget-cost-all --bin "$BIN/ctadl_pi_dj" --phase wget/index \\
    --probe --check --env DJ_VA=all --env DJ_VA_ORDER=cost
"""

import argparse
import csv
import hashlib
import json
import multiprocessing
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from bench import pair
from paths import DATA, RESULTS, WORK

COLUMNS = ["label", "bin", "phase", "workers", "rep", "exec_s", "wall_s", "rss_mb", "env", "check",
           "trace_records", "batcher_records", "lookup_rows"]
PROBED = {"trace_records": r"peak_total_trace_records=(\d+)", "batcher_records": r"peak_batcher_records=(\d+)",
          "lookup_rows": r"peak_lookup_rows_held=(\d+)"}

# ---------------------------------------------------------------- --check
# Rows are hashed and summed, per table. CTADL numbers some values by `ord`,
# a symbol's interning order, which differs from run to run, so those numbers
# are first renamed by what they number, as ctadl-flowlog's
# flowlog-backend/compare_pcode.py and compare_jadx.py do: P-code's source
# locations, JADX's allocated objects and intent keys, a query's slice vertices.

MASK = (1 << 64) - 1
PCODE_ID_COLUMN = {"CSourceInfo_Address": 0, "CAddress_AbsoluteAddress": 0, "CAddress_Kind": 0,
                   "CAddress_Name": 0, "CAddress_FullyQualifiedName": 0, "CSourceInfo_Location": 2}
JADX_KEY = re.compile(r"\.intent\[([^\[\]]*(?:\[\])*)\]\.(\d+)")
JADX_NUMBER = re.compile(r"/(\d+)(?!\d)")
RENAMES = {}  # database -> (kind, its renames); set before the pool forks


def cells(row):
    return tuple("" if value is None else str(value) for value in row)


def pcode_renames(con):
    ids = {}
    for location, flag, id_ in map(cells, con.execute('SELECT * FROM "CSourceInfo_Location"')):
        if flag != "1" or ids.setdefault(id_, "@" + location) != "@" + location:
            sys.exit(f"ctadl_run.py: unexpected CSourceInfo_Location row {location, flag, id_}")
    return ids


def jadx_renames(con, tables):
    objects, users = {}, {}
    if "CisAlloc" in tables:
        for var, path, obj in map(cells, con.execute('SELECT * FROM "CisAlloc"')):
            numbered = re.fullmatch(r"(.*)/(\d+)", obj)
            if path == "" and numbered:
                new = f"{numbered[1]}/@{var}"
                if objects.setdefault(obj, new) != new:
                    sys.exit(f"ctadl_run.py: object {obj} allocated by two variables")
    if "CInsn_ModelAssign" in tables:
        for row in map(cells, con.execute('SELECT * FROM "CInsn_ModelAssign"')):
            for cell in row[1:]:
                for key in JADX_KEY.finditer(cell):
                    users.setdefault(key[2], set()).add(row[0])
    keys = {number: "{" + ",".join(sorted(insns)) + "}" for number, insns in users.items()}
    types = {}
    for obj, new in objects.items():
        type_, _, number = obj.rpartition("/")
        types.setdefault(number, []).append((type_, new))
    for candidates in types.values():
        candidates.sort(key=lambda candidate: len(candidate[0]), reverse=True)
    return types, keys


def jadx_rename(text, renames):
    types, keys = renames
    parts, done = [], 0
    for number in JADX_NUMBER.finditer(text):
        for type_, new in types.get(number[1], ()):
            start = number.start() - len(type_)
            if start >= done and text.startswith(type_, start):
                parts += (text[done:start], new)
                done = number.end()
                break
    text = "".join(parts) + text[done:]
    return JADX_KEY.sub(lambda key: f".intent[{key[1]}].{keys.get(key[2], '?' + key[2])}", text)


def query_renames(con, tables):
    """Slice vertex ids (`ord` of the vertex's fields) -> those fields, per slice."""
    renames = {}
    for table in tables:
        if table.endswith(".ReachableVertex"):
            ids = renames.setdefault(table.rpartition(".")[0], {})
            for id_, v, p, label, cs, _, ctx in map(cells, con.execute(f'SELECT * FROM "{table}"')):
                new = f"@{v}:{p}:{label}:{cs}:{ctx}"
                if ids.setdefault(id_, new) != new:
                    sys.exit(f"ctadl_run.py: vertex id {id_} names two vertices in {table}")
    return renames


def add(total, data):
    return (total + int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "little")) & MASK


def table_digest(job):
    db, table = job
    kind, renames = RENAMES.get(db, (None, None))
    slice_, _, name = table.rpartition(".")
    vertex_columns = {"ReachableVertex": 1, "ReachableEdge": 2}.get(name, 0) if kind == "query" else 0
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    count = total = 0
    for row in map(cells, con.execute(f'SELECT * FROM "{table}"')):
        if kind == "pcode" and table in PCODE_ID_COLUMN:
            column = PCODE_ID_COLUMN[table]
            row = row[:column] + (renames[row[column]],) + row[column + 1 :]
        elif kind == "jadx":
            row = tuple(jadx_rename(v, renames) if "/" in v or ".intent[" in v else v for v in row)
        elif vertex_columns:
            ids = renames.get(slice_, {})
            row = tuple(ids.get(v, "?" + v) for v in row[:vertex_columns]) + row[vertex_columns:]
        total = add(total, repr(row).encode())
        count += 1
    con.close()
    return f"{db.name}:{table}", [count, f"{total:016x}"]


def file_digest(path):
    count = total = 0
    with open(path, "rb") as file:
        for line in file:
            total = add(total, line)
            count += 1
    return [count, f"{total:016x}"]


def digest(out, kind):
    """{name: [rows, digest]} for every table of every SQLite file in OUT and for
    every other file, with KIND's (jadx, pcode or query) ids renamed."""
    jobs, result = [], {}
    RENAMES.clear()
    for path in sorted(p for p in out.rglob("*") if p.is_file()):
        with open(path, "rb") as file:
            if file.read(16) != b"SQLite format 3\x00":
                result[str(path.relative_to(out))] = file_digest(path)
                continue
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        tables = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if kind == "pcode" and "CSourceInfo_Location" in tables:
            RENAMES[path] = ("pcode", pcode_renames(con))
        elif kind == "jadx":
            RENAMES[path] = ("jadx", jadx_renames(con, tables))
        elif kind == "query":
            RENAMES[path] = ("query", query_renames(con, tables))
        con.close()
        jobs += [(path, table) for table in tables]
    with ProcessPoolExecutor(16, mp_context=multiprocessing.get_context("fork")) as pool:
        result.update(pool.map(table_digest, jobs))
    return dict(sorted(result.items()))


def reference(phase, kind):
    """The digest of the staged outputs, cached in PHASE/ref.digest.json."""
    cached = phase / "ref.digest.json"
    if not cached.exists():
        cached.write_text(json.dumps(digest(phase / "ref", kind), indent=0))
    return json.loads(cached.read_text())


def compare(ref, new):
    bad = [name for name in sorted(set(ref) | set(new)) if ref.get(name) != new.get(name)]
    if not bad:
        return f"equal ({len(ref)} tables, {sum(rows for rows, _ in ref.values())} rows)"
    shown = ", ".join(f"{name} {ref.get(name, ['-'])[0]}->{new.get(name, ['-'])[0]}" for name in bad[:6])
    return f"DIFFERENT {len(bad)}: {shown}"


# ---------------------------------------------------------------- runs


def run_once(args, phase, env, log):
    """Runs the binary once; returns its CSV cells but label, rep and check, and whether it failed."""
    out = WORK / "ctadl_out" / args.label
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    command = ["/usr/bin/time", "-f", "[time] wall=%e maxrss_kb=%M", str(args.bin.resolve()),
               "-F", str(phase / "in"), "-D", str(out), "-w", str(args.workers)]
    with open(log, "w") as file:
        done = subprocess.run(command, env=env, stdout=file, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                              check=False)
    text = log.read_text(errors="replace")
    executed = re.search(r"([\d.]+)(m?s):\tDataflow executed", text)
    usage = re.search(r"\[time\] wall=([\d.]+) maxrss_kb=(\d+)", text)
    row = {
        "bin": args.bin.name, "phase": args.phase, "workers": args.workers,
        "exec_s": f"{float(executed[1]) / (1000 if executed[2] == 'ms' else 1):.2f}" if executed else "",
        "wall_s": usage[1] if usage else "",
        "rss_mb": f"{int(usage[2]) / 1024:.0f}" if usage else "",
        "env": " ".join(f"{name}={value}" for name, value in args.env),
    }
    if args.probe:
        row |= {column: m[1] for column, pattern in PROBED.items() if (m := re.search(pattern, text))}
    return row, out, done.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True, help="name of this configuration in the CSV")
    parser.add_argument("--bin", required=True, type=Path, help="the binary to run")
    parser.add_argument("--phase", required=True, metavar="WORKLOAD/PHASE", help="the staged phase it runs")
    parser.add_argument("--reps", type=int, default=1, metavar="N", help="runs (default 1)")
    parser.add_argument("--workers", type=int, default=32, metavar="N",
                        help="worker threads (default 32, as CTADL runs)")
    parser.add_argument("--check", action="store_true", help="compare the first run's outputs with the staged ones")
    parser.add_argument("--probe", action="store_true", help="record a probe build's peaks")
    parser.add_argument("--env", type=pair, action="append", default=[], metavar="NAME=VALUE",
                        help="environment variable for the binary; repeatable")
    parser.add_argument("--csv", type=Path, default=RESULTS / "ctadl_runs.csv",
                        help="CSV to append to (default $RESULTS/ctadl_runs.csv)")
    args = parser.parse_args()

    phase = DATA / "ctadl" / args.phase
    if not (phase / "in").is_dir():
        sys.exit(f"ctadl_run.py: {phase} is not staged yet; run: scripts/ctadl_stage.py {phase.parent.name} FACTS")
    kind = "query" if phase.name == "query" else (phase.parent / "language").read_text().strip()
    env = {**os.environ, **dict(args.env)}
    if args.probe:
        env["FLOWLOG_PROBE"] = "1"
        env.setdefault("PROBE_TOP", "100000")  # every arrangement, for scripts/candidates.py
    logs = RESULTS / "ctadl"
    logs.mkdir(parents=True, exist_ok=True)
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    new = not args.csv.exists()
    with open(args.csv, "a", newline="") as file:
        writer = csv.DictWriter(file, COLUMNS)
        if new:
            writer.writeheader()
        for rep in range(args.reps):
            row, out, returncode = run_once(args, phase, env, logs / f"{args.label}-{rep}.log")
            check = f"FAILED rc={returncode}" if returncode else ""
            if args.check and rep == 0 and not returncode:
                check = compare(reference(phase, kind), digest(out, kind))
            writer.writerow({"label": args.label, "rep": rep, "check": check, **row})
            file.flush()
            shutil.rmtree(out)
            print(f"{args.label} rep={rep} exec={row['exec_s']}s wall={row['wall_s']}s rss={row['rss_mb']}MB {check}",
                  flush=True)
            if returncode:
                sys.exit(1)


if __name__ == "__main__":
    main()
