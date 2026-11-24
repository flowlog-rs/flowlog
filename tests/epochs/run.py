#!/usr/bin/env python3
"""Checks every epoch of FlowLog's incremental engines against batch runs.

A case is a program in cases/ that mixes static, append and mutable
inputs, plus how cases.py draws its data. For each seed, the runner draws
the files the program loads and a transcript of transactions for its
shell, then runs the compiled program at several worker counts. Every
epoch's output files must hold exactly what changed between two batch
runs of the same rules, one over the inputs before the epoch and one over
the inputs after it.

    python3 tests/epochs/run.py [-j N] [--seeds N | --seed S ...]
                                [--workers W,...] [case ...]

"Every-epoch oracle" in tests/README.md says how to add a case and how to
turn a failure into a fixture.
"""

import argparse
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from textwrap import indent
from typing import NamedTuple

from cases import CASES, Case, Row

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
# Cargo resolves a relative CARGO_TARGET_DIR against its working directory,
# which is ROOT when this script builds the compiler.
TARGET = ROOT / os.environ.get("CARGO_TARGET_DIR", "target")
COMPILER = TARGET / "release" / "flowlog-compiler"
WORK = TARGET / "epochs"
KEPT = WORK / "failures"  # failed runs, laid out as fixtures
# Generated crates depend on this checkout's runtime by path, so a runtime
# change is checked before it is released.
COMPILE_ENV = {**os.environ, "FLOWLOG_RUNTIME_PATH": str(ROOT / "flowlog-runtime")}

# The transcript's shape, picked by hand rather than measured: rows should
# come and go many times in one transcript, and a case should take seconds.
COMMITS = 10  # transactions, each one epoch
MAX_UPDATES = 6  # per transaction
EMPTY = 0.08  # chance a transaction has no updates; it still makes an epoch
INSERT = 0.55  # chance a mutable update inserts when it could delete
DELETE_PRESENT = 0.85  # chance a delete names a present row
CANCEL = 0.15  # chance a transaction also inserts and deletes one row

# CI runs the defaults, so a CI failure replays exactly and no pull request
# trips over a newly drawn transcript; the cost is that CI never looks past
# these seeds. --seeds searches further.
DEFAULT_SEEDS = 10
# One worker exchanges no data; 4 and 7 workers exchange it, and as the
# counts share no factor, they place keys on workers differently.
DEFAULT_WORKERS = [1, 4, 7]

# A run takes well under a second; this only turns a hang into a failure.
RUN_TIMEOUT = 120
HUNG = f"no exit after {RUN_TIMEOUT} s"
# A report lists at most this many failures per case, differences per
# failure, and rows per difference, then counts the rest; every failed
# run is kept whole.
SHOWN = 3


class CaseError(Exception):
    """A case the runner cannot model, or whose program and data disagree."""


class SetupError(Exception):
    """A case that could not be checked: a compile or a batch run failed."""


# ==============================================================================
# Programs
# ==============================================================================


@dataclass(frozen=True)
class Program:
    """What the runner knows of a case's program."""

    name: str
    path: Path
    twin: str  # the batch program: same rules, every input static, from file
    inputs: dict[str, str]  # input relation -> static, append or mutable
    from_files: frozenset[str]  # inputs read from <Rel>.csv at the start
    outputs: tuple[str, ...]
    nullary: frozenset[str]  # relations declared with no columns

    @property
    def loads(self) -> bool:
        """Whether the first epoch is a load, before any transaction.

        flowlog-compiler starts with one when the program reads a file or
        states a fact (`has_preload` in src/io/input.rs); facts are refused.
        """
        return bool(self.from_files)


def file_input(rel: str) -> str:
    return f'.input {rel}(IO="file", filename="{rel}.csv", delimiter=",")'


def command_input(rel: str) -> str:
    return f'.input {rel}(IO="command", delimiter=",")'


DIRECTIVE = re.compile(r"\.(\w+)")
DECL = re.compile(r"(\.decl\s+(\w+)\s*\((.*)\))\s*(static|append|mutable)?")
INPUT = re.compile(r"\.input\s+(\w+)\s*\(.*\)")
OUTPUT = re.compile(r"\.output\s+(\w+)")
FACT = re.compile(r"\w+\s*\([^()]*\)\s*\.")


def expect(pattern: re.Pattern[str], text: str, where: str) -> re.Match[str]:
    if match := pattern.fullmatch(text):
        return match
    raise CaseError(f"{where}: cannot read {text!r}")


def read_program(name: str, case: Case) -> Program:
    """Reads cases/<name>.dl, writing its twin line by line alongside.

    The runner models `.decl`, `.input` in the two forms above, and bare
    `.output`. It refuses other directives and inline facts, which could
    add inputs, outputs or rows it does not know of.
    """
    path = HERE / "cases" / f"{name}.dl"
    shown = path.relative_to(ROOT)
    kinds: dict[str, str] = {}
    nullary: set[str] = set()
    read: dict[str, bool] = {}  # input relation -> read from a file
    outputs: list[str] = []
    twin: list[str] = []
    in_rule = False  # an earlier line began a rule that has not ended
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        text, where = line.strip(), f"{shown}:{number}"
        directive = DIRECTIVE.match(text)
        match directive and directive[1]:
            case None:
                code = text.partition("//")[0].rstrip()
                if FACT.fullmatch(code) and not in_rule:
                    raise CaseError(
                        f"{where}: inline facts are not modeled; "
                        "give the rows to the case's load in cases.py"
                    )
                if code:
                    in_rule = not code.endswith(".")
                twin.append(line)
            case "decl":
                decl = expect(DECL, text, where)
                kinds[decl[2]] = decl[4] or "static"
                if not decl[3].strip():
                    nullary.add(decl[2])
                twin.append(decl[1])
            case "input":
                rel = expect(INPUT, text, where)[1]
                if text not in (file_input(rel), command_input(rel)):
                    raise CaseError(
                        f"{where}: write {file_input(rel)} or {command_input(rel)}"
                    )
                read[rel] = text == file_input(rel)
                twin.append(file_input(rel))
            case "output":
                outputs.append(expect(OUTPUT, text, where)[1])
                twin.append(line)
            case other:
                raise CaseError(f"{where}: .{other} is not modeled")

    if undeclared := sorted(read.keys() - kinds.keys()):
        raise CaseError(f"{shown}: no .decl for {', '.join(undeclared)}")
    inputs = {rel: kinds[rel] for rel in read}
    from_files = frozenset(rel for rel, from_file in read.items() if from_file)
    updated = {rel for rel, kind in inputs.items() if kind != "static"}
    if not updated:
        raise CaseError(f"{shown}: no append or mutable input, so no epochs")
    if case.load.keys() != from_files:
        raise CaseError(
            f"{shown}: cases.py loads {sorted(case.load)}, but the program"
            f" reads {sorted(from_files)} from files"
        )
    if case.update.keys() != updated:
        raise CaseError(
            f"{shown}: cases.py updates {sorted(case.update)}, but the"
            f" append and mutable inputs are {sorted(updated)}"
        )
    if not outputs:
        raise CaseError(f"{shown}: no .output to check")
    twin_text = "\n".join(twin) + "\n"
    return Program(
        name, path, twin_text, inputs, from_files, tuple(outputs), frozenset(nullary)
    )


# ==============================================================================
# Transcripts
# ==============================================================================


class Update(NamedTuple):
    verb: str  # insert or delete
    relation: str
    row: Row


@dataclass(frozen=True)
class Transcript:
    """One seed's inputs: the files the program loads, the transactions
    its shell receives, and what every input holds after each epoch."""

    files: dict[str, list[Row]]
    transactions: list[list[Update]]
    after: list[dict[str, frozenset[Row]]]

    def commands(self) -> str:
        """The shell's input. Relations are named in lowercase: the shell
        ignores case, and the library-mode fixture runner matches only
        lowercase, so a kept run replays in both modes. A nullary row is no
        text: `insert flag`."""
        lines = []
        for updates in self.transactions:
            lines.append("begin")
            for verb, rel, row in updates:
                cells = f" {csv(row)}" if row else ""
                lines.append(f"{verb} {rel.lower()}{cells}")
            lines.append("commit")
        return "\n".join([*lines, "quit"]) + "\n"


def csv(row: Row) -> str:
    return ",".join(map(str, row))


def draw(program: Program, case: Case, seed: int) -> Transcript:
    """Draws one seed's transcript.

    A string seed is hashed the same way by every Python 3, so a seed
    replays the same transcript on any machine.
    """
    rng = random.Random(f"{program.name}:{seed}")
    files = {rel: draw_rows(rng) for rel, draw_rows in case.load.items()}
    state = {rel: set(files.get(rel, ())) for rel in program.inputs}
    after = [freeze(state)] if program.loads else []
    transactions = []
    for _ in range(COMMITS):
        updates = transaction(rng, program, case, state)
        commit(state, updates)
        transactions.append(updates)
        after.append(freeze(state))
    return Transcript(files, transactions, after)


def transaction(
    rng: random.Random, program: Program, case: Case, state: dict[str, set[Row]]
) -> list[Update]:
    """One transaction's updates, in the order the shell receives them.

    Only mutable inputs are deleted from, mostly rows present before the
    transaction. The order is shuffled, as only each row's net may count.
    """
    if rng.random() < EMPTY:
        return []
    names = list(case.update)
    mutable = [rel for rel in names if program.inputs[rel] == "mutable"]
    updates = []
    for _ in range(rng.randint(1, MAX_UPDATES)):
        rel = rng.choice(names)
        if rel in mutable and state[rel] and rng.random() >= INSERT:
            if rng.random() < DELETE_PRESENT:
                row = rng.choice(sorted(state[rel]))
            else:
                row = case.update[rel](rng)
            updates.append(Update("delete", rel, row))
        else:
            updates.append(Update("insert", rel, case.update[rel](rng)))
    if mutable and rng.random() < CANCEL:
        rel = rng.choice(mutable)
        row = case.update[rel](rng)
        updates += [Update("insert", rel, row), Update("delete", rel, row)]
    rng.shuffle(updates)
    return updates


def commit(state: dict[str, set[Row]], updates: list[Update]) -> None:
    """Applies a transaction as docs/design/mutability.md says the inputs
    do: per row, more inserts than deletes make it present, more deletes
    make it absent, and a balance leaves it as it was."""
    net: Counter[tuple[str, Row]] = Counter()
    for verb, rel, row in updates:
        net[rel, row] += 1 if verb == "insert" else -1
    for (rel, row), surplus in net.items():
        if surplus > 0:
            state[rel].add(row)
        elif surplus < 0:
            state[rel].discard(row)


def freeze(state: dict[str, set[Row]]) -> dict[str, frozenset[Row]]:
    return {rel: frozenset(rows) for rel, rows in state.items()}


# ==============================================================================
# Running programs
# ==============================================================================


def run(
    command: list[object], stdin: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Runs `command`, capturing its output; raises TimeoutExpired after
    RUN_TIMEOUT seconds."""
    with open(stdin or os.devnull) as source:
        return subprocess.run(
            [str(part) for part in command],
            stdin=source,
            capture_output=True,
            check=False,
            text=True,
            timeout=RUN_TIMEOUT,
        )


def tail(proc: subprocess.CompletedProcess[str], lines: int = 20) -> list[str]:
    return (proc.stdout + proc.stderr).splitlines()[-lines:]


def compile_program(source: Path, binary: Path, slot: Path) -> None:
    """Compiles `source` to `binary`, building in the slot's target dir."""
    proc = subprocess.run(
        [str(COMPILER), str(source), "-o", str(binary), "-T", str(slot)],
        env=COMPILE_ENV,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        lines = [f"flowlog-compiler failed on {source.name}:", *tail(proc)]
        raise SetupError("\n".join(lines))


def write_rows(folder: Path, tables: Mapping[str, Iterable[Row]]) -> None:
    """Writes each table to folder/<Rel>.csv, a sorted row a line; a
    nullary row is `True`, as the engine writes it."""
    folder.mkdir(parents=True, exist_ok=True)
    for rel, table in tables.items():
        lines = ((csv(row) if row else "True") + "\n" for row in sorted(table))
        (folder / f"{rel}.csv").write_text("".join(lines))


def write_files(folder: Path, files: Mapping[str, list[str]]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for name, lines in files.items():
        (folder / name).write_text("".join(line + "\n" for line in lines))


@dataclass(frozen=True)
class Expected:
    """What a run must write, and how much change that is."""

    files: dict[str, list[str]]  # every epoch's output files, by name
    gained: int  # rows that an epoch adds to an output, over every epoch
    lost: int  # rows that an epoch takes from an output, likewise


# The reference is FlowLog's own batch engine rather than an independent
# one such as Souffle: it runs every feature the cases use, lattices among
# them, needs no other tool, and is itself checked against Souffle by
# tests/oracle and by the fixtures. The cost is that a bug batch and
# incremental share, say in an operator both build, goes unseen here.
def reference(
    twin: Path, program: Program, transcript: Transcript, work: Path
) -> Expected:
    """What a run over `transcript` must write.

    Epoch k's <Rel>_t<k>.csv holds, sorted, a `+1` line for each row of
    Rel that a batch run over the inputs after epoch k has and the run
    over the inputs before it lacks, and a `-1` line for each the other
    way round. A nullary Rel's only row, `True`, carries no weight, as
    the engine writes it: whether it came or went follows from the epochs
    before. Before the first epoch, every output is empty.
    """
    files = {}
    gained = lost = 0
    before: dict[str, set[str]] = {rel: set() for rel in program.outputs}
    for k, inputs in enumerate(transcript.after, start=1):
        after = batch(twin, program, inputs, work / f"t{k}")
        for rel in program.outputs:
            came, went = after[rel] - before[rel], before[rel] - after[rel]
            plus, minus = ("", "") if rel in program.nullary else ("\t+1", "\t-1")
            lines = [line + plus for line in came] + [line + minus for line in went]
            files[f"{rel}_t{k}.csv"] = sorted(lines)
            gained, lost = gained + len(came), lost + len(went)
        before = after
    return Expected(files, gained, lost)


def batch(
    twin: Path, program: Program, inputs: Mapping[str, Iterable[Row]], work: Path
) -> dict[str, set[str]]:
    """The lines of every output after a batch run over `inputs`."""
    write_rows(work / "data", inputs)
    out = work / "out"
    out.mkdir()
    try:
        proc = run([twin, "-F", work / "data", "-D", out, "-w", 1])
    except subprocess.TimeoutExpired:
        raise SetupError(f"the batch twin ran past {RUN_TIMEOUT} s") from None
    if proc.returncode != 0:
        lines = [f"the batch twin exited with {proc.returncode}:", *tail(proc)]
        raise SetupError("\n".join(lines))
    try:
        return {
            rel: set((out / f"{rel}.csv").read_text().splitlines())
            for rel in program.outputs
        }
    except FileNotFoundError as missing:
        raise SetupError(f"the batch twin wrote no {missing.filename}") from None


def check(
    binary: Path,
    transcript: Transcript,
    expected: dict[str, list[str]],
    workers: int,
    work: Path,
) -> list[str]:
    """Runs the incremental program over `transcript` at `workers` workers
    and lists how its files differ from `expected`; none means they agree.

    `work` is left fixture-shaped (data/, commands.txt, actual/), so that
    `keep` need only add the rest.
    """
    write_rows(work / "data", transcript.files)
    commands = work / "commands.txt"
    commands.write_text(transcript.commands())
    actual = work / "actual"
    actual.mkdir()
    command = [binary, "-F", work / "data", "-D", actual, "-w", workers]
    try:
        proc = run(command, stdin=commands)
    except subprocess.TimeoutExpired:
        return [HUNG]
    if proc.returncode != 0:
        output = indent("\n".join(tail(proc)), "  ")
        return [f"exit status {proc.returncode}; its output ends:\n{output}"]
    found = {
        path.name: sorted(path.read_text().splitlines()) for path in actual.iterdir()
    }
    return differences(expected, found)


EPOCH_FILE = re.compile(r"(\w+)_t(\d+)\.csv")


def differences(
    expected: dict[str, list[str]], actual: dict[str, list[str]]
) -> list[str]:
    """How `actual` differs from `expected`, file by file, in epoch order."""

    def order(name: str) -> tuple[float, str]:
        epoch_file = EPOCH_FILE.fullmatch(name)
        return (int(epoch_file[2]), epoch_file[1]) if epoch_file else (math.inf, name)

    problems = []
    for name in sorted(expected.keys() | actual.keys(), key=order):
        if name not in actual:
            problems.append(f"{name} is missing")
        elif name not in expected:
            problems.append(f"{name} is unexpected")
        elif actual[name] != expected[name]:
            lacks = Counter(expected[name]) - Counter(actual[name])
            extra = Counter(actual[name]) - Counter(expected[name])
            parts = [f"lacks {sample(lacks)}"] if lacks else []
            parts += [f"has extra {sample(extra)}"] if extra else []
            problems.append(f"{name} {'; '.join(parts)}")
    return problems


def sample(lines: Counter[str]) -> str:
    """A few of `lines`, tabs shown as spaces, and how many more there are."""
    rows = sorted(lines.elements())
    text = ", ".join(repr(row.replace("\t", " ")) for row in rows[:SHOWN])
    return text + (f" and {len(rows) - SHOWN} more" if len(rows) > SHOWN else "")


def keep(
    work: Path,
    program: Program,
    expected: dict[str, list[str]],
    seed: int,
    workers: int,
) -> None:
    """Completes a failed run's directory as a fixture, with its output in
    actual/, and moves it to KEPT/<case>-s<seed>-w<workers>."""
    shutil.copy(program.path, work / "program.dl")
    (work / "runtime_flags").write_text(f"-w\n{workers}\n")
    write_files(work / "expected", expected)
    KEPT.mkdir(parents=True, exist_ok=True)
    shutil.move(work, KEPT / f"{program.name}-s{seed}-w{workers}")


# ==============================================================================
# Cases
# ==============================================================================


@dataclass
class Failure:
    seed: int
    workers: int
    problems: list[str]


@dataclass
class Outcome:
    """What one case's runs found."""

    name: str
    runs: int = 0
    epochs: int = 0
    gained: int = 0  # Expected.gained, over every seed
    lost: int = 0  # Expected.lost, likewise
    seconds: float = 0.0
    error: str | None = None  # why the case could not be checked
    failures: list[Failure] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and not self.failures

    @property
    def hung(self) -> bool:
        """Whether the case stopped at a run that hung."""
        return bool(self.failures) and self.failures[-1].problems == [HUNG]


def run_case(
    program: Program,
    case: Case,
    seeds: list[int],
    workers: list[int],
    slots: Queue[Path],
) -> Outcome:
    """Compiles a case and its twin in a free slot, then checks every seed
    at every worker count, up to the first run that hangs: the runs after
    it would most likely hang too, and cost RUN_TIMEOUT each."""
    outcome = Outcome(program.name)
    start = time.monotonic()
    scratch = WORK / "scratch" / program.name
    slot = slots.get()
    try:
        scratch.mkdir(parents=True)
        incremental, twin = scratch / "incremental", scratch / "twin"
        (scratch / "twin.dl").write_text(program.twin)
        compile_program(program.path, incremental, slot)
        compile_program(scratch / "twin.dl", twin, slot)
        for seed in seeds:
            transcript = draw(program, case, seed)
            expected = reference(twin, program, transcript, scratch / f"s{seed}")
            outcome.gained += expected.gained
            outcome.lost += expected.lost
            for count in workers:
                work = scratch / f"s{seed}-w{count}"
                problems = check(incremental, transcript, expected.files, count, work)
                outcome.runs += 1
                outcome.epochs += len(transcript.after)
                if problems:
                    keep(work, program, expected.files, seed, count)
                    outcome.failures.append(Failure(seed, count, problems))
                if outcome.hung:
                    return outcome
    except SetupError as error:
        outcome.error = str(error)
    finally:
        slots.put(slot)
        shutil.rmtree(scratch, ignore_errors=True)
        outcome.seconds = time.monotonic() - start
    return outcome


def report(outcome: Outcome) -> None:
    """Prints a case's line, then its failures and how to replay them.

    A seed's runs that fail alike at several worker counts print as one.
    """
    print(
        f"{'ok' if outcome.ok else 'FAIL':<5} {outcome.name:<19}"
        f" {counted(outcome.runs, 'run'):>8}, {counted(outcome.epochs, 'epoch'):>11},"
        f" +{outcome.gained} -{outcome.lost} rows, {outcome.seconds:.0f} s"
    )
    if outcome.error:
        print(indent(outcome.error, " " * 6))
    alike: dict[tuple[int, tuple[str, ...]], list[str]] = {}
    for failure in outcome.failures:
        key = failure.seed, tuple(failure.problems)
        alike.setdefault(key, []).append(str(failure.workers))
    shown = list(alike.items())[:SHOWN]
    for (seed, problems), workers in shown:
        print(f"      seed {seed}, workers {', '.join(workers)}:")
        for problem in problems[:SHOWN]:
            print(indent(problem, " " * 8))
        if len(problems) > SHOWN:
            print(f"        and {len(problems) - SHOWN} more")
        print(
            f"        replay: python3 tests/epochs/run.py --seed {seed}"
            f" --workers {','.join(workers)} {outcome.name}"
        )
    if hidden := len(outcome.failures) - sum(len(workers) for _, workers in shown):
        print(f"      failed runs not shown: {hidden}")
    if outcome.hung:
        hang = outcome.failures[-1]
        print(f"      stopped at the hang: seed {hang.seed}, workers {hang.workers}")


def counted(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


def display(path: Path) -> Path:
    return path.relative_to(ROOT) if path.is_relative_to(ROOT) else path


# ==============================================================================
# Command line
# ==============================================================================


def positive(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"{text} is not a positive integer")
    return value


def counts(text: str) -> list[int]:
    return list(dict.fromkeys(positive(part) for part in text.split(",")))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog=f"cases: {', '.join(CASES)}",
    )
    parser.add_argument(
        "-j",
        dest="jobs",
        type=positive,
        default=1,
        metavar="N",
        help="cases to run at once, each with its own Cargo target directory"
        " under target/epochs (default 1)",
    )
    seeds = parser.add_mutually_exclusive_group()
    seeds.add_argument(
        "--seeds",
        type=positive,
        default=DEFAULT_SEEDS,
        metavar="N",
        help=f"run seeds 0 to N-1 (default {DEFAULT_SEEDS})",
    )
    seeds.add_argument(
        "--seed",
        type=int,
        action="append",
        metavar="S",
        help="run seed S only; repeat for more",
    )
    parser.add_argument(
        "--workers",
        type=counts,
        default=DEFAULT_WORKERS,
        metavar="W,...",
        help="worker counts to run every seed at"
        f" (default {','.join(map(str, DEFAULT_WORKERS))})",
    )
    parser.add_argument(
        "cases", nargs="*", metavar="case", help="cases to run (default all)"
    )
    args = parser.parse_args()
    if unknown := [name for name in args.cases if name not in CASES]:
        parser.error(f"unknown case: {', '.join(unknown)}")
    return args


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    try:
        on_disk = {path.stem for path in (HERE / "cases").glob("*.dl")}
        if on_disk != CASES.keys():
            raise CaseError(
                f"cases/*.dl and CASES in cases.py disagree on"
                f" {', '.join(sorted(on_disk ^ CASES.keys()))}"
            )
        programs = {name: read_program(name, case) for name, case in CASES.items()}
    except CaseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    names = list(dict.fromkeys(args.cases)) or list(CASES)
    seeds = list(dict.fromkeys(args.seed or range(args.seeds)))
    if len(seeds) == 1:
        shown = f"seed {seeds[0]}"
    elif args.seed:
        shown = f"seeds {', '.join(map(str, seeds))}"
    else:
        shown = f"seeds 0 to {seeds[-1]}"
    jobs = min(args.jobs, len(names))
    shutil.rmtree(WORK / "scratch", ignore_errors=True)
    shutil.rmtree(KEPT, ignore_errors=True)
    slots = [WORK / f"slot-{i}" for i in range(jobs)]
    # Slots beyond this run's -j would only go stale, as in tests/fixtures.
    for stale in set(WORK.glob("slot-*")) - set(slots):
        shutil.rmtree(stale)
    free: Queue[Path] = Queue()
    for slot in slots:
        free.put(slot)

    print("Building flowlog-compiler (release)...")
    build = ["cargo", "build", "--release", "--quiet", "-p", "flowlog-compiler"]
    if subprocess.run(build, cwd=ROOT, check=False).returncode != 0:
        return 1
    print(
        f"Checking {counted(len(names), 'case')}: {shown};"
        f" workers {', '.join(map(str, args.workers))}; -j {jobs}"
    )

    outcomes = []
    with ThreadPoolExecutor(jobs) as pool:
        futures = [
            pool.submit(
                run_case, programs[name], CASES[name], seeds, args.workers, free
            )
            for name in names
        ]
        try:
            for future in as_completed(futures):
                outcomes.append(future.result())
                report(outcomes[-1])
        except KeyboardInterrupt:
            # Leaving the pool would wait out every build in flight; the
            # terminal has already sent the children the same interrupt.
            os._exit(130)

    failed = [outcome.name for outcome in outcomes if not outcome.ok]
    cases, runs = len(outcomes), sum(outcome.runs for outcome in outcomes)
    if failed:
        print(
            f"\n{len(failed)} of {counted(cases, 'case')} failed: {', '.join(failed)}"
        )
        if any(outcome.failures for outcome in outcomes):
            where = f"{display(KEPT)}/<case>-s<seed>-w<workers>/"
            print(f"Failed runs are kept, laid out as fixtures, in {where}")
        return 1
    print(
        f"\nEvery epoch agrees with batch:"
        f" {counted(cases, 'case')}, {counted(runs, 'run')}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
