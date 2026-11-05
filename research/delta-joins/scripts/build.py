#!/usr/bin/env python3
"""Builds the binaries listed in builds.txt.

usage: scripts/build.py NAME...   build each NAME, after any build it starts from
       scripts/build.py --list    list every build

A program build compiles a Datalog program with this checkout's compiler and
runtime, or a CTADL analysis that scripts/ctadl_stage.py staged (CTADL.md)
with CTADL's compiler flags. A variant build copies another build's generated
crate, rewrites its src/main.rs with the patches from patches/, and builds it
with cargo. Crates go to $WORK/crates/NAME and binaries to $BIN/NAME (see
paths.py). Every NAME given is rebuilt; a build it starts from is reused if
already built.
"""

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

from paths import BIN, DATA, REPO, ROOT, WORK

CATALOG = ROOT / "builds.txt"
PATCHES = ROOT / "patches"
CRATES = WORK / "crates"
CARGO_TARGET = WORK / "cargo"


@dataclass
class Build:
    name: str
    # Program builds: the .dl file, relative to the repository root, or ctadl:WORKLOAD/PHASE.
    program: str | None = None
    # Variant builds: the build whose crate this one copies and rewrites.
    base: str | None = None
    settings: dict[str, str] = field(default_factory=dict)
    patches: list[str] = field(default_factory=list)


def parse_settings(text):
    """Parses `A=1 B=x y` into {"A": "1", "B": "x y"}.

    A value runs to the next ` NAME=`, where NAME is upper case, or the end.
    """
    if not text:
        return {}
    items = re.split(r" (?=[A-Z][A-Z0-9_]*=)", text)
    return dict(item.split("=", 1) for item in items)


def read_catalog(path=CATALOG):
    builds = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        cells = [cell.strip() for cell in line.split("|")]
        if len(cells) == 2:
            build = Build(cells[0], program=cells[1])
        elif len(cells) == 4:
            build = Build(cells[0], base=cells[1], settings=parse_settings(cells[2]), patches=cells[3].split())
        else:
            sys.exit(f"{path}:{number}: expected 2 or 4 cells, found {len(cells)}")
        if build.name in builds:
            sys.exit(f"{path}:{number}: {build.name} is listed twice")
        builds[build.name] = build
    for build in builds.values():
        if build.base is not None and build.base not in builds:
            sys.exit(f"{path}: {build.name} starts from unknown build {build.base}")
        for patch in build.patches:
            if not (PATCHES / patch).is_file():
                sys.exit(f"{path}: {build.name} uses missing patch {patch}")
    return builds


def run(command, **kwargs):
    """Runs a command and returns its result; if it fails, stops the build (its own output says why)."""
    result = subprocess.run(command, check=False, **kwargs)
    if result.returncode != 0:
        sys.exit(f"build.py: exit status {result.returncode} from {shlex.join(command)}")
    return result


@cache
def compiler():
    """Builds this checkout's compiler, if it is out of date, and returns its path."""
    command = ["cargo", "build", "--release", "-p", "flowlog-compiler", "--message-format=json-render-diagnostics"]
    out = run(command, cwd=REPO, stdout=subprocess.PIPE, text=True).stdout
    for line in out.splitlines():
        message = json.loads(line)
        is_compiler = message.get("reason") == "compiler-artifact" and message["target"]["name"] == "flowlog-compiler"
        if is_compiler and message.get("executable"):
            return Path(message["executable"])
    sys.exit("build.py: cargo did not report a flowlog-compiler executable")


def rewrite(main_rs, build):
    """Applies the build's patches, in order, to a generated main.rs."""
    env = {**os.environ, **build.settings}
    for patch in build.patches:
        path = PATCHES / patch
        if path.suffix == ".py":
            run([sys.executable, str(path), str(main_rs)], env=env)
        else:
            run(["patch", "--quiet", "--no-backup-if-mismatch", str(main_rs), str(path)])


def compile_program(build, crate):
    # Point the crate at this checkout's runtime, not the released one.
    env = {**os.environ, "FLOWLOG_RUNTIME_PATH": str(REPO / "flowlog-runtime")}
    source = [str(REPO / build.program)]
    if build.program.startswith("ctadl:"):
        staged = DATA / "ctadl" / build.program.removeprefix("ctadl:")
        if not (staged / "program.fl").is_file():
            workload = staged.parent.name
            sys.exit(f"build.py: {staged} is not staged yet; run: scripts/ctadl_stage.py {workload} FACTS")
        source = [str(staged / "program.fl"), "--str-intern", "--udf-file", str(staged / "udf.rs")]
    command = [str(compiler()), *source, "-B", str(crate)]
    run([*command, "-T", str(CARGO_TARGET), "-o", str(BIN / build.name)], env=env)


def make_variant(build, crate):
    shutil.copytree(CRATES / build.base, crate)
    # A unique package name keeps each binary apart in the shared target dir.
    manifest = crate / "Cargo.toml"
    text = re.sub(r'(?m)^name = ".*"$', f'name = "{build.name}"', manifest.read_text(), count=1)
    manifest.write_text(text)
    rewrite(crate / "src" / "main.rs", build)
    # Run inside the crate so cargo reads its .cargo/config.toml (-Dwarnings).
    run(["cargo", "build", "--release", "--quiet", "--target-dir", str(CARGO_TARGET)], cwd=crate)
    shutil.copy2(CARGO_TARGET / "release" / build.name, BIN / build.name)


def build_one(name, builds, rebuild=True):
    build = builds[name]
    crate = CRATES / name
    if not rebuild and crate.is_dir() and (BIN / name).is_file():
        return
    if build.base is not None:
        build_one(build.base, builds, rebuild=False)
    print(f"build.py: building {name}", flush=True)
    shutil.rmtree(crate, ignore_errors=True)
    CRATES.mkdir(parents=True, exist_ok=True)
    BIN.mkdir(parents=True, exist_ok=True)
    if build.program is not None:
        compile_program(build, crate)
    else:
        make_variant(build, crate)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("names", nargs="*", metavar="NAME")
    parser.add_argument("--list", action="store_true", help="list every build and exit")
    args = parser.parse_args()
    builds = read_catalog()
    if args.list:
        for build in builds.values():
            source = build.program or f"{build.base} + {' '.join(build.patches)}"
            print(f"{build.name:22} {source}")
        return
    if not args.names:
        parser.error("name at least one build (see --list)")
    unknown = [name for name in args.names if name not in builds]
    if unknown:
        parser.error(f"unknown builds: {' '.join(unknown)} (see --list)")
    for name in args.names:
        build_one(name, builds)


if __name__ == "__main__":
    main()
