#!/usr/bin/env python3
"""Stages a CTADL workload: the programs it runs, their inputs and outputs.

usage: scripts/ctadl_stage.py WORKLOAD FACTS [--query MODELS]

FACTS is a `ctadl import` directory. On a scratch copy of it, the script runs
`ctadl index` and then `ctadl query` with the FlowLog engine at 32 workers,
and keeps, in $DATA/ctadl/WORKLOAD/PHASE for each PHASE (index, query):

  program.fl, udf.rs  the program CTADL generated and its UDFs, which
                      scripts/build.py compiles (builds.txt's ctadl: rows)
  in/                 the input directory CTADL gave the binary
  ref/, ref.log       the binary's outputs and log, which
                      scripts/ctadl_run.py --check compares against
  stage.log           ctadl's log

and the front end, jadx or pcode, in $DATA/ctadl/WORKLOAD/language. ctadl must
be on PATH, set up for the FlowLog engine (CTADL.md, "How to rerun"). --query
gives `ctadl query` its models; P-code's default query defines no sources or
sinks, so P-code workloads take CTADL's src/ctadl/models/pcode/default-query.json.
"""

import argparse
import importlib.machinery
import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from paths import DATA


def load_ctadl():
    """The ctadl script on PATH, loaded as a module."""
    path = shutil.which("ctadl")
    if path is None:
        sys.exit("ctadl_stage.py: ctadl is not on PATH (see -h)")
    loader = importlib.machinery.SourceFileLoader("ctadl_cli", path)
    module = importlib.util.module_from_spec(importlib.util.spec_from_loader("ctadl_cli", loader))
    sys.modules["ctadl_cli"] = module
    loader.exec_module(module)
    return module


def run_phase(save, argv):
    """Runs `ctadl ARGV`, keeping in SAVE what its FlowLog binary runs, reads and writes."""
    # ctadl loads every module on sys.path named ctadl_* as a plugin, so it would load these scripts.
    here = Path(__file__).resolve().parent
    sys.path = [entry for entry in sys.path if Path(entry or ".").resolve() != here]
    ctadl = load_ctadl()

    def analyze(self, fact_dir, out_dir, args):
        # FlowLogCompiledAnalysis.analyze, plus the copies.
        with tempfile.TemporaryDirectory(prefix="flowlog-", dir=args.tmpdir) as tmp:
            indir, outdir = self.analysis.read_inputs(fact_dir, Path(tmp))
            for name in ("program.fl", "udf.rs"):
                shutil.copy(self.analysis.dir / name, save)
            shutil.copytree(indir, save / "in")
            self.add_args(["-F", str(indir), "-D", str(outdir), "-w", str(args.jobs)])
            result = self.run(capture_output=True)
            (save / "ref.log").write_bytes(result.stdout)
            shutil.copytree(outdir, save / "ref")
            self.analysis.write_outputs(outdir)
            return result

    ctadl.FlowLogCompiledAnalysis.analyze = analyze
    sys.argv = ["ctadl", *argv, "--engine", "flowlog"]
    ctadl.main(sys.argv)


def language(facts):
    """The front end that wrote FACTS: jadx or pcode."""
    path = facts / "facts" / "CTADLLanguage.facts"
    if not path.is_file():
        sys.exit(f"ctadl_stage.py: no {path}; FACTS must be a `ctadl import` directory")
    return path.read_text().strip().lower()


def main():
    if sys.argv[1:2] == ["--phase"]:  # one phase, in a child process: ctadl runs once per process
        run_phase(Path(sys.argv[2]), sys.argv[3:])
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workload", metavar="WORKLOAD", help="the name to stage it as, such as binaryeye")
    parser.add_argument("facts", type=Path, metavar="FACTS", help="a `ctadl import` directory")
    parser.add_argument("--query", type=Path, metavar="MODELS", help="models for `ctadl query`")
    args = parser.parse_args()

    front_end = language(args.facts)
    top = DATA / "ctadl" / args.workload
    work = top / "work"
    shutil.rmtree(top, ignore_errors=True)
    shutil.copytree(args.facts.resolve(), work)
    (top / "language").write_text(front_end + "\n")
    models = [str(args.query.resolve())] if args.query else []
    phases = {
        "index": ["index", "-f", "-j", "32"],
        "query": ["query", *models, "-j", "32", "--format", "summary"],
    }
    for phase, argv in phases.items():
        save = top / phase
        save.mkdir()
        command = [sys.executable, __file__, "--phase", str(save), "--directory", str(work), *argv]
        with open(save / "stage.log", "w") as log:
            done = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                  check=False)
        if done.returncode:
            sys.exit(f"ctadl_stage.py: ctadl {phase} failed; see {save / 'stage.log'}")
        print(f"staged {save}", flush=True)
    shutil.rmtree(work)


if __name__ == "__main__":
    main()
