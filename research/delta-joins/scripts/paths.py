"""Paths shared by the study's Python scripts.

env.sh sets the same paths for the shell scripts. An environment variable of
the same name overrides each one.
"""

import os
from pathlib import Path

# research/delta-joins, and the FlowLog checkout that holds it.
ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]

# Scratch space: generated crates, cargo output, binaries, data, new results.
WORK = Path(os.environ.get("WORK", REPO / "target" / "delta-joins"))
BIN = Path(os.environ.get("BIN", WORK / "bin"))
DATA = Path(os.environ.get("DATA", WORK / "data"))
RESULTS = Path(os.environ.get("RESULTS", WORK / "results"))
