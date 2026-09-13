#!/usr/bin/env bash
# SQLite fixture setup and query export for the compiler runner.

setup_sqlite_fixture() {
    python3 - "$1" "$2" <<'PYTHON'
import os
from pathlib import Path
import sqlite3
import sys

fixture = Path(sys.argv[1]).resolve()
os.chdir(sys.argv[2])
with sqlite3.connect("input.sqlite") as database:
    database.executescript((fixture / "sqlite_setup.sql").read_text())
PYTHON
}

export_sqlite_outputs() {
    python3 - "$1" "$2" <<'PYTHON'
import json
from pathlib import Path
import sqlite3
import sys

fixture = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).resolve() / "output"
with sqlite3.connect((output / "output.sqlite").as_uri() + "?mode=ro", uri=True) as database:
    for expected in sorted((fixture / "expected").iterdir()):
        table = '"' + expected.name.replace('"', '""') + '"'
        with (output / expected.name).open("w") as target:
            for row in database.execute(f"SELECT * FROM {table}"):
                target.write(json.dumps(row, ensure_ascii=True) + "\n")
PYTHON
}
