"""GALEN's relations, for the galen_*.py estimators.

The inputs C, U and R come from $DATA/galen and the final P and Q from
$DATA/galen-ref; scripts/fetch_data.sh galen galen-ref fetches both.
"""

import collections

from paths import DATA


def relation(name):
    """The rows of C, U, R (inputs) or P, Q (final outputs) as int tuples."""
    if name in ("C", "U", "R"):
        path = DATA / "galen" / f"{name}.csv"
    else:
        path = DATA / "galen-ref" / f"{name.lower()}.csv"
    if not path.exists():
        raise SystemExit(f"{path} not found; run scripts/fetch_data.sh galen galen-ref")
    with path.open() as file:
        return [tuple(map(int, line.split(","))) for line in file]


def count_by(rows, column):
    """How many rows have each value in one column."""
    return collections.Counter(row[column] for row in rows)


def join_size(a, b):
    """The rows of a join on one key, from each side's count per key."""
    if len(a) > len(b):
        a, b = b, a
    return sum(n * b.get(key, 0) for key, n in a.items())


def millions(n):
    return f"{n / 1e6:,.1f}M"
