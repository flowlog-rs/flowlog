"""The data each every-epoch case runs on, drawn afresh for every seed.

A case is a program in cases/<name>.dl plus a Case here. `load` gives each
input the program reads from a file the rows that file holds; `update`
gives each append or mutable input the rows its inserts and deletes name.
The domains are small on purpose, so that random updates hit the same
keys, join, and retract one another.
"""

import math
import random
from collections.abc import Callable
from dataclasses import dataclass

Row = tuple[int, ...]
Draw = Callable[[random.Random], Row]
DrawRows = Callable[[random.Random], list[Row]]


@dataclass(frozen=True)
class Case:
    """How to draw one case's inputs.

    `load` maps every file input to its rows, and `update` maps every
    append and mutable input to the row a single update names; run.py
    refuses a case whose keys and program disagree.
    """

    load: dict[str, DrawRows]
    update: dict[str, Draw]


def row(*domains: int) -> Draw:
    """One row whose column i is uniform over 1..domains[i]; `row()` is a
    nullary relation's only row."""
    return lambda rng: tuple(rng.randint(1, d) for d in domains)


def rows(count: int, *domains: int) -> DrawRows:
    """`count` distinct rows, each drawn as `row(*domains)`, sorted."""
    if count > math.prod(domains):
        raise ValueError(f"{count} distinct rows do not fit in {domains}")
    draw = row(*domains)

    def drawn(rng: random.Random) -> list[Row]:
        found: set[Row] = set()
        while len(found) < count:
            found.add(draw(rng))
        return sorted(found)

    return drawn


def fixed(*table: Row) -> DrawRows:
    """The same rows for every seed."""
    return lambda rng: list(table)


# Run in this order; each name matches cases/<name>.dl.
CASES: dict[str, Case] = {
    "joins": Case(
        load={"Edge": rows(10, 8, 8)},
        update={"Tag": row(8, 8), "On": row(8)},
    ),
    "one_loop": Case(
        load={"Road": rows(8, 10, 10), "Home": fixed((1,))},
        update={"Rail": row(10, 10), "Ferry": row(10, 10), "Visit": row(10)},
    ),
    "append_over_static": Case(
        load={"Edge": rows(7, 9, 9), "Wall": rows(2, 9)},
        update={"Link": row(9, 9)},
    ),
    "chained_loops": Case(
        load={"Edge": rows(6, 9, 9)},
        update={"Link": row(9, 9), "Gate": row(9), "Stop": row(9)},
    ),
    "mutual_loops": Case(
        load={"Start": fixed((1,), (2,)), "Edge": rows(10, 8, 8)},
        update={"Link": row(8, 8), "Ferry": row(8, 8)},
    ),
    "negation": Case(
        load={
            "Edge": rows(10, 10, 10),
            "Node": fixed(*((x,) for x in range(1, 11))),
            "Bad0": rows(1, 10),
        },
        update={
            "Seen": row(10),
            "Block": row(10),
            "Muted": row(10),
            "Start": row(10),
            "Link": row(10, 10),
        },
    ),
    "aggregates": Case(
        load={"Edge": rows(7, 8, 8)},
        update={"Item": row(8, 9), "Key": row(8), "Link": row(8, 8)},
    ),
    "count_in_loop": Case(
        load={"Source": fixed((1,), (2,)), "Road": rows(4, 6, 6)},
        update={"Edge": row(6, 6), "Link": row(6, 6)},
    ),
    "lattices": Case(
        load={"Road": rows(9, 10, 10, 4), "Home": fixed((1,))},
        update={
            "Rail": row(10, 10, 4),
            "Ferry": row(10, 10, 4),
            "Src": row(10),
        },
    ),
    "hybrid": Case(
        load={"Edge": rows(7, 10, 10), "Base": fixed((1,))},
        update={
            "Seen": row(10),
            "Link": row(10, 10),
            "Known": row(10),
            "Near": row(10),
        },
    ),
    "shared_closure": Case(
        load={"Edge": rows(9, 9, 9)},
        update={"Link": row(9, 9), "Start": row(9), "Stop": row(9)},
    ),
    "preload": Case(
        load={
            "Edge": rows(6, 9, 9),
            "Link": rows(3, 9, 9),
            "Wall": rows(1, 9),
        },
        update={"Edge": row(9, 9), "Link": row(9, 9)},
    ),
    "no_files": Case(
        load={},
        update={"Link": row(6, 6), "Ferry": row(6, 6), "Start": row(6)},
    ),
    "static_prelude": Case(
        load={
            "Edge": rows(15, 10, 10),
            "Road": rows(12, 10, 10, 4),
            "Home": fixed((1,), (2,)),
            "Wall": rows(2, 10),
        },
        update={"Link": row(10, 10), "Ask": row(10)},
    ),
    "nullary": Case(
        load={"Start": rows(2, 4), "Ready": fixed(())},
        update={"Edge": row(4, 4), "On": row(), "Alarm": row()},
    ),
}
