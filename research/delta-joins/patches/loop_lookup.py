"""Batch, recursive 2-atom rules whose other atom is static: arrangement + join -> one lookup path.

The held-out version of tc_lookup.diff. Inside the loop, the binary plan arranges every change
of the recursive relation only to join it with the static arrangement. The lookup streams those
changes through the static arrangement instead, so the recursive relation is never arranged.

PROGRAM selects the program:
  reach:     Reach(y) :- Reach(x), Arc(x,y).
  cc:        CC(y, min c) :- Arc(x,y), CC(x,c).
  sssp:      sssp(y, min d1+d2) :- sssp(x,d1), arc(x,y,d2).
  csda:      NullNode(x,y) :- NullNode(x,w), Edge(w,y).
  bipartite: One(y) :- Zero(x), Arc(x,y).  One(y) :- Zero(x), Arc(y,x).  (and Zero from One alike)
             Four rules over two arrangements, Zero's and One's, each serving two rules; all four
             become lookups, so both arrangements go.
"""

import os
import sys
from pathlib import Path
from typing import NamedTuple

from _edit import find, replace_span

OPS = "::flowlog_runtime::operators::"


class Lookup(NamedTuple):
    """`let VAR = ...;` streaming ROWS (tuples of type ROW) through ARRANGED (values of type
    VALUE) by KEY = `|pattern| key`, emitting EMIT = `|pattern, rv| items` per match."""

    var: str
    rows: str
    arranged: str
    row: str
    value: str
    key: tuple[str, str]
    emit: tuple[str, str]
    label: str


def render(spec, indent):
    """The Rust statement for one Lookup, its lines after the first indented by INDENT."""
    var, rows, arranged, row, value, key, emit, label = spec
    key_pattern, key_body = key
    emit_pattern, emit_body = emit
    lines = [
        f"let {var} = {OPS}flowlog_land(",
        f"    {OPS}flowlog_lookup(",
        f'        {OPS}flowlog_path({rows}.clone(), "Path: {label}"),',
        f"        {arranged}.clone(),",
        f'        "Lookup: {label}",',
        f"        {OPS}Version::New,",
        f"        |{key_pattern}: &{row}| {key_body},",
        f"        |{emit_pattern}: &{row}, rv: &{value}| {emit_body},",
        "    ),",
        f'    "Land: {label}",',
        ");",
    ]
    return "\n".join(indent + line if i else line for i, line in enumerate(lines)) + "\n" + indent


# Per program: the span of the binary plan's arrangement(s) and join(s) to replace, as
# (first line, the statement after the span), and the lookups that replace it, which keep
# the names of the joins they replace.
PROGRAMS = {
    "reach": (
        "let t_4348782444986195675 = recursive_rel_2_reach.clone();",
        "let next_15861529767141734084 = ",
        [Lookup(var="t_16156898473735907250", rows="recursive_rel_2_reach",
                arranged="in_t_8895333882570235874_arr", row="(i32,)", value="(i32,)",
                key=("&(x,)", "(x,)"), emit=("_", "Some((rv.0,))"), label="dreach * arc")],
    ),
    "cc": (
        "let t_11420556021105551617 = ::flowlog_runtime::operators::flowlog_map(",
        "let next_17446237165013771863 = ",
        [Lookup(var="t_1004039422349225739", rows="recursive_rel_1_cc",
                arranged="in_t_17761163972671418955_arr", row="(i32, i32)", value="(i32,)",
                key=("&(x, _c)", "(x,)"), emit=("&(_x, c)", "Some((rv.0, c))"), label="dcc * arc")],
    ),
    "sssp": (
        "let t_8090914513161071579 = ::flowlog_runtime::operators::flowlog_map(",
        "let next_182081837971985307 = ",
        [Lookup(var="t_14933916918337605126", rows="recursive_rel_2_sssp",
                arranged="in_t_8209005679368531866_arr", row="(i32, i32)", value="(i32, i32)",
                key=("&(x, _d1)", "(x,)"),
                emit=("&(_x, d1)", "Some((rv.0, ::flowlog_runtime::arith::add(d1, rv.1)))"),
                label="dsssp * arc")],
    ),
    "csda": (
        "let t_12370286885403906144 = ::flowlog_runtime::operators::flowlog_map(",
        "let next_10043335049920060591 = ",
        [Lookup(var="t_12579374747054748257", rows="recursive_rel_2_nullnode",
                arranged="in_t_4387794716272245454_arr", row="(i32, i32)", value="(i32,)",
                key=("&(_x, w)", "(w,)"), emit=("&(x, _w)", "Some((x, rv.0))"), label="dnullnode * edge")],
    ),
    "bipartite": (
        "let t_9058697213283040634 = recursive_rel_3_zero.clone();",
        "let next_8474691169544981608 = ",
        [Lookup(var=var, rows=rows, arranged=arranged, row="(i32,)", value="(i32,)",
                key=("&(x,)", "(x,)"), emit=("_", "Some((rv.0,))"), label=label)
         for var, rows, arranged, label in [
             ("t_17638234450383305179", "recursive_rel_3_zero", "in_t_17761163972671418955_arr", "dzero * arc"),
             ("t_14861566485625734965", "recursive_rel_3_zero", "in_t_13007074777458153994_arr", "dzero * arc^T"),
             ("t_3284657103058092725", "recursive_rel_4_one", "in_t_17761163972671418955_arr", "done * arc"),
             ("t_15611474521345711859", "recursive_rel_4_one", "in_t_13007074777458153994_arr", "done * arc^T"),
         ]],
    ),
}

program = os.environ.get("PROGRAM", "")
if program not in PROGRAMS:
    sys.exit(f"{sys.argv[0]}: PROGRAM must be one of {', '.join(PROGRAMS)}, not {program!r}")
start, end, lookups = PROGRAMS[program]

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
at = find(code, start)
indent = code[code.rindex("\n", 0, at) + 1 : at]
new = f"// LOOKUP EXPERIMENT ({program}): the recursive relation is never arranged.\n{indent}"
new += "".join(render(spec, indent) for spec in lookups)
code = replace_span(code, start, end, new)
main_rs.write_text(code)
