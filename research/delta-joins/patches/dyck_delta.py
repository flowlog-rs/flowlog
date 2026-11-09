"""Dyck rules 3 and 4: Dyck(x,y) :- Zero(x,z), Dyck(z,w), Zero(w,y)   (rule 4 is the same with One).

Binary plan: Zero by c1 |><| Dyck by c0 -> I = pi[x,w] keyed by w (stored) -> |><| Zero by c0.

MODE=batch (default): one recursive atom -> lookup chain driven by dDyck through Zero by c1, then Zero by c0.
  Drops I_3 and I_4; every lookup reads an index that already exists (Dyck by c0/c1 stay for rule 5).
MODE=inc: every atom changes -> 3-path delta join per rule. All four indexes it reads already exist
  (Zero/One by c0 and c1 for rules 1-2, Dyck by c0 and c1 for rule 5), so Materialize's default rule fires.
Path i reads atoms before it as Version::New (<=) and atoms after it as Version::Old (<).
FILTER=snapshot (MODE=inc only) adds Materialize's snapshot filter to path 1, the only path whose first hop
  is Old: its load rows (time (0,0), the loop's minimum) would find nothing before them, so drop them first.
FILTER=loop also filters path 3 (all New): it probes Dyck, a loop variable, which holds nothing at (0,0)
  (Variable::new: the base rows come back through the feedback edge at (0,1)). So at the load only the dDyck
  path can produce anything, and the paths of the static atoms Zero and One are dropped there.
"""

import os
import sys
from pathlib import Path

from _edit import find, replace_span

mode = os.environ.get("MODE", "batch")
load_filter = os.environ.get("FILTER", "")
assert mode in ("batch", "inc"), mode
assert load_filter in ("", "snapshot", "loop") and (not load_filter or mode == "inc"), load_filter

# Per rule: the static atom's indexes by c1 and by c0, its relation, and the rule's output variable.
RULES = {
    "zero": ("in_t_6536940497821867264_arr", "in_t_3955239018239493738_arr", "rel_1_zero", "t_1525037165747138158"),
    "one": ("in_t_654422541893065854_arr", "in_t_5447180707643457834_arr", "rel_2_one", "t_6166195369591177442"),
}
DYCK_BY_C0, DYCK_BY_C1 = "t_4650450324051431199_arr", "t_4269017145473886434_arr"
KEY_RIGHT = "|&(_l, r): &(i32, i32)| (r,)"
KEY_LEFT = "|&(l, _r): &(i32, i32)| (l,)"
# Drops the rows at the loop's first time, (0,0).
SNAPSHOT_FILTER = (
    '::flowlog_runtime::operators::flowlog_map({src}, "Snapshot filter", '
    "|row: (i32, i32), t: ::flowlog_runtime::time::LexLoop, d| "
    "(t != ::flowlog_runtime::time::LexLoop::default()).then_some((row, t, d)))"
)


def chain(src, label, a1, v1, k1, o1, a2, v2, k2, o2):
    """A path: rows of `src`, then two lookups (index, version, key, output), then a land."""
    return f'''flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path({src}, "Path: {label}"),
                                                    {a1}.clone(), "Lookup: {label} hop 1", Version::{v1},
                                                    {k1}, {o1},
                                                ),
                                                {a2}.clone(), "Lookup: {label} hop 2", Version::{v2},
                                                {k2}, {o2},
                                            ),
                                            "Land: {label}",
                                        )'''


main_rs = Path(sys.argv[1])
code = main_rs.read_text()
start = "let t_1561512442803407138 = ::flowlog_runtime::operators::flowlog_join("
end = "let t_11057693110554412464 = ::flowlog_runtime::operators::flowlog_join("
# Dyck by c1 is built after rules 3 and 4; move it up, because path 3 reads it.
dyck_by_c1 = code[find(code, "let t_4269017145473886434 = ") : find(code, end)]
new = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};\n                                        " + dyck_by_c1
for name, (by_c1, by_c0, rel, out) in RULES.items():
    # dDyck(z,w) -> Z by c1 at z gives x -> (x,w) -> Z by c0 at w gives y -> (x,y)
    p2 = chain("recursive_rel_3_dyck.clone()", f"dDyck rule {name}", by_c1, "New", KEY_LEFT,
               "|&(_z, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w))", by_c0, "Old" if mode == "inc" else "New", KEY_RIGHT,
               "|&(x, _w): &(i32, i32), rv: &(i32,)| Some((x, rv.0))")
    if mode == "batch":
        new += f"// LOOKUP-CHAIN EXPERIMENT (dyck rule {name}, batch): no intermediate.\n                                        let {out} = {p2};\n                                        "
        continue
    # dZ(x,z) -> Dyck by c0 at z [Old] -> (x,w) -> Z by c0 at w [Old]
    src1 = f"{rel}.clone().enter(inner)"
    if load_filter:
        src1 = SNAPSHOT_FILTER.format(src=src1)
    p1 = chain(src1, f"d{name}1", DYCK_BY_C0, "Old", KEY_RIGHT,
               "|&(x, _z): &(i32, i32), rv: &(i32,)| Some((x, rv.0))", by_c0, "Old", KEY_RIGHT,
               "|&(x, _w): &(i32, i32), rv: &(i32,)| Some((x, rv.0))")
    # dZ(w,y) -> Dyck by c1 at w [New] gives z -> (z,y) -> Z by c1 at z [New] gives x
    src3 = f"{rel}.clone().enter(inner)"
    if load_filter == "loop":
        src3 = SNAPSHOT_FILTER.format(src=src3)
    p3 = chain(src3, f"d{name}3", DYCK_BY_C1, "New", KEY_LEFT,
               "|&(_w, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y))", by_c1, "New", KEY_LEFT,
               "|&(_z, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y))")
    new += f"""// DELTA-JOIN EXPERIMENT (dyck rule {name}, incremental): 3 paths, 0 new indexes.
                                        let {out} = {p1}.concatenate([{p2}, {p3}]);
                                        """
code = replace_span(code, start, end, new)
main_rs.write_text(code)
