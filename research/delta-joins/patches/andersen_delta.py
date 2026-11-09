"""Andersen (batch) delta joins for the two 3-atom rules; Load and Store are static, PointsTo (PT) is recursive.

  load : PT(y,w) :- Load(y,x), PT(x,z), PT(z,w).   binary stores I = pi[y,z](Load |><| PT) keyed z.
         paths: dPT1(x,z) -> Load by x [New] -> PT by z [Old];  dPT2(z,w) -> PT by c1 [New] (NEW, |PT|) -> Load by x [New]
  store: PT(z,w) :- Store(y,x), PT(y,z), PT(x,w).  binary stores I = pi[x,z](Store |><| PT) keyed x.
         paths: dPT1(y,z) -> Store by y [New] -> PT by x [Old]; dPT2(x,w) -> Store by c1 [New] (NEW, |Store|) -> PT by y [New]
Static atoms need no path in batch mode: they are complete at the loop's first time and read as New.
RULES=load,store picks the rules.
"""

import os
import sys
from pathlib import Path

from _edit import replace_span

rules = {rule.strip() for rule in os.environ.get("RULES", "load").split(",") if rule.strip()}
assert rules and rules <= {"load", "store"}, rules
PT, LOAD, STORE = "t_7882647657888222078_arr", "in_t_12307373497042889245_arr", "in_t_16623106336345954670_arr"
REC = "recursive_rel_4_pointsto"
USE = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};\n                                        "


def path(src, label, hops):
    """A path: rows of `src`, one lookup per hop (index, version, key, output), then a land."""
    expr = f'flowlog_path({src}.clone(), "Path: {label}")'
    for i, (arr, ver, key, out) in enumerate(hops):
        expr = f'flowlog_lookup({expr}, {arr}.clone(), "Lookup: {label} hop {i + 1}", Version::{ver}, {key}, {out})'
    return f'flowlog_land({expr}, "Land: {label}")'


main_rs = Path(sys.argv[1])
code = main_rs.read_text()
if "load" in rules:
    new = USE + f'''let pt_by_c1 = ::flowlog_runtime::operators::flowlog_arrange(
                                            ::flowlog_runtime::operators::flowlog_map({REC}.clone(), "Arrange: K:(V1), V:(V0) <- PointsTo",
                                                |(x0, x1): (i32, i32), t, d| std::iter::once((((x1,), (x0,)), t, d))),
                                            "Arrange: pointsto by c1 (delta path, NEW)",
                                        );
                                        // DELTA-JOIN EXPERIMENT (andersen load rule): 2 paths, no intermediate, new PT-by-c1 index.
                                        let t_8018265005551779197 = {path(REC, "load dPT1", [
        (LOAD, "New", "|&(x, _z): &(i32, i32)| (x,)", "|&(_x, z): &(i32, i32), rv: &(i32,)| Some((rv.0, z))"),
        (PT, "Old", "|&(_y, z): &(i32, i32)| (z,)", "|&(y, _z): &(i32, i32), rv: &(i32,)| Some((y, rv.0))")])}
                                        .concatenate([{path(REC, "load dPT2", [
        ("pt_by_c1", "New", "|&(z, _w): &(i32, i32)| (z,)", "|&(_z, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w))"),
        (LOAD, "New", "|&(x, _w): &(i32, i32)| (x,)", "|&(_x, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w))")])}]);
                                        '''
    code = replace_span(code, "let t_3228968080987312769 = ", "let t_7412337349186321050 = ", new)

if "store" in rules:
    use = "" if "load" in rules else USE
    new = use + f'''let store_by_c1 = ::flowlog_runtime::operators::flowlog_arrange(
                                            ::flowlog_runtime::operators::flowlog_map(rel_3_store.clone().enter(inner), "Arrange: K:(V1), V:(V0) <- Store",
                                                |(x0, x1): (i32, i32), t, d| std::iter::once((((x1,), (x0,)), t, d))),
                                            "Arrange: store by c1 (delta path, NEW)",
                                        );
                                        // DELTA-JOIN EXPERIMENT (andersen store rule): 2 paths, no intermediate, new Store-by-c1 index.
                                        let t_11416918076580984137 = {path(REC, "store dPT1", [
        (STORE, "New", "|&(y, _z): &(i32, i32)| (y,)", "|&(_y, z): &(i32, i32), rv: &(i32,)| Some((rv.0, z))"),
        (PT, "Old", "|&(x, _z): &(i32, i32)| (x,)", "|&(_x, z): &(i32, i32), rv: &(i32,)| Some((z, rv.0))")])}
                                        .concatenate([{path(REC, "store dPT2", [
        ("store_by_c1", "New", "|&(x, _w): &(i32, i32)| (x,)", "|&(_x, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w))"),
        (PT, "New", "|&(y, _w): &(i32, i32)| (y,)", "|&(_y, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w))")])}]);
                                        '''
    code = replace_span(code, "let t_7412337349186321050 = ", "let next_14161418664633571789 = ", new)
main_rs.write_text(code)
