"""SG rule 2, Sg(x,y) :- Arc(a,x), Sg(a,b), Arc(b,y).

Binary plan: Arc by a |><| Sg by a -> I = pi[x,b] keyed by b -> |><| Arc by b.

MODE=batch (default): one recursive atom, two static ones -> a lookup chain driven by dSg.
  Drops the Sg-by-c0 arrangement and the intermediate I (both only feed this join), no new index.
MODE=inc: every atom changes -> 3-path delta join. Paths dArc1 and dSg reuse Arc by c0 and Sg by c0;
  path dArc3 needs a new Sg-by-c1 index (Sg is recursive, so it costs |Sg| records).
Path i reads atoms before it as Version::New (<=) and atoms after it as Version::Old (<).
"""

import os
import sys
from pathlib import Path

from _edit import replace_span

mode = os.environ.get("MODE", "batch")
assert mode in ("batch", "inc"), mode
ARC = "in_t_17761163972671418955_arr"
if mode == "batch":
    new = f'''// LOOKUP-CHAIN EXPERIMENT (SG rule 2, batch): dSg -> Arc(a,x) -> Arc(b,y), nothing arranged.
                                        use ::flowlog_runtime::operators::{{flowlog_path, flowlog_lookup, flowlog_land, Version}};
                                        let t_14252931685534060960 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_1_sg.clone(), "Path: dSg"),
                                                    {ARC}.clone(),
                                                    "Lookup: dSg * Arc(a,x)",
                                                    Version::New,
                                                    |&(a, _b): &(i32, i32)| (a,),
                                                    |&(_a, b): &(i32, i32), rv: &(i32,)| Some((rv.0, b)),
                                                ),
                                                {ARC}.clone(),
                                                "Lookup: dSg * Arc * Arc(b,y)",
                                                Version::New,
                                                |&(_x, b): &(i32, i32)| (b,),
                                                |&(x, _b): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: sg rule 2",
                                        );
                                        '''
else:
    new = f'''// DELTA-JOIN EXPERIMENT (SG rule 2, incremental): three paths, no intermediate, new Sg-by-c1 index.
                                        use ::flowlog_runtime::operators::{{flowlog_path, flowlog_lookup, flowlog_land, Version}};
                                        let in_arc = rel_0_arc.clone().enter(inner);
                                        let sg_by_c0 = ::flowlog_runtime::operators::flowlog_map(
                                            recursive_rel_1_sg.clone(),
                                            "Arrange: K:(V0), V:(V1) <- Sg",
                                            |(x0, x1): (i32, i32), t, d| std::iter::once((((x0,), (x1,)), t, d)),
                                        );
                                        let sg_by_c0_arr = ::flowlog_runtime::operators::flowlog_arrange(sg_by_c0, "Arrange: sg by c0");
                                        let sg_by_c1 = ::flowlog_runtime::operators::flowlog_map(
                                            recursive_rel_1_sg.clone(),
                                            "Arrange: K:(V1), V:(V0) <- Sg",
                                            |(x0, x1): (i32, i32), t, d| std::iter::once((((x1,), (x0,)), t, d)),
                                        );
                                        let sg_by_c1_arr = ::flowlog_runtime::operators::flowlog_arrange(sg_by_c1, "Arrange: sg by c1 (delta path 3)");
                                        // Path 1: dArc(a,x) -> Sg(a,b) by a [Old] -> Arc(b,y) by b [Old]
                                        let path_1 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(in_arc.clone(), "Path: dArc1"),
                                                    sg_by_c0_arr.clone(),
                                                    "Lookup: dArc1 * Sg by a",
                                                    Version::Old,
                                                    |&(a, _x): &(i32, i32)| (a,),
                                                    |&(_a, x): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                                ),
                                                {ARC}.clone(),
                                                "Lookup: dArc1 * Sg * Arc by b",
                                                Version::Old,
                                                |&(_x, b): &(i32, i32)| (b,),
                                                |&(x, _b): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: path 1",
                                        );
                                        // Path 2: dSg(a,b) -> Arc(a,x) by a [New] -> Arc(b,y) by b [Old]
                                        let path_2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_1_sg.clone(), "Path: dSg"),
                                                    {ARC}.clone(),
                                                    "Lookup: dSg * Arc by a",
                                                    Version::New,
                                                    |&(a, _b): &(i32, i32)| (a,),
                                                    |&(_a, b): &(i32, i32), rv: &(i32,)| Some((rv.0, b)),
                                                ),
                                                {ARC}.clone(),
                                                "Lookup: dSg * Arc * Arc by b",
                                                Version::Old,
                                                |&(_x, b): &(i32, i32)| (b,),
                                                |&(x, _b): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: path 2",
                                        );
                                        // Path 3: dArc(b,y) -> Sg(a,b) by b [New] -> Arc(a,x) by a [New]
                                        let path_3 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(in_arc.clone(), "Path: dArc3"),
                                                    sg_by_c1_arr.clone(),
                                                    "Lookup: dArc3 * Sg by b",
                                                    Version::New,
                                                    |&(b, _y): &(i32, i32)| (b,),
                                                    |&(_b, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y)),
                                                ),
                                                {ARC}.clone(),
                                                "Lookup: dArc3 * Sg * Arc by a",
                                                Version::New,
                                                |&(a, _y): &(i32, i32)| (a,),
                                                |&(_a, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y)),
                                            ),
                                            "Land: path 3",
                                        );
                                        let t_14252931685534060960 = path_1.concatenate([path_2, path_3]);
                                        '''

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
code = replace_span(
    code, "let t_16402283467519064094 = ::flowlog_runtime::operators::flowlog_map(", "let next_8197946997800891532 =", new
)
main_rs.write_text(code)
