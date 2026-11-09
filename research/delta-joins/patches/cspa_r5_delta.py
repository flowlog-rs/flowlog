"""CSPA rule 6, VA(x,y) :- VF(z,x), MA(z,w), VF(w,y): binary plan -> 3-path delta join.

Rules count from 1 in source order; this file's name, its builds and their count labels keep an
older label for this rule, r5. The delta join removes the intermediate arrangement
pi[x,w](VF |><| MA) keyed by w, and adds MA keyed by column 1.
Path i sees atoms before it with Version::New (<=) and atoms after it with Version::Old (<).

SYM=1: MA is symmetric in CSPA (by induction over the rules, per iteration), so path 3 can read
MA(z,w) as MA(w,z) from the existing MA-by-column-0 index and add no index at all.
"""

import os
import sys
from pathlib import Path

from _edit import replace_span

SYM = os.environ.get("SYM") == "1"
new = r'''// DELTA-JOIN EXPERIMENT (CSPA rule 6): one path per recursive atom, no intermediate.
                                        use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};
                                        let ma_by_c1 = ::flowlog_runtime::operators::flowlog_map(
                                            recursive_rel_3_memoryalias.clone(),
                                            "Arrange: K:(V1), V:(V0) <- MemoryAlias",
                                            |(x0, x1): (i32, i32), t, d| std::iter::once((((x1,), (x0,)), t, d)),
                                        );
                                        let ma_by_c1_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                            ma_by_c1,
                                            "Arrange: memoryalias by c1 (delta path 3)",
                                        );
                                        // Path 1: dVF(z,x) -> MA(z,w) [Old] -> VF(w,y) [Old]
                                        let path_1 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_2_valueflow.clone(), "Path: dVF1"),
                                                    t_16640148831721227852_arr.clone(),
                                                    "Lookup: dVF1 * MA",
                                                    Version::Old,
                                                    |&(z, _x): &(i32, i32)| (z,),
                                                    |&(_z, x): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                                ),
                                                t_13733537956235401491_arr.clone(),
                                                "Lookup: dVF1 * MA * VF3",
                                                Version::Old,
                                                |&(_x, w): &(i32, i32)| (w,),
                                                |&(x, _w): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: path 1",
                                        );
                                        // Path 2: dMA(z,w) -> VF(z,x) [New] -> VF(w,y) [Old]
                                        let path_2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_3_memoryalias.clone(), "Path: dMA"),
                                                    t_13733537956235401491_arr.clone(),
                                                    "Lookup: dMA * VF1",
                                                    Version::New,
                                                    |&(z, _w): &(i32, i32)| (z,),
                                                    |&(_z, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w)),
                                                ),
                                                t_13733537956235401491_arr.clone(),
                                                "Lookup: dMA * VF1 * VF3",
                                                Version::Old,
                                                |&(_x, w): &(i32, i32)| (w,),
                                                |&(x, _w): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: path 2",
                                        );
                                        // Path 3: dVF(w,y) -> MA(z,w) by w [New] -> VF(z,x) [New]
                                        let path_3 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_2_valueflow.clone(), "Path: dVF3"),
                                                    ma_by_c1_arr.clone(),
                                                    "Lookup: dVF3 * MA",
                                                    Version::New,
                                                    |&(w, _y): &(i32, i32)| (w,),
                                                    |&(_w, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y)),
                                                ),
                                                t_13733537956235401491_arr.clone(),
                                                "Lookup: dVF3 * MA * VF1",
                                                Version::New,
                                                |&(z, _y): &(i32, i32)| (z,),
                                                |&(_z, y): &(i32, i32), rv: &(i32,)| Some((rv.0, y)),
                                            ),
                                            "Land: path 3",
                                        );
                                        let t_14654144511267710504 = path_1.concatenate([path_2, path_3]);
                                        '''
if SYM:
    new = new[: new.index("let ma_by_c1 =")] + new[new.index("// Path 1:") :]
    new = new.replace("ma_by_c1_arr.clone(),", "t_16640148831721227852_arr.clone(), // MA by c0, read as MA by c1")

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
code = replace_span(
    code, "let t_2277780816941667629 = ::flowlog_runtime::operators::flowlog_join(", "let next_4645727690522280340 =", new
)
main_rs.write_text(code)
