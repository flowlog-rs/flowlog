"""Triangle, Tri(x,y,z) :- Arc(x,y), Arc(y,z), Arc(x,z), batch mode: binary plan -> one lookup path.

Binary plan: Arc by y |><| Arc by y -> wedges W(x,z; y) arranged by (x,z) -> semijoin with Arc by (x,z).
All three atoms are static (non-recursive rule, one batch time), so a single path suffices:
stream Arc(x,y), look up Arc by c0 at y, then check Arc(x,z) exists. No wedge arrangement,
and no Arc-by-c1 arrangement.
"""

import sys
from pathlib import Path

from _edit import replace_span

new = r'''// LOOKUP-CHAIN EXPERIMENT (triangle, batch): one path, no wedge arrangement.
                            use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_lookup_exists, flowlog_land, Version};
                            let t_8895333882570235874 = ::flowlog_runtime::operators::flowlog_map(
                                rel_0_arc.clone(),
                                "Arrange: K:(V0), V:(V1) <- Arc",
                                |(x0, x1): (i32, i32), t, d| {
                                    std::iter::once((((x0,), (x1,)), t, d))
                                },
                            );
                            let t_8895333882570235874_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                t_8895333882570235874.clone(),
                                "Arrange: arc [0x7b72930965185fe2], key:(1.0), value:(1.1)",
                            );
                            let t_11435734910278871192 = rel_0_arc.clone();
                            let t_11435734910278871192_arr = ::flowlog_runtime::operators::flowlog_arrange_self(
                                t_11435734910278871192.clone(),
                                "Arrange: arc [0x9eb3e44fd0b4a898], key:(1.0, 1.1), value:()",
                            );
                            let t_13927455248685239126 = flowlog_land(
                                flowlog_lookup_exists(
                                    flowlog_lookup(
                                        flowlog_path(rel_0_arc.clone(), "Path: Arc(x,y)"),
                                        t_8895333882570235874_arr.clone(),
                                        "Lookup: Arc(x,y) * Arc(y,z)",
                                        Version::New,
                                        |&(_x, y): &(i32, i32)| (y,),
                                        |&(x, y): &(i32, i32), rv: &(i32,)| Some((x, y, rv.0)),
                                    ),
                                    t_11435734910278871192_arr.clone(),
                                    "Exists: Arc(x,z)",
                                    Version::New,
                                    |&(x, _y, z): &(i32, i32, i32)| (x, z),
                                ),
                                "Land: tri",
                            );
                            '''

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
code = replace_span(code, "let t_13007074777458153994 = ::flowlog_runtime::operators::flowlog_map(", "let rel_1_tri = ", new)
main_rs.write_text(code)
