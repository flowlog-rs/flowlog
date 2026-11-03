"""Triangle, Tri(x,y,z) :- Arc(x,y), Arc(y,z), Arc(x,z), incremental mode: binary plan -> 3-path delta join.

Binary plan keeps the wedge arrangement W(x,z; y) for as long as the dataflow runs.
The delta join uses only the three Arc indexes the binary plan already builds
(by c0, by c1, by (c0,c1)): zero new arrangements, so Materialize's default rule would pick it.
Path i reads atoms before it as Version::New (<=) and atoms after it as Version::Old (<).

ORDER=oldfirst reorders path 2 to probe its Old atom first: dArc(y,z) -> Arc(x,z) by z [Old] -> Arc(x,y) exists [New].
At the bulk load (epoch 0) every Old probe is empty, so path 2 stops at its first hop instead of
enumerating every wedge and then discarding it at the Old check. Same indexes, same results.

FILTER=snapshot keeps the hop order and adds Materialize's snapshot filter (build_update_stream_trace):
paths 1 and 2 each have an Old hop, which is empty at the load, so they drop their epoch-0 rows
before any lookup. Only path 3 (all New) works at the load.
"""

import os
import sys
from pathlib import Path

from _edit import replace_span

new = r'''// DELTA-JOIN EXPERIMENT (triangle, incremental): three paths, no wedge arrangement.
                            use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_lookup_exists, flowlog_land, Version};
                            let t_11435734910278871192 = rel_0_arc.clone();
                            let t_11435734910278871192_arr = ::flowlog_runtime::operators::flowlog_arrange_self(
                                t_11435734910278871192.clone(),
                                "Arrange: arc [0x9eb3e44fd0b4a898], key:(1.0, 1.1), value:()",
                            );
                            // Path 1: dArc(x,y) -> Arc(y,z) by y [Old] -> Arc(x,z) exists [Old]
                            let path_1 = flowlog_land(
                                flowlog_lookup_exists(
                                    flowlog_lookup(
                                        flowlog_path(rel_0_arc.clone(), "Path: dArc1(x,y)"),
                                        t_8895333882570235874_arr.clone(),
                                        "Lookup: dArc1 * Arc2 by y",
                                        Version::Old,
                                        |&(_x, y): &(i32, i32)| (y,),
                                        |&(x, y): &(i32, i32), rv: &(i32,)| Some((x, y, rv.0)),
                                    ),
                                    t_11435734910278871192_arr.clone(),
                                    "Exists: Arc3(x,z) [Old]",
                                    Version::Old,
                                    |&(x, _y, z): &(i32, i32, i32)| (x, z),
                                ),
                                "Land: path 1",
                            );
                            // Path 2: dArc(y,z) -> Arc(x,y) by y [New] -> Arc(x,z) exists [Old]
                            let path_2 = flowlog_land(
                                flowlog_lookup_exists(
                                    flowlog_lookup(
                                        flowlog_path(rel_0_arc.clone(), "Path: dArc2(y,z)"),
                                        t_13007074777458153994_arr.clone(),
                                        "Lookup: dArc2 * Arc1 by y",
                                        Version::New,
                                        |&(y, _z): &(i32, i32)| (y,),
                                        |&(y, z): &(i32, i32), rv: &(i32,)| Some((rv.0, y, z)),
                                    ),
                                    t_11435734910278871192_arr.clone(),
                                    "Exists: Arc3(x,z) [Old]",
                                    Version::Old,
                                    |&(x, _y, z): &(i32, i32, i32)| (x, z),
                                ),
                                "Land: path 2",
                            );
                            // Path 3: dArc(x,z) -> Arc(x,y) by x [New] -> Arc(y,z) exists [New]
                            let path_3 = flowlog_land(
                                flowlog_lookup_exists(
                                    flowlog_lookup(
                                        flowlog_path(rel_0_arc.clone(), "Path: dArc3(x,z)"),
                                        t_8895333882570235874_arr.clone(),
                                        "Lookup: dArc3 * Arc1 by x",
                                        Version::New,
                                        |&(x, _z): &(i32, i32)| (x,),
                                        |&(x, z): &(i32, i32), rv: &(i32,)| Some((x, rv.0, z)),
                                    ),
                                    t_11435734910278871192_arr.clone(),
                                    "Exists: Arc2(y,z) [New]",
                                    Version::New,
                                    |&(_x, y, z): &(i32, i32, i32)| (y, z),
                                ),
                                "Land: path 3",
                            );
                            let t_13927455248685239126 = path_1.concatenate([path_2, path_3]);
                            '''
if os.environ.get("ORDER") == "oldfirst":
    old_path_2 = new[new.index("// Path 2:") : new.index("// Path 3:")]
    new_path_2 = r'''// Path 2 (Old first): dArc(y,z) -> Arc(x,z) by z [Old] -> Arc(x,y) exists [New]
                            let path_2 = flowlog_land(
                                flowlog_lookup_exists(
                                    flowlog_lookup(
                                        flowlog_path(rel_0_arc.clone(), "Path: dArc2(y,z)"),
                                        t_13007074777458153994_arr.clone(),
                                        "Lookup: dArc2 * Arc3 by z",
                                        Version::Old,
                                        |&(_y, z): &(i32, i32)| (z,),
                                        |&(y, z): &(i32, i32), rv: &(i32,)| Some((rv.0, y, z)),
                                    ),
                                    t_11435734910278871192_arr.clone(),
                                    "Exists: Arc1(x,y) [New]",
                                    Version::New,
                                    |&(x, y, _z): &(i32, i32, i32)| (x, y),
                                ),
                                "Land: path 2",
                            );
                            '''
    new = new.replace(old_path_2, new_path_2)
if os.environ.get("FILTER") == "snapshot":
    for label in ("Path: dArc1(x,y)", "Path: dArc2(y,z)"):
        old = f'flowlog_path(rel_0_arc.clone(), "{label}")'
        assert old in new, label
        new = new.replace(old, 'flowlog_path(::flowlog_runtime::operators::flowlog_map(rel_0_arc.clone(), '
                               '"Snapshot filter", |row: (i32, i32), t: u32, d| (t != 0).then_some((row, t, d))), '
                               f'"{label}")')

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
code = replace_span(code, "let t_9176098109624583358 = ::flowlog_runtime::operators::flowlog_join(", "let rel_1_tri = ", new)
main_rs.write_text(code)
