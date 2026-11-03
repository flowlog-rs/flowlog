"""Incremental CSPA (Assign, Dereference declared `append`): rule 4 as a delta join.

  R4  MemoryAlias(x,w) :- Dereference(y,x), ValueAlias(y,z), Dereference(z,w).

The binary plan arranges I4 = pi[x,z](Dereference |x|_y ValueAlias) by z and joins it with
Dereference. In batch mode Dereference closes, the join drops its handle on I4, and I4 retains
nothing; here Dereference stays open for later transactions, so I4 is kept whole. Every atom can
change, so every atom gets a path; path i sees earlier atoms New (<=) and later atoms Old (<):

  dDereference(y,x) -> ValueAlias by y [Old]          -> Dereference by z [Old]
  dValueAlias(y,z)  -> Dereference by y [New]         -> Dereference by z [Old]
  dDereference(z,w) -> ValueAlias by z [New] (as VA(z,y): ValueAlias is symmetric, and both
                       orientations are derived in the same round) -> Dereference by y [New]

Every index the paths read exists in the binary plan already; I4 is gone.
"""

import sys
from pathlib import Path

from _edit import insert_before, remove_statement

INDENT = "\n                                        "
USES = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};"

paths = '''// DELTA-JOIN EXPERIMENT (incremental CSPA R4): one path per atom, no I4.
                                        use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_land, Version};
                                        let r4_p1 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(in_rel_1_dereference.clone(), "Path: dDeref1 (R4)"),
                                                    t_16522007716526504819_arr.clone(),
                                                    "Lookup: dDeref1 * VA by y [Old]",
                                                    Version::Old,
                                                    |&(y, _x): &(i32, i32)| (y,),
                                                    |&(_y, x): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                                ),
                                                in_t_3397545554825700508_arr.clone(),
                                                "Lookup: dDeref1 * VA * Deref by z [Old]",
                                                Version::Old,
                                                |&(_x, z): &(i32, i32)| (z,),
                                                |&(x, _z): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: R4 path Deref1",
                                        );
                                        let r4_p2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_4_valuealias.clone(), "Path: dVA (R4)"),
                                                    in_t_3397545554825700508_arr.clone(),
                                                    "Lookup: dVA * Deref by y [New]",
                                                    Version::New,
                                                    |&(y, _z): &(i32, i32)| (y,),
                                                    |&(_y, z): &(i32, i32), rv: &(i32,)| Some((rv.0, z)),
                                                ),
                                                in_t_3397545554825700508_arr.clone(),
                                                "Lookup: dVA * Deref * Deref by z [Old]",
                                                Version::Old,
                                                |&(_x, z): &(i32, i32)| (z,),
                                                |&(x, _z): &(i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: R4 path VA",
                                        );
                                        let r4_p3 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(in_rel_1_dereference.clone(), "Path: dDeref3 (R4)"),
                                                    t_16522007716526504819_arr.clone(),
                                                    "Lookup: dDeref3 * VA by z, symmetric [New]",
                                                    Version::New,
                                                    |&(z, _w): &(i32, i32)| (z,),
                                                    |&(_z, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w)),
                                                ),
                                                in_t_3397545554825700508_arr.clone(),
                                                "Lookup: dDeref3 * VA * Deref by y [New]",
                                                Version::New,
                                                |&(y, _w): &(i32, i32)| (y,),
                                                |&(_y, w): &(i32, i32), rv: &(i32,)| Some((rv.0, w)),
                                            ),
                                            "Land: R4 path Deref3",
                                        );
                                        let t_4206644099130053556 = r4_p1.concatenate([r4_p2, r4_p3]);
                                        '''

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
# I4's map and arrangement, and the join that reads it; the paths define its output again.
for name in ["t_16243381426714551124", "t_16243381426714551124_arr", "t_4206644099130053556"]:
    code = remove_statement(code, name)
code = insert_before(
    code, "let t_16640148831721227852 = ", "let in_rel_1_dereference = rel_1_dereference.clone().enter(inner);" + INDENT
)
if USES in code:  # cspa_r5_delta.py, applied first in cspa_inc_r45, imported them in this block
    paths = paths.replace(USES + INDENT, "")
code = insert_before(code, "let next_4645727690522280340 =", paths)
main_rs.write_text(code)
