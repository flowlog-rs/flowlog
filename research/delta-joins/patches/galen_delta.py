"""GALEN: replace binary plans that retain an intermediate with delta-join paths.

RULES selects the rules to convert, comma-separated: any of 3 and 6, and at most one of 4, 4s and 4p
(default "6").
Static atoms are ordered first, so recursive paths see them whole and their own paths are empty
(static updates sit at iteration 0, recursive ones start at 1). Among recursive atoms, path i sees
earlier atoms with Version::New (<=) and later ones with Version::Old (<).

  rule 3  P(x,z)   :- P(y,w), U(w,r,z), Q(x,r,y).   removes I1 = pi[y,r,z](P |x|_w U)   by (y,r)
          dP(y,w)   -> U by w -> Q by (c2,c1)=(y,r) [Old]
          dQ(x,r,y) -> P by c0=y [New] -> U by (c0,c1)=(w,r)          (new static index)
  rule 4  P(x,z)   :- C(y,w,z), P(x,w), P(x,y).     removes I2 = C |x|_w P              by (x,y)
          dP(x,w)   -> C by w -> exists P(x,y) [Old]
          dP(x,y)   -> C by c0=y -> exists P(x,w) [New]               (new static index)
  rule 4s as rule 4, but dP(x,y) is routed by the static degree of y in C (FLOWLOG_SPLIT_T):
          light     -> C by c0=y -> exists P(x,w) [New]
          heavy     -> P by c0=x [New] -> C by (c0,c1)=(y,w)           (new static indexes)
  rule 4p as rule 4, but dP(x,y) in the other fixed order, with no router:
          dP(x,y)   -> P by c0=x [New] -> C by (c0,c1)=(y,w)           (new static index)
  rule 6  Q(x,e,o) :- Q(x,y,z), R(y,u,e), Q(z,u,o). removes I3 = pi[x,z,u,e](Q |x|_y R) by (z,u)
          dQ(x,y,z) -> R by y -> Q by (c0,c1)=(z,u) [Old]
          dQ(z,u,o) -> R by c1=u -> Q by (c2,c1)=(z,y) [New]          (new static index)
"""

import os
import sys
from pathlib import Path

from _edit import insert_before, remove_statement

rules = {rule.strip() for rule in os.environ.get("RULES", "6").split(",") if rule.strip()}
assert rules and rules <= {"3", "4", "4s", "4p", "6"}, rules
if len(rules & {"4", "4s", "4p"}) > 1:
    sys.exit(f"{sys.argv[0]}: RULES may name at most one of 4, 4s and 4p")
SCOPE_INDENT = "\n" + " " * 28  # between statements of the dataflow scope
LOOP_INDENT = "\n" + " " * 40  # between statements inside the loop

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
statics = []  # built outside the loop
enters = []  # entered into the loop
paths = []  # delta paths, placed before the dedups

if "3" in rules:
    for name in ["t_10905337257110834278", "t_10905337257110834278_arr", "t_7901157479956461833"]:
        code = remove_statement(code, name)
    statics.append('''let x_u_by_c0c1 = ::flowlog_runtime::operators::flowlog_map(
                                rel_4_u.clone(),
                                "Arrange: K:(V0, V1), V:(V2) <- U",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x0, x1), (x2,)), t, d)),
                            );
                            let x_u_by_c0c1_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_u_by_c0c1,
                                "Arrange: u by (c0,c1) (delta rule 3)",
                            );''')
    enters.append("let in_x_u_by_c0c1_arr = x_u_by_c0c1_arr.clone().enter(inner);")
    paths.append('''// Rule 3, path dP(y,w): U by w, then Q(x,r,y) by (y,r), earlier updates only.
                                        let r3_p1 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP (rule 3)"),
                                                    in_t_342161247075010433_arr.clone(),
                                                    "Lookup: dP * U by w",
                                                    Version::New,
                                                    |&(_y, w): &(i32, i32)| (w,),
                                                    |&(y, _w): &(i32, i32), rv: &(i32, i32)| Some((y, rv.0, rv.1)),
                                                ),
                                                t_13170221501169253067_arr.clone(),
                                                "Lookup: dP * U * Q by (y,r) [Old]",
                                                Version::Old,
                                                |&(y, r, _z): &(i32, i32, i32)| (y, r),
                                                |&(_y, _r, z): &(i32, i32, i32), rv: &(i32,)| Some((rv.0, z)),
                                            ),
                                            "Land: rule 3 path P",
                                        );
                                        // Rule 3, path dQ(x,r,y): P by y (this iteration included), then U by (w,r).
                                        let r3_p2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_1_q.clone(), "Path: dQ (rule 3)"),
                                                    t_11377078873479422098_arr.clone(),
                                                    "Lookup: dQ * P by y [New]",
                                                    Version::New,
                                                    |&(_x, _r, y): &(i32, i32, i32)| (y,),
                                                    |&(x, r, _y): &(i32, i32, i32), rv: &(i32,)| Some((x, r, rv.0)),
                                                ),
                                                in_x_u_by_c0c1_arr.clone(),
                                                "Lookup: dQ * P * U by (w,r)",
                                                Version::New,
                                                |&(_x, r, w): &(i32, i32, i32)| (w, r),
                                                |&(x, _r, _w): &(i32, i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: rule 3 path Q",
                                        );
                                        let t_7901157479956461833 = r3_p1.concatenate([r3_p2]);''')

if "4s" in rules:
    # Rule 4 with path dP(x,y) split by the static degree of y in C. Light rows (deg <= T) go as
    # rule 4 does: C by y proposes (w,z), then P(x,w) [New] checks. Heavy rows propose w from
    # P by x [New] instead, then C by (y,w) gives z. T comes from FLOWLOG_SPLIT_T (default 64).
    for name in ["t_11513539025394583662", "t_11513539025394583662_arr", "t_7337068469573900806"]:
        code = remove_statement(code, name)
    statics.append('''let x_c_by_c0 = ::flowlog_runtime::operators::flowlog_map(
                                rel_3_c.clone(),
                                "Arrange: K:(V0), V:(V1, V2) <- C",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x0,), (x1, x2)), t, d)),
                            );
                            let x_c_by_c0_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_c_by_c0,
                                "Arrange: c by c0 (delta rule 4)",
                            );
                            let x_c_by_c0c1 = ::flowlog_runtime::operators::flowlog_map(
                                rel_3_c.clone(),
                                "Arrange: K:(V0, V1), V:(V2) <- C",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x0, x1), (x2,)), t, d)),
                            );
                            let x_c_by_c0c1_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_c_by_c0c1,
                                "Arrange: c by (c0,c1) (split rule 4)",
                            );
                            let x_c_deg = {
                                use ::flowlog_runtime::differential_dataflow::operators::CountTotal;
                                ::flowlog_runtime::operators::flowlog_map(
                                    rel_3_c.clone(),
                                    "Degree: C by c0",
                                    |(x0, _x1, _x2): (i32, i32, i32), t, _d| std::iter::once(((x0,), t, 1isize)),
                                )
                                .count_total()
                            };
                            let x_c_deg = ::flowlog_runtime::operators::flowlog_map(
                                x_c_deg,
                                "Degree: K:(V0), V:(count) <- C",
                                |((x0,), n): ((i32,), isize), t, _d: isize| {
                                    std::iter::once((((x0,), (n as i32,)), t, ::flowlog_runtime::diff::Static))
                                },
                            );
                            let x_c_deg_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_c_deg,
                                "Arrange: degree of c by c0 (split rule 4)",
                            );''')
    enters.append("let in_x_c_by_c0_arr = x_c_by_c0_arr.clone().enter(inner);")
    enters.append("let in_x_c_by_c0c1_arr = x_c_by_c0c1_arr.clone().enter(inner);")
    enters.append("let in_x_c_deg_arr = x_c_deg_arr.clone().enter(inner);")
    paths.append('''// Rule 4, path dP(x,w): C by w, then P(x,y) exists, earlier updates only.
                                        let r4_p1 = flowlog_land(
                                            flowlog_lookup_exists(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP2 (rule 4)"),
                                                    in_t_16735833253390438580_arr.clone(),
                                                    "Lookup: dP2 * C by w",
                                                    Version::New,
                                                    |&(_x, w): &(i32, i32)| (w,),
                                                    |&(x, _w): &(i32, i32), rv: &(i32, i32)| Some((x, rv.0, rv.1)),
                                                ),
                                                t_12746466074610925576_arr.clone(),
                                                "Exists: dP2 * C * P(x,y) [Old]",
                                                Version::Old,
                                                |&(x, y, _z): &(i32, i32, i32)| (x, y),
                                            ),
                                            "Land: rule 4 path P2",
                                        )
                                        .map(|(x, _y, z)| (x, z));
                                        // Rule 4, path dP(x,y): tag each row with the degree of y in C (rows
                                        // whose y is not in C have no match and drop out here), then route.
                                        let split_t: i32 = std::env::var("FLOWLOG_SPLIT_T")
                                            .ok()
                                            .and_then(|v| v.parse().ok())
                                            .unwrap_or(64);
                                        let r4_tag = flowlog_lookup(
                                            flowlog_path(recursive_rel_0_p.clone(), "Path: dP3 (rule 4)"),
                                            in_x_c_deg_arr.clone(),
                                            "Lookup: dP3 * degree of C by y",
                                            Version::New,
                                            |&(_x, y): &(i32, i32)| (y,),
                                            move |&(x, y): &(i32, i32), rv: &(i32,)| Some((x, y, rv.0 > split_t)),
                                        );
                                        let r4_light = ::flowlog_runtime::operators::flowlog_map(
                                            r4_tag.clone(),
                                            "Route: dP3 light",
                                            |((x, y, heavy), at): ((i32, i32, bool), _), t, d| {
                                                if heavy { None } else { Some((((x, y), at), t, d)) }
                                            },
                                        );
                                        let r4_heavy = ::flowlog_runtime::operators::flowlog_map(
                                            r4_tag,
                                            "Route: dP3 heavy",
                                            |((x, y, heavy), at): ((i32, i32, bool), _), t, d| {
                                                if heavy { Some((((x, y), at), t, d)) } else { None }
                                            },
                                        );
                                        // Light: C by y proposes (w,z), P(x,w) checks (this iteration included).
                                        let r4_p2l = flowlog_land(
                                            flowlog_lookup_exists(
                                                flowlog_lookup(
                                                    r4_light,
                                                    in_x_c_by_c0_arr.clone(),
                                                    "Lookup: dP3 light * C by y",
                                                    Version::New,
                                                    |&(_x, y): &(i32, i32)| (y,),
                                                    |&(x, _y): &(i32, i32), rv: &(i32, i32)| Some((x, rv.0, rv.1)),
                                                ),
                                                t_12746466074610925576_arr.clone(),
                                                "Exists: dP3 light * C * P(x,w) [New]",
                                                Version::New,
                                                |&(x, w, _z): &(i32, i32, i32)| (x, w),
                                            ),
                                            "Land: rule 4 path P3 light",
                                        )
                                        .map(|(x, _w, z)| (x, z));
                                        // Heavy: P by x proposes w (this iteration included), C by (y,w) gives z.
                                        let r4_p2h = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    r4_heavy,
                                                    t_11377078873479422098_arr.clone(),
                                                    "Lookup: dP3 heavy * P by x [New]",
                                                    Version::New,
                                                    |&(x, _y): &(i32, i32)| (x,),
                                                    |&(x, y): &(i32, i32), rv: &(i32,)| Some((x, y, rv.0)),
                                                ),
                                                in_x_c_by_c0c1_arr.clone(),
                                                "Lookup: dP3 heavy * P * C by (y,w)",
                                                Version::New,
                                                |&(_x, y, w): &(i32, i32, i32)| (y, w),
                                                |&(x, _y, _w): &(i32, i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: rule 4 path P3 heavy",
                                        );
                                        let t_7337068469573900806 = r4_p1.concatenate([r4_p2l, r4_p2h]);''')

if "4p" in rules:
    # Rule 4 with path dP(x,y) in the other fixed order and no router: P by x [New] proposes w,
    # then C by (y,w) gives z.
    for name in ["t_11513539025394583662", "t_11513539025394583662_arr", "t_7337068469573900806"]:
        code = remove_statement(code, name)
    statics.append('''let x_c_by_c0c1 = ::flowlog_runtime::operators::flowlog_map(
                                rel_3_c.clone(),
                                "Arrange: K:(V0, V1), V:(V2) <- C",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x0, x1), (x2,)), t, d)),
                            );
                            let x_c_by_c0c1_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_c_by_c0c1,
                                "Arrange: c by (c0,c1) (rule 4, P first)",
                            );''')
    enters.append("let in_x_c_by_c0c1_arr = x_c_by_c0c1_arr.clone().enter(inner);")
    paths.append('''// Rule 4, path dP(x,w): C by w, then P(x,y) exists, earlier updates only.
                                        let r4_p1 = flowlog_land(
                                            flowlog_lookup_exists(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP2 (rule 4)"),
                                                    in_t_16735833253390438580_arr.clone(),
                                                    "Lookup: dP2 * C by w",
                                                    Version::New,
                                                    |&(_x, w): &(i32, i32)| (w,),
                                                    |&(x, _w): &(i32, i32), rv: &(i32, i32)| Some((x, rv.0, rv.1)),
                                                ),
                                                t_12746466074610925576_arr.clone(),
                                                "Exists: dP2 * C * P(x,y) [Old]",
                                                Version::Old,
                                                |&(x, y, _z): &(i32, i32, i32)| (x, y),
                                            ),
                                            "Land: rule 4 path P2",
                                        )
                                        .map(|(x, _y, z)| (x, z));
                                        // Rule 4, path dP(x,y): P by x proposes w (this iteration included), C by (y,w) gives z.
                                        let r4_p2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP3 (rule 4)"),
                                                    t_11377078873479422098_arr.clone(),
                                                    "Lookup: dP3 * P by x [New]",
                                                    Version::New,
                                                    |&(x, _y): &(i32, i32)| (x,),
                                                    |&(x, y): &(i32, i32), rv: &(i32,)| Some((x, y, rv.0)),
                                                ),
                                                in_x_c_by_c0c1_arr.clone(),
                                                "Lookup: dP3 * P * C by (y,w)",
                                                Version::New,
                                                |&(_x, y, w): &(i32, i32, i32)| (y, w),
                                                |&(x, _y, _w): &(i32, i32, i32), rv: &(i32,)| Some((x, rv.0)),
                                            ),
                                            "Land: rule 4 path P3",
                                        );
                                        let t_7337068469573900806 = r4_p1.concatenate([r4_p2]);''')

if "4" in rules:
    for name in ["t_11513539025394583662", "t_11513539025394583662_arr", "t_7337068469573900806"]:
        code = remove_statement(code, name)
    statics.append('''let x_c_by_c0 = ::flowlog_runtime::operators::flowlog_map(
                                rel_3_c.clone(),
                                "Arrange: K:(V0), V:(V1, V2) <- C",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x0,), (x1, x2)), t, d)),
                            );
                            let x_c_by_c0_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_c_by_c0,
                                "Arrange: c by c0 (delta rule 4)",
                            );''')
    enters.append("let in_x_c_by_c0_arr = x_c_by_c0_arr.clone().enter(inner);")
    paths.append('''// Rule 4, path dP(x,w): C by w, then P(x,y) exists, earlier updates only.
                                        let r4_p1 = flowlog_land(
                                            flowlog_lookup_exists(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP2 (rule 4)"),
                                                    in_t_16735833253390438580_arr.clone(),
                                                    "Lookup: dP2 * C by w",
                                                    Version::New,
                                                    |&(_x, w): &(i32, i32)| (w,),
                                                    |&(x, _w): &(i32, i32), rv: &(i32, i32)| Some((x, rv.0, rv.1)),
                                                ),
                                                t_12746466074610925576_arr.clone(),
                                                "Exists: dP2 * C * P(x,y) [Old]",
                                                Version::Old,
                                                |&(x, y, _z): &(i32, i32, i32)| (x, y),
                                            ),
                                            "Land: rule 4 path P2",
                                        )
                                        .map(|(x, _y, z)| (x, z));
                                        // Rule 4, path dP(x,y): C by y, then P(x,w) exists (this iteration included).
                                        let r4_p2 = flowlog_land(
                                            flowlog_lookup_exists(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_0_p.clone(), "Path: dP3 (rule 4)"),
                                                    in_x_c_by_c0_arr.clone(),
                                                    "Lookup: dP3 * C by y",
                                                    Version::New,
                                                    |&(_x, y): &(i32, i32)| (y,),
                                                    |&(x, _y): &(i32, i32), rv: &(i32, i32)| Some((x, rv.0, rv.1)),
                                                ),
                                                t_12746466074610925576_arr.clone(),
                                                "Exists: dP3 * C * P(x,w) [New]",
                                                Version::New,
                                                |&(x, w, _z): &(i32, i32, i32)| (x, w),
                                            ),
                                            "Land: rule 4 path P3",
                                        )
                                        .map(|(x, _w, z)| (x, z));
                                        let t_7337068469573900806 = r4_p1.concatenate([r4_p2]);''')

if "6" in rules:
    for name in ["t_15051508251197390845", "t_15051508251197390845_arr", "t_3980751149897468841"]:
        code = remove_statement(code, name)
    statics.append('''let x_r_by_c1 = ::flowlog_runtime::operators::flowlog_map(
                                rel_2_r.clone(),
                                "Arrange: K:(V1), V:(V0, V2) <- R",
                                |(x0, x1, x2): (i32, i32, i32), t, d| std::iter::once((((x1,), (x0, x2)), t, d)),
                            );
                            let x_r_by_c1_arr = ::flowlog_runtime::operators::flowlog_arrange(
                                x_r_by_c1,
                                "Arrange: r by c1 (delta rule 6)",
                            );''')
    enters.append("let in_x_r_by_c1_arr = x_r_by_c1_arr.clone().enter(inner);")
    paths.append('''// Rule 6, path dQ(x,y,z): R by y, then Q(z,u,o) by (z,u), earlier updates only.
                                        let r6_p1 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_1_q.clone(), "Path: dQ1 (rule 6)"),
                                                    in_t_3853768777351039812_arr.clone(),
                                                    "Lookup: dQ1 * R by y",
                                                    Version::New,
                                                    |&(_x, y, _z): &(i32, i32, i32)| (y,),
                                                    |&(x, _y, z): &(i32, i32, i32), rv: &(i32, i32)| Some((x, z, rv.0, rv.1)),
                                                ),
                                                t_13694067113765097382_arr.clone(),
                                                "Lookup: dQ1 * R * Q by (z,u) [Old]",
                                                Version::Old,
                                                |&(_x, z, u, _e): &(i32, i32, i32, i32)| (z, u),
                                                |&(x, _z, _u, e): &(i32, i32, i32, i32), rv: &(i32,)| Some((x, e, rv.0)),
                                            ),
                                            "Land: rule 6 path Q1",
                                        );
                                        // Rule 6, path dQ(z,u,o): R by u, then Q(x,y,z) by (z,y) (this iteration included).
                                        let r6_p2 = flowlog_land(
                                            flowlog_lookup(
                                                flowlog_lookup(
                                                    flowlog_path(recursive_rel_1_q.clone(), "Path: dQ3 (rule 6)"),
                                                    in_x_r_by_c1_arr.clone(),
                                                    "Lookup: dQ3 * R by u",
                                                    Version::New,
                                                    |&(_z, u, _o): &(i32, i32, i32)| (u,),
                                                    |&(z, _u, o): &(i32, i32, i32), rv: &(i32, i32)| Some((z, o, rv.0, rv.1)),
                                                ),
                                                t_13170221501169253067_arr.clone(),
                                                "Lookup: dQ3 * R * Q by (z,y) [New]",
                                                Version::New,
                                                |&(z, _o, y, _e): &(i32, i32, i32, i32)| (z, y),
                                                |&(_z, o, _y, e): &(i32, i32, i32, i32), rv: &(i32,)| Some((rv.0, e, o)),
                                            ),
                                            "Land: rule 6 path Q3",
                                        );
                                        let t_3980751149897468841 = r6_p1.concatenate([r6_p2]);''')

uses = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_lookup_exists, flowlog_land, Version};"
if not rules & {"4", "4s", "4p"}:
    uses = uses.replace(" flowlog_lookup_exists,", "")
code = insert_before(
    code,
    "let (rel_0_p, rel_1_q) = scope",
    "// DELTA-JOIN EXPERIMENT: static indexes the delta paths read." + SCOPE_INDENT + SCOPE_INDENT.join(statics) + SCOPE_INDENT,
)
code = insert_before(
    code,
    "let (recursive_rel_0_p_var, recursive_rel_0_p) = Variable::<",
    LOOP_INDENT.join(enters) + LOOP_INDENT,
)
code = insert_before(
    code,
    "let next_2148319068693452643 =",
    f"// DELTA-JOIN EXPERIMENT (GALEN rules {','.join(sorted(rules))}): one path per recursive atom."
    + LOOP_INDENT + uses + LOOP_INDENT + LOOP_INDENT.join(paths) + LOOP_INDENT,
)
main_rs.write_text(code)

