"""Doop: replace binary plans with delta-join paths (held-out workloads, REPORT.md section 11).

RULES selects the rules to convert, comma-separated: ifpt, and at most one of ac and acv
(default "ac").
Static atoms are ordered first, so recursive paths see them whole and their own paths are empty
(static updates sit at iteration 0, recursive ones start at 1). The recursive atoms are ordered
Reachable, then VarPointsTo (VPT); path i sees earlier atoms with Version::New (<=) and later ones
with Version::Old (<).

  ac   VPT(heap,to) :- Reachable(m), AssignCast(type,from,to,m), SupertypeOf(type,heaptype),
                       HeapAllocation_Type(heap,heaptype), VPT(heap,from).
       removes I1 = pi[type,from,to](Reachable |x| AssignCast)   by type,
               I2 = pi[from,to,heaptype](I1 |x| SupertypeOf)      by heaptype,
               I3 = I2 |x| HeapAllocation_Type                    by (heap,from)
       dReachable(m)   -> AssignCast by m -> SupertypeOf by type -> HeapAllocation_Type by heaptype
                          -> exists VPT(heap,from) [Old]         (the hops in the rule's order)
       dVPT(heap,from) -> AssignCast by from -> HeapAllocation_Type by heap
                          -> exists SupertypeOf(type,heaptype) -> exists Reachable(m) [New]
                                                                 (new static index)
  acv  as ac, but path dReachable probes VPT before checking types (the cheaper order):
       dReachable(m)   -> AssignCast by m -> VPT by from [Old] -> HeapAllocation_Type by heap
                          -> exists SupertypeOf(type,heaptype)
       No path then reads VPT by (heap,var), an index only this rule's binary plan built, so it
       goes too, with the two static indexes only I2 and I3 read.
  ifpt IFPT(heap,fld,baseheap) :- Reachable(m), StoreInstanceField(from,base,fld,m),
                                   VPT(heap,from), VPT(baseheap,base).
       removes I1 = pi[from,base,fld](Reachable |x| StoreInstanceField) by from,
               I2 = pi[base,fld,heap](I1 |x| VPT)                        by base
       dReachable(m)       -> StoreInstanceField by m -> VPT by from [Old] -> VPT by base [Old]
       dVPT(heap,from)     -> StoreInstanceField by from -> exists Reachable(m) [New]
                              -> VPT by base [Old]               (new static index)
       dVPT(baseheap,base) -> StoreInstanceField by base -> exists Reachable(m) [New]
                              -> VPT by from [New]               (new static index)

Each hop is its own statement, so patches/count.py can count the rows between hops.
"""

import os
import re
import sys
from pathlib import Path

from _edit import find, insert_before, remove_statement, statement_end

rules = {rule.strip() for rule in os.environ.get("RULES", "ac").split(",") if rule.strip()}
assert rules and rules <= {"ac", "acv", "ifpt"}, rules
if {"ac", "acv"} <= rules:
    sys.exit(f"{sys.argv[0]}: RULES may name at most one of ac and acv")
SCOPE_INDENT = "\n" + " " * 28  # between statements of the dataflow scope
LOOP_INDENT = "\n" + " " * 40  # between statements inside the loop

# Generated names: recursive collections, and arrangements the paths read.
REACHABLE = "recursive_rel_98_reachable"
VPT = "recursive_rel_93_varpointsto"
REACHABLE_ARR = "t_17306484026126467169_arr"  # Reachable by m
VPT_BY_VAR = "t_2989372398964609559_arr"  # VPT(heap,var) by var
VPT_FULL = "t_235791700371182460_arr"  # VPT by (heap,var)
AC_BY_M = "in_t_1295387479783563197_arr"  # AssignCast(type,from,to,m) by m
ST_BY_TYPE = "in_t_2833359784610549514_arr"  # SupertypeOf(type,heaptype) by type
ST_FULL = "in_t_378177296609617801_arr"  # SupertypeOf by (type,heaptype)
HAT_BY_HEAP = "in_t_8433576658136945321_arr"  # HeapAllocation_Type(heap,heaptype) by heap
HAT_BY_TYPE = "in_t_9903784546044934961_arr"  # HeapAllocation_Type by heaptype
SIF_BY_M = "in_t_10429105980340420777_arr"  # StoreInstanceField(from,base,fld,m) by m

S1, S2, S3, S4, S5 = ("(String,)", "(String, String)", "(String, String, String)",
                      "(String, String, String, String)", "(String, String, String, String, String)")


def lookup(prefixes, arranged, name, version, row, key, value, emit):
    """A flowlog_lookup hop: `key` and `emit` are expressions in `row` (the path row, of type
    `row`) and, for `emit`, `rv` (the arranged value, of type `value`)."""
    used_row = "row" if "row." in emit else "_row"
    used_rv = "rv" if "rv." in emit else "_rv"
    return f'''flowlog_lookup(
    {prefixes},
    {arranged}.clone(),
    "Lookup: {name}",
    Version::{version},
    |row: &{row}| {key},
    |{used_row}: &{row}, {used_rv}: &{value}| Some({emit}),
)'''


def exists(prefixes, arranged, name, version, row, key):
    return f'''flowlog_lookup_exists(
    {prefixes},
    {arranged}.clone(),
    "Lookup: {name}",
    Version::{version},
    |row: &{row}| {key},
)'''


def path(rows, name):
    return f'flowlog_path({rows}.clone(), "Path: {name}")'


def land(rows, name):
    return f'flowlog_land(\n    {rows},\n    "Land: {name}",\n)'


def let(name, expr):
    """`let NAME = EXPR;`, indented for the loop."""
    return f"let {name} = {expr};".replace("\n", LOOP_INDENT)


def after_statement(code, name, new):
    """Inserts `new` right after the statement `let NAME = ...;`."""
    end = statement_end(code, find(code, f"let {name} = ")) + 1
    return code[:end] + new + code[end:]


def prune(code, names):
    """Removes each statement `let NAME = ...;` whose NAME nothing else reads, until none is left."""
    while unread := [name for name in names
                     if f"let {name} = " in code and len(re.findall(rf"\b{name}\b", code)) == 1]:
        for name in unread:
            code = remove_statement(code, name)
    return code


def static_index(name, relation, row, key, value):
    """A static arrangement built before the loop: `name` and `name_arr`."""
    return f'''let {name} = ::flowlog_runtime::operators::flowlog_map(
    {relation}.clone(),
    "Arrange: {name}",
    |{row}: {S4}, t, d| std::iter::once((({key}, {value}), t, d)),
);
let {name}_arr = ::flowlog_runtime::operators::flowlog_arrange(
    {name},
    "Arrange: {name} (delta paths)",
);'''.replace("\n", SCOPE_INDENT)


def enter(name):
    return f"let in_{name}_arr = {name}_arr.clone().enter(inner);"


main_rs = Path(sys.argv[1])
code = main_rs.read_text()
paths = []  # placed before the dedups
pruned = []  # statements to drop once nothing reads them

if rules & {"ac", "acv"}:
    for name in ["t_4110473446694679862", "t_4110473446694679862_arr", "t_10312393721571487248",
                 "t_10312393721571487248_arr", "t_3222412152729144609", "t_3222412152729144609_arr",
                 "t_7091137557361490329"]:
        code = remove_statement(code, name)
    pruned += [VPT_FULL, "t_235791700371182460", ST_BY_TYPE, ST_BY_TYPE[3:], ST_BY_TYPE[3:-4],
               HAT_BY_TYPE, HAT_BY_TYPE[3:], HAT_BY_TYPE[3:-4]]
    code = after_statement(code, AC_BY_M[3:], SCOPE_INDENT + static_index(
        "x_assigncast_by_from", "rel_52_assigncast", "(x0, x1, x2, x3)", "(x1,)", "(x0, x2, x3)"))
    code = after_statement(code, AC_BY_M, LOOP_INDENT + enter("x_assigncast_by_from"))
    paths.append("// AssignCast, path dReachable(m); rows (type,from,to) after AssignCast.")
    paths.append(let("ac_r1", lookup(path(REACHABLE, "dReachable (AssignCast)"), AC_BY_M,
                                     "dReachable * AssignCast by m", "New", S1, "(row.0.clone(),)", S3,
                                     "(rv.0.clone(), rv.1.clone(), rv.2.clone())")))
    if "ac" in rules:
        paths.append(let("ac_r2", lookup("ac_r1", ST_BY_TYPE, "dReachable * AC * SupertypeOf by type", "New",
                                         S3, "(row.0.clone(),)", S1,
                                         "(rv.0.clone(), row.1.clone(), row.2.clone())")))  # (heaptype,from,to)
        paths.append(let("ac_r3", lookup("ac_r2", HAT_BY_TYPE, "dReachable * AC * ST * HAT by heaptype", "New",
                                         S3, "(row.0.clone(),)", S1,
                                         "(rv.0.clone(), row.1.clone(), row.2.clone())")))  # (heap,from,to)
        paths.append(let("ac_r", land(lookup("ac_r3", VPT_FULL, "dReachable * AC * ST * HAT * VPT(heap,from) [Old]",
                                             "Old", S3, "(row.0.clone(), row.1.clone())", "()",
                                             "(row.0.clone(), row.2.clone())"), "AssignCast path dReachable")))
    else:
        paths.append(let("ac_r2", lookup("ac_r1", VPT_BY_VAR, "dReachable * AC * VPT by from [Old]", "Old",
                                         S3, "(row.1.clone(),)", S1,
                                         "(row.0.clone(), rv.0.clone(), row.2.clone())")))  # (type,heap,to)
        paths.append(let("ac_r3", lookup("ac_r2", HAT_BY_HEAP, "dReachable * AC * VPT * HAT by heap", "New",
                                         S3, "(row.1.clone(),)", S1,
                                         "(row.0.clone(), rv.0.clone(), row.1.clone(), row.2.clone())")))
        paths.append(let("ac_r", land(lookup("ac_r3", ST_FULL, "dReachable * AC * VPT * HAT * SupertypeOf(type,heaptype)",
                                             "New", S4, "(row.0.clone(), row.1.clone())", "()",
                                             "(row.2.clone(), row.3.clone())"), "AssignCast path dReachable")))
    paths.append("// AssignCast, path dVPT(heap,from); rows (type,heap,to,m) after AssignCast.")
    paths.append(let("ac_v1", lookup(path(VPT, "dVPT (AssignCast)"), "in_x_assigncast_by_from_arr",
                                     "dVPT * AssignCast by from", "New", S2, "(row.1.clone(),)", S3,
                                     "(rv.0.clone(), row.0.clone(), rv.1.clone(), rv.2.clone())")))
    paths.append(let("ac_v2", lookup("ac_v1", HAT_BY_HEAP, "dVPT * AC * HAT by heap", "New", S4,
                                     "(row.1.clone(),)", S1,
                                     "(row.0.clone(), rv.0.clone(), row.1.clone(), row.2.clone(), row.3.clone())")))
    paths.append(let("ac_v3", lookup("ac_v2", ST_FULL, "dVPT * AC * HAT * SupertypeOf(type,heaptype)", "New", S5,
                                     "(row.0.clone(), row.1.clone())", "()",
                                     "(row.4.clone(), row.2.clone(), row.3.clone())")))  # (m,heap,to)
    paths.append(let("ac_v", land(lookup("ac_v3", REACHABLE_ARR, "dVPT * AC * HAT * ST * Reachable(m) [New]", "New",
                                         S3, "(row.0.clone(),)", "()", "(row.1.clone(), row.2.clone())"),
                                  "AssignCast path dVPT")))
    paths.append("let t_7091137557361490329 = ac_r.concatenate([ac_v]);")

if "ifpt" in rules:
    for name in ["t_5597719949407859147", "t_5597719949407859147_arr", "t_6985478173463357620",
                 "t_6985478173463357620_arr", "t_8202494843590822758"]:
        code = remove_statement(code, name)
    code = after_statement(code, SIF_BY_M[3:], SCOPE_INDENT + SCOPE_INDENT.join([
        static_index("x_sif_by_from", "rel_47_storeinstancefield", "(x0, x1, x2, x3)", "(x0,)", "(x1, x2, x3)"),
        static_index("x_sif_by_base", "rel_47_storeinstancefield", "(x0, x1, x2, x3)", "(x1,)", "(x0, x2, x3)"),
    ]))
    code = after_statement(code, SIF_BY_M, LOOP_INDENT + LOOP_INDENT.join(
        [enter("x_sif_by_from"), enter("x_sif_by_base")]))
    paths.append("// IFPT, path dReachable(m); rows (from,base,fld) after StoreInstanceField.")
    paths.append(let("ifpt_r1", lookup(path(REACHABLE, "dReachable (IFPT)"), SIF_BY_M,
                                       "dReachable * SIF by m", "New", S1, "(row.0.clone(),)", S3,
                                       "(rv.0.clone(), rv.1.clone(), rv.2.clone())")))
    paths.append(let("ifpt_r2", lookup("ifpt_r1", VPT_BY_VAR, "dReachable * SIF * VPT by from [Old]", "Old", S3,
                                       "(row.0.clone(),)", S1,
                                       "(rv.0.clone(), row.1.clone(), row.2.clone())")))  # (heap,base,fld)
    paths.append(let("ifpt_r", land(lookup("ifpt_r2", VPT_BY_VAR, "dReachable * SIF * VPT * VPT by base [Old]",
                                           "Old", S3, "(row.1.clone(),)", S1,
                                           "(row.0.clone(), row.2.clone(), rv.0.clone())"), "IFPT path dReachable")))
    paths.append("// IFPT, path dVPT(heap,from); rows (m,heap,base,fld) after StoreInstanceField.")
    paths.append(let("ifpt_f1", lookup(path(VPT, "dVPT as from (IFPT)"), "in_x_sif_by_from_arr",
                                       "dVPT(from) * SIF by from", "New", S2, "(row.1.clone(),)", S3,
                                       "(rv.2.clone(), row.0.clone(), rv.0.clone(), rv.1.clone())")))
    paths.append(let("ifpt_f2", exists("ifpt_f1", REACHABLE_ARR, "dVPT(from) * SIF * Reachable(m) [New]", "New",
                                       S4, "(row.0.clone(),)")))
    paths.append(let("ifpt_f", land(lookup("ifpt_f2", VPT_BY_VAR, "dVPT(from) * SIF * Reachable * VPT by base [Old]",
                                           "Old", S4, "(row.2.clone(),)", S1,
                                           "(row.1.clone(), row.3.clone(), rv.0.clone())"), "IFPT path dVPT as from")))
    paths.append("// IFPT, path dVPT(baseheap,base); rows (m,baseheap,from,fld) after StoreInstanceField.")
    paths.append(let("ifpt_b1", lookup(path(VPT, "dVPT as base (IFPT)"), "in_x_sif_by_base_arr",
                                       "dVPT(base) * SIF by base", "New", S2, "(row.1.clone(),)", S3,
                                       "(rv.2.clone(), row.0.clone(), rv.0.clone(), rv.1.clone())")))
    paths.append(let("ifpt_b2", exists("ifpt_b1", REACHABLE_ARR, "dVPT(base) * SIF * Reachable(m) [New]", "New",
                                       S4, "(row.0.clone(),)")))
    paths.append(let("ifpt_b", land(lookup("ifpt_b2", VPT_BY_VAR, "dVPT(base) * SIF * Reachable * VPT by from [New]",
                                           "New", S4, "(row.2.clone(),)", S1,
                                           "(rv.0.clone(), row.3.clone(), row.1.clone())"), "IFPT path dVPT as base")))
    paths.append("let t_8202494843590822758 = ifpt_r.concatenate([ifpt_f, ifpt_b]);")

uses = "use ::flowlog_runtime::operators::{flowlog_path, flowlog_lookup, flowlog_lookup_exists, flowlog_land, Version};"
if "ifpt" not in rules:
    uses = uses.replace(" flowlog_lookup_exists,", "")
code = insert_before(
    code,
    "let next_1907318099776307745 =",
    f"// DELTA-JOIN EXPERIMENT (Doop rules {','.join(sorted(rules))}): one path per recursive atom."
    + LOOP_INDENT + uses + LOOP_INDENT + LOOP_INDENT.join(paths) + LOOP_INDENT,
)
code = prune(code, pruned)
main_rs.write_text(code)
