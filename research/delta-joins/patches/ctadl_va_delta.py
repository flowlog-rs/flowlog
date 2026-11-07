"""CTADL's VirtualAssign rules: the binary plan or a delta join, picked per rule at run time (CTADL.md).

Eight rules of CTADL's index phase, for JADX and P-code alike, have the shape

    VirtualAssign_choice(...) :- Vertex(v, p), VirtualAssign_choice(... v ...), ..., CAPNES(...).

CAPNES is ComputedAccessPathNonEmptySplit; in program.fl the rules carry choice ids 371-374 and
377-380 for JADX, 370-373 and 376-379 for P-code. FlowLog plans each as I = A |><| B by v, arranged
by C's key, then I |><| C. A is Vertex and B a view of VirtualAssign_choice, both without global
variables, and C is a view of CAPNES. All three change inside the loop, so the binary plan holds I.
The delta join replaces I with one path per atom (atom order A, B, C: a path reads the atoms before
its own as New, the atoms after it as Old):

    dA: B by v [Old] -> C by its key [Old]
    dB: A by v [New] -> C by its key [Old]
    dC: B by C's key [New] (a new index) -> A by v [New]

Every rule keeps its binary plan as well, and the binary picks one when it builds its dataflow, so
one build runs every configuration:

    DJ_VA=r1,r5,...    the rules to delta-join, or all (default: none)
    DJ_VA_ORDER=cost   dA and dB probe C first, through two new indexes of C by the driving atom's
                       columns (C is small); otherwise they hop as above, the text order
"""

import re
import sys
from pathlib import Path

from _edit import find, statement_end

# Each rule's intermediate I; JADX's index and P-code's give them the same names.
RULES = {
    "r1": "t_5566722344065419961",  # p1 = "", p2 != "", v1 != v2
    "r2": "t_17647071896910041152",  # p1 = "", p2 != ""
    "r3": "t_12900665211573367572",  # p1 != "", p2 = "", v1 != v2
    "r4": "t_559396743761194383",  # p1 != "", p2 = ""
    "r5": "t_10859189629111168972",  # p1 = "", v1 != v2
    "r6": "t_17735140767449333032",  # p1 = ""
    "r7": "t_14374907034803111120",  # p2 = "", v1 != v2
    "r8": "t_9037523228127469135",  # p2 = ""
}
JOIN = re.compile(
    r"let (\w+) = ::flowlog_runtime::operators::flowlog_join\(\s*(\w+)\.clone\(\),\s*(\w+)\.clone\(\),"
    r"\s*\"(?:[^\"\\]|\\.)*\",\s*\|(\w+), (\w+), (\w+)\|\s*(\{.*\}),\s*\);$",
    re.DOTALL,
)


def fail(message):
    sys.exit(f"{sys.argv[0]}: {message}")


def statement(code, name):
    start = find(code, f"let {name} = ")
    return start, statement_end(code, start) + 1


def join_parts(text):
    """A two-way join statement's output, left and right arrangements, closure parameters and body."""
    found = JOIN.match(text)
    if not found:
        fail(f"not a two-way join: {text[:120]!r}")
    out, left, right, k, lv, rv, body = found.groups()
    return out, left, right, (k, lv, rv), body


def collection(code, arr):
    """The collection that `let ARR = flowlog_arrange(COLL.clone(), ...)` arranges."""
    start, end = statement(code, arr)
    found = re.match(rf"let {arr} = ::flowlog_runtime::operators::flowlog_arrange\(\s*(\w+)\.clone\(\),",
                     code[start:end])
    if not found:
        fail(f"{arr} is not a plain arrangement")
    return found.group(1)


def bind(names, values, body):
    """BODY, a join closure's body, with its parameters bound to VALUES."""
    lets = "".join(f"let {n} = {v}; " for n, v in zip(names, values))
    return f"{{ {lets}let _ = ({', '.join(names)}); {body} }}"


def tup(items):
    return "(" + "".join(f"{x}, " for x in items) + ")"


def rekey(rule, coll, cols, what):
    """COLL, a (key, value) collection, arranged by the given columns as (key, (key, value))."""
    return (f"::flowlog_runtime::operators::flowlog_arrange("
            f"::flowlog_runtime::operators::flowlog_map({coll}.clone(), \"DJ {rule}: {what}\", "
            f"|(k, v), t, d| std::iter::once((({tup(cols)}, (k, v)), t, d))), \"Arrange: DJ {rule} {what}\")")


def delta(code, rule, inter):
    i_start, i_end = statement(code, inter)
    _, a_arr, b_arr, p1, body1 = join_parts(code[i_start:i_end])
    arr_start, arr_end = statement(code, f"{inter}_arr")
    # The binary branch keeps the intermediate's statements, which nothing else may read.
    if code[i_end:arr_start].strip() or code.count(f"{inter}.clone()") != 1 or code.count(f"{inter}_arr.clone()") != 1:
        fail(f"{rule}'s intermediate {inter} is read elsewhere")
    second = re.search(rf"let (\w+) = ::flowlog_runtime::operators::flowlog_join\(\s*{inter}_arr\.clone\(\),", code)
    if not second:
        fail(f"{rule}: no join reads {inter}_arr (already patched?)")
    o_start = second.start()
    o_end = statement_end(code, o_start) + 1
    out, _, c_arr, p2, body2 = join_parts(code[o_start:o_end])
    a, b, c = (collection(code, x) for x in (a_arr, b_arr, c_arr))

    # I's key, which is C's: each column is A's (lv.i) or B's (rv.j).
    key = re.match(r"\{\s*Some\(\(\s*\(([^()]*)\)", body1)
    cols = [x.strip() for x in key.group(1).split(",") if x.strip()] if key else []
    if not cols or any(not re.fullmatch(rf"({p1[1]}|{p1[2]})\.\d+", x) for x in cols):
        fail(f"{rule}: unexpected intermediate key {cols}")
    from_a = [(p, x.split(".")[1]) for p, x in enumerate(cols) if x.startswith(p1[1] + ".")]
    from_b = [(p, x.split(".")[1]) for p, x in enumerate(cols) if x.startswith(p1[2] + ".")]

    def f1(k, va, vb):
        return bind(p1, (k, va, vb), body1)

    def f2(k, v, vc):
        return bind(p2, (k, v, vc), body2)

    def f2_checked(row, kc, vc):
        """f2 on ROW, f1's (key, value), when its key is C's KC."""
        return f"match {row} {{ Some((k2, v2)) if k2 == {kc} => {f2('&k2', '&v2', vc)}, _ => None }}"

    def lookup(rows, arr, version, keyf, logic, what):
        return (f"flowlog_lookup({rows}, {arr}.clone(), \"DJ {rule}: {what}\", Version::{version}, "
                f"|d| {keyf}, |d, x| {logic})")

    def path(coll, what):
        return f"flowlog_path({coll}.clone(), \"DJ {rule}: {what}\")"

    def land(rows, what):
        return f"flowlog_land({rows}, \"DJ {rule}: land {what}\")"

    pair = "Some((d.clone(), x.clone()))"
    text_a = land(lookup(lookup(path(a, "dA"), b_arr, "Old", "d.0.clone()", f1("&d.0", "&d.1", "x"), "dA*B"),
                         c_arr, "Old", "d.0.clone()", f2("&d.0", "&d.1", "x"), "dA*B*C"), "dA")
    text_b = land(lookup(lookup(path(b, "dB"), a_arr, "New", "d.0.clone()", f1("&d.0", "x", "&d.1"), "dB*A"),
                         c_arr, "Old", "d.0.clone()", f2("&d.0", "&d.1", "x"), "dB*A*C"), "dB")
    path_c = land(lookup(lookup(path(c, "dC"), "dj_b_by", "New", tup(f"d.0.{p}" for p, _ in from_b), pair, "dC*B"),
                         a_arr, "New", "d.1.0.clone()",
                         f"{{ let r = {f1('&d.1.0', 'x', '&d.1.1')}; {f2_checked('r', 'd.0.0', '&d.0.1')} }}",
                         "dC*B*A"), "dC")
    if from_a:
        cost_a = land(lookup(lookup(path(a, "dA"), "dj_c_by_a", "Old", tup(f"d.1.{i}" for _, i in from_a), pair,
                                    "dA*C"),
                             b_arr, "Old", "d.0.0.clone()",
                             f"{{ let r = {f1('&d.0.0', '&d.0.1', 'x')}; {f2_checked('r', 'd.1.0', '&d.1.1')} }}",
                             "dA*C*B"), "dA")
        c_by_a = f"let dj_c_by_a = {rekey(rule, c, [f'k.{p}' for p, _ in from_a], 'C by A cols')};"
    else:
        cost_a, c_by_a = text_a, ""
    cost_b = land(lookup(lookup(path(b, "dB"), "dj_c_by_b", "Old", tup(f"d.1.{j}" for _, j in from_b), pair, "dB*C"),
                         a_arr, "New", "d.0.0.clone()",
                         f"{{ let r = {f1('&d.0.0', 'x', '&d.0.1')}; {f2_checked('r', 'd.1.0', '&d.1.1')} }}",
                         "dB*C*A"), "dB")
    c_by_b = f"let dj_c_by_b = {rekey(rule, c, [f'k.{p}' for p, _ in from_b], 'C by B cols')};"
    b_by = rekey(rule, b, [f"v.{j}" for _, j in from_b], "B by C key")
    binary = code[i_start:arr_end] + "\n" + code[o_start:o_end][len(f"let {out} = "):-1]
    new = f"""// DELTA-JOIN EXPERIMENT ({rule}): DJ_VA picks the delta join, DJ_VA_ORDER=cost its hop order.
let {out} = if ::std::env::var("DJ_VA").map_or(false, |s| s.split(',').any(|r| r == "{rule}" || r == "all")) {{
    use ::flowlog_runtime::operators::{{flowlog_path, flowlog_lookup, flowlog_land, Version}};
    let dj_b_by = {b_by};
    let path_c = {path_c};
    let (path_a, path_b) = if ::std::env::var("DJ_VA_ORDER").map_or(false, |s| s == "cost") {{
        {c_by_a}
        {c_by_b}
        ({cost_a}, {cost_b})
    }} else {{
        ({text_a}, {text_b})
    }};
    path_a.concatenate([path_b, path_c])
}} else {{
    {binary}
}};"""
    # The intermediate's statements move into the binary branch, at the second join (C's
    # arrangement may come between them).
    return code[:i_start] + code[arr_end:o_start] + new + code[o_end:]


main_rs = Path(sys.argv[1])
code = main_rs.read_text()
for rule, inter in RULES.items():
    code = delta(code, rule, inter)
main_rs.write_text(code)
