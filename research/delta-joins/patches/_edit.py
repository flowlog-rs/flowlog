"""Text edits on a generated main.rs, shared by the patches (this file is not a patch).

scripts/build.py runs each patch as `python3 patches/NAME.py MAIN_RS`, with the build's settings
in the environment, and the patch rewrites MAIN_RS in place. Patches find the code they rewrite by
the names FlowLog generates (`t_<hash>`, `rel_<n>_<relation>`). Every anchor must occur exactly
once, so a patch applied to code it does not match stops instead of rewriting the wrong place.
"""

import sys


def find(code, anchor):
    """The position of `anchor`, which must occur exactly once in `code`."""
    count = code.count(anchor)
    if count != 1:
        sys.exit(f"{sys.argv[0]}: expected {anchor!r} once in the generated code, found it {count} times")
    return code.index(anchor)


def replace_span(code, start, end, new):
    """Replaces the code from anchor `start` up to, not including, anchor `end` with `new`."""
    return code[: find(code, start)] + new + code[find(code, end) :]


def insert_before(code, anchor, new):
    """Inserts `new` right before `anchor`."""
    at = find(code, anchor)
    return code[:at] + new + code[at:]


def insert_after(code, anchor, new):
    """Inserts `new` right after `anchor`."""
    at = find(code, anchor) + len(anchor)
    return code[:at] + new + code[at:]


def statement_end(code, start):
    """The position of the `;` that ends the statement starting at `start`.

    Brackets are counted, so a `;` inside a closure body does not end the statement.
    """
    depth = 0
    for at in range(start, len(code)):
        if code[at] in "([{":
            depth += 1
        elif code[at] in ")]}":
            depth -= 1
        elif code[at] == ";" and depth == 0:
            return at
    sys.exit(f"{sys.argv[0]}: unterminated statement at {code[start:start + 60]!r}")


def remove_statement(code, name):
    """Removes the statement `let NAME = ...;`, with its indentation and line break."""
    start = find(code, f"let {name} = ")
    end = statement_end(code, start)
    line_start = code.rindex("\n", 0, start) + 1
    return code[:line_start] + code[end + 2 :]
