#!/usr/bin/env python3
"""STRUCTURAL assert: every child process is spawned under a cleanup guarantee.

Behavioural tests only cover the paths someone thought of. This asks a
structural question instead — *where is the call?* — so a NEW spawn added
anywhere outside a `try` whose `finally` can reap it fails regardless of how it
is written.

Why it exists: two consecutive review rounds found the same class of defect in
this harness. Round 1, `launch_workers` sat one line outside its `try`, so a
partial worker spawn was orphaned. Round 2, the perf `Popen` and its FIFO
descriptors had no `finally` at all, so a `die()` between spawn and reap left
perf running on the counted CPU set. Each fix was correct and the family kept
regenerating, which is the signature of a shape that was never named. This names
it: **a process may not be created outside a construct that is guaranteed to
clean it up.**
"""

import ast
import sys


# Only `Popen` can leak. `run`/`call`/`check_call`/`check_output` block until the
# child exits and reap it themselves, so they have no lifetime to guarantee —
# excluding them is a property of the API, not a convenience.
SPAWNERS = ("Popen",)


def spawning_calls(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            name = getattr(f, "attr", None) or getattr(f, "id", None)
            if name in SPAWNERS:
                yield node


def guarded_spans(tree):
    """Line ranges of every `try` that has a `finally`."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Try) and node.finalbody:
            body_lines = [n.lineno for stmt in node.body for n in ast.walk(stmt)
                          if hasattr(n, "lineno")]
            if body_lines:
                yield min(body_lines), max(body_lines)


def handed_to_caller(tree, call):
    """True if the handle is appended into a list the CALLER owns.

    The second legitimate pattern, and the fix round 1 landed: a helper that
    takes an in/out list and appends each child as it starts, so the CALLER's
    `finally` can reap even a PARTIAL spawn. Recognised precisely — the append
    target must be one of the enclosing function's own PARAMETERS, so appending
    to a local list (the shape that caused the original orphan leak, because the
    local is lost when the function raises) is still rejected.
    """
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(getattr(n, "lineno", None) == call.lineno for n in ast.walk(fn)):
            continue
        params = {a.arg for a in fn.args.args} | {a.arg for a in fn.args.kwonlyargs}
        for node in ast.walk(fn):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "append"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in params
                    and any(getattr(n, "lineno", None) == call.lineno
                            for n in ast.walk(node))):
                return True
    return False


def main(paths):
    bad = 0
    for path in paths:
        src = open(path).read()
        tree = ast.parse(src)
        spans = list(guarded_spans(tree))
        for call in spawning_calls(tree):
            if any(lo <= call.lineno <= hi for lo, hi in spans):
                continue
            if handed_to_caller(tree, call):
                continue
            if True:
                print(f"LIFECYCLE-FAIL {path}:{call.lineno}: a child process is spawned "
                      f"outside any try/finally. A die() or exception between the spawn and "
                      f"its reap leaks the process; an orphan holding a counted CPU set "
                      f"corrupts the NEXT rep with the cause already invisible.")
                bad += 1
    if bad:
        return 1
    print(f"LIFECYCLE-OK every child spawn in {len(paths)} file(s) is inside a try/finally")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
