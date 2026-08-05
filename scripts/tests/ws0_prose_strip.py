#!/usr/bin/env python3
"""`strip_prose` — the banned-idiom scan's prose filter, in ONE place (#3272 round 3 nit).

Used by `scripts/tests/test_ws0_fabrication_guards.sh`, whose STRUCTURAL scan greps the
executable source of every `scripts/perf/ws0_*.py` for permissive-default idioms
(`get('cycles', 0)`, `if errors > 0`, `float('inf')`, …).

The scan needs a filter because the fix comments and the DIAGNOSTICS necessarily quote the
idiom they refuse — `ws0_collect.py` really does say "the check used to be `if errors > 0`"
inside an `Invalid(...)` — so a literal grep over the raw file reds on its own
documentation, and the reflex fix for that is to stop documenting.

# Why this is a module rather than three heredocs

It was three: the assertion, its non-vacuity probe, and the strip's own test each carried a
copy. That is three things to keep in sync, and their divergence would be invisible in
exactly the permissive direction — a non-vacuity probe testing a DIFFERENT strip than the
assertion uses proves nothing about the assertion. One implementation, imported by all
three.

# The rule, and the two ways it has been got wrong

A string constant is PROSE when it is a LITERAL PART of a message reachable from a `raise`.
It is NOT prose when it is an ARGUMENT LITERAL, and the distinction is the whole content of
this file, because both directions have failed in review:

* TOO BROAD (round 2's first attempt): blanking EVERY string constant rewrites
  `rec.get('cycles', 0)` to `rec.get('', 0)` and makes THE WHOLE SCAN VACUOUS. Caught by
  planting a real idiom in a probe module and observing the scan stay green.
* STILL TOO BROAD, inside a `raise` (round 3): blanking every string constant REACHABLE
  from a `raise` catches the interpolated expressions of an f-string.
  `raise Invalid(f"...{rec.get('cycles', 0)}...")` becomes `Invalid(f"{rec.get('', 0)}")`,
  so an idiom written inside a diagnostic's interpolation is HIDDEN FROM THE SCAN — the
  round-2 fix repeating the round-2 defect one level in. MEASURED:

      raise Invalid(f"bad: {rec.get('cycles', 0)} is wrong")
        -> raise Invalid(f"{rec.get('', 0)}")          # the idiom is gone

So the traversal is explicit about WHICH constants under a `raise` are literal text:

* a bare `Constant` string argument (`raise Invalid("...")`) — prose;
* the LITERAL PARTS of a `JoinedStr` (an f-string's non-`{}` segments) — prose;
* everything inside a `FormattedValue.value` (an f-string's `{...}` EXPRESSION) — CODE,
  left alone, because that is where an idiom can hide.

Docstrings are blanked wherever they appear (module, class, function): they are prose by
construction and the scan's subject is EXECUTABLE source.
"""

from __future__ import annotations

import ast


def _prose_string_ids(tree: ast.AST) -> set[int]:
    """The `id()`s of every string Constant that is LITERAL TEXT under a `raise`.

    Walks each `raise` explicitly rather than via `ast.walk`, so an f-string's
    interpolated EXPRESSION is never entered — `ast.walk` cannot make that distinction,
    and that is the round-3 defect.
    """
    prose: set[int] = set()

    def collect(node: ast.AST) -> None:
        """Mark literal text in `node`, descending only where text can be."""
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                prose.add(id(node))
            return
        if isinstance(node, ast.JoinedStr):
            # An f-string: its `values` alternate literal Constants and FormattedValues.
            # Only the literal halves are prose; a FormattedValue's `.value` is CODE and is
            # deliberately NOT entered. Its `format_spec` is a nested JoinedStr of literal
            # text, which is prose — but it cannot contain an idiom, so it is skipped too
            # rather than reasoned about.
            for part in node.values:
                if isinstance(part, ast.Constant) and isinstance(part.value, str):
                    prose.add(id(part))
            return
        if isinstance(node, ast.BinOp):
            # String CONCATENATION of diagnostics (`"a" + f"{x}"`, `"a" * n`): both sides
            # may hold literal text.
            collect(node.left)
            collect(node.right)
            return
        if isinstance(node, ast.Call):
            # `Invalid("...")`, `Invalid(f"..." " ...")`, `Invalid("...".format(x))`: the
            # ARGUMENTS may be diagnostics. The callee is a name, never text.
            for arg in node.args:
                collect(arg)
            for kw in node.keywords:
                collect(kw.value)
            return
        # Anything else (a Name, an Attribute, a Subscript) holds no literal message text.

    for node in ast.walk(tree):
        if isinstance(node, ast.Raise):
            if node.exc is not None:
                collect(node.exc)
            if node.cause is not None:
                collect(node.cause)
    return prose


def strip_prose(source: str) -> str:
    """`source` with DOCSTRINGS and DIAGNOSTIC TEXT blanked, argument literals intact."""
    tree = ast.parse(source)
    prose = _prose_string_ids(tree)
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in prose:
                node.value = ""
    return ast.unparse(ast.fix_missing_locations(tree))
