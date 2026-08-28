#!/usr/bin/env python3
"""STRUCTURAL assert: a peak verdict is COMPUTED, never asserted.

`derive.py` must have no channel — flag, file or otherwise — by which a
bracketing verdict can be supplied by hand.

Why this is a guard and not a style preference. An earlier revision accepted a
`verdict-override.json`, and **its contents were entirely true**: it carried the
real extension evidence, verified independently. That is precisely what makes it
worth a structural check — **the mechanism is wrong even when the instance is
honest**:

  1. Nothing could verify the `why` prose against reality, so a stale or
     mistaken file would have printed `bracketed` with a straight face.
  2. The bracketing rule was PRE-REGISTERED before the data was seen, so that
     the verdict could not be chosen afterwards. An override channel hands that
     discretion straight back.
  3. It concealed the real gap — the tool could not read the extension trees —
     by asserting the conclusion instead of computing it, so the tool stayed
     unable to do the job while the report's claim rested on a JSON file.

The repo's own rule is the same shape: *"an escape hatch on a measurement guard
can only ever buy a confident wrong number."* A file-based hatch is no better
than an environment variable, and worse here, because the field it overrides is
a VERDICT.
"""

import ast
import sys

# The single sanctioned producer: it applies the pre-registered rule to an
# extension tree's own MEASURED points.
SANCTIONED_PRODUCER = "extension_verdicts"
# `merge_extension_verdicts` combines the producer's output across trees and
# REFUSES a disagreement; it manufactures no verdict of its own, so it is
# sanctioned as a conduit. Nothing else may feed the mapping.
SANCTIONED_PRODUCERS = (SANCTIONED_PRODUCER, "merge_extension_verdicts")
VERDICT_NAMES = ("verdicts", "derived_verdicts", "overrides")


def main(path):
    tree = ast.parse(open(path).read())
    bad = []

    for n in ast.walk(tree):
        # No CLI surface may mention an override.
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument":
            for a in n.args:
                if isinstance(a, ast.Constant) and isinstance(a.value, str) \
                        and "override" in a.value.lower():
                    bad.append(f"line {n.lineno}: CLI option {a.value!r} offers an override")
        # The verdict mapping may only ever be fed by the sanctioned producer.
        if (isinstance(n, ast.Call)
                and getattr(n.func, "attr", "") in ("update", "setdefault")
                and isinstance(getattr(n.func, "value", None), ast.Name)
                and n.func.value.id in VERDICT_NAMES):
            for a in n.args:
                if not (isinstance(a, ast.Call)
                        and getattr(a.func, "id", "") in SANCTIONED_PRODUCERS):
                    bad.append(f"line {n.lineno}: the verdict mapping is fed by something "
                               f"other than {SANCTIONED_PRODUCER}()")
        # ...AND THE SAME FOR AN ASSIGNMENT. The mapping is now BUILT by
        # `merge_extension_verdicts(...)` rather than accumulated with `.update()`,
        # so a check that only inspected update/setdefault would have let
        # `verdicts = json.load(...)` straight through — the file-based hatch this
        # module exists to keep out, reintroduced by a refactor that never touched
        # the hatch. Every CALL inside the assigned expression must be a
        # sanctioned producer.
        if isinstance(n, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id in VERDICT_NAMES for t in n.targets):
            for c in ast.walk(n.value):
                if isinstance(c, ast.Call) and getattr(c.func, "id", "") not in SANCTIONED_PRODUCERS:
                    where = getattr(c.func, "id", None) or ast.dump(c.func)
                    bad.append(f"line {n.lineno}: the verdict mapping is assigned from "
                               f"{where} — only {SANCTIONED_PRODUCERS} may produce one")
        # No verdict may be read out of a file.
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") in ("load", "loads") \
                and isinstance(getattr(n.func, "value", None), ast.Name) \
                and n.func.value.id == "json":
            src = ast.dump(n)
            if "verdict" in src.lower():
                bad.append(f"line {n.lineno}: a verdict appears to be read from a file")

    for b in bad:
        print("VERDICT-CHANNEL-FAIL", b)
    if bad:
        return 1
    print(f"VERDICT-CHANNEL-OK {path}: verdicts are computed by "
          f"{SANCTIONED_PRODUCER}() alone; no hand-written channel exists")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
