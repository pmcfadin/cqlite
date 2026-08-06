#!/usr/bin/env python3
"""`ws0_assert_fixed_inputs_recorded.py <results.json>` — every Flight rep RECORDS what was verified.

#3272 review round 11, F3. The refusal cases prove the reporter REJECTS a record produced under
different conditions; this proves the accepted ones SAY what conditions they were accepted under.
The distinction matters because the figures are conditional on those inputs: a reader comparing two
sessions' ratios needs to see both were concurrency-one, full-shape, single-step, v1-schema runs, and
"the reporter checked something" is not that.

The expected values are read from the SHIPPED `FIXED_INPUTS` rather than restated here — a literal
copy would be a second source of truth, and the one that drifts is the one in the test.

VACUITY IS REFUSED: a `results.json` in which no rep carries the block fails, because an empty
`reps` list would otherwise satisfy every assertion in a loop over it.
"""

from __future__ import annotations

import json
import pathlib
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: ws0_assert_fixed_inputs_recorded.py <results.json>", file=sys.stderr)
        return 2
    here = pathlib.Path(__file__).resolve().parent
    sys.path.insert(0, str(here.parent / "perf"))
    from ws0_loadgen_record import FIXED_INPUTS  # noqa: E402  (path set above)

    try:
        res = json.loads(pathlib.Path(argv[1]).read_text())
    except (OSError, ValueError) as exc:
        print(f"{argv[1]} is not readable JSON: {exc}", file=sys.stderr)
        return 1

    # Walked from `measurements` -> `reps`, which is where the per-arm blocks and their per-rep
    # records actually live. TWO wrong guesses were made writing this and BOTH were caught by the
    # vacuity guard below rather than passing silently — `res["arms"]` (that key holds the
    # SELECTION, a list of arm names) and a `per_rep` sub-key (the real one is `reps`). That is the
    # guard earning its place: a walk over the wrong shape yields zero reps, and zero reps would
    # satisfy every assertion in a loop over them.
    reps = [
        rep
        for arm in res.get("measurements", [])
        if isinstance(arm, dict)
        for rep in arm.get("reps", [])
        if isinstance(rep, dict) and "verified_fixed_inputs" in rep
    ]
    if not reps:
        # The vacuity guard: a loop over an empty list passes every assertion inside it.
        print("no rep in results.json carries `verified_fixed_inputs`, so nothing recorded WHAT was"
              " verified — and a loop over zero reps would satisfy every check below",
              file=sys.stderr)
        return 1
    if not FIXED_INPUTS:
        print("FIXED_INPUTS is empty, so this check has no subject", file=sys.stderr)
        return 1

    for rep in reps:
        got = rep["verified_fixed_inputs"]
        if not isinstance(got, dict):
            print(f"rep {rep.get('rep')}: verified_fixed_inputs is {type(got).__name__}, not an"
                  " object", file=sys.stderr)
            return 1
        for key, (want, _why) in FIXED_INPUTS.items():
            if key not in got:
                print(f"rep {rep.get('rep')}: verified_fixed_inputs records no {key!r}, so the"
                      " report does not state that input's value", file=sys.stderr)
                return 1
            if got[key] != want:
                print(f"rep {rep.get('rep')}: recorded {key}={got[key]!r}, expected {want!r}",
                      file=sys.stderr)
                return 1
    print(f"{len(reps)} rep(s) each record all {len(FIXED_INPUTS)} verified fixed input(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
