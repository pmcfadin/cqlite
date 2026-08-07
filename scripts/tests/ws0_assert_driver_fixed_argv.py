#!/usr/bin/env python3
"""`ws0_assert_driver_fixed_argv.py <repo-root>` — the DRIVER passes what the REPORTER demands.

#3272 review round 11, F3, and it is the half that keeps F3 from becoming F1.

F3 makes the reporter REQUIRE `target_concurrency == 1` and `shape == "full"`. Those values are only
correct because the driver's `flight-loadgen` invocation passes `--ramp 1 --shape full`. If the driver
ever moved to `--ramp 2`, the reporter would refuse EVERY REAL RUN — which is precisely round 9 F1 /
round 10 L1 / round 11 F1, a guard making the documented command unrunnable, three times over. A
reject-direction test cannot see it: refusing every real record satisfies every refusal case.

So the two are asserted to AGREE, with the expected values read from the shipped `FIXED_INPUTS` and
the actual ones read from `lib-measure.sh`'s real argv. Neither side is restated here.

The subject is the MEASURED invocation — located by `--round`, which the prewarm leg also carries, so
both legs are checked. That is deliberate rather than sloppy: the prewarm leg is also `--ramp 1
--shape full`, and a driver that ramped its prewarm would be warming with a different workload than
it measures.

VACUITY IS REFUSED: if no invocation can be located, this fails rather than passing over an empty
list of things to check.
"""

from __future__ import annotations

import pathlib
import re
import sys


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: ws0_assert_driver_fixed_argv.py <repo-root>", file=sys.stderr)
        return 2
    root = pathlib.Path(argv[1]).resolve()
    sys.path.insert(0, str(root / "scripts" / "perf"))
    from ws0_loadgen_record import FIXED_INPUTS  # noqa: E402  (path set above)

    lib = root / "scripts" / "perf" / "lib-measure.sh"
    try:
        text = lib.read_text()
    except OSError as exc:
        print(f"{lib} is unreadable ({exc}), so this check's SUBJECT is absent — which prints"
              " exactly like an agreeing driver", file=sys.stderr)
        return 1

    # Logical lines: the invocation spans `\`-continuations, so a physical-line scan would see the
    # command word and its options as separate lines with neither carrying both halves — the same
    # bypass `ws0_hermeticity_lint` records for its own predicate.
    joined = re.sub(r"\\\n\s*", " ", text)
    calls = [ln for ln in joined.splitlines() if "flight-loadgen" in ln and "--round" in ln]
    if not calls:
        print("could not locate a `flight-loadgen … --round …` invocation in lib-measure.sh, so"
              " nothing was compared — a check with no subject is not a passing check",
              file=sys.stderr)
        return 1

    want_ramp = FIXED_INPUTS["target_concurrency"][0]
    want_shape = FIXED_INPUTS["shape"][0]
    for call in calls:
        if f"--ramp {want_ramp}" not in call:
            print(f"the reporter requires target_concurrency == {want_ramp!r}, but this driver"
                  f" invocation does not pass `--ramp {want_ramp}`: {call.strip()[:200]}."
                  " A reporter demanding a value the driver stopped producing would refuse EVERY"
                  " REAL RUN (#3272 F1's defect class).", file=sys.stderr)
            return 1
        if f"--shape {want_shape}" not in call:
            print(f"the reporter requires shape == {want_shape!r}, but this driver invocation does"
                  f" not pass `--shape {want_shape}`: {call.strip()[:200]}", file=sys.stderr)
            return 1
    print(f"{len(calls)} driver loadgen invocation(s) pass --ramp {want_ramp} --shape {want_shape},"
          " agreeing with the reporter's FIXED_INPUTS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
