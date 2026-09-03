#!/usr/bin/env python3
"""Judge each A/B/C session's window AFTER the fact, from the committed sampler timeseries.

WHY POST HOC RATHER THAN IN-RUN. `ws0-baseline.sh --quiescence-timeseries` REFUSES a session
whose window shows any competing process, and a refused session leaves a NON-EMPTY `--out`
directory that the driver can never retry into (`--out` is refused if non-empty), so one peer
gate starting mid-set permanently strands a round. On a box shared with nine other delivery
lanes that is a routine event, not a corner case: of this issue's three sets, one was hit at
round 1 arm 4. So the sets are run WITHOUT that flag — which the rig records honestly as
`quiescence: NOT VERIFIED` — and the window is judged here instead, from the same committed
census the in-run gate would have used.

WHAT THIS BUYS AND WHAT IT COSTS. It buys a verdict for every session including the
contaminated ones, which is strictly more information than a set that died at round 1. It
costs the in-run REFUSAL: nothing stopped a contaminated session from being measured, so the
verdicts below must be read BEFORE the figures, and a contaminated session's figures are
reported as contaminated rather than quietly averaged in.

WHAT IT IS NOT. `competing_count == 0` bounds compilers, linkers and a named script — NOT
total foreign load (issue #3551 D3: 91 consecutive samples read 0 while `load1` reached 6.39
and the pinned CPUs measured a median 8% busy). It is not a substitute for the drift control,
which is what actually decides whether a treatment delta is readable.

AND THE PINNED-CPU COLUMN IS NOT A CONTAMINATION DETECTOR *DURING* A WINDOW — a first draft of
this tool implied it was, which would have been a claim it cannot support. `/proc/stat` reports
TOTAL busy per CPU, and during a session the pinned CPUs are busy BY DESIGN: that is the
measured server and the measured scan. Measured here, sessions read 42-46% while the same CPUs
read a median 8% when idle, so the column is dominated by the measurement itself and cannot
separate a peer's cycles from ours. What it IS good for: an UNDER-loaded session (a window that
should be saturated and is not) is visible, and the PRE-window baseline does bound foreign load
because nothing of ours is running then. Foreign load arriving mid-session is caught by the
census when it is a compiler or linker, and by nothing here when it is not.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

PINS = (2, 3, 10, 11)


def load_series(path: pathlib.Path) -> list[dict]:
    rows = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        raise SystemExit(f"REFUSED: {path} holds no samples")
    return rows


def busy_between(a: dict, b: dict, cpu: int) -> float | None:
    """Busy fraction of `cpu` between two cumulative /proc/stat snapshots.

    Returns None rather than 0.0 when it cannot be computed — an absent snapshot and an idle
    CPU are different facts and only one of them is a measurement (#3625's rule, one directory
    over).
    """
    pa, pb = (a.get("percpu") or {}).get(str(cpu)), (b.get("percpu") or {}).get(str(cpu))
    if not pa or not pb:
        return None
    dt = pb["total"] - pa["total"]
    di = pb["idle"] - pa["idle"]
    if dt <= 0:
        return None
    return 100.0 * (dt - di) / dt


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", required=True, help="a set directory holding r<N>-<arm>/")
    ap.add_argument("--timeseries", required=True, help="the sampler JSONL")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)

    rows = load_series(pathlib.Path(args.timeseries))
    sessions = []
    for d in sorted(pathlib.Path(args.root).glob("r*-*")):
        w = d / "abc-window.json"
        if not w.is_file():
            continue
        rec = json.loads(w.read_text())
        sessions.append((d.name, rec))
    if not sessions:
        raise SystemExit(f"REFUSED: no abc-window.json under {args.root}")

    out = []
    out.append("| session | arm | round | pos | window (UTC) | in-window samples | competing | "
               "verdict | pinned-CPU busy, TOTAL incl. our own (2,3,10,11) |")
    out.append("|---|---|--:|--:|---|--:|--:|---|--:|")
    dirty_sessions = []
    for name, rec in sorted(sessions, key=lambda s: s[1]["started"]):
        lo, hi = rec["started"], rec["ended"]
        win = [r for r in rows if lo <= r["ts"] <= hi]
        comp = sum(1 for r in win if r.get("competing_count"))
        busies = []
        for a, b in zip(win, win[1:]):
            vals = [busy_between(a, b, c) for c in PINS]
            vals = [v for v in vals if v is not None]
            if vals:
                busies.append(sum(vals) / len(vals))
        busies.sort()
        med = f"{busies[len(busies)//2]:.1f}%" if busies else "NOT MEASURED"
        if not win:
            verdict = "NOT MEASURED (no sample covers this window)"
        elif comp:
            verdict = f"**CONTAMINATED** ({comp} of {len(win)})"
            dirty_sessions.append(name)
        else:
            verdict = "clean (census 0)"
        out.append(f"| `{name}` | {rec['arm']} | {rec['round']} | {rec['position_in_round']} | "
                   f"{lo} → {hi} | {len(win)} | {comp} | {verdict} | {med} |")

    out.append("")
    if dirty_sessions:
        out.append(f"**{len(dirty_sessions)} of {len(sessions)} session(s) CONTAMINATED**: "
                   + ", ".join(f"`{s}`" for s in dirty_sessions))
    else:
        out.append(f"**All {len(sessions)} sessions clean** — every in-window sample recorded "
                   "`competing_count = 0`.")
    out.append("")
    out.append("`competing_count` bounds compilers, linkers and the `agent-gate.sh` script "
               "and NOT total foreign load (issue #3551 D3), and it does not replace the drift "
               "control, which is what decides readability.")
    out.append("")
    out.append("The pinned-CPU column is TOTAL busy and is dominated by THIS MEASUREMENT during a "
               "session (measured: 42-46% in-session against a median 8% idle), so it does NOT "
               "separate a peer's cycles from ours and is NOT a contamination bound. It is "
               "reported because an UNDER-loaded session is a real failure this makes visible.")

    text = "\n".join(out)
    print(text)
    if args.out:
        pathlib.Path(args.out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
