#!/usr/bin/env python3
"""Box-quiescence gate for a WS0 measurement rep (issue #3248).

WHY THIS EXISTS
---------------
The rig's own README states the limit it hit: "this rig produces no reusable absolute", after
an untouched warm bare scan read 370,134 rows/s and, an hour later, 333,206 — ~10% drift with
nothing changed on the measured path. What the rig does NOT model is that its box is SHARED
between delivery lanes. Observed while preparing #3248: `load1` reached 108 on 16 vCPUs with
~17 concurrent `rustc`, from a peer lane's gate.

There is a measured positive control for the mechanism, from #3299 at an identical S=1/N=1
point: co-scheduled 2.470 GHz vs quiescent 3.268-3.291 GHz — a **25% frequency reduction from
co-scheduled load alone**, with only 2 logical CPUs pinned. So load need not be HIGH to be
FATAL, which is why this gate keys on a competing-process CENSUS and not on load alone.

#3299's admitted gap is the one this closes: its quiescence was **PROCEDURAL, not MEASURED** —
"I never logged load per rep", so it could not correlate its own +-3% residual against load
even in hindsight. A rep that records its own quiescence can.

WHAT IT DOES AND DOES NOT ESTABLISH
-----------------------------------
It makes "the box was quiet" a CHECKABLE COLUMN rather than a claim. It does **not** establish
that quiescence is sufficient for a reusable absolute: #3299 measured +-3% residual drift under
enforced quiescence, and the drift REVERSES SIGN between within-session and across-session
scales, which a pure load response cannot do. That residual is unexplained and this tool does
not claim to explain it.

WHAT THIS GATE CANNOT SEE, STATED RATHER THAN LEFT TO BE DISCOVERED
------------------------------------------------------------------
The census deliberately does NOT include `cqlite-flight`, `flight-loadgen`, `ws0-scan-bench` or
`perf`, because THIS rig runs exactly those — including them would make the gate refuse its own
run. The consequence is precise and worth naming: **a PEER LANE's WS0 measurement is invisible
to this gate.** Two concurrent measurement sessions would each see a clean census and each
contaminate the other.

That hazard is handled by COORDINATION (a reserved box) and not by this check, and coordination
is a weaker mechanism than a lock. #3299 supplied the motivating evidence from its own
experience: two `sweep.sh` invocations against one results tree, where the second deleted the
first's evidence while its stop file ended the first's workers mid-window — and **every process
exited 0**. A cross-lane measurement mutex is the right fix and is proposed as a follow-up rather
than built here, because it is fleet tooling and outside this issue's scope.

THE RULE, AND WHY A MOVEMENT BOUND SITS BESIDE THE LEVEL BOUND
-------------------------------------------------------------
A rep is accepted only if, at BOTH boundaries: the competing-process census is zero, `load1` is
at or below the level bound, and `load1` moved by no more than the movement bound between them.
The movement bound is deliberately tighter than the level bound because **a CHANGING box is
worse than a uniformly busy one**: it breaks the interleaving that makes an A/B comparison
readable, so a rep whose load moved mid-flight is INVALID rather than merely slow.

Thresholds are ARGUMENTS with defaults, not constants, so the accepted value is recorded in the
artifact and a reader can judge it instead of trusting it. They may only be made STRICTER.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Dict, List, Optional

# Comm names that mean another lane is compiling or gating on this box. Matched against
# /proc/<pid>/comm EXACTLY. `comm` is capped at 15 characters by the kernel, so a longer
# executable name can never appear here in full — which is why the gate ALSO reads
# /proc/<pid>/cmdline for the gate script (below), and why `pgrep -x` is not a usable
# alternative for those (it "will result in zero matches", as pkill itself warns).
COMPETING_COMMS = ("rustc", "cargo", "cc1", "cc1plus", "ld", "lld", "mold", "sccache")

# Substrings searched in /proc/<pid>/cmdline. Needed for things whose comm is `bash` or
# `python3` and therefore indistinguishable from anything else by comm alone.
COMPETING_CMDLINE = ("agent-gate.sh", "cargo build", "cargo test", "cargo nextest")

DEFAULT_MAX_LOAD1 = 2.0
DEFAULT_MAX_LOAD1_MOVEMENT = 0.5


class NotQuiescent(Exception):
    """A named refusal. `cause` is a stable token so a test can assert it."""

    def __init__(self, cause: str, detail: str) -> None:
        super().__init__(f"{cause}: {detail}")
        self.cause = cause
        self.detail = detail


def _read_loadavg() -> Dict[str, float]:
    text = pathlib.Path("/proc/loadavg").read_text().split()
    return {"load1": float(text[0]), "load5": float(text[1]), "load15": float(text[2]),
            "runnable": text[3]}


def census(self_pid: Optional[int] = None) -> List[Dict[str, str]]:
    """Every competing process on this box, by comm OR cmdline.

    Read from /proc directly and NOT via `pgrep -f`: a `-f` pattern matches the census
    command's OWN cmdline and inflates the very count it is measuring. That defect was
    observed in the first version of this lane's sampler, where the field read `0\\n0`.
    """
    found: List[Dict[str, str]] = []
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if self_pid is not None and pid == self_pid:
            continue
        try:
            comm = (entry / "comm").read_text().strip()
        except OSError:
            continue  # the process exited between listdir and read; not a competitor
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace").strip()
        except OSError:
            cmdline = ""
        # Never count THIS tool or its own parent shell as a competitor.
        if "ws0_quiescence" in cmdline:
            continue
        why = ""
        if comm in COMPETING_COMMS:
            why = f"comm={comm}"
        else:
            for needle in COMPETING_CMDLINE:
                if needle in cmdline:
                    why = f"cmdline~{needle}"
                    break
        if why:
            found.append({"pid": str(pid), "comm": comm, "why": why,
                          "cmdline": cmdline[:160]})
    return found


def sample(self_pid: Optional[int] = None) -> Dict[str, object]:
    """One quiescence observation: load + the competing-process census."""
    load = _read_loadavg()
    comp = census(self_pid)
    return {"load": load, "competing_count": len(comp), "competing": comp}


def judge(before: Dict[str, object], after: Dict[str, object], *,
          max_load1: float, max_movement: float) -> Dict[str, object]:
    """Accept or refuse a rep from its two boundary samples. Refusal is fail-closed."""
    for name, s in (("before", before), ("after", after)):
        comp = s["competing"]
        if comp:
            names = ", ".join(f"{c['comm']}({c['pid']},{c['why']})" for c in comp[:8])
            raise NotQuiescent(
                "QUIESCENCE_COMPETING_PROCESSES",
                f"{len(comp)} competing process(es) at the {name} boundary: {names}"
                f"{' ...' if len(comp) > 8 else ''}. Co-scheduled load costs up to 25%"
                " FREQUENCY even with only 2 logical CPUs pinned (#3299's measured control),"
                " so this is refused on presence, not on load.",
            )
    l1_before = before["load"]["load1"]
    l1_after = after["load"]["load1"]
    for name, value in (("before", l1_before), ("after", l1_after)):
        if value > max_load1:
            raise NotQuiescent(
                "QUIESCENCE_LOAD_TOO_HIGH",
                f"load1 at the {name} boundary is {value} (> {max_load1}).",
            )
    movement = abs(l1_after - l1_before)
    if movement > max_movement:
        raise NotQuiescent(
            "QUIESCENCE_LOAD_MOVED",
            f"load1 moved {movement:.2f} between the boundaries ({l1_before} -> {l1_after},"
            f" bound {max_movement}). A rep whose load moved mid-flight is INVALID, not slow:"
            " it breaks the interleaving that makes the A/B comparison readable.",
        )
    return {
        "verdict": "QUIESCENT",
        "load1_before": l1_before,
        "load1_after": l1_after,
        "load1_movement": movement,
        "competing_before": 0,
        "competing_after": 0,
        "thresholds": {"max_load1": max_load1, "max_load1_movement": max_movement},
        "before": before,
        "after": after,
    }


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_s = sub.add_parser("sample", help="write one boundary sample")
    p_s.add_argument("--out", required=True)

    p_j = sub.add_parser("judge", help="accept or refuse a rep from two boundary samples")
    p_j.add_argument("--before", required=True)
    p_j.add_argument("--after", required=True)
    p_j.add_argument("--out", default=None)
    for p in (p_j,):
        p.add_argument("--max-load1", type=float, default=DEFAULT_MAX_LOAD1)
        p.add_argument("--max-load1-movement", type=float,
                       default=DEFAULT_MAX_LOAD1_MOVEMENT)

    args = ap.parse_args(argv)

    if args.cmd == "sample":
        rec = sample(self_pid=None)
        pathlib.Path(args.out).write_text(json.dumps(rec, indent=2, sort_keys=True) + "\n")
        load = rec["load"]
        print(f"ws0_quiescence: sampled load1={load['load1']} "
              f"competing={rec['competing_count']}")
        return 0

    # The knobs may only TIGHTEN. A looser threshold is the escape hatch a measurement guard
    # must not have: it can only ever buy a confident wrong number.
    if args.max_load1 > DEFAULT_MAX_LOAD1:
        print(f"ws0_quiescence: REFUSED: QUIESCENCE_THRESHOLD_LOOSENED: --max-load1"
              f" {args.max_load1} exceeds the maximum {DEFAULT_MAX_LOAD1}; this knob may"
              " only tighten", file=sys.stderr)
        return 2
    if args.max_load1_movement > DEFAULT_MAX_LOAD1_MOVEMENT:
        print(f"ws0_quiescence: REFUSED: QUIESCENCE_THRESHOLD_LOOSENED:"
              f" --max-load1-movement {args.max_load1_movement} exceeds the maximum"
              f" {DEFAULT_MAX_LOAD1_MOVEMENT}; this knob may only tighten", file=sys.stderr)
        return 2

    try:
        before = json.loads(pathlib.Path(args.before).read_text())
        after = json.loads(pathlib.Path(args.after).read_text())
    except (OSError, ValueError) as exc:
        print(f"ws0_quiescence: REFUSED: QUIESCENCE_SAMPLE_UNREADABLE: {exc}",
              file=sys.stderr)
        return 1

    for label, s in (("before", before), ("after", after)):
        if not isinstance(s, dict) or "load" not in s or "competing" not in s:
            print(f"ws0_quiescence: REFUSED: QUIESCENCE_SAMPLE_INCOMPLETE: the {label}"
                  " sample carries no load/census. An unverifiable rep is not a usable one.",
                  file=sys.stderr)
            return 1

    try:
        rec = judge(before, after, max_load1=args.max_load1,
                    max_movement=args.max_load1_movement)
    except NotQuiescent as exc:
        print(f"ws0_quiescence: REFUSED: {exc.cause}: {exc.detail}", file=sys.stderr)
        return 1

    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(rec, indent=2, sort_keys=True) + "\n")
    print(f"ws0_quiescence: {rec['verdict']} load1 {rec['load1_before']} -> "
          f"{rec['load1_after']} (moved {rec['load1_movement']:.2f} <= "
          f"{args.max_load1_movement}), competing 0 at both boundaries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
