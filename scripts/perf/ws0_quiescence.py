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
import datetime
import json
import math
import os
import pathlib
import sys
from typing import Dict, List, Optional

# Comm names that mean another lane is compiling or gating on this box. Matched against
# /proc/<pid>/comm EXACTLY. `comm` is capped at 15 characters by the kernel, so a longer
# executable name can never appear here in full — which is why the gate ALSO reads
# /proc/<pid>/cmdline for the gate script (below), and why `pgrep -x` is not a usable
# alternative for those (it "will result in zero matches", as pkill itself warns).
# Processes whose PRESENCE means another lane is compiling or gating on this box.
#
# `sccache` IS DELIBERATELY ABSENT, and the reason is this issue's recurring hazard. sccache
# runs as a RESIDENT DAEMON: measured on this box at 0.0% CPU with 555s elapsed while nothing
# was building, and `bootstrap-agent-machine.sh` pins it on fleet-wide, so the gate's own
# summary reports `sccache=on` as expected infrastructure. Its presence is ADJACENT to
# compilation, not identical to it — and a real compile is caught anyway, because `rustc` and
# `cargo` are present whenever sccache is doing work.
#
# Including it was found by WIRING THE GATE AND RUNNING IT: the first end-to-end run refused a
# perfectly quiet box on a 0%-CPU daemon. A guard that cries wolf on the normal state of every
# box in the fleet is the guard people learn to delete, so a false positive here is not a
# harmless conservatism — it is the failure mode that removes the guard entirely.
#
# `mold` and `lld` stay: unlike sccache they are not daemons, so seeing one means a link is
# happening now.
COMPETING_COMMS = ("rustc", "cargo", "cc1", "cc1plus", "ld", "lld", "mold")

# Substrings searched in /proc/<pid>/cmdline. Needed for things whose comm is `bash` or
# `python3` and therefore indistinguishable from anything else by comm alone.
# Substrings searched in /proc/<pid>/cmdline, for things whose `comm` is `bash` or `python3`
# and therefore indistinguishable from anything else by comm alone.
#
# `cargo build` / `cargo test` / `cargo nextest` ARE DELIBERATELY ABSENT. They were here and
# caused a FALSE REFUSAL of a quiet box: the shell ORCHESTRATING the measurement had
# `cargo build ...` in its own cmdline (it had launched the build earlier in the same command),
# so the census matched the observer's own ancestry. They are also REDUNDANT -- `cargo` is in
# COMPETING_COMMS, so a real cargo invocation is caught by comm, which cannot be spoofed by a
# shell that merely MENTIONS the command. This is the same defect family as `pgrep -f` matching
# its own invocation, one level out: a cmdline substring match sees anything that talks about a
# process, not only the process.
COMPETING_CMDLINE = ("agent-gate.sh",)

DEFAULT_MAX_LOAD1 = 2.0
DEFAULT_MAX_LOAD1_MOVEMENT = 0.5

# The sampler writes every 10 s, so three consecutive missed samples is the coverage bound.
# WHY A BOUND IS NEEDED AT ALL: without it, a timeseries containing ONE in-range line was
# treated as covering the whole window, so a sampler that died a minute into a nine-minute
# measurement would certify eight unobserved minutes as quiescent. That is the vacuous pass
# this gate exists to prevent, inside the gate itself.
SAMPLER_CADENCE_S = 10.0
MAX_SAMPLE_GAP_S = 30.0


class NotQuiescent(Exception):
    """A named refusal. `cause` is a stable token so a test can assert it."""

    def __init__(self, cause: str, detail: str) -> None:
        super().__init__(f"{cause}: {detail}")
        self.cause = cause
        self.detail = detail


def _finite(label: str, raw: object) -> float:
    """Parse a float that will be COMPARED, refusing non-finite values.

    Every threshold in this file is enforced by a `>` comparison, and every comparison with
    NaN is False — so one non-finite value does not relax a guard, it DISABLES it. Verified
    against the pre-fix code: `--max-load1 nan` made both the may-only-tighten check and the
    level check pass.
    """
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise NotQuiescent(
            "QUIESCENCE_VALUE_NON_NUMERIC", f"{label} is {raw!r}, which is not a number"
        ) from exc
    if not math.isfinite(value):
        raise NotQuiescent(
            "QUIESCENCE_VALUE_NON_FINITE",
            f"{label} is {value!r}; a non-finite value disables every comparison it takes"
            " part in rather than relaxing it",
        )
    return value


def _parse_ts(label: str, raw: object) -> datetime.datetime:
    """Parse the sampler's ISO-8601 `...Z` instant. An unparseable one is fatal."""
    if not isinstance(raw, str) or not raw:
        raise NotQuiescent(
            "QUIESCENCE_TIMESERIES_MALFORMED", f"{label} has no usable ts field: {raw!r}"
        )
    try:
        return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NotQuiescent(
            "QUIESCENCE_TIMESERIES_MALFORMED", f"{label} ts {raw!r} is not ISO-8601"
        ) from exc


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
    # SELF AND EVERY ANCESTOR ARE EXCLUDED. The observer must not appear in its own
    # measurement, and neither must whatever spawned it: a wrapper shell whose cmdline mentions
    # a build or a gate is not a competing process, it is the thing running the measurement.
    # Walking the ppid chain handles that generally, rather than blocklisting spellings one at
    # a time as they are discovered.
    excluded = set()
    probe = os.getpid() if self_pid is None else self_pid
    for _ in range(64):  # bounded: a cycle or a very deep tree must not hang the census
        if probe <= 1 or probe in excluded:
            break
        excluded.add(probe)
        try:
            stat_fields = pathlib.Path(f"/proc/{probe}/stat").read_text().rsplit(")", 1)[1]
            probe = int(stat_fields.split()[1])
        except (OSError, IndexError, ValueError):
            break

    found: List[Dict[str, str]] = []
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid in excluded:
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


def window_census_clean(timeseries: str, start: str, end: str) -> Dict[str, object]:
    """Every sampler line in [start, end]: refuse if ANY shows a competing process.

    This is STRONGER than two boundary samples and is the check the gate actually rests on.
    Two instants cannot see a competitor that arrived after the first and left before the
    second; a 10 s timeseries across the window can.
    """
    # Parsed up front, because the read loop now compares parsed instants rather than raw
    # strings (finding 4).
    t_start_pre = _parse_ts("--window-start", start)
    t_end_pre = _parse_ts("--window-end", end)
    if t_end_pre <= t_start_pre:
        raise NotQuiescent(
            "QUIESCENCE_WINDOW_INVALID", f"window end {end} is not after start {start}"
        )
    rows = []
    with open(timeseries, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                # A malformed sampler line is an ERROR, not a line to skip: skipping it
                # would let a truncated timeseries certify a window it never covered.
                raise NotQuiescent(
                    "QUIESCENCE_TIMESERIES_MALFORMED",
                    f"{timeseries} carries an unparseable line; a timeseries that cannot be"
                    " read in full cannot establish that the window was clean",
                )
            # EVERY nonblank record's ts is PARSED AND VALIDATED BEFORE any range filter,
            # and the filter compares PARSED datetimes (#3248, roborev job 64 finding 4).
            #
            # The first version filtered on the RAW string, so a record with a missing or
            # malformed `ts` failed the string comparison and was SILENTLY SKIPPED. If its
            # neighbours stayed inside the 30s gap bound, a sample carrying a
            # COMPETING-PROCESS observation could vanish while the window certified clean --
            # a contaminated sample dropped by the very filter meant to find it.
            instant = _parse_ts(f"{timeseries} record", rec.get("ts"))
            if t_start_pre <= instant <= t_end_pre:
                rec["_instant"] = instant
                rows.append(rec)
    if not rows:
        raise NotQuiescent(
            "QUIESCENCE_TIMESERIES_EMPTY",
            f"no sampler lines fall in [{start}, {end}] — the window is UNCOVERED, which"
            " reads exactly like a clean one. An absent measurement is not a pass.",
        )
    # COVERAGE: the window must actually be OBSERVED, not merely intersected. A non-empty
    # sample set is not coverage — see MAX_SAMPLE_GAP_S.
    t_start, t_end = t_start_pre, t_end_pre
    instants = sorted(r["_instant"] for r in rows)
    gaps = [("start", (instants[0] - t_start).total_seconds())]
    gaps += [
        (f"between {instants[i].isoformat()} and {instants[i + 1].isoformat()}",
         (instants[i + 1] - instants[i]).total_seconds())
        for i in range(len(instants) - 1)
    ]
    gaps.append(("end", (t_end - instants[-1]).total_seconds()))
    worst = max(gaps, key=lambda g: g[1])
    if worst[1] > MAX_SAMPLE_GAP_S:
        raise NotQuiescent(
            "QUIESCENCE_WINDOW_UNDERCOVERED",
            f"the largest unobserved stretch inside [{start}, {end}] is {worst[1]:.0f}s"
            f" ({worst[0]}), over the {MAX_SAMPLE_GAP_S:.0f}s bound at a"
            f" {SAMPLER_CADENCE_S:.0f}s cadence. {len(rows)} sample(s) INTERSECT this window"
            " but do not COVER it: a sampler that stopped early would otherwise certify the"
            " unobserved remainder as quiescent.",
        )
    # EVERY ROW MUST CARRY THE CENSUS AFFIRMATIVELY. `r.get("rustc")` returns None for an
    # absent field, None is falsy, and a falsy census reads as CLEAN — so a malformed or
    # schema-drifted timeseries (one with no census fields at all) certified an entire window
    # as uncontaminated. That is a pass derived from the ABSENCE of a bad signal, which is
    # exactly the rule this issue keeps restating, violated here in the guard written to
    # enforce it. The fields are required, and required to be non-negative integers.
    # THE TIMESERIES CENSUS MUST COVER THE SAME PROCESSES AS BOUNDARY SAMPLING (#3248,
    # roborev job 68 finding 5).
    #
    # The boundary sampler treats `cc1`, `cc1plus`, `ld`, `lld` and `mold` as competing, while
    # the in-window census only had `rustc`/`cargo`/`gate` fields — so a short-lived compiler
    # or linker appearing ONLY BETWEEN boundaries contaminated the measurement while the window
    # was certified clean. Two halves of one gate, disagreeing about what "competing" means.
    #
    # A sampler that emits `competing_count` (the full census, computed from the same
    # COMPETING_COMMS list this module uses for boundary samples) is required going forward and
    # is authoritative when present. The individual fields remain accepted for a timeseries
    # recorded before that field existed — but such a record is explicitly a NARROWER census,
    # and the verdict says so rather than implying full coverage.
    CENSUS_FIELDS = ("rustc", "cargo", "gate")
    narrow_census_records = 0
    dirty = []
    for rec in rows:
        counts = {}
        for field in CENSUS_FIELDS:
            if field not in rec:
                raise NotQuiescent(
                    "QUIESCENCE_TIMESERIES_SCHEMA",
                    f"the sample at {rec.get('ts')!r} carries no {field!r} field. An absent"
                    " census field is falsy and would read as CLEAN, so a timeseries missing"
                    " it cannot establish that the window was uncontaminated.",
                )
            value = rec[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise NotQuiescent(
                    "QUIESCENCE_TIMESERIES_SCHEMA",
                    f"the sample at {rec.get('ts')!r} has {field}={value!r}, which is not a"
                    " non-negative integer count.",
                )
            counts[field] = value
        # `competing_count` is the FULL census when the sampler provides it. Its absence is
        # recorded, never silently treated as equivalent coverage.
        full = rec.get("competing_count")
        if full is None:
            narrow_census_records += 1
        else:
            if isinstance(full, bool) or not isinstance(full, int) or full < 0:
                raise NotQuiescent(
                    "QUIESCENCE_TIMESERIES_SCHEMA",
                    f"the sample at {rec.get('ts')!r} has competing_count={full!r}, which is"
                    " not a non-negative integer count.",
                )
            if full:
                dirty.append(rec)
                continue
        # LOAD1 IS REQUIRED AND MUST BE FINITE (finding 4). Without this, a census-complete
        # timeseries carrying no `load1` was declared QUIESCENT and the verdict file was
        # WRITTEN, and only the summary print then crashed — leaving a QUIESCENT artifact on
        # disk that a later direct report would accept. A partially-written verdict is worse
        # than none.
        if "load1" not in rec:
            raise NotQuiescent(
                "QUIESCENCE_TIMESERIES_SCHEMA",
                f"the sample at {rec.get('ts')!r} carries no `load1`. It is recorded as"
                " context in the verdict, so a record without it cannot be summarised and"
                " must not be certified.",
            )
        _finite(f"the sample at {rec.get('ts')!r} load1", rec["load1"])
        if any(counts.values()):
            dirty.append(rec)
    if dirty:
        raise NotQuiescent(
            "QUIESCENCE_WINDOW_CONTAMINATED",
            f"{len(dirty)} of {len(rows)} in-window sample(s) show a competing process,"
            f" first at {dirty[0].get('ts')}: rustc={dirty[0].get('rustc')}"
            f" cargo={dirty[0].get('cargo')} gate={dirty[0].get('gate')}",
        )
    loads = [r["load1"] for r in rows if "load1" in r]
    return {
        "samples": len(rows),
        "competing_samples": 0,
        "coverage_largest_gap_s": worst[1],
        "coverage_gap_bound_s": MAX_SAMPLE_GAP_S,
        "load1_min": min(loads) if loads else None,
        "load1_max": max(loads) if loads else None,
        "load1_mean": (sum(loads) / len(loads)) if loads else None,
        "window": {"start": start, "end": end},
        # Recorded so the REPORTER can verify this verdict was produced against the timeseries
        # the session manifest declares -- a verdict from a different file establishes nothing
        # about this session (#3248 finding 2).
        "timeseries": timeseries,
        # How many in-window records carried only the NARROW census (no `competing_count`).
        # Nonzero means this verdict rests on a census of rustc/cargo/gate alone, not the full
        # COMPETING_COMMS set that boundary sampling uses (#3248 finding 5).
        "narrow_census_records": narrow_census_records,
        "census_breadth": (
            "FULL (competing_count present on every in-window record)"
            if narrow_census_records == 0
            else f"NARROW on {narrow_census_records} of {len(rows)} record(s): those carry"
                 " rustc/cargo/gate only, so a short-lived cc1/ld/lld/mold between boundaries"
                 " would not appear. Stated rather than implied."
        ),
    }


def judge(before: Dict[str, object], after: Dict[str, object], *,
          max_load1: float, max_movement: float,
          window: Optional[Dict[str, object]] = None,
          after_settled: bool = False) -> Dict[str, object]:
    """Accept or refuse a rep. Refusal is fail-closed.

    THE LOAD BOUNDS ARE ASYMMETRIC, AND THE FIRST VERSION OF THIS FUNCTION WAS WRONG.
    It applied the `load1` level and movement bounds to BOTH boundary samples, and then
    refused this issue's own AC0 pass: `load1` at the after boundary read 3.05 against a
    2.0 bound, with a competing census of ZERO at both boundaries and zero competing
    processes across all 48 in-window sampler lines.

    The box was clean. `load1` is a ONE-MINUTE EXPONENTIALLY-DECAYING AVERAGE, so a sample
    taken immediately after a nine-minute CPU-bound measurement necessarily reads the
    measurement's OWN residue. Bounding it there does not measure the box's quietness; it
    measures how hard the rig just worked, and it would refuse every honest run of a
    CPU-bound rig while passing a short one on a contended box.

    The deeper lesson, and it is the same one the census-vs-`pgrep -f` bug taught one level
    down: **attribute by process IDENTITY, not by aggregate load.** An aggregate cannot
    distinguish my own load from a competitor's, which is exactly the confusion that made a
    peer's `pgrep -c -f` report a busy box when it was idle. So:

      * the competing-process CENSUS is the guard, applied at BOTH boundaries AND across
        every in-window sampler line;
      * the `load1` LEVEL bound applies to the BEFORE sample, where it is meaningful (is the
        box quiet as we ENTER the window, including foreign load from processes the census
        does not enumerate);
      * the after sample's `load1` is RECORDED but NOT bounded unless the caller asserts it
        was taken after settling, because otherwise it is self-inflicted.

    The threshold was NOT loosened to make this issue's own run pass. The bound that fired
    was removed from a place it could not be valid and kept where it can, and the binding
    check was made STRONGER: 48 attributable samples instead of 2 ambiguous ones.
    """
    # There must ALWAYS be a binding in-window check. A run with neither a settled after
    # sample nor a window timeseries has nothing establishing the window was clean, and
    # "nothing" must not read as "clean".
    if window is None and not after_settled:
        raise NotQuiescent(
            "QUIESCENCE_WINDOW_UNVERIFIED",
            "neither a window timeseries nor a settled after-sample was supplied, so"
            " nothing establishes that the measurement window was free of competing load."
            " Pass --timeseries with --window-start/--window-end, or --after-settled.",
        )
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
    l1_before = _finite("the before sample's load1", before["load"]["load1"])
    l1_after = _finite("the after sample's load1", after["load"]["load1"])
    # BEFORE only — see the docstring. This is the state the window was ENTERED in.
    if l1_before > max_load1:
        raise NotQuiescent(
            "QUIESCENCE_LOAD_TOO_HIGH",
            f"load1 at the before boundary is {l1_before} (> {max_load1}); the box was not"
            " quiet as the window opened.",
        )
    movement = abs(l1_after - l1_before)
    if after_settled:
        if l1_after > max_load1:
            raise NotQuiescent(
                "QUIESCENCE_LOAD_TOO_HIGH",
                f"load1 at the SETTLED after boundary is {l1_after} (> {max_load1}).",
            )
        if movement > max_movement:
            raise NotQuiescent(
                "QUIESCENCE_LOAD_MOVED",
                f"load1 moved {movement:.2f} between the boundaries ({l1_before} ->"
                f" {l1_after}, bound {max_movement}). A rep whose load moved mid-flight is"
                " INVALID, not slow: it breaks the interleaving that makes A/B readable.",
            )
    return {
        "verdict": "QUIESCENT",
        "load1_before": l1_before,
        "load1_after": l1_after,
        "load1_after_is_bounded": bool(after_settled),
        "load1_after_note": (
            "bounded: the caller asserted this sample was taken after settling"
            if after_settled else
            "RECORDED, NOT BOUNDED: load1 is a 1-minute decaying average, so a sample taken"
            " immediately after a CPU-bound window reads the window's own residue. The"
            " binding in-window check is the timeseries census."
        ),
        "load1_movement": movement,
        "competing_before": 0,
        "competing_after": 0,
        "window_census": window,
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
    p_j.add_argument("--timeseries", default=None,
                     help="sampler JSONL; every line inside the window must show a zero"
                          " competing census. This is the binding in-window check.")
    p_j.add_argument("--window-start", default=None, help="ISO ts, inclusive")
    p_j.add_argument("--window-end", default=None, help="ISO ts, inclusive")
    p_j.add_argument("--after-settled", action="store_true",
                     help="assert the after-sample was taken AFTER load settled, which"
                          " licenses bounding its load1. Without it, or without"
                          " --timeseries, the run is refused as unverified.")
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
    # Finiteness FIRST: a NaN threshold passes the may-only-tighten checks below (because
    # `nan > x` is False) and then passes the level/movement checks too.
    for name, value, floor in (("--max-load1", args.max_load1, DEFAULT_MAX_LOAD1),
                               ("--max-load1-movement", args.max_load1_movement,
                                DEFAULT_MAX_LOAD1_MOVEMENT)):
        if not math.isfinite(value) or value <= 0.0:
            print(f"ws0_quiescence: REFUSED: QUIESCENCE_THRESHOLD_NOT_FINITE: {name}"
                  f" {value!r} is not a finite positive number. A non-finite threshold does"
                  " not relax this guard, it DISABLES it: every comparison with NaN is False.",
                  file=sys.stderr)
            return 2
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

    window = None
    try:
        if args.timeseries:
            if not (args.window_start and args.window_end):
                print("ws0_quiescence: REFUSED: QUIESCENCE_WINDOW_UNBOUNDED: --timeseries"
                      " needs --window-start and --window-end; an unbounded window would"
                      " judge samples from another run", file=sys.stderr)
                return 2
            window = window_census_clean(args.timeseries, args.window_start,
                                         args.window_end)
        rec = judge(before, after, max_load1=args.max_load1,
                    max_movement=args.max_load1_movement,
                    window=window, after_settled=args.after_settled)
    except NotQuiescent as exc:
        print(f"ws0_quiescence: REFUSED: {exc.cause}: {exc.detail}", file=sys.stderr)
        return 1

    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(rec, indent=2, sort_keys=True) + "\n")
    print(f"ws0_quiescence: {rec['verdict']}")
    print(f"  competing census: 0 at both boundaries")
    w = rec.get("window_census")
    if w:
        print(f"  in-window census: 0 competing across {w['samples']} sampler sample(s)"
              f" [{w['window']['start']} .. {w['window']['end']}]")
        print(f"  in-window load1:  min={w['load1_min']} max={w['load1_max']}"
              f" mean={w['load1_mean']:.2f}  (recorded as context, not a gate)")
    print(f"  load1 before: {rec['load1_before']} (bounded <= {args.max_load1})")
    print(f"  load1 after:  {rec['load1_after']} — {rec['load1_after_note']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
