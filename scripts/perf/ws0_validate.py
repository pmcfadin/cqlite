#!/usr/bin/env python3
"""Fail-closed validation for the WS0 measurement rig's reporting path (#3272).

Split out of `ws0_report.py` under the campsite rule: the reporter aggregates,
this module decides what it is allowed to aggregate. Every function here answers
one question — "was this actually OBSERVED?" — and raises `Invalid` when the
answer is no.

The governing rule, from CQLite's no-heuristics / authoritative-metadata-only
mandate applied to the reporting path (#3272 AC3):

    **A counter that was not observed is an ERROR, never a fabricated `0`.**

Every guard here exists because its absence was a REAL defect, and the shape of
all of them is the same: *an instrument that reports success without having
measured*. The four that came from #3272's review round:

* `load_corpus_identity` — an absent `corpus-identity.json` used to yield
  `corpus_rows=None`, which SILENTLY DISABLED the full-corpus-per-request assert
  while the generated NOTES kept claiming the property had been verified. That is
  fail-open inside the guard added to close the cold-blend defect, so the identity
  is now REQUIRED and every field it must carry is checked.
* `classify_prewarm` — the `skipped-cold-arm` sentinel used to count as a healthy
  prewarm at ANY temperature, so an unprewarmed WARM rep reached
  `prewarm_all_ok=true`: the prewarm guard satisfied by its own cold-arm sentinel.
  A status is now valid only at the temperature it can arise at.
* `read_perf_counters` — `.get("cycles", 0)` turned an absent or unparseable
  counter into a zero, so a run was reported "SETUP-SUBTRACTED" with no
  subtraction having happened. A perf CSV that does not carry a required event
  (including perf's own `<not counted>` / `<not supported>` markers) is an error.
* `positive_int` — `--reps 0` produced a vacuous but SUCCESSFUL report.

There is deliberately **no environment variable that switches any of these off**.
An escape hatch on a measurement guard can only ever buy a confident wrong number,
which is the failure mode the whole rig is built against.
"""

from __future__ import annotations

import json
import pathlib
import re

# perf's own markers for "this event produced no value". They are the exact shape
# of the silent-instrument failure: the CSV line EXISTS, the run EXITED ZERO, and
# the number is absent — so they must never reach an arithmetic path.
PERF_NOT_A_VALUE = ("<not counted>", "<not supported>", "<unsupported>", "")

# The prewarm status each temperature can legitimately record. A status outside its
# temperature's set is a STRUCTURAL inconsistency in the artifact set (the driver
# cannot emit it), which is fatal — as distinct from an honestly-recorded
# DEGRADATION (`unrecorded`, `FAILED-exit-N`), which is flagged loudly but reported.
PREWARM_REQUIRED = {
    "warm": "ok",
    "cold": "skipped-cold-arm",
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class Invalid(Exception):
    """A property the report depends on was not observed. Always fatal."""


# An upper bound on any count that drives a loop. Python ints are arbitrary-precision,
# so an absurd `--reps` does not overflow — it HANGS: `range(1, 10**20)` iterates
# essentially forever, statting a file per iteration, and a reporter that never
# terminates produces no verdict at all. Measured before this bound:
# `--reps 99999999999999999999` ran past a 10s timeout with no output. 100k is far
# past any session anyone would run (the recorded #3096 sessions used 3).
MAX_COUNT = 100_000


def positive_int(name: str, value: object) -> int:
    """`value` as an int in `1..MAX_COUNT`, or `Invalid`.

    `--reps 0` used to run the whole reporter over an empty rep range and exit
    ZERO with `measurements: []` — a report that measured nothing, indistinguishable
    at the exit code from one that measured everything (#3272 finding 5). The upper
    bound is the same class from the other end: not a wrong number but no number,
    since an unbounded loop never reaches a verdict.
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise Invalid(f"--{name} must be an integer >= 1 (got {value!r})") from None
    if n < 1:
        raise Invalid(
            f"--{name} must be at least 1 (got {n}). A run with {name}<1 measures"
            " nothing, and a report over nothing is not a smaller version of the"
            " requested claim — it is a vacuous success."
        )
    if n > MAX_COUNT:
        raise Invalid(
            f"--{name} is absurdly large ({n:,}; the cap is {MAX_COUNT:,}). Python"
            " ints do not overflow, so this would not be a wrong number — it would"
            " be a reporter that iterates for hours and never reaches a verdict."
        )
    return n


def existing_dir(name: str, value: str) -> pathlib.Path:
    """`value` as an existing directory, or `Invalid`."""
    p = pathlib.Path(value)
    if not p.is_dir():
        raise Invalid(f"--{name} {value!r} is not an existing directory")
    return p


def nonempty_selection(name: str, value: str, allowed: tuple[str, ...]) -> list[str]:
    """The whitespace-split selection, every member in `allowed`, or `Invalid`.

    An empty `--temps`/`--arms` would silently produce a report with no
    measurements and a zero exit, the same vacuous-green shape as `--reps 0`.
    """
    items = value.split()
    if not items:
        raise Invalid(f"--{name} is empty; expected one or more of {', '.join(allowed)}")
    bad = [x for x in items if x not in allowed]
    if bad:
        raise Invalid(
            f"--{name} carries unknown value(s) {', '.join(repr(b) for b in bad)};"
            f" expected only {', '.join(allowed)}"
        )
    dupes = [x for x in set(items) if items.count(x) > 1]
    if dupes:
        raise Invalid(f"--{name} repeats {', '.join(sorted(dupes))}")
    return items


def read_perf_counters(
    path: pathlib.Path, label: str, required: tuple[str, ...]
) -> dict[str, int]:
    """Sum a `perf stat -x,` CSV by event name, fail-closed on every gap.

    Summed across the CPUs in the `-C` set: `perf stat -C a,b` emits one line per
    event, already aggregated, but a `--per-core` variant would emit several — so
    summing is correct in both shapes and never silently drops a line.

    Four things are errors rather than a missing key that a caller would default
    to `0` (#3272 finding 4):

    * the file does not exist — the perf window never produced an artifact;
    * a `required` event has no line — the counter was not multiplexed in;
    * a value is one of perf's `<not counted>` / `<not supported>` markers — the
      line exists but the measurement does not, which is the silent-instrument
      failure in its purest form;
    * a value is present but unparseable — a corrupt artifact, not a zero.
    """
    if not path.exists():
        raise Invalid(
            f"{label}: no perf artifact at {path.name} — the counters for this leg"
            " were never observed. A report cannot substitute a zero for a counter"
            " it does not have; re-run the leg."
        )
    counters: dict[str, int] = {}
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) <= 2:
            continue
        raw, event = fields[0].strip(), fields[2].strip()
        if not event:
            continue
        if raw in PERF_NOT_A_VALUE:
            raise Invalid(
                f"{label}: {path.name} line {lineno} records event {event!r} as"
                f" {raw!r} — perf did not count it. That is an unmeasured counter,"
                " not a zero; re-run the leg (check event availability and"
                " perf_event_paranoid)."
            )
        try:
            counters[event] = counters.get(event, 0) + int(raw)
        except ValueError:
            raise Invalid(
                f"{label}: {path.name} line {lineno} has an unparseable value"
                f" {raw!r} for event {event!r}; a corrupt perf artifact is not a"
                " zero counter"
            ) from None
    absent = [e for e in required if e not in counters]
    if absent:
        raise Invalid(
            f"{label}: {path.name} carries no line for required event(s)"
            f" {', '.join(absent)} (present: {', '.join(sorted(counters)) or '<none>'})."
            " A counter that was not observed is an error, never a fabricated 0"
            " (#3272 AC3)."
        )
    return counters


# Every field the report prints or asserts on. Absent or malformed => the corpus
# identity is not authoritative, so nothing derived from it may be reported.
IDENTITY_INT_FIELDS = ("seed", "rows", "partitions", "cells_per_row", "data_db_bytes")


def load_corpus_identity(corpus: pathlib.Path) -> dict:
    """The corpus's recorded identity, or `Invalid`. Never a partial dict.

    #3272 finding 1: this used to be `identity = json.loads(...) if exists else {}`,
    and `corpus_rows = int(identity["rows"]) if identity.get("rows") else None`.
    A `None` there **turned off** `check_request_count`'s
    `rows == requests_ok x corpus_rows` assert — the property that catches a
    request which did not scan the whole corpus — while the report's NOTES kept
    stating that "every rep's rows [are] an exact multiple of the corpus row
    count". The reader was told a check had run that had been skipped.

    So the identity is REQUIRED, and required to be complete: the row count is the
    denominator of the only cross-arm property the rig asserts, and a denominator
    that might be `None` is not one.
    """
    idp = corpus / "corpus-identity.json"
    if not idp.exists():
        raise Invalid(
            f"no corpus identity at {idp} — the corpus row count is UNKNOWN, so the"
            " full-corpus-per-request property (every rep's rows == requests_ok x"
            " corpus_rows) cannot be checked. This is refused rather than skipped:"
            " skipping it silently while the report's NOTES claim it ran is how a"
            " partial scan gets published as a full one (#3272 finding 1)."
            " Regenerate the corpus with tools/ws0-corpus-gen, which writes this"
            " file beside the data."
        )
    try:
        identity = json.loads(idp.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{idp} is not readable JSON: {exc}") from None
    if not isinstance(identity, dict):
        raise Invalid(f"{idp} must hold a JSON object, got {type(identity).__name__}")

    for key in IDENTITY_INT_FIELDS:
        if key not in identity:
            raise Invalid(f"{idp} carries no {key!r} — the corpus identity is incomplete")
        try:
            identity[key] = int(identity[key])
        except (TypeError, ValueError):
            raise Invalid(f"{idp}: {key!r} is not an integer ({identity[key]!r})") from None
    for key in ("rows", "partitions", "cells_per_row", "data_db_bytes"):
        if identity[key] < 1:
            raise Invalid(
                f"{idp}: {key!r} is {identity[key]} — a corpus with no {key} is not a"
                " measurable corpus"
            )

    sha = identity.get("data_db_sha256")
    if not isinstance(sha, str) or not _SHA256_RE.match(sha):
        raise Invalid(
            f"{idp}: 'data_db_sha256' must be 64 lowercase hex characters (got"
            f" {sha!r}). It is the corpus's determinism pin; a truncated or absent"
            " digest cannot identify the bytes that were measured."
        )

    try:
        bpr = float(identity["bytes_per_row"])
    except (KeyError, TypeError, ValueError):
        raise Invalid(f"{idp}: 'bytes_per_row' is absent or not a number") from None
    # Cross-check rather than trust: an identity whose own fields disagree is not
    # authoritative metadata, whichever field is the wrong one.
    derived = identity["data_db_bytes"] / identity["rows"]
    if abs(derived - bpr) > max(0.01, derived * 1e-6):
        raise Invalid(
            f"{idp} is internally inconsistent: bytes_per_row={bpr} but"
            f" data_db_bytes/rows={derived:.6f}. One of the three is wrong, so none"
            " of them can be reported."
        )
    identity["bytes_per_row"] = bpr
    return identity


def classify_prewarm(temp: str, status: str) -> str:
    """`ok` | `degraded`, or `Invalid` for a status impossible at this temperature.

    #3272 finding 2 — the direct bypass of #3096's prewarm fix. The acceptance set
    used to be a flat tuple:

        OK_PREWARM = ("ok", "skipped-cold-arm")
        prewarm_all_ok = all(p["status"] in OK_PREWARM for p in prewarm)

    which is temperature-BLIND, so a **WARM** rep whose status file read
    `skipped-cold-arm` reached `prewarm_all_ok=true`. That is an UNPREWARMED WARM
    measurement passing the very guard added to prevent one, using the cold arm's
    own sentinel as the key.

    Three outcomes, and the middle one is the point:

    * the temperature's REQUIRED status (`warm`->`ok`, `cold`->`skipped-cold-arm`)
      => `ok`.
    * the OTHER temperature's required status => `Invalid`. The driver cannot emit
      it, so the artifact set is inconsistent: either a warm rep was never
      prewarmed while claiming a cold-arm skip, or a "cold" rep was prewarmed and
      is therefore not cold. Both invalidate the claim the figure carries, and
      neither is a degradation that could be honestly labelled.
    * anything else (`unrecorded`, `FAILED-exit-N`, a future label) => `degraded`.
      An honest record of a real failure: flagged loudly in the summary and in
      `prewarm_all_ok`, but reported, because a rep labelled `prewarm-failed` is
      more useful than a silently dropped one.
    """
    required = PREWARM_REQUIRED.get(temp)
    if required is None:
        raise Invalid(f"unknown temperature {temp!r} for a prewarm status")
    if status == required:
        return "ok"
    other = {v: k for k, v in PREWARM_REQUIRED.items()}.get(status)
    if other is not None and other != temp:
        raise Invalid(
            f"a {temp.upper()} rep recorded prewarm status {status!r}, which only a"
            f" {other.upper()} rep can record. The driver cannot produce this"
            " combination, so the artifact set is inconsistent and the temperature"
            " label on this figure is not verified."
            + (
                " A warm rep carrying the cold arm's skip sentinel is an UNPREWARMED"
                " WARM measurement — exactly what the prewarm guard exists to"
                " refuse, so it may not satisfy it (#3272 finding 2)."
                if temp == "warm"
                else " A prewarmed rep is not cold, so a 'cold' figure may not carry"
                " a successful prewarm."
            )
        )
    return "degraded"


def require_complete(
    label: str, per_rep: list, reps: int, missing: list[str]
) -> None:
    """FAIL unless all `reps` reps of a SELECTED (arm, temperature) were collected.

    Every (arm, temperature) this is called for is one the CALLER SELECTED via
    `--temps`/`--arms`; an unselected one is never iterated, so it is legitimately
    absent and never reaches here (#3272 finding 6). Which makes the rule simple
    and total: a selected combination must be complete.

    The pre-#3272 version documented a second case — "`per_rep` empty AND nothing
    missing -> this (arm, temperature) was never run; not an error" — that could
    NEVER occur, because the collectors append EVERY absent expected artifact to
    `missing` before reaching this call. So the branch was dead code guarding a
    claim the code did not implement, and an intentionally narrow run exited
    fatally anyway. The selection is now stated in `results.json` instead, where a
    reader can see it, rather than inferred from which arms happened to be absent.
    """
    if len(per_rep) < reps:
        raise Invalid(
            f"{label} collected {len(per_rep)} of {reps} requested reps"
            f" — missing artifacts: {', '.join(missing) or '<none named>'}."
            " A median over fewer reps than requested is a different claim than the"
            " one asked for; re-run the missing reps rather than reporting this."
        )
