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

import hashlib
import json
import math
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

# A CANONICAL decimal integer, and nothing else. No surrounding whitespace beyond what the
# caller strips, no `+`, no `_` separators (`int("1_0")` is 10 in Python, which would read a
# malformed artifact as a number), no fractional part, no exponent. A leading `-` is
# admitted so a NEGATIVE counter reaches its domain check and is refused BY NAME rather
# than as a format complaint — the two causes stay distinct, as the duration parser's do.
_INT_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")


class Invalid(Exception):
    """A property the report depends on was not observed. Always fatal."""


# ===========================================================================
# THE SHARED QUANTITY VALIDATOR (#3272 review round 3, B2 + B5)
# ===========================================================================
# Three rounds fixed this class PARTIALLY, each time at the site review named:
#
#   round 2 checked `cyc <= 0` after the setup subtraction, and `rows < 1`, and
#   `arm_rps` finite-and-positive — and never checked `ins`, which is the SAME
#   subtraction (`total["instructions"] - setup["instructions"]`) feeding
#   `ipc.append(ins / cyc)`. `spread()` refuses a non-positive MEDIAN, which is a
#   different property: one corrupt rep among three leaves the median positive and
#   publishes `ipc.min` computed from a value that cannot exist. `collect_flight` had
#   the identical gap on a perf CSV recording `instructions,0`.
#
#   and every coercion in the path was a bare `int()`, which SILENTLY TRUNCATES a
#   float and ACCEPTS a bool: `requests_error: 0.9` reported CLEAN (0), `requests_ok:
#   1.9` satisfied the exactly-one-cold-request guard (1), and `true` became 1.
#
# Fixing the cited line again would be the fourth partial fix, which is why the answer is a
# shared validator rather than another site fix.
#
# The INVENTORY that used to sit here — 11 coercions and 17 derived quantities, listed by
# hand — is DELETED (#3272 review round 4).
#
# It claimed to be "the complete inventory, enumerated MECHANICALLY", and it was neither
# complete nor mechanically checked: `rows_per_scan_observed` (ws0_collect), `spread_pct_of_median`
# (ws0_collect) and `within_round_span_ns` (ws0_rounds) were all absent from it. Prose that
# claims an audited set and is wrong is worse than no prose, because a reader who trusts it
# stops looking — the same shape as a guard that reports success without measuring.
#
# What replaced it is a MECHANISM, not a better list: `test_ws0_fabrication_guards.sh` walks the
# `ast` of every `ws0_*.py` and FAILS on any bare `int()`/`float()` coercion of an artifact value
# and on any defaulting `.get(k, <literal>)` in the reporting path. That check cannot go stale,
# because it derives its subject from the code rather than restating it — which is the property
# the deleted comment claimed and did not have.
#
# The RULE the list was trying to express, stated as a rule so it needs no enumeration: every
# quantity in the reporting path goes through ONE of the functions below, each of which states
# its VALIDITY DOMAIN in its name, and a new counter cannot be read without choosing one. The
# only quantities deliberately UNCONSTRAINED in sign are DELTAS (a cycles/row delta may
# legitimately be negative); their DIVISORS are validated, which is what needs the domain.
#
# There is deliberately no `lenient=` parameter and no env var. An escape hatch on a
# measurement domain can only buy a confident wrong number.


def _reject_bool(label: str, value: object, why: str = "") -> None:
    """`True`/`False` is not a counter, even though `int(True) == 1`.

    JSON `true` reaching a counter field means the artifact is not the artifact this
    reporter models; silently reading it as 1 let `requests_ok: true` satisfy the
    exactly-one-cold-request guard (#3272 review round 3, B5).
    """
    if isinstance(value, bool):
        raise Invalid(
            f"{label} is the boolean {value!r}, not a number. `int(True)` is 1, so a"
            " bare int() would have read this as a count of one — a JSON boolean in a"
            f" counter field means the artifact is not the one this reporter models."
            f"{(' ' + why) if why else ''}"
        )


def exact_int(label: str, value: object, why: str = "") -> int:
    """`value` as an int, refusing anything that is not EXACTLY an integer.

    Accepted: a JSON integer, and a string whose whole content is a canonical decimal
    integer (perf CSVs and `<tag>.round` files carry text).

    REFUSED, where a bare `int()` would have silently converted (#3272 B5):

    * a bool — `int(True) == 1`;
    * a FLOAT with a fractional part — `int(0.9) == 0`, so `requests_error: 0.9` was
      reported as a clean zero and `requests_ok: 1.9` satisfied the cold-rep guard;
    * a float that is integral but not exact in the domain (`1e30`), and `inf`/`nan`;
    * a string with surrounding junk or a fractional part — `int(" 3 ")` is 3, which
      hides a malformed artifact, and `int("3.7")` raises where the caller wants a
      NAMED refusal rather than a traceback.

    An integral float (`3.0`) IS accepted: `json` decodes `3.0` for a field a producer
    wrote as an integer-valued double, and the value is exactly the integer. What is
    refused is a value that is NOT the integer it would be read as.
    """
    _reject_bool(label, value, why)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Invalid(
                f"{label} is {value!r}, which is not a finite number."
                f"{(' ' + why) if why else ''}"
            )
        if value != int(value):
            raise Invalid(
                f"{label} is the fractional value {value!r}. A bare int() would have"
                f" TRUNCATED it to {int(value)} and reported that as the observed"
                " quantity — a fabricated value, arrived at by rounding rather than by"
                f" defaulting (#3272 AC3).{(' ' + why) if why else ''}"
            )
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not _INT_RE.match(text):
            raise Invalid(
                f"{label} must be an integer: {value!r} is an unparseable value, not a"
                " canonical decimal integer. A fractional, padded or junk-bearing value is"
                f" a corrupt artifact, not a number to be coerced.{(' ' + why) if why else ''}"
            )
        return int(text)
    raise Invalid(
        f"{label} must be an integer: {value!r} is a {type(value).__name__}."
        f"{(' ' + why) if why else ''}"
    )


def non_negative_int(label: str, value: object, why: str = "") -> int:
    """`exact_int`, and `>= 0`. The domain of every COUNT and every hardware counter.

    `why` is the CALLER's sentence about what this particular quantity IS — appended to
    the refusal so the diagnostic names the measurement rather than only the domain. The
    coercion and domain rules are shared (they are the same for every counter); what a
    number MEANS is local, and a shared validator that swallowed that would make every
    refusal read the same and name nothing (#3272 review round 3, B2/B5).
    """
    n = exact_int(label, value, why)
    if n < 0:
        raise Invalid(
            f"{label} is {n}, which is not a possible count. Counts and hardware"
            " counters are non-negative; a negative one is a CORRUPT artifact, and"
            " reading it as a small number publishes a figure that cannot exist"
            f" (#3272 R6).{(' ' + why) if why else ''}"
        )
    return n


def positive_int(label: str, value: object, why: str = "") -> int:
    """`exact_int`, and `>= 1`. The domain of a DENOMINATOR and a 1-based index."""
    n = exact_int(label, value, why)
    if n < 1:
        raise Invalid(
            f"{label} is {n}, which is not a positive integer. It is a denominator or a"
            " 1-based index; a non-positive value is refused rather than divided by or"
            f" indexed with.{(' ' + why) if why else ''}"
        )
    return n


def positive_finite_float(label: str, value: object, why: str = "") -> float:
    """`value` as a float that is finite and `> 0`.

    `inf`/`nan` are refused for the same reason a zero is: they are PRINTABLE numbers
    standing in for an absent measurement, and they propagate silently through
    `spread()` into the summary.
    """
    _reject_bool(label, value, why)
    if isinstance(value, str):
        value = value.strip()
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise Invalid(
            f"{label} is {value!r}, which is not a number.{(' ' + why) if why else ''}"
        ) from None
    if not math.isfinite(f):
        raise Invalid(
            f"{label} is {f!r}, which is not a finite number. inf/nan are printable"
            " values standing in for an absent measurement and would propagate into"
            f" every figure derived from this one.{(' ' + why) if why else ''}"
        )
    if f <= 0:
        raise Invalid(
            f"{label} is {f!r}, which is not positive. A zero or negative rate,"
            " window or size is not a small measurement — it is not a measurement."
            f"{(' ' + why) if why else ''}"
        )
    return f


def positive_derived(label: str, value: float, detail: str = "") -> float:
    """A quantity COMPUTED here, required to be finite and `> 0`.

    Distinct from `positive_finite_float` because the diagnostic is different: an
    observed value out of domain is a corrupt artifact, while a DERIVED one out of
    domain means the computation was not meaningful — a setup leg that cost more than
    the full run, a perf window that counted nothing. `detail` carries the operands, so
    the refusal names what was subtracted from what rather than only the result.
    """
    if not math.isfinite(value) or value <= 0:
        raise Invalid(
            f"{label} is {value!r}{(' (' + detail + ')') if detail else ''} — the"
            " computation is not meaningful, so nothing derived from it may be"
            " reported. A non-positive or non-finite derived quantity is refused"
            " rather than divided by or printed (#3272 B2)."
        )
    return value


# An upper bound on any count that drives a loop. Python ints are arbitrary-precision,
# so an absurd `--reps` does not overflow — it HANGS: `range(1, 10**20)` iterates
# essentially forever, statting a file per iteration, and a reporter that never
# terminates produces no verdict at all. Measured before this bound:
# `--reps 99999999999999999999` ran past a 10s timeout with no output. 100k is far
# past any session anyone would run (the recorded #3096 sessions used 3).
MAX_COUNT = 100_000


def cli_count(name: str, value: object) -> int:
    """A COMMAND-LINE count as an int in `1..MAX_COUNT`, or `Invalid`.

    `--reps 0` used to run the whole reporter over an empty rep range and exit
    ZERO with `measurements: []` — a report that measured nothing, indistinguishable
    at the exit code from one that measured everything (#3272 finding 5). The upper
    bound is the same class from the other end: not a wrong number but no number,
    since an unbounded loop never reaches a verdict.

    Named `cli_count` rather than `positive_int` since round 3 (#3272 B2): a CLI count
    and an OBSERVED counter need different diagnostics — this one names a `--flag` an
    operator can change, `positive_int` names an ARTIFACT FIELD that is corrupt — and a
    shared name for both invites reaching for whichever is imported. Its argument is
    always an argparse string, so it goes through `exact_int` for the bool/fractional
    refusals and then applies the CLI bounds and the CLI wording.
    """
    n = exact_int(f"--{name}", value)
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
    * a value is present but unparseable, fractional, or NEGATIVE — a corrupt artifact,
      not a zero. The negative half is #3272 review round 3, B2: hardware counters are
      non-negative by construction, and `int("-4")` used to sail through to become a
      negative `cycles`/`instructions`, then a negative setup-subtracted `ins`, then a
      negative IPC in `results.json`. Every value goes through `non_negative_int`, which
      also refuses `4.7` (a bare `int()` would truncate it to 4 and report that).
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
            value = non_negative_int(
                f"{label}: {path.name} line {lineno} event {event!r}", raw
            )
        except Invalid as exc:
            raise Invalid(
                f"{path.name} line {lineno} has an unusable value {raw!r} for event"
                f" {event!r}: {exc} A corrupt perf artifact is not a zero counter."
            ) from None
        counters[event] = counters.get(event, 0) + value
    absent = [e for e in required if e not in counters]
    if absent:
        raise Invalid(
            f"{label}: {path.name} carries no line for required event(s)"
            f" {', '.join(absent)} (present: {', '.join(sorted(counters)) or '<none>'})."
            " A counter that was not observed is an error, never a fabricated 0"
            " (#3272 AC3)."
        )
    return counters


# Every field the report prints or asserts on, WITH ITS VALIDITY DOMAIN (#3272 review
# round 3, B2). Domains, not a flat tuple plus a second list of the ones that must be
# positive: the pre-round-3 code coerced all five with a bare `int()` and then separately
# range-checked four of them, so `seed` had no domain at all and a FRACTIONAL or BOOLEAN
# value for any of the five was silently truncated (`rows: 0.9` -> 0, then refused for the
# wrong reason; `cells_per_row: true` -> 1, accepted).
IDENTITY_INT_FIELDS = {
    # a seed of 0 is legitimate — it is an INPUT, not a measured quantity
    "seed": "non_negative",
    # the row DENOMINATOR of every cross-arm property the rig asserts
    "rows": "positive",
    "partitions": "positive",
    "cells_per_row": "positive",
    "data_db_bytes": "positive",
}


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

    for key, domain in IDENTITY_INT_FIELDS.items():
        if key not in identity:
            raise Invalid(f"{idp} carries no {key!r} — the corpus identity is incomplete")
        label = f"{idp}: {key!r}"
        if domain == "positive":
            try:
                identity[key] = positive_int(label, identity[key])
            except Invalid as exc:
                raise Invalid(
                    f"{exc} A corpus with no {key} is not a measurable corpus."
                ) from None
        else:
            identity[key] = non_negative_int(label, identity[key])

    sha = identity.get("data_db_sha256")
    if not isinstance(sha, str) or not _SHA256_RE.match(sha):
        raise Invalid(
            f"{idp}: 'data_db_sha256' must be 64 lowercase hex characters (got"
            f" {sha!r}). It is the corpus's determinism pin; a truncated or absent"
            " digest cannot identify the bytes that were measured."
        )

    if "bytes_per_row" not in identity:
        raise Invalid(f"{idp}: 'bytes_per_row' is absent or not a number")
    # `positive_finite_float`, so `inf`/`nan`/0/negative are refused BEFORE the
    # cross-check below compares against them — `abs(derived - inf)` is `inf`, which
    # exceeds any tolerance and would report an INTERNAL INCONSISTENCY for what is
    # really an unusable field, naming the wrong cause (#3272 B2 enumeration).
    bpr = positive_finite_float(f"{idp}: 'bytes_per_row'", identity["bytes_per_row"])
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
