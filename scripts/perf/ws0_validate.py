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
# Fixing the cited line again would be the fourth partial fix. So every quantity in the
# reporting path goes through ONE of the functions below, each of which states its
# VALIDITY DOMAIN in its name, and a new counter cannot be read without choosing one.
# The complete inventory, enumerated MECHANICALLY (an `ast` walk for every `int()`/
# `float()` call and every arithmetic `BinOp` across `ws0_*.py`) rather than by reading:
#
# OBSERVED (read from an artifact):
#   perf CSV counter value          non_negative_int      hardware counters never go
#                                                         below zero; a negative one is
#                                                         a corrupt artifact
#   identity seed                   non_negative_int
#   identity rows/partitions/
#     cells_per_row/data_db_bytes   positive_int
#   identity bytes_per_row          positive_finite_float
#   <tag>.round round/position/
#     arms_in_round                 positive_int          1-based indices
#   scan rows_denominator           positive_int          a DENOMINATOR
#   scan timed_scan_secs            positive_finite_float a measurement WINDOW
#   flight rows_total               positive_int          a DENOMINATOR
#   flight rows_per_s               positive_finite_float
#   flight requests_ok              positive_int          >=1, and ==1 when cold
#   flight requests_error           non_negative_int      then required == 0
#   --reps / --scan-passes          positive_int (capped) CLI, not an artifact
#
# DERIVED (computed here):
#   cyc  = total.cycles - setup.cycles              positive_derived
#   ins  = total.instructions - setup.instructions  positive_derived   <- the B2 gap
#   flight cyc = counters["cycles"]                 positive_derived
#   flight ins = counters["instructions"]           positive_derived   <- the B2 gap
#   rows/secs, cyc/rows, ins/cyc (IPC)              positive by construction from the above
#   spread() median                                 positive_derived (in ws0_collect)
#   bare/flight ratio, 1.3x target                  positive_derived (in ws0_report)
#   per-round ratio                                 positive_derived (in ws0_rounds)
#   cycles/row DELTA, bytes/rows cross-check        UNCONSTRAINED — a delta may legitimately
#                                                   be negative; its DIVISOR is a validated
#                                                   median, which is what needs the domain
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


# Where a ws0 corpus's SSTable components live, relative to the corpus root — the
# layout `ws0-corpus-gen` writes and both measurement arms resolve.
CORPUS_TABLE_SUBPATH = ("ws0", "events")

# Read the Data.db in 8 MiB slices. The measurement corpus is ~2.8 GB, so the digest
# must stream: reading it whole would need 2.8 GB of RSS to verify a fixture.
_DIGEST_CHUNK = 8 << 20


def locate_corpus_data_db(corpus: pathlib.Path) -> pathlib.Path:
    """The single `*-Data.db` the measurement read, or `Invalid`.

    Ambiguity is fatal in both directions. NO `Data.db` means there is nothing for
    the recorded identity to be the identity OF. TWO means the identity records one
    digest for two candidate files, and picking either would be a guess about which
    the measurement actually streamed — a heuristic, in the one place the whole rig
    is trying to be authoritative about (#28, #3272 review B6).
    """
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    if not table_dir.is_dir():
        raise Invalid(
            f"{table_dir} is not a directory — the corpus identity cannot be verified"
            " against the bytes that were measured, because there are no bytes there."
            " Regenerate with tools/ws0-corpus-gen."
        )
    found = sorted(p for p in table_dir.iterdir() if p.name.endswith("-Data.db"))
    if not found:
        raise Invalid(
            f"{table_dir} holds no *-Data.db, so the recorded corpus identity"
            " describes nothing that is present. A report may not identify bytes it"
            " cannot read."
        )
    if len(found) > 1:
        raise Invalid(
            f"{table_dir} holds {len(found)} *-Data.db files"
            f" ({', '.join(p.name for p in found)}), but corpus-identity.json records"
            " ONE digest. Which one the measurement streamed cannot be determined, and"
            " guessing is exactly the heuristic this rig refuses. Measure a corpus with"
            " a single SSTable."
        )
    return found[0]


def sha256_file(path: pathlib.Path) -> str:
    """Streaming lowercase-hex sha256 of `path` (constant memory, any file size)."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_DIGEST_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def verify_corpus_bytes(
    corpus: pathlib.Path, identity: dict, skip_digest: bool = False
) -> dict:
    """Compare the RECORDED identity against the `Data.db` actually present.

    #3272 review B6: corpus identity was trusted ENTIRELY from
    `corpus-identity.json`. The file was validated for internal consistency and the
    `Data.db` was never opened, so stale metadata sitting beside different bytes
    misidentified the corpus while every other check — including the row-count
    validation the identity feeds — passed. The report then printed that recorded
    sha256 under "corpus sha256:" as the identity of the measured bytes.

    Two comparisons, deliberately split by cost:

    * **SIZE — always.** A `stat`. There is no argument for skipping it, so there is
      no flag that can.
    * **SHA-256 — streamed, opt-outable ONLY visibly.** Digesting 2.8 GB costs
      seconds of IO per report run. `skip_digest` (the driver/reporter's
      `--skip-corpus-digest`) omits it, and the returned record then carries
      `sha256_verified: False` with `data_db_sha256_measured: None`, which the
      reporter STAMPS into the summary as `CORPUS DIGEST UNVERIFIED`. A silent skip
      is not available: an unverified identity that reads like a verified one is the
      defect, not the cost.

    Returns the verification RECORD (what was measured, and by what), so the report
    carries the observation rather than a bare boolean.
    """
    data_db = locate_corpus_data_db(corpus)
    measured_bytes = data_db.stat().st_size
    recorded_bytes = identity["data_db_bytes"]
    if measured_bytes != recorded_bytes:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records data_db_bytes"
            f" {recorded_bytes:,} but {data_db.name} is {measured_bytes:,} bytes on"
            " disk. The recorded identity does not describe the corpus that would be"
            " measured, so every figure derived from it (bytes/row, the row"
            " denominator, the corpus digest printed in the summary) would name the"
            " wrong bytes. Regenerate the corpus, or point --corpus at the one the"
            " identity was recorded from."
        )

    record = {
        "data_db": str(data_db),
        "data_db_bytes_measured": measured_bytes,
        "size_verified": True,
        "data_db_sha256_recorded": identity["data_db_sha256"],
        "data_db_sha256_measured": None,
        "sha256_verified": False,
        "note": "",
    }
    if skip_digest:
        record["note"] = (
            "CORPUS DIGEST UNVERIFIED: --skip-corpus-digest was passed, so the"
            f" recorded sha256 was NOT compared against {data_db.name}. The size"
            " matched. Anything citing this report's corpus identity is citing the"
            " RECORDED digest, not an observed one."
        )
        return record

    measured_sha = sha256_file(data_db)
    record["data_db_sha256_measured"] = measured_sha
    if measured_sha != identity["data_db_sha256"]:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records data_db_sha256"
            f" {identity['data_db_sha256']} but {data_db.name} hashes to"
            f" {measured_sha}. The size matched, so this is the case a size check"
            " alone cannot see: DIFFERENT BYTES of the same length, or an identity"
            " file that outlived the corpus beside it. Every #3096 figure is bound to"
            " a specific corpus digest; measuring one corpus and reporting another's"
            " identity is how a comparison against a recorded number becomes"
            " meaningless. Regenerate, or measure the corpus this identity describes."
        )
    record["sha256_verified"] = True
    record["note"] = (
        f"the recorded size and sha256 were both re-derived from {data_db.name} at"
        " report time; the identity describes the bytes that were measured"
    )
    return record


# The name of the identity the DRIVER stamps into the session dir BEFORE it measures
# anything. Distinct from the corpus's own `corpus-identity.json`, which lives beside the data
# and can be replaced under a session at any time.
SESSION_CORPUS_PIN = "session-corpus-pin.json"


def session_pin_path(session_dir: pathlib.Path) -> pathlib.Path:
    return session_dir / SESSION_CORPUS_PIN


def write_session_corpus_pin(
    session_dir: pathlib.Path, corpus: pathlib.Path, identity: dict
) -> dict:
    """Record WHICH CORPUS this session is about to measure, into the session dir.

    Called by `ws0-baseline.sh` BEFORE the first rep, and read back by the reporter (see
    `verify_session_corpus_pin`).

    # The finding (#3272 review round 4)

    The corpus digest was verified only against the corpus present AT REPORT TIME. No corpus
    identity was captured in the session dir before measurement, so two real sequences
    attributed measurements to bytes that were never measured:

    * RE-REPORTING an old result dir against a DIFFERENT corpus. `ws0_report.py --dir <old>
      --corpus <other>` re-derives `<other>`'s digest, finds it self-consistent, and prints it
      as the identity of figures measured over something else. Nothing in the old dir said
      which corpus it came from.
    * CHANGING THE CORPUS MID-RUN. A regeneration (or a second lane writing the same path)
      between rep 1 and rep N leaves report time verifying the LAST state of the corpus while
      the earlier reps measured the earlier bytes.

    Verifying at report time cannot see either, because both are consistent at report time.
    The pin is the missing half: an identity captured BEFORE, compared AFTER.

    What is recorded is the SIZE and the recorded DIGEST plus the corpus path — never a
    re-hash: this runs on the measurement's critical path, and a 2.8 GB hash per session would
    be paid by every run. The digest RE-DERIVATION stays at report time
    (`verify_corpus_bytes`); what the pin adds is that the identity being re-derived is the one
    the session STARTED with.
    """
    pin = {
        "corpus": str(corpus),
        "rows": identity["rows"],
        "data_db_bytes": identity["data_db_bytes"],
        "data_db_sha256": identity["data_db_sha256"],
        "note": (
            "the corpus identity this session was STARTED against, stamped before the first"
            " rep. ws0_report.py REQUIRES it and refuses a report whose corpus no longer"
            " matches — re-reporting an old session dir against a different corpus, or a"
            " corpus that changed mid-run, is otherwise invisible because both are"
            " self-consistent at report time (#3272 round 4)."
        ),
    }
    session_pin_path(session_dir).write_text(json.dumps(pin, indent=1) + "\n")
    return pin


def verify_session_corpus_pin(
    session_dir: pathlib.Path, corpus: pathlib.Path, identity: dict
) -> dict:
    """Require the session's PRE-MEASUREMENT corpus pin, and require it to still match.

    REQUIRED, not optional: an absent pin means this session dir does not record which corpus
    it measured, and a report over it would attribute its figures to whatever `--corpus` the
    reader happened to pass. That is the fail-open shape — a check that silently does not run
    while the summary prints a digest as the measured one.

    Compared on all three of PATH, SIZE and DIGEST, each for a different reason:

    * the recorded DIGEST is the identity itself. A different digest is a different corpus.
    * the recorded SIZE is compared too, so a pin whose digest field was hand-edited to match
      still has to agree on a second, independent number.
    * the PATH is compared last and is the WEAKEST of the three — a corpus can legitimately be
      moved — so a path difference alone is REPORTED in the record rather than fatal. The two
      byte-level fields are what decide.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so it does not record"
            " WHICH CORPUS it measured. A report over it would attribute its figures to"
            " whatever --corpus the reader passed: re-reporting an old result dir against a"
            " different corpus is self-consistent AT REPORT TIME and therefore invisible to"
            " the report-time digest check (#3272 round 4). Re-run the session with"
            " scripts/perf/ws0-baseline.sh, which stamps the pin before the first rep."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    for key in ("rows", "data_db_bytes", "data_db_sha256"):
        if key not in pin:
            raise Invalid(
                f"{p} carries no {key!r} — the session's corpus pin is incomplete, so it"
                " cannot establish which bytes this session measured"
            )
    pinned_rows = positive_int(f"{p}: 'rows'", pin["rows"])
    pinned_bytes = positive_int(f"{p}: 'data_db_bytes'", pin["data_db_bytes"])
    pinned_sha = pin["data_db_sha256"]
    if not isinstance(pinned_sha, str) or not _SHA256_RE.match(pinned_sha):
        raise Invalid(
            f"{p}: 'data_db_sha256' must be 64 lowercase hex characters (got"
            f" {pinned_sha!r}); a truncated pin cannot identify the measured bytes"
        )
    if pinned_sha != identity["data_db_sha256"]:
        raise Invalid(
            f"THE CORPUS CHANGED. This session was started against a corpus whose Data.db"
            f" sha256 is {pinned_sha} (stamped in {SESSION_CORPUS_PIN} before the first rep),"
            f" but --corpus {corpus} now records {identity['data_db_sha256']}. Every figure in"
            " this session was measured over the PINNED bytes; reporting it under this"
            " corpus's identity would attribute the measurements to bytes that were never"
            " measured. Two real ways to get here, both invisible to the report-time digest"
            " check because both are self-consistent at report time: re-reporting an old"
            " result dir against a different corpus, and a corpus regenerated (or written by"
            " another lane) DURING the run (#3272 round 4). Point --corpus at the corpus this"
            " session measured, or re-run the session."
        )
    if pinned_bytes != identity["data_db_bytes"] or pinned_rows != identity["rows"]:
        raise Invalid(
            f"THE CORPUS SHAPE CHANGED under this session. {SESSION_CORPUS_PIN} records"
            f" {pinned_rows:,} rows / {pinned_bytes:,} Data.db bytes; --corpus {corpus} now"
            f" records {identity['rows']:,} rows / {identity['data_db_bytes']:,} bytes."
            " The digest matched, so this is an identity file that was edited rather than"
            " regenerated — two independent numbers must agree, not one."
        )
    return {
        "pinned_before_measurement": True,
        "pinned_corpus_path": pin.get("corpus"),
        "pinned_data_db_sha256": pinned_sha,
        "pinned_data_db_bytes": pinned_bytes,
        "pinned_rows": pinned_rows,
        # The WEAKEST of the three comparisons, reported rather than enforced: a corpus can
        # legitimately be moved, and the two byte-level fields already decided the question.
        "corpus_path_unchanged": pin.get("corpus") == str(corpus),
        "note": (
            "the corpus identity was captured in the session dir BEFORE the first rep and"
            " re-compared here on rows + data_db_bytes + sha256; the path is reported, not"
            " enforced (a corpus may be moved)"
        ),
    }


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
