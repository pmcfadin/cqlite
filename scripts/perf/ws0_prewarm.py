#!/usr/bin/env python3
"""DID THE PREWARM ACTUALLY WARM ANYTHING? — the affirmative measurement (#3272 round 10, F-A).

# The finding

`measure_flight` recorded `prewarm_status="ok"` from the load generator's EXIT STATUS alone:

    if taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" … --out /dev/null > …; then
      prewarm_status="ok"

and it passed `--out /dev/null`, DISCARDING the per-request JSONL that is the only record of what
the prewarm did. `flight-loadgen` exits 0 whenever the ramp completes, and a step whose every
request was classified `unavailable` (admission shed, #2420) or `error` completes normally — the
outcomes are COUNTED, not fatal. So a prewarm that served nothing, or that streamed zero rows, was
recorded as a healthy prewarm, and the rep it belongs to claims a WARM measurement having faulted in
nothing.

That is the same class as this issue's AC1 finding 2 (`skipped-cold-arm` counting as a successful
prewarm at any temperature) reappearing at a different line — the "a fix moved the problem" pattern
this split was opened for. The remedy is symmetric with the one AC1 took: a status may only read
`ok` when a MEASUREMENT SAYS SO.

# The rule

`ok` requires ALL of:

* the loadgen EXITED ZERO (necessary, and by itself was the defect);
* the retained JSONL exists, parses, and holds AT LEAST ONE step record — a run that wrote nothing
  measured nothing;
* `requests_ok >= 1` summed across its records — at least one request actually completed;
* `rows_total >= 1` — the completed requests streamed rows. A request can complete having streamed
  an empty stream, and an empty stream warms no page cache.

Anything else is a LABELLED DEGRADATION, never `ok`. The honest-degradation behaviour AC1 chose is
kept exactly: the Flight arm records the label and CONTINUES (a degraded Flight prewarm biases
AGAINST do_get, so it cannot manufacture a win, and a rep labelled `prewarm-failed` is more useful
than no rep), and `ws0_report.py` surfaces the label in every report it writes.

# Why NOT a "zero unavailable/error" rule

The reviewer suggested additionally requiring zero `unavailable`/`error`. Deliberately not adopted:
this is the UNTIMED prewarm, whose only job is to fault the corpus in and open the readers. A step
in which 9 of 10 requests were shed but one completed a full-corpus scan DID warm the corpus, and
refusing it would be a guard firing on a healthy prewarm — the mirror-image broken instrument (AC1's
own lesson: the documented operator command that could never go green). The MEASURED reps are where
a shed request is fatal, and there `ws0_loadgen_record.ZERO_REQUIRED_COUNTERS` already refuses any
non-zero `requests_unavailable`/`requests_error`. Prewarm asks "did anything get warmed", measurement
asks "was this a clean full-corpus scan"; conflating them would weaken neither-nor.

The counts are still RECORDED in the status label, so a shedding prewarm is visible to a reader
rather than merely tolerated.
"""

from __future__ import annotations

import json
import pathlib

# Status strings this module produces. `ok` is the ONLY one `ws0_validate.classify_prewarm` accepts
# for a warm rep; every other value here is a labelled degradation that lands in
# `prewarm_all_ok=false` and in the report's PREWARM DEGRADED line.
STATUS_OK = "ok"


def classify_prewarm_jsonl(exit_status: int, jsonl_path: pathlib.Path) -> str:
    """The status string to record for a Flight prewarm, from OBSERVED evidence.

    `ok` only on an affirmative measurement (see the module docstring). Never raises: the caller is
    a shell measurement loop whose whole contract is to record an honest label and continue, so an
    unreadable or absent artifact becomes a NAMED degradation rather than a traceback that would
    abort the rep.
    """
    if exit_status != 0:
        return f"FAILED-exit-{exit_status}"
    if not jsonl_path.exists():
        # The loadgen exited 0 but produced no record. Pre-fix this was INVISIBLE by construction:
        # `--out /dev/null` meant there was never a record to be absent.
        return "FAILED-no-jsonl"
    try:
        text = jsonl_path.read_text()
    except OSError as exc:
        return f"FAILED-unreadable-jsonl-{type(exc).__name__}"
    records = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            return "FAILED-malformed-jsonl"
        if not isinstance(rec, dict):
            return "FAILED-malformed-jsonl"
        records.append(rec)
    if not records:
        return "FAILED-empty-jsonl"

    # Summed across records rather than read from one: `--ramp 1` yields a single step, but a
    # future ramp must not be read as its last step alone (the defect `ws0_flight_arm` refuses for
    # the MEASURED reps). A non-integer or negative counter is not a count — treated as absent, so
    # it cannot satisfy the threshold.
    def total(key: str) -> int:
        acc = 0
        for rec in records:
            value = rec.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                return -1
            acc += value
        return acc

    requests_ok = total("requests_ok")
    rows_total = total("rows_total")
    if requests_ok < 0 or rows_total < 0:
        return "FAILED-uncounted-requests"
    if requests_ok < 1:
        # Exit 0 with nothing served. Every request was shed or errored, and the ramp completed
        # normally because those outcomes are COUNTED, not fatal.
        return "FAILED-zero-successful-requests"
    if rows_total < 1:
        # Requests completed but streamed no rows: an empty stream warms no page cache, so the rep
        # that follows is not warm however the label reads.
        return "FAILED-zero-rows"

    # A shed/errored request alongside a successful one does NOT downgrade this status — see the
    # module docstring for why (the prewarm's job demonstrably happened, and refusing it would be a
    # guard firing on a healthy prewarm). The counts are not lost: they are in the RETAINED
    # `<tag>.prewarm.jsonl`, which exists precisely because this fix stopped discarding it, and the
    # MEASURED reps refuse any non-zero value of either (`ws0_loadgen_record`).
    #
    # The status is returned EXACTLY `ok` rather than an `ok`-prefixed variant, because
    # `ws0_validate.PREWARM_REQUIRED` is an EXACT per-temperature match: a decorated `ok-…` would be
    # classified `degraded` and would flag every such rep. One vocabulary, one meaning.
    return STATUS_OK


def main(argv: list[str]) -> int:
    """`ws0_prewarm.py <exit-status> <jsonl-path>` — prints the status string. Always exits 0.

    Exits 0 even for a degradation: the STATUS is the output, and a non-zero exit here would make
    the calling shell's `set -e` abort a rep the rig has decided to keep and label (the honest
    degradation AC1 chose).
    """
    if len(argv) != 3:
        print("FAILED-bad-classifier-invocation")
        return 0
    try:
        status = int(argv[1])
    except ValueError:
        print("FAILED-bad-classifier-invocation")
        return 0
    print(classify_prewarm_jsonl(status, pathlib.Path(argv[2])))
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv))
