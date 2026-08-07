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
  an empty stream, and an empty stream warms no page cache;
* ...and, since round 12's F2, EVERY successful request STREAMED THE WHOLE CORPUS — see below.

Anything else is a LABELLED DEGRADATION, never `ok`. The honest-degradation behaviour AC1 chose is
kept exactly: the Flight arm records the label and CONTINUES (a degraded Flight prewarm biases
AGAINST do_get, so it cannot manufacture a win, and a rep labelled `prewarm-failed` is more useful
than no rep), and `ws0_report.py` surfaces the label in every report it writes.

# ROUND 12, F2 — A NON-ZERO SCAN IS NOT A COMPLETE SCAN, AND ONLY A COMPLETE ONE WARMS THE CORPUS

F-A's rule above was `requests_ok >= 1 AND rows_total >= 1`. That is a NON-ZERO check, and the
property a prewarm exists to establish is a COMPLETENESS one: the prewarm's entire job is to fault
THE WHOLE CORPUS in, so a request that streamed 40 of 200,000 rows satisfied every clause of F-A
while leaving essentially every page cold — and the reps that followed were reported WARM. Improving
"exit status" to "at least one row" moved the bar without reaching the property, which is this
issue's dominant defect class (a fix that lands one field short) recurring inside the fix for it.

THE ORACLE IS THE PINNED CORPUS ROW COUNT, never a threshold. `session-corpus-pin.json` records
`rows` — stamped before the first rep, over the corpus this session is measuring — so the completeness
question has an authoritative answer already on disk and there is no reason to invent a floor. A
threshold would be a heuristic, and a heuristic here is a number somebody chose rather than a number
that was measured.

The rule, symmetric with `ws0_flight_arm`'s check for the MEASURED reps:

    rows_total == requests_ok * pinned_rows

i.e. every successful request scanned the whole corpus. It deliberately keeps F-A's shed-tolerance —
2 requests served and 7 shed with `rows_total == 2 * pinned_rows` is still `ok`, because the corpus
demonstrably got faulted in — and it deliberately does NOT accept a set of PARTIAL scans that happen
to sum to the corpus (3 requests of a third each): nothing in the record says those thirds were
disjoint, so the sum establishes nothing about coverage. The strongest available statement is that
each completed request was a full pass, and that is what is required.

An ABSENT or unreadable pin is a NAMED DEGRADATION (`FAILED-no-corpus-pin`), never a skip: the pin
is the only oracle for this check, so a session without one cannot establish the property — and a
check that silently does not run prints exactly like one that passed.

# ...AND THE BARE-SCAN PATH, which had the value and threw it away

`measure_scan`'s prewarm ran `ws0-scan-bench --passes 1` and trusted PROCESS SUCCESS, while
redirecting the bench's JSON — which carries `rows_denominator` and a per-pass `rows` — to a file
nobody read. `scan_bench` refuses a ZERO-row pass itself, so exit 0 did establish "something was
read"; it establishes nothing about how much. A partial ingestion (round 10's F-B class: a table-dir
selection that picked up fewer directories than intended) exits 0 having scanned a fraction.

So [`classify_prewarm_scan_json`] reads that retained JSON and requires every timed pass to have
observed EXACTLY the pinned row count. The bare-scan arm stays FAIL-CLOSED on the result, unlike the
Flight arm's record-and-continue, for the reason `lib-measure.sh` records: a partly-cold bare scan
reads SLOWER, which SHRINKS `bare/flight` and makes the 1.3x target EASIER — a degradation that can
manufacture a win.

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

from ws0_session import session_pin_path

# Status strings this module produces. `ok` is the ONLY one `ws0_validate.classify_prewarm` accepts
# for a warm rep; every other value here is a labelled degradation that lands in
# `prewarm_all_ok=false` and in the report's PREWARM DEGRADED line.
STATUS_OK = "ok"

# The two prewarm legs this module classifies, as an EXACT closed set. An unrecognised arm is a
# labelled failure rather than a default, because defaulting to either one would classify a leg by
# the OTHER leg's rule — the Flight leg's artifact is a step JSONL and the bare scan's is the
# bench's JSON, so the wrong reader would report `FAILED-malformed-…` about a healthy prewarm.
PREWARM_ARMS = ("flight", "scan")


def pinned_corpus_rows(session_dir: pathlib.Path) -> int | str:
    """The PINNED corpus row count — the completeness oracle — or a degradation LABEL (F2).

    `session-corpus-pin.json` is stamped before the first rep, over the corpus this session is
    measuring, so "did the prewarm read the whole corpus" has an authoritative answer already on
    disk. A THRESHOLD is deliberately not used anywhere in this module: a floor is a number somebody
    chose, and the pin is a number that was measured.

    Returns the row count, or a `FAILED-…` label string. NEVER raises and never returns a
    placeholder count: an absent, unreadable or non-positive pin means the oracle for this check
    could not be consulted, and a check that could not run must not produce a passing verdict.

    Read DIRECTLY from the session dir rather than accepted as an argument from the shell,
    deliberately — the same argument `ws0_session._measure_ticket_digest` records: a caller-supplied
    count is a value this classifier would compare against without having observed it, and the
    caller that gets it wrong is exactly the caller whose prewarm is being judged.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        return "FAILED-no-corpus-pin"
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError):
        return "FAILED-unreadable-corpus-pin"
    if not isinstance(pin, dict):
        return "FAILED-unreadable-corpus-pin"
    rows = pin.get("rows")
    # `isinstance(True, int)` is True, so bools are excluded explicitly; a fractional or negative
    # value is not a row count. Same strictness as `ws0_validate.positive_int`, restated here rather
    # than imported because that helper RAISES and this module's contract is that it cannot.
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 1:
        return "FAILED-uncounted-corpus-pin"
    return rows


def _incomplete_label(rows_total: int, requests_ok: int, pinned_rows: int) -> str:
    """The label for a prewarm that read SOME of the corpus — carrying both numbers (F2)."""
    return (
        f"FAILED-partial-scan-{rows_total}-of-{requests_ok * pinned_rows}-rows"
    )


def classify_prewarm_jsonl(
    exit_status: int, jsonl_path: pathlib.Path, session_dir: pathlib.Path
) -> str:
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

    # COMPLETENESS, against the PINNED row count (#3272 round 12, F2). Everything above is a
    # NON-ZERO check, and a prewarm's job is to fault THE WHOLE CORPUS in — so a request that
    # streamed 40 of 200,000 rows satisfied every clause above while leaving essentially every page
    # cold, and the rep that followed was reported WARM.
    #
    # `requests_ok * pinned_rows`, i.e. EVERY completed request was a full pass — the same rule
    # `ws0_flight_arm` applies to the MEASURED reps. A set of PARTIAL scans that happens to SUM to
    # the corpus is deliberately NOT accepted: nothing in the record says those fractions were
    # disjoint, so the sum establishes nothing about coverage.
    pinned = pinned_corpus_rows(session_dir)
    if isinstance(pinned, str):
        # The oracle could not be consulted, so the property is UNVERIFIED — which is a degradation,
        # never an `ok`. The label names which half is missing.
        return pinned
    if rows_total != requests_ok * pinned:
        return _incomplete_label(rows_total, requests_ok, pinned)

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


def classify_prewarm_scan_json(
    exit_status: int, json_path: pathlib.Path, session_dir: pathlib.Path
) -> str:
    """The status string for the BARE-SCAN prewarm, from the bench's OWN JSON (#3272 round 12, F2).

    # The finding

    `measure_scan`'s prewarm trusted PROCESS SUCCESS while redirecting `ws0-scan-bench`'s JSON — which
    carries `rows_denominator` and a per-pass `rows` — to a file nobody read. `scan_bench` refuses a
    ZERO-row pass itself, so exit 0 did establish "something was read"; it establishes nothing about
    HOW MUCH. A partial ingestion (round 10's F-B class — a table-dir selection picking up fewer
    directories than intended) exits 0 having scanned a fraction, and the reps that follow are
    reported WARM over a mostly-cold cache.

    So the value that was being DISCARDED is read, and every timed pass must have observed EXACTLY
    the PINNED corpus row count. Same oracle as the Flight leg, same refusal of a threshold.

    Never raises, for the same reason `classify_prewarm_jsonl` does not: the caller is a shell
    measurement loop, and a traceback inside it would abort the rep at a point where the host's
    sysctls are already weakened. The bare-scan CALLER then fails closed on the label (a partly-cold
    bare scan reads slower, shrinking `bare/flight` — a degradation that can manufacture a win),
    which is a decision about the LEG, made in `lib-measure.sh`, not about the classification.
    """
    if exit_status != 0:
        return f"FAILED-exit-{exit_status}"
    if not json_path.exists():
        return "FAILED-no-scan-json"
    try:
        payload = json.loads(json_path.read_text())
    except (OSError, ValueError):
        return "FAILED-malformed-scan-json"
    if not isinstance(payload, dict):
        return "FAILED-malformed-scan-json"
    passes = payload.get("passes")
    if not isinstance(passes, list) or not passes:
        # Exit 0 with no per-pass record: `scan_bench` cannot produce this, so the artifact is not
        # the one this classifier models — which is a failure, not something to read around.
        return "FAILED-no-scan-passes"
    pinned = pinned_corpus_rows(session_dir)
    if isinstance(pinned, str):
        return pinned
    observed = 0
    for rec in passes:
        if not isinstance(rec, dict):
            return "FAILED-malformed-scan-json"
        rows = rec.get("rows")
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 1:
            return "FAILED-uncounted-scan-rows"
        # PER PASS, not on the sum, and for the reason the Flight leg refuses a summed set: two
        # half-passes summing to the corpus say nothing about which half each covered.
        if rows != pinned:
            return f"FAILED-partial-scan-{rows}-of-{pinned}-rows"
        observed += rows
    # ...and the bench's OWN aggregate must agree with the per-pass records it published. A
    # `rows_denominator` that disagrees means the artifact is internally inconsistent, and this
    # classifier would otherwise have validated the half of it nobody divides by.
    denom = payload.get("rows_denominator")
    if isinstance(denom, bool) or not isinstance(denom, int) or denom != observed:
        return f"FAILED-scan-denominator-{denom}-vs-{observed}-rows"
    return STATUS_OK


def main(argv: list[str]) -> int:
    """`ws0_prewarm.py <arm> <exit-status> <artifact> <session-dir>` — prints the status string.

    Always exits 0, even for a degradation: the STATUS is the output, and a non-zero exit here would
    make the calling shell's `set -e` abort a rep the rig has decided to keep and label (the honest
    degradation AC1 chose). For the bare-scan leg the CALLER fails closed on the label instead.

    The ARM is explicit rather than sniffed from the artifact's shape (#3272 round 12, F2): guessing
    which reader to use from the file's contents would classify a truncated Flight JSONL as a
    bare-scan artifact and report a shape failure instead of the partial scan that caused it.
    """
    if len(argv) != 5 or argv[1] not in PREWARM_ARMS:
        print("FAILED-bad-classifier-invocation")
        return 0
    try:
        status = int(argv[2])
    except ValueError:
        print("FAILED-bad-classifier-invocation")
        return 0
    artifact, session = pathlib.Path(argv[3]), pathlib.Path(argv[4])
    if argv[1] == "flight":
        print(classify_prewarm_jsonl(status, artifact, session))
    else:
        print(classify_prewarm_scan_json(status, artifact, session))
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main(sys.argv))
