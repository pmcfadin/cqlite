#!/usr/bin/env python3
"""
The driver's Python helpers, as an EXECUTABLE FILE rather than inline heredocs.

WHY THIS FILE EXISTS
--------------------
These two helpers used to live as `python3 - <<'EOF'` bodies inside `run_one` in
`ab-throughput.sh`. Nothing executed them: `run_one` needs a rig, so the whole
function -- including its record validator -- was covered by no test. That is
exactly how a 110-case self-test could be green while the driver's validator
hard-coded a SINGLE step record and made every `--ramp 1,2,4,8` session die
`replicate-invalid` after two release builds and a full measurement pass.

A helper that cannot be run on its own cannot be tested on its own. Both
subcommands here are directly invocable, and `selftest-analyze.sh` drives them
with real input, including the multi-step replicate that would have caught it.

TWO CONTRACTS, DELIBERATELY DIFFERENT
-------------------------------------
`validate-replicate` emits MESSAGES an operator reads, so every line it writes is
anchored `AB-3649: ` like the rest of the harness.

`parse-startup` returns a VALUE on stdout, consumed by command substitution in
the driver. It is deliberately NOT anchored -- an anchored value would have to be
stripped by the caller, and a caller that strips a prefix is a caller that can
strip the wrong thing. It writes exactly one line: the value, or the literal
`NOT-OBSERVED`. It never writes to stderr.
"""

import json
import re
import sys

from ab_common import err, out

NOT_OBSERVED = "NOT-OBSERVED"

USAGE = [
    "ab_driver_support.py validate-ramp <ramp>",
    "ab_driver_support.py validate-replicate <jsonl> <round-label> <ramp>",
    "ab_driver_support.py parse-startup <server-log> <scans|source>",
]


def parse_ramp(raw):
    """The declared ladder, or None if it is not one.

    `str.isdigit()` is True for characters like the superscript two, whose
    `int()` raises -- so the test is an explicit ASCII-digit match, not isdigit.
    A ramp must also be strictly increasing: duplicate or descending
    concurrencies make "the record at position i" ambiguous, which is the whole
    basis of the declared-versus-observed reconciliation below.
    """
    steps = []
    for part in raw.split(","):
        part = part.strip()
        if not re.fullmatch(r"[0-9]+", part):
            return None
        value = int(part)
        if value < 1:
            return None
        steps.append(value)
    if not steps:
        return None
    for earlier, later in zip(steps, steps[1:]):
        if later <= earlier:
            return None
    return steps


def ramp_section(steps):
    """Which analyzer section this ladder can be consumed by, or None.

    A ramp that maps to NEITHER section -- `--ramp 2` on its own, say -- would
    run a multi-hour session and produce a manifest no section will accept, so it
    is rejected before anything is built rather than discovered afterwards.
    """
    if steps == [1]:
        return "single-stream"
    if len(steps) >= 2:
        return "utilization"
    return None


def validate_ramp(raw_ramp):
    """Print `<top-step> <section>` as a VALUE, or refuse with a named cause."""
    steps = parse_ramp(raw_ramp)
    if steps is None:
        err("cause ramp-invalid")
        err(
            "cause-detail --ramp %r is not a strictly increasing, comma-separated "
            "list of positive integers. Every element is checked, not just the "
            "largest: a non-numeric token sorts as zero and would otherwise pass"
            % raw_ramp
        )
        return 1
    section = ramp_section(steps)
    if section is None:
        err("cause ramp-maps-to-no-section")
        err(
            "cause-detail --ramp %r is a single concurrency other than 1, so no "
            "analyzer section can consume the manifest it would produce: "
            "--single-stream requires exactly `1` and --utilization requires two "
            "or more steps" % raw_ramp
        )
        return 1
    sys.stdout.write("%d %s\n" % (steps[-1], section))
    return 0


def validate_replicate(path, tag, raw_ramp):
    steps = parse_ramp(raw_ramp)
    if steps is None:
        err("cause replicate-invalid")
        err("cause-detail %s: the ramp %r is not a strictly increasing list of "
            "positive integers" % (path, raw_ramp))
        return 1

    try:
        with open(path, encoding="utf-8") as handle:
            lines = [line for line in handle if line.strip()]
    except OSError as exc:
        err("cause replicate-invalid")
        err("cause-detail %s: %s" % (path, exc))
        return 1

    if len(lines) != len(steps):
        err("cause replicate-invalid")
        err(
            "cause-detail %s: %d step records, expected exactly %d -- "
            "flight-loadgen emits ONE record per ramp step and the declared ramp "
            "is %s" % (path, len(lines), len(steps), raw_ramp)
        )
        return 1

    single_stream = steps == [1]
    shed_seen = 0
    for position, line in enumerate(lines):
        try:
            record = json.loads(line)
        except ValueError as exc:
            err("cause replicate-invalid")
            err("cause-detail %s record %d: %s" % (path, position + 1, exc))
            return 1
        if not isinstance(record, dict):
            err("cause replicate-invalid")
            err("cause-detail %s record %d: not a JSON object" % (path, position + 1))
            return 1

        expected_concurrency = steps[position]
        problems = []
        if record.get("round") != tag:
            problems.append(
                "round is %r, expected %r" % (record.get("round"), tag)
            )
        if record.get("target_concurrency") != expected_concurrency:
            problems.append(
                "target_concurrency is %r, expected %d for ramp position %d"
                % (record.get("target_concurrency"), expected_concurrency, position)
            )
        if record.get("requests_error", 0):
            problems.append("requests_error=%s" % record["requests_error"])
        if not record.get("requests_ok", 0):
            problems.append("requests_ok=0")
        rate = record.get("rows_per_s", 0)
        if not isinstance(rate, (int, float)) or isinstance(rate, bool):
            problems.append("rows_per_s is not a number")
        elif not rate > 0 or rate != rate or rate in (float("inf"), float("-inf")):
            problems.append(
                "rows_per_s is %r -- not a positive finite rate; the scan returned "
                "no rows, or the duration was degenerate" % rate
            )
        if problems:
            err("cause replicate-invalid")
            for problem in problems:
                err("cause-detail %s record %d: %s" % (path, position + 1, problem))
            return 1

        shed = record.get("requests_unavailable", 0)
        if shed:
            shed_seen += 1
            if single_stream:
                # At concurrency 1 a shed can only mean something is badly wrong,
                # and the analyzer refuses such a run outright -- so refuse here,
                # while the rig is still up and the pin can be corrected.
                err("cause replicate-invalid")
                err(
                    "cause-detail %s record %d: requests_unavailable=%d at "
                    "concurrency 1 -- admission shedding (#2420) at single-stream "
                    "concurrency is not a throughput measurement"
                    % (path, position + 1, shed)
                )
                return 1
            # On a ramp the analyzer EXCLUDES shed steps and reports each
            # exclusion, so the driver must not contradict it by dying. Say so
            # loudly instead: shedding means --max-concurrent-scans was too low.
            out(
                "run %s step %d concurrency %d SHED requests-unavailable %d -- this "
                "step will be EXCLUDED by the analyzer; --max-concurrent-scans was "
                "too low for this ramp" % (tag, position, expected_concurrency, shed)
            )

        out(
            "run %s step %d concurrency %d rows-per-s %.2f requests-ok %d "
            "duration-s %.2f p50-ms %.3f"
            % (
                tag,
                position,
                expected_concurrency,
                float(record["rows_per_s"]),
                int(record["requests_ok"]),
                float(record.get("duration_s", 0.0)),
                float(record.get("latency_ms", {}).get("p50", 0.0)),
            )
        )

    out(
        "run %s validated %d step record(s) shed-steps %d RECOGNISED"
        % (tag, len(lines), shed_seen)
    )
    return 0


def parse_startup(path, want):
    """The resolved admission ceiling, or its provenance, from the server's own
    startup line -- or the literal NOT-OBSERVED.

    Best effort, and NAMED when it fails: a value we passed and a value the
    server resolved are different facts, and an unreadable line must never be
    silently upgraded into the value we hoped for.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    except OSError:
        return NOT_OBSERVED
    line = ""
    for candidate in text.splitlines():
        if "cqlite-flight starting" in candidate:
            line = candidate
    if not line:
        return NOT_OBSERVED
    if want == "scans":
        pattern = r"max_concurrent_scans[\"']?\s*[=:]\s*[\"']?(\d+)"
    else:
        pattern = r"max_concurrent_scans_source[\"']?\s*[=:]\s*[\"']?([A-Za-z-]+)"
    match = re.search(pattern, line)
    return match.group(1) if match else NOT_OBSERVED


def main(argv):
    if not argv:
        for line in USAGE:
            err(line)
        return 2
    command, rest = argv[0], argv[1:]
    if command == "validate-ramp":
        if len(rest) != 1:
            err("usage-error validate-ramp needs <ramp>")
            return 2
        return validate_ramp(rest[0])
    if command == "validate-replicate":
        if len(rest) != 3:
            err("usage-error validate-replicate needs <jsonl> <round-label> <ramp>")
            return 2
        return validate_replicate(rest[0], rest[1], rest[2])
    if command == "parse-startup":
        if len(rest) != 2 or rest[1] not in ("scans", "source"):
            err("usage-error parse-startup needs <server-log> <scans|source>")
            return 2
        sys.stdout.write(parse_startup(rest[0], rest[1]) + "\n")
        return 0
    err("usage-error unknown subcommand: %s" % command)
    for line in USAGE:
        err(line)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
