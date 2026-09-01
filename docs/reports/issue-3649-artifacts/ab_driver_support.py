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
import os
import re
import sys

from ab_common import err, out

NOT_OBSERVED = "NOT-OBSERVED"

USAGE = [
    "ab_driver_support.py pair-order <replicate>",
    "ab_driver_support.py effective-flag <flag> <global-value> <extra-string>",
    "ab_driver_support.py census-served <data-dir> <ticket.json>",
    "ab_driver_support.py parse-listening <server-log>",
    "ab_driver_support.py validate-ramp <ramp>",
    "ab_driver_support.py parse-duration <value>",
    "ab_driver_support.py validate-ticket <template.json>",
    "ab_driver_support.py check-affinity <pid> <cpu-list>",
    "ab_driver_support.py validate-replicate <jsonl> <round-label> <ramp>",
    "ab_driver_support.py parse-startup <server-log> "
    "<scans|source|batch-size|max-batch-bytes|wait-timeout-ms>",
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


# The startup-line fields worth reading back. `max_concurrent_scans` is the one
# the server RESOLVES (it may derive a value we did not pass); the rest are
# echoes of what we passed, and reading them back is how we know we configured
# the process we are actually talking to.
STARTUP_FIELDS = {
    "scans": r"max_concurrent_scans[\"']?\s*[=:]\s*[\"']?(\d+)",
    "source": r"max_concurrent_scans_source[\"']?\s*[=:]\s*[\"']?([A-Za-z-]+)",
    "batch-size": r"batch_size[\"']?\s*[=:]\s*[\"']?(\d+)",
    "max-batch-bytes": r"max_batch_bytes[\"']?\s*[=:]\s*[\"']?(\d+)",
    "wait-timeout-ms": r"admission_wait_timeout_ms[\"']?\s*[=:]\s*[\"']?(\d+)",
}


def parse_duration_seconds(raw):
    """MIRRORS `flight_loadgen::ramp::parse_duration` -- deliberately, field for
    field (tools/flight-loadgen/src/ramp.rs:224).

    A grammar STRICTER than the load generator's rejects work that has already
    been done: `--step-duration 60` is valid there (a bare number is seconds), so
    a session can build both arms, run every replicate and meter a rig for an
    hour, and then be refused by the analyzer over a missing unit suffix. On a
    box you cannot get back, a false refusal AFTER the data exists is worse than
    one before, because the input cannot be regenerated.

    So: optional `ms` / `m` / `s` suffix, bare means seconds, the number parsed
    as a float and required finite and non-negative, exactly as there. Returns
    None if it is not a duration at all.
    """
    text = raw.strip()
    for suffix, scale in (("ms", 0.001), ("m", 60.0), ("s", 1.0)):
        if text.endswith(suffix):
            text, factor = text[: -len(suffix)], scale
            break
    else:
        factor = 1.0
    try:
        value = float(text)
    except ValueError:
        return None
    if value != value or value in (float("inf"), float("-inf")) or value < 0.0:
        return None
    seconds = value * factor
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        return None
    # `Duration::try_from_secs_f64` is applied to the FINAL, SCALED value there,
    # and rejects anything at or beyond u64 seconds -- which is how "1e30" and
    # "1e308m" are refused. Mirrored, because a docstring claiming a field-for-
    # field mirror has to be true: accepting what the load generator rejects is a
    # smaller problem than rejecting what it accepts, but it is still a false
    # claim about the code.
    if seconds >= 18446744073709551616.0:
        return None
    return seconds


def validate_duration(raw):
    """Print the canonical seconds as a VALUE, or refuse with a named cause."""
    seconds = parse_duration_seconds(raw)
    if seconds is None:
        err("cause step-duration-invalid")
        err(
            "cause-detail --step-duration %r is not a duration flight-loadgen "
            "would accept: an optional ms/m/s suffix on a finite, non-negative "
            "number, bare meaning seconds" % raw
        )
        return 1
    if seconds <= 0.0:
        err("cause step-duration-invalid")
        err(
            "cause-detail --step-duration %r is zero: a step that holds for no "
            "time measures nothing" % raw
        )
        return 1
    sys.stdout.write("%r\n" % seconds)
    return 0


# A full-ring, unprojected, unfiltered ticket -- the only shape the target band
# is defined for. Each entry is (field, what makes it NOT a full scan).
_TICKET_NARROWING = (
    ("limit", "carries a LIMIT"),
    ("filter", "carries a filter"),
    ("aggregation", "carries an aggregation"),
    ("columns", "projects a column subset"),
    ("token_start", "starts at a token bound"),
    ("token_end", "ends at a token bound"),
)


def validate_ticket(path):
    """Refuse a ticket that is not a full-ring scan of every column."""
    try:
        with open(path, encoding="utf-8") as handle:
            ticket = json.load(handle)
    except (OSError, ValueError) as exc:
        err("cause ticket-template-unparseable")
        err("cause-detail %s: %s" % (path, exc))
        return 1
    if not isinstance(ticket, dict):
        err("cause ticket-template-unparseable")
        err("cause-detail %s: top level is not an object" % path)
        return 1
    problems = []
    for field, why in _TICKET_NARROWING:
        if ticket.get(field) is not None:
            problems.append("%s (%s = %r)" % (why, field, ticket[field]))
    predicates = ticket.get("predicates")
    if isinstance(predicates, list) and predicates:
        problems.append("carries %d predicate(s)" % len(predicates))
    if ticket.get("wraparound"):
        problems.append("sets wraparound")
    if problems:
        err("cause ticket-not-full-ring")
        err(
            "cause-detail %s is not a full-ring scan of every column -- it %s. "
            "The #3649 target band is defined for `--shape full` over the whole "
            "ring; a narrowed ticket would receive a verdict against a band that "
            "does not describe it. Pass --control <label> to run it anyway"
            % (path, "; it ".join(problems))
        )
        return 1
    return 0


def check_affinity(pid, expected):
    """Is the server actually pinned where we asked? Requested and effective are
    different facts, and a mis-pinned server invalidates the measurement without
    reporting anything."""
    want = expand_cpu_list(expected)
    if want is None:
        err("cause affinity-unverifiable")
        err("cause-detail %r is not a CPU list" % expected)
        return 1
    path = "/proc/%s/status" % pid
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        # Not every platform exposes this; an unreadable /proc is UNVERIFIABLE,
        # never "pinned correctly".
        sys.stdout.write("UNVERIFIABLE\n")
        return 0
    match = re.search(r"^Cpus_allowed_list:\s*(\S+)", text, re.MULTILINE)
    if not match:
        sys.stdout.write("UNVERIFIABLE\n")
        return 0
    have = expand_cpu_list(match.group(1))
    if have is None:
        sys.stdout.write("UNVERIFIABLE\n")
        return 0
    if have != want:
        err("cause affinity-mismatch")
        err(
            "cause-detail pid %s is allowed CPUs %s but %s was requested; the "
            "server is not pinned where the manifest will say it is"
            % (pid, match.group(1), expected)
        )
        return 1
    sys.stdout.write("VERIFIED\n")
    return 0


def expand_cpu_list(spec):
    cpus = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, _, high = part.partition("-")
            if not (re.fullmatch(r"[0-9]+", low) and re.fullmatch(r"[0-9]+", high)):
                return None
            cpus.update(range(int(low), int(high) + 1))
        elif re.fullmatch(r"[0-9]+", part):
            cpus.add(int(part))
        else:
            return None
    return cpus or None


def resolve_served_dir(data_dir, keyspace, table, snapshot):
    """MIRRORS `DirSource::resolve` / `table_base_dir`
    (cqlite-flight/src/producer.rs:160-204).

    `<data>/<ks>/<table>` when that is a directory; otherwise the
    LEXICOGRAPHICALLY LARGEST `<data>/<ks>/<table>-*` directory; otherwise the
    exact non-existent path. A non-empty snapshot resolves to
    `<table-dir>/snapshots/<name>/`.

    Mirrored for the same reason the duration grammar is: a census that answers a
    different question from the server's is not a census of the measurement. The
    old one scanned the WHOLE data root recursively, so unrelated tables,
    snapshot directories and hard-linked copies all counted toward both the
    256 MiB floor and the >=2-SSTable check -- and that second one exists
    specifically to stop the #3058 single-source bypass. Green census,
    single-source served table, both arms bypass the merger, ratio 1.0 by
    construction, rendered as a confident verdict.
    """
    base = os.path.join(data_dir, keyspace)
    exact = os.path.join(base, table)
    table_dir = exact
    if not os.path.isdir(exact):
        prefix = table + "-"
        best = None
        try:
            for name in os.listdir(base):
                candidate = os.path.join(base, name)
                if name.startswith(prefix) and os.path.isdir(candidate):
                    if best is None or candidate > best:
                        best = candidate
        except OSError:
            best = None
        table_dir = best if best is not None else exact
    if snapshot:
        return os.path.join(table_dir, "snapshots", snapshot)
    return table_dir


def census_served(data_dir, ticket_path):
    """Census the ONE directory the ticket will actually be served from.

    Enumeration is FLAT, not recursive, because the producer's is
    (cqlite-flight/src/producer.rs:210-220 reads the resolved dir and filters
    `*-Data.db` directly in it). A recursive count would re-admit the
    `snapshots/` subtree the server does not read for a live-dir ticket.

    Prints `<files> <bytes> <dir>` as a VALUE.
    """
    try:
        with open(ticket_path, encoding="utf-8") as handle:
            ticket = json.load(handle)
    except (OSError, ValueError) as exc:
        err("cause ticket-template-unparseable")
        err("cause-detail %s: %s" % (ticket_path, exc))
        return 1
    if not isinstance(ticket, dict):
        err("cause ticket-template-unparseable")
        err("cause-detail %s: top level is not an object" % ticket_path)
        return 1
    keyspace, table = ticket.get("keyspace"), ticket.get("table")
    for name, value in (("keyspace", keyspace), ("table", table)):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_]+", value):
            err("cause ticket-identifier-invalid")
            err(
                "cause-detail %s: %s is %r; cqlite-flight requires an unquoted "
                "Cassandra identifier (cqlite-flight/src/ticket.rs:328-333)"
                % (ticket_path, name, value)
            )
            return 1
    snapshot = ticket.get("snapshot")
    if snapshot is not None and (
        not isinstance(snapshot, str) or not re.fullmatch(r"[A-Za-z0-9_-]*", snapshot)
    ):
        err("cause ticket-identifier-invalid")
        err("cause-detail %s: snapshot is %r" % (ticket_path, snapshot))
        return 1

    served = resolve_served_dir(data_dir, keyspace, table, snapshot or None)
    if not os.path.isdir(served):
        err("cause served-dir-absent")
        err(
            "cause-detail the ticket resolves to %s, which does not exist under "
            "--data-dir %s. The census must describe the directory the server "
            "will read, not the disk it sits on" % (served, data_dir)
        )
        return 1
    total = 0
    count = 0
    # CONTAINMENT, not just scope. `DirSource::data_paths` excludes any entry
    # whose CANONICAL target escapes the resolved directory
    # (cqlite-flight/src/producer.rs:215-235, `pathsafe::assert_within`), because
    # a symlink inside an otherwise-valid directory can point anywhere. Mirroring
    # the enumeration without the containment check left symlinked decoys
    # satisfying both floors -- the round-4 fix closed the recursive-scan route
    # and this one stayed open. A Cassandra snapshot is a HARD link, which
    # canonicalises inside the directory and is therefore still counted.
    try:
        served_real = os.path.realpath(served)
        for name in os.listdir(served):
            if not name.endswith("-Data.db"):
                continue
            path = os.path.join(served, name)
            if not os.path.isfile(path):
                continue
            real = os.path.realpath(path)
            if os.path.dirname(real) != served_real:
                # Excluded exactly as the server excludes it, and counted by
                # neither floor.
                continue
            total += os.path.getsize(real)
            count += 1
    except OSError as exc:
        err("cause corpus-census-failed")
        err("cause-detail %s: %s" % (served, exc))
        return 1
    sys.stdout.write("%d %d %s\n" % (count, total, served))
    return 0


def parse_listening(path):
    """The address OUR OWN server reports having bound.

    `cli::log_listening` is emitted only AFTER a listener exists
    (cqlite-flight/src/cli.rs:228-241), so its presence is proof this process
    owns the socket -- which "something answered on the port" never was. With
    `--listen 127.0.0.1:0` it is also the only place the actual port appears.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    except OSError:
        return None
    found = None
    for line in text.splitlines():
        if "listening on" not in line:
            continue
        match = re.search(r"listening_on[\"']?\s*[=:]\s*[\"']?(\S+?)[\"',\s]*$", line)
        if not match:
            match = re.search(r"listening on\s+(\S+)", line)
        if match:
            found = match.group(1).strip("\"' ,")
    return found


def effective_flag(flag, global_value, extra):
    """The value a server will actually use for `flag`, given this arm's extras.

    ONE implementation of "what did this arm really get", because two rules that
    were each correct produced an unusable whole: round 2 required the analyzer
    to refuse cross-arm server-config differences, round 5 required asymmetric
    per-arm flags to carry `--control`, and nothing reconciled them -- so the
    runbook's own sensitivity control, which deliberately sets the head arm's
    `--max-batch-bytes 1`, was refused as a mismatch before the control label was
    even looked at. The control that tells you whether an INCONCLUSIVE means "no
    effect" or "this box cannot measure" could not be run.

    The fix is a DECLARED, STRUCTURED EXPECTATION rather than a blanket rule with
    an exception: each arm's effective configuration is computed here, recorded
    in the manifest as data, and the analyzer permits exactly the differences the
    manifest declares -- under a control label, and only where the observed
    values match the declared ones.

    The LAST occurrence wins, as it does on a real command line.
    """
    words = extra.split()
    value = global_value
    for index, word in enumerate(words):
        if word == flag and index + 1 < len(words):
            value = words[index + 1]
    return value


def pair_order(replicate):
    """Which arm runs FIRST in this replicate's pair.

    Executable, and therefore testable, for the reason round 1 paid for: the rule
    used to be three lines inline in the session loop, which needs a rig, so
    nothing could run it. This is the one rule in the driver whose failure mode is
    a CONFIDENT WRONG ANSWER rather than an error -- if base always ran first, a
    monotonic drift within a pair would land on the head arm every time and bias
    every ratio in one direction, and every test of the statistics would still
    pass. A rule like that must not be the untested one.

    Base first on odd replicates, head first on even ones, so over an even count
    each ordering runs exactly half the time.
    """
    return ("base", "head") if replicate % 2 == 1 else ("head", "base")


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
    match = re.search(STARTUP_FIELDS[want], line)
    return match.group(1) if match else NOT_OBSERVED


def main(argv):
    if not argv:
        for line in USAGE:
            err(line)
        return 2
    command, rest = argv[0], argv[1:]
    if command == "census-served":
        if len(rest) != 2:
            err("usage-error census-served needs <data-dir> <ticket.json>")
            return 2
        return census_served(rest[0], rest[1])
    if command == "parse-listening":
        if len(rest) != 1:
            err("usage-error parse-listening needs <server-log>")
            return 2
        bound = parse_listening(rest[0])
        sys.stdout.write((bound or "NOT-OBSERVED") + "\n")
        return 0
    if command == "effective-flag":
        if len(rest) != 3:
            err("usage-error effective-flag needs <flag> <global-value> <extra-string>")
            return 2
        sys.stdout.write(effective_flag(rest[0], rest[1], rest[2]) + "\n")
        return 0
    if command == "pair-order":
        if len(rest) != 1 or not re.fullmatch(r"[0-9]+", rest[0]) or int(rest[0]) < 1:
            err("usage-error pair-order needs a positive integer <replicate>")
            return 2
        sys.stdout.write("%s %s\n" % pair_order(int(rest[0])))
        return 0
    if command == "validate-ramp":
        if len(rest) != 1:
            err("usage-error validate-ramp needs <ramp>")
            return 2
        return validate_ramp(rest[0])
    if command == "parse-duration":
        if len(rest) != 1:
            err("usage-error parse-duration needs <value>")
            return 2
        return validate_duration(rest[0])
    if command == "validate-ticket":
        if len(rest) != 1:
            err("usage-error validate-ticket needs <template.json>")
            return 2
        return validate_ticket(rest[0])
    if command == "check-affinity":
        if len(rest) != 2:
            err("usage-error check-affinity needs <pid> <cpu-list>")
            return 2
        return check_affinity(rest[0], rest[1])
    if command == "validate-replicate":
        if len(rest) != 3:
            err("usage-error validate-replicate needs <jsonl> <round-label> <ramp>")
            return 2
        return validate_replicate(rest[0], rest[1], rest[2])
    if command == "parse-startup":
        if len(rest) != 2 or rest[1] not in STARTUP_FIELDS:
            err("usage-error parse-startup needs <server-log> <%s>"
                % "|".join(sorted(STARTUP_FIELDS)))
            return 2
        sys.stdout.write(parse_startup(rest[0], rest[1]) + "\n")
        return 0
    err("usage-error unknown subcommand: %s" % command)
    for line in USAGE:
        err(line)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
