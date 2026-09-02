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
import subprocess
import sys

from ab_common import MIN_CORPUS_BYTES_FLOOR, MIN_SSTABLES_FLOOR, Unmeasured, err, out
from ab_input import validate_record_shape, validate_record_usable

NOT_OBSERVED = "NOT-OBSERVED"

USAGE = [
    "ab_driver_support.py pair-order <replicate>",
    "ab_driver_support.py effective-flag <flag> <global-value> <extra-string>",
    "ab_driver_support.py server-argv <bin> <data-dir> <listen> <batch> <maxbytes> "
    "<wait> <scans> <extra>",
    "ab_driver_support.py resolve-session <batch> <maxbytes> <wait> <scans> "
    "<min-bytes> <min-sstables> <ramp> <control> <base-extra> <head-extra>",
    "ab_driver_support.py census-served <data-dir> <ticket.json>",
    "ab_driver_support.py probe-storage <path>",
    "ab_driver_support.py probe-compression <served-dir>",
    "ab_driver_support.py canonical-shape <shape>",
    "ab_driver_support.py parse-listening <server-log>",
    "ab_driver_support.py validate-ramp <ramp>",
    "ab_driver_support.py parse-duration <value>",
    "ab_driver_support.py validate-ticket <template.json>",
    "ab_driver_support.py validate-ticket-schema <template.json>",
    "ab_driver_support.py check-affinity <pid> <cpu-list>",
    "ab_driver_support.py validate-replicate <jsonl> <round-label> <ramp> "
    "<shape> <step-duration-s>",
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
)


def _token_range_is_full_ring(ticket):
    """MIRRORS `FlightTicket::token_in_range` (cqlite-flight/src/ticket.rs:395-421).

    Read from the source rather than assumed, and the source says something
    narrower than "any token bound is a narrowing":

      * both endpoints absent -> `(None, None)` -> no range filter at all;
      * endpoints EQUAL -> wrapping is DERIVED as `start >= end`, so membership
        is `token > start OR token <= end`, which with `start == end` is every
        token: the full ring, expressed the way a Cassandra wraparound range
        expresses it;
      * anything else narrows -- INCLUDING an explicit `(i64::MIN, i64::MAX]`,
        which is half-open and therefore drops the token equal to `i64::MIN`.
        That token is real (#3633), so this is not the full ring.

    And `wraparound` is NOT consulted: ticket.rs:244-255 records it as retained
    for wire compatibility and ignored since #3634, wrapping being derived from
    the endpoints wherever it is needed. So refusing a ticket for setting it
    would reject a template the server accepts -- a validator stricter than its
    consumer, which is the recurring defect this whole family is about.
    """
    start, end = ticket.get("token_start"), ticket.get("token_end")
    if start is None and end is None:
        return True
    return start == end and start is not None


I64_MIN, I64_MAX = -(2 ** 63), 2 ** 63 - 1
U64_MAX = 2 ** 64 - 1
# MIRRORS `PredicateOp` (cqlite-flight/src/ticket.rs:50-65). No `rename_all`, so
# serde accepts the Rust variant names verbatim.
PREDICATE_OPS = ("Equal", "In", "Gt", "Gte", "Lt", "Lte", "Prefix")


def _is_int(value):
    """JSON `true` is a Python bool, and a bool is an int -- serde is not so
    forgiving, and neither is this."""
    return isinstance(value, int) and not isinstance(value, bool)


def _check_ticket_predicates(value):
    """MIRRORS `Vec<Predicate>` (ticket.rs:70-77)."""
    if not isinstance(value, list):
        return "is %s, but FlightTicket declares Vec<Predicate>, which " \
               "deserialises only from a JSON array" % type(value).__name__
    for index, entry in enumerate(value):
        if not isinstance(entry, dict):
            return "entry %d is not an object" % index
        for key in ("column", "op", "value"):
            if key not in entry:
                return "entry %d is missing %r (Predicate declares it with no " \
                       "serde default)" % (index, key)
        if not isinstance(entry["column"], str):
            return "entry %d has a non-string column" % index
        if entry["op"] not in PREDICATE_OPS:
            return "entry %d has op %r, which is not one of %s" % (
                index, entry["op"], "|".join(PREDICATE_OPS))
    return None


# MIRRORS `AggFunc` (ticket.rs:162-178) and the `#[serde(tag = "type")]`
# variants of `PredicateExpr` (ticket.rs:107-147). Internally tagged, so the JSON
# is `{"type": "And", "exprs": [...]}`; no field in any variant carries a serde
# default, so every one listed here is REQUIRED.
AGG_FUNCS = ("Count", "Sum", "SumDouble", "Min", "Max")
PREDICATE_EXPR_VARIANTS = {
    "And": (("exprs", "exprs"),),
    "Or": (("exprs", "exprs"),),
    "Not": (("expr", "expr"),),
    "Compare": (("column", "str"), ("op", "op"), ("value", "any")),
    "In": (("column", "str"), ("values", "list")),
    "IsNull": (("column", "str"),),
}
# A bound, so a deeply nested tree is a NAMED refusal rather than a
# RecursionError -- an unanchored traceback is the failure mode this harness
# exists to not have. Far above anything a connector emits.
_MAX_PREDICATE_DEPTH = 64


def _check_predicate_expr(value, depth=0, where="filter"):
    """MIRRORS `PredicateExpr`, recursively. Returns a problem string or None.

    Round 13 fixed `predicates: {}` and stopped; `filter: {}` is the same defect
    one field over, and was still checked only for being an object -- so it
    passed pre-flight and failed deserialisation after all builds. The fix that
    does not generalise to its siblings is half a fix.
    """
    if depth > _MAX_PREDICATE_DEPTH:
        return "%s nests deeper than %d levels" % (where, _MAX_PREDICATE_DEPTH)
    if not isinstance(value, dict):
        return "%s is %s, but PredicateExpr deserialises only from an object" % (
            where, type(value).__name__)
    tag = value.get("type")
    if tag is None:
        return ("%s has no \"type\" tag; PredicateExpr is "
                "#[serde(tag = \"type\")], so the tag is required" % where)
    if tag not in PREDICATE_EXPR_VARIANTS:
        return "%s has type %r, which is not one of %s" % (
            where, tag, "|".join(sorted(PREDICATE_EXPR_VARIANTS)))
    for field, kind in PREDICATE_EXPR_VARIANTS[tag]:
        if field not in value:
            return "%s (%s) is missing %r, which carries no serde default" % (
                where, tag, field)
        inner = value[field]
        if kind == "str" and not isinstance(inner, str):
            return "%s (%s) has a non-string %s" % (where, tag, field)
        if kind == "list" and not isinstance(inner, list):
            return "%s (%s) has a non-array %s" % (where, tag, field)
        if kind == "op" and inner not in PREDICATE_OPS:
            # Compare's doc says "never In", but that is a SEMANTIC note, not a
            # deserialisation rule -- serde accepts any PredicateOp here, so
            # refusing `In` would be the too-strict half all over again.
            return "%s (%s) has op %r, which is not one of %s" % (
                where, tag, inner, "|".join(PREDICATE_OPS))
        if kind == "expr":
            problem = _check_predicate_expr(inner, depth + 1, "%s.%s" % (where, field))
            if problem:
                return problem
        if kind == "exprs":
            if not isinstance(inner, list):
                return "%s (%s) has a non-array exprs" % (where, tag)
            for index, child in enumerate(inner):
                problem = _check_predicate_expr(
                    child, depth + 1, "%s.exprs[%d]" % (where, index))
                if problem:
                    return problem
    return None


def _check_aggregation(value):
    """MIRRORS `Aggregation` / `AggregateSpec` / `AggFunc` (ticket.rs:189-217)."""
    if value is None:
        return None
    if not isinstance(value, dict):
        return "is %s, but Aggregation deserialises only from an object" % (
            type(value).__name__,)
    if "aggregates" not in value:
        return "is missing 'aggregates', which carries no serde default"
    if not isinstance(value["aggregates"], list):
        return "has a non-array 'aggregates'"
    group_by = value.get("group_by", [])
    if not isinstance(group_by, list) or not all(
            isinstance(g, str) for g in group_by):
        return "has a 'group_by' that is not an array of strings"
    for index, spec in enumerate(value["aggregates"]):
        if not isinstance(spec, dict):
            return "aggregates[%d] is not an object" % index
        for field in ("func", "output"):
            if field not in spec:
                return "aggregates[%d] is missing %r, which carries no serde " \
                       "default" % (index, field)
        if spec["func"] not in AGG_FUNCS:
            return "aggregates[%d] has func %r, which is not one of %s" % (
                index, spec["func"], "|".join(AGG_FUNCS))
        if not isinstance(spec["output"], str):
            return "aggregates[%d] has a non-string 'output'" % index
        column = spec.get("column")
        if column is not None and not isinstance(column, str):
            return "aggregates[%d] has a 'column' that is neither a string nor " \
                   "null" % index
    return None


def _predicate_expr_or_null(value):
    if value is None:
        return None
    return _check_predicate_expr(value)


def _optional(kind, what):
    def check(value):
        if value is None:
            return None
        if kind is int:
            if not _is_int(value):
                return "is %r, expected %s or null" % (value, what)
            return None
        if not isinstance(value, kind):
            return "is %r, expected %s or null" % (value, what)
        return None
    return check


def _bounded_int(low, high, what):
    def check(value):
        if value is None:
            return None
        if not _is_int(value):
            return "is %r, expected %s or null" % (value, what)
        if not low <= value <= high:
            return "is %d, outside the %s range" % (value, what)
        return None
    return check


def _string_list(value):
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        return "is %r, expected an array of strings or null" % (value,)
    return None


# THE COMPLETE CONSUMER SCHEMA, field for field from `FlightTicket`
# (cqlite-flight/src/ticket.rs:225-290). `required` means the field carries NO
# `#[serde(default)]`, so deserialisation fails without it -- and that is EXACTLY
# three fields. `version` is NOT one of them: it defaults via
# `default_ticket_version`, and demanding it here rejected a ticket the consumer
# accepts, which is the guard-an-operator-learns-to-waive shape on a metered rig.
TICKET_SCHEMA = {
    "version": (False, _bounded_int(0, 255, "u8")),
    "keyspace": (True, lambda v: None if isinstance(v, str) else
                 "is %r, expected a string" % (v,)),
    "table": (True, lambda v: None if isinstance(v, str) else
              "is %r, expected a string" % (v,)),
    "ddl": (True, lambda v: None if isinstance(v, str) else
            "is %r, expected a string" % (v,)),
    "snapshot": (False, _optional(str, "a string")),
    "token_start": (False, _bounded_int(I64_MIN, I64_MAX, "i64")),
    "token_end": (False, _bounded_int(I64_MIN, I64_MAX, "i64")),
    "wraparound": (False, lambda v: None if isinstance(v, bool) else
                   "is %r, expected a boolean" % (v,)),
    "columns": (False, _string_list),
    "predicates": (False, _check_ticket_predicates),
    "filter": (False, _predicate_expr_or_null),
    "aggregation": (False, _check_aggregation),
    "limit": (False, _bounded_int(0, U64_MAX, "u64")),
}


def validate_ticket_schema(path, ticket):
    """Would `serde_json` deserialise this into a `FlightTicket`?

    EIGHTH validator-versus-consumer instance, and the first wrong in BOTH
    directions at once -- which is what happens when a validator is written from
    a reading of the field list rather than from the deserialiser's behaviour.
    Too strict: it demanded `version`, which has a serde default. Too loose:
    `"predicates": {}` sailed through pre-flight and failed after all three
    release builds, because only four fields were looked at.

    Note `FlightTicket` is NOT `deny_unknown_fields`, so an unknown key is
    IGNORED by the consumer and must be ignored here: rejecting it would refuse
    a ticket from a newer connector that the server reads fine.
    """
    problems = []
    for field, (required, check) in sorted(TICKET_SCHEMA.items()):
        if field not in ticket:
            if required:
                problems.append(
                    "required field %r is missing; FlightTicket declares it with "
                    "no serde default, so the load generator would fail to "
                    "deserialise this ticket after every build had completed"
                    % field
                )
            continue
        if required and ticket[field] is None:
            problems.append("required field %r is null, and it is not an Option"
                            % field)
            continue
        detail = check(ticket[field])
        if detail:
            problems.append("field %r %s" % (field, detail))
    if isinstance(ticket.get("ddl"), str) and not ticket["ddl"].strip():
        problems.append("ddl is empty; it is parsed into the TableSchema that "
                        "drives the merge")
    return problems


def validate_ticket(path, full_ring=True):
    """Refuse a ticket the consumer would reject, and -- for a measurement --
    one that is not a full-ring scan of every column.

    The two halves are SEPARATE because they answer to different authorities.
    The schema half mirrors the deserialiser and applies to EVERY session,
    controls included: a control that cannot be deserialised wastes exactly the
    same three builds. The full-ring half is the #3649 target band's own
    restriction and applies only to measurements.
    """
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
    schema_problems = validate_ticket_schema(path, ticket)
    if schema_problems:
        err("cause ticket-schema-invalid")
        for problem in schema_problems:
            err("cause-detail %s: %s" % (path, problem))
        return 1
    if not full_ring:
        return 0

    problems = []
    # MIRRORS `pathsafe::validate_snapshot` (cqlite-flight/src/pathsafe.rs:60-73):
    # present-but-EMPTY is rejected there, at ticket parse time. The census used
    # to be the only place this was checked, and it accepted an empty name and
    # then resolved the LIVE directory -- so the session censused one directory,
    # built both arms, and only then had every request refused by the server's
    # own ticket validation. Checked HERE, at pre-flight, because that is before
    # the expensive step. `null` remains legal: it means the live directory.
    snapshot = ticket.get("snapshot")
    if snapshot is not None and (
        not isinstance(snapshot, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", snapshot)
    ):
        err("cause ticket-identifier-invalid")
        err(
            "cause-detail %s: snapshot is %r; cqlite-flight rejects an empty or "
            "non-conforming snapshot name at ticket parse time, so this would fail "
            "every request AFTER both arms had been built" % (path, snapshot)
        )
        return 1
    for field, why in _TICKET_NARROWING:
        if ticket.get(field) is not None:
            problems.append("%s (%s = %r)" % (why, field, ticket[field]))
    predicates = ticket.get("predicates")
    if isinstance(predicates, list) and predicates:
        problems.append("carries %d predicate(s)" % len(predicates))
    if not _token_range_is_full_ring(ticket):
        problems.append(
            "narrows the token range (token_start=%r token_end=%r)"
            % (ticket.get("token_start"), ticket.get("token_end"))
        )
    # `wraparound` is deliberately NOT checked -- see `_token_range_is_full_ring`.
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
    # MIRRORS `pathsafe::validate_snapshot` (cqlite-flight/src/pathsafe.rs:60-73):
    # empty is REJECTED there, so `*` here accepted a ticket the server refuses --
    # and it failed after both builds, the same rig economics as the relative
    # work directory. Sixth instance of validator-versus-consumer.
    if snapshot is not None and (
        not isinstance(snapshot, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", snapshot)
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
    uncompressed = []
    # CONTAINMENT, not just scope. `DirSource::data_paths` excludes any entry
    # whose CANONICAL target escapes the resolved directory
    # (cqlite-flight/src/producer.rs:215-235, `pathsafe::assert_within`), because
    # a symlink inside an otherwise-valid directory can point anywhere. Mirroring
    # the enumeration without the containment check left symlinked decoys
    # satisfying both floors -- the round-4 fix closed the recursive-scan route
    # and this one stayed open. A Cassandra snapshot is a HARD link, which
    # canonicalises inside the directory and is therefore still counted.
    # THE SAME ENUMERATION THE PROBE USES. Two components deciding separately
    # which files the server serves is how their views come apart -- which is
    # exactly what happened: this one applied containment and probe_compression
    # did not.
    data_files, enum_error = contained_data_files(served)
    if enum_error is not None:
        err("cause served-dir-absent")
        err("cause-detail %s" % enum_error)
        return 1
    try:
        for real in data_files:
            total += os.path.getsize(real)
            count += 1
            # THE COMPRESSED-CORPUS REQUIREMENT, ASKED RATHER THAN ASSUMED.
            # `FINDINGS.md` records it as a requirement -- the field is LZ4 and
            # the plan of record flags UNCOMPRESSED as a known artifact -- and
            # nothing checked it, so an uncompressed corpus cleared every floor.
            # Worse than a gap: removing LZ4 decode removes real CPU from the
            # denominator, so an uncompressed corpus BIASES THE RATIO TOWARD THE
            # TARGET. The failure was in the favourable direction, which is the
            # hardest kind to notice. A zero-length CompressionInfo.db counts as
            # absent: this repository records that an empty one makes SELECT
            # return 0 rows silently.
            info = real[: -len("-Data.db")] + "-CompressionInfo.db"
            if not os.path.isfile(info) or os.path.getsize(info) == 0:
                uncompressed.append(os.path.basename(real))
    except OSError as exc:
        err("cause corpus-census-failed")
        err("cause-detail %s: %s" % (served, exc))
        return 1
    # `<files> <bytes> <compressed> <dir>` -- the compression verdict travels
    # with the census so the manifest records it and the analyzer can re-check.
    compressed = "compressed" if count and not uncompressed else "UNCOMPRESSED"
    if uncompressed:
        err("note %d of %d served SSTables have no usable CompressionInfo.db: %s"
            % (len(uncompressed), count, ", ".join(sorted(uncompressed)[:4])))
    sys.stdout.write("%d %d %s %s\n" % (count, total, compressed, served))
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
            text = _ansi_stripped(handle.read())
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


#: EVERY INPUT THE SESSION RESOLVER TAKES. The driver must pass all of them, and
#: a structural case in selftest-analyze.sh asserts the call site does.
#:
#: THIS LIST IS THE STRUCTURAL MOVE. Three separate findings have been "a guard
#: exists on one entry point and a later path routes around it": the batch-size
#: floor via per-arm extras, the corpus floors via a lowered threshold, and a
#: per-arm `--max-concurrent-scans` against a globally-validated value. Guarding
#: each resolved value one at a time is the same trap as reconciling record
#: fields one at a time -- so there is now ONE resolver, it is the only producer
#: of session configuration, and the driver reads nothing else. A new option
#: cannot route around a guard because nothing else produces the values.
RESOLVER_INPUTS = (
    "--batch-size", "--max-batch-bytes", "--admission-wait-timeout-ms",
    "--max-concurrent-scans", "--min-corpus-bytes", "--min-sstables",
    "--ramp", "--control", "--base-server-extra", "--head-server-extra",
)

#: A DISPOSITION FOR EVERY OPTION THE DRIVER ACCEPTS.
#:
#: `RESOLVER_INPUTS` alone is a curated list, and a curated list is a second
#: place to forget an option -- which is the exact failure the resolver exists to
#: remove, reintroduced inside its own guard. What makes it a completeness
#: property is that the SUBJECT SET is derived: `selftest-analyze.sh` reads every
#: `--option)` arm out of the driver's own dispatch and requires each one to
#: appear here. So adding an option to the driver without deciding whether it
#: reaches the resolver reds, and the decision has to be written down with a
#: reason rather than implied by absence.
#:
#: Same standard as the record/workload disposition tables: the list is curated,
#: the COMPLETENESS is checked against the real thing.
OPTION_DISPOSITION = {
    "--batch-size": ("resolver-input", "server configuration"),
    "--max-batch-bytes": ("resolver-input", "server configuration"),
    "--admission-wait-timeout-ms": ("resolver-input", "server configuration"),
    "--max-concurrent-scans": ("resolver-input", "server configuration; bounds the ramp"),
    "--min-corpus-bytes": ("resolver-input", "a documented floor, not lowerable for a measurement"),
    "--min-sstables": ("resolver-input", "a documented floor; #3058's bypass depends on it"),
    "--ramp": ("resolver-input", "bounded against the admission ceiling"),
    "--control": ("resolver-input", "decides whether the floors may be lowered"),
    "--profile": (
        "not-server-config",
        "which target band this workload is measured against; recorded in the "
        "manifest and read from there by the analyzer. It configures no server "
        "and no workload -- it names which band the RESULT is compared to",
    ),
    "--attest-local-storage": (
        "not-server-config",
        "an operator attestation about the corpus device, recorded in the "
        "manifest and printed beside the verdict; it changes no server or "
        "workload configuration",
    ),
    "--base-server-extra": ("resolver-input", "per-arm overrides, resolved here"),
    "--head-server-extra": ("resolver-input", "per-arm overrides, resolved here"),
    "--corpus": ("not-server-config", "a path the server is pointed at, not a setting"),
    "--ticket-template": ("not-server-config", "a client-side workload description"),
    "--loadgen-ref": ("not-server-config", "which commit the ONE shared client is built from"),
    "--base-ref": ("not-server-config", "which commit to build"),
    "--head-ref": ("not-server-config", "which commit to build"),
    "--replicates": ("not-server-config", "how many times the session repeats"),
    "--work-dir": ("not-server-config", "where results are written"),
    "--repo": ("not-server-config", "where the arms are built from"),
    "--shape": ("not-server-config", "a load-generator flag; validated against the ticket"),
    "--step-duration": ("not-server-config", "a load-generator flag"),
    "--port": ("not-server-config", "the listen address, ephemeral by default"),
    "--server-cpus": ("not-server-config", "an external pin, verified against /proc"),
    "--client-cpus": ("not-server-config", "pins the load generator, not the server"),
    "--merge-path": ("not-server-config", "an environment variable, pinned separately"),
    "--rows-declared": ("not-server-config", "recorded, never measured"),
    "--temperature": ("not-server-config", "page-cache state"),
    "--no-prewarm": ("not-server-config", "whether a warming pass runs"),
}

#: The per-arm overridable options, and the ONLY ones an extras string may name.
#: Each is emitted exactly once in the constructed argv, resolved from the global
#: value and this arm's override -- NOT appended after it. The project's Clap
#: command does not enable self-overrides, so a duplicated option is an argument
#: PARSE FAILURE, not a last-wins resolution: `--batch-size 8192 --batch-size 1`
#: is rejected by the real binary. A stub cannot catch that, because the
#: permissiveness is in the parser rather than in a format -- so the argv is
#: constructed to make the duplicate unexpressible instead.
#: `--max-concurrent-scans` is deliberately NOT here. It was declared overridable
#: and validated globally, so any effective override failed at run time -- and
#: the honest fix is to reject it rather than thread a per-arm ceiling through,
#: because two arms admitted at different concurrencies shed at different ramp
#: steps, which leaves them with different surviving ladders. The analyzer
#: already refuses that as `ramp-steps-not-comparable`, so a per-arm admission
#: ceiling cannot produce a comparable measurement in the first place. The ramp
#: bound is also a session-level property, computed against one ceiling.
OVERRIDABLE = ("--batch-size", "--max-batch-bytes", "--admission-wait-timeout-ms")

NOT_REQUESTED = "NOT-REQUESTED"


def unresolvable_reason(option):
    """The ONE explanation of why a per-arm option is not resolvable.

    Shared by both refusal sites so the reason cannot drift between them -- and
    `--max-concurrent-scans` gets its own clause, because "not recognised" would
    invite the next person to fix it by adding the plumbing.
    """
    reason = (
        "%r is not an option this driver can resolve per arm. Recognised: %s. An "
        "unrecognised option cannot be merged with the global flags, so it could "
        "only be APPENDED -- and an option appearing twice is a Clap parse "
        "failure, not a last-wins override" % (option, " ".join(OVERRIDABLE))
    )
    if option == "--max-concurrent-scans":
        reason += (
            ". This one is excluded DELIBERATELY, not by omission: two arms "
            "admitted at different concurrencies shed at different ramp steps, so "
            "their surviving ladders differ and the analyzer refuses them as "
            "ramp-steps-not-comparable. Threading a per-arm ceiling through the "
            "observation check, the ramp bound and the manifest would only buy an "
            "UNMEASURED verdict"
        )
    return reason


def parse_uint_flag(name, value, minimum, rust_type, below_min_note=""):
    """ONE parser for every resolved integer: range-check AND canonical form.

    Returns `(canonical_decimal, problem)`, exactly one of which is None.

    TWO FAILURES THIS CLOSES, and they point opposite ways. `[0-9]+` accepted
    `08`, which Clap parses happily as 8 -- so the server's startup line echoed
    `8`, the read-back compared it against the string `08`, and the session died
    on a MISMATCH THAT WAS NOT ONE. A false red on correct input, after the
    builds. And an oversized value (`99999999999999999999`) passed every check
    here and was rejected by Clap only when the server was launched, which is
    after all three release builds on a metered box.

    So the canonical decimal produced here is what goes into the argv, the
    manifest and the comparison -- one representation, produced once, rather
    than a raw string that is equal to itself and unequal to what ran.
    """
    if not re.fullmatch(r"[0-9]+", value):
        return None, ("the resolved %s is %r, which is not a non-negative "
                      "integer" % (name, value))
    parsed = int(value)
    if parsed > U64_MAX:
        return None, ("the resolved %s is %s, which exceeds %s -- cqlite-flight "
                      "parses it into a Rust %s, so Clap would refuse it when the "
                      "server is launched, after every build had completed"
                      % (name, value, U64_MAX, rust_type))
    if parsed < minimum:
        return None, ("the resolved %s is %s, below the minimum %d%s"
                      % (name, value, minimum, below_min_note))
    return str(parsed), None


# The Rust types the server parses these into (cqlite-flight/src/cli.rs:57-93).
# `usize` is 64-bit on every host this runs on, so all four share u64's ceiling.
# The note belongs ONLY to the below-minimum branch: appending it to an overflow
# message produced "exceeds u64 max; cqlite-flight clamps 0 to one row per
# batch", which is two unrelated explanations of one number.
_BATCH_ZERO_NOTE = (
    "; cqlite-flight clamps 0 to one row per batch, so the manifest would not "
    "record the value that ran -- and the Arrow batch row cap is the mechanism "
    "#2820 changed"
)
RESOLVED_UINTS = (
    ("--batch-size", "batch_size_observed", 1, "usize", _BATCH_ZERO_NOTE),
    ("--max-batch-bytes", "max_batch_bytes_observed", 0, "usize", ""),
    ("--admission-wait-timeout-ms", "wait_timeout_ms_observed", 0, "u64", ""),
    ("--max-concurrent-scans", "max_concurrent_scans", 1, "usize", ""),
)


def validate_resolved(batch, maxbytes, wait, scans):
    """Range-check the values a server will ACTUALLY be given.

    ON THE RESOLVED VALUE, not on the flag. The batch-size floor was added in
    round 6 on `--batch-size` and per-arm extras were a second route to the same
    value -- symmetric `--base-server-extra '--batch-size 0'` and
    `--head-server-extra '--batch-size 0'` need no control label, the server
    clamps both to one row per batch, and the analyzer renders a measurement
    verdict for a configuration nothing recorded. That is the fifth time a guard
    on one entry point has been bypassable through a route added later, so the
    check lives where the value is RESOLVED: every present and future caller
    inherits it because there is only one place the resolved value exists.

    Returns a list of problems, empty when the configuration is usable.
    """
    problems = []
    supplied = {
        "batch_size_observed": batch,
        "max_batch_bytes_observed": maxbytes,
        "wait_timeout_ms_observed": wait,
        "max_concurrent_scans": scans,
    }
    for name, key, minimum, rust_type, note in RESOLVED_UINTS:
        value = supplied[key]
        if value == NOT_REQUESTED:
            # Not passed to the server at all, so there is no value to range-check.
            continue
        _, problem = parse_uint_flag(name, value, minimum, rust_type, note)
        if problem:
            problems.append(problem)
    return problems


def server_argv(binary, data_dir, listen, batch, maxbytes, wait, scans, extra):
    """The server command line, with every option emitted exactly once.

    `batch`/`maxbytes`/`wait`/`scans` are the ARM'S ALREADY-RESOLVED values (the
    caller applies `effective-flag` first). `extra` is passed only so its option
    names can be VALIDATED as recognised -- it is never merged here, because
    merging twice is how a duplicate reappears.
    """
    words = extra.split()
    index = 0
    while index < len(words):
        if words[index] not in OVERRIDABLE:
            err("cause server-extra-unrecognised")
            err("cause-detail %s" % unresolvable_reason(words[index]))
            return None
        if index + 1 >= len(words):
            err("cause server-extra-unrecognised")
            err("cause-detail %r has no value" % words[index])
            return None
        index += 2

    problems = validate_resolved(batch, maxbytes, wait, scans)
    if problems:
        err("cause resolved-config-invalid")
        for problem in problems:
            err("cause-detail %s" % problem)
        return None

    argv = [binary, "--data-dir", data_dir, "--listen", listen,
            "--batch-size", batch, "--max-concurrent-scans", scans]
    if maxbytes != NOT_REQUESTED:
        argv += ["--max-batch-bytes", maxbytes]
    if wait != NOT_REQUESTED:
        argv += ["--admission-wait-timeout-ms", wait]
    return argv


def resolve_session(batch, maxbytes, wait, scans, min_bytes, min_sstables,
                    ramp, control, base_extra, head_extra):
    """THE ONLY PRODUCER OF SESSION CONFIGURATION.

    Takes every raw input, applies every rule, and returns the complete resolved
    configuration -- or a list of problems. The driver reads nothing else, so a
    guard here cannot be routed around by a path added later; there is no other
    path that produces these values.
    """
    problems = []
    steps = parse_ramp(ramp)
    if steps is None or ramp_section(steps) is None:
        problems.append("--ramp %r is not a usable ramp" % ramp)
        steps = [1]

    resolved = {}
    for arm, extra in (("base", base_extra), ("head", head_extra)):
        words = extra.split()
        index = 0
        while index < len(words):
            if words[index] not in OVERRIDABLE:
                problems.append("%s: %s" % (arm, unresolvable_reason(words[index])))
                break
            if index + 1 >= len(words):
                problems.append("%s: %r has no value" % (arm, words[index]))
                break
            index += 2
        resolved[arm] = {
            "batch_size_observed": effective_flag("--batch-size", batch, extra),
            "max_batch_bytes_observed": effective_flag(
                "--max-batch-bytes", maxbytes, extra),
            "wait_timeout_ms_observed": effective_flag(
                "--admission-wait-timeout-ms", wait, extra),
            "max_concurrent_scans": scans,
        }
        # CANONICALISED HERE, at the one place a resolved value comes into
        # existence, so the argv, the manifest and the startup read-back all
        # carry the SAME representation. `08` and `8` are the same number and
        # different strings, and the read-back compares strings.
        for name, key, minimum, rust_type, note in RESOLVED_UINTS:
            value = resolved[arm][key]
            if value == NOT_REQUESTED:
                continue
            canonical, _ = parse_uint_flag(name, value, minimum, rust_type, note)
            if canonical is not None:
                resolved[arm][key] = canonical
        problems += [
            "%s: %s" % (arm, problem)
            for problem in validate_resolved(
                resolved[arm]["batch_size_observed"],
                resolved[arm]["max_batch_bytes_observed"],
                resolved[arm]["wait_timeout_ms_observed"],
                scans,
            )
        ]

    # THE FLOORS ARE FLOORS. Lowerable only under a control label, where the
    # verdict is disclaimed -- otherwise a measurement could authorise its own
    # validity by choosing a smaller number.
    if not control:
        if not re.fullmatch(r"[0-9]+", min_bytes) or int(min_bytes) < MIN_CORPUS_BYTES_FLOOR:
            problems.append(
                "--min-corpus-bytes %r is below the documented floor of %d; a "
                "measurement may not lower it. Run it as a --control if you mean to"
                % (min_bytes, MIN_CORPUS_BYTES_FLOOR)
            )
        if not re.fullmatch(r"[0-9]+", min_sstables) or int(min_sstables) < MIN_SSTABLES_FLOOR:
            problems.append(
                "--min-sstables %r is below the documented floor of %d; below it a "
                "single-source served table takes #3058's fast path on BOTH arms "
                "and the ratio is 1.0 by construction"
                % (min_sstables, MIN_SSTABLES_FLOOR)
            )

    if re.fullmatch(r"[0-9]+", scans) and steps and steps[-1] > int(scans):
        problems.append(
            "--ramp tops out at %d but --max-concurrent-scans is %s; every request "
            "past the ceiling is shed (#2420)" % (steps[-1], scans)
        )
    return resolved, problems


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


def validate_replicate(path, tag, raw_ramp, shape, step_duration_s):
    """Validate a replicate's records BY CALLING THE ANALYZER'S VALIDATOR.

    ONE VALIDATOR FOR ONE RECORD SCHEMA. This function used to check a handful
    of fields by hand -- round, target_concurrency, requests_error, requests_ok,
    rows_per_s -- and nothing else, so it accepted records the analyzer would
    later refuse (wrong `schema`, wrong `shape`, a `duration_s` from another
    session, `rows_per_s x duration_s` disagreeing with `rows_total`), and a
    malformed `latency_ms` reached `.get` on a non-dict and produced an
    unanchored traceback in the printing code below.

    A SECOND VALIDATOR WOULD DRIFT FROM THE FIRST WITHIN TWO ROUNDS, and the
    drift presents exactly as the symptom being fixed: the driver accepting what
    the analyzer rejects, discovered after the rig is gone. So the analyzer's
    typed validation is CALLED, not reimplemented -- the same move as one
    duration grammar, one resolver and one canonical findings section.

    What stays here is what the analyzer does NOT do at record level: the round
    tag (the analyzer reconciles it a layer up), the error count, and the
    single-stream shed refusal -- which exists precisely because the driver runs
    while the rig is still up and the pin can still be corrected.
    """
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
        # THE ANALYZER'S OWN VALIDATION, called. Its cause is carried into the
        # detail so an operator sees the exact refusal the analysis would give,
        # while the driver keeps its own cause vocabulary.
        try:
            validate_record_shape(record, path, position + 1, shape)
            validate_record_usable(
                record, path, position + 1, expected_concurrency, step_duration_s)
        except Unmeasured as exc:
            err("cause replicate-invalid")
            err("cause-detail %s record %d: %s (the analyzer refuses this with "
                "cause %s)" % (path, position + 1, exc.detail, exc.cause))
            return 1

        problems = []
        if record.get("round") != tag:
            problems.append(
                "round is %r, expected %r" % (record.get("round"), tag)
            )
        if record.get("requests_error", 0):
            problems.append("requests_error=%s" % record["requests_error"])
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


#: CSI / SGR escape sequences, as `tracing_subscriber::fmt` emits them.
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _ansi_stripped(text):
    """Colour is stripped AT THE PARSE SITE, which is the only place it can be.

    `tracing-subscriber`'s fmt layer has ANSI ON by default (`ansi` is a default
    feature and cqlite-flight does not disable it), and this repository already
    records the consequence from #3400: **colour SURVIVES redirection to a file**,
    so a log captured with `> file 2>&1` is coloured too. Worse, tracing styles
    the FIELD NAME and the `=`, so a pattern spanning `name=value` matches
    NOTHING -- which is the exact shape #3400 describes one subsystem over, where
    a pattern anchored on a status word survived and one spanning
    `<status> <payload>` did not.

    Left unfixed this was total: every `STARTUP_FIELDS` regex failing means every
    field NOT-OBSERVED, corroboration `none`, and -- after round 10's ruling --
    every real rig session refused as UNMEASURED.

    The driver ALSO sets `NO_COLOR=1` in the server's environment, which
    tracing-subscriber 0.3.23 honours in `Layer::default()` (verified in the
    locked source). That is a real control rather than a hopeful one, but it is
    still the belt: it depends on the crate version and on the server continuing
    to build its layer from `default()`, and neither is this harness's to
    guarantee. Stripping here works whatever the producer does.
    """
    return _ANSI.sub("", text)


def probe_storage(path):
    """Is this path on LOCAL storage, or on the network?

    CHECK THE PROPERTY THE LABEL STOOD FOR. The acceptance criteria name the
    field i4i rig, but they do not care about the string `i4i` -- they care about
    what it stood for, and the load-bearing part is **local NVMe rather than
    network storage**. That is what disqualified this lane's host: `lsblk` reports
    *Amazon Elastic Block Store* for its devices. A hostname pattern would red a
    correct rig the day someone uses `i4i.2xlarge`; the device model does not.

    FOUR-VALUED, and only ONE of them is a pass. `NOT-MEASURABLE` (no model to
    read) and `UNRECOGNISED` (a model that names neither service) are different
    facts from `LOCAL`, and neither is evidence of a local disk.

    WHAT THIS DEPENDS ON, because the next person will otherwise re-derive it:
    the NVMe **vendor model string**, and nothing else discriminates. Measured on
    an EBS-backed lane box -- `queue/rotational` is `0` and the filesystem type is
    `ext4`, which is exactly what instance storage reports too, and IMDS returns
    nothing. On Nitro, EBS is *presented as an NVMe device on purpose*, so there
    is no portable, non-vendor signal separating network-attached block storage
    from instance storage: the two are indistinguishable by design at every layer
    below the device identity.

    So this is measurable ON AWS and not portably. It is worth more than the
    hostname check it replaces for one reason: the string is reported BY THE
    DEVICE and names the storage service directly, rather than describing the
    machine class and leaving the storage to be inferred -- and both the pass and
    the fail are affirmative matches, so a device this does not recognise is
    reported as unrecognised rather than waved through.
    """
    try:
        source = subprocess.run(
            ["df", "--output=source", path],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return "NOT-MEASURABLE", "-", "df failed: %s" % exc
    lines = [l.strip() for l in source.stdout.splitlines() if l.strip()]
    if source.returncode != 0 or len(lines) < 2:
        return "NOT-MEASURABLE", "-", "df did not name a device"
    device = os.path.basename(lines[-1])
    # `nvme0n1p3` -> `nvme0n1`, `sda1` -> `sda`; a mapper/overlay name simply
    # will not have a model file, which lands in NOT-MEASURABLE.
    base = re.sub(r"(p?\d+)$", "", device) if not os.path.exists(
        "/sys/block/%s" % device) else device
    model_path = "/sys/block/%s/device/model" % base
    try:
        with open(model_path, encoding="utf-8", errors="replace") as handle:
            model = handle.read().strip()
    except OSError:
        return "NOT-MEASURABLE", base, "no device model at %s" % model_path
    return classify_storage_model(model), base, (model or "the device model is empty")


# MIRRORS the `CompressionInfo.db` header as the definitive guide records it
# (docs/sstables-definitive-guide/chapters/appendix-g-compression-chunk-formats.md
# lines 34-70), whose authority is `CompressionMetadata.java` at cassandra-5.0.8
# (`open()` 76-112, `writeHeader()` 375-398). Read from the guide and the pinned
# source, NEVER from CQLite's own reader: a CQLite file:line is evidence of what
# CQLite does, never of what is correct.
#
#   [algorithm: writeUTF -- 2-byte BE length + UTF-8 bytes]
#   [option count: u32 BE][key,value writeUTF pairs...]
#   [chunk length: u32 BE][max compressed length: u32 BE]
#   [data length: u64 BE][chunk count: u32 BE][chunk offsets: u64 BE * count]
#
# The field ORDER is the parse; there is no padding after the name.
COMPRESSOR_NAMES = {
    "LZ4Compressor": "LZ4",
    "SnappyCompressor": "SNAPPY",
    "DeflateCompressor": "DEFLATE",
    "ZstdCompressor": "ZSTD",
    "NoopCompressor": "NOOP",
}
#: The field is LZ4 (docs/research/throughput-program-2026-07.md line 21), so the
#: target band is defined for LZ4 decode work and nothing else.
REQUIRED_COMPRESSOR = "LZ4"


def parse_compression_info(path):
    """-> (state, detail). FOUR-valued, and only the first is acceptable.

      LZ4            the header parses and names LZ4Compressor
      OTHER          it parses and names a compressor we KNOW is not LZ4
      UNRECOGNISED   it parses and names something we do not know
      UNPARSEABLE    it does not parse, or the header is not self-consistent

    OTHER and UNRECOGNISED are deliberately distinct even though both refuse. A
    known non-LZ4 name means the corpus was generated with the wrong compressor
    -- the operator regenerates it. An unknown name means a newer Cassandra or a
    file that is well-formed but foreign, and the honest report is the name
    itself. UNPARSEABLE is a third operator action again: the file is damaged.
    Collapsing them would hand one remedy to three different problems.
    """
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as exc:
        return "UNPARSEABLE", "unreadable: %s" % (exc.strerror or exc)
    if len(raw) < 26:
        return "UNPARSEABLE", "%d bytes, shorter than the smallest legal header" % len(raw)

    def be(offset, width):
        return int.from_bytes(raw[offset:offset + width], "big")

    name_len = be(0, 2)
    if name_len == 0 or 2 + name_len > len(raw):
        return "UNPARSEABLE", "the algorithm-name length (%d) does not fit the file" % name_len
    try:
        name = raw[2:2 + name_len].decode("utf-8")
    except UnicodeDecodeError:
        return "UNPARSEABLE", "the algorithm name is not valid UTF-8"
    # A class simple name. Anything else means we are not reading a header.
    if not re.fullmatch(r"[A-Za-z0-9_$.]{1,255}", name):
        return "UNPARSEABLE", "the algorithm name %r is not a class name" % name

    cursor = 2 + name_len
    if cursor + 4 > len(raw):
        return "UNPARSEABLE", "truncated before the option count"
    option_count = be(cursor, 4)
    cursor += 4
    if option_count > 1000:
        return "UNPARSEABLE", "an option count of %d is not plausible" % option_count
    for _ in range(option_count * 2):  # each option is a key AND a value
        if cursor + 2 > len(raw):
            return "UNPARSEABLE", "truncated inside the compression options"
        item_len = be(cursor, 2)
        cursor += 2 + item_len
        if cursor > len(raw):
            return "UNPARSEABLE", "an option string runs past the end of the file"
    if cursor + 20 > len(raw):
        return "UNPARSEABLE", "truncated before the chunk header"
    chunk_length = be(cursor, 4)
    max_compressed = be(cursor + 4, 4)
    data_length = be(cursor + 8, 8)
    chunk_count = be(cursor + 16, 4)
    cursor += 20
    # SELF-CONSISTENCY, which is what separates "parsed" from "happens to have
    # readable bytes at the front". Zero max-compressed-length is corrupt per the
    # guide's own guard; a chunk map that does not fit is not a chunk map.
    if chunk_length == 0:
        return "UNPARSEABLE", "chunk_length is zero"
    if max_compressed == 0:
        return "UNPARSEABLE", "max_compressed_length is zero, which the format calls corrupt"
    if chunk_count == 0:
        return "UNPARSEABLE", "chunk_count is zero"
    if cursor + 8 * chunk_count > len(raw):
        return "UNPARSEABLE", (
            "the chunk map needs %d bytes and the file has %d left"
            % (8 * chunk_count, len(raw) - cursor))

    known = COMPRESSOR_NAMES.get(name)
    if known == REQUIRED_COMPRESSOR:
        return "LZ4", "%s chunk_length=%d data_length=%d chunks=%d" % (
            name, chunk_length, data_length, chunk_count)
    if known is not None:
        return "OTHER", known
    return "UNRECOGNISED", name


# MIRRORS `Shape::parse` and `Shape::label`
# (tools/flight-loadgen/src/shape.rs:34-55). Read from the parser, not from a
# list in a review comment: the aliases are real AND the match is on
# `to_ascii_lowercase()`, so `FULL` is accepted there and would have been
# refused here -- the too-strict half, which reds a correct session.
SHAPE_ALIASES = {
    "full": "full",
    "limit-k": "limit-k", "limitk": "limit-k", "limit": "limit-k",
    "point": "point", "ptr": "point",
    "mixed": "mixed", "mix": "mixed",
}


def canonical_shape(raw):
    """The record label flight-loadgen will emit for this --shape, or None.

    THE VALUE THE DRIVER CARRIES MUST BE THE ONE THE RECORDS WILL CARRY. The
    driver exported the RAW string and the load generator emitted the CANONICAL
    label, so `--shape limit` produced records saying `limit-k` which the
    manifest reconciliation then rejected as a shape mismatch -- after all three
    release builds. Canonicalising at preflight makes the two the same fact.
    """
    return SHAPE_ALIASES.get(raw.strip().lower())


def contained_data_files(served_dir):
    """The `*-Data.db` the SERVER will read, and no others. -> (paths, error).

    ONE ENUMERATION, because two components holding different views of which
    files are served is how a validator comes to red a corpus the server would
    happily serve. `census_served` applied the containment rule and
    `probe_compression` re-listed the directory without it, so an escaping
    symlink the server EXCLUDES could fail a valid LZ4 corpus as MISSING or as
    the wrong compressor -- a false refusal, which is the fail-closed direction
    and still the guard an operator learns to work around.

    MIRRORS `DirSource::data_paths` (cqlite-flight/src/producer.rs:215-235, via
    `pathsafe::assert_within`): a flat listing of the resolved directory,
    excluding any entry whose CANONICAL target escapes it. A Cassandra snapshot
    is a HARD link, which canonicalises inside the directory and is kept.
    Returns the CANONICAL paths, since that is what the server reads.
    """
    try:
        served_real = os.path.realpath(served_dir)
        names = sorted(os.listdir(served_dir))
    except OSError as exc:
        return None, "%s: %s" % (served_dir, exc.strerror or exc)
    kept = []
    for name in names:
        if not name.endswith("-Data.db"):
            continue
        path = os.path.join(served_dir, name)
        if not os.path.isfile(path):
            continue
        real = os.path.realpath(path)
        if os.path.dirname(real) != served_real:
            # Excluded exactly as the server excludes it.
            continue
        kept.append(real)
    return kept, None


def probe_compression(served_dir):
    """Every served SSTable's compressor, aggregated. -> (state, detail).

    Checked per FILE, not per directory: one Snappy table beside four LZ4 ones
    still means the measured decode work is not what the band was derived for,
    and an aggregate that reported only the majority would hide it.
    """
    data_files, error = contained_data_files(served_dir)
    if error is not None:
        return "UNPARSEABLE", error
    if not data_files:
        return "NO-SSTABLES", "no *-Data.db under %s" % served_dir
    worst = None
    for real in data_files:
        data_file = os.path.basename(real)
        stem = real[: -len("Data.db")]
        info = stem + "CompressionInfo.db"
        if not os.path.exists(info) or os.path.getsize(info) == 0:
            return "MISSING", "%s has no usable CompressionInfo.db" % data_file
        state, detail = parse_compression_info(info)
        if state != "LZ4":
            # First offender wins: the remedy is per-file and naming one file the
            # operator can look at beats a summary of how many were wrong.
            return state, "%s: %s" % (
                os.path.basename(stem) + "CompressionInfo.db", detail)
        worst = detail
    return "LZ4", "%d served SSTable(s), all LZ4Compressor" % len(data_files)


def classify_storage_model(model):
    """Sort a device model string into the four storage verdicts.

    AFFIRMATIVE ON BOTH SIDES. The first version returned LOCAL for any model
    that was not EBS -- a pass derived from the ABSENCE of a bad signal, which is
    the shape this lane keeps finding elsewhere. An unrecognised model is
    UNRECOGNISED: an NFS-backed loop device, another cloud's network volume or a
    SAN LUN would all have passed as local.

    Split out from the filesystem walk so it can be exercised against models no
    path on any one box reports.
    """
    if not model.strip():
        return "NOT-MEASURABLE"
    lowered = model.lower()
    if "elastic block store" in lowered:
        return "NETWORK"
    if "instance storage" in lowered:
        return "LOCAL"
    return "UNRECOGNISED"


def parse_startup(path, want):
    """The resolved admission ceiling, or its provenance, from the server's own
    startup line -- or the literal NOT-OBSERVED.

    Best effort, and NAMED when it fails: a value we passed and a value the
    server resolved are different facts, and an unreadable line must never be
    silently upgraded into the value we hoped for.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = _ansi_stripped(handle.read())
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
    if command == "validate-ticket-schema":
        if len(rest) != 1:
            err("usage-error validate-ticket-schema needs <template.json>")
            return 2
        return validate_ticket(rest[0], full_ring=False)
    if command == "canonical-shape":
        if len(rest) != 1:
            err("usage-error canonical-shape needs <shape>")
            return 2
        label = canonical_shape(rest[0])
        if label is None:
            err("cause shape-unknown")
            err("cause-detail --shape %r is not a shape flight-loadgen accepts; "
                "Shape::parse (tools/flight-loadgen/src/shape.rs:34) takes %s"
                % (rest[0], "|".join(sorted(SHAPE_ALIASES))))
            return 1
        sys.stdout.write("%s\n" % label)
        return 0
    if command == "probe-compression":
        if len(rest) != 1:
            err("usage-error probe-compression needs <served-dir>")
            return 2
        state, detail = probe_compression(rest[0])
        sys.stdout.write("%s %s\n" % (state, detail))
        return 0
    if command == "probe-storage":
        if len(rest) != 1:
            err("usage-error probe-storage needs <path>")
            return 2
        verdict, device, detail = probe_storage(rest[0])
        sys.stdout.write("%s %s %s\n" % (verdict, device, detail))
        return 0
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
    if command == "resolve-session":
        if len(rest) != len(RESOLVER_INPUTS):
            err("usage-error resolve-session needs %d arguments, one per declared "
                "input: %s" % (len(RESOLVER_INPUTS), " ".join(RESOLVER_INPUTS)))
            return 2
        resolved, problems = resolve_session(*rest)
        if problems:
            err("cause session-config-invalid")
            for problem in problems:
                err("cause-detail %s" % problem)
            return 1
        sys.stdout.write(json.dumps(resolved, sort_keys=True) + "\n")
        return 0
    if command == "validate-resolved":
        if len(rest) != 4:
            err("usage-error validate-resolved needs <batch> <maxbytes> <wait> <scans>")
            return 2
        problems = validate_resolved(*rest)
        if problems:
            err("cause resolved-config-invalid")
            for problem in problems:
                err("cause-detail %s" % problem)
            return 1
        return 0
    if command == "server-argv":
        if len(rest) != 8:
            err("usage-error server-argv needs 8 arguments (see --help)")
            return 2
        argv = server_argv(*rest)
        if argv is None:
            return 1
        # One token per line: the caller reads it into an array, so a value with
        # a space cannot silently split.
        sys.stdout.write("\n".join(argv) + "\n")
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
        if len(rest) != 5:
            err("usage-error validate-replicate needs <jsonl> <round-label> "
                "<ramp> <shape> <step-duration-s>")
            return 2
        seconds = parse_duration_seconds(rest[4])
        if seconds is None or seconds <= 0:
            err("usage-error validate-replicate needs a positive <step-duration-s>")
            return 2
        return validate_replicate(rest[0], rest[1], rest[2], rest[3], seconds)
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
