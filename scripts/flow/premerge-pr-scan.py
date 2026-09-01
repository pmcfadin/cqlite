#!/usr/bin/env python3
"""Structural scans of a PR thread for `premerge-review-binding.sh` (issue #3752).

Two subcommands, both reading RAW `gh --json` payloads as FILE ARGUMENTS and
writing `key=value` lines a shell can read with `sed`:

    premerge-pr-scan.py jobs <pr-json>
    premerge-pr-scan.py hold <window-secs> <pr-json> <timeline-json> [<thread-json>...]

WHY THIS IS PYTHON AND NOT SHELL (#3312, "control and data must not share a
channel when the data is attacker-controlled). A PR comment body is arbitrary
attacker-controlled text on a PUBLIC repository. Flattening bodies and authors
into one stream with an in-band delimiter lets a body forge its own author
record — the exact defect roborev job 25 fixed in `roborev-waiver-scan.py`. So
author, body and timestamp stay SEPARATE FIELDS of one decoded object here, and
nothing downstream ever re-associates them from text.

WHY THE MARKERS ARE COLUMN-ZERO ANCHORED. Same issue, instance 1: prose inside a
quoted diff or an indented example must not be able to act as control. Every
marker this file recognises must BE the start of its line. A `>`-quoted,
indented or mid-sentence copy is inert.

    jobs   -> `job=<id>` (one per DISTINCT roborev job id recorded in a
              `==== ROBOREV REVIEW SUMMARY ====` block on the PR body or a
              top-level comment), plus `recorded-verdict=<id>:<token>` for the
              terminal verdict line of that block, informational ONLY (#3752
              declared residual: this tool does not enforce it).
              exit 0 parsed (even with zero jobs) / 1 unparseable.

    hold   -> `event=<kind>:<iso>:<login>` for every recognised event, then
              `state=hold|clear`, plus `detail=` lines.
              exit 0 clear / 4 hold / 5 unmeasured.

THE TWO DIRECTIONS ARE NOT SYMMETRIC, AND THAT IS THE POINT. A HOLD is the
CONSERVATIVE direction: an unauthorized HOLD costs a stalled merge, which is
recoverable. A RELEASE is the PERMISSIVE direction: an unauthorized RELEASE
merges code somebody stopped. So a HOLD is honoured from ANY author, and a
release is honoured ONLY from `RELEASE_AUTHORS` below — hard-coded, with NO env
override, because an override is settable by the party it constrains.
"""

import json
import re
import sys
from datetime import datetime, timezone

# WHO MAY RELEASE A HOLD. Hard-coded on purpose (#3312 job 25 + its corollary):
# not a config file, not an env var, not a flag. One visible location keeps "who
# may release" inside the diff a reviewer already reads. A release from anyone
# else is IGNORED and the hold stands — reported, never silently dropped.
RELEASE_AUTHORS = ("pmcfadin",)

BLOCK_START = "==== ROBOREV REVIEW SUMMARY ===="
BLOCK_END = "==== END ROBOREV REVIEW SUMMARY ===="

# Column-zero anchored, and the key must be followed by whitespace: `job:` and
# `job-record:` are different keys, and a prefix match would read the second as
# the first and then fail to parse its value.
#
# THE VALUE MAY BE FOLLOWED BY MORE FIELDS, because the real block packs three
# onto one line — `job: 27      model: gpt-5.6-sol      census: 14 files` — so a
# `$`-anchored value matched NOTHING on any genuine block (measured against the
# block recorded on PR #3801). The digit run is still terminated by whitespace
# or end of line, so `job: 27x` is not a job id.
JOB_KEY = re.compile(r"^(?:job|recheck-of):[ \t]+([0-9]{1,12})(?:[ \t].*)?$")
# The terminal verdict key, assembled rather than written literally so this
# file's own text cannot be grepped as a certification (the needle-split idiom
# `scripts/tests/test_base_staleness.sh` Case 18 pins for the shell side).
VERDICT_KEY = re.compile(r"^" + "RESUL" + r"T:[ \t]+([A-Za-z0-9_-]+)")

HOLD_MARKER = re.compile(r"^HOLD:")
RELEASE_MARKER = re.compile(r"^(?:GO|RELEASE):")


def fail(msg):
    sys.stdout.write("detail=%s\n" % " ".join(str(msg).split()))


def load(path):
    """Decode one gh payload. Returns (obj, None) or (None, reason)."""
    try:
        with open(path, "r") as handle:
            return json.load(handle), None
    except (OSError, ValueError) as exc:
        return None, "%s could not be read as JSON (%s)" % (path, exc.__class__.__name__)


def block_lines(text):
    """Yield the lines inside each column-zero-anchored roborev block."""
    inside = False
    for line in text.splitlines():
        stripped = line.rstrip("\r")
        if stripped == BLOCK_START:
            inside = True
            continue
        if stripped == BLOCK_END:
            inside = False
            continue
        if inside:
            yield stripped


def thread_texts(payload):
    """Every body on a PR payload, as SEPARATE strings. Never concatenated."""
    texts = []
    if isinstance(payload, dict):
        body = payload.get("body")
        if isinstance(body, str):
            texts.append(body)
        comments = payload.get("comments")
        if isinstance(comments, list):
            for comment in comments:
                if isinstance(comment, dict) and isinstance(comment.get("body"), str):
                    texts.append(comment["body"])
    return texts


def cmd_jobs(argv):
    if len(argv) != 1:
        sys.stderr.write("usage: premerge-pr-scan.py jobs <pr-json>\n")
        return 2
    payload, why = load(argv[0])
    if payload is None:
        fail(why)
        return 1
    seen = []
    for text in thread_texts(payload):
        for line in block_lines(text):
            match = JOB_KEY.match(line)
            if match:
                job = match.group(1).lstrip("0") or "0"
                if job not in seen:
                    seen.append(job)
                continue
            verdict = VERDICT_KEY.match(line)
            if verdict and seen:
                sys.stdout.write("recorded-verdict=%s:%s\n" % (seen[-1], verdict.group(1)))
    for job in seen:
        sys.stdout.write("job=%s\n" % job)
    return 0


def parse_iso(value):
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def sane(value):
    """Collapse to one line: every emitted field must not break the anchor."""
    return " ".join(str(value).split())


def collect_marker_events(payload, events, unparsed):
    """Read HOLD / GO markers out of ONE thread payload, structurally."""
    if not isinstance(payload, dict):
        unparsed.append("a thread payload was not a JSON object")
        return
    comments = payload.get("comments")
    if comments is None:
        comments = []
    if not isinstance(comments, list):
        unparsed.append("a thread payload's comments field was not a list")
        return
    for comment in comments:
        if not isinstance(comment, dict):
            unparsed.append("a thread comment was not a JSON object")
            continue
        body = comment.get("body")
        author = comment.get("author")
        login = ""
        if isinstance(author, dict) and isinstance(author.get("login"), str):
            login = author["login"]
        stamp = parse_iso(comment.get("createdAt") or comment.get("created_at"))
        if not isinstance(body, str):
            continue
        if stamp is None:
            # A marker we cannot ORDER cannot be decided against its siblings.
            # Unmeasurable, never ignored: `latest wins` needs a `latest`.
            for line in body.splitlines():
                if HOLD_MARKER.match(line) or RELEASE_MARKER.match(line):
                    unparsed.append(
                        "a comment carrying a HOLD/GO marker has no readable timestamp")
                    break
            continue
        for line in body.splitlines():
            if HOLD_MARKER.match(line):
                events.append(("hold", stamp, login))
                break
        for line in body.splitlines():
            if RELEASE_MARKER.match(line):
                events.append(("release", stamp, login))
                break


def collect_disarm_events(payload, window, now, events, unparsed):
    if not isinstance(payload, list):
        unparsed.append("the timeline payload was not a JSON list")
        return
    for item in payload:
        if not isinstance(item, dict):
            unparsed.append("a timeline entry was not a JSON object")
            continue
        if item.get("event") != "auto_merge_disabled":
            continue
        stamp = parse_iso(item.get("created_at") or item.get("createdAt"))
        if stamp is None:
            unparsed.append("an auto_merge_disabled event has no readable timestamp")
            continue
        age = (now - stamp).total_seconds()
        if age > window:
            continue
        actor = item.get("actor")
        login = actor["login"] if isinstance(actor, dict) and isinstance(
            actor.get("login"), str) else ""
        events.append(("disarm", stamp, login))


def cmd_hold(argv):
    if len(argv) < 3:
        sys.stderr.write(
            "usage: premerge-pr-scan.py hold <window-secs> <pr-json> "
            "<timeline-json> [<thread-json>...]\n")
        return 2
    try:
        window = int(argv[0])
    except ValueError:
        fail("the window argument %r is not an integer" % argv[0])
        return 5
    if window < 0:
        fail("the window argument %r is negative" % argv[0])
        return 5
    now = datetime.now(timezone.utc)

    events = []
    unparsed = []

    pr_payload, why = load(argv[1])
    if pr_payload is None:
        fail(why)
        return 5
    collect_marker_events(pr_payload, events, unparsed)

    timeline, why = load(argv[2])
    if timeline is None:
        fail(why)
        return 5
    collect_disarm_events(timeline, window, now, events, unparsed)

    for path in argv[3:]:
        thread, why = load(path)
        if thread is None:
            fail(why)
            return 5
        collect_marker_events(thread, events, unparsed)

    for kind, stamp, login in sorted(events, key=lambda e: e[1]):
        sys.stdout.write("event=%s:%s:%s\n" % (kind, stamp.isoformat(), sane(login)))

    if unparsed:
        for reason in unparsed:
            fail(reason)
        fail("a thread carrying markers could not be read structurally, so `latest wins` "
             "cannot be decided; this is UNMEASURED and a consumer must treat it as a hold")
        return 5

    # `latest wins`, with the two directions kept asymmetric: a release counts
    # only from an allowlisted author, so a hold cannot be lifted by the party
    # it constrains. A release from anyone else is REPORTED and then ignored.
    latest_stop = None
    latest_release = None
    for kind, stamp, login in events:
        if kind in ("hold", "disarm"):
            if latest_stop is None or stamp > latest_stop[1]:
                latest_stop = (kind, stamp, login)
        elif kind == "release":
            if login in RELEASE_AUTHORS:
                if latest_release is None or stamp > latest_release[1]:
                    latest_release = (kind, stamp, login)
            else:
                fail("a %s marker from @%s is IGNORED: releasing a hold is the PERMISSIVE "
                     "direction and is honoured only from the hard-coded allowlist"
                     % ("GO/RELEASE", sane(login) or "<no author recorded>"))

    if latest_stop is None:
        sys.stdout.write("state=clear\n")
        return 0
    if latest_release is not None and latest_release[1] > latest_stop[1]:
        fail("the latest %s at %s is superseded by an authorized release at %s"
             % (latest_stop[0], latest_stop[1].isoformat(), latest_release[1].isoformat()))
        sys.stdout.write("state=clear\n")
        return 0
    fail("the latest stop order is a %s at %s by @%s, and no authorized release is newer"
         % (latest_stop[0], latest_stop[1].isoformat(),
            sane(latest_stop[2]) or "<no author recorded>"))
    sys.stdout.write("state=hold\n")
    return 4


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("usage: premerge-pr-scan.py <jobs|hold> ...\n")
        return 2
    if argv[1] == "jobs":
        return cmd_jobs(argv[2:])
    if argv[1] == "hold":
        return cmd_hold(argv[2:])
    sys.stderr.write("premerge-pr-scan.py: unknown subcommand %r\n" % argv[1])
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
