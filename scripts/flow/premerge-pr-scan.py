#!/usr/bin/env python3
"""Structural scans of a PR thread for `premerge-review-binding.sh` (issue #3752).

Two subcommands, both reading RAW `gh --json` payloads as FILE ARGUMENTS and
writing `key=value` lines a shell can read with `sed`:

    premerge-pr-scan.py jobs <pr-json>
    premerge-pr-scan.py hold <window-secs> <pr-json> <timeline-json> [<thread-json>...]
    premerge-pr-scan.py normalize <view-json> <comments-stream-json> <out-json>

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

    normalize -> merges a `gh pr view --json` payload with a PAGINATED REST
              comment stream and writes ONE payload in the shape every consumer
              here already expects. exit 0 written / 1 refused.

WHY `normalize` EXISTS (roborev job 59, finding 2). `gh pr view --json comments`
returns a BOUNDED connection, not the whole thread, so a persistent column-zero
`HOLD:` outside the returned window yielded a false `NO-HOLD-RECOGNISED` — the
same defect already fixed for the disarm TIMELINE, still live for the COMMENTS.
The complete thread is only available from `gh api --paginate`, which speaks
REST: `created_at`/`updated_at`/`user.login` where `gh pr view --json` speaks
`createdAt`/`updatedAt`/`author.login`.

THAT SPELLING DIFFERENCE IS A COUPLING THAT GREENS VACUOUSLY IF IGNORED. Feed a
REST payload to a consumer reading `author.login` and every author reads as
empty: a deferral authorization silently stops being granted, and a `GO:` from
the allowlist silently stops releasing. Both are fail-closed and BOTH ARE WRONG
ON CORRECT INPUT. So the spellings are reconciled ONCE, here, at the fetch
boundary — the #3229 "normalise once, at the census" rule — and every consumer
downstream keeps one input contract.

IT REFUSES RATHER THAN RETURNING LESS. A shape this code does not recognise is
exit 1 with a named cause, never a payload with fewer comments in it: a short
comment list is indistinguishable from a quiet thread, and that is precisely the
false clearance being fixed. `body` is passed through VERBATIM — null stays
null, a non-string stays whatever it is — because the three-valued body
judgement belongs to `collect_marker_events` and normalising it here would
destroy the distinction between "no text" and "a shape we cannot read".

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


def load_stream(path):
    """Decode a CONCATENATION of JSON documents into a list of them.

    `gh api --paginate` emits ONE JSON ARRAY PER PAGE, concatenated — NOT one
    array — so `json.load` fails on any genuinely paginated payload and a
    single-page read silently drops every later page. Both directions are
    wrong for this leg (#3752 blocker 3): a disarm on page 2 must be seen, and
    a stream that cannot be decoded IN FULL must be UNMEASURED rather than
    evaluated from the part that happened to parse.

    Returns (documents, None) or (None, reason).
    """
    try:
        with open(path, "r") as handle:
            text = handle.read()
    except OSError as exc:
        return None, "%s could not be read (%s)" % (path, exc.__class__.__name__)
    decoder = json.JSONDecoder()
    documents = []
    index = 0
    size = len(text)
    while True:
        while index < size and text[index] in " \t\r\n":
            index += 1
        if index >= size:
            break
        try:
            document, index = decoder.raw_decode(text, index)
        except ValueError:
            return None, (
                "%s holds a page that could not be decoded (at byte %d of %d); a paginated "
                "timeline that cannot be read in full is unmeasured, never clear"
                % (path, index, size))
        documents.append(document)
    if not documents:
        return None, "%s held no JSON document at all, so no page was read" % path
    return documents, None


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
        # ===== ORDERED BY `updatedAt`, NOT `createdAt` (roborev job 59, finding 3) =====
        # `latest wins`, and what a reader SEES is the current text — so the
        # ordering key has to be when the text last changed. Keyed on creation
        # time, an OLD comment EDITED to carry `HOLD:` loses to a NEWER `GO:`
        # that was posted before the edit, and the hold visible on the thread
        # right now is silently ignored. That is a false clearance in the
        # permissive direction, which is the one this leg exists to refuse.
        created_raw = comment.get("createdAt")
        if created_raw is None:
            created_raw = comment.get("created_at")
        updated_raw = comment.get("updatedAt")
        if updated_raw is None:
            updated_raw = comment.get("updated_at")
        if updated_raw is not None:
            stamp = parse_iso(updated_raw)
            if stamp is None:
                # PRESENT BUT UNREADABLE. The comment may have been edited and
                # we cannot tell WHEN, so a marker inside it cannot be ordered
                # against its siblings — unmeasurable, never ignored. Reported
                # only when the body actually carries a marker, so an
                # unparseable stamp on an ordinary comment does not red the run.
                edited_unreadable = True
            else:
                edited_unreadable = False
        else:
            # NO UPDATE METADATA AT ALL. Creation time is then the only key
            # available; an edit is undetectable rather than mis-ordered, which
            # is a DECLARED limit of the payload and not a choice made here.
            stamp = parse_iso(created_raw)
            edited_unreadable = False
        if not isinstance(body, str):
            # THREE-VALUED, not two (#3752, lane-3752 audit). An ABSENT or null
            # body is a comment with no text, and text is the only thing a
            # column-zero marker can live in, so there is genuinely nothing to
            # read and skipping it is correct. Any OTHER type is a payload
            # SHAPE we do not understand: silently skipping it would fold "no
            # marker here" onto "we could not look", and the fold lands on the
            # permissive side, because a skipped comment can only ever REDUCE
            # the hold set. So it is recorded and the leg goes UNMEASURED.
            if body is not None:
                unparsed.append("a thread comment's body was not a string")
            continue
        if stamp is None or edited_unreadable:
            # A marker we cannot ORDER cannot be decided against its siblings.
            # Unmeasurable, never ignored: `latest wins` needs a `latest`.
            for line in body.splitlines():
                if HOLD_MARKER.match(line) or RELEASE_MARKER.match(line):
                    if edited_unreadable:
                        unparsed.append(
                            "a comment carrying a HOLD/GO marker has an unreadable edit "
                            "timestamp, so whether its current text supersedes a later "
                            "marker cannot be decided")
                    else:
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


def _norm_author(comment):
    """(login, None) or (None, reason). REST says `user`, GraphQL says `author`."""
    for key in ("author", "user"):
        if key not in comment:
            continue
        value = comment[key]
        if value is None:
            # A DELETED ACCOUNT is a legitimate shape, not a broken payload: both
            # APIs answer null. It yields no login, which is fail-closed for the
            # two permissive directions (a release and a deferral both require an
            # allowlisted login) and harmless for the conservative one (a HOLD is
            # honoured from any author).
            return "", None
        if isinstance(value, dict):
            login = value.get("login")
            if login is None:
                return "", None
            if not isinstance(login, str):
                return None, "a comment's %s.login was not a string" % key
            return login, None
        if isinstance(value, str):
            return value, None
        return None, "a comment's %s field was neither an object, a string nor null" % key
    return "", None


def _norm_stamps(comment):
    """(createdAt, updatedAt, None) or (None, None, reason), spellings reconciled.

    A stamp is carried through as TEXT, unparsed: whether it can be parsed, and
    what an unparseable one means for a marker-bearing comment, is
    `collect_marker_events`' three-valued decision and not this function's.
    """
    out = []
    for camel, snake in (("createdAt", "created_at"), ("updatedAt", "updated_at")):
        value = None
        for key in (camel, snake):
            if key in comment and comment[key] is not None:
                value = comment[key]
                break
        if value is None:
            out.append(None)
            continue
        if not isinstance(value, str):
            return None, None, "a comment's %s was not a string" % camel
        out.append(value)
    return out[0], out[1], None


def cmd_normalize(argv):
    if len(argv) != 3:
        sys.stderr.write(
            "usage: premerge-pr-scan.py normalize <view-json> <comments-stream-json> "
            "<out-json>\n")
        return 2
    view, why = load(argv[0])
    if view is None:
        fail(why)
        return 1
    if not isinstance(view, dict):
        fail("the pull-request view payload was not a JSON object")
        return 1
    pages, why = load_stream(argv[1])
    if pages is None:
        fail(why)
        return 1
    comments = []
    for page in pages:
        # A PAGE THAT IS NOT A LIST IS A REFUSAL. `gh api --paginate` on a
        # comments endpoint emits one ARRAY per page; anything else (an error
        # object, an unexpected envelope) must not be read as "this page held no
        # comments", which would silently shorten the thread.
        if not isinstance(page, list):
            fail("a comment page was not a JSON list, so the thread could not be read in full")
            return 1
        for comment in page:
            if not isinstance(comment, dict):
                fail("a comment entry was not a JSON object")
                return 1
            login, why = _norm_author(comment)
            if login is None:
                fail(why)
                return 1
            created, updated, why = _norm_stamps(comment)
            if why is not None:
                fail(why)
                return 1
            entry = {"author": {"login": login}, "body": comment.get("body")}
            if created is not None:
                entry["createdAt"] = created
            if updated is not None:
                entry["updatedAt"] = updated
            comments.append(entry)
    out = dict(view)
    out["comments"] = comments
    try:
        with open(argv[2], "w") as handle:
            json.dump(out, handle)
    except OSError as exc:
        fail("the normalised payload could not be written (%s)" % exc.__class__.__name__)
        return 1
    sys.stdout.write("comments=%d\n" % len(comments))
    return 0


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

    # EVERY page, decoded BEFORE anything is evaluated.
    pages, why = load_stream(argv[2])
    if pages is None:
        fail(why)
        return 5
    for page in pages:
        collect_disarm_events(page, window, now, events, unparsed)

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
        sys.stderr.write("usage: premerge-pr-scan.py <jobs|hold|normalize> ...\n")
        return 2
    if argv[1] == "jobs":
        return cmd_jobs(argv[2:])
    if argv[1] == "hold":
        return cmd_hold(argv[2:])
    if argv[1] == "normalize":
        return cmd_normalize(argv[2:])
    sys.stderr.write("premerge-pr-scan.py: unknown subcommand %r\n" % argv[1])
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
