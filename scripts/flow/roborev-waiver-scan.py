#!/usr/bin/env python3
"""Decide the absence-waiver state for ONE review, from `gh pr view --json comments` JSON.

Usage:  roborev-waiver-scan.py <base-sha> <head-sha> <job-id> <allowlist> < comments.json

`<allowlist>` is a space-separated list of GitHub logins permitted to GRANT a waiver.
Prints a facts-style result on stdout (one `key=value` per line, values whitespace-collapsed):

    state=granted|unauthorized|stale|malformed|none
    author=<login>
    scope=base=<sha> head=<sha> job=<id>
    reason=<why>
    detail=<why the state is not `granted`>

Exit 0 whenever the JSON parsed; exit 1 if it did not (the caller treats that as UNAVAILABLE and
keeps the absence FAILing).

===================== WHY THIS IS A SEPARATE, STRUCTURED PARSE (#3312 job 26) =====================
CONTROL AND DATA MUST NOT SHARE A CHANNEL WHEN THE DATA IS ATTACKER-CONTROLLED. The previous
implementation flattened `gh`'s output into one text stream in which an author record was marked by a
leading SOH byte and the comment body followed VERBATIM. A comment body is attacker-controlled on a
public repository, so an unauthorized commenter could put a SOH line naming an allowlisted login
inside their OWN body, and the next marker line was attributed to that login — the allowlist added in
job 25 was bypassable with one control character.

The fix is to remove the delimiter rather than choose a rarer one: `gh --json` already returns author
and body as SEPARATE FIELDS, so the association is carried by the DATA STRUCTURE and there is nothing
to forge. Nothing a body can contain changes which object it sits in.

The whole decision lives here — shape, scope, reason and authorization — so the shell never associates
an author with a body at all. Values are whitespace-collapsed on output for the same reason
`roborev-job-facts.py` collapses its own: a newline inside a value would be a second in-band channel,
and a `reason` is free text.
"""
import json
import re
import sys

# The marker, matched against a WHOLE body line: every field required, in the documented order, with
# single-space separators. This is the one place the form is expressed.
MARKER = re.compile(
    r"^roborev-waive: prompt-content-absent"
    r" base=([0-9a-f]{7,40}) head=([0-9a-f]{7,40}) job=([0-9]+) reason=(.*)$"
)
PREFIX = "roborev-waive: prompt-content-absent "
PLACEHOLDERS = {
    "why", "todo", "tbd", "tba", "reason", "n/a", "na", "none", "-", "placeholder",
}
REQUIRED_FORM = (
    "'roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>' — every "
    "field is required, in that order, with single-space separators"
)


def collapse(value):
    return " ".join(str(value).split())


def emit(result):
    for key in ("state", "author", "scope", "reason", "detail"):
        sys.stdout.write("%s=%s\n" % (key, collapse(result.get(key, ""))))


def judge_line(line, author, base, head, job, allowlist):
    """Return (state, fields) for a line that starts with the marker prefix."""
    match = MARKER.match(line)
    if match is None:
        return "malformed", {
            "author": author,
            "detail": "the line does not match the required form %s" % REQUIRED_FORM,
        }
    m_base, m_head, m_job, m_reason = match.groups()
    reason = m_reason.strip()
    scope = "base=%s head=%s job=%s" % (m_base, m_head, m_job)
    if not reason:
        return "malformed", {
            "author": author,
            "detail": "the marker is missing a-non-empty-reason (the reason is empty or whitespace only)",
        }
    if "<" in reason and ">" in reason:
        return "malformed", {
            "author": author,
            "detail": "the marker is missing a-substituted-reason (the reason still holds an "
                      "unsubstituted <…> placeholder)",
        }
    if reason.lower() in PLACEHOLDERS:
        return "malformed", {
            "author": author,
            "detail": "the marker is missing a-substantive-reason (the reason '%s' is a bare "
                      "placeholder)" % reason,
        }
    diverged = []
    if m_base != base:
        diverged.append("base (%s != %s)" % (m_base, base))
    if m_head != head:
        diverged.append("head (%s != %s)" % (m_head, head))
    if m_job != job:
        diverged.append("job (%s != %s)" % (m_job, job))
    if diverged:
        return "stale", {
            "author": author,
            "scope": scope,
            "reason": reason,
            "detail": "the marker names a different review — %s — and a waiver may not outlive the "
                      "review its authorizer judged; re-request it for this base/head/job (a completed "
                      "job can be re-decided with --recheck-job <id>, which enqueues nothing)"
                      % ", ".join(diverged),
        }
    # AUTHORIZATION IS THE LAST GATE: a well-formed marker naming this exact review from a
    # non-allowlisted author is its own state — the marker was fine, the author was not permitted.
    if author not in allowlist:
        return "unauthorized", {
            "author": author,
            "scope": scope,
            "reason": reason,
            "detail": "the marker is well-formed and names this review, but its author '@%s' is not on "
                      "the waiver allowlist (%s) — this is a public repository, so the base/head/job "
                      "values printed in a failing block are public knowledge and authorship is the "
                      "only thing that separates an authorization from a stranger"
                      % (author, " ".join(allowlist)),
        }
    return "granted", {"author": author, "scope": scope, "reason": reason}


def main(argv):
    if len(argv) != 5:
        sys.stderr.write(
            "usage: roborev-waiver-scan.py <base-sha> <head-sha> <job-id> <allowlist> < comments.json\n")
        return 2
    base, head, job = argv[1], argv[2], argv[3]
    allowlist = [a for a in argv[4].split() if a]
    try:
        data = json.load(sys.stdin)
    except ValueError:
        return 1
    comments = data.get("comments") if isinstance(data, dict) else data
    if not isinstance(comments, list):
        comments = []

    granted = None
    first_refusal = None
    for comment in comments:
        if not isinstance(comment, dict):
            continue
        # THE ASSOCIATION COMES FROM THE OBJECT, not from anything inside the body. `author` is a
        # sibling FIELD of `body`, so no body content can change whose comment it is.
        author_obj = comment.get("author")
        author = ""
        if isinstance(author_obj, dict):
            author = author_obj.get("login") or ""
        elif isinstance(author_obj, str):
            author = author_obj
        author = collapse(author) or "unknown"
        body = comment.get("body")
        if not isinstance(body, str):
            continue
        for raw in body.split("\n"):
            line = raw.rstrip("\r")
            # ANCHORED: the marker must BE the line, so an indented, quoted, bulleted or
            # mid-sentence copy — the ways a human legitimately quotes the form — cannot match.
            if not line.startswith(PREFIX):
                continue
            state, fields = judge_line(line, author, base, head, job, allowlist)
            if state == "granted":
                granted = fields
            elif first_refusal is None:
                first_refusal = (state, fields)

    if granted is not None:
        emit(dict(granted, state="granted"))
    elif first_refusal is not None:
        state, fields = first_refusal
        emit(dict(fields, state=state))
    else:
        emit({"state": "none"})
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
