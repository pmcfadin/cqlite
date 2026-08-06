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

A marker counts ONLY when it is the SOLE NONBLANK CONTENT of its comment (#3312 job 29). That replaces
four successive Markdown recognisers — anywhere-in-comment, column-zero anchor, fence skipping, fence
state tracking — because deciding "data or control?" inside a grammar the author controls is an unbounded
game. No quoting construct can be the only thing in a comment, so quoting cannot grant.

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
# ===== AN AUTHORIZATION MUST BE THE SOLE NONBLANK CONTENT OF ITS COMMENT (#3312 job 29) =====
# THE FIFTH VARIATION OF ONE DEFECT, and the reason this rule replaces a parser rather than extending it.
# Four recognisers were tried, each correct about the case in front of it and each superseded:
#   1. accept the marker ANYWHERE in the comment            -> a quoted example granted
#   2. require it to BE its own line (column-zero anchor)    -> defeated indented/quoted/bulleted/mid-line
#   3. skip fenced regions (``` and ~~~)                    -> a fence preserves column zero, so a quoted
#                                                              example inside one granted
#   4. track fence open/close state properly                -> ````bash` inside a fence is CONTENT, not a
#                                                              closing fence, so fence state desynchronised;
#                                                              and HTML <pre>/<code> were never covered
# Each fix asked "is this line DATA or CONTROL?" of a grammar the AUTHOR controls and which has unbounded
# ways to say "this is data" — so the list of recognisers never closes. That is the umbrella lesson of this
# issue applied to itself: REMOVE THE SHARED CHANNEL, DO NOT PICK A RARER DELIMITER. Parsing Markdown to
# separate data from control IS sharing a channel with the author.
#
# THE RULE: the marker must be the ONLY nonblank line in the comment. Leading and trailing blank lines are
# fine; ANY other content — prose, a fence delimiter, an HTML tag, a second sentence — means the comment is
# not an authorization. No quoting construct can satisfy it, because every quoting construct requires
# additional content. It is decidable without parsing anything.
#
# COST: the authorizer posts a comment containing only the marker and puts commentary in a separate comment.
# The token accounting already lives inside `reason=`, so nothing is lost — and an authorization SHOULD be a
# clean unambiguous act rather than a sentence buried in prose.
#
# A COMMENT WITH OTHER CONTENT IS IGNORED SILENTLY, not reported as malformed: someone documenting the form
# (this repository's own PR threads do) has not attempted an authorization, and reporting MALFORMED on their
# comment would be a false accusation printed on every later run. A marker-only comment whose FIELDS are
# wrong is still MALFORMED — there the author plainly meant to authorize. The `NONE` cause teaches the rule.


def sole_marker_line(body):
    """The comment's only nonblank line when it is a marker line, else None."""
    lines = [raw.rstrip("\r") for raw in body.split("\n")]
    nonblank = [line for line in lines if line.strip()]
    if len(nonblank) != 1:
        return None
    line = nonblank[0]
    return line if line.startswith(PREFIX) else None


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
        # ONE DECISION, NO PARSE: is the marker the whole comment?
        line = sole_marker_line(body)
        if line is not None:
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
