#!/usr/bin/env python3
"""Decide ONE authorization state for ONE review, from `gh pr view --json ...` JSON.

Usage:
    roborev-waiver-scan.py prompt-content-absent <base> <head> <job> <allowlist> < comments.json
    roborev-waiver-scan.py findings-deferral     <base> <head> <job> <allowlist> <observed-count> \
        < comments-and-body.json

`<allowlist>` is a space-separated list of GitHub logins permitted to GRANT. Prints a facts-style
result on stdout (one `key=value` per line, values whitespace-collapsed):

    state=granted|unauthorized|stale|malformed|none      (both kinds)
          |count-mismatch|pr-unlinked                    (findings-deferral only)
    author=<login>
    scope=base=<sha> head=<sha> job=<id>
    reason=<why>
    detail=<why the state is not `granted`>
    issues=<N>,<N>       (findings-deferral only)
    count=<n>            (findings-deferral only)

Exit 0 whenever the JSON parsed; exit 1 if it did not (the caller treats that as UNAVAILABLE and
keeps the underlying FAIL in place); exit 2 on a usage error.

===================== TWO MARKER KINDS, ONE CHANNEL (issue #3626) =====================
`roborev-waive: prompt-content-absent` excuses a prompt-content ABSENCE (#3312). `roborev-defer:
findings` records that a LEAD DEFERRED the findings of one completed review (#3626). They are
SEPARATE authorizations producing separate summary keys and separate verdict tokens, and NEITHER
reads the other's marker: collapsing them would let a delivery-artifact waiver excuse a real defect.

They share this FILE, and that is the point: the CHANNEL rules — sole-nonblank-content, top-level
comment, structured author association, placeholder refusal, one anchored pattern per marker — are
expressed ONCE and reused BY CALL, never by copy. Five recogniser generations were superseded before
the channel rule closed (see below); a second implementation of it would be a second place for it to
diverge, and the divergence would be an authorization bypass.

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

# ===== THE MARKER FORMS, EXPRESSED ONCE EACH, AS ONE ANCHORED PATTERN =====
# Matched against a WHOLE body line: every field required, in the documented order, with single-space
# separators. Field ORDER and field-value BOUNDARIES are both properties of these patterns and of
# nothing else — a per-field extraction enforced neither, which is how `job=4656x` once survived.
WAIVE_KIND = "prompt-content-absent"
DEFER_KIND = "findings-deferral"

WAIVE_MARKER = re.compile(
    r"^roborev-waive: prompt-content-absent"
    r" base=([0-9a-f]{7,40}) head=([0-9a-f]{7,40}) job=([0-9]+) reason=(.*)$"
)
WAIVE_PREFIX = "roborev-waive: prompt-content-absent "
# `issues=` admits ONE OR MORE comma-separated integers, so "non-empty" is a property of the pattern
# rather than a later check that could be forgotten. `count=` is the AFFIRMATIVE half of the binding
# (#3626): the wrapper requires it to equal the OBSERVED findings count, so a marker written before
# its job's findings were read, and any NEW finding arriving at the same head, both fail on it.
DEFER_MARKER = re.compile(
    r"^roborev-defer: findings"
    r" issues=([0-9]+(?:,[0-9]+)*) count=([0-9]+)"
    r" base=([0-9a-f]{7,40}) head=([0-9a-f]{7,40}) job=([0-9]+) reason=(.*)$"
)
DEFER_PREFIX = "roborev-defer: findings "

# ===== NO EMITTED DIAGNOSTIC CARRIES ANY PART OF THE MARKER FORM (#3312 job 23, layer 3) =====
# The MALFORMED detail used to quote the whole required form, and that detail is interpolated into the
# summary block's `waiver:`/`deferral:` key — so a block naming a live base/head/job printed a
# complete, fillable authorization. Pasting a summary block into a PR comment is the documented
# practice throughout this repository, which is how an artifact that DESCRIBED the escape hatch BECAME
# it. Two later layers (the sole-content rule; the placeholder refusal on `reason=<why>`) do stop a
# pasted form from granting, but the rule is stated absolutely — "no emitted diagnostic SHALL carry
# any part of the marker, not even its prefix" — because a rule with an exception for "the layers
# below catch it anyway" is a rule that decays the next time a layer moves. So the detail names the
# DEFECT and points at `--help`, which is the ONE place the form lives.
MALFORMED_FORM_DETAIL = (
    "the line begins an authorization of this kind but does not match its required form — every "
    "field is required, in the documented order, with single-space separators. THE FORM IS "
    "DELIBERATELY NOT REPRODUCED HERE, not even its prefix: run 'bash scripts/flow/"
    "roborev-review.sh --help' for it, because this text reaches the summary block and a block "
    "carrying a fillable marker would authorize the next run by being quoted (#3312 job 23)"
)
EMIT_KEYS = {
    WAIVE_KIND: ("state", "author", "scope", "reason", "detail"),
    DEFER_KIND: ("state", "author", "scope", "reason", "detail", "issues", "count"),
}
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
#
# ONE IMPLEMENTATION, TWO KINDS (#3626): the prefix is a PARAMETER, so the deferral marker inherits this
# rule by CALL. A copy of it for the second kind would be a second place for the channel rule to diverge,
# and a divergence here is an authorization bypass, not a cosmetic difference.


def sole_marker_line(body, prefix):
    """The comment's only nonblank line when it is a marker line of this kind, else None."""
    lines = [raw.rstrip("\r") for raw in body.split("\n")]
    nonblank = [line for line in lines if line.strip()]
    if len(nonblank) != 1:
        return None
    line = nonblank[0]
    return line if line.startswith(prefix) else None


PLACEHOLDERS = {
    "why", "todo", "tbd", "tba", "reason", "n/a", "na", "none", "-", "placeholder",
}


def collapse(value):
    return " ".join(str(value).split())


def emit(kind, result):
    for key in EMIT_KEYS[kind]:
        sys.stdout.write("%s=%s\n" % (key, collapse(result.get(key, ""))))


def judge_reason(reason):
    """Return a MALFORMED detail for a reason that cannot carry an authorization, else None.

    Shared by both kinds, and the reason is TRIMMED BEFORE IT IS JUDGED, so `reason=TODO ` and a
    whitespace-only reason are refused exactly as their untrimmed forms are.
    """
    if not reason:
        return "the marker is missing a-non-empty-reason (the reason is empty or whitespace only)"
    if "<" in reason and ">" in reason:
        return ("the marker is missing a-substituted-reason (the reason still holds an "
                "unsubstituted <…> placeholder)")
    if reason.lower() in PLACEHOLDERS:
        return ("the marker is missing a-substantive-reason (the reason '%s' is a bare "
                "placeholder)" % reason)
    return None


def judge_scope(m_base, m_head, m_job, base, head, job):
    """Return the list of scope fields that name a DIFFERENT review, in report order."""
    diverged = []
    if m_base != base:
        diverged.append("base (%s != %s)" % (m_base, base))
    if m_head != head:
        diverged.append("head (%s != %s)" % (m_head, head))
    if m_job != job:
        diverged.append("job (%s != %s)" % (m_job, job))
    return diverged


def judge_waive_line(line, author, base, head, job, allowlist):
    """Return (state, fields) for a line that starts with the absence-waiver prefix."""
    match = WAIVE_MARKER.match(line)
    if match is None:
        return "malformed", {"author": author, "detail": MALFORMED_FORM_DETAIL}
    m_base, m_head, m_job, m_reason = match.groups()
    reason = m_reason.strip()
    scope = "base=%s head=%s job=%s" % (m_base, m_head, m_job)
    bad_reason = judge_reason(reason)
    if bad_reason is not None:
        return "malformed", {"author": author, "detail": bad_reason}
    diverged = judge_scope(m_base, m_head, m_job, base, head, job)
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
            "detail": unauthorized_detail(author, allowlist, "waiver"),
        }
    return "granted", {"author": author, "scope": scope, "reason": reason}
def unauthorized_detail(author, allowlist, what):
    return ("the marker is well-formed and names this review, but its author '@%s' is not on "
            "the %s allowlist (%s) — this is a public repository, so the base/head/job "
            "values printed in a failing block are public knowledge and authorship is the "
            "only thing that separates an authorization from a stranger"
            % (author, what, " ".join(allowlist)))


# ===== A LOCAL ISSUE REFERENCE, WITH TOKEN *AND* REPOSITORY BOUNDARIES (roborev job 225) =====
# THE PR BODY IS WRITTEN BY THE WORKER — THE CONSTRAINED PARTY. So this predicate decides whether the
# party being constrained satisfied its own constraint, and it must not be satisfiable by a reference
# that names something else or renders as inert text. The first version bounded DIGITS only, so three
# shapes passed while `gh issue view <N>` (which the caller runs) validated the unrelated LOCAL issue:
#   * `other/repo#3602`  — a CROSS-REPOSITORY reference. GitHub resolves it to a different repository
#                          entirely, so it records no disposition here.
#   * `#3602suffix`      — not an autolink at all; a token boundary was missing on the right.
#   * a copy inside a ``` fence, a `code span` or an <!-- HTML comment --> — content that a human
#                          reading the PR does not see as a link, so the requirement "it must name
#                          WHERE THE FINDING WENT" is not met by it.
#
# WHY REGEX AND NOT STRUCTURED LINK DATA. `gh pr view --json` exposes `closingIssuesReferences`, which
# is a DIFFERENT relation: it lists issues a closing KEYWORD would close on merge. A deferred finding's
# issue must stay OPEN — that is the whole point of deferring it — so a deferral PR closes nothing and
# that field is empty for exactly the case this predicate exists for. GitHub exposes no "issues
# mentioned by this body" field. So the reference is read from the body text, with the inert regions
# removed first.
#
# EVERY AMBIGUITY IS RESOLVED TOWARD *NOT FOUND*, which is the fail-closed direction here: an
# unrecognised shape yields `pr-unlinked`, whose remedy is one line in the PR body ("add `#<N>`"),
# while the opposite error lets a deferred finding be dropped with no recorded disposition. The full
# URL form (`https://github.com/<owner>/<repo>/issues/<N>`) is therefore NOT accepted either — this
# scanner is not told which repository is local, and guessing is the shape being removed.
FENCE_OPEN = re.compile(r"^(?P<fence>`{3,}|~{3,})(?P<info>.*)$")


def strip_inert_regions(body):
    """The body with HTML comments, fenced code blocks and inline code spans removed.

    CONSERVATIVE BY CONSTRUCTION, and that is what makes this bounded rather than the unbounded
    "data or control?" game #3312 job 29 closed: every unterminated construct swallows the REST of
    its scope (an unterminated `<!--` the rest of the body, an unclosed fence the rest of the body,
    an unmatched backtick the rest of its line), so a malformed body removes MORE text and can only
    make a reference harder to find.

    THE FENCE CLOSER FOLLOWS CommonMark, deliberately: a closing fence uses the SAME character, is at
    least as long as the opener, and carries NO info string. A naive "any fence line toggles" rule
    desynchronises on a ```` ```bash ```` line INSIDE a fence — GitHub's renderer keeps that as code
    while the naive rule would leave it as text, which is a reference the human sees as code and the
    machine counts as a link. That is fail-OPEN, in the one direction this predicate must not fail.
    """
    # HTML comments first: they can span lines and can contain fences.
    body = re.sub(r"<!--.*?-->", " ", body, flags=re.S)
    body = re.sub(r"<!--.*\Z", " ", body, flags=re.S)
    kept = []
    open_fence = None
    for line in body.split("\n"):
        stripped = line.strip()
        if open_fence is None:
            match = FENCE_OPEN.match(stripped)
            if match is not None:
                open_fence = match.group("fence")
                continue
            # Inline code spans, and an unmatched backtick to end of line.
            line = re.sub(r"`[^`]*`", " ", line)
            line = re.sub(r"`.*$", " ", line)
            kept.append(line)
            continue
        closer = FENCE_OPEN.match(stripped)
        if (closer is not None
                and closer.group("fence")[0] == open_fence[0]
                and len(closer.group("fence")) >= len(open_fence)
                and not closer.group("info").strip()):
            open_fence = None
    return "\n".join(kept)


# A LOCAL `#<N>`: no repository qualifier or word character before it, no token character after it.
# The left class covers `owner/repo#N`, `GH-`-style prefixes and any `word#N`; the right class covers
# `#Nsuffix`. `.` and `)` are NOT in the right class, so `#3602.` and `(#3602)` still count.
LOCAL_REF_LEFT = r"(?<![0-9A-Za-z._/-])"
LOCAL_REF_RIGHT = r"(?![0-9A-Za-z_-])"


def body_references_issue(pr_body, issue):
    """Is `#<issue>` referenced by the pull-request body as a LOCAL issue, in visible text?"""
    visible = strip_inert_regions(pr_body)
    pattern = LOCAL_REF_LEFT + "#" + re.escape(issue) + LOCAL_REF_RIGHT
    return re.search(pattern, visible) is not None


def judge_defer_line(line, author, base, head, job, allowlist, observed_count, pr_body):
    """Return (state, fields) for a line that starts with the findings-deferral prefix.

    THE MATCH IS AFFIRMATIVE, NEVER PERMISSIVE (#3626, on #3586's rule). A grant needs the scope to
    match, the declared `count=` to EQUAL the observed findings count, and every declared issue to be
    referenced from the PR body. Nothing here is derived from the ABSENCE of a contrary signal, and
    there is deliberately NO reconstruction of per-finding identity from the review's prose: that is a
    recogniser over author-controlled text, the class #3564 closed by REMOVING prose reconstruction.
    """
    match = DEFER_MARKER.match(line)
    if match is None:
        return "malformed", {"author": author, "detail": MALFORMED_FORM_DETAIL}
    m_issues, m_count, m_base, m_head, m_job, m_reason = match.groups()
    reason = m_reason.strip()
    scope = "base=%s head=%s job=%s" % (m_base, m_head, m_job)
    fields = {"author": author, "scope": scope, "issues": m_issues, "count": m_count}
    bad_reason = judge_reason(reason)
    if bad_reason is not None:
        return "malformed", {"author": author, "detail": bad_reason}
    fields["reason"] = reason
    diverged = judge_scope(m_base, m_head, m_job, base, head, job)
    if diverged:
        return "stale", dict(fields, detail=(
            "the marker names a different review — %s — and a deferral may not outlive the review "
            "its authorizer judged; a push, a different base or a re-run each need a fresh one "
            "(re-decide a completed job with --recheck-job <id>, which enqueues nothing)"
            % ", ".join(diverged)))
    # ===== THE AFFIRMATIVE HALF: THE DECLARED COUNT MUST EQUAL THE OBSERVED ONE =====
    # A job is a completed review and its findings do not change, so the job binding already fixes the
    # finding SET; this is what makes the match affirmative rather than "not listed as new". Two
    # consequences, both deliberate: a PRE-AUTHORIZATION written before the findings were read fails
    # on a count mismatch instead of passing silently, and ANY new finding at the same head raises the
    # observed count and therefore fails. That is how "the UNDEFERRED set" is computed without a
    # per-finding identity that does not exist.
    if m_count != observed_count:
        return "count-mismatch", dict(fields, detail=(
            "the marker authorizes %s finding(s) but this job reports %s — the counts must match "
            "EXACTLY, because that equality is the only affirmative evidence that the findings the "
            "authorizer judged are the findings this run observed. A new finding at the same head "
            "raises the observed count and must not ride an older authorization; re-triage, then "
            "re-authorize for the count actually observed" % (m_count, observed_count)))
    # ===== THE DISPOSITION HALF: A DEFERRAL WITHOUT A LINKED ISSUE IS A DROPPED FINDING =====
    # The nit rule already requires one follow-up issue at merge time; requiring the PR body to
    # reference each deferred issue makes that link MECHANICAL instead of remembered. Retrievability
    # is asserted by the caller (it needs the network); this half is decidable from the same JSON the
    # comments came from, so it lives with the rest of the structured parse.
    unlinked = [n for n in m_issues.split(",") if not body_references_issue(pr_body, n)]
    if unlinked:
        return "pr-unlinked", dict(fields, detail=(
            "the pull-request body does not reference issue(s) %s, so the deferred finding(s) have "
            "no recorded disposition — a deferral without a linked issue is a DROPPED finding. Add "
            "the reference to the PR body (the authorization comment records the ruling; the body "
            "records where the finding went)" % ", ".join("#" + n for n in unlinked)))
    # AUTHORIZATION IS THE LAST GATE, exactly as it is for the waiver.
    if author not in allowlist:
        return "unauthorized", dict(fields, detail=unauthorized_detail(author, allowlist, "deferral"))
    return "granted", fields


def scan(kind, comments, base, head, job, allowlist, observed_count, pr_body):
    """The LAST GRANTED marker wins; otherwise the FIRST refusal is reported."""
    granted = None
    first_refusal = None
    prefix = WAIVE_PREFIX if kind == WAIVE_KIND else DEFER_PREFIX
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
        line = sole_marker_line(body, prefix)
        if line is None:
            continue
        if kind == WAIVE_KIND:
            state, fields = judge_waive_line(line, author, base, head, job, allowlist)
        else:
            state, fields = judge_defer_line(
                line, author, base, head, job, allowlist, observed_count, pr_body)
        if state == "granted":
            granted = fields
        elif first_refusal is None:
            first_refusal = (state, fields)
    if granted is not None:
        return dict(granted, state="granted")
    if first_refusal is not None:
        state, fields = first_refusal
        return dict(fields, state=state)
    return {"state": "none"}


def main(argv):
    kinds = (WAIVE_KIND, DEFER_KIND)
    if len(argv) < 6 or argv[1] not in kinds:
        sys.stderr.write(
            "usage: roborev-waiver-scan.py <%s> <base-sha> <head-sha> <job-id> <allowlist> "
            "[<observed-findings-count>] < pr.json\n" % "|".join(kinds))
        return 2
    kind, base, head, job = argv[1], argv[2], argv[3], argv[4]
    allowlist = [a for a in argv[5].split() if a]
    observed_count = ""
    # THE COUNT IS REQUIRED FOR THE DEFERRAL AND REFUSED FOR THE WAIVER — a usage error, never a
    # default. An absent count would make the affirmative half of the deferral binding unenforceable,
    # and a default would let that happen SILENTLY, which is the shape a positive verdict must never
    # rest on. The waiver has no count to bind, so accepting one there would invite the two kinds'
    # arguments to be confused.
    if kind == DEFER_KIND:
        if len(argv) != 7:
            sys.stderr.write(
                "usage: roborev-waiver-scan.py findings-deferral <base> <head> <job> <allowlist> "
                "<observed-findings-count>\n")
            return 2
        observed_count = argv[6]
        if not re.match(r"^[0-9]+$", observed_count):
            sys.stderr.write(
                "roborev-waiver-scan.py: the observed findings count must be a non-negative "
                "integer, got '%s' — a deferral is matched against a MEASURED count, so an "
                "unmeasurable one fails closed rather than defaulting\n" % observed_count)
            return 2
    elif len(argv) != 6:
        sys.stderr.write(
            "usage: roborev-waiver-scan.py prompt-content-absent <base> <head> <job> <allowlist>\n")
        return 2
    try:
        data = json.load(sys.stdin)
    except ValueError:
        return 1
    comments = data.get("comments") if isinstance(data, dict) else data
    if not isinstance(comments, list):
        comments = []
    pr_body = data.get("body") if isinstance(data, dict) else None
    if kind == DEFER_KIND and not isinstance(pr_body, str):
        # THE PR BODY IS THE SOLE EVIDENCE for the disposition half, so a payload that does not carry
        # it cannot yield a grant. Reported as its own state rather than as `pr-unlinked`: "the body
        # says nothing about #N" and "there is no body to read" are different operator actions.
        emit(kind, {"state": "unavailable",
                    "detail": "the 'gh pr view' payload carries no readable 'body' field, so the "
                              "disposition of a deferred finding could not be established"})
        return 0
    emit(kind, scan(kind, comments, base, head, job, allowlist,
                    observed_count, pr_body if isinstance(pr_body, str) else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
