#!/usr/bin/env python3
"""Decide ONE authorization state for ONE review, from `gh pr view --json ...` JSON.

Usage:
    roborev-waiver-scan.py prompt-content-absent <base> <head> <job> <allowlist> < comments.json
    roborev-waiver-scan.py findings-deferral     <base> <head> <job> <allowlist> <observed-count> \
        < comments.json

`<allowlist>` is a space-separated list of GitHub logins permitted to GRANT. Prints a facts-style
result on stdout (one `key=value` per line, one line per value; see `safe_value`):

    state=granted|unauthorized|stale|malformed|none      (both kinds)
          |count-mismatch                                (findings-deferral only)
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
an author with a body at all. Output values are held to ONE LINE — a newline inside a value would be a
second in-band channel, the class this file exists to close — but INTERNAL PRINTABLE WHITESPACE IS
PRESERVED VERBATIM, because the `reason` is an audit record and the spec promises it is recorded as
given; only line breaks and other control characters are visibly escaped (see `safe_value`).
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

# ===== A SHA FIELD IS EXACTLY 40 HEX, IN BOTH MARKERS (roborev job 229) =====
# The pattern used to admit `{7,40}` while the documented form, `--help`, and every diagnostic say a
# full 40-hex sha. An ABBREVIATED sha therefore MATCHED the pattern and then diverged from the run's
# 40-hex base/head, so it was reported `STALE` — "the marker names a different review" — when the truth
# is `MALFORMED`: the marker names THIS review in a spelling the form does not permit, and an
# authorizer sent to re-check which review they named will not find anything wrong with it. Both kinds
# are tightened together: they share this parser, and a field rule that holds for one marker and not
# the other is a divergence in a channel rule. SAFE BY CONSTRUCTION — abbreviated-to-STALE and
# abbreviated-to-MALFORMED are BOTH non-granting, so no existing authorization's grantability changes,
# only its cause text.
_SHA_FIELD = r"[0-9a-f]{40}"

WAIVE_MARKER = re.compile(
    r"^roborev-waive: prompt-content-absent"
    r" base=(" + _SHA_FIELD + r") head=(" + _SHA_FIELD + r") job=([0-9]+) reason=(.*)$"
)
# ===== A MARKER *ATTEMPT* IS THE STEM PLUS WHITESPACE **OR END OF LINE** (roborev job 228) =====
# The attempt test used to be the stem plus a MANDATORY TRAILING SPACE, so a marker-only comment
# reading EXACTLY the stem — `roborev-defer: findings`, an authorization someone plainly meant to write
# and then truncated — was not recognised as an attempt at all and was reported `NONE` ("no
# authorization exists for this review"). That is a FAIL-QUIET ON AN ATTEMPTED AUTHORIZATION: the
# author re-reads the syntax, sees the prefix they typed, and concludes the mechanism is broken, which
# is the same diagnostic failure MALFORMED exists to prevent. So the STEM is separated from the field
# separator: an ATTEMPT is the stem followed by whitespace or by nothing, and the ONE anchored full
# pattern above — never a second, looser test — decides malformed-ness. The boundary is still TESTED
# rather than dropped: `roborev-defer: findingsfoo` is a different word, not a truncated marker.
WAIVE_STEM = "roborev-waive: prompt-content-absent"
# `issues=` admits ONE OR MORE comma-separated integers, so "non-empty" is a property of the pattern
# rather than a later check that could be forgotten. `count=` is the AFFIRMATIVE half of the binding
# (#3626): the wrapper requires it to equal the OBSERVED findings count, so a marker written before
# its job's findings were read, and any NEW finding arriving at the same head, both fail on it.
DEFER_MARKER = re.compile(
    r"^roborev-defer: findings"
    r" issues=([0-9]+(?:,[0-9]+)*) count=([0-9]+)"
    r" base=(" + _SHA_FIELD + r") head=(" + _SHA_FIELD + r") job=([0-9]+) reason=(.*)$"
)
DEFER_STEM = "roborev-defer: findings"

# ===== A THIRD KIND, FOR THE MERGE GATE, THAT JUDGES AUTHORIZATION AND NOT THE COUNT =====
# `premerge-review-binding.sh` (#3752) must decide whether a job whose RECORD verdict is `F` was
# nonetheless authorized to be deferred, before it may bind that job's range to the merge. It reads
# the SAME marker, in the same channel, under all the same rules — column-zero, sole nonblank
# content, top-level comments only, structured author association, hard-coded allowlist, placeholder
# refusal, scope bound to base AND head AND job.
#
# WHAT IT DELIBERATELY DOES NOT JUDGE, AND WHY THIS IS A SEPARATE KIND RATHER THAN A FLAG ON THE
# EXISTING ONE: the `count=` equality. That half is matched against the findings count OBSERVED BY
# THE REVIEW, and this kind is asked ONLY WHEN NO SUCH COUNT COULD BE DERIVED — so for the caller
# that asks it, there is no measured count to compare against, and the two dishonest ways to
# manufacture one are both refused here: passing a fabricated count would make an affirmative check
# pass on an unmeasured value (the exact shape #3586 forbids), and parsing the count out of the
# marker so it can be compared with itself would be a tautology dressed as a check.
#
# SCOPED TO THIS KIND SINCE #4050, AND IT USED TO BE STATED ABSOLUTELY. The old wording said the
# merge gate "never ran the review" and that enforcing the equality at review time is "where the
# measurement exists" — true of the reviewer, and FALSE as a claim about what the merge gate can
# measure. The job record carries no count FIELD, but it does carry the review TEXT, so the merge
# gate DERIVES the count from it with the same recogniser the wrapper uses and then asks DEFER_KIND,
# count included. Those sentences read as this design's rationale, so leaving them absolute made the
# rationale affirmatively wrong once #4050 landed — the class this repo grades worse than silence.
# See the AUTHZ_KIND note below for what the merge gate now does and when it still falls back here.
#
# So the state is `granted-authorization`, TEXTUALLY DISTINCT from the wrapper's `granted`, and the
# consumer prints it as its own token. Nobody grepping for a full deferral grant can match this. One
# parser, one marker grammar, one new disposition — never a second implementation of a channel rule,
# because a divergence there is an authorization bypass.
#
# WHAT THIS STATE MAY AND MAY NOT BE USED FOR (roborev job 103). It used to be enough to DECLARE the
# unjudged `count=` half in the merge gate's output and bind anyway. It is not: an allowlisted human
# can post a fresh marker AFTER the review carrying any count, so the merge gate was honouring an
# authorization the review-time path would REJECT — a non-invoker, accident-shaped route, i.e. a
# defect under #3312's triage rule and not an out-of-model bypass. `granted-authorization` therefore
# does NOT license a merge: `premerge-review-binding.sh` maps it to UNMEASURED (exit 5). Its remaining
# job is purely diagnostic — separating "no authorization exists" (a measured refusal) from "the
# authorization is good but its count is unverifiable at the merge point", which are different
# operator actions. A caller must never treat it as equivalent to `granted`.
#
# AND SINCE #4050 THE MERGE GATE MOSTLY DOES NOT NEED THIS KIND. Where it can DERIVE the observed
# count from the job record's own recorded review text — with the same recogniser the wrapper uses,
# `scripts/flow/lib/roborev-findings-count.sh` — it asks DEFER_KIND instead, count included, and binds
# only on `granted`. This kind is what it falls back to when no count can be derived, i.e. exactly the
# state in which nothing may be granted. Nothing about THIS kind's judgement changed: it still skips
# the count half and it still may not license a merge on its own.
AUTHZ_KIND = "findings-deferral-authorization"

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
    AUTHZ_KIND: ("state", "author", "scope", "reason", "detail", "issues", "count"),
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
# ONE IMPLEMENTATION, TWO KINDS (#3626): the STEM is a PARAMETER, so the deferral marker inherits this
# rule by CALL. A copy of it for the second kind would be a second place for the channel rule to diverge,
# and a divergence here is an authorization bypass, not a cosmetic difference.


def sole_marker_line(body, stem):
    """The comment's only nonblank line when it ATTEMPTS a marker of this kind, else None."""
    lines = [raw.rstrip("\r") for raw in body.split("\n")]
    nonblank = [line for line in lines if line.strip()]
    if len(nonblank) != 1:
        return None
    line = nonblank[0]
    if not line.startswith(stem):
        return None
    rest = line[len(stem):]
    # WHITESPACE OR END OF LINE, never a mandatory space (roborev job 228; see WAIVE_STEM above).
    if rest and not rest[:1].isspace():
        return None
    return line


PLACEHOLDERS = {
    "why", "todo", "tbd", "tba", "reason", "n/a", "na", "none", "-", "placeholder",
}


# ===== NO EMITTED VALUE CARRIES AN AUTHORIZATION KEYWORD (roborev job 230) =====
# THE SAME GUARANTEE, ONE FIELD OVER — which is why it is fixed as a CLASS and not as a field.
# `judge_reason` refuses a stem-bearing REASON (job 229) and the AUTHOR was left unguarded. An author
# is a GitHub login this process does not control: `unauthorized_detail` interpolated it VERBATIM, and
# that detail reaches `waiver:`/`deferral: UNAUTHORIZED (...)` in the summary block — so the standing
# invariant "no emitted diagnostic carries any part of a marker form, not even its prefix" (#3312 job
# 23) was violated by a login containing `roborev-defer`, which GitHub's login charset permits.
#
# WHAT THE THREAT IS AND IS NOT, recorded so nobody later mistakes this for a closed BYPASS or reopens
# it as one:
#   * IT IS NOT A BYPASS. A GitHub login admits letters, digits and hyphens and NOT colons or spaces,
#     so a login can contain `roborev-defer` but can NEVER contain a full stem (`roborev-defer:
#     findings`). And the emitted line begins `deferral: UNAUTHORIZED (`, which is not a stem, so
#     `sole_marker_line`'s `startswith` test refuses it — verified directly on job 229 with a positive
#     control (a genuine sole-content marker is still recognised, a pasted block is not).
#   * IT IS a spec-conformance and invariant-coverage defect. The invariant is stated ABSOLUTELY, and a
#     rule carrying an exception for "the layers below catch it anyway" decays the next time a layer
#     moves. The tests covered stem-bearing REASONS and not stem-bearing AUTHORS — a property asserted
#     only where it cannot fail is not asserted, the same shape as the `assert_lacks` that was attached
#     to the `NONE` state alone.
# So this is deliberately NOT a security-grade escaping layer and must not grow into one: it is a
# TWO-TOKEN DENYLIST applied at the ONE place every value leaves this process.
#
# FIXED AS A CLASS: every value this tool emits passes through `safe_value` (`emit` walks `EMIT_KEYS`),
# so `state`, `author`, `scope`, `reason`, `detail`, `issues`, `count` — and ANY KEY ADDED LATER, plus
# the argv-sourced `allowlist` that `unauthorized_detail` interpolates — inherit the guard instead of
# each needing its own fix.
#
# REDACTION HERE, REFUSAL IN `judge_reason` — the opposite choice, deliberately. A REASON is
# author-chosen prose with no legitimate need for a stem, so REFUSING it removes the class outright. An
# AUTHOR is an identity this tool must REPORT in order to say who was refused, so it is rendered with
# the keyword neutralised and the rest of the login intact and readable.
#
# DISPLAY ONLY, WHICH IS THE WHOLE SAFETY ARGUMENT. Authorization is decided on the RAW login against
# the allowlist BEFORE anything reaches this boundary (`author not in allowlist` in both judges), and
# nothing downstream re-parses these values as an authorization. So redaction cannot move any verdict,
# and a divergence between this spelling and the shell's sole emit boundary (`roborev_safe_line` in
# roborev-review.sh, which neutralises every block value and every DETAILS line) can only ever redact
# differently — never grant. THAT is why two boundaries are acceptable here where two marker PARSERS
# would not be: a parser decides, a renderer does not.
# A LONGER WORD IS A DIFFERENT WORD — this file's own rule for `roborev-defer: findingsfoo`, applied
# to the renderer. The negative lookahead is LOAD-BEARING, not cosmetic: THIS FILE'S OWN NAME,
# `roborev-waiver-scan.py`, is printed by the shell's fail-closed `waiver: UNAVAILABLE (... tool:
# <path>)` diagnostic, and an operator has to read that path to fix the state. It also keeps
# `judge_reason` from falsely accusing a reason that legitimately names this script. DECLARED
# RESIDUAL: a keyword embedded in a longer word (`roborev-waiverfoo`) is left alone — it carries no
# marker FORM, which needs the keyword, then its kind, then the fields.
MARKER_KEYWORD = re.compile(r"roborev-(?:waive|defer)(?![A-Za-z])", re.IGNORECASE)
MARKER_KEYWORD_REDACTION = "[authorization-keyword-redacted]"


# ===== ONE LINE, BUT THE REASON IS RECORDED VERBATIM (roborev job 229) =====
# The old `collapse()` was `" ".join(value.split())`, which rewrites INTERNAL whitespace — so a reason
# with a tab or with repeated spaces reached `deferral: GRANTED (... reason=...)` altered, while the
# spec, `--help` and the emitted NOTICE all promise the reason is recorded VERBATIM. An authorization
# whose recorded terms are not the terms that were given is a weaker audit trail than it claims to be,
# and the claim is the whole value of the record.
#
# What actually has to hold is narrower than "collapse": the output is `key=value` lines the shell
# reads with `sed -n 's/^reason=//p'`, so a value must not contain a LINE BREAK — that would be a
# second in-band channel, the class this file exists to close. So printable whitespace (space, tab) is
# preserved EXACTLY and only characters that could forge structure or corrupt a terminal are escaped:
# CR/LF become the two-character sequences `\n`/`\r`, and any other control character becomes `\xNN`.
# Escaping is VISIBLE rather than silent, so a reason that contained one is still readable as what it
# was. Leading/trailing whitespace is trimmed (a `reason` is already `.strip()`ed by `judge_reason`,
# and a trailing space in a `key=value` line is invisible noise).
def safe_value(value):
    out = []
    for ch in str(value):
        if ch in (" ", "\t"):
            out.append(ch)
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ord(ch) < 0x20 or ord(ch) == 0x7F or 0x80 <= ord(ch) <= 0x9F:
            out.append("\\x%02x" % ord(ch))
        else:
            out.append(ch)
    # REDACT AFTER ESCAPING, never before: the guarantee is over the text that is actually EMITTED, so
    # the denylist must run over the final rendering rather than over an intermediate one.
    return MARKER_KEYWORD.sub(MARKER_KEYWORD_REDACTION, "".join(out)).strip(" \t")


def emit(kind, result):
    for key in EMIT_KEYS[kind]:
        sys.stdout.write("%s=%s\n" % (key, safe_value(result.get(key, ""))))


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
    # ===== A REASON MAY NOT CARRY EITHER MARKER STEM (roborev job 229) =====
    # A granted reason is interpolated into `waiver:`/`deferral: GRANTED (... reason=...)`, which
    # reaches the summary block — and the block's standing invariant is that NO emitted diagnostic
    # carries any part of a marker form, not even its prefix, because summary blocks get pasted into
    # PR comments as a matter of course (#3312 job 23).
    #
    # THE REAL LESSON, recorded because it cost a review round: the STRUCTURAL assert covers the CODE
    # (no literal marker form in the shell), while a RUNTIME reason can carry one through a channel no
    # source scan can see. The assert and the invariant were treated as the same property and they are
    # not — an invariant over OUTPUT needs a check on the OUTPUT PATH.
    #
    # NOT a security layer, and deliberately not built as one: the sole-content rule refuses a pasted
    # block outright (probed — the emitted text is not recognised as a marker whether pasted whole or
    # embedded in a block, while a genuine sole-content marker still is), so this is spec conformance
    # and invariant coverage. Which makes REFUSAL the right shape rather than escaping: an authorizer
    # has no legitimate need to put a marker stem inside a reason, so the whole class goes away.
    # ONE SPELLING OF THE KEYWORD LIST, shared with the emit-boundary redaction above: a second copy
    # would be a second place for the class to be fixed only halfway, which is exactly job 230.
    if MARKER_KEYWORD.search(reason):
        return ("the marker is missing a-stem-free-reason (the reason names an authorization "
                "marker keyword, and a granted reason is printed in the summary block, which "
                "gets pasted into PR comments — no emitted diagnostic may carry any part of a "
                "marker form; restate the reason without it)")
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


# ============ A PR-BODY LINK CHECK WAS HERE, AND WAS DELIBERATELY REMOVED (#3626) ============
# DO NOT REINSTATE IT. What stood here — `strip_inert_regions()`, `body_references_issue()`, the
# `LOCAL_REF_LEFT`/`LOCAL_REF_RIGHT` token boundaries and the `pr-unlinked` state — required each
# deferred issue number to appear as a LOCAL, VISIBLE `#<N>` reference in the pull-request BODY. Three
# generations of it were built and every one leaked.
#
# THE PRIMARY REASON IT IS GONE IS NOT THE LEAKS — IT IS THE ARTIFACT IT VALIDATED (lead ruling).
# A PR BODY IS EDITABLE AT ANY TIME BY ANYONE WITH WRITE ACCESS, WITH NO PER-EDIT ATTRIBUTION. A
# TOP-LEVEL COMMENT IS PERMANENT AND ATTRIBUTABLE. So the body-link leg was the WEAKER artifact of the
# two, and it would STILL BE THE WEAKER ARTIFACT EVEN IF MARKDOWN PARSED TRIVIALLY: an authorization
# that can be silently rewritten by the constrained party after it is granted evidences nothing. The
# Markdown-recogniser problem below was never the root cause; it was a SYMPTOM of validating against a
# mutable, unattributed artifact.
#
# THE TRAP WAS ALSO IN THE REQUIREMENT'S OWN WORDING (lead): it said the PR must "name WHERE THE
# FINDING WENT", which invited a PROSE SCAN — when the property actually wanted is that the finding is
# TRACKED. Retrievability (`gh issue view <N>`, in roborev-review-oracles.sh) is what enforces
# not-dropped; a sentence in a body never did.
#
# THE MEASUREMENT, so nobody re-derives it: Markdown-handling references in this one predicate went
# 0 -> 11 across two review rounds, and the bypass census did not close:
#   shape                                          round   status when the leg was deleted
#   other/repo#3602 (cross-repository)               R1     closed
#   #3602suffix (missing right token boundary)       R1     closed
#   inside a fenced code block                       R1     closed
#   inside an <!-- HTML comment -->                  R1     closed
#   `#3602` single-backtick code span                R1     closed
#   ``#3602`` MULTI-backtick code span               R2     ACCEPTED (bypass)
#   [#3602](https://example.com) explicit link       R2     ACCEPTED (bypass)
#   4-space indented code block                       -     ACCEPTED (declared residual)
#   GFM autolinks, reference-style [#N][ref], raw
#     HTML, entity refs, nested emphasis              -     unhandled by any generation
# Each fix asked "is this text a LINK or is it INERT?" of a grammar the PR author controls, so the list
# of recognisers never closes — the identical shape #3312 closed by REMOVING a channel rather than
# picking a rarer delimiter, and the identical shape #3229's owner ruling removed: A GUARD WITH KNOWN
# DOCUMENTED FALSE-PASSES IS WORSE THAN NO GUARD, BECAUSE IT INVITES RELIANCE IT CANNOT SUPPORT.
#
# SUBTRACTION CANNOT INTRODUCE A FALSE PASS: with nothing predicted about the body, nothing is excused
# by it. The property is now carried by three legs, none of which reads the PR body — (1) the marker
# NAMES the issue numbers, in a top-level PR comment, from a hard-coded author allowlist, associated by
# `gh --json` structured fields, permanent and attributable; (2) each named issue must be RETRIEVABLE
# via `gh issue view`, four-valued so that "issue absent", "issue closed" and "could not ask" are never
# read as verified; (3) this block RECORDS the numbers, the count, the scope and the reason verbatim.
#
# REINSTATING A BODY SCAN HERE IS REINSTATING GENERATION THREE. If a stronger disposition signal is
# ever wanted, it must come from an IMMUTABLE OR ATTRIBUTED artifact (a structured GitHub relation, or
# the authorization comment itself), never from parsing the mutable body of the PR under review.


def judge_defer_line(line, author, base, head, job, allowlist, observed_count):
    """Return (state, fields) for a line that starts with the findings-deferral prefix.

    THE MATCH IS AFFIRMATIVE, NEVER PERMISSIVE (#3626, on #3586's rule). A grant needs the scope to
    match and the declared `count=` to EQUAL the observed findings count; the caller then requires
    every declared issue to be RETRIEVABLE. Nothing here is derived from the ABSENCE of a contrary
    signal, and there is deliberately NO reconstruction of per-finding identity from the review's
    prose, nor any scan of the PR BODY (see the tombstone above): both are recognisers over
    author-controlled text, the class #3564 and #3312 closed by REMOVING the recogniser.
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
    if observed_count is None:
        # THE AUTHORIZATION-ONLY KIND (AUTHZ_KIND). The count half is SKIPPED, not defaulted and not
        # compared with itself — see the AUTHZ_KIND comment for why the merge gate has no measured
        # count. Everything else above and below still applies, and the grant token differs.
        # Because the count is unjudged, the resulting state must not authorize a merge on its own;
        # since job 103 the merge gate treats it as UNMEASURED rather than declaring the gap.
        if author not in allowlist:
            return "unauthorized", dict(
                fields, detail=unauthorized_detail(author, allowlist, "deferral"))
        return "granted-authorization", fields
    if m_count != observed_count:
        return "count-mismatch", dict(fields, detail=(
            "the marker authorizes %s finding(s) but this job reports %s — the counts must match "
            "EXACTLY, because that equality is the only affirmative evidence that the findings the "
            "authorizer judged are the findings this run observed. A new finding at the same head "
            "raises the observed count and must not ride an older authorization; re-triage, then "
            "re-authorize for the count actually observed" % (m_count, observed_count)))
    # ===== THE DISPOSITION HALF LIVES ENTIRELY IN THE CALLER, AS RETRIEVABILITY =====
    # "The finding is TRACKED" is established by asking GitHub whether each named issue EXISTS (a
    # four-valued `gh issue view` in roborev-review-oracles.sh — an OPEN issue, since a closed one
    # tracks nothing), not by scanning the PR body for a
    # sentence about it — see the tombstone above for why the body scan was removed rather than fixed.
    # AUTHORIZATION IS THE LAST GATE, exactly as it is for the waiver.
    if author not in allowlist:
        return "unauthorized", dict(fields, detail=unauthorized_detail(author, allowlist, "deferral"))
    return "granted", fields


def scan(kind, comments, base, head, job, allowlist, observed_count):
    """The LAST GRANTED marker wins; otherwise the FIRST refusal is reported."""
    granted = None
    first_refusal = None
    # The AUTHORIZATION kind reads the SAME stem as the deferral: it is the same marker, judged on
    # a subset of its fields.
    stem = WAIVE_STEM if kind == WAIVE_KIND else DEFER_STEM
    grant_state = "granted-authorization" if kind == AUTHZ_KIND else "granted"
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
        author = " ".join(str(author).split()) or "unknown"
        body = comment.get("body")
        if not isinstance(body, str):
            continue
        # ONE DECISION, NO PARSE: is the marker the whole comment?
        line = sole_marker_line(body, stem)
        if line is None:
            continue
        if kind == WAIVE_KIND:
            state, fields = judge_waive_line(line, author, base, head, job, allowlist)
        else:
            state, fields = judge_defer_line(
                line, author, base, head, job, allowlist, observed_count)
        if state == grant_state:
            granted = fields
        elif first_refusal is None:
            first_refusal = (state, fields)
    if granted is not None:
        return dict(granted, state=grant_state)
    if first_refusal is not None:
        state, fields = first_refusal
        return dict(fields, state=state)
    return {"state": "none"}


def main(argv):
    kinds = (WAIVE_KIND, DEFER_KIND, AUTHZ_KIND)
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
    if kind == AUTHZ_KIND:
        # NO COUNT ARGUMENT, and passing one is a usage error rather than being ignored: this kind
        # exists BECAUSE the caller has no measured count, so accepting one would invite a
        # fabricated value into the one place this kind is defined not to look at. `None` — not the
        # empty string — is what tells `judge_defer_line` to skip the count half, so an accidental
        # `""` can never be read as "skip".
        if len(argv) != 6:
            sys.stderr.write(
                "usage: roborev-waiver-scan.py findings-deferral-authorization <base> <head> "
                "<job> <allowlist>\n")
            return 2
        observed_count = None
    elif kind == DEFER_KIND:
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
    # NO `body` IS READ, FOR EITHER KIND. Both authorizations are decided from top-level COMMENTS
    # alone — permanent, attributable artifacts — and the PR body is deliberately not evidence for
    # anything here (#3626; see the tombstone above `judge_defer_line`).
    emit(kind, scan(kind, comments, base, head, job, allowlist, observed_count))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
