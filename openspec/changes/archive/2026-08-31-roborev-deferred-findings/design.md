# Design — roborev-deferred-findings (issue #3626)

## The trap, stated first, because it is the whole design problem

The obvious fix — let a lane mark findings deferred so the tool passes — **hands the constrained
party the power to satisfy its own constraint.** That is the shape #3312 spent four High-severity
rounds on, and its conclusion is binding here:

> **CONTROL AND DATA MUST NOT SHARE A CHANNEL WHEN THE DATA IS ATTACKER-CONTROLLED** … the fix is to
> REMOVE the shared channel, **not to choose a rarer delimiter.**

And its corollary, learned the expensive way in the same issue: **the constrained party must not
choose its own enforcer.** So a `--defer-finding` flag, a deferral file in the worktree, and an env
var are all non-starters — a worker could clear its own findings. The authorization must live
somewhere the worker cannot write in its own name.

## Chosen mechanism: a second marker on the absence waiver's channel

**Reuse, do not reinvent.** The absence waiver is the studied precedent, and five recogniser
generations were tried and superseded before its class closed (marker-anywhere → own-line →
skip-fenced → fence-state-tracking → **sole nonblank content**). Every one of the first four asked
*"is this line DATA or CONTROL?"* of a grammar the comment author controls, which has unbounded ways
to say "this is data". The sole-content rule removes the shared channel and is decidable without
parsing anything. **We inherit that rule rather than re-deriving it**, and we inherit it by calling
the same enforcer, not by copying its logic — a second implementation of a channel rule is a second
place for it to diverge.

**Marker grammar** (one anchored pattern, reason trimmed before it is judged, exactly as the waiver's
is):

```
roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex> head=<40-hex> job=<id> reason=<why>
```

Inherited properties, each for the reason #3312 established:

| Property | Why |
|---|---|
| Top-level PR comment only | review-body / thread replies are not read; fail-closed |
| Column-zero, **sole nonblank content** of the comment | no quoting construct can be the only thing in a comment (§ above) |
| Hard-coded author allowlist (`ROBOREV_WAIVER_AUTHORS`) | a public repo prints base/head/job in the failing block; without it any commenter grants |
| Structured `gh --json` author parsing | author and body stay separate FIELDS — no in-band delimiter to forge |
| Bound to `base` **and** `head` **and** `job` | the authorizer judged ONE review; a push, a different base, or a re-run each need a fresh authorization |
| Applied via `--recheck-job <id>`, enqueueing nothing | the operator only learns the job id and the findings FROM the finished run; a re-run would enqueue a different job and stale the marker instantly (#3312 job 24 — without this the break-glass is a dead letter) |
| Placeholder reasons refused; no emitted diagnostic carries any part of the marker | the artifact must not become the credential (#3312 job 23) |
| Enforcer resolved from the wrapper's own directory, no override, no `${…:-…}` fallback | *the constrained party must not choose its own enforcer* (#3312 job 27) |

**A distinct marker keyword, not an extended waiver.** `roborev-waive: prompt-content-absent` and
`roborev-defer: findings` are separate markers producing separate keys and separate tokens. Neither
reads the other's. This is the issue's third constraint — collapsing them would let a
delivery-artifact waiver excuse a real defect — and it is cheaper to keep separate than to
re-separate later.

## Affirmative matching: why `count=` exists

The binding constraint is *"never derive `PASS` from the absence of a bad signal"* (#3586): a
deferred finding must be **affirmatively matched**, not merely unlisted. Two facts shape what is
achievable:

- The **job binding already fixes the finding set.** A job is a completed review; its findings do
  not change. The authorizer who names `job=262` saw exactly that job's findings.
- roborev's findings are **prose**, and the count is what the wrapper can establish
  affirmatively. There is no structured per-finding identity to match an issue number against, and
  building a recogniser over author-controlled prose to manufacture one is the exact class #3564
  closed by *removing* prose reconstruction. We do not reopen it.

So the authorization is **self-describing and cross-checked**: the authorizer states both the issue
numbers and the count, and the wrapper requires the **observed count to equal the declared `count=`**
and the declared `issues=` list to be **non-empty**. Consequences, all deliberate:

- A **pre-authorization** (a marker written for a job before its findings were read, the residual
  #3312 names) fails on a count mismatch instead of passing silently.
- **Any new finding** at the same head raises the observed count → mismatch → `PRESENT` → `FAIL`.
  The undeferred set is what gates the verdict, and this is how "undeferred" is computed without a
  per-finding identity that does not exist.
- `count=` is **not** load-bearing alone. It is the affirmative half of a binding whose primary is
  `job=`; neither substitutes for the other.

**What the deferral does NOT relax:** `findings: UNKNOWN` and `findings: SKIP` remain fail-closed
and are **not** deferrable. `UNKNOWN` means the findings state was never established — we cannot
count what we cannot see, so a deferral over it would be precisely "a pass resting on a state we
could not read". Only an affirmatively measured `PRESENT (n)` is deferrable.

## Where the finding went: the disposition requirement

The issue requires a finding be *addressed*, and a deferral naming an issue that does not exist is a
dropped finding wearing a link. So each `issues=` number must be:

1. **An OPEN GitHub issue, asked FOUR-VALUED.** `present` (a payload affirmatively naming that
   number) is the only state that permits a grant. `absent` — GitHub answered that it does not exist —
   is `ISSUE-ABSENT`. `closed` — GitHub answers, and exits 0, with a `CLOSED` state — is
   `ISSUE-CLOSED`; that state is why the check reads `state` at all, and why it is DELIBERATELY
   STRONGER than the lead's literal "retrievable" condition: a closed-as-duplicate issue is
   retrievable and tracks nothing, so accepting it would contradict the not-dropped property every
   statement of this leg claims. `could-not-ask` — no `gh`, no auth, a network/API failure, an
   unparseable payload, or **any diagnostic that does not say the issue is missing** — is
   `ISSUE-UNVERIFIABLE`.
   `gh issue view` **exits 1 for both** (measured on gh 2.98.0: `GraphQL: Could not resolve to an
   issue or pull request with the number of N.` vs `HTTP 401: Bad credentials`), so an exit-code-only
   test is the two-valued predicate that always picks the permissive answer, and it would grant over
   issues nobody confirmed exist. The verdict therefore comes from the diagnostic, unrecognised ⇒
   could-not-ask, and the two non-granting states are textually distinct because they are **different
   operator actions**.
2. **Citable to a ruling** — satisfied by the authorization comment itself, which is permanent,
   attributable, and in the PR. There is no separate ruling artifact to hunt for.

Both are recorded in the block, so the disposition of every deferred finding is legible from a pasted
summary alone.

### A PR-body link was ALSO required, and that leg was DELETED (lead ruling, option A)

The first two revisions of this design required each `issues=` number to appear as a **local, visible
`#N` in the PR body** (`PR-UNLINKED` otherwise). It is gone, and the reason is not the bypasses:

**A PR body is editable at any time by anyone with write access, with NO per-edit attribution. A
top-level comment is permanent and attributable.** So the body-link leg was the **weaker artifact**,
and it would stay weaker **even if Markdown parsed trivially** — an authorization the constrained
party can silently rewrite after it is granted evidences nothing. The recogniser problem was a
symptom. The wording invited it too: *"name where the finding went"* invited a **prose scan**, when
the property wanted is that the finding is **TRACKED**, which the issue-state leg enforces.

The bypass census, kept because it is the evidence the class does not close (Markdown-handling
references in that one predicate: **0 → 11**):

| shape | round | status when the leg was deleted |
|---|---|---|
| `other/repo#3602` cross-repository | R1 | closed |
| `#3602suffix` | R1 | closed |
| fenced code block | R1 | closed |
| `<!-- #3602 -->` HTML comment | R1 | closed |
| `` `#3602` `` single-backtick span | R1 | closed |
| ``` ``#3602`` ``` multi-backtick span | **R2** | **ACCEPTED (bypass)** |
| `[#3602](https://example.com)` explicit link | **R2** | **ACCEPTED (bypass)** |
| 4-space indented code block | — | ACCEPTED (declared residual) |
| GFM autolinks, `[#N][ref]`, raw HTML, entity refs, nested emphasis | — | unhandled by any generation |

#3312 (*remove the shared channel, do not pick a rarer delimiter*) and #3229's owner ruling (*a guard
with known documented false-PASSes is worse than no guard, because it invites reliance it cannot
support*) both apply. **Subtraction cannot introduce a false PASS**: with nothing predicted about the
body, nothing is excused by it. Any future strengthening must come from an **immutable or attributed**
artifact — a structured GitHub relation, or the authorization comment itself — never from parsing the
mutable body of the PR under review. Reinstating a body scan is reinstating generation three.

## Reporting: a distinct token, and a key that speaks when nothing was granted

```
findings: DEFERRED (2, issues=#3602,#3613, authorized @<login>, job 262)
deferral: GRANTED (author=@<login> issues=3602,3613 count=2 scope=base=<…> head=<…> job=262 reason=<…>)
RESULT: PASS
```

`findings:` **never reports `NONE`** for a deferral — `NONE` stays reachable only from the record's
structured `verdict` letter, so nobody grepping for a clean review finds a deferred one. `DEFERRED`
is a **new value in the closed verdict grammar**, non-failing **only** when the deferral oracle
affirmatively granted; an unrecognised or ungranted spelling still FAILs. The admission is
**confined to the `findings:` key by key name** — the scan carries each key's NAME beside its value —
so the deterministic-key affirmation backstop carries **no** `DEFERRED` arm and does not read the
coupled state at all; a deterministic key holding the token fails in the scan, by key name, under its
own diagnostic. Confining it by KEY rather than by PROVENANCE ALONE is the difference between the two
authorizations: a waiver authorizes a PROPERTY (an absence) only one key can report, while a deferral
authorizes a NAMED SET OF FINDINGS and says nothing about whether the reviewer's diff arrived — so an
unconfined admission would let one authorization excuse a check nobody authorized, prevented only by
the accident that no other key emits the token.

`deferral:` states its own state even when nothing was granted — `NONE` / `STALE` / `MALFORMED` /
`UNAUTHORIZED` / `COUNT-MISMATCH` / `ISSUE-ABSENT` / `ISSUE-CLOSED` / `ISSUE-UNVERIFIABLE` /
`UNAVAILABLE` — each
leaving the FAIL in place. This is the `waiver:` key's lesson: *"your marker names the wrong job"*
and *"there is no marker"* are different operator actions, and a bare FAIL distinguishes neither.
Per #3312's own finding, a **marker-only comment with bad fields is `MALFORMED`; a comment with other
content is ignored SILENTLY** (`NONE`), never reported malformed — someone documenting the form (this
repository's threads do) never attempted an authorization, and a false accusation reprinted on every
later run is worse than silence.

`MODE: recheck` already declares itself as the first key, so a deferred `PASS` can never be pasted
as evidence of a fresh clean review.

## Threat model, and the triage rule that comes with it

Inherited verbatim from #3312, because the adversary is identical:

- **A hostile invoker is OUT OF SCOPE, by construction.** Whoever runs the wrapper can edit it,
  shadow `gh` on `PATH`, or hand-write a summary block into the PR. No check inside a process defends
  against the party that controls the process. The protection against a hostile *worker* is the audit
  trail plus a human reading the PR.
- **What it DOES defend:** (1) parties who do not control the invocation — this is a public
  repository, anyone can comment, and a failing block *prints* base/head/job; (2) accident and drift,
  the larger category in practice — a pasted block, a quoted example, a stale marker riding to a
  later review, a re-run inheriting an authorization meant for another job, an unsubstituted
  placeholder.
- **Triage rule:** *"the INVOKER can bypass this"* ⇒ out of model, record it, do not patch it.
  *"a NON-INVOKER can bypass this"* or *"this can be bypassed BY ACCIDENT"* ⇒ defect.

**Residuals, named rather than left to be rediscovered:** the marker is read from top-level PR
comments only, so one posted inside a review body or thread reply is silently not applied (the run
reports `deferral: NONE` and the FAIL stands — fail-closed, but it reads as "my authorization was
ignored"); and an authorized human can authorize carelessly — pre-authorizing, or deferring without
checking that the findings really are the tracked ones. Nothing mechanical detects either; the
control is the permanent attributable comment, which is why a substantive reason is required and
recorded verbatim.

## Alternatives rejected

| Alternative | Why not |
|---|---|
| `--defer-finding <issue>` CLI flag | the constrained party writes its own authorization |
| A `.roborev-deferrals` file in the worktree | same, plus it is a config the daemon-vs-root class already bit three lanes on |
| Env var (`ROBOREV_DEFER_ISSUES`) | same, and #3312 job 27 is exactly "the enforcer was made env-settable" |
| Extend `roborev-waive:` with a `findings-deferred` scope | violates the separate-scoping constraint; one marker excusing two unrelated causes is one mistake away from excusing both |
| Suppress findings whose text matches a filed issue's title | a recogniser over author-controlled prose — the class #3564 closed by removing prose reconstruction |
| Let a lead paste a summary block as the authorization | #3312 job 23: the artifact becomes the credential |
| Loosen `job=` to base+head only | reopens the hole where one comment waives a later *vacuous* review at the same head |

## Demonstration, and why it is post-merge

A PR whose subject is how the wrapper reads authorizations **cannot certify itself**: the review of
this branch is performed by the wrapper as it exists on the branch, and any claim that the new
authorization path works must not rest on the same run it is meant to gate. Coverage is therefore
split:

- **Pre-merge, hermetic:** `scripts/tests/test_roborev_review_guard.sh` (already wired into the gate
  as `tooling-tests`) gains cases for every grant and every refusal state, each planting its artifact
  in its **own scratch copy of the tree** — never a path variable, because a test-only seam is one
  more thing a real invoker can set.
- **Post-merge, live:** a recorded probe on a real findings-bearing PR, following the precedent of
  the existing "recorded live worktree probe" and "recorded live probe of the narrowed exclusion"
  requirements. This is stated in the PR body.
