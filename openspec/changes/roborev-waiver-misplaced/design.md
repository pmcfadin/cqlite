# Design — roborev-waiver-misplaced (issue #3759)

## The one thing that must not happen, stated first

Every previous round in this subsystem that went wrong went wrong in **one** direction: an
authorization path was made easier to satisfy. So the design constraint that dominates every choice
below is not "make misplacement visible" — that part is easy — it is **make misplacement visible
WITHOUT creating a second granting surface.**

The naive implementation of this issue is a bypass. "Also look at the linked issue's comments" is one
`if` away from "…and grant if you find one there", and the difference between the two is exactly the
property #3312 spent five High-severity rounds defending: **only a marker on the PR grants.** An
issue thread has a different audience, a different retention, and — decisively — a **different
relationship to the review**: the PR is the artifact under gate, and the marker's whole purpose is to
be a permanent, attributable statement *about that gate*.

Hence the shape: **the probe answers a question whose answer is printed and thrown away.** It sets a
state that no granting branch reads.

## Chosen mechanism

```
PR-side scan (unchanged) ──► granted / unauthorized / stale / malformed / count-mismatch / unavailable
                          │        (all six: report as today, probe NOT run)
                          └──► none
                                 │
                                 ├─ resolve linked issues: gh pr view --json closingIssuesReferences
                                 ├─ for each, in GitHub's order, bounded:
                                 │     gh issue view <N> --json comments | <SAME scanner> <SAME kind> <SAME base/head/job/allowlist>
                                 ├─ scanner says `granted` ⇒ state := misplaced, naming #N   (GRANTS NOTHING)
                                 └─ otherwise            ⇒ state stays none, rendering DECLARES what was checked
```

Four properties make this cheap and safe, and each is a deliberate reuse rather than a new mechanism:

**1. The scanner is already thread-agnostic, so there is exactly one enforcer.** It reads
`{"comments":[{"author":{"login":…},"body":…}]}` from stdin and has no concept of a pull request;
`gh issue view --json comments` returns that same shape. So the sole-nonblank-content rule, the
column-zero anchor, the structured author association, the allowlist, the field grammar, the
placeholder refusal and the base/head/job binding are all inherited **by call, with the same
arguments** — not re-derived, not copied. #3626's rule applies verbatim: *a second implementation of a
channel rule is a second place for it to diverge, and a divergence in an authorization rule is a
bypass.* **The scanner needs no change at all**, which is the strongest available evidence that the
channel rules are not being loosened: there is nothing to loosen them in.

**2. The scanner must never EMIT `misplaced`, and the state is therefore set by the shell caller.**
The scanner cannot know which thread its stdin came from — thread identity is the *caller's*
knowledge. Making the scanner report `misplaced` would require telling it where the comments came
from, i.e. adding a provenance argument to an authorization decision, which is a new input to the one
component whose inputs we most want fixed. Keeping the assignment in `roborev-review-oracles.sh` keeps
the scanner's contract exactly as it is: *given these comments, does an authorization for this review
exist in them?*

**3. `waiver:` and `deferral:` are INFORMATIONAL keys.** They are outside the closed verdict grammar
and outside the affirmation loop (`roborev-review.sh:163`, `:896`), so a new value there **cannot make
anything pass by itself** — no grammar entry is needed and none is added. The actual granting gates are
two token-exact equalities, `[ "$ROBOREV_WAIVER_STATE" = "granted" ]` and
`[ "$ROBOREV_DEFERRAL_STATE" = "granted" ]`. `misplaced` is not `granted`. That is the whole of R2's
mechanism, and it is a *structural* argument rather than a behavioural one — which is why the test for
it is structural **and** behavioural.

**4. `misplaced` joins the recognised-state case lists as a BELT, not as a route.** Both lookup
functions validate the scanner's returned state against a closed list and rewrite anything
unrecognised to `unavailable`. The probe assigns `misplaced` *after* that validation, so no list change
is strictly required today — and the list is amended anyway, because a future refactor that routes the
probe result through the validation would otherwise silently rewrite an accurate diagnostic into a
generic `unavailable`, i.e. re-collapse exactly the state this change exists to split out. The list is
a **recognition** list, not a granting list; adding a value to it confers nothing.

## Escalation is only from `none`, and only from a would-have-granted marker

Both halves are the same rule: **`MISPLACED` must mean ONE unambiguous operator action** — *re-post the
identical marker as a top-level comment on the PR.*

**Only from `none`.** A PR-side `stale` says "your marker names a different review"; `malformed` says
"a field is wrong"; `unauthorized` says "this login may not grant"; `count-mismatch` says "re-triage,
the counts differ"; `unavailable` says "the oracle could not be consulted". Each is already specific,
already actionable, and **already correct**. Overwriting any of them with `MISPLACED` would replace a
precise diagnosis with a less precise one and send the operator to move a comment that would still not
grant. There is one and only one state that carries no information — `none` — and that is the one the
probe is allowed to refine.

**Only from an issue-side `granted`.** An issue-side marker that is itself stale, malformed or
unauthorized is **not** a misplacement — it is a *different* defect that happens to be on a different
thread, and re-posting it on the PR would not help. Reporting `MISPLACED` for it would be actively
misleading: the operator moves the comment, the run still FAILs, and the diagnostic that told them to
move it has now spent its credibility. So the escalation condition is exactly *"this marker WOULD have
been ACCEPTED BY THE CHANNEL had it been on the PR"*, and anything else leaves the state at `none`.

**What "accepted by the channel" excludes, declared rather than glossed.** The probe asks the
**scanner's** verdict — every property decidable from the comment itself: shape, sole content,
column-zero anchor, author allowlist, field grammar, reason substance, the base/head/job binding, and
for a deferral the `count=` match against the observed count. It does **not** run the deferral's
**network disposition leg** (`roborev_issue_retrievability` over each `issues=` number) issue-side.
Three reasons, in order of weight:

1. **It cannot produce a false grant.** `MISPLACED` grants nothing, so the worst case is advice that
   is one step short, never a pass.
2. **The remedy is identical either way.** A deferral naming a closed issue, posted on the wrong
   thread, has *two* problems; the first thing to do about it is still to put it on the PR, where the
   disposition leg then runs and reports its own precise `ISSUE-CLOSED` / `ISSUE-ABSENT` /
   `ISSUE-UNVERIFIABLE` state. Two specific steps beat one vague one.
3. **Cost.** Running disposition issue-side adds one `gh issue view` per declared issue per probed
   thread, on a path whose entire output is a diagnostic.

The rendering therefore says what was measured — *"would have been accepted by the channel"* — and
**not** *"would have granted"*, and it names that the disposition legs still apply once re-posted. A
diagnostic that overstates what it measured is the same defect class as a gate log's false rationale:
it is what stops the next person looking.

## The linked issue is resolved STRUCTURALLY, never from the PR body

`gh pr view --json closingIssuesReferences` — the structured GitHub relation. **Not** a scan of the PR
body for `#N`.

This is not a preference. #3626 **deleted** a PR-body link requirement, and the ruling is on the
record: *a PR body is editable at any time by anyone with write access, with NO per-edit attribution,
while a top-level comment is permanent and attributable* — so the body was the **weaker artifact**,
and it would stay weaker **even if Markdown parsed trivially**. Its Markdown recognisers leaked in two
successive rounds (0 → 11 Markdown-handling references in one predicate, with a multi-backtick span
and an explicit `[#N](url)` link accepted at the end), and #3312's rule closes the question: *remove
the shared channel, do not pick a rarer delimiter.* Reinstating a body scan here — for any purpose —
is reinstating a deleted generation.

**The honest caveat, stated because it is the one place this design touches a mutable artifact.**
`closingIssuesReferences` is itself derived from the PR body's closing keywords, so it is
*mutable-derived*: someone with write access can change which issues are linked. That is acceptable
**here and only here**, and the reason is precise: **the result grants nothing.** It selects *which
thread to print a diagnostic about*. The worst an attacker (or an accident) achieves by re-pointing it
is a diagnostic naming the wrong issue, or none — and the run FAILs either way. The moment anything
downstream of this relation could grant, this argument evaporates and the relation must go with it.
That boundary is written at the call site as well as here, because a future edit that adds a granting
consumer will read the code before it reads this file.

**Several linked issues:** probe each, **in the order GitHub returns them**, bounded, and report the
**first** carrying a matching marker. The order is GitHub's rather than a sort, because any sort is a
policy nobody asked for; the bound exists because the probe is a diagnostic and must not become an
unbounded fan-out of network calls on a failing run. When the declared set exceeds the bound, the
rendering **says so** — see the next section.

## The probe declares its own non-exhaustiveness

The probe is **best-effort**: no `gh`, no linked issue, an API error, an unparseable payload, or a
thread whose comments cannot be read all leave the state at `none`. It can never make anything pass,
and it can never make anything fail.

**But "best-effort" must not mean "silent".** This repository's standing rule is that *a lane that
omits coverage silently is indistinguishable from one that covers it*, and its house idioms are the
gate's `0 RECOGNISED` (never a bare `0`) and its `DECLARED GAP` lines. A `NONE` that might or might not
have checked the most likely misplacement location is exactly the two-slot collapse this issue is
about, one level down. So the `none` report carries a **closed set of renderings**:

| rendering | meaning |
|---|---|
| `… (linked issue #N checked: no matching marker there either)` | the probe RAN and read every thread it probed |
| `… (linked issues #A,#B checked — 2 of 5 declared, probe bounded at 2: no matching marker)` | the probe ran, and part of the declared set was NOT reached |
| `… (no linked issue is declared on this PR, so no linked-issue thread was checked)` | there was no subject |
| `… (the linked-issue thread could NOT be checked: <cause>)` | the probe was attempted and did not complete |

**The first rendering is emitted only when EVERY probed thread was read successfully.** A mixed
outcome — issue #A read, issue #B's comments unavailable — takes the could-not-check rendering and
names both halves. This is the same rule as the gate's *"a bare zero in a log reads as a verified
all-clear from a scan that is documented as incomplete"*: a partial scan reported as a complete one is
worse than an admitted failure, because it is the version nobody re-checks.

## Emit-boundary safety

The new detail strings interpolate two externally-sourced values: a **runtime issue number** (from
GitHub's structured payload) and a **`gh` diagnostic** (arbitrary remote text). Both go through the
**existing single emit boundary** — `roborev_safe_line` in the wrapper, `safe_value` in the scanner —
never a per-site escape. That is #3626 round 4's ruling and its generalisation: *the rule is over
EVERY emitted value, not over one field; each process neutralises the keywords at its ONE emit
boundary, never per interpolation site, because a per-site escape is a list to keep complete.*

Concretely: **no emitted diagnostic may carry any part of either marker stem** (`roborev-waive`,
`roborev-defer`), and the new cases are run through the existing `assert_no_marker_form` helper —
attached to *every* diagnostic-emitting case, because *a property asserted only where it cannot fail
is not asserted* (that helper exists because the MALFORMED detail leaked the whole marker form for a
whole release while a nearby comment denied it). The issue number is additionally validated as digits
before use: an affirmative shape test on a value from a remote payload, not a hope.

## Alternatives rejected

| Alternative | Why not |
|---|---|
| Let a marker on the linked issue GRANT | The entire security property of the channel is *only a marker on the PR grants*. This is the issue's own explicit scope note. |
| Grant on the issue thread "with a warning" | A warning nobody must act on is a grant. There is no half-granting state, and inventing one is how the next five rounds get spent. |
| Report `MISPLACED` for a stale/malformed issue-side marker | `MISPLACED` would stop meaning one operator action; the operator moves the comment, the run still FAILs, and the diagnostic has spent its credibility. |
| Overwrite a PR-side `STALE`/`MALFORMED` with `MISPLACED` | Replaces a precise diagnosis with a vaguer one. The only state carrying no information is `none`. |
| Resolve the linked issue by scanning the PR body for `#N` | #3626 deleted exactly this: a mutable, unattributed artifact, with a Markdown-recogniser class that provably does not close (0 → 11 references, two bypasses accepted at deletion time). |
| Teach the scanner a `--thread-kind` argument and let it emit `misplaced` | Adds a provenance input to the one component whose inputs must stay fixed; the scanner's thread-agnosticism is what makes there be exactly one enforcer. |
| Copy the channel rules into a new issue-side scanner | A second implementation of a channel rule is a second place for it to diverge, and a divergence in an authorization rule is a bypass. |
| Scan *every* issue and PR the marker's author has commented on | Unbounded, and it makes "which thread" meaningless — the diagnostic's value is that it names ONE place to move a comment from. |
| Run the deferral's disposition leg issue-side too | N extra network calls on a diagnostic path, for advice that is the same either way; and the disposition state is reported precisely once the marker is on the PR. Declared as a scoping, not hidden. |
| Fail the run when the probe cannot be performed | The probe is a diagnostic. Failing on it would make an unreachable GitHub API a merge blocker for a reason unrelated to the review, and would red on correct input — the guard agents learn to waive. |
| Fix it in doctrine only | Doctrine item 1 is done here too and is worth doing, but a residual paragraph is what was already there and did not name the linked issue. The eight-hour stall happened *with* a documented residual. |
| Have the lead's tooling verify placement at post time | Adopted as procedure (issue item 3, `gh pr view <PR> --json comments` after posting) and recorded in doctrine — but a procedure is not a diagnostic. The wrapper must still be able to say what it found. |

## Threat model

Inherited verbatim from #3312/#3626, because the adversary is identical, and **this change does not
move the boundary**:

- **A hostile invoker is OUT OF SCOPE by construction.** Whoever runs the wrapper can edit it, shadow
  `gh` on `PATH`, or hand-write a summary block. No check inside a process defends against the party
  that controls the process.
- **What is defended:** (1) parties who do not control the invocation — this is a public repository and
  a failing block prints base/head/job; (2) accident and drift, the larger category, of which *this
  issue is a textbook instance* — nobody attacked anything, a lead posted in the wrong thread.
- **Triage rule:** *"the INVOKER can bypass this"* ⇒ out of model, record it; *"a NON-INVOKER can
  bypass this"* or *"this can be bypassed BY ACCIDENT"* ⇒ defect.

**Applied to the new surface:** the only new inputs are `closingIssuesReferences` (mutable-derived,
grants nothing — see above) and a linked issue's comment list (public, anyone can post there). A
stranger commenting a perfect marker on the linked issue achieves exactly one thing: the run prints
`MISPLACED` naming their comment, and still FAILs. That is not a bypass; it is at worst noise, and it
is noise an allowlisted-author check already filters, since the probe runs the same allowlist.

**Residuals, named rather than left to be rediscovered:**

- The marker is still read for GRANTING purposes from **top-level PR comments only**. A marker inside
  a **review body** or a **review-thread reply** remains silently not applied — the probe does not read
  those either (`gh pr view --json comments` returns top-level comments), so those two locations stay
  `NONE`. They are named in doctrine beside the linked issue, which is now named as the **most
  probable** of the three.
- A marker on an issue that is **not linked** to the PR is not found. The relation is what makes the
  probe bounded and attributable; scanning arbitrary threads is the rejected alternative above.
- More linked issues than the bound leaves part of the declared set unprobed — **declared in the
  rendering**, never silent.
- An authorized human can still authorize carelessly. Nothing mechanical detects that; the control is
  the permanent attributable comment.

## Demonstration, and why it is post-merge

A PR whose subject is how the wrapper reads authorizations **cannot certify itself**: the review of
this branch is performed by the wrapper as it exists on the branch, and the new probe path cannot be
demonstrated by the same run it is meant to inform. Coverage splits the way #3626's did:

- **Pre-merge, hermetic:** `scripts/tests/test_roborev_review_guard.sh` (already executed by the
  gate's `tooling-tests` component) gains a case for the grant-shaped issue-side marker for **both**
  kinds, for each `none` rendering, for the two non-escalation rules, for the probe failure path, for
  the no-granting-path positive control, and for marker-form absence — each planting artifacts in its
  **own scratch copy of the tree**, never a path variable, because a test-only seam is one more thing a
  real invoker can set.
- **Post-merge, live:** a recorded probe on a real PR with a real linked issue, following the existing
  "recorded live probe" precedents. Stated in the PR body.
