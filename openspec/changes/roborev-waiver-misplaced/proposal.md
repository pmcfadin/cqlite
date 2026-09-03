# roborev-waiver-misplaced — issue #3759

## Why

**A correct authorization posted one thread over is indistinguishable from a refusal**, and that cost
PR #3710 — position 1 of a six-PR serial queue — roughly **eight hours of idle time**, blocking five
lanes behind it.

The coordination lead granted **both** markers for PR #3710 — field-perfect, verified base/head/job,
each as the sole nonblank content of its own comment, from an allowlisted author — on **issue
#3544**, the thread where that lane's coordination happens all day. The scanner reads top-level **PR**
comments only (`roborev-review-oracles.sh:1128` states it; the call itself is at `:1260`). Measured on
the record while the PR sat unarmed:

```
PR #3710 comments:  roborev-waive: 0   roborev-defer: 0
issue #3544:        both markers present, correct in every field
```

So `--recheck-job 317` would have reported `waiver: NONE` / `deferral: NONE`, the `prompt-content`
FAIL would have stood, and **the lane's natural reading of `NONE` is "the lead refused"** rather than
"the comment landed on the wrong thread".

Two lanes found it independently, and both behaved correctly: `lane-3544` validated the markers
field-by-field, confirmed 0 on the PR / 2 on the issue, and **declined to copy them across itself** —
on a shared login it could have made its own merge gate pass, and correctly identified that as the
exact bypass the channel exists to prevent. `lane-3650` reached the same conclusion from outside while
diagnosing why position 1 had not moved, verifying the read path from source rather than from memory.
Nothing in the tooling misbehaved. **The tooling was silent about the one fact that would have ended
the stall in a minute.**

**The doctrine gap is the same fact.** CLAUDE.md's recorded residual names two misplacement locations
— *"inside a review body or a review-thread reply"* — and **does not name the linked ISSUE thread**,
which is the **most probable** of the three, because that is where lane/lead coordination lives. A
lead working an issue thread all day posts there by default. This is not an exotic mistake; it is the
path of least resistance.

**The class is a familiar one.** `NONE` today covers "never granted", "granted and misplaced" and
"granted for a different scope", and the **operator action differs for each**. That is a multi-state
reality collapsed onto two slots where **the discarded value is the diagnostic one** — the shape the
three-valued findings on #3229 collected repeatedly, and the shape `dead-lanes`, `roborev_issue_retrievability`
and the gate's `0 RECOGNISED` idiom all exist to avoid. Splitting out the one misplacement that is
**mechanically detectable** costs a second `gh` call.

## What changes

A new, **distinct, NON-GRANTING** state — **`MISPLACED`** — for **both** authorization kinds.

When (and only when) the PR-side scan for a kind returns `none`, the wrapper additionally scans the
**top-level comments of the PR's linked issue(s)** with the **same scanner, the same kind and the same
base/head/job/allowlist arguments**. If that scan returns `granted`, the state becomes `misplaced`,
reported as `waiver: MISPLACED (…)` / `deferral: MISPLACED (…)`, naming **the issue number it was
found on** and the remedy: **re-post the identical marker as a top-level comment on the PR**.
Otherwise the state stays `none` — and the `none` report now **says whether the probe ran**, so
`NONE` is never silently ambiguous about it.

**The resolver is validated against the actual incident.** Measured live, on the PR that stalled:

```
$ gh pr view 3710 --json closingIssuesReferences
[{"number":3544, …}]
```

The markers were on **issue #3544**; **PR #3710 carried zero**. So the proposed
`closingIssuesReferences` resolver **would have found them on the real case** — one structured call, no
prose scan, no inference. That is the whole eight-hour stall, resolvable by a diagnostic that names
issue #3544 and says "re-post it on the PR".

**This is a diagnosability change, not a loosening.** `MISPLACED` grants nothing, anywhere. The
`prompt-content:` / `findings:` FAIL stands unchanged, and the author allowlist, the
sole-nonblank-content rule, the column-zero anchor and the base+head+job binding are all untouched.
The security property — **only a marker on the PR grants** — is preserved exactly; what is removed is
the failure mode where a correct authorization reads as a denial.

## Scope fences (owner "Proposed" section, issue #3759; lead brief 2026-09-01)

- **Subject is `scripts/flow/roborev-review-oracles.sh`** (the two lookup functions and their two
  RESIDUALS comment blocks), **`scripts/flow/roborev-review-checks.sh`** (the two report arms),
  `scripts/flow/roborev-review.sh` (`--help` and the key documentation), and
  `scripts/tests/test_roborev_review_guard.sh`.
- **`scripts/flow/roborev-waiver-scan.py` is reused UNMODIFIED, and that is a measured design result,
  not luck.** The scanner is already **thread-agnostic**: it consumes
  `{"comments":[{"author":{"login":…},"body":…}]}` on stdin and knows nothing about pull requests.
  **Measured live on issue #3626, `gh issue view <N> --json comments` emits that same document,
  byte-identical in shape to `gh pr view --json comments`.** So the channel rules are inherited **by
  call** — one enforcer, one grammar, one allowlist — which is #3626's *reuse, do not reinvent* ruling
  verbatim: *a second implementation of the marker grammar is a second place for it to diverge, and a
  divergence in an authorization grammar is a bypass.* The scanner file ends this change untouched, and
  the guard suite asserts it.
- **The existing `gh pr view --json comments` call is UNCHANGED and the two calls stay separate.** The
  resolver is a second, later, best-effort call made **only** on the `none` branch — never a combined
  `--json comments,closingIssuesReferences` fetch, and no restructuring of the existing call sites. The
  payload an *authorization* is decided from must not change shape as a side effect of adding a
  *diagnostic*, and the probe must be reachable only from a branch that has already failed to grant.
- **`scripts/agent-gate.sh` is NOT touched.**
- **The design is fixed by the owner's numbered "Proposed" section.** This change specs it; it does
  not redesign it.
- **The wrapper cannot certify itself.** A PR whose subject is how the wrapper reads authorizations
  cannot demonstrate that reading on its own review — the same shape as roborev reading
  `exclude_patterns` from the root checkout, and as `required` reading its registry from the base ref.
  The live demonstration is planned **post-merge** and is stated as such in the PR body.

## Non-goals

- **No route by which an issue-thread marker grants anything.** Not partially, not with a warning,
  not "for the waiver but not the deferral". `MISPLACED` is a diagnostic.
- **No PR-body parsing.** #3626 **deleted** a PR-body link check because a PR body is editable at any
  time by anyone with write access with no per-edit attribution, while a comment is permanent and
  attributable. Reinstating a body scan is reinstating a deleted generation.
- **No escalation from a specific PR-side state.** `stale` / `malformed` / `unauthorized` /
  `count-mismatch` / `unavailable` are already specific and actionable and are never overwritten.
- **No new channel, flag, file or env var** — the constrained party must not choose its own enforcer.
- **No change to the closed verdict grammar.** `waiver:` and `deferral:` are **informational** keys,
  outside the verdict scan and outside the affirmation loop, so a new value there cannot make anything
  pass by itself. The change adds a cause, not a verdict.
- **No probe that can fail a run.** A missing `gh`, no linked issue, an API error or an unparseable
  payload leaves the state exactly where it was.
