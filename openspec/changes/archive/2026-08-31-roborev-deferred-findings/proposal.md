# roborev-deferred-findings — issue #3626

## Why

Two doctrine rules are in direct tension, and five lanes hit it in one night (#1490, #1707, #3515,
#3549, #3473):

1. *"any non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and a
   blocked merge, never 'roborev clean'."*
2. The nit rule: *"Nits never trigger a re-verify round: batch all of a PR's nits into ONE linked
   follow-up issue at merge time."*

**Deferring a finding does not stop roborev reporting it.** Once a lead defers a finding — as a nit,
as a batched follow-up, or by explicit ruling — every subsequent round re-reports it,
`findings: PRESENT (n)` persists, the terminal `RESULT` stays `FAIL`, and rule 1 blocks the merge
forever. Neither escape hatch applies: the absence waiver excuses `prompt-content` **absence only**
(#3312, by design), and `--recheck-job` cannot help because since **#3586** `findings:` must be
affirmatively `NONE` for a `PASS` — a correct recheck of a findings-bearing job re-reports `FAIL`.
The tooling is behaving properly; the doctrine is unobtainable.

The measured instance is PR #3572, roborev job 262 (`c83d2092..daaca3e93`): `findings: PRESENT (2)`
with **zero new** findings — both were #3602 and #3613, already filed and already lead-deferred —
5,937,937 input / 5,703,168 cached tokens (~317× the vacuous baseline, the largest of 21 rounds),
and every deterministic key PASS. The merge required an out-of-band lead authorization comment.

The lane whose behaviour this change exists to protect is **#3515's**: it refused to arm `--auto`
over a `RESULT: FAIL`, refused to fix the deferrals to manufacture a green, refused a waiver that
does not apply, and asked the owner instead. That is exactly the discipline we want, and today its
reward is a stall. **A rule that punishes the correct behaviour will not survive contact.**

## What changes

Redefine **"roborev clean" as NO UNADDRESSED FINDINGS**, not "the tool printed zero", and make the
distinction mechanical rather than a matter of lead memory.

A second authorization marker — `roborev-defer:` — travels the **same channel as the absence
waiver** (top-level PR comment, column-zero, sole nonblank content, hard-coded author allowlist,
structured `gh --json` author parsing, `--recheck-job` application) and names **specific issue
numbers** plus the **finding count** it covers. When it is granted and affirmatively matched,
`findings:` reports a **distinct `DEFERRED (n, …)` token** — never `NONE` — and the terminal verdict
is gated on the **undeferred** set only.

## Scope fences (owner/lead, issue #3626)

- **Subject is `scripts/flow/roborev-review.sh`** and its siblings (`-oracles.sh`, `-checks.sh`,
  `roborev-waiver-scan.py`) plus `scripts/tests/test_roborev_review_guard.sh`.
- **`scripts/agent-gate.sh` is NOT touched** — #3544, #3473 and #3402 are live on it, and #3574's
  landing conflicted three PRs that had never rebased.
- **This is about roborev's verdict only.** It does not become a general "override any check"
  mechanism, and the gate of record is unaffected.
- **The wrapper cannot certify itself.** A PR whose subject is how the wrapper reads authorizations
  cannot demonstrate that reading on its own review — the same shape as roborev reading
  `exclude_patterns` from the root checkout and `required` reading its registry from the base ref.
  The live demonstration is planned **post-merge** and is stated as such in the PR body.

## Non-goals

- No `--defer-finding` flag, no deferral file in the worktree, no env var. Each hands the
  constrained party the power to satisfy its own constraint (#3312: *the constrained party must not
  choose its own enforcer*).
- No blanket "ignore findings" switch, at any scope.
- No collapsing of `prompt-content` and `findings`. A delivery-artifact waiver may never excuse a
  real defect, and a findings deferral may never excuse an absent prompt.
- No change to what counts as a finding, or to how roborev reviews.
