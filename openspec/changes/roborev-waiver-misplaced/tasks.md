# Tasks — roborev-waiver-misplaced (issue #3759)

Ordered. Each numbered group is independently reviewable; group 1 is the only new mechanism, groups
2–4 wire it, group 5 is doctrine, group 6 is coverage.

## 0. The premise, ALREADY VERIFIED — re-confirm, do not re-derive

Two of these were measured live by the lead on 2026-09-01 and are recorded as facts in `design.md`.
Re-confirm them on the box you implement on (a `gh` upgrade can move either), and STOP rather than
adapt if one is false: the design rests on them.

- [x] 0.1 `scripts/flow/roborev-waiver-scan.py` is **thread-agnostic** — `main()` reads
      `{"comments":[…]}` from stdin and nothing in `scan()`, `judge_waive_line()` or
      `judge_defer_line()` refers to a pull request. **Verified from source.** If it ever becomes
      false, STOP: the whole design rests on one enforcer serving both threads.
- [x] 0.2 `gh issue view <N> --json comments` emits `{"comments":[{"author":{"login":…},"body":…}]}`,
      **byte-identical in shape** to `gh pr view --json comments`. **Measured live on issue #3626.**
      A shape difference would mean a translation layer — a new component in an authorization path,
      needing its own review — or a second scanner, which is forbidden; never an assumption.
- [x] 0.2b `gh pr view 3710 --json closingIssuesReferences` → `[{"number":3544, …}]`. **Measured live:
      the resolver would have found the markers on the actual incident**, where PR #3710 carried zero
      and issue #3544 carried both. Quoted in `proposal.md` as the validating measurement.
- [x] 0.3 Verify that `waiver:` and `deferral:` are **informational** keys — absent from the closed
      verdict grammar scan and from the affirmation loop (`roborev-review.sh` ~`:163`, ~`:896`) — so a
      new value there cannot make anything pass by itself. This is R2's structural argument; if it is
      false, the grammar work has to be designed, not assumed.
- [x] 0.4 Confirm `scripts/agent-gate.sh` is out of scope and will end the change unmodified.

## 1. The linked-issue probe (the only new mechanism)

- [x] 1.1 Add ONE helper to `scripts/flow/roborev-review-oracles.sh` — e.g.
      `roborev_linked_issue_marker_probe <kind> <base> <head> <job> [<observed-count>]` — serving BOTH
      kinds. Two copies of a probe over an authorization channel is two places for it to diverge.
- [x] 1.2 Resolve the linked issues **structurally**: `gh pr view --json closingIssuesReferences`.
      **Never** scan the PR body — #3626 deleted that check because a PR body is editable by anyone
      with write access with no per-edit attribution. Write the *mutable-derived, grants-nothing*
      boundary as a comment AT THE CALL SITE (not only in `design.md`), because the next edit that adds
      a granting consumer will read the code first.
- [x] 1.3 Validate each returned issue number **affirmatively** as digits before it is used or
      emitted; a value that is not a number is a could-not-check cause, never interpolated raw.
- [x] 1.4 Probe each issue in **GitHub's returned order**, **bounded** (a named constant, with the
      bound stated in the rendering when the declared set exceeds it). Report the **first** thread
      carrying a matching marker; stop there.
- [x] 1.2b **The existing `gh pr view --json comments` call stays EXACTLY as it is.** Do NOT fetch
      `--json comments,closingIssuesReferences` in one call and do NOT restructure the existing call
      sites: the payload an AUTHORIZATION is decided from must not change shape as a side effect of
      adding a DIAGNOSTIC (it is the scanner's input, and the fixed measured shape is what licenses
      reusing the scanner unmodified), and the probe must be reachable only from a branch that has
      already failed to grant. The resolver is a **separate, later, best-effort** call issued only on
      the `none` branch — so on every other state the call is **not made**, not merely ignored, which
      is the only version the `gh` invocation-log assert can measure. The extra round-trip on a failing
      run is the accepted cost.
- [x] 1.5 For each probed thread: `gh issue view <N> --json comments`, piped to **the same
      `$WAIVER_SCAN_TOOL`**, with **the same kind** and **the same** `base`/`head`/`job`/
      `$ROBOREV_WAIVER_AUTHORS` (and `observed` for the deferral). No new arguments, no new grammar, no
      second scanner. The scanner file must end this change **unmodified**.
- [x] 1.6 Set the probe's outcome three ways and no more: `misplaced` + the issue number (the scanner
      returned `granted`); `checked` (every probed thread read, no match); `no-subject` (no linked
      issue declared); `could-not-check` + a cause. A partial read — one thread read, another
      unavailable — is `could-not-check` naming BOTH halves, never `checked`.
- [x] 1.7 The helper NEVER returns non-zero and NEVER exits: a two-valued return would re-import the
      collapse this change exists to remove. Every failure is a state with a cause.
- [x] 1.8 Do NOT run the deferral's `roborev_issue_retrievability` disposition leg issue-side. Record
      the scoping in a comment: the rendering claims *"would have been accepted by the channel"*, not
      *"would have granted"*, and the disposition legs run once the marker is on the PR.

## 2. Wire it into the two lookup functions

- [x] 2.1 In `roborev_absence_waiver_lookup()`, after the recognised-state validation, call the probe
      **only** when `ROBOREV_WAIVER_STATE = none`. On a probe `misplaced`, set
      `ROBOREV_WAIVER_STATE="misplaced"` and put the issue number + remedy in
      `ROBOREV_WAIVER_DETAIL`; otherwise leave the state at `none` and record the probe's declaration
      in the detail.
- [x] 2.2 Same in `roborev_findings_deferral_lookup()`, for `ROBOREV_DEFERRAL_STATE` /
      `ROBOREV_DEFERRAL_DETAIL`, passing the observed count through unchanged.
- [x] 2.3 Add `misplaced` to BOTH recognised-state `case` lists, with a comment saying it is a
      **belt**: the probe assigns after the validation today, and the entry exists so a future refactor
      that routes through it cannot rewrite an accurate diagnostic to a generic `unavailable`. State
      that the list is a **recognition** list, not a granting list.
- [x] 2.4 Leave both granting gates exactly as they are —
      `[ "$ROBOREV_WAIVER_STATE" = "granted" ]` / `[ "$ROBOREV_DEFERRAL_STATE" = "granted" ]`,
      token-exact equality — and add no branch that treats `misplaced` as granting, partially granting,
      or grant-with-a-notice.
- [x] 2.5 Confirm no probe call is made on `granted`, `unauthorized`, `stale`, `malformed`,
      `count-mismatch` or `unavailable` — not merely that the state is not overwritten, but that the
      network call is not made. A probe that runs and is ignored is latency and a future footgun.

## 3. The two report arms (`scripts/flow/roborev-review-checks.sh`)

- [x] 3.1 Add a **dedicated** `misplaced)` arm to the `WAIVER_REPORT` case (~`:185-235`), before the
      generic `*)`. The generic arm would render `MISPLACED (detail)` automatically — that is not
      enough: this state's whole value is its **remedy text**, and a remedy is not something to leave
      to a fall-through.
- [x] 3.2 Add the same dedicated `misplaced)` arm to the `DEFERRAL_REPORT` case (~`:565-655`).
- [x] 3.3 Both arms name: the issue number the marker was found on; that it **grants nothing and the
      FAIL stands**; the remedy — **re-post the identical marker as a TOP-LEVEL COMMENT ON THE PR**;
      and that only a marker on the PR grants. No part of either marker stem, and no fillable field
      skeleton — point at `--help`.
- [x] 3.4 Extend both `none)` arms to carry the probe's declaration, from the closed rendering set:
      `linked issue #N checked: no matching marker there either` /
      `linked issues #A,#B checked — N of M declared, probe bounded at N: no matching marker` /
      `no linked issue is declared on this PR, so no linked-issue thread was checked` /
      `the linked-issue thread could NOT be checked: <cause>`. Keep the existing sole-content and
      top-level teaching text; the declaration is additional, not a replacement.
- [x] 3.5 Verify every new interpolation passes through the ONE emit boundary
      (`roborev_safe_line` / `safe_value`) — no per-site escape. Sweep the `gh` diagnostic and the
      issue number specifically.
- [x] 3.6 Verify the block still carries exactly one `RESULT:` line and that no new value spans lines
      (control characters rendered as visible escapes at the boundary).

## 4. Key documentation and `--help`

- [x] 4.1 `scripts/flow/roborev-review.sh` header key documentation: add `MISPLACED (...)` to the
      documented value sets of BOTH `waiver` and `deferral`, saying it is **non-granting** and
      **informational**.
- [x] 4.2 `--help` (~`:487-489`): the *"THE COMMENT MUST BE TOP-LEVEL"* bullet currently names only a
      review body and a review-thread reply. Add the **linked issue thread** as the **most probable**
      misplacement, and name the new `MISPLACED` state and the lead-side verification step.
- [x] 4.3 Confirm no emitted `--help` or diagnostic text became newly pasteable as an authorization
      (the form stays in `--help` only, which is already the one sanctioned location).

## 5. Doctrine, in the same change (issue item 1 + item 3)

- [x] 5.1 `CLAUDE.md`, the **waiver** residual (*"the marker is read from top-level PR comments only,
      so one posted inside a review body or a review-thread reply is silently not applied…"*): add the
      **linked ISSUE thread** as the **most probable** of the three misplacements — that is where
      lane/lead coordination lives — and record the new `MISPLACED` state, that it grants nothing, and
      that the FAIL stands.
- [x] 5.2 `CLAUDE.md`, the **deferral** residual carrying the same sentence: the same correction. Both,
      not one — a residual corrected in one of two places is a residual that reads as correct in the
      other.
- [x] 5.3 Record issue item 3, the **lead-side procedure**: after posting either marker, verify with
      `gh pr view <PR> --json comments` that the marker line is **on the PR**. *A grant is only granted
      once it is readable by the scanner that reads it.*
- [x] 5.4 `scripts/flow/roborev-review-oracles.sh`: the SAME correction in BOTH "RESIDUALS" comment
      blocks (the waiver's ~`:1128` and the deferral's ~`:1345`). These are the artifacts an
      implementer actually reads; leaving them stale is how the doctrine gap regenerates.
- [x] 5.5 Website `agents-developing/roborev-findings/` — same content. **Publication verification is
      POST-MERGE and cannot be done here**: the site is served from `main`, so grepping the served page
      for a new phrase before this branch merges could only ever report `0`, which is the false signal
      the "never by HTTP 200" rule exists to prevent. Record the phrase to grep in the PR body.

## 6. Hermetic coverage (`scripts/tests/test_roborev_review_guard.sh`, gate `tooling-tests`)

Read the existing suite first and reuse its idioms: the `gh` stub (`STUB_GH_COMMENTS`,
`STUB_GH_COMMENTS_JSON`, `STUB_GH_RC`, `STUB_GH_ISSUES`, `STUB_GH_ISSUE_ERR`), `assert_no_marker_form`,
`assert_one_result_line`, and **artifact substitution in a per-case scratch copy of the tree — never a
path variable** (see the `WAIVER_SCAN_TOOL` comment: a test-only seam is one more thing a real invoker
can set).

- [x] 6.1 Extend the `gh` stub for the two new calls: `pr view --json closingIssuesReferences` (a
      fixturable linked-issue list, **defaulting to EMPTY** so a case that wants a probe has to SAY so
      — the fail-closed direction, and it stops a case passing because the double was permissive) and
      `issue view <N> --json comments` (per-issue comment fixtures, independently failable so a
      partial-read case is expressible).
- [x] 6.2 **(a)** A would-have-granted `roborev-waive:` marker on the linked issue, nothing on the PR
      ⇒ `waiver: MISPLACED` naming that issue, `prompt-content:` still FAIL, `RESULT: FAIL`.
- [x] 6.3 **(b)** The same for `roborev-defer:` ⇒ `deferral: MISPLACED` naming the issue,
      `findings: PRESENT (n)` unchanged (never `DEFERRED`, never `NONE`), `RESULT: FAIL`.
- [x] 6.4 **(c)** A **stale** and a **malformed** issue-side marker ⇒ state stays `NONE` with the
      probe's `checked` rendering (R3: escalation only from a would-have-granted marker). Include an
      **unauthorized-author** issue-side marker in the same group.
- [x] 6.5 **(d)** A PR-side `stale` marker WITH a perfect issue-side marker ⇒ stays `STALE`, is not
      overwritten, and — measured, not assumed — **no probe call was made** (assert against the stub's
      invocation log).
- [x] 6.6 **(e)** No linked issue ⇒ `NONE` carrying the declared *"no linked issue is declared on this
      PR"* rendering.
- [x] 6.7 **(f)** Probe unable to run — `gh issue view` fails / the payload is unparseable / the
      relation call fails ⇒ `NONE` carrying the declared *could-not-check* rendering with its cause,
      and the run still FAILs. Plus the **partial-read** case: one thread read, another unavailable ⇒
      `could-not-check` naming both halves, never `checked`.
- [x] 6.8 **(g)** Positive control that `MISPLACED` reaches **no** granting path: `prompt-content:`
      never reads `WAIVED` and `findings:` never reads `DEFERRED` under a `misplaced` state, in either
      kind — paired with a **structural** assert that the only granting gates remain the two token-exact
      `= "granted"` comparisons and that `misplaced` appears in no granting branch. Behavioural alone is
      not enough here (it covers the fixtures someone thought of); structural alone is not enough either
      (it cannot see a granting path built some other way).
- [x] 6.9 **(h)** `assert_no_marker_form` on **every** new diagnostic-emitting case — the MISPLACED
      arms and all four `NONE` renderings — plus a keyword-bearing `gh` diagnostic and a
      keyword-bearing author login on the issue-side path, proving the new values ride the existing
      emit boundary rather than a new one.
- [x] 6.10 A **more-linked-issues-than-the-bound** case: the rendering declares `N of M declared,
      probe bounded at N`, and the unprobed remainder is visible rather than silent.
- [x] 6.11 Structural assert that `scripts/flow/roborev-waiver-scan.py` is **unmodified** by this
      change (`git diff --name-only`), that no `closingIssuesReferences` consumer feeds a granting
      branch, that **no invocation requests `comments` and `closingIssuesReferences` in one call** and
      the pre-existing `--json comments` invocation is unchanged (task 1.2b), and that no PR-**body**
      read was reintroduced anywhere (`--json body`, a `#N` scan).
- [x] 6.12 Assert `scripts/agent-gate.sh` is unmodified.
- [x] 6.13 Run the suite and record the assertion-count delta; then plant the naive mutant for the
      escalation rule (probe on every state / escalate on any issue-side marker) in a scratch copy and
      confirm it REDS — a case that passes against both the real code and its naive form measures
      nothing.

## 7. PR body (for whoever opens the PR — not the implementer's to tick)

- [ ] 7.1 State that the **wrapper cannot certify itself**, that the **live demonstration is
      post-merge**, and that `MISPLACED` is a **diagnosability** change that grants nothing.
- [ ] 7.2 Record the website phrase to grep post-merge (task 5.5).
