# Tasks — roborev-deferred-findings (issue #3626)

## 1. Enforcer: extend the existing scanner, do not add a second one
- [ ] 1.1 Teach `scripts/flow/roborev-waiver-scan.py` a second marker kind, `roborev-defer: findings`,
      reusing its sole-nonblank-content, column-zero, top-level-comment and structured-author logic
      **by call, not by copy** — a second implementation of a channel rule is a second place for it to
      diverge.
- [ ] 1.2 Parse and validate the fields: `issues=` (non-empty, comma-separated integers), `count=`
      (integer), `base=`/`head=` (40-hex), `job=`, `reason=` — one anchored pattern, field order
      enforced, `reason` trimmed **before** it is judged, placeholders (`why`/`todo`/`tbd`, any
      unsubstituted `<…>`) refused.
- [ ] 1.3 Emit a structured record the wrapper consumes, distinguishing `granted` from each refusal
      state; a marker-only comment with bad fields is `MALFORMED`, a comment with other content is
      silently `NONE`.
- [ ] 1.4 Keep the allowlist (`ROBOREV_WAIVER_AUTHORS`) hard-coded in `roborev-review-oracles.sh`,
      not env-derived and not config-derived; the deferral uses the same list.

## 2. Wrapper: scope binding, affirmative matching, disposition
- [ ] 2.1 In `scripts/flow/roborev-review.sh`, evaluate the deferral **only** on `--recheck-job <id>`,
      after the findings state is established.
- [ ] 2.2 Assert scope: `base` == **merge-base** of base ref and HEAD (never the tip), `head` ==
      branch head, `job` == the job under decision, named explicitly and never resolved from
      base+head.
- [ ] 2.3 Assert `count=` equals the **observed** findings count and `issues=` is non-empty →
      otherwise `COUNT-MISMATCH`.
- [ ] 2.4 Assert disposition per issue: retrievable (`ISSUE-UNRESOLVABLE` otherwise) **and**
      referenced from the PR body (`PR-UNLINKED` otherwise).
- [ ] 2.5 Refuse to defer `findings: UNKNOWN` / `SKIP` in every mode; add **no** prose reconstruction
      of per-finding identity.
- [ ] 2.6 Resolve the scanner from the wrapper's own directory — no override, no `${…:-…}` fallback.

## 3. Reporting
- [ ] 3.1 `findings: DEFERRED (<n>, issues=#…, authorized @<login>, job <id>)`; never `NONE` for a
      deferral (`NONE` stays reachable only from the record's structured verdict letter).
- [ ] 3.2 Add `deferral:` key with the full cause set: `GRANTED` / `NONE` / `STALE` / `MALFORMED` /
      `UNAUTHORIZED` / `COUNT-MISMATCH` / `ISSUE-UNRESOLVABLE` / `PR-UNLINKED` / `UNAVAILABLE`;
      every non-`GRANTED` value leaves the FAIL. `GRANTED` names author, issues, count, scope and the
      **verbatim** reason. The `NONE` cause teaches both the sole-content and top-level rules.
- [ ] 3.3 Extend the **closed** verdict grammar: `DEFERRED` non-failing only when the oracle granted;
      exact **token** match (up to first space), never a prefix; extend the affirmation backstop so
      grammar and backstop read **one** coupled state.
- [ ] 3.4 Emit **no part** of the marker in any diagnostic; point at `--help`.
- [ ] 3.5 Update the wrapper's `usage()` / header doctrine block to document the marker and its
      constraints.

## 4. Separate scoping
- [ ] 4.1 Keep `waiver:`/`WAIVED` and `deferral:`/`DEFERRED` fully independent — no fallback either
      way; a run may carry both, each on its own marker.

## 5. Hermetic coverage (`scripts/tests/test_roborev_review_guard.sh`, gate `tooling-tests`)
- [ ] 5.1 Grant case: findings-bearing recheck + matching marker ⇒ `DEFERRED` + `RESULT: PASS`.
- [ ] 5.2 A case per refusal state: `NONE`, `STALE`, `MALFORMED`, `UNAUTHORIZED`, `COUNT-MISMATCH`,
      `ISSUE-UNRESOLVABLE`, `PR-UNLINKED`, `UNAVAILABLE`.
- [ ] 5.3 Non-deferrable: `findings: UNKNOWN` and `SKIP` with a granted-shaped marker ⇒ FAIL.
- [ ] 5.4 Sole-content refusals: indented, `>`-quoted, bulleted, mid-sentence, fenced, HTML
      `<pre>`/`<code>`.
- [ ] 5.5 Diagnostic-is-not-a-credential: a failing run's own output pasted as a PR comment grants
      nothing.
- [ ] 5.6 Separate-scoping pair: waiver-only with findings ⇒ FAIL; deferral-only with absent prompt
      content ⇒ FAIL.
- [ ] 5.7 Merge-base scope case: base ref advanced past the branch point ⇒ scope still matches.
- [ ] 5.8 Structural asserts: no flag/file/env deferral channel exists; no test-only enforcer-path
      seam (needle split so the guard cannot match its own line).
- [ ] 5.9 Each case plants artifacts in its **own scratch copy** of the tree, never a path variable.

## 6. Doctrine (same change)
- [ ] 6.1 `CLAUDE.md`: "roborev clean" = **no unaddressed findings**; the `roborev-defer:` marker, its
      channel and constraints; `DEFERRED` never `NONE`; separate scoping; `UNKNOWN`/`SKIP` not
      deferrable; the other non-`PASS` verdicts still block.
- [ ] 6.2 Website `agents-developing/roborev-findings/` — same content; verify publication by grepping
      the **served page** for a distinctive new phrase, never by HTTP 200 (CDN serves stale ~3 min).

## 7. Out of scope / guardrails
- [ ] 7.1 `scripts/agent-gate.sh` **unmodified** (verify by `git diff --name-only`).
- [ ] 7.2 PR body states: the wrapper cannot certify itself; the live demonstration is **post-merge**;
      the deferral is confined to the roborev verdict.
