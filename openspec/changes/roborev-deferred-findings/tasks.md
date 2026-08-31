# Tasks — roborev-deferred-findings (issue #3626)

## 1. Enforcer: extend the existing scanner, do not add a second one
- [x] 1.1 Teach `scripts/flow/roborev-waiver-scan.py` a second marker kind, `roborev-defer: findings`,
      reusing its sole-nonblank-content, column-zero, top-level-comment and structured-author logic
      **by call, not by copy** — a second implementation of a channel rule is a second place for it to
      diverge.
- [x] 1.2 Parse and validate the fields: `issues=` (non-empty, comma-separated integers), `count=`
      (integer), `base=`/`head=` (40-hex), `job=`, `reason=` — one anchored pattern, field order
      enforced, `reason` trimmed **before** it is judged, placeholders (`why`/`todo`/`tbd`, any
      unsubstituted `<…>`) refused.
- [x] 1.3 Emit a structured record the wrapper consumes, distinguishing `granted` from each refusal
      state; a marker-only comment with bad fields is `MALFORMED`, a comment with other content is
      silently `NONE`.
- [x] 1.4 Keep the allowlist (`ROBOREV_WAIVER_AUTHORS`) hard-coded in `roborev-review-oracles.sh`,
      not env-derived and not config-derived; the deferral uses the same list.

## 2. Wrapper: scope binding, affirmative matching, disposition
- [x] 2.1 In `scripts/flow/roborev-review.sh`, evaluate the deferral **only** on `--recheck-job <id>`,
      after the findings state is established.
- [x] 2.2 Assert scope: `base` == **merge-base** of base ref and HEAD (never the tip), `head` ==
      branch head, `job` == the job under decision, named explicitly and never resolved from
      base+head.
- [x] 2.3 Assert `count=` equals the **observed** findings count and `issues=` is non-empty →
      otherwise `COUNT-MISMATCH`.
- [x] 2.4 Assert disposition per issue: an **OPEN** issue, asked FOUR-VALUED — `present` (the only
      granting state) / `absent` ⇒ `ISSUE-ABSENT` / `closed` ⇒ `ISSUE-CLOSED` / could-not-ask ⇒
      `ISSUE-UNVERIFIABLE`, textually
      distinct, keyed on the DIAGNOSTIC because `gh issue view` exits 1 for both, unrecognised ⇒
      could-not-ask. **The PR-body link requirement is DELETED** (lead ruling, option A): a PR body is
      editable by anyone with write access with no per-edit attribution while a comment is permanent
      and attributable, so it was the weaker artifact — and its Markdown recognisers leaked in two
      successive rounds (census in `design.md`). Reinstating it is reinstating generation three.
- [x] 2.4b A marker ATTEMPT is the stem plus whitespace **or end of line**, both kinds: a marker-only
      comment that is exactly the stem is `MALFORMED`, never a fail-quiet `NONE`.
- [x] 2.5 Refuse to defer `findings: UNKNOWN` / `SKIP` in every mode; add **no** prose reconstruction
      of per-finding identity.
- [x] 2.6 Resolve the scanner from the wrapper's own directory — no override, no `${…:-…}` fallback.

## 3. Reporting
- [x] 3.1 `findings: DEFERRED (<n>, issues=#…, authorized @<login>, job <id>)`; never `NONE` for a
      deferral (`NONE` stays reachable only from the record's structured verdict letter).
- [x] 3.2 Add `deferral:` key with the full cause set: `GRANTED` / `NONE` / `STALE` / `MALFORMED` /
      `UNAUTHORIZED` / `COUNT-MISMATCH` / `ISSUE-ABSENT` / `ISSUE-CLOSED` / `ISSUE-UNVERIFIABLE` /
      `UNAVAILABLE`;
      every non-`GRANTED` value leaves the FAIL. `GRANTED` names author, issues, count, scope and the
      **verbatim** reason. The `NONE` cause teaches both the sole-content and top-level rules.
- [x] 3.3 Extend the **closed** verdict grammar: `DEFERRED` non-failing only when the oracle granted;
      exact **token** match (up to first space), never a prefix; the scan carries each key's NAME so
      the admission is **confined to `findings:`**, and the deterministic-key affirmation backstop
      carries no `DEFERRED` arm and does not read the coupled state.
- [x] 3.4 Emit **no part** of the marker in any diagnostic; point at `--help`.
- [x] 3.5 Update the wrapper's `usage()` / header doctrine block to document the marker and its
      constraints.

## 4. Separate scoping
- [x] 4.1 Keep `waiver:`/`WAIVED` and `deferral:`/`DEFERRED` fully independent — no fallback either
      way; a run may carry both, each on its own marker.

## 5. Hermetic coverage (`scripts/tests/test_roborev_review_guard.sh`, gate `tooling-tests`)
- [x] 5.1 Grant case: findings-bearing recheck + matching marker ⇒ `DEFERRED` + `RESULT: PASS`.
- [x] 5.2 A case per refusal state: `NONE`, `STALE`, `MALFORMED`, `UNAUTHORIZED`, `COUNT-MISMATCH`,
      `ISSUE-ABSENT`, `ISSUE-CLOSED`, `ISSUE-UNVERIFIABLE` (each with a naive mutant control proving the
      could-not-ask case is not vacuous), `UNAVAILABLE`.
- [x] 5.3 Non-deferrable: `findings: UNKNOWN` and `SKIP` with a granted-shaped marker ⇒ FAIL.
- [x] 5.4 Sole-content refusals: indented, `>`-quoted, bulleted, mid-sentence, fenced, HTML
      `<pre>`/`<code>`.
- [x] 5.5 Diagnostic-is-not-a-credential: a failing run's own output pasted as a PR comment grants
      nothing.
- [x] 5.6 Separate-scoping pair: waiver-only with findings ⇒ FAIL; deferral-only with absent prompt
      content ⇒ FAIL.
- [x] 5.7 Merge-base scope case: base ref advanced past the branch point ⇒ scope still matches.
- [x] 5.8 Structural asserts: no flag/file/env deferral channel exists; no test-only enforcer-path
      seam (needle split so the guard cannot match its own line).
- [x] 5.9 Each case plants artifacts in its **own scratch copy** of the tree, never a path variable.

## 6. Doctrine (same change)
- [x] 6.1 `CLAUDE.md`: "roborev clean" = **no unaddressed findings**; the `roborev-defer:` marker, its
      channel and constraints; `DEFERRED` never `NONE`; separate scoping; `UNKNOWN`/`SKIP` not
      deferrable; the other non-`PASS` verdicts still block.
- [x] 6.2 Website `agents-developing/roborev-findings/` — same content. **Publication verification is
      POST-MERGE and cannot be done here**: the site is built and served from `main`, so grepping the
      served page for a distinctive new phrase before this branch merges could only ever report `0` —
      which is exactly the false signal the "never by HTTP 200" rule exists to prevent. Recorded in the
      PR body beside the live-path demonstration, with the phrase to grep:
      `roborev clean" means NO UNADDRESSED FINDINGS`.

## 7. Out of scope / guardrails
- [x] 7.1 `scripts/agent-gate.sh` **unmodified** (verify by `git diff --name-only`).
- [ ] 7.2 PR body states: the wrapper cannot certify itself; the live demonstration is **post-merge**;
      the deferral is confined to the roborev verdict. **(For whoever opens the PR — not the
      implementer's to tick.)**

## 5. Round-3 review (roborev job 229 + rust-reviewer)

- [x] 5.1 The three WAIVER-side piped `grep -q` guards over whole-file writers extract to a file and
      grep the file, matching their already-hardened DEFERRAL siblings; the whole-file sweep census
      (31 piped sites, 13 fail-open by polarity, 3 fixed) is recorded in the suite (#3387).
- [x] 5.2 The disposition backstop is AFFIRMATIVE — it counts verifications performed and requires
      that count to equal the number of declared `issues=` fields — with a probe and a naive-`-z`
      mutant contrast.
- [x] 5.3 A CLOSED issue is a fourth, non-granting state (`ISSUE-CLOSED`), deliberately stronger than
      the lead's literal "retrievable" condition, with a number-only mutant contrast.
- [x] 5.4 Dead `roborev_waiver_author_allowed()` deleted (zero callers; a second shell-side
      authorization path waiting to happen).
- [x] 5.5 Both markers' `base=`/`head=` require exactly 40 hex, so an abbreviated sha is `MALFORMED`
      rather than `STALE`; malformed coverage added for both kinds.
- [x] 5.6 A recorded `reason` keeps internal whitespace verbatim (only the block boundary escapes
      control characters); covered with repeated spaces and a tab.
- [x] 5.7 A `reason` containing either marker stem is refused, with grant-path coverage for both kinds.
- [x] 5.8 Edit scars fixed: the mangled "EDITABLE AT ANY TIME" rationale, the un-reflowed help wrap,
      and the scanner's missing PEP8 blank lines.
