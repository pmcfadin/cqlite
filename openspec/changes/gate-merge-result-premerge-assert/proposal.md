# Proposal: gate the MERGE RESULT, and make `premerge-assert` require it fail-closed (issue #3680, slice 2 of #3650)

**Milestone:** maintenance / delivery-pipeline hygiene · **Priority:** P2 · **Routing:** **design-driven**
— there is no external oracle for *"what must a merge gate certify"*, and AC1 is an explicit owner
decision. · **Issue:** #3680 · **Predecessor:** #3650 slice 1 (PR #3707, merged) and #3465 (PR #3646) ·
**Refs:** #3358, #3362, #3514, #3616, #3646, #2910, #2926, #3544, #3752/PR #3842

## Why

Three facts decide whether a merge is safe. `premerge-assert.sh` proves two:

| fact | tree | mechanism | status |
|---|---|---|---|
| the diff has not moved since certification | PR head | head assert (`commit:`/`tree-start:`) | landed (#3646) |
| how far the base is behind, and how much of that churn is in the blast radius | — | `base-staleness.sh` advisory | landed (slice 1) |
| **the diff was certified against the main it will join** | `origin/main` + diff | **merge-result gate** | **THIS CHANGE** |

A squash-merge composes the diff with main's **current** tip, so for any stale-based PR the certified
tree and the merged tree are **different objects** — the certified tree will never exist. Slice 1 made
that visible and deliberately changed no verdict. Four `PREMERGE: SCOPE` lines say so on every success
path. This change is the enforcement, and it is what retires those lines.

The malign direction is the one to keep in view: a **PASS at a stale head** hiding an interaction with
something that landed in between. The head assert accepts it, stamps it as the gate of record, and the
merge composes two things never tested together. *Satisfied and wrong* — the same shape as the peer
summary that nearly merged #3616 on #3580's verdict.

## What lands

1. **A `--merge-result` gate mode** in `scripts/agent-gate.sh`. It composes `origin/main` + the branch
   diff into a **synthetic commit**, checks it out in a scratch worktree, runs the full component set
   there, and stamps a summary with its own marker pair, its own `MODE:` line, and keys naming the
   `origin/main` tip it composed against.
2. **`premerge-assert.sh` requires that certification, fail-closed**, when the slice-1 advisory reports
   staleness — with **exit `5`/`UNMEASURED` treated as STALE**, per the contract slice 1 wrote into its
   own header precisely so this change could not walk into the fail-open hole.
3. **The #3465 scope disclaimer retired** in all five locations, with the pinning test case extended.
4. **A case floor** on `scripts/tests/test_premerge_assert.sh`, which has none today.

## The two decisions this change does NOT take alone (Seam 1)

**AC1 — the freshness of the merge-result certification itself.** A merge-result gate against a `main`
that has since moved is stale the same way, so the question recurses. Slice 1 named this as slice 2's
problem and did not decide it. **Recommendation, and it is MEASURED rather than argued: apply the
slice-1 advisory to the merge-result gate's own composed base.** It needs **no new predicate and no CLI
change**, because `merge-base(origin/main, <synthetic merge commit>)` **is** the composed base — the
synthetic commit carries `origin/main`-at-composition-time as a parent. Verified end to end against the
unmodified `scripts/flow/base-staleness.sh`: composed against `origin/main~10`, it reports
`base 9da235a6c` · `behind 10` · `diff-paths 12` · `blast-radius 9 RECOGNISED` ·
`verdict STALE-RECOGNISED` · **exit 4**. Full alternatives and the termination argument: design D5.

**AC1b — a repo-wide git version floor**, which slice 1 recorded as belonging here "as a precondition of
*enforcement*, not of this advisory" (its `design.md:194`, `spec.md:215`). **Recommendation: none — and
no `git --version` parse.** Measure by *doing*, the `claim.sh smoke` precedent: the composition either
works or it does not, and its failure is already fail-closed. Design D6.

## Non-goals

- **Re-deciding the blast-radius definition.** Owner-ruled 2026-08-30T23:59:56Z; slice 1 mechanized it;
  this change consumes it unchanged. Widening or narrowing it here would silently move slice 1's verdict.
- **A dependency-closure blast radius.** Slice 1's declared gap 1 of 2, filed separately. It needs rustc
  dep-info as its information source; a heuristic import scan is refused. This change **inherits** the
  gap and must not imply it closed it: enforcement makes a non-exhaustive scan *blocking*, which raises
  the cost of a false negative but does not change its existence.
- **Making the merge-result gate the gate of record for every PR.** It is required **when the advisory
  says stale**, not always. A fresh-based PR is unaffected — that is the point of the owner's
  blast-radius ruling, and forcing every lane through a second full gate would be the re-gate loop the
  ruling exists to prevent.
- **Defending against a hostile invoker.** Out of model per #3312's triage rule. What is defended is
  **accident and drift**.
- **Deciding whether CI runs this mode.** `premerge-assert.sh` is read from the checkout and so can be
  exercised on its own PR; a `required`-registry or aggregator change cannot certify itself (#2910 reads
  them from the PR's BASE ref). Stated per-half in AC9 rather than claimed whole.

## Impact

- `scripts/agent-gate.sh` — a new mode. No new **component**, so `scripts/agent-gate.components` and the
  #3544 baseline comparison are untouched. Verified: the manifest's 37 names equal the `COMPONENTS` array.
- `scripts/flow/premerge-assert.sh` — a new fail-closed leg. **Collides with PR #3842** (#3752), open and
  rewriting the same file plus three of the five disclaimer locations. This change adopts its shape (one
  sourced helper per leg, a closed verdict-token set, every could-not-measure state non-passing) and
  rebases onto it rather than racing it.
- `scripts/gate-liveness.sh` — its three-dialect marker regex is **closed** and must learn the fourth, or
  a merge-result gate is unreadable to the liveness reader (#3473). Not listed in the issue.
- Doctrine: `CLAUDE.md`, `.claude/agents/flow-closer.md`, `.claude/skills/flow-address/SKILL.md`, the
  `premerge-assert.sh` header, and its success output.
