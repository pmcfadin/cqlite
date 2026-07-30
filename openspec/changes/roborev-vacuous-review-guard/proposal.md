# Proposal: Fail-closed guard against vacuous roborev reviews (issue #2964)

**Milestone:** maintenance (agent-team automation / delivery pipeline) · **Priority:** P1 ·
**Routing:** design-driven (the agent-delivery automation itself — no external oracle; the fix is a
new sanctioned invocation surface plus doctrine) · **Issue:** #2964 (`flow-meta`) ·
**Related:** #2433 (roborev agent/model pin), #2086/#2087/#2088 (review-first + severity triage),
#2084 (`flow-closer` endgame), #2950 (the delivery where the defect was caught).

## Why

`roborev` can report **"No issues found"** without having reviewed anything, and a vacuous pass is
**textually identical** to a genuine clean pass. Because CQLite's delivery pipeline treats "roborev
clean" as a merge condition (`flow-closer` runs a final roborev confirmation pass before arming
`gh pr merge --auto`), a vacuous pass can merge **unreviewed code**.

Three trigger paths are confirmed:

1. **Worktree + `--branch`.** `roborev review --branch --base origin/main` run from inside a git
   worktree resolves `--branch` against the **ROOT checkout**, not `$PWD` — worktrees are not
   registered in `roborev repo list`, and `roborev repo` has no `add` subcommand (repos self-register
   on first use). The root normally sits on `main`, so the run enqueues the **BASE** commit, the diff
   is empty, and the verdict is "No issues found. Summary: The provided combined diff contains no code
   changes to review." Observed: enqueued `39900e4db` (= `origin/main`) while the branch HEAD was
   `4e7ab591e`; jobs 4649/4651/4653/4655/4657 all enqueued `origin/main`.
2. **Commit-range form mis-enqueues.** `roborev review 89fdbb895 989d7d2c3` enqueued `90a17d376` —
   **neither endpoint**.
3. **Code-free diffs are silently discarded even on a correctly-targeted run.** A 5-file / 167+ / 63−
   **all-markdown** diff, invoked correctly by explicit SHA + `--repo <worktree-abs>`, enqueued the
   right SHA and still returned "No issues found. Summary: The provided diff contains no code changes
   to review." Reproducible (jobs 4658, 4659). **This path passes an enqueued-SHA check**, so SHA
   verification alone is insufficient.

**Token accounting is the observable tell** (`roborev log <job>` / `roborev show <job> --json`):

| job | sha | diff | input | cached | output | wall |
|-----|-----|------|-------|--------|--------|------|
| 4652 | `4e7ab591e` | 6f 216+/64− | 505,625 | 387,328 | 6,332 | 2m45s |
| 4654 | `90a17d376` | 5f 140+/54− | 398,204 | 314,624 | 5,073 | 2m28s |
| 4656 | `89fdbb895` | 5f 207+/110− | 648,582 | 554,496 | 5,067 | 2m25s |
| 4658 | `989d7d2c3` (docs-only) | 5f 167+/63− | 18,700 | 0 | 53 | 8s |
| 4659 | `989d7d2c3` retry | same | 18,801 | 0 | 56 | 8s |
| 4651 | known-EMPTY diff | 0 | 17,333 | — | 21 | — |

A genuine review is 400–650k input tokens with heavy cache reuse and minutes of wall time. The
vacuous baseline is ~18k input / 0 cached / <60 output / <10s.

**Blast radius is total.** The 1:1:1:1 rule puts **every** issue in a worktree, so **every** flow-\*
roborev run is exposed to trigger 1. Measured cost on #2950: two vacuous runs "passed"; re-run
correctly against the real SHA, the **same diff produced TWO REAL BLOCKERS** that would otherwise
have shipped.

## What Changes

1. **A single sanctioned invocation surface: `scripts/flow/roborev-review.sh`** — a fail-closed
   CQLite-side wrapper. `roborev` is an **external binary** (`/usr/local/bin/roborev`, not vendored
   here), so the guard cannot live upstream; it lives on our side of the call.
2. **A locally-computed diff census as the oracle.** `git diff --numstat <base>...HEAD` produces the
   authoritative files/+/− census. Every downstream vacuity claim is judged against that census, not
   against the reviewer's own prose.
3. **Ordered, fail-closed asserts**: push assert (an unpushed branch is itself an empty-diff cause) →
   census → explicit-SHA + explicit-`--repo` invocation (never bare `--branch`, never the
   two-positional range form) → reviewed-SHA assert against the `Enqueued job N for <sha>` line →
   two-tier vacuity assert (deterministic verdict-text-vs-census comparison primary; bounded token
   accounting corroborating) → a machine-greppable `==== ROBOREV REVIEW SUMMARY ====` block with a
   terminal `RESULT: PASS|FAIL|NOTHING-TO-REVIEW` and a non-zero exit on anything but PASS.
4. **A distinct `NOTHING-TO-REVIEW` status** for a genuinely empty census — explicitly **not** a pass,
   and not recordable as "roborev clean".
5. **Docs-only diffs are declared non-certifiable by roborev.** Trigger 3 makes roborev structurally
   unable to review a code-free diff, so a docs/spec/workflow-only diff FAILs the wrapper as vacuous;
   the sanctioned substitute is verification against **primary sources** (for #2950 that was
   `git show cassandra-5.0.8:<path>`) recorded in the PR.
6. **Call-site migration.** Every roborev invocation in `.claude/skills/{flow-implement,flow-activate,
   flow-address,flow-finalize,ci-cd-validation}` and `.claude/agents/{flow-closer,flow-lead,
   rust-reviewer,sstable-developer,test-validator}` routes through the wrapper; bare `--branch` becomes
   non-sanctioned.
7. **A hermetic regression check** (`scripts/tests/test_roborev_review_guard.sh`, stub `roborev` on
   `PATH` replaying the recorded outputs above) wired into the agent-gate component set, plus a
   documented **live worktree probe** proving a worktree-launched review reviews the worktree's HEAD.
8. **Doctrine in the same change** (CLAUDE.md's roborev-invocation paragraph + the
   `agents-developing/roborev-findings` page).

## Non-goals

- **Not patching or forking `roborev`.** It is an external binary outside this repo's control. An
  upstream fix (worktree-aware `--branch` resolution; a non-zero exit on a discarded code-free diff)
  is a worthwhile **follow-up**, not this change — the fleet needs the guard now, and the guard remains
  correct even after an upstream fix lands.
- **Not replacing roborev, and not a second reviewer.** The wrapper adds no review capability; it only
  proves that a review **happened against the right bytes**, and fails closed when it cannot.
- **Not changing review severity triage.** `docs/development/roborev-severity.md` (blocker vs nit,
  #2088) is untouched; the wrapper's verdict is about **liveness**, not about finding classes.
- **Not changing the `--agent`/`--model` pin policy.** The existing #2433 trap (pass **both** or codex
  hard-400s on the config-pinned Anthropic model name) is preserved and mechanically enforced by the
  wrapper, not re-litigated.
- **Not a new gate mode.** The wrapper is not an `agent-gate.sh` component and emits its own distinct
  summary block; it can never be confused with, or substituted for, `AGENT-GATE SUMMARY`.
- **No Rust code, no library surface, no on-disk format work.** Nothing touches `cqlite-core`, the
  bindings, the CLI, the no-heuristics decode path, or the <128MB memory budget.

## Impact

- **New scripts:** `scripts/flow/roborev-review.sh` (the sanctioned invocation),
  `scripts/tests/test_roborev_review_guard.sh` (hermetic regression check).
- **Gate:** the regression check is registered in `scripts/agent-gate.sh`'s shell-tooling component
  set (`tooling-tests`, and `roborev-lints` so it also runs in `--lite`), so a regression FAILs the
  fast loop rather than costing a review round.
- **Agent surfaces (call-site migration):** `.claude/skills/{flow-implement,flow-activate,flow-address,
  flow-finalize,ci-cd-validation}/SKILL.md` and `.claude/agents/{flow-closer,flow-lead,rust-reviewer,
  sstable-developer,test-validator}.md`.
- **Doctrine (ships in this change per CLAUDE.md):** CLAUDE.md's roborev-invocation bullet in
  *Agent-Team Conventions*, plus `website/src/content/docs/agents-developing/roborev-findings.md`.
  Publication is accepted by **grepping the served page for a new distinctive phrase** — an HTTP 200
  is not proof (CDN staleness ≈3 min).
- **No-heuristics mandate (#28):** unaffected. See `design.md` — that mandate governs on-disk
  TYPE/format inference in the SSTable read path, not review-tooling liveness detection. The
  deterministic census comparison is the authoritative check; the token thresholds are a bounded,
  evidenced corroboration whose only possible action is to **fail closed**.
- **Public binding surfaces (Python/Node/CLI):** untouched.
