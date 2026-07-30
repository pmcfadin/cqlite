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

**Four** trigger paths are confirmed (the fourth was found by this change's own live probe):

1. **Worktree + `--branch` WITHOUT an explicit `--repo`.** `roborev review --branch --base origin/main` run
   from inside a git worktree resolves `--branch` against the **ROOT checkout**, not `$PWD` — worktrees are
   not registered in `roborev repo list`, and `roborev repo` has no `add` subcommand (repos self-register
   on first use). The root normally sits on `main`, so the run enqueues the **BASE** commit, the diff
   is empty, and the verdict is "No issues found. Summary: The provided combined diff contains no code
   changes to review." Observed: enqueued `39900e4db` (= `origin/main`) while the branch HEAD was
   `4e7ab591e`; jobs 4649/4651/4653/4655/4657 all enqueued `origin/main`. **Measured correction:** adding
   an explicit `--repo <abs>` FIXES this form (it then reports "17 commits since origin/main" and delivers
   every code file) — so the defect is the MISSING `--repo`, not `--branch` itself, and `--branch --base
   <base> --repo <abs>` is the SANCTIONED invocation.
2. **The two-positional commit-range form** anchors the reviewed range at git's **EMPTY-TREE** hash
   (`4b825dc6…..<head40>`), delivering only 3 of 5 census code files. (An earlier observation of it
   enqueueing an unrelated `90a17d376` for `roborev review 89fdbb895 989d7d2c3` is the same class:
   the range it reviews is not the one requested.)
3. **Code-free diffs are silently discarded even on a correctly-targeted run.** A 5-file / 167+ / 63−
   **all-markdown** diff, invoked correctly with an explicit SHA + `--repo <worktree-abs>`, enqueued the
   right SHA and still returned "No issues found. Summary: The provided diff contains no code changes
   to review." Reproducible (jobs 4658, 4659). **This path passes an enqueued-SHA check**, so SHA
   verification alone is insufficient. **Mechanism, now measured:** roborev EXCLUDES non-code paths from
   the diff it constructs (on a 27-file census — 22 markdown + 5 code — the prompt carried headers for
   exactly the 5 code files), so for an all-prose diff the constructed diff is genuinely EMPTY and the
   reviewer's report is TRUTHFUL about an empty input rather than a malfunction. Re-running cannot help;
   only a deterministic pre-enqueue refusal can.
4. **The single-SHA form reviews ONE COMMIT, not the branch** — and it is the form this issue's own AC2
   prescribes. Measured: `git_ref = <head40>` (correct!) while only 3 of 5 census code files reached the
   prompt. On any multi-commit branch — every branch we ship — it certifies the branch from its last commit
   alone: a PARTIAL review reported as a complete one, invisible to every sha-equality check. Hence the
   sanctioned invocation reviews the **RANGE** `<base>..HEAD`, and the wrapper FAILs a single-commit job
   record even when it equals HEAD. This change implements AC2's **intent** — the reviewed content must
   match the requested range — rather than its letter, and says so in `design.md` and the spec delta.

**Token accounting is the observable tell** (`roborev log <job>` / `roborev show <job> --json`):

| job | sha | diff | input | cached | output | wall |
|-----|-----|------|-------|--------|--------|------|
| 4652 | `4e7ab591e` | 6f 216+/64− | 505,625 | 387,328 | 6,332 | 2m45s |
| 4654 | `90a17d376` | 5f 140+/54− | 398,204 | 314,624 | 5,073 | 2m28s |
| 4656 | `89fdbb895` | 5f 207+/110− | 648,582 | 554,496 | 5,067 | 2m25s |
| 4658 | `989d7d2c3` (docs-only) | 5f 167+/63− | 18,700 | 0 | 53 | 8s |
| 4659 | `989d7d2c3` retry | same | 18,801 | 0 | 56 | 8s |
| 4651 | known-EMPTY diff | 0 | 17,333 | — | 21 | — |
| 1 (this branch) | `155e12c`-line | 20f +2279 | 67,387 | 43,520 | 2,232 | 68s |

A genuine review is 400–650k input tokens with heavy cache reuse and minutes of wall time on a LARGE
diff; the last row is the measured **small** genuine review, added during implementation. The vacuous
baseline is ~18k input / 0 cached / <60 output / <10s. Two consequences the original framing missed:
the genuine band **scales with diff size**, so the input floor must be anchored on the vacuous ceiling
(25,000) rather than on the genuine band (a 50,000 floor would false-FAIL the 67k row's size class);
and **output counts collide** between a genuine CLEAN review and a vacuous one (both ~20–60), so an
output floor cannot discriminate them and is advisory only.

**Blast radius is total.** The 1:1:1:1 rule puts **every** issue in a worktree, so **every** flow-\*
roborev run is exposed to T1, and every multi-commit branch to T4. Measured cost on #2950: two
vacuous runs "passed"; re-run
correctly against the real SHA, the **same diff produced TWO REAL BLOCKERS** that would otherwise
have shipped.

## What Changes

1. **A single sanctioned invocation surface: `scripts/flow/roborev-review.sh`** — a fail-closed
   CQLite-side wrapper. `roborev` is an **external binary** (`/usr/local/bin/roborev`, not vendored
   here), so the guard cannot live upstream; it lives on our side of the call. Implemented as **five**
   files: the wrapper, a sourced `roborev-review-oracles.sh` (push assert + census/code-free), a sourced
   `roborev-review-checks.sh` (the five per-review checks), `roborev-job-facts.py` (job-record/token JSON
   decoding), and the hermetic regression check. Both sourced files FAIL CLOSED when missing or truncated,
   validated **before** the review is invoked so a broken install costs no review.
2. **DETERMINISTIC checks carry the verdict; prose and tokens only corroborate.** Each load-bearing
   check is judged against data the wrapper obtains ITSELF: the REMOTE (`git ls-remote`), its own
   `git diff --numstat --no-renames <base>...HEAD` census, its own code-free classification of that
   census, the job record's structured `git_ref`/`status`, and the census's own file paths inside the
   prompt ACTUALLY SENT to the reviewer. Judging the reviewer by its own narration is the defect, not
   the fix.
3. **A PASS requires POSITIVE evidence that a review completed** (`review-completed:` — job status
   `done` plus a terminal verdict marker from an allow-list). Absence of a vacuity phrase is never
   proof of a review: an unfinished job, a provider `400 … model is not supported`, and a `failed`
   status all carry no phrase, and all three previously reached `RESULT: PASS`.
4. **Ordered, fail-closed asserts**: push assert against the REMOTE (an unpushed branch is itself an
   empty-diff cause; a local mirror ref is NOT authority) → census (an unresolvable base or a failed
   `git diff` FAILs, distinctly from an empty census) → **deterministic code-free FAIL before anything
   is enqueued** → the RANGE invocation with an explicit absolute `--repo` (never `--branch` without
   `--repo`, never the two-positional range form, never a single sha) → a `job-record:` read that
   consults BOTH payload shapes and reports its own completeness → reviewed-**RANGE** assert against the
   job record's `git_ref`, asserting BOTH endpoints against the census range (the stdout `Enqueued job N
   for <sha>` line demoted to the carrier of the job id — for a range review it names only the base, so an
   unavailable record FAILs rather than falling back to it) →
   `review-completed` → `prompt-content` → findings-vs-error attribution (with a contradiction reported
   `INCONSISTENT` and failed) → the two corroborating
   vacuity tiers → a machine-greppable `==== ROBOREV REVIEW SUMMARY ====` block with a terminal
   `RESULT: PASS|FAIL|NOTHING-TO-REVIEW` and a non-zero exit on anything but PASS.
5. **A distinct `NOTHING-TO-REVIEW` status** for a genuinely empty census — explicitly **not** a pass,
   and not recordable as "roborev clean".
6. **Docs-only diffs are declared non-certifiable by roborev**, and enforced DETERMINISTICALLY.
   Trigger 3 makes roborev structurally unable to review a code-free diff, so an all-prose census FAILs
   under its own `code-free:` key from the wrapper's own classification, before a review is enqueued —
   never contingent on the reviewer admitting it. The sanctioned substitute is verification against
   **primary sources** (for #2950 that was `git show cassandra-5.0.8:<path>`) recorded in the PR.
7. **A non-zero roborev exit is ATTRIBUTED, not merely reported.** roborev exits non-zero **when it
   reports findings**, so `roborev-exit:` splits `FINDINGS (exit N)` (a genuine review to triage and
   fix) from `ERROR (exit N)` (the reviewer itself failed), with a new `findings:` key as the
   deterministic disambiguator. Both FAIL the run; misattributing findings as a malfunction is
   dangerous in the opposite direction — an agent told the reviewer broke retries or bypasses instead
   of fixing.
8. **Token accounting is repaired and demoted.** It was PERMANENTLY `UNAVAILABLE` on every real run
   (the payload's `token_usage` is a JSON-ENCODED STRING needing a double decode, and the output field
   is `total_output_tokens`) — i.e. a guard that silently was not there. Now three-state: `absent` ⇒
   `UNAVAILABLE`, `parsed` ⇒ evaluate, **`present-but-unparseable` ⇒ FAIL (drift)**. The input floor is
   re-anchored to **25,000** on the measured vacuous ceiling, and the **output-token floor is dropped
   as a FAIL condition** (it cannot discriminate a genuine CLEAN review from a vacuous one).
9. **Call-site migration across SIXTEEN surfaces** — thirteen under `.claude/**` (including the
   `/worker` and `/manager` fleet entry points) plus three non-`.claude` doctrine surfaces that
   previously prescribed the **inverse** rule ("no `--agent`/`--model` flags"; one called explicit
   agent/model "never doctrine"). Bare `--branch` becomes non-sanctioned everywhere.
10. **A hermetic regression check** (`scripts/tests/test_roborev_review_guard.sh`, a stub `roborev` on
    `PATH` replaying the recorded outputs — including the doubly-encoded token payload and the review row
    that nests the job row — against throwaway git fixtures covering the fleet's narrow-refspec topology
    and a detected rename) wired into BOTH the `--lite`
    `roborev-lints` component and the full-gate `tooling-tests`, plus a documented **live worktree
    probe** proving a worktree-launched review reviews the worktree's HEAD. **329 assertions.**
11. **Doctrine in the same change** (CLAUDE.md's roborev-invocation paragraph + the
    `agents-developing/roborev-findings` page, including a row in its mechanized-in-`--lite` table), plus
    the three measured corrections propagated to every surface that states the rule: the non-sanctioned
    form is `--branch` **without** `--repo`; the single-SHA form is a partial review; roborev excludes
    non-code paths from the diff it builds.

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
  `scripts/flow/roborev-review-oracles.sh` (sourced: push assert + census/code-free oracles),
  `scripts/flow/roborev-review-checks.sh` (sourced: the five per-review checks),
  `scripts/flow/roborev-job-facts.py` (job-record + token extraction),
  `scripts/tests/test_roborev_review_guard.sh` (hermetic regression check — 329 assertions).
- **Gate:** the regression check is registered in `scripts/agent-gate.sh`'s shell-tooling component
  set (`tooling-tests`, and `roborev-lints` so it also runs in `--lite`), so a regression FAILs the
  fast loop rather than costing a review round.
- **Agent surfaces (call-site migration, 13):** `.claude/skills/{flow-implement,flow-activate,
  flow-address,flow-finalize,ci-cd-validation}/SKILL.md`,
  `.claude/skills/ci-cd-validation/merge-process.md`, `.claude/agents/{flow-closer,flow-lead,
  rust-reviewer,sstable-developer,test-validator}.md`, and `.claude/commands/{worker,manager}.md`.
- **Fleet doctrine surfaces (3):** `website/src/content/docs/agents-developing/delivery-pipeline.md`,
  `docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md` — each previously
  prescribed the INVERSE rule (run roborev with the machine's configured agent and no flags).
- **Doctrine (ships in this change per CLAUDE.md):** CLAUDE.md's roborev-invocation bullet in
  *Agent-Team Conventions*, plus `website/src/content/docs/agents-developing/roborev-findings.md`.
  Publication is accepted by **grepping the served page for a new distinctive phrase** — an HTTP 200
  is not proof (CDN staleness ≈3 min).
- **No-heuristics mandate (#28):** unaffected. See `design.md` — that mandate governs on-disk
  TYPE/format inference in the SSTable read path, not review-tooling liveness detection. The
  deterministic census comparison is the authoritative check; the token thresholds are a bounded,
  evidenced corroboration whose only possible action is to **fail closed**.
- **Public binding surfaces (Python/Node/CLI):** untouched.
