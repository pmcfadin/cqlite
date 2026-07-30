# Design: fail-closed roborev vacuous-review guard (issue #2964)

## Context

`roborev` is the pipeline's code-review stage. Its verdict is a **merge condition**: `flow-implement`
runs it review-first on the lite-green diff (#2086), and `flow-closer` runs a final confirmation pass
before arming `gh pr merge --auto` (#2084/#2667). "roborev clean" is therefore load-bearing — if it can
be satisfied without a review having happened, the pipeline merges unreviewed code with no red anywhere.

Three confirmed triggers produce exactly that (evidence table in `proposal.md`):

| # | Trigger | Enqueued SHA | Detectable by a SHA check? |
|---|---------|--------------|----------------------------|
| 1 | worktree + `--branch` resolves against the ROOT checkout (on `main`) | `origin/main` | **yes** |
| 2 | two-positional commit-range form mis-enqueues | neither endpoint | **yes** |
| 3 | code-free (docs-only) diff silently discarded | correct SHA | **NO** |

Two structural facts constrain every option:

- **`roborev` is an external binary** (`/usr/local/bin/roborev`), not vendored in this repo. We cannot
  change its resolution logic or its exit codes.
- **A vacuous verdict is textually identical to a genuine clean one** at the top level ("No issues
  found"). The only distinguishing signals are (a) the `Summary:` sentence, (b) the enqueued SHA, and
  (c) token accounting / wall time.

So the guard must be a **CQLite-side wrapper**, and it must judge the reviewer's claims against
something computed **locally**, not against the reviewer's own prose.

## Recommended design

### `scripts/flow/roborev-review.sh` — the ONLY sanctioned roborev invocation

Flags: `--repo <path>` (default: `git rev-parse --show-toplevel` of `$PWD`), `--base <ref>` (default
`origin/main`), `--agent`, `--model`, and passthrough for the rest. Every step is ordered and
fail-closed; the first failure stops the run, emits the summary block, and exits non-zero.

**Step 1 — Resolve identity.** Repo root (**absolute** — `roborev --repo` must never receive a relative
path), current branch, HEAD sha. `--repo` is always passed explicitly; the wrapper never relies on
`roborev` inferring the repo from `$PWD` (that inference is trigger 1).

**Step 2 — Push assert (AC3).** `origin/<branch>` must exist and equal HEAD. Otherwise FAIL, naming
the unpushed commits. Rationale: an unpushed implementation commit is *itself* an empty-diff cause —
the reviewer can only see what the remote has. Ordering matters: this runs **before** the census so the
operator gets the actionable cause ("push your commits") rather than a downstream vacuity FAIL.

**Step 3 — Local diff census: the oracle.** `git diff --numstat <base>...HEAD` → files changed, lines
added, lines removed. This locally-computed census is **the authoritative statement of what must be
reviewed**, and every downstream vacuity claim is judged against it. Using `...` (three-dot,
merge-base) matches the semantics of the `--base` the reviewer is told to diff against, so the census
and the review are talking about the same change set.

If the census is **genuinely empty**, the wrapper exits with a **distinct `NOTHING-TO-REVIEW` status**
(its own non-zero exit code, `3`) — explicitly **not** a pass, and explicitly not recordable as
"roborev clean". A separate status (rather than PASS-with-a-note) is the point: a caller cannot
accidentally treat "there was nothing to look at" as "it was looked at and was clean".

**Step 4 — Invoke by explicit SHA + explicit repo (AC2).**

```
roborev review <head-sha> --repo <abs-repo> --agent <a> --model <m> --wait
```

**NEVER bare `--branch`** (trigger 1). **NEVER the two-positional range form** (trigger 2). Both
`--agent` and `--model` are always passed — the wrapper refuses to run with only one of them, preserving
the already-documented #2433 trap (`--agent codex` alone inherits `review_model` from `.roborev.toml`
and hard-400s; and to run the Claude reviewer you must override **both**). Enforcing it in the wrapper
converts a silent-looking outage into a usage error at the call site.

**Step 5 — Reviewed-SHA assert (AC2).** Parse `Enqueued job <N> for <sha>` from the invocation's
output and require `<sha>` to prefix-match HEAD (either direction, since roborev may print an
abbreviated sha). A mismatch FAILs loudly. When the mismatched sha resolves to `<base>` /
`origin/main`, the message must **say so explicitly** — that equality is the fingerprint of trigger 1,
and naming it turns a confusing failure into a one-line diagnosis. If the `Enqueued job` line is absent
entirely, that is also a FAIL (unparseable ⇒ unverifiable ⇒ fail closed), never a skip.

**Step 6 — Vacuity assert (AC1), two tiers, deterministic tier primary.**

- **Tier 1 — PRIMARY, deterministic, no thresholds.** The reviewer's own output claiming no code
  changes — `/contains no code changes to review/i`, `/no code changes/i` — while the **step-3 census
  is NON-empty** is a **HARD FAIL**. This is a comparison against a locally-computed oracle, not a
  guess: we know the diff is non-empty because we measured it ourselves, so a reviewer asserting the
  opposite has demonstrably not reviewed the change. This tier alone catches trigger 3 (which passes
  the SHA assert) and also back-stops triggers 1 and 2.
- **Tier 2 — CORROBORATING, bounded token accounting.** From `roborev show <N> --json` (fallback
  `roborev list --json`): on a **non-empty census**, `input < VACUOUS_MAX_INPUT_TOKENS` **OR**
  `cached_input == 0` **OR** `output < VACUOUS_MIN_OUTPUT_TOKENS` is also a HARD FAIL. The thresholds
  are **named constants at the top of the script** with the measured evidence table cited in a comment,
  and the failure message prints the **observed numbers next to the threshold**, so a future
  recalibration is a one-line, evidenced change rather than an archaeology exercise.

  Initial calibration from the recorded evidence (genuine: 398k–649k input / 314k–554k cached / ~5k
  output / 2m25s–2m45s; vacuous: ~18k input / 0 cached / ≤56 output / 8s):

  | Constant | Value | Margin vs evidence |
  |---|---|---|
  | `VACUOUS_MAX_INPUT_TOKENS` | 50,000 | ~2.7× above the vacuous ceiling (18.8k), ~8× below the genuine floor (398k) |
  | `VACUOUS_MIN_OUTPUT_TOKENS` | 200 | ~3.6× above the vacuous ceiling (56), ~25× below the genuine floor (5,067) |

  If token accounting is **unavailable** from the installed roborev build (no `show --json`, or the
  fields are absent), that is a **degraded-signal notice** stamped in the summary block and tier 1
  still governs — **never a silent skip**, and never a downgrade of tier 1.

**Step 7 — Emit a compact, machine-greppable summary block.**

One field per line — a reader greps a single `^<key>: ` anchor, never a column offset. The as-built
block (pinned by `scripts/tests/test_roborev_review_guard.sh`) is exactly:

```
==== ROBOREV REVIEW SUMMARY ====
repo: <abs>
branch: <name>
base: <ref>
head-sha: <sha>
reviewed-sha: <sha>|-
job: <N>|-
census: <F> files, +<A>/-<D>
tokens: input=<i> cached=<c> output=<o>   (or: tokens: UNAVAILABLE)
push-assert: PASS|FAIL(<reason>)|SKIP
census-check: PASS|FAIL(<reason>)|SKIP
sha-assert: PASS|FAIL(<reason>)|SKIP
vacuity-tier1: PASS|FAIL(<reason>)|SKIP
vacuity-tier2: PASS|FAIL(<reason>)|UNAVAILABLE|SKIP
log: <transcript path>
RESULT: PASS|FAIL|NOTHING-TO-REVIEW
```

The greppable key set is therefore `repo:` `branch:` `base:` `head-sha:` `reviewed-sha:` `job:`
`census:` `tokens:` `push-assert:` `census-check:` `sha-assert:` `vacuity-tier1:` `vacuity-tier2:`
`log:` and the terminal `RESULT:`. `census-check:` carries the census oracle's own verdict (it is the
key that FAILs when the base ref is unresolvable, and the key that marks the empty-census
`NOTHING-TO-REVIEW` path); `log:` names the transcript so a caller never needs to retain it. A
per-check key whose step was never reached reads `SKIP` — a value that can never be mistaken for a
pass, and never a blank.

Exit codes: `0` = PASS, `1` = FAIL, `3` = NOTHING-TO-REVIEW, and `2` = **usage error**. Exit 2 is
deliberately NOT a verdict: it emits **no** summary block at all (a loud `ERROR:` naming the missing
option, on stderr, before any repo identity is resolved and before anything is enqueued), because a
`RESULT:` line for a run that never happened would alias a usage error onto one of the three real
outcomes — recreating the very indistinguishability this change exists to eliminate. `--help` (exit 0)
is likewise not a verdict and emits no block. Modeled deliberately on the gate's
summary-file contract: **an agent retains only this block, never raw roborev stdout** (the raw
transcript goes to a log path named in the block). The block name is distinct from
`AGENT-GATE SUMMARY` / `AGENT-GATE LITE SUMMARY` / `AGENT-GATE DELTA SUMMARY` so it can never be pasted
as a gate verdict, and vice versa.

**Step 7b — the reviewer's own exit status gets its own key: `roborev-exit:`.** A non-zero exit from the
underlying `roborev` process is already a fail-closed FAIL (it forces `RESULT: FAIL` and adds an
`ERROR:` detail line naming the observed code), but a detail line is **prose**: a reader that retains
only the block and greps the per-check keys sees `push-assert`/`census-check`/`sha-assert`/
`vacuity-tier1`/`vacuity-tier2` all `PASS` and no key explaining the FAIL. That is exactly the
"which check tripped?" ambiguity every other key exists to remove, and it is the one failure cause a
grep-based reader cannot attribute. So the block carries `roborev-exit: PASS` when the process exited
zero and `roborev-exit: FAIL (exit <N>)` otherwise — placed with the other per-check keys, before
`log:` — and it participates in the same `FAIL*` scan that computes the terminal verdict. (This key is
the one field the step-7 sketch above does not yet list, because the sketch records the block as
currently built: emitting it is a one-line addition to the wrapper's `emit_summary`, and the fail-closed
BEHAVIOUR it reports is already implemented — only the greppable surfacing is missing.)

**Step 8 — Hygiene.** The script stays small (we adopt the campsite rule's spirit: the gate's
`file-size` ratchet covers `.rs` only, so this is a review expectation, not a mechanized one) and free
of the traps in CLAUDE.md's pre-roborev self-check list — notably no wall-clock threshold asserts in
the regression test's correctness path (#2642, mechanized by `roborev-lints`) and no unquoted
interpolation of external output into a shell command.

### Docs-only diffs cannot be roborev-certified

Trigger 3 is not a bug we can route around: roborev **structurally discards a code-free diff**. So the
sanctioned position is explicit — a docs/spec/workflow-only diff **cannot be certified by roborev at
all**. The wrapper FAILs it as vacuous (tier 1 fires: non-empty census, "no code changes" verdict), and
the sanctioned substitute is **verification against primary sources**, recorded in the PR body. For
#2950 that was `git show cassandra-5.0.8:<path>` — reading the pinned Cassandra source that the docs
claim to describe. A docs-only PR must **never** record "roborev clean".

This dovetails with existing doctrine rather than fighting it: CLAUDE.md already treats a docs-only diff
specially for the gate (the #3042 CITE-AND-WAIVE rule). The parallel is deliberate — a docs-only diff
cannot be *certified* by a tool whose subject is compiled code, in either the gate's or roborev's case.

### Call-site migration

Every roborev invocation in the agent surfaces routes through the wrapper, and bare `--branch` becomes
non-sanctioned prose. The ten touched surfaces are NOT homogeneous — six carry an invocation, four
carry only a *reference* — and conflating them would state an obligation four of them cannot satisfy:

- **Invocation sites** — the documented procedure runs the wrapper:
  - Review-round sites (run a round themselves; must also state push-first and non-PASS-is-failed):
    `.claude/skills/flow-implement/SKILL.md` (the review-first step — the primary call site, previously
    documenting `roborev review --branch --base origin/main --agent codex --model gpt-5.6-sol --wait`),
    `.claude/agents/flow-closer.md` (the final confirmation pass — the **merge-gating** call site;
    previously documenting a `/roborev-review-branch` form that does not exist),
    `.claude/skills/flow-address/SKILL.md` (the post-comment re-review).
  - Prescribing sites (name the wrapper as the invocation to be used, without running a round in-line):
    `.claude/agents/flow-lead.md` (the stage table + the doctrine bullet),
    `.claude/skills/ci-cd-validation/SKILL.md` + `.claude/skills/ci-cd-validation/merge-process.md`
    (the merge-readiness definition), `.claude/skills/flow-activate/SKILL.md` (the tasks it authors
    must name the wrapper for their roborev step).
- **Non-invoking surfaces** — they contain no roborev invocation at all and must say so, pointing at
  the wrapper as the only sanctioned invocation: `.claude/skills/flow-finalize/SKILL.md` (the
  telemetry `--roborev-findings` counter + what "roborev clean" means in the ledger),
  `.claude/agents/rust-reviewer.md` (the pre-roborev self-check classes — and it additionally flags a
  reintroduced bare `--branch`/range form as a **BLOCKER**), `.claude/agents/sstable-developer.md` and
  `.claude/agents/test-validator.md` (the `roborev-lints` lite component + never-invoke-directly).

### Regression check + gate wiring

`scripts/tests/test_roborev_review_guard.sh` — **hermetic**: a **stub `roborev`** placed first on
`PATH` replays the recorded real outputs from the evidence table (enqueue lines, verdict text, and
`show --json` token payloads), driven against throwaway `git init` fixtures with a synthetic `origin`
remote. No network, no real roborev, no datasets. Cases (a)–(g) map 1:1 to the spec scenarios: base-sha
mismatch, neither-endpoint sha, "no code changes" against a non-empty census, vacuous token signature,
unpushed branch, a genuine PASS, and a genuinely-empty census reporting NOTHING-TO-REVIEW.

**Wiring (concrete).** `scripts/agent-gate.sh` registers shell self-tests two ways, and this change
uses both:

- `run_tooling_tests()` (the `tooling-tests` **full-gate** component) runs a sequence of
  `bash "$REPO_ROOT/scripts/tests/<name>.sh"` guards, each FAILing the component on non-zero — the
  pattern used by `test_generator_keyspace_scoping.sh`, `test_udt_rowbuilder_tuple_shape.sh`,
  `test_check_dockerfile_rust_pin.sh`, `test_check_skill_flag_tables.sh`. The new test is appended
  there.
- `run_roborev_lints_cmd()` (the `roborev-lints` component) is in **both** `LITE_COMPONENTS` and
  `COMPONENTS`, and already chains `check-workflow-injection.sh && check-no-wallclock-asserts.sh`.
  Adding the guard test here is what makes a regression **FAIL the fast `--lite` loop** rather than
  cost a review round — the stated acceptance goal — and it is thematically exact: `roborev-lints` is
  precisely the component that mechanizes roborev-related delivery costs (#2656). The test must be
  fast (hermetic, seconds) to belong in `--lite`.

**Live worktree probe (documented, not gate-run).** A short recorded procedure: from a real
`issue-<N>-*` worktree with a pushed commit, run the wrapper and confirm `reviewed-sha == head-sha`
(and `!= origin/main`) in the summary block. This is the only check that can prove the real external
binary honours the explicit `--repo`; it is documented in the wrapper's usage text / the doctrine page
rather than run in the gate, because it requires network + a live reviewer and would make the gate
non-hermetic.

### Doctrine (ships in this change)

CLAUDE.md's roborev-invocation bullet (*Agent-Team Conventions*) and
`website/src/content/docs/agents-developing/roborev-findings.md` both state: the wrapper is the only
sanctioned invocation; **verify the reviewed SHA**; `"contains no code changes to review"` on a
non-empty diff is a **HARD FAIL**; docs-only diffs cannot be roborev-certified. The website page
already has a "**The reviewer default is codex**" section and a mechanized-in-`--lite` table — both are
the natural homes for the new rule and the new `roborev-lints` entry. Publication acceptance follows
CLAUDE.md's rule: **grep the served page for a new distinctive phrase**; a `200` is not proof (observed
CDN staleness ≈3 minutes).

## Why the token thresholds do NOT violate the no-heuristics mandate (#28)

Worth stating plainly, because "thresholds" reads like a heuristic at a glance. The no-heuristics
mandate governs **on-disk TYPE/format inference from byte patterns in the SSTable read path** — never
guessing a CQL type or a format version from data content instead of authoritative metadata (schema,
else `Statistics.db`). It says nothing about, and is not weakened by, **review-tooling liveness
detection**, which is a property of our own CI process, not of Cassandra's on-disk format.

Three structural properties keep this clean anyway:

1. **Tier 1 is authoritative and threshold-free.** The primary check compares the reviewer's claim
   against a locally-computed, exact census — the review-tooling analogue of "authoritative metadata
   only". Tier 2 never overrides it.
2. **Tier 2's only possible action is to FAIL CLOSED.** It can never manufacture a pass, relax a tier-1
   FAIL, or infer that a review *did* happen. Its worst-case error is a false FAIL, whose cost is one
   re-run — bounded, visible, and self-correcting. (A false FAIL is also *preferable* to the status quo,
   whose failure mode is merging unreviewed code.) The `cached_input == 0` clause is the most
   false-positive-prone of the three — a genuine review against a cold cache could plausibly report it —
   and that is an accepted, deliberate trade in the fail-closed direction, recorded here so a future
   recalibration knows it was a choice, not an oversight.
3. **The thresholds are evidenced, bounded, and named.** They are constants with the measured table
   cited beside them, and every failure prints observed-vs-threshold, so recalibration is a one-line
   evidenced edit — the opposite of an unexplained magic number buried in a branch.

## Alternatives considered (and why the recommendation beat them)

1. **Register worktrees with `roborev repo` so `--branch` resolves correctly.** *Rejected:* there **is
   no `add` subcommand** — repos self-register on first use, so there is nothing to call. Even if there
   were, it would fix only trigger 1: trigger 2 (range form mis-enqueue) and trigger 3 (code-free diff
   discarded) would still return an indistinguishable "No issues found".
2. **Patch/fork `roborev` upstream.** *Rejected for this change:* the binary is outside this repo's
   control, and the fleet is exposed on **every** issue right now — a guard we own ships today, an
   upstream change does not. Noted as a possible **upstream follow-up** (worktree-aware `--branch`
   resolution; a non-zero exit when a code-free diff is discarded). The wrapper stays correct and
   cheap after such a fix lands.
3. **Run roborev from the root checkout instead of the worktree.** *Rejected:* it breaks 1:1:1:1
   worktree isolation and requires **commandeering the shared root** — explicitly forbidden (a closer
   that switched the shared root's branch once stranded root off `main` and broke every session on the
   box). It also serializes concurrent lanes onto one checkout.
4. **Token-threshold-only detection.** *Rejected as a primary signal:* it is non-deterministic and
   recalibrates with every model/prompt change (`gpt-5.6-sol` is codex's own moving default, not a
   config pin — it already shifted once on a version bump). Demoted to a **corroborating tier** behind
   the deterministic census comparison, where a drifted threshold can only cost a re-run.
5. **Trust the verdict text.** *Rejected:* that is precisely the defect. "No issues found" is emitted
   identically whether 650k tokens of real diff were reviewed or zero bytes were.
6. **Detect the empty diff by asking roborev what it saw** (e.g. parse the job's reported diffstat).
   *Rejected:* it re-derives the answer from the same untrusted source. The census must be computed on
   our side with `git`, or the check is circular — the same reason a CQLite `file:line` is never format
   authority.
