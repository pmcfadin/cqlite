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
  found"). Distinguishing them requires evidence obtained OUTSIDE the reviewer's own narration.

So the guard must be a **CQLite-side wrapper**, and it must judge the reviewer's claims against
something computed **locally**, not against the reviewer's own prose.

### The thesis the implementation converged on

The first cut was *prose-primary*: match the reviewer's "no code changes" sentence, with token
thresholds corroborating. Four rounds of review found that this is the wrong axis, for a reason worth
recording: **prose matching can only see the half of the defect space where the reviewer TELLS you it
saw nothing.** The other half — a reviewer that never received the diff, a job that never finished, a
provider 400, a `failed` status — emits no phrase at all, and "no bad phrase" was being read as proof
of a review. Every one of those reached `RESULT: PASS`.

The final architecture is therefore **DETERMINISTIC-PRIMARY**. The verdict is carried by checks judged
against data the wrapper obtains itself:

| Check | Its own oracle | Which half it covers |
|---|---|---|
| `push-assert` | the REMOTE, via `git ls-remote` | the reviewer can only see what the remote has |
| `census-check` | our own `git diff --numstat --no-renames` | what MUST be reviewed |
| `code-free` | our own classification of that census | T3, before a review is even enqueued |
| `sha-assert` | the job record's structured `git_ref` | T1/T2 — the wrong commit was reviewed |
| `review-completed` | job `status` + an allow-list of terminal verdict markers | a review actually FINISHED |
| `prompt-content` | our census's own paths inside the prompt SENT | the reviewer never GOT the diff |

Prose (`vacuity-tier1`) and token accounting (`vacuity-tier2`) sit on top as **corroboration**. A PASS
now requires POSITIVE evidence; it is never inferred from the absence of a bad phrase.

## Recommended design (as built)

### `scripts/flow/roborev-review.sh` — the ONLY sanctioned roborev invocation

Flags: `--agent` and `--model` (**both required**), `--repo <path>` (default: `git rev-parse
--show-toplevel` of `$PWD`, absolutised), `--base <ref>` (default `origin/main`), `--log <path>`.
An option given an EMPTY value is a usage error, never a silent fallback to the default — `--repo ""`
falling back to `$PWD` is exactly how a caller reviews a repository it did not name. Every step is
ordered and fail-closed; the first hard failure stops the run, emits the summary block, and exits
non-zero.

Implementation is **three files**, split for size hygiene and one responsibility each:

- `scripts/flow/roborev-review.sh` (796 lines) — flags, identity, invocation, the asserts, the block.
- `scripts/flow/roborev-review-oracles.sh` (221) — **sourced**: `roborev_push_assert` + `roborev_census`
  (including the code-free classification). These two are the change's whole thesis in code, and both
  learned the "never trust a local proxy" lesson the hard way (below).
- `scripts/flow/roborev-job-facts.py` (178) — JSON decoding of the job record: `git_ref`, `status`,
  `model`/`requested_model`, the prompt, and the token counts with their three-state extraction.

The sourced path is resolved from `BASH_SOURCE` (never `$PWD` — the wrapper runs from arbitrary
worktrees) and the wrapper **FAILs CLOSED** when that file is missing OR truncated: an absent oracles
file that silently turned the push assert and the census into no-ops would be a worse failure than any
this guard was built to catch.

**Step 1 — Resolve identity.** Repo root (**absolute** — `roborev --repo` must never receive a relative
path), branch, full 40-char HEAD. `--repo` is always passed explicitly; the wrapper never relies on
`roborev` inferring the repo from `$PWD` (that inference is trigger 1). `symbolic-ref -q` /
`rev-parse --verify --quiet` are used deliberately: `rev-parse --abbrev-ref HEAD` ECHOES the literal
string `HEAD` in a repo with no commits, so a `|| fallback` concatenated onto real-looking values and
the no-commit guard never fired.

**Step 2 — Push assert (AC3), asking the REMOTE.** `git ls-remote --heads <remote> <branch>` is the
oracle; the remote comes from the branch's configured upstream, else `origin`. Four distinct, correctly
attributed failures: detached HEAD, `ls-remote` failure (**infra/auth — explicitly NOT "never
pushed"**, since `git` and `gh` are separate credential paths, #2942), branch absent on the remote, and
remote tip ≠ HEAD (naming the unpushed commits, or the divergence). This runs **before** the census so
the operator gets the actionable cause ("push your commits") rather than a downstream vacuity FAIL.

**There is no local mirror-ref fast path** — see *Alternatives* 7/8: the fleet's narrow fetch refspec
means the mirror ref never exists, and a cached one survives a force-push or a branch deletion.

**Step 3 — Local diff census: the oracle.** `git diff --numstat --no-renames <base>...HEAD` → files,
lines added, lines removed, and the list of paths. `--no-renames` is deliberate: with rename detection a
renamed file renders as the composite `dir/{old => new}.rs`, which is not a real path and so could never
be found in the reviewer's prompt (step 6b matches literal paths). Three distinguishable failure
states, none of which may alias to "genuinely empty":

- `FAIL (base '<ref>' unresolvable)` — a narrow-refspec clone that never fetched the base.
- `FAIL (git diff failed)` — the measurement never happened.
- `NOTHING-TO-REVIEW` (exit `3`) — genuinely 0 files, no review enqueued, explicitly **not** a pass and
  not recordable as "roborev clean".

"We could not tell" must never render as "there is nothing to look at".

**Step 3b — Code-free census: a DETERMINISTIC FAIL, pre-enqueue.** roborev structurally discards a
code-free diff, so a census that is entirely prose cannot be certified by roborev **at all** — and that
is a property of OUR census, measured locally, so it must not depend on the reviewer admitting it. The
previous revision computed this classification and used it only for attribution WORDING, so a docs-only
diff could reach `RESULT: PASS` whenever the verdict happened not to carry the phrase — violating this
change's own spec requirement and CLAUDE.md rule (4). Classification is by **file EXTENSION**
(`md markdown mdx txt rst adoc`) with a path assist limited to **extensionless** files under
`openspec/ docs/ website/ .claude/`. An earlier revision treated everything under `docs/`, `.github/`
or `.claude/` as prose, which misclassifies `docs/foo.py` and `.github/workflows/*.yml` — and now that
code-free is a FAIL condition, a false code-free classification is a **false FAIL**.

**Step 4 — Invoke by explicit SHA + explicit repo (AC2).**

```
roborev review <head-sha> --repo <abs-repo> --agent <a> --model <m> --wait
```

**NEVER bare `--branch`** (trigger 1). **NEVER the two-positional range form** (trigger 2). Both
`--agent` and `--model` are always passed — the wrapper refuses to run with only one, preserving the
#2433/#3037 trap (`--agent claude-code` alone inherits `review_model` from `.roborev.toml` and fails as
a silent-looking outage). Enforcing it in the wrapper converts that outage into a usage error at the
call site. The transcript goes to `--log`; stdout stays reserved for the block.

**Step 5 — Reviewed-SHA assert (AC2): the STRUCTURED field is the oracle.** The job record's `git_ref`
is a full 40-char sha recorded by roborev itself, so it is compared full-sha to full-sha. The stdout
`Enqueued job <N> for <sha>` line is **demoted to a cross-check** — parsing a tool's prose is the weaker
source whenever a structured one exists — but its absence is still a hard FAIL, because it carries the
job id every structured query needs. Parsing is defensive: lower-cased before matching (so an
upper-case announcement cannot survive the match and then fall out of field extraction as garbage
handed to `roborev show`), a **7**-hex-char floor (4 was loose enough that a 4-char prefix satisfied the
assert), both fields validated, and with several announcements the LAST is the effective enqueue with
the multiplicity recorded. A mismatch is ATTRIBUTED: equal to the base ⇒ the worktree `--branch`
signature; neither endpoint ⇒ the range-form signature. A disagreement between stdout and `git_ref` is
a NOTICE, because it means one of the two surfaces is misreporting.

**Step 5b — `model:`.** `requested_model` ≠ `model` is surfaced as a **loud NOTICE, not a FAIL**: a
model-alias resolution is legitimate, so a mismatch is not by itself evidence of a bad review, and an
always-red guard is the failure mode that gets guards bypassed (this change hit that twice). Review
integrity is carried by the deterministic checks; this line exists so a substitution can never happen
SILENTLY.

**Step 6a — `review-completed:` — POSITIVE evidence a review HAPPENED.** Required before PASS is
reachable: the job `status` must not be a value other than `done`, AND the transcript must carry a
**terminal verdict marker** from an allow-list (a severity-tagged findings marker, or the clean shape:
"no issues found" AND a `Summary:` line). Absence ⇒ FAIL CLOSED. This is the inverse of the original
logic and closes the worst defect found in review: a transcript showing only "waiting for job N", a
`400 … model is not supported`, or `status: failed (provider timeout)` each contain no vacuous phrase
and all three used to PASS. When `status` is unavailable, completion may rest on the marker alone — with
a NOTICE naming it as the weaker of the two signals, never a silent upgrade to `done`.

**Step 6b — `prompt-content:` — DETERMINISTIC, threshold-free.** Reads the prompt actually sent to the
agent (the job record's `prompt`, else `roborev show <job> --prompt`) and looks for OUR census's own
paths in it. This is the deterministic complement of tier 1: tier 1 catches "the reviewer GOT the diff
and discarded it"; this catches "the reviewer never GOT the diff". Bounded by
`PROMPT_CONTENT_MAX_PATHS_CHECKED=40` — all paths for a small census, an evenly sampled subset (all of
which must be present) for a large one — and the PASS value reports the coverage it checked. A
whitespace-only prompt file is a RETRIEVAL FAILURE ⇒ `UNAVAILABLE` (degraded, visible), never a FAIL:
an unsupported roborev build must not false-FAIL every run.

**Step 6c — `findings:` and `roborev-exit:`.** roborev **exits non-zero when it REPORTS FINDINGS**. The
original wording called every non-zero exit a reviewer malfunction, which is dangerous in the OPPOSITE
direction from the vacuity bug: an agent told the reviewer broke will retry or bypass instead of FIXING
the findings. So the exit is split — `FINDINGS (exit N)` when the review ran (structured `status` is the
authority) versus `ERROR (exit N)` when the reviewer itself failed — and the findings state gets its own
key (`NONE` / `PRESENT` / `PRESENT (n)` / `UNKNOWN` / `SKIP`). Both still force `RESULT: FAIL` (a review
with open findings is not "roborev clean"), but the FINDINGS message says explicitly: the review is
genuine, do not retry, do not bypass, triage and fix.

**Step 6d — `vacuity-tier1:` — AUTHORITATIVE, but ANCHORED and GATED.** The reviewer's own summary
claiming no code changes, against a census we measured as non-empty, is T3 and must FAIL, not merely
note. But the naive form false-FAILs: matched anywhere in the transcript, a genuine review that merely
QUOTED the phrase was failed as vacuous — and this change's own diff carries the phrase in five-plus
files. Two properties make the strict version safe:

1. **Anchoring** — only the verdict/summary region (the lines carrying `Summary:`) is matched, never
   arbitrary finding bodies. No such region ⇒ `UNAVAILABLE`.
2. **Gating on `findings:`** — `NONE` ⇒ the reviewer is CLAIMING CLEANLINESS, so the phrase is a vacuity
   claim ⇒ **HARD FAIL**. `UNKNOWN` ⇒ **HARD FAIL** too: an unknowable state must never DISARM the
   check. `PRESENT*` ⇒ the reviewer demonstrably analysed the diff, so the phrase is discussion ⇒
   advisory `NOTICE`, which does not fail the run.

Why the relaxation is recorded rather than silent: the systemic cost of a false FAIL here is agents
learning to **WAIVE tier-1 FAILs**, which restores the original defect wholesale. A guard that cries
wolf is a guard that gets bypassed.

**Step 6e — `vacuity-tier2:` — corroborating, fail-closed only, and now actually alive.** The tier was
**permanently `UNAVAILABLE` on every real run** before this fix: the real payload's `token_usage` is a
**JSON-ENCODED STRING** (it must be decoded TWICE) and the output count is `total_output_tokens`, not
`output_tokens` — so a single decode read no counts, reported a non-failing `UNAVAILABLE`, and the guard
silently was not there. Hence three explicit states in `roborev-job-facts.py`:

| state | meaning | wrapper |
|---|---|---|
| `absent` | no token field at all | `UNAVAILABLE` (legitimate build difference) |
| `parsed` | counts readable | evaluate the thresholds |
| `unparseable` | a token field IS present, no alias resolved | **`FAIL` (drift)** |

`unparseable` is a FAIL, not a notice, precisely because that is how the tier was disarmed. The remedy
in the message is "add the alias to `INPUT/CACHED/OUTPUT_TOKEN_KEYS`", never "waive it". A drift FAIL
costs one re-run; a silently disarmed guard costs an unreviewed merge.

Calibration, and the two deliberate changes from the original spec:

```
VACUOUS baseline:  17,333–18,801 input, 0 cached, 21–56 output, 8s   (jobs 4651/4658/4659)
GENUINE, SMALL:    67,387 input, 43,520 cached, 2,232 output, 68s    (20 files, +2279)
GENUINE, LARGE:    398k–649k input, 314k–554k cached, 5.0k–6.3k out  (jobs 4652/4654/4656)
```

- `ROBOREV_VACUITY_MIN_INPUT_TOKENS = 25000` — **lowered from the originally specified 50,000**, and
  re-anchored on the **vacuous ceiling** rather than the genuine band, because the genuine band scales
  with diff size. 25,000 is ~1.33× above the highest vacuous run (18,801) and ~2.7× below the smallest
  genuine one (67,387). The original 50,000 was only 1.35× below that 67k run — one modestly smaller
  genuine diff away from an always-red guard.
- **The output-token floor was DROPPED as a FAIL condition** (kept as an advisory NOTICE at 200). A
  genuine CLEAN review and a vacuous one emit near-identical output: both are "No issues found" plus
  one sentence (~20–60 tokens; vacuous measured 21–56). The counts COLLIDE, so an output floor would
  false-FAIL precisely the case we care most about — a real review that is legitimately clean. It cannot
  discriminate, so it is reported and never asserted.
- `cached_input_tokens == 0` **stays** a FAIL, with its false-positive caveat documented (a genuinely
  cold cache can report 0). It is an accepted trade in the fail-closed direction, affordable now that
  `prompt-content` gives a deterministic primary check — tier 2 is no longer the only thing standing
  between us and a vacuous pass. Wall time is deliberately NOT asserted (host-dependent, #2642).

**Step 7 — Emit a compact, machine-greppable summary block.** One field per line — a reader greps a
single `^<key>: ` anchor, never a column offset. The as-built block, in contract order (pinned by
`scripts/tests/test_roborev_review_guard.sh`):

```
==== ROBOREV REVIEW SUMMARY ====
repo: <abs>
branch: <name>
base: <ref>
head-sha: <sha40>|-
reviewed-sha: <sha>|-
job: <N>|-
model: <m> | <m> (SUBSTITUTED — requested '<r>') | <m> (UNCONFIRMED — no model field in the job record) | -
census: <F> file(s), +<A>/-<D>   |  -
tokens: input=<i> cached=<c> output=<o|unknown>   |  UNAVAILABLE
push-assert:      PASS | SKIP | FAIL (detached HEAD) | FAIL (ls-remote failed: infra/auth)
                                    | FAIL (branch absent on remote <r>) | FAIL (unpushed commits)
census-check:     PASS | SKIP | FAIL (git diff failed) | FAIL (base '<ref>' unresolvable) | FAIL (empty census)
code-free:        PASS | SKIP | FAIL (code-free census: n/n files are documentation/specification text)
sha-assert:       PASS | SKIP | FAIL (roborev not on PATH) | FAIL (no parseable enqueue announcement)
                                    | FAIL (unparseable enqueue announcement)
                                    | FAIL (reviewed-sha does not match head-sha)
review-completed: PASS | SKIP | FAIL (transcript unreadable) | FAIL (job status '<s>' is not done)
                                    | FAIL (no terminal verdict marker)
prompt-content:   PASS (k/n census paths present) | FAIL (k/n census paths absent from the prompt)
                                    | UNAVAILABLE | SKIP
vacuity-tier1:    PASS | FAIL (vacuous verdict vs non-empty census)
                                    | NOTICE (phrase present in a findings-bearing review) | UNAVAILABLE | SKIP
vacuity-tier2:    PASS | FAIL (vacuous token signature)
                                    | FAIL (token accounting present but unparseable — drift) | UNAVAILABLE | SKIP
findings:         NONE | PRESENT | PRESENT (n) | UNKNOWN | SKIP
roborev-exit:     PASS | FINDINGS (exit N) | ERROR (exit N) | SKIP
log: <transcript path>
RESULT: PASS|FAIL|NOTHING-TO-REVIEW
```

Note `sha-assert: FAIL (roborev not on PATH)`: an absent binary is a pre-enqueue fail-closed condition
reported under the assert that would have consumed the announcement. Every per-check key participates in
ONE scan: a value starting `FAIL` / `FINDINGS` / `ERROR` fails the run; `PASS` / `SKIP` / `UNAVAILABLE` /
`NOTICE` never do (NOTICE is the advisory tier's value and is deliberately non-failing). A key whose
step was never reached reads `SKIP` — never a blank, and never mistakable for a pass.

Exit codes: `0` PASS, `1` FAIL, `3` NOTHING-TO-REVIEW, `2` **usage error**. Exit 2 is deliberately NOT a
verdict: it emits **no** summary block at all (a loud `ERROR:` naming the missing option, before any repo
identity is resolved and before anything is enqueued), because a `RESULT:` line for a run that never
happened would alias a usage error onto one of the three real outcomes — recreating the very
indistinguishability this change exists to eliminate. `--help` (exit 0) is likewise not a verdict. An
unexpected mid-run abort is caught by an `EXIT` trap that still emits the block with `RESULT: FAIL`: a
run that died without a verdict must never look like a run that was never made. Modeled on the gate's
summary-file contract: **an agent retains only this block, never raw roborev stdout**. The block name is
distinct from `AGENT-GATE SUMMARY` / `… LITE …` / `… DELTA …` so neither can be pasted as the other.

**Step 8 — Hygiene.** No wall-clock threshold asserts in the regression test's correctness path (#2642,
mechanized by `roborev-lints`); no unquoted interpolation of external output into a shell command;
`shellcheck 0.10.0` clean at info level with `-x` across all three shell files (the `-x` matters — it is
what resolves the sourced-fragment variables across the source boundary).

### Docs-only diffs cannot be roborev-certified

Trigger 3 is not a bug we can route around: roborev **structurally discards a code-free diff**. So the
sanctioned position is explicit — a docs/spec-only diff **cannot be certified by roborev at all**. The
wrapper FAILs it deterministically at `code-free:` **before enqueuing anything** (the reviewer's prose is
not involved), and the sanctioned substitute is **verification against primary sources**, recorded in the
PR body. For #2950 that was `git show cassandra-5.0.8:<path>`. A docs-only PR must **never** record
"roborev clean".

This dovetails with existing doctrine rather than fighting it: CLAUDE.md already treats a docs-only diff
specially for the gate (the #3042 CITE-AND-WAIVE rule). The parallel is deliberate — a docs-only diff
cannot be *certified* by a tool whose subject is compiled code, in either the gate's or roborev's case.

### Call-site migration — SIXTEEN surfaces, three obligation classes

Every roborev invocation in the agent surfaces and the fleet-facing doctrine routes through the wrapper,
and bare `--branch` becomes non-sanctioned prose. The surfaces are NOT homogeneous, and conflating them
would state an obligation four of them cannot satisfy:

- **Review-round sites (4)** — run a round themselves; must also state push-first and
  non-PASS-is-a-failed-round: `.claude/skills/flow-implement/SKILL.md` (the review-first step — the
  primary call site; *previously* documented the now **NON-SANCTIONED, historical**
  `roborev review --branch --base origin/main --agent codex --model gpt-5.6-sol --wait`),
  `.claude/agents/flow-closer.md` (the final confirmation pass — the **merge-gating** call site;
  *previously* documented a `/roborev-review-branch` form that does not exist),
  `.claude/skills/flow-address/SKILL.md` (the post-comment re-review), and `.claude/commands/worker.md`
  (the fleet's **unattended entry point**, which runs the implement loop's review step itself).
- **Prescribing sites (5)** — name the wrapper without running a round in-line:
  `.claude/agents/flow-lead.md` (stage table + doctrine bullet),
  `.claude/skills/ci-cd-validation/SKILL.md` + `.claude/skills/ci-cd-validation/merge-process.md`
  (the merge-readiness definition), `.claude/skills/flow-activate/SKILL.md` (the `tasks.md` it authors),
  and `.claude/commands/manager.md` (what "roborev clean" means for the workers it dispatches).
- **Non-invoking surfaces (4)** — no roborev invocation at all; each must SAY so and point at the
  wrapper: `.claude/skills/flow-finalize/SKILL.md` (the telemetry `--roborev-findings` counter + what
  "roborev clean" means in the ledger), `.claude/agents/rust-reviewer.md` (pre-roborev self-check
  classes — and it additionally flags a reintroduced bare `--branch`/range form as a **BLOCKER**),
  `.claude/agents/sstable-developer.md` and `.claude/agents/test-validator.md` (the `roborev-lints`
  lite component + never-invoke-directly).
- **Non-`.claude` doctrine surfaces (3)** — `website/src/content/docs/agents-developing/delivery-pipeline.md`,
  `docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md`. These were the
  most important discovery of the migration sweep: each carried the **INVERSE** instruction — "roborev
  follows this machine's configured agent … run it with no `--agent`/`--model` flags", with
  `delivery-pipeline.md` calling explicit agent/model **"never doctrine"**. Leaving them would have left
  the fleet's *published* guidance prescribing exactly the invocation this change forbids.

Plus the two AC4 doctrine surfaces (CLAUDE.md and the `roborev-findings` page) = **18 files** referencing
the wrapper. `.claude/hooks/issue-gate.sh` is deliberately out of scope: it already documents that no
hook path runs roborev at all (#2671) and has no invocation to migrate.

### Regression check + gate wiring

`scripts/tests/test_roborev_review_guard.sh` (1232 lines) — **hermetic**: a **stub `roborev`** first on
`PATH` replays the recorded real outputs (enqueue lines, verdict text, and the `show --json` payload
*including* the doubly-encoded `token_usage` string with `total_output_tokens` — reproducing that shape
verbatim is what keeps tier 2 honest), driven against throwaway `git init` fixtures each with its own
local bare `origin`. Fixture modes cover the fleet's real topologies: wide/`narrow` refspec, behind,
`narrow-upstream` (a remote named `upstream`), `unreachable` (ls-remote fails), `no-base`,
`deleted-remote` (mirror ref equals HEAD, branch deleted), docs-only, mixed, workflow-yaml. No network,
no real roborev, no datasets, no cargo; ~0.5s. **258 assertions across ~60 named cases, 27/27 mutation
kills.** A fixture-integrity guard fails if a narrow-refspec fixture ever grows a feature mirror ref —
otherwise it would silently stop testing the condition it exists for. python3 absence is a loud SKIP for
the structured-payload cases, never a silent pass.

**Wiring (concrete).** Both registration points in `scripts/agent-gate.sh` are used:

- `run_roborev_lints_cmd()` (component `roborev-lints`) is in **both** `LITE_COMPONENTS` and
  `COMPONENTS`, and already chains `check-workflow-injection.sh && check-no-wallclock-asserts.sh`. The
  guard test is appended there — this is what makes a regression **FAIL the fast `--lite` loop** rather
  than cost a review round, and it is thematically exact: `roborev-lints` is the component that
  mechanizes roborev-related delivery costs (#2656).
- `run_tooling_tests()` (the full-gate `tooling-tests` component) runs it too, following the
  `test_check_dockerfile_rust_pin.sh` / `test_check_skill_flag_tables.sh` guard pattern (FAIL the
  component on non-zero, with a named failure line).

**Live worktree probe (documented, not gate-run).** From a real `issue-<N>-*` worktree with a pushed
commit and the root checkout on `main`, run the wrapper and confirm `reviewed-sha == head-sha` (and
`!= origin/main`) in the block. Only this can prove the real external binary honours the explicit
`--repo`; it needs network + a live reviewer, so it lives in the wrapper's `--help` text (so procedure
and implementation cannot drift) and the doctrine page, with an instruction to re-run it after any
roborev version bump.

### Doctrine (ships in this change)

CLAUDE.md's roborev-invocation bullet (*Agent-Team Conventions*) and
`website/src/content/docs/agents-developing/roborev-findings.md` both state: the wrapper is the only
sanctioned invocation; **verify the reviewed SHA**; `"contains no code changes to review"` on a
non-empty diff is a **HARD FAIL**; docs-only diffs cannot be roborev-certified — plus the exit-code
contract and "any non-PASS `RESULT`, `NOTHING-TO-REVIEW` included, is a blocked merge". The page also
gains a row in its **mechanized-in-`--lite`** table for the new guard (a mechanized class absent from
that table gets hand-checked forever). Publication acceptance follows CLAUDE.md's rule: **grep the
served page for a new distinctive phrase**; a `200` is not proof (observed CDN staleness ≈3 minutes).

## Why the token thresholds do NOT violate the no-heuristics mandate (#28)

Worth stating plainly, because "thresholds" reads like a heuristic at a glance. The no-heuristics
mandate governs **on-disk TYPE/format inference from byte patterns in the SSTable read path** — never
guessing a CQL type or a format version from data content instead of authoritative metadata (schema,
else `Statistics.db`). It says nothing about, and is not weakened by, **review-tooling liveness
detection**, which is a property of our own CI process, not of Cassandra's on-disk format.

Three structural properties keep this clean anyway, and the final architecture strengthens all three:

1. **The verdict is carried by DETERMINISTIC, threshold-free checks** compared against locally obtained
   oracles — the review-tooling analogue of "authoritative metadata only". Token accounting is the
   third-ranked signal, not the primary one.
2. **Tier 2's only possible action is to FAIL CLOSED.** It can never manufacture a pass, relax another
   check's FAIL, or infer that a review *did* happen. Its worst-case error is a false FAIL, whose cost
   is one re-run — bounded, visible and self-correcting, and preferable to the status quo, whose failure
   mode is merging unreviewed code. The `cached == 0` clause is the most false-positive-prone term and
   is an accepted, deliberate trade, recorded so a future recalibration knows it was a choice.
3. **The thresholds are evidenced, bounded and named**, with the measured table cited beside them, and
   every failure prints observed-vs-threshold — the opposite of an unexplained magic number. Where a
   threshold could NOT discriminate (output tokens), it was **dropped** rather than kept for
   appearances.

## Alternatives considered (and why the recommendation beat them)

1. **Register worktrees with `roborev repo` so `--branch` resolves correctly.** *Rejected:* there **is
   no `add` subcommand** — repos self-register on first use, so there is nothing to call. Even if there
   were, it would fix only trigger 1: triggers 2 and 3 would still return an indistinguishable "No
   issues found".
2. **Patch/fork `roborev` upstream.** *Rejected for this change:* the binary is outside this repo's
   control and the fleet is exposed on **every** issue right now — a guard we own ships today. Noted as
   a possible **upstream follow-up** (worktree-aware `--branch`; a non-zero exit when a code-free diff
   is discarded). The wrapper stays correct and cheap after such a fix lands.
3. **Run roborev from the root checkout instead of the worktree.** *Rejected:* it breaks 1:1:1:1
   isolation and requires **commandeering the shared root** — explicitly forbidden (a closer that
   switched the shared root's branch once stranded root off `main` and broke every session on the box).
   It also serializes concurrent lanes onto one checkout.
4. **PROSE MATCHING AS THE PRIMARY SIGNAL** (the original design). *Tried and REJECTED with evidence:*
   it only sees the half of the defect space where the reviewer *tells* you it saw nothing. A job that
   never finished, a provider `400`, a `status: failed`, and a reviewer that never received the diff all
   emit **no phrase at all** — and all four reached `RESULT: PASS`, because absence of a phrase was
   treated as proof of a review. Replaced by `review-completed` (positive evidence) + `prompt-content`
   (deterministic "did it GET the diff") with the prose check retained, anchored and gated, as
   corroboration.
5. **Token-threshold-only detection.** *Rejected as a primary signal:* non-deterministic and
   recalibrating with every model/prompt change (`gpt-5.6-sol` is codex's own moving default, not a
   config pin — it already shifted once on a version bump). Demoted to a corroborating tier where a
   drifted threshold can only cost a re-run.
6. **An OUTPUT-token floor as a FAIL condition** (originally specified at 200). *Tried and REJECTED with
   evidence:* a genuine CLEAN review and a vacuous one emit near-identical output counts (~20–60 for
   both; vacuous measured 21–56), so the signal cannot discriminate them and the floor would false-FAIL
   precisely the case that matters most. Kept as an advisory NOTICE only.
7. **A local mirror-ref (`refs/remotes/origin/<branch>`) push assert.** *Tried and REJECTED with
   evidence:* CQLite clones carry a NARROW fetch refspec
   (`+refs/heads/main:refs/remotes/origin/main`), so a feature branch's mirror ref is **never** created
   however often the branch is pushed. The first implementation false-FAILed **100% of the fleet** —
   which would have made the only sanctioned invocation unusable and pushed agents straight back to the
   bare `--branch` form. `git ls-remote` asks the REMOTE, which is the authority.
8. **A mirror-ref FAST PATH in front of `ls-remote`** (short-circuit when the cached ref happens to
   equal HEAD). *Tried and REJECTED with evidence:* a cached ref survives a **force-push or an outright
   deletion** of the remote branch, so it can equal HEAD while the remote no longer has the commit —
   enqueueing a review of a commit the reviewer cannot fetch, i.e. a vacuous-review setup. `ls-remote`
   costs ~1s, and not trusting local proxies is this wrapper's whole point. Removed entirely.
9. **Trust the verdict text.** *Rejected:* that is precisely the defect. "No issues found" is emitted
   identically whether 650k tokens of real diff were reviewed or zero bytes were.
10. **Detect the empty diff by asking roborev what it saw** (e.g. parse the job's reported diffstat).
    *Rejected:* it re-derives the answer from the same untrusted source. The census must be computed on
    our side with `git`, or the check is circular — the same reason a CQLite `file:line` is never format
    authority.
11. **Treat a `requested_model` ≠ `model` substitution as a FAIL.** *Rejected:* roborev legitimately
    resolves model aliases, so this would be red on healthy runs, and an always-red guard is the failure
    mode that gets guards bypassed (hit twice in this change's own rounds). Surfaced as a loud NOTICE
    instead, so a substitution can never be SILENT while integrity stays carried by the deterministic
    checks.
12. **Treat a present-but-unparseable token payload as a NOTICE.** *Rejected:* that is precisely how the
    tier was silently disarmed for its entire life before this fix (a double-encoded string and a
    renamed output field degraded it to a non-failing `UNAVAILABLE` while the real counts were the
    vacuous baseline). A drift FAIL costs one re-run after a one-line alias addition; a silently
    disarmed guard costs an unreviewed merge.
