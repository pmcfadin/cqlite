# roborev-review-guard — delta for roborev-vacuous-review-guard (issue #2964)

**Acceptance-criterion → requirement map** (issue #2964's numbered ACs):

| AC | Requirement(s) |
|----|----------------|
| 1 — an empty resolved diff on a non-empty requested range must fail loudly, never "No issues found" | *A non-empty local diff census with a vacuous review verdict is a hard failure*; *A genuinely empty census reports NOTHING-TO-REVIEW, never a pass*; *The wrapper emits a machine-greppable summary block with a terminal verdict*; *A non-zero exit from the roborev process is a hard failure under its own greppable key* |
| 2 — invoke with explicit SHA + explicit `--repo`, and assert the enqueued SHA equals branch HEAD | *The sanctioned invocation is by explicit SHA and explicit repository path*; *The reviewed SHA is asserted against branch HEAD*; *Every flow-\* roborev call site routes through the sanctioned wrapper* |
| 3 — push the implementation commit before reviewing | *The branch is asserted pushed before any review is requested* |
| 4 — doctrine updated (CLAUDE.md + the `agents-developing/roborev-findings` page) | *Doctrine records the verify-the-reviewed-SHA rule and the hard-fail verdict text* |
| 5 — a regression check proves a worktree-launched review reviews the worktree's HEAD | *A hermetic regression check pins every vacuity trigger and is wired into the agent gate*; *A documented live worktree probe proves the worktree's HEAD is what gets reviewed* |

## ADDED Requirements

### Requirement: A non-empty local diff census with a vacuous review verdict is a hard failure
The sanctioned roborev wrapper SHALL compute a **local diff census** — the files changed and lines
added/removed for `<base>...HEAD`, obtained from `git` in the target repository — and SHALL treat that
locally-computed census as the authoritative statement of what must be reviewed. When the census is
NON-empty, the wrapper SHALL FAIL LOUDLY (non-zero exit and an explicit message) rather than report a
pass if EITHER of the following holds:

- **Tier 1 (primary, deterministic, threshold-free):** the review output claims there are no code
  changes to review (e.g. matching `contains no code changes to review` or `no code changes`,
  case-insensitively).
- **Tier 2 (corroborating, bounded token accounting):** the job's reported token accounting shows an
  input token count below the named vacuity input threshold, OR a cached-input count of zero, OR an
  output token count below the named vacuity output threshold.

Tier 1 SHALL be authoritative: it SHALL NOT be relaxed, overridden, or skipped by tier 2's outcome or
availability. Tier 2's only permitted effect SHALL be to fail closed — it SHALL NEVER cause a run to
pass. The tier-2 thresholds SHALL be named constants declared at the top of the wrapper with the
measured evidence cited in a comment, and every tier-2 failure message SHALL print the observed values
next to the threshold that tripped. When token accounting is unavailable from the installed roborev
build, the wrapper SHALL record a degraded-signal notice in its summary block and SHALL still apply
tier 1 — never a silent skip.

#### Scenario: A "no code changes" verdict against a non-empty census fails loudly
- **GIVEN** a branch whose local census against the base is non-empty (for example 5 files, +167/−63)
- **WHEN** the wrapper runs and the review returns "No issues found" with a summary stating the diff contains no code changes to review
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, its message names the vacuous-verdict-vs-census contradiction and prints the census, and the run is NOT reportable as "roborev clean"

#### Scenario: The vacuous token signature against a non-empty census fails loudly
- **GIVEN** a branch whose local census against the base is non-empty
- **WHEN** the review completes without a "no code changes" phrase, but the job's token accounting reports an input count below the vacuity input threshold, or zero cached input, or an output count below the vacuity output threshold
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, and the message prints each observed token value beside the named threshold constant that tripped

#### Scenario: A genuine review with healthy accounting passes
- **GIVEN** a branch whose local census against the base is non-empty, whose branch is pushed, and whose review enqueued the branch HEAD
- **WHEN** the review returns a verdict containing no "no code changes" claim and reports token accounting above the thresholds (for example ~500k input, ~387k cached, ~6.3k output)
- **THEN** the wrapper exits zero with `RESULT: PASS` and every per-check line in the summary block reads PASS

#### Scenario: Unavailable token accounting degrades visibly and tier 1 still governs
- **GIVEN** a roborev build from which the wrapper cannot obtain token accounting for the job
- **WHEN** the wrapper evaluates vacuity on a non-empty census
- **THEN** the summary block records `vacuity-tier2: UNAVAILABLE` as an explicit degraded-signal notice, the tier-1 check is still applied and can still FAIL the run, and the unavailability alone never turns a FAIL into a PASS nor is recorded as a silent skip

### Requirement: A genuinely empty census reports NOTHING-TO-REVIEW, never a pass
When the local diff census for `<base>...HEAD` is genuinely empty, the wrapper SHALL NOT invoke a review
and SHALL exit with a DISTINCT `NOTHING-TO-REVIEW` status — a non-zero exit code distinct from the
failure exit code — that is explicitly NOT a pass. A `NOTHING-TO-REVIEW` outcome SHALL NOT be recordable
as "roborev clean" by any caller.

#### Scenario: An empty census yields NOTHING-TO-REVIEW rather than PASS
- **GIVEN** a pushed branch whose diff against the base is genuinely empty (no files changed)
- **WHEN** the wrapper runs
- **THEN** it does not enqueue a review, its summary block terminates in `RESULT: NOTHING-TO-REVIEW`, and it exits with a non-zero code distinct from the FAIL exit code

#### Scenario: NOTHING-TO-REVIEW is distinguishable from PASS by exit code alone
- **WHEN** a caller inspects only the wrapper's exit status
- **THEN** the PASS, FAIL, and NOTHING-TO-REVIEW outcomes are three distinct exit codes, so a caller can never mistake "there was nothing to review" for "it was reviewed and clean"

### Requirement: The sanctioned invocation is by explicit SHA and explicit repository path
The wrapper SHALL invoke roborev with an EXPLICIT commit SHA (the branch HEAD) and an EXPLICIT absolute
repository path, and SHALL NEVER use the bare `--branch` form (which resolves against the root checkout
from inside a git worktree) nor the two-positional commit-range form (which has been observed to enqueue
a commit that is neither endpoint). The wrapper SHALL require BOTH the reviewer agent and the reviewer
model to be supplied, refusing to run with only one of them.

#### Scenario: The invocation names the HEAD sha and an absolute repo path
- **WHEN** the wrapper invokes roborev
- **THEN** the command line carries the resolved HEAD sha as an explicit argument and an absolute `--repo` path for the target repository, and carries neither a bare `--branch` argument nor two positional commit arguments

#### Scenario: Supplying only an agent or only a model is a usage error
- **GIVEN** an invocation that supplies a reviewer agent but no reviewer model (which would inherit a mismatched model from the repository's roborev config and fail as a silent-looking review outage)
- **WHEN** the wrapper runs
- **THEN** it refuses with a non-zero exit and a message naming the missing option, before any review is enqueued

### Requirement: The reviewed SHA is asserted against branch HEAD
The wrapper SHALL parse the enqueued-job announcement (`Enqueued job <N> for <sha>`) and SHALL require
the announced sha to prefix-match the branch HEAD sha. A mismatch SHALL abort the round with a non-zero
exit. When the mismatched sha resolves to the base ref (for example `origin/main`), the failure message
SHALL say so explicitly, because that equality is the signature of the worktree `--branch` resolution
trigger. An absent or unparseable enqueue announcement SHALL also be a failure, never a skipped check.

#### Scenario: An enqueued sha equal to the base ref aborts and names the base
- **GIVEN** a worktree branch whose HEAD is `4e7ab591e` and whose base `origin/main` is `39900e4db`
- **WHEN** the review announces `Enqueued job N for 39900e4db`
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, and the message states that the enqueued sha equals the base ref `origin/main` and therefore no branch change was reviewed

#### Scenario: An enqueued sha that is neither endpoint aborts
- **GIVEN** a branch HEAD of `989d7d2c3` and a base of `89fdbb895`
- **WHEN** the review announces `Enqueued job N for 90a17d376`, which matches neither the HEAD nor the base
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, and the message prints the announced sha beside the expected HEAD sha

#### Scenario: A missing enqueue announcement fails closed
- **WHEN** the review output contains no parseable `Enqueued job <N> for <sha>` line
- **THEN** the wrapper exits non-zero with `RESULT: FAIL` because the reviewed sha is unverifiable, and does not report a pass

#### Scenario: A matching enqueued sha satisfies the assert
- **WHEN** the announced sha prefix-matches the branch HEAD sha
- **THEN** the summary block records `sha-assert: PASS` with both the head sha and the reviewed sha printed

### Requirement: The branch is asserted pushed before any review is requested
Before enqueuing a review, the wrapper SHALL assert that the remote tracking ref for the current branch
(`origin/<branch>`) exists and equals the local HEAD. If it does not, the wrapper SHALL FAIL with a
non-zero exit and name the unpushed commits, because an unpushed implementation commit is itself a cause
of an empty resolved diff.

#### Scenario: An unpushed commit fails before a review is enqueued
- **GIVEN** a branch with one local commit that has not been pushed, so `origin/<branch>` is behind HEAD
- **WHEN** the wrapper runs
- **THEN** it exits non-zero with `RESULT: FAIL`, its message names the unpushed commit(s), and no review job is enqueued

#### Scenario: A missing remote branch fails before a review is enqueued
- **GIVEN** a branch that has never been pushed, so `origin/<branch>` does not exist
- **WHEN** the wrapper runs
- **THEN** it exits non-zero with `RESULT: FAIL` naming the missing remote branch, and no review job is enqueued

### Requirement: The wrapper emits a machine-greppable summary block with a terminal verdict
The wrapper SHALL emit a single compact `==== ROBOREV REVIEW SUMMARY ====` block on every **VERDICT**
exit path — a pass, any failed check, or an empty census — carrying one field per line under the
greppable keys `repo:`, `branch:`, `base:`, `head-sha:`, `reviewed-sha:`, `job:`, `census:`, `tokens:`,
`push-assert:`, `census-check:`, `sha-assert:`, `vacuity-tier1:`, `vacuity-tier2:`, `log:`, and a
terminal `RESULT: PASS|FAIL|NOTHING-TO-REVIEW`. A per-check key whose step was never reached SHALL carry
an explicit `SKIP` rather than a blank, so an unreached check can never read as a pass. The block's name
SHALL be distinct from the agent gate's summary block names so neither can be pasted as the other. The
wrapper SHALL exit non-zero on any outcome other than PASS, and SHALL be usable such that a caller
retains ONLY this block and never the raw review transcript (which SHALL be written to the log path
named in the block's `log:` field).

A **USAGE ERROR is NOT a verdict.** When a required option is missing or invalid (notably `--agent`
without `--model`, or the reverse), the wrapper SHALL emit **NO summary block at all**: it SHALL print a
loud `ERROR:` line naming the missing or invalid option and SHALL exit with the dedicated usage code
`2`, before any repository identity is resolved and before anything is enqueued. This omission is
DELIBERATE and SHALL NOT be "fixed" by emitting a block: the three `RESULT:` values are reserved for the
three real outcomes, so a `RESULT:` line for a run that never happened would ALIAS a usage error onto a
genuine verdict — precisely the indistinguishability this capability exists to eliminate. The `--help`
path (exit `0`) is likewise not a verdict and SHALL emit no block. The "exactly one block" obligation
therefore scopes to the three verdict paths; the verdict paths SHALL be exhaustive for them, including
an unexpected mid-run abort, which SHALL emit the block with `RESULT: FAIL` rather than terminate
silently.

#### Scenario: Every verdict run emits exactly one block with a terminal RESULT
- **WHEN** the wrapper finishes on a verdict path (pass, any failed check, or an empty census)
- **THEN** it emits exactly one `==== ROBOREV REVIEW SUMMARY ====` block whose last line is `RESULT:` followed by exactly one of `PASS`, `FAIL`, or `NOTHING-TO-REVIEW`

#### Scenario: A usage error emits no block and exits with its own distinct code
- **GIVEN** an invocation supplying `--agent` but not `--model` (or `--model` but not `--agent`)
- **WHEN** the wrapper runs
- **THEN** it prints an `ERROR:` line naming the missing option, emits NO `==== ROBOREV REVIEW SUMMARY ====` block and NO `RESULT:` line at all, enqueues nothing, and exits `2` — a code distinct from PASS (`0`), FAIL (`1`), and NOTHING-TO-REVIEW (`3`), so a usage error can never be read as any of the three verdicts

#### Scenario: The block carries the census, the reviewed sha, and the token accounting
- **WHEN** a review was enqueued and completed
- **THEN** the block reports the base ref and census (files changed, lines added, lines removed), the head sha and the reviewed sha, the job id, and either the observed input/cached/output token counts or an explicit unavailable marker

#### Scenario: The block cannot be confused with an agent-gate summary
- **WHEN** the block is compared with the agent gate's `AGENT-GATE SUMMARY`, `AGENT-GATE LITE SUMMARY`, and `AGENT-GATE DELTA SUMMARY` blocks
- **THEN** its header is distinct from all three, so a roborev summary can never be pasted as a gate verdict nor a gate summary recorded as a review verdict

#### Scenario: A non-PASS outcome exits non-zero
- **WHEN** the terminal `RESULT:` is `FAIL` or `NOTHING-TO-REVIEW`
- **THEN** the wrapper's process exit code is non-zero

### Requirement: A non-zero exit from the roborev process is a hard failure under its own greppable key
A non-zero exit status from the underlying `roborev` process SHALL be a hard, fail-closed failure: it
SHALL force the terminal `RESULT: FAIL` on its own, independently of every other check's outcome, and
SHALL NEVER be reportable as "roborev clean". Because a caller retains ONLY the summary block and reads
it by grepping the per-check keys, this failure cause SHALL be surfaced in the block under its OWN
greppable key `roborev-exit:` — value `PASS` when the process exited zero, otherwise a `FAIL` carrying
the OBSERVED non-zero exit code — placed with the other per-check keys, ahead of the terminal `RESULT:`.
It SHALL participate in the same per-check scan that computes the terminal verdict. A prose detail line
alone SHALL NOT satisfy this requirement: without the key, a reader sees every per-check key reading
`PASS` beside a `RESULT: FAIL` and cannot attribute the failure, which is the one failure cause a
grep-based reader would otherwise be unable to name.

#### Scenario: A non-zero roborev exit FAILs the run and names itself under its own key
- **GIVEN** a pushed branch with a non-empty census whose push, census, sha, and both vacuity checks all pass
- **WHEN** the `roborev` process itself exits non-zero (for example `1`)
- **THEN** the block reports `roborev-exit: FAIL` carrying the observed exit code, the terminal `RESULT:` is `FAIL`, the wrapper exits non-zero, and the run is NOT reportable as "roborev clean"

#### Scenario: A zero roborev exit records the key as PASS
- **WHEN** the `roborev` process exits zero
- **THEN** the block reports `roborev-exit: PASS`, and that key alone never turns any other check's FAIL into a pass

### Requirement: A code-free diff cannot be certified by roborev
Because roborev structurally discards a code-free diff, a diff consisting only of documentation,
specification, or workflow text SHALL NOT be certifiable by roborev at all: the wrapper SHALL FAIL such a
run as vacuous, and no docs-only change SHALL record "roborev clean". The sanctioned substitute SHALL be
verification against primary sources, recorded in the pull request.

#### Scenario: A markdown-only diff is failed as vacuous, not passed
- **GIVEN** a pushed branch whose census against the base is 5 files, +167/−63, all markdown
- **WHEN** the wrapper runs and the correctly-targeted review returns "No issues found" with a summary stating the diff contains no code changes to review
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, and the failure is attributed to the code-free-diff condition rather than reported as a clean review

#### Scenario: The sanctioned substitute for a docs-only change is primary-source verification
- **GIVEN** a docs-only change that cannot be roborev-certified
- **WHEN** the change is prepared for merge
- **THEN** doctrine directs the author to record primary-source verification in the pull request (for example reading the pinned Cassandra source at the `cassandra-5.0.8` tag that the documentation describes) instead of recording "roborev clean"

### Requirement: Every flow-* roborev call site routes through the sanctioned wrapper
Every roborev invocation documented in the delivery-pipeline skills and agents SHALL be expressed as a
call to the sanctioned wrapper, and the bare `--branch` form SHALL be documented as non-sanctioned
everywhere it is mentioned. The affected surfaces fall into TWO classes carrying DIFFERENT obligations,
because four of them contain no roborev invocation at all and an obligation to "invoke the wrapper"
would be unsatisfiable for them:

**(a) Invocation sites** — surfaces whose documented procedure runs the wrapper. Each SHALL express its
roborev step as a call to `scripts/flow/roborev-review.sh`, SHALL pass BOTH the reviewer agent and the
reviewer model, and SHALL NOT instruct a bare `roborev review --branch` invocation nor the
two-positional commit-range form. They subdivide by what the surface itself does:

- **Review-round sites** — they run a review round in-line: `.claude/skills/flow-implement/SKILL.md`
  (review-first, the primary call site), `.claude/agents/flow-closer.md` (the final merge-gating
  confirmation pass), `.claude/skills/flow-address/SKILL.md` (the post-comment re-review). Each of
  these SHALL ADDITIONALLY state that the branch is pushed BEFORE the review is requested, and SHALL
  treat ANY non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` INCLUDED — as a failed review round and a
  blocked merge, never as "roborev clean".
- **Prescribing sites** — they name the wrapper as the invocation to be used without running a round
  in-line: `.claude/agents/flow-lead.md` (the stage table and the roborev doctrine bullet),
  `.claude/skills/ci-cd-validation/SKILL.md` and `.claude/skills/ci-cd-validation/merge-process.md`
  (the merge-readiness definition), `.claude/skills/flow-activate/SKILL.md` (the roborev step of the
  `tasks.md` it authors). Each SHALL name the wrapper as the ONLY sanctioned invocation, and any
  merge-readiness or finalizability rule it states SHALL require a terminal `RESULT: PASS` and SHALL
  NOT accept `NOTHING-TO-REVIEW` or `FAIL`.

**(b) Non-invoking surfaces** — surfaces that reference roborev (the `roborev-lints` gate component,
the pre-roborev self-check pointer, the telemetry `--roborev-findings` counter) but contain NO roborev
invocation: `.claude/skills/flow-finalize/SKILL.md`, `.claude/agents/rust-reviewer.md`,
`.claude/agents/sstable-developer.md`, `.claude/agents/test-validator.md`. Each SHALL state explicitly
that it never invokes roborev directly, SHALL point at `scripts/flow/roborev-review.sh` as the only
sanctioned invocation, and SHALL NOT contradict any of the four doctrine rules (wrapper-only; verify
the reviewed SHA; a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL; a
docs-only diff cannot be roborev-certified). `.claude/agents/rust-reviewer.md` SHALL ADDITIONALLY
require that a diff reintroducing a bare `roborev review --branch` or the two-positional range form is
flagged as a **BLOCKER**.

No surface in either class SHALL document a bare `--branch` or two-positional-range roborev invocation
as sanctioned.

#### Scenario: Every invocation site calls the wrapper and no surface documents a bare --branch invocation
- **WHEN** the six class-(a) invocation surfaces are inspected for roborev invocations
- **THEN** each expresses its roborev step as a `scripts/flow/roborev-review.sh` call passing both the reviewer agent and the reviewer model, none instructs a bare `roborev review --branch` invocation or the two-positional commit-range form, and the bare `--branch` form is explicitly marked non-sanctioned wherever it appears across all ten surfaces

#### Scenario: Each review-round site states push-first and treats any non-PASS RESULT as a failed round
- **WHEN** `.claude/skills/flow-implement/SKILL.md`, `.claude/agents/flow-closer.md`, and `.claude/skills/flow-address/SKILL.md` are inspected
- **THEN** each states that the branch is pushed before the review is requested, and each states that any non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and a blocked merge rather than "roborev clean"

#### Scenario: Each prescribing site names the wrapper and requires RESULT PASS for readiness
- **WHEN** `.claude/agents/flow-lead.md`, `.claude/skills/ci-cd-validation/SKILL.md`, `.claude/skills/ci-cd-validation/merge-process.md`, and `.claude/skills/flow-activate/SKILL.md` are inspected
- **THEN** each names `scripts/flow/roborev-review.sh` as the only sanctioned invocation with both flags, and every merge-readiness or finalizability rule any of them states requires a terminal `RESULT: PASS` and rejects both `NOTHING-TO-REVIEW` and `FAIL`

#### Scenario: Each non-invoking surface says so and points at the wrapper
- **GIVEN** the four class-(b) surfaces, whose only roborev references are the `roborev-lints` gate component, the pre-roborev self-check pointer, and the telemetry `--roborev-findings` counter
- **WHEN** `.claude/skills/flow-finalize/SKILL.md`, `.claude/agents/rust-reviewer.md`, `.claude/agents/sstable-developer.md`, and `.claude/agents/test-validator.md` are inspected
- **THEN** each states that it never invokes roborev directly, each points at `scripts/flow/roborev-review.sh` as the only sanctioned invocation, none contradicts any of the four doctrine rules, and `.claude/agents/rust-reviewer.md` additionally requires flagging a reintroduced bare `--branch` or two-positional range form as a BLOCKER

#### Scenario: The merge-gating confirmation pass routes through the wrapper
- **GIVEN** the `flow-closer` agent's final roborev confirmation pass, whose verdict gates arming auto-merge
- **WHEN** that step is inspected
- **THEN** it invokes the sanctioned wrapper and treats a non-PASS terminal `RESULT` (including `NOTHING-TO-REVIEW`) as a blocked merge rather than a clean review

#### Scenario: Both agent and model remain required at every invocation site
- **WHEN** each class-(a) invocation site is inspected
- **THEN** it passes both the reviewer agent and the reviewer model, preserving the documented trap that supplying only one inherits a mismatched model from the repository roborev config and fails as a silent-looking review outage

### Requirement: Doctrine records the verify-the-reviewed-SHA rule and the hard-fail verdict text
CLAUDE.md's roborev-invocation guidance and the published `agents-developing/roborev-findings` page
SHALL both state, in this same change: (a) the wrapper is the only sanctioned roborev invocation;
(b) the reviewed SHA must be verified against branch HEAD; (c) a "contains no code changes to review"
verdict on a non-empty diff is a HARD FAIL, never a pass; and (d) a docs-only diff cannot be
roborev-certified. The published page SHALL be accepted by confirming the NEW CONTENT is served — not by
an HTTP 200 — because the CDN can serve the previous page for minutes after a successful deploy.

#### Scenario: Both doctrine surfaces carry all four rules
- **WHEN** CLAUDE.md and `website/src/content/docs/agents-developing/roborev-findings.md` are inspected after this change
- **THEN** both state that the wrapper is the only sanctioned invocation, that the reviewed SHA must be verified against branch HEAD, that a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL, and that a docs-only diff cannot be roborev-certified

#### Scenario: Publication is accepted by the served content, not a status code
- **WHEN** the published `agents-developing/roborev-findings` page is verified after deployment
- **THEN** acceptance is established by fetching the page and matching a distinctive phrase introduced by this change, and an HTTP 200 without that phrase is treated as not-yet-published rather than as done

### Requirement: A hermetic regression check pins every vacuity trigger and is wired into the agent gate
A regression check SHALL exercise the wrapper hermetically — using a stub `roborev` on `PATH` that
replays recorded real outputs, with no network and no live reviewer — and SHALL assert that the wrapper:
(a) FAILs when the enqueued sha equals the base ref; (b) FAILs when the enqueued sha is neither endpoint;
(c) FAILs on a "contains no code changes to review" verdict against a non-empty census; (d) FAILs on the
vacuous token signature; (e) FAILs on an unpushed branch; (f) PASSes a genuine review with a matching sha
and healthy token accounting; and (g) reports `NOTHING-TO-REVIEW` rather than PASS on a genuinely empty
census. The check SHALL be registered in the agent gate's shell-tooling component set such that it runs
in the fast `--lite` loop as well as the full gate, so a regression FAILs the fast loop rather than
costing a review round. The check SHALL contain no wall-clock threshold assertion in its correctness path.

#### Scenario: All seven trigger cases are asserted
- **WHEN** the regression check runs
- **THEN** it asserts each of the seven cases (a) through (g) above, each against the wrapper's terminal `RESULT` and exit code

#### Scenario: The check is hermetic
- **WHEN** the regression check runs on a machine with no network access and no real roborev binary installed
- **THEN** it still runs to completion using the stub reviewer and throwaway git fixtures, requiring no dataset corpus, no live reviewer, and no network

#### Scenario: A regression fails the fast loop
- **GIVEN** a change that removes or weakens one of the wrapper's asserts
- **WHEN** `scripts/agent-gate.sh --lite` runs
- **THEN** the component that hosts the regression check FAILs, so the fast loop catches the regression rather than a later review round

#### Scenario: The check also runs in the full gate
- **WHEN** the full `scripts/agent-gate.sh` runs
- **THEN** the regression check executes as part of the shell-tooling component set and a failure FAILs that component and the run

### Requirement: A documented live worktree probe proves the worktree's HEAD is what gets reviewed
The change SHALL include a documented live probe, runnable against the real roborev binary from inside a
real issue worktree, that proves a worktree-launched review reviews the WORKTREE's HEAD rather than the
root checkout's commit. The probe SHALL be documented rather than executed by the gate, because it
requires network access and a live reviewer.

#### Scenario: The probe establishes reviewed-sha equals the worktree HEAD
- **GIVEN** a real issue worktree, on its own branch, with its implementation commit pushed, while the root checkout sits on `main`
- **WHEN** the documented probe runs the wrapper from inside that worktree
- **THEN** the summary block's reviewed sha equals the worktree branch's HEAD sha and does NOT equal the base ref, demonstrating the explicit-repo invocation defeats the root-checkout resolution trigger

#### Scenario: The probe is documented, not gate-run
- **WHEN** the agent gate's component set is inspected
- **THEN** the live probe is not among its components, and the probe's procedure and expected summary-block values are recorded in the wrapper's usage documentation and the doctrine page instead
