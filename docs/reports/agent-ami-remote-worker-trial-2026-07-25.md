# agent-ami remote-worker trial — full report

**Date:** 2026-07-24 → 2026-07-25
**Operator:** flow-lead (attended session, laptop `pmcfadin-ltmcx74`)
**Workload:** CQLite issues #2876 (MADV_RANDOM scan-plane split) and #2877 (scan chunk coalescing),
run on two dedicated remote EC2 workers instead of locally.
**Tooling under test:** `agent-ami` (`up`/`list`/`ssh`/`down`/`bake`/`auth`), profile-driven onboarding.
**Instances:** `i-0845337d7cca253c6` (box 0, #2876), `i-0999d9129a242eaf9` (box 1, #2877).
Both `c7i.4xlarge`, us-west-1, ~$0.85/hr each (~$1.70/hr for the pair). Runtime ≈ 10.5 h wall.
**Approximate spend:** ~$17–18 of EC2.

---

## 1. Verdict

**The concept works. The unattended lifecycle does not.**

Two remote workers each produced a real, gate-certified fix to a live performance regression, on
hardware that never touched the operator's laptop. Neither reached merge unattended. Every long
verification step required a human to notice a dead worker and relaunch it — four relaunches across
the two issues.

The trial is best read as **a successful compute experiment and a failed autonomy experiment**. The
value delivered was real; the supervision cost exceeded the compute saved.

### What was delivered

| Issue | Branch / PR | Gate | State at teardown |
|---|---|---|---|
| #2876 | `issue-2876-scan-read-plane-split` → PR #2882 | full `agent-gate.sh` **PASS 29/29** @ `57c9a90` | 3 roborev BLOCKERs open |
| #2877 | `issue-2877-scan-chunk-coalescing` → PR #2880 | full `agent-gate.sh` **PASS 29/29** @ `eaaf4d9` | round-1 findings fixed; round-2 findings open |

Both branches are on `origin`. No work was lost in teardown (see finding **F1** — it nearly was).

### Cost accounting, honestly

~$17 of EC2 bought two branches with ~1,100 lines of net change, each carrying a 29-component full
gate PASS that would otherwise have occupied the operator's laptop for ~50 minutes of wall-clock
apiece (and blocked all other local gate work, since the machine serializes full gates at
`CQLITE_GATE_MAX_CONCURRENCY=1`). That is a **good trade for the gate compute**.

It is a **bad trade for the endgame**: the remaining work at teardown was ~30 minutes of mechanical
local edits per issue. Continuing to pay $1.70/hr for a loop the operator had to babysit anyway was
the actual waste — not the experiment.

---

## 2. What happened, in order

1. **Setup (~90 min, mostly friction).** SSO profile confusion, a missing SSH key, a silently
   no-op'd onboard, and a missing repo manifest — findings F2–F6 below. Both boxes were only
   genuinely ready after manual intervention on each.
2. **First gate run FAILED on pristine `main`** (`core-tests`, `cli-tests`). This turned out to be a
   **real repo bug, not a tooling bug**: `test-data/scripts/fetch-datasets.sh` deletes git-tracked
   commitlog fixtures. Filed as **#2878** and inoculated both boxes with a `WORKER-NOTE.md`. The
   trial found a latent defect that local runs had never surfaced, because local checkouts already
   had the fixtures present.
3. **Both agents implemented their fixes correctly and passed the full gate.** This is the part that
   worked. #2876 produced `57c9a90`; #2877 produced `b3bb392`.
4. **Both agents then stalled on `ScheduleWakeup`** (F7) — no scheduler exists in a headless
   `claude -p` session, so the call never fires. #2876 idled **~8 hours**. Recovered via
   `claude --continue -p '<explicit state>'`.
5. **roborev was unusable on the boxes** (F8) — `claude-code` "Not logged in", `codex` 401. The owner
   decided the lead would run roborev locally and feed findings back. Round 1 found **3 BLOCKERs per
   issue** — every one of them real, and none of them catchable by the gate.
6. **Both agents then failed the same way twice more** (F9): `claude -p` exits at end-of-turn and
   kills its own pending background gate-poll task. Box 0 opened its PR at the *unfixed* commit and
   exited; box 1 completed a correct fix, passed the gate, and exited **without pushing**.
7. **Teardown.** Detected box 1's unpushed commit `eaaf4d9`, pushed it, then terminated both boxes.
   `agent-ami list` empty; every box accounted for.
8. **Work resumed locally**, which is where it now sits.

---

## 3. Findings for the agent-ami team

Ordered by severity. F1 and F9 are the two that matter most.

### F1 — 🔴 CRITICAL: teardown silently destroys unpushed work
`agent-ami down` terminates the instance and its EBS volume. Box 1 had a completed, gate-PASSing fix
commit (`eaaf4d9`) that existed **only on that instance's disk**. Had the operator terminated on the
owner's instruction without checking, that work would have been unrecoverable — and nothing in the
tool or the skill would have warned them.

**Fix:** `down` must refuse by default when the workspace has commits not on any remote, or
uncommitted changes. Something like:

```
$ agent-ami down 1
refusing: /home/ubuntu/workspace/repo has 1 commit not present on origin
  eaaf4d9 fix(#2877): correct window refill ...
  push it, or re-run with --force to discard
```

A `--force` escape hatch is fine. The default must be safe. This is the single highest-value change
in this report.

### F2 — 🔴 `claude -p` exits at end-of-turn and kills its own pending background tasks
See F9 — listed separately because it is arguably a Claude Code issue rather than an agent-ami one,
but agent-ami is where it bites.

### F9 — 🔴 CRITICAL: headless workers cannot own a long-running verify step
This is the root cause of the trial's failure. The pattern, observed **four times**:

1. Agent starts the full gate in the background (correct — it takes 15–25 min).
2. Agent starts a `sleep 60; grep RESULT` poll task (also correct).
3. Agent's turn ends. **The `claude -p` process exits and kills the poll task.**
4. The gate runs to completion and PASSes. Nobody ever reads the result.

Log evidence: `{"subtype":"task_updated","patch":{"status":"killed"}}` immediately followed by
`{"status":"stopped","summary":"until ! kill -0 543521 ...; do sleep 20; done"}`.

Consequences observed: box 0 opened a PR at an unfixed commit; box 1 never pushed a finished fix.
Both looked like agent incompetence and were actually **harness lifecycle**.

**Fix — any one of these would do:**
- Keep the `claude -p` process alive while background tasks are pending, or
- Provide a supported "run this long command and resume the agent when it finishes" primitive, or
- Have agent-ami own the verify step itself: run `verify.run` as a **supervised** step outside the
  agent's turn and re-invoke the agent with the summary pre-supplied.

The third is the most robust and fits agent-ami's existing `verify.run` config. **A remote worker
whose whole purpose is running a 25-minute gate cannot be architected around a turn-scoped process.**

### F7 — 🟠 HIGH: `ScheduleWakeup` hangs forever in headless sessions
No scheduler exists under `claude -p`, so the call never fires and the agent waits indefinitely.
Cost **~8 hours** of idle billing on box 0. The agent has no way to know this. Either stub it to
fail loudly, or document it as unavailable in headless mode and have the brief-writer omit it.

### F8 — 🟠 HIGH: no credential path for review tooling
`roborev` cannot authenticate on a worker: `claude-code` → "Not logged in", `codex` → HTTP 401
against `wss://api.openai.com`. `agent-ami auth` injects git/gh credentials but not the agent-tool
credentials a code-review step needs. Worse, box 0's agent **retried the 401 in a loop and burned an
entire turn on it** despite an explicit instruction not to run roborev.

**Fix:** extend `auth` to cover reviewer credentials, or document clearly that review must happen off-box.

### F3 — 🟠 HIGH: `up` exits 0 when onboarding silently no-ops
`agent-ami up` reported success while onboarding had actually failed with
`/var/log/agent-ami-onboard.log: Permission denied`. The repo was never cloned. Discovered only by
manually inspecting the box. Fixed by hand with `sudo touch` + `chown ubuntu` and re-running
`agent-ami-onboard`.

**Fix:** `up` must fail loudly if onboarding did not complete. Create the log with correct ownership.

### F4 — 🟠 HIGH: onboarding requires the profile to be committed to the default branch
Workers clone `main`. `.agent-ami/profile.yaml` was uncommitted (and CQLite's `main` is
branch-protected, so committing it requires a PR). Onboard failed with
`no manifest at .agent-ami/profile.yaml`. Worked around by base64-piping the file onto both boxes.

**Fix:** support pushing the local working-copy profile to the box, e.g. `up --profile <path>`.
Bootstrapping a repo for agent-ami should not require a PR into a protected branch first.

### F5 — 🟡 MEDIUM: `agent-ami ssh <N> -- bash -lc '...'` mangles argv
Recurred throughout the trial: `mkdir: missing operand`, unexpanded `~`,
`bash: line 1: file-size:: command not found`. Quoting is lost somewhere in the ssh invocation.
Every non-trivial remote command had to be written to a file, `scp`'d, and run as
`bash /path/script.sh`.

**Fix:** pass argv through without re-splitting. This one cost the most cumulative time of the
medium-severity findings.

### F6 — 🟡 MEDIUM: no `agent-ami scp`
`agent-ami scp` → `invalid choice: 'scp'`. Given F5 makes file-copying the *primary* way to run
remote commands, its absence is keenly felt. Fell back to raw
`scp -i ~/.agent-ami/agent-ami ubuntu@<public-dns>`.

**Fix:** add `agent-ami scp <N> <local> <remote>` (and `pull`), reusing the same DNS/key resolution
as `ssh`.

### F10 — 🟡 MEDIUM: `agent-ami` fails outside the project directory
`no .agent-ami/profile.yaml found from /private/tmp upward`. Even `list` and `down`, which are pure
infrastructure operations, require the cwd to be inside a configured project.

**Fix:** infrastructure subcommands should not need a project context.

### F11 — 🟡 MEDIUM: box indices renumber on teardown
After terminating an orphan box, `ssh 2` → "out of range (0..1)" and — worse — **`ssh 1` silently
retargeted a different machine**. Positional indices are unstable identifiers being used as if they
were stable.

**Fix:** accept instance IDs and job labels everywhere; keep indices as display-only sugar. A command
that silently acts on the wrong machine is a data-loss vector.

### F12 — 🟢 LOW: `.agent-ami/` left untracked in the workspace
Both boxes ended with `?? .agent-ami/` in `git status`, from the base64 workaround. Minor, but it
pollutes the tree the agent reasons about and risks being committed. Add to a local exclude.

### F13 — 🟢 LOW / FEEDBACK ONLY: a golden image would cut ~20 min/box
Each box spent ~20 minutes installing the Rust toolchain and fetching datasets. A pre-baked AMI
would eliminate this on every launch. **Per owner instruction, no bake was attempted** — this is the
tooling team's call, recorded as feedback only.

### F14 — 🟢 LOW: cost documentation is wrong in the skill
The skill states `m7i.2xlarge` ≈ $0.40/hr; the actual config pins `c7i.4xlarge` ≈ $0.85/hr — **2.1×**.
The operator quoted the wrong figure to the owner before catching it. Since the skill instructs the
operator to state cost aloud and get sign-off, this number must match reality.

---

## 4. Findings for *our* process (not agent-ami's)

### P1 — The gate cannot see the bugs that actually blocked these PRs
Both branches passed a **29-component full gate** and both contained real defects:

- #2876 claimed to fix the scan plane but left uncompressed scans on the MADV_RANDOM mapping, **and**
  stripped MADV_RANDOM from genuine point lookups — the mirror image of the bug it was fixing.
- #2877 had an integer underflow on window refill for a boundary-straddling partition, and its
  fixture fit entirely inside the 4 MiB window so the test **structurally could not reach** the
  broken path.

Both sets were caught only by roborev. **This is the strongest evidence yet for the review-first
doctrine (#2086)** — and it argues review-first should be treated as non-negotiable for read-path and
perf work, not merely the default.

### P2 — A remote agent's self-assessment is not evidence
Box 0 reported success and opened a PR while three BLOCKERs were open. Box 1's fix was genuinely
good but **unpushed**. In both cases the authoritative state was on `origin` and in the gate summary
file, not in the agent's report. Corollary: *always verify branch state against `origin` before
believing a completion claim, and before any destructive action.*

### P3 — Verbose prohibitions in a brief do not reliably bind
Box 0 ran roborev despite an all-caps prohibition, and opened a PR early despite explicit ordering.
Restructuring the brief to **lead with a DO-NOT list citing the agent's own prior failures** worked
better than burying rules in prose. Mechanism beats instruction: the durable fix is for the harness
to own the gate (F9), not for the brief to ask more firmly.

### P4 — The heartbeat model does not fit remote fan-out
`claim-heartbeat.sh` writes **one ref per machine** (`refs/heartbeats/<machine>`), not per issue.
Beating #2876 then #2877 force-overwrote the same ref, so only one issue was ever advertised as live.
A single lead driving N remote workers cannot express liveness for N issues.
**Fix:** key the heartbeat by issue, or add a per-issue sub-ref.

### P5 — `gh project item-list --limit 500` truncates silently
Returned exactly 500 items and neither issue, which read as "the board is empty" — a **false
negative** that briefly convinced the operator (and was reported to the owner) that the board was
stale. Both items were in fact present at `In Review`. Query `projectItems` per issue via GraphQL
instead of listing and filtering client-side.

### P6 — Filed defect #2878, found only because of this trial
`fetch-datasets.sh` line ~222 `rm -rf "${DATASET_ROOT}"` deletes git-tracked commitlog fixtures, and
`restore_ci_tracked_dataset_files` silently `return 0`s when the dataset dir is outside the repo
root — so the deletion is never undone. Invisible locally (fixtures already present); fatal on a
fresh box.

---

## 5. Recommendations

**For the agent-ami team, in priority order:**

1. **F1** — make `down` refuse to destroy unpushed commits. Highest value, smallest change.
2. **F9** — move the verify step out of the agent's turn, into supervised tooling. This is what makes
   unattended operation actually possible.
3. **F3/F4** — make `up` fail loudly on onboard failure; support an uncommitted local profile.
4. **F5/F6** — fix argv passthrough; add `scp`.
5. **F7/F8** — stub or document `ScheduleWakeup`; extend `auth` to reviewer credentials.
6. **F11** — stable identifiers everywhere; never let a stale index target the wrong box.
7. **F13/F14** — consider a golden image; correct the documented instance cost.

**For our own use of remote workers:**

Use them for **compute-heavy, well-scoped, single-shot work** — a gate run, a bisect, a fixture
regeneration, a parallel sweep. Do **not** yet hand them a full issue endgame that requires
gate → review → fix → re-gate round-trips; that loop needs either F9 fixed or a local driver. The
sweet spot today is *remote compute, local orchestration*.

---

## 6. Status at time of writing

- Both instances **terminated**; `agent-ami list` empty; all work pushed to `origin`.
- #2876 (PR #2882): 3 roborev BLOCKERs being fixed locally by `sstable-developer`.
- #2877 (PR #2880): round-1 findings **confirmed fixed** by roborev job 4628; **3 new findings**
  from that round outstanding, one of which (greedy 4 MiB fill vs. token pushdown) may be a genuine
  design call for the owner.
- Merge sequencing unchanged per owner decision: **#2876 first**, then #2877 rebases onto it and must
  prove its coalescing does not defeat #2876's readahead fix (the CASSANDRA-15452 trap).
- Filed during the trial: **#2878** (fetch-datasets destroys tracked fixtures).
