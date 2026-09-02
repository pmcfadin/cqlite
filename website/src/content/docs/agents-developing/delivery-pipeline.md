---
title: Delivery pipeline (flow-lead)
description: The manager-orchestrated delivery workflow — the flow-lead agent, the flow-* pipeline, the specialist roster, and the one standing human seam (spec approval; merge is autonomous on green).
sidebar:
  label: Delivery pipeline
---

CQLite delivery is driven by a **manager agent, `flow-lead`**, that orchestrates a team of specialist
agents through a defined pipeline. Start it as your session driver — `claude --agent flow-lead` (it is
the repo's default agent) — and it orients from the board. It orchestrates; the specialists do the
middle; you sit in one standing seat (spec approval — merge is autonomous on green).

## The pipeline

```
  flow-groom ─▶ flow-activate ─▶ flow-implement ─▶ flow-address ─▶ flow-finalize
   idea→issue    Seam 1:           team builds,      resolve PR       archive +
   (oracle vs    spec+design,      gate→C→roborev,   comments         cleanup +
    design)      STOP for you      open PR (no merge)                 close issue
                       ▲                                    ▲
                       │                                    │
                  flow-board surfaces the single next thing waiting on you
```

| Verb | What it does |
|------|--------------|
| `flow-groom` | rough idea → one scoped issue (one `P0`–`P3`, `status:ready`, testable criteria); decides oracle vs design |
| `flow-activate` | worktree + branch + `opsx:propose`; renders spec + design inline; **STOPS at Seam 1** |
| `flow-implement` | implement (TDD) → review-first (`rust-reviewer` + roborev on the lite-green diff) → open PR → spawn `flow-closer` for the endgame (full gate → **C** → final roborev → merge → finalize) |
| `flow-address` | resolves PR review comments; re-verifies; pushes; replies |
| `flow-finalize` | `opsx:archive` + **stamp the telemetry ledger** + remove worktree/branch + close issue (post-merge) |
| `flow-board` | status across in-flight work + drives the one item waiting on you |

## Oracle vs design (the routing decision)

- **Oracle-driven** (SSTable parsing, compaction/tombstone parity, type decode) — a Cassandra/sstabledump
  source of truth exists. Issue + a pinned parity test; **skip OpenSpec**; groom → implement.
- **Design-driven** (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process) — no oracle.
  Goes through `flow-activate` (OpenSpec proposal/design/specs/tasks).

## The one standing human seam

1. **Spec approval** (Seam 1, in `flow-activate`) — you approve the OpenSpec spec + design before any
   implementation. The lead renders it inline and stops. **This is the only standing human gate.**

**Merge is autonomous by default** — not a standing seam. A worker's/closer's **terminal state** for an
issue is PR-open + `agent-gate.sh` PASS + C PASS (design-driven) + roborev clean; at that point it **arms
`gh pr merge --auto` and ends its turn**, and GitHub lands the PR on green (see
[Merge-on-green](#merge-on-green-no-ci-busy-wait)) — it does **not** poll the PR's own external CI in a
yield/wake loop. An owner merge decision exists only **conditionally**, when an escalate-and-hold trigger
fires: a genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement,
work outside the issue, or an explicit `HOLD:` order. *Always escalated, never decided by the lead:*
product decisions, scope/title changes, epic closes.

## Merge-on-green (no CI busy-wait)

Once a worker/closer reaches **local certification** (PR-open + gate PASS + C PASS + roborev clean) — and
after the pre-merge SHA assert + `HOLD` re-read — it **arms auto-merge and stops**. It must **not** busy-poll
its PR's own external CI — repeatedly waking (`ScheduleWakeup`) to watch the cross-platform matrix after the
work is done is pure token bleed and is prohibited. GitHub owns the green-wait:

```bash
gh pr merge <pr> --auto --squash --delete-branch
```

GitHub lands the PR the instant the branch's **`required`** status check passes and auto-closes the issue via
`Closes #N`. This is the single default path — there is **no manager-owned poller/merge-engine** (that
mechanism was never built; it is gone).

**Why `--auto` is safe (#2433):** `main` carries a real `required` status check + `enforce_admins=true` —
**not** an empty `contexts=[]` set (see [GitHub-enforced merge gate](#github-enforced-merge-gate-2433) below).
`--auto` therefore can never land a PR against an unchecked head, and there is no admin bypass. Branch
protection *is* the green-signal guard, machine-enforced by GitHub.

**Finalize crosses a possible session boundary (#2667):** because `--auto` can complete after the arming
session exits, `flow-finalize` runs on whichever wake observes the merge. When the `required` check is
already green at arm time, the closer confirms `state=MERGED` and finalizes in-session (the fast-path
default); otherwise it returns `verdict: auto-armed` and a later wake confirms `state=MERGED` before
finalizing. The [#2667 gate completion push-signal](#gate-completion-push-signal-2667) and GitHub's own
auto-merge notification are the callbacks — the gate summary file is a push signal now, not a poll target.
`ScheduleWakeup` remains valid for genuinely external, harness-untracked state — just not for polling a PR's
own CI after the work is complete.

### Gate completion push-signal (#2667)

The full `agent-gate.sh` fires **one advisory push** at final-SUMMARY write time (title
`gate <RESULT> <branch>@<sha>`, body = RESULT + any failing components), converting the summary file from a
passive poll target into a **push signal**: a backgrounded gate calls its waiting closer/worker back instead
of being idle-polled. `--lite`/`--delta`/`--only` are exempt (iteration aids, never the gate of record). It
is advisory by contract — an absent notifier, an unset target, a failing notifier, or one that **rejects its
arguments** is a silent no-op and the summary file remains the artifact of record, so a broken notifier never
changes the gate's verdict.

**The payload contract is REPO-OWNED (#3119).** `scripts/lib/gate-notify.sh` builds the ntfy JSON itself and
POSTs it to the ntfy **server root** (topic in the body). PASS publishes priority 3 + `white_check_mark`;
FAIL publishes priority 5 + `rotating_light` — **a red gate is distinguishable at a glance**. This is not
cosmetic: the gate previously called `agent-notify` with a `--category` flag that upstream v1.1.0 has no arm
for, so it fell through to manual title/message mode — the title became the literal flag name, the message
became the category value, the real title/body were dropped, and **every FAIL paged as a green priority-3
success**; its ntfy path also POSTed the JSON to the topic URL, so phones rendered a raw JSON blob. Two of
those defects live inside that binary, past any caller-side flag probe, which is why the payload now lives in
git. `agent-notify` remains only an **optional local desktop/sound adjunct**, invoked positionally (never
`--category`) with its webhook env neutralized so it cannot double-publish. Configure the target with
`CODEX_NOTIFY_WEBHOOK=https://ntfy.sh/<topic>` (the fleet uses `/etc/environment`);
`bash scripts/bootstrap-agent-machine.sh` verifies the **capability** via `gate-notify.sh --self-test` and
records the pinned contract version. Payload fidelity is pinned by
`scripts/tests/test_gate_notify_contract.sh` (gate component `tooling-tests`), which asserts the **published
bytes** at the transport boundary — an argv-level assertion is explicitly not evidence, since that is exactly
the blind spot the swallowed flag hid behind.

### GitHub-enforced merge gate (#2433)

`main` now carries **full branch protection**: the `required` status check (the "Required PR Gate" CI
workflow) is a required context, with `enforce_admins` on. Merge-on-green is therefore
**local gate PASS + C (design) + roborev clean *and* the GitHub `required` check green** — the last term
is machine-enforced, not honor-system. Because `enforce_admins` is enabled, even `gh pr merge --admin`
is refused while the check is pending or red (proven on probe PR #2441: plain and `--admin` merges both
rejected with `mergeStateStatus: BLOCKED`), so **there is no bypass**. A red that is a known flake gets
`gh run rerun --failed` — never an admin override. This is load-bearing: if branch-protection settings
ever regress (contexts emptied, `enforce_admins` disabled), this doctrine governs catching it.

### Closer merge protocol (#2456)

The `flow-closer` certifies a **specific SHA** — the tree the full gate of record and the final
roborev pass actually ran on. Three mechanical rules keep the merge honest:

- **Pre-merge SHA + gate-of-record assertion (#2456/#2668/#3465, scripted hard precondition).**
  Immediately before arming `gh pr merge --auto`, the closer does `git push`, then runs
  `scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]` —
  which asserts the PR is
  OPEN and its `headRefOid` **equals the locally-certified tip**, exiting non-zero (and printing a
  loud refusal) on a moved head, a closed/merged PR, or a gh failure.
  **The third argument is REQUIRED (#3465), and it closes TWO distinct escapes with one mechanism.**
  Verifying the head against a *claimed* certified sha never verified that a certified sha EXISTS.
  **#3408 = no gate at all**: it merged on 22 `--lite` PASSes and no full `scripts/agent-gate.sh` run,
  because nothing in the merge path ever asked for the gate of record. **PR #3616 = a real gate,
  someone else's**: a closer located its run dir by RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER
  LANE's dir, saw 33 of 37 components PASS, and was about to merge #3616 on PR #3580's verdict — the
  count, the dir and the timestamps were all real, and only the `run-id:` line exposed it, read by a
  human. With 14000-27000 stale run dirs per box and up to 4 concurrent gates, recency picks a peer
  routinely. This script **cannot** verify `run-id:` (see below), so the `commit:`+`tree-start:`
  binding is what makes that class a mechanical refusal: a peer's summary carries the OTHER PR's
  branch head. It is now asked for here, at the one point every merge passes through: the summary file must
  hold exactly ONE whole-line-anchored `==== AGENT-GATE SUMMARY ====` block (`--lite`/`--delta` emit
  distinct headers and are refused by name; a second or unterminated block is ambiguous and also
  refused) with `RESULT: PASS` and `tree-integrity: PASS` compared **token-exactly** — `INCOMPLETE` is
  the launch-time liveness sentinel and not a verdict (#3041), and a mutated-mid-run tree is not a
  certification (#2926) — and with BOTH `commit:` (7 hex) and `tree-start:` (12 hex) prefix-matching
  the certified sha **at each value's own width**; a non-hex placeholder (`(not captured)`,
  `unverified`, `selftest`) REFUSES rather than being skipped. An OPTIONAL argument would have left the
  convention honour-system, so the pre-#3465 two-argument call is a loud usage failure.
  **What it does NOT do, stated rather than implied:** it cannot verify `run-id:` (the #2874 reader
  contract requires the party that LAUNCHED the run, which this script is not), and it cannot prove
  the summary came from a genuine gate rather than a hand-written file — a hostile invoker is out of
  the threat model, since whoever runs the script controls the process. What it closes is **accident
  and drift**, which is the observed failure mode. `dirty:` is reported in the success line **and
  enforced** (#3648): the gate-of-record block — and, in Case B, the delta block too — must read
  `dirty: no`, matched affirmatively, so an absent or unrecognised value REFUSES rather than being read
  as clean. A `dirty: yes` run certified the sha PLUS uncommitted tracked edits, which `commit:`/
  `tree-start:` cannot distinguish from the clean tree. No env opt-out exists and none may be added — a
  dirty tree is always re-gateable.
  **The OPTIONAL fourth argument is the only way a `--delta` re-cert can certify a merge.** #1892
  *mandates* `--delta` — "never a repeat full gate" — for a test/docs-only diff on top of a full PASS
  at anchor `X`, and mandates that the PR record BOTH blocks, so a 3-arg-only guard red on correct,
  doctrine-mandated input: the guard agents learn to waive. With four arguments the third is the
  ANCHOR's full PASS (its sha need NOT be the certified sha) and the fourth is exactly one
  `==== AGENT-GATE DELTA SUMMARY ====` block carrying `MODE: delta` (asserted affirmatively — the
  inverse of the full block's `MODE:` belt), `RESULT: PASS`, `tree-integrity: PASS`, a `delta-anchor:`
  naming exactly that anchor (an `(UNRESOLVED)` anchor refuses), and its OWN `commit:`/`tree-start:`
  at the certified sha. A block carrying `nested-under:` (#2874) is refused in either shape: a nested
  sub-gate runs at the SAME tree, so the sha binding provably cannot distinguish it.
  **And what `PREMERGE: OK` does NOT prove (#3650), which the success path states itself on a
  `PREMERGE: SCOPE` line:** it proves the diff is unchanged since certification and that a full gate
  PASSed on THAT EXACT TREE — not that the change was certified against the `main` it will join. A
  squash-merge composes the diff with main's CURRENT tip, so for any PR whose base is behind main the
  certified tree and the merged tree are different objects (measured on #3358/PR #3362). A gate on the
  MERGE RESULT is #3650 **slice 2** and is deliberately not implemented here. What slice 1 DID add is
  a **non-blocking base-staleness advisory** on `PREMERGE: ADVISORY` lines
  (`scripts/flow/base-staleness.sh`): `N` commits behind the merge-base with `origin/main` and `M` of
  those touching the diff's blast radius — measured at the **certified sha**, not the local checkout's
  `HEAD` (paths the diff touches + a hard-coded gate-global set; every run declares TWO gaps: it is not
  a dependency closure, and the gate-global list is itself curated and NON-CLOSED). It is information, never a verdict: it cannot change
  the exit code, an absent/failing/`UNMEASURED` advisory is non-fatal, and any consumer of it must
  treat `UNMEASURED` as STALE rather than fresh.
  Report a pass as "gate of record verified at `<sha>`", never "certified against main". The closer **refuses to merge on any
  non-zero exit** (fail closed). It also re-reads issue/PR comments for a fresh `HOLD:` order in the
  same pre-merge pass. Motivated by the 2026-07-14 stale-merge escape on
  #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never pushed it, so
  `gh pr merge` squashed the PR's *stale* pre-fix head and transiently landed a known data-loss
  blocker on `main` (remediated by PR #2455). The GitHub required check re-runs on push but cannot
  catch a "merge of an old green head" — the SHA assertion is the real guard.
- **Unique gate-summary paths.** Each gate writes its `AGENT_GATE_SUMMARY_FILE` to a `mktemp`-unique
  path (e.g. `$(mktemp /tmp/gate-<issue>-XXXXXX.txt)`) — shared `/tmp` names get contended under
  multi-lane load, so one lane's summary can clobber or be misread as another's.
- **Single full gate per machine — enforced mechanically (#2640).** The default posture is one full
  gate at a time on a box: `bootstrap-agent-machine.sh` persists `CQLITE_GATE_MAX_CONCURRENCY=1`
  into `/etc/environment` — which PAM reads at session creation, so non-interactive shells see it —
  and then **verifies from a fresh, profile-free session that the value is visible and that the gate
  honours it** (`gate-pin: VERIFIED`, #3414), rather than trusting that the write happened. That
  verdict is scoped to a PAM-created session, so a gate launched from a systemd unit or container
  entrypoint is not covered by it; it also measures that the file and the session AGREE, not that the
  file is where the session got the value, so a box setting the same value from a sudoers `env_file`
  would read VERIFIED with an `/etc/environment` no PAM stack loads. The per-run authority stays the
  gate's own `cpu-budget:` token.
  A visible value the gate discards or clamps reports `gate-pin: NOT-HONOURED` — its remedy is to
  fix the VALUE, since bootstrap never rewrites an existing one. With the
  pin in effect the #1825 machine-wide cap admits exactly one full gate and the #2640 per-gate core
  budget hands that sole gate the full core count; a gate that resolved its cap from the default
  formula instead says so on its own `cpu-budget:` line as `max-concurrency=N(default)`. The gate also derives `CARGO_BUILD_JOBS` + nextest `--test-threads`
  from the slot count and wraps itself in `taskpolicy -c utility` (macOS) / `nice` (Linux), so even
  if two gates do overlap neither oversubscribes the CPU. No manual `pgrep`-serialization is needed.

## The specialist roster

| Role | Agent / tool |
|------|--------------|
| implement / format debug (TDD) | `sstable-developer` |
| review-first (Rust review) | `rust-reviewer` — on the lite-green diff, BEFORE the full gate |
| endgame owner (full gate → C → final roborev → merge → finalize) | `flow-closer` — per issue, disposable context |
| intent audit (C) | `spec-auditor` (anchored to `openspec/changes/<name>/specs/**`) — see [Spec-driven audit](/cqlite/agents-developing/spec-driven-audit/) |
| parity / test execution | `test-validator` |
| test quality | `coverage-reviewer` |
| code review | roborev (review-first + the closer's final pass) |
| correctness | `scripts/agent-gate.sh` — the ONE gate of record, inside `flow-closer` |

## State model

- **Backlog** = GitHub issues; the Project `Status` field is the authoritative lifecycle
  (`Backlog → Ready → In Progress → In Review → Done`). Each issue carries one `P0`–`P3`; `status:*`
  labels are an **enforced read-mirror of board Status for discovery only** (Path A, #1886; #2855 — see
  [the claim board](#the-shared-claim-board)).
- **1:1:1:1** — one issue ↔ one worktree/branch `issue-<N>-<slug>` ↔ one OpenSpec change `<slug>` ↔ one
  PR. Worktrees branch from `origin/main` and lack the gitignored `Data.db` binaries — run the gate with
  `CQLITE_DATASETS_ROOT` pointed at the main repo's `test-data/datasets`.
- The **definition of done** is the [spec-driven audit](/cqlite/agents-developing/spec-driven-audit/)
  one: `agent-gate.sh` PASS + C PASS + roborev clean.

## The shared claim board

In-flight work is tracked on a shared **GitHub Project (v2)** with a single-select `Status` field
(`Backlog → Ready → In Progress → In Review → Done`). It is the cross-session, cross-machine view — and
the thing a human can also drive from mobile. `flow-board` renders it (`gh project item-list`) showing
each item's status, assignee, and priority; built-in server-side Project automations move items on
GitHub-side events (PR merged / issue closed → `Done`, assigned → `In Progress`), so the board stays
fresh even when an action came from the phone or web with no `flow-*` run.

**One-time setup (the owner's action):** Projects v2 needs the `project` token scope —
`gh auth refresh -s project` — then run `test-data/scripts/setup-project-board.sh` to create + link the
board and normalize the `Status` options. The built-in workflow automations (merge/close → `Done`,
assigned → `In Progress`) cannot be set via CLI; the script prints the manual web-UI step for them.

**Path A — the board is the sole dispatch authority (issue #1886):** work is selected and claimed by
the Project `Status` field ONLY. If the `project` scope or the board is **unreachable, STOP and fix the
auth** (`gh auth refresh -s project`) — do **not** fall back to labels to select work. An empty `Ready`
column means no work is ready (near a release it is *meant* to drain to zero), not a cue to dredge labels.

**`status:*` labels — an enforced read-mirror for cheap discovery (issue #2855):** the labels are no
longer decorative. `.github/workflows/project-board-sync.yml` is the *single writer*, deriving each OPEN
issue's `status:*` label from its board Status (Ready→`status:ready`, In Progress→`status:in-progress`,
In Review→`status:in-review`, Backlog/Done→none) on the 30-min sweep + on issue events, with a
drift-detector that FAILs the run on any label≠Status disagreement. So a session MAY *narrow* candidates
cheaply and server-side with `gh issue list --state open --label status:ready --json number,title` (no
issue bodies, no board pagination). But the label is **eventually-consistent (≤30-min lag) and NEVER the
dispatch/claim authority**: it only narrows the candidate set — the selection decision is by live board
`Status`, and the claim ref plus a fresh board read at claim time remain the sole double-work arbiter.
flow-* skills no longer write the board-derived labels (they set board Status only; the mirror follows);
`status:spec-review`/`status:addressing` stay transient skill-managed sub-markers the mirror does not touch.

## The claim protocol (no duplicate work)

Before working an item, a session claims it so no two sessions — **including two sessions authenticated
as the same GitHub user on different machines** — work the same item. Because assignee `@me` is identical
for the same user on two machines, the assignee is *not* the lock; the deciding lock is the **slugless
fixed-name ref `refs/claims/issue-<N>`**, acquired through `scripts/flow/claim.sh` (issue #2665).
`claim.sh claim <N>` pushes a **unique root commit** to that fixed-name ref; git arbitrates the ref
update server-side, so the winner is decided purely by the push result — regardless of slug or base.
This closes two field hazards the earlier slug-named branch lock left open: two sessions on **different
slugs** (`issue-<N>-a` vs `issue-<N>-b`) both succeeded (the #1632 slug pair), and two sessions branching
the **same `origin/main` tip** pushed an identical SHA, so git reported "up-to-date" to the loser and
both thought they won. The `issue-<N>-<slug>` branch survives only as **worktree/PR plumbing — never the
lock**.

1. **Eligibility** — the item is `Ready` AND has **no** `refs/claims/issue-<N>` claim ref
   (`bash scripts/flow/claim.sh status <N>`) and **no** legacy `issue-<N>-*` branch on origin (mixed-fleet
   safety; older workers still branch-lock). A surviving branch over a **free** claim ref is not a dead
   end — see *Resuming past the legacy-branch guard* below.
2. **Claim** — `bash scripts/flow/claim.sh claim <N>` acquires the lock (`CLAIM HELD` exit 0 / `CLAIM LOST`
   exit 2); only then create the worktree + branch and set assignee `@me` + `Status=In Progress` for board
   visibility. `flow-activate` claims immediately — before any spec work; oracle-driven issues claim in
   `flow-implement`.
3. **Verify** — `claim.sh` re-reads the ref after the push and reports `CLAIM HELD` only if you hold it
   (`claim.sh verify <N>` re-checks holder identity later); on `CLAIM LOST`, back off and take the next
   eligible item.

**Machine prerequisite: git itself must be authenticated (issue #2942).** The lock is a plain `git push`,
and `gh` auth is a *separate* credential path — a box with an authenticated `gh` CLI but no git credential
helper fails every claim with `fatal: could not read Username for 'https://github.com'`, so the claim
protocol does not work at all while `gh auth status` reports a healthy machine. `claim.sh` classifies that
signature as **`CLAIM: ERROR reason=auth … (NOT retryable)`** naming the fix, *not* the old
`reason=infra … (transient — retry)` that sent workers into a retry loop on a fault which can never
self-clear; `reason=infra (transient — retry)` continues to mean a genuine, retryable blip. That
classification covers `claim.sh` (`claim`/`adopt`/`release`/`smoke`) only — `claim-heartbeat.sh` surfaces
git's raw error on its own pushes. Fix a box with `gh auth setup-git` or
`bash scripts/bootstrap-agent-machine.sh --yes`, whose preflight checks git push credentials (configuring
a helper **scoped to the origin host** that dereferences `$GH_TOKEN` at call time — never writing the
token to disk; because it reads the environment it works only where `GH_TOKEN` is exported, so prefer
`gh auth setup-git` for systemd/cron workers) and probes **board access functionally** instead of trusting
the `project` scope string. Full delta list with the identifying messages:
`docs/development/fleet-runbook.md`.

Another machine that finds an existing claim can `git fetch` the branch to **resume** that work instead of
colliding; a **reaped** claim is adopted via compare-and-swap — `claim.sh adopt <N> --expect <old-sha>`,
which replaces the ref with force-with-lease so a resurrected original holder loses the lease and detects
the loss immediately (fixes the #2467/#2499 two-writer race).

**Resuming past the legacy-branch guard (issue #2945)** — when the claim ref is **free** but an
`issue-<N>-*` branch still stands on origin (a parked/reaped/released claim, an owner-approved spec that
lives on that branch, or just a merged-but-undeleted PR branch), `claim` refuses with
`reason=legacy-branch-lock … claim-ref=free resume=documented-procedure`. That refusal is a
**diagnosis, not a hand-off**: it names the blocking branch(es) and tells you the claim ref itself is
free, then points here. The ONE sanctioned resume is documented *only* here and in
`claim.sh -h` — it is deliberately **never printed as a runnable line** (see below):

```bash
bash scripts/flow/claim.sh adopt 1234 --expect none --reason resume-legacy-branch-lock:branch-outlived-claim
```

`--expect none` is git's **empty lease** ("this ref must not exist"), so the create is still arbitrated
server-side: a machine that actually holds the claim ref keeps it and the resumer gets `ADOPT-LOST`
(exit 2), and two machines racing the resume still yield exactly one winner. `--reason` is **required** —
it is recorded in the claim commit next to who took it (machine/actor/ts) and rendered by
`claim.sh status`, so a resume is auditable; a reason with nothing recordable in it (`'   '`, `'---'`, an
unset variable) is a **usage error** (exit 64), never a silent `reason=unspecified` — and so is a bare
**placeholder** (`why`, `todo`, `tbd`, `xxx`, …) or a reason still carrying an **unsubstituted `<…>`**
(a copied `--reason resume-legacy-branch-lock:<branch>` sanitizes to a non-sentinel token, so it is
rejected on the raw text): the record must say why. That is also why the example above substitutes a
concrete issue number and reason — the documented invocation is one that works when run verbatim.
`--actor` is fail-closed the same way (an actor with nothing recordable in it would alias two distinct
identities onto one holder, and the actor gates re-entrancy/`verify`/`release`). A hex
`--expect` must be a **full** object name (40/64 hex) — a truncated sha is a usage error, not a lost race.

**Why the command is never printed for you (owner decision, #2945).** `claim.sh` used to decide, from an
in-script liveness probe, whether to print a copy-pasteable version of that command. That probe is gone.
The readers of a refusal are agents that run printed remediations **literally**, and an older-fleet worker
locks with the *branch* while holding **no claim ref** (`claim-ref=free` is true for it) — so a printed
empty-lease adopt would take an **actively-worked** lane and create a second writer. Judging abandonment
needs signals `claim.sh` cannot read soundly, and three successive revisions of the probe each shipped a
fresh version of that hazard (a vacuous branch-tip date, a cross-process ref race, a fleet-wide permanent
withhold). So the refusal diagnoses and points here, and **you** establish abandonment first with the same
test `flow-board`'s reaper uses:

```bash
bash scripts/flow/claim-heartbeat.sh should-reap <machine>   # exit 0 = reapable, 1 = keep, 2 = no ref
```

i.e. claim age > 4h **and** no open PR **and** (pid-dead, when the claim is local) — plus the board
`Status` and the branch/PR author. Only then run the documented resume. Retrying after a transient
`ERROR reason=infra` is safe: an
adopt whose ref is already held by *this* machine+actor reports `ADOPTED … (re-entrant)` exit 0 rather
than abandoning an issue you own. This is the only sanctioned way past that refusal — **never hand-craft
a claim commit or push the ref directly** (the field failure that motivated #2945). The claiming session also maintains a
liveness **heartbeat** (`scripts/flow/claim-heartbeat.sh beat <N>` — a cheap origin git ref under
`refs/heartbeats/<machine>`, never a GitHub API call — refreshed at claim time and on every stage
transition: activate/implement/gate/PR). `flow-board` reaps **abandoned claims deterministically** (issue
#2089): an `In Progress` item is reaped only when its heartbeat age exceeds the documented threshold (4h —
the `claim-heartbeat.sh` header is the single source of truth) **AND** it has no open PR — reap = a
traceable comment + assignee clear + `Status → Ready` + an adopt-eligibility note on the claim ref (never
deleting a branch that carries commits). This replaces the old "no recent commits" guess. `flow-finalize`
releases the claim (`claim.sh release <N>`, which refuses under an open PR without `--force`) and clears
the heartbeat on cleanup.

For unattended/overnight runs a **worker supervisor** (`scripts/local/worker-supervisor.sh`, issue #2090)
recycles one worker process per issue — the hard context bound is process exit: the worker rehydrates from
the board, resumes this machine's own claim branch first (crash recovery) else claims the next Ready item,
runs it to merged + finalized, writes a `.worker-last-iteration.json` marker, and **exits** (never a second
issue per session). The supervisor adds a **per-LANE** single-instance lock (scoped to the lane's checkout root, so a box runs
several lanes while two supervisors in the *same* worktree still refuse to coexist — it mechanized
one-worker-per-machine until #3393 retracted that),
fail-closed preflight (load/disk/leftover-process/stop-file), a crash-loop breaker, budgets, and ntfy
notifications. See the [fleet runbook](https://github.com/pmcfadin/cqlite/blob/main/docs/development/fleet-runbook.md).

**Supervisor-authored claim + CI-side reaper (issue #2655 / #2499 design).** Heartbeats used to depend on
the worker LLM *remembering* to `beat`, and the reap threshold was enforced only in prose. Liveness is now
**mechanism-driven**: the supervisor stamps `refs/lane-claims/<machine>/<issue>` (issue + supervisor-PID + ts, via
`claim-heartbeat.sh stamp`) at every worker spawn, refreshes it each iteration, and clears it on a clean
exit — where `reap` **refuses to delete a claim whose issue still has an open PR** (an unfinished endgame
stays owned for adoption rather than orphaned; the #2499 orphaned-endgame case). This `refs/lane-claims/*`
namespace is deliberately distinct from `claim.sh`'s per-issue lock `refs/claims/issue-<N>`.
**Claims are PER LANE since #3393's owner ruling** — `refs/lane-claims/<machine>/<issue>`, replacing
one-ref-per-machine. The old layout was justified by #1930's "one worker per machine", which the fleet
had not followed all day: several lanes on a box overwrote each other's claim, so a monitor could see
at most one and two of #3393's three silent lane deaths (both on one host) were structurally
invisible. **#1930 is retracted; design for N lanes per box.** The legacy `refs/machine-claims/*` is
still *read* so a pre-ruling ref is drained rather than pinning its board item at In Progress
forever. A new namespace was required rather than a sub-path because git forbids a ref being both a
file and a directory, and `<machine>-<issue>` is ambiguous when machine names contain dashes.

`claim-heartbeat.sh should-reap` is the single, **fail-safe** reap predicate. It has **two forms, and a
two-argument call is ALWAYS the legacy one** — `should-reap <machine> [threshold_secs]` acts on the legacy
per-machine ref, and a lane needs all three: `should-reap <machine> <issue> <threshold_secs>`. The grammar
refuses to guess from arity, so `should-reap <box> <issue>` reads the issue number as a **threshold** and
answers about the *legacy* ref — a real answer to a different question, which can report an active
per-lane claim as absent. (#3393 round 21: this page previously advertised `<machine> [issue]`, i.e. that
trap written down as doctrine.) Exit `0` = reap,
`1` = keep, `2` = no ref): it reaps ONLY when age > threshold (4h) **AND** the issue has no open PR **AND**
(the PID is dead, *when the claim is local* — a foreign machine's PID is unknowable, so from CI that clause
is skipped and age + no-open-PR govern). It KEEPS on a fresh ref, an open PR, a live local PID, or an
unparseable age; a `gh`/network hiccup in the open-PR probe assumes an open PR (keeps). The
`project-board-sync` **30-minute cron** now carries a `reap-claims` job applying exactly this predicate
server-side, deleting the stale claim ref and flipping the freed board item back to `Ready` with a traceable
comment. Two workflow hardenings ship with it: **`PROJECTS_TOKEN` absence now fails the workflow loudly**
(`::error::` + non-zero exit — a persistent red run is the alert) instead of the old silent green
`::notice::` no-op; and the scheduled board sweep only backlogs a null-status issue once it is **older than a
10-minute auto-add grace window**, so it no longer races the built-in Auto-add workflow's default-status
write on a freshly created issue.

**`should-reap` is a REAP GATE, not a liveness monitor — and that gap cost three lanes (#3393).** It
consults the recorded PID only *after* age > threshold, so a worker the kernel OOM-killed a minute ago is
indistinguishable from a healthy one for **four hours** — and even then the answer is an exit code nobody is
watching. On 2026-08-27/28 the kernel issued **10 global OOM kills** across two 30 GB workers (every victim
a `python3` at 20–28 GB) and three lanes died silently, each leaving a clean worktree, a held claim and an
open PR. **Memory exhaustion is invisible to any monitor that iterates existing sessions**: a dead tmux
session cannot report itself.

**The tool is `claim-heartbeat.sh dead-lanes`, and per-lane claim refs are what make it work.** It asks
"is anything dead RIGHT NOW", inverting both of the reaper's conservative guards on purpose: no age gate (a
fresh claim with a dead PID *is* the shape of an OOM kill), and an open PR does not suppress the report — for
the reaper an open PR means KEEP, but for a report it is the most urgent row on the page. It is a REPORT: no
ref is deleted, no board item moved. Read `dead-lanes --help` for the authoritative verdict set; it lives
beside the code and cannot drift from it.

**Why the layout had to change first (#3393 owner ruling A).** The OLD `refs/machine-claims/<machine>` was
keyed per **MACHINE** and force-updated every supervisor iteration, so on a multi-lane box a surviving lane's
stamp overwrote a dead sibling's PID — a live sibling did not merely hide a dead lane, it **masked** it. Two
of the three deaths above were on one host, which is exactly the case that collapsed. Claims are now
`refs/lane-claims/<machine>/<issue>`, one per lane, and **#1930's one-worker-per-machine invariant is
retracted** — multiple lanes per box is the standing model, so design for it. A new namespace was required
rather than a sub-path: git forbids a ref being both a file and a directory, and `<machine>-<issue>` is
ambiguous when machine names contain dashes. The legacy namespace is still *read* by `list-claims`,
`dead-lanes` and the CI reaper so a pre-ruling ref is drained rather than pinning its board item at In
Progress forever.

Exit codes are what a cron reads, so they are worth knowing: **3** = a dead lane was reported, **1** =
none was reported, which also covers zero claim refs and a run where every claim belongs to another
machine. **This slice never exits 0** (#3393 split ruling) — act on 3, and never read 1 as a clean
bill of health. Per-lane refs do make a sound clean verdict possible, since a surviving sibling now
stamps a different ref and can no longer mask a dead lane; it was split out rather than shipped
because the fail-open defect family clustered in that exit-0 path, and being wrong there is silent.
It claims nothing about lanes that never stamped (a lane run with `CLAIM_CMD=""` is invisible) and
nothing about other machines — a PID is only checkable where it runs, so **run it ON the suspect
box**.

**AND ON A SUPERVISOR-LESS FLEET IT ANSWERED ABOUT THE EMPTY SET — supervisor fleets only, DESCOPED
by owner ruling 2026-09-01 on #3548 (option C; completes #3393).** The subject set is
`refs/lane-claims/*` plus the legacy `refs/machine-claims/*`, and the only in-tree CALLER that writes
either is `worker-supervisor.sh` (`stamp` is a public subcommand and can be invoked directly). This fleet runs `/drive-issue` lanes, so when #3548 was measured the command
had no subject and exited 1 — persisted or manually `stamp`ed refs can still produce rows, and either
way **1 means "nothing was reported", never a clean bill of health.** The two *populated* namespaces
are deliberately not read, both refusals measured: `refs/claims/issue-<N>` records the transient
claiming shell's pid (dead while its lane runs), and `refs/heartbeats/<machine>` is single-slot per
machine. **AC4** survives as a counterfactual: were a later change ever to read a non-refreshing
carrier, a stale pid there must abstain rather than yield `DEAD-*`.

**Everything else is stated once, not here.** What lane liveness on this fleet actually rests on, and
both board signatures — neither of which is a verdict — live in
`docs/development/fleet-runbook.md` → *Lane liveness on a supervisor-less `/drive-issue` fleet*. Seven
review rounds on #3548 were propagation failures of duplicated prose, so nothing restates it.

A suspected dead lane still has a diagnostic **order, and it matters** — full procedure in
`docs/development/fleet-runbook.md`. The one line worth memorising: when a box accepts TCP but sends no SSH
banner from inside the VPC, **check `dmesg` for an OOM kill before concluding the instance is broken**.
Reading that symptom as a broken instance already cost one healthy machine (terminated, losing a
measurement lane's 43 minutes and an unpushed commit), and a soft reboot may be silently ignored on a
memory-exhausted host.

**Never block on a question (park-and-resume, #2666).** A worker runs unattended, so `AskUserQuestion` (and
any interactive prompt) is attended-sessions-only. When a worker hits Seam 1 (an unapproved spec) or a
genuine mid-run owner decision it does not wait — it **parks**: posts ONE structured question comment
(options + recommendation + default), adds the `needs-decision` label, writes a `blocked` marker with
`reason: seam1-approval|needs-decision`, and exits, releasing the machine. The supervisor judges this
`parked-on-owner`, pages the owner once, and moves to the next Ready issue; a worker that nonetheless wedges
on a prompt is caught mid-iteration by a log-tail watchdog and paged as `stuck-on-question`. Neither counts
toward the crash breaker. The parked issue resumes only on a strictly-newer owner reply (the worker reads
the answer and clears the label); a durable `resume-dont-ask` label is a standing Seam-1 seal `flow-implement`
honors in place of asking.

## Concurrency model

- **One active worker per machine; the worker paces the machine's load (#1930).** A single lead/worker
  session owns a machine at a time — the load + worktree-isolation rule that sits *above* the claim
  protocol. Two efforts on one box collide on the shared worktree and oversubscribe the CPU, which flakes
  scheduling-sensitive tests (write-throughput, the streaming GIL-release test) and can SIGKILL gates. The
  owning worker is responsible for load: **serialize your own full-gate runs — never two full
  `scripts/agent-gate.sh` at once on one box** (the machine-wide gate cap is a backstop, not a license to
  overlap). **Subagents are exempt:** a worker fanning out `sstable-developer`/reviewers is not "multiple
  workers" — they never launch competing full gates. The rule targets independent lead/worker *sessions*.
- **Default (recommended): one lead → subagents.** A single `flow-lead` spawns subagents and assigns each
  **disjoint** work — zero duplicate work by construction.
- **Multiple independent sessions: the claim protocol is mandatory.** Each acquires work only through the
  claim protocol above — and, per the rule above, independent sessions belong on *separate* machines
  (one-per-machine handles a single box; different machines coordinate via the `refs/claims/issue-<N>` ref lock, #2665).
- **Agent Teams is optional, desktop-only.** `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` gives a built-in
  file-locked shared task list for coordinated parallel sessions, but it is experimental and desktop/tmux
  -only (no `/resume`, one team per session). Use it if you want; it is not required.
- **Never run N bare `flow-lead`s without the claim protocol** — independent leads with no claim each pick
  the same top `Ready` item and collide.

## Driving from mobile / remote

The Claude Code mobile app cannot run the local pipeline itself (no local bash, skills, worktrees, or the
dataset binaries). Two supported ways to still drive work from the phone:

- **Remote Control (primary).** Run `claude remote-control` on the laptop and connect from the mobile
  app; the phone drives the **full local `flow-*` pipeline** (worktrees, `agent-gate.sh`, `gh`,
  `openspec`) in that local session. The laptop must stay online.
- **Claude Code on the web (secondary, cloud).** A cloud session uses the repo-committed `.claude/`
  (skills/agents/hooks) but not user-scoped config or local data. Run the **cloud setup script**
  `test-data/scripts/cloud-setup.sh` first — it installs `openspec` + `gh` and fetches the dataset
  (`fetch-datasets.sh`) so `flow-implement` can run the gate in the cloud.

**Spec approval is the only standing human seam, and it is GitHub-mobile-native** regardless of how you
drive: approve the OpenSpec spec + design in the session (Seam 1). For worker-owned issues **merge is no
longer a hand-merge step** — the closer arms `gh pr merge --auto` and GitHub lands the PR on green (see
[merge-on-green](#merge-on-green-no-ci-busy-wait)), and the merge event moves the board item to `Done`. The owner intervenes on merge (from the mobile app / web
UI) **only on escalation** — a genuine design-call roborev finding, a scope/product question, or work
outside the issue.

## The implement loop: review before gate, gate once at the end (issues #1821, #2084, #2086, #2087, #2088)

Inside `flow-implement` the loop is ONE coherent design, not three patches:

```
implement (TDD) → lite (each fix round) → rust-reviewer + roborev on the lite-green diff
  (review-first, DEFAULT) → fix (lite re-cert + diff-scoped targets, NEVER a full gate)
  → open PR → flow-closer { FULL gate ONCE → C → final roborev → merge-on-green → finalize }
```

- **Review-first is the default (issue #2086).** `rust-reviewer` + roborev run on the **lite-green** diff
  **before** the first full gate, so review discovers fixable problems before we pay for the 12–25 min gate.
  Skip only for a genuinely mechanical diff (no `pub`-item change AND single call site AND no new surface).
- **Scoped re-cert, one full gate (issue #2087).** A roborev blocker that touches src re-certifies with
  `scripts/agent-gate.sh --lite` (blast-radius-scoped) + any diff-relevant parity/integration target — NOT
  a full gate. The single full gate of record runs **once**, immediately pre-merge; lite re-certs (their
  `MODE: lite` marker) are never the gate of record.
- **Severity-triaged findings (issue #2088).** Findings are classified per the
  [roborev severity rubric](https://github.com/pmcfadin/cqlite/blob/main/docs/development/roborev-severity.md):
  **blockers** (correctness, data-parity, no-heuristics, safety, wiring-evidence, security, any acceptance
  criterion) are fixed pre-merge; **nits** (style, naming, comment/doc polish, no-repro test suggestions)
  are batched into ONE linked follow-up issue at merge time and never trigger a re-verify round. When in
  doubt, blocker.
- **The disposable `flow-closer` owns the endgame (issue #2084/#2668).** `flow-implement` opens the PR, then
  spawns a per-issue `flow-closer` that runs the ONE full `scripts/agent-gate.sh` of record (via
  `run_in_background` + the summary-file pattern — it **never idle-waits**, which would trip the #1855 stall
  watchdog and orphan the gate; polling the summary file is mandatory on a hard 45-min deadline, with
  `grep -qE 'RESULT: (PASS|FAIL)'` — never a bare `grep -q` on the bare `RESULT:` token, which also matches the startup
  `RESULT: INCOMPLETE` liveness placeholder and would accept a just-launched gate as a verdict, #3041),
  the **C**
  intent audit, the final roborev pass, then merges on green and `flow-finalize`s. The closer has **no
  `Agent` tool**, so it never spawns directly: for **C** (and any src-design fix) it emits a structured
  `NEEDS-SPAWN` packet and ends its turn — the lead spawns `spec-auditor`/`sstable-developer` and re-invokes
  the closer with the result. It returns only a terminal packet (verdict, PR URL, summary-file path, ≤10 lines
  residual), so gate stdout and review churn die with its context instead of accreting in the lead session.
  Any src change after the full gate INVALIDATES it — the gate of record must postdate the final src change
  and rebase.
- **Division of labor.** An implementer subagent (`sstable-developer`) edits/commits/pushes and verifies
  with `--lite`/targeted tests **only** — it must **never** invoke the full gate.

Every gate invocation — full and `--lite` — uses the **summary-file redirect** by default
(`AGENT_GATE_SUMMARY_FILE=<path> … > gate.log 2>&1 < /dev/null`, then `cat <path>`); raw gate stdout is
never read into a persistent agent context (issue #2079). See the
[gate contract](/cqlite/agents-developing/gate-contract/) for the summary-file default and the
`accelerators:` line.

## Inter-issue reset for the lead (issue #2085)

The `flow-lead` is the only long-lived agent, so it compacts between issues: after each `flow-finalize` it
carries **zero prior-issue history** (board renders, gate summaries, roborev findings, PR bodies, and
Seam-1 spec renders are dropped — `spec-auditor` re-reads specs from `openspec/changes/<slug>/` anyway),
re-hydrates the **next** item from the **board alone**, and stays re-runnable from board + disk state at any
point (worktree, origin claim branch, issue/PR bodies, OpenSpec files, summary files, telemetry ledger).
Durable cross-issue lessons route to `MEMORY.md` / `process_improvements.md`, never the live window. The
same board-only rehydration rule applies to worker sessions (see the supervisor below).

## Machine setup + accelerators

A fresh machine that will run the pipeline should first run
`bash scripts/bootstrap-agent-machine.sh` (details in `docs/development/agent-machine-setup.md`): it
verifies the gate accelerators (`sccache`, `cargo-nextest`, modern bash — issue #1848), the datasets +
`CQLITE_DATASETS_ROOT`, `gh` auth + the `project` scope, and roborev's local config. **roborev is invoked
ONLY through the fail-closed wrapper `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>
[--repo <abs-path>] [--base <ref>]`** (#2964) — fleet form `--agent codex --model gpt-5.6-sol`; the Claude
reviewer is `--agent claude-code --model claude-opus-5`. **BOTH `--agent` and `--model` are ALWAYS
required** (the wrapper rejects a missing one as a usage error; one alone inherits the mismatched
`.roborev.toml`-pinned model and fails as a silent-looking review outage), and the branch must be **pushed
first** — the wrapper asserts that and FAILs otherwise. Three direct-CLI forms are **NON-SANCTIONED**:
`roborev review --branch` **without an explicit `--repo`** (from a worktree it resolves against the ROOT
checkout), the two-positional commit-range form (its range base is git's empty tree), and a single-SHA
review (it reviews **one commit, not the branch**). Each can report clean having reviewed NOTHING — or, for
the single-SHA form, only the last commit — and a vacuous pass is textually identical to a genuine one.
Measured: **`--repo` is what makes `--branch` correct**, so the wrapper reviews the RANGE `<base>..HEAD` and
verifies BOTH endpoints against the job record (`reviewed-sha:` is a range, not a sha; `job-record:` reports
the record's completeness). Note too that **roborev drops exactly what its configured `exclude_patterns` pathspecs match — it makes no
code/non-code judgement** — so a docs-only diff cannot be roborev-certified at all. "docs-only" means a
**code-free CENSUS**, never a `docs/` path prefix: the `docs/reports/*-artifacts/` measurement harnesses
this repo ships by convention are executable code that IS reviewed, so a PR carrying them must be
certified like any other code change (#3229). The remedy that shipped is the **configuration**: a narrowed
prose/artifact deny-list (`*.md` plus artifact extensions scoped to artifact-bearing *directories*, never a
blanket `docs/**` — which is what swallowed 33 harness executables on PR #3222), measured at 72 `docs/`
executables reaching the reviewer and 0 markdown.
**NOTHING PREDICTS THE EXCLUSION SET PRE-ENQUEUE (#3283 configured, #3278 compiled-in).** A key that did
was built on #3229 and REMOVED by owner ruling: its false-PASS count was *increasing* across review rounds,
and **a guard with known documented false-PASSes is worse than no guard, because it invites reliance it
cannot support**. So a swallowed path — by configuration or by roborev's compiled-in lockfile/cache
deny-list (`**/Cargo.lock`, `**/go.sum`, `**/pnpm-lock.yaml`, …) — surfaces **after** the review under
**`prompt-content:`**, fail-closed, with a cause that names the symptom rather than the mechanism.
Practically: **if `prompt-content:` FAILs, suspect `.roborev.toml` first**; a lockfile-only dependency bump
is still **not** roborev-certifiable; and `prompt-content:` can never print a `PASS (0/0 …)`. Verdicts still
follow one rule — **FAIL where the author can act; NOTICE where only the information is actionable; never
silence** — and no key is exempt from the affirmation backstop: all six deterministic keys must be
affirmatively `PASS`, matched on the exact verdict token, never a prefix glob.
Note also that **a `.roborev.toml` change cannot certify itself**: roborev reads `exclude_patterns` from the
repo **root path** and snapshots it at daemon start, so a worktree edit is invisible and the demonstration
belongs after the merge — generally, *any PR whose subject is a config a daemon or gate reads from root
cannot certify itself*. **Any** non-PASS terminal `RESULT` —
`NOTHING-TO-REVIEW` included — is a failed review round and a blocked merge, never a clean pass. Verify
which reviewer a box can actually serve with `roborev check-agents`; why:
[roborev findings](/cqlite/agents-developing/roborev-findings/) + CLAUDE.md.

## Pipelining independent lanes (retro #1889)

The lead **pipelines** near-independent issues rather than serializing on long waits (a full gate is
15–25 min, plus CI and roborev round-trips):

- While one lane's full gate / CI / roborev runs, the lead advances **other independent lanes** —
  implementation and review stages overlap freely.
- Merge-on-green is **armed per PR** (it lands when green) rather than blocking the queue on each PR's CI.
- **Only the full-gate step serializes** across lanes (respecting the #1825 machine-wide cap and measured
  ~2-gate contention); everything else overlaps.
- Long waits use **scheduled wakeups**, never idle polling.

## Operational caveats

- **Subagent model pin.** The `model:` pinned in a subagent's frontmatter is not always accessible — always
  pass an explicit, accessible `model` (e.g. `opus`) when spawning, or the spawn fails.
- **GitHub REST resilience.** Board / `gh` operations run in bursts and can hit GitHub's secondary rate
  limits. Batch reads (one `gh project item-list` over per-item polls) and, on a `403`/secondary-limit
  response, back off and retry rather than failing the run.

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal — **sense → diagnose → improve**:

- **Sense.** `flow-finalize` stamps one record per delivery cycle (issue, pr) into the append-only
  ledger `docs/reports/delivery-telemetry.jsonl` (governed by
  `docs/reports/delivery-telemetry.schema.json`) using `scripts/delivery-telemetry.py record`. A
  reopened issue that ships more than once legitimately gets one record per shipped PR — retro
  aggregation by issue treats such multi-cycle issues as multiple deliveries, not one (issue #2314).
  The same holds for an issue that ships one or more **slices** while deliberately remaining OPEN
  (issue #3550): stamp each with `--slice`, which writes `closed_at: null` (the marker) and bounds
  `cycle_time_s` on the PR's `mergedAt` — the authoritative terminal timestamp of a slice — and `retro`
  reports those records as their own SLICE class rather than as completed issues. `--slice` asserts the
  issue was open **when the PR merged**, which its CURRENT state cannot decide (GitHub records an
  auto-close AFTER the merge, so an ordinary completed delivery and a late-stamped slice have
  indistinguishable timestamps). Since issue #3559 the tool decides it by replaying the issue's own
  **timeline** to the PR's `mergedAt`, and the rule is a **conjunction**: slice ⟺ the issue was OPEN at
  `mergedAt` **AND** this PR closes NOTHING. Both halves are permanent — every auto-closing PR's issue
  was *also* open at `mergedAt`, because the close is recorded afterwards, so only the PR's own
  `closingIssuesReferences` separates "open because the issue is never closing" from "open because the
  close lands five seconds later" (a slice PR closes NOTHING). A slice is therefore stampable after its
  issue has been closed or reopened — which is what unblocked the three owed #3393 records
  (#3407/#3429/#3467) — and is refused when the **last** `closed`/`reopened` event
  STRICTLY BEFORE `mergedAt` is a `closed`, because that delivery COMPLETED the issue and a later
  reopen does not change it. The *last* one decides, so a close FOLLOWED by a reopen before the merge
  leaves the issue open at `mergedAt` and is ACCEPTED. An
  event in the SAME SECOND as `mergedAt` is a third answer, neither before nor after: both GitHub
  timestamps are one-second resolution, so the tie is **unmeasurable** and is refused as such rather
  than resolved permissively.
  `--slice` is an operator **assertion**: the tool refuses it wherever it can be **disproved**, and
  where it cannot be, the assertion stands. One residual is genuinely **undecidable** and is not
  claimed: a completed delivery whose PR omits `Closes #N` and whose issue is closed by hand later is
  observationally identical to a genuine slice completed later by another PR, so the difference is
  intent — bounded by doctrine (`flow-implement` mandates `Closes #<N>`), not by mechanism. The two available
  workarounds are FORBIDDEN: closing the issue to satisfy the tool (a tool's data model must never
  decide whether a problem is recorded as solved), and hand-appending a line to the JSONL past the
  validator (the tool is the gate on the ledger's shape).
  Records carry **authoritative data only**: GitHub-derived
  timestamps (issue/PR open + merge + close → cycle time and coarse phase durations) plus run-observed
  counters — claim collisions, rebase/conflict events, agent-gate pass/fail + run count, roborev findings,
  and rework. A counter that was not observed is an **error**, never a fabricated `0` (no-heuristics
  mandate). `delivery-telemetry.py lint` schema-validates every line. **The stamp lands via a
  `telemetry-<N>` PR-in-worktree, not a direct push** — `main` blocks direct pushes (PR required for every
  commit, `enforce_admins=true`). `flow-finalize` branches a throwaway worktree off `origin/main`, appends
  the record (note `record` writes to the script's repo ledger, not `$PWD` — verify it lands in the
  worktree and leave root clean), and opens a telemetry-only PR that merges on its own green `required`
  check. The ledger is a hot append-only file: resolve any rebase conflict by **keeping all lines**, never
  dropping a peer's record. Never `git checkout` in the shared root to do this — a closer that switched
  root onto a telemetry branch and died stranded it off `main` and broke every concurrent session.
- **Diagnose.** On a cadence (per-epic or weekly) the manager runs `delivery-telemetry.py retro`, which
  ranks the recorded failure categories by a **documented weighted tally** (`Σ count × weight` — a
  deterministic policy table, not an inferred or learned model) and reports the single highest-cost
  recurring failure. Default is a dry-run print; `--file` files a `flow-meta` improvement issue, **deduped**
  against open `flow-meta` issues by a stable category marker.
- **Improve.** That `flow-meta` issue enters Ready and flows through the normal pipeline.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.
