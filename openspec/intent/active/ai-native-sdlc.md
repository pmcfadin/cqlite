---
slug: ai-native-sdlc
areas: [scripts, .github, .claude, docs]
stream: delivery
priority: P1
status: active
after: v0.17
---

# Intent: the delivery loop costs more than the work it certifies

## What I cannot do today

I cannot land a small, correct change in less than a day.  Measured over 590 completed deliveries in
`docs/reports/delivery-telemetry.jsonl`: median cycle time from PR open to merge is **34.2 hours**.
Median time from issue to PR open is about **4 hours**.  So roughly **27 hours per change** is spent
after the code is written.

The certification that consumes those hours almost never rejects anything.  The gate passed 581 of
590 deliveries (**98.5%**).  Over the same population **71.4%** of PRs needed rework, at a mean of
**3.32 rounds** and **6.92 roborev findings** each.  The rework comes from the review loop, not from
the gate.

Each rework round pays the inner loop again, and the inner loop is not scoped to the change.
`CLAUDE.md` records the mechanism plainly: `--lite` dispatches the identical `run_clippy` the full
gate does (`scripts/agent-gate.sh:17233` versus `:18220`), and `run_clippy` never reads the diff.
The repo's own measured bands, over 188 completed lite runs: a no-op warm, **2-7 minutes part-warm**,
and **16-24 minutes cold**.  A `cqlite-core/src/` diff measures **median 20 minutes, range 3.8-43**
(n=20), with **~104 minutes under peer load** reported in #3764.

Two further costs compound it.  `--lite` is exempt from the #1825 gate-slot cap, so on a shared box
its build competes with a peer's gate of record with nothing arbitrating (#3763 owns that gap).  And
a rebase voids the roborev round by doctrine, while `main` moved 12 times in 4 hours on the night
#3650 was filed.  So a change can be forced to re-review work that did not change.

## Who is affected

Every lane.  19 worktrees exist and `CQLITE_GATE_MAX_CONCURRENCY=1` is persisted by
`bootstrap-agent-machine.sh`, so full gates serialize per box regardless of how many agents run.  The
throughput ceiling is set by certification cost, not by how much work is ready.

## What better looks like

A fix round costs a time proportional to the diff.  A narrow diff certifies in under two minutes,
cold or warm.  Recurring review findings are mechanized as lints rather than found by a reviewer for
the seventh time.  Nothing that certifies is silently narrowed: a scoped check names what it did not
check, per the affirmative-zero doctrine.

## Constraints, taken as given

- **The full gate stays the gate of record.**  `scripts/agent-gate.sh` and its `AGENT-GATE SUMMARY`
  remain the only verdict that counts (#719).  No slice weakens that.
- **The full gate stays out of the required per-PR check set.**  `openspec/specs/gate-ci-coverage/spec.md:22`
  makes that a deliberate requirement so it does not duplicate the light always-running
  `pr-gate.yml` from epic #1360.  This intent does not reverse it.
- **No component may pass vacuously.**  Every narrowing must be visible in the SUMMARY and must
  distinguish *checked and clean* from *not checked*.
- **No new bypass flags.**  A cost fix that can be switched off buys a vacuous green.

## Slices

Each slice is its own OpenSpec change, filed separately, and each stands alone.  Ordered by measured
value over cost.

| # | Slice | Removes | Status |
|---|---|---|---|
| 1 | `--lite` clippy scoped to the diff's blast radius | The 16-24 min cold clippy leg on every fix round | **this intent's first change** |
| 2 | Bound the `cqlite-core/src/` fan-out leg | The `--all-targets --no-run` compile of every dependent, reported at +18 GB in #3763/#3764 | filed after slice 1 |
| 3 | A nit cap and named passes in a root `REVIEW.md` | Uncapped findings driving 3.32 rework rounds | filed after slice 1 |
| 4 | Mechanize the remaining recurring roborev classes as `roborev-lints` | Findings a reviewer still catches by hand | #3499 owns one of these already |
| 5 | Trim `CLAUDE.md` to its rules-and-pointers charter | Context spent on mechanism that `gate-ops.md` and `pm-operating-loop.md` already hold | partly done by #4092 |
| 6 | Batch the delivery-telemetry stamp | 44% of the last 200 commits being one telemetry PR per delivery | independent |
| 7 | Delete verified-dead delivery machinery | ~5,800 lines nothing invokes | independent |

## Non-goals for the whole intent

- **Relocating the gate of record into CI.**  Ruled out above by an existing requirement.
- **Raising `CQLITE_GATE_MAX_CONCURRENCY`.**  #1825 capped it for measured reasons; slices 1 and 2
  reduce what a slot costs instead of contending for more slots.
- **Changing the endgame order.**  Rebase → gate → C → roborev → premerge-assert → arm is the
  #2084/#2086/#2087 design and is not re-litigated here.
- **Changing Seam 1.**  Owner approval of spec and design stays the standing human gate.
- **A dependency-closure blast radius.**  Slice 1 uses the package set `scoped-tests` already
  derives.  A true closure needs rustc dep-info, which is #3366's route.

## Open questions for the owner

1. Slice 3 sets a nit cap.  What number, and does a capped nit become one linked follow-up issue at
   merge, as the current severity triage already says?
2. Slice 7 deletes `.github/issue-monitoring/` (2,321 lines, no CI job invokes it).  Delete, or wire
   it?
3. Slice 6 changes when telemetry lands.  Is a nightly batch commit acceptable, given
   `delivery-telemetry.py retro` is the only consumer?

## Evidence

- Delivery numbers: `docs/reports/delivery-telemetry.jsonl`, 590 records.
- Lite cost bands and the clippy scoping defect: `CLAUDE.md`, the Agent Gate section, which cites its
  own measured populations (n=43 narrow-warm, n=20 core-src, n=188 lite runs).
- Gate slot cap: `docs/development/pm-operating-loop.md:146`.
- Base churn rate: `openspec/changes/certified-tree-vs-merged-tree/proposal.md`.
- Full audit behind this intent: `docs/development/ai-native-sdlc-redesign.md`.
