# 0.19 release plan and testing regimen

Owner rulings from the roadmap evaluation of 2026-11-10. This file records what was decided and why.
[Tracker state](#tracker-state) lists what was applied to GitHub and what was not.

## What 0.19 is

A scope-driven, read-only forensics release. It ships when the required items are done; there is no date.

| Tier | Items |
|---|---|
| Required | Vortex export #4237, audit #4195, find #4205, diagnose #4204, explain #4193 |
| Stretch (does not hold the release) | extract/split #4199, with the write-verb contract #4496 |
| Moved to 0.20 | scrub #4198, convert #4202, rekey #4203, tombstones #4200, diff #4201, purge #4276, BTI locator follow-up #4337 |
| Out of the release | CEP-11 memtable plugin #1807, which continues as an unmilestoned research spike |

Why the cut: 0.18 planned the whole of epic #4192 and shipped four tools. At evaluation time the 0.19
milestone held 38 open issues, about 14 on theme, and no on-theme work had landed since v0.18.0. The five
required items are read-only and each has an oracle that exists today. Scrub and convert have no
Cassandra-side oracle commissioned; diff and tombstones have no spec.

## Order of work

1. **Prerequisites, in sequence.**
   - #4159 (a scan over an unreadable SSTable returns `Ok(empty)`). Redo on current main under the earlier
     option (A) ruling. PR #4162 is conflicting, untouched since 2026-09-08 and touches 45 files; it is
     closed and kept as a reference for its tests. Audit, find and diagnose cannot report "nothing found"
     truthfully until this lands.
   - #4230 (silent fixture skips), #4255 (point-vs-full never proves the point path served), #4256
     (declared outcomes, non-vacuousness meta-tests). Draft PR #4247 for #4230 is redone on main the same
     way. #4255 and #4256 join the milestone.
   - The Cassandra 5.0.8 migration, to completion: #4482 stages 2 and 3 (PRs #4489, #4493) and the twelve
     `cassandra:5.0.2` image pins in `.github/workflows`. **No new goldens until it is finished.**
2. **Parallel lane from the start:** Vortex export #4237. Its oracle is the Parquet differential, so it
   needs no Cassandra goldens.
3. **Tools:** audit, find, diagnose, then explain. Explain is the largest item and touches the reconcile
   hot path, so it goes last, with the reconciliation-law tests (#4497) landing just before it.
4. **Alongside the tools:** the fuzzing additions below, #4498 on `salvage` and `rebuild`, #4360
   (cross-binding varint).
5. **Stretch:** extract/split #4199 with #4496.
6. **Release criteria**, below.

## Rulings on scope questions

- **Spec approvals.** The 2026-09-23 Seam 1 approvals stand after a mechanical refresh of each spec branch
  (rebase, 5.0.8 references, stale blocker notes removed). Re-approval is needed only when an acceptance
  criterion changes. Explain starts from current main; its stalled branches are reference only.
- **Writer bugs.** extract/split fails closed on tables it cannot write correctly: it refuses, with the
  cause named, any table with vector columns or `list<frozen<udt>>` (#4164, #4165), and its `--help` says
  so. #4163, #4164, #4165 and #4154 move to 0.20, where fixing them is a prerequisite for the write verbs.
- **Random schema generator #4257.** Out of 0.19. It becomes the lead testing item for 0.20, raised from
  P3 to P1. The #4497 generators are written so it can reuse them.
- **Milestone trim.** Off-theme issues leave the milestone without being closed: perf epics (#2817, #1467,
  #1470, #1685), allocator work (#3997, #4120, #3287), compressed writes and fixtures (#3558, #1509,
  #2380), gate and process items (#4310, #4150, #4331, #3680, #4174, #4153, #1760, #2180). This follows
  the 2026-09-01 ruling that the release milestone carries product work only.
- **0.18 leftovers.** #4361, #4362, #4394, #4395, #4377 and #4378 are post-release CI flakes, not 0.18
  work. They are unmilestoned and the 0.18 milestone closes, after #4361 (ws0 prober) is retested in case
  it is obsolete with the fleet.
- **Cross-binding parity** stays nightly and label-triggered for this release. #4360 stays in 0.19.

## Testing regimen

### What the evaluation found

Guarded mechanically since the recent escapes: unexecuted suites (disposition record plus five
merge-gating CI tiers), SIGPIPE under pipefail (ratchet), worktree binary reuse (per-worktree target
directories), stale dataset cache (committed fixtures re-asserted after restore).

Still open:

1. Nothing merge-gating has Cassandra read a CQLite-written SSTable.
2. CI runs Cassandra 5.0.2 while the authority is 5.0.8.
3. Corpus-gated tests can skip silently (#4230); CI is stricter than the local gate on write and CLI tests.
4. The nightly full gate was red from 2026-11-02 to 2026-11-08 and blocked nothing (#2911).
5. The merged result is not certified: `strict: false`, no merge queue (#3680).
6. Fixture coverage by type and position is thin; the vector and UDT bugs were found by reading Cassandra
   source, not by tests.
7. Two fuzz targets run nowhere; nothing fuzzes above the decoder layer.
8. Complexity is unguarded: the clippy threshold is dead config (#4042).

### What was added

| Issue | What | Placement |
|---|---|---|
| #4497 | Property tests: reconciliation is commutative, associative, idempotent; byte-comparable encoding preserves order | 0.19, required |
| #4498 | Crash-consistency harness: inject a failure at every write, fsync and rename boundary | 0.19, required, scoped to `salvage` and `rebuild`; covers extract/split if it lands |
| #4496 | Write-verb contract: metamorphic relations, a Cassandra-reads-our-output leg, self-verifying writers | 0.19 stretch, ships with extract/split or moves to 0.20 with it |
| #4499 | Kani bounded proofs on the vint codec, ending in an adopt/drop recommendation | 0.20 |
| #4500 | Weekly advisory `cargo-mutants` over writer and reconcile; never a gate (#4259 non-goal respected) | Unmilestoned tooling, Ready |
| #4042 | Complexity as a ratchet: baseline today's scores, fail on growth or a new function over 30 | Unmilestoned tooling, Ready |

Fuzzing, tracked in #4502:

- Wire `fuzz_block_emit` and `fuzz_commitlog_frame` into the nightly matrix. `find` reads unflushed
  CommitLog mutations, so the commitlog target is no longer optional.
- Add a target each for audit and find, seeded from the corruption corpus: never panic, hang or exceed the
  memory budget, and never report success on input that could not be read.
- A fuzz finding is still filed as a bug and does not block merges, but a finding in a 0.19 tool blocks
  the release.

Considered and deferred:

- **Lean 4 / formal proof.** Not adopted. A proof shows code matches a model; the recurring defects here
  (#4155, #4164, #4165, #3002) are a wrong model of Cassandra, which only Cassandra-written bytes can
  reveal. If tried later, the byte-comparable encoder is the best target.
- **Miri and Loom.** Later items.
- **PMD.** Java only. It could cover `trino-connector/`; nothing is set up. The Rust equivalent is clippy
  plus `rust-code-analysis` for triage.
- **"How CQLite Is Tested" docs headline #4258.** Waits for 0.20, when the randomized layer exists.

### Lessons added to `test-oracles.md`

1. A compiled test is not an executed test (#4420, #4421, #4422).
2. The oracle itself drifts between Cassandra versions: a golden can change while `Data.db` is
   byte-identical (#4482, #4486).
3. Wall-clock and host-dependent asserts stay out of the merge path (#4232, #4376, #4403).
4. A read error must reach the exit code; an empty result is never a substitute (#4159, #3721).

## Release criteria

- Every required item merged through the normal endgame.
- A field round on easy-db-lab: audit, find, diagnose and explain run against snapshots from a real
  multi-node Cassandra 5.0.8 cluster. Each completes on every table, reports no false corruption on
  healthy data, and stays under the 128 MB budget. It needs a new test plan (#4503); the existing
  plans cover only Flight/Trino load testing. Vortex is exempt.
- The nightly full gate is green for the seven consecutive nights before the tag; a red night is triaged
  the next day.
- The release commit itself gets a full gate of record on Linux, run on main after the last merge.
- No open fuzz finding in a 0.19 tool.

None of this changes the standing decision that CI becomes the gate of record only after the coverage
audit and after the tooling-tests tier is required.

## Tracker state

Applied on 2026-11-10:

- Milestone 0.20 created. The trim, the 0.20 moves and the 0.18 clean-up above are done; milestone 0.18
  is closed. #4255 and #4256 are in 0.19; #4257 is P1.
- Issues filed: #4496 to #4500, #4502 (fuzzing), #4503 (field round), #4504 (these docs).
- PR #4162 and draft PR #4247 closed, branches kept as reference.
- Rulings posted on epics #4192 and #4259, and on #4159, #4199 and #4230. The write verbs (#4198, #4199,
  #4202, #4203, #4276) carry a comment binding them to #4496 and #4498; #4042 carries the ratchet scoping
  note.

Not applied:

- Board Status for the new issues, for #4042 and #4500 (Ready), and for the stale `status:*` labels on
  #4193, #4237 and #4259. The session token lacked the `project` scope, and the labels are derived from
  the board.
- #4361 was not retested; it is on the `dev` gate host and still live as far as its text shows.
