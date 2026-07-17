# The CQLite Agent-Delivery Journey

*A case study and reusable playbook for engineering teams adopting LLM-agent delivery workflows.*

**As of:** 2026-07-11 · **Repo:** `pmcfadin/cqlite` · **Scope of the data:** the delivery-telemetry
ledger (`docs/reports/delivery-telemetry.jsonl`, 273 records, 2026-06-28 → 2026-07-11), the operating
manual (`CLAUDE.md`), the doctrine site (`agents-developing/`), the OpenSpec change archive, and the
git history.

> Every number in this report is computed from a repo artifact and the source is named inline. Where
> a claim comes from lived operation and is not reconstructable from a committed file, it is marked
> *(observed in practice)* rather than given a false citation.

---

## Executive summary

CQLite is a Rust library that reads and writes Apache Cassandra 5.0 SSTable files without a cluster.
Almost all of its code is written by LLM agents. Over roughly two weeks of instrumented delivery the
project shipped **273 delivery cycles across 272 distinct issues** (ledger row count), each one an
issue that went from a scoped GitHub issue to a merged, gated, reviewed PR — under a small standing
crew of agents supervised by one human owner.

The interesting result is not that agents can write code. It is that a **process** made agent output
trustworthy enough to merge without a human reading every diff. The load-bearing ideas, each covered
below, are:

1. **One gate of record.** A single script, `scripts/agent-gate.sh`, produces the only verdict that
   counts. Ad-hoc test runs never count. Its 24-component summary block is the merge criterion.
2. **Fail-closed everywhere.** Missing fixtures fail the gate; a staleness-probe error forces a
   rebuild; an unsupported config returns a typed error. Nothing degrades silently.
3. **Two human seams only.** A person approves the design spec, and the merge is pre-authorized on
   green. Everything between is automated with evidence.
4. **Tiered gates + review-first.** A cheap `--lite` gate runs every fix round; the expensive full
   gate runs after review, so the one costly certification is spent on already-reviewed code.
5. **Disposable contexts.** The high-volume text streams (full-gate stdout, adversarial-review
   churn) run inside short-lived sub-agents that return a compact packet, so the orchestrator's
   context stays the size of one issue no matter how many issues flow through it.
6. **A board is the only dispatch authority, a pushed branch is the only lock.** Coordination across
   machines is deterministic, not conventional.

Measured against the project's own earlier baseline (1.87 gate-runs/issue, 1.92 rework rounds/issue,
54% first-pass — recorded in the project's memory as the pre-restructure numbers), the mature process
holds steady-to-better: **1.79 gate-runs/issue** overall and **1.71 on the most recent 80 cycles**,
with **first-pass rate rising from 54% to 61%** on that recent window (all computed from the ledger,
method below). The point is less the exact deltas than that the process is *measured at all* — every
change to how work is done is run as an experiment against a telemetry baseline.

The report closes with a single issue traced end-to-end — **#2310, the Arrow Flight "warm handles"
performance epic** — because it exercises every part of the machine at once: a design spec with 8
requirements, a mid-flight feasibility STOP, six rounds of adversarial review with a security class
that recurred three times, and a disposable-context endgame that merged on green.

---

## The journey, in phases

### Phase 0 — Origins: from a single agent to a gate of record

The project began the way most do: one agent, one prompt, "make the tests pass." That does not
scale, because *the agent grades its own homework*. An agent that runs `cargo test` in some scoped
way and reports green has told you almost nothing — you don't know which tests, with which features,
against which fixtures.

The first structural move (issue #719, the "orchestration harness v2") was to make **one script the
sole source of truth**: `scripts/agent-gate.sh`. Its rule, stated in `CLAUDE.md`, is blunt — "its
`==== AGENT-GATE SUMMARY ====` block is the verdict; ad-hoc cargo runs never count." The full gate
today runs **24 components** (verified via `scripts/agent-gate.sh --list`): `file-size`, `fmt`,
`clippy`, `core-tests`, `tombstones-scan`, `integration-tests`, `write-tests`, `cli-tests`,
`compaction-byte-parity`, `query-semantics-oracle`, `python-bindings`, `node-bindings`,
`minimal-build`, `smoke`, and ten more guards.

Two refinements made the gate agent-proof rather than just strict:

- **Summary-block-only retention (#1175 / #2079).** The gate writes its verdict to a summary file;
  the agent is instructed to read *only* that file, never the multi-thousand-line `gate.log`. An
  agent that pastes raw stdout as "the summary" is caught because the real summary has a distinct
  header the log does not.
- **Fail-closed on missing inputs (#2078).** If the validation corpus is absent, the full gate does
  not pass-with-a-shrug — it stamps `missing-fixtures: FAIL-CLOSED` and fails. A test that would
  return "0 rows, all green" on an empty dataset is treated as a failure, not a pass.

**Generalizable rule:** *the definition of "done" must be a single artifact the agent cannot forge
and cannot satisfy vacuously.*

### Phase 1 — Correctness doctrine: parity is truth

CQLite's job is byte-level compatibility with Cassandra, so "looks right" is never enough. Three
doctrines encode that:

- **No-heuristics (#28).** Decode from authoritative metadata only — the schema, else `Statistics.db`.
  No guessing a type from a byte pattern. Legacy guesswork lives behind an opt-in feature flag and
  nowhere else. This is enforced in code (`BigVersionGates::from_version` rejects pre-`na` formats),
  not just in review.
- **Two parity oracles (#1742).** This is the subtle one. *Physical-dump parity* compares CQLite's
  output against `sstabledump` cell-for-cell — but it enumerates tombstones and shadowed rows too, so
  it **cannot catch a read-time reconciliation bug** (both sides keep the shadowed row → green while a
  real `SELECT` diverges). So a second oracle, *query-semantics parity*, records the
  post-reconciliation result of a canonical `SELECT` at a **pinned** `now` (never wall-clock). Bug
  work must add the *correct* oracle for the property under test.
- **Wiring evidence.** A feature is done only when its public surface exercises it: a named entry
  point + a call chain + an end-to-end test. Green helper-unit tests are explicitly declared
  insufficient.
- **Red-proven regression tests.** A regression test must be shown to fail on the unfixed code
  (revert the source, re-run, watch it go red) before the fix counts. A test that passes both before
  and after proves nothing.

**Generalizable rule:** *pick your oracle for the actual property under test; an oracle that is green
on a real bug is worse than no oracle because it manufactures false confidence.*

### Phase 2 — The delivery pipeline

The work was factored into a five-verb pipeline, each verb a skill an agent runs:

```
flow-groom  →  flow-activate  →  flow-implement  →  flow-address  →  flow-finalize
   (scope)      (SEAM 1: spec)     (build+review)     (PR comments)    (archive+close)
```

- **Exactly two human seams.** Seam 1 is the owner approving a design spec. The second is the merge —
  and even that is *pre-authorized*: a worker merges its own PR the moment the bar is met (gate PASS +
  intent-audit PASS for design work + review clean). The human is not in the inner loop.
- **Routing at groom time.** Design-driven work (new surface area, UX, perf strategy) goes through
  **OpenSpec** — a proposal/design/tasks/spec artifact set that *is* the plan. Oracle-driven bug
  fixes (a parsing bug with a parity test) skip OpenSpec entirely and stay a GitHub issue plus a
  pinned test. Choosing the wrong track is a real cost, so it is decided explicitly at intake. In the
  ledger, **63 of 273 cycles are design-routed, 210 oracle-routed** — bug fixes dominate, as expected
  for a maturing library.
- **1:1:1:1.** One issue ↔ one worktree/branch ↔ one OpenSpec change ↔ one PR. This makes every unit
  of work independently claimable, gate-able, and reversible.

The archive holds **68 shipped OpenSpec changes** (`openspec/changes/archive/`), the durable record
of every design-driven delivery.

### Phase 3 — Scaling out: dispatch and locking as mechanism, not convention

Multiple agents on multiple machines need coordination that doesn't rely on anyone being polite.

- **The board is the sole dispatch authority (Path A, #1886).** Work is selected from one GitHub
  Project `Status` field (`Backlog / Ready / In Progress / In Review / Done`). Status *labels* are
  decorative and are never a selection source. Empty `Ready` column = stop. Board unreachable = stop
  and fix auth — never fall back to guessing from labels.
- **A pushed branch is the only lock.** Claiming an issue means pushing its `issue-<N>-<slug>` branch
  to origin. The branch — not an assignee field — is the lock; you proceed only after re-reading and
  confirming you still hold it. This makes claim collisions a git push rejection, which is
  deterministic.
- **Heartbeats + deterministic reaping (#2089).** A claim emits a liveness heartbeat; the board
  reaps a claim only when it is provably dead (age > 4h **and** no open PR). No human judgment call
  about whether an agent is stuck.
- **One worker per machine (#1930).** A machine runs one lead/worker that fans out sub-agents but
  **serializes its own full gates** (a hardware backstop, since a full gate is heavy). An unattended
  supervisor (`scripts/local/worker-supervisor.sh`, #2090) recycles one worker process per issue —
  the hard context bound is the process exiting after each issue.

**Generalizable rule:** *encode coordination as mechanism (a lock that is a git push, a reap that is
an arithmetic on timestamps), not as etiquette an agent might not follow.*

### Phase 4 — Context economy: keeping the orchestrator O(1 issue)

This is the phase that made high throughput sustainable, shipped as epic #2083 (12 children merged
2026-07-06). The problem: an LLM orchestrator has a finite context window, and naively it fills with
gate logs, review transcripts, and prior-issue state until it degrades. The fixes:

- **Tiered gates.** A cheap `--lite` gate (~1–5 min: file-size + fmt + scoped clippy + blast-radius
  tests) runs **every fix round** (#1821). A `--delta` gate re-certifies a *post-PASS, test/docs-only*
  polish round without a full rebuild (#1892). The **full gate runs once per issue**, at the endgame.
- **Review-first (#2086).** Adversarial review (`rust-reviewer` + the `roborev` reviewer) runs on the
  lite-green diff *before* the one full gate, so the expensive certification is spent on
  already-reviewed code — not on a diff that review is about to change.
- **The disposable `flow-closer` (#2084).** The endgame — the one full gate, the intent audit, the
  final review pass, the merge — runs inside a short-lived sub-agent that returns only a compact
  terminal packet (verdict, PR URL, ≤10 residual lines). The two largest text streams in the whole
  system (full-gate stdout, review churn) never touch the persistent lead session.
- **Severity triage (#2088).** Review findings are **blockers** or **nits**. Blockers are fixed
  pre-merge and each re-triggers a fix → lite → re-review loop. Nits never trigger a re-verify round —
  they are batched into one follow-up issue at merge time. "When in doubt, blocker."
- **Inter-issue reset (#2085).** After each finalize, the lead drops all prior-issue context and
  re-hydrates the next item from *board + disk alone*. Durable lessons go to a `MEMORY.md` file, never
  the live window.

**Generalizable rule:** *treat the orchestrator's context window as the scarcest resource. Run
high-volume streams in disposable contexts and keep only decisions.*

### Phase 5 — The audit program: scoping as a first-class artifact

Rather than wait for bugs to surface, the project ran systematic static audits of each subsystem —
read-path, write-path, parser, export, bindings, platform. Each audit produced a **lettered epic with
pre-scoped child issues whose bodies are written to be "lesser-model-ready"** — detailed enough that a
cheaper model can implement them. For example the read-path audit alone produced epics A–G with ~39
children (#1562–#1600). Across all audit blocks this is on the order of **~200 pre-scoped children**
*(the individual epic ranges are recorded in the project memory; the total is an aggregate across
blocks A through AL)*.

The insight: **scoping is itself a deliverable.** A well-scoped issue is the interface between an
expensive planning model and a cheap implementation model. Investing a strong model in decomposition
lets weaker, cheaper agents do the bulk implementation reliably.

### Phase 6 — 0.14 field-readiness and the performance journey

The 0.14 release was re-scoped by a **291-issue viability audit** *(observed in practice; the audit
artifact is a published report)* into a "Flight field-readiness" release, validated by a five-tier
test plan: per-PR gates → a combined-main sweep → a local field-repro end-to-end run → close blockers
→ an AWS field run on real infrastructure as the handoff gate. A field-shaped Docker testbed (#2289 —
Trino → Arrow Flight → CQLite over real SSTables) was hardened over ~11 find-fix rounds and used as
the standing local reproduction loop.

The performance story is the sharp edge of that work, and it is worth stating in numbers because it
motivates the case study. Cost-model research (`docs/architecture/issue-2310-ms-point-reads-research.md`,
static analysis at `main`) established the baseline against a **2.16M-partition-per-node** AWS table:

| Query | Baseline latency | Source |
|---|---|---|
| `WHERE key = '<pk>'` point read | **271 s** (full scan, killed) | #2157 round-3 |
| `SELECT * … LIMIT 5` | **190–433 s** | #2157 round-3 |
| `SELECT count(*)` | **358 s+** | #2157 round-3 |

The root cause: a `WHERE pk = X` query was **not a point read at all** — it full-scanned every
partition through the k-way merge and applied the predicate as a per-row egress filter, paying
`O(table)` I/O to return one row. That single term dominated latency "by 3–4 orders of magnitude."

The fix was sequenced, not heroic:

- **Phase 1 (#2207 PK pushdown + #2295 snapshot completeness + #2302 pair resolution, all shipped):**
  turn the scan into an index probe — `O(log n)` index lookup + ~3 partition reads + reconcile. The
  research projects this at **single-digit ms flight-direct**, with the through-Trino path
  "low-double-digit to ~100ms" (the coordinator becomes the bound). *(Field-observed through-Trino
  figures land in the tens-to-~150ms range depending on shape — observed in practice.)*
- **Phase 2 (#2310 warm handles):** once the probe is cheap, the dominant *remaining* fixed cost is
  re-parsing the same schema + Index + Summary + Statistics + bloom on every request. Phase 2 caches
  that parsed state across requests. This is the case study below — and it was benched the day it
  merged (flight-direct over real tonic transport, 100,000-partition table, 1 cold + 5 repeated
  requests per shape; results posted on #2310): a repeated point read dropped from **832 ms to
  0.62 ms (~1,350×)**, `LIMIT 100` from 825 ms to **0.79 ms (~1,040×)**, and a full 100k-row scan
  from 985 ms to **159 ms (6.2×**, the remainder being decode/stream, not parse). The work-done
  counters prove the mechanism: `reader_opens=1, hits=5, refresh_unchanged=5` — only the cold
  request opened readers; every repeat elided the parse entirely.

**Generalizable rule:** *measure before you optimize, and sequence the levers — #2310 caches the
parse cost of a scan that Phase 1 first had to stop doing. Caching the wrong thing first would have
optimized a bug.*

---

## The operating system we ended with

Putting the phases together, the steady-state machine looks like this.

**Roles (agents):**

| Role | Responsibility |
|---|---|
| `flow-lead` | Orchestrator/PM. Drives the pipeline, sequences specialists, writes no production code. |
| `sstable-developer` | Implementation (TDD) inside the issue's worktree. |
| `rust-reviewer` | Read-only Rust review against project standards. |
| `roborev` | Adversarial automated review (the security/correctness net). |
| `spec-auditor` | Intent audit ("C") — implementation vs the OpenSpec requirements. |
| `coverage-reviewer` | Test-quality review — meaningful, not merely present. |
| `flow-closer` | Disposable endgame owner — the one full gate, C, final review, merge, finalize. |

**The implement loop (the inner cycle):**

```
implement (TDD)
  → --lite gate every fix round        (cheap, summary-file redirect)
  → rust-reviewer + roborev on the lite-green diff   (review-first)
  → fix rounds: --lite re-cert + diff-scoped tests   (never a full gate per round)
  → open PR
  → flow-closer { full gate ONCE → C intent audit → final roborev → merge-on-green → finalize }
```

**The gate tiers:**

| Tier | When | What it certifies |
|---|---|---|
| **Full** (24 components) | Once per issue, in `flow-closer` | The merge verdict of record. |
| **Lite** (~1–5 min) | Every fix round | fmt + scoped clippy + blast-radius tests. Distinct summary — can never be pasted as the full one. |
| **Delta** | Post-PASS test/docs-only polish | Re-certifies a narrow diff without a rebuild; fails closed on anything else. |

**The two human seams:** design-spec approval (Seam 1) and merge (pre-authorized on green). That is
the entire standing human surface.

---

## Metrics

All figures below are computed directly from `docs/reports/delivery-telemetry.jsonl` (273 records,
schema `docs/reports/delivery-telemetry.schema.json`). Each record is one delivery cycle and holds
*authoritative data only* — GitHub timestamps and run-observed counters, never an estimate. A
reopened issue that ships twice is legitimately two records (hence 273 records over 272 issues).

**Method:** `gate_runs` counts full-gate runs through and including the first PASS, so
`gate_runs == 1` is a clean first-pass. `rework` is the re-open/re-review count. `roborev_findings`
is the raised-finding count; where classified, `roborev_blockers + roborev_nits` reconcile to it.

### Overall (all 273 cycles)

| Metric | Value | Baseline *(project memory, pre-restructure)* |
|---|---|---|
| Delivery cycles / distinct issues | 273 / 272 | — |
| Gate-runs per issue (mean) | **1.79** | 1.87 |
| Gate-runs p90 / max | **3 / 9** | p90 3 |
| Rework rounds per issue (mean) | **1.83** | 1.92 |
| First-pass rate (`gate_runs == 1`) | **55.3%** (151/273) | 54% |
| Roborev findings (mean / total) | **3.26 / 889** | — |
| Final gate outcome | 272 pass / 1 fail | — |
| Median cycle time (issue open → close) | **27.4 h** | — |

### Recent window (80 most recent cycles, PR > 2098 — i.e. under the mature context-economy process)

| Metric | Value |
|---|---|
| Gate-runs per issue (mean) | **1.71** |
| First-pass rate | **61.2%** (49/80) |
| Rework rounds (mean) | 1.95 |

First-pass rate improved from 54% (baseline) to **61%** on the recent window; gate-runs/issue held at
the better end of the baseline. (Rework ticked up slightly on the recent window — consistent with a
period weighted toward harder Flight/perf epics.)

### Routing tells you where the cost is

| Routing | n | gate-runs | rework | first-pass | findings/issue |
|---|---|---|---|---|---|
| **design** (OpenSpec) | 63 | 1.71 | **2.51** | 59% | **5.17** |
| **oracle** (bug + parity test) | 210 | 1.81 | 1.63 | 54% | 2.68 |

Design-driven work draws roughly **2× the review findings** and **1.5× the rework** of oracle-driven
bug fixes — new surface area is where reviewers earn their keep. This is the empirical justification
for routing new-surface work through a spec and a heavier review.

### Review severity (81 cycles with classified findings)

Of 273 findings across those cycles, **161 blockers vs 112 nits — a 59% blocker share.** The
adversarial reviewer is not producing mostly noise: a majority of what it raises is fixed pre-merge.

---

## Case study: issue #2310, warm handles, end-to-end

This one issue exercised every part of the machine in one week. It shipped as **PR #2350**, merged
**2026-07-11** (squash commit `2ebd883f`). The endgame delivery cycle is recorded in the ledger under
its spec child **#2345** (slug `flight-warm-handles`, routing `design`).

### 1. Design-driven, Seam-1 approval of an 8-requirement spec

Because warm handles add new server behavior, it went through OpenSpec. The owner approved a spec
(`openspec/changes/archive/2026-07-11-flight-warm-handles/specs/flight-warm-handles/spec.md`) with
**8 requirements**, each with red-provable scenarios:

1. Cache keyed on **generation identity** (device+inode of each `Data.db`, cross-checked with the
   parsed generation number), *not* the directory path — so the same files reached through a
   different per-query snapshot hardlink dir are one warm entry.
2. A **per-request staleness probe** with a **zero staleness window** — an authoritative directory/
   generation listing, never mtime or filesystem-timing inference (#28-clean).
3. A snapshot **`manifest.json` fast path** — a byte-identical manifest skips the `read_dir`, as an
   optimization only, never a weaker freshness guarantee.
4. **Fail-closed refresh** mirroring `Database::refresh()` (#1749): open every added generation
   before swapping; any open failure returns the typed error and leaves the prior warm set fully
   intact; a probe error counts as "changed."
5. **LRU byte budget** (fixed 64 MiB) inside the <128 MB discipline; a generation removed on disk is
   evicted immediately regardless of LRU age.
6. **hit/miss/evict/refresh-outcome metrics** on the existing observability contract, no new knob.
7. **Cancellation discipline** (#2264/#1473) held through probe and rebuild — a pre-cancelled request
   does zero warm-path work.
8. **Bench evidence** of ~zero parse cost on a repeated unchanged query.

This is the "design-driven" cost profile from the metrics section made concrete: high requirement
count, security-adjacent surface, heavy review ahead.

### 2. A feasibility STOP mid-flight — and a split

Partway in, the implementing agent discovered the planned approach needed a **core seam that did not
exist**: the k-way merger could not be constructed from already-open shared readers. Rather than force
a workaround into the Flight layer, the work **stopped and re-planned**. The seam was split out as
**WS0 / #2346** (`KWayMerger::new_from_readers` + per-call `scan_cancel`) and landed first as **PR
#2347** — a clean, oracle-routed delivery (ledger: `gate_runs 1`, `rework 1`, first-pass). The warm-
handle epic then consumed that seam.

**Lesson (and it is a doctrine, not an accident):** *agents must be allowed to STOP and re-plan.* The
failure mode to design against is an agent bulldozing a bad approach to completion because "finish the
task" is the instruction. A feasibility STOP that spawns a prerequisite is a success, not a stall.

### 3. The adversarial-review walk, and how it converged

Review ran review-first on the lite-green diff. Across the epic there were **six roborev rounds**
(jobs 1637–1642) plus `rust-reviewer` and `coverage-reviewer` *(round numbering observed in
practice; the ledger records the aggregate — 15 findings, 11 blockers, 4 nits for the cycle)*.

- **Rounds 1–5 each surfaced a genuine blocker, all fixed red-proven.** The PR body confirms one of
  these independently: a concurrent same-key rebuild that **duplicated readers and inflated
  `used_bytes`**, found by both `rust-reviewer` and roborev job 1637, fixed with a deterministic
  swap-barrier test that fails `left:4, right:2` with the dedup disabled.
- **A security class recurred three times.** A **path-containment** check (#1430) had to be applied
  again on each *new* file-access surface the feature introduced — the warm-probe `Data.db` read, the
  `manifest.json` read, and budget accounting. The same class of finding reappearing on successive new
  surfaces is the signature of a genuinely new attack surface, not reviewer repetition.
- **Round 6 converged by an explicit rule.** The final round produced 2 non-security findings. Under a
  **pre-declared convergence-by-scope rule**, they were declined-with-evidence and batched into
  follow-up **#2351**, and a separate UDT-registry posture question was split to **#2349**. Without an
  explicit convergence rule, an adversarial reviewer will always find *something*, and the walk never
  ends.

**Lesson:** *adversarial review on new cache/security-adjacent code converges in roughly six rounds —
but only if you declare convergence-by-scope explicitly, decline with evidence, and batch nits.* And a
second one, learned the hard way this week: **verify a reviewer's claim against ground truth before
dispatching a fix** — one roborev job false-positived a "duplicate import" that was actually a doc
comment *(observed in practice)*. Reviewer output is an input to judgment, not an order.

### 4. The endgame, in a disposable context

The close ran inside a `flow-closer` sub-agent: **one full gate of record (PASS, 24/24 components)**,
the **spec-auditor C intent audit (PASS — all 8 requirements satisfied with public-surface tests)**, a
final roborev confirmation, the squash-merge, the `openspec archive`, board → Done, and the telemetry
stamp. The lead session retained only a ~20-line terminal packet — none of the gate stdout or review
churn.

The ledger's honest record of the cycle: **`gate_runs: 5`, `rework: 5`, `roborev_findings: 15`
(11 blockers, 4 nits), `gate: pass`**, cycle time **7.7 h** (4.3 h to PR, 3.4 h in review). Note the
tension worth being honest about: doctrine's ideal is *one* full gate per issue, and this flagship
design epic took **five** full-gate runs through first PASS. Big, security-adjacent design work is
exactly where the one-gate ideal bends — and the telemetry records it rather than hiding it, which is
the point of instrumenting the process at all.

The wiring evidence, from the PR: `do_get → do_get_setup → WarmTableRegistry::warm_readers →
produce_streaming_from_readers → new_from_readers`, with an end-to-end test over real tonic transport
(`do_get_over_transport_second_request_is_a_warm_hit`) asserting per-column `ArrayData` equality
across the cold and warm requests. Every `#### Scenario:` in the spec has a test that fails by
construction on pre-#2310 `main`.

### 5. The measured payoff (spec requirement 8: bench evidence)

Benched the day of the merge — BEFORE = `2ebd883f~1` (main immediately pre-merge), AFTER = main with
the feature; flight-direct over a real loopback tonic server, 100,000-partition table, release
builds, 1 cold + 5 repeated identical requests per shape, fully serialized runs (results posted on
issue #2310):

| Query shape | BEFORE cold | BEFORE repeat (med) | AFTER cold | AFTER warm (med) | Warm speedup |
|---|---|---|---|---|---|
| Point read (`key = X`) | 821.6 ms | 832.1 ms | 832.0 ms | **0.62 ms** | **~1,353×** |
| `LIMIT 100` | 821.9 ms | 825.2 ms | 821.5 ms | **0.79 ms** | **~1,042×** |
| Full scan (100k rows) | 980.6 ms | 985.1 ms | 994.8 ms | **158.8 ms** | **~6.2×** |

The BEFORE build pays the full parse on *every* request (repeats ≈ cold); the AFTER build's repeats
are sub-millisecond on point/LIMIT — the ~830 ms of per-request parse work is fully elided — while
full-scan repeats are bounded by row decode/stream, exactly as the design predicts. The counters are
the proof of mechanism, not just outcome: `hits=5, misses=1, reader_opens=1, evicts=0,
refresh_unchanged=5, refresh_rebuilt_delta=1, refresh_fail_closed_retained=0` — one reader-open for
the cold build, zero for all repeats. Row counts were identical cold vs warm on every shape, and an
independent second run reproduced the numbers within noise. The unmodified #1494 `flight_do_get`
criterion bench (2,000 rows, steady-state) independently showed **1.59×** (5.97 → 3.77 ms/request).
Numbers are flight-direct by design — the through-Trino path adds a coordinator bound on top, so the
warm-handle effect is measured where it lives.

**Scope qualification (added after the fact — see the postscript):** these speedups apply to live
mode and stable snapshot paths. In the connector's default *per-query snapshot* mode, each query's
snapshot directory is cleared after the query, and the correctness fix below rightly forces a rebuild
whenever cached backing paths are dead — so snapshot-mode requests currently re-pay the parse.
Restoring the warm benefit there (rebind-by-inode, keeping all parsed state) is tracked as #2356.

### 6. Postscript: the nightly caught what the PR pipeline structurally couldn't

The day after the merge, the nightly Flight↔Trino docker-compose E2E on `main` went red — 9
assertions, all `streaming merge producer error: No such file or directory`. Root cause (#2352):
scans lazily re-open `Data.db` by the reader's stored *path* while point reads use a held file
descriptor, and the connector deletes each per-query snapshot dir after its query — so a warm hit
could serve a reader whose backing path was gone. Every PR-branch CI run had passed, because the
docker-compose job ran **only on the main schedule, never on PRs** (now tracked as #2358).

The machine handled it the way it is designed to: overnight red → bisect from CI history (branch
tips green, squash-merge red) → P1 issue with hypotheses → red-proven repro → fail-closed fix (a
warm hit is served only when every cached backing path still resolves; dead paths rebuild, counted
as a refresh outcome, never a stale hit) → local E2E 34/34 green → review-first (one reviewer
finding declined with an in-code adjudication, which kept the final review pass clean) → one gate of
record → merge. Same-day turnaround. Three lessons worth exporting: **(a)** integration jobs that
run only on a schedule are a structural blind spot for the exact surfaces under active change —
path-filter them onto PRs; **(b)** a test can *encode* a bug — the original fast-path test "proved"
its property by deleting live files, i.e. it asserted the unsound behavior; reviewers should ask
what a test's setup implies, not just what it asserts; **(c)** honest qualification beats a
retracted claim — the bench numbers above were correct as measured, and the fix narrowed their
scope; the report says so rather than quietly standing on the headline.

---

## Lessons / playbook — the generalizable rules

These are the transferable rules, stated so another team can adopt them without CQLite's specifics.

1. **One gate of record; everything else is iteration.** Have exactly one artifact that means
   "mergeable." Make it un-forgeable (a distinct summary block) and un-satisfiable-vacuously
   (fail-closed on missing inputs). No ad-hoc test run ever counts.

2. **Tier your gates.** A cheap gate every fix round, the expensive full gate once. Otherwise agents
   either under-verify (cheap only) or burn hours (full every round). Give the cheap gate a *distinct*
   output so it can never be passed off as the real one.

3. **Fail-closed everywhere.** Missing fixtures fail the gate. A probe error forces a rebuild. An
   unsupported config returns a typed error. Silent degradation is how agent systems ship confident
   wrongness — design it out.

4. **Two human seams only.** Approve the design, then pre-authorize the merge on green. If a human is
   in the inner loop, throughput is bounded by the human. Keep them at the two decisions that actually
   need judgment.

5. **Review before the expensive gate.** So your one costly certification lands on already-reviewed
   code, not on a diff review is about to change. The metrics show design work draws ~2× the findings
   — spend review there.

6. **Disposable contexts for high-volume streams.** Run gate stdout and review churn in short-lived
   sub-agents that return a compact packet. The orchestrator keeps decisions, not logs — its context
   stays the size of one issue no matter how many flow through.

7. **The board is the only dispatch authority; a pushed branch is the only lock.** Encode
   coordination as mechanism (a git-push lock, a timestamp-arithmetic reap), not etiquette. Make
   "stuck" deterministically detectable via heartbeats.

8. **Let agents STOP and re-plan.** A feasibility STOP that spawns a prerequisite is a success. The
   worst outcome is an agent forcing a bad approach to completion because the instruction was "finish."

9. **Adversarial review converges — if you make it.** New cache/security-adjacent code takes ~6
   rounds. Declare convergence-by-scope explicitly, decline with evidence, batch the nits into one
   follow-up. Otherwise the walk is infinite, because a good reviewer always finds one more thing.

10. **Verify reviewer claims against ground truth before acting.** Reviewer output is an input to
    judgment, not a work order. False positives (a doc comment mistaken for a duplicate import) will
    happen; a cheap check before dispatching a fix pays for itself.

11. **Route work at intake: oracle vs design.** Bug-with-a-test skips the spec; new-surface work gets
    one. Wrong routing is real cost — either bureaucracy on a one-line fix or unreviewed surface area.

12. **Pick the oracle for the actual property.** A parity check that enumerates tombstones can't catch
    a reconciliation bug. An oracle that is green on a real bug is worse than none.

13. **Red-proven regression tests; wiring evidence.** A regression test must fail on the unfixed code.
    A feature is done only when its public surface exercises it end-to-end, not when helper units pass.

14. **Measure every delivery; retro against a baseline.** One telemetry record per cycle (gate-runs,
    rework, findings) turns each process change into an experiment with a number attached. "It feels
    faster" is not a result.

15. **Scoping is a deliverable.** A well-scoped issue is the interface between an expensive planning
    model and a cheap implementation model. Invest strong models in decomposition; let cheaper agents
    implement the pre-scoped children.

16. **Keep doctrine current in the same change.** Any change to how work is done updates the operating
    manual (`CLAUDE.md`) and the doctrine site in the *same* PR. Stale process docs are how a good
    process quietly rots.

---

## What we would do differently

- **Instrument earlier.** The richest lessons here are the ones the telemetry ledger made legible.
  The pre-instrument period is a blur of "it felt fine"; the instrumented period is a set of
  experiments. Start the ledger on day one, even crude.

- **Declare the convergence rule up front.** The review walks that stayed cheap were the ones with a
  pre-declared convergence-by-scope rule. The ones that sprawled were the ones where "are we done
  reviewing?" was litigated live, per round. Make it a standing rule, not a per-issue negotiation.

- **Build the feasibility-STOP culture before the first hard epic, not during it.** The #2310 WS0
  split worked because stopping was already legitimate. A team that treats a STOP as a failure will
  get bulldozed approaches on exactly the issues where they hurt most.

- **The one-gate ideal needs a documented exception for large design work.** #2310 took 5 full-gate
  runs, not 1. Rather than treat that as a doctrine violation, name it: design epics with new
  security-adjacent surface get a higher gate budget, and that is fine. Pretending otherwise just
  produces telemetry that looks like non-compliance instead of a legible cost of hard work.

- **Route more aggressively to oracle where possible.** Oracle-routed work has half the findings and
  1.5× fewer rework rounds. Some work genuinely needs a spec — but any work that *can* be reduced to
  a bug-plus-pinned-test is cheaper delivered that way. The temptation to over-spec is real.

---

## Appendix: sources and how to reproduce the numbers

- **Ledger aggregates:** `docs/reports/delivery-telemetry.jsonl` (273 records) +
  `delivery-telemetry.schema.json`. Reproduce with a JSON pass: mean/p90 of `gate_runs`, `rework`;
  first-pass = share with `gate_runs == 1`; recent window = records with `pr > 2098`.
- **#2310 case study:** OpenSpec change `openspec/changes/archive/2026-07-11-flight-warm-handles/`
  (proposal, design, spec with 8 requirements, tasks); PR #2350 (squash `2ebd883f`, merged
  2026-07-11); ledger record under issue #2345; WS0 seam PR #2347 (issue #2346); follow-ups #2351,
  #2349.
- **Gate:** `scripts/agent-gate.sh --list` → 24 components; contract in `CLAUDE.md`; deep mechanics in
  `docs/development/gate-ops.md`.
- **Performance baselines:** `docs/architecture/issue-2310-ms-point-reads-research.md`.
- **Process doctrine:** `CLAUDE.md`, `docs/development/pm-operating-loop.md`,
  `docs/development/fleet-runbook.md`, `docs/development/roborev-severity.md`, and the doctrine site
  under `agents-developing/`.
- **Not repo-reconstructable, marked *(observed in practice)* in text:** the 291-issue viability
  audit count, the roborev job numbers 1637–1642, the duplicate-import false positive, and specific
  field-observed through-Trino latencies. These come from lived operation this week, not a committed
  artifact.
