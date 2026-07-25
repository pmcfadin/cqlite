# Proposal: Pin the reconcile generation-overlap multiplier (issue #2043 / M9)

**Milestone:** 0.17 · **Priority:** P3 · **Routing:** design-driven (new measurement instrument +
research-record verdict) · **Issue:** #2043 (WS7 repoint) · **Epic:** #2817 (0.17 throughput program,
manifest item M9)

## Why

The 0.17 throughput arithmetic (`docs/architecture/throughput-program-2026-07.md` §3) divides the
per-core fixed-pipeline rate by a **field derate of 1.5–3× (central 2×)** before applying `C(N)`.
Inside that derate sits a **compaction generation-overlap term of ~1.1–1.5×** (§3:127-129). It is the
one factor in the chain with no measurement behind it — the program's own graveyard entry (:199)
records that this term is what survived after "RF=3 as a reconcile multiplier" was killed as a
misattribution. The chain lands A4 Stage-2 at **~420k rows/s/pod against a 600k rung**, i.e. inside
the band where a 1.4× error in one term changes the verdict.

The reconcile **base** is no longer unknown and this issue must not re-measure it. Phase-0 CPU
profiling (`docs/research/phase0-scan-cost-breakdown-2026-07.md:121`) attributes **50.0 CPU-s /
32.5%** of scan time to stage 4a (k-way merge / reconcile). Phase-2 divides that out
(`docs/research/phase2-verify-stage2.md:226-232`): **~2.0 µs/row on narrow disjoint singleton
clusters** — and rules that cost **machinery-dominated** (`ReconcileState::new` per cluster, the
`winners` HashMap, the `order` Vec), *not* comparison work. Phase-0's corpus has **no overlap**, so
that number is the `k=1` point of a curve whose slope nobody has measured. The original WS7 framing
(`[ASSUMED]` 10–500 ns/row) is obsolete and is explicitly overruled at `phase2-verify-stage2.md:239`.

Second consumer: §4 carries an **UNRESOLVED tension flag** on lever **L3** (reconcile singleton
fast-path). `P2:stage2` ranks L3 the #2 highest-value ceiling lever (~1.20× disjoint-narrow);
`P2:row-engine` rules it WEAKENED (~1.03–1.08×, because field data with TTL/overlap never reaches the
fast-path). The disagreement is entirely about cluster shape. §4:174 states the resolution rule:
*"L3's slot is gated on that overlap data; do not commit it as a headline lever until the field
cluster shape is measured."* **M7 (#2822) is blocked on this issue.**

## What Changes

1. **A new k-parameterized Criterion bench** (`cqlite-core/benches/reconcile_overlap.rs`) that reports
   per-row merge cost as a function of **k** (the number of generations a row cluster spans) crossed
   with realistic collision mixes (disjoint, LWW overwrite, row/cell tombstone, expiring TTL, and a
   mixed field-shaped blend), at a **pinned `now`**.
2. **A k-parameterized multi-generation fixture builder** in the shared bench fixtures, generalizing
   the fixed-`L0_SSTABLES` same-key pattern that `benches/compaction.rs::build_tombstone_heavy`
   already uses.
3. **A research record** (`docs/research/issue-2043-reconcile-overlap-multiplier.md`) carrying the
   measured k→ns/row table with machine specs + commit SHA, the derived **overlap multiplier
   `cost(k)/cost(1)`**, and a verdict paragraph that either tightens the §3 1.1–1.5× band or states
   precisely why it cannot.
4. **Program-doc updates** to §3 and the §4 L3 tension flag citing the measurement.

## Non-goals

- **No production scan-path change.** Pure measurement + docs. No new `pub` item on the reconcile path.
- **No L3 implementation.** The singleton fast-path is M7 (#2822); this issue only supplies its gating data.
- **No `nb` decode-throughput, Arrow IPC/Parquet cache-format, S4A-vs-S4B, or CDC-re-pose work.** Those
  are the pre-repoint WS7 deliverables, dropped by owner decision (2026-07-24); they remain epic
  #2037's, owner-gated.
- **No strict perf-regression gate.** The bench is an instrument, registered **advisory**.

## Doctrine impact

None to CLAUDE.md or the agents-developing site. The record is a new `docs/research/` file; the two
program-doc edits are in-place citations. Per the website page rule, no new website page is created.

## Honesty boundary (read before approving)

The vendored fixture corpus is **single-generation**, so the *field* k-distribution is **not
measurable on this machine**. It lives on the i4i rig (M0 / #2818, unscheduled). This change therefore
delivers the **curve** (k→multiplier, measured) plus an **STCS-derived expected-k band** (reasoned),
and marks the field point as an explicit assumption rather than a measurement. Consequently L3's
disposition is resolved **conditionally** — bounded, with the arithmetic written out — and the final
call still needs M0's field cluster shape. Blocking this issue on M0 instead is a live alternative
(see design D5).
