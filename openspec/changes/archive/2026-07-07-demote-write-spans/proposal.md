## Why

The July 2026 platform observability audit
(`docs/reports/platform-observability-audit-2026-07-01.md`, finding **AI3**, epic
#1686 "observability honesty") found the **write side is noisy at the CLI's
default INFO level**, while the read side is already correctly quiet:

- **22 `#[tracing::instrument]` spans** in `cqlite-core/src/storage` — **none**
  specify a level, so all default to **INFO**. This includes hot per-operation
  spans: `write.mutation` (per mutation), `memtable.insert` (per insert),
  `wal.append`/`wal.sync` (per append), `merger.step` + `writer.write_partition`
  (per partition), and `compression.write_chunk` (per chunk). At INFO, a single
  batch of N mutations emits ≥3N spans.
- **~8 `info!` lines in the SELECT/query path** — per-query chatter at the
  default level.

Meanwhile the 2%-overhead CI gate **deliberately runs subscriber-less**, so this
default-on posture is **completely unmeasured** — the gate proves an overhead
number that **no real CLI user configuration experiences**.

**Routing: design-driven.** This changes the crate's **default observability
posture** (what an embedder/CLI user sees at INFO) and adds a gate variant — a
doctrine/behavior decision, not an oracle parse-correctness fix — so it is
captured as an OpenSpec change per the spec-driven doctrine. Epic: #1686 (AI3).

Milestone: **0.17** (as filed). Priority: **P2**. Owner directed this be taken
now (2026-07-07) alongside #1706.

## What Changes

- **Demote the write-side / compaction `#[tracing::instrument]` spans to DEBUG**
  (`level = "debug"`), matching the read side's existing `debug_span!` discipline.
  Span **names and attribute keys are unchanged** (dashboards/docs reference them)
  — only the **level** changes.
- **Demote the per-query SELECT `info!` chatter to `debug!`** so the default
  level emits **≤1 info line per SELECT** (vs ~5–7 today). Content is owned by
  AG5 (#1694, landed); this owns level/volume only.
- **Add a subscriber-on overhead bench variant**: run the existing overhead
  measurement with a real fmt/`tracing` subscriber installed at INFO (the CLI
  default posture) and record its number alongside the subscriber-less one.
  **Advisory-first** (record + warn; promote to failing later).

### Sequencing (hard dependency on #1706)

The SELECT `info!` demotion touches the **same lines** #1706 (log→tracing event
migration) is rewriting. **Implementation of this change MUST land after #1706
merges** so the `info!` sites are already `tracing::info!` when demoted (the span
part is independent of #1706 and carries no conflict). The claim branch is held
until #1706 is on main.

### Explicitly out of scope (do NOT)

- **Do not delete spans** — demote them; they are correct at DEBUG.
- **Do not change span NAMES or attribute keys** — level only.
- **Do not change message CONTENT** (AG5 owns it) — level/volume only.
- Read side already uses `debug_span!` — untouched.
