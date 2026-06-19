# Compaction & Write-Path Byte-Parity Rules

CQLite's write path and compaction (k-way merge) must produce **byte-for-byte
identical SSTables to Apache Cassandra** for the same inputs. This file is the
codified rule set: each rule is something Cassandra's reference path does that the
output must match exactly.

**Source of truth.** These rules are derived from Apache Cassandra's "cursor
compaction" work (`rustyrazorblade/cassandra` branch `cursor-compaction-completion`
+ `cursor-compaction-test-harness`), which re-implemented compaction to be
byte-identical and whose commit history enumerates every divergence that had to be
fixed. The guiding principle, from that branch:

> **No allowlist — nothing is allowed to diverge byte-for-byte.** (commit `82026ab7`)

**How this is used.**
- The byte-for-byte parity CI (umbrella issue #842) asserts these rules by diffing
  CQLite output against Cassandra output.
- The **differential parity harness** (`compaction-parity/`, designed in
  [`docs/plans/2026-06-18-compaction-parity-harness-design.md`](../plans/2026-06-18-compaction-parity-harness-design.md))
  is the runtime mechanism: it runs Cassandra's compaction in-JVM and `cqlite compact`
  over the same input SSTables, then compares the outputs. Each scenario below maps to
  one or more of these rules.
- The `compaction-parity-auditor` agent (`.claude/agents/`) audits the code against
  this list and the branch history.
- `Status` / `Tracking` columns reflect the current CQLite gap analysis. Update them
  as issues are resolved.

Status legend: **Covered** (handling + test), **Partial** (handling incomplete / no
test), **Gap** (not handled), **N/A** (out of scope, e.g. intentionally rejected).

---

## 1. Cell reconciliation & tie-breaks

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Cells reconcile by `(column, cell_path)` on timestamp; the higher timestamp wins. | (baseline) | Partial — per-column only, no cell_path | #844 |
| At equal timestamp, a **tombstone beats a live cell**. | (baseline) | Covered | merge.rs reconcile |
| At equal timestamp, a **tombstone beats an expiring (TTL) cell** before any localDeletionTime compare. | `a62c749` | Gap — `ttl` never consulted | #848 |
| Counter tombstones tied on `(timestamp, localDeletionTime)` are resolved by **raw value bytes** (greater wins). | `d1f0678d`, `6336451` | N/A — counters rejected | (limitation) |

## 2. Tombstones & deletions

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Range tombstones (including **open-ended** and nested) shadow covered cells during compaction. | `996ae4dc` | Gap — not applied in merge | #846 |
| Row / partition / cell deletions shadow covered data during the merge. | (baseline) | Partial — row shadowing only | #846 |
| **gcBefore / gc_grace purging**: tombstones (and data they cover) older than `gcBefore` are purged. | `8d47ebb2` | Gap — purge utility unwired | #845 |
| **Dropped-column** cells (timestamp ≤ column drop time) are filtered out. | `cb34ad47` | Gap | #847 |
| A merged **complex (collection) deletion** is dropped unless it **strictly supersedes** the active deletion (equal timestamps do not). | `bd244649` | Gap — no complex-deletion entity | #844 |
| Complex-column cells are **shadowed before** the merged complex deletion is purged (no resurrection). | `f66fa14f` | Gap | #844 |

## 3. Complex columns (collections / UDTs)

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Multi-cell complex columns merge **per cell-path** — disjoint map keys / set elements from different SSTables both survive. | `0c85d26f`, `19bcffd4` | Gap — whole-collection LWW | #844 |
| UDT cell-path comparison uses **signed `ShortType`** (matters past field index 32768). | `d14c96b8` | Gap — no cell-path model | #844 |
| Complex columns are matched **by name** across source headers, not by `ColumnMetadata` identity. | `5e636f9` | Partial | #844 |
| Column-subset encoding is byte-identical to `Columns.Serializer`, including the **≥64-column large-subset** form and sparse rows. | `1522988`, `de47b505`, `1be942de` | **Covered** | data_writer.rs (tests at 64/65 cols) |

## 4. Counters

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Counter columns are supported in compaction; counter (tombstone) values preserved; CounterContext window/merge ops applied. | `2855d2d4`, `6336451`, `81bc893` | N/A — counters rejected at the write boundary (and tested) | limitation |

## 5. Clustering order

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Clustering **reversal (DESC)** is respected when comparing absent/empty components; NULLs sort first regardless of ASC/DESC, and empty-vs-valued is routed through the type. | `587612cd` | Partial — blanket reverse flips null order | #849 |

## 6. Static rows

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| Static-row **presence is read from the input SSTable headers** (union), not only the current schema. | `cb34ad47` | Gap | #850 |

## 7. Statistics.db

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| **Empty (static) rows are not counted** in `totalRows` / `totalColumnsSet`. | `1502b0a9` | Gap — unconditional increment | #851 |
| `NO_DELETION` / `i64::MAX` deletion markers are treated as **LIVE** during stats collection (no live-marker poisoning of minTimestamp / tombstone counts). | `d5bc7fb5` | Partial — verify collector | #851 |

## 8. Writer / on-disk format

| Rule | Cassandra commit | Status | Tracking |
|------|------------------|--------|----------|
| A complex-deletion marker's **encoded size equals the bytes written** for far-future `localDeletionTime` in `[2^31, 2^32)` (same i32 cast on both paths). | `c81fbae1` | Partial — verify size path | #853 |
| **Disabled bloom filter** (`bloom_filter_fp_chance = 1.0`) is tolerated (always-present/empty filter). | `6ab1d9c0` | Gap — fp_chance validated to (0,1) | #852 |
| **Primary-key columns are never written as cells** — clustering (and partition-key) values are encoded positionally (clustering prefix), not duplicated in the row body. | (baseline) | **Covered** | #857 — `merge_row_group` drops key columns from ops; tests in data_writer.rs + compaction-parity harness |
| Index block **start offset is 64-bit** (partitions past 2 GiB). | `e1c6aed4` | **Covered** | index_writer.rs (u64) |
| **Fail loudly** on unknown writer formats. | `673b694f` | Partial — verify | — |
| Output format: CQLite writes **BIG** (BTI output is a separate format; `e6822e93` adds BTI in Cassandra). | `e6822e93` | N/A — BIG only | — |

---

## Differential test scenarios to mirror (from the harness)

The Cassandra harness drives these scenarios through both compaction paths and diffs
byte-for-byte; the parity CI (#842) should grow toward the same corpus:

- Giant partition / 2 GiB index boundary (`40c06bcb`)
- Two-million-row volume; streamed comparison (`523dab8d`)
- Pathological wide-schema (`641c5fbd`, `428b35dc`)
- Range-tombstone-dense + open-ended range tombstones (`14c3fa09`, `996ae4dc`)
- Tombstone-interleaving (`d5bc7fb5`)
- Materialized-view tables (`d62c7339`)
- Two-generation compaction (`96d95f78`)
- Partial-set & multi-output compaction (`8937549`)
- Purge-boundary + Harry history + config-axis coverage (`52ce7206`)
- Randomized differential soak (`1b5d7f67`, `dafd000e`)

---

## Cross-cutting note

Several gaps share one root cause: the reader→merge entry currently carries only a
**single row-level timestamp** and **no cell-path**, so per-cell and per-element
reconciliation cannot be byte-faithful regardless of tie-break logic. Closing the
complex-column (#844) and tombstone (#846, #848) rules will likely require a richer
merge-entry representation: per-cell `timestamp`, `ttl`/`localDeletionTime`,
`cell_path`, and first-class complex/range deletions.
