# SELECT Access-Path Signal (Issue #960, Epic #951)

Correct result rows do **not** prove a storage capability is wired into the CQL
query path. #949 returned the right rows while still scanning every SSTable and
filtering in memory. This document describes the explicit, testable
*access-path* signal that #960 adds so tests, reviewers, and (eventually)
`EXPLAIN` can distinguish a full scan from a targeted lookup — and so an
*accidental* fallback to a full scan cannot masquerade as a targeted success.

The signal is defined in `cqlite-core/src/query/access_path.rs`.

## Access-path classes (`AccessPath`)

| Variant | Meaning | Produced today by |
|---|---|---|
| `FullScan` | Every SSTable is scanned, rows filtered in memory. The honest baseline. | (reserved; current fallbacks report `FallbackFullScan`) |
| `PartitionLookup` | A single fully-constrained `WHERE pk = ?` served by a partition-targeted lookup that prunes SSTables (bloom/BTI presence). | Materializing `SelectExecutor` (#949 fast path) |
| `MultiPartitionLookup` | Several fully-constrained partitions (`IN` / token fan-out) via repeated targeted lookups. | Reserved for #955 (not yet produced) |
| `ClusteringSlice` | Partition targeted + clustering-key predicate prunes rows within the partition. | Reserved for #954 (not yet produced) |
| `MetadataPartitionLookup` | WRITETIME/TTL metadata projection resolved a single fully-constrained partition via a targeted lookup that prunes SSTables (bloom/BTI) before decoding per-cell metadata. | Materializing `SelectExecutor` metadata branch (#962) |
| `StreamingPartitionLookup` | The streaming analogue of `PartitionLookup`. | Streaming `SelectExecutor` background task |
| `FallbackFullScan { reason }` | A targeted path was not selected; carries a documented `FallbackReason`. | All honest fallbacks |

`AccessPath::is_full_scan()` is true for `FullScan` and any `FallbackFullScan`.
`AccessPath::is_targeted()` is true for the four lookup variants.

## Allowed fallback reasons (`FallbackReason`)

This is a **closed set**. Adding a variant is a public-contract change and must
be reflected here. A reason must never be used to paper over an *unexpected*
fallback — an unexpected fallback should surface as a failing access-path
assertion in a test, not a silently "successful" full scan.

| Reason | When | Owner |
|---|---|---|
| `NoSchema` | No schema available, so partition-key columns cannot be identified. | — |
| `PartitionKeyNotFullyConstrained` | WHERE does not fully constrain the partition key with equality (partial key, no restriction, or a range/`IN`). Mirrors Cassandra's single-partition-read rule. | #955 widens (`IN`/token), #954 (clustering) |
| `PartitionKeyEncodingFailed` | The constrained values could not be encoded to the on-disk key form (e.g. type mismatch). Full scan is the safe fallback. | — |
| `MetadataScanPath` | The WRITETIME/TTL metadata projection falls back to a full scan when the partition key is NOT fully constrained by equality — e.g. `WRITETIME(col)` with an `IN`-list partition key (the IN-metadata fan-out is a documented follow-up) or no/partial restriction. A fully-constrained `WHERE pk = ?` metadata projection is now targeted (`MetadataPartitionLookup`, #962). | #962 (IN-metadata fan-out follow-up) |
| `LegacyExecutorPath` | The legacy `QueryExecutor` (simple-id-lookup and prepared SELECTs) issues an unconditional `storage.scan`. | #962 (route through modern executor) + #961 (param binding) |
| `TombstonesBuildNoPrune` | The `tombstones` build compiles out the partition-targeted prune: `scan_partition` / `scan_partition_with_cell_metadata` become full-scan + retain fallbacks with NO bloom/BTI pruning. A fully-constrained `WHERE pk = ?` (or `IN (...)` / WRITETIME-TTL) therefore opens the whole table even though the rows are byte-identical to the pruned build. The executor reports this honest reason whenever the storage call returns `engaged == false`, so a targeted label is never claimed for a full-table scan. | Epic #951 (re-enable prune on the `tombstones` build) |

## How the signal is exposed

Two observable surfaces, both from the modern `SelectExecutor`:

1. **Result-attached**: `QueryResult.metadata.access_path: Option<AccessPath>`.
   `Some(_)` for the materializing path; `None` for the streaming path (the
   scan runs in a spawned task, so the path is not known at iterator-construction
   time) and for constant queries (`SELECT 1`, which touch no SSTable).
2. **Test-accessible probe**: `cqlite_core::query::access_path::{last, reset}`.
   A process-global `Mutex<Option<AccessPath>>` mirroring the
   `scan_for_key_call_count` pattern (issue #831). This is the **only** signal
   observable from the streaming path (different thread) without parsing logs.
   `SelectExecutor::execute` / `execute_streaming` call `reset()` on entry so a
   stale value cannot satisfy a later assertion.

## What each SELECT surface reports today

| Surface | Entry | Today's honest report |
|---|---|---|
| Materializing `WHERE pk = ?` (full PK, `=`) | `SelectExecutor::execute` | `PartitionLookup` |
| Materializing, no/partial/range restriction | `SelectExecutor::execute` | `FallbackFullScan { PartitionKeyNotFullyConstrained }` |
| Materializing, PK value won't encode | `SelectExecutor::execute` | `FallbackFullScan { PartitionKeyEncodingFailed }` |
| Materializing, no schema | `SelectExecutor::execute` | `FallbackFullScan { NoSchema }` |
| Materializing, `WRITETIME(col)`/`TTL(col)` + `WHERE pk = ?` (full PK, `=`) | `SelectExecutor::execute` (metadata branch) | `MetadataPartitionLookup` (#962 — prunes SSTables before decoding metadata) |
| Materializing, `WRITETIME(col)`/`TTL(col)` with `IN` / no / partial restriction | `SelectExecutor::execute` (metadata branch) | `FallbackFullScan { MetadataScanPath }` (IN-metadata fan-out is a follow-up) |
| Streaming `WHERE pk = ?` (full PK, `=`) | `SelectExecutor::execute_streaming` | `StreamingPartitionLookup` (via probe) |
| Streaming, no/partial restriction | `SelectExecutor::execute_streaming` | `FallbackFullScan { ... }` (via probe) |
| Legacy `QueryExecutor` (simple-id-lookup, prepared) | `engine.rs` legacy route / `prepared.rs` | **does not report yet** — see below |
| Any targeted surface (`WHERE pk = ?` / `IN` / WRITETIME-TTL) under the `tombstones` build | `SelectExecutor` (materializing + streaming) | `FallbackFullScan { TombstonesBuildNoPrune }` — the `tombstones` build compiles out the prune, so the storage call returns `engaged == false` and the executor reports the honest full-scan reason instead of `PartitionLookup` / `MultiPartitionLookup` / `MetadataPartitionLookup` / `StreamingPartitionLookup`. Rows are byte-identical to the pruned build. |

## Honest-reporting mandate (#960 vs #962)

#960 only *reports* the path; it does not make every path targeted (that is
#962). The reported path must be **honest**: a path that still full-scans today
reports `FullScan` / `FallbackFullScan`, and a test pins that current reality so
#962 can later flip it without the change going unnoticed.

`cqlite-core/tests/issue_960_access_path_signal.rs` pins:
- `WHERE pk = <uuid>` ⇒ `PartitionLookup` (NOT a full scan),
- unrestricted SELECT ⇒ `FallbackFullScan { PartitionKeyNotFullyConstrained }`,
- `WRITETIME(col)` + `WHERE pk = ?` ⇒ `MetadataPartitionLookup` (#962 flipped this
  from the old `FallbackFullScan { MetadataScanPath }`),
- streaming `WHERE pk = ?` ⇒ `StreamingPartitionLookup`.

## Paths NOT yet threaded (for #962 to pick up)

- **Legacy `QueryExecutor`** (`cqlite-core/src/query/executor.rs`) — handles
  simple-id-lookup SELECTs and all prepared SELECTs. It always issues
  `storage.scan(...)`. It does **not** record an access path yet; the intended
  honest report is `FallbackFullScan { LegacyExecutorPath }`. This was left
  un-threaded because #962 will restructure (route these surfaces through the
  modern `SelectExecutor`, or port the fast path into the legacy executor), and
  recording in a soon-to-be-replaced path adds churn. The `LegacyExecutorPath`
  reason and the enum are already defined so #962 can wire it in one place.
- **IN-metadata fan-out** — `WRITETIME(col)`/`TTL(col)` with a `WHERE pk IN (...)`
  partition key still full-scans (`MetadataScanPath`); fanning it out to N
  targeted metadata lookups (the metadata analogue of `MultiPartitionLookup`) is a
  documented follow-up. The single-key case is targeted via #962's
  `Storage::scan_partition_with_cell_metadata`.

## `EXPLAIN`

The existing `QueryEngine::explain` (`engine.rs`) is *planner*-based (it returns
the planned plan/cost), not execution-based, and does not run the query. Threading
the executed `AccessPath` into a real `EXPLAIN ANALYZE`-style output is a
follow-up, not built here. `AccessPath::label()` / `FallbackReason::label()`
provide stable lowercase strings ready for that output.
