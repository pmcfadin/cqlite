# cqlite-flight warm-handle service: generation-keyed parse cache across requests (epic #2310)

## Milestone
Unmilestoned by design — owner triages (candidate for 0.15+ after the 0.14 flight-correctness
program closes). This is **Phase 2** of the ms-point-read program (research:
`docs/architecture/issue-2310-ms-point-reads-research.md`). Phase 1 (#2207 PK point-read/prune,
#2295 complete snapshots, #2302 present-pair resolution) is **all shipped**, so the probe path this
epic caches already exists. Routing: **design-driven** — Seam-1 owner approval of this spec + design
precedes any implementation; children (WS1–WS5) are filed after approval and each carries its own
OpenSpec change at activation.

## Why (measured problem)
cqlite-flight is a long-running server that behaves **statelessly per request**. Every `do_get`
re-does the same fixed rediscovery from cold and throws it away:

- **Schema parse from the ticket every request** — `CqliteFlightService::parse_schema` →
  `parse_cql_schema` at `cqlite-flight/src/service.rs:138`, called from `build_producer` on every
  RPC.
- **Directory resolve + listing every request** — `DirSource::resolve`
  (`cqlite-flight/src/producer.rs:146`) `is_dir`/`read_dir`s the snapshot (or live) table dir, then
  `data_paths` re-lists it and `prune_paths_cancellable` reads each SSTable's `Summary.db` for the
  token-span prune.
- **Per-SSTable reader open + Index/Summary/Statistics/bloom parse every request** across every
  surviving SSTable.

The cost model (research §"Cost model", lines 82–92) names this the **Phase-2 residual** the epic
removes: once Phase 1 turns `WHERE pk = X` into an index probe, the dominant remaining fixed cost
for a point/LIMIT query is exactly this per-request schema parse + `DirSource` resolve +
reader-open/Index/Summary/Statistics/bloom parse. On unchanged data it is repeated verbatim on every
request. There is also no warm state, so the server has **no cheap answer to "did anything change?"**
— every request assumes everything changed.

## What changes
Give flight a **warm, generation-keyed handle** per table:

1. **Cache key = SSTable generation identity, not directory path.** Snapshot mode creates a new
   hardlinked dir per query, but the files are the same inodes; parsed Index/Summary/Statistics/
   bloom/schema state cached per generation is valid across snapshot dirs. Per-query snapshots stay
   (isolation + flush-on-snapshot freshness per the #2305 decision); **parse-once-per-generation
   replaces parse-per-query.**
2. **A per-request staleness probe** decides warm-hit vs rebuild: a generation-set diff (dir
   listing, or the snapshot `manifest.json` fast path) with **zero staleness window** — a listing is
   authoritative, so an unchanged set is a warm hit that skips all reader-open/parse; a changed set
   triggers a rebuild of only the delta.
3. **Reuse core's freshness contract.** `Database::refresh()` (#1749) is atomic, fail-closed, and
   isolates in-flight queries on `Arc` reader clones; the flight warm set adopts the same semantics
   rather than inventing new machinery.
4. **Memory budget + eviction + metrics.** Warm state lives inside the <128MB discipline: LRU
   eviction by (table, generation), explicit budget accounting, and hit/miss/evict/refresh-outcome
   counters (the #2289 measurement plan consumes these).

The observable answer set is **unchanged**: a warm hit returns exactly what a cold request returns
for the same ticket over the same generation set.

## Non-goals
- **No change to snapshot semantics.** The #2305 owner decision stands: flush-on-snapshot,
  point-in-time snapshot mode is unchanged. This epic is about parse-cost, not read semantics.
- **NOT the #905 Phase-C compaction daemon.** This is flight-internal warm state, not the
  standalone engine daemon; scopes stay distinct (also distinct from #1934 engine-name work).
- **NOT #2306 (snapshot reuse / TTL).** #2306 reuses SNAPSHOTS and trades freshness; this epic
  reuses PARSED STATE and trades nothing. Doing this first may reduce #2306's urgency; they are
  adjacent but separable.
- **No filesystem-watching heuristics.** No inotify/FSEvents/mtime-only inference (platform-
  dependent, heuristic). The refresh trigger is an explicit, documented, authoritative contract.
- **No new user-facing config knob, CLI/binding method, or ticket field** beyond the warm-state
  internals and the existing observability surface.
- **No change to the KWayMerger reconciliation, write, or compaction logic.**

## Doctrine impact
**No-heuristics (#28):** the refresh trigger MUST be an explicit, documented contract, never an
inference from byte patterns or filesystem timing. The chosen trigger (per-request generation-set
probe + snapshot `manifest.json` fast path) is authoritative by construction — a directory listing /
manifest IS ground truth for the generation set, not a guess. Fail-closed mirrors #1749: a probe
error is treated as "changed" (full re-resolve), and a refresh error leaves the previously warm set
fully intact (no partial view).

## Cross-links
Program: epic #2310 (this proposal is Phase 2), research
`docs/architecture/issue-2310-ms-point-reads-research.md`. Prereqs (shipped): #2207 (PK point-read),
#2295 (snapshot completeness), #2302 (Summary/Index resolution). Reused contract: #1749
(`Database::refresh()`). Constraints: #2305 (snapshot flush semantics, unchanged), #2306 (snapshot
reuse, separable), #2264 (cancellation discipline, must hold). Measurement: #2289 harness, #1494
bench suite. Distinct scope: #905 Phase-C daemon, #1934 engine-name.
