# CQLite Tail-Merge Seam Map — Findings

*Research pass for the memtable-plugin design doc (spike #1807). All anchors verified in source at the CQLite repo (main @ ~9c7c4e80, 2026-07-03).*

Note: no DataFusion code exists in the repo — "DataFusion table provider" is design-doc-only (`docs/architecture/issue-941-datafusion-table-provider-council.md`); the shipped read surfaces are **library handle (`Database`)**, **CLI**, and **Arrow Flight → Trino**.

---

## 1. Current source enumeration

Two **independent** discovery mechanisms exist; the Flight server does NOT use `Database`/`SSTableManager` at all.

### 1a. Flight path — stateless per-request directory listing
- `SstableSource` trait: `cqlite-flight/src/producer.rs:80-83` — returns `Vec<PathBuf>` of `Data.db` files, "newest generation first". Explicitly designed as a DI seam ("so Phase 3 can swap in a snapshot-directory source without touching the merge logic", :77-79).
- `DirSource`: `producer.rs:86-89`; `DirSource::resolve(data_dir, keyspace, table, snapshot)`: `producer.rs:119-138` — resolves `<data>/<ks>/<table>` or Cassandra-layout `<data>/<ks>/<table>-<uuid>` (lexicographically-largest uuid dir wins, :141-163); optional `snapshots/<name>` subdir (Sidecar snapshot hardlink set). Symlink-escape containment via `crate::pathsafe::assert_within` (:135, issue #1430).
- `DirSource::data_paths()`: `producer.rs:167-205` — `std::fs::read_dir`, filter `*-Data.db`, per-file symlink containment (:186-198, fail-closed exclude), then **sort newest-generation-first by filename** via `generation_of` (:203, :210-215 — parses first numeric segment of `nb-12-big-Data.db`; returns 0 if unparseable).
- Wiring: `CqliteFlightService` holds a single `data_dir: PathBuf` (`cqlite-flight/src/service.rs:109`, set at `new` :116 from CLI `--data-dir`, `cqlite-flight/src/main.rs:21-22`). Every `do_get` calls `DirSource::resolve(&self.data_dir, …)` **fresh per request** (`service.rs:375`) and runs `producer.produce(&source)` in `spawn_blocking` (`service.rs:386`). `table_stats` action reuses the same resolve (`service.rs:443`).
- **Flight freshness = fresh directory listing per request. There is no cached reader state, no refresh needed, no snapshot-at-open on this surface.**

### 1b. Library path — snapshot-at-open + explicit refresh (#1749, just landed)
- `Database::refresh()`: `cqlite-core/src/lib.rs:321-322` → `StorageEngine::refresh` (`cqlite-core/src/storage/mod.rs:256-257`) → `SSTableManager::refresh_tables` (`cqlite-core/src/storage/sstable/refresh.rs:216-358`).
- Contract (module docs `refresh.rs:1-37` + spec `openspec/specs/storage-freshness/spec.md`): library handle is a **snapshot at open**; `refresh_tables` is the ONLY way the reader set changes. It re-runs the *same* discovery recorded at construction (`DiscoverySource` enum, `refresh.rs:67-74`: `BasePath` recursive scan vs **`TableDirs(Vec<PathBuf>)` — a fixed set of multiple table directories**), diffs canonicalized `Data.db` paths, opens added generations *before* taking the write guard (atomic, fail-closed — a corrupt new gen aborts the whole refresh, nothing mutates), drops removed generations, keeps unchanged `Arc<SSTableReader>` warm (pointer-identity preserved). Concurrent refreshes fully serialize on a dedicated mutex (:222); queries never take it.
- `SSTableManager::new_from_discovered_paths` (`cqlite-core/src/storage/sstable/mod.rs:344`) already accepts **multiple table directories** → the library surface can already treat a tail dir as an extra discovery root today.
- Per-surface freshness contract (spec `openspec/specs/storage-freshness/spec.md:96-110`): library = snapshot-at-open + explicit refresh; CLI one-shot = fresh per process; Flight = fresh per request; torn-window posture recorded as a decision consumed by the Flight rewrite. (Archived change: `openspec/changes/archive/2026-07-03-sstable-freshness-refresh/`.)

---

## 2. Merge path — the tail dir needs ZERO new merge code

- `MergeProducer` (`producer.rs:262-273`); `produce(&dyn SstableSource)` :364-367 → `produce_from_paths(Vec<PathBuf>)` :376-382 (**public** — accepts an arbitrary path list) → token-prune :389-412 → `merge_paths` :420-468.
- `merge_paths` constructs `KWayMerger::new(paths, &self.schema)` (`producer.rs:430`), steps per-partition (`MergeStep::Partition`), applies the token backstop (:437), rebuilds `QueryRow`s via `build_row_from_scan` (:544), suppresses row tombstones (`entry_to_row`, :524-529 — `RowData::Tombstone → None`) and cell tombstones (:539). Aggregation pushdown feeds the **same post-reconcile** rows (:478-517), so COUNT/SUM never double-count duplicates.
- `KWayMerger::new(input_paths: Vec<PathBuf>, schema)`: `cqlite-core/src/storage/write_engine/merge/mod.rs:1933` (struct :1015; full ctor `new_with_gc_and_registry` :1969). It opens an `SSTableRowIteratorAdapter` per path (:~1994-1996) with **no assumption that paths share a directory** — inputs are just files. It already merges N generations of the same table; generations from N directories are indistinguishable to it.
- LWW/tombstone reconciliation: `merge/reconcile.rs` (7-step Cassandra-parity pipeline, module docs :12-33 — row-deletion fold, per-`(column, cell_path)` LWW, complex-deletion supersede, row-tombstone shadowing, dropped-column filter, TTL-expiry-as-tombstone #1382, gc-grace purge, phantom-row guard). Cell tie-break is the shared `reconcile_rules::cell_wins` (`cqlite-core/src/storage/write_engine/reconcile_rules.rs`, Cassandra `Cells#reconcile` parity commit `a62c749`): higher ts wins; **equal ts → tombstone beats live/expiring**; equal ts + equal liveness → keep first-seen (run order = the newest-generation-first sort).
- Cross-SSTable LWW is already tested end-to-end through the Flight producer: `merge_resolves_last_write_wins_across_sstables` (`producer.rs:974-997`), `row_tombstones_are_suppressed` (:1000-1018).
- **Critical property for the Flight path**: `KWayMerger::new` (the ctor the producer uses) passes `gc_before_secs = None` and `purge_safe` defaults `false` (`merge/mod.rs:1053-1063` region) — **no tombstone purging ever happens in a Flight read merge**. Purging exists only on the compaction write path.

### Where to inject the second root (options, cheapest first)
1. **Composite source (recommended, zero merge changes)**: a `TailAwareSource`/`CompositeSource` implementing `SstableSource` (`producer.rs:80`) that concatenates `DirSource::resolve(data_dir,…)` paths with `<tail_root>/<ks>/<table>/` paths (tail paths first = newest). `MergeProducer.produce` is already `&dyn SstableSource`. Alternatively call the already-public `produce_from_paths` with the concatenated list.
2. **Server config**: add `--tail-dir: Option<PathBuf>` to `Args` (`main.rs:19-31`) and a field on `CqliteFlightService` (`service.rs:109,116`); `do_get_inner` (`service.rs:373-386`) builds the composite source. Tail-dir path must go through `pathsafe::assert_within` with the tail root as its own containment base (`cqlite-flight/src/pathsafe.rs:90`).
3. **Flight ticket**: `FlightTicket` (`cqlite-flight/src/ticket.rs:225-265`) — could carry `include_tail: bool` (or a tail-snapshot name mirroring `snapshot: Option<String>` :237). Ticket-side identifier validation exists (`pathsafe::validate_identifier` :39, `validate_snapshot` :60). Server-side root should stay CLI config; the ticket at most opts in/out.
4. **Library surface**: `Database::open_with_discovered_sstables` (`lib.rs:220`) / `SSTableManager::new_from_discovered_paths` (`sstable/mod.rs:344`) with `DiscoverySource::TableDirs` including the tail table dir — `refresh()` then handles tail churn natively (removed exports drop, new exports add). This is the seam if the future DataFusion provider builds on `Database`.

If the tail export is a real `nb` SSTable, the merge, reconcile, tombstone, TTL, token-prune, predicate, and aggregation code paths all work unmodified. If it were Arrow IPC instead, an entirely new source+reconcile shim would be required (the merge consumes `SSTableRowIteratorAdapter`s keyed by `DecoratedKey` — Arrow IPC would need its own `SSTableRowIterator` impl, `merge/mod.rs` trait near :934, plus timestamp/tombstone-carrying columns). **Strong argument for the real-SSTable export format.**

---

## 3. Dedup / watermark

### Statistics.db decode status
- min/max timestamps: `StatisticsReader::timestamp_range` (`cqlite-core/src/storage/sstable/statistics_reader.rs:199`) and the fail-closed authoritative accessor `max_timestamp() -> Option<i64>` (:211-215, `i64::MIN` = unavailable sentinel). **#1728 and #1729 are both CLOSED** — authoritative `maxTimestamp` decode from nb STATS field 4 shipped (end-to-end test `statistics_reader.rs:740-794`), and the writer-side NO_DELETION_TIME fix landed. Consumers already honor the sentinel: `authoritative_max_timestamp` in `merge/fully_expired.rs:52-56` (drop-gate refuses unknown-max candidates, :222).
- Baselines/purge bounds read Statistics.db per input path via `stats_path_for` (`merge/mod.rs:~1095`) + `parse_statistics_with_fallback` — the same mechanism a watermark check would use.
- **commitLogIntervals: parsed-past but NOT surfaced.** The only decoder is `cqlite-core/src/parser/repair_metadata.rs` — `skip_commit_log_intervals` (:621-633) and the skip at :740-747 (`commitLogLowerBound` = i64 segmentId + i32 position, then IntervalSet of CommitLogPosition pairs). Version gates confirm presence in `nb`/`da`: `version_gate/big.rs:22-26`, `version_gate/bti.rs:18-19,64-65`. **To implement "ignore tail rows covered by flushed generations' commit-log intervals ⊇ export watermark", the intervals must be promoted from skip → decode+expose** (small, format-known change in `repair_metadata.rs`, surfaced through `StatisticsReader`).
- Where the rule lives: inside the composite source's path-listing (a pre-merge file-level prune, exactly analogous to the existing token prune `prune_paths` `producer.rs:389-412`, which also reads sibling component files per path and fails open). Watermark source of truth: the tail export's own Statistics.db commitLogIntervals (if the plugin writes them) or a sidecar manifest in the tail dir.

### Is stale-tail-while-flush-present dangerous? (the design-simplifying question)
**For the Flight/Trino read path: NO — with one bounded exception.**

- **Upserts**: the flushed generation contains a superset of the tail export's cells at ≥ timestamps (flush persists the same memtable the export snapshotted, plus later writes). LWW (`cell_wins`) picks the identical-or-newer cell; equal-ts+equal-liveness keeps first-seen with identical bytes. Result set ≡ flushed-only. Double-counting cannot happen (dedup by `(partition, clustering, column, cell_path)` in reconcile, and aggregates run post-reconcile).
- **Tombstones**: a tombstone in the tail is also in the flush; equal-ts tombstone-beats-live protects the delete. A newer flushed tombstone shadows stale tail live cells (Step 3/4 of reconcile).
- **TTL**: expiry evaluated per-cell from `localDeletionTime` — identical cells expire identically. (#1382's expire-then-purge only affects the compaction path, not Flight reads.)
- **No purge in Flight reads** (`KWayMerger::new` → no gc), so the read merge itself can never resurrect.
- **The exception — orphaned tail file surviving past a Cassandra gc_grace purge**: Cassandra's compaction controller (`maxPurgeableTimestamp`) considers only SSTables *it* knows about. The tail dir is invisible to it. If a tail export containing live row X (ts=100) is orphaned (plugin crash) and, ≥ gc_grace later, Cassandra compacts away both a covering tombstone (ts=200) *and* X, a CQLite merge of {compacted gens + orphaned tail} **resurrects X**. So: for a *properly churned* tail (deleted at each flush, lifetime ≪ gc_grace ≈ 10 days), watermark dedup is **purely an efficiency optimization**; the watermark/commitLogInterval rule is a **correctness backstop specifically against orphaned/stale exports older than gc_grace**, not a hot-path requirement. A far cheaper backstop: ignore tail files whose mtime/watermark is older than some small TTL (minutes), no Statistics decoding needed.
- **Counter caveat**: counter columns don't reconcile by pure LWW in Cassandra (context merge). CQLite maps `Counter` as BigInt with "full" pushdown (`producer.rs:631,668`). Duplicated counter shards across tail+flush could misresolve — flag counters out-of-scope for tail merging in the design.

---

## 4. Refresh / lifecycle under tail-dir churn

- **Flight surface**: listing is per-request (`data_paths` at `do_get` time), so churn between requests is invisible — the next request simply sees the new listing. **Torn window within one request**: `DirSource` lists, then `KWayMerger::new` opens every path immediately (`merge/mod.rs:1994-1996`); a file deleted between list and open → open error → `ProducerError::Merge` → gRPC error (fail-closed, no partial results); a file unlinked *after* open is safe on POSIX (fd held). The plugin's delete-after-flush therefore produces at worst a retryable request error, never wrong data. (Trino retries the split or the query fails cleanly.) Snapshot mode (`ticket.snapshot` → Sidecar hardlink set, `producer.rs:127`) eliminates the window entirely and is the documented production posture — the tail dir could adopt the same hardlink-set trick if churn errors matter.
- **Library surface**: `refresh_tables` handles removal safely — removed generations are dropped from the map; the underlying reader closes when the last in-flight scan's `Arc` drops (`refresh.rs:19-21`); in-flight scans complete against their captured pre-refresh set (test `refresh.rs:500-587`); removal + addition in one refresh is atomic. Staleness window = caller's refresh cadence; no filesystem watching (explicit non-goal, `lib.rs:320`).
- Spec scenarios covering exactly the tail lifecycle (add gen → invisible until refresh; delete gen → `readers_removed`, no panic): `openspec/specs/storage-freshness/spec.md:15-41`.

---

## 5. Token-range pruning + Trino (brief)

Yes, fully wired, and it composes with a tail source automatically:
- **Trino side**: `CqliteFlightSplitManager.buildSplits` (`trino-connector/src/main/java/.../CqliteFlightSplitManager.java:67-96`) asks Cassandra **Sidecar** for `tokenRangeReplicas` (:41) and emits **one split per read-replica token range pinned to one deterministically-chosen replica** (local-DC preferred, :100+) — this IS per-node cluster-mode pruning. Each split carries `(tokenStart exclusive, tokenEnd inclusive, wraparound)` into the ticket (`FlightTicketJson.java:41-54`, `CqliteFlightSplit.java:13-22`). Aggregations use one finalize split that fans out (`CqliteFlightSplitManager.java:45-60`).
- **Server side**: `get_flight_info` returns a single endpoint per ticket (`service.rs:340`); the ticket's token range drives (a) input SSTable file pruning by `Summary.db` min/max token span (`prune_paths` `producer.rs:389-412`, `sstable_token_span` :223-259, fail-open on missing Summary) and (b) a per-partition backstop (:437). A tail export written as a real SSTable with a Summary.db gets pruned identically per split — otherwise it's fail-open (merged by every split, correct but wasteful; another reason to write full component sets).

---

## 6. Test harness + proposed parity-test home

- **In-crate (fastest reuse)**: `cqlite-flight/src/testutil.rs` builds **real SSTables in-process via the write engine** into tempdirs (`build_sstables(&schema, vec![gen1_rows, gen2_rows, …])` → one Data.db per batch; returns `(TempDir, data_dir, table_dir)`), plus `make_snapshot`, `delete_row`, clustered/uuid schema helpers. No external fixtures needed. Multi-generation LWW/tombstone merge is already proven this way (`producer.rs:974,1000`). Run via `cargo test --package cqlite-flight --lib` (CI lane `.github/workflows/flight-ci.yml:70`).
- Fixture-based tests use `CQLITE_DATASETS_ROOT` (`cqlite-flight/src/stats.rs:760`; fixtures at `test-data/datasets/sstables/`).
- **E2E lanes**: `flight-trino-e2e.yml` runs `./trino-connector/docker/e2e-test.sh` (docker-compose: Cassandra + Sidecar + flight server + Trino); `e2e-readback.yml` (Cassandra readback gate); `flight-image.yml`, `trino-connector-ci.yml`.
- **Proposed parity-test home**: a new inline test module in a **new file** `cqlite-flight/src/tail.rs` (or `producer/tail_tests.rs`) — NOT appended to `producer.rs`, which at 2,080 lines is already over the campsite ratchet. The spike's criterion maps directly onto existing helpers:
  1. `build_sstables` with batch A (the "flushed" state) into `main_dir`, batch A′ = subset/equal rows written into `tail_dir` (simulated export, same timestamps), assert `produce(composite{main+tail})` rows ≡ `produce(main only)` rows (pre-flush merged read == post-flush read), including a tombstone and a TTL case;
  2. churn case: delete the tail file, re-request, assert clean result;
  3. the CEP-11 end-to-end variant (real Cassandra memtable export) belongs in `trino-connector/docker/e2e-test.sh` / a new compose stage.

### Pointer docs (skimmed, verified against source)
- `cassandra-index/cqlite-flight-trino.md` — accurate on the big picture; its "Option B: CDC tail merged in MergeProducer" (:126-128) is exactly this design's shape (with plugin export replacing CDC). One stale ref: `merge.rs:645` (merge is now a directory, `merge/mod.rs`).
- `cassandra-index/cqlite-write-engine.md` — accurate: Flight "does NOT see memtable or unflushed WAL" (:101-102); hypothesizes the memtable-export seam this design fills (:111-118). Its `WriteEngine` line anchors are approximate.

---

## Minimal-change inventory (tail dir as one more generation source, real-SSTable export)

| # | Change | File(s) | Size |
|---|--------|---------|------|
| 1 | `CompositeSource`/`TailAwareSource` impl of `SstableSource` (main paths + tail paths, tail-first) | new small file in `cqlite-flight/src/` (trait at `producer.rs:80`) | ~50 LOC |
| 2 | `--tail-dir` CLI arg + service field + wire into `do_get_inner`/`table_stats` | `main.rs:19-31`, `service.rs:109,116,375,443` | ~30 LOC |
| 3 | Pathsafe containment for the tail root | reuse `pathsafe.rs:90` | ~5 LOC |
| 4 | (Optional, ticket opt-in) `include_tail: bool` on `FlightTicket` | `ticket.rs:225` | ~10 LOC |
| 5 | (Backstop) decode + expose `commitLogIntervals` (currently skipped) | `parser/repair_metadata.rs:621-633,740-747`, surface via `statistics_reader.rs` | ~80 LOC |
| 6 | (Backstop) watermark prune in CompositeSource (intervals ⊇ watermark → drop tail path), or cheap max-age prune | item 1's file | ~40 LOC |
| 7 | Parity tests (pre-flush merged == post-flush) | new `cqlite-flight/src/tail.rs` test module reusing `testutil.rs` | ~150 LOC |
| — | **Merge/reconcile/LWW/tombstone/TTL/aggregation code** | `merge/mod.rs`, `merge/reconcile.rs`, `reconcile_rules.rs` | **ZERO changes** |
| — | Library/DataFusion-future seam: tail dir as extra `DiscoverySource::TableDirs` entry | `sstable/mod.rs:344`, `refresh.rs:67-74` | zero-to-small |

## Risks / unknowns
1. **Orphaned tail file older than gc_grace = data resurrection** (Cassandra purges tombstones without knowing the tail dir exists). Watermark/age prune (items 5-6) is the correctness backstop; normal churn is safe by huge margin. The design must state the invariant explicitly.
2. **commitLogIntervals not yet exposed** — the ⊇-watermark rule needs the `repair_metadata.rs` skip promoted to a decode; format is known and version-gated, but it's the one genuinely new parsing surface. A simpler max_timestamp- or age-based prune avoids it at slightly weaker precision.
3. **Counters**: LWW-dedup of duplicated counter cells across tail+flush is not Cassandra-correct; recommend excluding counter tables from the tail spike.
4. **Torn window on the Flight surface**: list→open race under plugin churn yields fail-closed request errors, not wrong data; consider snapshot-style hardlink sets for the tail dir if error rate matters. The `#1477`-referenced Flight rewrite/torn-window posture: the issue is now retitled "AB2 — Flight LIMIT pushdown" (OPEN); the recorded torn-window decision should be re-located before relying on it.
5. **Generation-number collision across dirs**: `data_paths` sorts newest-first by filename generation (`producer.rs:203,210-215`); a tail export named `nb-1-…` sorts below main gens. Only affects equal-timestamp+equal-liveness tie-break determinism (identical bytes in the duplicate case, so no wrong answers) — but the composite source should still order tail paths first for determinism.
6. **Tail export must be a complete component set** (Summary.db for token pruning, Statistics.db for the #1626 open-time hard-fail and watermark). A Data.db-only export degrades pruning (fail-open) and may fail `SSTableReader::open` on the library surface.
7. **Whole-file materialization**: the Flight merge drains inputs into memory per request (`producer.rs:13-15`, issue #591); every tail file adds to the per-request footprint — reinforces small, frequently-superseded exports.
8. **Cassandra-side timestamp semantics**: the §3 analysis assumes the export snapshot's cells appear in the eventual flush with identical timestamps (true for memtable snapshot exports; would NOT hold for anything that rewrites timestamps).
