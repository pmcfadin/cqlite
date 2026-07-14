# Design — Lazy Summary-guided BIG partition index (#2412)

This is the design for the "real fix (b)" of #2385: stop materializing the whole `Index.db` at BIG
open; make open O(summary), point lookup O(log n + interval), scans summary-guided streaming, and
resident memory ≈ summary only. BTI is already lazy and out of scope.

## Context / measured anchors (in-tree, this worktree)
- **Eager parse site:** `IndexReader::open_with_summary_cancellable`
  (`cqlite-core/src/storage/sstable/index_reader/mod.rs`) does `File::open` → `read_to_end(&mut buffer)`
  → `parse_index_data_cancellable` into `IndexData.partition_entries: Vec<PartitionIndexEntry>`. This is
  the O(N) cold parse and the ~500MB-resident structure. Emits `cqlite.sstable.index_parses_total` once
  per full pass (`index_reader/parse.rs`). `fully_parsed` records whether the whole file was consumed
  (Signal A, #2302).
- **Open wiring:** `SSTableReader::open` → `load_index_reader` (`reader/component_loading.rs`) →
  `IndexLoadOutcome::{Loaded(Box<IndexReader>), Absent, PresentButUnloadable}`. The redundant legacy
  `SSTableIndex` second parse was retired by #2385/#2395 (`load_index` Strategy 2 gone; Strategy 1
  integrated-format is inert for real 5.0). `load_summary_reader` already loads `Summary.db` but only for
  token-range iteration + the C5 range short-circuit — it does NOT guide `Index.db` access.
- **`Summary.db` reader:** `SummaryReader` (`summary_reader.rs`) exposes `get_entries()` (sampled
  key→`Index.db` position), `get_header()` (`min_index_interval`, `entries_count`), `get_first_key()` /
  `get_last_key()`, `find_entry_for_position()`, `get_entry_at()`. It has **no find-by-key binary
  search** today — that is the core new primitive.
- **Point lookup:** `big_get_with_resolution` (`data_access/big_point.rs`) — fast path is the resident
  raw-key `index_reader` map (O(1)); on soft-miss/absent it falls to the whole-file `scan_for_key`
  oracle. The C5 first/last-key range short-circuit (`RANGE_SHORT_CIRCUITS`, from `Summary.db`) already
  answers out-of-range point reads with zero probe work.
- **Scan:** `iterate_all_partitions_via_full_index` (`data_access/full_index_scan.rs`) consumes
  `index_reader.get_partition_entries()` (the whole materialized `Vec`) + `is_fully_parsed()`. The
  streaming walk lives in `full_index_stream` (#2361), sharing one implementation with the materializing
  walk, with a `(token, key)` order guard and cancel-aware `Drop` teardown.
- **Warm registry:** `cqlite-flight/src/warm/` (`registry.rs`, `identity.rs`, `probe.rs`, `rebuild.rs`)
  — `WarmTableRegistry` caches `Arc<SSTableReader>` keyed on inode-stable generation identity
  (dev+ino+size+generation, #2383 rebind-by-inode), pinning the resident index for the process lifetime.
- **Cassandra reference:** BIG `getPosition` = binary-search the summary sample + walk ≤
  `min_index_interval` (`BigTableReader.java`); BTI `PartitionIndex.load` = read three longs + on-demand
  trie DFS. CQLite's BTI path already mirrors the latter → lazy. Format spec:
  `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`.

## A — Summary.db as the open-time structure (+ absent/corrupt posture)
**Chosen: at BIG open, load `Summary.db` only and add a `find_by_key` binary search to `SummaryReader`.**
The reader keeps a lazy `Index.db` accessor (path + platform + a cheap `[first_key, last_key]` bound
already available) but performs **no full parse** at open. `index_data.partition_entries` is no longer
materialized eagerly; the reader holds the summary + a handle to seek/parse `Index.db` intervals on
demand. `Summary.db` is authoritative for sample key → `Index.db` position (little-endian position,
#1054) and for `min_index_interval` (the walk bound).

**Absent / corrupt `Summary.db` posture — RECOMMENDED: counted one-time linear `Index.db` scan (a
distinct FellBack full parse), NOT a hard error.** Options considered:
- **(A1, chosen) One-time linear `Index.db` scan, explicitly counted.** When `Summary.db` is absent or
  fails to parse, fall back to a single full `Index.db` parse (today's behavior), recorded as a distinct
  **full** parse in the work counters and surfaced via a `FellBack` reason (mirrors the existing
  `full_index_stream` FellBack gating). This is NOT a heuristic — it reads authoritative `Index.db`
  structure in full; it is simply the non-lazy path, made explicit and observable. Preserves correctness
  for the shapes that legitimately ship without a `Summary.db` (or with a stale/rebuildable one), which
  read fine today; a hard error would regress them.
- **(A2, rejected) Hard error on absent `Summary.db`.** Too aggressive: `Summary.db` is optional/
  rebuildable in Cassandra; erroring would break reads that work today. Reserve hard failure for the
  genuinely unusable case (both `Summary.db` AND `Index.db` corrupt/truncated — already covered by the
  existing corruption paths + `is_fully_parsed()` Signal A).
- **(A3, rejected) Silent linear fallback (today, un-counted).** Rejected because the field rounds
  (#2367) rely on the parse counters to prove laziness; an invisible full parse would mask a regression.

## B — Point-lookup path (O(log n + interval))
`big_get_with_resolution` gains a summary-guided fast path:
1. C5 first/last-key short-circuit (unchanged) — out-of-range → authoritative absence, zero probe work.
2. **Binary-search the summary** (`SummaryReader::find_by_key`) for the sample entry covering the query
   key → an `Index.db` start position.
3. **Read + parse ONE `Index.db` interval** (≤ `min_index_interval` entries) from that position, scanning
   forward for the exact partition entry. Bounded work: index entries touched ≤ one summary interval.
4. On an exact hit → resolve into `Data.db` as today. On a soft-miss within the interval (key genuinely
   absent) → authoritative absent (the interval is complete between two summary samples), so the
   whole-file `scan_for_key` oracle is **no longer required** for the common case — it is retained only
   for the absent-`Summary.db` FellBack path and genuinely index-less readers, preserving #1572
   correctness. The interval boundary is structural (two adjacent summary samples), not a guess.

## C — Scan path (summary-guided streaming) + #2361 interplay
Scans iterate `Index.db` forward from a summary-guided start offset without building the full map:
- Default full scan: start at the summary's first sample (offset 0) and stream forward to EOF, feeding
  the existing #2361 streaming walk (`full_index_stream`). The `(token, key)` order guard, fail-closed
  FellBack gating, cancel-aware `Drop` teardown, and phase-active counter are preserved unchanged — this
  design changes the *source* of entries (streamed from disk vs. read from a resident `Vec`), not the
  walk contract.
- `is_fully_parsed()` / Signal A (#2302) completeness semantics are preserved: a mid-entry-truncated
  `Index.db` streamed to EOF must still be detectable as incomplete (the streaming walk already carries
  the last-partition coverage check); the completeness signal moves from a post-parse buffer check to the
  streaming terminus.

## D — Warm-registry integration (memory accounting)
The `WarmTableRegistry` (`cqlite-flight/src/warm/registry.rs`) continues to pin `Arc<SSTableReader>` per
generation, but the pinned reader now holds **the summary + a lazy `Index.db` accessor**, not the full
partition map. Effect:
- Resident memory per warm generation drops from ~full-index (~500MB/gen at field scale) to ~summary
  (O(n/128) sampled entries). The `warm/budget.rs` accounting and `warm/metrics.rs` gauges are updated to
  reflect the summary-only footprint (the budget's per-generation cost estimate shrinks accordingly).
- The #2383 inode-stable identity + rebind and the #2310 diff/swap eviction are unchanged: identity keys
  on `(dev, ino, size, generation)`; a rebind swaps the backing path without re-parsing. Because open no
  longer parses `Index.db`, the single-flight `OpenCoalescer` (#2383) now coalesces a much cheaper open;
  the index-parse storm class it guarded against is structurally reduced (interval parses are per-lookup
  and bounded, not whole-file).

## E — The #2413 interplay (REQUIRED position; owner decides at Seam 1)
#2413 pushes the split's token range into the per-SSTable partition walk (today the token filter is
applied only downstream at `MergeProducer::drive_merge` via `token.contains`, so every scan decodes ALL
partition bodies from ring start; `iterate_token_range` is a deprecated no-op).

- **Option A (RECOMMENDED) — this change SUBSUMES #2413.** A range-bounded walk falls out of
  summary-guided iteration naturally: binary-search the summary to the split's token-range **start**
  sample, begin forward iteration there, and stop when entries pass the range **end**. Out-of-range
  partition body reads never happen. #2413's flip criterion (`walked <= 4` for a single-partition
  token-range warm scan) becomes a scenario of this change. Compaction consumers keep full-ring by NOT
  supplying a range (the range is a query-serve split concern only), exactly as #2413 requires. The
  FellBack (absent-`Summary.db`) path has no summary to binary-search → it stays a full walk (or a
  linear range filter), explicitly adjudicated and counted — the same open question #2413 flags for
  `sequential_scan`.
- **Option B — #2413 lands standalone FIRST on the existing resident index; this change preserves its
  pin.** #2413 binary-searches the (already token-ordered) resident index for the range start; then this
  change rebuilds the same range-bounded walk on the summary-guided iterator, keeping #2413's pin green.

**Recommendation: Option A.** #2412 redesigns the exact walk #2413 targets; landing #2413 first on the
resident index that #2412 *removes* is parity-sensitive work thrown away, and the summary-guided iterator
gives the range bound essentially for free (the same binary search that bounds a point lookup bounds a
range scan). The one reason to prefer B is scheduling: #2412 is larger and Seam-1-gated under P1 field
pressure, so if the token-pushdown win is needed before this design ships, B delivers it sooner. The
owner weighs that tradeoff at Seam 1; the spec's range-scoped scan scenario reflects the recommended
Option A and is the pin #2413 closes into.

## F — Metrics: `index_parses_total` semantics (field rounds depend on this)
`cqlite.sstable.index_parses_total` (catalog, #2383) today counts one **full** `Index.db` parse per
generation. This change **extends, not breaks** its semantics:
- **Full parses** (the FellBack absent-`Summary.db` path, §A1) continue to increment
  `index_parses_total` — so a lazy-open regression (accidentally full-parsing) is still visible as the
  counter climbing per generation, exactly as the field rounds (#2367) check.
- **Interval parses** (the bounded per-lookup `Index.db` reads, §B) are counted by a NEW **distinct**
  counter (`cqlite.sstable.index_interval_parses_total` — separate metric, added to the catalog) so
  interval work is observable but never conflated with full parses. A cold lazy open of K generations
  yields `index_parses_total += 0` (no full parse) and `index_interval_parses_total += 0` at open, then
  `+= 1` per point lookup — the work-probe pin for AC1.
- A scale-free work-probe (mirroring the #2383/#2385 pin style) asserts: lazy BIG open over an
  N-partition generation performs **zero** full parses and touches **zero** `Index.db` entries at open;
  a subsequent point read touches ≤ one `min_index_interval` of entries.

## G — Rollout / compatibility
- SSTables **with** `Summary.db` (the common case): lazy open, bounded lookups, summary-only resident.
- SSTables **without** `Summary.db`: §A1 counted full-parse FellBack — same correctness as today,
  explicitly observable. No behavior regression.
- The public surfaces (`Database::query`/`get`, flight `do_get` cold+warm, CLI) are unchanged in
  contract; the change is internal to `SSTableReader` open + the point/scan data-access paths + the flight
  warm registry's memory accounting. Wiring evidence: an end-to-end flight `do_get` (cold + warm) asserts
  correct rows AND the work-probe/memory-probe deltas.

## Sequencing (one branch, staged)
1. `SummaryReader::find_by_key` binary search + the lazy `Index.db` interval accessor (unit-pinned).
2. Lazy BIG open: `IndexReader`/`load_index_reader` stop materializing the full map; open loads summary
   only; §A1 FellBack path + counters. Work-probe pins AC1/AC5.
3. Point lookup §B on the summary-guided interval; parity + `rows_scanned`-bound pins (AC2).
4. Scan §C summary-guided streaming into the #2361 walk; physical-dump + query-semantics parity (AC3);
   the Option-A range-scoped scan scenario / #2413 pin flip (AC3, contingent on Seam-1 choice).
5. Warm-registry §D memory accounting + budget/metrics; resident-memory probe (AC4).
6. Flight `do_get` cold+warm e2e wiring evidence (AC7).

## Risks
- **Parity regression on the point/scan rewrite** — mitigated by the physical-dump + query-semantics
  oracles as the load-bearing net, and the `is_fully_parsed()`/Signal-A completeness semantics preserved.
- **Interval-boundary correctness** (a partition straddling a summary sample) — the interval is
  `[sample_i, sample_{i+1})` from authoritative summary positions; the forward walk within the interval
  is exact. Pinned by a point read whose key sits at an interval boundary.
- **Absent-`Summary.db` silent regression** — pre-empted by §A1's counted FellBack (never silent).
- **#2413 double-work** if landed on the doomed resident index — the recommendation (Option A) avoids it;
  the owner's Seam-1 call is explicit.
