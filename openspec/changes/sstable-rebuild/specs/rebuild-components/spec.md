# rebuild-components — new capability (sstable-rebuild, issue #4197)

`cqlite-core` SHALL regenerate requested derived SSTable components (Index.db, Summary.db,
Filter.db, Digest.crc32, TOC.txt, CRC.db, Statistics.db) from a healthy, UNCHANGED Data.db, and
SHALL report field-level provenance for every value it cannot derive purely from Data.db. All
requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Deterministic components are byte-identical to the Cassandra-written original

`rebuild_components` SHALL produce Digest.crc32, TOC.txt (component-set equality), and — for
uncompressed BIG input — CRC.db byte-identical (Digest/CRC.db) or set-identical (TOC.txt) to the
original, because each is a pure function of Data.db's existing, unchanged bytes.

#### Scenario: R1.1 Digest and CRC.db byte parity
- **Given** every committed uncompressed `test_basic` table with Digest.crc32 and CRC.db deleted
  from a temp copy
- **When** `rebuild_components(..., &[Digest, Crc])` runs
- **Then** both regenerated files are byte-identical to the originals Cassandra wrote
  (`cqlite-core/tests/issue_4197_rebuild_byte_parity.rs`, per-case, fail-closed on a missing
  committed fixture).

#### Scenario: R1.2 CRC.db not applicable to compressed input or BTI
- **Given** a compressed BIG fixture and a BTI fixture, each requesting `crc`
- **When** rebuild runs
- **Then** `crc` appears in `skipped_not_applicable` (design.md §D5), never in `regenerated`, and
  no `CRC.db` is written for either.

#### Scenario: R1.3 TOC.txt set-equality
- **Given** any committed table with TOC.txt deleted
- **When** rebuild regenerates it alongside every other requested component
- **Then** the rebuilt TOC.txt names exactly the components present on disk afterward (including
  itself), and `cqlite verify --mode full` reports no missing-component finding for the directory.

### Requirement: R2 — BIG Index.db is byte-identical from Data.db structure alone, measured against the file's own authoritative encoding baseline (BTI index deferred to #4336)

`rebuild_components` SHALL derive every BIG Index.db entry — key, data offset, promoted-index
blocks — from a byte-extent-aware structural walk of the existing Data.db, without decoding any
Data.db bytes not required to establish those extents, and SHALL produce output byte-identical to
the Cassandra-written original for every BIG table in the committed corpus.

**Scope amendment (owner ruling): the BTI (`da`) index is NOT in this change.** Rebuilding BTI's own
index — `Partitions.db`/`Rows.db`, which is what [`Component::Index`] names for a `da` input — is
deferred to follow-up **issue #4336** ("rebuild: BTI (`da`) Partitions.db/Rows.db index rebuild byte
parity", epic #4192). Reason: the byte-extent walk plus `PartitionsTrieWriter`/`RowsTrieWriter`
wiring (design.md §D2's BTI row, kept as forward-looking reference for #4336) is substantial enough
to warrant its own review pass rather than landing unreviewed inside an already-large change. This
requirement therefore makes NO byte-parity claim for `Partitions.db`/`Rows.db`; what it requires
instead is that the gap be FAIL-CLOSED, per R2.6 below. The original "R2.2 BTI Partitions.db/Rows.db
byte parity" scenario is dropped here and belongs to #4336, which is also where
`cqlite-core/tests/issue_4197_rebuild_bti_scope.rs` must be replaced by the byte-parity assertions
R2.2 described.

Every other component stays format-agnostic exactly as R1/R3/R4 state: a `da` input still rebuilds
`Filter.db`/`Digest.crc32`/`TOC.txt`/`Statistics.db`, and still reports `summary`/`crc` as
`skipped_not_applicable` (R1.2) — the deferral is confined to the index itself.

Because every promoted-index block offset/width is measured in bytes whose VInt widths are
delta-encoded against the whole-SSTable `SerializationHeader.EncodingStats` baseline,
`rebuild_components` SHALL take that baseline from the ORIGINAL `Statistics.db`'s own
SerializationHeader whenever it is readable, and SHALL NOT prefer a value re-derived from the
decoded content. The baseline is not bounded above by this file's own content — Cassandra carries
EncodingStats minima forward from compaction inputs (`SerializationHeader.make(metadata,
sstables)` → `EncodingStats.merge`), and a minimum-carrying row can be shadow-dropped by
reconciliation before rebuild decodes it — so a re-derivation can only come out too HIGH, silently
narrowing every delta. A re-derivation from the decoded content is the DOCUMENTED FALLBACK, used
only when the original `Statistics.db`/header is genuinely unreadable (rebuild's own headline case
is a MISSING `Statistics.db`), and it never certifies byte parity.

Independently of which baseline is in force, `rebuild_components` SHALL refuse an `index`/`summary`
request whenever a partition that CARRIES a promoted-index payload (≥ 2 blocks, the gate
Cassandra's `RowIndexEntry.create()` applies as `columnIndexCount > 1`) does not re-encode to its
ACTUAL on-disk byte span, rather than writing block offsets it cannot reproduce. The check is
scoped to payload-bearing partitions BY MEASUREMENT, not by preference: every other value in an
Index.db/Summary.db entry (key, data offset, the zero promoted-size VInt, and hence the entry size
a Summary sample records) comes from the authoritative boundary walk rather than the re-encode, and
an unconditional span comparison was measured to refuse 58 of 114 committed BIG generations whose
rebuilt Index.db is byte-identical to Cassandra's own.

#### Scenario: R2.1 BIG uncompressed and compressed Index.db byte parity
- **Given** every committed `test_basic`/`test_collections`/`test_wide_rows` table (uncompressed
  and compressed variants), Index.db deleted from a temp copy
- **When** rebuild regenerates `index`
- **Then** the output is byte-identical to the original, including promoted-index payloads for
  every wide partition present in the corpus
  (`cqlite-core/tests/issue_4197_rebuild_index_parity.rs`).

#### Scenario: R2.6 BTI index rebuild is refused fail-closed, not silently attempted (replaces the dropped R2.2; owned by #4336)
- **Given** every committed `test_da` (BTI) table, requesting `index` alone and mixed with
  components that ARE supported for `da`
- **When** rebuild runs
- **Then** it returns `Error::UnsupportedFormat` — a USAGE error, never a panic, never a silent
  success, and never a different variant that would read as data corruption — whose message names
  the deferral's tracking issue #4336; NOTHING is written under `--out`; no BIG-shaped `Index.db`
  ever appears beside a BTI generation; and the same fixture WITHOUT `index` still succeeds, with
  `Partitions.db`/`Rows.db` under `--out` byte-identical to the Cassandra-written originals because
  they were COPIED verbatim, never rebuilt
  (`cqlite-core/tests/issue_4197_rebuild_bti_scope.rs`; mutation-verified against removing the
  guard, changing its variant, and dropping the issue reference).

#### Scenario: R2.4 the original header's baseline wins over any re-derivation
- **Given** a generation written with a whole-SSTable EncodingStats baseline deliberately BELOW
  every timestamp present in its own content (the state Cassandra reaches by inheriting minima at
  compaction; reproduced through `SSTableWriter::pre_seed_encoding_baselines`), wide enough to
  carry real promoted-index payloads, with `Index.db` deleted from a working copy
- **When** rebuild regenerates `index`
- **Then** the output is byte-identical to that generation's own original `Index.db`, and
  `classification.index.encoding_stats_baseline == "recovered"` — whereas a baseline re-derived
  from the decoded content differs from the real one and produces different promoted-index bytes
  (`cqlite-core/tests/issue_4197_rebuild_baseline_provenance.rs`; the committed-corpus fixtures
  cannot observe this, their derived and true baselines coincide).

#### Scenario: R2.5 an unrecoverable baseline refuses, never ships desynced offsets
- **Given** the same generation (its partitions carry promoted-index payloads) with BOTH
  `Index.db` and `Statistics.db` deleted — the baseline is then genuinely unrecoverable, since
  Data.db stores timestamps as unsigned deltas FROM it
- **When** rebuild regenerates `index`
- **Then** `report.refused.reason == "reencode-mismatch"` (NOT `data-corrupt` — the input is
  healthy, rebuild just cannot reproduce its encoding), the remedy names restoring the original
  `Statistics.db` first and `salvage` (#4196) as the fallback, `index` never appears in
  `regenerated`, and no `Index.db` is left under `--out`.
- **And** the check costs no working capability: an `index` rebuild sweep over all 114 committed
  BIG generations produces the SAME 109 byte-identical Index.db files / 0 mismatches with the
  check armed as with it disabled.

#### Scenario: R2.3 No header hunting
- **Given** `scripts/tests/test_rebuild_no_resync_scan.sh` (`tooling-tests`, mirrors salvage's
  R4.3/#4196)
- **When** it greps `cqlite-core/src/storage/write_engine/rebuild/` for any byte-pattern search
  primitive (`memchr`, `windows(`, `find(|b|`, `position(|b|`) outside tests
- **Then** none is present; any hit FAILs naming the line.

### Requirement: R3 — Summary.db and Filter.db are byte-identical only when their governing parameter is recoverable, and the manifest always says which

`rebuild_components` SHALL classify `min_index_interval` (Summary.db) and `bloom_filter_fp_chance`
(Filter.db) as `recovered` or `recomputed` per design.md §D2, and SHALL NEVER present a
`recomputed`-valued component as byte-equal to an original that used a different value.

#### Scenario: R3.1 bloom_filter_fp_chance recovered from schema
- **Given** a committed table's schema `.cql` stating a `bloom_filter_fp_chance` value, Filter.db
  deleted
- **When** rebuild regenerates `filter` with `--schema` pointing at that file
- **Then** `classification.filter.bloom_filter_fp_chance == "recovered"`, and the rebuilt Filter.db
  matches the original's membership (no false negatives for any key present) and `hash_count`.
  **Full byte-identity is NOT claimed**: CQLite's `fp_chance -> hash_count` step selection disagrees
  with Cassandra's own `BloomCalculations` table by one step (tracked separately as issue #4335,
  out of scope for this change — rebuild drives the existing `FilterWriter`, it does not define the
  bloom spec), and the filter's bit-array size additionally depends on
  compaction-inherited `estimatedKeys`, not a recount of Data.db's final partition set
  (`cqlite-core/tests/issue_4197_rebuild_summary_classification.rs`).

#### Scenario: R3.2 min_index_interval cannot be recovered today — documented, not silently wrong
- **Given** a schema constructed in-test with a non-default `min_index_interval` WITH-clause value
  and a Summary.db written at that value, then deleted
- **When** rebuild regenerates `summary`
- **Then** the rebuilt Summary.db uses Cassandra's default 128 (NOT the schema's stated value —
  today's `SSTableWriter` hardcodes it, design.md §D2),
  `classification.summary.min_index_interval == "recomputed"`, and the test asserts the rebuilt
  bytes DIFFER from the original at that value — proving the gap is disclosed, never masked
  (`cqlite-core/tests/issue_4197_rebuild_summary_classification.rs`).

#### Scenario: R3.3 sampling_level is always recomputed at the hardcoded default
- **Given** any Summary.db rebuild (R3.1 or R3.2's fixtures)
- **When** it runs
- **Then** `sampling_level` in the output is `BASE_SAMPLING_LEVEL` (128) — correct for a
  never-downsampled Cassandra Summary.db but WRONG for one Cassandra's `IndexSummaryManager`
  downsampled (level < 128), since nothing reads the original's actual value — and
  `classification.summary.sampling_level == "recomputed"` (roborev job 130 Medium finding:
  this was mislabelled `recovered`, same class of bug as `min_index_interval` already avoids).

### Requirement: R4 — Statistics.db rebuild is opt-in, and every field is classified recovered/recomputed/lost

`rebuild_components` SHALL NOT include `statistics` in a default request, and when explicitly
requested SHALL recompute every aggregate field from a full Data.db decode while classifying
`repaired_at`/`pending_repair`/`is_transient` as `recovered` (original Statistics.db readable) or
`lost` (unreadable), and origin-host/compaction-ancestry as unconditionally `lost`.

The six timestamp/TTL/local-deletion-time aggregates are a THIRD case, because Data.db stores each
of them as an unsigned delta from the very `EncodingStats` baseline being regenerated (R2):

- original SerializationHeader readable — all six are a genuine fold over correctly-decoded
  content, classified `recomputed`. The recovered baseline is NOT one of them: `EncodingStats`
  (the SERIALIZATION_HEADER triple every row's VInt was delta-encoded against, which Cassandra
  merges forward from compaction INPUTS and which can therefore sit strictly BELOW anything in
  this file's own rows) and `StatsMetadata.minTimestamp`/`minLocalDeletionTime`/`minTTL`
  (Cassandra's `MetadataCollector` fold over the cells and tombstones actually WRITTEN) are TWO
  DIFFERENT VALUES. The rebuilt header SHALL carry the recovered baseline verbatim — or the
  unchanged Data.db would delta-decode against a baseline it was never encoded with — while the
  STATS minima SHALL be established independently by the pass-2 content fold, and SHALL NOT be
  pre-seeded from the baseline (which, being a minimum, silently pins them). The baseline's own
  provenance is reported under `classification.statistics.encoding_stats_baseline`, the same key
  the `index` component uses for it.
- original unreadable — the decode that would feed a recomputation is circular, so all six SHALL be
  classified `lost`: default-valued and named so, never advertised as `recomputed`. Counts and
  key bounds do not depend on the baseline and stay `recomputed`.

#### Scenario: R4.1 aggregates recomputed correctly
- **Given** any committed table, Statistics.db deleted
- **When** rebuild regenerates `statistics`
- **Then** every recomputed field (min/max timestamp, min/max local-deletion-time, min/max TTL,
  partition/row/column counts, both estimated histograms, first/last key,
  has-partition-level-deletions) equals the value an independent re-derivation from the fixture's
  `*-Data.db.jsonl` golden computes — never compared against CQLite's own prior Statistics.db
  output (`cqlite-core/tests/issue_4197_rebuild_statistics_recompute.rs`), the six timestamp/TTL/LDT
  aggregates are classified `recomputed` while `encoding_stats_baseline` is classified `recovered`
  when the original header supplied it, and — with the original `Statistics.db` deleted outright —
  all six are classified `lost` while the counts stay `recomputed`, with the written baseline
  demonstrably differing from the original's (so the `lost` label is load-bearing, not decorative).

#### Scenario: R4.1a the STATS minima and the EncodingStats baseline can legitimately differ
- **Given** a generation whose `EncodingStats` baseline sits strictly BELOW every timestamp present
  in its own content (the compaction-inherited state, reached here via
  `SSTableWriter::pre_seed_encoding_baselines`), its original `Statistics.db` still readable
- **When** rebuild regenerates `statistics`
- **Then** the regenerated `StatsMetadata` minimum equals the minimum a full decode actually finds
  in the rows (NOT the lower baseline), the regenerated SERIALIZATION_HEADER's `EncodingStats`
  minimum equals the original header's value verbatim, the two demonstrably differ, and the
  manifest classifies the fold `recomputed` and the baseline `recovered`
  (`cqlite-core/src/storage/write_engine/rebuild/stats_baseline_tests.rs`)

#### Scenario: R4.2 repair fields recovered when the original is readable
- **Given** a temp copy where Statistics.db is renamed aside (readable, but not at its expected
  path) before rebuild reads it as the recovery source, for a source whose `repaired_at != 0`
- **When** rebuild regenerates `statistics` using the renamed-aside file as the recovery input
- **Then** the rebuilt `repaired_at`/`pending_repair`/`is_transient` equal the original's, and
  `classification.statistics.repaired_at == "recovered"`.

#### Scenario: R4.3 repair fields lost when the original is gone, never guessed
- **Given** a temp copy where Statistics.db is deleted outright (not renamed aside — genuinely
  unreadable)
- **When** rebuild regenerates `statistics`
- **Then** `repaired_at == 0`, `pending_repair == None`, `is_transient == false`, and
  `classification.statistics.{repaired_at,pending_repair,is_transient} == "lost"` — never silently
  `"recovered"` with a default value.

#### Scenario: R4.4 opt-in enforced
- **Given** a request that does not name `statistics`
- **When** rebuild runs
- **Then** no Statistics.db write is attempted and `statistics` never appears in `regenerated`.

### Requirement: R5 — Refuses when Data.db itself cannot be trusted

`rebuild_components` SHALL refuse, writing nothing, when a chunk-CRC check over Data.db fails, a
partition fails to decode structurally while walking it, or the boundary walk finds the same
partition key at two different on-disk offsets, and SHALL name `salvage` (#4196) as the remedy.

The boundary walk SHALL enumerate one entry per on-disk partition BOUNDARY and SHALL NOT
deduplicate by partition key: a repeated key is Cassandra's `Verifier` "Key out of order"
condition (a non-increasing `(token, key)` step), so deduplicating it under-enumerates the file and
computes every derived component over a partition set that does not match it.

#### Scenario: R5.1 damaged Data.db refuses and names salvage
- **Given** `test_comp_corrupt/data_db_bit_flip` (skip-clean if absent; required under
  `CQLITE_REQUIRE_FIXTURES=1`)
- **When** rebuild runs requesting any component
- **Then** `report.refused.reason == "data-corrupt"`, `remedy` names `salvage` (#4196), the exact
  chunk offset is reported, and NOTHING is written to `--out`
  (`cqlite-core/tests/issue_4197_rebuild_refusal.rs`).

#### Scenario: R5.1a one partition key at two offsets refuses
- **Given** a Data.db synthesized by doubling a healthy single-partition extent, so the same key
  appears at two ascending offsets, with NARROW partitions (no promoted-index payload, so R2's
  re-encoded-span cross-check cannot be what catches it)
- **When** rebuild runs requesting `index`
- **Then** `report.refused.reason == "data-corrupt"`, the offset of the REPEAT is reported,
  `remedy` names `salvage`, and nothing is regenerated
  (`cqlite-core/tests/issue_4197_rebuild_refusal.rs`).

#### Scenario: R5.2 input never modified
- **Given** a recursive sha256 listing of the input Data.db (and any other untouched components)
  before every scenario in this spec
- **Then** the listing is identical afterwards, for both successful and refused runs.

### Requirement: R6 — Bounded memory

`rebuild_components` SHALL hold at most one partition's structural state resident on the read side
for Index/Summary/Filter/CRC, and one partition's decoded mutations for a Statistics rebuild. In
particular the partition-boundary walk SHALL stream the data section (one chunk plus one in-flight
structure resident) and SHALL NOT materialise the whole decompressed section first.

What legitimately remains proportional to the input is the enumerated boundary LIST itself — one
`(data_offset, raw_key)` per partition — which both passes index into for the next partition's end
bound.

#### Scenario: R6.1 wide partitions under the budget lane
- **Given** `test_wide_rows` (every table) under the gate's `memory-budget` component (dhat)
- **When** rebuild runs requesting every component including `statistics`
- **Then** peak heap stays within the existing lane threshold for a single-input compaction of the
  same table, AND within the tighter pinned ceiling the lane records alongside it — which is set
  close enough to the measured figure that a return to whole-section materialisation reddens it
  (`cqlite-core/tests/issue_4197_rebuild_memory_budget.rs`).
