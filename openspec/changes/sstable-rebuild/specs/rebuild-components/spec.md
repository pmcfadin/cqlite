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

### Requirement: R2 — Index.db and BTI Partitions.db/Rows.db are byte-identical from Data.db structure alone

`rebuild_components` SHALL derive every Index.db entry (or BTI trie leaf) — key, data offset,
promoted-index/row-index blocks — from a byte-extent-aware structural walk of the existing Data.db,
without decoding any Data.db bytes not required to establish those extents, and SHALL produce
output byte-identical to the Cassandra-written original for every table in the committed corpus.

#### Scenario: R2.1 BIG uncompressed and compressed Index.db byte parity
- **Given** every committed `test_basic`/`test_collections`/`test_wide_rows` table (uncompressed
  and compressed variants), Index.db deleted from a temp copy
- **When** rebuild regenerates `index`
- **Then** the output is byte-identical to the original, including promoted-index payloads for
  every wide partition present in the corpus
  (`cqlite-core/tests/issue_4197_rebuild_index_parity.rs`).

#### Scenario: R2.2 BTI Partitions.db/Rows.db byte parity
- **Given** every committed `test_da` table, Partitions.db and Rows.db deleted from a temp copy
- **When** rebuild regenerates `index` (BTI's equivalent request)
- **Then** both files are byte-identical to the originals, including `RowsOffset` payloads for wide
  partitions (`cqlite-core/tests/issue_4197_rebuild_bti_index_parity.rs`).

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
- **Given** a committed table's schema `.cql` stating `WITH bloom_filter_fp_chance = 0.01` (matches
  the fixture's actual write-time value), Filter.db deleted
- **When** rebuild regenerates `filter` with `--schema` pointing at that file
- **Then** the rebuilt Filter.db is byte-identical to the original and
  `classification.filter.bloom_filter_fp_chance == "recovered"`.

#### Scenario: R3.2 min_index_interval cannot be recovered today — documented, not silently wrong
- **Given** a schema constructed in-test with a non-default `min_index_interval` WITH-clause value
  and a Summary.db written at that value, then deleted
- **When** rebuild regenerates `summary`
- **Then** the rebuilt Summary.db uses Cassandra's default 128 (NOT the schema's stated value —
  today's `SSTableWriter` hardcodes it, design.md §D2),
  `classification.summary.min_index_interval == "recomputed"`, and the test asserts the rebuilt
  bytes DIFFER from the original at that value — proving the gap is disclosed, never masked
  (`cqlite-core/tests/issue_4197_rebuild_summary_classification.rs`).

#### Scenario: R3.3 sampling_level always recovered
- **Given** any Summary.db rebuild (R3.1 or R3.2's fixtures)
- **When** it runs
- **Then** `sampling_level` in the output is `BASE_SAMPLING_LEVEL` (128), matching every
  freshly-written (never-downsampled) Cassandra Summary.db, and
  `classification.summary.sampling_level == "recovered"`.

### Requirement: R4 — Statistics.db rebuild is opt-in, and every field is classified recovered/recomputed/lost

`rebuild_components` SHALL NOT include `statistics` in a default request, and when explicitly
requested SHALL recompute every aggregate field from a full Data.db decode while classifying
`repaired_at`/`pending_repair`/`is_transient` as `recovered` (original Statistics.db readable) or
`lost` (unreadable), and origin-host/compaction-ancestry as unconditionally `lost`.

#### Scenario: R4.1 aggregates recomputed correctly
- **Given** any committed table, Statistics.db deleted
- **When** rebuild regenerates `statistics`
- **Then** every recomputed field (min/max timestamp, min/max local-deletion-time, min/max TTL,
  partition/row/column counts, both estimated histograms, first/last key,
  has-partition-level-deletions) equals the value an independent re-derivation from the fixture's
  `*-Data.db.jsonl` golden computes — never compared against CQLite's own prior Statistics.db
  output (`cqlite-core/tests/issue_4197_rebuild_statistics_recompute.rs`).

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

`rebuild_components` SHALL refuse, writing nothing, when a chunk-CRC check over Data.db fails or a
partition fails to decode structurally while walking it, and SHALL name `salvage` (#4196) as the
remedy.

#### Scenario: R5.1 damaged Data.db refuses and names salvage
- **Given** `test_comp_corrupt/data_db_bit_flip` (skip-clean if absent; required under
  `CQLITE_REQUIRE_FIXTURES=1`)
- **When** rebuild runs requesting any component
- **Then** `report.refused.reason == "data-corrupt"`, `remedy` names `salvage` (#4196), the exact
  chunk offset is reported, and NOTHING is written to `--out`
  (`cqlite-core/tests/issue_4197_rebuild_refusal.rs`).

#### Scenario: R5.2 input never modified
- **Given** a recursive sha256 listing of the input Data.db (and any other untouched components)
  before every scenario in this spec
- **Then** the listing is identical afterwards, for both successful and refused runs.

### Requirement: R6 — Bounded memory

`rebuild_components` SHALL hold at most one partition's structural state resident on the read side
for Index/Summary/Filter/CRC, and one partition's decoded mutations for a Statistics rebuild.

#### Scenario: R6.1 wide partitions under the budget lane
- **Given** `test_wide_rows` (every table) under the gate's `memory-budget` component (dhat)
- **When** rebuild runs requesting every component including `statistics`
- **Then** peak heap stays within the existing lane threshold for a single-input compaction of the
  same table.
