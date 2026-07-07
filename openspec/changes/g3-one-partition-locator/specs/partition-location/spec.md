## ADDED Requirements

### Requirement: A single format-tagged partition locator resolves every point read
Partition location SHALL be resolved through ONE format-tagged façade,
`SSTableReader::locate(key) -> Result<Option<(u64, u32)>>`, that returns the
partition's uncompressed `Data.db` offset (and its size where the format records
one; `0` where it does not). The BIG (`nb`/uncompressed) implementation SHALL
compose the Summary-derived range bound and the raw-key `Index.db` map; the BTI
(`da`) implementation SHALL walk the `Partitions.db` trie. The point-read path and
the candidate-prune path SHALL both resolve partitions through this façade, and the
now-unreachable per-path entry points SHALL be deleted.

#### Scenario: locate() parity with the legacy BIG path for present keys
- **WHEN** `locate` is called for every partition key present in each BIG (`nb` and
  uncompressed) fixture
- **THEN** it returns `Some((offset, size))` whose offset is byte-identical to the
  offset the pre-migration `lookup_partition_with_index` resolved for that key

#### Scenario: locate() parity with the legacy BTI path for present keys
- **WHEN** `locate` is called for every partition key present in each BTI (`da`)
  fixture (narrow and wide partitions)
- **THEN** it returns `Some((offset, _))` whose offset is byte-identical to the
  offset the pre-migration `lookup_partition_via_bti_trie` resolved for that key

#### Scenario: locate() parity for absent keys
- **WHEN** `locate` is called for keys that are absent from a BIG fixture and from a
  BTI fixture
- **THEN** it returns exactly what the corresponding legacy path returned for that
  key (`None` for a BTI trie miss; for BIG, the same `None`-vs-fallthrough result the
  legacy `lookup_partition_with_index` produced, so the point path takes the same
  branch it took pre-migration)

#### Scenario: locate() parity for boundary keys
- **WHEN** `locate` is called for the physically-first (`first_key`) and
  physically-last (`last_key`) partition of each fixture
- **THEN** the boundary partitions resolve to the same offsets as the legacy paths
  and are never short-circuited as out-of-range (the bound is inclusive at both ends)

### Requirement: Per-format bloom ordering is preserved bit-for-bit
The consolidation SHALL NOT change the deliberate, verified per-format presence
ordering. The BIG path SHALL keep its bloom-filter pre-check FIRST (a definite bloom
miss short-circuits to `None` before any `Index.db` probe). The BTI path SHALL keep
skipping the bloom filter entirely and treat the `Partitions.db` trie as the sole
authoritative present/absent oracle.

#### Scenario: BIG keeps bloom-first ordering
- **WHEN** a BIG point read runs for an absent key whose bloom filter reports a
  definite miss
- **THEN** the read returns `None` via the bloom pre-check and performs zero
  `Index.db` probes (`INDEX_PROBES` unchanged), exactly as before the change

#### Scenario: BTI keeps bloom-skip ordering
- **WHEN** a BTI point read runs
- **THEN** the bloom filter is never consulted, the `Partitions.db` trie decides
  presence, and `READ_BLOOM_CHECKS` is emitted exactly once from the trie descent
  (not from a bloom pre-check)

### Requirement: No behavior change is acceptable collateral
The migration SHALL preserve identical offsets, identical negative results, and
identical error classification on every read path. The 33-table golden parity harness
SHALL remain green.

#### Scenario: Golden parity is preserved
- **WHEN** the SSTable read/parity tests run against the real test datasets after the
  migration
- **THEN** every previously passing table parses to the same rows

#### Scenario: Error classification is unchanged
- **WHEN** a structurally-invalid input (a `RowsOffset` with no `Rows.db`, a corrupt
  trie, an offset past EOF) is read through `locate`
- **THEN** the same typed `Error::Corruption` (same variant and message class) is
  raised as the corresponding legacy path raised

### Requirement: B4 key cache and C5 range short-circuit live in the façade, written once
The system SHALL provide a single implementation of the B4 key→partition-offset cache
and a single implementation of the C5 `[first_key, last_key]` range short-circuit, both
reachable only through the façade and serving both formats, with unchanged semantics: the
B4 cache MUST be positive-only (an absent key is never cached), and the C5 short-circuit
MUST be inclusive at both ends, MUST compare in Cassandra Murmur3 token order, and MUST be
a no-op when no authoritative Summary bound exists (e.g. BTI).

#### Scenario: B4 cache serves a repeated present-key read without re-probing
- **WHEN** the same present key is located twice through the façade
- **THEN** the second call is served from the key-offset cache with zero new
  `Index.db` probes / trie walks (`INDEX_PROBES` / `TRIE_WALKS` unchanged on the hit)

#### Scenario: C5 short-circuit answers an out-of-range key before any presence work
- **WHEN** a key that sorts outside a BIG SSTable's `[first_key, last_key]` bound is
  located
- **THEN** the façade returns absence recording exactly one range short-circuit and
  performing zero bloom checks, `Index.db` probes, or trie descents

### Requirement: Over-threshold files are split as they are touched, not grown
Per campsite rule #1116, `index_reader.rs` and `reader/data_access/bti.rs` SHALL be
split into smaller modules as this change touches them, and the touched files SHALL
NOT exceed their pre-change line counts. The library SHALL contain no `unwrap()` /
`expect()` in non-test code and SHALL compile clean under `RUSTFLAGS="-D warnings"`.

#### Scenario: Touched over-threshold files shrink
- **WHEN** `index_reader.rs` and `reader/data_access/bti.rs` are modified by this
  change
- **THEN** each resulting primary file has fewer lines than before the change, with
  the extracted logic moved into a sibling submodule

#### Scenario: Warnings-clean, no unwrap/expect
- **WHEN** `cqlite-core` is built and clippy-linted under `-D warnings`
- **THEN** the build and lint pass with no warnings and no new `unwrap()`/`expect()`
  in library code
