# lazy-big-partition-index Specification

## Purpose
TBD - created by archiving change lazy-summary-guided-index. Update Purpose after archive.
## Requirements
### Requirement: BIG open loads only Summary.db and performs no full Index.db parse

BIG-format `SSTableReader::open` SHALL load `Summary.db` (O(n/128) sampled entries) as its open-time
partition-index structure and SHALL NOT parse the whole `Index.db` into a resident in-memory map. Open
cost SHALL be O(summary): the number of `Index.db` entries touched at open SHALL be zero and the number
of full `Index.db` parses at open SHALL be zero. This applies to `na`/`nb` BIG SSTables that ship a
`Summary.db`; BTI (`da`/`oa`) is out of scope (already lazy).

#### Scenario: Cold BIG open touches zero Index.db entries and performs zero full parses

- **GIVEN** a BIG SSTable with a `Summary.db` and an N-partition `Index.db`, with the read-work counters
  reset
- **WHEN** `SSTableReader::open` completes a cold open of the generation
- **THEN** the full-`Index.db`-parse counter (`cqlite.sstable.index_parses_total`) increments by 0 for
  that open
- **AND** the count of `Index.db` entries touched at open is 0 (scale-free: independent of N).

#### Scenario: Open cost does not grow with partition count

- **GIVEN** two BIG generations of the same shape differing only in partition count (small vs large)
- **WHEN** each is cold-opened with work counters reset between opens
- **THEN** both opens touch 0 `Index.db` entries and perform 0 full `Index.db` parses (the open-time work
  is bounded by `Summary.db` size, not by partition count).

### Requirement: Point lookup reads a single Summary-bounded Index.db interval

A BIG point lookup (`WHERE pk = ?`) SHALL binary-search `Summary.db` for the sample covering the query
key and SHALL read/parse at most one `Index.db` interval (≤ `min_index_interval` entries) to resolve the
partition. The number of `Index.db` index entries touched by one point lookup SHALL be bounded by one
summary interval. Results SHALL remain byte-identical to the sstabledump/JSONL goldens.

#### Scenario: A present-key point read touches at most one summary interval of index entries

- **GIVEN** a BIG SSTable with `Summary.db` and a partition key known present, work counters reset
- **WHEN** the partition is fetched via the public read path
- **THEN** the returned partition matches the physical-dump golden byte-for-byte
- **AND** the count of `Index.db` index entries touched is ≤ `min_index_interval` (one summary interval),
  not O(N).

#### Scenario: An absent-key point read within range is authoritative from one interval

- **GIVEN** a BIG SSTable with `Summary.db` and a partition key that is in `[first_key, last_key]` but
  genuinely absent
- **WHEN** the partition is fetched
- **THEN** the read returns "not found" as an authoritative absence after touching ≤ one summary interval
  of `Index.db` entries
- **AND** it does not fall back to a whole-file `scan_for_key` of the generation.

#### Scenario: A key at an interval boundary resolves correctly

- **GIVEN** a partition key whose position sits at the boundary between two adjacent `Summary.db` samples
- **WHEN** the partition is fetched
- **THEN** the returned partition matches the physical-dump golden (the interval `[sample_i, sample_{i+1})`
  is derived from authoritative summary positions, not guessed).

### Requirement: Full and range scans stream via Summary-guided Index.db iteration

BIG full scans SHALL iterate `Index.db` forward from a `Summary.db`-guided start offset without
materializing the whole index, feeding the existing streaming full-index walk (its `(token, key)` order
guard, fail-closed FellBack gating, and cancel-aware teardown preserved). A token-range-scoped query
split SHALL begin iteration at the summary sample covering the range start and stop at the range end, so
out-of-range partition bodies are never read (the recommended #2413 Option A; the exact posture is
confirmed at Seam 1). Compaction consumers SHALL keep full-ring, non-range-scoped walks. Parity SHALL
hold on both the physical-dump and query-semantics oracles.

#### Scenario: A full scan streams the index and matches both parity oracles

- **GIVEN** a BIG SSTable with `Summary.db`
- **WHEN** a full scan enumerates every partition via the summary-guided streaming walk
- **THEN** the enumerated cells match the physical-dump (`*-Data.db.jsonl`) golden
- **AND** the post-reconciliation `SELECT` result at a pinned `now` matches the query-semantics oracle
- **AND** the `(token, key)` order guard is not violated.

#### Scenario: A token-range split reads only in-range partitions (#2413 pin flip)

- **GIVEN** a BIG SSTable and a query split whose token range covers a single partition
- **WHEN** the warm scan enumerates partitions for that split
- **THEN** the number of partition bodies decoded is ≤ 4 (bounded by the in-range set + LIMIT), not the
  whole-SSTable count
- **AND** the returned rows match the physical-dump + query-semantics oracles.

#### Scenario: Compaction still walks the full ring

- **WHEN** a compaction consumer enumerates partitions from a BIG reader
- **THEN** it walks every partition in the generation (no token range is applied to compaction
  consumers) and existing compaction byte-parity tests stay green.

### Requirement: Resident per-generation index memory is Summary-only

A BIG reader retained by the flight `WarmTableRegistry` SHALL pin only the `Summary.db`-derived structure
(O(n/128) sampled entries) plus a lazy `Index.db` accessor, NOT a full resident partition map. Resident
index memory per warm generation SHALL be bounded by summary size, not by partition count.

#### Scenario: Warm-held generation retains summary-only index memory

- **GIVEN** a BIG generation held warm in the `WarmTableRegistry` after a `do_get`
- **WHEN** its resident index footprint is measured (memory probe or dhat budget)
- **THEN** the retained index memory is bounded by `Summary.db` size (O(n/128)), materially below the
  full-index footprint the pre-change reader pinned
- **AND** it does not scale linearly with the generation's partition count.

### Requirement: Index-parse work counters distinguish interval parses from full parses

The observability catalog SHALL preserve `cqlite.sstable.index_parses_total` as the count of **full**
`Index.db` parses (so a lazy-open regression that full-parses is still visible as the counter climbing
per generation) and SHALL add a **distinct** counter for bounded **interval** parses so per-lookup
interval work is observable but never conflated with full parses. Both counters SHALL be registered and
namespaced under `cqlite.`.

#### Scenario: Lazy open reports zero full parses; interval work is counted separately

- **GIVEN** a BIG SSTable with `Summary.db` and both counters reset
- **WHEN** the generation is cold-opened and then one point lookup is performed
- **THEN** `cqlite.sstable.index_parses_total` increments by 0 (no full parse at open or lookup)
- **AND** the interval-parse counter increments by exactly 1 for the single lookup
- **AND** both counter names are present in the catalog and start with `cqlite.`.

### Requirement: All offsets and intervals derive from Summary.db/Index.db structure (no-heuristics)

Every `Index.db` seek offset and interval boundary SHALL come from authoritative `Summary.db` sample
positions and `Index.db` structure — never from guessed boundaries or byte-pattern inference. When
`Summary.db` is absent or fails to parse, the reader SHALL fall back to a single, explicit, **counted**
full `Index.db` parse (recorded as a full parse via `cqlite.sstable.index_parses_total` and surfaced as a
FellBack reason) rather than a silent guess or a hard error, preserving today's correctness for shapes
that ship without a usable `Summary.db`.

#### Scenario: Absent Summary.db falls back to a counted full parse, not a guess

- **GIVEN** a BIG SSTable whose `Summary.db` is absent (or present-but-unparseable), counters reset
- **WHEN** the generation is opened and scanned end-to-end
- **THEN** exactly one full `Index.db` parse occurs, `cqlite.sstable.index_parses_total` increments by 1,
  and the FellBack reason is surfaced (not silent)
- **AND** the enumerated cells still match the physical-dump golden (correctness preserved).

#### Scenario: No offset is inferred from value bytes

- **WHEN** any partition is located via the summary-guided path
- **THEN** the seek offset and interval bounds are taken from `Summary.db` positions / `Index.db` entry
  framing only, with no boundary inferred from partition or cell byte patterns.

### Requirement: Exercised end-to-end through the flight do_get path (cold and warm)

The lazy Summary-guided BIG index SHALL be exercised end-to-end through the flight `do_get` read path on
both a cold first request and a warm repeat request, proving the wiring (a named surface + call chain +
an end-to-end test), not helper-only unit coverage.

#### Scenario: Cold do_get resolves rows with bounded open work

- **GIVEN** a BIG-backed table served over flight with work counters reset
- **WHEN** a cold `do_get` (`SELECT` returning known rows) is served
- **THEN** the returned rows match the query-semantics oracle
- **AND** the cold open performed 0 full `Index.db` parses (open work bounded by `Summary.db`).

#### Scenario: Warm do_get reuses the summary-only reader

- **GIVEN** the same table already held warm in the `WarmTableRegistry`
- **WHEN** the `do_get` is repeated over the unchanged generation set
- **THEN** the rows again match the query-semantics oracle
- **AND** the warm request performs 0 reader-opens and 0 full `Index.db` parses over the unchanged
  generations.

