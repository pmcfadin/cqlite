# extract-split-core — new capability (issue #4199)

`cqlite-core` SHALL expose `extract_partitions` and `split_sstable` as new, unconditional
(`pub mod extract_split`) entry points under `storage/write_engine/`, built on the existing
point-read merger, boundary-source walk, and decode-at-offset primitives established by `salvage`
(#4196) and `rebuild` (#4197) rather than a new engine. All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Reconciled extract matches `SELECT *` for the key, on both read paths

`extract_partitions` in its default (non-`--raw`) mode SHALL reconcile the selected partition(s)
across every input generation using `build_single_partition_merger` (no purge) and write ONE output
generation whose dump for each selected key is row-for-row and column-for-column identical to a live
`SELECT *` for that key.

#### Scenario: R1.1 single-generation extract matches SELECT *
- **Given** a committed `test_basic` table with one generation and a known live key
- **When** `extract_partitions` runs with `Selection::Key(key)` against that table dir
- **Then** the output generation's dump for `key` equals `SELECT *` for `key` run against the
  original table dir under both `CQLITE_READ_PATH=point` and `CQLITE_READ_PATH=full`, comparing
  columns in both directions per #3890 (no scan column absent from the extracted row, no extracted
  column the scan lacks) — `cqlite-core/tests/issue_4199_extract_parity.rs`.

#### Scenario: R1.2 a key updated across two generations reconciles, not just concatenates
- **Given** a table flushed twice, where the second flush updates one column of a partition first
  written in the first flush (a fixture derivable from any committed table via two `compact`
  invocations, or an existing multi-generation committed table with a known such key)
- **When** `extract_partitions` selects that key across both generation paths
- **Then** the output has exactly ONE row set for the key, matching Cassandra's own compacted view
  of the same two SSTables (the existing byte-for-byte compaction parity harness), not the
  concatenation of both generations' independent values —
  `cqlite-core/tests/issue_4199_extract_parity.rs`.

#### Scenario: R1.3 token-range selection is (a, b] and matches every partition whose token qualifies
- **Given** a committed table with at least 3 partitions of known, distinct Murmur3 tokens `t1 < t2 < t3`
- **When** `Selection::TokenRange(t1, t2)` is extracted
- **Then** the output contains the partition at `t2` and excludes the partition at `t1`, matching
  Cassandra's left-exclusive/right-inclusive token-range convention —
  `cqlite-core/tests/issue_4199_extract_token_range.rs`.

### Requirement: R2 — `--raw` preserves each generation's own bytes verbatim

`extract_partitions` in `--raw` mode SHALL decode each selected partition at its offset from ONE
input generation at a time with no cross-generation reconciliation, writing one output generation
per input generation that held a match, and its dump for the selected key(s) SHALL equal that
INPUT generation's own Cassandra-written JSONL golden rows for the key, tombstones included.

#### Scenario: R2.1 raw output per generation equals that generation's own golden, not the reconciled view
- **Given** the R1.2 two-generation fixture, where generation 1's golden for the key differs from
  generation 2's golden (the update)
- **When** `extract_partitions` runs with `--raw` selecting that key
- **Then** two output generations are written; generation 1's output dump equals generation 1's OWN
  sstabledump JSONL golden for the key (the pre-update value) and generation 2's equals generation
  2's own golden (the post-update value) — neither equals the reconciled R1.2 result —
  `cqlite-core/tests/issue_4199_extract_raw.rs`.

#### Scenario: R2.2 raw output includes a tombstone the reconciled view would resolve away
- **Given** a committed `test_tomb` fixture where a later generation deletes a row a middle
  generation wrote
- **When** `--raw` extracts that key across all generations
- **Then** the tombstone-bearing generation's output dump includes the tombstone row/cell exactly as
  the source's own golden records it — `cqlite-core/tests/issue_4199_extract_raw.rs`.

### Requirement: R3 — an absent key is named, never silently dropped

`extract_partitions` SHALL report, by name, every requested key that resolves to zero partitions
across every input generation; it SHALL NOT return a vacuous success for a request naming a key
that was never present.

#### Scenario: R3.1 keys-file with some keys absent from every generation
- **Given** a committed table and a key set mixing 3 live keys with 2 synthetic keys guaranteed
  absent from every generation
- **When** `extract_partitions` runs with `Selection::KeySet` over all 5
- **Then** the report's `not_found` names exactly the 2 absent keys, the output holds exactly the 3
  live keys' partitions dump-equal to their sources, and the CLI (R-CLI-3 below) exits 3 —
  `cqlite-core/tests/issue_4199_extract_not_found.rs`.

### Requirement: R4 — split partitions the source exactly once, provably

`split_sstable` SHALL divide ONE input generation into N (or byte-bounded) output generations such
that the union of every part's rows equals the source's rows exactly once each, every part's token
range is disjoint from and strictly less than the next part's, and each part's own `Statistics` are
recomputed from only that part's written rows.

#### Scenario: R4.1 union of parts equals source, `--parts N`
- **Given** a committed table restaged into one generation via `compact`, with a known partition
  count `T`
- **When** `split_sstable` runs with `Parts(4)`
- **Then** exactly 4 output generations exist, the sum of their partition counts equals `T`, the
  union of their per-partition dumps equals the staged source's dump with no duplicate and no
  missing partition, and `min_token(part[i+1]) > max_token(part[i])` for every adjacent pair —
  `cqlite-core/tests/issue_4199_split_parts.rs`.

#### Scenario: R4.2 union of parts equals source, `--max-bytes`
- **Given** the same staged source
- **When** `split_sstable` runs with `MaxBytes(B)` for a `B` smaller than the source's total Data.db
  size
- **Then** every part's `bytes` is `<= B` plus at most one partition's width, the union/disjoint/
  ascending assertions from R4.1 hold, and the LAST part absorbs any remainder —
  `cqlite-core/tests/issue_4199_split_max_bytes.rs`.

#### Scenario: R4.3 every part passes `verify --mode full` and reads back its own row count
- **Given** the R4.1 output parts
- **When** `cqlite verify --mode full` runs on each part directory and each is read back
- **Then** every part reports zero findings and its read-back row count equals its manifest's `rows`
  — `cqlite-core/tests/issue_4199_split_parts.rs`.

#### Scenario: R4.4 per-part Statistics describe only that part
- **Given** the R4.1 output parts
- **When** each part's `Statistics.db` is read
- **Then** its min/max timestamp and partition/row counts match a fold over ONLY that part's own
  written rows, not the source's whole-table Statistics —
  `cqlite-core/tests/issue_4199_split_statistics.rs`.

### Requirement: R5 — neither verb ever modifies its input

`extract_partitions` and `split_sstable` SHALL NOT write, truncate, or otherwise alter any byte of
any input SSTable component, on any code path including a refusal.

#### Scenario: R5.1 sha256 of every input component is unchanged
- **Given** a committed table directory with a recorded sha256 of every component
- **When** `extract_partitions` (reconciled and `--raw`) and `split_sstable` each run to completion,
  and separately each is made to REFUSE (an absent boundary source, a decode failure injected via
  `corrupt_byte_fixture.rs`)
- **Then** every input component's sha256 is unchanged across every run —
  `cqlite-core/tests/issue_4199_extract_split_input_immutability.rs`.

### Requirement: R6 — a partition that cannot be read cleanly refuses the whole run

`extract_partitions` and `split_sstable` SHALL refuse the entire run (no output published under
`--out`) when a decode failure or chunk-CRC failure occurs on a partition either verb has
selected/enumerated — unlike `salvage`'s skip-and-record policy, neither verb emits a partial or
lossy output.

#### Scenario: R6.1 extract refuses on a decode failure in the requested partition
- **Given** `corrupt_byte_fixture.rs`'s `ClusteringTextLiteral` mutation targeted at a specific
  partition, and `extract_partitions` requesting exactly that partition
- **Then** the run refuses, names the generation and data offset the mutation targeted, points at
  `salvage`/`rebuild` as the remedy, and writes nothing under `--out` —
  `cqlite-core/tests/issue_4199_extract_refuses_on_decode_failure.rs`.

#### Scenario: R6.2 split refuses whole-run on a decode failure anywhere in the source
- **Given** the same corrupted fixture as the single input generation to `split_sstable`
- **Then** the run refuses (no parts published, including any parts that would have decoded cleanly)
  and names the offending generation/offset — `cqlite-core/tests/issue_4199_split_refuses_on_decode_failure.rs`.
