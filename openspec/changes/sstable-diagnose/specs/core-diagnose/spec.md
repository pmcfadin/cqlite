# core-diagnose — new capability (sstable-diagnose, issue #4204)

`cqlite-core` SHALL provide a read-only `diagnose` module that reports authoritative,
provenance-tagged performance metadata for a table's generations, matching `sstablemetadata`'s cheap
and `-s` scan tiers, and explicitly deferring to `cqlite tombstones` (#4200) and `verify --mode
audit` (#4195) for the questions those verbs own. All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — Every reported number states its source; nothing unmeasured renders as zero

`diagnose` SHALL tag every reported value with its source (`statistics | index | scan`), and SHALL
render a value as `unmeasured: <cause>` rather than `0` or a silent default whenever its source
component is absent, older-format, or otherwise not authoritatively available (issue #1653's honest
`Option` fields, and the #4159 class generally).

#### Scenario: R1.1 no bare zero for an absent source
- **Given** a generation whose Statistics.db layout does not carry `max_timestamp` (an issue #1653
  case already `None` in `TimestampStatistics`)
- **When** `diagnose_table` runs
- **Then** that field renders `{"value": null, "source": "unmeasured", "cause": "..."}`, never `0`
  or a bare `null` with no cause
  (`cqlite-core/tests/issue_4204_diagnose_provenance.rs`).

### Requirement: R2 — Cheap-tier fields match Cassandra's own `StatsMetadata` formulas and `sstablemetadata`'s printed values

The default (non-`--deep`) tier SHALL read only Statistics.db/Index.db/Summary.db/
CompressionInfo.db/TOC.txt (never Data.db), and every field it reports SHALL match
`sstablemetadata`'s own printed value for the identical file, including
`getEstimatedDroppableTombstoneRatio`'s exact formula (`design.md` D3) and the fully-expired-at-`now`
prediction (`write_engine::merge::fully_expired::is_fully_expired`, reused read-only).

#### Scenario: R2.1 cheap-tier fields match the `sstablemetadata` oracle
- **Given** a committed generation and its captured `sstablemetadata` text output (pinned
  `cassandra-5.0.8` tool version, recorded in the oracle file)
- **When** `cqlite diagnose <table-dir>` runs without `--deep`
- **Then** min/max timestamp, min/max local-deletion-time, compression ratio, repaired-at,
  pending-repair, and estimated-droppable-tombstones each equal the captured `sstablemetadata`
  value, and Data.db is never opened (asserted via a read-counter)
  (`cqlite-core/tests/issue_4204_diagnose_cheap_tier.rs`).

#### Scenario: R2.2 fully-expired-at-now prediction
- **Given** a generation whose `max_deletion_time` is provably below a chosen `gcBefore`
- **When** `diagnose_table` runs with `--now` implying that `gcBefore`
- **Then** `fully_expired_at_now: true`, matching `is_fully_expired`'s own existing test oracle
  (`cqlite-core/tests/issue_4204_diagnose_cheap_tier.rs`).

### Requirement: R3 — `--deep` adds one bounded full scan, never a write, never a second traversal per metric

`--deep` SHALL perform exactly one additional streaming scan per table (existing full-scan
primitive, one partition resident at a time), producing partition-size and clustering-width
histograms and `--top N` rankings in that SAME pass, and SHALL NOT write any output.

#### Scenario: R3.1 histograms and top-N match independently-derived golden values
- **Given** `test_wide_rows` and its Cassandra-written JSONL goldens
- **When** `cqlite diagnose <table-dir> --deep --top 5` runs
- **Then** the partition-size histogram, clustering-width histogram, and top-5 largest partitions
  each match values the TEST computes independently from the golden — never from `diagnose`'s own
  output compared to itself
  (`cqlite-core/tests/issue_4204_diagnose_deep_scan.rs`).

#### Scenario: R3.2 top-N tombstone-heaviest is the RAW per-generation count, not #4200's reconciled report
- **Given** `test_tomb/*`
- **When** `cqlite diagnose <table-dir> --deep --top 5` runs
- **Then** `top_tombstone_heaviest_partitions` matches a flat per-partition marker count computed
  independently by the test from the golden (partition/range/row/cell tombstone markers summed, NO
  `gcBefore` filtering, NO cross-generation reconciliation) — matching `sstablemetadata -s`'s
  `mostTombstones` definition exactly (`design.md` D2), and the report's own field description names
  `cqlite tombstones` (#4200) as the authoritative, reconciled alternative
  (`cqlite-core/tests/issue_4204_diagnose_deep_scan.rs`).

#### Scenario: R3.3 bounded memory, no write
- **Given** `test_wide_rows` under the memory-budget dhat lane
- **When** `--deep` runs
- **Then** peak memory stays within the existing full-scan bound (no new unbounded buffering), and
  no file is created or modified anywhere under `<table-dir>` or any other path
  (`cqlite-core/tests/issue_4204_diagnose_deep_scan.rs`).

### Requirement: R4 — Generation overlap per token range is metadata-only

`diagnose` SHALL compute, from each generation's own min/max partition token alone (no Data.db
read), how many generations' token spans intersect each of a fixed `K` equal-width buckets covering
the table's token union.

#### Scenario: R4.1 token overlap buckets match independently-computed intersection counts
- **Given** a multi-generation table with distinct, committed first/last keys per generation
- **When** `diagnose_table` runs
- **Then** `token_overlap.counts` equals the TEST's own independently-computed per-bucket
  intersection count, and no component beyond each generation's reader-exposed `first_key`/
  `last_key` is read for this field
  (`cqlite-core/tests/issue_4204_diagnose_token_overlap.rs`).

### Requirement: R5 — `diagnose` never validates consistency and never reads Data.db in the cheap tier

`diagnose` SHALL trust Statistics.db's declared values in its cheap tier without cross-checking them
against a scan (that is `verify --mode audit`'s job, #4195), and SHALL NOT open Data.db at all
unless `--deep` is given.

#### Scenario: R5.1 cheap tier never opens Data.db
- **Given** any committed generation
- **When** `cqlite diagnose <table-dir>` runs without `--deep`
- **Then** Data.db is never opened (read-counter assertion), and the report contains no
  consistency verdict of any kind — only reported values and their sources
  (`cqlite-core/tests/issue_4204_diagnose_cheap_tier.rs`).
