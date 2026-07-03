## ADDED Requirements

### Requirement: The perf gate benches the real point-read path, not a LIMIT-1 scan proxy
The performance regression gate SHALL measure a benchmark that drives the real point-read path — a
fully-constrained `WHERE pk = ?` lookup through the public `Database` query API, which engages the
partition-targeted access path (bloom/BTI presence prune → single-candidate seek → chunk decode). The
benchmark SHALL NOT be a `SELECT … LIMIT 1` scan. The benchmark SHALL prove at setup that the real
targeted path engaged by asserting the returned `QueryResult.access_path` is a targeted path
(`PartitionLookup`), never a `FallbackFullScan`.

#### Scenario: The gated point-read bench drives the targeted access path
- **WHEN** the `read/get_partition` bench setup runs a `SELECT * … WHERE id = <uuid-literal>` against the fixture through `Database::execute`
- **THEN** `QueryResult.access_path` is `Some(PartitionLookup)` (a targeted path), not `FullScan` or `FallbackFullScan`
- **AND** the query returns at least one row

#### Scenario: An accidental full-scan fallback fails the bench loudly
- **WHEN** the point query would fall back to a full scan (targeted path did not engage)
- **THEN** the bench setup panics rather than silently measuring the scan path
- **AND** no `read/get_partition_*` measurement is produced from the fallback

### Requirement: Both BIG (multi-chunk) and BTI point-read variants are gated
The gate SHALL track a BIG-format point-read bench over a fixture whose Data.db spans more than one
compression chunk, and a BTI-format point-read bench, each with a median-regression failure threshold of
at least 10%. The old `read/point_lookup` LIMIT-1 proxy SHALL NOT be present in the gate configuration.

#### Scenario: perf-gate.json tracks the real point-read benches
- **WHEN** `cqlite-core/benches/perf-gate.json` is read
- **THEN** it contains a BIG point-read bench id and a BTI point-read bench id, each with `threshold_pct >= 10`
- **AND** it does NOT contain a `read/point_lookup` entry

#### Scenario: The BIG fixture spans multiple compression chunks
- **WHEN** the BIG fixture's `CompressionInfo.db` is parsed via `CompressionInfo::parse`
- **THEN** the parsed chunk count (`chunk_offsets.len()`) is greater than 1
- **AND** a committed test asserts this, so the multi-chunk guarantee cannot silently erode

### Requirement: The point-read benches never silently measure an empty dataset
A dataset-dependent point-read bench SHALL error loudly (panic at setup) when its fixture is present but
yields zero rows or a non-targeted access path, and SHALL be skipped (not registered, so the gate reports
SKIP and does not fail) only when its fixture table directory is entirely absent.

#### Scenario: Present-but-broken fixture panics
- **WHEN** the fixture table directory exists but the point query returns zero rows
- **THEN** the bench setup panics with an actionable message (never records a 0-row measurement)

#### Scenario: Absent optional fixture skips without failing the gate
- **WHEN** an optional fixture (e.g. the BTI `test_da` table) is not present in the checkout
- **THEN** that bench variant is not registered and `check_perf_regression.py` reports it as SKIP without failing the gate

### Requirement: The gate demonstrably fails on a regressed point path
A slowdown on the real point-read path SHALL cause the gated point-read bench to fail the regression
check. This SHALL be demonstrated by a red-run (artificially slowing the point path, then showing
`check_perf_regression.py` reports the bench as a REGRESSION with a non-zero exit).

#### Scenario: A slowed point path reds the gate
- **WHEN** the point-read path is artificially slowed and the bench is re-measured against the fast baseline
- **THEN** `scripts/ci/check_perf_regression.py` flags the point-read bench as a REGRESSION and exits non-zero
