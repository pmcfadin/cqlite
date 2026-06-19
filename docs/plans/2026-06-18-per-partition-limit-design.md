# PER PARTITION LIMIT — Design

**Issue:** #757 (child of Epic #756 — Query engine completeness)
**Date:** 2026-06-18
**Status:** Approved design, pre-implementation

## Problem

`SELECT ... PER PARTITION LIMIT n` is parsed-around but unsupported.
`select_parser.rs:992` hardcodes `per_partition: false` with a TODO. The clause
must work end-to-end: lexer → grammar/AST → query plan → executor enforcement,
matching Cassandra semantics (cap rows emitted per partition key, applied before
the global `LIMIT`).

## Findings (verified against the code + corpus, 2026-06-18)

- **Live query path:** `select_parser.rs` → `select_ast.rs` →
  `select_optimizer.rs` (builds `ExecutionStep`s) → `select_executor.rs`
  (streaming `execute_streaming` + batch paths). `parser.rs` is the legacy M2
  path and is out of scope.
- **Lexer already handles multi-word keywords** (`GROUP BY`, `ORDER BY`) by
  reading ahead in `next_token` and emitting a single token. The same technique
  applies to the three-word `PER PARTITION LIMIT`.
- **Current AST cannot represent the feature.** `LimitClause { count,
  per_partition: bool }` conflates the two limits, but Cassandra permits both
  `PER PARTITION LIMIT n` *and* `LIMIT m` in one query. The `per_partition` bool
  is dead — always `false`, never read meaningfully.
- **The scan emits one row per clustering row, contiguous by partition.**
  Verified empirically: `SELECT sensor_id FROM test_timeseries.sensor_data
  LIMIT 30` returned 30 rows all sharing one `sensor_id`. The K-way merge in
  `storage/sstable/mod.rs::scan_stream` orders by `RowKey`, so a partition's
  rows remain contiguous in the merged stream even when split across SSTables.
- **Fixture reality:** `test_wide_rows` (named in the issue) is entirely
  1-row-per-partition and cannot exercise this feature. `test_timeseries`
  has genuine wide partitions: `sensor_data` (10 partitions × 172–220 rows),
  `stock_prices` (3 × 63–70), `tick_data` (24 × 2–13). Tests use these and
  document the deviation.

## Design

### 1. AST (`cqlite-core/src/query/select_ast.rs`)
- Add `per_partition_limit: Option<u64>` to `SelectStatement`.
- Simplify `LimitClause` to `{ count: u64 }` — remove the dead `per_partition`
  bool. Update all constructors/tests that set it.

### 2. Lexer (`cqlite-core/src/query/select_parser.rs`)
- Add `Token::PerPartitionLimit`.
- In `next_token`, when the identifier is `PER` (case-insensitive), consume the
  following `PARTITION` then `LIMIT` keywords and emit `Token::PerPartitionLimit`.
  Mirror the existing `expect_by_keyword` pattern used for `GROUP`/`ORDER`; a
  malformed sequence (`PER` not followed by `PARTITION LIMIT`) is a clear parse
  error.

### 3. Parser (`cqlite-core/src/query/select_parser.rs`)
- Parse the clause in CQL order: after `ORDER BY`, before `LIMIT`.
- `PER PARTITION LIMIT <n>` reads an integer into
  `statement.per_partition_limit`.
- **Validation:**
  - Reject `n < 1` (zero/negative) with a clear `cql_parse` error.
  - Reject `PER PARTITION LIMIT` appearing *after* `LIMIT` (token-order check)
    with a clear error.
- `parse_limit_clause` returns the simplified `LimitClause { count }`.

### 4. Query plan (`cqlite-core/src/query/select_optimizer.rs`)
- Add `ExecutionStep::PerPartitionLimit { count: u64 }`.
- Emit it before the `Limit` step (and before `Project`), so per-partition
  capping happens upstream of the global limit.

### 5. Executor (`cqlite-core/src/query/select_executor.rs`)
Enforce in **both** the streaming scan loop (`execute_streaming`) and the batch
scan path, for parity:
- Maintain a **partition signature** and a per-partition counter.
- The signature is the partition-key bytes decoded from the scan `key` via the
  canonical `storage::partition_key_codec::decode_partition_key_columns`
  (schema-driven; no heuristics; robust to projection dropping PK columns from
  the row). Reuse the partition-key prefix so the check is independent of
  clustering bytes in the key.
- On signature change → reset the counter. While `counter >= per_partition_count`
  for the current partition, skip the row (do not send, do not count toward
  OFFSET/LIMIT).
- Ordering: per-partition cap → OFFSET → global LIMIT, matching Cassandra
  (`PER PARTITION LIMIT` applies before `LIMIT`).
- `LIMIT 0` / per-partition `0` already rejected at parse time; the existing
  early-return for `limit_count == 0` is retained.

### 6. Tests
- **Parser unit tests** (`select_parser.rs` test module):
  - `PER PARTITION LIMIT 2` parses to `per_partition_limit: Some(2)`.
  - `PER PARTITION LIMIT 2 LIMIT 5` sets both fields.
  - Rejected: `PER PARTITION LIMIT 0`, negative, and `LIMIT 5 PER PARTITION
    LIMIT 2` (wrong order).
- **Executor parity tests** (integration, real SSTable data):
  - On `test_timeseries.sensor_data`: `PER PARTITION LIMIT k` yields at most `k`
    rows per `sensor_id`; assert grouped counts.
  - Combined `PER PARTITION LIMIT k LIMIT m`: global cap applies after the
    per-partition cap.
  - Baseline: a `stock_prices` (3 partitions) case for a small, exactly-countable
    assertion.
- `scripts/agent-gate.sh` passes; raw summary block pasted in the PR.

## Cassandra semantics reference
`PER PARTITION LIMIT n` caps the number of rows returned *per partition* before
the query-wide `LIMIT` is applied. Both clauses may appear together; order in
CQL is `... [ORDER BY ...] [PER PARTITION LIMIT n] [LIMIT m] [ALLOW FILTERING]`.

## Out of scope / documented limitations
- **Cross-SSTable duplicate-row reconciliation.** The scan merge does not dedup
  identical `(partition, clustering)` rows across generations; this is a
  pre-existing condition affecting all queries, not introduced here.
- **Legacy `parser.rs` (M2) path** is not modified.

## Affected files
- `cqlite-core/src/query/select_ast.rs` — AST fields
- `cqlite-core/src/query/select_parser.rs` — lexer token, grammar, validation,
  parser tests
- `cqlite-core/src/query/select_optimizer.rs` — `ExecutionStep::PerPartitionLimit`
- `cqlite-core/src/query/select_executor.rs` — enforcement (streaming + batch)
- Integration tests — `test_timeseries` fixtures
