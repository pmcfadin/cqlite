# Journal — CQLite → Arrow Flight → Trino

Append-only log so this task can be resumed by reading top-to-bottom. Newest entries
at the bottom. Each entry: date, what changed, why, files touched, how verified, next step.

Format:
```
## YYYY-MM-DD — <short title>
- What: ...
- Why: ...
- Files: ...
- Verified: ...
- Next: ...
```

---

## 2026-06-17 — Design + plan established (no code yet)

- **What:** Completed design through Q&A; wrote `PLAN.md`. No implementation started.
- **Decisions locked** (see PLAN §2): compact-on-read; plain Arrow Flight; per-table
  scope; server co-located on each C* node reading a Sidecar snapshot; per-token-range
  dedup pinned to one replica with **server-side token-range filtering**; **predicate
  pushdown** evaluated server-side; new `cqlite-flight` Rust crate + `trino-connector/`
  Java 25 Gradle project, both in this repo with a docker-compose for E2E.
- **Spike findings (read-only, grounded the plan):**
  - `KWayMerger::new(Vec<PathBuf>, &TableSchema)` at `cqlite-core/src/storage/write_engine/merge.rs:645`; pull-based `step()` → `MergeStep::Partition{key,rows}`.
  - `DecoratedKey { token: i64, key: Vec<u8> }` at `mutation.rs:419` — token is a stored
    `i64`. Range filtering is a plain integer compare; **no Murmur3 computation** (per user).
  - SSTable enumeration: `discover_table_sstables` → `TableInfo.sstable_files` (`sstable_data_manager.rs:398`).
  - `Value` enum (29 variants) at `types.rs:28`.
  - CQL→Arrow conversion in `export/parquet.rs` (`cql_type_to_arrow_field` :329, `build_schema`,
    `convert_to_arrays`) is loosely coupled → extractable to a shared module.
  - Reusable predicate evaluator `evaluate_predicates(&QueryRow, &[SSTablePredicate])` at
    `query/select_executor.rs:156`; ops `Equal/In/Range/Gt/Gte/Lt/Lte/Prefix` + numeric coercion.
  - Reference for Sidecar discovery: `../cassandra-analytics` `analytics-sidecar-client*`
    (`RingResponse`, `TokenRangeReplicasResponse`); standalone, not Spark-coupled.
    Sidecar endpoints: `/api/v1/cassandra/ring`, `/api/v1/keyspaces/:ks/token-range-replicas`,
    `/api/v1/cassandra/schema`, snapshot `PUT/GET/DELETE .../snapshots/:name`.
- **Files:** added `docs/flight-trino/PLAN.md`, `docs/flight-trino/JOURNAL.md`.
- **Verified:** N/A (planning only).
- **Next:** On go-ahead, begin **Phase 1** — scaffold `cqlite-flight` crate, attempt
  Arrow-conversion extraction from `export/parquet.rs`, implement `do_get` full-table
  merge → Arrow stream, validate with a `pyarrow.flight` client against `test-data/datasets`.
```

## 2026-06-17 — Phase 1 foundation: arrow feature split + crate scaffold + ticket (TDD)

- **What:** Landed the foundation for Phase 1. Phase 1 is NOT complete yet (merge→Arrow
  producer and the tonic FlightService still to do).
  1. **cqlite-core `arrow` feature split.** Extracted all CQL→Arrow conversion out of
     `export/parquet.rs` into new `export/arrow_convert.rs`, gated on a new `arrow`
     feature. `parquet = ["arrow", "dep:parquet"]` now. Public API:
     `cqlite_core::export::{build_arrow_schema, rows_to_record_batch, ArrowConvertError}`.
     This lets `cqlite-flight` reuse the converter WITHOUT pulling the `parquet` crate.
  2. **`cqlite-flight` crate** created and added to workspace members.
  3. **`ticket.rs`** (TDD): `FlightTicket` (keyspace, table, ddl, snapshot, token range,
     wraparound, columns projection, predicates) — pure serde, no core types, so the Java
     connector can produce it as plain JSON. Includes `token_in_range()` implementing the
     Cassandra `(start, end]` + wraparound semantics. 8 unit tests.
  4. Git remotes fixed: `origin` → git@github.com:rustyrazorblade/cqlite.git, old origin → `upstream`.
- **Why:** Arrow split is the highest-risk refactor (per PLAN §7) — doing it first, gated by
  the existing parquet tests, de-risks the rest. Ticket is pure/self-contained → ideal first TDD unit.
- **Files:** `Cargo.toml` (+member); `cqlite-core/Cargo.toml`, `cqlite-core/src/export/{mod.rs,parquet.rs}`,
  new `cqlite-core/src/export/arrow_convert.rs`; new `cqlite-flight/{Cargo.toml,src/lib.rs,src/ticket.rs}`.
- **Verified:**
  - `cargo test -p cqlite-core --features parquet` — green (1 pre-existing failure
    `debug_schema_extraction` needs binary SSTables absent from this env; unrelated).
  - `cargo build -p cqlite-core --no-default-features --features all-compression,state_machine,arrow` — green (arrow w/o parquet).
  - `cargo build -p cqlite-core` (default), `cargo build -p cqlite-cli` — green.
  - `cargo test -p cqlite-flight` — 8/8 green. `clippy -p cqlite-flight -D warnings` — clean.
  - **TDD note:** `token_in_range` test caught a real min-token edge case; confirmed the
    exclusive-start `(start,end]` behavior is correct (Murmur3 never emits i64::MIN), fixed the test.
- **Branch:** `feat/flight-trino-phase1`. Committed foundation (pre-review).
- **Next:** Phase 1 remainder — investigate partition/clustering key decoding into `Value`s,
  build merge→`QueryRow`→`RecordBatch` producer (TDD), then the tonic `FlightService`
  (do_get/get_flight_info/get_schema). Then run the code-review team on the whole Phase 1,
  fix findings, commit, before Phase 2.

## 2026-06-17 — Phase 1 complete: merge→Arrow producer + tonic FlightService (TDD)

- **What:** Phase 1 functionally complete — `cqlite-flight` now serves a table's
  compaction-merged rows over Arrow Flight.
  - **Key-decoding investigation:** `RowKey`/`DecoratedKey.key` is the PARTITION KEY
    only; clustering + regular columns arrive as decoded cells in the row's `Value::Map`
    (the merge sets `MergeEntry.clustering_key = None`, "deferred"). Read path proves this.
  - **`producer.rs`** (TDD): `MergeProducer` drives `KWayMerger`, reconstructs each row by
    rebuilding the `(RowKey, Value::Map)` pair and calling cqlite-core's `build_row_from_scan`
    (now `pub`) — so Flight output is identical to a `SELECT`. Row tombstones suppressed,
    cell tombstones → null. `schema_columns()` builds key-first `ColumnInfo` with authoritative
    `CqlType`. `SstableSource` trait + `DirSource` (DI for Phase 3 snapshot swap). Batches at
    configurable size. 6 tests build real SSTables in-process via `WriteEngine`+flush (no
    external data) and assert LWW resolution, tombstone suppression, batch splitting, ordering.
  - **`service.rs`** (TDD): tonic `FlightService` — `get_flight_info`/`get_schema` (Arrow
    schema from ticket DDL, no file access) and `do_get` (merge on `spawn_blocking` → 
    `FlightDataEncoderBuilder` stream; always emits schema even when empty). Other RPCs
    return `unimplemented`. 3 async tests incl. full do_get→decode round-trip verifying LWW.
  - **`main.rs`**: CLI (`--data-dir --listen --batch-size`), serves `FlightServiceServer`.
  - **`testutil.rs`** (cfg-test): shared in-process SSTable builders (DRY across producer/service).
  - cqlite-core: `build_row_from_scan` made `pub` (re-exported from `query`) for output parity.
- **Why:** Reusing `build_row_from_scan` guarantees Flight == SELECT output (the Trino
  correctness target). Merge on spawn_blocking keeps the gRPC reactor responsive.
- **Files:** new `cqlite-flight/src/{producer,service,main,testutil}.rs`; `lib.rs`,
  `Cargo.toml` (+tonic, arrow-flight, arrow[ipc], tokio, futures, clap, tracing); cqlite-core
  `query/{mod.rs,select_executor.rs}` (pub `build_row_from_scan`).
- **Verified:** `cargo test -p cqlite-flight` 17/17 green; `clippy -p cqlite-flight -D warnings` clean
  (added module `allow(result_large_err)` — tonic Status is the mandated trait error; rewrote
  merge loop as `while let`). cqlite-core default build green.
- **Known limitations (documented, not bugs):** wide-row/clustering correctness inherits the
  merge engine's current behavior (clustering_key deferred); collections/UDT/composite-PK not yet
  test-covered here (Phase 2+); token-range/predicate/projection filters are Phase 2; live-dir
  reads only (snapshot is Phase 3).
- **Next:** Run the Phase 1 code-review team (testing gaps, smells, SOLID); fix; commit; then Phase 2.

## 2026-06-17 — Phase 1 code-review team + fixes

- **What:** Ran 3 parallel review agents (correctness/merge semantics, test gaps, SOLID/design).
  Resolved findings in the cqlite-flight crate (this commit) + spawned a cqlite-core fix (separate).
- **CRITICAL found & confirmed:** wide-row/clustering tables collapse to one row per partition
  because the merge sets `MergeEntry.clustering_key = None`. Verified with a new test
  (`clustering_table_preserves_distinct_rows_in_a_partition`: got 1, expected 2). This is a real
  cqlite-core compaction defect (its own tests only covered non-clustering schemas). Per the
  "fix root cause, never disable" rule, dispatched a background sstable-developer fix to populate
  the clustering key from cells in the merge. The flight cross-check test is `#[ignore]` until it lands.
- **Fixes applied (cqlite-flight):**
  - Ticket wire-contract hardening: `#[non_exhaustive]` on `FlightTicket`/`Predicate`/`PredicateOp`,
    added `version` field (`TICKET_VERSION`) + `Default` impl — forward-compat for the Java connector.
  - `From<ProducerError>/From<TicketError> for Status`: real gRPC codes (invalid_argument / not_found /
    internal) instead of flattening everything to `internal`; messages preserved. Java connector can
    now branch on status code.
  - `DirSource::resolve` owns table-dir resolution (write-engine + Cassandra `<table>-<uuid>` layouts),
    deterministically picking the lexicographically-largest match; removed the duplicated `table_dir`
    from the service (DRY/SRP; sets up Phase 3 snapshot swap behind `SstableSource`).
  - Missing table dir → `not_found`; existing-but-empty table → schema-only stream. Removed the
    spurious empty `RecordBatch` (verified `FlightDataEncoderBuilder::with_schema` emits schema for
    an empty stream via a new test).
  - Tightened surface: `DirSource.dir` private, `schema_columns` → `pub(crate)`.
  - New tests: UUID round-trip + Arrow UUID extension metadata; null column → Arrow null;
    get_flight_info schema/endpoint; invalid-DDL → invalid_argument; missing-table → not_found code;
    empty-table → schema-only. 22 passing, 1 ignored (clustering, pending core fix). clippy clean.
- **Deferred/documented (not fixed):** TTL expiry not evaluated (reader doesn't surface TTL) — known
  limitation; PK/regular column name collision inherited from shared `build_row_from_scan` — low risk.
  Full-result-set buffered in memory (no streaming backpressure) — documented #1 perf item for later.
- **Next:** await background cqlite-core clustering fix → un-ignore + verify → commit → Phase 2.

## 2026-06-17 — Phase 1 CLOSED: wide-row merge fix landed + verified

- **What:** The cqlite-core clustering-key merge fix is complete and verified; Phase 1 is done.
- **Root cause (deeper than first thought):** two parts in `merge.rs` `SSTableRowIteratorAdapter::open`:
  (1) it called `iterate_all_partitions_for_compaction(None)` — no schema — so the reader rebuilt a
  schema from Statistics.db with GENERIC names (`"clustering_key"`) instead of the real CQL name (`ck`),
  putting clustering values under the wrong cell name; (2) every `MergeEntry` had `clustering_key: None`,
  so all rows of a partition collapsed in `reconcile_cluster`.
- **Fix:** `open` now takes `&TableSchema`, passes `Some(schema)` to the reader (correct column names),
  and a new `extract_clustering_key(row_data, schema)` builds the `ClusteringKey` from the decoded cells
  (clustering columns stay in the cells for read-back). `KWayMerger::new` threads the schema through.
  New core test `clustering_key_rows_survive_compaction` (compaction_integration.rs).
- **Verified:** `cargo test -p cqlite-flight` 23/23 (clustering cross-check now un-ignored & passing);
  `cargo test -p cqlite-core --features write-support --test compaction_integration` 7/7;
  `clippy -D warnings` on cqlite-flight + cqlite-core clean. All pre-existing compaction/issue_587/
  issue_591 tests still pass.
- **Impact:** cqlite's OWN compaction now correctly preserves wide-partition rows (was a latent defect
  uncovered by this work), and the Flight server serves clustering tables correctly.
- **Phase 1 deliverable:** `cqlite-flight` serves a `keyspace.table`'s compaction-merged rows over Arrow
  Flight, output matching SELECT, validated for int/text/uuid/null/tombstone/wide-row/LWW.
- **Next:** Phase 2 — server-side token-range + predicate + projection filtering in `do_get`.

## 2026-06-17 — Phase 2: server-side filtering (token range + predicates + projection)

- **What:** `do_get` now applies the ticket's filters during the merge.
  - cqlite-core: exposed `evaluate_predicates`, `SSTablePredicate`, `SSTableFilterOp` (pub +
    re-export) for reuse — same predicate semantics as SELECT.
  - `filter.rs` (new): `ScanSpec::from_ticket(ticket, schema)` translates ticket fields into a
    `TokenFilter` + `Vec<SSTablePredicate>` + projection. Typed JSON→`Value` conversion via the
    column's authoritative `CqlType` (int/bigint/float/double/bool/text/uuid/timestamp; `IN`
    expands a JSON array). `token_in_half_open_range` factored out of the ticket (shared, DRY).
  - `producer.rs`: `MergeProducer::with_spec(schema, batch_size, spec)`. Per-partition token filter
    (drops whole partitions outside `(start,end]` — cross-replica dedup), per-row predicate eval,
    projection restricts both the Arrow columns and `build_row_from_scan`.
  - `service.rs`: `build_producer(ticket)` builds the spec-aware producer for all RPCs (so the Arrow
    schema reflects projection); `From<FilterError> for Status` → invalid_argument; `ProducerError::Predicate`.
- **Tests:** filter.rs (8: token bounds, int/IN/uuid/clustering-col translation, unknown-column &
  type-mismatch rejection) + producer (token selectivity, predicate `>`, projection). 34 total, clippy clean.
- **Scope note:** predicate pushdown supports scalar columns; collections/complex types error as
  non-pushable (logged, not silently dropped). Token filtering uses the stored `DecoratedKey.token`
  (no Murmur3 computation), per the design.
- **Next:** Phase 2 code-review team → fix → commit → Phase 3 (snapshot dir reads).

## 2026-06-17 — Phase 2 code-review team + fixes

- **What:** 2 parallel reviewers (correctness/security, tests/design). Both independently flagged the
  same CRITICAL; resolved it plus several hardening items.
- **CRITICAL fixed — predicate on a projected-out column rejected ALL rows.** Projection was applied in
  `build_row_from_scan` BEFORE predicate eval, so the predicate's column was absent → `evaluate_predicates`
  rejected every row. Fix: `entry_to_row` now builds the FULL row (no projection); predicates evaluate on
  the full row; output projection is applied solely via `self.columns` during Arrow conversion. This also
  removed the projection-applied-twice duplication the design reviewer flagged. New test
  `predicate_on_projected_out_column_still_filters`.
- **Fixed:** integer operands now use `i32::try_from` (no silent `as i32` wrap → `BadOperand` on overflow);
  empty `IN ()` rejected; `null` operand rejected with a clear message. (Float operands already stored as
  f64 `Value::Float` — no f32 narrowing, contrary to one review note.)
- **Tests added:** multiple-AND predicates intersect; predicate value-identity (asserts WHICH rows survive,
  not just counts); service-level do_get predicate pushdown end-to-end; unknown predicate column → 
  invalid_argument; empty-IN / null-operand / int-overflow rejection. 41 total, clippy clean.
- **Documented decisions (not bugs):** projection may drop partition-key columns — SAFE here because
  cross-replica dedup is SPLIT-level (one token range → one replica), not row-value-level, so the server
  never emits duplicate rows regardless of projected columns. Timestamp predicate operand is epoch i64
  (Trino sends epoch); `wraparound:true` with a single bound is a client footgun (documented).
- **Next:** Phase 3 — read SSTables from a Sidecar snapshot directory.
