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
