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

## 2026-06-17 — Phase 3: read from Sidecar snapshot directory

- **What:** `DirSource::resolve` now honors `ticket.snapshot`: `Some(name)` resolves to
  `<table-dir>/snapshots/<name>/` (Cassandra's frozen hardlink set, created by Sidecar
  `PUT .../snapshots/:name`); `None`/empty reads the live data dir. Refactored resolution into
  `table_base_dir` + snapshot join. Service `do_get` passes `ticket.snapshot.as_deref()`.
- **Why:** reading a snapshot (not live files) is the consistency guarantee from PLAN §2 — avoids the
  shifting-file-set / issue-#591 SIGBUS risk while Cassandra compacts underneath.
- **Tests:** `make_snapshot` testutil hardlinks all components into `snapshots/<name>/`;
  `reads_from_snapshot_directory` (produces correct rows from the snapshot) +
  `resolve_builds_snapshot_path` (path construction, live fallback). 43 total, clippy clean.
- **Next:** Phase 3 quick review → commit → Phase 4 (Java Trino connector skeleton + Sidecar discovery).

## 2026-06-17 — Phase 4: Trino connector skeleton + Sidecar discovery (Java 25)

- **What:** New `trino-connector/` Gradle project (Java 25, Trino SPI 481, Arrow Flight Java 18.1).
  - **Toolchain:** Gradle wrapper 9.1.0 + foojay-resolver 1.0.0 auto-provisions JDK 25, so the build
    is independent of the host JDK (host has 21). (foojay 0.9.0 was incompatible with Gradle 9.1 —
    bumped to 1.0.0.)
  - **Sidecar client** (`sidecar/`): `SidecarClient` over `java.net.http` + Jackson, with `parse*`
    statics for unit-testing. Models (records, ignore-unknown for version drift): RingResponse,
    TokenRangeReplicasResponse (ReplicaInfo with start/end token longs + replicasByDatacenter), SchemaResponse.
  - **ArrowTypeMapper:** Arrow `Field` → Trino `Type`. KEY DESIGN: the connector maps the cqlite-flight
    server's Arrow schema (from GetSchema) → Trino types, instead of re-parsing CQL DDL in Java — CQL
    parsing stays solely in the Rust core. Handles scalars, uuid extension → UuidType, list → ArrayType.
  - **Trino plumbing (compiles vs SPI 481):** Plugin (ServiceLoader-registered), ConnectorFactory
    (`cqlite_flight`), Connector (read-only, single tx handle, shutdown no-op), Config (sidecar-uri /
    flight-port / local-datacenter), TableHandle (carries keyspace+table+ddl), ColumnHandle, minimal Metadata.
- **Tests:** 9 green — sidecar JSON parsing (ring/token-ranges/schema/unknown-fields/malformed) + Arrow→Trino
  mapping (scalars/uuid-extension/list). `./gradlew test` builds clean on the provisioned JDK 25.
- **Deferred:** listSchemaNames/listTables enumeration, getColumnHandles via GetSchema→Arrow (needs the
  Flight client from Phase 6), split manager (Phase 5), page source (Phase 6). Connector review team will
  run once it's functional end-to-end (post Phase 6) — reviewing a non-querying skeleton in isolation is low value.
- **Next:** Phase 5 — SplitManager: token-range-replicas → one split per range pinned to a single replica.

## 2026-06-17 — Phase 5: splits (token range → single replica)

- **What:** `CqliteFlightSplitManager.getSplits` calls Sidecar `token-range-replicas` and emits one
  `CqliteFlightSplit` per range, each pinned to exactly ONE replica → a row (on RF replicas) is read
  once cluster-wide. Split carries keyspace/table/ddl + replica host + flight port + (start,end]+wraparound.
  Replica selection (`pickReplica`, static/pure): prefer local DC, else any DC, deterministic
  (lexicographically-smallest address). Wired `Connector.getSplitManager`.
- **Tests:** 4 — one-split-per-range + single-replica pinning, local-DC preference, wraparound detection
  (start>end), skip ranges with no replica. 13 total in the connector. `./gradlew test` green on JDK 25.
- **Next:** Phase 6 — page source (Flight DoGet → Arrow → Trino Page), GetSchema-driven column metadata,
  TupleDomain→ticket predicate/projection pushdown, and the docker-compose E2E.

## 2026-06-17 — Phase 6 (part 1): docker-compose E2E topology

- **What:** Stood up the E2E infrastructure (validated, not yet run live).
  - `docker/docker-compose.yml`: custom bridge `scylla_rust_driver_public` (172.42.0.0/16);
    `cassandra:5.0` bound to its network IP `172.42.0.2` (not 127.0.0.1, per request);
    `ghcr.io/apache/cassandra-sidecar:latest` via `network_mode: service:cassandra` → SAME IP as
    Cassandra (per request); `cqlite-flight` co-located (shares the IP) reading the SSTable volume,
    serving Flight on :8815; `trinodb/trino:481` with the connector plugin mounted.
  - `Dockerfile.flight` (multi-stage Rust build of cqlite-flight), `sidecar.yaml` (co-located single
    node, JMX on 7199 via LOCAL_JMX=no), `trino/catalog/cqlite.properties`, Gradle `installPlugin`
    task (assembles jar + runtime deps into build/plugin/cqlite_flight for Trino's isolated classloader).
  - `trino-connector/README.md` with run steps + a status table.
- **Verified:** `docker compose config` OK; `./gradlew installPlugin` assembles the plugin dir with all
  deps (arrow/jackson/connector). Did NOT run the live cluster (heavy image pulls + multi-container
  bring-up; better as focused interactive validation).
- **Remaining (Phase 6, part 2 — the functional connector):** Metadata column resolution
  (getTableHandle via Sidecar DDL; getColumnHandles via the server's GetSchema → Arrow →
  ArrowTypeMapper); FlightClient wrapper; PageSource (DoGet → Arrow VectorSchemaRoot → Trino Page,
  per-type block building); ConstraintTranslator (TupleDomain + required columns → FlightTicket JSON
  predicates + projection); wire getPageSourceProvider. Then run the live E2E: load data into
  Cassandra, snapshot via Sidecar, query through Trino, assert results + verify pushdown reduces bytes.
- **Session checkpoint:** Phases 1–5 complete & committed (Rust server end-to-end + connector discovery/
  types/splits). Phase 6 infra committed. The functional page source + live E2E is the remaining work.

## 2026-06-18 — Phase 6 COMPLETE: functional connector + live E2E passing

- **What:** Finished the functional connector and ran the live docker-compose E2E successfully.
  - Connector code (committed 9f349654): CqliteFlightClient (Flight Java GetSchema/DoGet), Metadata
    (getTableHandle via Sidecar DDL + CreateTableExtractor; getColumnHandles via GetSchema→Arrow→
    ArrowTypeMapper), ArrowToTrino (VectorSchemaRoot→Page), FlightTicketJson, PageSource(+Provider)
    using Trino 481 SourcePage. uuid→VARCHAR to dodge Trino UUID byte-order.
  - **E2E debugging (the live integration surfaced several real issues, all fixed):**
    1. `.dockerignore` — build context was 12GB (target/) and filled the Docker VM; excluded artifacts.
    2. compose `LOCAL_JMX=no` broke nodetool healthcheck + Sidecar JMX → removed (default 127.0.0.1:7199).
    3. Sidecar config mount path is `/conf/sidecar.yaml` (not /etc/...).
    4. Sidecar instance `host` must equal the address clients use (172.42.0.2) or it 421s.
    5. Sidecar needs `driver_parameters.contact_points` for its CQL session (was 503 without it).
    6. Sidecar `ring` returns a bare JSON array (not {entries:[...]}) → fixed parseRing.
    7. token-range-replicas returns replicas as `ip:storage_port` → strip port for the flight host (hostOnly).
    8. Trino JVM needs `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow off-heap → mounted jvm.config.
  - Sidecar shares Cassandra's IP via `network_mode: service:cassandra` (verified: NetworkMode=container:<cassandra>).
- **E2E results** (`analytics.events`: id int pk, name text, score int, active boolean; 5 rows, flushed):
  - `SELECT *` → 5 rows correct. Projection `id,name` → correct. `WHERE score > 25` → 3 rows.
  - Aggregates `count/sum/avg` → 5 / 150 / 30.0. `WHERE active = true` → 3 rows. All correct.
- **Verified:** connector `./gradlew test` green (20 tests); Rust `cargo test -p cqlite-flight` 43;
  live query path Trino→connector→Sidecar→splits→cqlite-flight DoGet→merge→Arrow→Trino works end-to-end.
- **Deferred (noted, not blocking):** predicate pushdown into the ticket (Trino currently post-filters;
  projection IS pushed); Cassandra default 16 vnodes → 16 splits each reading the SSTable (works,
  inefficient — a future optimization); listSchemaNames enumeration.
- **Next:** commit E2E fixes; run connector code-review team; address findings; final commit.

## 2026-06-18 — Phase 6 code-review team + fixes (final)

- **What:** 2 reviewers (correctness/resources, tests/design). Both independently flagged the same
  top issue; fixed the high-value findings and re-validated the live E2E.
- **CRITICAL fixed — mapper↔page-source type drift.** `ArrowTypeMapper` mapped TIMESTAMP/DECIMAL/LIST/
  FixedSizeBinary-VARBINARY that `ArrowToTrino` couldn't materialize → would plan fine then crash mid-scan
  (UnsupportedOperationException/ClassCastException). Fix: implemented TIMESTAMP_TZ (pack epoch millis,
  UTC) and VARBINARY-from-FixedSizeBinary + LargeUtf8 in the converter; the mapper now REJECTS complex
  types (collections/decimal/udt) at planning with a clear message so the two stay in lockstep. New
  round-trip test covers timestamp/date/real/binary/uuid; mapper test asserts complex-type rejection.
- **Resource safety (B2):** `openStream` closes the FlightClient if `getStream` throws; `getNextSourcePage`
  closes the handle on any exception (Trino doesn't guarantee close() on the throw path).
- **Error masking (W1):** `SidecarException` now carries the HTTP status; `getTableHandle` returns null
  only on a genuine 404, rethrowing real Sidecar failures instead of reporting them as "table not found".
- **Cleanup:** deleted dead `ArrowTypeMapper.toRow/bySignature`; refreshed the stale README status table.
- **E2E re-validated** with a diverse-type table `analytics.typed (id uuid PK, label text, amount int,
  created timestamp, active boolean)`: `SELECT` returns uuid as `11111111-...` and timestamp as
  `2024-01-01 00:00:00.000 UTC` correctly; events table still returns count=5. Connector tests green.
- **Deferred (documented, not blocking):** predicate pushdown into the ticket (projection IS pushed;
  Trino post-filters predicates — safe); per-stream child allocators + getMemoryUsage accounting (W3/W4);
  TrinoException error taxonomy (W2); scripted/asserting E2E harness; complex CQL types.
- **PHASE 6 COMPLETE. All 6 phases done.** Stack: cqlite-flight (Rust) + cqlite-trino (Java 25) +
  docker-compose (Cassandra + Sidecar sharing IP + cqlite-flight + Trino on 172.42.0.0/16).

## 2026-06-18 — Docs, container, CI, and automated E2E

- **What:** README + container + CI/CD + load-test + scripted integration test.
  - `cqlite-flight/README.md`: architecture diagram, gRPC surface, the Flight ticket JSON contract,
    CLI usage, pyarrow.flight client example, limitations.
  - **Container:** canonical `cqlite-flight/Dockerfile` (build context = repo root); compose now points
    at it; removed the duplicate `trino-connector/docker/Dockerfile.flight`.
  - **Load testing:** `cassandra-easy-stress` service behind a `loadtest` compose profile
    (`ghcr.io/apache/cassandra-easy-stress:latest`); validated with a 10k-op KeyValue run (0 errors).
  - **Scripted E2E:** `trino-connector/docker/e2e-test.sh` — clean→build plugin→`up --build`→load→flush→
    assert through the connector (counts, projection, aggregate, uuid, timestamp) + SSTable-semantics
    (memtable invisible until flush). Reproducible; runs locally and in CI. Added a Sidecar CQL-session
    readiness gate (first queries raced the warmup). **All 8 assertions PASS locally.**
  - **CI (3 workflows, YAML-validated):**
    - `flight-ci.yml` — fmt/clippy/test cqlite-flight, upload release binary artifact, build & push the
      container image to `ghcr.io/<owner>/cqlite-flight` (main/tags, not PRs).
    - `trino-connector-ci.yml` — gradle test (JDK 21 runs Gradle 9.1; foojay provisions JDK 25), assemble
      + upload the plugin artifact, upload test reports.
    - `flight-trino-e2e.yml` — runs `e2e-test.sh` (both sides, full docker-compose integration) on PR/main.
- **Artifacts published:** flight binary + GHCR container image (Rust side); Trino plugin dir (Java side).
- **Next:** commit; push.

## 2026-07-13 — do_get admission control (issue #2420, WS4)

- **What:** Added an application-level admission ceiling on `do_get`. An owned
  `tokio::sync::Semaphore` with `K` permits (`--max-concurrent-scans`, env
  `CQLITE_MAX_CONCURRENT_SCANS`, default **64**) gates entry to the merge: a
  request acquires a permit BEFORE any SSTable is opened or any batch produced,
  holds it (RAII, moved into the response stream) for the scan's lifetime, and
  releases it on completion/disconnect/cancel. On saturation a request waits up to
  `--admission-wait-timeout-ms` (env `CQLITE_ADMISSION_WAIT_TIMEOUT_MS`, default
  30000); if no permit frees it is shed with gRPC **`UNAVAILABLE`** (never
  `RESOURCE_EXHAUSTED`) BEFORE any batch. A coarse tonic `max_concurrent_streams`
  (≥ `max(K·4, 1024)`) backstops the HTTP/2 accept loop — the Semaphore, not the
  transport cap, is the real throttle. New `cqlite.flight.admission.*` instruments
  (limit/in_use/waiting/rejected_total/wait_seconds), distinct from
  `cqlite.rpc.in_flight`.
- **Overload contract:** short bursts under the wait budget are absorbed
  transparently (no client-visible error, no connector change); sustained overload
  sheds to another replica via the connector's #2241 `ReplicaFailoverStream`
  (retry-safe: the reject provably precedes the first batch, and failover triggers
  ONLY on `UNAVAILABLE`); only when EVERY replica saturates does the query fail
  loudly. Admission is transparent to results — byte-identical rows/order/schema/
  batch boundaries.
- **Default `K` sizing:** from the binding constraints, NOT `num_cpus`: blocking
  pool 512 ÷ ~2 threads/scan ≈ 256 ceiling; fd ~1024 ÷ M SSTables. 64 sits well
  below both. Conservative pending WS1-ramp (WS8) validation before the default is
  locked.
- **Files:** `cqlite-flight/src/admission.rs` (+ `admission_tests.rs`),
  `service.rs` (acquire before setup; permit into the response stream),
  `main.rs` (CLI/env knobs + `max_concurrent_streams`), `lib.rs`, `Cargo.toml`
  (dev `tokio` `test-util`); `cqlite-core/.../observability/catalog.rs` (5
  instruments).
- **Verified:** 11 deterministic tests (paused Tokio clock, injected barriers —
  no wall-clock) covering all 6 spec requirements; the excess-do_get gate reds when
  the acquire-before-setup wiring is removed (setup `resolves` counter jumps).
- **Next:** WS1-ramp validation of the default `K` (WS8); record on #2420.

## 2026-07-13 — admission-control roborev fixes (rounds 1696–1699)

- **What (roborev-1696):** Fast-pathed `Semaphore::try_acquire_owned()` first so
  an UNCONTENDED acquire never touches the `waiting` gauge or records a
  permit-wait histogram sample (was transiently over-reporting queue depth on
  every admit, even instant ones). The slow, contended (`NoPermits`) path is
  unchanged — it still bumps `waiting` and records a genuine wait sample.
- **What (roborev-1697):** `Admission::new` now clamps `max_concurrent_scans`
  symmetrically to `[1, Semaphore::MAX_PERMITS]` (was floored at 1 only) —
  `Semaphore::new` PANICS above `MAX_PERMITS`, so an absurd
  `--max-concurrent-scans`/env value must be capped, never crash startup. Logs a
  `warn` when a clamp actually changes the requested value.
- **What (roborev-1698 → refined by 1699):** `do_get`'s pre-permit validation is
  now MINIMAL and filesystem-free: `validate_do_get_ticket` parses ONLY the
  ticket bytes (`FlightTicket::from_bytes`). Producer/schema construction (CQL
  DDL parse, predicate/projection/aggregation-spec lowering — expensive,
  attacker-influenced work) moved INTO `do_get_resolve`, run AFTER admission
  alongside the filesystem resolve (both inside one `spawn_blocking`, exactly as
  pre-#2420). A syntactically malformed ticket still fails fast with its own
  status (never `UNAVAILABLE`, never waits, never consumes a permit); a
  syntactically VALID ticket for a bogus/nonexistent table now also does not
  reach producer construction until a permit is admitted.
- **What (roborev-1699, `new()` semantics):** `CqliteFlightService::new()` no
  longer reads the environment or applies a default admission ceiling — it uses
  `Admission::unconstrained()` (`Semaphore::MAX_PERMITS`, `Duration::MAX` wait),
  restoring this constructor's exact pre-#2420 behavior for library callers.
  `with_admission(data_dir, batch_size, admission)` is the explicit opt-in the
  `cqlite-flight` SERVER BINARY (`main`) uses to wire a real, CLI/env-configured
  `K` — the server still gets admission-by-default in deployment even though the
  library constructor stays unconstrained.
- **Tests:** 16 deterministic admission tests (was 11), incl. two new
  roborev-pinned scenarios: `req1_uncontended_acquire_never_touches_waiting_gauge`
  / `req1_contended_acquire_path_unchanged` (1696),
  `req4_absurd_configured_k_clamps_instead_of_panicking` (1697),
  `req_malformed_ticket_bypasses_admission_entirely` /
  `req_valid_ticket_for_bogus_table_does_not_reach_producer_construction_before_admission`
  (1698/1699). Re-verified the red-on-main proof after each restructure by
  moving the acquire back past resolve — the affected tests correctly red.
- **Files:** `cqlite-flight/src/admission.rs`, `admission_tests.rs`,
  `service.rs` (`new`/`with_admission`, `validate_do_get_ticket`/
  `do_get_resolve` split).

## 2026-07-13 — admission-control roborev round 5 (job 1700)

- **What (finding 1, admission is a first-class phase):** Added a new
  `admission` phase to the `do_get` phase set (`admission` → `resolve` →
  `merge_setup` → `stream`, was 3 phases, now 4). `PhaseTimer::start` now opens
  `admission` (was `resolve`) and the timer is constructed BEFORE
  `Admission::acquire()`, transitioning to `resolve` only once a permit is
  granted. Previously admission wait was invisible in the
  `cqlite.rpc.phase.duration`/`cqlite.rpc.phase.active` breakdown (folded into
  neither phase, since the timer didn't exist yet) even though
  `cqlite.rpc.duration` already counted it — field triage localizes latency FROM
  the phase breakdown (that's how #2398 was diagnosed), so queue time is now a
  first-class, directly observable phase. Updated the catalog doc comments
  (`RPC_PHASE`, `RPC_PHASE_DURATION`, `RPC_PHASE_ACTIVE`) to register the new
  label.
- **What (finding 2, gauge-overlap fix, same class as roborev-1696):**
  `Admission::acquire`'s slow (contended) path now drops the `WaitGuard`
  immediately when the timed acquire future resolves — BEFORE recording the
  wait sample or constructing the `AdmissionPermit` (which bumps `in_use`) — so
  an admitted/rejected request is never simultaneously counted both `waiting`
  AND `in_use`.
- **Tests:** 18 deterministic admission tests (was 16). New:
  `req_admission_wait_is_visible_as_its_own_phase` /
  `req_admission_phase_opens_even_on_an_uncontended_admit` (finding 1, via a new
  `pub(crate) #[cfg(test)] obs::phase_active_level_for` isolating one
  `(method, phase)` counter — feature-independent, no OTel needed); extended
  `req1_contended_acquire_path_unchanged` with an explicit no-overlap assertion
  (finding 2). Updated two pre-existing `obs.rs` unit tests
  (`phase_active_counter_reflects_concurrent_overlap_not_a_flag`,
  `phase_active_counter_moves_on_transition`) that hardcoded `PHASE_RESOLVE` as
  the assumed starting phase — now `PHASE_ADMISSION`. Re-verified red-on-main:
  moving the acquire back past `do_get_resolve` now reds 6 tests (up from 5 —
  the new admission-phase pin also reds).
- **Files:** `cqlite-flight/src/obs.rs` (phase set, `PhaseTimer::start`,
  `phase_active_level_for`, 2 updated unit tests), `service.rs` (timer starts
  before `acquire`), `admission.rs` (gauge-overlap fix), `admission_tests.rs`
  (2 new tests + 1 extended), `cqlite-core/.../observability/catalog.rs`
  (doc updates for the 4-phase set).
- **Post-commit self-caught flakiness fix (same round):** the first cut of the
  two finding-1 phase-visibility tests asserted EXACT deltas on
  `phase_active_level_for("do_get", ...)` — a PROCESS-WIDE counter keyed only by
  `(method, phase)`, shared with every OTHER concurrently-running test that
  drives a real `do_get` (this crate's test suite runs thread-parallel, not
  process-isolated, for `cargo test -p cqlite-flight --lib`). Standalone re-runs
  flaked (`cargo test --lib admission` failed ~1/6 runs on the "resolve
  untouched"/"vacated to exact baseline" assertions). Fixed by splitting into
  (a) `req_admission_phase_opens_before_resolve` — a fully deterministic
  `PhaseTimer` mechanics test against a synthetic, otherwise-unused method slot
  (`"handshake"`, never driven by any test), proving the exact admission→resolve
  ordering with zero cross-test interference, and (b)
  `req_admission_wait_is_visible_as_its_own_phase` — wiring evidence on the REAL
  `do_get` path, loosened to a robust `>= 1` lower bound (always true while our
  own request is genuinely parked, immune to concurrent noise in either
  direction). Verified stable across 5 repeated runs of both the admission
  filter and the full 254-test lib suite; red-on-main re-confirmed after the
  robustness rewrite (still 6 tests red).

## 2026-07-13 — admission-control roborev round 6 (job 1701)

- **What:** The round-5 `admission` phase addition was correct but escaped the
  lite-gate blast radius for `cqlite-flight/tests/metrics_capture_test.rs`
  (feature-gated behind `observability-testing`, not in the default scoped-test
  set), which still hardcoded the closed 3-phase set `{resolve, merge_setup,
  stream}` in its phase-value assertion. Fixed:
  - `metrics_capture_test.rs`: the closed-set match now asserts exactly
    `{admission, resolve, merge_setup, stream}` (still a CLOSED set — a future
    5th phase must update this assertion deliberately, not drift past it via a
    loosened check). Added an `admission`-tagged phase-sample assertion
    alongside the existing `merge_setup` one, proving even an UNCONTENDED
    `do_get` (this fixture's default `Admission::unconstrained()`) records one
    admission-phase OTel sample — the actual emitted histogram series, not just
    the feature-independent atomic this round's earlier tests pinned.
  - `issue_2370_gauge_readback_test.rs`: updated a stale prose comment
    (`resolve→merge_setup→stream`) referencing the old 3-phase transition
    window to include `admission` — the assertion logic itself (`phase_active_level`
    sums ALL phases) was already phase-count-agnostic and needed no functional
    change.
  - Swept the whole workspace (`rg merge_setup`) for other 3-phase assumptions —
    none found; `obs.rs`'s own `phase_slot`/`phase_index` fallbacks were already
    derived generically from `RPC_PHASES[0]`/`.len()` in round 5, not hardcoded.
- **Verified:** `cargo test -p cqlite-flight --features observability-testing
  --test metrics_capture_test` PASSES directly (was the test that escaped this
  round's lite blast radius); `cargo test -p cqlite-flight --lib admission`
  (18/18) and the full 254-test lib suite both still pass; clippy clean incl.
  `--features observability-testing`.
- **Files:** `cqlite-flight/tests/metrics_capture_test.rs`,
  `cqlite-flight/tests/issue_2370_gauge_readback_test.rs` (comment only).

## 2026-07-13 — admission-control roborev round 7 (job 1702)

- **What (finding 1, mechanical, gauge-vs-availability ordering):**
  `AdmissionPermit`'s `_permit: OwnedSemaphorePermit` field is now
  `permit: Option<OwnedSemaphorePermit>`. `Drop::drop` explicitly `take()`s and
  drops the permit BEFORE decrementing the `in_use` gauge — Rust drops a
  struct's fields AFTER `Drop::drop` returns, so the bare-field version
  released real semaphore capacity to a waiter strictly AFTER the gauge already
  said "one fewer in use," a transient window where the gauge undercounted
  relative to what a waiter could observe. Gauge and availability now move in
  the same order.
- **What (finding 2, ADJUDICATED — real regression, fixed with option (b)):**
  Verified against `origin/main` (`c2a86299`): pre-#2420, `PhaseTimer::start`
  opened `resolve` BEFORE `do_get_setup` even ran, and `do_get_setup`'s
  `FlightTicket::from_bytes(...)?` failure propagated through `do_get_inner`'s
  `Err(status) => return Err(...)` — the still-open `resolve`-phase timer then
  dropped at the function return, recording a `resolve`-tagged
  `cqlite.rpc.phase.duration` sample. So `main` DID give phase visibility for a
  malformed ticket (a `resolve` sample), and round-6's `validate_do_get_ticket`
  (run before the `PhaseTimer` even existed) silently dropped that to ZERO
  phase samples — a real regression, not a non-issue. Fixed per option (b):
  added a dedicated `validate` phase (`validate` → `admission` → `resolve` →
  `merge_setup` → `stream`, 5-phase CLOSED set), giving the pre-existing
  behavior a MORE PRECISE label instead of restoring the imprecise `resolve`
  one. `PhaseTimer::start` now opens `validate`; `do_get_inner` constructs the
  timer before `validate_do_get_ticket` and transitions to `admission` only
  once the ticket parses.
- **Tests:** `req_validate_then_admission_phase_chain` (replaces
  `req_admission_phase_opens_before_resolve`) — deterministic mechanics test on
  the dedicated `"handshake"` slot proving the full `validate → admission →
  resolve` sequence AND that a malformed-ticket-shaped drop (never
  transitioning) still records a `validate` sample. New Stage 5 in
  `metrics_capture_test.rs` (same test fn, sequenced after the happy path via
  `mc.reset()` — a second `#[test]` fn would risk cross-test OTel contamination
  per the capture harness's "single serial test" doc invariant): a real
  malformed `do_get` records EXACTLY ONE phase sample, tagged `validate` (never
  zero, never admission/resolve/merge_setup/stream). Updated the two obs.rs
  mechanics tests + the 5-value closed sets in `metrics_capture_test.rs` and
  the `catalog.rs` doc comments.
- **Verified:** `cargo test -p cqlite-flight --features observability-testing
  --test metrics_capture_test` and the admission target (18/18) both pass,
  stable across 3 repeated runs each; full 254-test lib suite stable across 3
  runs; clippy clean (incl. `--features observability-testing`); red-on-main
  re-confirmed (still 6 tests red when the acquire moves back past
  `do_get_resolve`).
- **Files:** `cqlite-flight/src/admission.rs` (permit drop ordering),
  `obs.rs` (5-phase set, 2 updated unit tests), `service.rs` (timer starts
  before ticket parse), `admission_tests.rs`, `tests/metrics_capture_test.rs`
  (Stage 5 + closed-set update), `cqlite-core/.../observability/catalog.rs`
  (doc updates for the 5-phase set).
