# CQLite → Arrow Flight → Trino: Implementation Plan

**Status:** Planning complete, implementation not started.
**Owner:** Jon Haddad
**Journal:** see `JOURNAL.md` in this directory — append an entry for every change so work is resumable.

---

## 1. Goal

Let Trino query Apache Cassandra data **directly from SSTables**, bypassing the
coordinator/read path. On read, CQLite performs an on-the-fly **compaction merge**
of a node's SSTables (originals untouched), applies **token-range** and
**predicate** filters server-side, and streams the result to Trino as **Arrow
Flight** record batches.

Two deliverables, both in this repo so the whole thing builds and tests together:

1. **`cqlite-flight/`** — Rust Arrow Flight server (a crate in the cargo workspace).
   Runs **co-located on every Cassandra node**, reads node-local SSTables, merges,
   filters, serves Arrow.
2. **`trino-connector/`** — Java 25 Trino plugin (Gradle). Uses Cassandra **Sidecar**
   for node + token-range discovery, builds splits, pulls Arrow Flight streams.
   Ships a `docker-compose` (Trino + Cassandra + Sidecar + cqlite-flight) for E2E tests.

---

## 2. Locked design decisions

| Concern | Decision |
|---|---|
| Compaction trigger | **Compact-on-read** — Flight `DoGet` drives a full k-way merge |
| Flight protocol | **Plain Arrow Flight** (`GetFlightInfo` + `DoGet`), not Flight SQL |
| Query scope | Per `keyspace.table` |
| Server topology | **Co-located on every C* node**; reads a node-local Sidecar **snapshot** |
| Cross-replica dedup | Connector pins each token range to **one** replica; **server filters output by token range** |
| Predicate pushdown | Connector passes predicates in the Flight ticket; **server evaluates each merged row** and emits only matches |
| Consistency | Read from a Sidecar-created snapshot, not live files (avoids issue #591 SIGBUS / shifting file set) |
| Server home | New `cqlite-flight` crate |
| Connector | Java 25, latest Trino SPI, Gradle |

### Why server-side token-range filtering is mandatory
Cassandra replicates each row to RF nodes. A co-located server's SSTables contain
every range the node *replicates*, not just ranges it "owns". Without per-range
filtering pinned to a single replica, Trino would see each row RF times. cqlite's
compaction reconciles only *within* a node; cross-replica dedup is the connector's
job (one range → one replica) backed by the server's range filter.

### Statistics semantics: two different de-replication mechanisms (issue #1336)
The **scan path** and the **optimizer-stats path** both have to undo RF over-counting,
but by different means, and they must not be confused:

- **Scan path — dedupe by token range.** One split per read-replica token range, pinned
  to exactly one replica; the server filters output to that `(start, end]` range, so each
  logical row is emitted once cluster-wide (above).
- **Stats path — de-replicate by uniform replica-count division.** `table_stats` has no
  token fields, so `CqliteFlightMetadata.getTableStatistics` sums the whole-table
  `live_rows` across the keyspace's DISTINCT scoped replica hosts (≈ RF × logical
  cardinality) and reports `ROW_COUNT = live_rows / R`. `R` is the **uniform per-token-range
  distinct scoped read-replica count** derived ONLY from the authoritative Sidecar
  `tokenRangeReplicas` response (never by parsing `replication = {...}` strategy strings —
  no-heuristics mandate #28), under the same `localDatacenter` scoping the stats sum was
  collected under.

**Fail-closed conditions** (report `TableStatistics.empty()`, today's behavior — statistics
failures never fail query planning): non-uniform per-range replica counts (topology
mid-transition), a zero-replica range, `table_stats` `complete=false` (undecodable
`Statistics.db` / unreachable replica / #1327 count contradiction), or ANY Sidecar/Flight
error or timeout. The result is memoized per `(keyspace, table)` so one planning pass
fetches `tokenRangeReplicas` and `table_stats` at most once each per table.

**Caveats (documented, accepted):** under replica divergence (repair lag, differing
compaction of expired data) the quotient is the mean across replicas — a well-behaved
`Estimate`, not an exact count. **Transient replication** (Cassandra experimental) is
invisible in Sidecar's replica lists, so such keyspaces may over-divide; documented out of
scope. The AUTOMATIC GROUP-BY pushdown gate (`estimateGroupRatio`) is UNCHANGED — it uses
the RF-invariant `partition_count / live_rows` ratio where replica over-count cancels.

---

## 3. Grounded API facts (from spike — quote these when implementing)

All paths under `cqlite-core/src/`.

- **Merge engine** `storage/write_engine/merge.rs`
  - `KWayMerger::new(input_paths: Vec<PathBuf>, schema: &TableSchema) -> Result<Self>` (:645)
    — takes SSTable **file paths** (newest→oldest) + schema; builds its own readers.
  - `KWayMerger::step(&mut self) -> Result<MergeStep>` (:729), pull-based, sync
    (bridges to async I/O internally).
  - `MergeStep::Partition { key: DecoratedKey, rows: Vec<MergeEntry> }` | `Complete`.
  - `MergeEntry { run_index, key: DecoratedKey, clustering_key: Option<ClusteringKey>, timestamp: i64, row_data: RowData }` (:65).
  - `RowData::Live { cells: Vec<CellData> }` | `Tombstone { deletion_time, local_deletion_time }` (:156).
  - `CellData { column: String, value: Value, timestamp: i64, ttl: Option<u32> }` (:174).
- **Token** `storage/write_engine/mutation.rs:419`
  - `DecoratedKey { token: i64, key: Vec<u8> }` — token is a public `i64`, already
    computed and stored. **Filtering = compare this number; no Murmur3 needed.**
- **SSTable enumeration** `storage/sstable_data_manager.rs`
  - `discover_table_sstables(&self, table_path, table_name) -> Result<TableInfo>` (:398),
    `TableInfo.sstable_files: Vec<SSTableFileInfo>` with `path: PathBuf`.
- **CQL values** `types.rs:28` — `Value` enum (29 variants: scalars, List/Set/Map/Tuple/Udt/Frozen/...).
- **CQL→Arrow** `export/parquet.rs`
  - `cql_type_to_arrow_field(name, &CqlType, nullable) -> Option<Field>` (:329) maps types recursively.
  - `build_schema(&columns)` + `convert_to_arrays(&columns, rows)` produce an Arrow schema/arrays.
  - Loosely coupled (takes rows + column metadata + `CqlType`), extractable to a shared module.
- **Predicate eval** `query/select_executor.rs:156`
  - `fn evaluate_predicates(row: &QueryRow, predicates: &[SSTablePredicate]) -> Result<bool>`
  - `SSTableFilterOp`: `Equal, In, Range, Gt, Gte, Lt, Lte, Prefix, BloomFilter` with numeric coercion.

---

## 4. Repository layout

```
cqlite/
├── Cargo.toml                  # add "cqlite-flight" to workspace members
├── cqlite-flight/              # Rust Arrow Flight server  (NEW)
│   ├── Cargo.toml              # tonic, arrow-flight, arrow, tokio, cqlite-core
│   └── src/
│       ├── main.rs             # CLI: --data-dir --listen --schema-mode ...
│       ├── service.rs          # FlightService impl (get_flight_info/get_schema/do_get/list_flights)
│       ├── ticket.rs           # Ticket JSON (keyspace, table, snapshot, ddl, token range, predicates, columns)
│       ├── merge_stream.rs     # KWayMerger → filtered rows → Arrow RecordBatch stream
│       └── filter.rs           # token-range + predicate evaluation over merged rows
├── trino-connector/            # Java 25 Trino plugin  (NEW)
│   ├── build.gradle(.kts)      # Java 25 toolchain, latest Trino SPI, arrow-flight (Java)
│   ├── src/main/java/.../
│   │   ├── CqliteFlightPlugin.java        # Plugin (ServiceLoader)
│   │   ├── CqliteFlightConnectorFactory.java
│   │   ├── CqliteFlightMetadata.java      # list schemas/tables via Sidecar /schema
│   │   ├── CqliteFlightSplitManager.java  # token-range-replicas → splits (1 range → 1 replica)
│   │   ├── CqliteFlightPageSource.java     # Flight DoGet → Arrow → Trino Page
│   │   ├── sidecar/...                      # Sidecar client (mirror analytics-sidecar-client)
│   │   └── ConstraintTranslator.java       # Trino TupleDomain → ticket predicates
│   ├── src/main/resources/META-INF/services/io.trino.spi.Plugin
│   └── docker/
│       ├── docker-compose.yml  # cassandra:5 + sidecar + cqlite-flight + trino
│       └── catalog/cqlite.properties
└── docs/flight-trino/
    ├── PLAN.md                 # this file
    └── JOURNAL.md              # append-only change log
```

`cqlite-flight` joins the cargo workspace. `trino-connector` is a standalone Gradle
build (not part of the cargo workspace).

---

## 5. Flight ticket contract (server ⇄ connector)

`DoGet` ticket is JSON (UTF-8 bytes):

```json
{
  "keyspace": "ks",
  "table": "tbl",
  "snapshot": "cqlite-<uuid>",        // Sidecar snapshot name; null = live data dir (dev only)
  "ddl": "CREATE TABLE ks.tbl (...)", // parsed → TableSchema for KWayMerger
  "token_start": -3074457345618258602, // exclusive
  "token_end":   3074457345618258602,  // inclusive  (range is (start, end])
  "wraparound": false,                  // true when start > end (min-token range)
  "columns": ["pk","ck","v"],          // projection pushdown (optional; null = all)
  "predicates": [
    {"column": "v", "op": "Gt", "value": {"Integer": 10}}
  ]
}
```

- Token range semantics mirror Sidecar `token-range-replicas`: **`(start, end]`**.
  Filter keeps partitions where `token > start && token <= end`; the wraparound
  range (`start > end`) keeps `token > start || token <= end`.
- `GetFlightInfo` returns the Arrow schema (built from `ddl`) + a ticket per endpoint.

---

## 6. Phased plan

Each phase is independently committable. Update `JOURNAL.md` per change.

### Phase 0 — Plan + journal  ✅ (this document)

### Phase 1 — `cqlite-flight` server, full-table merge (no filtering)
- Scaffold crate; add to workspace.
- Extract CQL→Arrow conversion from `export/parquet.rs` into a reusable core module
  (feature-gated `arrow`) consumed by both Parquet export and Flight. **Fallback:**
  if extraction is invasive, vendor a self-contained converter in `cqlite-flight`
  and note it in the journal.
- `do_get`: parse ticket → `discover_table_sstables` → `KWayMerger::new(paths, schema)`
  → loop `step()` → reconstruct logical rows → batch into `RecordBatch` →
  stream via `FlightDataEncoderBuilder`. Run merge on `spawn_blocking`, feed a channel.
- `get_flight_info` / `get_schema` from `ddl`.
- **Validate:** `pyarrow.flight` client vs `test-data/datasets`; row counts vs CLI/JSONL goldens.

### Phase 2 — Server-side filtering
- **2a token-range:** drop partitions whose `DecoratedKey.token` is outside `(start, end]`
  (handle wraparound). Cheap, per-partition.
- **2b predicate pushdown:** reconstruct each logical row (column→`Value`), evaluate via
  reused `evaluate_predicates` / `SSTablePredicate`; emit only matches. Map ticket
  predicate JSON → `SSTablePredicate`.
- **2c projection (optional):** if `columns` set, emit only those Arrow columns.
- **Validate:** filtered streams vs unfiltered + post-filter in pyarrow.

### Phase 3 — Snapshot reading
- Read SSTables from a named snapshot dir (`<data>/<ks>/<tbl>-<id>/snapshots/<name>/`)
  instead of live data dir. Server resolves snapshot path from ticket `snapshot`.

### Phase 4 — Trino connector skeleton + Sidecar discovery
- Gradle, Java 25 toolchain, latest Trino SPI (pin exact version at impl time).
- `Plugin` + `ConnectorFactory` + `Metadata`: list schemas (keyspaces) / tables via
  Sidecar `GET /api/v1/cassandra/schema` (or per-keyspace `/schema`); derive Trino
  column types from CQL DDL.
- Sidecar client: reuse patterns from `cassandra-analytics/analytics-sidecar-client`
  (`RingRequest`/`RingResponse`, `TokenRangeReplicasRequest/Response`). mTLS/JWT config.

### Phase 5 — Splits (token range → single replica → Flight endpoint)
- `SplitManager`: call `GET /api/v1/keyspaces/:ks/token-range-replicas`; for each range
  pick exactly one replica; emit a split = `{token range, replica host, ks, tbl}`.
- Resolve replica host → `host:<flight_port>` (configured port convention).
- Connector triggers Sidecar snapshot (`PUT .../snapshots/:name`) before scan; ticket
  carries the snapshot name; connector deletes it after (`DELETE`).

### Phase 6 — Arrow → Trino Page + constraint pushdown + E2E
- `PageSource`: Flight `DoGet` (Java arrow-flight) → `VectorSchemaRoot` → Trino `Page`.
- `ConstraintTranslator`: Trino `TupleDomain` / `Constraint` → ticket `predicates`
  (+ projection from required columns). Implement `applyFilter` / `applyProjection`.
- `docker-compose` E2E: load data into Cassandra, run Trino SQL, assert results +
  assert predicate pushdown reduced bytes (compare with/without pushdown).

---

## 7. Key risks / open items

- **Arrow conversion extraction** (Phase 1) — main refactor risk; fallback documented above.
- **Logical row reconstruction from `MergeEntry`** — merge yields cells grouped by
  partition; need to assemble (partition+clustering)→row for predicate eval and Arrow.
  Confirm clustering-key grouping semantics from `merge.rs` during Phase 1.
- **Latest Trino + Java 25 compatibility** — pin exact Trino version supporting Java 25
  at Phase 4 start; Trino tracks newest JDK closely.
- **Java Arrow Flight client version** must match the Rust server's Arrow IPC version.
- **Snapshot lifecycle** — ensure connector cleans up snapshots even on query failure.
- **Counters / unsupported types** — cqlite does not merge counters; document as unsupported.

---

## 8. Build & test entry points (to fill in as built)

- Server: `cargo build -p cqlite-flight` / `cargo run -p cqlite-flight -- --data-dir ... --listen 0.0.0.0:8815`
- Connector: `cd trino-connector && ./gradlew build`
- E2E: `cd trino-connector/docker && docker compose up`
