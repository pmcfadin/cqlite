# Issue #1045 — Spark connector research packet (2026-07-04)

Status: research complete; architecture **A (Spark DSv2 on the existing Flight data plane)** owner-approved
2026-07-04. Consistency posture + epic filing pending owner decision (see "Decisions").

Four research passes feed this packet: (1) repo map of the existing Trino/Flight connector, (2) Spark
DSv2 API surface, (3) Spark↔Flight prior art, (4) Apache Cassandra Analytics + Sidecar API state.

## 1. Approved architecture (A)

A thin **Spark DataSourceV2 read connector** that reuses the entire existing data plane: Cassandra
Sidecar as control plane (ring / `token-range-replicas` / schema DDL), the co-located `cqlite-flight`
server per Cassandra node as data plane (node-local SSTable reads, KWayMerger read-time reconciliation,
Arrow batches over Flight `DoGet`), and the JSON `FlightTicket` wire contract unchanged.

The Spark connector reimplements only the engine adapter layer, mirroring `trino-connector/`:

| Trino SPI | Spark DSv2 equivalent |
|---|---|
| `CqliteFlightSplitManager.buildSplits()` | `Batch.planInputPartitions()` — one `InputPartition` per token range, pinned to one replica |
| `CqliteFlightPageSource` (DoGet → Page) | `PartitionReaderFactory.createColumnarReader()` → `PartitionReader<ColumnarBatch>` (DoGet → `VectorSchemaRoot` → `ArrowColumnVector`) |
| `PredicateTreeTranslator` | `SupportsPushDownV2Filters` → same predicate tree in the ticket |
| `applyProjection` | `SupportsPushDownRequiredColumns` |
| Sidecar client | shared verbatim (already Spark-agnostic by design, per flight-trino JOURNAL) |

## 2. Findings by lane

### 2.1 Repo map (what exists, what's reusable)

- `cqlite-flight/` (~7.4k LOC Rust): `service.rs` (FlightService), `producer.rs` (MergeProducer/DirSource),
  `ticket.rs` (JSON ticket + predicate tree + token range + snapshot name), `filter.rs`, `agg.rs`
  (aggregation pushdown exists server-side), `stats.rs`, `pathsafe.rs` (#1430 guards).
- `trino-connector/` (~3.7k LOC Java, package `in.mcfad.cqlite.flight`): the engine-neutral 80% to
  extract = `sidecar/SidecarClient.java` + `SidecarModels.java`, `buildSplits()` range→replica pinning
  (prefer local DC, deterministic lexicographic tiebreak), `FlightTicketJson.java`.
- Correctness is two-layer: connector pins range→replica; Rust server re-filters rows by
  `DecoratedKey.token ∈ (start, end]` as backstop.
- **Gap 1 — snapshot lifecycle designed but unwired**: ticket carries optional `snapshot`, producer
  resolves snapshot dirs, but the connector passes `Optional.empty()` and reads live files
  (`CqliteFlightPageSourceProvider.java:69`). Freshness/torn-window gap tracked under #1477.
- **Gap 2 — core-internals coupling**: `cqlite-flight` reaches `cqlite_core::storage::write_engine::KWayMerger`,
  `SummaryReader`, `DecoratedKey`, `VersionGates` directly — the #1934 Phase 1 porting list. Does NOT
  block a Spark connector (it talks Flight, never touches core).
- Key docs: `docs/flight-trino/PLAN.md` (ticket contract, phase 5 = snapshot lifecycle spec),
  `docs/plans/2026-06-17-cassandra-fast-analytics-arrow-flight-design.md` (frames vs Cassandra Analytics).

### 2.2 Spark DSv2 surface (no architecture-changers)

- **Target Spark 3.5.x (extended LTS → Nov 2027) + 4.0/4.1.** DSv2 read interfaces stable across both;
  Spark 4 breakage (Scala 2.13-only, JDK 17+, Arrow 18, ANSI) is user-facing, not interface-facing.
- **Write the connector in Java, single Maven module** (both reference connectors are ~92% Java) —
  avoids the Scala cross-build matrix. Artifact per Spark major: `cqlite-flight-spark3.5_2.12` etc.
- **Top engineering risk = Arrow Java version alignment**, not the API. `ArrowColumnVector` binds to
  Spark's bundled unshaded Arrow (~15.x on 3.5, 18.x on 4.0). Rule: `arrow-vector`/`arrow-memory-core`
  = `provided`; bring only `arrow-flight-core` pinned to match; shade gRPC/Netty/protobuf, NEVER Arrow.
  Allocator lifecycle: off-heap refcounted buffers must be closed in `PartitionReader.close()`; budget
  `spark.executor.memoryOverhead`.
- **Columnar path**: `supportColumnarReads()=true`, DoGet stream → `VectorSchemaRoot` →
  `ArrowColumnVector` → `ColumnarBatch`. All-or-nothing per scan (fine — all partitions are Flight).
- **Pushdown phase 1**: `SupportsPushDownRequiredColumns` + `SupportsPushDownV2Filters` (return
  unhandled predicates honestly — over-claiming silently drops rows). Phase 2 optional:
  `SupportsPushDownAggregates` (server `agg.rs` already supports it), `SupportsPushDownLimit`.
  Skip: legacy `SupportsPushDownFilters`, `TopN`, `Offset`.
- **Locality**: `InputPartition.preferredLocations()` = replica host (DataStax-connector pattern);
  best-effort within `spark.locality.wait`. Correctness never depends on it — every partition carries
  a routable Flight endpoint and works from any executor.
- **Effort**: ~1.5–3k LOC, single engineer. Novel code = split generation + replica pinning (extracted
  from Trino anyway); Spark plumbing is well-trodden.

### 2.3 Prior art (verdict: write our own; copy patterns, don't fork)

- `rymurr/flight-spark-source` — Apache-2.0 prototype, dead since 2023-07, pre-modern DSv2. Reference
  for VectorSchemaRoot→ColumnarBatch plumbing only.
- `qwshen/spark-flight-connector` — dormant since 2023-04, Dremio-shaped, no custom tickets / host
  pinning. Reference for pushdown + type mapping only.
- Apache Arrow ships no official Spark source (none planned); Dremio retired theirs.
- **Flight SQL / JDBC route ruled out**: Spark's JDBC source partitions only by numeric column ranges,
  cannot express host-pinned token-range splits or opaque tickets; the JDBC wrapper collapses Flight's
  multi-endpoint model to one connection. Plain Flight with custom JSON tickets stays necessary.
- Doris/StarRocks ship the same architecture (tablet ≈ token range, Arrow transport, ~10× over JDBC) —
  independent validation of the shape.
- Open design point: raw Flight's idiomatic pattern is server-side `GetFlightInfo` returning one
  `FlightEndpoint{ticket, locations}` per split. Today split intelligence is client-side (Sidecar-driven,
  in the Trino connector). See Decision D2.

### 2.4 Cassandra Analytics + Sidecar (the big finding)

- `apache/cassandra-analytics` is **active** (0.4.0, 2026-06-11), Cassandra 3.0/4.0/5.0 bridges,
  Spark 3.x/4.x. It is the canonical implementation of the sidecar-snapshot/ring technique (CEP-28).
- **Snapshot lifecycle to adopt** (confirmed from Sidecar source, `ApiEndpointsV1.java`):
  `PUT /api/v1/keyspaces/{ks}/tables/{table}/snapshots/{name}?ttl=<duration>` (TTL ≥1 min; snapshot
  self-deletes — leak-safety for crashed jobs, CASSANDRASC-85), `GET` lists components,
  `DELETE` clears, `GET .../components/{component}` streams (honors HTTP Range). Use the plural route
  form (singular `/keyspace/...` is deprecated). **Always create with `?ttl=`.**
- **Consistency semantics — Analytics does NOT read single-replica.** Default `LOCAL_QUORUM`: per range
  it opens `blockFor(rf, dc)` replicas (RF=3 → 2), async with retry-on-backup (`MultipleReplicas`),
  and merges all of them through Cassandra's `CompactionIterator` (LWW by cell timestamp + tombstone
  resolution). CL is a user knob; `ONE` is available but not default.
  **CQLite's Trino connector single-replica pinning is therefore CL.ONE semantics** — may miss the
  newest write, may resurrect data hidden by a tombstone on an un-read replica. A genuine correctness
  weakening vs the ASF reference, not a neutral choice. See Decision D1.
- **Borrow**: snapshot TTL; primary/backup async open + retry failover model; structure split assignment
  around `blockFor(rf, dc)` even if v1 ships single-replica, so quorum-merge can be added without redesign.
- **Avoid**: static-IP ring discovery (Analytics limitation — we already discover via Sidecar);
  undocumented single-replica reads.
- **CQLite's opening**: Analytics is JVM-bound (embeds Cassandra for `CompactionIterator`) and still
  lacks BTI/`da` support (CASSANALYTICS-27, in progress). CQLite reads BTI natively. The only non-JVM
  prior art (`datastax/sstable-to-arrow`) is abandoned. Native Rust reader + Arrow transport fills a
  real gap.

## 3. Decisions

- **D1 (OWNER — DECIDED 2026-07-04): ship CL.ONE, documented.** v1 reads each range from ONE replica,
  with the CL.ONE contract prominently documented (Spark docs + a docs fix for the shipped Trino
  connector, which has the same undocumented semantics today). Split-assignment API structured around
  `blockFor(rf, dc)` so cross-replica quorum-merge can be added later without redesign; a follow-up
  research issue covers quorum-merge (hard: cell-timestamp reconciliation across nodes' Arrow streams —
  today KWayMerger reconciles only node-locally).
- **D2 (design-time, lead default = client-side).** Split intelligence stays client-side in shared
  connector-commons (mirror Trino; zero Rust changes) vs server-side `GetFlightInfo` (Flight-idiomatic
  but couples the per-node server to Sidecar). Revisit at OpenSpec design for the epic.
- **D3 (lead defaults, owner may veto):** Java over Scala; Maven; Spark 3.5 + 4.x dual target;
  columnar-only reads; phase-1 pushdown = columns + V2 filters.
- **D4 (scope).** Read-only v1. Bulk write (Analytics writer equivalent) explicitly out of scope.
- **D5 (dependency).** Sidecar snapshot lifecycle (#1477 territory) is shared infrastructure both
  connectors want; recommend it as a named sibling issue (adopting the `?ttl=` pattern), with the Spark
  connector documenting the live-read freshness caveat until it lands.

## 4. Epic shape (FILED 2026-07-04)

#1045 retitled → "[EPIC] Spark connector — DSv2 read connector on the Flight data plane". Children
(all design-driven → OpenSpec at pickup, board Status=Backlog, P3):

1. **#1947 (S1)** connector-commons extraction — SidecarClient/Models, `TokenRangeSplitPlanner`
   (ex-buildSplits pinning), FlightTicketJson out of `trino-connector/` into a shared Gradle module.
   AC: trino-connector green against it, zero behavior change. Coordination note vs #1910 embedded.
2. **#1948 (S2)** Spark DSv2 read connector — Java, `TableProvider`/`Scan`/`Batch`/`InputPartition`/
   columnar `PartitionReader`; schema from Sidecar DDL; `preferredLocations` = replica host.
   AC: value-parity vs JSONL goldens on a loopback harness. Depends on #1947.
3. **#1949 (S3)** pushdown phase 1 — RequiredColumns + V2Filters → ticket predicate tree, honest
   unhandled-return. AC: pushdown-on vs -off result identity + plan assertions. Depends on #1948.
4. **#1950 (S4)** E2E on 3-node RF=3 docker-compose + website docs incl. CL.ONE + freshness contracts.
   Closes the epic's wiring-evidence. Depends on #1948/#1949.

Related (pre-existing #941 children cover two planned siblings — no dups filed):
- **#1906** (df-provider A2) IS the Sidecar snapshot lifecycle work (adopt `?ttl=` per §2.4).
- **#1911** (df-provider A7) carries the Trino-side CL.ONE contract documentation (research finding
  cross-posted there 2026-07-04).
- **#1951** quorum-merge research (decision packet on lifting CL.ONE toward blockFor-replica reads).

Placement: engine-side connector tier per the #1934 split decision. Priority P3 unless owner reschedules.
