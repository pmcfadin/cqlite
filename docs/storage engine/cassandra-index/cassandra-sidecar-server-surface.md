# Cassandra Sidecar Server-Side HTTP Contract

**Scope:** Cassandra Sidecar API surface assumed by CQLite's Arrow Flight + Trino connector and Design B remote-sidecar range provider. Focuses on discovery, component access, and gaps needed for Q1 (freshness) and Q2 (engine feasibility).

**Status:** INDEX ONLY — no specification yet; apache/cassandra-sidecar repo must be pulled to confirm exact endpoints and behavior. This reflects what CQLite's client code currently calls and what Design B proposes but does NOT yet exist.

**Audience:** CQLite maintainers, engineers evaluating CQLite as an alternative/adjacent storage engine or OLAP path.

---

## Summary

The Cassandra Sidecar exposes HTTP REST endpoints for cluster metadata, per-keyspace topology, and schema. CQLite's `SidecarClient` (Java) currently uses **three** read-only endpoints; **Design B** proposes an additional **component range-read** endpoint for remote SSTable access. **None of these endpoints expose freshness control** (flush, snapshot creation, memtable stats) — a critical gap for Q1's sanctioned answer ("client asks Sidecar to flush + snapshot, then reads the fresh hardlink set").

---

## Key Classes & Endpoints (Current)

### Discovery & Topology

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `SidecarClient` | `trino-connector/src/main/java/in/mcfad/cqlite/flight/sidecar/SidecarClient.java:23` | HTTP client for Sidecar cluster discovery (ring, token ranges, schema). JSON parsing testable w/o live Sidecar. |
| `SidecarModels.RingEntry` | `SidecarModels.java:18` | One node's ring metadata: datacenter, address, port, rack, status, state, token, fqdn, hostId. |
| `SidecarModels.TokenRangeReplicasResponse` | `SidecarModels.java:55` | Write and read replicas per keyspace, grouped by datacenter. |
| `SidecarModels.SchemaResponse` | `SidecarModels.java:61` | Keyspace name + CQL DDL string. |

### HTTP Endpoints (READ-ONLY, CURRENT)

```
GET /api/v1/cassandra/ring
  └─ Returns: List<RingEntry> (bare JSON array)
  └─ Used by: Trino connector split generation, replica affinity
  └─ SidecarClient.ring() @ line 39

GET /api/v1/keyspaces/{keyspace}/token-range-replicas
  └─ Returns: { writeReplicas: [ReplicaInfo], readReplicas: [ReplicaInfo] }
  └─ ReplicaInfo: { start: "i64", end: "i64", replicasByDatacenter: {dc: [node]} }
  └─ Used by: Exact token-range partitioning, one replica per (start,end]
  └─ SidecarClient.tokenRangeReplicas(keyspace) @ line 44

GET /api/v1/keyspaces/{keyspace}/schema
  └─ Returns: { keyspace: string, schema: string (CQL DDL) }
  └─ Used by: Schema acquisition for Parquet type mapping
  └─ SidecarClient.schema(keyspace) @ line 50
```

All endpoints:
- Use HTTP `Accept: application/json` header
- Have 30-second timeout (line 57)
- Fail on non-2xx status (line 63)
- Ignore unknown JSON fields (Jackson `FAIL_ON_UNKNOWN_PROPERTIES = false` for forward compat, line 25)

---

## Design B: Proposed Component Range-Read Path

**Status:** Architecture doc only; NO IMPLEMENTATION YET. Depends on Cassandra Sidecar support for:

```
GET /api/v1/keyspaces/:ks/tables/:table/snapshots/:snapshot/components/:component
  Range: bytes=<start>-<end>
  └─ Must honor HTTP Range header for random-access streaming (NOT buffering whole component)
  └─ Returns: Byte range of SSTable component (Data.db, Index.db, Summary.db, CompressionInfo.db, Statistics.db)
  └─ Used by: CqliteSidecarRangeExec partition execution
  └─ Critical assumption: "Does Sidecar's component route honor HTTP Range efficiently?"
     (Unverified against apache/cassandra-sidecar ApiEndpointsV1; some paths are whole-file upload/download)
```

**Implied prerequisites (NOT YET EXPOSED):**
- `POST /api/v1/keyspaces/:ks/tables/:table/snapshots` → create snapshot, return snapshot name + epoch
- `GET /api/v1/keyspaces/:ks/tables/:table/snapshots/:snapshot` → list components + sizes + checksums
- `DELETE /api/v1/keyspaces/:ks/tables/:table/snapshots/:snapshot` → release snapshot lease

---

## Hard Couplings

### Cassandra Data Model Assumptions

1. **Ring as source of truth for topology** (`RingEntry.token` is a decimal Murmur3 i64 string).
   - Wraps at -(2^63) and 2^63-1; CQLite assumes exact range coverage with no gaps (fail if not).
   - No support for vnode reconfiguration mid-query.

2. **Token-range replicas are replicated on read** (`readReplicas` list).
   - CQLite picks one healthy replica per `(start, end]` range (exactly-once coverage).
   - Quorum/consistency is a caller concern; Sidecar returns canonical ring state.

3. **Schema via `DESCRIBE TABLE` DDL string**.
   - No-heuristics mandate requires authoritative schema; Sidecar provides CQL source.
   - Schema discovery is a pre-query operation; changes are not tracked or versioned by Sidecar itself.

### Network & Protocol

1. **HTTP/1.1 or HTTP/2 (unspecified in CQLite client)**. Java `HttpClient` will use best available.
2. **JSON only** — no protobuf, XML, or binary formats.
3. **10-second connection timeout, 30-second request timeout** (hard-coded; no user override).
4. **No authentication in current client** — assumes Sidecar is trusted or network-isolated.

### Cassandra Versioning

- Code references "Sidecar API v1" (`/api/v1/`).
- No version negotiation or compatibility layer; version mismatch is a caller error.
- Trunk has CEP-11 (pluggable Memtable API) and pluggable SSTableFormat API; 5.0.x lacks both.
  - **Implication:** Design B snapshots and flush control do not exist in 5.0; Trunk-only feature.

---

## Extension Points & Pluggability

### JSON Parsing Abstraction

- `SidecarClient.parseRing/parseTokenRangeReplicas/parseSchema` are static methods (line 79–96).
- Tests can call them directly with mock JSON strings; no HTTP round-trip required.
- Jackson `ObjectMapper` is instantiated once as `MAPPER` (line 24); all parsing reuses it.
- **Extension point:** Subclasses could override `read()` (line 98) to support different JSON libraries or custom validation.

### Response Field Ignorance

- `@JsonIgnoreProperties(ignoreUnknown = true)` on all model records (e.g., line 30, 38, 54, 60).
- Allows Sidecar to add fields without breaking old clients.
- Missing fields that are required will deserialize as `null` — caller must validate.

### HTTP Client Configuration

- Single `HttpClient` instance, shared across all requests (line 27).
- Constructor accepts `base` URI as a parameter (line 31).
- **Extension point:** Subclasses could inject custom `HttpClient` with proxy, TLS, auth, or retry policy.

---

## Q1 Relevance: Freshness & Memtable Access

**Q1:** When DataFusion or Trino reads a node through CQLite's Arrow Flight connector, the read sees only flushed SSTables. What must change in Cassandra so an analytical read reflects ALL node-local state — memtable contents + every SSTable?

### Current Gap

- **Sidecar exposes zero freshness-control endpoints.** No way to:
  - Trigger a flush (`POST .../memtables/{memtable}/flush`?)
  - Create a snapshot (`POST .../snapshots`)
  - Query memtable dirty state or last-flush time
  - Enforce a freshness boundary (e.g., "wait for writes < T to flush")

### Sanctioned Q1 Answer (NOT YET IMPLEMENTED)

> "Client asks Sidecar to flush + snapshot, then reads the fresh hardlink set."

**Implies:**
1. `POST /api/v1/keyspaces/:ks/tables/:table/memtables/flush` → force memtable flush, return generation ID of resulting SSTable(s)
2. `POST /api/v1/keyspaces/:ks/tables/:table/snapshots` → create snapshot over the flushed generation, return snapshot name + creation time
3. `GET .../snapshots/:snapshot/components` → list component files for that snapshot
4. `GET .../components/:component` with HTTP Range → stream component bytes (Design B)

**Currently missing:** Endpoints 1, 2, and 3 above.

### Memtable Stats Gaps

- No endpoint to query memtable dirty state, entry count, or memory usage.
- No endpoint to poll "last flush at T" — can only diff SSTable listings.
- **Workaround (current design docs):** Filesystem watch on `TOC.txt` (written last ⇒ SSTable complete) as a flush trigger; not Sidecar-driven.

---

## Q2 Relevance: Storage Engine Feasibility

**Q2:** How feasible is CQLite as (a) an alternative/replacement storage engine inside Cassandra, and (b) an adjacent OLAP storage engine running alongside the normal engine?

### Current Observations

1. **Sidecar is read-only metadata + file-access plane.**
   - Topology discovery, schema, component listing, range reads.
   - No writes, compaction control, or replication hooks.
   - **Implication for (a):** Replacing the storage engine would require Sidecar to expose flush, compaction scheduling, replication calls — design effort.

2. **No two-phase-commit or consensus.**
   - Sidecar does not orchestrate writes or provide consistency guarantees.
   - **Implication for (a):** A plugged-in storage engine must still hook into Cassandra's RepairMessage, WriteCallback, and HintedHandoff — Sidecar cannot be the only seam.

3. **Snapshot model is Cassandra's (hardlinks + manifest).**
   - Design B assumes snapshots are Cassandra-native (flushed SSTables + manifest).
   - **Implication for (b):** An adjacent OLAP engine reading Cassandra snapshots is viable but depends on Cassandra's snapshot lifecycle (cleanup, expiry), which Sidecar does not control.

4. **Schema sourcing is a pre-query step.**
   - CQLite must pull schema separately; no versioning or change-notification.
   - **Implication:** Hot schema changes (new columns, type changes) are caller's responsibility to refresh.

---

## Trunk-vs-5.0 Deltas

### Trunk (7.0, cassandra-6.0 merged 2026-07-03)

- **CEP-11:** Pluggable Memtable API in place. Sidecar *could* expose `POST .../memtables/{memtable}/flush` backed by a pluggable hook.
- **Pluggable SSTableFormat API:** New format implementations (e.g., columnar, compressed-first) could register and Sidecar could serve them through the same component APIs.
- **Diagnostic events / JMX:** Flush lifecycle events may be available; Sidecar could subscribe and publish them via polling or push stream (not yet done).

### Cassandra 5.0.x

- **No CEP-11:** Memtable is hard-coded; no flush hook for Sidecar to call.
- **BIG+BTI formats only:** No pluggable SSTableFormat API.
- **Snapshot creation via `nodetool snapshot`:** Manual or operator-driven; not exposed via HTTP.
- **Flush is implicit** during compaction and memtable pressure; no explicit control.

**Implication:** Design B (snapshots + component range reads) cannot work on 5.0 without shelling out to `nodetool snapshot` or filesystem hardlinks, which Sidecar does not manage.

---

## Outstanding Questions to Verify Against apache/cassandra-sidecar

1. **Does the component range-read path exist?** (`GET /api/v1/.../components/:component` with HTTP Range)
   - If not, Design B cannot work.
   - If yes, does it stream from disk or buffer the whole component in memory?

2. **Can Sidecar trigger a flush?** Is there a `POST .../memtables/flush` or similar?
   - Current docs mention "create snapshots" but do not say who calls `nodetool snapshot`.
   - CQLite assumes a caller (e.g., a daemon or Trino plugin) would invoke this; not exposed yet.

3. **Are snapshots Cassandra-native hardlinks?** Or does Sidecar manage its own copy-on-write layer?
   - Affects cost of "snapshot + query" workflow.
   - Affects cleanup semantics if client crashes mid-query.

4. **Is there a snapshot lease or TTL?** How does Sidecar prevent snapshot cleanup while a query is mid-scan?
   - Required for consistency contract.

5. **Does Sidecar expose freshness metadata** (last-flush time, dirty memtable count, write rate)?
   - Would enable smarter client-side freshness decisions.

---

## Summary for Q1 & Q2

**For Q1 (freshness):** The current Sidecar HTTP surface is insufficient. CQLite's client assumes:
- Ring + token-range replicas (for replica selection) ✓
- Schema DDL (for type mapping) ✓
- Component range reads (Design B, unverified) ?
- **Missing:** Flush trigger, snapshot creation, memtable stats

The sanctioned answer ("flush + snapshot + read") requires at least two new Sidecar endpoints (POST to flush/snapshot) **that do not exist yet**. Feasibility depends on verifying component range-read support in the real apache/cassandra-sidecar repo.

**For Q2 (engine feasibility):**
- (a) **As a replacement engine:** Would require Cassandra seams beyond Sidecar (write hooks, replication, repair). Feasible but requires CEP-11 in Trunk; 5.0 lacks pluggability entirely.
- (b) **As an adjacent OLAP engine:** Viable if snapshots + component access are reliably exposed. Depends on real Sidecar range-read implementation and snapshot lifecycle management.
