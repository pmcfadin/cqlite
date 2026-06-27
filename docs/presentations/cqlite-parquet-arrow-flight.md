# CQLite: Parquet & Arrow Support

### Reading Cassandra SSTables directly into the analytics stack — Parquet files, Arrow over gRPC, and Arrow Flight → Trino

> Speaker deck. Each slide has **talking points** plus a **Graphic** block describing the visual to build in PowerPoint/Keynote/Google Slides. Code/paths are real and current as of June 2026.

---

## Slide 1 — Title / The Problem

**Title:** Querying Cassandra without querying Cassandra

**Talking points**
- Cassandra is built for OLTP: low-latency point reads/writes. Analytics scans steal that capacity — coordinator hops, read repair, GC pressure on production nodes.
- The data already lives on disk as immutable **SSTables**. What if analytics tools read those files directly, off the hot path?
- CQLite reads Cassandra 5.0 SSTables with **no cluster dependency**. New work (Epic #682 + the Flight/Trino effort) turns that reader into two analytics on-ramps:
  1. **Parquet/Arrow export** — materialize SSTable data as columnar files.
  2. **Arrow Flight sidecar** — stream SSTable data live to Trino over gRPC.

**Graphic**
- Split-screen contrast. Left half labeled "Today": a Trino/Spark icon arrow piercing a Cassandra cluster, red "OLTP contention" warning badge on the nodes. Right half labeled "With CQLite": the same analytics engines reading from a stack of SSTable file icons (Data.db / Index.db / Statistics.db), Cassandra nodes shown calm/green and untouched.
- Bottom ribbon with the CQLite wordmark and the two on-ramps as pills: `Parquet/Arrow files` and `Arrow Flight → Trino`.

---

## Slide 2 — Layered Architecture Overview

**Title:** One reader, two output paths

**Talking points**
- Everything sits on the **CQLite core reader** (`cqlite-core`): parses Data.db rows, decodes CQL types with authoritative schema metadata (no type guessing).
- One shared conversion layer — **CQL → Arrow** (`cqlite-core/src/export/arrow_convert.rs`) — feeds both outputs. Write the type mapping once, reuse everywhere.
- Path A (batch): Arrow RecordBatch → **Parquet file** (`export/parquet.rs`), exposed via CLI, Python, Node.
- Path B (live): Arrow RecordBatch → **Arrow Flight gRPC stream** (`cqlite-flight` crate) → **Trino connector** (Java).
- Feature-gated: `arrow` and `parquet` are off by default, so the base library's dependency surface is unchanged.

**Graphic**
- Horizontal layer cake, bottom to top:
  1. **SSTables** (file icons).
  2. **CQLite core reader** — "row reconstruction + CQL type decode".
  3. **CQL → Arrow conversion** (`arrow_convert.rs`) — highlight this as the shared hinge, centered, with two arrows fanning upward.
  4. Branch left: **Parquet writer** → `.parquet` file icon → consumers (DuckDB, Spark, pandas).
  5. Branch right: **Arrow Flight server** → gRPC cloud → **Trino connector** → Trino SQL.
- Color the shared `arrow_convert.rs` block distinctly (e.g., gold) to emphasize reuse. Annotate feature flags `arrow` / `parquet` as small toggle chips on the relevant blocks.

---

## Slide 3 — Reading the SSTable → Arrow

**Title:** From bytes to columns

**Talking points**
- A query result is **row-oriented** (`QueryRow` objects with decoded partition key, clustering, regular columns). Arrow/Parquet are **column-oriented**. The conversion transposes.
- Two-step public API in `arrow_convert.rs`:
  - `build_arrow_schema(columns)` → derives the Arrow `Schema` from CQL column metadata.
  - `rows_to_record_batch(columns, rows)` → builds one `RecordBatch` (column arrays).
- **High-fidelity mapping**: when a column carries its CQL type, we map to the precise Arrow type, not a generic blob. Recursive — handles nested collections, tuples, UDTs. `Frozen<T>` unwraps transparently.
- No-heuristics mandate honored: types come from schema, never inferred from byte patterns.

**Graphic**
- Left: a small grid of 3 logical rows (row-major) with cells colored by column.
- Center: a rotating/transpose arrow labeled "transpose: row → column".
- Right: the same data as Arrow columnar arrays (each column a vertical contiguous buffer), stacked into a labeled `RecordBatch`.
- Callout box listing the two functions `build_arrow_schema()` and `rows_to_record_batch()` with their crate path `cqlite-core/src/export/arrow_convert.rs`.

---

## Slide 4 — CQL → Arrow Type Mapping

**Title:** Type fidelity, not lossy blobs

**Talking points**
- The mapping is the heart of correctness. A few highlights:
  - Scalars: `int→Int32`, `bigint→Int64`, `boolean→Boolean`, `text→Utf8`, `blob→Binary`.
  - Temporal: `timestamp→Timestamp(ms, UTC)`, `date→Date32`, `time→Time64(ns)`.
  - Numeric edge: `decimal→Decimal128(38,9)` (rescaled via `num_bigint`, errors on >38 digits — never silent truncation); `varint→Decimal128(38,0)`.
  - Identity: `uuid/timeuuid→FixedSizeBinary(16)` carrying the **Arrow UUID extension** metadata so Parquet records the logical type.
  - Nested: `list/set→List<T>`, `map→Map<Struct<key,value>>`, `tuple→Struct(field_0..)`, `udt→Struct(named fields)`.
- Design choice: fixed decimal scale of 9 across columns so files interoperate without per-column negotiation.

**Graphic**
- A clean two-column reference table (CQL type | Arrow type) grouped into bands: Scalars, Temporal, Numeric, Identity, Nested. Use the rows above.
- Right margin: three "gotcha" callout bubbles — (1) "decimal rescaled to fixed scale 9, overflow = error", (2) "UUID extension → Parquet logical type", (3) "set has no Arrow equivalent → List".
- Keep it visually a "cheat sheet" — monospace type names, subtle zebra striping.

---

## Slide 5 — Parquet Export (Path A)

**Title:** Materializing files — batch and streaming

**Talking points**
- Two writers in `export/parquet.rs`:
  - `ParquetWriter` — loads all rows, one RecordBatch, returns the full file bytes. Good for small results.
  - `StreamingParquetWriter<W>` — buffers up to `row_group_size` (default 10,000) rows, flushes **one Parquet row group per chunk**, bounded memory for arbitrarily large exports.
- Critical detail: we call `WriterProperties::set_max_row_group_size()` so the Arrow writer doesn't silently coalesce into ~1M-row groups (which would blow the memory budget).
- Options: compression `Snappy` (default, matches Cassandra), `Zstd` (smaller), or `Uncompressed`; configurable row-group size.
- Exposed everywhere:
  - CLI: `--out parquet`.
  - Python: `db.export_parquet(query, path, row_group_size=, compression=)` (releases the GIL for the whole export).
  - Node: `await db.exportParquet(query, path, { rowGroupSize, compression })`.

**Graphic**
- A pipeline diagram: streaming row source → a buffer box labeled "row_group_size (10k)" → "flush → Parquet row group" repeating into a `.parquet` file made of stacked row-group blocks (footer at bottom).
- Side-by-side mini panels for the two writers ("Batch: all-in-memory, returns Vec<u8>" vs "Streaming: bounded memory, W: Write+Send").
- Three small SDK badges (CLI / Python / Node) each with their one-line call signature.

---

## Slide 6 — The Sidecar: `cqlite-flight` (Path B)

**Title:** A read-only Arrow Flight server, co-located on every node

**Talking points**
- `cqlite-flight` is a Rust binary that runs **on each Cassandra node**, reading that node's local SSTables (`/var/lib/cassandra/data`). Originals are never modified.
- Built on `tonic` (gRPC) + `arrow-flight` 53. Implements the Arrow Flight `FlightService` — read-only subset:
  - `GetFlightInfo` / `GetSchema` — return the Arrow schema for a request.
  - `DoGet` — stream the table as Arrow record batches.
  - `Handshake/DoPut/DoExchange/...` deliberately unimplemented.
- On each `DoGet`, the server does real work before streaming:
  1. **k-way compaction merge** of the node's SSTables on the fly (`KWayMerger`) — reconciles tombstones/updates, originals untouched.
  2. **Token-range filter** — half-open `(start, end]`, wraparound aware.
  3. **Predicate pushdown** — `=, IN, >, >=, <, <=, prefix` via the shared `evaluate_predicates`.
  4. **Column projection** — only requested columns.
  5. Reconstruct rows → `rows_to_record_batch` → encode as Arrow IPC `FlightData`.

**Graphic**
- A single Cassandra node box. Inside it, side by side: the Cassandra process (greyed, "untouched") and the `cqlite-flight` process. Show `cqlite-flight` with a read-only arrow into the shared SSTable volume.
- Inside `cqlite-flight`, a vertical pipeline of 5 labeled stages: `k-way merge → token filter → predicate pushdown → projection → Arrow encode`.
- Out the top: a gRPC stream pipe labeled "Arrow Flight DoGet → FlightData (IPC batches)".
- Small RPC legend in corner: ✅ GetFlightInfo, ✅ GetSchema, ✅ DoGet, ✗ DoPut/DoExchange/Handshake.

---

## Slide 7 — The Flight Ticket Contract

**Title:** One JSON ticket describes the whole scan

**Talking points**
- The client (Trino connector) and server agree on a versioned JSON **ticket** (`cqlite-flight/src/ticket.rs`). It's the contract that makes pushdown and parallelism work.
- Fields: `keyspace`, `table`, full `ddl` (so the server builds the exact Arrow schema), optional `snapshot` (null = live dir), token bounds (`token_start` exclusive, `token_end` inclusive, `wraparound`), optional `columns` projection, optional `predicates`.
- The Java side (`FlightTicketJson.java`) mirrors the Rust serde contract field-for-field — one source of truth, two languages.

**Graphic**
- A literal "ticket stub" graphic (like an event ticket / boarding pass) rendered with the JSON:
  ```json
  {
    "version": 1,
    "keyspace": "analytics",
    "table": "events",
    "ddl": "CREATE TABLE analytics.events (...)",
    "snapshot": null,
    "token_start": -3074457345618258602,
    "token_end":   3074457345618258602,
    "wraparound": false,
    "columns": ["name"],
    "predicates": [{ "column": "score", "op": "Gt", "value": 25 }]
  }
  ```
- Annotate each field group with a small icon: identity (keyspace/table/ddl), placement (token range + wraparound), pushdown (columns + predicates).
- Show two endpoints — Rust `ticket.rs` and Java `FlightTicketJson.java` — with a "⇄ same contract" connector between them.

---

## Slide 8 — Trino Connector & Parallelism

**Title:** Trino splits the ring, reads every node in parallel

**Talking points**
- Java connector (Trino SPI 481, `trino-connector/`). Two discovery channels:
  - **HTTP to Cassandra Sidecar** for topology: `/cassandra/ring`, `/token-range-replicas`, `/keyspaces/{ks}/schema`.
  - **Arrow Flight** to `cqlite-flight` for schema (`GetSchema`) and data (`DoGet`).
- **Split strategy** (`CqliteFlightSplitManager`): one split per token range, pinned to exactly one replica (prefer local DC). Guarantees each row is read **once** despite replication — no dedup needed downstream.
- **Page source**: streams Arrow batches, converts `VectorSchemaRoot` → Trino `Page` via `ArrowToTrino` (Arrow vector → Trino BlockBuilder). UUID extension → formatted VARCHAR; Timestamp → TIMESTAMP_TZ_MILLIS.
- Status today: projection pushdown ✅; predicate pushdown deferred (server supports it, Trino post-filters for now); complex CQL types rejected **at planning time**, not mid-scan.

**Graphic**
- Top: Trino coordinator receiving `SELECT name FROM cqlite.analytics.events WHERE score > 25`.
- Coordinator has two dashed arrows to the **Sidecar** (HTTP, "ring + token ranges + schema").
- Coordinator fans out N solid arrows to **Trino workers**; each worker has a Flight `DoGet` arrow to a different Cassandra node's `cqlite-flight` (8815). Label "1 split = 1 token range = 1 replica".
- Bottom of each worker: a small "Arrow → Page" converter box. Converged result flows back up to the coordinator.
- Status legend chips: `projection ✅ | predicate ⏳ post-filter | complex types ✗ at-plan`.

---

## Slide 9 — End-to-End Data Flow

**Title:** SQL in, columns out — the full round trip

**Talking points**
- Walk the lifecycle of one query end to end:
  1. Trino parses SQL, asks Sidecar for the ring + token ranges + schema.
  2. Connector builds one ticket per token range with projection + (eventually) predicates.
  3. Each Trino worker issues `DoGet` to the co-located `cqlite-flight` on the owning replica.
  4. `cqlite-flight` merges that node's SSTables, filters by token/predicate, projects columns, encodes Arrow.
  5. Arrow IPC batches stream back over gRPC; connector converts to Trino Pages.
  6. Trino applies residual filters/aggregates and returns the result.
- **SSTable semantics matter**: only flushed data is visible. The E2E test proves an un-flushed row is invisible until `nodetool flush`. This is a feature — you're reading the on-disk truth.

**Graphic**
- A single left-to-right swimlane diagram with numbered stages (1–6) across lanes: **Trino Coordinator → Sidecar → Trino Worker → cqlite-flight → (SSTables) → back to Trino**.
- Use distinct line styles: HTTP (dashed) for Sidecar discovery, gRPC/Arrow (solid bold) for data.
- Inset callout near the SSTable stage: "flushed SSTables only — memtable rows invisible (verified by E2E)".
- Keep the numbers visible so the presenter can narrate stage by stage.

---

## Slide 10 — Status, Trade-offs & Roadmap

**Title:** Where it stands and what's next

**Talking points**
- **Done & tested**
  - CQL→Arrow conversion with high-fidelity types (scalars, temporal, decimal/varint, UUID extension, collections, tuples, UDTs).
  - Parquet export: batch + streaming, Snappy/Zstd, CLI + Python + Node.
  - `cqlite-flight` Arrow Flight server: merge, token filter, predicate + projection pushdown, `GetFlightInfo/GetSchema/DoGet`.
  - Trino connector: discovery, split-per-range, Arrow→Page, projection pushdown, docker-compose E2E passing.
- **Trade-offs / current limits**
  - Flight server buffers batches in memory (streaming backpressure is future work).
  - Trino predicate pushdown not yet wired (post-filtered); complex CQL types rejected at planning.
  - Counters not yet supported in the Flight path.
- **Roadmap**: streaming backpressure, wire predicate pushdown into Trino, broaden type coverage, snapshot lifecycle automation, lakehouse commit (Iceberg/Delta) handled by external committers — out of CQLite's scope by design.

**Graphic**
- Three-column board: **✅ Done** | **⚠️ Trade-offs** | **🛣️ Roadmap**, each with the bullets above as cards.
- Top banner: a maturity bar showing Parquet path (solid/GA-ish) vs Flight/Trino path (labeled "Phase 1, E2E green").
- Footer with reference paths for the curious: `cqlite-core/src/export/`, `cqlite-flight/`, `trino-connector/`, `docs/flight-trino/PLAN.md`.

---

## Appendix — Key file references (speaker backup, not a slide)

| Area | Path |
|------|------|
| CQL→Arrow conversion | `cqlite-core/src/export/arrow_convert.rs` |
| Parquet writers | `cqlite-core/src/export/parquet.rs` |
| Export module gating | `cqlite-core/src/export/mod.rs` |
| Python binding | `bindings/python/src/database.rs` (`export_parquet`) |
| Node binding | `bindings/node/src/database.rs` (`exportParquet`) |
| Flight server | `cqlite-flight/src/{main,service,producer,ticket}.rs` |
| Merge engine | `cqlite-core/src/storage/write_engine.rs` (`KWayMerger`) |
| Trino connector | `trino-connector/src/main/java/com/rustyrazorblade/cqlite/flight/` |
| Design docs | `docs/flight-trino/PLAN.md`, `cqlite-flight/README.md`, `trino-connector/README.md` |
| E2E harness | `trino-connector/docker/{docker-compose.yml,e2e-test.sh}` |

**Feature flags:** `arrow` (pulls `arrow` 53), `parquet` (pulls `parquet` 53 with `arrow,snap,zstd`). Both off by default.
