---
title: "Lakehouse Projections with the Cassandra Sidecar"
description: How to use CQLite alongside the Cassandra Sidecar to materialize Parquet projections from SSTable flush events — architecture, delta semantics, and what is still in progress.
sidebar:
  label: Sidecar / Lakehouse
  order: 1
---

# Lakehouse Projections with the Cassandra Sidecar

This page explains how CQLite fits into a **Parquet projection pipeline** that is
triggered by Cassandra SSTable flush events, co-located with the
[Apache Cassandra Sidecar](https://github.com/apache/cassandra-sidecar).

The internal position document (`docs/architecture/cassandra-sidecar-parquet-projections.md`)
is the authoritative source of truth for CQLite maintainers. This page is the
user-facing distillation.

## What is already built

The read-to-Parquet pipeline is largely in place:

1. **CQLite reads externally-discovered SSTables** via `open_with_discovered_sstables()` —
   the integration hook for "the Sidecar told me where a new SSTable is, read it."
2. **Memory-bounded streaming reads** via `execute_streaming()` keep peak memory
   predictable on large tables.
3. **Parquet output** is available via `--out parquet` in the CLI.

A minimal per-SSTable projection is already expressible as a one-shot CLI call:

```bash
cqlite --schema users.cql \
       --data-dir /var/lib/cassandra/data/my_ks/users-<uuid>/ \
       --query "SELECT * FROM my_ks.users" \
       --out parquet -o /projections/users/nb-<gen>.parquet
```

## Architecture

```
┌─────────────────┐   flush     ┌──────────────────────┐
│  Cassandra node │────────────▶│  data dir (SSTables) │
└─────────────────┘  new SSTable└──────────┬───────────┘
                                            │ TOC.txt appears (file complete)
                                ┌───────────▼───────────┐
                                │  Projection service   │  (co-located w/ Sidecar)
                                │  - detect new SSTable  │
                                │  - debounce + dedupe   │
                                └───────────┬───────────┘
                                            │ path + keyspace/table + schema
                                ┌───────────▼───────────┐
                                │  CQLite                │
                                │  open_with_discovered_ │
                                │    sstables()          │
                                │  execute_streaming()   │
                                │  StreamingParquetWriter│
                                └───────────┬───────────┘
                                            │
                                ┌───────────▼───────────┐
                                │  /projections/<ks>/    │
                                │    <table>/nb-<gen>.parquet
                                │  (ideally Iceberg/Delta)
                                └────────────────────────┘
```

### Trigger options

The Sidecar does not expose a push stream of "memtable flushed" events. Options:

| Trigger | Mechanism | Trade-off |
|---------|-----------|-----------|
| Filesystem watch | `inotify` on the table data dir; key on `TOC.txt` (written last ⇒ SSTable complete) | Simplest, reliable; per-host |
| Commitlog CDC | Cassandra CDC on the commitlog | Ordered + carries timestamps; closest to correct CDC; more setup |
| Sidecar API polling | Diff SSTable component listings per table | Easy; latency = poll interval |
| Diagnostic events / JMX | SSTable lifecycle notifications | JVM-side; reintroduces a cluster dependency |

For a simple bulk projection, **filesystem watch keyed on `TOC.txt`** is the pragmatic
default. For correctness-sensitive pipelines, commitlog CDC is the better source — see
the delta semantics section below for why.

:::note
A `*-Data.db` file appearing is not always a memtable flush — it can also be a
**compaction** output. For downstream lake purposes this is usually fine (and often
desirable), but if "flush only" semantics matter, the source must distinguish them.
:::

## ⚠ Delta semantics — this caveat is load-bearing

**A flushed SSTable is a *delta*, not a table snapshot.**

This is inherent to Cassandra's storage model — not a CQLite defect. Four properties
combine to make a naive Parquet-union of per-flush files silently wrong:

1. **Rows are partial / superseded.** The same primary key can appear across many
   SSTables; the live value is the per-cell last-write-wins (LWW) merge of all of them.
   One flush's Parquet reflects only that flush's writes.

2. **Tombstones carry deletes.** A flush can contain row, range, or cell tombstones and
   TTL expirations. The current projection has no representation of these, so a naive
   union of insert rows will **resurrect deleted data**.

3. **Reconciliation requires write-timestamps.** LWW merge is driven by each cell's
   `writetime`. A plain `SELECT *` drops it, so two flushes cannot be correctly merged
   downstream without it.

4. **Absent-vs-null is collapsed.** CQLite's `SELECT *` flattens Cassandra's sparse
   cell-sets into a rectangular result; a cell not written in a particular flush is
   indistinguishable from a null.

### Two coherent approaches — pick deliberately

**CDC / append-log projection (recommended).** Treat each flushed SSTable as an
immutable event batch → one Parquet file per generation. Embrace that it is a delta.
For correctness, **carry `writetime` and represent tombstones** (e.g. `__writetime` /
`__deleted` columns, Debezium-style). Downstream (Spark/Trino/Iceberg) performs the
merge. Idempotent; matches the immutable-SSTable grain.

**Current-snapshot projection.** To reflect current table state, point
`open_with_discovered_sstables()` at *all* live SSTables for the table so CQLite's read
path performs the LWW merge, then export. Correct, but re-reads everything on each flush.

## Type mapping fidelity

The type mapping is **analytics-grade for schema-aware queries**
([epic #673](https://github.com/pmcfadin/cqlite/issues/673)): query results carry the
authoritative schema `CqlType`, and the Parquet writer maps it recursively to a
faithful Arrow schema.

| CQL type | Arrow/Parquet | Fidelity |
|----------|---------------|----------|
| boolean, tinyint … bigint, float, double | Boolean, Int8/16/32/64, Float32/64 | Clean |
| text / varchar / ascii | Utf8 | Clean |
| blob | Binary | Clean |
| timestamp | Timestamp(ms, UTC) | Clean |
| uuid / timeuuid | FixedSizeBinary(16) + `arrow.uuid` extension | Clean (UUID logical annotation) |
| date | `Date32` | Clean |
| time | `Time64(Nanosecond)` | Clean |
| decimal | `Decimal128(38, 9)` | Fixed column scale 9; checked rescale, overflow errors deterministically |
| varint | `Decimal128(38, 0)` | Values > 38 digits error deterministically |
| list&lt;T&gt; / set&lt;T&gt; | `List<T>` | Typed elements, recursive (sets export as List — Arrow has no set type) |
| map&lt;K,V&gt; | `Map<K, V>` | Typed keys (non-null) and values, recursive |
| tuple / UDT | `Struct` | Positional `field_N` / schema field names |
| frozen&lt;T&gt; | Same as T | Transparent at every nesting level |
| inet | Utf8 (canonical text) | Deliberate: no standard Arrow inet type |
| duration | Utf8 (CQL text form) | `parquet` crate v53 cannot write `Interval(MonthDayNano)` |

Collections, UDTs, and high-precision scalars all support columnar predicate pushdown
in downstream consumers (Trino, Spark, DuckDB). The batch and streaming writers produce
identical schemas and values. See
[Output Formats](/cqlite/user-docs/output-formats/) for the full table and notes.

## What epics #682 and #696 unlock

These in-progress epics complete the lakehouse story:

**[Epic #682: Lift Parquet writer into cqlite-core](https://github.com/pmcfadin/cqlite/issues/682)**
*(in progress)*

Moves the Parquet writer from `cqlite-cli` into `cqlite-core` behind a `parquet`
feature flag. Today, embedding projection in a long-running service requires shelling
out to the CLI. After this epic, a projection service can call the writer directly as a
library without a subprocess, and the Python and Node bindings can export Parquet.

**[Epic #696: Delta-scan envelope for CDC-style projections](https://github.com/pmcfadin/cqlite/issues/696)**
*(in progress)*

Implements a `scan_delta` streaming API that emits `DeltaRecord`s with per-cell
`writetime`, `expires_at`, and an `__op` discriminator covering all five Cassandra
delete shapes (`upsert`, `static_upsert`, `row_delete`, `range_delete`,
`partition_delete`). Paired with a `DeltaParquetWriter` and a `cqlite delta-export`
CLI subcommand. This is what makes CDC-mode projections reconcilable downstream.

## Schema sourcing

CQLite requires the CQL schema to decode an SSTable (no-heuristics mandate, issue #28).
The projection service must source and cache the schema — flush events do not carry it.
The practical approach is to pull it from `DESCRIBE TABLE` output and keep it in sync
with schema changes.

## Comparison with TiDB's approach

TiDB's TiFlash is instructive: it is a Raft learner that receives every committed write
in real-time, providing a strongly consistent, transactionally-consistent columnar
replica. TiCDC (the structural analog to this pipeline) tails the ordered row-change
log and emits Kafka or cloud-storage changefeeds with explicit insert/update/delete
events and commit timestamps.

Our approach's distinct value is **open, lake-native columnar output from Cassandra with
no cluster dependency in the read path**. The trade-off is that Cassandra has no ordered
committed log — its source of truth is independently-flushed, LWW-merged SSTables — so
any columnar projection is inherently a delta-reconciliation problem. Epic #673
(delivered) addressed type fidelity; epics #682 and #696 each address one remaining
part of that problem.

## Recommendations

1. **Model it as CDC/append-log, not snapshot.** It matches the immutable-SSTable grain
   and is idempotent per generation.
2. **Land in a table format (Iceberg or Delta), not bare Parquet files.** Those provide
   snapshot isolation, compaction, and merge-on-read that otherwise you must build by hand.
3. **Preserve `writetime` and represent tombstones** (`__writetime` / `__deleted`
   columns) so projections are reconcilable. Without this, a union of per-flush Parquet
   is silently wrong.
4. **Prefer commitlog CDC over raw flush events** when correctness matters.
5. **Lift the Parquet writer into `cqlite-core`** (epic #682) if you want embeddable
   non-CLI projection.

<!-- TODO(W4): link to CLI reference when merged -->
<!-- TODO(W5): link to Python and Node bindings pages when merged -->
