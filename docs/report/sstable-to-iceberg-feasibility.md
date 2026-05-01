# SSTable-to-Iceberg Feasibility Report

**CQLite as an Offline Bridge from Apache Cassandra to Apache Iceberg**

| | |
|---|---|
| **Date** | February 2026 |
| **Author** | CQLite Project |
| **Status** | Research Complete |
| **Version** | 1.0 |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [CQLite Capabilities Assessment](#2-cqlite-capabilities-assessment)
3. [Apache Iceberg Overview](#3-apache-iceberg-overview)
4. [Rust Ecosystem Readiness](#4-rust-ecosystem-readiness)
5. [CQL-to-Iceberg Type Mapping](#5-cql-to-iceberg-type-mapping)
6. [Apache Cassandra Sidecar & Analytics Ecosystem](#6-apache-cassandra-sidecar--analytics-ecosystem)
7. [Existing Landscape & Competitive Analysis](#7-existing-landscape--competitive-analysis)
8. [Architecture Options](#8-architecture-options)
9. [Proposed CQLite→Iceberg Pipeline](#9-proposed-cqliteiceberg-pipeline)
10. [Implementation Roadmap](#10-implementation-roadmap)
11. [Use Cases & Value Proposition](#11-use-cases--value-proposition)
12. [Sources](#12-sources)

---

## 1. Executive Summary

This report evaluates the feasibility of using CQLite as an offline bridge to publish
Apache Cassandra SSTable data directly to Apache Iceberg tables — enabling analytics
projections on all table data from a Cassandra node without requiring a running cluster.

### Key Findings

**CQLite is uniquely positioned for this bridge.** It is the only tool that combines
offline SSTable reading, full SQL projection support, streaming APIs with memory bounds,
and a modern Rust implementation. No existing tool provides a direct, lightweight
SSTable→Iceberg path.

**The Rust ecosystem is ready.** The `iceberg-rust` crate (Apache official, v0.8.0) provides
catalog integration, composable writers, and Parquet output. The `arrow-rs` and `parquet`
crates are production-grade, powering DataFusion, Ballista, and other Arrow-native query
engines.

**No direct SSTable→Iceberg tool exists today.** The current landscape requires either a
running Cassandra cluster (Spark Cassandra Connector), a heavyweight Spark runtime
(Cassandra Analytics), or a multi-hop pipeline through Kafka (Netflix pattern). CQLite
can fill this gap with a single binary that reads SSTables from disk or S3 and writes
directly to Iceberg.

**The Apache Cassandra Sidecar and Analytics projects** provide complementary infrastructure
(CDC via Kafka, Spark Bulk Reader/Writer) but neither offers a direct Iceberg export path.
They require either a running cluster or the Spark runtime — both of which CQLite eliminates.

### Feasibility Assessment

| Dimension | Rating | Notes |
|-----------|--------|-------|
| Technical feasibility | **High** | All building blocks exist in Rust |
| Type system coverage | **High** | Full CQL→Arrow mapping achievable |
| Ecosystem maturity | **Medium-High** | iceberg-rust v0.8.0 in production use |
| Competitive differentiation | **Very High** | No lightweight offline alternative exists |
| Implementation effort | **Medium** | ~4-8 weeks for Phase 1 (Parquet export) |

### Recommendation

Proceed with a phased implementation. Phase 1 delivers standalone Parquet export (high
value, low risk). Phase 2 adds Iceberg catalog integration. Phase 3 handles schema
evolution and partition mapping. Phase 4 adds multi-node support via primary-range token
filtering and optional CDC.

---

## 2. CQLite Capabilities Assessment

CQLite (Milestone 5.2, February 2026) provides the foundational capabilities needed for
an SSTable-to-Iceberg bridge. This section assesses the relevant subsystems.

### 2.1 Query Engine & Projections

CQLite supports full SELECT projections without a running Cassandra cluster:

- **Column selection**: `SELECT col1, col2 FROM ks.tbl`
- **WHERE predicates**: Partition key and clustering key filtering
- **Aggregations**: COUNT, MIN, MAX, SUM, AVG
- **ORDER BY**: Clustering order with ASC/DESC
- **LIMIT**: Row count caps
- **Multi-table discovery**: Scans entire Cassandra data directories

Key source: `cqlite-core/src/query/` — the query engine operates directly on parsed SSTable data.

### 2.2 Discovery Service

The discovery service scans Cassandra data directories to find all keyspaces, tables, and
their SSTables. This enables a single CQLite invocation to process an entire node's data:

```
/var/lib/cassandra/data/
├── keyspace_a/
│   ├── table_1-{uuid}/
│   │   ├── nb-1-big-Data.db
│   │   ├── nb-1-big-Index.db
│   │   └── ...
│   └── table_2-{uuid}/
└── keyspace_b/
    └── ...
```

CQLite resolves table UUIDs, loads schemas, and provides unified access across all tables.

### 2.3 Streaming API

CQLite's streaming architecture is directly applicable to Iceberg batch writing:

| Config | Default | Purpose |
|--------|---------|---------|
| `row_buffer_size` | 1,024 rows | Rows buffered before yielding |
| `chunk_size` | 10,000 rows | Processing chunk size |
| `peak_memory` | ~11 MB | Bounded memory usage |

The `StreamingConfig::for_parquet()` preset is already defined (1024-row buffer, 10K chunks),
indicating Parquet output was anticipated in the design.

### 2.4 Type System Coverage

CQLite's `TypeSerializer` (`cqlite-core/src/storage/serialization/types.rs`) supports all
CQL types needed for Iceberg mapping:

**Primitives**: Boolean, TinyInt, SmallInt, Int, BigInt, Counter, Float, Double

**Text/Binary**: Text, Ascii, Varchar, Blob, Inet

**Temporal**: Timestamp (ms epoch), Date (offset-encoded), Time (ns since midnight), Duration

**Numeric**: Varint (arbitrary precision), Decimal (scale + varint)

**Collections**: List, Set, Map, Tuple, Frozen variants

**User-Defined Types**: Schema-aware serialization with field ordering

### 2.5 Write Engine & Export

The M5.2 write engine provides:

- **Memtable**: In-memory BTreeMap ordered by Murmur3 token
- **WAL**: Write-ahead log with crash recovery
- **Flush**: Explicit `flush()` API for controlled SSTable creation
- **K-way merge**: `KWayMerger` with incremental `step()` API (partition-by-partition streaming)
- **Export**: `export_sstable()` for Cassandra-compatible SSTable output

The K-way merger's `step()` API is particularly relevant: it yields one partition at a time
with bounded memory (k × 8KB), enabling streaming merge-and-export to Iceberg.

### 2.6 Output Formats

The CLI defines four output modes:

```rust
pub enum OutputMode {
    Table,    // cqlsh-compatible tabular
    Json,     // JSON array of objects
    Csv,      // CSV with header
    Parquet,  // Binary Parquet (requires --output file)
}
```

Parquet is defined as an output format but **not yet implemented** as a writer. This is the
natural extension point for Iceberg integration.

### 2.7 Bindings

- **Python** (PyO3): `Database.execute()`, `Database.executeStreaming()` — enables Python-based
  Iceberg pipelines via `pyiceberg`
- **Node.js** (napi-rs): `Database.executeNative()`, `Database.executeStreaming()` — enables
  JavaScript integration
- **Rust** (native): Direct access to all internal APIs — optimal for Iceberg integration

### 2.8 Key Source Files

| File | Relevance |
|------|-----------|
| `cqlite-core/src/storage/serialization/types.rs` | CQL type system (1,047 lines) |
| `cqlite-core/src/storage/write_engine/export.rs` | SSTable export (945 lines) |
| `cqlite-core/src/storage/write_engine/merge.rs` | K-way merge (1,029 lines) |
| `cqlite-core/src/storage/write_engine/mod.rs` | Write engine API (2,049 lines) |
| `cqlite-cli/src/cli_types.rs` | CLI output formats (417 lines) |
| `cqlite-core/src/query/` | Query engine (projections, filtering) |

---

## 3. Apache Iceberg Overview

Apache Iceberg is an open table format for analytic datasets. Understanding its architecture
is essential for designing the CQLite bridge.

### 3.1 What Iceberg Is (and Isn't)

Iceberg is a **table format**, not a file format or query engine. It manages metadata that
describes how data files (Parquet, ORC, or Avro) form a logical table:

```
Iceberg Table
├── Metadata (JSON)           ← Table schema, partitioning, properties
│   ├── Snapshot 1            ← Point-in-time view
│   │   └── Manifest List     ← List of manifest files
│   │       ├── Manifest 1    ← Data file metadata + column stats
│   │       └── Manifest 2
│   └── Snapshot 2
│       └── ...
└── Data Files (Parquet)      ← Actual row data
    ├── part-00001.parquet
    ├── part-00002.parquet
    └── ...
```

### 3.2 Key Properties

| Property | Description |
|----------|-------------|
| **ACID transactions** | Atomic commits via metadata swap |
| **Schema evolution** | Add, drop, rename, reorder columns without rewriting data |
| **Partition evolution** | Change partitioning without rewriting existing data |
| **Time travel** | Query any previous snapshot |
| **Hidden partitioning** | Users write queries against logical schema, not partition layout |
| **Column-level stats** | Min/max/null counts per column per file — enables efficient predicate pushdown |

### 3.3 Underlying File Formats

Parquet is the de facto standard for Iceberg data files:

- **Columnar storage**: Efficient for analytics workloads (projections, aggregations)
- **Compression**: Snappy, Zstd, LZ4, Gzip per column
- **Encoding**: Dictionary, RLE, delta encoding
- **Row groups**: Configurable partitioning within files (typically 128MB chunks)

### 3.4 Catalog Types

The catalog stores the current metadata pointer for each Iceberg table:

| Catalog | Best For | Notes |
|---------|----------|-------|
| **REST** | New deployments | Vendor-neutral, modern architecture |
| **Hive Metastore** | Existing Hive/Spark infrastructure | Requires RDBMS backend |
| **AWS Glue** | AWS-native deployments | Serverless, managed |
| **Nessie** | Git-like versioning | Multi-table transactions |
| **HDFS / Local FS** | Development/testing | File-based metadata |

### 3.5 Query Engine Compatibility

Iceberg tables are readable by all major analytics engines:

- Apache Spark, Apache Flink, Trino/Presto
- Snowflake, BigQuery, Redshift Spectrum
- DuckDB, DataFusion, Polars
- Dremio, StarRocks, Doris

This broad compatibility is the core value proposition: write Cassandra data to Iceberg once,
query it from anywhere.

---

## 4. Rust Ecosystem Readiness

### 4.1 iceberg-rust (Apache Official)

| | |
|---|---|
| **Repository** | [apache/iceberg-rust](https://github.com/apache/iceberg-rust) |
| **Crate** | [iceberg](https://crates.io/crates/iceberg) |
| **Version** | v0.8.0 (January 2026) |
| **License** | Apache 2.0 |
| **Stars** | 1,200+ |
| **Production Users** | Databend, RisingWave, Supabase |

#### Catalog API

```rust
use iceberg_catalog_rest::RestCatalog;

let catalog = RestCatalog::new(RestCatalogConfig {
    uri: "http://localhost:8181".to_string(),
    ..Default::default()
});

let table = catalog.load_table(&TableIdent::new(
    NamespaceIdent::new("analytics".to_string()),
    "cassandra_data".to_string(),
)).await?;
```

Supported catalog crates:
- `iceberg-catalog-rest` — REST API catalog
- `iceberg-catalog-hms` — Hive Metastore
- `iceberg-catalog-glue` — AWS Glue
- `iceberg-catalog-memory` — In-memory (testing)

#### Writer API

The writer is composable — physical writers are wrapped with logical writers:

```rust
use iceberg::writer::{
    IcebergWriter,
    file_writer::ParquetWriter,
    base_writer::data_file_writer::DataFileWriter,
    base_writer::fanout_partition_writer::FanoutPartitionWriter,
};

// 1. Physical writer (Parquet)
let parquet_writer = ParquetWriterBuilder::new(
    file_io.clone(),
    location_generator,
    file_name_generator,
);

// 2. Logical wrapper (Iceberg metadata)
let data_writer = DataFileWriter::new(parquet_writer);

// 3. Optional: auto-partitioning
let writer = FanoutPartitionWriter::new(data_writer, partition_spec);

// 4. Write Arrow RecordBatches
writer.write(&record_batch).await?;
let data_files = writer.close().await?;

// 5. Commit to catalog
let tx = table.new_transaction();
tx.fast_append(data_files)?;
tx.commit().await?;
```

#### Production Readiness

| Aspect | Status |
|--------|--------|
| Core table operations | Stable |
| Parquet write | Stable |
| REST catalog | Stable |
| Schema evolution | Supported |
| Partition evolution | Supported |
| Compaction | Implemented (RisingWave validated) |
| Merge-on-read | Partial |

**Risk**: As v0.8.0, API breaking changes are possible. Pin dependency versions.

### 4.2 arrow-rs / parquet Crate

| | |
|---|---|
| **Crate** | [arrow](https://crates.io/crates/arrow) + [parquet](https://crates.io/crates/parquet) |
| **Version** | v57.2.0 (stable, production-grade) |
| **Maturity** | Very high — powers DataFusion, Ballista, Delta Lake |

#### RecordBatch Construction

```rust
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Utf8, false),
    Field::new("name", DataType::Utf8, true),
    Field::new("age", DataType::Int32, true),
    Field::new("created_at", DataType::TimestampMillisecond, true),
]));

let batch = RecordBatch::try_new(schema, vec![
    Arc::new(StringArray::from(vec!["uuid-1", "uuid-2"])),
    Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
    Arc::new(Int32Array::from(vec![Some(30), Some(25)])),
    Arc::new(TimestampMillisecondArray::from(vec![Some(1704067200000), None])),
])?;
```

#### Parquet Writing

```rust
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

let props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    .set_max_row_group_size(128 * 1024 * 1024) // 128MB row groups
    .build();

let file = File::create("output.parquet")?;
let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
writer.write(&batch)?;
writer.close()?; // Finalizes footer and flushes
```

Async writing is also supported via `AsyncArrowWriter` for streaming to S3/GCS.

### 4.3 Complete Write Pipeline

```
CQLite SSTable Reader
    │
    ▼
CQL Row Iterator (streaming, memory-bounded)
    │
    ▼
Arrow RecordBatch Builder (CQL→Arrow type mapping)
    │  - Accumulate rows into columnar arrays
    │  - Flush every N rows (e.g., 10,000)
    │
    ▼
iceberg::writer::ParquetWriter
    │  - Serializes RecordBatch to Parquet
    │  - Manages row group boundaries
    │
    ▼
iceberg::writer::DataFileWriter
    │  - Adds column-level statistics
    │  - Generates Iceberg DataFile metadata
    │
    ▼
iceberg::Transaction::fast_append()
    │  - Creates manifest entry
    │  - Commits new snapshot
    │
    ▼
Iceberg Catalog (REST/Glue/HMS)
    │  - Updates metadata pointer
    │
    ▼
Cloud Storage (S3/GCS/ADLS/Local)
    └── Parquet data files + Iceberg metadata
```

All crates in this pipeline are production-grade and actively maintained.

---

## 5. CQL-to-Iceberg Type Mapping

### 5.1 Primitive Type Mapping

| CQL Type | Arrow Type | Iceberg Type | Notes |
|----------|-----------|--------------|-------|
| `boolean` | `Boolean` | `boolean` | Direct mapping |
| `tinyint` | `Int8` | `int` | Widened to 32-bit in Iceberg |
| `smallint` | `Int16` | `int` | Widened to 32-bit in Iceberg |
| `int` | `Int32` | `int` | Direct mapping |
| `bigint` | `Int64` | `long` | Direct mapping |
| `counter` | `Int64` | `long` | Counter semantics lost |
| `float` | `Float32` | `float` | Direct mapping |
| `double` | `Float64` | `double` | Direct mapping |
| `varint` | `Decimal128` | `decimal(38,0)` | Precision capped at 38 digits |
| `decimal` | `Decimal128` | `decimal(38,s)` | Scale from CQL value |

### 5.2 Text and Binary Types

| CQL Type | Arrow Type | Iceberg Type | Notes |
|----------|-----------|--------------|-------|
| `text` | `Utf8` | `string` | Direct mapping |
| `ascii` | `Utf8` | `string` | ASCII subset of UTF-8 |
| `varchar` | `Utf8` | `string` | Alias for `text` |
| `blob` | `Binary` | `binary` | Raw bytes |
| `inet` | `Utf8` | `string` | Serialize as string (e.g., "192.168.1.1") |

### 5.3 UUID and Temporal Types

| CQL Type | Arrow Type | Iceberg Type | Notes |
|----------|-----------|--------------|-------|
| `uuid` | `FixedSizeBinary(16)` | `uuid` | Iceberg has native UUID |
| `timeuuid` | `FixedSizeBinary(16)` | `uuid` | Time component available via metadata |
| `timestamp` | `TimestampMillisecond` | `timestamptz` | Millisecond precision |
| `date` | `Date32` | `date` | Days since epoch |
| `time` | `Time64Nanosecond` | `time` | Nanosecond precision preserved |
| `duration` | `Struct{months,days,nanos}` | `struct` | No native Iceberg duration type |

### 5.4 Collection Types

| CQL Type | Arrow Type | Iceberg Type | Notes |
|----------|-----------|--------------|-------|
| `list<T>` | `List(T')` | `list(T')` | Recursive type mapping |
| `set<T>` | `List(T')` | `list(T')` | Set semantics lost; deduplicated on write |
| `map<K,V>` | `Map(K',V')` | `map(K',V')` | Direct mapping |
| `tuple<T1,T2,...>` | `Struct{f0:T1',f1:T2',...}` | `struct` | Positional fields |
| `frozen<T>` | Same as `T` | Same as `T` | Frozen flag is Cassandra-internal |

### 5.5 User-Defined Types

| CQL Type | Arrow Type | Iceberg Type | Notes |
|----------|-----------|--------------|-------|
| `UDT` | `Struct{field:Type,...}` | `struct` | Fields in schema definition order |

UDT mapping requires schema context. CQLite's `serialize_udt()` provides the field ordering
needed for correct Arrow struct construction.

### 5.6 Challenge Areas

#### Varint Precision

CQL `varint` supports arbitrary precision integers. Arrow `Decimal128` caps at 38 digits.
For values exceeding 38 digits:

- **Option A**: Serialize as `Utf8` string (lossless, queryable with CAST)
- **Option B**: Use `Decimal256` (76 digits, supported in Arrow but not Iceberg)
- **Option C**: Error on overflow (safest for analytics correctness)

**Recommendation**: Default to `Decimal128(38,0)`, with overflow → `Utf8` fallback.

#### Frozen vs Non-Frozen Collections

Cassandra distinguishes frozen (immutable, serialized as blob) and non-frozen (mutable,
cell-per-element) collections. Iceberg has no equivalent concept.

**Recommendation**: Treat both identically in Iceberg schema. Annotate frozen status in
Iceberg table properties if needed for round-trip fidelity.

#### Cell-Level Metadata

Each Cassandra cell carries metadata that Iceberg cannot natively represent:

| Metadata | Description | Recommendation |
|----------|-------------|----------------|
| `write_time` | Microsecond timestamp per cell | Optional metadata column |
| `ttl` | Time-to-live per cell | Optional metadata column |
| Tombstones | Soft-delete markers | Filter out (default) or flag column |

**Recommendation**: Phase 1 ignores cell metadata. Phase 2 adds optional columns:
`_cql_write_time_<col>` and `_cql_ttl_<col>`.

#### Counter Columns

CQL counters are distributed increment-only values. In Iceberg they become plain `long`
values — the counter semantics (merge function) are lost.

**Recommendation**: Export as `long`. Document that counter tables represent point-in-time
snapshots, not mergeable counters.

### 5.7 Analytics Mode

For maximum compatibility, offer an "analytics mode" where complex types are simplified:

| Complex Type | Analytics Mode Representation |
|-------------|-------------------------------|
| `list<T>` | JSON string: `[1, 2, 3]` |
| `set<T>` | JSON string: `[1, 2, 3]` |
| `map<K,V>` | JSON string: `{"key": "value"}` |
| `tuple<...>` | JSON string: `[val1, val2]` |
| UDT | JSON string: `{"field": "value"}` |
| `duration` | ISO 8601 string: `P1Y2M3DT4H5M6S` |

This trades type fidelity for universal queryability — every analytics engine can process
JSON strings, but not all handle nested Parquet structs correctly.

---

## 6. Apache Cassandra Sidecar & Analytics Ecosystem

Two Apache projects provide Cassandra data access patterns that complement (and compete with)
a CQLite-based approach.

### 6.1 Apache Cassandra Sidecar

**Repository**: [apache/cassandra-sidecar](https://github.com/apache/cassandra-sidecar)

The Sidecar is a companion Java process that runs alongside each Cassandra node, providing
operational capabilities via REST API.

#### Relevant Capabilities

**SSTable Streaming** (CEP-40):
- HTTP endpoints for streaming SSTable component bytes
- Token range-aware routing — can proxy requests to appropriate nodes
- Snapshot-based access without impacting live reads/writes
- Enables remote SSTable access without direct filesystem access

**CDC via Kafka** (CEP-44):
- Captures table mutations from Cassandra's commit log
- Streams mutations to Kafka topics in Avro format
- Maintains configured consistency levels
- At-least-once delivery semantics
- Token range-based ownership for distributed processing

**Bulk Operations**:
- SSTable upload endpoint for bulk writes (used by Cassandra Analytics)
- Snapshot management (create, list, delete)
- Ring topology and schema information endpoints

#### Limitations for Iceberg

- **Requires running Cassandra**: Sidecar needs a live Cassandra node
- **No Parquet/Iceberg output**: Streams raw SSTable bytes, not analytics formats
- **Java/REST only**: No Rust client library
- **Operational focus**: Designed for cluster operations, not analytics export

### 6.2 Apache Cassandra Analytics

**Repository**: [apache/cassandra-analytics](https://github.com/apache/cassandra-analytics)
**CEP**: [CEP-28](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28)

Cassandra Analytics provides Spark-based bulk reading and writing of Cassandra data.

#### Spark Bulk Reader

- **Direct SSTable access**: Reads raw SSTables into Spark DataFrames
- **Streaming compaction**: Uses `CompactionIterator` to merge multiple SSTables
- **No cluster impact**: Bypasses Cassandra's query engine entirely
- **Performance**: Documented at 1.7 Gbps per instance for reads
- **Type support**: All CQL primitive types, collections, UDTs
- **Cassandra versions**: 3.0, 4.0, 5.0

Architecture:
```
SSTable Files → CompactionIterator → SparkCellIterator → SparkRowIterator → DataFrame
```

#### Spark Bulk Writer

- **DataFrame → SSTable**: Writes Spark DataFrames as Cassandra SSTables
- **Sidecar upload**: Streams generated SSTables to target nodes via Sidecar
- **Performance**: Documented at 7 Gbps per instance for writes
- **Consistency**: Validates token ranges and replica placement

#### Limitations for Iceberg

- **Requires Spark runtime**: JVM + Spark cluster overhead (~2GB+ baseline memory)
- **No direct Iceberg output**: Produces DataFrames, not Iceberg tables
- **Cluster dependency**: Bulk Writer requires Sidecar for uploads
- **Complexity**: Requires Spark job orchestration infrastructure

### 6.3 CQLite's Differentiation

| Capability | CQLite | Sidecar | Analytics |
|-----------|--------|---------|-----------|
| Offline SSTable reading | Yes | No (needs Cassandra) | Yes (via Spark) |
| No JVM required | Yes | No (Java) | No (Spark/Java) |
| Memory footprint | ~11 MB streaming | N/A | ~2 GB+ (Spark) |
| Single binary | Yes | No | No |
| SQL projections | Yes | No | Yes (SparkSQL) |
| Schema evolution tracking | No | Partial | No |
| CDC support | No | Yes (CEP-44) | No |
| Direct Iceberg output | Planned | No | No (DataFrame only) |

**Key insight**: Neither the Sidecar nor Cassandra Analytics provides a direct path to Iceberg.
Both require additional infrastructure (Kafka consumers, Spark jobs) to bridge the gap.
CQLite can provide a single-binary solution with no external dependencies.

---

## 7. Existing Landscape & Competitive Analysis

### 7.1 DataStax sstable-to-arrow

**Repository**: [datastax/sstable-to-arrow](https://github.com/datastax/sstable-to-arrow)

A C++17 tool that parses Cassandra SSTables and converts to Arrow/Parquet format, designed
for GPU analytics with NVIDIA RAPIDS.

| Aspect | Details |
|--------|---------|
| Language | C++17 |
| Cassandra version | 3.11 only |
| Output | Arrow tables, Parquet files |
| Multiple SSTables | No (single file) |
| Deduplication | No |
| Iceberg support | No |
| Maintenance | Minimal (last significant update ~2023) |

**Gap**: No Cassandra 5.0 support, no deduplication, no Iceberg metadata.

### 7.2 DataStax CDC Connector

DataStax provides a CDC-based connector that captures Cassandra mutations and publishes
to Kafka:

- Requires a running Cassandra cluster with CDC enabled
- Kafka as intermediate transport
- No direct Iceberg sink — requires additional Kafka→Iceberg consumer
- Proprietary DataStax additions on top of open-source Cassandra

### 7.3 Spark Cassandra Connector

The DataStax Spark Cassandra Connector reads from a **live Cassandra cluster** via CQL:

- Requires running cluster and network access
- Uses CQL queries (not direct SSTable access)
- Can leverage Spark's Iceberg connector for output
- Significant resource overhead (Spark + Cassandra)

### 7.4 Instaclustr Cassandra Parquet Transformer

A Java tool that converts Cassandra SSTables to Parquet format:

- Limited type support
- No Iceberg metadata generation
- Not actively maintained

### 7.5 Netflix Pattern: Cassandra → Kafka → Iceberg

Netflix operates a production pipeline synchronizing Cassandra with Iceberg:

```
Cassandra (Source of Truth)
    ↓ CDC
Kafka Topics
    ↓ Consumer
Iceberg Tables (Analytics)
```

Key characteristics:
- **Scale**: Part of Netflix's ~20,000 distinct data movement jobs
- **Async**: Eventual consistency between Cassandra and Iceberg
- **Infrastructure**: Requires Kafka, CDC configuration, custom consumers
- **Validation**: Proves Iceberg handles Cassandra-scale analytics workloads

**Insight**: Netflix validates the destination (Iceberg) but uses a heavyweight pipeline
(Kafka CDC) to get there. CQLite can offer the same destination with a dramatically
simpler path.

### 7.6 Competitive Gap Summary

```
                          Direct SSTable   No Cluster   Iceberg    Lightweight
                          Access           Required     Output     (No JVM/Spark)
                          ─────────────    ──────────   ────────   ──────────────
sstable-to-arrow          ✓                ✓            ✗          ✗ (C++ + Python)
Spark Bulk Reader         ✓                ✓*           ✗**        ✗ (Spark)
Spark Cassandra Conn.     ✗ (CQL)          ✗            ✓**        ✗ (Spark)
DataStax CDC              ✗ (CDC)          ✗            ✗          ✗ (Kafka)
Netflix Pattern           ✗ (CDC)          ✗            ✓          ✗ (Kafka + ?)
CQLite (proposed)         ✓                ✓            ✓          ✓

* Requires Spark runtime but not Cassandra for reads
** Via Spark's Iceberg connector
```

**No existing tool provides all four properties.** CQLite fills a clear market gap.

---

## 8. Architecture Options

### 8.1 Option A: Offline SSTable Export (Recommended for Phase 1)

```
Cassandra Node (stopped or snapshot)
    ↓ Copy SSTable files
Local Disk / S3 / Object Store
    ↓
CQLite SSTable Reader
    ↓ Discovery + schema-aware parsing
CQL Row Stream (memory-bounded)
    ↓ CQL→Arrow type mapping
Arrow RecordBatch
    ↓ ParquetWriter
Parquet Files
    ↓ Iceberg catalog commit
Iceberg Table
```

**Pros**: Simplest architecture, no running services, fully offline
**Cons**: Point-in-time only, no real-time updates
**Use cases**: Migration, archival, periodic analytics refresh

### 8.2 Option B: Sidecar-Assisted Export

```
Running Cassandra Cluster
    ↓ Sidecar REST API
SSTable Snapshot Bytes (HTTP stream)
    ↓
CQLite SSTable Reader
    ↓ Parse + project
Arrow RecordBatch → Iceberg
```

**Pros**: No filesystem access needed, works with remote clusters
**Cons**: Requires running Cassandra + Sidecar, HTTP overhead
**Use cases**: Production cluster analytics without filesystem access

### 8.3 Option C: CDC Pipeline

```
Running Cassandra Cluster
    ↓ CDC (Sidecar CEP-44)
Kafka Topics (Avro mutations)
    ↓
CQLite CDC Consumer
    ↓ Mutation → Row materialization
Arrow RecordBatch → Iceberg
    ↓ Incremental append
Iceberg Table (near real-time)
```

**Pros**: Near real-time, incremental updates, no full scan
**Cons**: Requires Kafka + Sidecar, complex deployment, eventual consistency
**Use cases**: Real-time analytics, streaming dashboards

### 8.4 Option D: Spark Bulk Reader → Iceberg (Existing Tools)

```
SSTable Files
    ↓ Cassandra Analytics (Spark)
Spark DataFrame
    ↓ Spark Iceberg Connector
Iceberg Table
```

**Pros**: Battle-tested components, full Spark SQL support
**Cons**: JVM + Spark overhead (~2GB+ memory), complex deployment, slower iteration
**Use cases**: Organizations already running Spark infrastructure

### 8.5 Multi-Node Considerations

In a Cassandra cluster, data is replicated across nodes. A naive approach would collect
SSTables from every node and K-way merge to deduplicate — but this is unnecessarily complex.

#### Primary-Range-Only Strategy (Recommended)

Each Cassandra node is the **primary owner** for a contiguous token range (or multiple
non-contiguous ranges with vnodes). The Murmur3 partitioner assigns every partition key
to exactly one primary range. By filtering to only primary-range data on each node, you
get complete coverage with zero duplication — no merge needed.

```
                    Token Ring
                   ┌──────────┐
                   │  -2^63   │
              Node A│          │Node C
              range │          │ range
                   │          │
              Node B range
                   └──────────┘

Node A: export tokens in [-2^63, -2^63 + R)     ─┐
Node B: export tokens in [-2^63 + R, -2^63 + 2R) ─┼── Union = complete dataset
Node C: export tokens in [-2^63 + 2R, 2^63)      ─┘         (zero duplicates)
```

**How it works**:
1. Each node runs CQLite independently on its local SSTables
2. CQLite filters partitions by Murmur3 token — only exports rows whose token falls
   in the node's primary range
3. The union of all node outputs is the complete dataset with no duplicates
4. No need to copy SSTables between nodes or run a centralized merge

**Token range sources**:
- `nodetool ring` — lists token ownership per node
- `system.local` / `system.peers` tables — programmatic access to ring topology
- `--token-range` CLI flag — explicit range specification

**Vnode consideration**: With vnodes (default in Cassandra 4+), each node owns multiple
non-contiguous token ranges (typically 16-256). CQLite accepts multiple ranges and filters
partitions against all of them. CQLite already computes Murmur3 tokens internally (the
write engine uses `DecoratedKey`), so token-range filtering is a natural extension.

#### K-Way Merge Fallback

For edge cases where primary-range-only export isn't possible — node failures, incomplete
snapshots, or recovery from backup where node identity is lost — CQLite's `KWayMerger`
provides a fallback. It accepts multiple SSTable inputs, orders by token, and resolves
conflicts via last-write-wins timestamp comparison.

```
# Fallback: when node-level filtering isn't available
Node 1 SSTables ─┐
Node 2 SSTables ──┼── KWayMerger ── Deduplicated Stream ── Iceberg
Node 3 SSTables ─┘
```

---

## 9. Proposed CQLite→Iceberg Pipeline

### 9.1 Pipeline Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    SSTable Source                                │
│  Local disk  │  S3/GCS/ADLS  │  Sidecar snapshot  │  CDC      │
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              CQLite SSTable Reader                             │
│  Discovery service → Schema resolution → SSTable enumeration  │
│  Optional: Token range filter for primary-range-only export   │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              SELECT Projections                                │
│  Column selection → WHERE predicates → LIMIT                  │
│  Streaming: 1,024-row buffer, ~11MB peak memory               │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Arrow RecordBatch Builder                         │
│  CQL→Arrow type mapping (Section 5)                           │
│  Accumulate rows → Flush every batch_size rows                │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Iceberg Writer Pipeline                           │
│  ParquetWriter → DataFileWriter → FanoutPartitionWriter       │
│  Row group size: 128MB │ Compression: Zstd                    │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Iceberg Catalog                                   │
│  Transaction → fast_append() → commit()                       │
│  REST  │  AWS Glue  │  Hive Metastore  │  Local FS           │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              Storage                                           │
│  S3  │  GCS  │  ADLS  │  HDFS  │  Local filesystem           │
└──────────────────────────────────────────────────────────────┘
```

### 9.2 Proposed CLI Command

```bash
# Basic export: all rows from a table
cqlite export-iceberg \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --keyspace my_keyspace \
    --table my_table \
    --catalog-uri http://localhost:8181 \
    --catalog-type rest \
    --warehouse s3://my-bucket/warehouse

# With projections and filtering
cqlite export-iceberg \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --query "SELECT id, name, created_at FROM my_ks.my_tbl WHERE created_at > '2025-01-01'" \
    --catalog-uri http://localhost:8181 \
    --catalog-type rest \
    --warehouse s3://my-bucket/warehouse \
    --iceberg-table analytics.cassandra_export

# Export all tables in a keyspace
cqlite export-iceberg \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --keyspace my_keyspace \
    --all-tables \
    --catalog-uri http://localhost:8181 \
    --catalog-type rest \
    --warehouse s3://my-bucket/warehouse

# Parquet-only mode (no catalog, local files)
cqlite export-parquet \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --keyspace my_keyspace \
    --table my_table \
    --output /tmp/parquet-export/

# Per-node export (primary range only — no deduplication needed)
# Get token range from: nodetool ring, system.local, or system.peers
cqlite export-iceberg \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --keyspace my_keyspace \
    --all-tables \
    --token-range -3074457345618258603..-1024819115206086201 \
    --catalog-uri http://iceberg-rest:8181 \
    --catalog-type rest \
    --warehouse s3://my-bucket/warehouse
```

### 9.3 Partition Strategy Mapping

Cassandra partition keys can inform Iceberg partitioning for query performance:

| Cassandra Concept | Iceberg Equivalent | Mapping Strategy |
|------|------|------|
| Partition key (single column) | Identity partition | Direct mapping |
| Composite partition key | Identity partition (multiple cols) | Each PK column becomes a partition field |
| Clustering columns | Sort order | Map to Iceberg sort order |
| Token (Murmur3) | Bucket transform | `bucket(N, partition_key)` |
| TimeUUID partition | Time-based partition | `hour(timestamp)` or `day(timestamp)` |

**Default strategy**: Use Cassandra's partition key as Iceberg partition fields with identity
transform. This preserves the data locality that Cassandra already provides.

### 9.4 Schema Creation

When the target Iceberg table doesn't exist, CQLite auto-creates it:

```
CQL Schema (CREATE TABLE)
    ↓ Parse
TableSchema { partition_key, clustering_columns, regular_columns }
    ↓ Map types (Section 5)
Iceberg Schema { fields: [IcebergField { name, type, required }] }
    ↓ Add partition spec
PartitionSpec { partition_key_columns → Identity transform }
    ↓ Add sort order
SortOrder { clustering_columns → ASC/DESC as defined }
    ↓ Create table
catalog.create_table(ident, schema, partition_spec, sort_order)
```

### 9.5 Memory Profile

| Stage | Memory | Notes |
|-------|--------|-------|
| SSTable reader | ~8 KB per file | Buffered reads |
| Streaming buffer | ~11 MB | 1,024 rows × row size |
| RecordBatch | ~10-50 MB | Depends on batch size and row width |
| Parquet writer | ~128 MB | Row group buffer |
| **Total peak** | **~150-200 MB** | Configurable via batch size |

This is 10-20× less than a Spark-based approach (2GB+ baseline).

---

## 10. Implementation Roadmap

### Phase 1: Parquet Export (4-8 weeks)

**Goal**: `cqlite export-parquet` produces valid Parquet files from SSTables.

| Task | Effort | Description |
|------|--------|-------------|
| CQL→Arrow type mapper | 2 weeks | Map all CQL types to Arrow DataTypes |
| RecordBatch builder | 1 week | Accumulate CQL rows into columnar batches |
| ArrowWriter integration | 1 week | Write RecordBatches to Parquet files |
| CLI command | 1 week | `export-parquet` with schema/projection flags |
| Collection handling | 1-2 weeks | Analytics mode (JSON) for collections/UDTs |
| Testing & validation | 1 week | All 33 test tables → Parquet → DuckDB verification |

**Dependencies**: `arrow` (v57+), `parquet` (v57+)

**Deliverable**: Single binary that reads SSTables and writes Parquet files. No catalog needed.
Queryable immediately by DuckDB, Spark, Trino, etc.

### Phase 2: Iceberg Catalog Integration (4-6 weeks)

**Goal**: `cqlite export-iceberg` writes to Iceberg tables with full catalog support.

| Task | Effort | Description |
|------|--------|-------------|
| Iceberg schema creation | 1 week | CQL schema → Iceberg schema + partition spec |
| DataFileWriter integration | 1-2 weeks | Wrap ParquetWriter with Iceberg metadata |
| REST catalog support | 1 week | Connect to REST catalog, create/load tables |
| Transaction management | 1 week | fast_append, atomic commits, retry logic |
| AWS Glue catalog | 1 week | Optional second catalog backend |

**Dependencies**: `iceberg` (v0.8+), `iceberg-catalog-rest`, `iceberg-catalog-glue`

**Deliverable**: Full Iceberg integration with REST and Glue catalogs.

### Phase 3: Schema Evolution & Metadata (3-4 weeks)

**Goal**: Handle schema changes gracefully and expose CQL metadata.

| Task | Effort | Description |
|------|--------|-------------|
| Schema evolution detection | 1 week | Detect schema changes between exports |
| Iceberg schema evolution | 1 week | Add/drop/rename columns via Iceberg API |
| CQL metadata columns | 1 week | Optional `_cql_write_time`, `_cql_ttl` columns |
| Full type decomposition | 1 week | Nested Parquet structs for collections/UDTs |

**Deliverable**: Production-grade schema handling with optional CQL metadata preservation.

### Phase 4: Multi-Node & Streaming (3-5 weeks)

**Goal**: Handle multi-node export via primary-range filtering and CDC-based incremental updates.

| Task | Effort | Description |
|------|--------|-------------|
| Token range filtering | 1 week | `--token-range` flag, Murmur3 partition filtering per node |
| Incremental export | 1-2 weeks | Track last-exported generation, append new data |
| CDC consumer (optional) | 2 weeks | Consume Sidecar CDC → Iceberg append |
| Tombstone handling | 1 week | Configurable: filter, flag, or preserve tombstones |
| K-way merge (fallback) | — | Already implemented; available for incomplete datasets |

**Deliverable**: Complete multi-node export via primary-range-only strategy (no deduplication
needed) and optional real-time CDC. K-way merge available as fallback for edge cases.

### Feature Flag Strategy

```toml
[features]
# Phase 1
parquet-export = ["dep:arrow", "dep:parquet"]

# Phase 2
iceberg = ["parquet-export", "dep:iceberg", "dep:iceberg-catalog-rest"]
iceberg-glue = ["iceberg", "dep:iceberg-catalog-glue"]
iceberg-hms = ["iceberg", "dep:iceberg-catalog-hms"]

# Phase 4
iceberg-cdc = ["iceberg", "dep:rdkafka"]
```

---

## 11. Use Cases & Value Proposition

### 11.1 Analytics on Operational Data

**Problem**: Querying Cassandra directly for analytics impacts production performance.
Analytics queries (full scans, aggregations) conflict with Cassandra's optimized read path.

**Solution**: Export SSTables to Iceberg offline. Query with Trino, Spark, or DuckDB without
touching the production cluster.

```bash
# Export overnight
cqlite export-iceberg \
    --data-dir /backup/cassandra/daily/2026-02-09 \
    --schema schema.cql \
    --all-tables \
    --catalog-uri http://iceberg-rest:8181

# Query with Trino
trino> SELECT date, count(*) FROM iceberg.analytics.events GROUP BY date;
```

### 11.2 Historical Data Archival

**Problem**: Cassandra TTLs delete data permanently. Compliance requirements may demand
longer retention.

**Solution**: Export before TTLs expire. Iceberg preserves the data with time-travel
capabilities.

```bash
# Monthly archival cron job
cqlite export-iceberg \
    --data-dir /var/lib/cassandra/data \
    --schema schema.cql \
    --keyspace audit_logs \
    --all-tables \
    --catalog-uri http://iceberg-rest:8181

# Query historical data with time travel
trino> SELECT * FROM iceberg.archive.audit_events
       FOR TIMESTAMP AS OF TIMESTAMP '2025-06-01 00:00:00';
```

### 11.3 Data Lake / Lakehouse Integration

**Problem**: Cassandra data is siloed. Joining with data from other systems requires ETL
pipelines.

**Solution**: Iceberg tables are accessible from any lakehouse engine. Cross-system joins
become trivial.

```sql
-- Join Cassandra user data with PostgreSQL order data (both in Iceberg)
SELECT u.name, o.total
FROM iceberg.cassandra_export.users u
JOIN iceberg.postgres_export.orders o ON u.id = o.user_id;
```

### 11.4 ML Feature Engineering

**Problem**: ML pipelines need reproducible snapshots of training data. Cassandra doesn't
provide point-in-time consistency.

**Solution**: Each Iceberg export creates an immutable snapshot. ML pipelines reference
specific snapshot IDs for reproducibility.

```python
import pyiceberg
catalog = pyiceberg.catalog.load_catalog("rest", uri="http://localhost:8181")
table = catalog.load_table("ml_features.user_profiles")

# Always train on the same snapshot
df = table.scan(snapshot_id=12345).to_pandas()
model.fit(df)
```

### 11.5 Cassandra Migration / Retirement

**Problem**: Migrating off Cassandra requires keeping data accessible during and after
transition.

**Solution**: Export all keyspaces to Iceberg. Applications gradually migrate reads from
Cassandra to Iceberg-backed analytics engines.

```bash
# Parallel per-node export — each node exports only its primary token range
# No deduplication needed: primary-range-only guarantees zero overlap

# Node 1 (tokens: -9223372036854775808..-3074457345618258603)
cqlite export-iceberg \
    --data-dir /backup/cassandra/nodes/node1/data \
    --schema schema.cql \
    --all-tables \
    --token-range -9223372036854775808..-3074457345618258603 \
    --catalog-uri http://iceberg-rest:8181 &

# Node 2 (tokens: -3074457345618258603..3074457345618258602)
cqlite export-iceberg \
    --data-dir /backup/cassandra/nodes/node2/data \
    --schema schema.cql \
    --all-tables \
    --token-range -3074457345618258603..3074457345618258602 \
    --catalog-uri http://iceberg-rest:8181 &

# Node 3 (tokens: 3074457345618258602..9223372036854775807)
cqlite export-iceberg \
    --data-dir /backup/cassandra/nodes/node3/data \
    --schema schema.cql \
    --all-tables \
    --token-range 3074457345618258602..9223372036854775807 \
    --catalog-uri http://iceberg-rest:8181 &

wait  # All nodes export in parallel
```

### 11.6 Development & Testing

**Problem**: Developers need production-like data for testing but can't access production
clusters.

**Solution**: Export production SSTables to local Parquet/Iceberg files. Developers query
locally with DuckDB.

```bash
# Export production snapshot
cqlite export-parquet \
    --data-dir /backup/cassandra/prod-snapshot \
    --schema schema.cql \
    --keyspace my_app \
    --output ./test-data/

# Query locally
duckdb -c "SELECT * FROM read_parquet('./test-data/users/*.parquet') LIMIT 10;"
```

### 11.7 Value Proposition Summary

| Benefit | CQLite Approach | Alternative |
|---------|----------------|-------------|
| **No cluster needed** | Read SSTables directly | Spark Connector needs live cluster |
| **Lightweight** | ~150 MB memory, single binary | Spark: 2GB+ JVM + cluster |
| **Fast iteration** | Seconds to start | Minutes to provision Spark |
| **Full projections** | SQL WHERE, aggregations | Limited in bulk readers |
| **Iceberg-native** | Direct catalog integration | Multi-hop pipeline |
| **Rust performance** | Zero-copy parsing, SIMD | JVM garbage collection |

---

## 12. Sources

### Apache Iceberg

1. [Apache Iceberg Official Documentation](https://iceberg.apache.org/docs/latest/)
2. [Apache iceberg-rust GitHub Repository](https://github.com/apache/iceberg-rust) — v0.8.0
3. [iceberg crate on crates.io](https://crates.io/crates/iceberg)
4. [Iceberg Rust API Documentation](https://rust.iceberg.apache.org/api/iceberg/index.html)
5. [Iceberg Writer API](https://rust.iceberg.apache.org/api/iceberg/writer/index.html)
6. [Iceberg ParquetWriter](https://rust.iceberg.apache.org/api/iceberg/writer/file_writer/struct.ParquetWriter.html)
7. [Iceberg Catalog Management: Hive, Glue, and Nessie](https://www.conduktor.io/glossary/iceberg-catalog-management-hive-glue-and-nessie)
8. [Iceberg Catalogs 2025: Emerging Catalogs](https://www.e6data.com/blog/iceberg-catalogs-2025-emerging-catalogs-modern-metadata-management)
9. [Choosing an Iceberg Catalog (lakeFS)](https://lakefs.io/blog/iceberg-catalog/)

### Apache Arrow / Parquet (Rust)

10. [Apache Arrow Rust Documentation](https://docs.rs/arrow/latest/arrow/)
11. [Arrow RecordBatch API](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html)
12. [Parquet ArrowWriter API](https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html)
13. [parquet crate on crates.io](https://crates.io/crates/parquet) — v57.2.0

### Apache Cassandra Sidecar

14. [Apache Cassandra Sidecar GitHub](https://github.com/apache/cassandra-sidecar)
15. [CEP-40: Data Transfer API](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-40)
16. [CEP-44: Kafka CDC Integration via Sidecar](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-44:+Kafka+integration+for+Cassandra+CDC+using+Sidecar)
17. [Cassandra Sidecar + Analytics Integration (dev@cassandra mailing list)](https://www.mail-archive.com/dev@cassandra.apache.org/msg27531.html)

### Apache Cassandra Analytics

18. [Apache Cassandra Analytics GitHub](https://github.com/apache/cassandra-analytics)
19. [CEP-28: Reading and Writing Cassandra Data with Spark Bulk Analytics](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28:+Reading+and+Writing+Cassandra+Data+with+Spark+Bulk+Analytics)
20. [CASSANDRA-16222: Spark Cassandra Bulk Reader](https://issues.apache.org/jira/browse/CASSANDRA-16222)
21. [spark-cassandra-bulkreader (Original Repository)](https://github.com/jberragan/spark-cassandra-bulkreader)

### DataStax Tools

22. [DataStax sstable-to-arrow GitHub](https://github.com/datastax/sstable-to-arrow)
23. [Analyzing Cassandra Data Using GPUs (NVIDIA Blog)](https://developer.nvidia.com/blog/analyzing-cassandra-data-using-gpus-part-1/)
24. [DataStax CDC Connector Documentation](https://docs.datastax.com/en/cdc-for-cassandra/)

### Netflix Patterns

25. [Netflix Marken Service: Cassandra + Iceberg (InfoQ)](https://www.infoq.com/news/2023/02/netflix-annotations-cassandra/)
26. [Netflix CDC Events from Cassandra (InfoQ Presentation)](https://www.infoq.com/presentations/netflix-cdc-events-cassandra/)
27. [Netflix: Keeping Cassandra and Iceberg in Sync (Netflix Tech Blog)](https://netflixtechblog.medium.com/)
28. [Netflix Data Bridge: Simplifying Data Movement (Netflix Tech Blog, Jan 2026)](https://netflixtechblog.medium.com/data-bridge-how-netflix-simplifies-data-movement-36d10d91c313)

### Industry Analysis

29. [RisingWave: Implementing Iceberg Compaction in Rust](https://risingwave.com/blog/implementing-iceberg-compaction-rust/)
30. [Databend: Iceberg Integration](https://databend.rs/)
31. [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)
32. [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
33. [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

### CQLite Internal References

34. `cqlite-core/src/storage/serialization/types.rs` — CQL type serialization (1,047 lines)
35. `cqlite-core/src/storage/write_engine/export.rs` — SSTable export (945 lines)
36. `cqlite-core/src/storage/write_engine/merge.rs` — K-way merge (1,029 lines)
37. `cqlite-core/src/storage/write_engine/mod.rs` — Write engine API (2,049 lines)
38. `cqlite-cli/src/cli_types.rs` — CLI output formats (417 lines)
39. `docs/sstables-definitive-guide/` — SSTable format reference

### Cassandra Source

40. [Apache Cassandra 5.0 Source](https://github.com/apache/cassandra/tree/cassandra-5.0.0)
41. [Cassandra SSTable Format Documentation](https://cassandra.apache.org/doc/latest/cassandra/architecture/sstable/)

---

*Report generated February 2026. Crate versions and API details should be verified against
current releases before implementation begins.*
