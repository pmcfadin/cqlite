---
title: Limitations
description: What CQLite can and cannot read — honest about unsupported formats, index types, and known gaps.
sidebar:
  label: Limitations
  order: 6
---

# Limitations — What CQLite Can and Cannot Read

CQLite is production-ready for the common case: reading Cassandra 5.0 BIG-format
SSTables with standard data types. This page is honest about what it cannot do yet,
so you know before you depend on it.

For the exhaustive engineering detail, see [Appendix F: Known Limitations](/cqlite/sstable-format/appendix-f/) in the SSTable Format Guide.

## Format support

| SSTable format | Cassandra versions | CQLite support |
|----------------|-------------------|----------------|
| `nb-*-big-*` (BIG format) | Cassandra 5.0+ | **Full** — all 33 test tables pass |
| `md-*` | Cassandra 4.0–4.1 | **Not supported** |
| `mc-*` | Cassandra 3.11 | **Not supported** |
| `la-*`, `ma-*` | Cassandra 3.x | **Not supported** |
| BTI format (Partitions.db / Rows.db) | Cassandra 5.0 opt-in | **Partial** — see below |

CQLite targets Cassandra 5.0 exclusively. If you need older formats, export your
data with Cassandra's `sstabledump` tool first.

## Index types

### BIG format — full support

The default Cassandra 5.0 index format (`nb-*-big-Index.db` / `nb-*-big-Summary.db`)
is fully supported. All 33 test tables in the CQLite test corpus use this format and
pass validation against `sstabledump` output.

### BTI format — partial support

BTI (trie-based index) is an opt-in, experimental feature in Cassandra 5.0, requiring
`selected_format: bti` in `cassandra.yaml`. It produces `Partitions.db` and `Rows.db`
files instead of the standard `Index.db`.

Current BTI status:

- Format detection works (magic number `0x6461`)
- Byte-comparable encoding (CEP-25) is implemented
- Trie node structure parsing is partially implemented
- Range queries and full partition iteration are **not implemented**
- No BTI test data exists in the test corpus (BTI requires explicit opt-in at the cluster level)

**In practice**: because BTI requires explicit opt-in, it is rarely used in production.
If you use BTI, CQLite will fall back to a sequential scan of `Data.db` which is
functionally correct but O(n) instead of O(log n) for partition lookups.

## Data type support

All CQL primitive types are supported. Collections and complex types are fully
supported in read mode (all 33 test tables, including UDTs, frozen collections, and
nested collections, pass 100% of validation tests).

| Type category | Examples | Read support | Write support |
|---------------|---------|-------------|--------------|
| Primitives | `text`, `int`, `uuid`, `timestamp`, `boolean`, `blob`, `inet` | Full | Full |
| Large numerics | `varint`, `decimal`, `counter` | Full | Full |
| Collections | `list<T>`, `set<T>`, `map<K,V>` | Full | Full |
| Frozen collections | `frozen<list<T>>`, `frozen<map<K,V>>` | Full | Full |
| User-defined types (UDTs) | `CREATE TYPE …` | Full | Full |
| Tuples | `tuple<T1, T2>` | Full | Full |
| Nested collections | `map<text, frozen<list<int>>>` | Full | Full |

## Write support limitations

CQLite M5.1 introduces SSTable write support. The implementation is correct and
produces Cassandra-compatible SSTables, but includes some known trade-offs:

### Promoted index deferred

`Index.db` entries always write `promoted_index_length = 0`.

**Impact**: wide partitions with 10 000+ rows per partition cannot use fast
within-partition seeks. CQLite must scan rows linearly within the partition.

- Narrow partitions (less than 100 rows): no impact
- Wide partitions (10 000+ rows): O(n) linear scan within the partition

### BTI format writing not implemented

The write engine produces BIG-format SSTables only. BTI-format writing
(`Partitions.db`, `Rows.db`) is not implemented.

**Rationale**: BTI is opt-in in Cassandra 5.0 and covers less than 5% of production
deployments. BIG format covers all current use cases.

### IndexWriter memory buffering

The `IndexWriter` buffers all index entries in memory until `finish()` is called.

**Impact**: approximately 20 MB per 1 million partitions. For extremely large SSTables
(hundreds of millions of partitions), split writes into multiple generation files.

### Compaction not yet executable

The k-way merge compaction API is defined (STCS policy, `maintenance_step()`, etc.)
but execution requires M5.3 SSTable reader integration to convert entries back to
mutations. `set_merge_policy()` currently returns an error.

**Impact**: `maintenance_step()` currently performs flush operations only. Full
compaction is deferred to M5.3.

## Query engine limitations

| Feature | Status |
|---------|--------|
| `SELECT` with `LIMIT` | Full |
| `SELECT` with partition-key `WHERE` | Full |
| `SELECT` with clustering-key `WHERE` | Partial (point-lookup path works; range filtering via residual scan) |
| `ORDER BY` | Not implemented |
| `INSERT` / `UPDATE` / `DELETE` via CQL | Requires write-support feature flag; write mutations via API |
| Aggregate functions (`COUNT`, `SUM`, etc.) | Not implemented |
| `GROUP BY` | Not implemented |

## Collection tombstone gap (issue #493)

Set element tombstones — individual element deletions inside a `set<T>` — are not
fully surfaced. Rows containing only element tombstones may appear empty rather
than absent. This affects a narrow edge case and is tracked in
[issue #493](https://github.com/pmcfadin/cqlite/issues/493) for v0.9.1.

## Operational constraints

- **Local files only**: CQLite reads SSTable files from the local filesystem.
  There is no network protocol, no cluster connection, and no Cassandra driver.
- **No live cluster writes**: You can write SSTables offline and load them into
  Cassandra with `nodetool refresh`, but CQLite does not connect to a running cluster.
- **Single-node perspective**: CQLite reads one SSTable at a time. It has no
  knowledge of replication, consistency levels, or coordinator routing.
- **Memory target**: CQLite targets less than 128 MB for files up to 1 GB.
  Files larger than 1 GB may require the streaming API or a partition-key filter.

## Workarounds for unsupported scenarios

### Cassandra 3.x / 4.x SSTables

Upgrade your cluster to Cassandra 5.0 and run:

```bash
nodetool upgradesstables
```

or use Cassandra's `sstabledump` to export to JSON and reimport.

### BTI format (trie index)

If your Cassandra cluster is configured with `selected_format: bti`, CQLite will
still return correct results via sequential scan fallback. For large tables the
scan may be slow; partition-key `WHERE` filters help bound the scan.

### Wide partitions

For partitions with thousands of rows and a specific clustering-key range,
a `WHERE` clause that includes the partition key will let CQLite locate the
partition quickly via the index and then scan within it:

```cql
SELECT * FROM my_ks.my_table
WHERE user_id = 42 AND timestamp > '2025-01-01'
LIMIT 1000;
```
