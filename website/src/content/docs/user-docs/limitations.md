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

This page covers what CQLite **cannot do by design or yet**. For active bugs and
sharp edges in the current release, see [Known Issues](/cqlite/user-docs/known-issues/);
for what is coming next, see the [Roadmap](/cqlite/user-docs/roadmap/).

For the exhaustive engineering detail, see [Appendix F: Known Limitations](/cqlite/sstable-format/appendix-f/) in the SSTable Format Guide.

## Format support

| SSTable format | Cassandra versions | CQLite support |
|----------------|-------------------|----------------|
| `nb-*-big-*` (BIG format) | Cassandra 5.0+ | **Full** — all 33 test tables pass |
| `oa-*-big-*` (BIG format) | Cassandra 5.0 | **Full** — 6 `oa` fixture tables pass `sstabledump` parity |
| `md-*` | Cassandra 4.0–4.1 | **Not supported** |
| `mc-*` | Cassandra 3.11 | **Not supported** |
| `la-*`, `ma-*` | Cassandra 3.x | **Not supported** |
| `da-*-bti-*` / BTI format (Partitions.db / Rows.db) | Cassandra 5.0 opt-in | **Not supported** — detected and rejected with a clear error; see below |

CQLite targets Cassandra 5.0 exclusively. If you need older formats, export your
data with Cassandra's `sstabledump` tool first.

## Index types

### BIG format — full support

The default Cassandra 5.0 index format (`nb-*-big-Index.db` / `nb-*-big-Summary.db`)
is fully supported. All 33 test tables in the CQLite test corpus use this format and
pass validation against `sstabledump` output.

### BTI format (`da`) — not yet supported, fails cleanly

BTI (trie-based index) is an opt-in feature in Cassandra 5.0, enabled with
`selected_format: bti` in `cassandra.yaml`. It produces `da-*-bti-*` SSTables with
`Partitions.db` and `Rows.db` trie indexes instead of the standard
`Index.db` / `Summary.db`.

As of v0.12.0, CQLite **reads `da`-format (BTI) SSTables end-to-end** — a dedicated
trie-walk read path with `ByteComparable` decode and Data.db chaining, validated
against `sstabledump` goldens in the `datasets-v3` test set (#897). CQLite can also
**write** canonical `da`-format SSTables with `Partitions.db`/`Rows.db` trie indexes
(#872).

**In practice**: BTI requires explicit cluster opt-in (`selected_format: bti`) and is
less common than the default BIG format (`nb` / `oa`), but `da`-format files are now a
first-class read and write target rather than a rejected one.

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

### Compaction is STCS-only

STCS (size-tiered) compaction executes via `set_merge_policy()` and
`maintenance_step()`. Other strategies (LCS, TWCS, UCS) are not implemented — a
non-STCS policy is not selectable.

**Impact**: SSTables written and compacted by CQLite follow size-tiered behavior.
This matches the most common Cassandra default and is sufficient for offline
write/flush/compact workflows.

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

If your Cassandra cluster is configured with `selected_format: bti`, CQLite detects
the `da-*-bti-*` SSTables and rejects them with a clear error rather than misreading
them. Convert them to the default BIG format first — run `nodetool upgradesstables`
after switching `selected_format` back to `big`, or export with `sstabledump`.
End-to-end BTI read support is on the [roadmap](/cqlite/user-docs/roadmap/)
([#660](https://github.com/pmcfadin/cqlite/issues/660)).

### Wide partitions

For partitions with thousands of rows and a specific clustering-key range,
a `WHERE` clause that includes the partition key will let CQLite locate the
partition quickly via the index and then scan within it:

```cql
SELECT * FROM my_ks.my_table
WHERE user_id = 42 AND timestamp > '2025-01-01'
LIMIT 1000;
```
