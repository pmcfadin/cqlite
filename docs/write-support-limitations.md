# Write Support Limitations (v0.9.0)

This document lists the known constraints and open issues for CQLite write support
as of v0.9.0. Issues not listed here are expected to work correctly.

---

## Counter columns are not writable

**Behaviour**: `WriteEngine::write()` and `write_async()` return
`Error::InvalidOperation` immediately when a mutation contains a `Value::Counter`
cell. No data is written to the WAL or memtable.

**Reason**: Cassandra counter increments require distributed CAS semantics.
CQLite runs outside the cluster and cannot safely implement the read-before-write
protocol that Cassandra uses to increment counter values atomically.

**Workaround**: Write counter data directly via a live Cassandra cluster using
`cqlsh` or a Cassandra driver.

---

## BTI index writer not implemented

**Behaviour**: The SSTable writer always emits BIG-format index files
(`nb-{gen}-big-Index.db`, `nb-{gen}-big-Summary.db`). Trie-based BTI format
index files are not produced.

**Impact**: Flushed SSTables are readable by Cassandra 5.0 because Cassandra
supports both formats. The read path in CQLite supports BTI indexes for reading;
only the writer is BIG-format only.

**Tracking**: No separate issue; this is a planned enhancement for M6/M7.

---

## Python concurrent-query race (Issue #311)

**Behaviour**: Concurrent queries on the same `Database` handle from multiple
threads may encounter a race in schema metadata access. The symptom is an
occasional `QueryError` on one of the concurrent queries.

**Workaround**: Run one warm-up query (any SELECT) before spawning parallel
threads. This materialises the schema cache and makes subsequent concurrent
accesses safe.

```python
with cqlite.open(data_dir, schema=schema) as db:
    # Warm up schema cache before threading
    list(db.execute('SELECT * FROM ks.tbl LIMIT 1'))

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as pool:
        futures = [pool.submit(db.execute, 'SELECT * FROM ks.tbl') for _ in range(4)]
        results = [f.result() for f in futures]
```

**Tracking**: Issue #311.

---

## Set-element tombstone decoding (Issue #493)

**Behaviour** (resolved in v0.9.1): The V5CompressedLegacy parser now carries the
per-cell `is_deleted` flag out of complex-cell parsing and skips tombstoned set
(and list) elements. Data deleted at the element level
(via `DELETE my_set[element] FROM ...`) no longer appears as still-present in
query results.

**Tracking**: Issue #493 (fixed in v0.9.1).

---

## Schema-aware tuple decoding (Issue #501)

**Behaviour** (resolved in v0.9.1): The reader decodes tuple element types from the
schema's type string for arbitrary arity (e.g. `tuple<int, text, uuid>`), with
bounds-checked per-element parsing. Tuples that previously read back as `Null` or
`Blob` now decode to typed values.

**Tracking**: Issue #501 (fixed in v0.9.1).

---

## frozen<udt> field decoding (Issue #502)

**Behaviour** (resolved in v0.9.1): The reader now decodes `frozen<NAME>` columns
by resolving `NAME` through the UDT registry attached to the parser. When a
registered UDT definition is found the field bytes are decoded field-by-field into
a typed `Value::Udt` rather than a raw byte array.

**Requirement**: The UDT must be registered in the `UdtRegistry` before opening
the SSTable. If `NAME` is not present in the registry the reader returns a
`schema` error:

```
frozen<NAME>: UDT 'NAME' not found in registry for keyspace 'KS';
register it before reading
```

**Impact**: Read-only. Frozen UDTs created by the CQLite writer decode correctly.
The error is actionable: pass the UDT schema to `UdtRegistry::register_udt` and
re-open the reader.

**Tracking**: Issue #502.

---

## Compaction dropped input tombstones (Issue #505)

**Behaviour** (resolved in v0.9.2): The k-way compaction merger now surfaces row
and cell tombstones from input SSTables (via a dedicated compaction read path that
does not filter tombstones) and carries their authoritative `markedForDeleteAt`
timestamps. A higher-timestamp tombstone in a later SSTable now correctly shadows
a live row from an earlier SSTable after compaction.

**Tracking**: Issue #505 (fixed in v0.9.2).

---

## Compaction equal-timestamp Delete-vs-Live reconcile (Issue #498)

**Behaviour** (resolved in v0.9.2): At equal timestamp, the merger now resolves a
Delete-vs-Live conflict in favour of the tombstone, independent of which input
file it came from — matching Cassandra `Cells#reconcile`. Previously the newer
input file won regardless of liveness.

**Tracking**: Issue #498 (fixed in v0.9.2).

---

> The v0.9.2 reader correctness fixes (#516 `scan()` token ordering, #517
> `get()`/`scan()` consistency, #518 `stats().block_count`) are read-side; see the
> CHANGELOG and PRD §4.2 for details.
