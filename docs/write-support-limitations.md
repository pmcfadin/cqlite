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

**Behaviour**: Tombstones for individual set elements within a cell are silently
ignored during query. The parent set is returned but tombstoned elements may
appear as still-present.

**Impact**: Read-only. Writes are unaffected. Data that was deleted at the
element level (via `DELETE my_set[element] FROM ...`) may appear in query results.

**Tracking**: Issue #493. Planned for v0.9.1.

---

## Schema-aware tuple decoding (Issue #501)

**Behaviour**: Tuple fields are decoded without schema context in some edge cases,
returning raw byte arrays instead of typed values.

**Impact**: Read-only. Tuples created by the CQLite writer decode correctly.
Tuples in pre-existing SSTables written by Cassandra may be affected.

**Tracking**: Issue #501.

---

## frozen<udt> field decoding (Issue #502)

**Behaviour**: Fields within frozen UDT values may be returned as raw byte arrays
in some edge cases rather than as typed `dict` values.

**Impact**: Read-only. Frozen UDTs created by the CQLite writer decode correctly.
Pre-existing frozen UDTs from Cassandra may be affected.

**Tracking**: Issue #502.
