# Finding: `count(*)` on the `cqlite` catalog does not complete

Recorded here because #4137 says a demo whose result contradicts the story is still reported, and
because this changes D2's query list. The other three D2 queries in #4137 are unaffected.

## What happens

```sql
SELECT count(*) FROM cqlite.cassandra_easy_stress.keyvalue
```

ran for **10.83 minutes** in state `RUNNING` with:

```
processedInputPositions   0
processedInputDataSize     0B
```

Zero rows ever flowed. Retried under a 420 s bound: no output, no completion. Cancelled via
`DELETE /v1/query/<id>`.

The same query against the `cassandra` catalog completes in **17.8 s** and returns
22,339,536 — so the table is readable and the query is well-formed.

## It is not a scan problem

The decisive comparison: a **full-table aggregate over the same table** through the *same*
`cqlite` catalog finishes fine.

```sql
SELECT sum(length(value)) FROM cqlite.cassandra_easy_stress.keyvalue
-- 9,490 ms (median of 3), 22,339,536 rows, 3,451,144,643 bytes, FINISHED
```

And the LIMIT ladder is linear with no cliff (`d2-scan-limit-ladder.csv`): 5 → 245 ms,
1M → 27,190 ms. So the scan path, the snapshot resolution, the Arrow egress and the connector's
split handling all work. Whatever stalls is specific to `count(*)` — most plausibly an
aggregation-pushdown path that produces no rows and never terminates, since `count(*)` is the one
query here that needs no column data.

## Server-side signature

While the query was stuck, exactly one Flight pod burned ~2 cores and logged the **same three
SSTables being re-opened repeatedly**, roughly every 1–2 minutes:

```
Loaded CompressionInfo.db for NB format: algorithm=LZ4Compressor, chunk_length=16384, chunks=19027
Loaded CompressionInfo.db for NB format: algorithm=LZ4Compressor, chunk_length=16384, chunks=48082
Loaded CompressionInfo.db for NB format: algorithm=LZ4Compressor, chunk_length=16384, chunks=192731
```

(01:33:13, 01:34:42, 01:36:51, 01:37:34, …) — a re-open loop, not forward progress. The three
`chunks=` values match the table's 3 SSTables per node, so it is re-opening the whole set each time.

## Consequence for the talk

#4137's D2 lists `SELECT count(*)` as the first of four queries. **It is replaced by
`sum(length(value))` as the full-scan probe**, which is a strictly better probe anyway: it forces
every row's `value` to be read, whereas `count(*)` can in principle be answered from metadata and
would make a scan comparison less meaningful.

No chart claims a `count(*)` number for the `cqlite` catalog. The `cassandra`-catalog
`count(*)` timings are retained only as the row-count oracle.

## Environment

Full pins in `../REPORT.md`. Briefly: image `talk017` @ `sha256:847c93ba…c82980` (trunk
`f22ce842b`), connector `0.16.1`, Trino 481, Cassandra 5.0.9, `read-mode=snapshot`,
`local-datacenter=us-west-2`, 3× i4i.2xlarge + 2× m6i.2xlarge.
