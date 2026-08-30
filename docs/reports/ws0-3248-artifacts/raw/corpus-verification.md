# Corpus verification — re-hashed ON DISK, not read from the identity file (issue #3248, AC0 precondition)

Corpus root: `/data/ws0-3096`  ·  table dir `ws0/events`

The identity file *claims* the pin. A claim about a corpus is not the corpus, so every
component was re-hashed from the bytes on disk and compared to the recorded value.

| component | recorded sha256 | on-disk sha256 | match |
|---|---|---|---|
| `nb-1-big-CRC.db` | `b38c33aeb85a9267…` | `b38c33aeb85a9267…` | **YES** |
| `nb-1-big-Data.db` | `4a903f6fa27c04db…` | `4a903f6fa27c04db…` | **YES** |
| `nb-1-big-Digest.crc32` | `81530bedf46a526b…` | `81530bedf46a526b…` | **YES** |
| `nb-1-big-Filter.db` | `bb1c7b82ed2b256c…` | `bb1c7b82ed2b256c…` | **YES** |
| `nb-1-big-Index.db` | `94c1aa0e28d27387…` | `94c1aa0e28d27387…` | **YES** |
| `nb-1-big-Statistics.db` | `aff85755f8946fce…` | `aff85755f8946fce…` | **YES** |
| `nb-1-big-Summary.db` | `77126573c848b5a4…` | `77126573c848b5a4…` | **YES** |
| `nb-1-big-TOC.txt` | `ae4483b226c87145…` | `ae4483b226c87145…` | **YES** |

All components match: **True**

## The pin named by the issue

Issue #3248 names the corpus by `sha256 4a903f6f…ae269` (Data.db).
On-disk Data.db sha256: `4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269`
Prefix/suffix match against the issue's `4a903f6f…ae269`: **True**

## Corpus shape (inputs to later per-row arithmetic)

- `issue` = `#3096`
- `seed` = `30960001`
- `table` = `ws0.events`
- `rows` = `4000000`
- `partitions` = `40000`
- `rows_per_partition` = `100`
- `cells_per_row` = `12`
- `data_db_bytes` = `2774760422`
- `bytes_per_row` = `693.6901055`
- `total_component_bytes` = `2779185469`
- `compression_info_present` = `False`
- `schema_sha256` = `6bdd1d06ad7eb597b3103ace250930b28b19a76aa128bbf2e4170c90406baed0`

## Schema (12 columns; var-len count is the P2 prediction target)

```
CREATE TABLE ws0.events (part_id text, seq int, event_time timestamp, blob_a blob, blob_b blob, device_id uuid, metric_a int, metric_b bigint, metric_c double, payload text, region text, status text, PRIMARY KEY (part_id, seq, event_time)) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC);
```

Var-len columns (owned String/Vec materialization before append): `part_id`, `blob_a`,
`blob_b`, `payload`, `region`, `status` = **6**. Fixed-width: `seq`, `event_time`,
`device_id`, `metric_a`, `metric_b`, `metric_c` = 6. So prediction **P2** (allocations/row
~= var-len column count) predicts **~6 allocations/row** from that source alone.
