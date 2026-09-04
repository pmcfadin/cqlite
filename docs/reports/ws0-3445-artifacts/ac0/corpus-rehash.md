# AC0 precondition — corpus re-hashed ON DISK by an INDEPENDENT tool (issue #3445)

Corpus root: `/data/ws0-3096`  ·  table dir `ws0/events`  ·  host `ip-172-31-5-53`

The #3248 corpus lived on a DIFFERENT EC2 instance (`ip-172-31-7-163`, i-04ac0a860eef7f241);
this lane runs on `ip-172-31-5-53`, where `/data/ws0-3096` did not exist. The corpus was therefore
REGENERATED from the pinned seed (`ws0-corpus-gen --seed 30960001 --rows 4000000
--rows-per-partition 100`) rather than copied, and then verified two INDEPENDENT ways:

1. the generator's own `--verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json`
   (field-by-field; verdict `PASS`, see `corpusgen-run.log` excerpt in this directory), and
2. this table — every component re-hashed from the bytes on disk by **`hashlib.sha256` in a
   separate process**, not by the generator that wrote them. An identity file's claim is not the
   corpus, and neither is the writer's own opinion of what it just wrote.

| component | pinned sha256 | on-disk sha256 | pinned bytes | on-disk bytes | match |
|---|---|---|---|---|---|
| `nb-1-big-CRC.db` | `b38c33aeb85a9267…` | `b38c33aeb85a9267…` | 169364 | 169364 | **YES** |
| `nb-1-big-Data.db` | `4a903f6fa27c04db…` | `4a903f6fa27c04db…` | 2774760422 | 2774760422 | **YES** |
| `nb-1-big-Digest.crc32` | `81530bedf46a526b…` | `81530bedf46a526b…` | 10 | 10 | **YES** |
| `nb-1-big-Filter.db` | `bb1c7b82ed2b256c…` | `bb1c7b82ed2b256c…` | 47936 | 47936 | **YES** |
| `nb-1-big-Index.db` | `94c1aa0e28d27387…` | `94c1aa0e28d27387…` | 4196097 | 4196097 | **YES** |
| `nb-1-big-Statistics.db` | `aff85755f8946fce…` | `aff85755f8946fce…` | 5252 | 5252 | **YES** |
| `nb-1-big-Summary.db` | `77126573c848b5a4…` | `77126573c848b5a4…` | 6308 | 6308 | **YES** |
| `nb-1-big-TOC.txt` | `ae4483b226c87145…` | `ae4483b226c87145…` | 80 | 80 | **YES** |

All 8 components match pin: **True**

## Corpus shape (the inputs every per-row figure in this report divides by)

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
- `data_db_sha256` = `4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269`

## Schema and the var-len column count

```
CREATE TABLE ws0.events (part_id text, seq int, event_time timestamp, blob_a blob, blob_b blob,
  device_id uuid, metric_a int, metric_b bigint, metric_c double, payload text, region text,
  status text, PRIMARY KEY (part_id, seq, event_time))
  WITH CLUSTERING ORDER BY (seq ASC, event_time ASC);
```

12 columns. **6 var-len** (`part_id`, `blob_a`, `blob_b`, `payload`, `region`, `status`) and 6
fixed-width (`seq`, `event_time`, `device_id`, `metric_a`, `metric_b`, `metric_c`).

The var-len count is load-bearing for THIS issue, not just inherited from #3248: a var-len column
carries a VInt LENGTH prefix that a fixed-width column does not, so the per-row VInt population is
dominated by those 6 lengths plus the per-cell timestamp-delta VInts (`cell_value.rs:99`) and the
row-level framing VInts (`row_data.rs:886-906`). This is what makes the per-row VInt count, and
hence the expected share, computable rather than guessed — see `../ac1/`.

