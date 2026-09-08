# Flight metric time series, exported from the run's VictoriaMetrics

**These are ephemeral data rescued from the cluster.** VictoriaMetrics lived inside the `talk017`
k3s cluster and was destroyed by teardown; nothing here can be reconstructed from the other CSVs
in `../`, which record per-query *elapsed time* and not rates, RSS, or counters. Exported before
`easy-db-lab down`.

Window: the full run, 15 s step. Source: `cqlite-flight` pods on db0/db1/db2 exporting via OTLP to
the per-node otel-collector, into VictoriaMetrics.

| File | PromQL |
|---|---|
| `rows_served_per_sec.tsv` | `sum(rate(cqlite_rpc_rows_total[1m]))` |
| `rows_read_per_sec.tsv` | `sum(rate(cqlite_read_rows_total[1m]))` |
| `rows_served_per_pod.tsv` | `rate(cqlite_rpc_rows_total[1m])` |
| `rows_read_per_pod.tsv` | `rate(cqlite_read_rows_total[1m])` |
| `bytes_served_per_sec.tsv` | `sum(rate(cqlite_rpc_bytes_total[1m]))` |
| `proc_rss_bytes.tsv` | `cqlite_proc_rss_bytes` |
| `index_parses_total.tsv` | `cqlite_sstable_index_parses_total` |
| `index_interval_parses_total.tsv` | `cqlite_sstable_index_interval_parses_total` |
| `rpc_in_flight_ratio.tsv` | `cqlite_rpc_in_flight_ratio` |
| `admission_in_use_ratio.tsv` | `cqlite_flight_admission_in_use_ratio` |
| `errors_total.tsv` | `cqlite_errors_total` |
| `partitions_per_sec.tsv` | `sum(rate(cqlite_read_partitions_total[1m]))` |
| `sstables_open.tsv` | `cqlite_sstables_open` |
| `merge_rows_out_per_sec.tsv` | `sum(rate(cqlite_merge_rows_out_total[1m]))` |
| `proc_threads.tsv` | `cqlite_proc_threads` |

Format: `unix_ts <TAB> series <TAB> value`. `series` is the pod/instance label, or `agg` for a
summed expression.

## Two corrections these files force on the sibling CSVs

1. **`d4-ladder.csv` says a real VmRSS reading "was NOT collected".** That is wrong —
   `cqlite_proc_rss_bytes` was being recorded the whole time. It is also not merely a correction of
   provenance: real RSS peaks at **3.5–5.9 GiB**, i.e. HIGHER than the 2–3 GiB `kubectl top` figures
   that CSV attributed to page-cache inflation. The memory question is therefore open, not explained.
2. **`d6-coldstart.csv` reports `index_parses_total` as UNAVAILABLE.** It is **127**, not 0. #2412's
   lazy Summary-guided open predicts an O(summary) open with no full `Index.db` parse, so a nonzero
   count needs explaining rather than assuming.

Both are recorded as open questions in `../../REPORT.md`; neither is silently patched over.
