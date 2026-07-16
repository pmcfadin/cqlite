# CQLite Flight metrics — operator reference

> GENERATED from the observability catalog (`cqlite-core/src/observability/catalog.rs` + `operator_docs.rs`) by `cargo run -p cqlite-core --example gen_operator_metrics_doc`. Do NOT edit by hand — edit the catalog/annotations and regenerate. The `operator-metrics-doc` agent-gate component fails if this file drifts from the catalog (issue #2426).

Operator-facing reference for every `cqlite.*` instrument CQLite emits over Arrow Flight and the storage/write/compaction paths. Names, units, and bounded attribute sets come straight from the code, so this page cannot silently diverge from what a running server exposes.

Related: the Flight/Trino operator docs (`docs/flight-trino/`) and the round scoreboard template (issue #2399) link back to the entries here.

Total instruments: **68**.

## All instruments

| Metric | Type | Unit | Attributes | Operator meaning | Healthy vs alarming |
|---|---|---|---|---|---|
| `cqlite.cache.key.capacity_bytes` | gauge | `By` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). The global key cache's fixed byte budget (0 when block caching is disabled). | A fixed named constant inside the <128MB envelope; not a user knob. Zero means the read caches are disabled. |
| `cqlite.cache.key.evictions` | counter | `1` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). Global key-cache entries evicted to stay within the byte budget (budget-driven; distinct from invalidations). | Sustained growth means the hot location set exceeds the fixed budget; expected to be flat on a small working set. |
| `cqlite.cache.key.hits` | counter | `1` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). Hits on the process-global key→partition-offset cache (a point read skips the Index.db interval parse / trie descent). | A high hit rate means hot partitions are resolving from cache; near-zero on a cold or highly-random point-read workload. |
| `cqlite.cache.key.invalidations` | counter | `1` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). Global key-cache entries dropped on generation removal/compaction/warm-evict (distinct from budget evictions). | Tracks generation turnover (compaction/refresh); a #2383 rebind over a byte-identical generation does NOT invalidate. |
| `cqlite.cache.key.misses` | counter | `1` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). Misses on the global key cache (incl. fail-closed identity mismatch); each pays one interval parse / trie descent then populates. | Rises with working-set churn or eviction pressure; a persistently high miss ratio suggests the budget is small vs the hot set. |
| `cqlite.cache.key.resident_bytes` | gauge | `By` | _(none)_ | Surfaced only via `Database::stats().memory_stats` — NOT emitted as a live OTel instrument (not scrapeable from Prometheus/an OTel collector). Approximate resident footprint of the process-global key cache. | Bounded above by capacity_bytes; a single global cap regardless of how many readers are open. |
| `cqlite.compaction.budget.consumed` | histogram | `s` | _(none)_ | Maintenance budget actually consumed per maintenance_step call. | Consumed materially exceeding requested means maintenance is overrunning its budget. |
| `cqlite.compaction.budget.requested` | histogram | `s` | _(none)_ | Maintenance budget requested per maintenance_step call. | Compare against budget.consumed to confirm the scheduler honors its ~10% tolerance. |
| `cqlite.compaction.bytes_written` | counter | `By` | _(none)_ | Bytes written to compaction output SSTables. | Compare with write.bytes/flush.bytes to reason about write amplification from compaction. |
| `cqlite.compaction.duration` | histogram | `s` | _(none)_ | Compaction run durations. | Use with compaction.rows_merged for merge throughput; a growing tail means compaction is falling behind. |
| `cqlite.compaction.finalize.duration` | histogram | `s` | _(none)_ | Compaction finalize (atomic rename / publication-barrier) durations. | Normally small; a growing tail points at slow filesystem rename/barrier operations. |
| `cqlite.compaction.lag` | gauge | `{sstable}` | _(none)_ | L0 SSTables currently pending compaction (compaction lag). | A stable low level is healthy; a monotonic rise means compaction cannot keep up with flush. |
| `cqlite.compaction.rows_merged` | counter | `{row}` | _(none)_ | Rows emitted by the k-way merge across all compactions. | With compaction.duration this yields rows-merged-per-second; a drop signals compaction stalling. |
| `cqlite.compaction.sstables_in` | counter | `{sstable}` | _(none)_ | Input SSTables consumed by compactions. | In vs out ratio is the consolidation factor; in >> out is healthy consolidation. |
| `cqlite.compaction.sstables_out` | counter | `{sstable}` | _(none)_ | Output SSTables produced by compactions. | See compaction.sstables_in; out approaching in means little consolidation is happening. |
| `cqlite.compaction.tombstones_emitted` | counter | `{tombstone}` | _(none)_ | Tombstone markers retained (carried forward, not purgeable) into merge output. | Expected while data is within gc_grace; a persistent climb means tombstones are not aging out. |
| `cqlite.compaction.tombstones_purged` | counter | `{tombstone}` | _(none)_ | Tombstones genuinely purged (gc-grace/overlap-safe) during compaction. | Rising means space is being reclaimed; zero while tombstones accumulate means purge is blocked (gc_grace/overlap). |
| `cqlite.compaction.tombstones_suppressed` | counter | `{tombstone}` | _(none)_ | Live cells/rows shadowed by a tombstone during merge reconciliation. | Suppression without a matching purge or retained marker is the resurrection-risk smell. |
| `cqlite.compression.ratio` | histogram | `1` | `cqlite.compression` | Per-chunk compression ratio (compressed/uncompressed; <=1.0 means the chunk shrank). | Ratios near 1.0 mean incompressible data; a sudden rise means less benefit from the configured codec. |
| `cqlite.errors.total` | counter | `{error}` | `cqlite.error.category`<br>`cqlite.subsystem` | Canonical error-rate signal, keyed by bounded {category, subsystem}; eagerly registered at 0 on startup. | Absent metric name = error counting isn't wired (never 'no errors'); any sustained increase is the error-rate alarm. |
| `cqlite.flight.admission.in_use` | gauge | `1` | _(none)_ | do_get admission permits currently held (admitted, in-flight scans only). | Below the limit is healthy headroom; sitting at admission.limit with a non-zero waiting gauge is saturation. |
| `cqlite.flight.admission.limit` | gauge | `1` | _(none)_ | The configured do_get admission ceiling K (--max-concurrent-scans); constant while the server runs. | Chart admission.in_use against this ceiling; in_use pinned at the limit means the server is saturated. |
| `cqlite.flight.admission.rejected_total` | counter | `1` | _(none)_ | do_get requests rejected (gRPC UNAVAILABLE) because no permit freed within the wait timeout. | Should stay 0 under normal load; any sustained increase means clients are being shed and should fail over. |
| `cqlite.flight.admission.wait_seconds` | histogram | `s` | _(none)_ | How long a do_get waited on acquire before it was admitted or rejected. | A near-zero distribution is healthy; a rising tail means requests are increasingly queuing for permits. |
| `cqlite.flight.admission.waiting` | gauge | `1` | _(none)_ | do_get requests parked waiting for an admission permit — the backpressure signal. | Zero is healthy; a sustained non-zero value means offered concurrency exceeds the ceiling and requests are queuing. |
| `cqlite.flight.blocking_tasks_in_use` | gauge | `{thread}` | _(none)_ | Flight spawn_blocking tasks currently outstanding (flight-managed proxy, NOT the global tokio pool queue depth). | Rises with concurrent do_get scans and returns to baseline; a level pinned near the blocking-pool size with flat rpc.rows is blocking-pool saturation. Distinct from admission.in_use. |
| `cqlite.flush.bytes` | counter | `By` | _(none)_ | Data.db bytes produced by memtable flushes. | Write volume landing on disk from flush; use with flush.duration for flush throughput. |
| `cqlite.flush.duration` | histogram | `s` | _(none)_ | Memtable-to-SSTable flush durations. | A rising tail alongside a climbing memtable size means flush cannot drain the memtable fast enough. |
| `cqlite.flush.rows` | counter | `{row}` | _(none)_ | Rows flushed from the memtable to L0 SSTables. | Should track write.mutations over time; a lag is the flush-behind signal. |
| `cqlite.flush.sstables` | counter | `{sstable}` | _(none)_ | L0 SSTables created by memtable flushes. | Feeds compaction.lag; a high rate with rising lag means L0 is accumulating faster than compaction clears it. |
| `cqlite.memtable.rows` | gauge | `{row}` | _(none)_ | Buffered rows in the active memtable. | Same sawtooth shape as memtable.size_bytes; a rising floor signals flush pressure. |
| `cqlite.memtable.size_bytes` | gauge | `By` | _(none)_ | Current approximate in-memory size of the active memtable. | Sawtooth (rise then drop at flush) is healthy; a monotonic climb means flush is not keeping up. |
| `cqlite.merge.egress_channel_depth` | gauge | `{entry}` | _(none)_ | Live occupancy of the bounded merge egress sync_channel (cap 256) feeding do_get / compaction. | Near zero = consumer keeping up (or a stalled producer); riding near capacity = producer outrunning a slower consumer (back-pressured egress). |
| `cqlite.merge.producer_threads` | gauge | `{thread}` | _(none)_ | Live OS producer threads the k-way merge currently holds (one per input SSTable). | Bounded by O(M) inputs and returns to baseline at merge completion; a stuck-high value is a thread leak (#2316). |
| `cqlite.merge.rows_in` | counter | `{row}` | _(none)_ | Input rows consumed at the k-way merge reconcile boundary (once per merge). | Compare with merge.rows_out; the gap is rows removed by last-write-wins collapse and tombstone suppression. |
| `cqlite.merge.rows_out` | counter | `{row}` | _(none)_ | Rows emitted by the merge reconcile boundary post-reconciliation. | See merge.rows_in; rows_out much smaller than rows_in means heavy reconciliation. |
| `cqlite.proc.fds` | gauge | `{fd}` | _(none)_ | Process open fd count sampled from /proc/self/fd (Linux; absent off-/proc, never 0). | Rises as concurrent scans open SSTables (no reader pool, #815); a level nearing the container ulimit is the fd-exhaustion binding point. |
| `cqlite.proc.rss_bytes` | gauge | `By` | _(none)_ | Process resident set size from /proc/self/status VmRSS (Linux; absent off-/proc, never 0). | Rises with concurrent scan payloads (Flight bypasses the result-byte budget); a level nearing the memory limit is the OOMKill risk. |
| `cqlite.proc.threads` | gauge | `{thread}` | _(none)_ | Process OS thread count sampled from /proc/self/task (Linux; absent off-/proc, never 0). | Rises with concurrent scans and settles to baseline; a level climbing toward the thread ceiling is thread/scheduler collapse. |
| `cqlite.query.degraded_path.total` | counter | `1` | `cqlite.query.fallback_reason` | SELECTs that took a soundness fallback to a full-scan read path, tagged by fallback reason. | Should stay near 0 for targeted queries; a climbing value means queries are silently degrading to full scans. |
| `cqlite.query.duration` | histogram | `s` | `cqlite.subsystem` | End-to-end query execution durations. | Watch p99; a rising tail is the top-level query-latency alarm. |
| `cqlite.query.rows` | counter | `{row}` | `cqlite.query.access_path`<br>`cqlite.query.plan_type` | Rows returned to callers by the query engine. | Compare with query.rows_scanned; a large gap is read amplification. |
| `cqlite.query.rows_scanned` | counter | `{row}` | `cqlite.query.access_path` | Rows examined by the SELECT scan step before filter/projection/LIMIT. | ≈ query.rows for a point lookup; far larger means a full_scan is doing the work. |
| `cqlite.read.bloom.checks` | counter | `1` | `cqlite.result`<br>`cqlite.sstable.format` | Bloom-filter / BTI-trie present-or-absent checks; miss = definitely absent. | A high miss-rate is healthy pruning; pairing with partition_lookup reveals the bloom false-positive rate. |
| `cqlite.read.bloom.false_negatives` | counter | `1` | `cqlite.sstable.format` | Opt-in soundness alarm: keys found on an authoritative scan that the presence oracle said were absent. | MUST stay 0. Any non-zero value is a corruption/soundness alarm (only emitted when verification is enabled). |
| `cqlite.read.bytes` | counter | `By` | `cqlite.sstable.format`<br>`cqlite.compression` | Total Data.db bytes read (post-decompression). | Track against read.rows to spot read amplification; a spike with flat rows means wide scans. |
| `cqlite.read.duration` | histogram | `s` | `cqlite.sstable.format` | Distribution of single read/scan operation durations. | Watch p99; a growing tail is the read-latency alarm. |
| `cqlite.read.partition_lookup.total` | counter | `1` | `cqlite.result`<br>`cqlite.read.lookup_route`<br>`cqlite.sstable.format` | Partition point lookups attempted, tagged hit/miss so a dashboard computes the hit ratio from one series. | A healthy point-read workload is dominated by hits; a miss-heavy ratio means keys are absent or mis-routed. |
| `cqlite.read.partitions` | counter | `{partition}` | `cqlite.sstable.format` | Total partitions scanned by the read path. | Rising in step with read.rows is healthy; many partitions for few rows indicates a full scan. |
| `cqlite.read.rows` | counter | `{row}` | `cqlite.sstable.format` | Total rows materialised by the read path (climbs incrementally during a long Flight merge scan). | Steadily rising under load is healthy; flat while a scan is in flight suggests a stall. |
| `cqlite.read.scan.window_refill` | counter | `1` | _(none)_ | Windowed streaming-scan refills at a compression-chunk boundary (a partition straddles the chunk edge and the driver awaits the next decompressed chunk). | Non-zero proves the multi-chunk stitch path was exercised; it stays 0 for single-chunk SSTables. A runaway value means excessive chunk-boundary straddling. |
| `cqlite.read.sstables_pruned` | counter | `{sstable}` | `cqlite.sstable.format` | SSTables skipped by a definitive presence-oracle negative (bloom/BTI-trie miss). | Higher is better — it is the dashboard-honest prune signal; zero on a point read means no pruning happened. |
| `cqlite.rpc.bytes` | counter | `By` | `cqlite.rpc.method` | Record-batch payload bytes streamed to clients by do_get (pre-IPC-framing). | Tracks egress volume; pair with rpc.rows to see batch sizing. |
| `cqlite.rpc.duration` | histogram | `s` | `cqlite.rpc.method`<br>`cqlite.rpc.status` | Arrow Flight RPC handler durations (includes admission wait time). | Watch do_get p99; localize a rising tail with rpc.phase.duration. |
| `cqlite.rpc.in_flight` | gauge | `1` | `cqlite.rpc.method` | Arrow Flight RPCs currently being handled. | A sustained-high value with flat rpc.rows is a stall; distinct from admission.in_use (all RPCs, not just admitted scans). |
| `cqlite.rpc.phase.active` | gauge | `1` | `cqlite.rpc.method`<br>`cqlite.rpc.phase` | In-flight phase a do_get is CURRENTLY in (1 on entry, 0 on exit) — visible before the phase completes. | stream=1 for a whole hang exposes a wedged do_get that phase.duration would never record; admission=1 exposes an admission queue wait. |
| `cqlite.rpc.phase.duration` | histogram | `s` | `cqlite.rpc.method`<br>`cqlite.rpc.phase` | Per-phase do_get wall time across the closed set validate/admission/resolve/merge_setup/stream. | Localizes WHERE a slow do_get spent time: piling up in merge_setup, queued in admission, or stuck parsing in validate. |
| `cqlite.rpc.requests` | counter | `1` | `cqlite.rpc.method`<br>`cqlite.rpc.status` | Arrow Flight RPC requests served, tagged by method + ok/error. | Compute per-method error rate from the status arm; a rising error fraction is the RPC-health alarm. |
| `cqlite.rpc.rows` | counter | `{row}` | `cqlite.rpc.method` | Rows returned to clients by do_get (emitted per record batch). | Climbing = healthy long scan; flat while rpc.in_flight>0 = a stalled do_get. |
| `cqlite.sstable.index_interval_parses_total` | counter | `1` | _(none)_ | Bounded Summary-guided Index.db interval parses (one per point lookup); distinct from whole-file parses. | Grows ~1 per point lookup; a cold lazy open adds 0. Whole-file spikes stay visible on index_parses_total. |
| `cqlite.sstable.index_parses_total` | counter | `1` | _(none)_ | Whole-file BIG/NB Index.db parses; the scale-free probe for the #2383 resolve-phase CPU spin. | Healthy ≈ sum of generations per query; climbing far past that means redundant re-parses (a #2383-class regression). |
| `cqlite.sstables.open` | gauge | `{sstable}` | `cqlite.sstable.format` | SSTables currently held open. | A stable level is healthy; unbounded growth is a handle/file-descriptor leak. |
| `cqlite.storage.open.bytes` | counter | `By` | _(none)_ | Total on-disk Data.db bytes across the SSTables discovered at open. | The dataset size an open must account for; use with open.sstables to size cold-start cost. |
| `cqlite.storage.open.sstables` | counter | `{sstable}` | _(none)_ | SSTables discovered and opened per StorageEngine open, summed over process lifetime. | Tracks how much on-disk state each open touches; sudden growth signals compaction lag or fan-out. |
| `cqlite.storage.open.tables` | counter | `1` | _(none)_ | Logical tables represented by the SSTables discovered at open. | Baseline inventory count; unexpected changes mean schema/keyspace drift on disk. |
| `cqlite.wal.sync.duration` | histogram | `s` | _(none)_ | WAL fsync durations. | A growing tail points at slow/contended disk — the write-durability latency alarm. |
| `cqlite.warm.cache.evicts` | counter | `1` | _(none)_ | Warm generations evicted by LRU byte-budget pressure or removal on disk. | A high evict rate with a low hit ratio means the warm byte budget is too small for the working set. |
| `cqlite.warm.cache.hits` | counter | `1` | _(none)_ | Flight warm-handle cache hits served from warm parsed state with zero reader-open. | A high hit ratio (hits vs misses) is the warm-cache win; a low ratio means generations keep changing. |
| `cqlite.warm.cache.misses` | counter | `1` | _(none)_ | Flight warm-handle cache misses (no cached entry, or generation set changed). | Pair with warm.cache.hits for the hit ratio; a rising miss rate erodes the warm-open speedup. |
| `cqlite.warm.cache.refresh` | counter | `1` | `cqlite.warm.refresh_outcome` | Warm-handle refresh outcomes, tagged unchanged/rebuilt_delta/fail_closed_retained. | Mostly unchanged/rebuilt_delta is healthy; a rising fail_closed_retained arm means refreshes are failing and serving stale-but-safe state. |
| `cqlite.write.bytes` | counter | `By` | _(none)_ | Data.db bytes produced by the SSTable writer across all outputs. | Total on-disk write volume; use with compaction.bytes_written to reason about write amplification. |
| `cqlite.write.mutations` | counter | `{row}` | _(none)_ | Mutations accepted by the write path (one per successful memtable insert). | Tracks write throughput; flatlining under offered load means writes are blocked. |
| `cqlite.write.partitions` | counter | `{partition}` | _(none)_ | Partitions written by the SSTable writer (flush + compaction output). | Baseline write-shape signal; interpret alongside write.bytes. |

## Round-scoreboard mapping (issue #2399)

Metrics a #2399 round-template scoreboard item consumes. Round handoffs (#2367-style) link the item to its metric here instead of re-explaining it.

| Metric | Scoreboard item |
|---|---|
| `cqlite.cache.key.capacity_bytes` | read-path perf (#2059) |
| `cqlite.cache.key.evictions` | read-path perf (#2059) |
| `cqlite.cache.key.hits` | read-path perf (#2059) |
| `cqlite.cache.key.invalidations` | read-path perf (#2059) |
| `cqlite.cache.key.misses` | read-path perf (#2059) |
| `cqlite.cache.key.resident_bytes` | read-path perf (#2059) |
| `cqlite.compaction.lag` | compaction-lag watch (#2399) |
| `cqlite.errors.total` | error-rate watch (#2193/#2399) |
| `cqlite.flight.admission.in_use` | admission saturation (#2420/#2399) |
| `cqlite.flight.admission.limit` | admission saturation (#2420/#2399) |
| `cqlite.flight.admission.rejected_total` | admission saturation (#2420/#2399) |
| `cqlite.flight.admission.wait_seconds` | admission saturation (#2420/#2399) |
| `cqlite.flight.admission.waiting` | admission saturation (#2420/#2399) |
| `cqlite.flight.blocking_tasks_in_use` | blocking-pool pressure watch (#2419/#2313) |
| `cqlite.merge.egress_channel_depth` | egress backpressure watch (#2419/#2399) |
| `cqlite.merge.producer_threads` | thread-budget watch (#2313/#2399) |
| `cqlite.proc.fds` | fd-exhaustion watch (#2419/#2313) |
| `cqlite.proc.rss_bytes` | memory-pressure watch (#2419/#2313) |
| `cqlite.proc.threads` | thread-budget watch (#2419/#2313) |
| `cqlite.rpc.duration` | do_get latency (#2367/#2399) |
| `cqlite.rpc.phase.active` | do_get hang detection (#2361/#2399) |
| `cqlite.rpc.phase.duration` | do_get phase breakdown (#2398/#2399) |
| `cqlite.rpc.requests` | rpc-error-rate watch (#2399) |
| `cqlite.rpc.rows` | do_get progress (#2367/#2399) |
| `cqlite.sstable.index_interval_parses_total` | index-parse audit (#2367/#2399) |
| `cqlite.sstable.index_parses_total` | index-parse audit (#2367/#2399) |
| `cqlite.warm.cache.hits` | warm-cache hit ratio (#2310/#2399) |
| `cqlite.warm.cache.misses` | warm-cache hit ratio (#2310/#2399) |

