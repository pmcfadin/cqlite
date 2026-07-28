# WS0 — stock-Cassandra read-throughput baseline (issue #3026)

**Date:** 2026-07-27
**Issue:** #3026 (WS0 of umbrella #3023; program epic #2817)
**Purpose:** produce the first cqlite-vs-Cassandra throughput comparison in this repo, and a
re-runnable method for it. Prior to this, every "we are slower than stock Cassandra" statement in the
0.17 program was an intuition, not a measurement.

---

## 1. Verdict

**The headline is two numbers, and they point in opposite directions.**

| Comparison | Result |
|---|---|
| **Read + materialise** (nothing shipped) | **CQLite is 1.18× stock Cassandra** — 367,760 rows/s (1 physical core) vs 311,196 (1 hardware thread), warm — mixed pinnings, see below |
| **Ship every row to a client** | **CQLite Flight is 0.29× — Cassandra is 3.50× FASTER** — 60,892 vs 212,981 rows/s per physical core, warm |

The 1.18× grants Cassandra its *better* pinning (its 1-hw-thread figure against CQLite's 1-core figure).
There are **two** matched pinnings and they straddle it rather than both beating it: 1-core↔1-core is
**1.24×**, 1-hw-thread↔1-hw-thread is **1.13×** — *below* the headline. So treat 1.18× as mid-range, not
as a conservative floor.

This is **not** a CQLite win. On the read path the advantage is a margin, not a multiple. On the honest
end-to-end surface — the only one comparable to Cassandra actually serving rows to a client — CQLite
loses by 3.5×.

**Strategic consequence:** the 0.17 program is optimising the read path, where CQLite already leads,
while the measured deficit sits in serialization/egress, which no #3023 workstream owned at the time of
writing. Follow-ups filed: **#3058** (the dominant, confirmed cause), #3060, #3061.

---

## 2. Environment — read this before comparing any absolute number

| | Value |
|---|---|
| Instance | **c7i.4xlarge** — 16 vCPU / **8 physical cores**, Xeon Platinum 8488C (Sapphire Rapids) |
| Memory | 30 GiB + a 16 GiB swapfile (`vm.swappiness=10`), **added mid-run** as an OOM safety valve, not a tuning change (see the heap note below) |
| Storage | **EBS only.** Both devices report `Amazon Elastic Block Store` (`nvme0n1`→`/`, `nvme1n1`→`/data`) |
| Kernel | 6.17.0-1019-aws |
| Cassandra | **5.0.8** (`1722270...`), OpenJDK 17.0.19 |
| CQLite | 0.16.1 release + DWARF |

> **⚠️ This is NOT the i4i hardware that produced the 0.17 program's ground truth.** `c7i` is an
> EBS-only family; local NVMe is on `c7id`/`i4i`. **The ratios in §1 transfer; the absolute rows/s do
> not.** A trap to avoid: instance metadata lists `ephemeral0`/`ephemeral1`, but no such devices exist —
> those are stale AMI block-device declarations, and reading them as "instance store present" would
> silently poison any I/O measurement taken here.

### Cassandra configuration — one disclosed deviation from stock

`cassandra.yaml` deltas are **paths + listen/rpc addresses only**, with one exception:

**`MAX_HEAP_SIZE=8G` was set explicitly.** Stock auto-sizing on a 30 GiB box does **not** give ~7.5 GiB:
`cassandra-env.sh` computes `heap_limit=15872 MB` (`:54`, exact) and `half_system_memory=15775 MB`
(`:21` uses `free -m` = 31551 on this box, and `expr 31551 / 2` = 15775), and because half <
limit it selects **half of RAM → a 15.4 GiB heap** (confirmed: an unbounded daemon reported
`Heap Memory (MB): 766.41 / 15776.00`). An unbounded daemon is what OOM-killed this box (recorded in
`head-to-head-method.md` §6 and `ws0-corpus/rerun.sh:40-45`; not an `fio` matter, so not in §8). Since
`file_cache_enabled=false` (§5), heap size should have little effect on a scan — but it is stated rather
than assumed. `HEAP_NEWSIZE` deliberately left unset: this build uses G1 and `cassandra-env.sh`
warns/ignores it under G1.

---

## 3. Corpus — Cassandra wrote it, so the bytes are authoritative

```sql
CREATE TABLE ws0.events (
    part_id text, seq int, event_time timestamp,
    blob_a blob, blob_b blob, device_id uuid,
    metric_a int, metric_b bigint, metric_c double,
    payload text, region text, status text,
    PRIMARY KEY (part_id, seq, event_time)
) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC)
  AND compression = {'chunk_length_in_kb': '16', 'class': '...LZ4Compressor'};
```

The DDL sets **no** compression clause — those values are the inherited stock default. 200,000
partitions × 20 rows. No collections or UDTs.

| Measured (not modeled) | Value |
|---|---|
| Rows | **3,999,890** — two independent oracles agree (`sstablemetadata totalRows`, and a 512-way token-range count) |
| Data.db (compressed) | 783,799,203 B |
| Uncompressed `dataLength` | 2,770,741,510 B (read from the CompressionInfo.db header) |
| **Uncompressed B/row** | **692.70** (target 690, +0.4%) |
| Compressed B/row | 195.96 |
| LZ4 ratio | **3.535×** — measured, not assumed |
| `chunk_length` | **16384 B**, verified in the CompressionInfo.db header *and* `system_schema.tables` |
| Format | **`nb` BIG**, **single SSTable** |

**Disclosure — flushed then `nodetool compact` to one SSTable.** A scan is therefore one sequential pass
with **no k-way merge**. This holds for Cassandra and for CQLite's bare scan; it is **false for CQLite's
Flight path**, which runs the merge anyway (#3058) — an assumption this baseline was built on and which
the measurement invalidated.

### Why the shipped fixtures could not be used

`test-data/datasets` tables are ~500–1000 rows and **L3-resident**, so per-scan fixed costs dominate and
no throughput number taken from them is meaningful. (An earlier draft of this work claimed the shipped
corpus had *no compressed tables*; that was wrong — it has 136 `*CompressionInfo.db`, including
LZ4 at `chunk_length: 16384`. The error came from `find -name 'CompressionInfo.db'`, which misses
generation-prefixed names like `nb-1-big-CompressionInfo.db`. The conclusion stands for the size reason,
not the compression reason.)

### Calibration gotcha, if regenerating

The dominant lever on the achieved LZ4 ratio is **not** the declared `population`: `cassandra-stress`
derives regular-column values from `(partition, LAST clustering value)`, so the **first** clustering
column's fan-out acts as a value-duplication factor. At identical row width, a `4×5` clustering split
measured **4.32×** and `2×10` measured **3.02×**. Blob cardinality is the fine knob (pop 5 → 4.09×,
8 → 3.85×, 16 → 3.53×). **Verify the achieved ratio by measurement.**

---

## 4. Results

Both engines read **byte-identical files** — all 8 components, re-verified before, between and after the
arms, and across a Cassandra drain+restart:

```
Data.db sha256 = 22d9ae224b439b2176c287a59eee6a7d1f08b4f1fafc4d2198b3da50cdce922c
```

Median of 3 replicates, spread <±2%. Physical core = CPUs 2+10.

| engine / surface | pin | rows/s | unc MB/s | cmp MB/s | cyc/row | IPC | mem B/row |
|---|---|--:|--:|--:|--:|--:|--:|
| **WARM** | | | | | | | |
| Cassandra `count(*)` | 1 hw thr | 311,196 | 215.6 | 61.0 | 10,837 | 3.73 | 9,650 |
| Cassandra `count(*)` | 1 core | 297,653 | 206.2 | 58.3 | *22,464* | 1.81 | 10,157 |
| Cassandra `SELECT *` | 1 hw thr | 198,002 | 137.2 | 38.8 | 16,363 | 3.15 | 17,654 |
| Cassandra `SELECT *` | 1 core | 212,981 | 147.5 | 41.7 | *30,061* | 1.71 | 18,008 |
| CQLite bare scan | 1 hw thr | 350,580 | 242.8 | 68.7 | 9,692 | 2.67 | **1,240** |
| **CQLite bare scan** | **1 core** | **367,760** | **254.7** | **72.1** | *17,648* | 1.57 | 1,933 |
| CQLite scan+Arrow | 1 hw thr | 215,207 | 149.1 | 42.2 | 15,564 | 2.03 | 14,147 |
| CQLite scan+Arrow | 1 core | 227,531 | 157.6 | 44.6 | *20,611* | 1.58 | 14,208 |
| CQLite Flight | 1 hw thr | 55,593 | 38.5 | 10.9 | 60,825 | 2.39 | 30,914 |
| CQLite Flight | 1 core | 60,892 | 42.2 | 11.9 | *96,260* | 1.52 | 32,438 |
| **COLD** (1 run each) | | | | | | | |
| Cassandra `count(*)` | 1 core | 295,717 | 204.8 | 57.9 | *22,675* | 1.80 | 10,604 |
| Cassandra `SELECT *` | 1 core | 209,023 | 144.8 | 41.0 | *30,593* | 1.69 | 18,740 |
| CQLite bare scan | 1 core | 350,904 | 243.1 | 68.8 | *17,700* | 1.56 | 2,361 |
| CQLite scan+Arrow | 1 core | 213,117 | 147.6 | 41.8 | *21,991* | 1.48 | 14,529 |
| CQLite Flight | 1 core | 59,542 | 41.2 | 11.7 | *96,767* | 1.52 | 32,910 |

`cyc/row` and IPC are valid only on the **1-hw-thread** rows (SMT sibling idle). Starred figures are the
2-sibling sum and double-count core cycles. **The `mem B/row` column carries the same caveat and is
equally 1-hw-thread-only** — a busy SMT sibling pollutes `l2_lines_in.all` (§10.5), and the effect is
large, not marginal: CQLite bare scan reads **1,240** B/row at 1 hw thread vs **1,933** at 1 core, a 56%
gap on a headline metric. All five COLD rows are 1-core and inherit this. MB/s basis: uncompressed
2,770,741,510 B, compressed 783,799,203 B — always state which.

**Cold ≈ warm within ~6.3% worst case (CQLite scan+Arrow 1-core, 227,531→213,117); all other arms within
4.6%. This workload is not I/O-bound on this box.**

### Fairness of the comparators

- Cassandra was measured over the **native CQL protocol**, which includes its own result serialization.
  Comparing a CQLite number with serialization *removed* against that would be unfair, which is why the
  Flight row is the headline end-to-end comparator and the bare scan is reported as the read-path ceiling.
- Confound named rather than argued away: Arrow IPC over gRPC is a **cheaper and structurally different**
  serialization than Cassandra's per-cell length-prefixed protocol, so this pairing is fair on the read
  leg and mildly favours CQLite on the serialization leg.
- **Cassandra's `count(*)` does not skip cell deserialisation** (verified from the profile), so it is a
  fair read-path comparator rather than a flattering one.
- **Correctness held**: CQLite returned exactly 3,999,890 rows, digest **`0xd1fba762150c532c`** — the
  `--no-fold` digest used for every reported number, and identical in **all 21** run artefacts that record
  one (21 of 21 `summary-*` and 21 of 21 `scan-*.json`; the 5 Flight summaries carry no digest field, so
  the Flight arm is row-count-verified only) — matching Cassandra's two oracles on the row count.
  (`0x4903ffa446163c4b` is the separate once-per-change `--fold` check; it appears in no run artifact.) **"12 cells per row" is unverified here**: it follows
  from the 12-column DDL, but the two summaries carrying a cell counter record `"cells": 0`, so this
  baseline did not measure it.

---

## 5. Where the time goes

**Cassandra `SELECT *`** (ReadStage 61.5% / NTR 38.5%): `ReadResponse.createDataResponse` →
`UnfilteredPartitionIterators$Serializer.serialize` = **58.4% inclusive.** A *local* read deserialises
from the SSTable, **re-serialises into an internal ReadResponse**, which NTR then re-decodes and
re-encodes (`ResultSet$Codec.encode` 11.5% self). `BTree.apply*` 53.0%. Actual SSTable iteration is only
**31.1%**. This structural tax is why CQLite beats Cassandra on the read path at all.

**CQLite bare scan**: cell/row decode ~22%, **LZ4 only 3.0%**, malloc/free ~10%, `QueryRow` drop +
`Value::into_owned` 6.1%, and **6.6% SipHashing column names into a per-row `HashMap<Arc<str>, Value>`**.
Decode is not the bottleneck; row materialisation and allocation are.

**CQLite Flight** — the 10× CPU gap, and **not** gRPC/IPC framing: `do_get` routes through
`MergeProducer` → `KWayMerger::reconcile_cluster_with_overlap_counted` → `build_compaction_row_data` →
`CompactionPolicy::on_data_row`, with a `HashMap<String, CellWriteMetadata>` **per cell**. ~13% of server
CPU is SipHash, ~16% allocator, **only ~5% actual cell decode**. Filed as **#3058**.

---

## 6. Cassandra's structural differences (AC#5) — cited

Source: `cassandra-5.0.8` tag. All verified by hand against the source.

| Finding | Citation |
|---|---|
| **256 KiB scan readahead is ACTIVE on a 16 KiB-chunk table.** Gate is `readAheadBufferSize > 0 && readAheadBufferSize > metadata.chunkLength()` → `262144 > 16384` = true. Only disabled at `chunk_length_in_kb >= 256`. | `io/util/CompressedChunkReader.java:236-238`; default 256 KiB at `config/Config.java:341`; `DEFAULT_CHUNK_LENGTH` 16 KiB at `schema/CompressionParams.java:47` |
| One 256 KiB `pread` per aligned block; each 16 KiB chunk memcpy'd out of it | `io/util/ThreadLocalReadAheadBuffer.java:99-113`, `:120-130` |
| Scan-only — point reads get none | `forScan()` reached only via `SSTableScanner.java:78`, `SSTableSimpleScanner.java:76` |
| Buffer keyed per `(thread, filePath)` — so interleaved scanners under merge don't thrash. **A single shared readahead buffer in CQLite would thrash.** | `ThreadLocalReadAheadBuffer.java:41-48, 90-93` |
| **Queue depth is 1 in Cassandra too** — one blocking positional read; zero vectored/async I/O; no `FADV_WILLNEED` | `io/util/ChannelProxy.java:141-152`; only `fadvise` use is `DONTNEED` at `utils/NativeLibrary.java:211-257` |
| **ChunkCache disabled by default** — so stock Cassandra re-decompresses every chunk, exactly like CQLite | `config/Config.java:499` + `CassandraRelevantProperties.java:227`; `conf/cassandra.yaml:742` |
| **Enabling the chunk cache DISABLES the readahead** — `isScan` is silently dropped. The two knobs fight. | `cache/ChunkCache.java:261-265` |
| `disk_access_mode` resolves to Data.db=pread, index=mmap | `config/DatabaseDescriptor.java:533-537`, `io/sstable/format/IndexComponent.java:30-34` |
| **Cassandra CRC32s every compressed chunk** (`crc_check_chance` = 1.0) — it does MORE per-chunk CPU work than CQLite, so per-chunk CPU cannot explain CQLite being slower | `schema/TableParams.java:359`, `io/util/CompressedChunkReader.java:190-197` |
| **Stock 5.0.8 writes `nb`, not `oa`** — `storage_compatibility_mode` defaults to `CASSANDRA_4`. A default 5.0 node will not produce BTI. | `io/sstable/format/big/BigFormat.java:343` |

### Empirically confirmed on the live node

- **Chunk cache OFF**, three independent ways: `system_views.settings` shows `file_cache_enabled false`
  (evidence A1); `nodetool info` prints no `Chunk Cache` line while KeyCache/RowCache/CounterCache **are**
  present (evidence A2 — and `Info.java:126-146` swallows `InstanceNotFoundException`, so absence means
  the MBean was never registered); the startup Config dump agrees (A4).
  *Not load-bearing:* evidence A3 attempted an MBean-name grep of a live-JVM dump and recorded
  `ChunkCache MBean name occurrences: 0` — but it also records `(sjk mxdump unavailable)`. **No dump was
  retained and no dump size was recorded, so A3 is not independently verifiable and should not be cited
  as the strongest line.** (The KeyCache/RowCache/CounterCache positive control belongs to A2, above, not
  to A3.) The conclusion stands on A1/A2/A4.
  *Red herring:* the log line `Global buffer pool limit is 512.000MiB for chunk-cache` **is** emitted —
  that is the buffer *pool*, not the cache.
- **256 KiB preads**, on a cold full scan: **4,041 of 4,067 preads (99.4%) were exactly 262,144 B**; the
  `[16K,32K)` bucket is **empty**; not one 16 KiB pread. The remainder were sub-16 KiB metadata reads plus
  one 250,787 B short read at EOF. `max_hw_sectors_kb=256` caps *device* I/O at 256 KiB, so the **syscall**
  histogram is the load-bearing evidence, not the device one.
- **~1.352× syscall-level read amplification** (1,059,659,423 B **returned** to userspace — `rchar` counts
  bytes returned, not requested — vs a 783,799,203 B file; **1.3520×**). The *requested* counter from
  bpftrace (`@pread_req_bytes`, a different run) is 1,059,588,899 B → **1.3519×**. The distinction is
  immaterial to the conclusion, but state which counter you mean. This happens because
  the readahead buffer is per-thread and two ReadStage threads re-fetched each other's blocks. The page
  cache absorbed it — device reads were **1.0002×** the file size — so it costs syscall+memcpy CPU, not
  I/O. Consequence: the same per-thread keying that makes the buffer merge-safe makes it redundant across
  threads on one file, so **CQLite could plausibly beat Cassandra here rather than merely match it**.

---

## 7. Roofline verdict (AC#3, AC#4)

### Measured memory bandwidth — the assumed 8 GB/s/core is CONSERVATIVE

STREAM-style, arrays ≥512 MiB (4.9× the 105 MiB L3), re-verified at 2 GiB with identical results.
**This table mixes two run sets — read the pin column.** The 4/8/16-thread rows come from the pinned
sweeps (`stream-with-read.txt`, `readbw-scaling.txt`); the two quoted 1-thread figures come from the
*unpinned* quiet runs (`stream-1t-quiet.txt`, `readbw-1t-quiet.txt`), so the pinned 1-thread pure-read
value is given alongside them.

| Threads | pin | Triad GB/s | Pure Read GB/s | Read GB/s per core |
|--:|---|--:|--:|--:|
| 1 | unpinned (quiet) | 13.49 | **10.84** | **10.84** |
| 1 | pinned (core 0) | 13.21 | **10.73** | **10.73** |
| 4 | pinned | 49.07 | 41.78 | 10.45 |
| 8 (all physical) | pinned | 85.22 | **81.16** | **10.15** |
| 16 (SMT) | pinned | 88.99 | 94.20 | — |

Saturation ≈ 89 GB/s triad / 94 GB/s read. **8 GB/s/core is achievable and conservative — the measured
single-core pure read is 34% above it on the pinned 10.73 and 35% above it on the unpinned 10.84** — and
survives the all-cores case where such assumptions usually die.

*Caveat:* the STREAM Copy/Scale/Add/Triad convention undercounts DRAM traffic (stores incur
read-for-ownership it does not count), which is why the read-only kernel leads — there, counted bytes =
actual DRAM bytes.

### Measured memory traffic per row — T3's denominator is REFUTED

Uncore IMC counters **do not exist in this VM** (hard null): `/sys/bus/event_source/devices/` has only
`breakpoint cpu kprobe msr software tracepoint uprobe`; `LLC-load-misses` is `<not supported>`;
`longest_lat_cache.miss`/`cache-misses`/`offcore_requests.*` appear in `perf list` but count **exactly 0**
(hypervisor-masked); `/sys/fs/resctrl` absent entirely, and `/sys/class/powercap` present **but empty**
(no RAPL zones), which is equally useless but is not the same fact.

Validated proxy: **`l2_lines_in.all × 64`**, calibrated against analytic ground truth at three
working-set scales with `ws0-cqlite/membw_cal.c` (medians of 3 replicates, `passes=340`, `taskset -c 2`;
retained output: `ws0-results/membw-calibration.txt`) —

| working set | vs bytes touched | vs analytic DRAM traffic | verdict |
|---|--:|--:|---|
| 512 MiB (4.9× L3) | **+1.08%** | **1.0×** | proxy **is** DRAM traffic |
| 32 MiB (< L3, > L2) | +0.74% | **342.5× over** | proxy measures L3→L2 refill |
| 1 MiB (< L2) | −99.34% (**~0**) | — | proxy collapses; nothing leaves L2 |

So it equals DRAM traffic **only** when the working set exceeds L3. (`passes` must be large: `membw_cal`
memsets before starting its own timer while `perf` counts the whole process, so one extra buffer-fill
amortizes as ~1/passes — the same 512 MiB point reads +15.5% at `passes=8` and +1.1% at `passes=340`.)

```bash
taskset -c 2 perf stat -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent -- <workload>
# bytes_of_memory_traffic_per_row = l2_lines_in.all * 64 / rows
```

**CQLite bare scan measures 1,240 B/row on this 783 MB (>L3) corpus vs the modeled 4.4 KB — ~3.5× too
high.** At the 350,580 rows/s measured on the same 1-hw-thread arm that is **0.435 GB/s ≈ 4.0% of one
core's 10.84 GB/s**, with IPC 2.67–3.73. (The earlier "0.55 GB/s ≈ 5%" mixed in a 291.7k rows/s figure
that is not a WS0 measurement; the corrected, self-consistent figure is *lower*, so it **strengthens** the
not-memory-bound conclusion.)

> **T3 (~1.8M rows/s/core, "memory-bandwidth bound") does not survive.** Both inputs were wrong in the
> same direction: bandwidth is 34–35% *higher* than assumed and traffic per row is ~3.5× *lower*. This
> scan is **instruction-throughput-bound**, not memory-bound, and the umbrella's "memory bandwidth binds
> before cycles" framing is refuted.

Suspected origin of the error: the modeled 4.4 KB/row is numerically identical to the measured **disk**
`rareq-sz` of 4.4 KB. Those are different quantities (bytes per disk *request* vs bytes of DRAM traffic
per *row*).

**Limit on the proxy, stated:** it equals DRAM traffic only for the tight Rust scan. Cassandra's 9,650
B/row is **13.9× its own decompressed stream** (9,650 / the measured 692.70 B/row), and Arrow's 14,147
B/row is likewise inflated; both are JVM allocation/GC and Arrow-builder churn refilling from **L3** — for
those arms the figure is an upper bound, not DRAM traffic. (The earlier "~12×" divided Cassandra's rate by
*CQLite's* 243 MB/s decompression floor, which is not "its" own stream — Cassandra's is 215.6 MB/s.)

### Do the tiers survive contact with Cassandra?

**Yes — but the framing does not.** Stock Cassandra sits at **297,653 rows/s per physical core** on the
read path (the `1 core` row of §4 — *not* §1's 311,196, which is its 1-hardware-thread figure), ~**0.50×**
T1, so the tiers need not be re-derived from its numbers. But T1 (600k) would **extend a lead CQLite
already holds**, while the real deficit is the 3.50× shipping-path loss.

---

## 8. `fio` — the efficiency denominator (AC#2: **OPEN**)

O_DIRECT, read-only, 48 GiB file (> 30 GiB RAM), ≥60 s/arm, 5 s ramp discarded, root volume.

| Arm | IOPS | MB/s | mean µs | p50 µs | p99 µs |
|---|--:|--:|--:|--:|--:|
| **4k QD1 randread** | 1,377 | **5.6** | 722 | 709 | 1,090 |
| 4k QD1 seqread | 1,385 | 5.7 | 719 | 709 | 1,012 |
| **1M QD32 randread** | 125 | **131.3** | 255,835 | 252,707 | 252,707 |
| 1M QD32 seqread | 125 | 131.3 | 255,860 | 252,707 | 252,707 |

Both bounds confirmed arithmetically:
- **4k/QD1 is LATENCY-bound, not IOPS-bound**: 1/722 µs = 1,385 IOPS ≈ the 1,377 observed, nowhere near
  gp3's 3,000 IOPS floor. **Buying provisioned IOPS would not move this number**; only latency would.
- **1M/QD32 is CAP-bound**: 32 MiB ÷ 125.2 MiB/s = 256 ms = the observed mean. The 250 ms latency is pure
  queueing behind a provisioned ceiling — so this arm is a poor denominator on principle, not just
  magnitude.
- **Sequential ≈ random at both sizes.** Under O_DIRECT there is no readahead and every EBS I/O is a
  network round trip, so locality buys nothing. Do not model a sequential-scan advantage on EBS.

Volume type **inferred** gp3 @ default 125 MiB/s / 3,000 IOPS (no AWS API credentials): the layout write
plateaued at 125.3 MiB/s, both 1M arms pinned at 125.2 MiB/s (stddev 0.9 randread / 1.0 seqread), and
300 s sustained showed **no decay**, ruling out gp2. Also `max_hw_sectors_kb=256`, so the kernel splits
each 1 MiB request into 4× 256 KiB device I/Os.

> **AC#2 remains OPEN.** These are correct **c7i/EBS** numbers, and they **cannot** replace the i4i
> instance-store denominator this AC exists to correct: prior work's 711.8 MB/s figure is **5.4×** what
> this box produces, and the "honest" i4i 4k/QD1 figure of 36 MB/s is **6.4×** ours. The two sets must
> never be combined in one ratio. Closing AC#2 requires `c7id`/`i4i` hardware.

---

## 9. Contradicted premises — carry these forward

1. **The 288,725 rows/s Cassandra reference is CLIENT-bound, not a Cassandra capability.** Reproduced
   exactly at `--inflight 1` (292,849 rows/s / 13.66 s); with concurrency the same warm scan reaches
   **2,086,231 rows/s** full-box — **7.1× higher**. Never quote it as Cassandra throughput.
   *(The 292,849 / 13.66 s and 2,086,231 figures are recorded in `head-to-head-method.md` F3–F7 only; no
   raw run artefact was retained. The 288,725 rows/s reference itself is backed, by
   `ws0-corpus/claims-evidence.txt`.)*
2. **"No k-way merge on either side" is FALSE for Flight** — CQLite's Flight producer runs
   merge/compaction reconciliation unconditionally, even on one SSTable (#3058). True for the bare scan.
3. **CQLite mmaps `Data.db` here — it does not issue ~4.4 KB reads.** Two `r--s` mappings of 783,799,203 B
   *(the mapping observation is recorded in `head-to-head-method.md` F3–F7; no raw run artefact retained)*;
   the entire scan makes **115 `read()` syscalls / 3.08 MB `rchar`** (metadata only), vs Cassandra's ~4,190
   preads / 1.0596 GB — those four are artefact-backed. **This does not reproduce the "99.3% of reads in
   [4K,8K), rareq-sz 4.4 KB" ground truth** — the discrepancy (surface? build? resolved disk-access mode?)
   is unresolved and is a prerequisite for #3031. Syscall counts across engines are descriptive only.
4. **Arrow encode costs +59% cycles per row and −37% throughput on this corpus — not 15–20%.** Stated
   unambiguously, because two different quantities collide on "59/37": on the single `cq-warm-*-1t` runs
   Arrow *raises* cycles/row by **+59.0%** (15,430 vs 9,704) — an increase, not a share — and Arrow's
   *share* of total cycles is **37%**, coincidentally the same number as the throughput loss. The
   median-of-3 §4 table values are **+60.6% cycles** (15,564 vs 9,692) and **−38.6% throughput** (215,207
   vs 350,580); prefer those. Wide blob/text rows make the Arrow payload 2.70 GB ≈ the entire
   uncompressed dataset (675 B/row copied).
5. **`perf stat -p` costs >2× on the CQLite scan** (163K vs 360K rows/s; ~540K context switches saving
   per-task counters). **All figures here use CPU-wide `-C`.** This artefact alone would publish a
   2.1×-wrong ratio. *(The 360K rows/s unmetered and ~540K context-switch figures are recorded in
   `head-to-head-method.md` F3–F7; no raw run artefact retained.)*
6. **Unpinned CQLite is SLOWER than pinned** (18.74 s vs 11.16 s; 1.98M vs 310K voluntary context
   switches) — the `tokio::sync::mpsc` handoff is the scaling limiter. *(Recorded in
   `head-to-head-method.md` F3–F7; no raw run artefact retained.)*
7. **rows/s is not portable across row shapes.** On one core across four shipped tables: 81.2k → 629.4k
   rows/s, a **7.8× spread**; `test_comp.lz4_table` hit 290.0k on a bare single-thread scan, numerically
   near the 291.7k Flight reference on a totally different shape. **Fix the row shape or the number is
   meaningless** — hence both arms here use one table. *(The 81.2k/629.4k, 290.0k and 291.7k inputs come
   from earlier harness validation, not from a WS0 artefact, and are unverifiable here; the 7.8×
   arithmetic on them is correct.)*
8. **Cassandra emits a trailing zero-length chunk**: `chunkCount == ceil(dataLength/chunkLength) + 1`.
   **Independently re-verified by re-parsing this corpus's `nb-16-big-CompressionInfo.db`**: `chunkCount`
   **169,114** vs `ceil(2,770,741,510 / 16,384)` = **169,113**, and the 9 B chunk (5 B LZ4 payload + 4 B
   CRC) is **uniquely the last**, index 169113 — it is the single smallest stored chunk in the file. The
   earlier "reproduced in all 6 SSTables" observation is **not** backed by retained artefacts (only the
   final compacted SSTable was kept). CQLite's reader is safe (reads `chunk_count` from the header) but its
   unwired compressed *writer* omits it — recorded on #1406.

---

## 10. Method — how to re-run this (AC#1)

Artifacts on the measurement box: corpus `/home/ubuntu/ws0/ws0-corpus/` (`rerun.sh`, `gen-corpus.sh`,
`ws0-profile.yaml`, `measure-sstable.py`, `fullscan.py`, `trace-scan.bt`, `verify-claims.sh`,
`schema-as-created.cql`); harness `/home/ubuntu/ws0/ws0-cqlite/`; head-to-head drivers
`/home/ubuntu/ws0/ws0-h2h/`; results + full method `/home/ubuntu/ws0/ws0-results/`
(`head-to-head-method.md`, 236 raw artefacts under `h2h/`).

1. **Provision**: `fio`, OpenJDK 17, the Cassandra 5.0.8 binary tarball, and a shallow
   `cassandra-5.0.8` source clone for citations. None ship on a stock agent box.
2. **Single node, RF=1**, stock `cassandra.yaml` except paths/addresses — **and bound
   `MAX_HEAP_SIZE`** (§2). Single node is deliberate: one node owns the whole token range, so a range scan
   reads every partition locally with **no coordinator fan-out, no read-repair, no digest reads** — the
   confounds are removed rather than averaged.
3. **Generate the corpus with Cassandra itself** (§3), verify geometry by measurement, then flush +
   compact to a single SSTable.
4. **Both engines on byte-identical files** — `sha256sum` all components and re-verify between arms.
   Stop the other engine during each arm so its RSS and threads cannot contend.
5. **Pin, always.** Physical core *N* = logical *N* and *N+8*. `taskset -c 2,10` for per-physical-core
   rows/s; `taskset -c 2` for cycles/traffic (a busy SMT sibling pollutes `l2_lines_in.all`, and both
   siblings reporting cycles double-counts). Use CPU-wide `perf stat -C`, never `-p` (§9.5).
6. **Page-cache state**: cold = `sync; echo 3 > /proc/sys/vm/drop_caches`; warm = one full pre-pass, then
   measure. Do both, report separately.
7. **Report cycles/row AND bytes-of-memory-traffic/row**, never CPU%. Report `/proc/<pid>/io`
   `read_bytes` / `rchar` / `syscr` **separately — never divide them** (that produced a bogus
   "~59 KB/syscall" in earlier work).

### Tooling traps (already paid for — do not rediscover)

- **`samply` records 0 samples** on these AWS kernels. Use `perf record -F 999 --call-graph=dwarf`.
- bcc `xfsslower` will not compile here; use `bpftrace` on `block:block_rq_complete`.
- **`kernel.perf_event_paranoid` silently reverts to 4** — re-assert `-1` before each run.
- `perf record --call-graph=dwarf` **hangs past 120 s against the 239 MB `cqlite` binary** (fine against a
  smaller harness). Profile a small harness binary.
- Build with `CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true` as **env overrides** —
  `[profile.release]` hardcodes `strip = true` and would discard DWARF.
- **`cqlsh` does not run on a Python-3.12-only box** (needs 3.6–3.11); use the `cassandra-driver` python
  path. **`SELECT count(*)` over 4M rows server-side-timeouts** — use `sstablemetadata totalRows` or a
  token-range split.
- `pgrep -f CassandraDaemon` matches the launching shell (every counter reads 0), and
  `pkill -f 'cqlite-flight --data-dir'` matches the running shell's own cmdline **and kills the shell**.
  Target PIDs explicitly.
- **Do not run `fetch-datasets.sh` against a git checkout** — it deleted 4 tracked commitlog fixtures
  during this work.
- **roborev, three traps, all measured on this box:**
  - `.roborev.toml` sets `exclude_patterns = ['docs/**', '*.md']`, so a **docs-only diff has zero
    reviewable content** — roborev returns "No issues found / no code changes to review". That is a
    vacuous pass, not a code review. Do not report it as review coverage of a report like this one.
  - **CLAUDE.md's prescribed `--model gpt-5.6-sol` is REJECTED by the installed codex-cli 0.142.5**
    (`The 'gpt-5.6-sol' model requires a newer version of Codex`). codex **auth is fine** (`Logged in
    using personal access token`) and `~/.codex/config.toml` carries no model pin, so the doctrine claim
    that the model is configured there does not hold here. The **working invocation is**
    `roborev review --branch --base origin/main --agent codex --model gpt-5.5 --wait` (verified).
  - The `claude-code` roborev agent fails separately with `Failed to authenticate: OAuth session expired
    and could not be refreshed` — a different failure from the model rejection above; do not conflate them.

### Surfaces that would flatter CQLite — do not measure with these

- **`cqlite-core/benches/read.rs::read/full_scan` is triple-flattering**: one `loaded.db` reused across
  every criterion iteration (reader + index + page cache warm), 999 L3-resident rows, *and* the
  materializing `db.execute()`. It is a regression micro-bench, not a throughput surface.
- **`db.execute()` inflates 2.05×** (209.7k vs 102.3k rows/s streaming) by bypassing the bounded mpsc
  channel; it is also O(rows) resident.
- `flight-loadgen` reads only `batch.num_rows()` and never touches cell values — safe only with
  server-side counters, never client wall-time as a cycles denominator.
- Not scan-throughput surfaces at all: `concurrent_scan.rs`, `open.rs`, `partition_lookup.rs`,
  `decode_bench.rs`.

---

## 11. Acceptance criteria status

| AC | Status |
|---|---|
| 1. Defensible X× + re-runnable method in `docs/reports/` | ✅ §1, §10 |
| 2. `fio` at both `bs=4k iodepth=1` and `bs=1M iodepth=32`, fair denominator noted | ⚠️ **OPEN** — measured on EBS (§8); this box has no instance store, so it cannot replace the i4i denominator |
| 3. Measured achievable memory bandwidth per core | ✅ 10.84 single-core / 10.15 all-cores (§7) |
| 4. Verdict on whether the roofline tiers survive | ✅ tiers survive, framing refuted, **T3 refuted** (§7) |
| 5. Cassandra structural difference named with `file:line` | ✅ §6 |

## 12. Follow-ups filed

- **#3058** (P1, confirmed) — Flight `do_get` runs k-way merge/compaction reconciliation unconditionally
  on a single SSTable. The dominant cause of the 3.50× shipping-path loss.
- **#3060** (P2, observed once) — `cqlite-flight` may never exit when signalled mid-stream.
- **#3061** (P3, observed once) — 1,017 MiB peak RSS with `Data.db` mapped twice; also asks whether the
  `<128MB` target is measured correctly at all.
- **#1406** — noted the trailing zero-length chunk parity detail for the compressed-write path.
- **#3031** — read-size premise needs the mmap-vs-pread reconciliation in §9.3 before it can proceed.
