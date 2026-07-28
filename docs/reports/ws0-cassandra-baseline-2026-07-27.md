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
| **Read + materialise** (nothing shipped) | **CQLite is 1.13–1.24× stock Cassandra**, warm — the two *matched* pinnings: 1-core↔1-core **1.24×** (367,760 vs 297,653 rows/s), 1-hw-thread↔1-hw-thread **1.13×** (350,580 vs 311,196) |
| **Ship every row to a client** | **CQLite Flight is 0.29× — Cassandra is 3.50× FASTER** — 60,892 vs 212,981 rows/s, **both 1 physical core**, warm (the matched 1-hw-thread pair is 3.56×: 55,593 vs 198,002) |

**Why the read-path headline is a range and not a point.** Every arm was measured at two pinnings, so the
read-path comparison has *two* matched pairs and they disagree by **0.11×** (~10% of the ratio); picking
either alone is a
choice of denominator, and the range is the only summary that does not make that choice silently. An
earlier version of this report headlined **1.18×** — that figure is the one combination in the dataset
that is **not** like-for-like: CQLite's 1-physical-core throughput divided by Cassandra's
1-hardware-thread throughput. It is not a conservative floor, not a mid-range, and not quoted here any
more. §4's `1 hw thr` / `1 core` rows are the inputs; the two Cassandra pinnings differ by more than
replicate noise and in *opposite directions* per arm, which is discussed with the SMT note in §4. The
shipping-path headline needs no such treatment — it is already a matched 1-core↔1-core pair.

This is **not** a CQLite win. On the read path the advantage is a margin, not a multiple. On the honest
end-to-end surface — the only one comparable to Cassandra actually serving rows to a client — CQLite
loses by 3.5×.

**Strategic consequence:** the 0.17 program is optimising the read path, where CQLite already leads,
while the measured deficit sits in serialization/egress, which no #3023 workstream owned at the time of
writing. Follow-ups filed: **#3058** (the dominant, confirmed cause), #3060, #3061, #3068.

---

## 2. Environment — read this before comparing any absolute number

| | Value |
|---|---|
| Instance | **c7i.4xlarge** — 16 vCPU / **8 physical cores**, Xeon Platinum 8488C (Sapphire Rapids) |
| Memory | 30 GiB + a 16 GiB swapfile (`vm.swappiness=10`), **added mid-run at 17:59:08 UTC** as an OOM safety valve, not a tuning change (see the heap note and the before/after attribution below) |
| Storage | **EBS only.** Both devices report `Amazon Elastic Block Store` (`nvme0n1`→`/`, `nvme1n1`→`/data`) |
| Kernel | 6.17.0-1019-aws |
| Cassandra | **5.0.8** (`1722270...`), OpenJDK 17.0.19 |
| CQLite | `main`@`d0344b76` + this issue's branch — Cargo version string **0.16.1**, but **78 commits AHEAD of the `v0.16.1` tag**; release profile with DWARF retained. Read the build note below before comparing to any tagged release |

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
`Heap Memory (MB): 766.41 / 15776.00`). An unbounded daemon is what got OOM-killed on this box (recorded in
`head-to-head-method.md` §6 and `ws0-corpus/rerun.sh:40-45`; not an `fio` matter, so not in §8) — precisely:
the kill was *invoked by* `rustc` and the *victim* was the 17 GB-RSS JVM, per the kernel log quoted below.
Since
`file_cache_enabled=false` (§5), heap size should have little effect on a scan — but it is stated rather
than assumed. `HEAP_NEWSIZE` deliberately left unset: this build uses G1 and `cassandra-env.sh`
warns/ignores it under G1.

### CQLite configuration — which read-path work is actually in the measured build

"0.16.1" above is the **workspace version string, not the `v0.16.1` tag.** The binaries were built from
`/home/ubuntu/workspace/wt-3026` (this issue's worktree, branched from `main`@`d0344b76`, 2026-07-27),
which is **78 commits ahead** of the `v0.16.1` tag (2026-07-24) — `git rev-list --count v0.16.1..HEAD` =
78. `ws0-cqlite/build.log` records `Compiling cqlite-core v0.16.1 (/home/ubuntu/workspace/wt-3026/cqlite-core)`,
and `target/release/{cqlite,cqlite-flight,flight-loadgen}` are stamped 2026-07-27 18:02–18:04, before every
measured arm. Build env: `CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true` (the manifest
hardcodes `strip = true`). **So the tag's contents are the wrong reference for these numbers; the branch's
are the right one.** Determined from git, not from the version string:

| read-path change | in the measured build? | evidence |
|---|---|---|
| **#2210** — dedicated `MADV_RANDOM` point-read mmap for large SSTables | **YES** | `f5e18390`, contained in tags `v0.15.0`/`v0.16.0`/`v0.16.1`. Gate `POINT_MMAP_MADV_RANDOM_MIN_BYTES = 8 MiB` (`reader/mod.rs:343`); this `Data.db` is 783,799,203 B, so the second mapping **was** created |
| **#2877** — 4 MiB coalesced Summary-guided compressed scan window | **YES — but NOT in the `v0.16.1` tag** | `c0241415`, landed 2026-07-25, *after* the tag. `COMPRESSED_SCAN_WINDOW_TARGET_BYTES = 4 * 1024 * 1024` at `reader/data_access/summary_scan/compressed_scan_window.rs:26` |
| **#2876** — split scan-side positional reads off the `MADV_RANDOM` point mapping | **YES — also post-tag** | `bc14ff0e`, 2026-07-25. Consequence: the scan reads the **unadvised** mapping, so the `MADV_RANDOM` advice does *not* apply to any arm here |

Two things follow that were previously unstated. First, **#2877's coalescing is present but is a coalesced
read against an mmap, not a coalesced `pread`** — hence 115 `read()` syscalls total (§9.3), not the ~190
4 MiB preads a 783,799,203 B file would imply. Second, this **explains §9.3's and #3061's "Data.db mapped twice"**: the two `r--s` mappings of
783,799,203 B are exactly #2210's dedicated point mapping plus the scan mapping — designed behaviour of
this build, not a leak. #3061's separate question (whether the `<128MB` RSS target is measured correctly)
stands on its own.

Flight server configuration, as logged by the server itself (`ws0-results/h2h/flight-*.server.log`):
`batch_size=8192`, `max_batch_bytes=4194304`, `max_inflight_egress_bytes=12582912`,
`max_concurrent_scans=64`, `max_concurrent_streams=1024`, `listen=127.0.0.1:8815`.

### What ran before and after the swapfile

The 16 GiB swapfile was created at **2026-07-27 17:59:08 UTC** (`Adding 16777212k swap on /swapfile.
Priority:-2 extents:395`), 54 minutes after the OOM kill it responds to — **17:05:28 UTC**, `Out of memory:
Killed process 2590 (java) total-vm:19431136kB, anon-rss:17085992kB`, with `Total swap = 0kB` at the time,
and **invoked by `rustc`**: a concurrent cargo build was the allocation that tipped the box over, so the
box was demonstrably not quiet at that moment. Attribution of the arms, from timestamps the committed
artifacts carry themselves:

- **Before the swapfile, and on an *earlier boot*:** all four §8 `fio` arms — `00-layout` 15:40:55Z,
  `randread-4k-qd1` 15:46:31Z, `randread-1M-qd32` 15:48:37Z, `seqread-4k-qd1` 15:50:12Z,
  `seqread-1M-qd32` 15:52:17Z (the `timestamp` field inside each `ws0-results/fio-*.json`). The box
  **rebooted** at 16:58:36Z — the prior boot's last log entry is 16:00:05Z — so §8 and §4 are not merely
  before/after a swapfile, they are **different kernel sessions of the same instance**. Neither the report
  nor the method doc had noted the reboot.
- **After the swapfile:** every §4 head-to-head arm. The daemon of record started 18:20:39Z
  (`ws0-corpus/claims-evidence.txt`, evidence A4), the corpus was written 18:26, the Cassandra profiles
  were sampled 19:12:36Z / 19:13:42Z, the Flight steps ran 19:33–20:24Z (`ts_unix_ms` in
  `h2h/flight-*.jsonl`), and the §7 traffic-proxy calibration is stamped `2026-07-28T00:38:43+00:00`.
- **Unattributable:** the §7 STREAM / pure-read bandwidth outputs carry no timestamp. Immaterial for those
  arms specifically — a 1.5 GiB resident footprint on a 30 GiB box cannot reach swap — but stated rather
  than assumed.

Verbatim kernel-log lines: `ws0-3026-artifacts/ws0-results/session-timeline.txt`.

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

**The 110-row shortfall against the nominal 4,000,000 is real, and not accounted for.** 200,000
partitions × (2 × 10) clustering rows = 4,000,000 nominal; both oracles measure **3,999,890** — 110 rows
short, −0.00275% — and every figure in this report uses the *measured* count, never the nominal one.
`ws0-corpus/full-load.log` records `Total errors: 0` on all four load batches, so nothing was lost to a
write failure. The available mechanism is in the generator profile: `ws0-corpus/ws0-profile.yaml` draws
`part_id`, `seq` and `event_time` from `uniform(...)` populations rather than guaranteed-distinct
sequences — the profile's own comment on `seq` reads "wide => the draws are ~always distinct", i.e. *not
always* — so duplicate primary keys collapse on upsert. **That mechanism was not verified and the 110 rows
were not accounted for individually.** It cannot bias any ratio here (both engines read the identical
file) and 0.00275% is immaterial to every figure, but it is a generator gap rather than a rounding
artefact, and a regenerated corpus should not be expected to land on exactly 3,999,890.

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

Physical core = CPUs 2+10. **SMT topology verified, not asserted:**
`/sys/devices/system/cpu/cpu2/topology/thread_siblings_list` reads `2,10` on this box (and `cpu0`→`0,8`,
`cpu7`→`7,15`), so logical *N* and *N*+8 really are the two siblings of one physical core — captured in
`ws0-results/session-timeline.txt`.

Warm rows are the **median of 3** replicates from the `rep-*` triples in `ws0-results/h2h/`, **except the
Flight 1-hw-thread row**; per-arm dispersion and provenance follow the table.

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

### Replicate dispersion, per arm

The single aggregate "spread <±2%" this table used to carry was **wrong for two arms**. Computed from the
`rep-*` triples:

| arm | pin | min | median | max | (max−min)/median |
|---|---|--:|--:|--:|--:|
| Cassandra `count(*)` | 1 hw thr | 308,999 | **311,196** | 312,801 | 1.22% |
| Cassandra `count(*)` | 1 core | 297,473 | **297,653** | 298,466 | 0.33% |
| Cassandra `SELECT *` | 1 hw thr | 195,917 | **198,002** | 201,323 | **2.73%** |
| Cassandra `SELECT *` | 1 core | 211,442 | **212,981** | 213,715 | 1.07% |
| CQLite bare scan | 1 hw thr | 345,963 | **350,580** | 351,770 | 1.66% |
| CQLite bare scan | 1 core | 362,360 | **367,760** | 368,678 | 1.72% |
| CQLite scan+Arrow | 1 hw thr | 207,074 | **215,207** | 217,607 | **4.90%** |
| CQLite scan+Arrow | 1 core | 224,179 | **227,531** | 228,541 | 1.92% |
| CQLite Flight | 1 core | 60,411 | **60,892** | 61,410 | 1.64% |

Two arms break ±2%: Cassandra `SELECT *` at 1 hw thread (2.73%) and **CQLite scan+Arrow at 1 hw thread
(4.90%)** — the noisiest measurement in the report, and one leg of the Arrow-cost claim in §9.4.

**The Flight 1-hw-thread row is n=1, not a median.** It is the single `fl1t` step: 5 completed full scans
inside one 359.75 s step (`h2h/flight-fl1t.jsonl`) = 55,593.9 rows/s, with `h2h/perf-fl1t.txt` supplying its
cycles (60,825/row), IPC (2.39) and `l2_lines_in.all` (30,914 B/row). The two attempted 1-hw-thread
replicates — `rep-fl-2-r1` and `fl-warm-1t`, both 20:16–20:17Z — **failed**:
`flight-loadgen: ... could not connect to http://127.0.0.1:8815 within 5s: transport error`; their `.jsonl`
files are empty and their `perf` files hold ~5 s of near-idle counters. They were discarded, not averaged.
Treat 55,593 as a single observation.

### The SMT delta flips sign on `count(*)` — and that arm is the 1.24× denominator

Adding the second SMT sibling helps every arm except one. CQLite bare scan **+4.9%** (350,580→367,760),
Cassandra `SELECT *` **+7.6%** (198,002→212,981), CQLite scan+Arrow **+5.7%**, CQLite Flight **+9.5%** —
but Cassandra `count(*)` goes **−4.3%** (311,196→297,653). This is not replicate noise: those two
configurations are the *tightest* in the dispersion table (1.22% and 0.33%), and the ±2% figure describes
run-to-run spread *within* one pinning, not a delta *across* pinnings — so a 4.3% cross-pinning move is a
different quantity that the ±2% claim never covered, and it was left undiscussed.

The measured IPC column explains it without any new measurement. Instructions per row are essentially
identical at both pinnings (40,456 at 1 hw thread vs 40,475 at 1 core), so per-**real**-core issue
throughput is `IPC × siblings`: **3.73** on one thread versus **1.81 × 2 = 3.62** on two. `count(*)` is the
most ILP-dense arm in the table and already close to the core's issue width, so the second thread adds
contention instead of filling idle slots. The lower-IPC arms have slots to fill and gain accordingly —
`SELECT *` 3.15 → 1.71 × 2 = **3.42**; CQLite bare scan 2.67 → 1.57 × 2 = **3.14**. (Mechanism consistent
with the measured counters; not itself a separate measurement.)

**Consequence for the headline:** the matched 1-core↔1-core ratio (**1.24×**) divides by Cassandra's
*worse* pinning and the matched 1-hw-thread ratio (**1.13×**) by its *best*. Neither is wrong; that is
exactly why §1 reports a range.

### The Cassandra arms were server-bound — the configuration, and the proof

§9.1's client-bound finding (288,725 rows/s, reproducing at 292,849 at `--inflight 1`) used a **different
client** from the arms above, and that difference is the entire reason the sharded driver exists. The
reported arms, in full:

| | Value |
|---|---|
| Client | DataStax `cassandra-driver` for Python, native protocol **v5**, `WhiteListRoundRobinPolicy(["127.0.0.1"])`, `default_fetch_size=5000`, `execute_async` (`ws0-h2h/cas-scan.py`) |
| Concurrency | `--inflight 8` async range queries **per shard** × **6 shard processes** = up to **48 in flight** (`"inflight": 8`, `"client_shards": 6` in every `summary-rep-cas-*.txt`) |
| Splitter | **512 token ranges**, prepared `WHERE token(part_id) > ? AND token(part_id) <= ?`, ring split into 512 equal spans, shard *i* taking ranges where `i % 6 == shard_index`. **Both modes use it** — `"ranges": 512` appears in every Cassandra summary, `count` **and** `rows` alike, so yes, the `count(*)` arm used the 512-range splitter |
| Server pinning | **every** daemon thread, `taskset -acp <cpus> <pid>`, on a PID selected by executable name plus a >20-thread assertion (`arm1c-cassandra.sh`; see the `pgrep` trap in §10) — CPU `2` for 1 hw thread, `2,10` for 1 physical core |
| Client pinning | the 6 shard processes pinned one per logical CPU to **4,5,6,7,12,13** = physical cores 4–7 with cores 4 and 5 running both siblings (`shardrun.sh`); disjoint from the metered core either way |
| Metering | `perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent -C <cpus>` — CPU-wide, over the daemon's pinned CPUs only |

**Saturation of the pinned server core is measured, not assumed.** `arm1c-cassandra.sh` reads the daemon's
`utime+stime` from `/proc/<pid>/stat` across the run and divides by wall × pinned-CPU count. For every
reported arm: `count(*)` 1 hw thread **1.0035 / 1.0083 / 1.0182**, `count(*)` 1 core **1.0014 / 1.0059 /
1.0080**, `SELECT *` 1 hw thread **1.0021 / 1.0034 / 1.0071**, `SELECT *` 1 core **0.9973 / 0.9983 /
1.0008** — and every summary carries `"client_bound": false`. The metered CPUs were busy ~100% of wall
time, so wall-clock rows/s on these arms is a server-throughput measurement.

The **negative control is in the artifacts too**: the same workload driven by ONE unsharded client reached
utilisation **0.35** (`summary-cas2-warm-rows-1c.txt`) and **0.63** (`...-1t`), producing 94,324 / 95,037
rows/s — less than half. That is what a client-bound Cassandra arm looks like on this rig, and no reported
row resembles it. **So §9.1's ceiling does not bound the 311,196 figure**: 288,725 was one sequential
process at inflight 1 driving an unpinned box; 311,196 is 48 in-flight queries against a daemon core
measured at 100% utilisation. The 6% numerical proximity of the two is a coincidence between unrelated
configurations, not a ceiling being approached.

**Cold ≈ warm within ~6.3% worst case (CQLite scan+Arrow 1-core, 227,531→213,117); all other arms within
4.6%. This workload is not I/O-bound on this box** — with three limits on that sentence:

1. **Every cold arm is n=1.** No cold replicates were run, so a 6.3% cold-vs-warm delta is not separable
   from the 4.9% replicate spread the *same* arm shows warm. The direction is trustworthy; the magnitudes
   are not.
2. **Cache eviction IS proven — by an I/O counter this report previously failed to cite.** `read_bytes`
   from `/proc/<pid>/io`, captured per run, is **0 on every warm arm** and **783.9–793.2 MB on every cold
   arm** (Cassandra 783,921,152 B; CQLite bare scan 788,164,608 B; scan+Arrow 788,250,624 B; Flight
   793,174,016 B) against a 783,799,203 B `Data.db`. Cold runs fetched the file from the device about once
   (1.000–1.012×); warm runs fetched nothing. `drop_caches` did what it was supposed to.
3. **"Cold" means page-cache-cold ONLY — the JVM was never restarted.** `arm1c-cassandra.sh` does
   `sync; echo 3 > /proc/sys/vm/drop_caches` and then runs against the *same* long-lived daemon, so JIT
   compilation, the key cache (889 live entries per `claims-evidence.txt` A2), the 512 MiB buffer pool and
   the G1 heap all stayed warm across every cold Cassandra arm. A genuinely cold Cassandra — fresh JVM —
   would be slower than any figure here, and this baseline does not measure it.

The tension with the program's own `stream_cold_fault` sub-phase timer (**4437.9 → 105.8 ms**; that timer
is the Flight cold body-chunk page-in, `docs/observability/README.md`) is **not resolved here and should
not be read as resolved.** Those figures are not reproduced by any WS0 artefact, were taken on different
hardware and a different surface, and the most likely reconciliation is the mmap-vs-pread discrepancy in
§9.3 — a page-fault-driven scan on a warm-ish page cache simply does not have a cold-fault phase to pay.
Flagged, unexplained, and a prerequisite for anyone quoting either number as the other's context.

### Fairness of the comparators

- Cassandra was measured over the **native CQL protocol**, which includes its own result serialization.
  Comparing a CQLite number with serialization *removed* against that would be unfair, which is why the
  Flight row is the headline end-to-end comparator and the bare scan is reported as the read-path ceiling.
- Confound named rather than argued away: Arrow IPC over gRPC is a **cheaper and structurally different**
  serialization than Cassandra's per-cell length-prefixed protocol, so this pairing is fair on the read
  leg and mildly favours CQLite on the serialization leg.
- **Cassandra's `count(*)` does not skip cell deserialisation** — **ASSERTED, not substantiated by any
  committed artifact.** The stated basis is a `count(*)` profile (`head-to-head-method.md` F2:
  `Cell$Serializer.deserialize` 8.0% self; `ReadResponse.createDataResponse` →
  `UnfilteredPartitionIterators$Serializer.serialize` 76.6% inclusive). That profile's full output
  (`h2h/prof-count.txt`, 121 MB) was **not committed**, and the retained substitute
  `h2h-perf-summaries/prof-count.top400.txt` does **not** contain it: it is the first 400 lines of the raw
  `SJK ssa --print` dump — one sample timestamp, overwhelmingly parked threads, **one** `ReadStage` mention
  (thread 89, state `WAITING`), **zero** occurrences of any frame the claim names, and **no percentage
  figures anywhere in the file**. F2's numbers survive only as prose. Label accordingly: this is an
  assertion, and the headline read-path comparator therefore has no committed profile.
- **The asymmetry runs in CQLite's disfavour, so the comparison is probably conservative** — stated
  explicitly rather than left implicit. Whatever `count(*)` may or may not skip, CQLite's bare scan
  definitely does work that a counting query cannot need: it materialises a `QueryRow` per row whose values
  live in a `HashMap<Arc<str>, Value>` — that part is **source-verified**, not profile-derived
  (`cqlite-core/src/query/result.rs:77`) — and §5 puts **6.6%** of that arm's cycles into SipHashing column
  names into it plus **6.1%** into `QueryRow` drop + `Value::into_owned` (those two *shares* are unretained;
  see §5's provenance note — the direction does not depend on their exact size). Meanwhile the harness's own
  per-cell anti-elision digest, ~+28% cycles, is **excluded** — every reported number uses `--no-fold` — so
  that overhead is not inflating CQLite's side either. A row-materialising scan running 1.13–1.24× a
  counting scan is therefore a **floor** on the read-path gap, not a ceiling.
- **The Flight arm is the one arm whose pinned set was NOT saturated.**
  `server_cpu_utilization_of_pinned_set` is **0.864–0.866** across the three warm 1-core runs and **0.844**
  cold — versus ~1.00 for every Cassandra arm. So roughly 13.5% of the Flight arm's wall time is not
  accounted for on the metered CPUs, and its rows/s is not a pure server-CPU-bound figure. This is a
  caveat on the **precision** of the 3.50× ratio, not on its existence: the per-row *cost* side of that
  comparison is a cycles measurement (**96,260 cyc/row** at 1 core against the bare scan's **17,648** on
  the identical pinning, both from `perf stat -C`), and cycles per row do not depend on utilisation.
  **#3058 rests on that CQLite-vs-CQLite cycle and throughput gap, which no Cassandra number and no
  utilisation figure can move.**
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

> **Provenance limit on every percentage in this section.** They come from `perf report --stdio
> --no-children` (CQLite / Flight) and SJK `ssa` (Cassandra) outputs that were **not retained**:
> `h2h/prof-{rows,count}.txt` (161 MB / 121 MB), the `*.data` perf binaries and the `*.stcap` captures are
> all excluded from the committed artifacts, and the retained substitutes —
> `h2h-perf-summaries/prof-{rows,count}.top400.txt` and `jstack-rows.top300.txt` — are head-truncations of
> raw thread dumps that contain **no percentage figures at all** (verified: zero `%` characters in any of
> the three). The artifact README's claim that they "back the §5 percentages" is **not correct**; they do
> not. What *is* independently checkable is the **structure**, in source rather than in a profile: Flight's
> `do_get` → `MergeProducer` → `KWayMerger::reconcile_cluster_with_overlap_counted` →
> `build_compaction_row_data` routing, the per-cell `HashMap<String, CellWriteMetadata>`
> (`cqlite-core/src/query/result.rs:93`) and the per-row `HashMap<Arc<str>, Value>` (`:77`). And **#3058's
> priority does not depend on this section**: it rests on the artifact-backed 60,892-vs-350,580 rows/s and
> 96,260-vs-17,648 cyc/row gaps in §4. **The percentages themselves are unretained, and should be
> re-measured before anyone optimises against them.**

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
high.** At the 350,580 rows/s measured on the same 1-hw-thread arm that is **0.435 GB/s ≈ 4.1% of one
core's 10.73 GB/s** — the **pinned** single-core pure-read ceiling, which is the matched denominator for a
pinned numerator. (Dividing by the *unpinned* 10.84 gives 4.0%; the earlier text did exactly that, mixing a
pinned measurement with an unpinned ceiling. It changes nothing about the conclusion, but the matched pair
is the correct one.) **IPC on that arm is 2.67.** The "IPC 2.67–3.73" previously quoted here was not a range
for one engine — 3.73 is **Cassandra's** `count(*)` 1-hw-thread IPC. Separated: CQLite bare scan **2.67**,
Cassandra `count(*)` **3.73**, Cassandra `SELECT *` **3.15**, CQLite scan+Arrow **2.03**, CQLite Flight
**2.39** (all 1-hw-thread; the 1-core figures are SMT-summed and not comparable). (The earlier "0.55 GB/s ≈
5%" mixed in a 291.7k rows/s figure
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

**Second limit on the proxy — it is INBOUND-only, and the write-heavy arms are unqualified.** The `perf`
line above also counts **`l2_lines_out.non_silent`**, and this report has never reported or explained it.
The reason it is not reported is that it does not separate what its name suggests: **dirty writebacks are
not distinguishable from clean evictions on this PMU.** In the *read-only* calibration kernel it lands
within **1.0%** of `l2_lines_in.all` (2,854,000,095 vs 2,883,012,912 at 512 MiB, `membw-calibration.txt`),
and across every WS0 arm — read-only and write-heavy alike — it stays a flat **94.6–97.4%** of the inbound
counter: CQLite bare scan 1,174 / 1,229 B/row (95.5%), Cassandra `count(*)` 9,305 / 9,650 (96.4%),
scan+Arrow 13,774 / 14,147 (**97.4%**), Flight 30,678 / 32,438 (**94.6%**) — note that the two *most*
write-heavy arms sit at the top and bottom of that band, so the ratio does not even trend with write
volume. A counter that tracks inbound traffic at a fixed ratio both on a pure-read
kernel *and* on an arm copying 2.70 GB of Arrow payload carries **no independent writeback signal on this
PMU**, so the proxy is inbound-only by necessity, not by choice.

The consequence, stated plainly: **`l2_lines_in.all × 64` is an inbound-only proxy being used to refute a
*total*-traffic model.** On the tight Rust scan that is sound enough — the arm writes almost nothing, so
inbound ≈ total, and the calibration ties it to DRAM traffic within +1.1%. It is **not** qualified on the
write-heavy arms: **scan+Arrow at 14,147 B/row and Flight at 32,438 B/row** each build a **2.70 GB** Arrow
payload (675 B/row copied), and the store traffic that implies is precisely what this counter cannot see.
Those two figures are therefore simultaneously *upper* bounds on DRAM traffic (L3 refill, per the table
above) and *lower* bounds on total memory traffic — a two-sided caveat, with neither side measured. **The
T3 refutation does not depend on them**: it rests on the CQLite bare-scan arm, where the proxy is
calibrated and the write volume is negligible.

### Do the tiers survive contact with Cassandra?

**No. T1 has lost its derivation, and this report offers no replacement ceiling.**

The earlier answer here ("yes, but the framing does not") rested on one observation: stock Cassandra sits at
**297,653 rows/s per physical core** on the read path (the `1 core` row of §4 — *not* §1's 311,196, its
1-hardware-thread figure), ~**0.50×** T1. That observation is correct and it is **not evidence that T1 is
achievable.** It says one existing engine is at half of T1, which is exactly as consistent with 600k being
unreachable as with it being reachable. It cannot do the work the previous wording asked of it.

T1 = 600k rows/s/core was derived from the **same roofline** whose T3 this section refutes at **both** of
its inputs — memory bandwidth **34–35% low** and traffic per row **~3.5× high**. A tier cannot keep its
number after the model that produced the number has been falsified in both terms. Any other tier from that
roofline inherits the same problem. So:

- **T3 — refuted** (measured, §7 above).
- **T1 — withdrawn pending re-derivation.** Not disproved; *underivable* from anything measured here.
- The scan is **instruction-throughput-bound**, so a replacement ceiling has to come from an
  instruction/IPC budget. This baseline did not derive one: it measured **25,918 instructions/row at IPC
  2.67** for the bare scan and nothing whatsoever about how far either of those can be pushed. Producing
  that budget is the missing piece, and it is not in scope here.

What survives is the **direction**, which is the part that was actually measured: CQLite leads the bare read
path by **1.13–1.24×** and loses the shipping path by **3.50×**. That direction is enough to say the
program is aiming at the wrong surface (§1) without needing a ceiling at all.

---

## 8. `fio` — the efficiency denominator (AC#2: **OPEN**)

O_DIRECT, read-only, 48 GiB file (> 30 GiB RAM), 5 s ramp discarded, root volume.

**Job shape, in full** — at QD32 the `ioengine`/`numjobs` pair is what decides whether the depth was real,
so it is stated rather than implied. `fio-3.36`; `--ioengine=libaio`; **`numjobs` unset, i.e. 1 job per
arm**; `--direct=1`; `--time_based`; `--group_reporting`; `--ramp_time=5`; `--log_avg_msec=1000`;
`--percentile_list=50:90:99:99.9`; one target file. Runtimes: **300 s** randread-4k-qd1, **90 s**
seqread-4k-qd1, **120 s** each 1M-QD32 arm. So each QD32 arm is **one libaio job holding 32 genuinely
concurrent in-flight I/Os**, not 32 threads at QD1 — the depth was real. Job file / literal invocation:
`ws0-results/rerun.sh` (the `run()` helper and the `layout` pre-write); the per-arm `job options` block is
committed inside each `ws0-results/fio-*.json`, and per-second bandwidth/latency samples in
`bwlog-*` / `latlog-*`.

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
>
> **Both prior figures are quoted here SHAPE-UNKNOWN.** No `bs`, `iodepth`, `numjobs`, `ioengine` or
> read/write mix was recorded with either the **711.8 MB/s** or the **36 MB/s** number, and no WS0 artefact
> carries them. That missing shape *is* the mispricing this AC exists to correct, so it must not be
> reproduced by treating 711.8 MB/s as if it were comparable to anything above. **Until its job shape is
> recovered, 711.8 MB/s is not a denominator**; the 5.4× and 6.4× are order-of-magnitude orientation only,
> not ratios to plan against. Every number in the table above, by contrast, has its full job shape
> committed.

---

## 9. Contradicted premises — carry these forward

1. **The 288,725 rows/s Cassandra reference is CLIENT-bound, not a Cassandra capability.** Reproduced
   exactly at `--inflight 1` (292,849 rows/s / 13.66 s); with concurrency the same warm scan reaches
   **2,086,231 rows/s** full-box — **7.1× higher**. Never quote it as Cassandra throughput.
   *(The 292,849 / 13.66 s and 2,086,231 figures are recorded in `head-to-head-method.md` F3–F7 only; no
   raw run artefact was retained. The 288,725 rows/s reference itself is backed, by
   `ws0-corpus/claims-evidence.txt`.)*
   **This does NOT make §4's Cassandra figures client-bound floors** — a natural but wrong inference from
   the 6% gap between 288,725 and the headline 311,196. Different client entirely: 288,725 was one
   `fullscan.py` process issuing 512 range queries **strictly sequentially** against an unpinned box, while
   every §4 arm ran **6 sharded `cas-scan.py` processes at `--inflight 8` (48 queries in flight)** against a
   daemon whose pinned core is *measured* at 0.997–1.018 utilisation with `"client_bound": false`. See §4's
   server-bound subsection, including the unsharded negative control at utilisation 0.35/0.63. The numerical
   proximity is a coincidence between unrelated configurations, not a ceiling being approached.
2. **"No k-way merge on either side" is FALSE for Flight** — CQLite's Flight producer runs
   merge/compaction reconciliation unconditionally, even on one SSTable (#3058). True for the bare scan.
3. **CQLite mmaps `Data.db` here — it does not issue ~4.4 KB reads.** Two `r--s` mappings of 783,799,203 B
   *(the mapping observation is recorded in `head-to-head-method.md` F3–F7; no raw run artefact retained)*;
   the entire scan makes **115 `read()` syscalls / 3.08 MB `rchar`** (metadata only), vs Cassandra's ~4,190
   preads / 1.0596 GB — those four are artefact-backed. **This does not reproduce the "99.3% of reads in
   [4K,8K), rareq-sz 4.4 KB" ground truth** — the discrepancy (surface? build? resolved disk-access mode?)
   is unresolved and is a prerequisite for #3031. Syscall counts across engines are descriptive only.
   **Now traceable to code: #3068** — `scan_positional_source` is backed by `MmapReadAt` at
   `cqlite-core/src/storage/sstable/reader/mod.rs:608-613` (`ScanSource::Mapped` arm), and per §2 the
   measured build carries #2876, which deliberately points the scan at the **unadvised** mapping. So the
   mmap is by design, not an accident of configuration.

   **What the §8 `fio` numbers do and do not bear on, given this.**
   `ws0-results/rerun.sh` labels its `randread-4k-qd1` arm *"CQLite's real pattern (~4.4 KB
   reads at QD1)"*. **That premise is contradicted by this very finding, and the arm was selected under
   it.** CQLite's data path here issues **no** `read`/`pread` syscalls at all — 115 total, 3.08 MB `rchar`,
   metadata only; `Data.db` bytes arrive by **page fault on an mmap**, served by kernel readahead
   (`read_ahead_kb=128` on this device, `claims-evidence.txt`), not by 4 KiB O_DIRECT requests. Therefore:
   - the 4k/QD1 arm (**5.6 MB/s**) does **not** price CQLite's scan I/O on this box, and **no fio arm taken
     here does**;
   - what it *does* bound is device behaviour — EBS latency at QD1 — which is why §8's latency-bound
     arithmetic stands on its own and needs no change;
   - **no figure anywhere in this report was computed by dividing a CQLite rate by an fio number**, so
     nothing above requires retraction. The exposure is prospective: **#3031 and any future efficiency
     denominator must not price this mmap scan with a 4 KiB O_DIRECT arm.**

   The cold device counters confirm the fault-driven path was not amplifying anyway — `read_bytes`
   788,164,608 B against a 783,799,203 B file, 1.0056× — it simply is not measurable with the fio shapes
   taken. An honest denominator for this surface needs either a fault-latency measurement or a scan run
   against a non-mmap backend.
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

Artifacts are **committed** under `docs/reports/ws0-3026-artifacts/`: corpus generation in `ws0-corpus/`
(`rerun.sh`, `gen-corpus.sh`, `ws0-profile.yaml`, `measure-sstable.py`, `fullscan.py`, `trace-scan.bt`,
`verify-claims.sh`, `schema-as-created.cql`, `claims-evidence.txt`, `corpus-geometry.txt`,
`full-load.log`); harness sources in `ws0-cqlite/`; head-to-head drivers in `ws0-h2h/`; results + the full
method doc in `ws0-results/` (`head-to-head-method.md`, **228** raw per-run artefacts under `h2h/`, the
four `fio-*.json`, the bandwidth outputs, `membw-calibration.txt`, `session-timeline.txt`). Paths *inside*
those scripts still name the measurement box's `/home/ubuntu/ws0/...` working tree, which will not outlive
the instance — rewrite the four roots when re-running. What is **not** committed, and why, is enumerated in
`ws0-3026-artifacts/README.md`: the 783 MB corpus (regenerate via `ws0-corpus/rerun.sh`, verify against
`corpus-geometry.txt`), the perf/SJK binaries, and the two large profile texts — whose truncated
substitutes do **not** carry the §5 percentages (see the provenance note there).

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
5. **Pin everything — and say *what* was pinned.** Physical core *N* = logical *N* and *N+8*, verified from
   `thread_siblings_list` (§4), not assumed. There are **three** distinct pinnings and all of them are
   load-bearing:
   - **Engine under test → the metered set.** `taskset -c 2` = one hardware thread, SMT sibling 10 idle:
     the only valid basis for cycles/IPC/`l2_lines_in.all` (a busy sibling pollutes the traffic counter, and
     both siblings reporting cycles double-counts). `taskset -c 2,10` = one whole physical core: the basis
     for rows/s per physical core. For Cassandra this is `taskset -acp 2,10 <daemon-pid>` — **every** thread
     of the JVM, on a PID selected by *executable name* plus a >20-thread assertion, never `pgrep -f`
     (see the trap below).
   - **Client / loadgen → a physical-core set disjoint from the metered one, never the metered core.** The
     six `cas-scan.py` shards on CPUs 4,5,6,7,12,13; `flight-loadgen` on `4-7,12-15`.
   - **Profiler → off the metered core.** SJK under `taskset -c 12-15`.
   Then meter with CPU-wide `perf stat -C <cpus>`, never `-p` (§9.5). **And read the utilisation the driver
   emits** (`daemon_core_utilization` / `server_cpu_utilization_of_pinned_set`): **below ~0.9 you measured
   the client, not the engine** — §4 shows both the saturated reported arms (~1.00) and the unsharded
   negative controls (0.35 / 0.63) that this check exists to catch.
6. **Page-cache state**: cold = `sync; echo 3 > /proc/sys/vm/drop_caches`; warm = one full pre-pass, then
   measure. Do both, report separately — and **capture `/proc/<pid>/io` `read_bytes` per run** so eviction is
   *proven* rather than asserted (§4). Note what this does **not** give you: the JVM is not restarted, so a
   "cold" Cassandra arm still has warm JIT, key cache and buffer pool (§4). Restart it if you want that.
7. **Report cycles/row AND bytes-of-memory-traffic/row**, never CPU%. Report `/proc/<pid>/io`
   `read_bytes` / `rchar` / `syscr` **separately — never divide them** (that produced a bogus
   "~59 KB/syscall" in earlier work).
8. **On-CPU profiling only. Off-CPU analysis was NOT performed — declined here, not overlooked.** Every
   profile in this report is an on-CPU sampler (`perf record -F 999`, SJK `stcap`) and every headline is a
   saturated-core throughput number. No off-CPU work was done: no blocked-thread stacks, no scheduler-latency
   tracing, no wall-clock-vs-on-CPU decomposition. **Named blind spot:** on an arm whose pinned core measures
   ~100% utilised, off-CPU time is small by construction and on-CPU sampling is the right instrument — but
   that argument covers *these* arms, not the surface in general. Three things this report consequently
   cannot speak to: (a) **§9.6's unpinned regression**, where 1.98M voluntary context switches *are* the
   entire phenomenon and are exactly what an off-CPU profile would explain; (b) **the Flight arm's 0.865
   `server_cpu_utilization_of_pinned_set`** — ~13.5% of that arm's wall time is not on the metered CPUs and
   is unaccounted for by anything measured here; (c) any **latency** (as opposed to throughput) question.
   (a) and (b) are where a future off-CPU pass would pay for itself first.

### The literal commands

Every measurement point in §4/§7/§8 came from one of these. The drivers themselves are committed under
`docs/reports/ws0-3026-artifacts/`; the absolute paths inside them are the measurement box's and need
rewriting.

```bash
# ---- Arm 1: Cassandra (one measurement point per invocation) -----------------
# arm1c-cassandra.sh <label> <warm|cold> <count|rows> <cpus> <shards> [inflight] [ranges]
bash ws0-h2h/arm1c-cassandra.sh rep-cas-count-2x10-r1 warm count 2,10 6 8 512
bash ws0-h2h/arm1c-cassandra.sh rep-cas-rows-2-r1     warm rows  2    6 8 512
#   pins:    taskset -acp 2,10 <daemon-pid>          (ALL daemon threads)
#   meters:  perf stat -x, -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent -C 2,10
#   drives:  6 x  taskset -c {4,5,6,7,12,13} python3 ws0-h2h/cas-scan.py \
#                   --mode count --inflight 8 --ranges 512 --shards 6 --shard-index <i>
#   the queries, prepared, once per token range (512 equal spans over the full ring):
#     SELECT count(*) FROM events WHERE token(part_id) > ? AND token(part_id) <= ?
#     SELECT part_id, seq, event_time, blob_a, blob_b, device_id, metric_a, metric_b,
#            metric_c, payload, region, status
#       FROM events WHERE token(part_id) > ? AND token(part_id) <= ?          # --mode rows
#   protocol v5, fetch_size 5000, WhiteListRoundRobinPolicy(["127.0.0.1"])

# ---- Arm 2a: CQLite bare scan / scan+Arrow ----------------------------------
# arm2-cqlite.sh <label> <warm|cold> <scan|scan-arrow|scan-collect> <cpus>
bash ws0-h2h/arm2-cqlite.sh rep-cq-scan-2x10-r1 warm scan 2,10
#   perf stat ... -C 2,10 -- taskset -c 2,10 ws0-scan-harness \
#     --datasets-root <datasets> --stage-dir <datasets>/sstables \
#     --keyspace ws0 --table events --schema ws0-h2h/schemas/ws0-events.cql \
#     --mode scan --passes 1 --no-fold
#   warm = one untimed pre-pass in a SEPARATE process, then a fresh timed process
#   cold = sync; echo 3 > /proc/sys/vm/drop_caches      (page cache only — see §4)

# ---- Arm 2b: CQLite Flight --------------------------------------------------
# arm2-flight.sh <label> <warm|cold> <cpus> <step-seconds>
bash ws0-h2h/arm2-flight.sh rep-fl-2x10-r2 warm 2,10 195
#   taskset -c 2,10 cqlite-flight --data-dir <datasets>/sstables --listen 127.0.0.1:8815
#   taskset -c 4-7,12-15 flight-loadgen --endpoint http://127.0.0.1:8815 \
#     --ticket-template ws0-h2h/ws0-events-template.json --shape full --ramp 1 \
#     --step-duration 195s --round <label> --out flight-<label>.jsonl

# ---- §8 fio (all four arms; see ws0-results/rerun.sh) -----------------------
fio --name=layout --filename=$T --rw=write --bs=1M --size=48G --direct=1 \
    --iodepth=32 --ioengine=libaio --end_fsync=1 --output-format=json > 00-layout.json
COMMON="--filename=$T --direct=1 --ioengine=libaio --time_based --ramp_time=5 \
--group_reporting --output-format=json --log_avg_msec=1000 --percentile_list=50:90:99:99.9"
fio --name=randread-4k-qd1  $COMMON --rw=randread --bs=4k --iodepth=1  --runtime=300 ...
fio --name=randread-1M-qd32 $COMMON --rw=randread --bs=1M --iodepth=32 --runtime=120 ...
fio --name=seqread-4k-qd1   $COMMON --rw=read     --bs=4k --iodepth=1  --runtime=90  ...
fio --name=seqread-1M-qd32  $COMMON --rw=read     --bs=1M --iodepth=32 --runtime=120 ...

# ---- §7 memory bandwidth ---------------------------------------------------
gcc -O3 -march=native -fopenmp -o ws0-stream ws0-results/stream_bench.c
gcc -O3 -march=native -fopenmp -o ws0-readbw ws0-results/read_bw.c
OMP_NUM_THREADS=$N OMP_PROC_BIND=true OMP_PLACES=cores taskset -c 0-$((N-1)) ./ws0-stream 512 12
OMP_NUM_THREADS=$N OMP_PROC_BIND=true OMP_PLACES=cores taskset -c 0-$((N-1)) ./ws0-readbw 2048 10

# ---- §7 traffic-proxy calibration ------------------------------------------
taskset -c 2 perf stat -e cycles,instructions,l2_lines_in.all,l2_lines_out.non_silent \
  -- ws0-cqlite/membw_cal <buf_mib> 340        # passes must be large; see §7
```

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
| 1. Defensible X× + re-runnable method in `docs/reports/` | ✅ §1 (a **range**, 1.13–1.24× read path / 3.50× shipping path), §10, evidence committed under `ws0-3026-artifacts/` |
| 2. `fio` at both `bs=4k iodepth=1` and `bs=1M iodepth=32`, fair denominator noted | ⚠️ **OPEN** — measured on EBS with the full job shape committed (§8); this box has no instance store, so it cannot replace the i4i denominator, and the prior **711.8 MB/s** figure it exists to correct remains **shape-unknown** and is not a denominator. Separately, **no `fio` shape taken here prices CQLite's own scan I/O**, which is mmap-served (§9.3 / #3068) |
| 3. Measured achievable memory bandwidth per core | ✅ **10.73 pinned** / 10.84 unpinned single-core, 10.15 all-cores (§7) — use the pinned figure against pinned measurements |
| 4. Verdict on whether the roofline tiers survive | ✅ **verdict delivered, and it is NO** — **T3 refuted** at both inputs and **T1's derivation withdrawn** with no replacement ceiling offered (§7). The AC asked for a verdict, not for the tiers to hold; the tiers themselves now need re-deriving |
| 5. Cassandra structural difference named with `file:line` | ✅ §6 |

## 12. Follow-ups filed

- **#3058** (P1, confirmed) — Flight `do_get` runs k-way merge/compaction reconciliation unconditionally
  on a single SSTable. The dominant cause of the 3.50× shipping-path loss.
- **#3060** (P2, observed once) — `cqlite-flight` may never exit when signalled mid-stream.
- **#3061** (P3, observed once) — 1,017 MiB peak RSS with `Data.db` mapped twice; also asks whether the
  `<128MB` target is measured correctly at all. **The double mapping is now explained** (§2): it is #2210's
  dedicated `MADV_RANDOM` point-read mapping plus the unadvised scan mapping, both expected in this build.
  The RSS question is the part that stands.
- **#3068** — the scan path's positional source is backed by an **mmap**:
  `cqlite-core/src/storage/sstable/reader/mod.rs:608-613` maps `ScanSource::Mapped` → `MmapReadAt`, and
  #2876 (in this build, §2) deliberately keeps the scan on the *unadvised* mapping. This is why the measured
  scan issues **115 `read()` syscalls** rather than the ~4.4 KB preads the 0.17 program's ground truth
  describes (§9.3), and why **no `fio` shape taken in §8 prices CQLite's scan I/O**. Blocks #3031.
- **#1406** — noted the trailing zero-length chunk parity detail for the compressed-write path.
- **#3031** — read-size premise needs the mmap-vs-pread reconciliation in §9.3 before it can proceed.
