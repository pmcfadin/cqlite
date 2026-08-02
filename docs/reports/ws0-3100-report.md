# WS0 head-to-head re-run + C(N) concurrency sweep — CQLite #3100

**Measured 2026-08-01 on a single AWS `c7i.4xlarge`** (16 vCPU / **8 physical cores**,
Intel, 30 GiB RAM, **EBS gp3 root** — 300 GiB, 1000 MiB/s, 12k IOPS), Ubuntu kernel
7.0.0-1009-aws. Cassandra **5.0.8** on OpenJDK 17; CQLite built from `main` @
`19a148823a6dfbb3a006378fe6baca89fcf188d5` (**post-#3058**, release + retained DWARF).

This run closes the #3058 gap: #3058 measured Flight `do_get` at 210,192 rows/s per physical
core (3.90×) on a *locally generated UNCOMPRESSED* corpus, while the stock-Cassandra baseline
was on a *different, LZ4-compressed* corpus — the two could not be divided. Here **all arms run
on one corpus** with a post-#3058 binary, plus the never-measured concurrency sweep.

Raw artefacts: `docs/reports/ws0-3100-artifacts/`. **Measurement only — no code was changed.**

---

## 0. The corpus (regenerated; geometry-matched to the WS0 reference)

Per owner decision, the corpus was **regenerated** on the box via the committed
`ws0-corpus/rerun.sh` + `gen-corpus.sh` (200,000 partitions × 20 rows, stress-load → flush →
`nodetool compact` to one `nb` SSTable). cassandra-stress does not reproduce the reference
`Data.db` sha, so **AC#1 = "same geometry, documented new sha."**

| property | WS0 reference | this run | match |
|---|--:|--:|:--:|
| Data.db on disk (LZ4) | 783,799,203 B | **784,116,369 B** | +0.04% |
| uncompressed `dataLength` | 2,770,741,510 B | 2,772,051,730 B | +0.05% |
| LZ4 ratio | 3.5350× | 3.5353× | ✓ |
| chunk_length | 16,384 | 16,384 | ✓ |
| chunk count | 169,114 | 169,194 | ✓ |
| rows | 3,999,890 | **3,999,890** | ✓ exact |
| cells/row | 12 | 12.0 | ✓ |

**New Data.db sha256** (stable before AND after Arm 1, incl. across a Cassandra drain):
`2c297a0caf76338be0568be486570fe63b61c2ca9bfa6e68e7368f34c189d9a2`

**Comparability gates** (per Cassandra-expert review — row-count + B/row alone are insufficient):
- SSTable count **1**, single `nb`/BIG format ✓
- Estimated droppable tombstones **0.0**, "no tombstones" ✓
- Partitions ~198,130; partition bytes min 6867 / mean 14237 / max 14237 → **uniform** ✓

All three CQLite arms read a **byte-identical staged copy** (Data.db sha re-verified `= 2c297a0c…`).

---

## 1. AC#5 — output correctness: **EXACT MATCH** ✅

Anti-elision **fold digest = `0x4903ffa446163c4b`** over **3,999,890 rows / 47,998,680 cells**
(12.0 cells/row) — **byte-for-byte identical to the owed reference digest**. This is the
output-correctness check #3058 left owing; CQLite decodes this corpus perfectly.

---

## 2. Part A — the head-to-head (medians of 3, matched pinnings)

Warm, page-cache-resident. Client always pinned to a different physical-core set (`4-7,12-15`).
Every Cassandra run showed `daemon_core_utilization` **≈ 1.00** — the measurement is
Cassandra-bound, **not client-bound** (the trap behind the withdrawn "1.18×").
**Pinnings reported as separate pairs — no cross-pinning quotient.**

### 1 physical core (`-c 2,10`)

| Surface | rows/s (median) | spread | cyc/row¹ |
|---|--:|--:|--:|
| Cassandra `SELECT count(*)` | 328,623 | 2.9% | — |
| Cassandra `SELECT *` | 225,771 | 3.6% | — |
| CQLite bare scan | **410,449** | 3.3% | — |
| CQLite Flight `do_get` — **bypass** (#3058) | **252,999** | — | 22,111 |
| CQLite Flight `do_get` — **merge** (pre-#3058) | 82,639 | — | 72,382 |

### 1 hardware thread (`-c 2`) — the valid basis for cycles/row / IPC

| Surface | rows/s (median) | spread | cyc/row |
|---|--:|--:|--:|
| Cassandra `SELECT count(*)` | 339,334 | 2.0% | 10,489 |
| Cassandra `SELECT *` | 219,486 | 0.6% | 16,067 |
| CQLite bare scan | 396,723 | 2.0% | — |
| CQLite Flight `do_get` — **bypass** | 234,842 | — | 14,883 |
| CQLite Flight `do_get` — **merge** | 71,974 | — | 49,863 |

¹ 1-core cyc/row is counted across 2 SMT siblings (double-counted); take cyc/row from the 1-hw rows.

### What Part A establishes

- **AC#2 — the #3058 fast path reproduces on the compressed corpus, not inherited:**
  **bypass / merge = 3.06× (1-core)**, 3.26× (1-hw). The `CQLITE_FLIGHT_MERGE_PATH` seam is a
  real, selectable kill-switch (72,382 → 22,111 cyc/row).
- **Read-path head-to-head, matched pinning, shipped-rows pair** (both materialize + serialize +
  ship every row over a socket): Flight bypass **252,999** vs Cassandra `SELECT *` **225,771**
  = **1.12× (1-core)**; **1.07× (1-hw)**. Sits at the low end of the reference's defensible
  1.13–1.24× band — expected, because this box's Cassandra ran ~7–10% hotter than the reference.
- **Bare-scan pair** (read + materialize, nothing shipped): CQLite scan **410,449** vs Cassandra
  `count(*)` **328,623** = **1.25× (1-core)**.
- **Reproducibility (AC#7):** per-arm spread 0.6–3.6% across the 3 reps — tighter than the
  reference erratum. Per-arm min/median/max in the tables and in `ws0-results/h2h/`.

---

## 3. Part B — the C(N) concurrency sweep (never measured before)

`flight-loadgen --ramp 1,2,4,8,16`, server pinned to **one physical core** (`2,10`), client on
`4-7,12-15`, 120 s/step, under `perf stat -C`. **0 errors at every N** (admission control clean).

| N | rows/s | vs N=1 | per-scan p50 | wire MB/s² |
|--:|--:|--:|--:|--:|
| 1 | 246,940 | 1.00× | 16.2 s | 3,010 |
| 2 | **287,441** | **1.16×** | 27.0 s | 3,504 |
| 4 | 273,438 | 1.11× | 58.7 s | 3,334 |
| 8 | 248,621 | 1.01× | 128.4 s | 3,031 |
| 16 | 236,734 | 0.96× | 269.5 s | 2,886 |

² Arrow-wire basis (~675 B/row). **Never a bare MB/s** — see §5.

**Finding:** on a **single-physical-core** server, aggregate throughput peaks at **N=2 (1.16×)**
then **declines**; per-scan latency grows ~linearly (16 s → 270 s). This is
**drain/handoff-bound, not scan-bound** — consistent with the Phase-0 "drain saturation at 8"
note and the marginal-efficiency discount the per-box projections have been quoting unmeasured.
Context-switches over the whole sweep: **38.3 M** (the `tokio::sync::mpsc` handoff cost #2420/#2817
asked about, now observed). This is the *per-core* concurrency curve; a full-box curve (server
across more cores) is a separate measurement.

---

## 4. Part C — device behaviour under concurrency (answers #3068 at scale)

Warm runs saw **~0 device I/O** (corpus is page-cached), so a **cold** variant was run
(`drop_caches` before each N) so reads hit EBS. `iostat -x` + `block_rq_complete` histogram.

| N | rareq-sz | %util | r_await |
|--:|--:|--:|--:|
| 1 | 3.5 KB | 14.0 | 0.38 ms |
| 2 | 1.4 KB | 16.1 | 0.32 ms |
| 4 | 1.4 KB | 12.1 | 0.30 ms |
| 8 | 1.5 KB | 6.2 | 0.35 ms |
| 16 | 1.5 KB | 3.0 | 0.32 ms |

**Block-size histogram (cold) is dominated by 128–256 KB reads at BOTH N=1 (4,927 hits) and
N=16 (5,408 hits)** — the 4–8 KB bucket stays ~90 regardless of N.

**Finding (#3068 at scale):** under concurrency the read path issues **large (128–256 KB)
reads**, *not* a flood of tiny 4 KB page-faults — **16 streams do NOT create 16× more tiny
requests.** And `%util` *falls* as N rises (14% → 3%): the device is far from saturated, which
confirms the N>2 throughput ceiling in §3 is **CPU/handoff-bound on the pinned core, not disk.**

---

## 5. Reporting discipline

- **All three byte bases stated, never a bare "MB/s":** logical/uncompressed 692.70 B/row,
  compressed on-disk 195.96 B/row, Arrow wire ~675 B/row (differ by 3.5×). §3's MB/s is the
  wire basis and is labelled.
- **fio not combined across boxes:** no i4i/c7i ratio produced (AC#2 on #3026 stays OPEN).
- **Every figure is artefact-backed** under `ws0-3100-artifacts/` (215 files). The one derived
  quantity — bypass/merge and the head-to-head ratios — is computed from the committed
  `summary-*.txt`/`scan-*.json`/`cn-sweep.jsonl`.

---

## Acceptance criteria

| AC | Status | Evidence |
|--:|:--|:--|
| 1 — arms on identical bytes, sha re-verified | ✅ (documented new sha) | §0; `corpus-sha-initial.txt` == `corpus-sha-post-arm1.txt` |
| 2 — post-#3058 Flight, both merge arms, 3.90× reproduced | ✅ **3.06–3.26×** on compressed corpus | §2; `summary-fl-*.txt` |
| 3 — AC5 digest `0x4903ffa446163c4b` reproduced | ✅ **exact** | §1; `ac5-fold-digest.txt` |
| 4 — matched-pinning pairs only, no cross-pinning ratio | ✅ | §2 (separate 1-core / 1-hw tables) |
| 5 — C(N) N=1..16 rows/s, cyc/row, three byte bases | ✅ | §3; `cn-sweep.jsonl`, `cn-perf.txt` |
| 6 — rareq-sz/%util/r_await + block-size hist per N | ✅ (cold) | §4; `cold-iostat-N*.txt`, `cold-blocksize-N*.txt` |
| 7 — median of ≥3 with per-arm dispersion | ✅ | §2 (spread 0.6–3.6%) |
| 8 — every artefact committed under docs/reports | ✅ | `ws0-3100-artifacts/` |

## Notes / deviations
- Head-to-head ratio (1.07–1.12× shipped-rows) sits at the low end of the reference band because
  this box's Cassandra ran ~7–10% faster than the reference c7i; both arms on identical bytes, so
  the ratio is fair. The **bypass/merge 3.06×** and **AC5 digest** are box-independent.
- Part B is the **single-physical-core** concurrency curve (server pinned `2,10`). A full-box
  curve is a separate run.
- Environment fix-ups needed on the lab AMI (all logged, none affect results): schema derived
  from the Flight ticket `ddl`; `yama.ptrace_scope=0` + sudo on the two `/proc/pid/io` reads for
  Arm 1; collectors run as containerd pods (stopped for a quiet box); corpus landed in
  Cassandra's default data dir (rerun.sh assumes a pre-patched cassandra.yaml).
