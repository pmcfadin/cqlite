# Lane 1 — Thread-per-core in Rust vs. our measured LLC-capacity ceiling

**2026-08-04, owner-commissioned research survey (lane 1 of 3).** Synthesis + verdicts:
`throughput-2026-08-research-synthesis.md`. Claims tagged MEASURED vs FOLKLORE; our own numbers
(mission doc §0/§6) treated as ground truth.

**Bottom line:** every documented win of thread-per-core (TPC) is a *coherence, synchronization,
scheduling, or OS-interface* win. None of the primary sources claims — or measures — an **LLC
capacity** win. Our measured signature (LLC-load-misses/row 11.9→38.1 while instructions/row, L1d
loads/misses, and branch-misses stay flat within 1.3%) is the signature TPC does **not** address.
Recommendation: against a runtime migration; for two footprint/admission levers plus one
per-core-path audit.

## 1. Seastar / ScyllaDB / Redpanda — what TPC actually solved

**MEASURED / claimed by authors:**
- Seastar's rationale names locks ("unscalable programming practices (such as taking locks) can
  devastate performance on many cores"), atomics, and memory-ordering fences — "dramatically slower
  than operations that involve only data in a single core's cache"
  ([Seastar tutorial](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md)). That is
  **coherence traffic and cache-line bouncing**, not cache capacity.
- ScyllaDB's 2024 post is qualitative: thread contention, uneven resource distribution, latency
  spikes; **no cache measurements, no LLC discussion**; the linear-scaling claim is unsourced
  ([ScyllaDB](https://www.scylladb.com/2024/10/21/why-scylladbs-shard-per-core-architecture-matters/)).
- Redpanda names context-switch overhead, heavy concurrency control, kernel buffer cache — **no
  benchmark numbers**
  ([Redpanda](https://www.redpanda.com/blog/engineering-redpanda-multi-core-hardware);
  [QCon 2023](https://qconlondon.com/presentation/mar2023/performance-adventures-thread-core-async-redpanda-and-seastar)).
- The one peer-reviewed measurement: Enberg et al., ANCS'19 — TPC KV store cut tail latency up to
  71% vs Memcached ([paper](https://penberg.org/papers/tpc-ancs19.pdf)); its own attribution is IRQ
  affinity, request steering, syscall overhead, NUMA; concedes shared-nothing "can limit system
  throughput for skewed workloads." No cache counters for its "cache efficiency" aside.

**FOLKLORE:** "TPC improves cache locality" — asserted by Seastar/Datadog/monoio docs, measured by
none of them. HN practitioner consensus is about *contention*, not capacity
([HN 38503439](https://news.ycombinator.com/item?id=38503439)).

**Counterpoint:** Seastar *statically and equally divides memory between shards*
([docs](https://docs.seastar.io/master/split/24.html)). Over a shared read-only mmap'd corpus,
per-shard partitioning tends to *duplicate* per-shard structures — the wrong direction for a
capacity-bound workload.

## 2. Rust TPC runtimes — maturity, and what their benchmarks measure

| Runtime | Status | Benchmark shape | Relevance |
|---|---|---|---|
| [glommio](https://github.com/DataDog/glommio) | Alive but fragile — open ["Call for maintainers"](https://github.com/DataDog/glommio/issues/707) (Mar 2026) | [Launch post](https://www.datadoghq.com/blog/engineering/introducing-glommio/): zero benchmarks; cites Enberg | Low (io_uring + I/O-bound thesis) |
| [monoio](https://github.com/bytedance/monoio) | Production at ByteDance ([monolake](https://github.com/cloudwego/monolake) proxies) | [100-byte network echo](https://github.com/monoio-rs/monoio/blob/master/docs/en/benchmark.md): ~2× tokio @4 cores, ~3× @16; *worse* than tokio at 1 core | Low (syscall/IRQ amortization, not decode) |
| [tokio-uring](https://github.com/tokio-rs/tokio-uring) | Explicitly young, sparse releases | n/a | Low |

**There is no published TPC-vs-tokio benchmark for CPU-bound columnar streaming.** Every headline
number is a network echo test. Adopting TPC for our shape would be extrapolation, not replication.

## 3. tokio work-stealing's cache cost — and what analytical engines do

- Tokio's scheduler post is about **L1/L2 message-passing latency**, not LLC; the LIFO-slot fix
  bought chained_spawn 12×, ping_pong 2.3×, Hyper +34% — all message-passing microbenchmarks
  ([tokio.rs](https://tokio.rs/blog/2019-10-scheduler)).
- **Mechanism check against our data:** Ice-Lake-class LLCs are shared, non-inclusive, distributed
  as ~1.5 MiB slices ([WikiChip](https://fuse.wikichip.org/news/4734/intel-launches-3rd-gen-ice-lake-xeon-scalable/)),
  lines homed to slices by an undocumented **address hash**, not the reading core
  ([Maurice et al., RAID'15](https://cmaurice.fr/pdf/raid15_maurice.pdf)). A migrated task pays
  L1d/L2 refill; the line is *still in LLC*. Migration inflates L1/L2 misses, **not** LLC misses.
  Our L1d loads/misses are flat within 1.3% while LLC misses tripled — **affirmative evidence that
  work-stealing migration is not our mechanism.**
- **InfluxDB IOx** uses tokio *for* CPU-bound analytics; its `DedicatedExecutor` protects tail
  latency/liveness, **not** cache behavior; per-task overhead "~10 nanoseconds range," amortized by
  vectorizing thousands of rows per task
  ([InfluxData](https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/)).
- **DataFusion**: core scaling is a known open problem; `RepartitionExec` round-robin "is not NUMA
  friendly" — qualitative, no measured cache numbers
  ([datafusion#7001](https://github.com/apache/datafusion/issues/7001)).
- **Polars** is rayon ([docs](https://docs.pola.rs/user-guide/misc/multiprocessing/)) — which also
  work-steals. Analytical engines avoid *fine-grained* stealing by making the unit a batch, not by
  avoiding stealing.
- **tokio has no core-pinning API**; `Builder::on_thread_start` + `core_affinity` makes pinning a
  ~20-line experiment, not a migration
  ([docs.rs](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html)).

## 4. Adjudication: does TPC help an LLC-*capacity* problem?

**Steelman FOR (the one argument that survives):** run-to-completion bounds simultaneously-live
working sets to one per core; under tokio, K in-flight scans across W workers make live footprint ≈
tasks-in-flight × per-scan state. If S=6 has >6 live scans resident, TPC would shrink aggregate
footprint. This is the morsel-driven insight
([Leis et al., SIGMOD'14](https://dl.acm.org/doi/10.1145/2588555.2610507)) — achieved *with*
work-stealing at coarse granularity.

**Steelman AGAINST (stronger, on our hardware):**
1. **Pinning cannot partition a hash-sliced LLC** ([RAID'15](https://cmaurice.fr/pdf/raid15_maurice.pdf));
   only Intel CAT/RDT partitions ways ([Intel RDT](https://www.intel.com/content/www/us/en/architecture-and-technology/resource-director-technology.html),
   [CMU TR](https://www.cs.cmu.edu/~sboucher/tr2017.pdf)) — and CAT redistributes, it does not create.
2. **Shared-socket caveat**: 54 MiB is the full-socket L3 of the Xeon 8375C behind i4i
   ([AWS](https://aws.amazon.com/ec2/instance-types/i4i/)); a virtualized slice shares it with
   co-tenants no runtime can reach. (Our #3224 numbers are from i4i.metal — single-tenant — but
   production pods on shared slices face this.)
3. **TPC tends to increase footprint** via per-shard duplication; over a shared read-only mmap,
   sharing is the asset.
4. Our counters already acquitted what TPC fixes.

**Verdict: TPC is the wrong-shaped lever for the measured mechanism.** Its one transferable benefit
— bounding concurrent live working sets — is available from admission control we already ship
(`--max-concurrent-scans`), at ~0 architectural cost. The mechanism-matched lever for a capacity
ceiling is the X100/DuckDB one: shrink the per-unit working set
([MonetDB/X100, CIDR'05](https://www.cidrdb.org/cidr2005/papers/P19.pdf);
[DuckDB vectors](https://duckdb.org/docs/current/internals/vector);
[DataFusion configs](https://datafusion.apache.org/user-guide/configs.html)).

## 5. Cheapest falsifiable experiments (adopted into #3299's protocol)

- **E1 — direct occupancy (~1 h, first):** Linux `resctrl` / Intel CMT `llc_occupancy` per
  monitoring group at S=1 and S=6. Occupancy/scan ~constant and Σ crossing effective LLC at S≈4–6 ⇒
  capacity confirmed (lever = footprint/admission; TPC dead). Σ well under 54 MiB while misses
  triple ⇒ capacity hypothesis wrong (suspect prefetcher throttling, TLB, co-tenant eviction).
- **E2 — footprint sweep at fixed S=6 (~half day, the discriminator):** hold 6 threads, vary
  per-scan working set ~4× across ~4 points; plot LLC-misses/row AND rows/s vs Σ footprint. Knee
  near effective LLC ⇒ capacity confirmed and the batch-sizing lever is licensed + sized; flat ⇒
  the footprint lever is dead before anyone writes it.
- **E3 — pin / N single-threaded runtimes (~1 day, optional last):** expected null given §4; a
  citable closure either way. If misses drop materially, migration-driven resident-set duplication
  was real — and pinning alone captured it without changing runtimes.

## Ranked candidates (max 3)

1. **Right-size the per-scan working set (batch rows + read-ahead/decode buffers) to a stated LLC
   budget.** Gap: 6-core scaling. Recover roughly half to two-thirds of the 29% marginal loss if E2
   shows a knee (mechanism confidence high, magnitude medium-low). Validate: E2, reporting rows/s
   alongside misses/row (tiny-batch overhead is DataFusion-documented).
2. **Make `--max-concurrent-scans` LLC-footprint-aware** (cap = effective_LLC × safety /
   bytes_per_scan from E1). Gap: scaling stability under overload; low confidence it raises peak.
   Honest limit: co-tenant LLC pressure is unobservable — budget must be measured, not computed.
3. **Audit the do_get path for per-ROW work across the 4 stacked bounded channels.** Gap: per-core
   base (1.2 µs/row is far too large for tokio's ~10 ns/task amortized overhead — if any hop is
   per-row rather than per-batch, that is the mispricing). Validate: count channel sends and
   allocations per 1M rows; sends/row ≫ 4/batch ⇒ fix batching granularity, not the runtime.

**Explicitly NOT recommended:** migrating to glommio/monoio/tokio-uring, or restructuring to
shared-nothing. Mechanism unmatched; evidence base is echo benchmarks; glommio is seeking
maintainers; the cheap subset (pinning) is testable in a day as E3 without committing to anything.
