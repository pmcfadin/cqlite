# Performance: methodology, reproducibility, and the CI gate

This page explains what the in-repo performance gate enforces, why CI absolute
numbers are not authoritative, how to reproduce benchmark results locally, and
why ~282 ops/sec is the expected single-writer throughput when durability is on.

For the operational side — *finding and fixing* the bottlenecks the gate
measures (pprof flamegraphs, dhat heap profiling against the 128 MiB budget,
and the `scripts/profile.sh` improvement loop) — see
[profiling.md](profiling.md).

---

## 1. What the gate enforces vs. what it tracks

The perf regression gate (`perf-regression.yml`) divides benches into two
classes, configured in
[`cqlite-core/benches/perf-gate.json`](../cqlite-core/benches/perf-gate.json).

| Class | Benches | CI behavior |
|-------|---------|-------------|
| **Strictly gated** | `read/point_lookup`, `read/clustering_slice`, `read/full_scan`, `read/type_heavy`, `write/ingest_wal_off`, `write/flush` | Non-zero exit (merge blocked) if Criterion median regresses beyond per-bench `threshold_pct` (default 10%). |
| **Advisory** | `write/ingest_wal_on` | Delta always reported in CI output. **Never causes a non-zero exit**, regardless of magnitude. |

The distinction tracks a real technical difference:

- **Strictly gated benches are CPU-bound.** `read/*` runs queries over fixed
  SSTable fixtures. `write/ingest_wal_off` runs a 256-row ingest loop with
  `Durability::Disabled` — WAL append and fsync are skipped, so every cycle is
  cqlite code, not kernel I/O. `write/flush` flushes a pre-filled memtable.
  These are stable enough on shared runners to detect regressions reliably.

- **`write/ingest_wal_on` is I/O-dominated.** It runs the same 256-row loop
  with `Durability::SyncEachWrite` (the default): every `write` call calls
  `wal.append()` + `wal.sync()` (one fsync). On shared GitHub-hosted runners,
  fsync latency swings 10–100 ms run-to-run depending on host load and
  underlying storage. The variance exceeds the 10% threshold routinely, so it
  cannot gate without producing constant false-positive failures on PRs that
  touch nothing performance-related.

The gate compares the PR branch against `main` **on the same runner in the same
job**. This relative comparison cancels out machine-level noise; only
*relative* change is measured.

See [`cqlite-core/benches/README.md`](../cqlite-core/benches/README.md) for
the full gate specification, the tolerance model, and how to adjust thresholds.

---

## 2. Why CI absolute numbers are not authoritative

Even for strictly gated benches, the absolute Criterion numbers from CI are
informational only. They tell you "X ns on a GitHub-hosted ubuntu-latest runner
at the time this PR ran." They do not tell you "cqlite takes X ns on your
hardware."

Reasons the CI absolute number should not be quoted as the library's performance:

- **Shared runners are not a controlled environment.** CPU frequency scaling,
  co-tenant load, kernel scheduler behavior, and ephemeral disk performance all
  vary between runs. The gate isolates these by always running PR and main
  side-by-side on the same runner — only the delta is meaningful.

- **fsync latency drives `ingest_wal_on` entirely.** A single fsync on a shared
  runner can take anywhere from 1 ms to 100 ms depending on the underlying
  virtual disk and host load at that moment. Because `SyncEachWrite` does one
  fsync per `write` call, the reported throughput reflects disk latency, not
  cqlite code. This is why `ingest_wal_on` is advisory.

- **Absolute baselines drift.** GitHub periodically migrates runner hardware.
  A committed absolute-time baseline would go stale and require constant
  maintenance. cqlite uses `main` as the living baseline — merging a legitimate
  change automatically updates it.

For authoritative absolute numbers, run the benches locally on fixed hardware
(see §3 below), or use a dedicated test machine with a tmpfs-backed WAL dir to
isolate CPU-bound behavior from storage.

---

## 3. How to reproduce locally

### Prerequisites

- Rust 1.85+, `cargo` in `~/.cargo/bin/`
- Test data fetched: `bash test-data/scripts/fetch-datasets.sh`
- `CQLITE_DATASETS_ROOT` set (or defaults to `test-data/datasets` from
  `CARGO_MANIFEST_DIR`)

### Running the full write bench suite

```bash
# Full Criterion run — writes HTML reports to target/criterion/
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  ~/.cargo/bin/cargo bench -p cqlite-core --bench write \
  --features write-support
```

### Running the full read bench suite

```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  ~/.cargo/bin/cargo bench -p cqlite-core --bench read \
  --features cli-helpers
```

### Matching the CI gate's sample parameters

The gate workflow uses tighter timing to keep CI wall time short:

```bash
# Write bench — CI-equivalent parameters
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  ~/.cargo/bin/cargo bench -p cqlite-core --bench write \
  --features write-support \
  -- --sample-size 20 --warm-up-time 1 --measurement-time 3

# Read bench — CI-equivalent parameters
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  ~/.cargo/bin/cargo bench -p cqlite-core --bench read \
  --features cli-helpers \
  -- --sample-size 20 --warm-up-time 1 --measurement-time 3
```

For a stable local number, omit those flags and let Criterion use its defaults
(`--sample-size 100`, `--warm-up-time 3`, `--measurement-time 5`). The longer
run gives a tighter confidence interval.

### The `Durability` knob

`WriteEngineConfig` accepts a `Durability` setting that controls whether each
`write` call fsyncs the WAL:

| Setting | What happens | Throughput character |
|---------|-------------|----------------------|
| `Durability::SyncEachWrite` (default) | `wal.append()` + `wal.sync()` on every `write` | Bounded by fsync latency (~1 000 / fsync_ms ops/sec) |
| `Durability::Disabled` | WAL skipped entirely; mutations accumulate in the memtable only | CPU-bound; orders of magnitude faster |

```rust
use cqlite_core::storage::write_engine::{Durability, WriteEngineConfig};

// CPU-bound measurement — no per-write fsync
let config = WriteEngineConfig::new(data_dir, wal_dir, schema)
    .with_durability(Durability::Disabled);
```

Use `Durability::Disabled` when benchmarking pure cqlite throughput or for
bulk-load pipelines that can trade crash safety for throughput. Use
`SyncEachWrite` (the default) in production. See
[`docs/using-cqlite-core-as-a-dependency.md` §5](using-cqlite-core-as-a-dependency.md#5-write-path-concurrency--durability-model)
for the full concurrency and durability model.

### tmpfs vs disk — WAL directory matters

`ingest_wal_on` throughput depends entirely on where the WAL files live:

| Storage | Typical fsync latency | Expected `ingest_wal_on` throughput |
|---------|----------------------|-------------------------------------|
| tmpfs (in-memory) | <0.1 ms | ~10 000+ ops/sec |
| NVMe SSD | 0.1–1 ms | ~1 000–10 000 ops/sec |
| SATA SSD | 1–5 ms | ~200–1 000 ops/sec |
| Spinning disk | 5–20 ms | ~50–200 ops/sec |
| Shared CI runner (virtual disk) | 1–100 ms (variable) | 10–1 000 ops/sec (noisy) |

To isolate the WAL from disk latency, point `wal_dir` at a tmpfs mount:

```bash
# Linux example
sudo mount -t tmpfs tmpfs /mnt/tmpfs
```

Then build your `WriteEngineConfig` with `wal_dir = /mnt/tmpfs/cqlite-wal`.
This removes fsync from the equation and makes `ingest_wal_on` behave like
`ingest_wal_off`.

---

## 4. Point-lookup cost model

### O(1) index-based lookup (current, after #553)

A `get()` call for a known partition key goes through:

1. **Bloom filter** (O(1)) — decides whether the key *might* exist in this SSTable.
   Currently skipped for V5CompressedLegacy/NB format SSTables (tracked separately).
2. **Index.db raw-key lookup** (O(1)) — `lookup_partition_with_index` looks up the raw
   partition key bytes directly in the in-memory `IndexReader::key_lookup` HashMap.
   This was restored to O(1) in Issue #553; prior to that fix every lookup missed
   (due to a digest/raw-key mismatch introduced in #552) and fell back to the scan path.
3. **On hit**: seek to offset in Data.db and parse a single partition (O(1) I/O).
4. **On miss**: return `None` immediately.

`read/point_lookup` is strictly gated in CI.

### O(n) sequential scan fallback

If the index miss occurs (wrong key, missing Index.db, or pre-#553 code), the reader
falls back to `scan_for_key` which decompresses and scans **all** chunks in Data.db.
For a 1 000-partition SSTable this is O(file-size) per lookup. This path exists purely
as a correctness safety net; it should not be the hot path for any production workload.

---

## 5. The single-writer / per-fsync durability model

`WriteEngine` is a **single-writer** component. `write`, `write_async`,
`flush`, `maintenance_step`, and `close` all take `&mut self`, so the borrow
checker prevents concurrent calls. There is no internal sharding or
multi-writer mode.

With `Durability::SyncEachWrite`, each `write` call does:

1. Append the serialized mutation to the WAL file.
2. `fsync` the WAL file descriptor.
3. Insert into the in-memory memtable.

Adding threads does **not** raise single-writer throughput: writers serialize
through the single `&mut self` engine, and each still pays one fsync.

For the complete concurrency model, intended usage patterns, and the async write
path, see
[`docs/using-cqlite-core-as-a-dependency.md` §5](using-cqlite-core-as-a-dependency.md#5-write-path-concurrency--durability-model).
That page is the canonical reference; this one does not duplicate it.

---

## 5. Is ~282 ops/sec expected?

**Yes.** 282 ops/sec with `Durability::SyncEachWrite` on typical spinning or
virtual disk is expected disk-bound behavior, not a cqlite regression.

Here is the math:

```
throughput (ops/sec) ≈ 1 000 / fsync_latency_ms
```

At 3–4 ms per fsync (a reasonable number for a shared virtual disk or SATA
SSD), the formula gives:

```
1 000 / 3.5 ms ≈ 286 ops/sec
```

282 ops/sec sits squarely in that range. The bottleneck is the kernel waiting
for the storage device to confirm the write is on stable media — cqlite is idle
for the entire duration of the fsync. No amount of cqlite optimization changes
this number; only faster storage does.

**With `Durability::Disabled`, the same write loop runs orders of magnitude
faster.** The in-repo `write/ingest_wal_off` bench (the CPU-bound, strictly
gated measurement) demonstrates this: observed speedup is ~430× over
`ingest_wal_on` on the same runner, because the fsync is gone and only
serialization + memtable insertion remain.

If you need higher write throughput:

1. **Use `Durability::Disabled` for bulk loads** — throughput becomes CPU-bound
   and scales with core speed.
2. **Group more cells into each `Mutation`** — the per-write overhead (fsync or
   memtable insert) is fixed; wider mutations amortize it across more data.
3. **Use faster storage** — NVMe with `SyncEachWrite` gives 1 000–10 000
   ops/sec; tmpfs gives >10 000 ops/sec.
4. **Batch writes and flush less frequently** — `flush` is amortized; the
   per-write fsync is not.

The `write/ingest_wal_on` bench in this repo tracks the fsync-bound number as
an advisory metric. It is not gated because shared-runner fsync variance (10–100
ms) makes strict gating impractical. On your own hardware, run it to characterize
your storage stack.
