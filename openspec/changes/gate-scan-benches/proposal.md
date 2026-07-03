## Why

`cqlite-core/benches/concurrent_scan.rs` and `cqlite-core/benches/read_while_write.rs` already exist and
measure exactly the read-path properties the July 2026 read-path audit (Epic A, #1513) cares about —
concurrency scaling of a shared `SSTableReader` (#815/#917) and reader-side latency under concurrent
write load (#1143) — but **neither is in `cqlite-core/benches/perf-gate.json`**. Both bench source files
deliberately opted out with the rationale that *absolute-timing* gating is too noisy on shared CI
runners. That rationale is correct for absolute times, but it leaves a real hole: today a concurrency
regression (e.g. someone reintroduces a shared `Mutex` on the read path, re-serializing scans — exactly
what #815 removed) or a reader-side regression under write contention **merges silently**.

Epic A is "measurement first": no downstream optimization claim is trustworthy until the gate watches
the path. This change closes the concurrency + read-under-write hole without reintroducing runner-noise
flakiness, by gating a **machine-independent invariant** rather than an absolute time.

Audit / measurement facts that constrain the design (baseline measured on this machine, `--sample-size
20 --warm-up-time 1 --measurement-time 3`):
- `concurrent_scan` throughput scaling `throughput(n4)/throughput(n1) = 4 · median(n1)/median(n4)`:
  **buffered ≈ 2.98**, **mmap ≈ 3.19**. A shared-mutex re-serialization collapses `median(n4) → ~4·
  median(n1)`, i.e. **scaling → ~1.0**. The *ratio within a single run* cancels machine speed, so it is
  robust to the cross-machine variance the bench doc warned about — unlike an absolute-time comparison.
- `read_while_write` exposes reader-side **p99 only via stderr `eprintln`** (not a machine-readable
  Criterion metric); its Criterion **median** (per-iteration aggregate reader latency under write load)
  is machine-readable and stable (p50 ≈ 5.0–5.4 ms across runs; p99 6–9 ms is the noisy tail).
- `read_while_write` has a **pre-existing zero-ingest race**: readers finish and set `stop=true` before
  both `spawn_blocking` writers are scheduled, so the `total_written > 0` correctness floor panics on
  most runs. The bench cannot be gated until this bit-rot is fixed.

## What Changes

- **Add a new `scaling_floors` check to the perf gate** (`scripts/ci/check_perf_regression.py` +
  `cqlite-core/benches/perf-gate.json`): an *intra-run* invariant `degree_ratio · median(baseline_id) /
  median(scaled_id) ≥ min_scaling`, evaluated on the PR (`new`) baseline only. This is additive gate
  wiring — a new check kind alongside the existing pr-vs-main median-regression check.
- **Gate `concurrent_scan` n4/n1 scaling** for both backends: `concurrent_scan/buffered/n4` vs
  `.../n1` and `concurrent_scan/mmap/n4` vs `.../n1`, `degree_ratio: 4`, **`min_scaling: 1.8`** —
  set well below observed healthy (≈2.98/3.19) for CI-runner headroom, well above the serialized-
  regression value (≈1.0) so it fails decisively. The floor and its derivation are documented in the
  policy file and README.
- **Gate `read_while_write` median** as a standard strict median-regression entry
  (`read_while_write/readers6_writers2`, `threshold_pct: 25`) with a note that the **p99 tail is owned
  by A2's tail-latency harness (#1563)**; the gate watches the stable median, not the noisy tail.
- **Fix the `read_while_write` zero-ingest bit-rot** minimally: each writer ingests at least once before
  honoring `stop` (a do-while), guaranteeing `total_written ≥ WRITERS`. This does not change what the
  bench measures (reader-side latency under sustained write contention).
- **Wire both benches into `.github/workflows/perf-regression.yml`**: add `--bench concurrent_scan
  --bench read_while_write` to both the PR and main Criterion runs so the gate has data.
- **Demonstrated red-run** (committed as a deterministic test + shown in the PR): synthetic serialized
  medians (`median(n4) ≈ 4·median(n1)`) drive scaling to ~1.0 and the gate exits non-zero.
- **Docs**: `cqlite-core/benches/README.md` gains a "concurrency scaling floor" subsection; the two
  bench module docs are updated to reflect that they are now gated (scaling floor / strict median),
  superseding the "Not a CI gate (by design)" note.

## Non-goals

- **No read-path / write-path production-code changes.** The only source edit is the `read_while_write`
  bench's writer-loop robustness fix (bench code, not library code); everything else is gate wiring,
  policy, tests, and docs.
- **No p99/tail gating.** The reader-side p99 is A2's (#1563) harness responsibility; this gate uses the
  machine-readable median with a documented note.
- **No absolute-time gating of the concurrency benches** — the whole point is to gate a machine-
  independent *ratio* (scaling floor), not committed absolute timings.
- **No change to the existing read/write median-regression benches or their thresholds.**
