# Resource-Leak Soak Test (issue #2013)

A long-running soak test that proves CQLite's SSTable read path does not leak file
descriptors or resident memory across many open/scan/drop cycles.

- Test: `cqlite-core/tests/soak_resource_leak.rs`
- Nightly workflow: `.github/workflows/soak-resource-leak.yml`

## What it does

Per iteration it opens a real `SSTableReader` on a real fixture `Data.db`, drains a
full `scan_stream()` to completion, then drops the reader — exercising the real
reader/scan open+drop path (no mocks). Every `K` iterations it samples two OS
counters from `/proc/self`:

- **open FD count** — `read_dir("/proc/self/fd").count()`
- **RSS bytes** — resident pages from `/proc/self/statm` × page size

At the end the FD and RSS series are fed to a pure detector (`analyze_samples`):

- **FD rule** — a run of `>= N` consecutive strictly-increasing FD samples (after a
  warmup head) is a leak.
- **RSS rule** — resident growth over the post-warmup baseline must stay under a
  bounded ceiling.

Both rules ignore the first ~10% of samples (warmup: JIT/alloc/mmap settling). The
full series is always printed (and embedded in any failure message) so the leak-onset
iteration is visible.

## Variants

- `soak_open_scan_drop_no_cache` — block cache DISABLED; isolates FD/mmap leaks in the
  reader open/drop path (RSS ceiling 96 MiB).
- `soak_open_scan_drop_with_cache` — block cache ENABLED at a small bounded 4 MiB
  budget; RSS must plateau at a known ceiling (budget + slack), catching cache-entry
  leaks distinct from FD leaks.

Both are `#[ignore]` so they never run in the normal gate; run them with `--ignored`.

Always-on regression guards (run in the normal suite, fast):

- `sabotage_fd_leak_is_detected` — runs the loop with a DELIBERATE per-iteration fd
  leak and asserts the detector fires on real `/proc` samples. This is a permanent
  self-test; there is no leak toggle anywhere in library code.
- `analyze_detects_monotonic_fd_growth`, `analyze_ignores_bounded_wiggle`,
  `analyze_detects_rss_growth` — pure, deterministic detector unit tests (no `/proc`).

Linux-only (uses `/proc`); on other OSes the soak bodies skip with a notice.

## Run it locally

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets   # real Data.db binaries
CQLITE_SOAK_ITERATIONS=120 cargo test -p cqlite-core --test soak_resource_leak \
  --features write-support -- --ignored --nocapture
```

The fast self-tests (no `--ignored`) run with a plain
`cargo test -p cqlite-core --test soak_resource_leak --features write-support`.

### Env knobs

| Var | Default | Meaning |
|-----|---------|---------|
| `CQLITE_SOAK_ITERATIONS` | `120` (nightly `500`) | loop iterations |
| `CQLITE_SOAK_SAMPLE_EVERY` | `10` | sample every N iterations |
| `CQLITE_REQUIRE_FIXTURES` | unset | `1` panics instead of skipping when datasets absent |

## What to do when it trips

Read the printed `fd_samples` / `rss_samples` series (and the failure message, which
embeds them) to find the iteration where the counter starts climbing.

- **FD-only growth** (RSS flat) ⇒ suspect an unclosed `File`/mmap in the reader
  `open`/drop path — a handle not released when the `SSTableReader` (or an `Arc` it
  holds) is dropped.
- **RSS growth with the cache variant but NOT the no-cache variant** ⇒ suspect a
  cache-entry leak: entries retained past the configured `block_cache.max_size`, or a
  cache not evicting/dropping across reader lifetimes.
- **RSS growth in BOTH variants** ⇒ suspect a buffer/mmap retained across the reader
  drop independent of the cache.

Bisect by iteration: rerun with a smaller `CQLITE_SOAK_ITERATIONS` bracketing the
onset sample to localize the offending code path, then instrument the reader
open/drop (or cache eviction) path.
