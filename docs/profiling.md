# Profiling CQLite: tools, workflow, and the recursive-improvement loop

This page is the operational companion to [performance.md](performance.md).
That page explains *what the CI perf gate enforces*; this one explains *how to
find and fix the bottlenecks* the gate measures — and how to keep doing it
iteratively with auditable output.

The entry point for everything below is one script:

```bash
./scripts/profile.sh help
```

---

## 1. Profiler selection: what works on this project, and where

CQLite is an async (Tokio) Rust workspace that is exercised through Criterion
benches over real SSTable fixtures. Profilers were selected against three
constraints:

1. **Containers/CI without `perf`.** Dev containers and CI runners typically
   have `perf_event_paranoid=2` and no `perf` binary, so anything built on
   `perf_event_open` is out for the default loop.
2. **Symbolized stacks.** The workspace `release` profile sets `strip = true`,
   so profiling must run under the `bench` profile (`debug = true`), which
   Criterion uses automatically.
3. **The 128 MiB memory target.** CPU time alone is not enough; the project
   has an explicit peak-heap budget (CLAUDE.md), so heap profiling is a
   first-class concern, not an afterthought.

### Integrated (work everywhere, including this repo's dev containers)

| Tool | Measures | How it's wired in |
|------|----------|-------------------|
| **Criterion** (existing) | wall time, throughput, regression deltas | `cqlite-core/benches/{read,write,partition_lookup,m1_performance}.rs`, gated by `benches/perf-gate.json` |
| **pprof-rs** (`pprof` crate) | CPU call stacks → flamegraph SVG per bench | attached to every bench via `benches/profiling/mod.rs`; activates only with `--profile-time` |
| **dhat-rs** (`dhat` crate) | allocation counts, churn, peak heap vs 128 MiB budget | `cqlite-core/examples/heap_profile.rs` behind the `dhat-heap` feature |

pprof samples via `SIGPROF`/`setitimer` in-process — no `perf` binary, no
elevated `perf_event_paranoid`, no container privileges needed. dhat is a
global-allocator wrapper, so it works on any target.

### Optional (local machines, deeper dives)

| Tool | When to reach for it | Notes |
|------|----------------------|-------|
| `cargo flamegraph` / `perf` | local Linux with `perf` installed | kernel + user stacks; needs `perf_event_paranoid ≤ 1` for kernel frames |
| `samply` | local Linux/macOS | Firefox Profiler UI; great for timeline views of the Tokio runtime |
| Instruments (Time Profiler / Allocations) | macOS | best-in-class UI; use on Apple-silicon dev machines |
| Valgrind DHAT / callgrind | available in the dev container (`valgrind` is installed) | deterministic instruction counts; very slow but noise-free — useful when criterion deltas are within noise |
| `iai-callgrind` | possible future CI addition | instruction-count benchmarking on top of callgrind; immune to shared-runner noise |
| `tokio-console` | suspected async stalls (tasks blocked, not CPU) | needs `tokio_unstable` + instrumentation; reach for it when flamegraphs show idle time |

Rule of thumb: **flamegraph wide frames** → CPU bottleneck (decode, hashing,
copies); **fast CPU but slow wall time** → I/O or async scheduling (check
`write/ingest_wal_on` vs `_off`, consider `tokio-console`); **slow and
allocation-heavy in dhat** → memory churn (clones, `Vec` growth,
`String`/`Bytes` copies — see the `rust-patterns` zero-copy guidance).

---

## 2. The workloads being profiled

Profiling reuses the deterministic Criterion benches (Epic #541) — same
fixtures, same seeded RNG, so two profile runs are comparable:

- `read/point_lookup`, `read/clustering_slice`, `read/full_scan`,
  `read/type_heavy` — the read path: open → index → decode (collections
  isolated by `type_heavy`).
- `write/ingest_wal_off` (CPU-bound, strictly gated), `write/ingest_wal_on`
  (fsync-bound, advisory), `write/flush` — the write engine.
- `partition_lookup/*` — Index.db lookup micro-benches.

The heap harness (`examples/heap_profile.rs`) runs the `full_scan` and
`type_heavy` workloads under dhat and verdicts peak heap against the 128 MiB
budget.

---

## 3. The recursive-improvement loop

Every iteration produces machine-readable artifacts under `target/profiling/`,
so each round starts from the previous round's output instead of folklore.

```
        ┌────────────────────────────────────────────────────────┐
        ▼                                                        │
  1. baseline      ./scripts/profile.sh baseline                 │
  2. profile       ./scripts/profile.sh flame   (CPU)            │
                   ./scripts/profile.sh heap    (allocations)    │
  3. diagnose      ./scripts/profile.sh report  → report.md      │
                   pick the ONE widest frame / worst bench       │
  4. fix           one targeted change (zero-copy, fewer clones, │
                   better data structure, skip redundant work)   │
  5. verify        ./scripts/profile.sh bench                    │
                   ./scripts/profile.sh compare current base     │
  6. record        ./scripts/profile.sh report  → history.jsonl ─┘
                   improvement confirmed? re-run `baseline`, commit,
                   and let the CI gate (perf-regression.yml) hold the line
```

### Artifacts per iteration

| File | Purpose |
|------|---------|
| `target/criterion/<group>/<bench>/profile/flamegraph.svg` | where CPU time goes, per bench |
| `target/profiling/dhat-heap.json` | full allocation profile (open in the [dhat viewer](https://nnethercote.github.io/dh_view/dh_view.html)) |
| `target/profiling/heap-summary.json` | peak heap + budget verdict, machine-readable |
| `target/profiling/report.json` | ranked bench table with deltas — input for the *next* iteration (or an agent driving it) |
| `target/profiling/report.md` | the same, human-readable |
| `target/profiling/history.jsonl` | append-only ledger: one line per iteration with git rev, medians, peak heap |

`history.jsonl` is what makes the loop *recursive* rather than one-shot: it
survives baseline overwrites, so you can always answer "did the last five
changes actually compound?" with `git rev` precision.

### Exit criteria for a round of optimization

Stop iterating when any of these hold:

- the top frames in the flamegraph are irreducible work (decompression,
  checksums, kernel I/O) rather than cqlite code;
- the strict benches improve by less than their gate threshold (10%) two
  rounds in a row — you are now tuning noise;
- peak heap is comfortably inside the 128 MiB budget and allocation churn is
  dominated by row materialization the API contract requires.

Then lock in the wins: re-save the baseline, update
`cqlite-core/benches/perf-gate.json` thresholds if a bench got dramatically
faster (a 10% regression on a 2× faster bench is still a win worth gating),
and let `perf-regression.yml` enforce it on every PR.

---

## 4. Command reference

```bash
# one-time setup: binary SSTable fixtures
bash test-data/scripts/fetch-datasets.sh

# 1. baseline on a clean tree
./scripts/profile.sh baseline

# 2a. CPU flamegraphs (all benches, 10 s sampling each)
./scripts/profile.sh flame
# ... or a single bench, 30 s
./scripts/profile.sh flame read/full_scan 30

# 2b. heap profile + 128 MiB budget verdict
./scripts/profile.sh heap

# 3. ranked bottleneck report (+ history ledger)
./scripts/profile.sh report

# 5. after a change: re-measure and gate-check against the baseline
./scripts/profile.sh bench
./scripts/profile.sh compare            # current vs base, same thresholds as CI
```

Raw equivalents, if you need to bypass the script:

```bash
# flamegraph for one bench
cargo bench -p cqlite-core --features cli-helpers --bench read -- --profile-time 10 read/full_scan

# heap profile (bench profile keeps symbols; release strips them)
cargo run -p cqlite-core --example heap_profile --features cli-helpers,dhat-heap --profile bench

# gate check (same script CI uses)
python3 scripts/ci/check_perf_regression.py target/criterion current base cqlite-core/benches/perf-gate.json
```

---

## 5. Practices (and pitfalls) for trustworthy profiles

- **Profile the `bench` profile only.** It is optimized like release but keeps
  debug info. `--release` builds strip symbols (`strip = true`) and produce
  unreadable flamegraphs; `dev` builds profile the compiler's laziness, not
  your code.
- **Change one thing per iteration.** The report ranks benches by delta; two
  simultaneous changes make the ledger unattributable.
- **Trust deltas, not absolute numbers.** Same machine, same session, new vs
  base — exactly the relative-comparison model the CI gate uses
  (see performance.md §2).
- **`write/ingest_wal_on` is advisory for a reason.** Its time is fsync; a
  flamegraph will correctly show almost no cqlite frames. Optimize
  `ingest_wal_off` for CPU and treat `_on` as a durability-cost probe.
- **Watch allocation churn, not just peak.** `total_bytes_allocated` in
  `heap-summary.json` catches clone-storms that GC-less Rust hides from peak
  numbers but pays for in allocator time — they show up as `malloc`/`memcpy`
  frames in the CPU flamegraph too.
- **Keep the no-heuristics mandate (Issue #28).** An optimization that guesses
  instead of reading authoritative metadata is a correctness regression, not a
  performance win.
