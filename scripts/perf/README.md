# `scripts/perf/` — the issue #3096 Arrow-encode measurement rig

Runnable end to end from a clean checkout. No path outside the repository, no
uncommitted helper.

```bash
# 1. corpus (~2.8 GB of scratch; corpus binaries are never committed)
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out /data/ws0-3096

# 2. both arms, one session, one verified physical-core sibling pair
scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096
```

| file | role |
|---|---|
| `ws0-baseline.sh` | the driver: both arms, warm+cold, median of N, fail-closed pinning |
| `lib-cpu.sh` | `thread_siblings_list` verification — the pinning is READ, never assumed |
| `ws0_report.py` | aggregation → `results.json` + a human summary |

Full method, the traps, the recorded pinning and the residual caveats:
**`docs/reports/ws0-3096-artifacts/measurement-method.md`** — read it before
believing any number this rig prints.

Non-negotiables baked into the scripts (issue #3096 spec R1/R2):

* CPU-wide `perf stat -C <cpu-list>`; **never** `perf stat -p` (>2x observer
  cost). `ws0-baseline.sh` greps itself for a `-p` form and refuses to run.
* `taskset` to a **verified** physical-core sibling pair; a non-sibling request
  fails closed rather than silently measuring two different cores.
* **rows/s AND cycles/row**, never a CPU-share.
* **Warm and cold are separate claims**, never blended.
* Setup subtracted from the cycles/row denominator; the row denominator printed
  beside every figure.
* A rep that observes **zero rows exits non-zero** rather than reporting a
  measurement.
