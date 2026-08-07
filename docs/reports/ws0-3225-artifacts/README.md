# `ws0-3225-artifacts` — the #3225 §2 measurement rig

These are **reviewed code**, not docs (CLAUDE.md): the PR carrying them is not a docs-only
change and must be roborev-certified.

What this round measures (`openspec/changes/concurrency-admission-defaults/design.md` D7):
the peak-`N` concurrency curve at five server widths, with the **shipped default 64 inside
the ramp**, so `clamp(2 × P, 2, 64)` can be evaluated against a measured peak per width
instead of against two uncensored points. §2 **gates** §3 — do not ship the default first.

## Layout

| Path | What |
|---|---|
| `rig/verify-rig.sh` → `rig/rig-verification.txt` | CPU/topology/SMT/NUMA read from sysfs, competing load, `/data` capacity, and a **live demonstration** that `sweep.sh`'s server/client core-overlap refusal fires. **Exits non-zero** if any demonstrated guard does not hold. |
| `corpus/regen-corpus.sh` | Regenerates the `ws0.events` corpus (path-parameterized adaptation of #3026's `gen-corpus.sh`). ~2.5 min. |
| `corpus/compare-geometry.py` → `corpus/corpus-geometry.txt` | Field-by-field geometry vs #3217, every number **parsed** from a committed artifact. Exits non-zero on a material divergence. |
| `corpus/corpus-provenance.txt` | How the corpus was made, and every deviation from #3217's recipe. |
| `run/run-3225.sh` | The five-arm sweep driver. Restartable per arm. |
| `run/analyze-3225.py` (+ `analyze_3225_render.py`, `analyze_3225_validity.py`) | Peak-N by width, over-admission cost in both currencies, formula deviation, three byte bases, and the three fail-closed validity checks: corpus identity (per-arm content digest), admission ceiling vs ramp, evidence completeness. |

Reused **unchanged** from `../ws0-3217-artifacts/harness/`: `common.sh`, `sweep.sh`,
`emit-point.py`, `summarize-sweep.py`, `corpus-basis.py`, `selftest.sh`.
**Not run**: anything under `../ws0-3217-artifacts/partB-run/`, and the
`profile-*` / `classify-offcpu` / `runqlat` attribution chain — this round measures a
curve, and skipping them also drops the `perf_event_paranoid` / `kptr_restrict`
symbolization dependency.

## Launching the sweep

```bash
cd /home/ubuntu/workspace/cqlite-wt/issue-3225
nohup bash docs/reports/ws0-3225-artifacts/run/run-3225.sh \
  > /data/ws0/logs/run-3225.log 2>&1 < /dev/null &
```

~1 h per arm, **~5–6 h** for all five. Progress ledger:
`/data/ws0/logs/driver/run-3225-progress.txt`. Per-arm stdout:
`/data/ws0/logs/driver/<arm>.out`. Results: `/data/ws0/results/<arm>/`
(`points.jsonl`, `summary.{json,txt}`, `run-config.json`, `cpu-topology.json`,
`corpus-basis.json`). `--list` prints the plan without running anything.

Prerequisites the script checks and **fails closed** on: a staged corpus under
`/data/ws0/ws0-h2h/datasets/sstables`, executable `target/release/{cqlite-flight,flight-loadgen}`,
the committed ticket template, **no** running Cassandra daemon, **no** already-running
`cqlite-flight`.

## Resuming after a crash

Re-run the **same command**. Every point is written to `points.jsonl` the moment it
completes and each arm writes `summary.json` when it finishes, so:

- an arm with a valid `summary.json` is **SKIPPED** — completed arms are never redone
  and never lost;
- an arm with `points.jsonl` but no `summary.json` is **QUARANTINED** to
  `<arm>.partial-<utc>` and re-run from rep 1.

The quarantine is deliberate. `sweep.sh` always starts at rep 1 and *appends*, so
resuming into an existing `points.jsonl` would silently mix a truncated first attempt
with a second and corrupt every per-`N` median. Redoing a partial arm costs ~1 h; a
silently doubled arm costs the result's credibility.

## Analysing

```bash
python3 docs/reports/ws0-3225-artifacts/run/analyze-3225.py /data/ws0/results
```

Writes `analysis-3225.{json,txt}` into the results dir (`-o` to redirect).
`--smoke` runs the same pipeline against #3217's **committed** `points.jsonl` and
cross-checks the recomputed medians against #3217's own `partA-analysis.json`
(29/29 exact at time of writing), so the analyser is known-executable before the
6-hour run produces the data it is meant to read.

## Two things to know before reading the output

- **`server_physical_cores_S` is `null` for the S=3 arm.** `sweep.sh` only stamps it for
  its `s1|s2|s4|s6` shorthands, and S=3 goes through the literal CPU-list form
  (`0-2,8-10`) — which is exactly what design D7 said needs no `sweep.sh` change.
  `analyze-3225.py` re-derives `S` from that arm's own `cpu-topology.json` sibling groups
  intersected with `server_cpus`, records **which method it used**, and reports an
  unresolvable arm as `UNRESOLVED` rather than guessing.
- **`--max-concurrent-scans` is raised to 64 for the sweep.** `common.sh` defaults it to
  16, at which every `N > 16` point would measure the admission gate rather than the
  concurrency curve. 64 is both the shipped default and the top of the ramp, so the ceiling
  could not bind — and *that* is the evidence: `analyze-3225.py`'s `admission_ceiling` check
  reads `--max-concurrent-scans` out of every point's `server_flags` and compares it against
  the largest N that arm drove, failing closed if any arm's ceiling is lower. `run-3225.sh`
  refuses to start when an inherited `WS0_MAX_CONCURRENT_SCANS` is below `max(ramp)`.
  **The rejection count cannot substitute for that check.** With
  `--admission-wait-timeout-ms 30000` an over-ceiling request WAITS for a permit and then
  SUCCEEDS, so `requests_unavailable` stays 0 while the curve is throttled. A non-zero total
  still proves the ceiling bound; zero proves nothing on its own.
- **Corpus identity is verified by a per-arm CONTENT digest, not by matching metadata.**
  `run-3225.sh` stamps `data_db_sha256` into each arm's `corpus-basis.json`, measured
  immediately before and after that arm. The analyzer requires every arm to carry one,
  requires them to agree with each other and with `corpus/corpus-sha-staged.txt`, and treats
  a MISSING digest as `UNVERIFIED` — explicitly not a pass, and a non-zero exit. The six arms
  committed under `results/` predate the stamp, so they report `UNVERIFIED`; that is the
  honest state, and it cannot be repaired retroactively (a digest taken now records now's
  bytes).
