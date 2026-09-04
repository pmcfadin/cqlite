# `box-load-frozen.jsonl` — what it is, and what was dropped from it

The box-load timeseries every figure in `ws0-3551-report.md` was judged against, written by the
committed `scripts/perf/ws0_quiescence.py sample-loop` at a 10 s cadence, **outside every
worktree** while the three A/B/C sets ran (a worktree file appended every 10 s trips the gate's
`tree-integrity` check — #2926 — and a worktree is deleted at finalize).

* samples: **1091**
* window: `2026-09-03T01:39:45Z` → `2026-09-03T04:41:37Z`
* this file: 259,855 B

## TWO FIELDS WERE DROPPED, and this file is therefore NOT the sampler's raw output

The live file was **1,255,510 B**, sha256 `2065f4d56a6ac6f777891df75052071b6af3e7045317e84f6f730687291916a4`. It is not committed as-is: the
`ws0-3248-artifacts` precedent for this artifact is 89 KB, and the raw form is ~14x that. Dropped:

* **`percpu`** — the 16-CPU cumulative `/proc/stat` snapshot, which is the bulk of the size. Its
  DERIVED values are preserved: the per-CPU busy column in each `set*/window-census.md` was
  computed from the raw file before trimming.
* **`competing[]`** — the per-process census DETAIL (pid, comm, why, matched argv element,
  cmdline). The `competing_count` and the per-comm counts that the judge actually reads are
  KEPT, so every quiescence verdict in this report re-derives from this file unchanged. The
  detail's own evidence is quoted where it is used: `quiescence/live-reproduction.md` and
  §7 of the report.

## What that costs, stated rather than left to be discovered

`window-census.py` re-run against THIS file reports `NOT MEASURED` in its pinned-CPU column,
because the input no longer carries `percpu`. That is the honest answer for a trimmed input and
not a regression — but it means the per-CPU figures in the committed `window-census.md` outputs
are **not reproducible from this file alone**. Every census verdict (`clean` / `contaminated` /
`undercovered` / `unobserved`) IS, because those depend only on fields that were kept.
