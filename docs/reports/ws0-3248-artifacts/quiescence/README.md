# Box-quiescence evidence

`box-load-frozen.jsonl` is a frozen copy of the external load timeseries recorded across every
measurement in this issue. One JSON line per **10 s**: `ts`, `load1/5/15`, the kernel runnable
count, and a per-process census of `rustc`, `cargo`, `perf`, `agent-gate`, `cqlite-flight`,
`flight-loadgen`.

**Why it exists.** The rig's own README states its limit — "this rig produces no reusable absolute"
— after an untouched warm bare scan drifted ~10% in an hour. What the rig does not model is that its
box is **shared between delivery lanes**: `load1` reached **108** on 16 vCPUs during this issue's
preparation, from a peer lane's gate. #3299's admitted gap is the one this closes: its quiescence was
*procedural*, and it "never logged load per rep", so it could not correlate its own ±3% residual
against load even in hindsight.

**Why the census and not the load average.** Counts come from `/proc/<pid>/comm` and
`/proc/<pid>/cmdline`, never `pgrep -f` — a `-f` pattern matches the census command's own cmdline and
inflates the count it is measuring. `pgrep -x` is not the alternative either: the kernel `comm` field
caps at 15 characters, so a longer binary name can never match. And `load1` cannot distinguish this
lane's own load from a competitor's, which is why the **binding** check is the census and `load1` is
context. See `../measurement-method.md` §6.

## Windows covered

| measurement | window (UTC) | verdict |
|---|---|---|
| **AC0** reproduction (release, no profiler) | 16:15:27 – 16:24:14 | **QUIESCENT** — 48 samples, 0 competing |
| **AC1** profile (perfsym + sampling) | 16:47:06 – 16:53:xx | 0 competing across the window |
| codegen control (perfsym, no profiler) | 16:56:22 – 17:0x | 0 competing across the window |
| bytes-touched differential | 17:07:56 – 17:13:xx | 0 competing across the window |

The AC0 verdict record is `../ac0/quiescence-verdict.json`, produced by
`scripts/perf/ws0_quiescence.py judge`, which **refuses** rather than warns.

## Reproducing a verdict from this frozen file

```bash
python3 scripts/perf/ws0_quiescence.py judge \
  --before docs/reports/ws0-3248-artifacts/ac0/quiescence-before.json \
  --after  docs/reports/ws0-3248-artifacts/ac0/quiescence-after.json \
  --timeseries docs/reports/ws0-3248-artifacts/quiescence/box-load-frozen.jsonl \
  --window-start 2026-08-28T16:15:00Z --window-end 2026-08-28T16:25:00Z
```

## Recording a timeseries the judge will accept (#3551)

The **boundary** sampler (`sample`) and the **in-window** judge (`judge --timeseries`) use
different schemas, and until #3551 no committed subcommand produced the second one — so the two
halves of this one gate did not compose. Use `sample-loop`:

```bash
python3 scripts/perf/ws0_quiescence.py sample-loop \
  --out /data/ws0-<issue>/sampler/box-load.jsonl   # OUTSIDE every worktree; see below
# optional: --cadence 10 (default SAMPLER_CADENCE_S), --samples N (0 = until signalled)
```

Detach it (`setsid`/`nohup`) so it outlives the session that started it, and judge against the
file it appends. One JSON object per line, flushed per tick, so `tail -f` works. Every record
carries `ts`, flat `load1/load5/load15`, `runnable`, the authoritative `competing_count`, a
count per census rule (`rustc`, `cargo`, `cc1`, `cc1plus`, `ld`, `lld`, `mold`, `gate`), the
census entries themselves — each naming the **argv element that matched** — and a per-CPU
`/proc/stat` jiffy snapshot. All of it comes from the **same `census()`** the boundary sampler
uses, so the two halves cannot disagree about what "competing" means.

`--out` is **required and has no default**, and is **refused inside any git worktree**: see the
two reasons in the next section, both learned by hitting them. It is also refused when worktree
membership cannot be *measured* — an unmeasurable answer is not a clean one.

## What a ZERO CENSUS bounds, and what it does not (#3551)

Measured 2026-09-02 on the delivery box: **91 consecutive samples reported
`competing_count=0` while `load1` reached 6.39 with 9 runnable tasks**, and the four CPUs the
measurement pins measured a **median 8% and a max 86% busy** with foreign work under that zero
census. `COMPETING_COMMS` is compilers and linkers plus one named script, so a peer lane running
node, jest, python, git or a shell suite is **invisible** to it, and in-window `load1` is
explicitly context rather than a gate.

The census is **not** widened in response — this repo has the measurement for why (including
`sccache` "refused a perfectly quiet box", and a guard that cries wolf on the normal state of
every box is the guard people delete). Instead every verdict now carries a `census_scope` field
that says so in words, derived from the record's own sample count so it cannot be softened, and
the per-CPU snapshot makes the contamination visible. **Read `census_scope` and the per-CPU
column before treating a QUIESCENT verdict as "the box was idle".**

## Note on where the LIVE sampler runs, which is not here

The live sampler writes **outside any worktree** (`/data/ws0-3248/sampler/box-load.jsonl`), for two
reasons that were both learned by hitting them:

1. **A worktree file appended every 10 s trips the gate's `tree-integrity` check mid-run** (#2926).
   The first version of this instrument lived in the worktree and failed a `--lite` gate with
   `tree-mutated-midrun; changed: .ws0-3248/box-load.jsonl` — the guard working exactly as intended,
   on an instrument that had no business being inside the tree.
2. **A worktree is deleted at finalize.** A sampler meant to outlive this issue (requested by the
   coordination lead for #3389, so the next WS0 lane inherits the per-rep load column #3299 could not
   produce) cannot live somewhere that gets removed when the issue closes.
