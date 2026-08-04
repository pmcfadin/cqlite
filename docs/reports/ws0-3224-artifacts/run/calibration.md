# Calibration + the staging bug the smoke test caught

## The staging bug (why a smoke test is not optional)

The first smoke run of `capture-endpoint.sh` returned **rc=0** while measuring
**nothing**: `requests_ok=0`, `rows_total=0`, and **2,258,606 `NotFound` errors**.
The server log said it plainly:

```
discovered 0 tables across 0 keyspaces under /data/ws0/ws0-corpus/sstables
```

`cqlite-flight` discovers `<keyspace>/<table>-<uuid>/` directories; the corpus had
been staged **flat** (`sstables/nb-16-big-*`). `common.sh`'s own stage check
(`ls $WS0_STAGE/*Data.db || ls $WS0_STAGE/*/*Data.db`) accepts the flat layout, so
it warned about nothing.

Worse, the capture **passed its own validity checks** with zero rows, because the
occupancy test asked `rows_total % corpus_rows == 0` and `0 % 3999890 == 0`. That
is a vacuously green empty measurement — precisely what CLAUDE.md forbids
("never let a dataset-dependent test pass on an empty dataset; 0-rows-when-present
is a failure"). Both were fixed before any real capture:

- corpus restaged to `sstables/ws0/events-52ff1a008fa211f1ac2485829b296e3f/`,
  `sha256(Data.db)` unchanged at `b1656ae8…41042` (verified after the move);
- `occupancy()` now requires `rows_total > 0` **and** whole scans **and**
  `requests_error == 0` **and** `requests_unavailable == 0` **and**
  `requests_ok > 0`, and the capture **exits non-zero** if any validity gate
  fails (warmth, client saturation, occupancy) instead of writing a green
  `meta.json`.

Confirmed after the fix: `discovered 1 tables across 1 keyspaces`.

## Calibration (S=1/N=2, this host)

`run/calibration-s1-N2.jsonl` is the raw record.

| quantity | measured |
|---|--:|
| requested step | 120 s |
| **actual `duration_s`** | **144.205 s** |
| `requests_ok` / `requests_error` | 8 / **0** |
| `rows_total` | 31,999,120 = **exactly 8 × 3,999,890** |
| `rows_per_s` | 221,900.46 |
| full-scan latency p50 / max | 35,880.96 ms / 36,405.25 ms |

### The property that makes both denominator conventions well-defined

`duration_s` (144.2 s) **exceeds** the requested 120 s: the loadgen holds the step
open until in-flight requests drain. Consequences, both load-bearing for AC4:

1. **A step contains only WHOLE scans.** `rows_total` is an exact multiple of the
   corpus row count — no partial-scan row credit, so no truncation bias.
2. **Occupancy is ~100%.** 8 requests × 35.88 s ÷ 2 workers = 143.5 s against a
   144.2 s step ⇒ busy fraction ≈ 0.995. The workers are never idle, so the
   step-average rate *is* the steady-state rate, which is exactly the condition
   #3217's interior-window convention needs in order to be valid.

This is the property #3217's report never stated, and it is why its S=1/N=2
denominator can be checked rather than merely doubted. Measured here, not assumed.

### Not comparable to #3217, by design

#3217's S=1/N=2 measured 250,046 rows/s at 31.85 s scan latency; this host gives
221,900 rows/s at 35.88 s (~11% slower). Different microarchitecture (Ice Lake-SP
bare metal vs virtualized Sapphire Rapids), so **no absolute is compared across
hosts** — both endpoints are re-measured here, per RUNBOOK.

## Step sizing chosen

`step=120 s` (⇒ ~144 s actual), `settle=20 s`, `interior window=60 s`, `reps=3`.
`settle + window = 80 s` sits strictly inside the ~144 s step, so the interior
window never spills into the drain. reps=3 closes #3217's reps=1 gap.
