# `ws0-3551-artifacts/` — index

Report: [`../ws0-3551-report.md`](../ws0-3551-report.md).

**Read a set's `window-census.md` BEFORE its `AGGREGATE.md`.** Which sessions were contaminated
is a precondition for reading any figure, not a footnote — and no session in this issue carries an
*in-run* quiescence verdict (§9 residual 1 explains why, and what that costs).

| path | what it is |
|---|---|
| `clean-pairs.md` | **the primary estimate.** Pooled CLEAN within-round pairs across all three sets |
| `clean-pairs.py` | the tool that produces it. Imports the committed judge's `MAX_SAMPLE_GAP_S` rather than restating it |
| `window-census.py` | judges each session's window post hoc from the frozen timeseries |
| `set1/` | 4 arms x 3 rounds, **12 of 12 sessions clean** — no arm D |
| `set2/` | 5 arms x 3 rounds, 6 of 15 clean (a peer lane's gate) — first arm D data |
| `set3/` | 5 arms x 3 rounds, 12 of 15 clean |
| `set*/AGGREGATE.md` | that set's own control table, Layer 1, Layer 2 and configuration read back from its recorded pinning |
| `set*/window-census.md` | per-session census verdict + the pinned-CPU column (which is TOTAL busy, **not** a contamination bound) |
| `set*/abc-run.json` | that set's run fingerprint: corpus sha, binary digests, per-arm flag lists |
| `quiescence/box-load-frozen.jsonl` | the timeseries every verdict was judged against — **trimmed**, see its README |
| `quiescence/box-load-frozen.README.md` | what was dropped from it, why, and what that costs |
| `quiescence/live-reproduction.md` | the live reproduction of all three quiescence defects |

## Reproducing the verdicts from what is committed here

```bash
python3 docs/reports/ws0-3551-artifacts/window-census.py \
  --root <a set directory> \
  --timeseries docs/reports/ws0-3551-artifacts/quiescence/box-load-frozen.jsonl
```

**Verified, not asserted:** all 42 session verdicts re-derive identically from the committed
trimmed timeseries. The pinned-CPU column does not — it needs the `percpu` field that was dropped
for size, and the tool correctly reports `NOT MEASURED` for it rather than inventing a number.

The `set*/` roots themselves are NOT committed: each is ~300–380 MB of server logs and perf CSVs,
and they lived at `/data/ws0-3551/{abc,abcd,abcd2}` on `ip-172-31-7-163`. What is committed is
every derived table plus the fingerprints and digests that say what produced them.

## The guard suite

`scripts/tests/test_ws0_3551_artifact_tools.sh` (86 checks, in the gate's `tooling-tests`) covers
the two tools here. It exists because they had **no tests when they produced these figures** and
roborev found a real coverage defect in them; the figures were re-derived under it afterwards,
unchanged.
