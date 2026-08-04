# Issue #3096 — rig-guard audit: did the two measurement-integrity defects contaminate the shipped figures?

> # ⚠ THE RIG AND THE GUARDS AUDITED HERE ARE RE-ANCHORED TO #3272
>
> **Note added 2026-08-03 (owner-ordered split).** The instruments this audit is
> *about* — `scripts/perf/ws0-baseline.sh`, `scripts/perf/ws0_report.py` — and the
> guard that pins their fixes, `scripts/tests/test_ws0_report_guards.sh`, are
> **re-anchored to issue #3272** and are **not part of the #3096 PR**. Where this
> note says the defects "are now fixed and guarded", the fixes and the guard script
> ship under **#3272**, not in this branch; they are not present in a #3096
> checkout, so do not look for them here or treat their absence as a missing
> deliverable.
>
> **No measurement, figure, ratio, superseded-figure label or AC1 statement in this
> directory is changed by that split** — and none is changed by this audit either
> (that is the audit's own finding, below). The re-anchor is a delivery-scope fact
> about the TOOLING, not a revision of the RESULTS. R4's delivery status is
> recorded in `openspec/changes/arrow-encode-doget/tasks.md`.

**Audit result: no.** Two defects were found in the measurement *instruments*
(`scripts/perf/ws0-baseline.sh`, `scripts/perf/ws0_report.py`) after the baseline
session of 2026-08-03 was recorded. Both are now fixed and guarded
(`scripts/tests/test_ws0_report_guards.sh`). This note records the evidence that the
already-published figures in `baseline-2026-08-03.md` / `baseline-results.json` are
unaffected, so a later reader does not have to redo the audit.

**Nothing in this note changes a recorded measurement.** No figure, no
superseded-figure label, and not the AC1 (1.3x) `unmet` status or its re-anchor to
#3248 was altered by the guard work. The audit was performed *on* the committed
artifact, not by re-measuring.

## Defect 1 — the warm BARE-SCAN arm had no untimed prewarm

The Flight arm prewarmed before its perf window; the bare-scan arm did not.
`--setup-only` opens the corpus and ingests the schema but never reads the `Data.db`
pages the scan streams, so on a genuinely cold page cache the first "warm" rep would
have been measured partly cold — and the bare scan is the **denominator** of the
1.3x ratio.

**Evidence the recorded warm figures were genuinely warm** — the per-rep times in
`baseline-results.json` (measurement 0, `bare_scan`/`warm`; measurement 2 is the
cold arm):

| rep | warm secs | vs the warm median (rep 2) |
|-----|-----------|----------------------------|
| 1   | 10.978    | +1.6%                      |
| 2   | 10.807    | — (median, 370,134 rows/s) |
| 3   | 10.750    | −0.5%                      |

A **genuinely cold** rep on the same corpus and pinning took **20.162 s** (cold rep
1) — **+84%** over warm rep 1. Warm rep 1 is 1.6% off the median and 2.1% off the
fastest rep, not 84% off, so its page cache was demonstrably already warm. The
reported warm median came from **rep 2** regardless, so even a partly-cold rep 1
could not have moved it.

Note the shipped `baseline-results.json` carries **no** `prewarm` block for either
arm (that recording landed in a later review round on the same branch): the evidence
above is the rep-time evidence, not a recorded prewarm status. Sessions run after
the fix record `prewarm` / `prewarm_all_ok` per rep for **both** arms, and the
bare-scan arm now fails closed on a prewarm failure — its bias direction (a
partly-cold bare scan reads slower, shrinking `bare/flight` and making the 1.3x
target easier) is the one that could manufacture a win, unlike the Flight arm's.

## Defect 2 — cold Flight reps accepted any successful-request count

`collect_flight` accepted any `requests_ok` for a cold rep. Only the first request
after the cache drop is cold, so requests 2..N would have blended warm rows into a
figure labelled "cold".

**Evidence no blending occurred.** Every recorded cold Flight rep
(`baseline-results.json` measurement 3) has `requests_ok: 1` and `rows: 4,000,000` —
exactly one full scan of the 4,000,000-row corpus. The `requests_ok: 3` reps belong
to the **warm** arm (measurement 1) at `rows: 12,000,000` = 3 x 4,000,000, where
three requests is correct by design.

Re-derivable directly: the new `check_request_count` was run over every recorded
Flight rep of the committed artifact and **accepted all six** (3 warm at 3x, 3 cold
at 1x) — i.e. the session already satisfied the guard that now enforces it.

```bash
python3 - <<'PY'
import json, importlib.util
spec = importlib.util.spec_from_file_location("ws0r", "scripts/perf/ws0_report.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
r = json.load(open("docs/reports/ws0-3096-artifacts/baseline-results.json"))
rows = r["corpus_identity"]["rows"]
for meas in r["measurements"]:
    if not meas["arm"].startswith("flight"):
        continue
    for rep in meas["reps"]:
        m.check_request_count(f"{meas['arm']}-{meas['temperature']}-{rep['rep']}",
                              meas["temperature"], rep["requests_ok"], rep["rows"], rows)
print("every recorded flight rep satisfies the post-fix guard")
PY
```

## Third finding (corpus identity), for completeness

`CorpusIdentity::diff` compared 4 of 15 recorded fields, so a divergence in `seed`,
`table`, `rows_per_partition`, `cells_per_row`, `compression_info_present` or the
recorded caveats could read as "reproduced exactly". That is a guard on **future**
re-generations; it cannot have altered a figure in this session, whose corpus
identity is the one recorded in `corpus-identity.json`
(`data_db_sha256` prefix `4a903f6f`, 4,000,000 rows). The comparison is now
exhaustive by construction (destructured field patterns, so a new field is a compile
error rather than an unchecked one).
