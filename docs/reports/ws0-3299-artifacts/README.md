# #3299 artifacts — what each tree is, and which ones are DATA

Report: [`../ws0-3299-report.md`](../ws0-3299-report.md).

**Read this before using any number from this tree.** Three of the six
directories below are *not* measurements, and two of those hold files that look
exactly like measurements.

| tree | status | what it is |
|---|---|---|
| `sweep/` | ✅ **THE DELIVERABLE** | The 25-point × 3-rep main grid, 75/75 reps, all guards passed, 0 discarded. `CS-table.md` is the derived table. Every AC1/AC2 figure comes from here. |
| `host/` | ✅ evidence | The PMU census taken **on this instance**, with #3224's `cache-hostile` as a positive control, and the AC3-unanswerable verdict it establishes. |
| `harness/` | 🔧 code | The measurement rig, reviewed code (#3229). `README.md` states the aligned-window convention; `selftest.sh` is hermetic (41 guard cases). |
| `smoke/` | ⚠️ **NOT results** | Reduced-grid plumbing proofs and the budget calibration. 1 rep per point. Labelled inside. |
| `sweep-ext-aborted/` | ⛔ **INVALID** | An aborted extension run. Its `perf.csv` files are real and read `100.00% pct_running`, and it is **structurally indistinguishable from valid data** — the invalidity is in the run's DESIGN (1 rep; no contemporaneous incumbent). Every rep directory carries its own `INVALID-DO-NOT-USE.txt`. |
| `freq-calibration/` | 📋 plan only | `PLAN.md` — written, **not run**. No data here. |

## The one rule this tree exists to enforce

**A file that reads `100.00% pct_running` is not thereby a measurement.** This
whole issue is downstream of that: the census found LLC counters that program
cleanly, report `100.00%`, and return a hard zero on a workload that cannot have
zero, and the aborted extension produced counter files that are perfectly valid
bytes from an invalid experiment. In both cases validity is a property of the
*design* and not of the *data*, so it has to be written down beside the data —
which is what every README and marker file in this tree is for.
